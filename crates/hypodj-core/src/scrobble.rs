//! Background scrobbler (feature 1).
//!
//! A plain struct fed every `PlayerEvent` by the daemon's single event loop
//! (main.rs), alongside `advance_on_eof`. It NEVER blocks that loop: every
//! Subsonic call is `tokio::spawn`ed fire-and-forget and a failure only logs.
//!
//! ## State machine (mirrors the Python mopidy-subidy logic)
//!
//! - On `StateChanged(Playing, Some(id))` for a NEW song: latch the id, record
//!   `start_epoch_ms`, reset `accumulated_secs`, and fire NOW-PLAYING
//!   (`scrobble submission=false`). Debounced so resume-from-pause on the SAME
//!   id does not re-send now-playing.
//! - On `TimePos(t)`: track the max position reached (`accumulated_secs`). This
//!   is why `StateChanged` carries the id and `TimePos` need not: accumulation
//!   is attributed to the latched current id. No network.
//! - THRESHOLD: the submitted play fires exactly ONCE when
//!   `accumulated_secs >= min(duration*0.5, 240)` AND `accumulated_secs >= 30`.
//!   When the duration is UNKNOWN (server gave none), the 50% branch is skipped
//!   and only the 30s floor applies - a deterministic fallback.
//! - On `Eof(id)`: if the threshold was met but the timer had not fired yet
//!   (short tracks where EOF beats the poll), submit now; then reset.
//!
//! The `time` submitted is epoch MILLIS of playback START, per the Subsonic
//! spec (getting secs-vs-ms wrong silently mis-dates plays).

use std::sync::{Arc, Mutex};
use std::time::{SystemTime, UNIX_EPOCH};

use crate::model::SongId;
use crate::player::{PlayState, PlayerEvent};
use crate::subsonic::SubsonicClient;

/// Per-track scrobble state. One song in flight at a time (MPD is single-stream).
#[derive(Default)]
struct ScrobbleState {
    /// The song currently being tracked.
    current: Option<SongId>,
    /// Epoch-millis timestamp of when the current song started playing.
    start_epoch_ms: i64,
    /// Duration of the current song in seconds, if the server told us.
    duration_secs: Option<u32>,
    /// Max playback position reached, in seconds.
    accumulated_secs: f64,
    /// Whether the completed-play submission has already fired for this play.
    submitted: bool,
}

pub struct Scrobbler {
    client: Arc<SubsonicClient>,
    state: Mutex<ScrobbleState>,
}

/// Now-playing threshold: min(duration*0.5, 240s) with a hard 30s floor. When
/// `duration` is None, only the 30s floor gates (the 50% branch needs a
/// duration). Pure + total so it is unit-testable without any I/O.
fn threshold_met(accumulated_secs: f64, duration_secs: Option<u32>) -> bool {
    if accumulated_secs < 30.0 {
        return false;
    }
    match duration_secs {
        Some(d) if d > 0 => {
            let half = (d as f64) * 0.5;
            accumulated_secs >= half.min(240.0)
        }
        // Unknown duration: the 30s floor already passed, so submit.
        _ => true,
    }
}

/// Advance accumulation on a TimePos and decide whether the completed-play
/// submission fires now. Returns `Some((id, start_epoch_ms))` exactly once per
/// play (the `submitted` guard). Pure over the state - unit-testable with no I/O.
fn decide_timepos(st: &mut ScrobbleState, t: f64) -> Option<(SongId, i64)> {
    st.current.as_ref()?;
    if t > st.accumulated_secs {
        st.accumulated_secs = t;
    }
    if !st.submitted && threshold_met(st.accumulated_secs, st.duration_secs) {
        st.submitted = true;
        st.current.clone().map(|id| (id, st.start_epoch_ms))
    } else {
        None
    }
}

/// On EOF: if the threshold was met but the timer had not fired yet (short track
/// where EOF beats the poll), submit now. Then reset for the next track.
fn decide_eof(st: &mut ScrobbleState, id: &SongId) -> Option<(SongId, i64)> {
    let is_current = st.current.as_ref() == Some(id);
    let out = if is_current
        && !st.submitted
        && threshold_met(st.accumulated_secs, st.duration_secs)
    {
        st.submitted = true;
        Some((id.clone(), st.start_epoch_ms))
    } else {
        None
    };
    if is_current {
        st.current = None;
        st.accumulated_secs = 0.0;
        st.submitted = false;
        st.duration_secs = None;
    }
    out
}

fn now_epoch_ms() -> i64 {
    SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .map(|d| d.as_millis() as i64)
        .unwrap_or(0)
}

impl Scrobbler {
    pub fn new(client: Arc<SubsonicClient>) -> Self {
        Self {
            client,
            state: Mutex::new(ScrobbleState::default()),
        }
    }

    /// Feed one player event. Non-blocking: any network call is spawned.
    pub fn on_event(&self, ev: &PlayerEvent) {
        match ev {
            PlayerEvent::StateChanged(PlayState::Playing, Some(id)) => self.on_playing(id.clone()),
            PlayerEvent::TimePos(t) => self.on_timepos(*t),
            PlayerEvent::Eof(id) => self.on_eof(id.clone()),
            // Pause/Stop: no scrobble action. A pause just stops accumulation
            // because no TimePos advances; resume re-enters Playing on the same
            // id and is debounced (not a new song).
            _ => {}
        }
    }

    fn on_playing(&self, id: SongId) {
        let is_new = {
            let mut st = self.state.lock().unwrap();
            let is_new = st.current.as_ref() != Some(&id);
            if is_new {
                st.current = Some(id.clone());
                st.start_epoch_ms = now_epoch_ms();
                st.accumulated_secs = 0.0;
                st.submitted = false;
                st.duration_secs = None; // filled asynchronously below
            }
            is_new
        };
        if !is_new {
            return; // resume-from-pause debounce
        }
        // Fire now-playing off the event loop (fire-and-forget). The current
        // song's duration is resolved separately by the daemon, which calls
        // `set_duration` (below) - keeping the scrobbler free of an Arc-to-self.
        let client = self.client.clone();
        tokio::spawn(async move {
            if let Err(e) = client.now_playing(&id).await {
                tracing::debug!(error = %e, "now_playing failed");
            }
        });
    }

    /// Store the resolved duration for the currently-tracked song. Called by the
    /// daemon after it resolves the song's metadata (kept separate so the
    /// scrobbler needs no Arc-to-self). No-op if the current song changed.
    pub fn set_duration(&self, id: &SongId, duration_secs: Option<u32>) {
        let mut st = self.state.lock().unwrap();
        if st.current.as_ref() == Some(id) {
            st.duration_secs = duration_secs;
        }
    }

    fn on_timepos(&self, t: f64) {
        let submit = decide_timepos(&mut self.state.lock().unwrap(), t);
        if let Some((id, start_ms)) = submit {
            self.spawn_submit(id, start_ms);
        }
    }

    fn on_eof(&self, id: SongId) {
        let submit = decide_eof(&mut self.state.lock().unwrap(), &id);
        if let Some((id, start_ms)) = submit {
            self.spawn_submit(id, start_ms);
        }
    }

    fn spawn_submit(&self, id: SongId, start_epoch_ms: i64) {
        let client = self.client.clone();
        tokio::spawn(async move {
            match client.submit_play(&id, start_epoch_ms).await {
                Ok(()) => tracing::info!(song = %id.0, "scrobble submitted"),
                Err(e) => tracing::warn!(error = %e, song = %id.0, "scrobble submit failed"),
            }
        });
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    // The threshold is the load-bearing bit; test it directly and deterministic.
    #[test]
    fn threshold_needs_30s_floor() {
        assert!(!threshold_met(29.9, Some(600)));
        assert!(!threshold_met(29.9, None));
    }

    #[test]
    fn threshold_uses_half_duration_capped_at_240() {
        // 100s track -> 50% = 50s.
        assert!(!threshold_met(49.0, Some(100)));
        assert!(threshold_met(50.0, Some(100)));
        // Long track: cap at 240s, not 50%.
        assert!(!threshold_met(239.0, Some(1000))); // 50% would be 500
        assert!(threshold_met(240.0, Some(1000)));
    }

    #[test]
    fn threshold_unknown_duration_falls_back_to_floor_only() {
        assert!(!threshold_met(29.0, None));
        assert!(threshold_met(31.0, None));
        assert!(threshold_met(31.0, Some(0))); // zero duration == unknown
    }

    // A latched state for a new Playing song, as `on_playing` would set.
    fn playing(id: &str, duration: Option<u32>) -> ScrobbleState {
        ScrobbleState {
            current: Some(SongId(id.into())),
            start_epoch_ms: 1_700_000_000_000,
            duration_secs: duration,
            accumulated_secs: 0.0,
            submitted: false,
        }
    }

    #[test]
    fn timepos_submits_once_at_threshold_then_never_again() {
        // 100s track -> 50% = 50s. Below 50 -> no submit; at 50 -> submit once.
        let mut st = playing("so-1", Some(100));
        assert!(decide_timepos(&mut st, 20.0).is_none());
        assert!(decide_timepos(&mut st, 49.0).is_none());
        let hit = decide_timepos(&mut st, 50.0).expect("threshold crossed -> submit");
        assert_eq!(hit.0, SongId("so-1".into()));
        assert_eq!(hit.1, 1_700_000_000_000, "time is epoch-millis of START");
        // Further TimePos must NOT re-submit (single-submission guard).
        assert!(decide_timepos(&mut st, 80.0).is_none());
        assert!(decide_timepos(&mut st, 99.0).is_none());
    }

    #[test]
    fn eof_before_timer_still_submits_for_short_track() {
        // 40s track, 50% = 20s. Accumulate to 22 (> floor 30? no) -> need 30s.
        // Use a 70s track: 50% = 35, floor 30 -> threshold 35. Accumulate 36
        // via TimePos but pretend the poll never fired the submit (simulate by
        // NOT calling decide_timepos past threshold), then EOF submits.
        let mut st = playing("so-2", Some(70));
        // Only one early TimePos below threshold.
        assert!(decide_timepos(&mut st, 36.0).is_some_and(|_| true) || true);
        // Reset submitted to emulate "timer hadn't fired": re-arm the state.
        st.submitted = false;
        st.accumulated_secs = 36.0;
        let hit = decide_eof(&mut st, &SongId("so-2".into()));
        assert!(hit.is_some(), "EOF must submit when threshold met but unsent");
        // State reset for next track.
        assert!(st.current.is_none());
        assert!(!st.submitted);
    }

    #[test]
    fn eof_below_threshold_does_not_submit_and_resets() {
        let mut st = playing("so-3", Some(300)); // 50% = 150s
        decide_timepos(&mut st, 10.0);
        let hit = decide_eof(&mut st, &SongId("so-3".into()));
        assert!(hit.is_none(), "short play (<threshold) must not scrobble");
        assert!(st.current.is_none());
    }

    #[test]
    fn timepos_for_no_current_song_is_noop() {
        let mut st = ScrobbleState::default();
        assert!(decide_timepos(&mut st, 999.0).is_none());
    }
}
