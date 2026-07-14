//! Audio player abstraction.
//!
//! FOUNDATION - and this is the load-bearing foundation decision the persona
//! critiques all converged on, so it is settled NOW rather than after the MPD
//! server is built on top of it.
//!
//! ## Why an actor, not a `&mut self` trait
//!
//! The chosen backend is `libmpv2` (wraps libmpv - see README/deps rationale).
//! `libmpv2::Mpv` is NOT freely `Send`/`Sync`, and mpv's event model is a
//! pull-based blocking `wait_event` loop that must be drained on every wakeup or
//! its ring buffer overflows and drops events. The natural shape is therefore a
//! dedicated OS thread that owns the `Mpv` handle, receiving commands over a
//! channel and pushing events back out.
//!
//! So the public boundary is a cloneable [`PlayerHandle`]:
//!   - commands go over an `mpsc` and get a `oneshot` reply (so callers `&self`,
//!     never `&mut self`; the MPD server holds a clone per connection while the
//!     real state lives behind the actor - MPD state is SHARED across clients);
//!   - playback state is a `watch::Receiver<PlayState>` (cheap, always-current
//!     snapshot, no round-trip);
//!   - an event stream ([`PlayerEvent`]: time-pos / eof / state) flows back out.
//!     This is a FOUNDATION concern, not a later detail: eof + time-pos are
//!     exactly what drives queue-advance and the scrobble trigger (feature 1).
//!
//! [`NullPlayer::spawn`] is a REAL working actor over this exact boundary
//! (headless / tests), which proves the handle contract before mpv lands. The
//! Phase-1 `MpvPlayer` slots in behind the SAME [`PlayerHandle`] by swapping the
//! actor body for the mpv thread; nothing above it changes.

use crate::model::SongId;
use tokio::sync::{mpsc, oneshot, watch};

#[derive(Debug, Clone, Copy, PartialEq, Eq, Default)]
pub enum PlayState {
    #[default]
    Stopped,
    Playing,
    Paused,
}

#[derive(Debug, thiserror::Error)]
pub enum PlayerError {
    #[error("player backend: {0}")]
    Backend(String),
    #[error("player actor is gone")]
    Gone,
}

/// Events pushed OUT of the player actor. In the mpv impl these originate from
/// mpv's `time-pos` / `end-file` observations. The MPD server + a scrobble
/// driver subscribe to this stream.
#[derive(Debug, Clone)]
pub enum PlayerEvent {
    /// Current playback position, seconds. Emitted periodically while playing.
    TimePos(f64),
    /// The current track finished (natural end). Triggers queue-advance +
    /// scrobble submission.
    Eof(SongId),
    /// Play state changed (e.g. paused, stopped). Carries the id of the song the
    /// state applies to so the scrobbler is self-describing: it can start a
    /// now-playing on a NEW Playing id and attribute later `TimePos` to that
    /// latched id (TimePos itself is id-less). `None` on Stop (no current song).
    StateChanged(PlayState, Option<SongId>),
}

/// Commands sent INTO the player actor. Each carries a `oneshot` for its reply,
/// so the caller-facing methods on [`PlayerHandle`] are `&self` + `await`.
///
/// `allow(dead_code)`: `PlayUrl.url` and `SetVolume.vol` are part of the locked
/// command contract but the headless `NullPlayer` actor ignores them; the mpv
/// actor (next-phase) reads both (loadfile(url) / set_property("volume", vol)).
#[allow(dead_code)]
enum PlayerCommand {
    PlayUrl {
        /// The song id to attribute playback to, or `None` for a raw stream
        /// (internet radio) that must never be scrobbled.
        song: Option<SongId>,
        url: String,
        reply: oneshot::Sender<Result<(), PlayerError>>,
    },
    Pause(oneshot::Sender<Result<(), PlayerError>>),
    Resume(oneshot::Sender<Result<(), PlayerError>>),
    Stop(oneshot::Sender<Result<(), PlayerError>>),
    Seek {
        secs: f64,
        reply: oneshot::Sender<Result<(), PlayerError>>,
    },
    SetVolume {
        vol: u8,
        reply: oneshot::Sender<Result<(), PlayerError>>,
    },
}

/// The cloneable handle every other layer holds. Cheap to clone (just channel
/// senders + a watch receiver). This is the whole public player surface.
#[derive(Clone)]
pub struct PlayerHandle {
    cmd_tx: mpsc::Sender<PlayerCommand>,
    state_rx: watch::Receiver<PlayState>,
}

impl PlayerHandle {
    /// Load a resolved stream URL and begin playback. `song` is `Some(id)` for a
    /// library track (scrobbled) or `None` for a raw internet-radio stream
    /// (never scrobbled - the player emits no id-bearing now-playing/eof for it).
    pub async fn play_url(&self, song: Option<SongId>, url: &str) -> Result<(), PlayerError> {
        self.request(|reply| PlayerCommand::PlayUrl {
            song,
            url: url.to_string(),
            reply,
        })
        .await
    }

    pub async fn pause(&self) -> Result<(), PlayerError> {
        self.request(PlayerCommand::Pause).await
    }

    pub async fn resume(&self) -> Result<(), PlayerError> {
        self.request(PlayerCommand::Resume).await
    }

    pub async fn stop(&self) -> Result<(), PlayerError> {
        self.request(PlayerCommand::Stop).await
    }

    /// Seek to absolute position in seconds.
    pub async fn seek(&self, secs: f64) -> Result<(), PlayerError> {
        self.request(|reply| PlayerCommand::Seek { secs, reply }).await
    }

    /// Set volume; `0..=100` to match MPD's range.
    pub async fn set_volume(&self, vol: u8) -> Result<(), PlayerError> {
        self.request(|reply| PlayerCommand::SetVolume { vol, reply })
            .await
    }

    /// Always-current play state snapshot (no round-trip to the actor).
    pub fn state(&self) -> PlayState {
        *self.state_rx.borrow()
    }

    /// Subscribe to state changes (e.g. for `idle player` in the MPD layer).
    pub fn subscribe_state(&self) -> watch::Receiver<PlayState> {
        self.state_rx.clone()
    }

    async fn request<F>(&self, make: F) -> Result<(), PlayerError>
    where
        F: FnOnce(oneshot::Sender<Result<(), PlayerError>>) -> PlayerCommand,
    {
        let (tx, rx) = oneshot::channel();
        self.cmd_tx
            .send(make(tx))
            .await
            .map_err(|_| PlayerError::Gone)?;
        rx.await.map_err(|_| PlayerError::Gone)?
    }
}

/// A no-op player ACTOR over the real [`PlayerHandle`] boundary. Used to exercise
/// the handle contract and the MPD layer without an audio device (headless CI,
/// unit tests). It is a genuine actor (owns state, processes the command loop,
/// updates the `watch`), not a stub that pretends to play. NOT the production
/// backend - that is `MpvPlayer` (next-phase).
pub struct NullPlayer;

impl NullPlayer {
    /// Spawn the actor; returns the handle plus the event stream receiver.
    /// (The event stream is unused by NullPlayer beyond `StateChanged`, but the
    /// wiring is real so the mpv impl inherits an identical spawn contract.)
    pub fn spawn() -> (PlayerHandle, mpsc::Receiver<PlayerEvent>) {
        let (cmd_tx, mut cmd_rx) = mpsc::channel::<PlayerCommand>(32);
        let (state_tx, state_rx) = watch::channel(PlayState::Stopped);
        let (evt_tx, evt_rx) = mpsc::channel::<PlayerEvent>(64);

        tokio::spawn(async move {
            let mut current: Option<SongId> = None;
            while let Some(cmd) = cmd_rx.recv().await {
                match cmd {
                    PlayerCommand::PlayUrl { song, url: _, reply } => {
                        current = song.clone();
                        let _ = state_tx.send(PlayState::Playing);
                        let _ = evt_tx
                            .send(PlayerEvent::StateChanged(PlayState::Playing, song))
                            .await;
                        let _ = reply.send(Ok(()));
                    }
                    PlayerCommand::Pause(reply) => {
                        let _ = state_tx.send(PlayState::Paused);
                        let _ = evt_tx
                            .send(PlayerEvent::StateChanged(PlayState::Paused, current.clone()))
                            .await;
                        let _ = reply.send(Ok(()));
                    }
                    PlayerCommand::Resume(reply) => {
                        let _ = state_tx.send(PlayState::Playing);
                        let _ = evt_tx
                            .send(PlayerEvent::StateChanged(PlayState::Playing, current.clone()))
                            .await;
                        let _ = reply.send(Ok(()));
                    }
                    PlayerCommand::Stop(reply) => {
                        current = None;
                        let _ = state_tx.send(PlayState::Stopped);
                        let _ = evt_tx
                            .send(PlayerEvent::StateChanged(PlayState::Stopped, None))
                            .await;
                        let _ = reply.send(Ok(()));
                    }
                    PlayerCommand::Seek { secs, reply } => {
                        let _ = evt_tx.send(PlayerEvent::TimePos(secs)).await;
                        let _ = reply.send(Ok(()));
                    }
                    PlayerCommand::SetVolume { vol: _, reply } => {
                        let _ = reply.send(Ok(()));
                    }
                }
            }
            // Channel closed: nothing more to do. `current` kept only to model
            // what the real actor tracks.
            let _ = current;
        });

        (PlayerHandle { cmd_tx, state_rx }, evt_rx)
    }
}

/// Audio-output configuration for [`MpvPlayer`]. The whole point is the HARD
/// CONSTRAINT: a test/headless run must NEVER hit the real speakers.
#[derive(Debug, Clone, Default)]
pub enum AudioOut {
    /// Decode audio but send it nowhere (`ao=null`). Fully headless, no device
    /// touched, no file written. This is the default so a mistaken construction
    /// can never play to the user's speakers.
    #[default]
    Null,
    /// Decode and encode audio to a WAV file (`ao=null` is NOT used; instead
    /// `--o=<path>` drives mpv's encode path with a wav muxer + pcm codec). The
    /// resulting file contains REAL decoded PCM, which is what the play-probe
    /// checks (bytes grew / songrec) to prove playback actually happened.
    File(std::path::PathBuf),
    /// The real default device. Only reachable by explicitly asking for it;
    /// nothing in the test path constructs this.
    Device,
}

/// The real, libmpv-backed player actor. Lives behind the SAME [`PlayerHandle`]
/// boundary as [`NullPlayer`], so no layer above it changes.
///
/// libmpv2::Mpv is not freely Send/Sync and its event loop is a blocking pull,
/// so the Mpv handle is created ON a dedicated OS thread and never leaves it;
/// only channel ends cross the thread boundary.
pub struct MpvPlayer;

impl MpvPlayer {
    /// Spawn the mpv actor with the given audio-output policy. Returns the
    /// handle + the event stream, identical contract to `NullPlayer::spawn`.
    ///
    /// If the `Mpv` handle cannot even be constructed (no libmpv at runtime),
    /// we log and fall back to a `NullPlayer` actor so the daemon does not
    /// panic - a playback backend failure must degrade, not crash.
    pub fn spawn(out: AudioOut) -> (PlayerHandle, mpsc::Receiver<PlayerEvent>) {
        use libmpv2::Mpv;

        let (cmd_tx, cmd_rx) = mpsc::channel::<PlayerCommand>(32);
        let (state_tx, state_rx) = watch::channel(PlayState::Stopped);
        let (evt_tx, evt_rx) = mpsc::channel::<PlayerEvent>(64);

        // Build the Mpv instance up front so a construction failure can fall
        // back to NullPlayer BEFORE we hand out a broken handle.
        let mpv = Mpv::with_initializer(|init| {
            // Audio-only, no window, no terminal control.
            init.set_property("vid", "no")?;
            init.set_property("video", "no")?;
            init.set_property("terminal", "no")?;
            match &out {
                AudioOut::Null => {
                    init.set_property("ao", "null")?;
                }
                AudioOut::File(path) => {
                    // mpv's encode mode: write decoded audio to a WAV file
                    // instead of a device. Real PCM bytes land on disk.
                    init.set_property("o", path.to_string_lossy().as_ref())?;
                    init.set_property("of", "wav")?;
                    init.set_property("oac", "pcm_s16le")?;
                }
                AudioOut::Device => {}
            }
            Ok(())
        });

        let mpv = match mpv {
            Ok(m) => m,
            Err(e) => {
                tracing::error!(error = %e, "mpv init failed; falling back to NullPlayer");
                drop((cmd_rx, state_tx, state_rx, evt_tx, evt_rx, cmd_tx));
                return NullPlayer::spawn();
            }
        };

        std::thread::Builder::new()
            .name("mpv-player".into())
            .spawn(move || mpv_actor(mpv, cmd_rx, state_tx, evt_tx))
            .expect("spawn mpv thread");

        (PlayerHandle { cmd_tx, state_rx }, evt_rx)
    }
}

/// The classified `EndFile` reason. mpv reports the raw code as a u32; we name
/// every value (see `end_reason`) so the EndFile arm reasons over intent, not a
/// bare integer.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
enum EndReason {
    /// Natural end of the current track.
    Eof,
    /// Our own `loadfile ... replace` (next/prev/play) or an explicit stop.
    Stop,
    /// mpv is quitting.
    Quit,
    /// Playback failed after a successful loadfile (expired token, 404, dropped).
    Error,
    /// The current URL was redirected to another.
    Redirect,
    /// Any other/unknown reason.
    Other,
}

// libmpv2 surfaces EndFile's reason as a raw u32 (the bindgen
// `mpv_end_file_reason` alias), so we name ALL reason codes here and classify
// them rather than matching a lone magic value.
const MPV_END_FILE_REASON_EOF: u32 = 0;
const MPV_END_FILE_REASON_STOP: u32 = 2;
const MPV_END_FILE_REASON_QUIT: u32 = 3;
const MPV_END_FILE_REASON_ERROR: u32 = 4;
const MPV_END_FILE_REASON_REDIRECT: u32 = 5;

/// Classify a raw mpv `EndFile` reason code. `Eof` (natural end) and `Error` (a
/// track that failed AFTER a successful loadfile - expired token, 404, dropped
/// stream) both advance the queue and scrobble the just-finished id.
/// `Stop`/`Quit`/`Redirect`/`Other` are ignored by the EndFile arm.
fn end_reason(raw: u32) -> EndReason {
    match raw {
        MPV_END_FILE_REASON_EOF => EndReason::Eof,
        MPV_END_FILE_REASON_STOP => EndReason::Stop,
        MPV_END_FILE_REASON_QUIT => EndReason::Quit,
        MPV_END_FILE_REASON_ERROR => EndReason::Error,
        MPV_END_FILE_REASON_REDIRECT => EndReason::Redirect,
        _ => EndReason::Other,
    }
}

/// The mpv actor body, running on its own OS thread. Owns the `Mpv` handle and
/// drives it from the command channel, draining mpv events on every wakeup.
fn mpv_actor(
    mut mpv: libmpv2::Mpv,
    mut cmd_rx: mpsc::Receiver<PlayerCommand>,
    state_tx: watch::Sender<PlayState>,
    evt_tx: mpsc::Sender<PlayerEvent>,
) {
    use libmpv2::{Format, events::Event};

    // Observe time-pos so we can push TimePos events (drives scrobble + UI).
    let ectx = mpv.event_context_mut();
    let _ = ectx.observe_property("time-pos", Format::Double, 0);
    let _ = ectx.observe_property("eof-reached", Format::Flag, 1);

    let mut current: Option<SongId> = None;

    loop {
        // 1. Drain any pending commands without blocking.
        match cmd_rx.try_recv() {
            Ok(cmd) => {
                if handle_cmd(&mpv, &state_tx, &evt_tx, &mut current, cmd) {
                    // Stop requested with shutdown intent is not modeled;
                    // channel close is the only exit. Continue.
                }
                continue;
            }
            Err(mpsc::error::TryRecvError::Disconnected) => break,
            Err(mpsc::error::TryRecvError::Empty) => {}
        }

        // 2. Pump mpv events with a short timeout so we stay responsive to
        //    commands. wait_event MUST be drained each wakeup.
        let ectx = mpv.event_context_mut();
        match ectx.wait_event(0.1) {
            Some(Ok(Event::PropertyChange { name: "time-pos", change, .. })) => {
                if let libmpv2::events::PropertyData::Double(t) = change {
                    let _ = evt_tx.blocking_send(PlayerEvent::TimePos(t));
                }
            }
            Some(Ok(Event::EndFile(reason))) => {
                // Only a NATURAL end-of-file advances the queue / scrobbles. mpv
                // also fires EndFile with reason `Stop` when our own `loadfile`
                // REPLACES the current track (next/prev/play) and on an explicit
                // stop, plus `Redirect`/`Quit`/`Error`. Treating those as EOF
                // caused a phantom-advance cascade: a manual `next` loads the new
                // track (repointing `current` in handle_cmd), then the outgoing
                // track's EndFile(Stop) was read as an EOF, taking the NEW id and
                // emitting Eof -> advance_on_eof double-skipped and could leave
                // the queue index desynced to None while audio kept playing (the
                // ncmpcpp `>` freeze + empty currentsong/MPRIS notification).
                //
                // So act ONLY on `Eof` and `Error`. For `Stop` we must NOT
                // `current.take()`: by the time this event is pumped, handle_cmd
                // has already set `current` to the incoming track, so taking it
                // would clear the now-playing id. The explicit-stop path reports
                // Stopped itself.
                //
                // `Error` reuses the exact Eof body so a track that fails AFTER a
                // successful loadfile (expired token/404/dropped stream) advances
                // the queue and scrobbles the just-finished id like a natural EOF
                // - never wedging the player in a phantom Playing. (A hard
                // loadfile failure returns Err synchronously in handle_cmd and
                // does not repoint `current`, so this path only covers post-load
                // failures.)
                match end_reason(reason) {
                    EndReason::Eof | EndReason::Error => {
                        // A library track carries a SongId -> emit Eof (drives
                        // queue-advance + scrobble). A raw stream has no id -> just
                        // report Stopped, never scrobble it.
                        let song = current.take();
                        let _ = state_tx.send(PlayState::Stopped);
                        if let Some(song) = song {
                            let _ = evt_tx.blocking_send(PlayerEvent::Eof(song));
                        }
                        let _ = evt_tx
                            .blocking_send(PlayerEvent::StateChanged(PlayState::Stopped, None));
                    }
                    // Stop/Quit/Redirect/Other: do NOTHING. Critically do NOT take
                    // `current` (handle_cmd already repointed it), which preserves
                    // the phantom-skip cascade fix.
                    _ => {}
                }
            }
            Some(Ok(_)) => {}
            Some(Err(e)) => {
                tracing::warn!(error = %e, "mpv event error");
            }
            None => {}
        }
    }
    tracing::debug!("mpv actor exiting (command channel closed)");
}

/// Apply one command to the mpv handle. Errors are logged (never panic): a
/// failed loadfile must reply Err to the caller and keep the actor alive.
fn handle_cmd(
    mpv: &libmpv2::Mpv,
    state_tx: &watch::Sender<PlayState>,
    evt_tx: &mpsc::Sender<PlayerEvent>,
    current: &mut Option<SongId>,
    cmd: PlayerCommand,
) -> bool {
    match cmd {
        PlayerCommand::PlayUrl { song, url, reply } => {
            let res = mpv
                .command("loadfile", &[&quote(&url), "replace"])
                .and_then(|_| mpv.set_property("pause", false))
                .map_err(|e| PlayerError::Backend(e.to_string()));
            match &res {
                Ok(()) => {
                    *current = song.clone();
                    let _ = state_tx.send(PlayState::Playing);
                    let _ = evt_tx.blocking_send(PlayerEvent::StateChanged(
                        PlayState::Playing,
                        song,
                    ));
                }
                Err(e) => tracing::error!(error = %e, "mpv loadfile failed"),
            }
            let _ = reply.send(res);
        }
        PlayerCommand::Pause(reply) => {
            let res = mpv
                .set_property("pause", true)
                .map_err(|e| PlayerError::Backend(e.to_string()));
            if res.is_ok() {
                let _ = state_tx.send(PlayState::Paused);
                let _ = evt_tx.blocking_send(PlayerEvent::StateChanged(
                    PlayState::Paused,
                    current.clone(),
                ));
            }
            let _ = reply.send(res);
        }
        PlayerCommand::Resume(reply) => {
            let res = mpv
                .set_property("pause", false)
                .map_err(|e| PlayerError::Backend(e.to_string()));
            if res.is_ok() {
                let _ = state_tx.send(PlayState::Playing);
                let _ = evt_tx.blocking_send(PlayerEvent::StateChanged(
                    PlayState::Playing,
                    current.clone(),
                ));
            }
            let _ = reply.send(res);
        }
        PlayerCommand::Stop(reply) => {
            let res = mpv
                .command("stop", &[])
                .map_err(|e| PlayerError::Backend(e.to_string()));
            *current = None;
            let _ = state_tx.send(PlayState::Stopped);
            let _ = evt_tx.blocking_send(PlayerEvent::StateChanged(PlayState::Stopped, None));
            let _ = reply.send(res);
        }
        PlayerCommand::Seek { secs, reply } => {
            let res = mpv
                .command("seek", &[&secs.to_string(), "absolute"])
                .map_err(|e| PlayerError::Backend(e.to_string()));
            if res.is_ok() {
                let _ = evt_tx.blocking_send(PlayerEvent::TimePos(secs));
            }
            let _ = reply.send(res);
        }
        PlayerCommand::SetVolume { vol, reply } => {
            let res = mpv
                .set_property("volume", vol as i64)
                .map_err(|e| PlayerError::Backend(e.to_string()));
            let _ = reply.send(res);
        }
    }
    false
}

/// mpv's `command`/`loadfile` treats spaces as argument separators, so a URL
/// with special chars must be wrapped in `%N%<bytes>` percent-length quoting or
/// double quotes. We use double-quotes (URLs never contain a literal `"`).
fn quote(url: &str) -> String {
    format!("\"{url}\"")
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::model::SongId;

    // Locks the FOUNDATION contract: the NullPlayer actor is drivable purely
    // through the `&self` PlayerHandle, state is observable via the watch, and
    // play/pause/resume/stop transition as expected. This proves the actor
    // boundary is usable headless before the mpv backend exists.
    #[tokio::test]
    async fn null_player_transitions_via_handle() {
        let (player, _events) = NullPlayer::spawn();
        assert_eq!(player.state(), PlayState::Stopped);

        player
            .play_url(Some(SongId("42".into())), "http://example/stream")
            .await
            .unwrap();
        assert_eq!(player.state(), PlayState::Playing);

        player.pause().await.unwrap();
        assert_eq!(player.state(), PlayState::Paused);

        player.resume().await.unwrap();
        assert_eq!(player.state(), PlayState::Playing);

        player.set_volume(80).await.unwrap();
        player.seek(12.5).await.unwrap();

        player.stop().await.unwrap();
        assert_eq!(player.state(), PlayState::Stopped);
    }

    // The EndFile arm advances (take current + emit Eof) on exactly Eof AND
    // Error, and does NOTHING on Stop/Quit/Redirect - encoded here as: does this
    // reason classify into the advancing set? This locks the F1 fix (a
    // post-loadfile failure advances/scrobbles like EOF) without regressing the
    // phantom-skip cascade cure (our own loadfile-replace fires Stop, ignored).
    #[test]
    fn end_reason_advances_on_eof_and_error_only() {
        let advances = |raw: u32| matches!(end_reason(raw), EndReason::Eof | EndReason::Error);
        assert!(advances(MPV_END_FILE_REASON_EOF));
        assert!(advances(MPV_END_FILE_REASON_ERROR));
        assert!(!advances(MPV_END_FILE_REASON_STOP));
        assert!(!advances(MPV_END_FILE_REASON_QUIT));
        assert!(!advances(MPV_END_FILE_REASON_REDIRECT));
        assert!(!advances(999));
    }

    // The handle is cloneable and shared: a second clone sees the same state,
    // which is what the (shared-across-connections) MPD handler relies on.
    #[tokio::test]
    async fn handle_clone_shares_state() {
        let (player, _events) = NullPlayer::spawn();
        let other = player.clone();
        player
            .play_url(Some(SongId("1".into())), "http://x")
            .await
            .unwrap();
        assert_eq!(other.state(), PlayState::Playing);
    }
}
