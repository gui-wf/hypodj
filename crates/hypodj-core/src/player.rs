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

use crate::event::QueueId;
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
    ///
    /// `queue_id` is the LATCHED identity of the entry the actor is currently
    /// playing (stamped at load time, not derived from any mutable index). The
    /// enrichment join uses it to attribute a buffered position to the right
    /// entry even after an off-spine `next`/`prev`/`delete` repointed the current
    /// index. `None` only if nothing is latched.
    TimePos { pos: f64, queue_id: Option<QueueId> },
    /// The current track finished (natural end). Triggers queue-advance +
    /// scrobble submission. `song` is `None` for a raw stream (never scrobbled);
    /// `queue_id` is the latched entry identity, present for anything we play -
    /// so a mid-queue STREAM end still advances (emits `Eof { song: None,
    /// queue_id: Some(..) }`) instead of silently halting.
    Eof {
        song: Option<SongId>,
        queue_id: Option<QueueId>,
    },
    /// Play state changed (e.g. paused, stopped). Carries the id of the song the
    /// state applies to so the scrobbler is self-describing: it can start a
    /// now-playing on a NEW Playing id and attribute later `TimePos` to that
    /// latched id. `song`/`queue_id` are `None` on Stop (no current entry).
    StateChanged(PlayState, Option<SongId>, Option<QueueId>),
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
        /// The stable queue-entry identity to LATCH so every subsequent event
        /// (TimePos/StateChanged/Eof) is attributed to this exact entry.
        queue_id: Option<QueueId>,
        url: String,
        reply: oneshot::Sender<Result<(), PlayerError>>,
    },
    /// TEST-ONLY: make the actor emit a natural `Eof` for the latched entry
    /// (mirrors an mpv `EndFile(Eof)`), so a director test can drive a queue
    /// boundary headlessly without a real mpv end-of-stream.
    #[cfg(test)]
    TestEof,
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
    /// Fractional volume set, used by the sub-dB fade envelope (see
    /// [`crate::fade`]). Distinct from `SetVolume(u8)` because mpv's softvol is
    /// CUBIC (gain = (vol/100)^3), so a fade must step in equal-dB and invert the
    /// cube to fractional mpv volume - a u8 step 0..=100 is the fast-then-plunge
    /// startle curve the fade design forbids. This is the seam BELOW the dB sink.
    SetVolumeF64 {
        vol: f64,
        reply: oneshot::Sender<Result<(), PlayerError>>,
    },
}

/// Perceptual floor of the mpv softvol curve, in dB. At or below this the fade
/// driver treats the signal as silence (mpv volume 0). Keeping the domain
/// bottomed at a finite -60 dB (rather than -inf) is what lets the schedule math
/// stay finite: `mpv_volume_to_db(0)` returns this, never negative infinity.
pub(crate) const SYNTH_FLOOR_DB: f64 = -60.0;

/// Invert the cubic softvol curve: perceptual dB -> mpv `volume` (0..=100).
///
/// mpv softvol applies gain = (volume/100)^3, so dB = 60*log10(volume/100) and
/// the inverse is volume = 100 * 10^(dB/60). At or below [`SYNTH_FLOOR_DB`] we
/// snap to 0 (true silence); above 0 dB we clamp to 100 (no boost). Sanity:
/// 0 dB -> 100, -18.06 dB -> ~50, -36.12 dB -> ~25, floor -> 0.
pub(crate) fn db_to_mpv_volume(db: f64) -> f64 {
    if db <= SYNTH_FLOOR_DB {
        return 0.0;
    }
    (100.0 * 10f64.powf(db / 60.0)).clamp(0.0, 100.0)
}

/// The forward cubic softvol mapping: mpv `volume` (0..=100) -> perceptual dB.
/// `vol <= 0` maps to [`SYNTH_FLOOR_DB`] (a finite floor, NOT -inf) so a fade
/// resumed from silence never feeds negative infinity into the schedule math.
pub(crate) fn mpv_volume_to_db(vol: f64) -> f64 {
    if vol <= 0.0 {
        return SYNTH_FLOOR_DB;
    }
    60.0 * (vol / 100.0).log10()
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
    pub async fn play_url(
        &self,
        song: Option<SongId>,
        queue_id: Option<QueueId>,
        url: &str,
    ) -> Result<(), PlayerError> {
        self.request(|reply| PlayerCommand::PlayUrl {
            song,
            queue_id,
            url: url.to_string(),
            reply,
        })
        .await
    }

    /// TEST-ONLY: drive a natural end-of-file for the latched entry.
    #[cfg(test)]
    pub(crate) async fn test_emit_eof(&self) -> Result<(), PlayerError> {
        let (tx, rx) = oneshot::channel::<()>();
        // TestEof carries no reply; use a throwaway to keep `request` shape simple.
        drop(tx);
        drop(rx);
        self.cmd_tx
            .send(PlayerCommand::TestEof)
            .await
            .map_err(|_| PlayerError::Gone)
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
    ///
    /// This is the EXTERNAL (MPD/MPRIS) volume seam: its integer 0..=100 IS the
    /// cubic softvol control, exactly what a user's `setvol` expects. Sub-dB fade
    /// envelopes must NOT step this integer (that is the fast-then-plunge startle
    /// curve); they use [`set_volume_f64`](Self::set_volume_f64) with
    /// [`db_to_mpv_volume`] instead. Manual `setvol` cancels any in-flight fade
    /// first (see the handler), so the two never fight.
    pub async fn set_volume(&self, vol: u8) -> Result<(), PlayerError> {
        self.request(|reply| PlayerCommand::SetVolume { vol, reply })
            .await
    }

    /// Set a FRACTIONAL mpv volume (0.0..=100.0). The fade envelope drives this
    /// through the [`VolumeSink`](crate::fade::VolumeSink) impl so it can invert
    /// the cubic softvol curve and step in equal perceptual dB. Not for external
    /// callers - use [`set_volume`](Self::set_volume) for user-facing volume.
    pub async fn set_volume_f64(&self, vol: f64) -> Result<(), PlayerError> {
        self.request(|reply| PlayerCommand::SetVolumeF64 { vol, reply })
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
            let mut current_qid: Option<QueueId> = None;
            while let Some(cmd) = cmd_rx.recv().await {
                match cmd {
                    PlayerCommand::PlayUrl { song, queue_id, url: _, reply } => {
                        current = song.clone();
                        current_qid = queue_id;
                        let _ = state_tx.send(PlayState::Playing);
                        // Reply BEFORE the (bounded) event send so a full event
                        // channel can never wedge the caller's play_url().await
                        // (the deadlock-avoidance discipline; mirrors the mpv actor).
                        let _ = reply.send(Ok(()));
                        let _ = evt_tx
                            .send(PlayerEvent::StateChanged(PlayState::Playing, song, queue_id))
                            .await;
                    }
                    PlayerCommand::Pause(reply) => {
                        let _ = state_tx.send(PlayState::Paused);
                        let _ = reply.send(Ok(()));
                        let _ = evt_tx
                            .send(PlayerEvent::StateChanged(
                                PlayState::Paused,
                                current.clone(),
                                current_qid,
                            ))
                            .await;
                    }
                    PlayerCommand::Resume(reply) => {
                        let _ = state_tx.send(PlayState::Playing);
                        let _ = reply.send(Ok(()));
                        let _ = evt_tx
                            .send(PlayerEvent::StateChanged(
                                PlayState::Playing,
                                current.clone(),
                                current_qid,
                            ))
                            .await;
                    }
                    PlayerCommand::Stop(reply) => {
                        current = None;
                        current_qid = None;
                        let _ = state_tx.send(PlayState::Stopped);
                        let _ = reply.send(Ok(()));
                        let _ = evt_tx
                            .send(PlayerEvent::StateChanged(PlayState::Stopped, None, None))
                            .await;
                    }
                    PlayerCommand::Seek { secs, reply } => {
                        let _ = reply.send(Ok(()));
                        let _ = evt_tx
                            .send(PlayerEvent::TimePos { pos: secs, queue_id: current_qid })
                            .await;
                    }
                    PlayerCommand::SetVolume { vol: _, reply } => {
                        let _ = reply.send(Ok(()));
                    }
                    PlayerCommand::SetVolumeF64 { vol: _, reply } => {
                        let _ = reply.send(Ok(()));
                    }
                    #[cfg(test)]
                    PlayerCommand::TestEof => {
                        // Mirror the mpv EndFile(Eof) shape: emit Eof for the
                        // latched entry, then the trailing Stopped. `song` is None
                        // for a raw stream; `queue_id` present for anything played.
                        let song = current.take();
                        let qid = current_qid.take();
                        let _ = state_tx.send(PlayState::Stopped);
                        if qid.is_some() {
                            let _ = evt_tx
                                .send(PlayerEvent::Eof { song, queue_id: qid })
                                .await;
                        }
                        let _ = evt_tx
                            .send(PlayerEvent::StateChanged(PlayState::Stopped, None, None))
                            .await;
                    }
                }
            }
            // Channel closed: nothing more to do. `current`/`current_qid` kept
            // only to model what the real actor tracks.
            let _ = current;
            let _ = current_qid;
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
    let mut current_qid: Option<QueueId> = None;

    loop {
        // 1. Drain any pending commands without blocking.
        match cmd_rx.try_recv() {
            Ok(cmd) => {
                if handle_cmd(&mpv, &state_tx, &evt_tx, &mut current, &mut current_qid, cmd) {
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
                    // try_send (drop-on-Full): a stale position is harmless to
                    // lose, and blocking here on a full event ring would wedge the
                    // actor (and thus advance) while the spine is busy. Eof /
                    // StateChanged keep the guaranteed blocking_send (low-rate).
                    let _ = evt_tx.try_send(PlayerEvent::TimePos {
                        pos: t,
                        queue_id: current_qid,
                    });
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
                        // Emit Eof for the LATCHED entry so the queue advances.
                        // A library track carries a SongId (drives scrobble); a raw
                        // stream carries `song: None` but still `queue_id: Some`, so
                        // a mid-queue stream end advances and yields a real TrackEnd
                        // instead of silently halting. Eof is only emitted when
                        // something was actually latched (queue_id present).
                        let song = current.take();
                        let qid = current_qid.take();
                        let _ = state_tx.send(PlayState::Stopped);
                        if qid.is_some() {
                            let _ = evt_tx
                                .blocking_send(PlayerEvent::Eof { song, queue_id: qid });
                        }
                        let _ = evt_tx.blocking_send(PlayerEvent::StateChanged(
                            PlayState::Stopped,
                            None,
                            None,
                        ));
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
    current_qid: &mut Option<QueueId>,
    cmd: PlayerCommand,
) -> bool {
    match cmd {
        PlayerCommand::PlayUrl { song, queue_id, url, reply } => {
            let res = mpv
                .command("loadfile", &[&quote(&url), "replace"])
                .and_then(|_| mpv.set_property("pause", false))
                .map_err(|e| PlayerError::Backend(e.to_string()));
            match &res {
                Ok(()) => {
                    *current = song.clone();
                    *current_qid = queue_id;
                    let _ = state_tx.send(PlayState::Playing);
                    // Reply BEFORE the StateChanged blocking_send: the spine's
                    // play_url().await must complete and return to draining before
                    // the state edge is pushed, or a full event ring would wedge
                    // advance (deadlock-avoidance).
                    let _ = reply.send(res);
                    let _ = evt_tx.blocking_send(PlayerEvent::StateChanged(
                        PlayState::Playing,
                        song,
                        queue_id,
                    ));
                    return false;
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
                    *current_qid,
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
                    *current_qid,
                ));
            }
            let _ = reply.send(res);
        }
        PlayerCommand::Stop(reply) => {
            let res = mpv
                .command("stop", &[])
                .map_err(|e| PlayerError::Backend(e.to_string()));
            *current = None;
            *current_qid = None;
            let _ = state_tx.send(PlayState::Stopped);
            let _ = evt_tx.blocking_send(PlayerEvent::StateChanged(PlayState::Stopped, None, None));
            let _ = reply.send(res);
        }
        PlayerCommand::Seek { secs, reply } => {
            let res = mpv
                .command("seek", &[&secs.to_string(), "absolute"])
                .map_err(|e| PlayerError::Backend(e.to_string()));
            if res.is_ok() {
                let _ = evt_tx.blocking_send(PlayerEvent::TimePos {
                    pos: secs,
                    queue_id: *current_qid,
                });
            }
            let _ = reply.send(res);
        }
        PlayerCommand::SetVolume { vol, reply } => {
            let res = mpv
                .set_property("volume", vol as i64)
                .map_err(|e| PlayerError::Backend(e.to_string()));
            let _ = reply.send(res);
        }
        PlayerCommand::SetVolumeF64 { vol, reply } => {
            // The fractional fade seam: mpv's `volume` property accepts a double,
            // so a sub-integer envelope step lands without rounding to the u8
            // grid. The caller (the dB sink) has already inverted the cube.
            let res = mpv
                .set_property("volume", vol)
                .map_err(|e| PlayerError::Backend(e.to_string()));
            let _ = reply.send(res);
        }
        // The mpv actor never receives TestEof (only NullPlayer does), but the
        // command enum carries it in test builds so this arm keeps the match total.
        #[cfg(test)]
        PlayerCommand::TestEof => {}
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
            .play_url(Some(SongId("42".into())), Some(QueueId(0)), "http://example/stream")
            .await
            .unwrap();
        assert_eq!(player.state(), PlayState::Playing);

        player.pause().await.unwrap();
        assert_eq!(player.state(), PlayState::Paused);

        player.resume().await.unwrap();
        assert_eq!(player.state(), PlayState::Playing);

        player.set_volume(80).await.unwrap();
        player.set_volume_f64(63.5).await.unwrap();
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

    // The cubic softvol dB<->volume helpers round-trip and honor the boundaries
    // the fade math depends on: vol0 -> floor -> vol0, vol100 -> 0 dB -> vol100,
    // and the sanity points (vol50 ~ -18.06 dB, vol25 ~ -36.12 dB). A finite
    // floor (never -inf) is the invariant that keeps the schedule math finite.
    #[test]
    fn db_volume_round_trip_and_boundaries() {
        // Boundaries.
        assert_eq!(db_to_mpv_volume(0.0), 100.0);
        assert_eq!(db_to_mpv_volume(SYNTH_FLOOR_DB), 0.0);
        assert_eq!(db_to_mpv_volume(f64::NEG_INFINITY), 0.0);
        assert_eq!(mpv_volume_to_db(0.0), SYNTH_FLOOR_DB);
        assert_eq!(mpv_volume_to_db(100.0), 0.0);
        // Positive dB never boosts past 100.
        assert_eq!(db_to_mpv_volume(12.0), 100.0);

        // Sanity points from the design.
        assert!((mpv_volume_to_db(50.0) - (-18.061)).abs() < 0.01);
        assert!((mpv_volume_to_db(25.0) - (-36.123)).abs() < 0.01);

        // Round-trip holds for every volume ABOVE the -60 dB synth floor. The
        // cubic maps vol 10 -> exactly -60 dB (100*10^(-1)), so volumes 0..=10 sit
        // at or below the floor and collapse to 0 (the intended practical-silence
        // floor); 11..=100 round-trip losslessly.
        for v in 11..=100u8 {
            let back = db_to_mpv_volume(mpv_volume_to_db(v as f64));
            assert!((back - v as f64).abs() < 1e-6, "vol {v} round-trip -> {back}");
        }
        // At/below the floor (vol <= 10, i.e. <= -60 dB) collapses to silence.
        for v in 0..=10u8 {
            assert_eq!(db_to_mpv_volume(mpv_volume_to_db(v as f64)), 0.0, "vol {v}");
        }
    }

    // LIVE empirical confirmation (task 06nr729): on THIS libmpv2 build, the
    // `volume` property accepts and returns a fractional f64 set/get round-trip.
    // Ignored by default because it constructs a real Mpv (needs libmpv at
    // runtime, absent in the network/link-isolated build sandbox). Run manually
    // with `cargo test -p hypodj-core -- --ignored live_mpv_fractional_volume`
    // to lock the cubic-curve assumption before trusting the helpers in the wild.
    #[test]
    #[ignore = "needs a real libmpv runtime; run manually to confirm softvol"]
    fn live_mpv_fractional_volume_round_trip() {
        use libmpv2::Mpv;
        let mpv = Mpv::with_initializer(|init| {
            init.set_property("vid", "no")?;
            init.set_property("ao", "null")?;
            init.set_property("terminal", "no")?;
            Ok(())
        })
        .expect("construct mpv");
        for target in [50.0f64, 25.0] {
            mpv.set_property("volume", target).expect("set volume");
            let got: f64 = mpv.get_property("volume").expect("get volume");
            assert!((got - target).abs() < 1e-6, "vol {target} read back as {got}");
        }
    }

    // The handle is cloneable and shared: a second clone sees the same state,
    // which is what the (shared-across-connections) MPD handler relies on.
    #[tokio::test]
    async fn handle_clone_shares_state() {
        let (player, _events) = NullPlayer::spawn();
        let other = player.clone();
        player
            .play_url(Some(SongId("1".into())), Some(QueueId(1)), "http://x")
            .await
            .unwrap();
        assert_eq!(other.state(), PlayState::Playing);
    }
}
