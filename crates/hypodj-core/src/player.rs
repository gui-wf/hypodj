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

/// The play state to REPORT outward (MPD `status`, MPRIS `PlaybackStatus`,
/// resume checkpoints). A media player with nothing loaded is Stopped, never
/// Playing/Paused, regardless of the raw backend state: an mpv started under
/// `--idle` can report not-paused before any file is loaded, and that must not
/// leak out as a phantom Playing. `has_current` is whether a current queue item
/// actually exists. This is the single source of truth for the idle guard.
pub fn effective_play_state(raw: PlayState, has_current: bool) -> PlayState {
    if has_current {
        raw
    } else {
        PlayState::Stopped
    }
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
    /// A post-decode audio LEVEL sample for the cosmetic HUD visualizer, read from
    /// the labelled `@viz` lavfi `astats` node at mpv's af-metadata cadence (~20 Hz).
    /// `rms_db`/`peak_db` are the RAW (pre-softvol) levels in dBFS; `gain_db` is the
    /// actor's current softvol gain (from `cur_vol`), so a subscriber recovers the
    /// AUDIBLE post-gain level as `rms_db + gain_db` (a startle-safe fade then reads
    /// as a genuine descent of the bars). Emitted best-effort via `try_send`
    /// (drop-on-Full): a stale level is harmless to lose and MUST NEVER wedge the
    /// actor. `NullPlayer` never emits it; a filter failure degrades to no Viz.
    Viz {
        rms_db: f32,
        peak_db: f32,
        gain_db: f32,
        playing: bool,
    },
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

/// A silent, not-playing Viz frame. The af-metadata arm only emits while audio is
/// decoding, so pause/stop/EOF must push ONE resting frame or the client's
/// latest-wins viz slot keeps the last playing=true level and freezes the level
/// field lit. `rms_db`/`peak_db` sit at the silence floor and `playing = false`, so
/// the client targets the resting hairline regardless of the reported level.
fn resting_viz(cur_vol: f64) -> PlayerEvent {
    PlayerEvent::Viz {
        rms_db: -120.0,
        peak_db: -120.0,
        gain_db: mpv_volume_to_db(cur_vol) as f32,
        playing: false,
    }
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

    // VIZ TAP (cosmetic HUD level meter). Add a LABELLED lavfi `astats` node to the
    // audio filter chain, POST-construction and NON-FATALLY: a filter error here
    // (stripped ffmpeg, no astats) must degrade to no-viz, NEVER silence the deck -
    // so `let _`, never `?`. The `@viz:` label binds the node we read metadata from
    // (`af-metadata/viz`); labelling the chain or asetnsamples yields a dead node.
    // No asetnsamples: mpv coalesces af-metadata dispatch to ~20 Hz regardless.
    if let Err(e) = mpv.set_property("af", "@viz:astats=metadata=1:reset=1") {
        tracing::warn!(error = %e, "viz astats filter unavailable; playback continues without the HUD level meter");
    }

    // Observe time-pos so we can push TimePos events (drives scrobble + UI).
    let ectx = mpv.event_context_mut();
    let _ = ectx.observe_property("time-pos", Format::Double, 0);
    let _ = ectx.observe_property("eof-reached", Format::Flag, 1);
    // Observe the labelled astats node as a whole Node (a map of string metadata);
    // read `lavfi.astats.Overall.RMS_level` / `Peak_level` off it. Do NOT observe the
    // sub-key path `af-metadata/viz/lavfi.astats...` - it fires no events (mpv regr).
    let _ = ectx.observe_property("af-metadata/viz", Format::Node, 2);

    let mut current: Option<SongId> = None;
    let mut current_qid: Option<QueueId> = None;
    // The actor's current softvol volume (0..=100, fractional). The user af chain
    // runs BEFORE mpv's internal softvol, so astats measures PRE-gain; carrying the
    // live gain lets the emitted Viz recover the audible post-gain level. Seeded to
    // mpv's default volume (100 == 0 dB) and updated on every SetVolume(F64).
    let mut cur_vol: f64 = 100.0;
    // Whether the deck is currently playing, for the Viz `playing` flag.
    let mut playing = false;

    loop {
        // 1. Drain any pending commands without blocking.
        match cmd_rx.try_recv() {
            Ok(cmd) => {
                if handle_cmd(&mpv, &state_tx, &evt_tx, &mut current, &mut current_qid, &mut cur_vol, &mut playing, cmd) {
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
            Some(Ok(Event::PropertyChange { name: "af-metadata/viz", change, .. })) => {
                // The labelled astats node fired: parse RMS/peak (dBFS) and emit a
                // best-effort Viz. try_send (drop-on-Full) mirrors the TimePos
                // discipline - a cosmetic level must never wedge the actor. A node
                // that carries no parseable levels (a transient empty dispatch) is
                // simply skipped; playback is untouched either way.
                if let libmpv2::events::PropertyData::Node(node) = change {
                    if let Some((rms_db, peak_db)) = parse_astats_levels(node) {
                        let gain_db = mpv_volume_to_db(cur_vol) as f32;
                        let _ = evt_tx.try_send(PlayerEvent::Viz {
                            rms_db,
                            peak_db,
                            gain_db,
                            playing,
                        });
                    }
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
                        playing = false;
                        // Trailing resting frame: EOF stops decode, so the last live
                        // Viz would otherwise stay lit at the pre-stop loudness forever.
                        let _ = evt_tx.try_send(resting_viz(cur_vol));
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
    // Tracked softvol volume + play flag, so the Viz emit can report the audible
    // post-gain level and whether sound is flowing. Mutated here (the single place
    // volume + play-state actually change) and read by the af-metadata arm.
    cur_vol: &mut f64,
    playing: &mut bool,
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
                    *playing = true;
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
                *playing = false;
                // The af-metadata arm stops firing once decode halts, so without a
                // trailing resting frame the client's latest-wins viz slot would keep
                // the last playing=true level and freeze the level field lit. Emit one
                // silent, not-playing Viz so the bars settle to the resting hairline.
                let _ = evt_tx.try_send(resting_viz(*cur_vol));
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
                *playing = true;
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
            *playing = false;
            // Resting frame so the level field settles instead of freezing at the
            // pre-stop loudness (the af-metadata arm goes silent once decode stops).
            let _ = evt_tx.try_send(resting_viz(*cur_vol));
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
            if res.is_ok() {
                *cur_vol = vol as f64;
            }
            let _ = reply.send(res);
        }
        PlayerCommand::SetVolumeF64 { vol, reply } => {
            // The fractional fade seam: mpv's `volume` property accepts a double,
            // so a sub-integer envelope step lands without rounding to the u8
            // grid. The caller (the dB sink) has already inverted the cube.
            let res = mpv
                .set_property("volume", vol)
                .map_err(|e| PlayerError::Backend(e.to_string()));
            if res.is_ok() {
                *cur_vol = vol;
            }
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

/// A hard floor (dBFS) for a parsed astats level: astats reports silence as
/// `-inf`, which would poison the downstream normalize. Any non-finite / very low
/// value is clamped to this so the wire always carries a finite number.
pub(crate) const VIZ_FLOOR_DBFS: f32 = -120.0;

/// Extract `(rms_db, peak_db)` in dBFS from an `af-metadata/viz` node (a string
/// map). astats writes `lavfi.astats.Overall.RMS_level` / `Overall.Peak_level` as
/// stringified dBFS. Returns `None` when NEITHER level is present (a transient
/// empty dispatch), so a dead node never emits a bogus sample. `-inf` (true
/// silence) parses to `f32::NEG_INFINITY`, which we clamp to [`VIZ_FLOOR_DBFS`].
pub(crate) fn parse_astats_levels(node: libmpv2::mpv_node::MpvNode) -> Option<(f32, f32)> {
    let map = node.map()?;
    let mut rms: Option<f32> = None;
    let mut peak: Option<f32> = None;
    for (key, value) in map {
        // Values are strings; tolerate a stray numeric node just in case.
        let parsed: Option<f32> = match value {
            libmpv2::mpv_node::MpvNode::String(s) => s.trim().parse::<f32>().ok(),
            libmpv2::mpv_node::MpvNode::Double(d) => Some(d as f32),
            _ => None,
        };
        let Some(v) = parsed else { continue };
        let v = if v.is_finite() { v } else { VIZ_FLOOR_DBFS };
        let v = v.max(VIZ_FLOOR_DBFS);
        if key.ends_with("RMS_level") {
            rms = Some(v);
        } else if key.ends_with("Peak_level") {
            peak = Some(v);
        }
    }
    match (rms, peak) {
        (None, None) => None,
        // If only one surfaced, mirror it so a subscriber still gets a usable pair.
        (r, p) => Some((r.or(p).unwrap(), p.or(r).unwrap())),
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::model::SongId;

    // Locks the FOUNDATION contract: the NullPlayer actor is drivable purely
    // through the `&self` PlayerHandle, state is observable via the watch, and
    // play/pause/resume/stop transition as expected. This proves the actor
    // boundary is usable headless before the mpv backend exists.
    // The idle guard: with nothing loaded the reported state is always Stopped,
    // no matter what the raw backend claims; with a current song the raw state
    // passes through unchanged.
    #[test]
    fn effective_state_forces_stop_without_current() {
        assert_eq!(effective_play_state(PlayState::Playing, false), PlayState::Stopped);
        assert_eq!(effective_play_state(PlayState::Paused, false), PlayState::Stopped);
        assert_eq!(effective_play_state(PlayState::Stopped, false), PlayState::Stopped);
        assert_eq!(effective_play_state(PlayState::Playing, true), PlayState::Playing);
        assert_eq!(effective_play_state(PlayState::Paused, true), PlayState::Paused);
        assert_eq!(effective_play_state(PlayState::Stopped, true), PlayState::Stopped);
    }

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

    // LIVE empirical proof of the viz data tap (the crux of the waveform feature):
    // on THIS installed mpv/libmpv, the labelled `@viz:astats` node surfaces
    // `lavfi.astats.Overall.RMS_level`/`Peak_level` via `af-metadata/viz` and the
    // observe pushes at ~20 Hz. We synthesize a tiny amplitude-RAMPED tone WAV so the
    // RMS is guaranteed to CHANGE over time, then assert we read real, finite,
    // VARYING levels. Ignored by default (needs a real libmpv runtime, absent in the
    // link-isolated Nix build sandbox); run with:
    //   cargo test -p hypodj-core -- --ignored live_astats_viz_levels_change
    #[test]
    #[ignore = "needs a real libmpv runtime; run manually to confirm the astats viz tap"]
    fn live_astats_viz_levels_change() {
        use libmpv2::{events::Event, events::PropertyData, Format, Mpv};
        use std::io::Write;

        // 1. Synthesize ~2s of 440 Hz sine, mono s16le @ 44100, whose amplitude
        //    ramps 0 -> full so the windowed RMS genuinely rises over time.
        let sample_rate = 44100u32;
        let secs = 2.0f64;
        let n = (sample_rate as f64 * secs) as usize;
        let mut pcm: Vec<u8> = Vec::with_capacity(n * 2);
        for i in 0..n {
            let t = i as f64 / sample_rate as f64;
            let amp = (t / secs).clamp(0.0, 1.0); // 0 -> 1 ramp
            let s = (2.0 * std::f64::consts::PI * 440.0 * t).sin() * amp;
            let v = (s * i16::MAX as f64) as i16;
            pcm.extend_from_slice(&v.to_le_bytes());
        }
        // Minimal 44-byte WAV header + PCM body.
        let data_len = pcm.len() as u32;
        let mut wav: Vec<u8> = Vec::with_capacity(44 + pcm.len());
        wav.extend_from_slice(b"RIFF");
        wav.extend_from_slice(&(36 + data_len).to_le_bytes());
        wav.extend_from_slice(b"WAVE");
        wav.extend_from_slice(b"fmt ");
        wav.extend_from_slice(&16u32.to_le_bytes()); // PCM fmt chunk size
        wav.extend_from_slice(&1u16.to_le_bytes()); // PCM
        wav.extend_from_slice(&1u16.to_le_bytes()); // mono
        wav.extend_from_slice(&sample_rate.to_le_bytes());
        wav.extend_from_slice(&(sample_rate * 2).to_le_bytes()); // byte rate
        wav.extend_from_slice(&2u16.to_le_bytes()); // block align
        wav.extend_from_slice(&16u16.to_le_bytes()); // bits per sample
        wav.extend_from_slice(b"data");
        wav.extend_from_slice(&data_len.to_le_bytes());
        wav.extend_from_slice(&pcm);

        let path = std::env::temp_dir().join("hypodj_viz_probe.wav");
        std::fs::File::create(&path)
            .and_then(|mut f| f.write_all(&wav))
            .expect("write probe wav");

        // 2. Headless mpv (ao=null) with the SAME labelled viz filter the actor sets.
        let mut mpv = Mpv::with_initializer(|init| {
            init.set_property("vid", "no")?;
            init.set_property("ao", "null")?;
            init.set_property("terminal", "no")?;
            Ok(())
        })
        .expect("construct mpv");
        mpv.set_property("af", "@viz:astats=metadata=1:reset=1")
            .expect("set viz astats filter");
        {
            let ectx = mpv.event_context_mut();
            ectx.observe_property("af-metadata/viz", Format::Node, 2)
                .expect("observe af-metadata/viz");
        }
        mpv.command("loadfile", &[&quote(&path.to_string_lossy()), "replace"])
            .expect("loadfile");
        mpv.set_property("pause", false).expect("unpause");

        // 3. Collect RMS samples for up to ~4s of wall time.
        let mut rms_samples: Vec<f32> = Vec::new();
        let start = std::time::Instant::now();
        while start.elapsed() < std::time::Duration::from_secs(4) && rms_samples.len() < 40 {
            let ectx = mpv.event_context_mut();
            match ectx.wait_event(0.2) {
                Some(Ok(Event::PropertyChange { name: "af-metadata/viz", change, .. })) => {
                    if let PropertyData::Node(node) = change {
                        if let Some((rms, _peak)) = parse_astats_levels(node) {
                            rms_samples.push(rms);
                        }
                    }
                }
                Some(Ok(Event::EndFile(_))) => break,
                _ => {}
            }
        }
        let _ = std::fs::remove_file(&path);

        // 4. Assert we got REAL, finite, non-silent, VARYING levels.
        assert!(
            rms_samples.len() >= 2,
            "expected multiple af-metadata/viz RMS samples, got {rms_samples:?}"
        );
        assert!(
            rms_samples.iter().all(|v| v.is_finite()),
            "every level is finite (no raw -inf leaked): {rms_samples:?}"
        );
        assert!(
            rms_samples.iter().any(|&v| v > VIZ_FLOOR_DBFS + 1.0),
            "at least one level is above the floor (real signal): {rms_samples:?}"
        );
        let lo = rms_samples.iter().cloned().fold(f32::INFINITY, f32::min);
        let hi = rms_samples.iter().cloned().fold(f32::NEG_INFINITY, f32::max);
        assert!(
            (hi - lo) > 1.0,
            "the ramped tone must make RMS CHANGE over time (lo={lo}, hi={hi}): {rms_samples:?}"
        );
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
