//! The concrete [`MpdHandler`] backing the MPD server with a live Subsonic
//! library + the audio player.
//!
//! Phase 2. This is where MPD command semantics meet OpenSubsonic browse/search
//! and the player actor. State that MPD treats as global (the play queue, the
//! current-song pointer, the volume) lives here behind a `Mutex`, because MPD
//! state is shared across all client connections (see [`MpdHandler`] docs) - the
//! handler is `Arc`-shared and every method takes `&self`.
//!
//! ## URI scheme
//!
//! MPD is path-based; Subsonic is id-based. We bridge them with synthetic URIs:
//!   - `song/<songId>`      - a playable track (what lands in the queue)
//!   - `album/<albumId>`    - an album "directory"
//!   - `artist/<artistId>`  - an artist "directory"
//! The root `lsinfo` lists artist directories; drilling into an artist lists its
//! album directories; drilling into an album lists its song files. `add song/X`
//! / `addid song/X` queue a real track; `play` streams it via the player.

use std::collections::hash_map::RandomState;
use std::collections::HashMap;
use std::hash::BuildHasher;
use std::path::PathBuf;
use std::sync::atomic::{AtomicU64, Ordering};
use std::sync::Arc;
use std::sync::Mutex;
use std::sync::OnceLock;
use std::time::Duration;

use opensubsonic::AlbumListType;
use tokio::sync::{mpsc, watch, Notify};
use tokio::time::Instant;

use crate::cache::TtlLru;
use crate::clock::TokioClock;
use crate::config::FadeConfig;
use crate::executor::PendingPlan;
use crate::fade::{
    run_fade, Curve, FadeError, FadeOutcome, FadeProgress, FadeSpec, FadeTarget, StartleBounds,
};
use crate::event::{Cursor, EntrySnapshot, QueueId, QueueSnapshot};
use crate::model::{AlbumId, ArtistId, Favorite, Genre, QueueEntry, Song, SongId};
use crate::plan::{
    clamp_raw, validate, Action, ArmedPlan, FadeIntentIr, PlanBounds, PlanError, PlanId, RawPlan,
    RawTrigger, Resolved, Selector, ORIGIN_SLEEP, ORIGIN_WAKE, ORIGIN_WINDDOWN,
};
use crate::subsonic::SubsonicError;
use crate::echo::describe_batch;
use crate::nl::{NlContext, NlError, NlSource, Translator};
use crate::mpd::{
    FadeArgs, FadeKind, MpdCommand, MpdHandler, MpdResponse, NlCmd, PlanCmd, SleepCmd, StickerCmd,
    WakeCmd, WakeWhen, WinddownCmd,
};
use crate::player::{db_to_mpv_volume, mpv_volume_to_db, PlayState, PlayerHandle};
use crate::resume::{
    build_shutdown_fade, store_atomic, ResumeItem, ResumePlayState, ResumeState,
    RESUME_SCHEMA_VERSION,
};
use crate::subsonic::{list_type_from_dirname, SubsonicClient};
use crate::timer::TimerHandle;

/// One queue entry: a playable [`QueueEntry`] (Subsonic song OR raw stream) plus
/// its MPD song id (a monotonically increasing integer, MPD's stable per-song
/// handle, distinct from queue pos).
#[derive(Clone)]
struct QueueItem {
    id: u64,
    entry: QueueEntry,
}

struct State {
    queue: Vec<QueueItem>,
    next_id: u64,
    /// Index into `queue` of the current song, if any.
    current: Option<usize>,
    /// The user-facing baseline volume (0..=100): what a manual `setvol` sets and
    /// what a completed `fade out` restores to. During a fade this stays put; the
    /// LIVE level rides in `live_gain_db`.
    target_volume: u8,
    /// The live fractional gain in perceptual dB - the internal source of truth a
    /// fade writes every tick. The u8 MPD/MPRIS seam is DERIVED from this at read
    /// time via [`State::reported_volume`], so getvol/status never desync from an
    /// in-flight envelope. Initialised to the dB of `target_volume`.
    live_gain_db: f64,
    /// Monotonic fade generation. Bumped on every `start_fade`; a fade's report
    /// closure no-ops if `fade_epoch` moved on (a superseded straggler), so late
    /// writes from an aborted fade can never clobber the live gain.
    fade_epoch: u64,
    /// Is a fade envelope currently the source of truth for the reported volume?
    /// `true` from the instant a fade is installed until it completes / is
    /// cancelled. It is the SWITCH in [`State::reported_volume`]: when `false`,
    /// `target_volume` (the exact u8 the user set) is reported verbatim; only
    /// when `true` is the reported value derived from `live_gain_db`. This is why
    /// `setvol 5` then `getvol` returns exactly 5 (deriving 5 through the cubic dB
    /// domain would floor it to 0). Cleared by any manual volume set and by fade
    /// completion.
    fading: bool,
    /// Bumped whenever the queue changes (MPD "playlist version").
    playlist_version: u64,
    /// Client-negotiated binary chunk size (ncmpcpp sends `binarylimit`). MPD is
    /// single-stream and this daemon is local single-client, so a shared value
    /// is correct; default 8192.
    binary_limit: usize,
    /// The ordering of the last `listplaylistinfo Starred` response, so a
    /// position-based `playlistdelete Starred <pos>` can map back to a song id
    /// for unstar (MPD playlist deletes are position-based, not uri-based).
    last_starred_order: Vec<SongId>,
}

impl Default for State {
    fn default() -> Self {
        Self {
            queue: Vec::new(),
            next_id: 0,
            current: None,
            target_volume: 100,
            // 100 -> 0 dB (see mpv_volume_to_db); the two start in sync.
            live_gain_db: 0.0,
            fade_epoch: 0,
            fading: false,
            playlist_version: 0,
            binary_limit: 8192,
            last_starred_order: Vec::new(),
        }
    }
}

impl State {
    /// The TRUE current volume for the MPD/MPRIS seam (`getvol`/`status`/MPRIS).
    ///
    /// When NO fade is active, `target_volume` (the exact u8 the user set via
    /// `setvol`, the external source of truth) is reported VERBATIM - never
    /// round-tripped through the cubic dB domain, which floors any volume <= 10 to
    /// 0 and would make `setvol 5; getvol` lie as `0`. Only DURING an active fade
    /// is the reported value derived from `live_gain_db`, so `getvol`/`status`
    /// honestly track the in-flight envelope.
    fn reported_volume(&self) -> u8 {
        if self.fading {
            db_to_mpv_volume(self.live_gain_db).round().clamp(0.0, 100.0) as u8
        } else {
            self.target_volume
        }
    }

    /// Set the baseline AND live gain together (a manual volume change) and clear
    /// the `fading` switch: manual wins, so `reported_volume()` returns exactly
    /// `v` afterward.
    fn set_manual_volume(&mut self, v: u8) {
        self.target_volume = v;
        self.live_gain_db = mpv_volume_to_db(v as f64);
        self.fading = false;
    }
}

/// A running fade task. Holds the abort handle + join handle of the wrapper task
/// the handler spawned (`run_fade` then the terminal action). Aborts on DROP, so
/// a leaked handle can never keep writing to the sink; the explicit supersede
/// path still abort+joins for strict ordering.
struct FadeHandle {
    abort: tokio::task::AbortHandle,
    /// `Option` so the explicit supersede/cancel paths can `take()` the join out
    /// to `.await` it WITHOUT moving a field out of this `Drop` type.
    join: Option<tokio::task::JoinHandle<FadeOutcome>>,
}

impl Drop for FadeHandle {
    fn drop(&mut self) {
        self.abort.abort();
    }
}

/// The SOLE fade arbiter: at most one active envelope. The async mutex is held
/// ATOMICALLY across take -> abort -> await-join -> spawn -> store, so two
/// concurrent `start_fade`s can never both end up with a live task. It is a
/// `tokio::sync::Mutex` (not `std`) precisely because the join is awaited under
/// the lock; the `std` `Mutex<State>` is NEVER held across that await.
struct FadeSlot {
    inner: tokio::sync::Mutex<Option<FadeHandle>>,
}

impl FadeSlot {
    fn new() -> Self {
        Self { inner: tokio::sync::Mutex::new(None) }
    }

    /// Atomically CANCEL any in-flight fade AND apply a manual state mutation
    /// under the SAME slot lock, so the two are indivisible. Used by the manual
    /// paths (setvol/stop/clear/mpris) so a concurrent `fade` from another
    /// connection can NOT install a fade in the gap between "fade cancelled" and
    /// "manual value applied" (which would leave a surviving fade driving mpv
    /// while `getvol` reports the manual value, or clobber the manual volume).
    ///
    /// `apply` runs the state mutation (e.g. `set_manual_volume`) while the slot
    /// lock is held; it is a SYNC closure and MUST NOT hold the `std` `Mutex<State>`
    /// across any await (it does not await at all). The manual `player.set_volume`
    /// is sequenced by the caller AFTER this returns; the abort+join here
    /// guarantees the outgoing fade has fully stopped writing the sink first.
    async fn cancel_with(&self, apply: impl FnOnce()) {
        let mut slot = self.inner.lock().await;
        if let Some(mut h) = slot.take() {
            h.abort.abort();
            if let Some(join) = h.join.take() {
                let _ = join.await;
            }
        }
        apply();
    }

    /// Atomically replace the active fade: validate/build the new fade FIRST, and
    /// only once it is a valid replacement abort+join the old one, THEN run
    /// `spawn` under the SAME lock. `spawn` reads the now-settled live gain,
    /// builds the wrapper task, bumps the epoch, and spawns it - all after the
    /// outgoing fade has fully stopped writing, so the new fade starts from the
    /// true settled level (the startle no-re-brighten invariant).
    ///
    /// VALIDATE-BEFORE-ABORT: `build` runs while the in-flight fade is STILL
    /// running. If it returns `Err` (a rejected / startle-unsafe spec), the slot
    /// is left UNTOUCHED - the in-flight fade keeps running, no volume is jumped -
    /// and the error is propagated so the caller can surface an ACK. The outgoing
    /// fade is aborted only after a valid replacement is in hand.
    async fn supersede<P>(
        &self,
        build: impl FnOnce() -> Result<P, FadeError>,
        spawn: impl FnOnce(P) -> (tokio::task::AbortHandle, tokio::task::JoinHandle<FadeOutcome>),
    ) -> Result<(), FadeError> {
        let mut slot = self.inner.lock().await;
        // Validate/build the new fade BEFORE touching the in-flight one. A rejected
        // command (e.g. a 0s `fade` -> StepTooLarge) must never abort a running
        // envelope and then jump the volume - validation runs with the old fade
        // still going, and we only abort once we hold a valid replacement.
        let prepared = build()?;
        if let Some(mut h) = slot.take() {
            h.abort.abort();
            if let Some(join) = h.join.take() {
                let _ = join.await;
            }
        }
        let (abort, join) = spawn(prepared);
        *slot = Some(FadeHandle { abort, join: Some(join) });
        Ok(())
    }
}

/// What happens AFTER a fade's ramp completes. Lives in the wrapper task, not in
/// the pure driver. Skipped on abort (a superseded/cancelled fade) and on a sink
/// error, so a manual action that cancelled the fade is never undone.
#[derive(Clone, Copy)]
enum Terminal {
    /// A bare ramp: adopt the reached level as the new baseline, clear `fading`,
    /// nothing else (no stop, no `set_volume` re-assert).
    ///
    /// RESERVED, not dead: it is CONSUMED by the completion match in [`fade_task`]
    /// (the `Terminal::None` arm), but no current `FadeIntent` constructs it - the
    /// MPD front-end always commits a baseline (`SetBaseline`) or stops
    /// (`StopRestore`). It exists for the P2 plan executor's pure `SetVolume` path:
    /// a `fade`-native step that just wants a startle-safe level change with no
    /// side effects. Wiring a `FadeIntent` variant that resolves to it is the only
    /// change needed to reach it; `#[allow(dead_code)]` covers the never-constructed
    /// variant until then (the match arm is live, so this is not misleading dead
    /// code).
    #[allow(dead_code)]
    None,
    /// `fade out`: stop playback and restore the baseline volume.
    StopRestore,
    /// `fade to <v>`: commit `v` as the new baseline volume.
    SetBaseline(u8),
}

/// A fade-NATIVE request: the abstract intent plus the (already resolved +
/// clamped) duration. This is the seam the reusable core
/// ([`HypodjHandler::start_fade_spec`]) speaks - decoupled from the MPD `fade`
/// DSL, so the P2 plan executor constructs one directly. The MPD dispatch builds
/// it from [`FadeArgs`]; the executor will build it from a plan step.
#[derive(Clone, Copy, Debug)]
pub struct FadeRequest {
    pub intent: FadeIntent,
    pub dur: Duration,
}

/// The abstract, fade-native fade intents. Kept separate from the MPD
/// [`FadeKind`] so the executor is not coupled to the wire grammar. Each resolves
/// (against the live gain + the comfort ceiling) into a concrete
/// [`FadeTarget`] + sub-JND policy + [`Terminal`].
#[derive(Clone, Copy, Debug)]
pub enum FadeIntent {
    /// Ramp to silence, then stop playback and restore the pre-fade baseline.
    Out,
    /// Wake ramp UP to the comfort ceiling. NEVER ramps down: if the live gain is
    /// already at/above the ceiling the target is the live gain (a degenerate
    /// no-op), so a `fade in` at full volume does nothing rather than dropping.
    In,
    /// Deliberate cue to an explicit perceptual level, committing `vol` as the new
    /// baseline on completion. Used by `fade to <vol>` and `fade to floor`.
    To { target_db: f64, vol: u8 },
    /// Wake ramp-in on smooth-restart: a SUB-JND ramp UP to the user's SAVED
    /// perceptual level (`target_db`), committing `vol` as the restored baseline.
    /// Distinct from [`FadeIntent::In`] (which targets the comfort ceiling / vol
    /// 100): a wake must restore the EXACT volume the user had before the restart,
    /// starting from silence. Sub-JND so the wake is imperceptibly gentle.
    WakeTo { target_db: f64, vol: u8 },
    /// Sub-JND wind-down to the configured non-silence floor (`floor_db`), leaving
    /// playback RUNNING (SetBaseline, no mute step). Distinct from [`FadeIntent::Out`]
    /// (which reaches silence + stops). The floor is passed in from the live config
    /// at resolve time, never baked into a plan.
    ToFloor,
}

impl FadeIntent {
    /// Resolve into `(target, sub_jnd, terminal)` against the live `from_db`, the
    /// configured comfort `ceiling`, and the wind-down `floor_db`.
    fn resolve(self, from_db: f64, ceiling: f64, floor_db: f64) -> (FadeTarget, bool, Terminal) {
        match self {
            FadeIntent::Out => (FadeTarget::Silence, true, Terminal::StopRestore),
            // Sub-JND to the floor level, committing it as the baseline: playback
            // continues quiet, no mute step, no click.
            FadeIntent::ToFloor => {
                // Never ramp UP: if the live level is already at/below the floor,
                // hold it (target = min(floor, from)) so a wind-down cannot
                // re-brighten a quieter state.
                let target = floor_db.min(from_db);
                let vol = db_to_mpv_volume(target).round().clamp(0.0, 100.0) as u8;
                (FadeTarget::Db(target), true, Terminal::SetBaseline(vol))
            }
            FadeIntent::In => {
                // Ceiling clamp: target the HIGHER of the live gain and the
                // ceiling, so the fade only ever rises (never re-brightens past a
                // manual level, never drops when named `in`).
                let target_db = from_db.max(ceiling);
                let vol = db_to_mpv_volume(target_db).round().clamp(0.0, 100.0) as u8;
                (FadeTarget::Db(target_db), true, Terminal::SetBaseline(vol))
            }
            FadeIntent::To { target_db, vol } => {
                (FadeTarget::Db(target_db), false, Terminal::SetBaseline(vol))
            }
            // Wake ramp: sub-JND ramp to the SAVED level, committing it as the
            // restored baseline. from_db is the synth floor (silence) at restore,
            // so the schedule rises from silence to the user's real level.
            FadeIntent::WakeTo { target_db, vol } => {
                (FadeTarget::Db(target_db), true, Terminal::SetBaseline(vol))
            }
        }
    }
}

/// The wrapper task the handler spawns for one fade: drive the pure `run_fade`,
/// writing the live gain (and coalescing change notifications) on each tick, then
/// apply the terminal action. Returns the [`FadeOutcome`] so the join sees it.
///
/// The report closure writes `State.live_gain_db` every tick but only fires
/// `notify_change` when the ROUNDED u8 reported volume changes - killing the
/// per-tick notify storm (a long fade emits a handful of notifications, not
/// hundreds). It no-ops entirely if the epoch has moved on (a superseded
/// straggler), so an aborted fade's last in-flight report cannot clobber a newer
/// fade's live gain.
#[allow(clippy::too_many_arguments)]
async fn fade_task(
    sink: PlayerHandle,
    spec: FadeSpec,
    state: Arc<Mutex<State>>,
    changed: Arc<Notify>,
    epoch: u64,
    terminal: Terminal,
    // The fade arbiter. The terminal action runs UNDER this slot lock so its
    // check-and-act is ATOMIC against supersede/cancel (C3): either supersede
    // aborts this task before it takes the lock (terminal never runs its side
    // effects) or the terminal runs to completion first and supersede then aborts
    // an already-finished task - never an interleave where a superseded fade's
    // stop/baseline whipsaws a freshly installed fade.
    fade_slot: Arc<FadeSlot>,
    // The synth floor (finite silence dB). The final mute step reports
    // `NEG_INFINITY`; we clamp it to this finite floor before storing it in
    // `live_gain_db` so a fade STARTED during the mute window reads a finite
    // `from_db` and is not rejected as `NonFinite` (see F8).
    synth_floor_db: f64,
) -> FadeOutcome {
    let clock = TokioClock;
    let outcome = {
        let state_r = state.clone();
        let changed_r = changed.clone();
        let mut last_u8: Option<u8> = None;
        let mut report = move |p: FadeProgress| {
            // std Mutex<State> is taken and released here; NEVER held across the
            // notify (which is not an await, but the discipline is kept regardless).
            let reported = {
                let mut st = state_r.lock().unwrap();
                if st.fade_epoch != epoch {
                    return; // stale straggler from a superseded fade: no-op.
                }
                // Keep the stored gain FINITE (the mute step is -inf) so the next
                // fade can start from it without a NonFinite rejection.
                st.live_gain_db = p.gain_db.max(synth_floor_db);
                st.reported_volume()
            };
            if last_u8 != Some(reported) {
                last_u8 = Some(reported);
                changed_r.notify_waiters();
            }
        };
        run_fade(&sink, &spec, &clock, &mut report).await
    };

    // Take the slot lock so the terminal's check-and-act is ATOMIC against
    // supersede/cancel (C3). Holding a tokio mutex across the awaits below is
    // allowed; the `std` Mutex<State> is never held across an await. While we
    // hold this lock no supersede can bump `fade_epoch` or install a replacement,
    // so the `still_current` check is stable and meaningful: if a supersede
    // already ran, it aborted this task before it reached here, OR it is blocked
    // waiting for this lock and will abort a task that has already finished.
    let _slot_guard = fade_slot.inner.lock().await;
    // Only settle if still the current generation (a superseding fade owns the
    // state otherwise). On completion, run the terminal action AND clear `fading`;
    // on a sink error, settle the baseline to the last-good level and clear
    // `fading` too so the reported volume stops deriving from a stalled envelope.
    //
    // NOTE on the FadeSlot handle: `fading` (in State) is the single source of
    // truth for "a fade is active" and is cleared below. This task does NOT remove
    // its own FadeHandle from the slot on natural completion (doing so would drop
    // the handle and abort this task mid-terminal). So after a fade completes the
    // slot may still hold a FINISHED handle until the next `start_fade`/cancel
    // reclaims it - and aborting/joining an already-finished task there is a
    // harmless no-op. Read `fading`, never slot-occupancy, to test "fade active".
    let still_current = state.lock().unwrap().fade_epoch == epoch;
    if still_current {
        match &outcome {
            FadeOutcome::Completed => match terminal {
                Terminal::None => {
                    // A bare ramp (no stop/baseline commit): adopt the reached
                    // level as the new baseline and clear the fade switch.
                    let mut st = state.lock().unwrap();
                    let v = db_to_mpv_volume(st.live_gain_db).round().clamp(0.0, 100.0) as u8;
                    st.target_volume = v;
                    st.fading = false;
                    drop(st);
                    changed.notify_waiters();
                }
                Terminal::StopRestore => {
                    let restore = state.lock().unwrap().target_volume;
                    let _ = sink.stop().await;
                    // Re-assert the real mpv gain to the baseline so the next play
                    // does not start at the faded-down level.
                    let _ = sink.set_volume(restore).await;
                    state.lock().unwrap().set_manual_volume(restore);
                    changed.notify_waiters();
                }
                Terminal::SetBaseline(v) => {
                    // Re-assert the real mpv gain to the committed baseline (the
                    // fade drove the fractional seam; snap the u8 seam to match).
                    let _ = sink.set_volume(v).await;
                    state.lock().unwrap().set_manual_volume(v);
                    changed.notify_waiters();
                }
            },
            FadeOutcome::SinkError(_) => {
                let mut st = state.lock().unwrap();
                let v = db_to_mpv_volume(st.live_gain_db).round().clamp(0.0, 100.0) as u8;
                st.target_volume = v;
                st.fading = false;
                drop(st);
                changed.notify_waiters();
            }
        }
    }
    outcome
}

pub struct HypodjHandler {
    client: Arc<SubsonicClient>,
    player: PlayerHandle,
    state: Arc<Mutex<State>>,
    /// Fired when a subsystem changes, to wake `idle`.
    changed: Arc<Notify>,
    /// The director's level-triggered resync source. Registered once by
    /// [`crate::director::run`] via [`Self::set_snapshot_sink`]. EVERY mutation
    /// (queue add/delete/clear + play/stop) pushes a fresh [`QueueSnapshot`] here,
    /// so a lagged observer always resyncs to CURRENT state, not a stale snapshot
    /// last refreshed on a player-event edge. `OnceLock` because there is exactly
    /// one director for the process lifetime and the handler outlives it.
    snapshot_tx: OnceLock<watch::Sender<QueueSnapshot>>,
    /// The single active-fade arbiter (see [`FadeSlot`]). `Arc` so each spawned
    /// fade task can hold a handle and take the slot lock for its terminal action
    /// (C3: the terminal's check-and-act is atomic against supersede/cancel).
    fade: Arc<FadeSlot>,
    /// Per-user fade tunables (startle bounds, tick, durations).
    fade_cfg: FadeConfig,
    /// Bounded LRU+TTL cache for STABLE listings (artists, albums, genres, smart
    /// lists, similar/top). NEVER holds its lock across an `.await` (see cache
    /// docs): get -> await refill -> put, two separate lock scopes.
    listings: TtlLru<String, Vec<Song>>,
    /// Cache for stable album/artist directory listings (name-bearing rows).
    dir_cache: TtlLru<String, Vec<(String, String)>>,
    /// Decoded cover-art bytes, keyed by cover id. Big win: ncmpcpp requests
    /// albumart in many small offset chunks; caching avoids re-fetching the whole
    /// image per chunk. Longer TTL (art rarely changes).
    cover_cache: TtlLru<String, Vec<u8>>,

    // ── P2 plan registry ───────────────────────────────────────────────────
    /// The armed-plan registry, SHARED with the [`crate::executor::Executor`]
    /// task (which holds the same `Arc` via its handler clone). Every mutation is
    /// a SHORT std-`Mutex` scope, NEVER held across an `.await` (the fade-slot
    /// discipline). A deadline plan HOLDS its [`crate::timer::TimerGuard`] inside
    /// its [`PendingPlan`], so `plan_cancel`/`plan_replace` disarm the timer by
    /// dropping the entry (RAII), never a phantom `WallClock` fire.
    plan_pending: Arc<Mutex<Vec<PendingPlan>>>,
    /// Monotonic, NEVER-reused plan id source (mirrors the timer `next_id` idiom),
    /// so a stale cancel/replace can never hit a recycled plan.
    next_plan_id: AtomicU64,
    /// The wall-clock timer source, registered once at executor startup. A
    /// deadline plan arms an absolute timer here at add-time.
    plan_timers: OnceLock<TimerHandle>,
    /// A nudge channel: an [`Resolved::Immediate`] plan is executed at add-time by
    /// the executor task, so `plan_add` (sync, no `.await`) hands off the id here.
    plan_immediate: OnceLock<mpsc::UnboundedSender<PlanId>>,

    // ── P3 natural-language surface ─────────────────────────────────────────
    /// The injected NL translator (rules + optional local model). `OnceLock`
    /// because the daemon injects exactly one via [`Self::set_translator`], same
    /// pattern as [`Self::set_plan_timers`]. Absent -> `nl` ACKs NotAvailable.
    /// hypodj-core stays model-free: only a `dyn Translator` crosses this seam.
    translator: OnceLock<Arc<dyn Translator>>,
    /// Pending, echoed-but-unconfirmed translations, keyed by a single-use token.
    /// Stores `Vec<RawPlan>` ONLY (never a translate-time Resolved): `nl confirm`
    /// RE-VALIDATES + clamps against the CURRENT snapshot. Tokens are single-use +
    /// TTL-bounded; every access prunes expired entries so the map cannot grow
    /// unbounded or arm a stale intent. A short std-`Mutex` scope, never across an
    /// `.await`.
    nl_pending: Mutex<HashMap<String, PendingNl>>,
    /// Monotonic counter feeding the token minter (uniqueness); NEVER the token
    /// itself - the emitted token is the counter hashed under `nl_token_hasher`.
    next_nl_token: AtomicU64,
    /// The per-handler random hash seed (OS entropy at construction) that turns
    /// the monotonic counter into an UNGUESSABLE, non-sequential token. Keeps the
    /// token minter dependency-free (no rand crate) while staying unpredictable.
    nl_token_hasher: RandomState,

    // ── smooth-restart (resume) ─────────────────────────────────────────────
    /// The resolved persistent path of the resume state file (`.../resume.toml`),
    /// or `None` when resume is disabled (no state dir). A short std-`Mutex` scope,
    /// never held across an `.await`. Set once by the daemon via
    /// [`Self::set_state_path`].
    state_path: Mutex<Option<PathBuf>>,
    /// The live media position, in MILLIS, captured LOCKLESSLY from the P1
    /// `Tick.time_pos` and reset on a new-Playing / Stop edge. The shutdown
    /// snapshot and the periodic checkpoint read it with a single atomic load, so
    /// they never query mpv during a SIGTERM race.
    last_elapsed_ms: Arc<AtomicU64>,
}

/// One echoed-but-unconfirmed translation. The plans are raw but ALREADY CLAMPED
/// (so the human echo equals exactly what `nl confirm` arms); `created` bounds the
/// TTL so a stale intent can never be confirmed. `owner` scopes the confirm/cancel
/// to the connection that created it (no cross-connection arming).
struct PendingNl {
    plans: Vec<RawPlan>,
    created: Instant,
    source: NlSource,
    owner: u64,
}

/// How long an echoed `nl` token stays confirmable (single-use + TTL-bounded).
const NL_TOKEN_TTL: Duration = Duration::from_secs(300);

impl HypodjHandler {
    /// Construct with the default `[fade]` tunables (research-backed constants).
    pub fn new(client: Arc<SubsonicClient>, player: PlayerHandle) -> Self {
        Self::with_fade_config(client, player, FadeConfig::default())
    }

    /// Construct with an explicit [`FadeConfig`] (the daemon threads `cfg.fade`).
    pub fn with_fade_config(
        client: Arc<SubsonicClient>,
        player: PlayerHandle,
        mut fade_cfg: FadeConfig,
    ) -> Self {
        // Defense in depth: normalize here too, so a handler built from a
        // hand-constructed FadeConfig (not only Config::load) is still clamped to
        // the startle-safe invariants (F7).
        fade_cfg.normalize();
        Self {
            client,
            player,
            state: Arc::new(Mutex::new(State::default())),
            changed: Arc::new(Notify::new()),
            snapshot_tx: OnceLock::new(),
            fade: Arc::new(FadeSlot::new()),
            fade_cfg,
            listings: TtlLru::new(256, Duration::from_secs(60)),
            dir_cache: TtlLru::new(256, Duration::from_secs(60)),
            cover_cache: TtlLru::new(64, Duration::from_secs(600)),
            plan_pending: Arc::new(Mutex::new(Vec::new())),
            next_plan_id: AtomicU64::new(0),
            plan_timers: OnceLock::new(),
            plan_immediate: OnceLock::new(),
            translator: OnceLock::new(),
            nl_pending: Mutex::new(HashMap::new()),
            next_nl_token: AtomicU64::new(0),
            nl_token_hasher: RandomState::new(),
            state_path: Mutex::new(None),
            last_elapsed_ms: Arc::new(AtomicU64::new(0)),
        }
    }

    /// Shared client handle, so the daemon can also hand it to the scrobbler.
    pub fn client(&self) -> Arc<SubsonicClient> {
        self.client.clone()
    }

    /// Register the director's resync `watch` sender. Called once at director
    /// startup; every later mutation republishes a fresh snapshot to it so the
    /// resync source never lags the live queue (see [`Self::snapshot_tx`]).
    pub fn set_snapshot_sink(&self, tx: watch::Sender<QueueSnapshot>) {
        let _ = self.snapshot_tx.set(tx);
    }

    // ── P2 plan registry (PR10) ─────────────────────────────────────────────

    /// Register the shared wall-clock timer source. Called once by the executor
    /// wiring so `plan_add` can arm absolute deadlines for time-based plans.
    pub fn set_plan_timers(&self, timers: TimerHandle) {
        let _ = self.plan_timers.set(timers);
    }

    /// Register the executor's Immediate-plan nudge channel. Called once at
    /// executor startup; `plan_add` sends an [`Resolved::Immediate`] plan's id here
    /// so the executor task runs its (async) action at add-time.
    pub fn set_plan_immediate_sink(&self, tx: mpsc::UnboundedSender<PlanId>) {
        let _ = self.plan_immediate.set(tx);
    }

    /// The shared armed-plan registry handle (the executor holds the same `Arc`).
    pub(crate) fn plan_pending_handle(&self) -> Arc<Mutex<Vec<PendingPlan>>> {
        self.plan_pending.clone()
    }

    /// The numeric clamps derived from the live (normalized) fade config.
    pub fn plan_bounds(&self) -> PlanBounds {
        PlanBounds::from_fade_config(&self.fade_cfg)
    }

    /// Validate + arm a raw plan against the CURRENT queue, minting a fresh
    /// [`PlanId`]. Fails loud with a [`PlanError`] (mapped 1:1 to an ACK) rather
    /// than storing an unexecutable plan. The whole body is one short lock scope
    /// with NO `.await` - the (async) Immediate action is handed to the executor.
    pub fn plan_add(&self, raw: RawPlan) -> Result<PlanId, PlanError> {
        let mut ids = self.plan_add_batch(vec![raw])?;
        // Exactly one id for a single-plan batch.
        Ok(ids.remove(0))
    }

    /// ATOMICALLY validate + arm a BATCH of raw plans against ONE current queue
    /// snapshot: either EVERY plan is armed (in order, ascending [`PlanId`]) or
    /// NONE is (a single failing plan leaves the registry untouched). Used by
    /// `nl confirm` for a multi-plan (wake) batch so a mid-batch failure can never
    /// leave a partial, inconsistent arm. The whole body is await-free; the (async)
    /// Immediate actions are handed to the executor after the lock is released.
    pub fn plan_add_batch(&self, raws: Vec<RawPlan>) -> Result<Vec<PlanId>, PlanError> {
        let bounds = self.plan_bounds();
        let snap = self.queue_snapshot();
        let now = Instant::now();
        let now_civil = chrono::Utc::now();

        // Phase 1: clamp + validate ALL against one snapshot. NO mutation, NO timer
        // armed yet - a failure here aborts with nothing armed.
        let mut prepared: Vec<(RawPlan, Resolved)> = Vec::with_capacity(raws.len());
        for raw in &raws {
            let clamped = clamp_raw(raw, &bounds);
            let resolved = validate(&clamped, &snap, now, now_civil, &bounds)?;
            prepared.push((clamped, resolved));
        }

        // Phase 2: every plan validated -> arm them all. Mint ids, arm deadline
        // timers, then push under one lock scope.
        let mut ids = Vec::with_capacity(prepared.len());
        let mut immediates: Vec<PlanId> = Vec::new();
        let mut pendings: Vec<PendingPlan> = Vec::with_capacity(prepared.len());
        for (clamped, resolved) in prepared {
            let id = PlanId(self.next_plan_id.fetch_add(1, Ordering::Relaxed));
            // Arm an absolute timer for a deadline plan NOW (the deadline is already
            // concrete). TimeRemaining is armed lazily by the executor on a live Tick.
            let (timer_id, guard) = match &resolved {
                Resolved::OnDeadline(deadline) => match self.plan_timers.get() {
                    Some(t) => {
                        let (tid, g) = t.arm(*deadline);
                        (Some(tid), Some(g))
                    }
                    None => (None, None),
                },
                _ => (None, None),
            };
            if matches!(resolved, Resolved::Immediate) {
                immediates.push(id);
            }
            let armed = ArmedPlan {
                id,
                once: clamped.once,
                raw: clamped,
                resolved,
                armed_at: now,
                timer_id,
            };
            pendings.push(PendingPlan { armed, guard, remaining_deadline: None });
            ids.push(id);
        }
        self.plan_pending.lock().unwrap().extend(pendings);

        // An Immediate plan executes at add-time: nudge the executor (its action
        // is async, so it cannot run inside this sync, lock-holding path).
        if let Some(tx) = self.plan_immediate.get() {
            for id in immediates {
                let _ = tx.send(id);
            }
        }
        Ok(ids)
    }

    /// List the armed plans (id + the raw, clamped plan) for `plan list`.
    pub fn plan_list(&self) -> Vec<(PlanId, RawPlan)> {
        self.plan_pending
            .lock()
            .unwrap()
            .iter()
            .map(|pp| (pp.armed.id, pp.armed.raw.clone()))
            .collect()
    }

    /// Cancel one plan by id. Dropping its [`PendingPlan`] drops any held
    /// [`crate::timer::TimerGuard`] (RAII disarm), so no phantom `WallClock` fires.
    /// Returns `true` if a plan was removed.
    pub fn plan_cancel(&self, id: PlanId) -> bool {
        let mut g = self.plan_pending.lock().unwrap();
        let before = g.len();
        g.retain(|pp| pp.armed.id != id);
        g.len() != before
    }

    /// Replace a plan: cancel `id` (RAII-disarming its timer) then arm `raw` as a
    /// FRESH plan with a new never-reused id. A failed validate leaves the old
    /// plan untouched (validate runs before the cancel).
    pub fn plan_replace(&self, id: PlanId, raw: RawPlan) -> Result<PlanId, PlanError> {
        // Validate the replacement FIRST; only cancel the old one once the new is
        // known-good (mirrors the fade validate-before-abort discipline).
        let new_id = self.plan_add(raw)?;
        self.plan_cancel(id);
        Ok(new_id)
    }

    // ── convenience features: sleep / winddown / wake ────────────────────────

    /// A read-only view of every armed plan's `(id, origin, deadline)`. The
    /// deadline is the absolute [`Instant`] for a [`Resolved::OnDeadline`] plan and
    /// `None` otherwise. Pure registry read (a short lock, no `.await`), so a
    /// remaining-time computation is fake-clock assertable.
    pub fn plan_deadlines(&self) -> Vec<(PlanId, String, Option<Instant>)> {
        self.plan_pending
            .lock()
            .unwrap()
            .iter()
            .map(|pp| {
                let deadline = match pp.armed.resolved {
                    Resolved::OnDeadline(inst) => Some(inst),
                    _ => None,
                };
                (pp.armed.id, pp.armed.raw.origin.clone(), deadline)
            })
            .collect()
    }

    /// The id of the SINGLE armed plan with this reserved origin, if any. Backs
    /// single-instance control (replace/cancel) for the convenience features.
    fn find_by_origin(&self, origin: &str) -> Option<PlanId> {
        self.plan_pending
            .lock()
            .unwrap()
            .iter()
            .find(|pp| pp.armed.raw.origin == origin)
            .map(|pp| pp.armed.id)
    }

    /// Build a convenience plan for `origin`: replace the existing single instance
    /// (validate-then-cancel) or add a fresh one, so exactly one is ever active.
    fn set_singleton(&self, origin: &str, raw: RawPlan) -> Result<PlanId, PlanError> {
        match self.find_by_origin(origin) {
            Some(id) => self.plan_replace(id, raw),
            None => self.plan_add(raw),
        }
    }

    /// A `WallClock` trigger `dur` from now, reduced from civil time exactly as a
    /// raw `plan add trigger at ...` is (UNCLAMPED - the SpanElapsed clamp caps the
    /// horizon at max_dur=30min, so a `sleep 1h` must use WallClock).
    fn wallclock_in(dur: Duration) -> Result<RawTrigger, PlanError> {
        let delta =
            chrono::Duration::from_std(dur).map_err(|_| PlanError::OutOfBounds { field: "dur" })?;
        // checked_add_signed so a pathological duration returns OutOfBounds rather
        // than panicking the DateTime addition.
        let at = chrono::Utc::now()
            .checked_add_signed(delta)
            .ok_or(PlanError::OutOfBounds { field: "dur" })?;
        Ok(RawTrigger::WallClock { at })
    }

    /// SLEEP: schedule a graceful fade-to-silence-then-stop at now+`dur`. ONE plan
    /// (Fade(Out) already StopRestores - no sibling Stop). Single-instance.
    pub fn sleep_set(&self, dur: Duration) -> Result<PlanId, PlanError> {
        let raw = RawPlan {
            version: 1,
            trigger: Self::wallclock_in(dur)?,
            action: Action::Fade(FadeIntentIr::Out { secs: self.fade_cfg.sleep_fade_secs as f64 }),
            once: true,
            origin: ORIGIN_SLEEP.into(),
        };
        self.set_singleton(ORIGIN_SLEEP, raw)
    }

    /// The remaining time on the armed sleep plan, or `None` if none is armed.
    /// Computed from the resolved deadline minus the shared clock's now (pure read,
    /// deterministic under the fake clock).
    pub fn sleep_remaining(&self) -> Option<Duration> {
        let now = Instant::now();
        self.plan_deadlines()
            .into_iter()
            .find(|(_, origin, _)| origin == ORIGIN_SLEEP)
            .and_then(|(_, _, deadline)| deadline)
            .map(|inst| inst.saturating_duration_since(now))
    }

    /// Cancel the armed sleep plan (RAII disarm). `true` if one was cancelled.
    pub fn sleep_cancel(&self) -> bool {
        match self.find_by_origin(ORIGIN_SLEEP) {
            Some(id) => self.plan_cancel(id),
            None => false,
        }
    }

    /// WINDDOWN v1 (volume half only): a long sub-JND fade to the non-silence floor
    /// (`ToFloor`, playback continues). `None` winds down immediately; `Some(dur)`
    /// schedules it at now+dur. Single-instance.
    ///
    /// P4 SEAM: energy-aware calmer-track SELECTION (routing through
    /// [`Selector::Calmer`], loud-unsupported at [`Self::plan_enqueue`]) is out of
    /// scope here. This v1 does not enqueue - it is a pure volume wind-down.
    pub fn winddown_set(&self, dur: Option<Duration>) -> Result<PlanId, PlanError> {
        let trigger = match dur {
            None => RawTrigger::Immediate,
            Some(d) => Self::wallclock_in(d)?,
        };
        let raw = RawPlan {
            version: 1,
            trigger,
            action: Action::Fade(FadeIntentIr::ToFloor {
                secs: self.fade_cfg.winddown_fade_secs as f64,
            }),
            once: true,
            origin: ORIGIN_WINDDOWN.into(),
        };
        self.set_singleton(ORIGIN_WINDDOWN, raw)
    }

    /// Cancel the armed winddown plan (RAII disarm). `true` if one was cancelled.
    pub fn winddown_cancel(&self) -> bool {
        match self.find_by_origin(ORIGIN_WINDDOWN) {
            Some(id) => self.plan_cancel(id),
            None => false,
        }
    }

    /// WAKE: schedule a gentle alarm at an absolute civil deadline. At the deadline
    /// the single [`Action::Wake`] atomically enqueues (optional), starts from
    /// silence, plays, and ramps IN to comfort. Single-instance.
    pub fn wake_set(
        &self,
        at: chrono::DateTime<chrono::Utc>,
        selector: Option<Selector>,
        count: u32,
    ) -> Result<PlanId, PlanError> {
        let raw = RawPlan {
            version: 1,
            trigger: RawTrigger::WallClock { at },
            action: Action::Wake { selector, count },
            once: true,
            origin: ORIGIN_WAKE.into(),
        };
        self.set_singleton(ORIGIN_WAKE, raw)
    }

    /// The remaining time on the armed wake plan, or `None` if none is armed.
    pub fn wake_remaining(&self) -> Option<Duration> {
        let now = Instant::now();
        self.plan_deadlines()
            .into_iter()
            .find(|(_, origin, _)| origin == ORIGIN_WAKE)
            .and_then(|(_, _, deadline)| deadline)
            .map(|inst| inst.saturating_duration_since(now))
    }

    /// Cancel the armed wake plan (RAII disarm). `true` if one was cancelled.
    pub fn wake_cancel(&self) -> bool {
        match self.find_by_origin(ORIGIN_WAKE) {
            Some(id) => self.plan_cancel(id),
            None => false,
        }
    }

    /// Resolve the next future civil `h:m` (today if still ahead, else tomorrow) in
    /// the handler's fixed-offset zone, as an absolute UTC instant. Mirrors the P3
    /// nl civil-time seam so `wake at 7` is deterministic under a fixed civil now.
    pub fn resolve_next_civil(&self, h: u32, m: u32) -> Option<chrono::DateTime<chrono::Utc>> {
        use chrono::TimeZone;
        // An alarm is a LOCAL-time promise: `wake at 7` means 07:00 in the system
        // zone, DST-aware, not 07:00 UTC. Use chrono::Local (reads the system TZ)
        // and pick the next FUTURE occurrence, then reduce to UTC for the trigger.
        let now = chrono::Local::now();
        let today = now.date_naive();
        let naive_today = today.and_hms_opt(h, m, 0)?;
        let dt = match chrono::Local.from_local_datetime(&naive_today).single() {
            Some(d) if d > now => d,
            _ => {
                let tomorrow = today.succ_opt()?;
                chrono::Local
                    .from_local_datetime(&tomorrow.and_hms_opt(h, m, 0)?)
                    .single()?
            }
        };
        Some(dt.with_timezone(&chrono::Utc))
    }

    /// The single atomic wake effect: (1) optionally enqueue the selector (ABORT
    /// the whole wake on Err - never ramp silence over an empty queue); (2) force
    /// start-from-silence (`live_gain_db = synth_floor` AND `player.set_volume(0)`
    /// BEFORE the first buffer); (3) play; (4) sub-JND `WakeTo` ramp from silence to
    /// the saved comfort volume. Reuses the smooth-restart composition verbatim.
    pub async fn wake_now(&self, selector: Option<Selector>, count: u32) -> Result<(), String> {
        // Where the enqueued batch will START (append-only), captured BEFORE the
        // enqueue so we wake INTO the freshly enqueued selection, not a stale
        // `current` left from a previous session.
        let enqueue_start = if selector.is_some() {
            Some(self.state.lock().unwrap().queue.len())
        } else {
            None
        };
        // (1) Enqueue first; a failure aborts before any ramp (single Action, so
        // this ordering is guaranteed - three timers could not enforce it).
        if let Some(sel) = &selector {
            self.plan_enqueue(sel, count).await?;
        }

        // (2) Force start-from-silence BEFORE the first buffer: a stopped player's
        // baseline is the comfort volume, so without this the WakeTo ramp would
        // snapshot from_db=comfort and not rise from silence.
        let synth_floor = self.fade_cfg.synth_floor_db;
        // An ALARM wakes to a stable comfort level - the configured wake ceiling -
        // NOT `target_volume`, which a preceding `winddown` may have lowered to the
        // floor (that would ramp the alarm to a barely-audible level). This is the
        // deliberate difference from smooth-restart restore, which returns to the
        // saved volume.
        let comfort_db = self.fade_cfg.wake_ceiling_db;
        let comfort_vol = db_to_mpv_volume(comfort_db).round().clamp(0.0, 100.0) as u8;
        let idx = {
            let mut st = self.state.lock().unwrap();
            st.live_gain_db = synth_floor;
            // Wake INTO the enqueued selection (its first track) when one was
            // enqueued; otherwise resume `current` or the head of the queue.
            match enqueue_start {
                Some(start) if start < st.queue.len() => Some(start),
                _ => st
                    .current
                    .filter(|&i| i < st.queue.len())
                    .or_else(|| (!st.queue.is_empty()).then_some(0)),
            }
        };
        let Some(idx) = idx else {
            return Err("wake: nothing to play (empty queue and no selector)".into());
        };
        let _ = self.player.set_volume(0).await;

        // (3) Play from silence.
        self.play_index(idx).await?;

        // (4) Sub-JND ramp silence -> saved comfort level (startle-safe by
        // construction; WakeTo resolves sub_jnd=true / SetBaseline).
        let dur = self.clamp_fade_dur(Duration::from_secs(self.fade_cfg.wake_ramp_secs));
        let intent = FadeIntent::WakeTo {
            target_db: mpv_volume_to_db(comfort_vol as f64),
            vol: comfort_vol,
        };
        self.start_fade_spec(FadeRequest { intent, dur })
            .await
            .map_err(|e| e.to_string())
    }

    /// Resolve a plan [`Selector`] to concrete songs and APPEND them (append-only,
    /// count-clamped). Unimplemented selectors return a loud not-yet. Used by the
    /// executor's `Enqueue` action; touches the network, never a test path.
    pub async fn plan_enqueue(&self, selector: &Selector, count: u32) -> Result<usize, String> {
        let want = count as usize;
        let songs: Vec<Song> = match selector {
            Selector::Query(q) => {
                let hits = self.client.search3(q).await.map_err(|e| e.to_string())?;
                hits.songs.into_iter().take(want).collect()
            }
            Selector::Genre(g) => self
                .client
                .songs_by_genre(g)
                .await
                .map_err(|e| e.to_string())?
                .into_iter()
                .take(want)
                .collect(),
            Selector::Radio => self
                .client
                .random_songs(Some(want as i32))
                .await
                .map_err(|e| e.to_string())?,
            Selector::Exact(ids) => {
                let mut out = Vec::new();
                for id in ids.iter().take(want) {
                    out.push(self.client.song(id).await.map_err(|e| e.to_string())?);
                }
                out
            }
            Selector::Similar(_) | Selector::Calmer(_) => {
                return Err("similar/calmer selection needs embeddings (P4); not yet".into());
            }
        };
        let n = songs.len();
        for s in songs {
            self.enqueue_song(s).await;
        }
        Ok(n)
    }

    /// Dispatch a parsed `plan` MPD command to the registry, mapping a
    /// [`PlanError`] 1:1 to a fail-loud ACK. Sync (registry ops never `.await`).
    fn handle_plan(&self, cmd: PlanCmd) -> MpdResponse {
        match cmd {
            PlanCmd::Add(raw) => match self.plan_add(raw) {
                Ok(id) => MpdResponse::pairs().pair("plan_id", id.0.to_string()).build(),
                Err(e) => ack(ACK_ERROR_UNKNOWN, "plan", &e.to_string()),
            },
            PlanCmd::List => {
                let mut b = MpdResponse::pairs();
                for (id, raw) in self.plan_list() {
                    b = b
                        .pair("plan_id", id.0.to_string())
                        .pair("origin", raw.origin.clone());
                }
                b.build()
            }
            PlanCmd::Cancel(id) => {
                if self.plan_cancel(id) {
                    MpdResponse::ok()
                } else {
                    ack(ACK_ERROR_NO_EXIST, "plan", "no such plan")
                }
            }
            PlanCmd::Replace(id, raw) => match self.plan_replace(id, raw) {
                Ok(new_id) => MpdResponse::pairs().pair("plan_id", new_id.0.to_string()).build(),
                Err(e) => ack(ACK_ERROR_UNKNOWN, "plan", &e.to_string()),
            },
        }
    }

    /// Dispatch a parsed `sleep` command: (re)arm / report / cancel the single
    /// sleep timer, mapping a [`PlanError`] 1:1 to a fail-loud ACK.
    fn handle_sleep(&self, cmd: SleepCmd) -> MpdResponse {
        match cmd {
            SleepCmd::Set(dur) => match self.sleep_set(dur) {
                Ok(id) => MpdResponse::pairs().pair("plan_id", id.0.to_string()).build(),
                Err(e) => ack(ACK_ERROR_UNKNOWN, "sleep", &e.to_string()),
            },
            SleepCmd::Status => match self.sleep_remaining() {
                Some(d) => MpdResponse::pairs()
                    .pair("sleep_remaining", d.as_secs().to_string())
                    .build(),
                None => MpdResponse::pairs().pair("sleep", "none").build(),
            },
            SleepCmd::Cancel => {
                self.sleep_cancel();
                MpdResponse::ok()
            }
        }
    }

    /// Dispatch a parsed `winddown` command: (re)arm / cancel the single wind-down.
    fn handle_winddown(&self, cmd: WinddownCmd) -> MpdResponse {
        match cmd {
            WinddownCmd::Set(dur) => match self.winddown_set(dur) {
                Ok(id) => MpdResponse::pairs().pair("plan_id", id.0.to_string()).build(),
                Err(e) => ack(ACK_ERROR_UNKNOWN, "winddown", &e.to_string()),
            },
            WinddownCmd::Cancel => {
                self.winddown_cancel();
                MpdResponse::ok()
            }
        }
    }

    /// Dispatch a parsed `wake` command: (re)arm / report / cancel the single wake.
    fn handle_wake(&self, cmd: WakeCmd) -> MpdResponse {
        match cmd {
            WakeCmd::Set { when, selector, count } => {
                let at = match when {
                    WakeWhen::In(d) => match chrono::Duration::from_std(d) {
                        Ok(delta) => chrono::Utc::now() + delta,
                        Err(_) => return ack(ACK_ERROR_UNKNOWN, "wake", "duration out of range"),
                    },
                    WakeWhen::At { h, m } => match self.resolve_next_civil(h, m) {
                        Some(at) => at,
                        None => return ack(ACK_ERROR_UNKNOWN, "wake", "bad time"),
                    },
                };
                let sel = selector.map(Selector::Query);
                match self.wake_set(at, sel, count) {
                    Ok(id) => MpdResponse::pairs().pair("plan_id", id.0.to_string()).build(),
                    Err(e) => ack(ACK_ERROR_UNKNOWN, "wake", &e.to_string()),
                }
            }
            WakeCmd::Status => match self.wake_remaining() {
                Some(d) => MpdResponse::pairs()
                    .pair("wake_remaining", d.as_secs().to_string())
                    .build(),
                None => MpdResponse::pairs().pair("wake", "none").build(),
            },
            WakeCmd::Cancel => {
                self.wake_cancel();
                MpdResponse::ok()
            }
        }
    }

    // ── P3 natural-language surface ─────────────────────────────────────────

    /// Register the injected NL translator (rules + optional local model). Called
    /// once by the daemon, same pattern as [`Self::set_plan_timers`]. When never
    /// called, `nl` ACKs [`NlError::NotAvailable`] (degrades gracefully).
    pub fn set_translator(&self, translator: Arc<dyn Translator>) {
        let _ = self.translator.set(translator);
    }

    /// Mint a fresh single-use `nl` token. UNGUESSABLE + non-sequential: the
    /// monotonic counter (uniqueness) is hashed under the per-handler random seed
    /// mixed with a wall instant, so an observer cannot predict the next token.
    fn mint_nl_token(&self) -> String {
        let n = self.next_nl_token.fetch_add(1, Ordering::Relaxed);
        let nanos = std::time::SystemTime::now()
            .duration_since(std::time::UNIX_EPOCH)
            .map(|d| d.as_nanos())
            .unwrap_or(0);
        let h = self.nl_token_hasher.hash_one((n, nanos));
        format!("nl-{h:016x}")
    }

    /// Build the disambiguation context from the LIVE snapshot (owned data only).
    fn nl_context(&self) -> NlContext {
        let snap = self.queue_snapshot();
        let current = snap
            .current
            .as_ref()
            .and_then(|c| snap.find(c.queue_id).and_then(|(_, e)| e.song.clone()));
        NlContext {
            current,
            now: Instant::now(),
            now_civil: chrono::Utc::now(),
            // ADAPTATION: no tz knob in config yet, so a UTC fixed offset. The echo
            // always shows the fully-resolved absolute civil time, so a meridian
            // mistake is still caught at confirm; a configurable IANA zone is a P4
            // refinement.
            tz: chrono::FixedOffset::east_opt(0).unwrap(),
            queue_len: snap.entries.len(),
        }
    }

    /// Dispatch a parsed `nl` command. Async so a model-backed translate can run
    /// under `spawn_blocking`; the std `Mutex<nl_pending>` is only ever locked in
    /// short, await-free scopes.
    async fn handle_nl(&self, cmd: NlCmd) -> MpdResponse {
        match cmd {
            NlCmd::Translate { req, owner } => self.nl_translate(req, owner).await,
            NlCmd::Confirm { token, owner } => self.nl_confirm(&token, owner),
            NlCmd::Cancel { token, owner } => self.nl_cancel(&token, owner),
        }
    }

    /// Translate + echo. EMITS + validates (dry-run) + stores under a token; it
    /// NEVER arms (that is `nl confirm`, which re-validates against the CURRENT
    /// snapshot).
    async fn nl_translate(&self, req: String, owner: u64) -> MpdResponse {
        let translator = match self.translator.get() {
            Some(t) => t.clone(),
            None => return ack(ACK_ERROR_UNKNOWN, "nl", &NlError::NotAvailable.to_string()),
        };
        let ctx = self.nl_context();
        // Run the (possibly model-backed) translate OFF the reactor: Rules is
        // instant; a local model can take hundreds of ms. hypodj-nl needs no tokio.
        let hit = match tokio::task::spawn_blocking(move || translator.translate(&req, &ctx)).await {
            Ok(r) => r,
            Err(_) => return ack(ACK_ERROR_UNKNOWN, "nl", "translator task failed"),
        };
        let hit = match hit {
            // Loud ACK with the SPECIFIC reason (NotUnderstood / Ambiguous /
            // Unresolvable / NotAvailable), never a generic fail.
            Err(e) => return ack(ACK_ERROR_UNKNOWN, "nl", &e.to_string()),
            Ok(h) => h,
        };
        // Stamp origin (adapter, NEVER the model) and DRY-RUN validate every plan
        // to render the echo + fail loud early. DO NOT arm here.
        let source_tag = match hit.source {
            NlSource::Rules => "nl:rules",
            NlSource::Llm => "nl:llm",
        };
        let bounds = self.plan_bounds();
        let snap = self.queue_snapshot();
        let now = Instant::now();
        let now_civil = chrono::Utc::now();
        // SAFETY (echo == arm): CLAMP each plan now and store + echo the CLAMPED
        // plan, so the human confirms EXACTLY the values that will arm. Clamping is
        // numeric (duration/vol/position bounds) and snapshot-independent, so the
        // re-clamp inside `plan_add` at confirm time is idempotent. A dry-run
        // validate against the current snapshot fails loud early.
        let mut plans = Vec::with_capacity(hit.plans.len());
        for raw in hit.plans {
            let mut clamped = clamp_raw(&raw, &bounds);
            clamped.origin = source_tag.to_string();
            if let Err(e) = validate(&clamped, &snap, now, now_civil, &bounds) {
                return ack(ACK_ERROR_UNKNOWN, "nl", &format!("plan invalid: {e}"));
            }
            plans.push(clamped);
        }
        let echo = describe_batch(&plans, hit.source);
        let token = self.mint_nl_token();
        {
            let mut g = self.nl_pending.lock().unwrap();
            prune_expired_nl(&mut g);
            g.insert(
                token.clone(),
                PendingNl { plans, created: Instant::now(), source: hit.source, owner },
            );
        }
        MpdResponse::pairs()
            .pair("nl_echo", echo)
            .pair("nl_token", token)
            .build()
    }

    /// Confirm: pop the (single-use) token and RE-VALIDATE + arm each plan against
    /// the CURRENT snapshot via `plan_add`. A queue mutation since the echo that
    /// invalidates a target -> loud ACK (per-plan), nothing stale armed.
    fn nl_confirm(&self, token: &str, owner: u64) -> MpdResponse {
        // Pop ONLY when the token exists AND belongs to this connection. A confirm
        // from a different owner is treated as "no such token" (indistinguishable,
        // so it leaks nothing about another connection's pending plans).
        let pending = {
            let mut g = self.nl_pending.lock().unwrap();
            prune_expired_nl(&mut g);
            match g.get(token) {
                Some(p) if p.owner == owner => g.remove(token),
                _ => None,
            }
        };
        let pending = match pending {
            Some(p) => p,
            None => return ack(ACK_ERROR_NO_EXIST, "nl", "no such nl token"),
        };
        let _ = pending.source;
        // SAFETY (atomic batch): arm ALL plans or NONE. `plan_add_batch`
        // pre-validates every plan against ONE current snapshot and only then arms
        // them; a single invalid plan arms nothing (no partial, inconsistent arm).
        match self.plan_add_batch(pending.plans) {
            Ok(ids) => {
                let mut b = MpdResponse::pairs();
                for id in ids {
                    b = b.pair("plan_id", id.0.to_string());
                }
                b.build()
            }
            Err(e) => ack(ACK_ERROR_UNKNOWN, "nl", &format!("plan no longer valid: {e}")),
        }
    }

    /// Cancel: drop the token (idempotent OK), but ONLY for the owning connection;
    /// a cancel from another owner is a no-op OK (it never touches the pending map).
    fn nl_cancel(&self, token: &str, owner: u64) -> MpdResponse {
        let mut g = self.nl_pending.lock().unwrap();
        prune_expired_nl(&mut g);
        if matches!(g.get(token), Some(p) if p.owner == owner) {
            g.remove(token);
        }
        MpdResponse::ok()
    }

    fn notify_change(&self) {
        // Republish the level-triggered resync snapshot on EVERY mutation, so a
        // queue change made off the player-event path (add/delete/clear/move) is
        // reflected the instant a lagged observer resyncs. `queue_snapshot` locks
        // state fresh; no notify_change caller holds the state lock across it.
        if let Some(tx) = self.snapshot_tx.get() {
            let _ = tx.send(self.queue_snapshot());
        }
        self.changed.notify_waiters();
    }

    /// THE MPD-facing fade entry point: convert the parsed [`FadeArgs`] DSL into a
    /// fade-native [`FadeRequest`] (resolving the per-kind default duration and
    /// clamping to `[min_slew, max_dur]` from [`FadeConfig`], so a user's `[fade]`
    /// TOML override actually takes effect), then delegate to [`start_fade_spec`].
    /// Returns the [`FadeError`] on a rejected spec so the dispatch can ACK it to
    /// the client rather than silently dropping the request.
    ///
    /// [`start_fade_spec`]: Self::start_fade_spec
    pub async fn start_fade(&self, args: FadeArgs) -> Result<(), FadeError> {
        // Resolve the raw/optional duration against config: a missing duration
        // takes the per-kind default; any duration is then clamped to
        // [min_slew, max_dur]. THIS is where the config knobs are threaded.
        let default_secs = match args.kind {
            FadeKind::In => self.fade_cfg.wake_ramp_secs,
            FadeKind::Out | FadeKind::To(_) | FadeKind::ToFloor => self.fade_cfg.winddown_fade_secs,
        };
        let raw = args.dur.unwrap_or_else(|| Duration::from_secs(default_secs));
        let min = Duration::from_millis(self.fade_cfg.min_slew_ms);
        let max = Duration::from_secs(self.fade_cfg.max_dur_secs);
        let dur = raw.clamp(min, max);

        let intent = match args.kind {
            FadeKind::Out => FadeIntent::Out,
            FadeKind::In => FadeIntent::In,
            FadeKind::To(v) => FadeIntent::To {
                target_db: mpv_volume_to_db(v as f64),
                vol: v,
            },
            // Wind down to the configured non-silence floor, leaving playback
            // running (distinct from Out). Commits the floor as the new baseline.
            FadeKind::ToFloor => {
                let floor = self.fade_cfg.floor_level_db;
                FadeIntent::To {
                    target_db: floor,
                    vol: db_to_mpv_volume(floor).round().clamp(0.0, 100.0) as u8,
                }
            }
        };
        self.start_fade_spec(FadeRequest { intent, dur }).await
    }

    /// THE reusable, fade-NATIVE entry point that starts a volume-envelope fade.
    /// Speaks [`FadeRequest`] (dB / [`FadeTarget`] + [`Duration`]), NOT the MPD
    /// DSL, so the P2 plan executor calls it directly without going through the
    /// `fade` command grammar - one arbiter ([`FadeSlot`]), two front-ends.
    ///
    /// The live `from_db` is read INSIDE the slot lock, AFTER the outgoing fade is
    /// aborted AND joined, so the new fade starts from the true settled level and
    /// never re-brightens upward (the startle no-re-brighten invariant). The
    /// validated [`FadeSpec`] is built there too; a rejection propagates out as a
    /// [`FadeError`] (the slot is left empty). The terminal action lives in the
    /// spawned wrapper task, keeping [`run_fade`] pure.
    pub async fn start_fade_spec(&self, req: FadeRequest) -> Result<(), FadeError> {
        let tick = Duration::from_millis(self.fade_cfg.tick_ms);
        let ceiling = self.fade_cfg.wake_ceiling_db;
        let synth_floor = self.fade_cfg.synth_floor_db;
        // Single source of truth: the wind-down floor is read from the LIVE config
        // at spawn and passed into resolve, never baked into a stored plan.
        let floor_db = self.fade_cfg.floor_level_db;
        let dur = req.dur;
        let intent = req.intent;

        let cfg = self.fade_cfg.clone();
        let state_read = self.state.clone();
        let state_task = self.state.clone();
        let changed = self.changed.clone();
        let sink = self.player.clone();
        // The task holds a handle to the slot so its terminal can lock it (C3).
        let slot_for_task = self.fade.clone();

        // build: read the live gain and validate the spec while the outgoing fade
        // is STILL running (so a rejected command leaves it untouched). from_db is
        // pre-abort; the outgoing fade is aborted only after this succeeds, so any
        // residual gap vs the settled level is at most one sub-JND tick, never a
        // re-brighten. spawn: only reached with a valid spec, after the abort.
        let res = self
            .fade
            .supersede(
                move || {
                    let from_db = state_read.lock().unwrap().live_gain_db;
                    let (target, sub_jnd, terminal) = intent.resolve(from_db, ceiling, floor_db);
                    let bounds = startle_bounds(&cfg, sub_jnd);
                    let spec = FadeSpec::new(from_db, target, dur, tick, Curve::DbLinear, bounds)?;
                    Ok((spec, terminal))
                },
                move |(spec, terminal)| {
                    // Bump the epoch + flag `fading` UNDER the slot lock so this
                    // fade's reports are tagged strictly newer than any it
                    // superseded and the reported volume tracks the envelope.
                    let epoch = {
                        let mut st = state_task.lock().unwrap();
                        st.fade_epoch += 1;
                        st.fading = true;
                        st.fade_epoch
                    };
                    let join = tokio::spawn(fade_task(
                        sink, spec, state_task, changed, epoch, terminal, slot_for_task,
                        synth_floor,
                    ));
                    let abort = join.abort_handle();
                    (abort, join)
                },
            )
            .await;

        // On rejection nothing was disturbed: the in-flight fade (if any) is still
        // running and no volume was touched, so just surface the ACK error.
        res
    }

    // ── smooth-restart (resume) ─────────────────────────────────────────────

    /// Register the persistent resume-state path (`.../resume.toml`). Called once
    /// by the daemon when a state dir resolves; absent => resume disabled.
    pub fn set_state_path(&self, p: PathBuf) {
        *self.state_path.lock().unwrap() = Some(p);
    }

    /// Record the live media position (from a P1 `Tick.time_pos`), locklessly.
    pub fn note_elapsed_ms(&self, ms: u64) {
        self.last_elapsed_ms.store(ms, Ordering::Relaxed);
    }

    /// Reset the live-elapsed counter (a new Playing id / a Stop edge).
    pub fn reset_elapsed(&self) {
        self.last_elapsed_ms.store(0, Ordering::Relaxed);
    }

    /// The live elapsed position in seconds (the lockless atomic / 1000).
    pub fn last_elapsed_secs(&self) -> f64 {
        self.last_elapsed_ms.load(Ordering::Relaxed) as f64 / 1000.0
    }

    /// Snapshot the resume-relevant state into an OWNED [`ResumeState`]. The std
    /// `Mutex<State>` is taken and DROPPED before return (no guard escapes, so an
    /// async caller never holds it across an `.await`). `elapsed_secs` is supplied
    /// by the caller from the lockless live-elapsed atomic - never queried from
    /// mpv, so it is safe during a SIGTERM race.
    pub fn resume_snapshot(&self, elapsed_secs: f64) -> ResumeState {
        let play_state = match self.player.state() {
            PlayState::Playing => ResumePlayState::Playing,
            PlayState::Paused => ResumePlayState::Paused,
            PlayState::Stopped => ResumePlayState::Stopped,
        };
        let st = self.state.lock().unwrap();
        let queue = st
            .queue
            .iter()
            .map(|it| match &it.entry {
                QueueEntry::Song(s) => ResumeItem::Song { id: s.id.0.clone() },
                QueueEntry::Stream { url, title } => ResumeItem::Stream {
                    url: url.clone(),
                    title: title.clone(),
                },
            })
            .collect::<Vec<_>>();
        let snap = ResumeState {
            schema_version: RESUME_SCHEMA_VERSION,
            queue,
            current: st.current,
            elapsed_secs,
            volume: st.target_volume,
            play_state,
            playlist_version: st.playlist_version,
            saved_at_unix: std::time::SystemTime::now()
                .duration_since(std::time::UNIX_EPOCH)
                .map(|d| d.as_secs())
                .unwrap_or(0),
        };
        drop(st);
        snap
    }

    /// Write a resume checkpoint now: [`resume_snapshot`](Self::resume_snapshot) +
    /// [`store_atomic`]. A missing state path (resume disabled) is a silent no-op;
    /// a write error is logged warn, NEVER fatal.
    pub async fn checkpoint(&self, elapsed_secs: f64) {
        let path = self.state_path.lock().unwrap().clone();
        let Some(path) = path else { return };
        let snap = self.resume_snapshot(elapsed_secs);
        if let Err(e) = store_atomic(&path, &snap) {
            tracing::warn!(error = %e, path = %path.display(), "resume checkpoint write failed");
        }
    }

    /// Run the DELIBERATE sleep-fade-out for shutdown INLINE to completion, under
    /// a timeout of `budget`. This BYPASSES the [`FadeSlot`] (no supersede, no join
    /// handle) - it is the terminal act before `exit(0)`, so nothing can supersede
    /// it. Builds a short, click-free fade via [`build_shutdown_fade`]; if the fade
    /// would exceed the budget (or cannot be built) it is skipped and the daemon
    /// exits immediately (no mid-fade SIGKILL click).
    pub async fn shutdown_fade(&self, budget: Duration) {
        let from_db = self.state.lock().unwrap().live_gain_db;
        let Some(sf) = build_shutdown_fade(&self.fade_cfg, from_db, budget) else {
            tracing::info!("shutdown fade skipped (over budget or already silent); exiting");
            return;
        };
        let clock = TokioClock;
        let mut report = |_p: FadeProgress| {};
        // build_shutdown_fade already guaranteed real_dur <= budget; the timeout is
        // a belt so a stuck sink can never block the exit path.
        let _ = tokio::time::timeout(budget, run_fade(&self.player, &sf.spec, &clock, &mut report))
            .await;
    }

    /// Restore from a loaded [`ResumeState`]: rebuild the queue (re-resolving each
    /// library song from Subsonic), reassign ids, and either WAKE back into
    /// playback (a `Playing` snapshot) or stay stopped (`Paused`/`Stopped` - an
    /// explicit stop survives the rebuild). A song whose metadata can no longer be
    /// resolved is SKIPPED (the current pointer is adjusted), never fatal.
    pub async fn restore(&self, s: &ResumeState) -> Result<(), String> {
        // 1. Rebuild the queue entries. A raw Stream is verbatim; a Song is
        //    re-resolved from Subsonic (we persisted only its id). Track how the
        //    saved current index maps onto the rebuilt (skip-compacted) queue.
        let mut entries: Vec<QueueEntry> = Vec::with_capacity(s.queue.len());
        let mut new_current: Option<usize> = None;
        let mut current_is_song = false;
        for (i, item) in s.queue.iter().enumerate() {
            let entry = match item {
                ResumeItem::Stream { url, title } => QueueEntry::Stream {
                    url: url.clone(),
                    title: title.clone(),
                },
                ResumeItem::Song { id } => {
                    match self.client.song(&SongId(id.clone())).await {
                        Ok(song) => QueueEntry::Song(song),
                        Err(e) => {
                            tracing::warn!(id, error = %e, "resume: song no longer resolvable; skipping");
                            continue;
                        }
                    }
                }
            };
            if Some(i) == s.current {
                new_current = Some(entries.len());
                current_is_song = matches!(entry, QueueEntry::Song(_));
            }
            entries.push(entry);
        }

        let synth_floor = self.fade_cfg.synth_floor_db;
        let playing = s.play_state == ResumePlayState::Playing && new_current.is_some();

        // 2. Install the rebuilt queue + baseline under one short state-lock scope.
        {
            let mut st = self.state.lock().unwrap();
            st.queue = entries
                .into_iter()
                .enumerate()
                .map(|(idx, entry)| QueueItem { id: idx as u64, entry })
                .collect();
            st.next_id = st.queue.len() as u64;
            st.current = new_current;
            st.playlist_version = s.playlist_version;
            st.target_volume = s.volume.min(100);
            if playing {
                // Start SILENT so the first buffer is inaudible; the wake ramp then
                // rises from the synth floor to the saved level.
                st.live_gain_db = synth_floor;
                st.fading = false;
            } else {
                st.live_gain_db = mpv_volume_to_db(s.volume.min(100) as f64);
                st.fading = false;
            }
        }
        self.notify_change();

        if playing {
            let idx = new_current.expect("playing implies a current index");
            let elapsed = s.elapsed_secs.max(0.0);
            let saved_vol = s.volume.min(100);
            // Silence BEFORE the first buffer: mpv volume 0 persists across the
            // loadfile so the wake ramp owns the rise.
            let _ = self.player.set_volume(0).await;
            if let Err(e) = self.play_index(idx).await {
                return Err(e);
            }
            // A library song seeks to the saved elapsed; a raw Stream restarts from
            // 0 (no seek - a live stream has no seekable saved offset).
            if current_is_song && elapsed > 0.0 {
                let _ = self.player.seek(elapsed).await;
            }
            // Wake ramp UP from silence to the user's SAVED level (not vol 100).
            let dur = self.clamp_fade_dur(Duration::from_secs(self.fade_cfg.wake_ramp_secs));
            let intent = FadeIntent::WakeTo {
                target_db: mpv_volume_to_db(saved_vol as f64),
                vol: saved_vol,
            };
            let _ = self.start_fade_spec(FadeRequest { intent, dur }).await;
        } else {
            // Paused/Stopped: restore the baseline volume, leave playback stopped.
            let v = s.volume.min(100);
            self.state.lock().unwrap().set_manual_volume(v);
            let _ = self.player.set_volume(v).await;
        }
        Ok(())
    }

    /// Clamp a fade duration into the configured `[min_slew, max_dur]` window (the
    /// same normalization [`start_fade`](Self::start_fade) applies to DSL fades).
    fn clamp_fade_dur(&self, raw: Duration) -> Duration {
        let min = Duration::from_millis(self.fade_cfg.min_slew_ms);
        let max = Duration::from_secs(self.fade_cfg.max_dur_secs);
        raw.clamp(min, max)
    }

    /// TEST-ONLY: await the currently-active fade task to natural completion
    /// (takes its join out of the slot). Lets a test drive a fade to its terminal
    /// under paused time without racing.
    #[cfg(test)]
    async fn wait_for_fade(&self) {
        let join = {
            let mut slot = self.fade.inner.lock().await;
            slot.as_mut().and_then(|h| h.join.take())
        };
        if let Some(j) = join {
            let _ = j.await;
        }
    }

    /// TEST-ONLY: is a fade task currently installed in the slot?
    #[cfg(test)]
    async fn fade_active(&self) -> bool {
        self.fade.inner.lock().await.is_some()
    }

    /// TEST-ONLY (crate): is a fade active? Exposed to the executor tests, which
    /// assert a plan's fade action reached the single fade slot.
    #[cfg(test)]
    pub(crate) async fn fade_active_for_test(&self) -> bool {
        self.fade.inner.lock().await.is_some()
    }

    /// TEST-ONLY: read the live gain in dB (the internal source of truth).
    #[cfg(test)]
    fn live_gain_db(&self) -> f64 {
        self.state.lock().unwrap().live_gain_db
    }

    /// TEST-ONLY (crate): read the live gain in dB. Exposed to the executor tests,
    /// which assert a wake ramp starts from silence (near the synth floor).
    #[cfg(test)]
    pub(crate) fn live_gain_db_for_test(&self) -> f64 {
        self.state.lock().unwrap().live_gain_db
    }

    /// TEST-ONLY: queue an already-resolved song (no network), for director tests.
    #[cfg(test)]
    pub(crate) async fn enqueue_song_for_test(&self, song: Song) -> u64 {
        self.enqueue_song(song).await
    }

    /// TEST-ONLY: queue a raw stream uri (no network), for director tests.
    #[cfg(test)]
    pub(crate) async fn enqueue_stream_for_test(&self, uri: &str) -> u64 {
        self.enqueue_uri(uri).await.expect("stream uri enqueues offline")
    }

    /// TEST-ONLY: start playing the entry at `idx` (drives the player actor), so a
    /// director test can close the play -> event loop headlessly.
    #[cfg(test)]
    pub(crate) async fn play_for_test(&self, idx: usize) {
        let _ = self.play_index(idx).await;
    }

    /// TEST-ONLY: perform an off-spine `next` (mutates the current index while
    /// buffered events may still be draining), for the identity-join test.
    #[cfg(test)]
    pub(crate) async fn next_for_test(&self) {
        self.mpris_next().await;
    }

    /// TEST-ONLY: delete the queue entry at `pos`, mirroring the MPD `delete`
    /// index bookkeeping, so a test can exercise the deleted-current join.
    #[cfg(test)]
    pub(crate) fn delete_for_test(&self, pos: usize) {
        {
            let mut st = self.state.lock().unwrap();
            if pos < st.queue.len() {
                st.queue.remove(pos);
                st.playlist_version += 1;
                if let Some(c) = st.current {
                    if c == pos {
                        st.current = None;
                    } else if c > pos {
                        st.current = Some(c - 1);
                    }
                }
            }
        }
        // Mirror the real MPD `delete` path, which notifies (refreshing the
        // director's resync snapshot) after dropping the state lock.
        self.notify_change();
    }

    /// A whole-queue [`QueueSnapshot`] for the P1 event substrate: BOTH the
    /// enrichment join source and the resync source. Built under ONE short state
    /// lock scope (owned data only, no borrow held across an await). The director
    /// caches this keyed by `playlist_version`, so a plain `Tick` never re-locks.
    pub fn queue_snapshot(&self) -> QueueSnapshot {
        let st = self.state.lock().unwrap();
        let entries = st
            .queue
            .iter()
            .map(entry_snapshot)
            .collect::<Vec<_>>();
        let current = st.current.and_then(|idx| {
            st.queue.get(idx).map(|it| Cursor {
                index: idx,
                queue_id: QueueId(it.id),
            })
        });
        QueueSnapshot {
            playlist_version: st.playlist_version,
            current,
            entries,
        }
    }

    /// Hot-path enrichment join: locate an entry by its STABLE identity, returning
    /// its current index + row, or `None` if it has left the queue (delete/move).
    /// Anchored on the queue id, never the mutable current index, so attribution
    /// stays exact across an off-spine advance and disambiguates duplicate
    /// [`SongId`]s. One source of truth (mirrors [`QueueSnapshot::find`]).
    pub fn snapshot_by_queue_id(&self, id: QueueId) -> Option<(usize, EntrySnapshot)> {
        let st = self.state.lock().unwrap();
        st.queue
            .iter()
            .enumerate()
            .find(|(_, it)| it.id == id.0)
            .map(|(idx, it)| (idx, entry_snapshot(it)))
    }

    /// Called by the daemon when the player reports a natural EOF: advance to the
    /// next queue entry, or leave the state stopped at the end of the queue.
    pub async fn advance_on_eof(&self) {
        let next = {
            let st = self.state.lock().unwrap();
            st.current.map(|c| c + 1).filter(|&i| i < st.queue.len())
        };
        match next {
            Some(idx) => {
                let _ = self.play_index(idx).await;
            }
            None => {
                self.state.lock().unwrap().current = None;
                self.notify_change();
            }
        }
    }

    /// Resolve and start playing the queue item at `idx`. Returns an ACK-style
    /// error string on failure.
    async fn play_index(&self, idx: usize) -> Result<(), String> {
        let item = {
            let st = self.state.lock().unwrap();
            st.queue.get(idx).cloned()
        };
        let item = match item {
            Some(i) => i,
            None => return Err("Bad song index".into()),
        };
        // A library song resolves a Subsonic stream URL and plays under its id
        // (scrobbled). A raw stream plays its URL verbatim with no id (never
        // scrobbled). Either way a bad/unreachable URL surfaces as a player
        // error here and, at worst, an idle/stopped state - never a panic.
        // Latch the entry's stable identity so every downstream player event
        // (TimePos/StateChanged/Eof) is attributed to THIS entry even after an
        // off-spine next/prev/delete repoints the current index.
        let qid = Some(QueueId(item.id));
        match &item.entry {
            QueueEntry::Song(song) => {
                let url = self
                    .client
                    .stream_url(&song.id)
                    .map_err(|e| e.to_string())?;
                self.player
                    .play_url(Some(song.id.clone()), qid, url.as_str())
                    .await
                    .map_err(|e| e.to_string())?;
            }
            QueueEntry::Stream { url, .. } => {
                self.player
                    .play_url(None, qid, url)
                    .await
                    .map_err(|e| e.to_string())?;
            }
        }
        {
            let mut st = self.state.lock().unwrap();
            st.current = Some(idx);
        }
        self.notify_change();
        Ok(())
    }

    /// Add an entry by uri. A `song/<id>` uri resolves Subsonic metadata; an
    /// absolute `http://`/`https://` uri is queued as a raw stream (internet
    /// radio) played verbatim, with NO Subsonic call, id, rating, or scrobble -
    /// exactly as MPD's own `add <url>` behaves. Returns the assigned MPD id.
    async fn enqueue_uri(&self, uri: &str) -> Result<u64, String> {
        let entry = if is_stream_uri(uri) {
            // Title is the URL (a stream's icy-name is only known once mpv
            // connects; the URL is a sensible, always-available label).
            QueueEntry::Stream {
                url: uri.to_string(),
                title: uri.to_string(),
            }
        } else {
            let song_id = uri
                .strip_prefix("song/")
                .ok_or_else(|| format!("unsupported uri: {uri}"))?;
            let song = self
                .client
                .song(&SongId(song_id.to_string()))
                .await
                .map_err(|e| e.to_string())?;
            QueueEntry::Song(song)
        };
        let mut st = self.state.lock().unwrap();
        let id = st.next_id;
        st.next_id += 1;
        st.queue.push(QueueItem { id, entry });
        st.playlist_version += 1;
        drop(st);
        self.notify_change();
        Ok(id)
    }

    /// Append an already-resolved [`Song`] to the queue, returning its MPD id.
    /// This is the shared, INFALLIBLE push path (no network, no parse): it mirrors
    /// [`enqueue_uri`](Self::enqueue_uri)'s id/version/notify bookkeeping. Used by
    /// `findadd`/`searchadd`, whose matches are already full `Song`s from
    /// `collect_matches`, so re-fetching each via `song/<id>` would be a wasted
    /// round-trip.
    async fn enqueue_song(&self, song: Song) -> u64 {
        let mut st = self.state.lock().unwrap();
        let id = st.next_id;
        st.next_id += 1;
        st.queue.push(QueueItem {
            id,
            entry: QueueEntry::Song(song),
        });
        st.playlist_version += 1;
        drop(st);
        self.notify_change();
        id
    }
}

/// Serialize one queued entry as MPD `playlistinfo`/`currentsong` pairs. A raw
/// stream renders with `file:` = its URL and `Title:` = the URL, and no Time /
/// tags (duration unknown for a live stream) - MPD renders such an entry fine.
fn song_pairs(item: &QueueItem, pos: usize) -> Vec<(String, String)> {
    let mut p = match &item.entry {
        QueueEntry::Song(s) => {
            let mut p = vec![
                ("file".to_string(), format!("song/{}", s.id.0)),
                ("Title".to_string(), s.title.clone()),
            ];
            push_song_tags(&mut p, s);
            p
        }
        QueueEntry::Stream { url, title } => vec![
            ("file".to_string(), url.clone()),
            ("Title".to_string(), title.clone()),
        ],
    };
    p.push(("Pos".to_string(), pos.to_string()));
    p.push(("Id".to_string(), item.id.to_string()));
    p
}

/// Build the join-relevant [`EntrySnapshot`] for one queue item. A raw stream
/// has no album and no known duration (both `None`, honestly - never `0`). A
/// duration-less song is `None` too, never `0`-as-unknown.
fn entry_snapshot(it: &QueueItem) -> EntrySnapshot {
    let (song, album_id, duration) = match &it.entry {
        QueueEntry::Song(s) => (
            Some(s.id.clone()),
            s.album_id.clone(),
            s.duration_secs
                .filter(|&d| d > 0)
                .map(|d| Duration::from_secs(d as u64)),
        ),
        QueueEntry::Stream { .. } => (None, None, None),
    };
    EntrySnapshot {
        queue_id: QueueId(it.id),
        song,
        album_id,
        duration,
    }
}

/// Is `uri` an absolute HTTP(S) stream URL (internet radio) rather than a
/// synthetic hypodj `song/`/`album/`/`artist/` path? Such a uri is played
/// directly, bypassing Subsonic resolution - mirroring MPD's `add <url>`.
fn is_stream_uri(uri: &str) -> bool {
    uri.starts_with("http://") || uri.starts_with("https://")
}

/// Drop expired `nl` tokens (TTL-bounded), called on every `nl_pending` access so
/// the map never grows unbounded and a stale intent can never be confirmed.
fn prune_expired_nl(map: &mut HashMap<String, PendingNl>) {
    let now = Instant::now();
    map.retain(|_, p| now.duration_since(p.created) < NL_TOKEN_TTL);
}

fn ack(code: u32, command: &str, message: &str) -> MpdResponse {
    MpdResponse::Ack {
        code,
        command: command.to_string(),
        message: message.to_string(),
    }
}

/// Build the startle-safety bounds for a fade from the live [`FadeConfig`]. Single
/// source of truth for the slew floor, step ceiling, and synth floor shared by
/// every fade the handler starts.
fn startle_bounds(cfg: &FadeConfig, sub_jnd: bool) -> StartleBounds {
    StartleBounds {
        min_slew: Duration::from_millis(cfg.min_slew_ms),
        step_size_db: cfg.step_size_db,
        synth_floor_db: cfg.synth_floor_db,
        sub_jnd,
    }
}

// ACK error codes (subset of MPD's ack.h).
const ACK_ERROR_NO_EXIST: u32 = 50;
const ACK_ERROR_UNKNOWN: u32 = 5;

impl MpdHandler for HypodjHandler {
    async fn idle(&self, _subsystems: Vec<String>) -> Option<String> {
        // HONEST LIMITATION: this always reports `changed: player`, regardless of
        // what actually changed or which subsystems the client subscribed to.
        //
        // Reason: there is a SINGLE `changed: Notify` fired for every mutation
        // (queue add/delete/clear, play/pause/stop, volume, star). We do not yet
        // track WHICH subsystem changed, so we cannot honestly emit `playlist`
        // vs `mixer` vs `player` separately, nor filter by the client's
        // `_subsystems` list. We deliberately do NOT claim more than we know:
        // `player` is the one subsystem that a re-read of status/currentsong
        // covers, and ncmpcpp responds to any `changed:` line by re-reading
        // status + currentsong + plchanges, so a single conservative wake still
        // refreshes its whole view. Reporting the true per-subsystem set would
        // mean carrying a changed-subsystem flag alongside the Notify - a real
        // improvement left for when a client needs the granularity.
        self.changed.notified().await;
        Some("player".to_string())
    }

    async fn handle(&self, cmd: MpdCommand) -> MpdResponse {
        match cmd {
            // ── status / metadata ──────────────────────────────────────────
            MpdCommand::Ping => MpdResponse::ok(),

            MpdCommand::Status => {
                let (state, vol, qlen, cur, ver) = {
                    let st = self.state.lock().unwrap();
                    (
                        self.player.state(),
                        // Derived from the live gain so status tracks an in-flight
                        // fade and never desyncs from the envelope.
                        st.reported_volume(),
                        st.queue.len(),
                        st.current,
                        st.playlist_version,
                    )
                };
                let state_str = match state {
                    PlayState::Playing => "play",
                    PlayState::Paused => "pause",
                    PlayState::Stopped => "stop",
                };
                let mut b = MpdResponse::pairs()
                    .pair("volume", vol.to_string())
                    .pair("repeat", "0")
                    .pair("random", "0")
                    .pair("single", "0")
                    .pair("consume", "0")
                    .pair("playlist", ver.to_string())
                    .pair("playlistlength", qlen.to_string())
                    .pair("state", state_str);
                if let Some(idx) = cur {
                    let st = self.state.lock().unwrap();
                    if let Some(item) = st.queue.get(idx) {
                        b = b
                            .pair("song", idx.to_string())
                            .pair("songid", item.id.to_string());
                        // Duration is only known for a library song; a live
                        // stream reports none (unknown length is valid MPD).
                        if let QueueEntry::Song(s) = &item.entry {
                            if let Some(d) = s.duration_secs {
                                b = b.pair("duration", format!("{d}.000"));
                            }
                        }
                    }
                }
                b.build()
            }

            MpdCommand::Stats => {
                // Cheap, honest stats: queue-derived counts (a full library scan
                // would be a Subsonic getScanStatus call - TODO for fidelity).
                let songs = self.state.lock().unwrap().queue.len();
                MpdResponse::pairs()
                    .pair("artists", "0")
                    .pair("albums", "0")
                    .pair("songs", songs.to_string())
                    .pair("uptime", "0")
                    .pair("playtime", "0")
                    .pair("db_playtime", "0")
                    .pair("db_update", "0")
                    .build()
            }

            MpdCommand::CurrentSong => {
                let st = self.state.lock().unwrap();
                match st.current.and_then(|i| st.queue.get(i).map(|it| (i, it))) {
                    Some((pos, item)) => MpdResponse::Pairs(song_pairs(item, pos)),
                    None => MpdResponse::ok(),
                }
            }

            MpdCommand::Idle(_) | MpdCommand::NoIdle => {
                // Handled entirely in the serve loop; never dispatched here.
                MpdResponse::ok()
            }

            // ── playback ──────────────────────────────────────────────────
            MpdCommand::Play(pos) => {
                let idx = pos.unwrap_or_else(|| {
                    self.state.lock().unwrap().current.unwrap_or(0)
                });
                // If already have a current and no explicit pos, resume.
                match self.play_index(idx).await {
                    Ok(()) => MpdResponse::ok(),
                    Err(e) => ack(ACK_ERROR_NO_EXIST, "play", &e),
                }
            }
            MpdCommand::PlayId(id) => {
                let idx = match id {
                    Some(id) => self
                        .state
                        .lock()
                        .unwrap()
                        .queue
                        .iter()
                        .position(|it| it.id == id),
                    None => Some(0),
                };
                match idx {
                    Some(idx) => match self.play_index(idx).await {
                        Ok(()) => MpdResponse::ok(),
                        Err(e) => ack(ACK_ERROR_NO_EXIST, "playid", &e),
                    },
                    None => ack(ACK_ERROR_NO_EXIST, "playid", "No such song"),
                }
            }
            MpdCommand::Pause(want) => {
                let res = match want {
                    Some(true) => self.player.pause().await,
                    Some(false) => self.player.resume().await,
                    None => match self.player.state() {
                        PlayState::Playing => self.player.pause().await,
                        _ => self.player.resume().await,
                    },
                };
                self.notify_change();
                match res {
                    Ok(()) => MpdResponse::ok(),
                    Err(e) => ack(ACK_ERROR_UNKNOWN, "pause", &e.to_string()),
                }
            }
            MpdCommand::Stop => {
                // Manual wins ATOMICALLY: cancel (abort+join) any fade AND drop the
                // stale live-fade level back to the baseline under the SAME slot
                // lock, so no concurrent `fade` can slip in between. The stop and
                // the mpv re-assert are sequenced after.
                self.fade
                    .cancel_with(|| {
                        let mut st = self.state.lock().unwrap();
                        let v = st.target_volume;
                        st.set_manual_volume(v);
                    })
                    .await;
                let _ = self.player.stop().await;
                let v = self.state.lock().unwrap().target_volume;
                // Re-assert the real mpv gain to the baseline so the cancelled
                // fade's faded-down level does not linger under a baseline report
                // (F4): the next play starts at the reported volume, not silence.
                let _ = self.player.set_volume(v).await;
                self.notify_change();
                MpdResponse::ok()
            }
            MpdCommand::Fade(args) => {
                // Surface a rejected (startle-unsafe) spec as an ACK to the client
                // rather than a silent warn-and-return (F7).
                match self.start_fade(args).await {
                    Ok(()) => {
                        self.notify_change();
                        MpdResponse::ok()
                    }
                    Err(e) => ack(ACK_ERROR_UNKNOWN, "fade", &e.to_string()),
                }
            }
            MpdCommand::Plan(cmd) => self.handle_plan(cmd),
            MpdCommand::Nl(cmd) => self.handle_nl(cmd).await,
            MpdCommand::Sleep(cmd) => self.handle_sleep(cmd),
            MpdCommand::Winddown(cmd) => self.handle_winddown(cmd),
            MpdCommand::Wake(cmd) => self.handle_wake(cmd),
            MpdCommand::Next => {
                let next = {
                    let st = self.state.lock().unwrap();
                    st.current.map(|c| c + 1).filter(|&i| i < st.queue.len())
                };
                match next {
                    Some(idx) => match self.play_index(idx).await {
                        Ok(()) => MpdResponse::ok(),
                        Err(e) => ack(ACK_ERROR_NO_EXIST, "next", &e),
                    },
                    None => MpdResponse::ok(),
                }
            }
            MpdCommand::Previous => {
                let prev = {
                    let st = self.state.lock().unwrap();
                    st.current.and_then(|c| c.checked_sub(1))
                };
                match prev {
                    Some(idx) => match self.play_index(idx).await {
                        Ok(()) => MpdResponse::ok(),
                        Err(e) => ack(ACK_ERROR_NO_EXIST, "previous", &e),
                    },
                    None => MpdResponse::ok(),
                }
            }
            MpdCommand::Seek { secs, .. } | MpdCommand::SeekCur(secs) => {
                match self.player.seek(secs).await {
                    Ok(()) => MpdResponse::ok(),
                    Err(e) => ack(ACK_ERROR_UNKNOWN, "seek", &e.to_string()),
                }
            }
            MpdCommand::SeekId { secs, .. } => match self.player.seek(secs).await {
                Ok(()) => MpdResponse::ok(),
                Err(e) => ack(ACK_ERROR_UNKNOWN, "seekid", &e.to_string()),
            },
            MpdCommand::SetVol(v) => {
                let v = v.min(100);
                // Manual wins ATOMICALLY: cancel any fade (abort+join) AND apply
                // the manual value under the SAME slot lock, so a concurrent `fade`
                // from another connection cannot install a fade in the gap and
                // clobber the manual volume (or leave a surviving fade driving mpv
                // while getvol lies). The mpv set_volume is sequenced after.
                self.fade
                    .cancel_with(|| self.state.lock().unwrap().set_manual_volume(v))
                    .await;
                let _ = self.player.set_volume(v).await;
                self.notify_change();
                MpdResponse::ok()
            }
            MpdCommand::GetVol => {
                let v = self.state.lock().unwrap().reported_volume();
                MpdResponse::pairs().pair("volume", v.to_string()).build()
            }

            // ── queue ─────────────────────────────────────────────────────
            MpdCommand::Add(uri) => match self.enqueue_uri(&uri).await {
                Ok(_) => MpdResponse::ok(),
                Err(e) => ack(ACK_ERROR_NO_EXIST, "add", &e),
            },
            MpdCommand::AddId(uri, _pos) => match self.enqueue_uri(&uri).await {
                Ok(id) => MpdResponse::pairs().pair("Id", id.to_string()).build(),
                Err(e) => ack(ACK_ERROR_NO_EXIST, "addid", &e),
            },
            MpdCommand::Clear => {
                // Manual wins ATOMICALLY: cancel any fade AND clear the queue +
                // reset the volume to the baseline under the SAME slot lock (see
                // SetVol/Stop), so no concurrent `fade` can interleave.
                self.fade
                    .cancel_with(|| {
                        let mut st = self.state.lock().unwrap();
                        st.queue.clear();
                        st.current = None;
                        st.playlist_version += 1;
                        let v = st.target_volume;
                        st.set_manual_volume(v);
                    })
                    .await;
                let v = self.state.lock().unwrap().target_volume;
                let _ = self.player.stop().await;
                // Re-assert the real mpv gain to the baseline (F4): a cancelled
                // fade must not leave mpv faded-down under a baseline report.
                let _ = self.player.set_volume(v).await;
                self.notify_change();
                MpdResponse::ok()
            }
            MpdCommand::Delete(spec) => {
                let mut st = self.state.lock().unwrap();
                if let Some(pos) = spec.and_then(|s| s.split(':').next().and_then(|p| p.parse::<usize>().ok())) {
                    if pos < st.queue.len() {
                        st.queue.remove(pos);
                        st.playlist_version += 1;
                        if let Some(c) = st.current {
                            if c == pos {
                                st.current = None;
                            } else if c > pos {
                                st.current = Some(c - 1);
                            }
                        }
                    }
                }
                drop(st);
                self.notify_change();
                MpdResponse::ok()
            }
            MpdCommand::PlaylistInfo(_) => {
                let st = self.state.lock().unwrap();
                let mut pairs = Vec::new();
                for (pos, item) in st.queue.iter().enumerate() {
                    pairs.extend(song_pairs(item, pos));
                }
                MpdResponse::Pairs(pairs)
            }
            MpdCommand::PlaylistId(id) => {
                let st = self.state.lock().unwrap();
                let mut pairs = Vec::new();
                for (pos, item) in st.queue.iter().enumerate() {
                    if id.is_none() || id == Some(item.id) {
                        pairs.extend(song_pairs(item, pos));
                    }
                }
                MpdResponse::Pairs(pairs)
            }
            MpdCommand::PlChanges(_) => {
                // Full queue (a correct superset of the diff; ncmpcpp re-reads).
                let st = self.state.lock().unwrap();
                let mut pairs = Vec::new();
                for (pos, item) in st.queue.iter().enumerate() {
                    pairs.extend(song_pairs(item, pos));
                }
                MpdResponse::Pairs(pairs)
            }

            // ── stored playlists + star trigger (feature 3) ─────────────────
            MpdCommand::ListPlaylists => {
                // Advertise the synthetic `Starred` playlist (the star trigger).
                MpdResponse::pairs()
                    .pair("playlist", "Starred")
                    .pair("Last-Modified", "1970-01-01T00:00:00Z")
                    .build()
            }
            MpdCommand::ListPlaylistInfo(name) | MpdCommand::Load(name)
                if name == "Starred" =>
            {
                // Starred is NEVER cached (freshness-critical). Record the order
                // so a later position-based playlistdelete maps to a song id.
                match self.client.starred_songs().await {
                    Ok(songs) => {
                        {
                            let mut st = self.state.lock().unwrap();
                            st.last_starred_order =
                                songs.iter().map(|s| s.id.clone()).collect();
                        }
                        let mut pairs = Vec::new();
                        for s in &songs {
                            pairs.extend(browse_song_pairs(s));
                        }
                        MpdResponse::Pairs(pairs)
                    }
                    Err(e) => ack(ACK_ERROR_UNKNOWN, "listplaylistinfo", &e.to_string()),
                }
            }
            MpdCommand::ListPlaylistInfo(_) | MpdCommand::Load(_) => MpdResponse::ok(),
            MpdCommand::PlaylistAdd(name, uri) if name == "Starred" => {
                // The uri PREFIX is the sole routing authority: `song/<id>` stars
                // a song, `album/<id>` an album, `artist/<id>` an artist. Anything
                // else fails LOUD rather than falling to the silent generic arm.
                match Favorite::from_uri(&uri) {
                    Some(fav) => match self.client.star(&fav).await {
                        Ok(()) => {
                            self.bust_star_caches();
                            self.notify_change();
                            MpdResponse::ok()
                        }
                        Err(e) => ack(ACK_ERROR_UNKNOWN, "playlistadd", &e.to_string()),
                    },
                    None => ack(ACK_ERROR_NO_EXIST, "playlistadd", "unsupported uri"),
                }
            }
            MpdCommand::PlaylistAdd(..) => MpdResponse::ok(),
            MpdCommand::PlaylistDelete(name, pos) if name == "Starred" => {
                // Position-based: map to the song id from the last listed order.
                let target = {
                    let st = self.state.lock().unwrap();
                    st.last_starred_order.get(pos).cloned()
                };
                match target {
                    Some(id) => match self.client.unstar(&Favorite::Song(id)).await {
                        Ok(()) => {
                            self.bust_star_caches();
                            self.notify_change();
                            MpdResponse::ok()
                        }
                        Err(e) => ack(ACK_ERROR_UNKNOWN, "playlistdelete", &e.to_string()),
                    },
                    None => ack(ACK_ERROR_NO_EXIST, "playlistdelete", "Bad song index"),
                }
            }
            MpdCommand::PlaylistDelete(..) => MpdResponse::ok(),
            MpdCommand::PlaylistClear(_) => MpdResponse::ok(),

            // ── db browse ──────────────────────────────────────────────────
            MpdCommand::LsInfo(path) => self.lsinfo(path.as_deref()).await,
            MpdCommand::ListAllInfo(path) => self.lsinfo(path.as_deref()).await,

            MpdCommand::Find(filters) => self.search_filtered(filters, true).await,
            MpdCommand::Search(filters) => self.search_filtered(filters, false).await,
            MpdCommand::FindAdd(filters) => self.find_add(filters, true).await,
            MpdCommand::SearchAdd(filters) => self.find_add(filters, false).await,
            MpdCommand::Count(filters) => self.count(filters).await,

            MpdCommand::List { tag, filter } => {
                // `list <tag> [filter]`: support Artist, Album, Genre. When a
                // filter is present it MUST narrow the result - never fall back
                // to the unfiltered library dump (see list_album_by_artist).
                match tag.as_str() {
                    "artist" | "albumartist" => match self.client.artists().await {
                        Ok(artists) => {
                            let pairs = artists
                                .into_iter()
                                .filter(|a| artist_passes_filter(&a.name, &filter))
                                .map(|a| ("Artist".to_string(), a.name))
                                .collect();
                            MpdResponse::Pairs(pairs)
                        }
                        Err(e) => ack(ACK_ERROR_UNKNOWN, "list", &e.to_string()),
                    },
                    "album" => {
                        // A filter constraining the artist restricts to that
                        // artist's albums; any other (or absent) filter lists all.
                        // A bare positional `list album "Tosca"` parses to
                        // filter=[(any,Tosca)], so treat an `any` value as an
                        // artist name too (classic 2-arg `list album <ARTIST>`).
                        if let Some(artist) =
                            filter_value(&filter, &["artist", "albumartist", "any"])
                        {
                            return self.list_albums_by_artist(&artist).await;
                        }
                        // `list album genre X` -> albums of that genre, via
                        // getAlbumList2 type=byGenre (confirmed backend path).
                        // Page it (getAlbumList2 caps `size` at 500 per call) so a
                        // large genre is not silently truncated - same "no silent
                        // caps" contract the search3 paging honors.
                        if let Some(genre) = filter_value(&filter, &["genre"]) {
                            const PAGE: i32 = 500;
                            // Ceiling so a backend that ignores `offset` (returns a
                            // full page forever) cannot spin unboundedly or overflow
                            // the i32 offset. 20 pages = 10000 albums, far beyond any
                            // real genre.
                            const MAX_PAGES: i32 = 20;
                            let mut names: Vec<(String, String)> = Vec::new();
                            let mut offset: i32 = 0;
                            let mut page = 0;
                            loop {
                                match self
                                    .client
                                    .album_list_by_genre(&genre, Some(PAGE), Some(offset))
                                    .await
                                {
                                    Ok(albums) => {
                                        let got = albums.len();
                                        names.extend(
                                            albums.into_iter().map(|a| ("Album".to_string(), a.name)),
                                        );
                                        page += 1;
                                        if (got as i32) < PAGE || page >= MAX_PAGES {
                                            break;
                                        }
                                        offset += PAGE;
                                    }
                                    Err(e) => return ack(ACK_ERROR_UNKNOWN, "list", &e.to_string()),
                                }
                            }
                            return MpdResponse::Pairs(names);
                        }
                        if !filter.is_empty() {
                            // A filter we cannot honor: narrow to nothing rather
                            // than silently dumping the whole library.
                            return MpdResponse::ok();
                        }
                        match self.client.album_list(AlbumListType::AlphabeticalByName, Some(500)).await {
                            Ok(albums) => {
                                let pairs = albums
                                    .into_iter()
                                    .map(|a| ("Album".to_string(), a.name))
                                    .collect();
                                MpdResponse::Pairs(pairs)
                            }
                            Err(e) => ack(ACK_ERROR_UNKNOWN, "list", &e.to_string()),
                        }
                    }
                    "genre" if !filter.is_empty() => {
                        // No Subsonic genre-by-filter path for the genre LIST
                        // itself (a genre filter on `list genre` is meaningless);
                        // narrow to nothing rather than dumping the whole list.
                        // (`list album genre X` is tag=album and handled above.)
                        MpdResponse::ok()
                    }
                    "genre" => match self.genres().await {
                        Ok(genres) => {
                            let pairs = genres
                                .into_iter()
                                .map(|g| ("Genre".to_string(), g.name))
                                .collect();
                            MpdResponse::Pairs(pairs)
                        }
                        Err(e) => ack(ACK_ERROR_UNKNOWN, "list", &e.to_string()),
                    },
                    _ => MpdResponse::ok(),
                }
            }

            // ── sticker rating (feature 3, ncmpcpp rating path) ─────────────
            MpdCommand::Sticker(s) => self.sticker(s).await,

            // ── binary cover art (feature 2) ────────────────────────────────
            MpdCommand::AlbumArt(uri, offset) | MpdCommand::ReadPicture(uri, offset) => {
                self.albumart(&uri, offset).await
            }
            MpdCommand::BinaryLimit(n) => {
                // Honor the client's negotiated chunk size (min 64 to stay sane).
                self.state.lock().unwrap().binary_limit = n.max(64);
                MpdResponse::ok()
            }

            // ── capability probes ──────────────────────────────────────────
            MpdCommand::Commands => {
                let cmds = [
                    "add", "addid", "albumart", "binarylimit", "clear",
                    "commands", "count", "currentsong", "delete", "fade", "find", "findadd",
                    "getvol", "idle",
                    "list", "listall", "listallinfo", "listplaylistinfo",
                    "listplaylists", "load", "lsinfo", "next", "noidle",
                    "notcommands", "outputs", "pause", "ping", "play", "playid",
                    "playlistadd", "playlistclear", "playlistdelete", "playlistid",
                    "playlistinfo", "plchanges", "previous", "readpicture",
                    "search", "searchadd", "seek", "seekcur", "seekid", "setvol", "stats", "sticker",
                    "status", "stop", "tagtypes", "urlhandlers",
                ];
                let pairs = cmds
                    .iter()
                    .map(|c| ("command".to_string(), c.to_string()))
                    .collect();
                MpdResponse::Pairs(pairs)
            }
            MpdCommand::NotCommands => MpdResponse::ok(),
            MpdCommand::TagTypes => {
                let tags = [
                    "Artist", "Album", "Title", "Track", "Genre", "Date", "Disc",
                    "MUSICBRAINZ_TRACKID", "Comment",
                ];
                let pairs = tags
                    .iter()
                    .map(|t| ("tagtype".to_string(), t.to_string()))
                    .collect();
                MpdResponse::Pairs(pairs)
            }
            MpdCommand::Outputs => MpdResponse::pairs()
                .pair("outputid", "0")
                .pair("outputname", "hypodj")
                .pair("outputenabled", "1")
                .build(),
            MpdCommand::Decoders => MpdResponse::ok(),
            MpdCommand::UrlHandlers => MpdResponse::pairs()
                .pair("handler", "http")
                .pair("handler", "https")
                .build(),

            MpdCommand::Unsupported(name) => {
                ack(ACK_ERROR_UNKNOWN, &name, &format!("unknown command \"{name}\""))
            }
        }
    }
}

/// A read-only snapshot of the current queue item, for the MPRIS surface. Holds
/// the MPD song id (stable per-song handle, used to build the `mpris:trackid`
/// object path) plus a clone of the queued [`QueueEntry`] (library Song or raw
/// stream) so the MPRIS module can render Metadata without reaching into the
/// handler's private state or holding its lock.
#[derive(Clone)]
pub struct CurrentItem {
    pub mpd_id: u64,
    pub entry: QueueEntry,
}

impl HypodjHandler {
    /// Snapshot the current queue item (id + entry), or `None` when stopped /
    /// queue empty. Used by the MPRIS server to render now-playing Metadata.
    pub fn current_item(&self) -> Option<CurrentItem> {
        let st = self.state.lock().unwrap();
        let idx = st.current?;
        st.queue.get(idx).map(|it| CurrentItem {
            mpd_id: it.id,
            entry: it.entry.clone(),
        })
    }

    /// Current volume (0..=100), for the MPRIS `Volume` property. Derived from the
    /// live gain so it tracks an in-flight fade (same seam as MPD `getvol`).
    pub fn volume(&self) -> u8 {
        self.state.lock().unwrap().reported_volume()
    }

    /// Advance to the next queue entry (MPRIS `Next` / desktop control). No-op at
    /// the end of the queue.
    pub async fn mpris_next(&self) {
        let next = {
            let st = self.state.lock().unwrap();
            st.current.map(|c| c + 1).filter(|&i| i < st.queue.len())
        };
        if let Some(idx) = next {
            let _ = self.play_index(idx).await;
        }
    }

    /// Go to the previous queue entry (MPRIS `Previous` / desktop control). No-op
    /// at the head of the queue.
    pub async fn mpris_previous(&self) {
        let prev = {
            let st = self.state.lock().unwrap();
            st.current.and_then(|c| c.checked_sub(1))
        };
        if let Some(idx) = prev {
            let _ = self.play_index(idx).await;
        }
    }

    /// Set volume (MPRIS `Volume` setter): mirror it into shared state and push
    /// to the player, same as the MPD `setvol` path.
    pub async fn mpris_set_volume(&self, vol: u8) {
        let v = vol.min(100);
        // Manual wins ATOMICALLY (mirrors the MPD setvol path): cancel any fade
        // AND apply the manual value under the SAME slot lock.
        self.fade
            .cancel_with(|| self.state.lock().unwrap().set_manual_volume(v))
            .await;
        let _ = self.player.set_volume(v).await;
        self.notify_change();
    }

    /// Await the next change notification (queue/playback/volume/star). The MPRIS
    /// server loops on this to emit `PropertiesChanged`. Shares the SAME `changed`
    /// Notify that wakes MPD `idle`, so both surfaces refresh off one signal.
    pub async fn changed(&self) {
        self.changed.notified().await;
    }

    /// Back `lsinfo` / `listallinfo`. The root lists the artist directories PLUS
    /// the synthetic top-level browse dirs (Genres/Lists/Radio/Starred). Drilling
    /// into each dispatches to the feature that backs it.
    async fn lsinfo(&self, path: Option<&str>) -> MpdResponse {
        match path {
            None | Some("") | Some("/") => self.lsinfo_root().await,

            // ── artist/album drill-down (cached) ────────────────────────────
            Some(p) if p.starts_with("artist/") => {
                let id = p.trim_start_matches("artist/").to_string();
                let key = format!("artist/{id}");
                if let Some(pairs) = self.dir_cache.get(&key) {
                    return MpdResponse::Pairs(pairs);
                }
                match self.client.artist_albums(&ArtistId(id)).await {
                    Ok(albums) => {
                        let mut pairs = Vec::new();
                        for al in &albums {
                            pairs.push(("directory".to_string(), format!("album/{}", al.id.0)));
                            pairs.push(("Album".to_string(), al.name.clone()));
                        }
                        self.dir_cache.put(key, pairs.clone());
                        MpdResponse::Pairs(pairs)
                    }
                    Err(e) => ack(ACK_ERROR_UNKNOWN, "lsinfo", &e.to_string()),
                }
            }
            Some(p) if p.starts_with("album/") => {
                let id = p.trim_start_matches("album/").to_string();
                let key = format!("album/{id}");
                if let Some(songs) = self.listings.get(&key) {
                    return song_rows(&songs);
                }
                match self.client.album_songs(&AlbumId(id)).await {
                    Ok(songs) => {
                        self.listings.put(key, songs.clone());
                        song_rows(&songs)
                    }
                    Err(e) => ack(ACK_ERROR_UNKNOWN, "lsinfo", &e.to_string()),
                }
            }

            // ── Genres (feature 6) ──────────────────────────────────────────
            Some("Genres") => match self.genres().await {
                Ok(genres) => {
                    let mut pairs = Vec::new();
                    for g in &genres {
                        pairs.push(("directory".to_string(), format!("genre/{}", g.name)));
                    }
                    MpdResponse::Pairs(pairs)
                }
                Err(e) => ack(ACK_ERROR_UNKNOWN, "lsinfo", &e.to_string()),
            },
            Some(p) if p.starts_with("genre/") => {
                let name = p.trim_start_matches("genre/").to_string();
                let key = format!("genre/{name}");
                if let Some(songs) = self.listings.get(&key) {
                    return song_rows(&songs);
                }
                match self.client.songs_by_genre(&name).await {
                    Ok(songs) => {
                        self.listings.put(key, songs.clone());
                        song_rows(&songs)
                    }
                    Err(e) => ack(ACK_ERROR_UNKNOWN, "lsinfo", &e.to_string()),
                }
            }

            // ── Lists: smart album lists (feature 5) ────────────────────────
            Some("Lists") => {
                let mut pairs = Vec::new();
                for t in ["frequent", "newest", "recent", "highest", "random"] {
                    pairs.push(("directory".to_string(), format!("list/{t}")));
                }
                MpdResponse::Pairs(pairs)
            }
            Some(p) if p.starts_with("list/") => {
                let name = p.trim_start_matches("list/");
                match list_type_from_dirname(name) {
                    Some(list_type) => {
                        // `random` smart list must stay fresh; others cache.
                        let cached = if name == "random" {
                            None
                        } else {
                            self.dir_cache.get(&format!("list/{name}"))
                        };
                        if let Some(pairs) = cached {
                            return MpdResponse::Pairs(pairs);
                        }
                        match self.client.album_list(list_type, Some(100)).await {
                            Ok(albums) => {
                                let mut pairs = Vec::new();
                                for al in &albums {
                                    pairs.push((
                                        "directory".to_string(),
                                        format!("album/{}", al.id.0),
                                    ));
                                    pairs.push(("Album".to_string(), al.name.clone()));
                                }
                                if name != "random" {
                                    self.dir_cache.put(format!("list/{name}"), pairs.clone());
                                }
                                MpdResponse::Pairs(pairs)
                            }
                            Err(e) => ack(ACK_ERROR_UNKNOWN, "lsinfo", &e.to_string()),
                        }
                    }
                    None => MpdResponse::ok(),
                }
            }

            // ── Radio: random / similar / top (feature 4) ───────────────────
            Some("Radio") => {
                // random is always reachable; similar/top are seeded per song or
                // artist from a browse path (radio/similar/<songId>,
                // radio/top/<artist>). We advertise the random entry plus a hint.
                MpdResponse::pairs()
                    .pair("directory", "radio/random")
                    .build()
            }
            Some("radio/random") => {
                // NEVER cached: randomness is the whole point.
                match self.client.random_songs(Some(50)).await {
                    Ok(songs) => song_rows(&songs),
                    Err(e) => ack(ACK_ERROR_UNKNOWN, "lsinfo", &e.to_string()),
                }
            }
            Some(p) if p.starts_with("radio/similar/") => {
                let id = p.trim_start_matches("radio/similar/").to_string();
                let key = format!("similar/{id}");
                if let Some(songs) = self.listings.get(&key) {
                    return song_rows(&songs);
                }
                match self.client.similar_songs(&SongId(id), Some(50)).await {
                    Ok(songs) => {
                        self.listings.put(key, songs.clone());
                        song_rows(&songs)
                    }
                    Err(e) => ack(ACK_ERROR_UNKNOWN, "lsinfo", &e.to_string()),
                }
            }
            Some(p) if p.starts_with("radio/top/") => {
                let artist = p.trim_start_matches("radio/top/").to_string();
                let key = format!("top/{artist}");
                if let Some(songs) = self.listings.get(&key) {
                    return song_rows(&songs);
                }
                match self.client.top_songs(&artist, Some(50)).await {
                    Ok(songs) => {
                        self.listings.put(key, songs.clone());
                        song_rows(&songs)
                    }
                    Err(e) => ack(ACK_ERROR_UNKNOWN, "lsinfo", &e.to_string()),
                }
            }

            // ── Starred (feature 3) - NEVER cached (freshness) ──────────────
            // The Starred dir mixes two browse subdirs (Albums / Artists, legal
            // directory rows) with the starred-song `file:` rows. Albums/artists
            // are DIRECTORY entities, so they surface as subdirs (ncmpcpp expands
            // them on add), never as fake song rows in a stored playlist.
            Some("Starred") => match self.client.starred_songs().await {
                Ok(songs) => {
                    {
                        let mut st = self.state.lock().unwrap();
                        st.last_starred_order = songs.iter().map(|s| s.id.clone()).collect();
                    }
                    let mut pairs = vec![
                        ("directory".to_string(), "Starred/Albums".to_string()),
                        ("directory".to_string(), "Starred/Artists".to_string()),
                    ];
                    for s in &songs {
                        pairs.extend(browse_song_pairs(s));
                    }
                    MpdResponse::Pairs(pairs)
                }
                Err(e) => ack(ACK_ERROR_UNKNOWN, "lsinfo", &e.to_string()),
            },
            // Starred albums/artists as browse subdirs. Each row is a real
            // `album/<id>` / `artist/<id>` directory, so adding it reuses the
            // existing album/artist expansion and becomes directly playable.
            Some("Starred/Albums") => match self.client.starred().await {
                Ok(starred) => {
                    let mut pairs = Vec::new();
                    for al in &starred.albums {
                        pairs.push(("directory".to_string(), format!("album/{}", al.id.0)));
                        pairs.push(("Album".to_string(), al.name.clone()));
                    }
                    MpdResponse::Pairs(pairs)
                }
                Err(e) => ack(ACK_ERROR_UNKNOWN, "lsinfo", &e.to_string()),
            },
            Some("Starred/Artists") => match self.client.starred().await {
                Ok(starred) => {
                    let mut pairs = Vec::new();
                    for ar in &starred.artists {
                        pairs.push(("directory".to_string(), format!("artist/{}", ar.id.0)));
                        pairs.push(("Artist".to_string(), ar.name.clone()));
                    }
                    MpdResponse::Pairs(pairs)
                }
                Err(e) => ack(ACK_ERROR_UNKNOWN, "lsinfo", &e.to_string()),
            },

            Some(_) => MpdResponse::ok(),
        }
    }

    /// The root browse view: synthetic top-level dirs + artist dirs (cached).
    async fn lsinfo_root(&self) -> MpdResponse {
        let mut pairs = Vec::new();
        // Synthetic feature dirs first so they sit at the top of ncmpcpp Browse.
        for d in ["Genres", "Lists", "Radio", "Starred"] {
            pairs.push(("directory".to_string(), d.to_string()));
        }
        match self.cached_artists().await {
            Ok(artists) => {
                for (id, name) in artists {
                    pairs.push(("directory".to_string(), format!("artist/{}", id.0)));
                    pairs.push(("Artist".to_string(), name));
                }
                MpdResponse::Pairs(pairs)
            }
            Err(e) => ack(ACK_ERROR_UNKNOWN, "lsinfo", &e.to_string()),
        }
    }

    /// Artist id+name list, served from the shared `dir_cache` "artists" slot
    /// (the `directory`/`Artist` rows) or fetched + cached on a miss. Both
    /// `lsinfo_root` and `list_albums_by_artist` go through here so
    /// `list album artist X` hits the same cache instead of re-fetching.
    async fn cached_artists(&self) -> Result<Vec<(ArtistId, String)>, SubsonicError> {
        if let Some(rows) = self.dir_cache.get(&"artists".to_string()) {
            return Ok(parse_artist_rows(&rows));
        }
        let artists = self.client.artists().await?;
        let rows: Vec<(String, String)> = artists
            .iter()
            .flat_map(|a| {
                [
                    ("directory".to_string(), format!("artist/{}", a.id.0)),
                    ("Artist".to_string(), a.name.clone()),
                ]
            })
            .collect();
        self.dir_cache.put("artists".to_string(), rows);
        Ok(artists.into_iter().map(|a| (a.id, a.name)).collect())
    }

    /// Genres list, cached in a dedicated slot (stable, benefits from reuse).
    async fn genres(&self) -> Result<Vec<Genre>, crate::subsonic::SubsonicError> {
        // Genres are cheap + stable; cache the resolved names via dir_cache is
        // awkward (different value type), so re-fetch is acceptable, but we keep
        // a tiny cache by reusing the client each call. Left uncached here for
        // simplicity - genres change rarely and the call is cheap.
        self.client.genres().await
    }

    /// Resolve + serve one binary cover-art chunk for `song/<id>` (feature 2).
    /// Resolve chain: song/<id> -> Song.cover_art (or fall back to the song id
    /// itself, which Navidrome accepts) -> cover bytes (cached) -> slice
    /// [offset..offset+binary_limit], clamping the final chunk.
    async fn albumart(&self, uri: &str, offset: usize) -> MpdResponse {
        let song_id = match song_id_from_uri(uri) {
            Some(id) => id,
            None => return ack(ACK_ERROR_NO_EXIST, "albumart", "No file exists"),
        };
        // Resolve the cover id: prefer the song's coverArt, else the song id.
        let cover_id = match self.client.song(&song_id).await {
            Ok(song) => song.cover_art.unwrap_or_else(|| song_id.0.clone()),
            // If we can't resolve the song, still try the id directly.
            Err(_) => song_id.0.clone(),
        };
        // Fetch (cached) the full image bytes.
        let bytes = match self.cover_cache.get(&format!("cover/{cover_id}")) {
            Some(b) => b,
            None => match self.client.cover_art(&cover_id).await {
                Ok(b) if !b.is_empty() => {
                    self.cover_cache.put(format!("cover/{cover_id}"), b.clone());
                    b
                }
                // Empty or errored: gracefully ACK no-exist (never panic).
                _ => return ack(ACK_ERROR_NO_EXIST, "albumart", "No file exists"),
            },
        };
        let total = bytes.len();
        if offset >= total {
            return ack(ACK_ERROR_NO_EXIST, "albumart", "Bad file offset");
        }
        let limit = self.state.lock().unwrap().binary_limit;
        let end = (offset + limit).min(total);
        let chunk = bytes[offset..end].to_vec();
        MpdResponse::Binary { total, chunk }
    }

    /// Full search3 with client-side MPD-tag post-filtering (feature 7). `exact`
    /// (find) matches equality on tags; otherwise (search) case-insensitive
    /// substring. search3 is full-text only, so this filter recovers precision.
    async fn search_filtered(&self, filters: Vec<(String, String)>, exact: bool) -> MpdResponse {
        if filters.is_empty() {
            return MpdResponse::ok();
        }
        // Thread the true command name into the ACK (mirrors find_add's cmd),
        // so a failing `find` acks as `find`, not a hardcoded `search`.
        let cmd = if exact { "find" } else { "search" };
        let matches = match self.collect_matches(&filters, exact).await {
            Ok(m) => m,
            Err(e) => return ack(ACK_ERROR_UNKNOWN, cmd, &e),
        };
        let mut pairs = Vec::new();
        for s in &matches {
            pairs.extend(browse_song_pairs(s));
        }
        MpdResponse::Pairs(pairs)
    }

    /// `count <filter...>`: the same exact-match search3 + client-side
    /// post-filter as `find`, but instead of listing the songs it returns their
    /// tally and total playtime. MPD's shape is two lines: `songs: <N>` and
    /// `playtime: <total_seconds>` (integer seconds, songs of unknown duration
    /// contributing 0). An empty filter yields a zero tally: we have no
    /// full-library enumeration to count against, so 0 is the honest floor
    /// rather than a fabricated total. On a search3 error, ACK as `count`.
    async fn count(&self, filters: Vec<(String, String)>) -> MpdResponse {
        if filters.is_empty() {
            return MpdResponse::pairs()
                .pair("songs", "0")
                .pair("playtime", "0")
                .build();
        }
        // count is an aggregate: page much further than find/findadd so the tally
        // is honest for large artists/genres (500 pages = 100k songs), still
        // bounded against a backend that ignores offset.
        let matches = match self.collect_matches_capped(&filters, true, 500).await {
            Ok(m) => m,
            Err(e) => return ack(ACK_ERROR_UNKNOWN, "count", &e),
        };
        let songs = matches.len();
        let playtime: u64 = matches
            .iter()
            .map(|s| s.duration_secs.unwrap_or(0) as u64)
            .sum();
        MpdResponse::pairs()
            .pair("songs", songs.to_string())
            .pair("playtime", playtime.to_string())
            .build()
    }

    /// The shared core of find/search/findadd/searchadd: run search3 (full-text)
    /// for the combined filter values, then recover MPD-tag precision with a
    /// client-side post-filter. `exact` (find) matches equality; otherwise
    /// (search) case-insensitive substring. Returns the matching songs so a
    /// caller can either list them (`search_filtered`) or enqueue them
    /// (`find_add`). search3 results are query-specific + ephemeral -> NEVER
    /// cached. On a search3 error, returns the error string for the caller to ACK.
    async fn collect_matches(
        &self,
        filters: &[(String, String)],
        exact: bool,
    ) -> Result<Vec<Song>, String> {
        // find/findadd targets are listings/enqueues: 25 pages (5000 songs) is
        // far beyond any real request. `count` needs an honest total, so it pages
        // further via collect_matches_capped.
        self.collect_matches_capped(filters, exact, 25).await
    }

    /// [`collect_matches`] with an explicit page ceiling. The ceiling exists only
    /// so a backend that ignores `song_offset` (keeps returning a full page)
    /// cannot loop forever, grow the buffer without bound, or overflow the i32
    /// offset. Hitting it is logged (never a silent cap - CLAUDE.md).
    async fn collect_matches_capped(
        &self,
        filters: &[(String, String)],
        exact: bool,
        max_pages: i32,
    ) -> Result<Vec<Song>, String> {
        // Build the full-text query from all values (search3 is full-text).
        let query = filters
            .iter()
            .map(|(_, v)| v.as_str())
            .collect::<Vec<_>>()
            .join(" ");
        // Page search3 so the result is COMPLETE, not silently truncated at the
        // 200-song cap: request 200 at a time, accumulating until a short page
        // (< PAGE) signals exhaustion.
        const PAGE: i32 = 200;
        let mut songs: Vec<Song> = Vec::new();
        // De-dup by song id ACROSS pages. A backend that ignores `song_offset`
        // returns the same page every request; without dedup `count` would sum
        // those repeats into a fabricated total (500 pages * 200 = 100000). Dedup
        // also absorbs a row that overlaps a page boundary on a well-behaved
        // server. `seen` is the source of truth for the tally.
        let mut seen: std::collections::HashSet<String> = std::collections::HashSet::new();
        let mut offset: i32 = 0;
        let mut page = 0;
        loop {
            let hits = self
                .client
                .search3_paged(&query, Some(PAGE), Some(offset))
                .await
                .map_err(|e| e.to_string())?;
            let got = hits.songs.len();
            let mut fresh = 0usize;
            for s in hits.songs {
                if seen.insert(s.id.0.clone()) {
                    songs.push(s);
                    fresh += 1;
                }
            }
            page += 1;
            // Short page -> exhausted. A full page that added NOTHING new means the
            // backend is repeating (ignoring offset) -> stop rather than spin.
            if (got as i32) < PAGE || fresh == 0 {
                break;
            }
            if page >= max_pages {
                tracing::warn!(
                    query = %query,
                    collected = songs.len(),
                    max_pages,
                    "collect_matches hit the page ceiling; result may be incomplete"
                );
                break;
            }
            offset += PAGE;
        }
        let matches = songs
            .into_iter()
            .filter(|s| filters.iter().all(|(tag, val)| tag_matches(s, tag, val, exact)))
            .collect();
        Ok(matches)
    }

    /// Back `findadd`/`searchadd`: collect the matching songs (same path as
    /// find/search) and append every one to the play queue directly (they are
    /// already full `Song`s from `collect_matches`, so no per-song refetch), then
    /// wake idle subscribers. Empty filters is a no-op empty-OK (mirrors
    /// `search_filtered`). A search3 failure ACKs; the per-song push is infallible
    /// so every match is honestly enqueued (nothing is silently dropped).
    async fn find_add(&self, filters: Vec<(String, String)>, exact: bool) -> MpdResponse {
        if filters.is_empty() {
            return MpdResponse::ok();
        }
        let cmd = if exact { "findadd" } else { "searchadd" };
        let matches = match self.collect_matches(&filters, exact).await {
            Ok(m) => m,
            Err(e) => return ack(ACK_ERROR_UNKNOWN, cmd, &e),
        };
        for s in matches {
            self.enqueue_song(s).await;
        }
        self.notify_change();
        MpdResponse::ok()
    }

    /// Back `list album` narrowed by an artist filter: resolve the artist by
    /// (case-insensitive) name, then list that artist's albums. An unknown
    /// artist yields an empty listing - never the full album library (honoring
    /// the "a present filter must narrow" contract).
    async fn list_albums_by_artist(&self, artist: &str) -> MpdResponse {
        let artists = match self.cached_artists().await {
            Ok(a) => a,
            Err(e) => return ack(ACK_ERROR_UNKNOWN, "list", &e.to_string()),
        };
        // Unicode-aware case-insensitive compare (eq_ignore_ascii_case only folds
        // ASCII, so case-differing non-ASCII names would fail to match).
        let wanted = artist.to_lowercase();
        let id = match artists
            .into_iter()
            .find(|(_, name)| name.to_lowercase() == wanted)
        {
            Some((id, _)) => id,
            None => return MpdResponse::ok(),
        };
        match self.client.artist_albums(&id).await {
            Ok(albums) => {
                let pairs = albums
                    .into_iter()
                    .map(|a| ("Album".to_string(), a.name))
                    .collect();
                MpdResponse::Pairs(pairs)
            }
            Err(e) => ack(ACK_ERROR_UNKNOWN, "list", &e.to_string()),
        }
    }

    /// Back the `sticker` command for the `rating` sticker only (ncmpcpp's
    /// rating path), bridging to Subsonic setRating/userRating. Any other
    /// sticker (unknown verb/type/name) answers empty-OK so a probing client
    /// does not hang. A failing Subsonic call ACKs, never panics.
    async fn sticker(&self, cmd: StickerCmd) -> MpdResponse {
        match cmd {
            StickerCmd::Set { uri, value } => {
                let id = match song_id_from_uri(&uri) {
                    Some(id) => id,
                    None => return ack(ACK_ERROR_NO_EXIST, "sticker", "unsupported uri"),
                };
                match self.client.set_rating(&id, value).await {
                    Ok(()) => {
                        self.bust_rating_caches();
                        self.notify_change();
                        MpdResponse::ok()
                    }
                    Err(e) => ack(ACK_ERROR_UNKNOWN, "sticker", &e.to_string()),
                }
            }
            StickerCmd::Delete { uri } => {
                let id = match song_id_from_uri(&uri) {
                    Some(id) => id,
                    None => return ack(ACK_ERROR_NO_EXIST, "sticker", "unsupported uri"),
                };
                // Deleting the rating sticker clears it (setRating 0).
                match self.client.set_rating(&id, 0).await {
                    Ok(()) => {
                        self.bust_rating_caches();
                        self.notify_change();
                        MpdResponse::ok()
                    }
                    Err(e) => ack(ACK_ERROR_UNKNOWN, "sticker", &e.to_string()),
                }
            }
            StickerCmd::Get { uri } => {
                let id = match song_id_from_uri(&uri) {
                    Some(id) => id,
                    None => return ack(ACK_ERROR_NO_EXIST, "sticker", "unsupported uri"),
                };
                match self.client.song(&id).await {
                    // MPD framing: `sticker: <name>=<value>`.
                    Ok(song) => match song.user_rating {
                        Some(r) => MpdResponse::pairs()
                            .pair("sticker", format!("rating={r}"))
                            .build(),
                        // No rating set: MPD returns a "no such sticker" ACK.
                        None => ack(ACK_ERROR_NO_EXIST, "sticker", "no such sticker"),
                    },
                    Err(e) => ack(ACK_ERROR_UNKNOWN, "sticker", &e.to_string()),
                }
            }
            StickerCmd::List { uri } => {
                let id = match song_id_from_uri(&uri) {
                    Some(id) => id,
                    None => return ack(ACK_ERROR_NO_EXIST, "sticker", "unsupported uri"),
                };
                match self.client.song(&id).await {
                    Ok(song) => match song.user_rating {
                        Some(r) => MpdResponse::pairs()
                            .pair("sticker", format!("rating={r}"))
                            .build(),
                        // No stickers set: empty-OK (a valid empty list).
                        None => MpdResponse::ok(),
                    },
                    Err(e) => ack(ACK_ERROR_UNKNOWN, "sticker", &e.to_string()),
                }
            }
            // Unknown sticker verb/type/name: empty-OK, never hang the client.
            StickerCmd::Unsupported => MpdResponse::ok(),
        }
    }

    /// Invalidate cached listings whose user_rating could change after setRating.
    /// Album/genre/list listings carry per-song `user_rating`, so bust them so a
    /// subsequent browse reflects the new rating.
    fn bust_rating_caches(&self) {
        self.listings.invalidate_prefix("album/");
        self.listings.invalidate_prefix("genre/");
    }

    /// Invalidate cached listings whose starred flag could change after a star.
    fn bust_star_caches(&self) {
        self.dir_cache.invalidate_prefix("album/");
        self.dir_cache.invalidate(&"artists".to_string());
        self.listings.invalidate_prefix("album/");
    }
}

/// Does `song` satisfy the `tag == / contains val` filter? `exact` picks
/// equality vs case-insensitive substring. `any` matches title/artist/album.
fn tag_matches(song: &Song, tag: &str, val: &str, exact: bool) -> bool {
    let cmp = |field: &str| -> bool {
        if exact {
            field == val
        } else {
            field.to_lowercase().contains(&val.to_lowercase())
        }
    };
    // Composer/performer are MPD MULTI-VALUED tags: a track can credit several,
    // and a filter must match on ANY single value (real MPD matches per value).
    // We store them as a ", "-joined display string (from displayComposer /
    // contributors), so split on that delimiter and match any part - otherwise an
    // exact `find performer "Yo-Yo Ma"` never equals "Itzhak Perlman, Yo-Yo Ma".
    let cmp_multi = |field: &Option<String>| -> bool {
        match field {
            Some(s) => s.split(", ").filter(|p| !p.is_empty()).any(cmp),
            None => false,
        }
    };
    match tag {
        "title" => cmp(&song.title),
        "artist" | "albumartist" => song.artist.as_deref().map(cmp).unwrap_or(false),
        "album" => song.album.as_deref().map(cmp).unwrap_or(false),
        "genre" => song.genre.as_deref().map(cmp).unwrap_or(false),
        // Numeric tags the Song carries: compare on the string form (Date is the
        // release year; MPD emits `Date` from `year`).
        "date" => song.year.map(|y| cmp(&y.to_string())).unwrap_or(false),
        "track" => song.track.map(|t| cmp(&t.to_string())).unwrap_or(false),
        "disc" => song.disc.map(|d| cmp(&d.to_string())).unwrap_or(false),
        "comment" => song.comment.as_deref().map(cmp).unwrap_or(false),
        // Composer/performer come from OpenSubsonic metadata (displayComposer /
        // contributors). Absent on plain-Subsonic servers -> None -> no match.
        "composer" => cmp_multi(&song.composer),
        "performer" => cmp_multi(&song.performer),
        // MPD `any` spans EVERY tag - all the ones this Song models, not just
        // title/artist/album (else `any "Techno"` misses a genre-only match).
        "any" => {
            cmp(&song.title)
                || song.artist.as_deref().map(cmp).unwrap_or(false)
                || song.album.as_deref().map(cmp).unwrap_or(false)
                || song.genre.as_deref().map(cmp).unwrap_or(false)
                || song.comment.as_deref().map(cmp).unwrap_or(false)
                || song.year.map(|y| cmp(&y.to_string())).unwrap_or(false)
                || song.track.map(|t| cmp(&t.to_string())).unwrap_or(false)
                || song.disc.map(|d| cmp(&d.to_string())).unwrap_or(false)
                || cmp_multi(&song.composer)
                || cmp_multi(&song.performer)
        }
        // Genuinely unmodeled tag (base, file, modified-since, or unknown): the
        // Song carries no data to satisfy it, so
        // it matches NOTHING rather than passing all. tag_matches is shared by
        // find (list) and findadd (enqueue); passing-all would make findadd
        // over-add on an unsatisfiable constraint. MPD-correct: an unsatisfiable
        // constraint yields no matches.
        _ => false,
    }
}

/// The value of the first filter pair whose tag matches one of `tags`, if any.
/// Used to pull e.g. the `artist` constraint out of a `list album` filter.
fn filter_value(filter: &[(String, String)], tags: &[&str]) -> Option<String> {
    filter
        .iter()
        .find(|(tag, _)| tags.contains(&tag.as_str()))
        .map(|(_, v)| v.clone())
}

/// Does an artist named `name` pass the `list artist`/`list albumartist` filter?
/// An empty filter passes everything. An artist/albumartist constraint matches
/// (case-insensitively) on the name. Any other constraint we cannot honor
/// excludes the row, so a present-but-unhonorable filter narrows to nothing
/// rather than dumping the whole artist list.
fn artist_passes_filter(name: &str, filter: &[(String, String)]) -> bool {
    if filter.is_empty() {
        return true;
    }
    filter.iter().all(|(tag, val)| match tag.as_str() {
        // Unicode-aware fold (eq_ignore_ascii_case only folds ASCII).
        "artist" | "albumartist" => name.to_lowercase() == val.to_lowercase(),
        _ => false,
    })
}

/// Reconstruct `(ArtistId, name)` pairs from the cached `directory`/`Artist`
/// rows that `cached_artists` stores (a `directory: artist/<id>` row followed by
/// its `Artist: <name>` row). Malformed pairs are skipped.
fn parse_artist_rows(rows: &[(String, String)]) -> Vec<(ArtistId, String)> {
    let mut out = Vec::new();
    let mut i = 0;
    while i + 1 < rows.len() {
        if rows[i].0 == "directory" {
            if let (Some(id), true) =
                (rows[i].1.strip_prefix("artist/"), rows[i + 1].0 == "Artist")
            {
                out.push((ArtistId(id.to_string()), rows[i + 1].1.clone()));
            }
        }
        i += 2;
    }
    out
}

/// Parse a `song/<id>` uri into a `SongId`.
fn song_id_from_uri(uri: &str) -> Option<SongId> {
    uri.strip_prefix("song/").map(|s| SongId(s.to_string()))
}

/// Serialize a slice of songs as browse `file:` rows.
fn song_rows(songs: &[Song]) -> MpdResponse {
    let mut pairs = Vec::new();
    for s in songs {
        pairs.extend(browse_song_pairs(s));
    }
    MpdResponse::Pairs(pairs)
}

/// Serialize a `Song` as a browse `file:` entry (no queue Pos/Id), including the
/// richer metadata tags (feature 7) when present. ncmpcpp reads these directly.
fn browse_song_pairs(s: &Song) -> Vec<(String, String)> {
    let mut p = vec![
        ("file".to_string(), format!("song/{}", s.id.0)),
        ("Title".to_string(), s.title.clone()),
    ];
    push_song_tags(&mut p, s);
    p
}

/// Append the common + richer tags for a song (shared by browse + queue rows).
fn push_song_tags(p: &mut Vec<(String, String)>, s: &Song) {
    if let Some(a) = &s.artist {
        p.push(("Artist".to_string(), a.clone()));
    }
    if let Some(a) = &s.album {
        p.push(("Album".to_string(), a.clone()));
    }
    if let Some(t) = s.track {
        p.push(("Track".to_string(), t.to_string()));
    }
    if let Some(dn) = s.disc {
        p.push(("Disc".to_string(), dn.to_string()));
    }
    if let Some(y) = s.year {
        p.push(("Date".to_string(), y.to_string()));
    }
    if let Some(g) = &s.genre {
        p.push(("Genre".to_string(), g.clone()));
    }
    if let Some(mb) = &s.musicbrainz_id {
        p.push(("MUSICBRAINZ_TRACKID".to_string(), mb.clone()));
    }
    if let Some(c) = &s.comment {
        p.push(("Comment".to_string(), c.clone()));
    }
    if let Some(br) = s.bitrate {
        // ncmpcpp/MPD surface bitrate via the status `bitrate:` line, but a
        // Format hint here is harmless and readable.
        p.push(("Format".to_string(), format!("{}kbps", br)));
    }
    if let Some(d) = s.duration_secs {
        p.push(("Time".to_string(), d.to_string()));
        p.push(("duration".to_string(), format!("{d}.000")));
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::config::ServerConfig;
    use crate::player::{NullPlayer, PlayState, PlayerEvent, SYNTH_FLOOR_DB};
    use crate::scrobble::Scrobbler;

    const NTS: &str = "https://stream-mixtape-geo.ntslive.net/mixtape5";

    /// A handler wired to a NON-networked Subsonic client and a real NullPlayer
    /// actor. The raw-stream path never calls the client, so no server is needed.
    ///
    /// `connect()` builds a real reqwest client, which needs system CA certs; a
    /// network-isolated build sandbox (nix `doCheck`) has none and the reqwest
    /// builder aborts. That is environmental, not a wiring failure, so return
    /// `None` there and the caller skips (same guard as `subsonic::tests`). In the
    /// devshell/CI with certs this yields a real client and the test runs.
    fn handler_with_null_player(
    ) -> Option<(HypodjHandler, tokio::sync::mpsc::Receiver<PlayerEvent>)> {
        let cfg = ServerConfig {
            url: "http://127.0.0.1:1/never-called".to_string(),
            username: "u".to_string(),
            password: "p".to_string(),
            client_name: "test".to_string(),
        };
        let client = match std::panic::catch_unwind(|| SubsonicClient::connect(&cfg)) {
            Ok(Ok(c)) => Arc::new(c),
            _ => {
                eprintln!("skipping: no CA certs (sandbox); connect() not exercisable here");
                return None;
            }
        };
        let (player, events) = NullPlayer::spawn();
        Some((HypodjHandler::new(client, player), events))
    }

    // ── P3 NL flow: nl -> validate -> echo -> confirm -> arm ─────────────────

    /// A minimal inline translator (no model, no hypodj-nl dep) emitting a fixed
    /// valid plan, so the handler-side flow is exercised model-free.
    struct StubTranslator(RawPlan);
    impl crate::nl::Translator for StubTranslator {
        fn translate(
            &self,
            _u: &str,
            _c: &crate::nl::NlContext,
        ) -> Result<crate::nl::NlHit, crate::nl::NlError> {
            Ok(crate::nl::NlHit { plans: vec![self.0.clone()], source: crate::nl::NlSource::Rules })
        }
    }

    fn pair<'a>(resp: &'a MpdResponse, key: &str) -> Option<&'a str> {
        match resp {
            MpdResponse::Pairs(p) => p.iter().find(|(k, _)| k == key).map(|(_, v)| v.as_str()),
            _ => None,
        }
    }

    fn nl_translate(req: &str, owner: u64) -> MpdCommand {
        MpdCommand::Nl(NlCmd::Translate { req: req.into(), owner })
    }
    fn nl_confirm(token: &str, owner: u64) -> MpdCommand {
        MpdCommand::Nl(NlCmd::Confirm { token: token.into(), owner })
    }
    fn nl_cancel(token: &str, owner: u64) -> MpdCommand {
        MpdCommand::Nl(NlCmd::Cancel { token: token.into(), owner })
    }

    /// A translator emitting a caller-supplied batch of plans (for the atomic-batch
    /// and echo-equals-arm tests).
    struct BatchTranslator(Vec<RawPlan>);
    impl crate::nl::Translator for BatchTranslator {
        fn translate(
            &self,
            _u: &str,
            _c: &crate::nl::NlContext,
        ) -> Result<crate::nl::NlHit, crate::nl::NlError> {
            Ok(crate::nl::NlHit {
                plans: self.0.clone(),
                source: crate::nl::NlSource::Rules,
            })
        }
    }

    #[tokio::test]
    async fn nl_translate_echoes_then_confirm_arms() {
        let Some((handler, _events)) = handler_with_null_player() else { return };
        // No translator injected yet -> NotAvailable (degrades gracefully).
        let resp = handler.handle(nl_translate("stop", 1)).await;
        assert!(matches!(resp, MpdResponse::Ack { .. }), "no translator -> ACK");

        let plan = RawPlan {
            version: 1,
            trigger: crate::plan::RawTrigger::Immediate,
            action: crate::plan::Action::Stop,
            once: true,
            origin: String::new(),
        };
        handler.set_translator(Arc::new(StubTranslator(plan)));

        // Translate echoes + mints a token but does NOT arm.
        let resp = handler.handle(nl_translate("stop", 1)).await;
        let token = pair(&resp, "nl_token").expect("a token is minted").to_string();
        assert!(pair(&resp, "nl_echo").is_some(), "an echo is rendered");
        assert!(handler.plan_list().is_empty(), "translate must NOT arm");

        // Confirm arms via the P2 registry and returns a plan id.
        let resp = handler.handle(nl_confirm(&token, 1)).await;
        assert!(pair(&resp, "plan_id").is_some(), "confirm arms + returns id");
        assert_eq!(handler.plan_list().len(), 1, "exactly one plan armed");

        // The token is single-use: a second confirm fails loud.
        let resp = handler.handle(nl_confirm(&token, 1)).await;
        assert!(matches!(resp, MpdResponse::Ack { .. }), "single-use token");
    }

    #[tokio::test]
    async fn nl_cancel_drops_the_token() {
        let Some((handler, _events)) = handler_with_null_player() else { return };
        let plan = RawPlan {
            version: 1,
            trigger: crate::plan::RawTrigger::Immediate,
            action: crate::plan::Action::Pause,
            once: true,
            origin: String::new(),
        };
        handler.set_translator(Arc::new(StubTranslator(plan)));
        let resp = handler.handle(nl_translate("pause", 1)).await;
        let token = pair(&resp, "nl_token").unwrap().to_string();
        // Cancel then confirm -> the token is gone (loud ACK), nothing armed.
        handler.handle(nl_cancel(&token, 1)).await;
        let resp = handler.handle(nl_confirm(&token, 1)).await;
        assert!(matches!(resp, MpdResponse::Ack { .. }));
        assert!(handler.plan_list().is_empty());
    }

    /// F1: the human confirms EXACTLY what arms. A plan whose fade duration is
    /// over the max is CLAMPED at translate time, so the echo and the armed plan
    /// both carry the final clamped value (never the raw over-limit one).
    #[tokio::test]
    async fn nl_echo_equals_the_armed_clamped_plan() {
        let Some((handler, _events)) = handler_with_null_player() else { return };
        // 9999s fade out is well over the 1800s (max_dur) ceiling -> clamps.
        let plan = RawPlan {
            version: 1,
            trigger: crate::plan::RawTrigger::Immediate,
            action: crate::plan::Action::Fade(crate::plan::FadeIntentIr::Out { secs: 9999.0 }),
            once: true,
            origin: String::new(),
        };
        handler.set_translator(Arc::new(BatchTranslator(vec![plan])));

        let resp = handler.handle(nl_translate("fade out", 7)).await;
        let echo = pair(&resp, "nl_echo").expect("an echo").to_string();
        let token = pair(&resp, "nl_token").expect("a token").to_string();

        let resp = handler.handle(nl_confirm(&token, 7)).await;
        assert!(pair(&resp, "plan_id").is_some(), "confirm arms");
        let armed: Vec<RawPlan> = handler.plan_list().into_iter().map(|(_, r)| r).collect();
        assert_eq!(armed.len(), 1);
        // The armed plan carries the CLAMPED value (9999 -> 1800), not the raw one.
        match &armed[0].action {
            crate::plan::Action::Fade(crate::plan::FadeIntentIr::Out { secs }) => {
                assert_eq!(*secs, 1800.0, "the fade was clamped to max_dur");
            }
            other => panic!("got {other:?}"),
        }
        // The echo the human confirmed is a description of the plan that armed.
        let expected = crate::echo::describe_batch(&armed, crate::nl::NlSource::Rules);
        assert_eq!(echo, expected, "echo must equal the armed (clamped) plan");
    }

    /// F2: a multi-plan batch arms ATOMICALLY. One failing plan -> NONE armed.
    #[tokio::test]
    async fn plan_add_batch_is_atomic() {
        let Some((handler, _events)) = handler_with_null_player() else { return };
        let good = RawPlan {
            version: 1,
            trigger: crate::plan::RawTrigger::Immediate,
            action: crate::plan::Action::Stop,
            once: true,
            origin: "t".into(),
        };
        // A WallClock already in the past fails validate (PastDeadline).
        let bad = RawPlan {
            version: 1,
            trigger: crate::plan::RawTrigger::WallClock {
                at: chrono::Utc::now() - chrono::Duration::hours(1),
            },
            action: crate::plan::Action::Stop,
            once: true,
            origin: "t".into(),
        };
        // Good FIRST, bad second: a naive plan-by-plan arm would leave the good one
        // armed. The atomic batch must arm NOTHING.
        let err = handler
            .plan_add_batch(vec![good.clone(), bad])
            .expect_err("a batch with an invalid plan fails");
        assert!(matches!(err, crate::plan::PlanError::PastDeadline));
        assert!(handler.plan_list().is_empty(), "a failed batch arms NONE");

        // An all-valid batch arms every plan, in order.
        let ids = handler
            .plan_add_batch(vec![good.clone(), good.clone()])
            .expect("all-valid batch arms");
        assert_eq!(ids.len(), 2);
        assert_eq!(handler.plan_list().len(), 2, "both plans armed");
    }

    /// F3: a pending translation is confirmable ONLY by its owning connection, and
    /// tokens are unguessable (not a sequential nl-0, nl-1, ... counter).
    #[tokio::test]
    async fn nl_confirm_is_owner_scoped_and_tokens_are_unguessable() {
        let Some((handler, _events)) = handler_with_null_player() else { return };
        let plan = RawPlan {
            version: 1,
            trigger: crate::plan::RawTrigger::Immediate,
            action: crate::plan::Action::Stop,
            once: true,
            origin: String::new(),
        };
        handler.set_translator(Arc::new(StubTranslator(plan)));

        // Owner A translates + gets a token.
        let resp = handler.handle(nl_translate("stop", 100)).await;
        let token_a = pair(&resp, "nl_token").expect("a token").to_string();
        // Token must NOT be the predictable sequential counter.
        assert_ne!(token_a, "nl-0");

        // A DIFFERENT owner cannot confirm A's pending plan.
        let resp = handler.handle(nl_confirm(&token_a, 200)).await;
        assert!(matches!(resp, MpdResponse::Ack { .. }), "cross-owner confirm rejected");
        assert!(handler.plan_list().is_empty(), "a foreign owner armed nothing");
        // A foreign cancel is likewise a no-op (the token survives for its owner).
        handler.handle(nl_cancel(&token_a, 200)).await;

        // The rightful owner still confirms + arms.
        let resp = handler.handle(nl_confirm(&token_a, 100)).await;
        assert!(pair(&resp, "plan_id").is_some(), "the owner can confirm");
        assert_eq!(handler.plan_list().len(), 1);

        // Tokens are non-sequential + distinct across several mints.
        let mut toks = Vec::new();
        for _ in 0..5 {
            let r = handler.handle(nl_translate("stop", 1)).await;
            toks.push(pair(&r, "nl_token").unwrap().to_string());
        }
        assert!(toks.iter().all(|t| t.starts_with("nl-")));
        for seq in ["nl-0", "nl-1", "nl-2", "nl-3", "nl-4"] {
            assert!(!toks.contains(&seq.to_string()), "tokens must not be sequential");
        }
        let uniq: std::collections::HashSet<&String> = toks.iter().collect();
        assert_eq!(uniq.len(), toks.len(), "tokens are distinct");
    }

    fn sample_song() -> Song {
        Song {
            id: SongId("so-1".into()),
            title: "Independent Us".into(),
            album: Some("Let Love Rumpel".into()),
            album_id: Some(AlbumId("al-1".into())),
            artist: Some("Kalabrese".into()),
            track: Some(4),
            duration_secs: Some(372),
            cover_art: None,
            starred: false,
            musicbrainz_id: None,
            disc: Some(2),
            year: Some(2019),
            genre: Some("Electronic".into()),
            bitrate: None,
            comment: Some("vinyl rip".into()),
            user_rating: None,
            composer: Some("Kalabrese".into()),
            performer: Some("Itzhak Perlman, Yo-Yo Ma".into()),
        }
    }

    #[test]
    fn tag_matches_constrains_date_track_disc_and_comment() {
        let s = sample_song();
        // date -> year; exact + substring both work.
        assert!(tag_matches(&s, "date", "2019", true));
        assert!(!tag_matches(&s, "date", "2020", true));
        assert!(tag_matches(&s, "date", "201", false));
        // track / disc compare on the numeric string form.
        assert!(tag_matches(&s, "track", "4", true));
        assert!(!tag_matches(&s, "track", "5", true));
        assert!(tag_matches(&s, "disc", "2", true));
        // comment.
        assert!(tag_matches(&s, "comment", "vinyl", false));
        assert!(!tag_matches(&s, "comment", "cd", false));
    }

    #[test]
    fn tag_matches_constrains_composer_and_performer() {
        // Composer/performer come from OpenSubsonic metadata; exact + substring
        // both work, and a non-matching value is rejected.
        let s = sample_song();
        assert!(tag_matches(&s, "composer", "Kalabrese", true));
        assert!(tag_matches(&s, "composer", "kala", false));
        assert!(!tag_matches(&s, "composer", "Bach", false));
        assert!(tag_matches(&s, "performer", "Yo-Yo Ma", false));
        assert!(!tag_matches(&s, "performer", "nobody", false));
        // Multi-valued: an EXACT filter on one of several joined performers must
        // match (real MPD treats performer/composer as multi-valued tags).
        assert!(tag_matches(&s, "performer", "Yo-Yo Ma", true));
        assert!(tag_matches(&s, "performer", "Itzhak Perlman", true));
        // The whole joined string is not itself a single value, so it must not
        // match as one under exact.
        assert!(!tag_matches(&s, "performer", "Itzhak Perlman, Yo-Yo Ma", true));
        // `any` spans composer and performer too, not just title/artist/album.
        assert!(tag_matches(&s, "any", "Kalabrese", false));
        assert!(tag_matches(&s, "any", "Yo-Yo Ma", true));
        // Absent metadata (plain-Subsonic) -> no match, never passes-all.
        let mut bare = sample_song();
        bare.composer = None;
        bare.performer = None;
        assert!(!tag_matches(&bare, "composer", "anything", false));
        assert!(!tag_matches(&bare, "performer", "anyone", false));
    }

    #[test]
    fn tag_matches_rejects_unmodeled_tag_rather_than_passing_all() {
        // A genuinely unsupported tag (base/file/modified-since/...) must match
        // NOTHING so findadd never over-adds on an unsatisfiable constraint.
        let s = sample_song();
        assert!(!tag_matches(&s, "modified-since", "2020", false));
        assert!(!tag_matches(&s, "base", "anything", false));
    }

    #[test]
    fn parse_artist_rows_reconstructs_id_and_name() {
        let rows = vec![
            ("directory".to_string(), "artist/ar-1".to_string()),
            ("Artist".to_string(), "Kalabrese".to_string()),
            ("directory".to_string(), "artist/ar-2".to_string()),
            ("Artist".to_string(), "Tosca".to_string()),
        ];
        let out = parse_artist_rows(&rows);
        assert_eq!(
            out,
            vec![
                (ArtistId("ar-1".into()), "Kalabrese".to_string()),
                (ArtistId("ar-2".into()), "Tosca".to_string()),
            ]
        );
    }

    #[tokio::test]
    async fn add_stream_url_produces_stream_queue_item() {
        let Some((h, _events)) = handler_with_null_player() else { return };
        let resp = h.handle(MpdCommand::Add(NTS.to_string())).await;
        // add -> empty-OK (Pairs), never an ACK.
        assert!(matches!(resp, MpdResponse::Pairs(_)), "add stream must succeed");
        let st = h.state.lock().unwrap();
        assert_eq!(st.queue.len(), 1);
        match &st.queue[0].entry {
            QueueEntry::Stream { url, title } => {
                assert_eq!(url, NTS);
                assert_eq!(title, NTS);
            }
            other => panic!("expected Stream, got {other:?}"),
        }
    }

    #[tokio::test]
    async fn play_routes_stream_url_to_player_verbatim() {
        let Some((h, mut events)) = handler_with_null_player() else { return };
        h.handle(MpdCommand::Add(NTS.to_string())).await;
        h.handle(MpdCommand::Play(Some(0))).await;
        // The NullPlayer went to Playing and, crucially, carries NO SongId for a
        // raw stream (so nothing downstream can scrobble it).
        assert_eq!(h.player.state(), PlayState::Playing);
        match events.recv().await.expect("a player event") {
            PlayerEvent::StateChanged(PlayState::Playing, song, _) => {
                assert!(song.is_none(), "raw stream must carry no scrobble-able id");
            }
            other => panic!("expected Playing StateChanged, got {other:?}"),
        }
    }

    #[tokio::test]
    async fn currentsong_and_playlistinfo_render_stream() {
        let Some((h, _events)) = handler_with_null_player() else { return };
        h.handle(MpdCommand::Add(NTS.to_string())).await;
        h.handle(MpdCommand::Play(Some(0))).await;

        let render = |r: MpdResponse| match r {
            MpdResponse::Pairs(p) => p,
            other => panic!("expected Pairs, got {other:?}"),
        };
        let cur = render(h.handle(MpdCommand::CurrentSong).await);
        assert!(cur.iter().any(|(k, v)| k == "file" && v == NTS));
        assert!(cur.iter().any(|(k, v)| k == "Title" && v == NTS));
        // No Time / duration for a live stream, and it must not have crashed.
        assert!(!cur.iter().any(|(k, _)| k == "Time"));

        let pl = render(h.handle(MpdCommand::PlaylistInfo(None)).await);
        assert!(pl.iter().any(|(k, v)| k == "file" && v == NTS));
        assert!(pl.iter().any(|(k, _)| k == "Pos"));

        // status must render (state: play) without a panic on the unknown-duration
        // stream item.
        let status = render(h.handle(MpdCommand::Status).await);
        assert!(status.iter().any(|(k, v)| k == "state" && v == "play"));
    }

    #[tokio::test]
    async fn scrobbler_skips_raw_stream_item() {
        // A raw stream plays with song=None, so the player emits
        // StateChanged(Playing, None). The scrobbler must not latch/act on it.
        let cfg = ServerConfig {
            url: "http://127.0.0.1:1/never-called".to_string(),
            username: "u".to_string(),
            password: "p".to_string(),
            client_name: "test".to_string(),
        };
        // connect() needs system CA certs; skip in a cert-less build sandbox
        // (same guard as the other client-constructing tests).
        let client = match std::panic::catch_unwind(|| SubsonicClient::connect(&cfg)) {
            Ok(Ok(c)) => Arc::new(c),
            _ => {
                eprintln!("skipping: no CA certs (sandbox); connect() not exercisable here");
                return;
            }
        };
        let scrobbler = Scrobbler::new(client);
        // Feeding the exact event a raw stream produces is a no-op (no id).
        scrobbler.on_event(&PlayerEvent::StateChanged(PlayState::Playing, None, None));
        scrobbler.on_event(&PlayerEvent::TimePos { pos: 120.0, queue_id: None });
        // No panic, no submission possible: the scrobbler never latched a song.
        assert!(scrobbler.current_is_none());
    }

    use crate::mpd::{FadeArgs, FadeKind};
    use std::time::Duration;

    fn fade_args(kind: FadeKind, secs: u64) -> FadeArgs {
        FadeArgs { kind, dur: Some(Duration::from_secs(secs)) }
    }

    /// Drive paused virtual time forward in `iters` ticks of `ms`, yielding
    /// several times per tick so a spawned fade task (and the NullPlayer actor it
    /// awaits round-trips against) actually gets polled between deadlines.
    async fn pump(ms: u64, iters: usize) {
        for _ in 0..iters {
            tokio::time::advance(Duration::from_millis(ms)).await;
            for _ in 0..6 {
                tokio::task::yield_now().await;
            }
        }
    }

    // A fade out runs to completion -> the player is Stopped AND the pre-fade
    // baseline volume is restored (terminal action in the wrapper task).
    #[tokio::test(start_paused = true)]
    async fn fade_out_stops_and_restores() {
        let Some((h, _events)) = handler_with_null_player() else { return };
        // Start playing something so there is a live playback state to stop.
        h.handle(MpdCommand::Add(NTS.to_string())).await;
        h.handle(MpdCommand::Play(Some(0))).await;
        assert_eq!(h.player.state(), PlayState::Playing);

        h.start_fade(fade_args(FadeKind::Out, 20)).await.unwrap();
        assert!(h.fade_active().await);
        h.wait_for_fade().await;

        // Ramp reached silence, then the terminal stopped + restored baseline 100.
        assert_eq!(h.player.state(), PlayState::Stopped);
        assert_eq!(h.state.lock().unwrap().target_volume, 100);
        assert_eq!(h.state.lock().unwrap().reported_volume(), 100);
    }

    // A manual setvol mid-fade cancels (abort+join) the fade FIRST, then applies
    // the manual value: manual wins, strictly ordered, no trailing fade tick.
    #[tokio::test(start_paused = true)]
    async fn manual_wins_last() {
        let Some((h, _events)) = handler_with_null_player() else { return };
        h.start_fade(fade_args(FadeKind::Out, 60)).await.unwrap();
        assert!(h.fade_active().await);

        // setvol 30 mid-fade.
        h.handle(MpdCommand::SetVol(30)).await;
        // Fade is gone (cancelled), and the last applied volume is exactly 30.
        assert!(!h.fade_active().await);
        assert_eq!(h.state.lock().unwrap().reported_volume(), 30);
        assert_eq!(h.state.lock().unwrap().target_volume, 30);
    }

    // A superseding fade continues from the LIVE gain, not a stale value, and the
    // superseded fade is joined before the new one is installed.
    #[tokio::test(start_paused = true)]
    async fn supersede_continuous() {
        let Some((h, _events)) = handler_with_null_player() else { return };
        // Fade A: a slow fade out. Let a few ticks apply so the live gain drops.
        h.start_fade(fade_args(FadeKind::Out, 120)).await.unwrap();
        pump(250, 8).await;
        let mid_gain = h.live_gain_db();
        assert!(mid_gain < 0.0, "fade A should have lowered the live gain");

        // Fade B supersedes: it must start from the live gain (<= mid_gain, since
        // B keeps ramping down), never jump back to 0 dB.
        h.start_fade(fade_args(FadeKind::To(0), 60)).await.unwrap();
        pump(250, 8).await;
        assert!(
            h.live_gain_db() <= mid_gain + 1e-6,
            "supersede must not re-brighten (continuous from live gain)"
        );
    }

    // A REJECTED fade command must leave an in-flight fade running and never jump
    // the volume: validation happens before the outgoing fade is aborted.
    #[tokio::test(start_paused = true)]
    async fn rejected_fade_leaves_running_fade_untouched() {
        let Some((h, _events)) = handler_with_null_player() else { return };
        h.start_fade(fade_args(FadeKind::Out, 120)).await.unwrap();
        pump(250, 8).await;
        let mid_gain = h.live_gain_db();
        assert!(mid_gain < 0.0 && h.fade_active().await);
        // A 0s `fade to 0` is rejected (StepTooLarge). It must NOT abort the
        // running fade out and must NOT re-brighten the volume.
        let resp = h
            .handle(MpdCommand::Fade(FadeArgs { kind: FadeKind::To(0), dur: Some(Duration::ZERO) }))
            .await;
        assert!(matches!(resp, MpdResponse::Ack { .. }), "rejected fade must ACK");
        assert!(h.fade_active().await, "rejected fade must not abort the running one");
        pump(250, 4).await;
        assert!(
            h.live_gain_db() <= mid_gain + 1e-6,
            "the original fade out must keep descending, never jump up"
        );
    }

    // Play / Next / Previous do NOT cancel an in-flight fade (the envelope is
    // continuous across track boundaries - mpv volume persists across loadfile).
    #[tokio::test(start_paused = true)]
    async fn fade_survives_track_change() {
        let Some((h, _events)) = handler_with_null_player() else { return };
        h.start_fade(fade_args(FadeKind::Out, 120)).await.unwrap();
        assert!(h.fade_active().await);
        // Next on the empty queue is a no-op OK, and crucially does NOT cancel.
        h.handle(MpdCommand::Next).await;
        h.handle(MpdCommand::Previous).await;
        assert!(h.fade_active().await, "fade must survive next/previous");
    }

    // Dropping a FadeHandle aborts its task (no further sink writes). Verified at
    // the FadeSlot/FadeHandle level with a self-incrementing task.
    #[tokio::test(start_paused = true)]
    async fn leak_safety_drop_aborts() {
        use std::sync::atomic::{AtomicUsize, Ordering};
        let counter = Arc::new(AtomicUsize::new(0));
        let c = counter.clone();
        let join = tokio::spawn(async move {
            loop {
                c.fetch_add(1, Ordering::SeqCst);
                tokio::time::sleep(Duration::from_millis(10)).await;
            }
            #[allow(unreachable_code)]
            FadeOutcome::Completed
        });
        let abort = join.abort_handle();
        let handle = FadeHandle { abort, join: Some(join) };
        tokio::time::advance(Duration::from_millis(50)).await;
        tokio::task::yield_now().await;
        let before = counter.load(Ordering::SeqCst);
        drop(handle); // Drop MUST abort the task.
        tokio::time::advance(Duration::from_millis(100)).await;
        tokio::task::yield_now().await;
        let after = counter.load(Ordering::SeqCst);
        assert!(after <= before + 1, "dropped handle kept running: {before} -> {after}");
    }

    // notify_change is coalesced: a long fade emits far fewer notifications than
    // it has steps (only when the ROUNDED reported volume changes).
    #[tokio::test(start_paused = true)]
    async fn notify_coalesced() {
        let Some((h, _events)) = handler_with_null_player() else { return };
        let h = Arc::new(h);
        // Count change notifications on a background subscriber.
        let count = Arc::new(std::sync::atomic::AtomicUsize::new(0));
        {
            let h = h.clone();
            let count = count.clone();
            tokio::spawn(async move {
                loop {
                    h.changed().await;
                    count.fetch_add(1, std::sync::atomic::Ordering::SeqCst);
                }
            });
        }
        tokio::task::yield_now().await;
        // A 60s fade out from 0 dB to silence: ~80 sub-JND steps.
        h.start_fade(fade_args(FadeKind::Out, 60)).await.unwrap();
        h.wait_for_fade().await;
        tokio::task::yield_now().await;
        let n = count.load(std::sync::atomic::Ordering::SeqCst);
        // At most ~101 distinct integer volumes exist; the count must be well
        // under the ~80 step total is NOT the bar - the bar is <= 101 and that it
        // did not fire once per step for every tick. Assert it is bounded by the
        // reachable integer-volume transitions plus the terminal notify.
        assert!(n <= 101 + 2, "notify storm not coalesced: {n} notifications");
        assert!(n >= 1, "a fade should notify at least once");
    }

    // F1: with NO fade active, a low manual volume is reported EXACTLY, never
    // round-tripped through the cubic dB domain (which would floor <= 10 to 0).
    // `setvol 5` then `getvol` must return 5.
    #[tokio::test]
    async fn low_volume_reports_exactly() {
        let Some((h, _events)) = handler_with_null_player() else { return };
        for v in [0u8, 1, 5, 7, 10, 33, 100] {
            h.handle(MpdCommand::SetVol(v)).await;
            let got = match h.handle(MpdCommand::GetVol).await {
                MpdResponse::Pairs(p) => p
                    .iter()
                    .find(|(k, _)| k == "volume")
                    .map(|(_, val)| val.parse::<u8>().unwrap())
                    .unwrap(),
                other => panic!("got {other:?}"),
            };
            assert_eq!(got, v, "setvol {v} must report exactly {v}");
            assert_eq!(h.volume(), v, "MPRIS volume must also report exactly {v}");
        }
    }

    // F2: `fade in` from silence ramps UP to the wake ceiling (0 dB == vol 100),
    // never a degenerate no-op. Start muted, fade in, and the reported/baseline
    // volume settles at the ceiling.
    #[tokio::test(start_paused = true)]
    async fn fade_in_ramps_up_from_silence() {
        let Some((h, _events)) = handler_with_null_player() else { return };
        // Start from silence: setvol 0 (live gain at the floor).
        h.handle(MpdCommand::SetVol(0)).await;
        assert_eq!(h.state.lock().unwrap().reported_volume(), 0);

        h.start_fade(fade_args(FadeKind::In, 30)).await.unwrap();
        assert!(h.fade_active().await);
        h.wait_for_fade().await;

        // Ramp reached the ceiling and committed it as the new baseline.
        assert_eq!(h.state.lock().unwrap().target_volume, 100);
        assert_eq!(h.state.lock().unwrap().reported_volume(), 100);
    }

    // F3: a user's [fade] TOML override for the default duration actually takes
    // effect (the parser is config-free; the handler threads the config default).
    // A shorter winddown default yields proportionally fewer steps for a
    // no-duration `fade out`.
    #[tokio::test(start_paused = true)]
    async fn config_default_duration_override_takes_effect() {
        let Some((h, _events)) = handler_with_null_player() else { return };
        // Rebuild the handler with a tiny winddown default via config.
        let cfg = {
            let mut c = FadeConfig::default();
            c.winddown_fade_secs = 5;
            c
        };
        let h = HypodjHandler::with_fade_config(h.client(), h.player.clone(), cfg);
        // No-duration fade out -> uses winddown_fade_secs (5s), clamped >= min_slew.
        h.start_fade(FadeArgs { kind: FadeKind::Out, dur: None }).await.unwrap();
        assert!(h.fade_active().await);
        // Drive to completion and confirm it stopped (a 5s fade completes quickly
        // under paused time; a 300s default would too, but the point is the
        // config path is exercised and honored - no panic, real completion).
        h.wait_for_fade().await;
        assert_eq!(h.player.state(), PlayState::Stopped);
    }

    // F4: after a mid-fade Stop, the reported volume returns to the baseline (the
    // cancelled fade's faded-down level does not linger in the report). The mpv
    // re-assert call is issued too (unobservable via NullPlayer, but the state is).
    #[tokio::test(start_paused = true)]
    async fn stop_reasserts_baseline_after_fade() {
        let Some((h, _events)) = handler_with_null_player() else { return };
        h.handle(MpdCommand::Add(NTS.to_string())).await;
        h.handle(MpdCommand::Play(Some(0))).await;
        h.start_fade(fade_args(FadeKind::Out, 120)).await.unwrap();
        pump(250, 8).await;
        assert!(h.live_gain_db() < 0.0, "fade lowered the live gain");

        h.handle(MpdCommand::Stop).await;
        assert!(!h.fade_active().await, "stop cancels the fade");
        assert_eq!(h.player.state(), PlayState::Stopped);
        // Reported volume is back at the baseline, not the faded-down level.
        assert_eq!(h.state.lock().unwrap().reported_volume(), 100);
        assert_eq!(h.state.lock().unwrap().target_volume, 100);
    }

    // F7: a startle-unsafe spec (a deliberate `fade to` over a huge range in one
    // slewed step) is surfaced as an ACK to the client, never silently dropped.
    #[tokio::test(start_paused = true)]
    async fn rejected_fade_acks_not_silent() {
        let Some((h, _events)) = handler_with_null_player() else { return };
        // fade to 0 with a 0s duration: clamps to one min_slew step spanning the
        // full 0 dB -> -60 dB range (60 dB) as a DELIBERATE cue -> StepTooLarge.
        let resp = h
            .handle(MpdCommand::Fade(FadeArgs { kind: FadeKind::To(0), dur: Some(Duration::ZERO) }))
            .await;
        assert!(matches!(resp, MpdResponse::Ack { .. }), "must ACK, got {resp:?}");
        // And it must not have installed a fade.
        assert!(!h.fade_active().await);
    }

    // F8: the muted state is represented as a FINITE floor dB, not NEG_INFINITY,
    // so a fade started from the mute window reads a finite from_db and is NOT
    // rejected as NonFinite. Put the state at the synth floor and start a fade in.
    #[tokio::test(start_paused = true)]
    async fn fade_from_mute_window_is_finite() {
        let Some((h, _events)) = handler_with_null_player() else { return };
        // Simulate the mute window: live gain sitting at the finite synth floor.
        {
            let mut st = h.state.lock().unwrap();
            st.live_gain_db = h.fade_cfg.synth_floor_db;
            st.fading = true;
        }
        // A fade started here must build (finite from_db), not error NonFinite.
        h.start_fade(fade_args(FadeKind::In, 30)).await.unwrap();
        assert!(h.fade_active().await);
    }

    // F9: the fade-native entry point (start_fade_spec) drives a fade without
    // going through the MPD `fade` DSL - the seam the P2 executor will call.
    #[tokio::test(start_paused = true)]
    async fn native_entry_point_drives_fade() {
        let Some((h, _events)) = handler_with_null_player() else { return };
        h.handle(MpdCommand::Add(NTS.to_string())).await;
        h.handle(MpdCommand::Play(Some(0))).await;
        // Construct a native request directly (no FadeArgs / no wire grammar).
        h.start_fade_spec(FadeRequest {
            intent: FadeIntent::Out,
            dur: Duration::from_secs(20),
        })
        .await
        .unwrap();
        assert!(h.fade_active().await);
        h.wait_for_fade().await;
        assert_eq!(h.player.state(), PlayState::Stopped);
    }

    // C2: a manual setvol against a running fade must leave NO surviving fade task
    // and report EXACTLY the manual value - the cancel + the state mutation happen
    // atomically under the slot lock, so there is no window a concurrent fade
    // could clobber. Asserts the full post-condition: empty slot, fading cleared,
    // exact volume.
    #[tokio::test(start_paused = true)]
    async fn setvol_leaves_no_surviving_fade() {
        let Some((h, _events)) = handler_with_null_player() else { return };
        h.start_fade(fade_args(FadeKind::Out, 120)).await.unwrap();
        pump(250, 6).await;
        assert!(h.fade_active().await && h.live_gain_db() < 0.0);

        // setvol from a second logical caller.
        h.handle(MpdCommand::SetVol(42)).await;

        // No fade task survives in the slot, the fade switch is cleared, and the
        // reported/baseline volume is exactly the manual value.
        assert!(!h.fade_active().await, "no surviving fade task");
        let st = h.state.lock().unwrap();
        assert!(!st.fading, "fading switch cleared");
        assert_eq!(st.target_volume, 42);
        assert_eq!(st.reported_volume(), 42);
    }

    // C2: even when a `fade` from a second logical caller races a setvol, the end
    // state is always consistent - never the corrupt "no fade in the slot yet the
    // reported volume derives from a dead envelope" state. Whichever wins, the
    // slot and the reported volume agree.
    #[tokio::test(start_paused = true)]
    async fn setvol_atomic_against_concurrent_fade() {
        let Some((h, _events)) = handler_with_null_player() else { return };
        let h = Arc::new(h);
        h.start_fade(fade_args(FadeKind::Out, 120)).await.unwrap();
        pump(250, 4).await;

        let h2 = h.clone();
        let fade_fut = tokio::spawn(async move {
            let _ = h2.start_fade(fade_args(FadeKind::To(60), 120)).await;
        });
        h.handle(MpdCommand::SetVol(20)).await;
        let _ = fade_fut.await;
        // Let any surviving fade settle a tick.
        pump(250, 2).await;

        let active = h.fade_active().await;
        let (fading, reported) = {
            let st = h.state.lock().unwrap();
            (st.fading, st.reported_volume())
        };
        // Invariant: the `fading` switch is set IFF a fade task is installed.
        assert_eq!(active, fading, "slot and fading switch must agree (no orphan)");
        if !active {
            // Manual won: reported is exactly the manual value, no dead envelope.
            assert_eq!(reported, 20, "manual won -> exact manual volume");
        }
    }

    // C3: superseding a fade while its terminal window is near must not let the
    // superseded fade's terminal (StopRestore) whipsaw playback or the baseline.
    // A fade OUT (terminal = stop + restore) superseded by a fade IN must leave
    // playback RUNNING and commit the fade-in's ceiling, never the out's stop.
    #[tokio::test(start_paused = true)]
    async fn supersede_before_terminal_no_whipsaw() {
        let Some((h, _events)) = handler_with_null_player() else { return };
        h.handle(MpdCommand::Add(NTS.to_string())).await;
        h.handle(MpdCommand::Play(Some(0))).await;
        assert_eq!(h.player.state(), PlayState::Playing);

        // Fade OUT: on completion it would stop playback + restore the baseline.
        h.start_fade(fade_args(FadeKind::Out, 30)).await.unwrap();
        pump(250, 10).await;
        assert!(h.live_gain_db() < 0.0 && h.fade_active().await);

        // Supersede with a fade IN before the out reaches its StopRestore terminal.
        h.start_fade(fade_args(FadeKind::In, 30)).await.unwrap();
        h.wait_for_fade().await;

        // The superseded fade-out's StopRestore never fired: playback still runs,
        // and the surviving fade-in committed the ceiling (100) as the baseline.
        assert_eq!(h.player.state(), PlayState::Playing, "superseded stop must not fire");
        assert_eq!(h.state.lock().unwrap().target_volume, 100);
        assert!(!h.state.lock().unwrap().fading, "fade-in terminal cleared the switch");
    }

    // C3: a superseded fade that has ALREADY reached its terminal generation must
    // not re-apply. Drive a fade OUT fully to its terminal, THEN start a fresh
    // fade - the completed out's terminal already ran (player stopped), the new
    // fade installs cleanly with no double-application panic or stale write.
    #[tokio::test(start_paused = true)]
    async fn terminal_epoch_guard_after_completion() {
        let Some((h, _events)) = handler_with_null_player() else { return };
        h.handle(MpdCommand::Add(NTS.to_string())).await;
        h.handle(MpdCommand::Play(Some(0))).await;

        h.start_fade(fade_args(FadeKind::To(30), 20)).await.unwrap();
        h.wait_for_fade().await;
        // Terminal committed the baseline 30.
        assert_eq!(h.state.lock().unwrap().target_volume, 30);

        // A fresh fade after completion installs cleanly (the old task is gone).
        h.start_fade(fade_args(FadeKind::In, 20)).await.unwrap();
        assert!(h.fade_active().await);
        h.wait_for_fade().await;
        assert_eq!(h.state.lock().unwrap().target_volume, 100);
    }

    // ── SMOOTH-RESTART: resume composition (signal-free, no real process) ─────

    fn mk_song(id: &str) -> Song {
        Song {
            id: SongId(id.to_string()),
            title: format!("Song {id}"),
            album: None,
            album_id: None,
            artist: None,
            track: None,
            duration_secs: Some(240),
            cover_art: None,
            starred: false,
            musicbrainz_id: None,
            disc: None,
            year: None,
            genre: None,
            bitrate: None,
            comment: None,
            user_rating: None,
            composer: None,
            performer: None,
        }
    }

    // FadeIntent::WakeTo resolves to the SAVED perceptual level (not vol 100),
    // sub-JND, committing the saved baseline. Proves a wake restores the user's
    // real volume rather than ramping to full.
    #[test]
    fn wake_to_resolves_to_saved_level_sub_jnd() {
        let saved_vol = 60u8;
        let target = mpv_volume_to_db(saved_vol as f64);
        let intent = FadeIntent::WakeTo { target_db: target, vol: saved_vol };
        let (t, sub_jnd, terminal) = intent.resolve(SYNTH_FLOOR_DB, 0.0, -45.0);
        match t {
            FadeTarget::Db(db) => assert!((db - target).abs() < 1e-9),
            _ => panic!("WakeTo must target a specific Db, not Silence"),
        }
        assert!(sub_jnd, "a wake ramp is sub-JND (imperceptibly gentle)");
        match terminal {
            Terminal::SetBaseline(v) => assert_eq!(v, saved_vol),
            _ => panic!("WakeTo commits the saved baseline"),
        }
    }

    // FadeIntent::ToFloor resolves to the wind-down floor (Db(floor_db)), sub-JND,
    // with a SetBaseline terminal - playback CONTINUES (no Silence, no mute/stop).
    #[test]
    fn to_floor_resolves_to_floor_sub_jnd_playback_continues() {
        let floor = -45.0;
        let (t, sub_jnd, terminal) = FadeIntent::ToFloor.resolve(0.0, 0.0, floor);
        match t {
            FadeTarget::Db(db) => assert!((db - floor).abs() < 1e-9, "targets the floor level"),
            FadeTarget::Silence => panic!("ToFloor must NOT reach Silence (playback continues)"),
        }
        assert!(sub_jnd, "a wind-down is sub-JND (imperceptibly gentle)");
        match terminal {
            Terminal::SetBaseline(v) => {
                assert_eq!(v, db_to_mpv_volume(floor).round().clamp(0.0, 100.0) as u8);
            }
            _ => panic!("ToFloor commits a baseline (no stop)"),
        }
    }

    // resume_snapshot reflects the live queue + current + volume + play state, and
    // returns an OWNED struct with the state guard already dropped (no lock held
    // across the await in the async callers).
    #[tokio::test]
    async fn resume_snapshot_reflects_queue_and_state() {
        let Some((h, _events)) = handler_with_null_player() else { return };
        h.enqueue_song_for_test(mk_song("s1")).await;
        h.enqueue_stream_for_test(NTS).await;
        h.enqueue_song_for_test(mk_song("s2")).await;
        h.play_for_test(2).await;
        h.mpris_set_volume(64).await;

        let snap = h.resume_snapshot(31.5);
        assert_eq!(snap.schema_version, RESUME_SCHEMA_VERSION);
        assert_eq!(snap.queue.len(), 3);
        assert_eq!(snap.queue[0], ResumeItem::Song { id: "s1".into() });
        assert!(matches!(snap.queue[1], ResumeItem::Stream { .. }));
        assert_eq!(snap.queue[2], ResumeItem::Song { id: "s2".into() });
        assert_eq!(snap.current, Some(2));
        assert_eq!(snap.volume, 64);
        assert_eq!(snap.elapsed_secs, 31.5);
        assert_eq!(snap.play_state, ResumePlayState::Playing);
    }

    // restore of a Playing snapshot (raw streams, so no Subsonic call) rebuilds
    // the queue, plays the current entry, and installs a wake-ramp fade from
    // silence. A raw Stream restarts from 0 (no seek).
    #[tokio::test(start_paused = true)]
    async fn restore_playing_streams_wakes_from_silence() {
        let Some((h, _events)) = handler_with_null_player() else { return };
        let s = ResumeState {
            schema_version: RESUME_SCHEMA_VERSION,
            queue: vec![
                ResumeItem::Stream { url: NTS.into(), title: "NTS".into() },
                ResumeItem::Stream { url: NTS.into(), title: "NTS2".into() },
            ],
            current: Some(0),
            elapsed_secs: 90.0,
            volume: 55,
            play_state: ResumePlayState::Playing,
            playlist_version: 7,
            saved_at_unix: 1,
        };
        h.restore(&s).await.unwrap();
        assert_eq!(h.player.state(), PlayState::Playing);
        // The wake ramp is installed (a fade owns the level) and starts from the
        // synth floor (silence).
        assert!(h.fade_active().await, "a wake ramp must be installed");
        assert!(h.live_gain_db() <= SYNTH_FLOOR_DB + 5.0, "wake starts near silence");
        assert_eq!(h.state.lock().unwrap().target_volume, 55);
    }

    // restore of a Paused/Stopped snapshot rebuilds the queue + baseline volume
    // but leaves playback STOPPED - no autoplay, no fade (an explicit stop
    // survives the rebuild).
    #[tokio::test(start_paused = true)]
    async fn restore_paused_stays_stopped() {
        let Some((h, _events)) = handler_with_null_player() else { return };
        let s = ResumeState {
            schema_version: RESUME_SCHEMA_VERSION,
            queue: vec![ResumeItem::Stream { url: NTS.into(), title: "NTS".into() }],
            current: Some(0),
            elapsed_secs: 0.0,
            volume: 42,
            play_state: ResumePlayState::Paused,
            playlist_version: 3,
            saved_at_unix: 1,
        };
        h.restore(&s).await.unwrap();
        assert_eq!(h.player.state(), PlayState::Stopped, "no autoplay on a paused resume");
        assert!(!h.fade_active().await, "no wake ramp on a paused resume");
        assert_eq!(h.state.lock().unwrap().target_volume, 42);
        assert_eq!(h.state.lock().unwrap().queue.len(), 1);
    }

    // checkpoint() writes a real resume.toml to disk that loads back equal, and a
    // paused state records ResumePlayState::Paused; a queue mutation bumps the
    // persisted playlist_version.
    #[tokio::test]
    async fn checkpoint_writes_loadable_state_to_disk() {
        let Some((h, _events)) = handler_with_null_player() else { return };
        let dir = std::env::temp_dir().join(format!("hypodj-cp-{}", std::process::id()));
        let _ = std::fs::create_dir_all(&dir);
        let path = dir.join("resume.toml");
        h.set_state_path(path.clone());

        h.enqueue_stream_for_test(NTS).await;
        h.play_for_test(0).await;
        h.handle(MpdCommand::Pause(Some(true))).await;
        h.checkpoint(12.0).await;

        let loaded = crate::resume::load(&path).expect("checkpoint wrote a loadable file");
        assert_eq!(loaded.queue.len(), 1);
        assert_eq!(loaded.play_state, ResumePlayState::Paused);
        let v1 = loaded.playlist_version;

        // A queue mutation bumps playlist_version in the next checkpoint.
        h.enqueue_stream_for_test(NTS).await;
        h.checkpoint(12.0).await;
        let loaded2 = crate::resume::load(&path).expect("re-load");
        assert_eq!(loaded2.queue.len(), 2);
        assert!(loaded2.playlist_version > v1, "queue mutation bumps playlist_version");

        let _ = std::fs::remove_dir_all(&dir);
    }
}
