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
use std::sync::atomic::{AtomicBool, AtomicU64, Ordering};
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
    min_deliberate_dur, run_fade, Curve, FadeError, FadeOutcome, FadeProgress, FadeSpec, FadeTarget,
    StartleBounds,
};
use crate::event::{Cursor, EntrySnapshot, QueueId, QueueSnapshot};
use crate::intelligence::{
    lexicon_pull, pull_reweight, FeatureStore, MetadataStore, Pull, PullField, TrackFeatures,
    LEXICON_PULL_STRENGTH,
};
use crate::model::{AlbumId, ArtistId, Favorite, Genre, Playlist, QueueEntry, Song, SongId, Station};
use crate::plan::{
    clamp_raw, validate, Action, ArmedPlan, FadeIntentIr, PlanBounds, PlanError, PlanId, RawPlan,
    RawTrigger, Resolved, Selector, ORIGIN_SLEEP, ORIGIN_WAKE, ORIGIN_WINDDOWN,
};
use crate::subsonic::SubsonicError;
use crate::echo::describe_batch;
use crate::nl::{NlContext, NlError, NlSource, Translator};
use crate::mpd::{
    ContinuationCmd, FadeArgs, FadeKind, FieldCmd, FieldNudge, KnobDir, MpdCommand, MpdHandler,
    MpdResponse, NlCmd, PlanCmd, SleepCmd, StickerCmd, WakeCmd, WakeWhen, WinddownCmd,
};
use crate::player::{
    db_to_mpv_volume, effective_play_state, mpv_volume_to_db, PlayState, PlayerError, PlayerHandle,
    StreamMeta,
};
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
    /// The committed PERCEPTUAL target in dB - the authoritative source for BOTH
    /// knob stepping and the resume baseline. The FadeSlot merely animates
    /// `live_gain_db` toward it; every volume commit path (setvol glide, knob
    /// detent, baseline terminal) writes it SYNCHRONOUSLY so a key-mash or a
    /// superseded glide always leaves the true intended level committed here, not
    /// the mid-flight live gain. Invariant: after any baseline commit it equals
    /// `mpv_volume_to_db(target_volume)`. Initialised to the dB of `target_volume`.
    logical_gain_db: f64,
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
    /// PENDING-PAUSE intent: `true` from the instant a pause is REQUESTED until it
    /// is superseded (a fresh play/next/prev/stop) or explicitly resumed. It is the
    /// reported-state override in [`Self::reported_play_state`]: while `true` the
    /// outward play state is Paused IMMEDIATELY, without waiting for the pause fade
    /// to reach silence and freeze mpv. This collapses the inconsistent window where
    /// mpv is still raw-Playing during the fade - status/MPRIS/checkpoint all report
    /// Paused at request time, so an ACK, a mid-fade checkpoint, and a Play-during-
    /// fade branch all see the true intent, not a stale Playing.
    pending_pause: bool,
    /// PENDING-SKIP intent: the TARGET index of an in-flight startle-safe user
    /// skip (Next/Previous while playing). Set the instant the skip is requested,
    /// mirroring [`Self::pending_pause`], so status/MPRIS/currentsong report the
    /// TARGET track IMMEDIATELY during the dip-to-silence WITHOUT mutating
    /// `current` yet (mpv still plays the OLD track through the dip). Committed to
    /// `current` and cleared in the [`Terminal::SkipLoad`] terminal when the target
    /// actually loads; cleared by any manual volume set / stop / end-of-queue / pause
    /// so a superseded skip never leaves the reported current pointing at a track
    /// that never loaded.
    pending_skip: Option<usize>,
    /// Does `logical_gain_db` currently reflect a COMMITTED baseline the knob can
    /// step from directly? `true` at rest and while a knob/glide fade animates (its
    /// baseline is committed synchronously at install, so N rapid presses = N
    /// detents). `false` while a NON-committing fade (transport resume-in, sleep
    /// wind-down, wake, alarm ramp, skip dip) animates: those leave `logical_gain_db`
    /// at the STALE pre-fade level and only move `live_gain_db`, so a knob press must
    /// step from the live in-flight gain instead - otherwise a DOWN during a gentle
    /// wake would compute its target from the loud pre-sleep baseline and jump the
    /// volume UP (a startle). Set `true` by every baseline commit (set_manual_volume,
    /// the commit_logical install, and each settling terminal); set `false` when a
    /// non-committing fade is installed.
    baseline_committed: bool,
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
    /// MPD `random` flag: pick the next entry at random instead of sequentially.
    random: bool,
    /// MPD `repeat` flag: at the end of the queue, loop back to the first entry
    /// instead of stopping (repeat-all). Combined with `single`, repeats the one
    /// current track.
    repeat: bool,
    /// MPD `single` flag: after the current track, stop (or, with `repeat`,
    /// replay the same track) instead of advancing.
    single: bool,
    /// MPD `consume` flag: remove each entry from the queue once it has played.
    consume: bool,
    /// Deterministic RNG state for `random` next-track selection (splitmix64). A
    /// plain u64 (not a heavyweight RNG) so it is trivially seedable from tests
    /// via [`State::seed_rng`], keeping `random` advance assertions non-flaky.
    rng_state: u64,
    /// The library [`Song`] that most recently reached its natural EOF (finished
    /// playing), if any. Set ONLY on a real natural track-end (the
    /// [`Self::advance_on_eof`] path); a live/continuous stream leaves it untouched
    /// (streams have no library id and must never become a similar seed). It is the
    /// RECENCY seed for "more like this one" when nothing is currently playing: the
    /// just-finished track is the most pertinent thing to seed from - the user was
    /// almost certainly listening to it when they typed the ask. Preferred over the
    /// first-queued song, but the current playing song still wins over it. A user
    /// skip does NOT set it (advance_on_eof early-returns on a pending skip), so it
    /// only ever holds a track that genuinely played through to its end. The whole
    /// [`Song`] (not just its [`SongId`]) is kept so the synchronous status render can
    /// name the finished title without an `.await`, and so it survives consume mode
    /// where the finished entry is already evicted from the queue by status time.
    last_finished: Option<Song>,
    /// The MPD id ([`QueueItem::id`]) of the FIRST song appended by the most recent
    /// fresh IDLE enqueue gesture (an append that landed while `current` was `None`).
    /// `None` at rest and whenever a track is/was playing since the last idle gesture
    /// (every play-time current-commit clears it). LATEST GESTURE WINS: a fresh idle
    /// enqueue of new music makes that newly-appended music the pertinent context, so
    /// [`Self::seed_source`] must seed from the freshly-appended tail rather than a
    /// finished track still LINGERING at the queue head (consume-off leaves the played
    /// entry at pos 0). Scenarios R (recency) and G (gesture) present an IDENTICAL
    /// queue/current/last_finished snapshot; the ONLY difference is temporal - in G a
    /// fresh idle enqueue happened AFTER the finish, in R it did not - so that bit must
    /// be RECORDED when it happens (it cannot be recovered from the snapshot). A raw id
    /// (not a bool, not a position) is the honest floor: it BOTH records that a fresh
    /// gesture occurred AND names which tail to seed past the lingering head, and qids
    /// are monotonic + stable across removals (positions are not), so the anchor keeps
    /// pointing at the fresh music even as the lingering head is deleted or the queue
    /// reorders. EPHEMERAL: never persisted to resume.toml (a daemon restart is not a
    /// fresh gesture), so it defaults `None` after a restart.
    fresh_enqueue_anchor: Option<u64>,
    /// Deterministic RNG state for the volume-glide human-noise DITHER, SEPARATE
    /// from `rng_state` so drawing a dither on every setvol never desyncs the
    /// `random` next-track selection (whose seeded advances tests assert). Seeded
    /// from a fixed non-zero constant in Default and the wall clock in the
    /// constructor; tests pin it directly.
    vol_dither_state: u64,
    /// LIVE ICY now-playing metadata for the current RAW STREAM entry, keyed by the
    /// LATCHED [`QueueId`] it belongs to, or `None` at rest / for a library track.
    /// Set from the lossless `PlayerEvent::StreamMetadata` spine arm (via
    /// [`HypodjHandler::set_stream_meta`]) and read by `currentsong` to surface the
    /// station in `Name:` and the now-playing line in `Title:` instead of the raw
    /// URL. The qid key means it can ONLY decorate the entry it came from - a library
    /// song never inherits a station's label and a stale slot from a prior stream
    /// never leaks onto a new entry. EPHEMERAL: never persisted to resume.toml.
    stream_meta: Option<(QueueId, StreamMeta)>,
    /// The recognized Shazam cover-art URL for the current raw stream (task
    /// f7vnd3i), keyed by the LATCHED [`QueueId`] it belongs to, or `None` when no
    /// on-demand `identify` has matched the current entry. Surfaced toward the
    /// dj-gui art pane as the `X-CoverArt` currentsong extension field. Separate
    /// from [`Self::stream_meta`] (which carries only the ICY-shaped Name/Title) so
    /// the recognition cover rides its own qid-gated slot without reshaping the ICY
    /// type. EPHEMERAL: never persisted to resume.toml; cleared on every play edge
    /// alongside `stream_meta` so a stale cover can never leak onto a new track.
    recognized_cover: Option<(QueueId, String)>,
    /// End-of-queue CONTINUATION-radio arming toggle (`continuation on|off`). When
    /// ON and a station is configured, the [`Self::advance_on_eof`] drain edge flows
    /// into the continuation station instead of stopping silent. Default OFF and
    /// startle-safe (never default-on): a configured station does nothing until this
    /// is explicitly armed. PERSISTED to resume.toml (unlike random/repeat), so the
    /// arming survives a restart - the one runtime toggle whose intent outlives the
    /// session.
    continuation: bool,
    /// RE-ENTRANCY guard for end-of-queue continuation: the stable MPD id of the
    /// currently-active continuation stream (the raw [`QueueEntry::Stream`] that
    /// [`Self::try_continuation`] cold-loaded), or `None` when no continuation
    /// stream is live. This is the linchpin that makes continuation ONE-SHOT: mpv's
    /// `loadfile` returns Ok the instant the load is QUEUED (not when the stream
    /// CONNECTS), so a dead/unreachable/404 continuation URL surfaces LATER as an
    /// `EndFile(Error)` -> [`PlayerEvent::Eof`] on the SAME spine that feeds
    /// [`Self::advance_on_eof`]. Without this guard that Eof would re-enter the
    /// drain edge, fire `try_continuation` AGAIN, and loop unbounded (queue grows,
    /// deck Playing over silence). When the finishing entry's id matches this, the
    /// drain edge knows the just-ended entry was itself a continuation stream and
    /// stops HONESTLY and LOUDLY instead of re-firing - a good station plays
    /// indefinitely, a drop is an honest stop, never a retry loop. EPHEMERAL: never
    /// persisted (a restart rebuilds it as a plain queued stream); ids are
    /// monotonic within a session, so a stale value can never false-match a later
    /// entry. Set on a successful cold-start; cleared on the honest stop.
    continuation_active: Option<u64>,
}

/// Which pertinence branch a [`Handler::seed_source`] resolution landed on - the
/// SINGLE source of truth shared by the enqueue seed and the ambient context hint.
/// The seed_source ladder ranks: `NowPlaying` -> fresh-gesture tail (`UpNext`) ->
/// `JustFinished` -> first-queued fallback (`UpNext`). What is playing beats a fresh
/// idle-enqueue gesture (latest gesture wins over recency), which beats what merely
/// finished, which beats what is merely queued. `UpNext` covers BOTH the fresh-gesture
/// tail and the cold-start first-queued song (both are "what is up"); the temporal
/// distinction between them lives in the ladder order, not the kind. The future
/// centroid extends this enum (a `Selected` / `TimeOfDay` / `Ask` variant) without
/// forking the ranking.
#[derive(Debug, Clone, PartialEq)]
enum SeedKind {
    /// A library song is currently playing - it wins over everything.
    NowPlaying,
    /// Nothing is playing; the most recently finished track is the recency seed.
    JustFinished,
    /// What is up: the freshly-enqueued tail (latest gesture) or, at cold start, the
    /// first queued library song (nothing has played yet).
    UpNext,
}

/// The one resolved seed the DJ would enqueue "more like this" from, with the
/// pertinence branch it came from and the title captured synchronously (so a
/// non-`await` status render can name it). Produced by [`Handler::seed_source`] and
/// read by BOTH [`Handler::similar_seed_id`] (the enqueue seed) and
/// [`Handler::ambient_hint_pairs`] (the hint) - one computation, two readers, so the
/// hint can never name a seed the enqueue would not use.
#[derive(Debug, Clone, PartialEq)]
struct SeedSource {
    kind: SeedKind,
    id: SongId,
    title: String,
}

/// One splitmix64 step: advance `state` and return a well-mixed u64. The shared
/// deterministic mixer for `random` next-track selection AND the volume-glide
/// dither draw; seedable so both are reproducible in tests.
fn splitmix64(state: &mut u64) -> u64 {
    *state = state.wrapping_add(0x9E37_79B9_7F4A_7C15);
    let mut z = *state;
    z = (z ^ (z >> 30)).wrapping_mul(0xBF58_476D_1CE4_E5B9);
    z = (z ^ (z >> 27)).wrapping_mul(0x94D0_49BB_1331_11EB);
    z ^ (z >> 31)
}

impl State {
    /// One splitmix64 step over `rng_state`, the deterministic source for
    /// `random` next-track selection; seedable in tests.
    fn next_rand(&mut self) -> u64 {
        splitmix64(&mut self.rng_state)
    }

    /// Pick a random in-range next index for `random` playback, avoiding an
    /// immediate repeat of `current` when the queue has more than one entry.
    fn random_next_index(&mut self, current: Option<usize>) -> Option<usize> {
        let len = self.queue.len();
        if len == 0 {
            return None;
        }
        if len == 1 {
            return Some(0);
        }
        let pick = (self.next_rand() % len as u64) as usize;
        match current {
            // Avoid an immediate repeat: rotate off the current index by one.
            Some(c) if pick == c => Some((pick + 1) % len),
            _ => Some(pick),
        }
    }
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
            // 100 -> 0 dB; the committed logical target starts in sync too.
            logical_gain_db: 0.0,
            fade_epoch: 0,
            fading: false,
            pending_pause: false,
            pending_skip: None,
            // At rest the committed logical target IS the current baseline.
            baseline_committed: true,
            playlist_version: 0,
            binary_limit: 8192,
            last_starred_order: Vec::new(),
            random: false,
            repeat: false,
            single: false,
            consume: false,
            // A fixed non-zero default seed; production is seeded from the wall
            // clock at handler construction, tests override via `seed_rng`.
            rng_state: 0x243F_6A88_85A3_08D3,
            // No track has finished yet at construction.
            last_finished: None,
            // No fresh idle enqueue gesture at rest; also defaults None after a
            // restart (never persisted - a restart is not a fresh gesture).
            fresh_enqueue_anchor: None,
            // A fixed non-zero default seed for the glide dither; production seeds
            // it from the wall clock at construction, tests pin it directly.
            vol_dither_state: 0x8B7F_A1C2_D3E4_F506,
            // No live stream metadata until a stream connects and pushes ICY tags.
            stream_meta: None,
            // No recognized cover until an on-demand `identify` matches.
            recognized_cover: None,
            // Continuation radio starts DISARMED - never a surprise on first run.
            continuation: false,
            // No continuation stream is live until one is cold-started.
            continuation_active: None,
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
    /// `v` afterward. Also clears `pending_pause`: a manual volume commit
    /// (setvol/mpris/clear/stop/fade-terminal) reconciles the deck to a concrete
    /// baseline, so it must never leave the reported state stuck at Paused. In
    /// particular, when a `setvol` supersedes an in-flight PauseOut fade before its
    /// Terminal::Pause runs (mpv still Playing), clearing here is what keeps the
    /// reported state from lying Paused forever while audio keeps playing.
    fn set_manual_volume(&mut self, v: u8) {
        self.target_volume = v;
        self.live_gain_db = mpv_volume_to_db(v as f64);
        // Keep the committed logical target in lockstep with every baseline
        // commit so the next knob press steps from the true settled level, never
        // a stale rung (the SetBaseline / StopRestore / Clear / Stop / resume
        // paths all route through here).
        self.logical_gain_db = self.live_gain_db;
        // A concrete baseline is now committed: the knob steps from it directly.
        self.baseline_committed = true;
        self.fading = false;
        self.pending_pause = false;
        // A manual volume commit also supersedes any in-flight skip: the deck is
        // being reconciled to a concrete baseline on the STILL-loaded current
        // track, so the reported current must revert from the (never-loaded) skip
        // target back to `current` (mirrors clearing `pending_pause`).
        self.pending_skip = None;
    }

    /// The current index to REPORT outward (status/MPRIS/currentsong). During an
    /// in-flight user skip this is the SKIP TARGET (`pending_skip`), so the outward
    /// view collapses the dip window to the target immediately; otherwise it is the
    /// real `current`. Mirrors [`Self::reported_play_state`]'s pending-pause layer.
    fn reported_current(&self) -> Option<usize> {
        self.pending_skip.or(self.current)
    }

    /// Whether the queue is GENUINELY exhausted at an EOF edge - the TRUE-DRAIN
    /// signal that gates end-of-queue continuation so it can only fire when there is
    /// genuinely nothing left to play. It asks "IGNORING the `single`
    /// stop-after-current mode, would a normal advance still have somewhere to go?".
    /// This is what separates a real end-of-queue None from a `single`-mode None:
    /// with `single on` and tracks STILL queued, [`Self::plan_next`] returns None by
    /// design (stop after the current track), but those remaining tracks must still
    /// play - continuation must NOT hijack them with radio. `repeat` and `random`
    /// keep a non-empty queue cycling / drawing forever (never a genuine end); a
    /// plain sequential queue drains only when the current entry is the last one;
    /// `consume` drains only when removing the current would empty the queue (the
    /// same last-entry test here, evaluated BEFORE plan_next mutates the queue).
    /// Does NOT consume any `random` walk state (it never calls the mutating
    /// `random_next_index` - random simply never drains while entries remain).
    fn is_true_drain(&self) -> bool {
        let Some(cur) = self.reported_current() else {
            // No current entry to advance from: only an already-empty queue reads as
            // drained (a stopped/empty deck is not a live EOF edge).
            return self.queue.is_empty();
        };
        let len = self.queue.len();
        if len == 0 {
            return true;
        }
        // repeat cycles the queue; random keeps drawing from a non-empty queue:
        // neither reaches a genuine end while entries remain.
        if self.repeat || self.random {
            return false;
        }
        // Sequential (and consume, which removes the current): drained iff the
        // current entry is the last one.
        cur + 1 >= len
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
    async fn cancel_with<R>(&self, apply: impl FnOnce() -> R) -> R {
        let mut slot = self.inner.lock().await;
        if let Some(mut h) = slot.take() {
            h.abort.abort();
            if let Some(join) = h.join.take() {
                let _ = join.await;
            }
        }
        apply()
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
/// A target's play arguments, pre-resolved SYNCHRONOUSLY (the Subsonic
/// `stream_url` is sync) so the skip dip terminal only needs a SINK-level
/// `play_url`, never a `&self` handler call under the fade slot lock.
#[derive(Clone)]
struct ResolvedPlay {
    song_id: Option<SongId>,
    qid: QueueId,
    url: String,
}

// NOTE: no `Copy` - the `SkipLoad` arm carries owned, non-Copy fields
// (`ResolvedPlay`, `FadeSpec`) that are MOVED exactly once (into the terminal,
// then into the follow-on spawn). The remaining arms stay trivially matchable.
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
    /// Startle-safe transport PAUSE: the ramp has reached silence, so now PAUSE mpv
    /// (audio already silent - no click) and clear `fading`, WITHOUT touching
    /// `target_volume` (the baseline is preserved as the level a later RESUME rises
    /// to). Then RESTORE mpv's real volume to that baseline while paused - paused is
    /// silent anyway, so this is inaudible, and it guarantees that ANY later play
    /// path (a fresh play, a new queue, a plan) starts at the correct level rather
    /// than stuck at the faded-down ~0. RESUME re-forces silence and fades back in,
    /// so this restore never causes a resume to skip the ramp. Distinct from
    /// [`Terminal::StopRestore`] (which stops and restores).
    Pause,
    /// The heart of SKIP-FADE: the dip-to-silence has landed, so LOAD the
    /// pre-resolved target from silence and hand off to a follow-on ResumeIn.
    /// Runs UNDER the fade slot lock, only when still the current epoch (a
    /// superseding skip/setvol/stop aborted this task before it reached here, so a
    /// stale dip can NEVER load the wrong track). The deck is already at silence;
    /// `sink.play_url` loads the target (mpv softvol ~0 persists across loadfile),
    /// then `current` is committed, `pending_skip` cleared, and a fresh ResumeIn
    /// fade (`resume_spec` -> `SetBaseline(resume_vol)`) is spawned into the SAME
    /// slot - one path, one arbiter.
    SkipLoad {
        idx: usize,
        play: ResolvedPlay,
        resume_spec: FadeSpec,
        resume_vol: u8,
        /// The dB level the dip bottomed out at (the deck sits here when the target
        /// loads, and the ResumeIn rises FROM here). A shallow dip (see
        /// [`SKIP_DIP_DB`]) keeps a skip snappy: the dip/resume step count scales
        /// with this depth, so a shallower floor means far fewer 250ms steps.
        dip_floor_db: f64,
    },
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
    /// If `Some((db, vol))`, the fade's install (the spawn closure, under the
    /// FadeSlot lock alongside the epoch bump) SYNCHRONOUSLY commits
    /// `logical_gain_db = db` and `target_volume = vol` - BEFORE any tick. So a
    /// superseded key-mash / slider-drag still commits every intermediate rung,
    /// and the off-click pause reads the true quiet baseline. Does NOT touch
    /// `live_gain_db` and does NOT clear `fading` (the envelope still animates).
    pub commit_logical: Option<(f64, u8)>,
}

/// The abstract, fade-native fade intents. Kept separate from the MPD
/// [`FadeKind`] so the executor is not coupled to the wire grammar. Each resolves
/// (against the live gain + the comfort ceiling) into a concrete
/// [`FadeTarget`] + sub-JND policy + [`Terminal`].
#[derive(Clone, Copy, Debug)]
pub enum FadeIntent {
    /// Ramp to silence, then stop playback and restore the pre-fade baseline.
    Out,
    /// Startle-safe transport PAUSE: a SHORT DELIBERATE ramp to silence (3 dB/step,
    /// NOT the long sub-JND fade), then PAUSE mpv (not stop) leaving the baseline
    /// volume untouched, so a later RESUME ramps back to exactly the pre-pause level.
    /// Resolves to (Silence, sub_jnd=false, [`Terminal::Pause`]) with the duration
    /// clamped UP to the deliberate-safe minimum (never a hard cut). The RESUME half
    /// reuses [`FadeIntent::ResumeIn`] (a short deliberate wake from silence).
    PauseOut,
    /// User-initiated RESUME ramp: a SHORT DELIBERATE ramp UP from silence to the
    /// pre-pause level, committing it as the baseline. Unlike [`FadeIntent::WakeTo`]
    /// (the long sub-JND alarm/restore wake), a resume is a responsive, click-safe
    /// cue - deliberate 3 dB/step, duration clamped UP to the safe minimum. Resolves
    /// to (Db(target_db), sub_jnd=false, [`Terminal::SetBaseline`]).
    ResumeIn { target_db: f64, vol: u8 },
    /// Wake ramp UP to the comfort ceiling. NEVER ramps down: if the live gain is
    /// already at/above the ceiling the target is the live gain (a degenerate
    /// no-op), so a `fade in` at full volume does nothing rather than dropping.
    In,
    /// Deliberate cue to an explicit perceptual level, committing `vol` as the new
    /// baseline on completion. Used by `fade to <vol>` and `fade to floor`.
    To { target_db: f64, vol: u8 },
    /// One physical-potentiometer knob detent to an explicit perceptual level,
    /// committing `vol` as the baseline. Like [`FadeIntent::To`] but `clamp_dur_up`
    /// so a short single-step request always LANDS (never rejected as too-short) -
    /// the knob's "every press moves" guarantee. Works up OR down from any level.
    Knob { target_db: f64, vol: u8 },
    /// A graduated absolute-volume GLIDE (the humanized `setvol`/MPRIS drag): a
    /// short deliberate perceptual ramp to `target_db`, committing `vol` as the
    /// baseline. Resolves IDENTICALLY to [`FadeIntent::Knob`] (Db target,
    /// deliberate, SetBaseline, clamp_dur_up) - a distinct variant only for
    /// intent/readability. NEVER Silence/Pause, so `setvol 0` lands at the -60
    /// floor as a committed baseline and stays Playing (never the off-click pause).
    Glide { target_db: f64, vol: u8 },
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

/// The REAL, execute-time outcome of a `plan add`'s immediate action, threaded back
/// to the client so the DJ pane / `dj` CLI can report what ACTUALLY happened (the
/// true resolved count/effect) instead of the plan-ASKED count. This is the fix for
/// the silent no-op: an "add 5 tracks matching X" that resolves to 0 real songs must
/// say "added 0 - no matches for X", never echo the asked 5 as if it happened.
/// Rendered to a single human line by [`PlanOutcome::render`]; the daemon returns
/// that line as the `result` pair on the `plan add` response.
#[derive(Clone, Debug, PartialEq, Eq)]
pub enum PlanOutcome {
    /// An `enqueue`: `n` tracks were APPENDED (0 = the selector matched nothing).
    /// `selector` is the human phrase used in the 0-match message.
    Added { n: usize, selector: String },
    /// A `playnow`: `n` tracks enqueued + started. When `n > 0`, `title` names the
    /// track playback jumped to; when `n == 0` it is a no-match no-op.
    Played { n: usize, title: Option<String>, selector: String },
    /// A queue REMOVE: `n` entries removed (0 = clean no-op, no match).
    Removed(usize),
    /// A queue MOVE: `n` entries moved.
    Moved(usize),
    /// A queue CLEAR: `n` entries cleared.
    Cleared(usize),
    /// A queue PLAY (jump to an in-queue match): 1 = jumped, 0 = no match.
    Jumped(usize),
    /// A non-selecting immediate effect (fade / stop / pause / setvol / wake): a
    /// short verb the client shows verbatim.
    Effect(String),
    /// The action errored (log-and-continue); the message for the client.
    Failed(String),
}

impl PlanOutcome {
    /// The single human line the client shows after `y`/confirm. Speaks the REAL
    /// count/effect (0 when the selector resolved to nothing), NEVER the asked count.
    pub fn render(&self) -> String {
        match self {
            PlanOutcome::Added { n: 0, selector } => {
                format!("added 0 - no matches for {selector}")
            }
            PlanOutcome::Added { n, .. } => format!("added {n} {}", tracks_word(*n)),
            PlanOutcome::Played { n: 0, selector, .. } => {
                format!("added 0 - no matches for {selector}")
            }
            PlanOutcome::Played { title: Some(t), .. } => format!("played {t}"),
            PlanOutcome::Played { n, .. } => format!("played {n} {}", tracks_word(*n)),
            PlanOutcome::Removed(n) => format!("removed {n} {}", tracks_word(*n)),
            PlanOutcome::Moved(n) => format!("moved {n} {}", tracks_word(*n)),
            PlanOutcome::Cleared(n) => format!("cleared {n} {}", tracks_word(*n)),
            PlanOutcome::Jumped(0) => "no matching track to play".to_string(),
            PlanOutcome::Jumped(_) => "jumped to the matching track".to_string(),
            PlanOutcome::Effect(s) => s.clone(),
            PlanOutcome::Failed(e) => format!("could not do that: {e}"),
        }
    }
}

/// "track" vs "tracks" for a count.
fn tracks_word(n: usize) -> &'static str {
    if n == 1 {
        "track"
    } else {
        "tracks"
    }
}

/// The human phrase for a library [`Selector`], used in the 0-match "no matches for
/// X" outcome: a quoted query/genre, or a short label for the non-literal pools.
fn selector_phrase(sel: &Selector) -> String {
    match sel {
        Selector::Query(q) => format!("\"{q}\""),
        Selector::Genre(g) => format!("\"{g}\""),
        Selector::Radio => "random tracks".to_string(),
        Selector::Similar(_) => "similar tracks".to_string(),
        Selector::SimilarToCurrent => "the current track".to_string(),
        Selector::Calmer(_) => "calmer tracks".to_string(),
        Selector::Exact(_) => "those tracks".to_string(),
    }
}

impl FadeIntent {
    /// Resolve into `(target, sub_jnd, terminal, clamp_dur_up)` against the live
    /// `from_db`, the configured comfort `ceiling`, and the wind-down `floor_db`.
    /// `clamp_dur_up` requests that a too-short DELIBERATE duration be extended to
    /// the startle-safe minimum instead of being rejected as `StepTooLarge` (the
    /// pause/resume transport ramps must always land, never hard-cut).
    fn resolve(
        self,
        from_db: f64,
        ceiling: f64,
        floor_db: f64,
    ) -> (FadeTarget, bool, Terminal, bool) {
        match self {
            FadeIntent::Out => (FadeTarget::Silence, true, Terminal::StopRestore, false),
            // Short DELIBERATE ramp to silence, then PAUSE (not stop): the baseline
            // is preserved as the resume level. clamp_dur_up so a 0.5s request over a
            // large span extends to the safe minimum rather than being rejected.
            FadeIntent::PauseOut => (FadeTarget::Silence, false, Terminal::Pause, true),
            // Sub-JND to the floor level, committing it as the baseline: playback
            // continues quiet, no mute step, no click.
            FadeIntent::ToFloor => {
                // Never ramp UP: if the live level is already at/below the floor,
                // hold it (target = min(floor, from)) so a wind-down cannot
                // re-brighten a quieter state.
                let target = floor_db.min(from_db);
                let vol = db_to_mpv_volume(target).round().clamp(0.0, 100.0) as u8;
                (FadeTarget::Db(target), true, Terminal::SetBaseline(vol), false)
            }
            FadeIntent::In => {
                // Ceiling clamp: target the HIGHER of the live gain and the
                // ceiling, so the fade only ever rises (never re-brightens past a
                // manual level, never drops when named `in`).
                let target_db = from_db.max(ceiling);
                let vol = db_to_mpv_volume(target_db).round().clamp(0.0, 100.0) as u8;
                (FadeTarget::Db(target_db), true, Terminal::SetBaseline(vol), false)
            }
            FadeIntent::To { target_db, vol } => {
                (FadeTarget::Db(target_db), false, Terminal::SetBaseline(vol), false)
            }
            // One knob detent: deliberate, commits the new baseline, clamp_dur_up so
            // a short 3 dB step always lands rather than rejecting as StepTooLarge.
            FadeIntent::Knob { target_db, vol } => {
                (FadeTarget::Db(target_db), false, Terminal::SetBaseline(vol), true)
            }
            // Absolute-volume glide: identical resolve to Knob (deliberate, commits
            // the baseline, clamp_dur_up so a large 0->100 span always lands as a
            // multi-step ramp rather than rejecting as StepTooLarge).
            FadeIntent::Glide { target_db, vol } => {
                (FadeTarget::Db(target_db), false, Terminal::SetBaseline(vol), true)
            }
            // Wake ramp: sub-JND ramp to the SAVED level, committing it as the
            // restored baseline. from_db is the synth floor (silence) at restore,
            // so the schedule rises from silence to the user's real level.
            FadeIntent::WakeTo { target_db, vol } => {
                (FadeTarget::Db(target_db), true, Terminal::SetBaseline(vol), false)
            }
            // Short deliberate resume ramp from silence to the saved level. Like To
            // but clamp_dur_up so a short request never rejects; SetBaseline commits.
            FadeIntent::ResumeIn { target_db, vol } => {
                (FadeTarget::Db(target_db), false, Terminal::SetBaseline(vol), true)
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
// Returns a BOXED, explicitly-`Send` future rather than being a plain
// `async fn`: `fade_task` is RECURSIVE (the `Terminal::SkipLoad` arm spawns a
// follow-on `fade_task`), and a recursive `async fn` has an infinitely-sized,
// self-referential future whose `Send` auto-trait cannot be inferred (cyclic).
// Boxing to a `dyn Future + Send` breaks the type cycle and asserts `Send` at
// the boundary, so `tokio::spawn(fade_task(..))` type-checks at every call site.
#[allow(clippy::too_many_arguments)]
fn fade_task(
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
) -> std::pin::Pin<Box<dyn std::future::Future<Output = FadeOutcome> + Send>> {
    Box::pin(async move {
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
    let mut slot_guard = fade_slot.inner.lock().await;
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
                    // Adopt the reached level as the committed logical target too
                    // (this writer bypasses set_manual_volume).
                    st.logical_gain_db = mpv_volume_to_db(v as f64);
                    // The ramp settled to a concrete baseline: the knob steps from it.
                    st.baseline_committed = true;
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
                Terminal::Pause => {
                    // The ramp reached silence: PAUSE mpv now (audio already muted,
                    // no click). Awaiting the player actor here is fine - we hold the
                    // tokio slot mutex, never the std Mutex<State>, across the await.
                    // Surface a pause failure honestly instead of swallowing it (the
                    // pause happens here, off the set_pause call stack - see F6).
                    if let Err(e) = sink.pause().await {
                        tracing::warn!(error = %e, "pause fade terminal: mpv pause failed");
                    }
                    // Restore mpv's real volume to the preserved baseline while paused
                    // (paused is silent, so inaudible + no click): this guarantees ANY
                    // later play path starts at the correct level, never stuck at the
                    // faded-down ~0. RESUME re-forces silence and fades back in, so
                    // this never lets a resume skip its ramp.
                    let baseline = state.lock().unwrap().target_volume;
                    let _ = sink.set_volume(baseline).await;
                    // Clear `fading` and re-sync the live gain to the restored
                    // baseline so the reported volume snaps back to it; leave
                    // target_volume (the resume level) untouched.
                    {
                        let mut st = state.lock().unwrap();
                        st.live_gain_db = mpv_volume_to_db(baseline as f64);
                        st.fading = false;
                        // Restored to the baseline: the knob (a knob-up resume) once
                        // more steps from the committed logical target.
                        st.baseline_committed = true;
                        // The real pause has landed (mpv is Paused): the pending
                        // intent is fulfilled and the raw state now carries it.
                        st.pending_pause = false;
                    }
                    // Fire the change signal AFTER the Paused state edge, so the MPRIS
                    // property-update loop re-emits PlaybackStatus = Paused (the GNOME
                    // widget refresh) and MPD `idle` wakes.
                    changed.notify_waiters();
                }
                Terminal::SkipLoad { idx, play, resume_spec, resume_vol, dip_floor_db } => {
                    // The dip reached its floor AND this is still the current epoch, so
                    // no superseding skip/setvol/stop got here first: it is SAFE to
                    // load the target. mpv's softvol (at the dip floor) persists across
                    // the switch, so the new track starts at that shallow-duck level
                    // and the follow-on ResumeIn owns the rise back to the baseline.
                    // switch_warmed lands on the prefetched entry near-instant (the
                    // trough gap collapses toward ~0); if the warm never completed it
                    // falls back to a plain loadfile-replace - today's behavior, so a
                    // prefetch miss/failure is never worse than before.
                    let _ = sink.switch_warmed(play.song_id, Some(play.qid), &play.url).await;
                    // Commit the target as the real current, clear the reported-target
                    // override, pin the live gain to the dip floor (where the deck and
                    // the ResumeIn's from_db agree) and keep `fading` true - the
                    // follow-on ResumeIn continues the envelope without a gap. Bump the
                    // epoch so the follow-on is tagged strictly newer than this
                    // (now-finished) dip.
                    let epoch2 = {
                        let mut st = state.lock().unwrap();
                        st.current = Some(idx);
                        st.pending_skip = None;
                        // A track is now current: clear any fresh-enqueue anchor
                        // (defensive - a skip always starts from a playing deck, so the
                        // anchor is already None; keeps the "cleared the instant any
                        // track becomes current" invariant total across every commit).
                        st.fresh_enqueue_anchor = None;
                        st.live_gain_db = dip_floor_db;
                        st.fading = true;
                        st.fade_epoch += 1;
                        st.fade_epoch
                    };
                    // Spawn the follow-on ResumeIn (silence -> baseline) into the SAME
                    // slot we already hold, reclaiming the dip's now-finished handle.
                    // REUSES fade_task verbatim: the FadeSlot + fade_epoch stay the sole
                    // arbiter (a 2nd skip during this ramp is an ordinary slot fade that
                    // supersede aborts, SetBaseline never running).
                    // fade_task returns a boxed Send future (it is recursive), so
                    // this follow-on spawn is a plain call - the FadeSlot + epoch
                    // stay the sole arbiter.
                    let join = tokio::spawn(fade_task(
                        sink,
                        resume_spec,
                        state.clone(),
                        changed.clone(),
                        epoch2,
                        Terminal::SetBaseline(resume_vol),
                        fade_slot.clone(),
                        synth_floor_db,
                    ));
                    *slot_guard = Some(FadeHandle { abort: join.abort_handle(), join: Some(join) });
                    // Notify the Playing / new-track edge (MPD idle + MPRIS refresh).
                    changed.notify_waiters();
                }
            },
            FadeOutcome::SinkError(_) => {
                let mut st = state.lock().unwrap();
                let v = db_to_mpv_volume(st.live_gain_db).round().clamp(0.0, 100.0) as u8;
                st.target_volume = v;
                // Adopt the settled level as the committed logical target too
                // (this writer bypasses set_manual_volume).
                st.logical_gain_db = mpv_volume_to_db(v as f64);
                // Settled to a concrete baseline: the knob steps from it.
                st.baseline_committed = true;
                st.fading = false;
                drop(st);
                changed.notify_waiters();
            }
        }
    }
    outcome
    })
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

    // ── P4 content-intelligence ──────────────────────────────────────────────
    /// The per-song feature source backing the `Calmer` (and future energy-ramp)
    /// selectors. Defaults to [`MetadataStore`] (pure genre/year heuristics). This
    /// is the durable seam: an Essentia-backed store returning real embeddings can
    /// swap in behind the same trait WITHOUT touching selector or wire code. Used
    /// READ-ONLY via pure `features(...)` calls - no lock, never held across an
    /// `.await`.
    store: Arc<dyn FeatureStore>,
    /// The latent-field FIRST SLICE: a small, decaying set of active PULLS biasing
    /// P4 candidate ranking. `FieldSource` with its wings folded
    /// (docs/design/latent-field-interface.md). Read/mutated under a SHORT std-`Mutex`
    /// scope, NEVER held across an `.await` - the pure `intelligence::pull_*` work
    /// happens on cloned values. Empty by default: with no pull the selection path is
    /// byte-identical to today.
    pulls: Mutex<PullField>,
    /// In-flight guard for on-demand `identify` (task f7vnd3i): `true` while a
    /// capture + `songrec` recognition is running, so rapid repeat triggers are
    /// debounced to ONE at a time and the Shazam endpoint is never hammered.
    /// Reset by an RAII guard on every exit path (including a panic/timeout).
    recognizing: AtomicBool,
    /// The configured end-of-queue CONTINUATION station (a Navidrome station NAME or
    /// an absolute `http(s)://` stream URL), or `None` when the `[continuation]`
    /// section is unset (feature off). Set once by the daemon via
    /// [`Self::set_continuation_station`]. Read under a SHORT std-`Mutex` scope, the
    /// station-URL resolution network call happening only AFTER the lock is released
    /// (see [`Self::resolve_continuation_url`]). Mutex (not OnceLock) mirrors
    /// [`Self::state_path`]: an unset feature is a valid state.
    continuation_station: Mutex<Option<String>>,
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

/// The dB floor a USER-skip dip bottoms out at (a shallow duck, NOT full
/// `synth_floor` silence). A deliberate fade costs one >= 250ms step per 3 dB
/// (the startle-safe minimum step interval), so dipping all the way to the -60 dB
/// synth floor and back takes ~20 steps each way (~5s each, ~10s round trip) - far
/// too long for a skip. A shallow -18 dB duck is ~6 steps each way (~1.5s each),
/// so a skip stays snappy while remaining a smooth, startle-safe transition (the
/// old track ducks to ~1/8 loudness, the new track loads there and rises back to
/// the baseline). Closer to 0 = shallower + faster; deeper = slower.
const SKIP_DIP_DB: f64 = -18.0;

/// NEAR-EOF GUARD threshold for the warm-skip prefetch. When the CURRENT track is
/// within this many seconds of its natural end, [`HypodjHandler::skip_with_fade`]
/// DECLINES the background warm and takes the plain trough loadfile-replace path.
/// A warm appends the target as a 2nd playlist entry, and mpv would AUTO-ADVANCE
/// into it at the current track's natural EOF - so a skip pressed this close to the
/// end could make the warm target audible at the shallow duck AND fire a phantom
/// queue advance for the outgoing track. Declining sidesteps that entirely; the
/// warm has no time to pay off this near the end anyway.
const NEAR_EOF_GUARD_SECS: f64 = 2.0;

/// The perceptual dB at which the wake/resume ramp-in first becomes HEARABLE, 20 dB
/// above the -60 dB synth floor. The resume path reads the wall-clock LEAD - the
/// time the wake ramp first crosses this level - off the real schedule and seeks
/// the track back by that LEAD, so the playhead lands at the saved position at the
/// first-audible instant (no audible content skipped or replayed under the inaudible
/// head of the ramp). A judgement call on where mpv's cubic softvol becomes audible;
/// named so it is easy to tune - higher loses audible content, lower rewinds more.
const AUDIBILITY_DB: f64 = -40.0;

/// One physical-potentiometer knob detent, in dB. 3 dB is a clear, EQUAL-loudness
/// "one notch" everywhere on the range (a ~just-noticeable-strong step), curing the
/// linear `setvol +/-5` unevenness (which is ~+18 dB near the bottom but ~+0.4 dB
/// near the top). It equals `fade::DELIBERATE_STEP_CAP_DB`, so one detent is exactly
/// one legal deliberate fade step - no multi-step startle, no sub-JND dithering.
const KNOB_STEP_DB: f64 = crate::fade::DELIBERATE_STEP_CAP_DB;

/// Per-keyword cap on songs pulled when resolving a free-text mood/multi-word
/// `Query`. A huge library can return hundreds of hits per common keyword; without
/// a bound the OR-merge (and the state mutation it feeds) would grow with library
/// richness. This caps EACH keyword's contribution so resolution cost stays flat.
const QUERY_KEYWORD_SONG_CAP: usize = 50;

/// Command/filler words that carry no library-search signal. Stripping them keeps
/// the per-keyword full-text `search3` keyed on content (genre/mood/artist words).
///
/// SHARED SHAPE (copied, not linked): this mirrors `hypodj-client`'s grounding
/// stopword list. The daemon must not depend on the client crate, so the tiny pure
/// helper is duplicated here rather than coupling the two. Keep the two lists in
/// rough sync when either grows.
const QUERY_STOPWORDS: &[&str] = &[
    "play", "queue", "add", "put", "on", "some", "a", "an", "the", "few", "couple",
    "bunch", "of", "track", "tracks", "song", "songs", "music", "please", "at", "end",
    "next", "now", "after", "current", "me", "something", "stuff", "and", "to", "for",
    "up", "more", "bit", "little", "playing", "start", "with", "but", "super",
];

/// Words that flip a free-text `Query` into EXCLUSION mode: every content keyword
/// AFTER one of these is a term the user does NOT want. Kept OUT of
/// [`QUERY_STOPWORDS`] (a stopword would be silently dropped, turning "not chill"
/// into a positive "chill" search - the exact wrong-song bug). The latch stays on
/// for the rest of the phrase (a single mood ask rarely re-includes after a "not").
const QUERY_NEGATIONS: &[&str] = &["not", "no", "without", "except", "excluding", "minus"];

/// Split content keywords of a free-text `Query` phrase into INCLUDE terms (wanted)
/// and EXCLUDE terms (explicitly rejected via a [`QUERY_NEGATIONS`] word). Each list
/// is lowercased, stopword/1-char-filtered, and deduped preserving first-seen order;
/// a term that lands in `exclude` is never also kept in `include`. Pure + unit-tested.
#[derive(Debug, Default, PartialEq)]
struct QueryKeywords {
    include: Vec<String>,
    exclude: Vec<String>,
}

/// Lowercased content keywords from a free-text `Query` phrase, partitioned into
/// wanted vs rejected terms (see [`QueryKeywords`]). Split on non-alphanumeric, drop
/// stopwords and 1-char tokens. A [`QUERY_NEGATIONS`] token latches EXCLUSION on for
/// the remaining tokens so an explicit "not X" pushes X into `exclude` instead of
/// searching for it. Mirrors `hypodj-client::grounding::content_keywords` (see
/// [`QUERY_STOPWORDS`]) but keeps the split tokens so the resolver can search per
/// keyword and filter the rejected ones out.
fn query_content_keywords(phrase: &str) -> QueryKeywords {
    let mut kw = QueryKeywords::default();
    let mut negated = false;
    for w in phrase.split(|c: char| !c.is_alphanumeric()) {
        let w = w.to_lowercase();
        if QUERY_NEGATIONS.contains(&w.as_str()) {
            negated = true;
            continue;
        }
        if w.len() <= 1 || QUERY_STOPWORDS.contains(&w.as_str()) {
            continue;
        }
        if negated {
            if !kw.exclude.contains(&w) {
                kw.include.retain(|k| k != &w);
                kw.exclude.push(w);
            }
        } else if !kw.include.contains(&w) && !kw.exclude.contains(&w) {
            kw.include.push(w);
        }
    }
    kw
}

/// OR-merge per-keyword `search3` song-hit lists into ONE relevance-ordered, deduped
/// list. A song that matched MORE keywords leads (a track hitting both "chill" and
/// "electronic" outranks one hitting only "electronic"); within an equal match count
/// the first-seen order (keyword order, then per-keyword result order) is preserved
/// via a STABLE sort. Deduped by [`SongId`]. Truncated to `want`. Pure + unit-tested:
/// no network, no clock, no lock.
fn merge_keyword_hits(per_keyword: Vec<Vec<Song>>, want: usize) -> Vec<Song> {
    let mut order: Vec<SongId> = Vec::new();
    let mut counts: HashMap<SongId, usize> = HashMap::new();
    let mut songs: HashMap<SongId, Song> = HashMap::new();
    for hits in per_keyword {
        // A song appearing twice under the SAME keyword must not double-count that
        // keyword; only distinct keywords raise the relevance score.
        let mut seen_this_kw: std::collections::HashSet<SongId> = std::collections::HashSet::new();
        for s in hits {
            if !seen_this_kw.insert(s.id.clone()) {
                continue;
            }
            *counts.entry(s.id.clone()).or_insert(0) += 1;
            if !songs.contains_key(&s.id) {
                order.push(s.id.clone());
                songs.insert(s.id.clone(), s);
            }
        }
    }
    // Stable sort by match count DESC; ties keep first-seen order (slice sort is stable).
    order.sort_by(|a, b| counts[b].cmp(&counts[a]));
    order.into_iter().take(want).filter_map(|id| songs.remove(&id)).collect()
}

/// PURE re-rank for the `Calmer` selector. Given a `seed`, a candidate `pool`, and
/// the desired `want`, sort candidates ASCENDING by energy (via the injected
/// [`FeatureStore`]), keep those strictly calmer than the seed, and if that leaves
/// fewer than `want`, top up from the remaining lowest-energy candidates. Truncate
/// to `want`. Deterministic given fixed inputs (a stable sort over a total energy
/// key, with `SongId` as the tiebreak so equal-energy ties never reorder
/// nondeterministically). No network, no clock, no lock - unit-testable in
/// isolation with a fabricated pool and a fake store.
fn calmer_rerank(
    store: &dyn FeatureStore,
    seed: &Song,
    mut pool: Vec<Song>,
    want: usize,
) -> Vec<Song> {
    let energy = |s: &Song| store.features(s).map(|f| f.energy).unwrap_or(0.5);
    let seed_e = energy(seed);
    // Ascending by energy; break ties by id so the order is fully deterministic.
    pool.sort_by(|a, b| {
        energy(a)
            .partial_cmp(&energy(b))
            .unwrap_or(std::cmp::Ordering::Equal)
            .then_with(|| a.id.0.cmp(&b.id.0))
    });
    // The strictly-calmer-than-seed set is the ascending prefix of length
    // `calmer_count`. When it already meets `want`, the first `want` are all
    // calmer than the seed. When it falls short, the remaining lowest-energy
    // candidates (the next slice of the SAME ascending pool) top it up. In both
    // cases the answer is exactly the first `want` of the ascending pool, so a
    // single truncate covers keep-calmer AND top-up. `calmer_count` is computed
    // only to log the honest split (how many were genuinely calmer).
    let calmer_count = pool.iter().filter(|s| energy(s) < seed_e).count();
    tracing::debug!(calmer_count, pool = pool.len(), want, "calmer re-rank");
    pool.truncate(want);
    pool
}

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
        // Seed the random-play RNG from the wall clock so a fresh daemon does not
        // always shuffle the same order across restarts (tests override via
        // `seed_rng`). Any non-zero seed is fine for splitmix64.
        let mut init_state = State::default();
        let wall = std::time::SystemTime::now()
            .duration_since(std::time::UNIX_EPOCH)
            .map(|d| d.as_nanos() as u64)
            .unwrap_or(0x243F_6A88_85A3_08D3);
        init_state.rng_state = wall | 1;
        // A distinct non-zero seed for the glide dither (mixed off the same wall
        // clock so it varies across restarts but never collides with rng_state).
        init_state.vol_dither_state = wall.rotate_left(32) | 1;
        Self {
            client,
            player,
            state: Arc::new(Mutex::new(init_state)),
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
            store: Arc::new(MetadataStore),
            pulls: Mutex::new(PullField::new()),
            recognizing: AtomicBool::new(false),
            continuation_station: Mutex::new(None),
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
        let (pendings, ids, immediates) = self.prepare_batch(raws)?;
        self.plan_pending.lock().unwrap().extend(pendings);
        self.nudge_immediates(immediates);
        Ok(ids)
    }

    /// Arm a single `plan add` and, when it is Immediate, EXECUTE its action inline
    /// (awaited) so the response can carry the REAL execute-time outcome (the true
    /// resolved count/effect), not the plan-ASKED count. The y/n + arm SEMANTICS are
    /// unchanged: the plan is still clamped, validated, and minted exactly as
    /// [`Self::plan_add`]; the only difference is that an Immediate action runs HERE
    /// (an inline await) instead of via the executor's on_immediate nudge, so the
    /// count is known synchronously and reported. The pending is NOT enrolled in the
    /// registry and NOT nudged, so there is exactly ONE execution and no zombie.
    /// Non-immediate plans arm identically to [`Self::plan_add`] and carry no outcome.
    pub async fn plan_add_reporting(
        &self,
        raw: RawPlan,
    ) -> Result<(PlanId, Option<PlanOutcome>), PlanError> {
        let (mut pendings, mut ids, immediates) = self.prepare_batch(vec![raw])?;
        // Single-plan batch: exactly one id + one pending.
        let id = ids.remove(0);
        if immediates.contains(&id) {
            // Immediate: run inline for the true outcome. An Immediate plan holds no
            // timer guard, so dropping the (un-enrolled) pending disarms nothing.
            let action = pendings.remove(0).armed.raw.action;
            let outcome = self.run_action_outcome(&action).await;
            Ok((id, Some(outcome)))
        } else {
            self.plan_pending.lock().unwrap().extend(pendings);
            Ok((id, None))
        }
    }

    /// Execute one immediate plan [`Action`] against the existing primitives,
    /// returning its REAL [`PlanOutcome`]. The SINGLE dispatch used by BOTH the
    /// executor (arm-and-forget plans, which ignore the outcome and only log a
    /// failure) and the `plan add` reporting path (which threads it back to the
    /// client). No `.await` is held across a std Mutex; each primitive owns its own
    /// locking discipline.
    ///
    /// Returns an explicitly BOXED (`Pin<Box<dyn Future + Send>>`) future rather than
    /// an `async fn`'s opaque one. This action dispatch can reach `self.handle` (for
    /// Stop/Pause/SetVol/Clear), and `handle` reaches back here via the `plan add`
    /// path (`handle` -> `handle_plan` -> `plan_add_reporting` -> here) - an opaque
    /// self-referential future cycle whose `Send`-ness the compiler cannot infer. A
    /// nameable boxed return type cuts the cycle: `handle`'s hidden type sees a
    /// concrete `Send` future here, never this body's opaque one.
    pub fn run_action_outcome<'a>(
        &'a self,
        action: &'a Action,
    ) -> std::pin::Pin<Box<dyn std::future::Future<Output = PlanOutcome> + Send + 'a>> {
        Box::pin(async move {
        match action {
            // Fade precedence: start_fade_spec IS the single FadeSlot (validates
            // before aborting; logs an autonomous takeover).
            Action::Fade(ir) => match self.start_fade_spec(crate::executor::map_fade(ir)).await {
                Ok(_) => PlanOutcome::Effect("fading".into()),
                Err(e) => PlanOutcome::Failed(e.to_string()),
            },
            Action::Stop => {
                self.handle(MpdCommand::Stop).await;
                PlanOutcome::Effect("stopped".into())
            }
            Action::Pause => {
                self.handle(MpdCommand::Pause(Some(true))).await;
                PlanOutcome::Effect("paused".into())
            }
            Action::SetVolume(v) => {
                self.handle(MpdCommand::SetVol(*v)).await;
                PlanOutcome::Effect(format!("set volume to {v}"))
            }
            // Strictly append-only (adds to the END, never starts/jumps).
            Action::Enqueue { selector, count } => {
                // Snapshot the id the FIRST appended song will take BEFORE the append
                // (plan_enqueue pushes via enqueue_song, each bumping next_id), so the
                // arm below anchors the freshly-appended tail. Single local client, so
                // this capture cannot race a concurrent append.
                let first = self.state.lock().unwrap().next_id;
                match self.plan_enqueue(selector, *count).await {
                    Ok(n) => {
                        // This append-only ask is a NEWER gesture than any prior finish:
                        // while the deck is idle (no autoplay here) anchor the just-queued
                        // music so the hint/seed names it, skipping a track that finished
                        // before this ask and lingers at the queue head (exactly the
                        // staleness this feature exists to prevent, on its most central
                        // entry point - the DJ NL surface). Scoped to THIS action, so the
                        // autoplay paths (PlayNow, wake) - which start playback and let the
                        // now-current track win the seed - and the shared enqueue_song
                        // helper stay untouched. Idle- and empty-gated inside (a no-op on
                        // an honest 0 or a playing deck).
                        self.arm_fresh_enqueue_anchor_on_append(first, n);
                        PlanOutcome::Added { n, selector: selector_phrase(selector) }
                    }
                    Err(e) => PlanOutcome::Failed(e),
                }
            }
            // Play a library song NOW: enqueue-then-start; name the started track.
            Action::PlayNow { selector, count } => {
                match self.plan_play_now(selector, *count).await {
                    Ok(0) => {
                        PlanOutcome::Played { n: 0, title: None, selector: selector_phrase(selector) }
                    }
                    Ok(n) => {
                        let title = self.current_song().map(|s| format!("\"{}\"", s.title));
                        PlanOutcome::Played { n, title, selector: selector_phrase(selector) }
                    }
                    Err(e) => PlanOutcome::Failed(e),
                }
            }
            Action::Wake { selector, count } => match self.wake_now(selector.clone(), *count).await {
                Ok(_) => PlanOutcome::Effect("waking".into()),
                Err(e) => PlanOutcome::Failed(e),
            },
            // DETERMINISTIC queue edits: the selector resolves against the LIVE queue;
            // a no-match is a clean no-op (count 0), never a wrong-target edit.
            Action::Remove { .. } => match self.plan_queue_edit(action).await {
                Ok(n) => PlanOutcome::Removed(n),
                Err(e) => PlanOutcome::Failed(e),
            },
            Action::Move { .. } => match self.plan_queue_edit(action).await {
                Ok(n) => PlanOutcome::Moved(n),
                Err(e) => PlanOutcome::Failed(e),
            },
            Action::Clear { .. } => match self.plan_queue_edit(action).await {
                Ok(n) => PlanOutcome::Cleared(n),
                Err(e) => PlanOutcome::Failed(e),
            },
            Action::Play { .. } => match self.plan_queue_edit(action).await {
                Ok(n) => PlanOutcome::Jumped(n),
                Err(e) => PlanOutcome::Failed(e),
            },
            Action::Noop => PlanOutcome::Effect("nothing to do".into()),
        }
        })
    }

    /// Validate + arm (mint ids, arm deadline timers) a batch of raw plans WITHOUT
    /// inserting them into the registry, returning the ready-to-commit
    /// `PendingPlan`s alongside their ids and the subset that are Immediate. Splitting
    /// prepare from the registry mutation lets a caller (e.g. [`Self::set_singleton`])
    /// commit under ONE lock scope that also removes a prior instance, so no window
    /// ever exposes two plans of the same origin. Whole body is await-free.
    fn prepare_batch(
        &self,
        raws: Vec<RawPlan>,
    ) -> Result<(Vec<PendingPlan>, Vec<PlanId>, Vec<PlanId>), PlanError> {
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
        // timers, then hand back the pendings for the caller to commit.
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
        Ok((pendings, ids, immediates))
    }

    /// An Immediate plan executes at add-time: nudge the executor (its action is
    /// async, so it cannot run inside a sync, lock-holding path).
    fn nudge_immediates(&self, immediates: Vec<PlanId>) {
        if let Some(tx) = self.plan_immediate.get() {
            for id in immediates {
                let _ = tx.send(id);
            }
        }
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

    /// Build a convenience plan for `origin`, so EXACTLY one is ever active: arm the
    /// replacement (validate-before-mutate, mirroring the fade discipline) then, under
    /// ONE registry lock, atomically drop EVERY prior plan of this origin and insert
    /// the new one. The swap being a single critical section is load-bearing: the
    /// handler is `Arc`-shared and connections run concurrently, so a
    /// find-then-add-then-cancel sequence would momentarily expose two plans of the
    /// same origin - a concurrent Status poll would then emit a duplicate `X-hypodj-*`
    /// key (malformed MPD status), and two racing re-arms would leak a permanent
    /// second instance. A failed validate leaves the old plan untouched (arm runs
    /// before the lock).
    fn set_singleton(&self, origin: &str, raw: RawPlan) -> Result<PlanId, PlanError> {
        let (pendings, mut ids, immediates) = self.prepare_batch(vec![raw])?;
        {
            let mut g = self.plan_pending.lock().unwrap();
            g.retain(|pp| pp.armed.raw.origin != origin);
            g.extend(pendings);
        }
        self.nudge_immediates(immediates);
        // Exactly one id for a single-plan batch.
        Ok(ids.remove(0))
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
    /// [`Selector::Calmer`], now resolved at [`Self::plan_enqueue`]) is out of
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

    /// The armed human-features (sleep / wind-down / wake) as X- prefixed MPD
    /// status pairs, computed from a SINGLE plan-registry snapshot so the three
    /// features never desync among themselves. Empty when nothing is armed, so the
    /// Status response stays lean. This is a pure SURFACING of the existing armed
    /// plan deadlines (see [`Self::plan_deadlines`]) - it recomputes nothing.
    ///
    /// - `X-hypodj-sleep-remaining`   secs until the sleep fade-to-stop fires
    /// - `X-hypodj-winddown-active`   `1` while a wind-down plan is armed
    /// - `X-hypodj-winddown-remaining` secs until a scheduled wind-down fires
    ///   (omitted for an Immediate wind-down, which has no deadline)
    /// - `X-hypodj-wake-remaining`    secs until the scheduled wake alarm
    /// - `X-hypodj-wake-at`           the wake alarm as a unix epoch second
    ///
    /// X- pairs are the MPD-safe extension mechanism (ncmpcpp ignores unknown
    /// fields). Keys carry no colon/newline and values are digits only, so the
    /// status line stays well-formed.
    pub fn armed_feature_pairs(&self) -> Vec<(&'static str, String)> {
        let now = Instant::now();
        let deadlines = self.plan_deadlines();
        let mut out = Vec::new();
        for (_, origin, deadline) in &deadlines {
            let remaining = deadline.map(|inst| inst.saturating_duration_since(now));
            match origin.as_str() {
                ORIGIN_SLEEP => {
                    if let Some(r) = remaining {
                        out.push(("X-hypodj-sleep-remaining", r.as_secs().to_string()));
                    }
                }
                ORIGIN_WINDDOWN => {
                    out.push(("X-hypodj-winddown-active", "1".to_string()));
                    if let Some(r) = remaining {
                        out.push(("X-hypodj-winddown-remaining", r.as_secs().to_string()));
                    }
                }
                ORIGIN_WAKE => {
                    if let Some(r) = remaining {
                        out.push(("X-hypodj-wake-remaining", r.as_secs().to_string()));
                        // The absolute alarm instant as a unix epoch second, so a
                        // clock display survives poll-to-poll drift. now + remaining
                        // reconstructs it from the same monotonic snapshot.
                        if let Ok(sys_now) =
                            std::time::SystemTime::now().duration_since(std::time::UNIX_EPOCH)
                        {
                            out.push((
                                "X-hypodj-wake-at",
                                (sys_now.as_secs() + r.as_secs()).to_string(),
                            ));
                        }
                    }
                }
                _ => {}
            }
        }
        out
    }

    /// Surface the ACTIVE latent-field pulls as X- status pairs, mirroring
    /// [`Self::armed_feature_pairs`]. PURE surfacing of the pulls the daemon already
    /// holds - it recomputes NO selection. Emits ZERO pairs when the field is not
    /// active (all pulls pruned/dead), so a resting field leaves status lean and the
    /// clients render nothing.
    ///
    /// - `X-hypodj-field-count`         number of live pulls
    /// - `X-hypodj-field-{i}-label`     the i-th pull's label (lexicon token(s))
    /// - `X-hypodj-field-{i}-strength`  decayed strength as an integer 0..=100
    /// - `X-hypodj-field-{i}-age`       whole minutes since the pull was born
    ///
    /// The values are digit/ASCII, colon/newline-free, so the status line stays
    /// well-formed. The client re-renders the compact HUD line from these numbers,
    /// so decay shows numerically each poll (strength ticks down, age climbs).
    pub fn field_feature_pairs(&self) -> Vec<(String, String)> {
        let now = Instant::now();
        // SHORT std-Mutex scope: prune, snapshot the live pulls into owned values,
        // then drop the lock BEFORE building the pairs (Status is synchronous, but
        // the clone-then-drop discipline keeps the std Mutex off any await).
        let snapshot: Vec<(String, u8, u64)> = {
            let mut f = self.pulls.lock().unwrap();
            f.prune(now);
            if !f.is_active(now) {
                return Vec::new();
            }
            f.snapshot(now)
        };
        let mut out = Vec::new();
        out.push(("X-hypodj-field-count".to_string(), snapshot.len().to_string()));
        for (i, (label, strength, age)) in snapshot.iter().enumerate() {
            out.push((format!("X-hypodj-field-{i}-label"), label.clone()));
            out.push((format!("X-hypodj-field-{i}-strength"), strength.to_string()));
            out.push((format!("X-hypodj-field-{i}-age"), age.to_string()));
        }
        out
    }

    /// Surface the single MOST-PERTINENT context string as X- status pairs - the
    /// ambient "btw, DJ knows" hint. A PURE re-surfacing of [`Self::seed_source`] (the
    /// same ordering the enqueue seed reads), so the hint can never name a seed the DJ
    /// would not enqueue from.
    ///
    /// - `X-hypodj-hint-kind`   the pertinence branch: `just-finished` | `up-next`
    /// - `X-hypodj-hint-title`  the seed song title verbatim
    ///
    /// Emits TWO pairs or ZERO. The `NowPlaying` branch is suppressed AT the daemon (the
    /// Now Playing pane already shows the current track, so a hint would only duplicate
    /// it), as is `None` - so while a library track plays, or nothing is seedable, the
    /// wire carries no hint pair at all and every downstream renderer stays dumb (draw
    /// one faint line iff a hint is present, no per-client play-state check). Defensive:
    /// refuses to emit a title carrying a newline, which would tear the status line.
    pub fn ambient_hint_pairs(&self) -> Vec<(&'static str, String)> {
        let (kind, title) = match self.seed_source() {
            Some(SeedSource { kind: SeedKind::JustFinished, title, .. }) => ("just-finished", title),
            Some(SeedSource { kind: SeedKind::UpNext, title, .. }) => ("up-next", title),
            // NowPlaying (the pane already shows it) and None -> lean status, no hint.
            _ => return Vec::new(),
        };
        if title.contains('\n') {
            return Vec::new();
        }
        vec![("X-hypodj-hint-kind", kind.to_string()), ("X-hypodj-hint-title", title)]
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
        self.play_index_from_silence(idx).await?;

        // (4) Sub-JND ramp silence -> saved comfort level (startle-safe by
        // construction; WakeTo resolves sub_jnd=true / SetBaseline).
        let dur = self.clamp_fade_dur(Duration::from_secs(self.fade_cfg.wake_ramp_secs));
        let intent = FadeIntent::WakeTo {
            target_db: mpv_volume_to_db(comfort_vol as f64),
            vol: comfort_vol,
        };
        self.start_fade_spec(FadeRequest { intent, dur, commit_logical: None })
            .await
            .map_err(|e| e.to_string())
    }

    /// Resolve a plan [`Selector`] to concrete songs and APPEND them (append-only,
    /// count-clamped). `Similar`/`Calmer` (P4) resolve via the gated similar-tracks
    /// call and degrade gracefully (similar -> seed genre -> random, never an error
    /// on a plain-Subsonic backend); `Calmer` additionally re-ranks the pool by the
    /// injected [`FeatureStore`] energy. Used by the executor's `Enqueue` action;
    /// touches the network, never a test path.
    pub async fn plan_enqueue(&self, selector: &Selector, count: u32) -> Result<usize, String> {
        let want = count as usize;
        let songs: Vec<Song> = match selector {
            Selector::Query(q) => {
                // A free-text mood/multi-word ask ("some chill electronic stuff") is
                // NOT a literal title/artist; Subsonic search3 is whole-string/token
                // full-text, so passing the whole phrase almost never matches any
                // library title/artist/album and resolves 0 songs. Mirror the
                // client-side grounding shape (see `query_content_keywords`): strip
                // stopwords/filler, split into content keywords, run search3 PER
                // keyword, then OR-merge (dedup + relevance-order). This recovers real
                // tracks ("chill"/"electronic" hits) that the glued phrase never would.
                let keywords = query_content_keywords(q);
                if keywords.include.is_empty() {
                    // No wanted content keywords (e.g. an explicit title that is all
                    // filler, or an empty ask) - fall back to the literal whole-phrase
                    // search so an exact title/artist ask still resolves as before.
                    let hits = self.client.search3(q).await.map_err(|e| e.to_string())?;
                    hits.songs.into_iter().take(want).collect()
                } else {
                    // Per-keyword search, each capped so a huge library cannot blow up.
                    // Track the FIRST error: a search3 failure must NOT be silently
                    // swallowed into a fake "no matches" zero (an infra outage would
                    // read as an honest empty library). We only surface real matches,
                    // but if the merge comes back empty AND a search errored we cannot
                    // honestly claim zero - we propagate the error instead.
                    let mut per_keyword: Vec<Vec<Song>> = Vec::with_capacity(keywords.include.len());
                    let mut first_err: Option<String> = None;
                    for kw in &keywords.include {
                        match self.client.search3(kw).await {
                            Ok(hits) => per_keyword
                                .push(hits.songs.into_iter().take(QUERY_KEYWORD_SONG_CAP).collect()),
                            Err(e) => {
                                if first_err.is_none() {
                                    first_err = Some(e.to_string());
                                }
                            }
                        }
                    }
                    // Collect the song ids of every EXCLUDED term so a "not chill" ask
                    // never enqueues the very tracks the user rejected. A failed exclude
                    // search must ABORT (propagate) - filtering silently on partial data
                    // could still slip a rejected song through, so we refuse to guess.
                    let mut excluded_ids: std::collections::HashSet<SongId> =
                        std::collections::HashSet::new();
                    for kw in &keywords.exclude {
                        let hits = self.client.search3(kw).await.map_err(|e| e.to_string())?;
                        for s in hits.songs.into_iter().take(QUERY_KEYWORD_SONG_CAP) {
                            excluded_ids.insert(s.id);
                        }
                    }
                    // OR-merge everything, drop rejected songs, THEN take the requested
                    // count (filtering before truncation so exclusions do not starve the
                    // result). A GENUINE 0 stays 0 honestly - the fix-1 result line
                    // surfaces "added 0 - no matches"; NEVER fabricate random songs.
                    let mut merged = merge_keyword_hits(per_keyword, usize::MAX);
                    merged.retain(|s| !excluded_ids.contains(&s.id));
                    merged.truncate(want);
                    // An empty merge with an in-flight search error is NOT an honest zero
                    // - report the failure rather than fabricating a false no-match.
                    if merged.is_empty() {
                        if let Some(e) = first_err {
                            return Err(e);
                        }
                    }
                    merged
                }
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
            Selector::Similar(id) => {
                // Similar tracks (sonic if the backend advertises it, else
                // getSimilarSongs2), degrading gracefully all the way down to a
                // genre pick and then random - NEVER an error on a plain-Subsonic
                // backend that lacks the endpoint.
                let seed = self.client.song(id).await.map_err(|e| e.to_string())?;
                let mut songs = self.client.similar(id, Some(want as i32)).await.unwrap_or_default();
                if songs.is_empty() {
                    if let Some(g) = &seed.genre {
                        songs = self.client.songs_by_genre(g).await.map_err(|e| e.to_string())?;
                    }
                }
                if songs.is_empty() {
                    songs = self
                        .client
                        .random_songs(Some(want as i32))
                        .await
                        .map_err(|e| e.to_string())?;
                }
                // "More like this" must not re-enqueue the seed itself.
                songs.retain(|s| &s.id != id);
                songs.truncate(want);
                songs
            }
            Selector::SimilarToCurrent => {
                // The MODEL emitted no id (off-surface-id boundary): fill the seed
                // server-side from the current (or first-queued) track. Nothing to
                // seed from -> honest 0 (empty vec), NEVER a fabricated pick.
                match self.similar_seed_id() {
                    Some(id) => {
                        // Same graceful degrade as Selector::Similar: sonic/similar,
                        // then the seed's genre, then random - never an error on a
                        // plain-Subsonic backend that lacks the endpoint.
                        let seed = self.client.song(&id).await.map_err(|e| e.to_string())?;
                        let mut songs =
                            self.client.similar(&id, Some(want as i32)).await.unwrap_or_default();
                        if songs.is_empty() {
                            if let Some(g) = &seed.genre {
                                songs =
                                    self.client.songs_by_genre(g).await.map_err(|e| e.to_string())?;
                            }
                        }
                        if songs.is_empty() {
                            songs = self
                                .client
                                .random_songs(Some(want as i32))
                                .await
                                .map_err(|e| e.to_string())?;
                        }
                        // Never re-enqueue the seed (the track already playing/queued).
                        songs.retain(|s| s.id != id);
                        songs.truncate(want);
                        songs
                    }
                    None => Vec::new(),
                }
            }
            Selector::Calmer(id) => {
                // Over-fetch the candidate pool (2x) so the calmer half can still
                // fill `count`; same graceful genre/random fallback as Similar.
                let seed = self.client.song(id).await.map_err(|e| e.to_string())?;
                let mut pool = self
                    .client
                    .similar(id, Some((want * 2) as i32))
                    .await
                    .unwrap_or_default();
                if pool.is_empty() {
                    if let Some(g) = &seed.genre {
                        pool = self.client.songs_by_genre(g).await.map_err(|e| e.to_string())?;
                    }
                }
                if pool.is_empty() {
                    pool = self
                        .client
                        .random_songs(Some((want * 2) as i32))
                        .await
                        .map_err(|e| e.to_string())?;
                }
                // "Something calmer" must not re-enqueue the seed itself.
                pool.retain(|s| &s.id != id);
                calmer_rerank(self.store.as_ref(), &seed, pool, want)
            }
        };
        // THE REWEIGHT HOOK: the single seam P4 HEURISTIC candidate selection
        // funnels through. It ALSO owns setting the latent-field pull, because a pull
        // must be registered EXACTLY when a mood enqueue is COMMITTED (this method runs
        // only after `nl confirm`/arm), never speculatively before confirmation - so a
        // rejected or non-enqueue ask can never leave a lingering bias behind. It ONLY
        // reorders candidate ranking + edits the belief field; it never touches the
        // queue, never arms.
        //
        // Per selector:
        // - A MOOD `Query` ("calmer tracks") carries a lexicon direction word: it
        //   SEEDS a lingering pull from its own text AND biases THIS enqueue toward it
        //   (the cc translator resolves a mood/comparative ask to `Query`, so this is
        //   the seam that makes the pull bias the very enqueue it was primed for). An
        //   EXPLICIT `Query` (a title/artist, no direction word) yields no pull and is
        //   left byte-identical - an explicit ordered list is NEVER silently reordered,
        //   and a lingering pull from an earlier ask never reorders it either.
        // - `Calmer` seeds a "calmer" pull (its intrinsic direction) then biases THIS
        //   enqueue - the daemon-rules mood path's parity with the cc `Query` path.
        // - `Similar`/`Radio` carry no intrinsic direction (they seed nothing) but are
        //   NON-DETERMINISTIC pools, so an already-active lingering pull may reorder
        //   WHICH picks lead the append (harmless + byte-identical when no pull is set).
        // - `Exact`/`Genre` name a definite user/plan-specified ordered list; a live
        //   pull must NEVER silently reorder that observable list, so they are left as-is.
        let songs = match selector {
            Selector::Query(q) => match lexicon_pull(q, LEXICON_PULL_STRENGTH, Instant::now()) {
                Some(pull) => {
                    self.pulls.lock().unwrap().add(pull, Instant::now());
                    self.pull_rerank(songs)
                }
                None => songs,
            },
            Selector::Calmer(_) => {
                let pull = Pull::new("calmer", vec![-1.0, 0.0], LEXICON_PULL_STRENGTH, Instant::now());
                self.pulls.lock().unwrap().add(pull, Instant::now());
                self.pull_rerank(songs)
            }
            Selector::Similar(_) | Selector::SimilarToCurrent | Selector::Radio => {
                self.pull_rerank(songs)
            }
            Selector::Exact(_) | Selector::Genre(_) => songs,
        };
        let n = songs.len();
        for s in songs {
            self.enqueue_song(s).await;
        }
        Ok(n)
    }

    /// Play a specific LIBRARY song NOW: resolve `selector` from the library, APPEND
    /// it (via [`Self::plan_enqueue`], which stays literally append-only), then START
    /// playback on the first newly-appended track. This is the enqueue-then-start
    /// path behind [`crate::plan::Action::PlayNow`] - the honest "play X now" that
    /// [`crate::plan::Action::Enqueue`] (append-only) and [`crate::plan::Action::Play`]
    /// (in-queue jump only) could not express. Non-destructive: it only appends and
    /// starts, never deletes. Returns the number of tracks enqueued.
    pub async fn plan_play_now(&self, selector: &Selector, count: u32) -> Result<usize, String> {
        // Index the first newly-appended track will land at. std Mutex dropped
        // immediately, never held across the await below.
        let start_idx = self.state.lock().unwrap().queue.len();
        let n = self.plan_enqueue(selector, count).await?;
        if n > 0 {
            // Jump to (and start) the just-enqueued track. When a track was already
            // playing this interrupts it (the intended "play X now"); on a stopped
            // deck it simply starts.
            self.play_index(start_idx).await?;
        }
        Ok(n)
    }

    /// Snapshot the currently-playing library [`Song`] (a clone), or `None` when
    /// nothing is playing or the current entry is a stream. Short lock scope, no
    /// `.await` held.
    fn current_song(&self) -> Option<Song> {
        let st = self.state.lock().unwrap();
        match st.current.and_then(|i| st.queue.get(i)) {
            Some(item) => match &item.entry {
                QueueEntry::Song(s) => Some(s.clone()),
                QueueEntry::Stream { .. } => None,
            },
            None => None,
        }
    }

    /// Resolve the one seed to "more like what is playing" from, in strict
    /// preference order (the branch is captured in [`SeedSource::kind`]):
    ///
    /// 1. the CURRENT playing song (what is playing wins over everything);
    /// 2. else the FRESH-GESTURE tail - LATEST GESTURE WINS: when a fresh idle
    ///    enqueue armed [`State::fresh_enqueue_anchor`], seed from the first library
    ///    Song AT OR AFTER the anchor position, SKIPPING the already-played track
    ///    consume-off leaves LINGERING at the queue head. This sits ABOVE recency so
    ///    a fresh enqueue of new music moves the seed to that music rather than the
    ///    just-finished track (scenario G); only reached while idle (NowPlaying
    ///    handled `current` above). A dangling anchor finds no position and falls
    ///    through to recency (self-healing);
    /// 3. else the RECENTLY-FINISHED song ([`State::last_finished`]) - the track
    ///    that just ended is the most pertinent recency seed when nothing is
    ///    playing and no fresh gesture followed the finish (scenario R: a track
    ///    finished, then the user asks "more like this one" - they meant the one
    ///    that was just playing, not an unrelated queued song). This also covers the
    ///    stream-as-current edge: a live stream has no library id, so the
    ///    current-song branch yields `None` and we fall to the last real finished
    ///    track rather than an unrelated first-queued one;
    /// 4. else the FIRST queued Song (nothing has played yet - seed from what is up);
    /// 5. else `None` - the caller then yields an HONEST 0, never a fabricated pick.
    ///
    /// Streams have no library id and are skipped at every level. The std `Mutex` is
    /// read then dropped here, NEVER held across the later `.await`.
    ///
    /// This is the SINGLE resolution of the seed ordering: both the enqueue seed
    /// ([`Self::similar_seed_id`]) and the ambient hint ([`Self::ambient_hint_pairs`])
    /// read it, so the hint can never name a seed the DJ would not enqueue from. The
    /// resolved [`SeedSource`] also carries the branch it came from and the seed
    /// title, captured under the lock so a synchronous render can name it without an
    /// `.await`.
    fn seed_source(&self) -> Option<SeedSource> {
        let st = self.state.lock().unwrap();
        // 1. the CURRENT playing library song (a live stream has no library id, so
        //    the current-song branch yields None and we fall through to recency).
        if let Some(item) = st.current.and_then(|i| st.queue.get(i)) {
            if let QueueEntry::Song(s) = &item.entry {
                return Some(SeedSource {
                    kind: SeedKind::NowPlaying,
                    id: s.id.clone(),
                    title: s.title.clone(),
                });
            }
        }
        // 2. FreshGesture (LATEST-GESTURE-WINS) - a fresh idle enqueue anchored the
        //    newly appended music; seed from the first library Song AT OR AFTER the
        //    anchor position, SKIPPING the already-played lingering head consume-off
        //    leaves at the front. Consulted ONLY while idle: branch 1 already returned
        //    for a current LIBRARY song, but a current STREAM (no library id) falls
        //    through it, so this guards `current.is_none()` explicitly - a stream that
        //    is playing must fall to the recency seed (branch 3), not an anchored tail
        //    (the reset discipline already clears the anchor the instant any track
        //    becomes current, so this guard is defense in depth). Self-heals: a dangling
        //    anchor finds no position and falls through to recency.
        if st.current.is_none() {
            if let Some(anchor) = st.fresh_enqueue_anchor {
                if let Some(pos) = st.queue.iter().position(|it| it.id == anchor) {
                    if let Some(s) = st.queue[pos..].iter().find_map(|it| match &it.entry {
                        QueueEntry::Song(s) => Some(s),
                        QueueEntry::Stream { .. } => None,
                    }) {
                        return Some(SeedSource {
                            kind: SeedKind::UpNext,
                            id: s.id.clone(),
                            title: s.title.clone(),
                        });
                    }
                }
            }
        }
        // 3. else the RECENTLY-FINISHED song (the recency seed).
        if let Some(s) = st.last_finished.as_ref() {
            return Some(SeedSource {
                kind: SeedKind::JustFinished,
                id: s.id.clone(),
                title: s.title.clone(),
            });
        }
        // 4. else the FIRST queued library song (nothing has played yet).
        st.queue.iter().find_map(|it| match &it.entry {
            QueueEntry::Song(s) => Some(SeedSource {
                kind: SeedKind::UpNext,
                id: s.id.clone(),
                title: s.title.clone(),
            }),
            QueueEntry::Stream { .. } => None,
        })
    }

    /// The library [`SongId`] to seed a "more like what is playing" enqueue from, in
    /// the strict preference order documented on [`Self::seed_source`]. A thin
    /// projection of that single resolution to just the id - behaviorally identical
    /// to the pre-refactor ladder for a given state.
    fn similar_seed_id(&self) -> Option<SongId> {
        self.seed_source().map(|s| s.id)
    }

    /// Arm the fresh-gesture anchor ([`State::fresh_enqueue_anchor`]) at
    /// `first_qid` - the MPD id of the FIRST song a fresh idle enqueue just appended -
    /// when the enqueue landed while the deck is stopped/idle (`current` is `None`).
    /// LATEST GESTURE WINS: a just-finished track A lingering at the queue head is the
    /// pertinent seed only until the user makes a newer gesture; enqueuing a fresh
    /// selection D is more recent than A, so the seed - and the ambient hint that
    /// re-surfaces it - must move to D rather than keep naming A (a hint that says
    /// "just finished A" right after the user loaded D is exactly the staleness this
    /// feature exists to avoid). Unlike the prior clear-last_finished approach we KEEP
    /// `last_finished` (the honest recency memory that survives consume eviction) and
    /// instead RECORD the fresh gesture as an anchor, so [`Self::seed_source`] branch 2
    /// seeds from the freshly-appended tail past the lingering head. Each arm
    /// OVERWRITES the anchor, so the newest gesture always wins. A no-op while a track
    /// is current (the current-song branch wins the seed there anyway).
    fn arm_fresh_enqueue_anchor(&self, first_qid: u64) {
        let mut st = self.state.lock().unwrap();
        if st.current.is_none() {
            st.fresh_enqueue_anchor = Some(first_qid);
        }
    }

    /// Arm the fresh-gesture anchor after an APPEND-ONLY [`Action::Enqueue`] landed
    /// fresh music on an idle deck. `first_qid` is the id the first appended song took
    /// (the caller snapshots `next_id` BEFORE the append); `n` is the count actually
    /// appended: an honest 0 (no library match) is a no-op so no stale anchor is set
    /// without any fresh music behind it, mirroring the empty-list no-op of
    /// [`Self::enqueue_songs`]. The idle-gating lives in
    /// [`Self::arm_fresh_enqueue_anchor`] (a no-op while a track is current). This is
    /// the ONE seam scoped to the append-only Enqueue action: [`Self::plan_enqueue`]
    /// is shared with the autoplay `PlayNow`/`wake` paths (which start playback and let
    /// the now-current track win the seed), so the arm lives HERE rather than inside
    /// `plan_enqueue` or the shared [`Self::enqueue_song`] helper - those stay unarmed.
    fn arm_fresh_enqueue_anchor_on_append(&self, first_qid: u64, n: usize) {
        if n > 0 {
            self.arm_fresh_enqueue_anchor(first_qid);
        }
    }

    /// Apply the active latent-field pulls to a resolved candidate list, biasing the
    /// ranking toward the pulled direction relative to the current track (or a neutral
    /// center when nothing is playing). Returns the list UNCHANGED when no pull is
    /// active - the "degrades to today exactly" guarantee. Pure work on cloned values;
    /// the `pulls` lock is dropped before the reweight.
    fn pull_rerank(&self, songs: Vec<Song>) -> Vec<Song> {
        let now = Instant::now();
        let field = {
            let mut f = self.pulls.lock().unwrap();
            f.prune(now);
            if !f.is_active(now) {
                return songs;
            }
            f.clone()
        };
        // reference_features = the current track, or a neutral center (0.5, 0.5).
        let reference = self
            .current_song()
            .and_then(|s| self.store.features(&s))
            .unwrap_or(TrackFeatures { energy: 0.5, valence: 0.5, embedding: None });
        pull_reweight(&field, now, self.store.as_ref(), &reference, songs)
    }

    /// Dispatch a parsed `field` command: SET a pull, SEE the field, one-nudge
    /// correct, or clear. All non-destructive - it only reads/edits the belief list.
    fn handle_field(&self, cmd: FieldCmd) -> MpdResponse {
        let now = Instant::now();
        match cmd {
            FieldCmd::Status => {
                let lines = {
                    let mut f = self.pulls.lock().unwrap();
                    f.prune(now);
                    f.describe(now)
                };
                if lines.is_empty() {
                    return MpdResponse::pairs().pair("field", "no pulls active").build();
                }
                let mut b = MpdResponse::pairs();
                for line in lines {
                    b = b.pair("pull", line);
                }
                b.build()
            }
            FieldCmd::Set(words) => match lexicon_pull(&words, LEXICON_PULL_STRENGTH, now) {
                Some(pull) => {
                    let label = pull.label.clone();
                    self.pulls.lock().unwrap().add(pull, now);
                    MpdResponse::pairs().pair("pull_set", label).build()
                }
                // The honest echo the design mandates: never "not understood".
                None => MpdResponse::pairs()
                    .pair("field", format!("no pull felt from '{}' - say more?", words.trim()))
                    .build(),
            },
            FieldCmd::Nudge(dir) => {
                let factor = match dir {
                    FieldNudge::Less => 0.5,
                    FieldNudge::More => 1.5,
                };
                match self.pulls.lock().unwrap().nudge_recent(factor, now) {
                    Some(label) => MpdResponse::pairs().pair("pull_nudged", label).build(),
                    None => MpdResponse::pairs().pair("field", "no pulls active").build(),
                }
            }
            FieldCmd::Clear => {
                self.pulls.lock().unwrap().clear();
                MpdResponse::ok()
            }
        }
    }

    /// The searchable text for one queue entry (title + artist + album), used by a
    /// [`QueueSelector::QueryMatch`]. Pure over the entry.
    fn item_search_text(entry: &QueueEntry) -> String {
        match entry {
            QueueEntry::Song(s) => {
                let mut t = s.title.clone();
                if let Some(a) = &s.artist {
                    t.push(' ');
                    t.push_str(a);
                }
                if let Some(al) = &s.album {
                    t.push(' ');
                    t.push_str(al);
                }
                t
            }
            QueueEntry::Stream { title, .. } => title.clone(),
        }
    }

    /// Snapshot the per-entry search text + current index under the lock, so the
    /// pure [`crate::plan::resolve_selector`] can resolve a selector without holding
    /// the lock across the match.
    fn queue_texts(&self) -> (Vec<String>, Option<usize>) {
        let st = self.state.lock().unwrap();
        let texts = st.queue.iter().map(|it| Self::item_search_text(&it.entry)).collect();
        (texts, st.current)
    }

    /// DETERMINISTIC queue-edit executor for the confirmed [`Action::Remove`] /
    /// [`Action::Move`] / [`Action::Clear`] / [`Action::Play`] plan actions. The
    /// selector resolves against the LIVE queue here (never pre-baked to indices),
    /// so a NO-MATCH is a clean no-op - never a wrong-target delete. Returns the
    /// number of entries affected (0 = clean no-op). Preserves the current-track
    /// identity across a rebuild by tracking its stable id.
    pub async fn plan_queue_edit(&self, action: &Action) -> Result<usize, String> {
        match action {
            Action::Remove { sel } => {
                let (texts, current) = self.queue_texts();
                let idxs = crate::plan::resolve_selector(sel, &texts, current);
                Ok(self.remove_indices(&idxs).await)
            }
            Action::Clear { scope } => match scope {
                crate::plan::ClearScope::All => {
                    let n = self.state.lock().unwrap().queue.len();
                    self.handle(MpdCommand::Clear).await;
                    Ok(n)
                }
                crate::plan::ClearScope::AfterCurrent => {
                    let idxs: Vec<usize> = {
                        let st = self.state.lock().unwrap();
                        match st.current {
                            Some(c) => ((c + 1)..st.queue.len()).collect(),
                            // Nothing is playing: there is no "after current", so this
                            // is a clean no-op rather than a surprise clear-all.
                            None => Vec::new(),
                        }
                    };
                    Ok(self.remove_indices(&idxs).await)
                }
                crate::plan::ClearScope::Range { start, end } => {
                    let (texts, current) = self.queue_texts();
                    let idxs = crate::plan::resolve_selector(
                        &crate::plan::QueueSelector::Range { start: *start, end: *end },
                        &texts,
                        current,
                    );
                    Ok(self.remove_indices(&idxs).await)
                }
            },
            Action::Move { sel, dest } => {
                let (texts, current) = self.queue_texts();
                let idxs = crate::plan::resolve_selector(sel, &texts, current);
                Ok(self.move_indices(&idxs, *dest).await)
            }
            Action::Play { sel } => {
                let (texts, current) = self.queue_texts();
                let idxs = crate::plan::resolve_selector(sel, &texts, current);
                match idxs.first() {
                    Some(&idx) => {
                        self.play_index(idx).await?;
                        Ok(1)
                    }
                    None => Ok(0),
                }
            }
            Action::Noop => Ok(0),
            other => Err(format!("not a queue-edit action: {other:?}")),
        }
    }

    /// Remove the 0-based `idxs` from the queue (descending, so the earlier indices
    /// stay valid), fixing up the current index by the stable id of the current
    /// track. If the currently-PLAYING entry is removed, playback is stopped (never
    /// left dangling on a gone track). Returns the count removed.
    async fn remove_indices(&self, idxs: &[usize]) -> usize {
        if idxs.is_empty() {
            return 0;
        }
        let mut sorted: Vec<usize> = idxs.to_vec();
        sorted.sort_unstable();
        sorted.dedup();
        let (removed, current_gone) = {
            let mut st = self.state.lock().unwrap();
            let len = st.queue.len();
            let cur_id = st.current.and_then(|c| st.queue.get(c).map(|it| it.id));
            let mut removed = 0usize;
            for &i in sorted.iter().rev() {
                if i < st.queue.len() && i < len {
                    st.queue.remove(i);
                    removed += 1;
                }
            }
            if removed > 0 {
                st.playlist_version += 1;
                // Re-anchor current by the tracked id; None if it was removed.
                st.current = cur_id.and_then(|id| st.queue.iter().position(|it| it.id == id));
            }
            (removed, removed > 0 && cur_id.is_some() && st.current.is_none())
        };
        if current_gone {
            // The playing entry itself was removed: stop rather than leave the player
            // running a track no longer in the queue.
            let _ = self.player.stop().await;
        }
        if removed > 0 {
            self.notify_change();
        }
        removed
    }

    /// Move the 0-based `idxs` (order preserved) to `dest`, tracking the current
    /// track's id so playback never jumps to a neighbour. Returns the count moved.
    async fn move_indices(&self, idxs: &[usize], dest: crate::plan::MoveDest) -> usize {
        if idxs.is_empty() {
            return 0;
        }
        let mut sorted: Vec<usize> = idxs.to_vec();
        sorted.sort_unstable();
        sorted.dedup();
        let moved = {
            let mut st = self.state.lock().unwrap();
            let len = st.queue.len();
            if sorted.iter().any(|&i| i >= len) {
                return 0;
            }
            let cur_id = st.current.and_then(|c| st.queue.get(c).map(|it| it.id));
            // Extract the moving items in order; keep the remainder.
            let selset: std::collections::HashSet<usize> = sorted.iter().copied().collect();
            let mut moving: Vec<QueueItem> = Vec::with_capacity(sorted.len());
            let mut rest: Vec<QueueItem> = Vec::with_capacity(len - sorted.len());
            for (i, it) in st.queue.drain(..).enumerate() {
                if selset.contains(&i) {
                    moving.push(it);
                } else {
                    rest.push(it);
                }
            }
            // Compute the insertion point among the REMAINING items.
            let insert_at = match dest {
                crate::plan::MoveDest::Position(p) => p.saturating_sub(1).min(rest.len()),
                crate::plan::MoveDest::Relative(d) => {
                    // Relative to where the current track sits among the remainder.
                    let base = cur_id
                        .and_then(|id| rest.iter().position(|it| it.id == id))
                        .unwrap_or(0) as i64;
                    (base + d as i64).clamp(0, rest.len() as i64) as usize
                }
            };
            let moved = moving.len();
            let mut out = rest;
            for (k, it) in moving.into_iter().enumerate() {
                out.insert(insert_at + k, it);
            }
            st.queue = out;
            st.playlist_version += 1;
            st.current = cur_id.and_then(|id| st.queue.iter().position(|it| it.id == id));
            moved
        };
        if moved > 0 {
            self.notify_change();
        }
        moved
    }

    /// Dispatch a parsed `plan` MPD command to the registry, mapping a
    /// [`PlanError`] 1:1 to a fail-loud ACK. Sync (registry ops never `.await`).
    async fn handle_plan(&self, cmd: PlanCmd) -> MpdResponse {
        match cmd {
            // An Immediate `plan add` executes inline and reports the REAL outcome as
            // a `result` pair (the client shows exactly what happened - "added 0 - no
            // matches for X" on a mood that resolves to nothing, "added N" / "played
            // X" otherwise). A deferred (armed) plan carries no outcome yet.
            PlanCmd::Add(raw) => match self.plan_add_reporting(raw).await {
                Ok((id, outcome)) => {
                    let mut b = MpdResponse::pairs().pair("plan_id", id.0.to_string());
                    if let Some(o) = outcome {
                        b = b.pair("result", o.render());
                    }
                    b.build()
                }
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
            // An alarm is a LOCAL-time promise: `nl "wake me at 7"` means 07:00 in
            // the SYSTEM zone, same as the direct `wake at 7` command (which uses
            // chrono::Local in resolve_next_civil). Use the current local UTC offset
            // so both NL and direct surfaces resolve the same local instant and the
            // echo prints local time. A full IANA zone (DST transitions between now
            // and the wake) is a P4 refinement; the current offset is exact for the
            // civil->UTC reduction at translate time.
            tz: *chrono::Local::now().offset(),
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

    /// Store the LIVE ICY metadata for the raw stream identified by `queue_id`
    /// (delivered on the lossless `PlayerEvent::StreamMetadata` spine), then wake
    /// idling clients so ncmpcpp / dj-gui re-read `currentsong` and see the fresh
    /// station Name / now-playing Title. The `std` state lock is held ONLY for the
    /// field write and dropped BEFORE `notify_change`, so it is never held across an
    /// await (the Mutex-never-across-await invariant); State is mutated through
    /// interior mutability (`&self`), never `&mut self`. Keyed by the latched
    /// identity so the slot can only ever decorate the entry it came from.
    pub(crate) fn set_stream_meta(&self, queue_id: QueueId, name: Option<String>, title: Option<String>) {
        {
            let mut st = self.state.lock().unwrap();
            st.stream_meta = Some((queue_id, StreamMeta { name, title }));
        }
        self.notify_change();
    }

    /// Store the recognized Shazam cover-art URL for the raw stream identified by
    /// `queue_id` (task f7vnd3i), then wake idling clients so the dj-gui art pane can
    /// re-read the `X-CoverArt` currentsong field. Keyed by the latched identity so
    /// the slot can only ever decorate the entry it came from. The std lock is held
    /// ONLY for the field write and dropped BEFORE `notify_change` (never across an
    /// await); mutated through `&self` interior mutability, never `&mut self`.
    pub(crate) fn set_recognized_cover(&self, queue_id: QueueId, url: String) {
        {
            let mut st = self.state.lock().unwrap();
            st.recognized_cover = Some((queue_id, url));
        }
        self.notify_change();
    }

    /// Drop any stored live stream metadata that does NOT belong to `keep`, called on
    /// every play edge / stop so a station's Name/Title can never linger onto the next
    /// track. Passing `Some(qid)` keeps the slot when it matches (a mid-stream ICY
    /// title change on the SAME entry survives); passing `None` clears unconditionally
    /// (stop / end of queue). A pure field write under a brief lock, no await held.
    pub(crate) fn clear_stream_meta_except(&self, keep: Option<QueueId>) {
        let mut st = self.state.lock().unwrap();
        if let Some((qid, _)) = &st.stream_meta {
            if keep != Some(*qid) {
                st.stream_meta = None;
            }
        }
        // Drop the recognized cover on the same edge (task f7vnd3i) so a Shazam
        // cover from a prior stream can never linger onto the next entry.
        if let Some((qid, _)) = &st.recognized_cover {
            if keep != Some(*qid) {
                st.recognized_cover = None;
            }
        }
    }

    /// ON-DEMAND now-playing RECOGNITION for the current raw stream (task f7vnd3i).
    ///
    /// The gap: sibling jmrwr99 surfaces a stream's ICY tags, but some real streams
    /// (the NTS mixtapes) carry NO ICY, so the now-playing text must come from
    /// OUTSIDE the stream. This captures a short SIDE-BAND clip of the SAME stream
    /// URL and fingerprints it with `songrec` (open-source Shazam), then surfaces the
    /// recognized artist / title into the exact same `Name`/`Title` path ICY rides
    /// (via [`Self::set_stream_meta`]) and the cover URL toward the dj-gui art pane
    /// (via the qid-gated `recognized_cover` slot -> currentsong `X-CoverArt`).
    ///
    /// LOCK/ASYNC DISCIPLINE: the stream URL + qid are read under ONE short std lock
    /// which is DROPPED before the recognition await (the Mutex-never-across-await
    /// invariant); the heavy capture + subprocess work happens off the reactor in
    /// [`crate::recognize::recognize_stream_url`]. An in-flight [`AtomicBool`] guard
    /// (reset by RAII) debounces rapid repeats to one recognition at a time.
    ///
    /// Only a raw [`QueueEntry::Stream`] is recognized: a library song already
    /// carries metadata, and nothing playing has nothing to identify - both
    /// short-circuit WITHOUT capturing. On a hit the qid is RE-CHECKED before
    /// surfacing so a late result only decorates the entry it came from (mirroring
    /// the currentsong qid gate); a no-match leaves any prior stream_meta untouched.
    async fn identify(&self) -> MpdResponse {
        // Debounce: one recognition at a time (protects the Shazam endpoint).
        if self.recognizing.swap(true, Ordering::AcqRel) {
            return MpdResponse::pairs()
                .pair("identify", "already identifying")
                .build();
        }
        // RAII: reset the in-flight flag on EVERY exit path (hit / miss / error / early
        // return), so a failed or short-circuited identify can never wedge the guard.
        let _guard = RecognizingGuard(&self.recognizing);

        // Read the current entry's stream URL + latched qid under ONE lock, then DROP
        // the lock before any await. A library song / nothing-playing short-circuits.
        enum Target {
            Stream(QueueId, String),
            LibrarySong,
            Nothing,
        }
        let target = {
            let st = self.state.lock().unwrap();
            match st.reported_current().and_then(|i| st.queue.get(i)) {
                Some(item) => match &item.entry {
                    QueueEntry::Stream { url, .. } => Target::Stream(QueueId(item.id), url.clone()),
                    QueueEntry::Song(_) => Target::LibrarySong,
                },
                None => Target::Nothing,
            }
        };
        let (qid, url) = match target {
            Target::Stream(qid, url) => (qid, url),
            Target::LibrarySong => {
                return MpdResponse::pairs()
                    .pair("identify", "current track is already known")
                    .build();
            }
            Target::Nothing => {
                return MpdResponse::pairs().pair("identify", "nothing playing").build();
            }
        };

        // Heavy work off the reactor; no std lock is held across this await.
        let track = match crate::recognize::recognize_stream_url(url).await {
            Ok(Some(track)) => track,
            Ok(None) => {
                // Clean no-match: leave any prior ICY stream_meta untouched.
                return MpdResponse::pairs().pair("identify", "no match").build();
            }
            Err(e) => return ack(ACK_ERROR_UNKNOWN, "identify", &e.to_string()),
        };

        let now_playing = crate::recognize::now_playing_title(&track);

        // Re-check the qid + preserve any existing ICY station Name under ONE lock, so
        // a track that advanced during the capture is NOT clobbered by a stale result.
        let existing_name = {
            let st = self.state.lock().unwrap();
            let still_current = st
                .reported_current()
                .and_then(|i| st.queue.get(i))
                .is_some_and(|it| QueueId(it.id) == qid);
            if !still_current {
                // The stream advanced away from what we captured: report the hit but
                // do NOT decorate the now-different entry.
                return identify_hit_response(&track, now_playing.as_deref());
            }
            st.stream_meta
                .as_ref()
                .and_then(|(q, m)| (*q == qid).then(|| m.name.clone()))
                .flatten()
        };

        // Surface the recognized cover toward the dj-gui pane (qid-gated slot).
        if let Some(url) = &track.cover_url {
            self.set_recognized_cover(qid, url.clone());
        }
        // Surface artist/title into the same Name/Title path as ICY. Preserve the
        // station Name if one was already latched; the recognized now-playing rides
        // Title. set_stream_meta wakes idling clients (notify_change).
        self.set_stream_meta(qid, existing_name, now_playing.clone());

        identify_hit_response(&track, now_playing.as_deref())
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

    /// The Subsonic song ids of the CURRENT QUEUE, in queue order. Raw
    /// [`QueueEntry::Stream`] entries have no song id and are skipped (a Navidrome
    /// playlist can only hold library tracks). Backs `save <name>`.
    fn queue_song_ids(&self) -> Vec<SongId> {
        let st = self.state.lock().unwrap();
        st.queue
            .iter()
            .filter_map(|item| match &item.entry {
                QueueEntry::Song(s) => Some(s.id.clone()),
                QueueEntry::Stream { .. } => None,
            })
            .collect()
    }

    /// The default label for saving `url` as an internet radio station: the LIVE
    /// icy-name of the currently-playing stream when it is THIS url, else the url
    /// itself (the NTS-mixtape no-ICY case). Reads State ONCE under the std lock and
    /// resolves into an OWNED String before returning, so the caller never holds the
    /// lock across the create await (Mutex-never-across-await). The qid-gated pure
    /// [`resolve_station_name`] owns the fallback chain so it is unit-testable
    /// without a lock or a network.
    fn default_station_name(&self, url: &str) -> String {
        let st = self.state.lock().unwrap();
        let current = st.reported_current().and_then(|idx| st.queue.get(idx));
        resolve_station_name(url, current, st.stream_meta.as_ref())
    }

    /// Append one song to a REAL named Navidrome playlist, create-or-append: if a
    /// playlist with `name` already exists, `updatePlaylist` adds the song to it;
    /// otherwise `createPlaylist` mints a new one seeded with the song. Backs the
    /// non-`Starred` `playlistadd <name> <uri>` path (GAP cusq3zaw). Any Subsonic
    /// error surfaces to the caller so it becomes a proper ACK, never a silent
    /// success.
    async fn playlist_add_song(&self, name: &str, id: SongId) -> Result<(), SubsonicError> {
        let existing = self
            .client
            .get_playlists()
            .await?
            .into_iter()
            .find(|p| p.name == name);
        match existing {
            Some(p) => self.client.add_to_playlist(&p.id, &[id]).await,
            None => self.client.create_playlist(name, &[id]).await.map(|_| ()),
        }
    }

    /// Fetch the starred songs and record their order under the state lock, so a
    /// later position-based `playlistdelete Starred <pos>` maps back to a song id.
    /// Shared by `listplaylistinfo Starred` and `load Starred` (which must agree on
    /// the exact order they present). Starred is NEVER cached (freshness-critical).
    async fn starred_songs_recording_order(&self) -> Result<Vec<Song>, SubsonicError> {
        let songs = self.client.starred_songs().await?;
        let mut st = self.state.lock().unwrap();
        st.last_starred_order = songs.iter().map(|s| s.id.clone()).collect();
        Ok(songs)
    }

    /// Resolve a real Navidrome playlist by NAME to its full song list, or `None`
    /// when no playlist carries that name. Backs `listplaylistinfo <name>` and
    /// `load <name>` so a `save`d set is inspectable and loadable by name.
    async fn playlist_by_name(&self, name: &str) -> Result<Option<Playlist>, SubsonicError> {
        let existing = self
            .client
            .get_playlists()
            .await?
            .into_iter()
            .find(|p| p.name == name);
        match existing {
            Some(p) => Ok(Some(self.client.get_playlist(&p.id).await?)),
            None => Ok(None),
        }
    }

    /// Append already-resolved songs to the queue as ONE atomic push (a single
    /// playlist_version bump and ONE notify_change), mirroring the album fan-out in
    /// [`enqueue_uri`](Self::enqueue_uri). An empty list is a no-op (no spurious
    /// wake). THE shared idle-append point for every batch enqueue that does NOT
    /// autoplay - `load` (a named playlist / `Starred`) and `findadd`/`searchadd` all
    /// funnel through here - so the fresh-enqueue anchor is armed at THIS single seam
    /// and no such path can forget it (see [`Self::arm_fresh_enqueue_anchor`]).
    fn enqueue_songs(&self, songs: Vec<Song>) {
        if songs.is_empty() {
            return;
        }
        let mut st = self.state.lock().unwrap();
        // The id the FIRST appended song will take, so a fresh idle enqueue can anchor
        // the freshly-appended tail (non-empty is guaranteed by the guard above).
        let first = st.next_id;
        for song in songs {
            let qid = st.next_id;
            st.next_id += 1;
            st.queue.push(QueueItem { id: qid, entry: QueueEntry::Song(song) });
        }
        st.playlist_version += 1;
        drop(st);
        self.notify_change();
        // A fresh idle enqueue outranks a prior finish - anchor the just-appended
        // music so the hint/seed names it, skipping any track that finished before this
        // gesture and lingers at the queue head. A no-op while a track is current.
        self.arm_fresh_enqueue_anchor(first);
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
        self.start_fade_spec(FadeRequest { intent, dur, commit_logical: None }).await
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
        let commit_logical = req.commit_logical;
        // ANY fade that installs here SUPERSEDES whatever is running. If it superseded
        // a live skip dip, that dip's Terminal::SkipLoad/switch_warmed will NEVER run,
        // so `pending_skip` and the prefetched warm target are now STALE - the warm
        // stays appended behind the still-playing current track and mpv would
        // auto-advance into it (audible bleed at the live gain) at the current track's
        // natural EOF, and the `warmed` guard would then swallow that EOF and stall the
        // queue. This is TRUE regardless of intent: a committing fade (setvol glide /
        // knob) AND every non-committing fade (resume-in, wind-down `fade to`/`toFloor`/
        // `fade in`, scheduled wake) alike abort the dip. `start_fade_spec` is NEVER the
        // skip machinery itself (the dip installs via `install_skip_dip`; the follow-on
        // ResumeIn spawns straight from the SkipLoad terminal), so a superseded skip is
        // always dead here. The install closure therefore clears `pending_skip`
        // unconditionally and the warm is dropped after the abort+join below - both
        // idempotent no-ops when nothing was skipping/warmed.

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
                    let (target, sub_jnd, terminal, clamp_dur_up) =
                        intent.resolve(from_db, ceiling, floor_db);
                    let bounds = startle_bounds(&cfg, sub_jnd);
                    // A DELIBERATE transport ramp (pause/resume) must always land:
                    // clamp the duration UP to the shortest length that keeps every
                    // step under the 3 dB cap, so FadeSpec::new never rejects it as
                    // StepTooLarge and it is never a hard cut.
                    let eff_dur = if clamp_dur_up {
                        // Use the SAME per-step interval FadeSpec::new will use
                        // (tick.max(min_slew)); passing bare min_slew when
                        // tick > min_slew under-counts the steps and the clamp
                        // fails to prevent the StepTooLarge rejection.
                        let min_slew = Duration::from_millis(cfg.min_slew_ms);
                        let step_interval = tick.max(min_slew);
                        dur.max(min_deliberate_dur(from_db, target, step_interval, synth_floor))
                    } else {
                        dur
                    };
                    let spec =
                        FadeSpec::new(from_db, target, eff_dur, tick, Curve::DbLinear, bounds)?;
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
                        // Synchronously commit the logical target + baseline at
                        // INSTALL (under this slot lock, atomic against supersede),
                        // before any tick. A superseded key-mash / slider-drag thus
                        // still commits every intermediate rung; does NOT touch
                        // live_gain_db (the envelope keeps animating) or `fading`.
                        // Also reconcile the pending intents like a manual commit
                        // (mirrors set_manual_volume): a glide/knob commit means the
                        // deck is being driven to a concrete baseline, so it must
                        // never leave the reported state stuck Paused (a setvol that
                        // supersedes an in-flight PauseOut) nor at a never-loaded
                        // skip target - the difference from set_manual_volume is it
                        // leaves `fading`/`live_gain_db` alone so the ramp animates.
                        // Clear `pending_skip` for EVERY install, committing or not: if
                        // this fade superseded a live skip dip, that dip is dead (its
                        // SkipLoad never runs) and the reported current must revert from
                        // the never-loaded target back to `current`. A no-op when no skip
                        // was in flight. Pairs with the drop_warm below (which discards the
                        // now-orphaned parked warm entry) so a non-committing wake/resume/
                        // wind-down can never leave a stale target to auto-advance.
                        st.pending_skip = None;
                        if let Some((db, vol)) = commit_logical {
                            st.logical_gain_db = db;
                            st.target_volume = vol;
                            // A knob/glide commits its baseline synchronously, so the
                            // knob keeps stepping from logical_gain_db (rapid presses
                            // each advance a detent).
                            st.baseline_committed = true;
                            st.pending_pause = false;
                        } else {
                            // A non-committing fade (resume-in, wind-down, wake, skip
                            // dip) leaves logical_gain_db at the stale pre-fade level
                            // and only animates live_gain_db, so a knob press during
                            // it must step from the LIVE gain, not the stale baseline
                            // (else a DOWN mid-wake jumps the volume up - a startle).
                            st.baseline_committed = false;
                        }
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
        //
        // On ANY successful install, `pending_skip` was cleared (a superseded skip):
        // drop any now-stale parked warm target so it can never auto-advance behind the
        // still-playing current track. This must run for NON-committing fades too
        // (scheduled wake, reconnect restart, user resume-in, wind-down `fade to`/
        // `toFloor`/`fade in`) - each supersedes and aborts a live skip dip just like a
        // committing setvol/knob does, so leaving the warm parked would let mpv
        // auto-advance into it at the current track's natural EOF (audible bleed) and
        // then stall the queue on the swallowed `warmed` EOF. Best-effort + idempotent
        // (a no-op when nothing was warmed). Done after the supersede's abort+join, so it
        // never races a SkipLoad. Intent no longer gates this drop - the install closure
        // still distinguishes the baseline-commit behavior separately above.
        if res.is_ok() {
            let _ = self.player.drop_warm().await;
        }
        res
    }

    // ── startle-safe transport (pause / resume) ─────────────────────────────

    /// The play state to REPORT outward (MPD `status`, MPRIS `PlaybackStatus`,
    /// resume checkpoints). Layers TWO guards over the raw mpv state:
    ///   1. the idle guard ([`effective_play_state`]): nothing loaded -> Stopped;
    ///   2. the pending-pause intent: a pause has been requested but the fade to
    ///      silence has not yet frozen mpv -> report Paused IMMEDIATELY, so the whole
    ///      pause window is consistent (no stale Playing at the ACK, in a mid-fade
    ///      checkpoint, or on a Play-during-fade branch).
    /// One source of truth shared by `status`, the MPRIS surface, and the resume
    /// checkpoint - so they can never disagree about the play state.
    pub fn reported_play_state(&self) -> PlayState {
        let (has_current, pending) = {
            let st = self.state.lock().unwrap();
            (
                st.current.and_then(|i| st.queue.get(i)).is_some(),
                st.pending_pause,
            )
        };
        if !has_current {
            return PlayState::Stopped;
        }
        if pending {
            return PlayState::Paused;
        }
        effective_play_state(self.player.state(), has_current)
    }

    /// The clamped pause/resume fade duration (float-second `pause_fade_secs` into
    /// `[min_slew, max_dur]`). Saturating parse: a pathological float never panics.
    fn pause_fade_dur(&self) -> Duration {
        let raw = Duration::try_from_secs_f64(self.fade_cfg.pause_fade_secs)
            .unwrap_or_else(|_| Duration::from_millis(self.fade_cfg.min_slew_ms));
        self.clamp_fade_dur(raw)
    }

    /// The clamped absolute-volume GLIDE fade duration (float-second
    /// `glide_fade_secs` into `[min_slew, max_dur]`). Distinct from
    /// `pause_fade_dur` so the human-feel of a setvol glide is tunable
    /// independently of the pause ramp. Saturating parse; a large span still
    /// extends past this via `clamp_dur_up` to keep every step <= 3 dB.
    fn glide_fade_dur(&self) -> Duration {
        let raw = Duration::try_from_secs_f64(self.fade_cfg.glide_fade_secs)
            .unwrap_or_else(|_| Duration::from_millis(self.fade_cfg.min_slew_ms));
        self.clamp_fade_dur(raw)
    }

    /// THE startle-safe transport toggle, shared by the MPD `pause` command and the
    /// MPRIS Pause/PlayPause/Play controls. `want`: `Some(true)` pause, `Some(false)`
    /// resume, `None` toggle from the live state.
    ///
    /// PAUSE first runs a SHORT sub-JND fade to silence via [`Self::start_fade_spec`]
    /// (reused verbatim), THEN pauses mpv in the fade terminal (silent at the freeze,
    /// no click). RESUME unpauses from silence, THEN ramps back to the pre-pause
    /// level. Both notify on the transition so the MPRIS PropertiesChanged loop (and
    /// MPD `idle`) refresh - the fix for a desktop widget that would otherwise never
    /// see the Paused edge (it never went through `notify_change`).
    pub async fn set_pause(&self, want: Option<bool>) -> Result<(), PlayerError> {
        // Decide from the EFFECTIVE (pending-pause-aware) state, not the raw mpv
        // state: during a pause-out fade mpv is still raw-Playing but the intent is
        // Paused, so a Play/Resume/PlayPause issued in that window must take the
        // resume branch (aborting the pause), never re-pause or drop (F5).
        let state = self.reported_play_state();
        let should_pause = match want {
            Some(p) => p,
            None => matches!(state, PlayState::Playing),
        };
        match (should_pause, state) {
            // Playing -> pause (fade to silence, then pause).
            (true, PlayState::Playing) => self.pause_with_fade().await,
            // Paused (or pending-pause) -> resume (unpause from silence, then fade
            // in). This also aborts an in-flight pause-out fade (F5).
            (false, PlayState::Paused) => self.resume_with_fade().await,
            // Stopped and asked to play: nothing loaded to fade; mirror the prior
            // direct-resume behavior and notify so a listener still refreshes.
            (false, PlayState::Stopped) => {
                let r = self.player.resume().await;
                self.notify_change();
                r
            }
            // Already in the desired state (or pause requested while not playing):
            // a no-op OK, no fade, no spurious notify.
            _ => Ok(()),
        }
    }

    /// One physical-potentiometer detent, up or down (the `knob` command). The
    /// server owns all the dB math and the off-click pause decision; the client only
    /// signals direction.
    ///
    /// Each detent is a fixed [`KNOB_STEP_DB`] (3 dB) equal-loudness step on a grid
    /// anchored at 0 dB. The reference level is the COMMITTED logical target while at
    /// rest or during a knob-glide (so rapid presses climb/descend monotonically -
    /// each supersedes the in-flight knob fade from a synchronously-committed
    /// baseline rather than collapsing onto the same not-yet-reached live gain), but
    /// the LIVE in-flight gain while a non-committing fade (resume-in, wind-down,
    /// wake, skip dip) animates - there the committed target is a stale pre-fade
    /// value and stepping from it would jolt the volume the wrong way (see
    /// [`State::baseline_committed`]). A settled level is a u8-requantized rung, so
    /// the step SNAPS a near-grid start onto its rung before advancing (one press =
    /// one detent, never a sub-rung plateau). The bottom of the usable knob is the
    /// configured `floor_level_db`; a down-step that would cross below it is the OFF-CLICK,
    /// which reuses the EXACT `set_pause` pause path (one pause mechanism). A knob-up
    /// while paused resumes. Because each down-step commits its rung as the baseline,
    /// `target_volume` already sits at the bottom detent when you off-click, so the
    /// resume ramp climbs back from the bottom - faithful to a real pot.
    async fn knob(&self, dir: KnobDir) -> Result<(), PlayerError> {
        let floor = self.fade_cfg.floor_level_db;
        // Use the EFFECTIVE play state, not the bare `pending_pause` flag: once a
        // pause fade settles the deck is `player`-Paused with pending_pause already
        // cleared, and a knob-up then must still RESUME (not step volume). This
        // covers both the mid-fade window (pending_pause) and the settled pause.
        let paused = self.reported_play_state() == PlayState::Paused;
        // Brief lock, dropped BEFORE any await (never hold State across .await).
        // Pick the reference level per `baseline_committed`: the committed logical
        // target while at rest / during a knob-glide (so rapid presses each advance
        // a detent from a synchronously-committed baseline, and a settled u8 rung is
        // stepped cleanly), but the LIVE in-flight gain while a non-committing fade
        // (resume-in, wind-down, wake, skip dip) animates - there logical is a stale
        // pre-fade level and stepping from it would drive the volume the wrong way
        // (a DOWN mid-wake would jump up from the loud pre-sleep baseline).
        let ref_db = {
            let st = self.state.lock().unwrap();
            if st.baseline_committed {
                st.logical_gain_db
            } else {
                st.live_gain_db
            }
        };
        match (dir, paused) {
            // Up while paused -> resume (climbs from the bottom detent baseline).
            (KnobDir::Up, true) => self.set_pause(Some(false)).await,
            // Down while already paused -> idempotent no-op (already off).
            (KnobDir::Down, true) => Ok(()),
            (KnobDir::Up, false) => {
                // The next 3 dB detent up (snapping a settled near-grid start onto
                // its rung first, so one press is always a full detent), capped at
                // the 0 dB ceiling.
                let target = Self::knob_detent(ref_db, true).min(0.0);
                if target <= ref_db {
                    return Ok(()); // at the ceiling: no-op
                }
                self.knob_step_to(target).await
            }
            (KnobDir::Down, false) => {
                // The next 3 dB detent down (same near-grid snap).
                let target = Self::knob_detent(ref_db, false);
                if target < floor {
                    // Off-click: below the lowest audible detent -> pause.
                    self.set_pause(Some(true)).await
                } else {
                    self.knob_step_to(target).await
                }
            }
        }
    }

    /// The 3 dB detent strictly beyond `from_db` in `dir` (up = `true`), on the grid
    /// anchored at 0 dB. A settled level is a u8-requantized rung (e.g. vol 79 =
    /// -6.14 dB, ~0.05 rung off the -6 line), so a naive strict `floor()`/`ceil()`
    /// step would move only that sub-rung sliver and then plateau (or oscillate)
    /// forever. We therefore SNAP a near-grid start onto its rung first (within a
    /// quarter rung - larger than any u8 requant error, smaller than a mid-rung
    /// gap) so one press always advances a full detent; a GENUINELY mid-rung start
    /// (left by a prior absolute setvol) is not snapped and lands on the adjacent
    /// bracketing rung, preserving the "one press = adjacent rung" off-grid rule.
    fn knob_detent(from_db: f64, up: bool) -> f64 {
        let idx = from_db / KNOB_STEP_DB;
        let nearest = idx.round();
        let rung = if (idx - nearest).abs() < 0.25 {
            // Essentially on a rung: one full detent in the pressed direction.
            if up { nearest + 1.0 } else { nearest - 1.0 }
        } else if up {
            // Between rungs: the adjacent rung above / below.
            idx.ceil()
        } else {
            idx.floor()
        };
        rung * KNOB_STEP_DB
    }

    /// Drive one knob detent to `target_db` as a single deliberate slewed fade
    /// through the one FadeSlot, committing the new baseline. Supersedes any
    /// in-flight knob fade (validate-before-abort, epoch-guarded), so a key-mash
    /// resolves to one smooth monotonic ramp. A later absolute `setvol` supersedes
    /// this in turn (manual-wins).
    async fn knob_step_to(&self, target_db: f64) -> Result<(), PlayerError> {
        let vol = db_to_mpv_volume(target_db).round().clamp(0.0, 100.0) as u8;
        let dur = self.pause_fade_dur();
        let _ = self
            .start_fade_spec(FadeRequest {
                intent: FadeIntent::Knob { target_db, vol },
                dur,
                // Commit this rung to the logical target + baseline at INSTALL, so
                // a key-mash whose fades supersede still commits every intermediate
                // detent (bug b) and the off-click pause resumes at the true quiet
                // rung, not the loud pre-mash level.
                commit_logical: Some((target_db, vol)),
            })
            .await;
        self.notify_change();
        Ok(())
    }

    /// Run the sub-JND fade to silence, then pause (via [`Terminal::Pause`]). The
    /// pause happens in the spawned fade terminal, so this returns as soon as the
    /// fade is installed. A rejected spec (should not happen for a sub-JND Silence
    /// fade) degrades to a direct pause so playback never stays audible.
    async fn pause_with_fade(&self) -> Result<(), PlayerError> {
        // Flip the REPORTED state to Paused IMMEDIATELY (F2): set the pending-pause
        // intent and notify BEFORE the fade runs, so status/MPRIS/checkpoints see
        // Paused at once and the whole fade window is consistent.
        {
            let mut st = self.state.lock().unwrap();
            st.pending_pause = true;
            // A pause issued during a skip dip supersedes that dip: the PauseOut fade
            // aborts it before Terminal::SkipLoad runs, so the skip target never
            // loads and mpv stays paused on the OLD (still-loaded) track. Clear the
            // skip intent so the reported current reverts from the never-loaded
            // target back to `current`, matching what mpv actually holds (mirrors
            // set_manual_volume clearing pending_skip).
            st.pending_skip = None;
        }
        self.notify_change();
        let dur = self.pause_fade_dur();
        let r = self
            .start_fade_spec(FadeRequest { intent: FadeIntent::PauseOut, dur, commit_logical: None })
            .await;
        // A pause issued during a skip dip supersedes it (above): the skip target never
        // loads, so drop any parked warm entry - else the OLD track (still loaded, now
        // paused) would auto-advance into the stale target at its natural EOF once
        // resumed. Best-effort + idempotent; PauseOut is non-committing so start_fade_spec
        // does not do this itself. Done after the fade install (abort+join), no SkipLoad race.
        let _ = self.player.drop_warm().await;
        match r {
            Ok(()) => Ok(()),
            Err(e) => {
                tracing::warn!(error = %e, "pause fade rejected; pausing without fade");
                let r = self.player.pause().await;
                self.notify_change();
                r
            }
        }
    }

    /// Resume playback the startle-safe way, ramping UP to the pre-pause baseline.
    /// Distinguishes TWO live cases (keyed off the RAW player state, not the pending
    /// intent), because the pre-pause fade may be either settled or still in flight.
    /// The SETTLED vs IN-FLIGHT decision is made INSIDE the cancel closure, under the
    /// slot lock, AFTER the in-flight fade's abort+join - so a racing Terminal::Pause
    /// can never flip the state between the decision and the cancel (the TOCTOU that
    /// would otherwise leave the deck mpv-Paused with no resume()):
    ///
    ///   - SETTLED pause (mpv raw-Paused at silence): the deck already faded to
    ///     silence and froze, so the ResumeIn MUST start from silence. Under the same
    ///     slot lock drop the live gain to the synth floor, then set mpv volume 0,
    ///     unpause, and ramp UP from silence to the baseline.
    ///
    ///   - IN-FLIGHT PauseOut (mpv raw-Playing, the ramp mid-descent above silence):
    ///     this is the F5 resume-during-window abort. mpv never paused and the live
    ///     gain is at e.g. 50%, so forcing to silence first would be a VISIBLE snap to
    ///     0 followed by a fade up. Instead SUPERSEDE the PauseOut and ramp UP from the
    ///     CURRENT live gain (a normal ResumeIn whose from_db is read live inside the
    ///     slot lock) - a smooth, monotonic un-dip with no snap to silence and no
    ///     set_volume(0). No resume() either: the deck was never frozen.
    ///
    /// Either way the pending-pause intent is cleared and the resume ramp reuses
    /// [`FadeIntent::ResumeIn`] verbatim, so it never drops below the current gain.
    async fn resume_with_fade(&self) -> Result<(), PlayerError> {
        // The level to restore: the baseline preserved across the pause.
        let vol = self.state.lock().unwrap().target_volume;
        let synth_floor = self.fade_cfg.synth_floor_db;
        // Decide SETTLED vs IN-FLIGHT atomically INSIDE the cancel_with closure, which
        // runs under the slot lock AFTER the abort+join has driven any racing
        // Terminal::Pause to completion (the terminal holds the slot lock across its
        // whole check-and-act). Reading the raw player state here - not before the
        // cancel - closes the TOCTOU window: a PauseOut fade whose Terminal::Pause is
        // racing the same lock can no longer flip the deck to Paused between the read
        // and the cancel, because the cancel already joined it before this closure
        // observes the state.
        //   - Paused => the pause fade SETTLED and froze the deck at silence: force the
        //     live gain to the synth floor so the ResumeIn ramps UP from silence, then
        //     (below) set mpv volume 0 and unpause.
        //   - Playing => IN-FLIGHT PauseOut abort (un-dip): leave the live gain at its
        //     mid-descent value so the ResumeIn ramps UP from there, no set_volume(0),
        //     no resume() - the deck was never frozen.
        let settled = self
            .fade
            .cancel_with(|| {
                let is_paused = matches!(self.player.state(), PlayState::Paused);
                let mut st = self.state.lock().unwrap();
                if is_paused {
                    st.live_gain_db = synth_floor;
                }
                // Clear the pending-pause intent either way so the reported state flips
                // to Playing.
                st.pending_pause = false;
                is_paused
            })
            .await;
        let mut r = Ok(());
        if settled {
            let _ = self.player.set_volume(0).await;
            // Unpause from silence.
            r = self.player.resume().await;
        }
        // Reflect the Playing edge immediately so the MPRIS widget flips to a pause
        // symbol without waiting for the ramp to finish.
        self.notify_change();
        // Short DELIBERATE ramp -> the saved level (ResumeIn: SetBaseline). from_db is
        // read live inside the slot lock: silence for a settled pause, the mid-fade
        // gain for an in-flight abort. Either way the ramp only rises (never re-dips).
        // A user resume is responsive, not the long sub-JND alarm wake.
        let dur = self.pause_fade_dur();
        let intent = FadeIntent::ResumeIn { target_db: mpv_volume_to_db(vol as f64), vol };
        let _ = self.start_fade_spec(FadeRequest { intent, dur, commit_logical: None }).await;
        r
    }

    /// Stop playback the startle-safe way the MPD `stop` path does: atomically cancel
    /// any in-flight fade and settle the baseline, then stop mpv and re-assert the
    /// baseline gain, then notify. Shared by the MPD `stop` command and MPRIS Stop
    /// (so an MPRIS-initiated stop also refreshes the desktop widget).
    pub async fn stop_playback(&self) {
        self.fade
            .cancel_with(|| {
                let mut st = self.state.lock().unwrap();
                let v = st.target_volume;
                st.set_manual_volume(v);
                // A stop clears any pending-pause intent (the deck is stopping, not
                // paused): the reported state must not stick at Paused.
                st.pending_pause = false;
            })
            .await;
        let _ = self.player.stop().await;
        let v = self.state.lock().unwrap().target_volume;
        let _ = self.player.set_volume(v).await;
        self.notify_change();
    }

    // ── smooth-restart (resume) ─────────────────────────────────────────────

    /// Register the persistent resume-state path (`.../resume.toml`). Called once
    /// by the daemon when a state dir resolves; absent => resume disabled.
    pub fn set_state_path(&self, p: PathBuf) {
        *self.state_path.lock().unwrap() = Some(p);
    }

    /// Register the configured end-of-queue CONTINUATION station (a Navidrome station
    /// NAME or an absolute `http(s)://` stream URL). Called once by the daemon from
    /// `[continuation].station`; `None` (or an unset section) leaves the feature off.
    /// The runtime toggle (`continuation on|off`) still gates whether it ever fires.
    pub fn set_continuation_station(&self, station: Option<String>) {
        *self.continuation_station.lock().unwrap() = station;
    }

    /// The end-of-queue continuation status as X- extension pairs for `status`,
    /// present ONLY when the feature is ARMED (the runtime toggle is ON AND a
    /// non-empty station is configured) so a disarmed/unconfigured deck stays lean -
    /// mirroring the armed-feature / ambient-hint HUD discipline. `X-hypodj-
    /// continuation` is `on` and `X-hypodj-continuation-station` names the configured
    /// station, so a client renders the standing "then: <station>" queue-tail hint
    /// BEFORE the handoff (the future is visible before it happens - anti-surprise).
    fn continuation_status_pairs(&self) -> Vec<(&'static str, String)> {
        if !self.state.lock().unwrap().continuation {
            return Vec::new();
        }
        match self.continuation_station.lock().unwrap().clone() {
            Some(station) if !station.trim().is_empty() => vec![
                ("X-hypodj-continuation", "on".to_string()),
                ("X-hypodj-continuation-station", station),
            ],
            _ => Vec::new(),
        }
    }

    /// The `continuation on|off` runtime toggle + `continuation [status]` report - the
    /// startle-safe opt-in for end-of-queue continuation radio. Default OFF and NEVER
    /// default-on: a configured station does nothing until this is armed. The flip is
    /// PERSISTED (a resume checkpoint right after) so the arming survives a restart;
    /// `status` reports the live toggle + configured station honestly.
    async fn handle_continuation(&self, cmd: ContinuationCmd) -> MpdResponse {
        match cmd {
            ContinuationCmd::On | ContinuationCmd::Off => {
                let on = matches!(cmd, ContinuationCmd::On);
                self.state.lock().unwrap().continuation = on;
                self.notify_change();
                // Persist the flip immediately so the arming survives a restart (the
                // resume checkpoint carries the toggle). Best-effort: a missing state
                // path is a silent no-op and a write error is logged, never fatal. An
                // empty-stopped deck is skipped by the checkpoint guard, but there is
                // nothing to continue from there anyway - it persists on the next real
                // checkpoint once a queue exists.
                self.checkpoint(self.last_elapsed_secs()).await;
                MpdResponse::ok()
            }
            ContinuationCmd::Status => {
                let on = self.state.lock().unwrap().continuation;
                let station = self.continuation_station.lock().unwrap().clone();
                let mut b = MpdResponse::pairs()
                    .pair("continuation", if on { "on" } else { "off" });
                if let Some(s) = station {
                    b = b.pair("continuation_station", s);
                }
                b.build()
            }
        }
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
        // The pending-pause-aware, idle-guarded reported state: a checkpoint taken
        // mid-pause-fade persists Paused (never a stale Playing that would make a
        // crash-resume auto-play), and never claims Playing/Paused with no current
        // song. Computed BEFORE locking `State` (it locks internally).
        let play_state = match self.reported_play_state() {
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
            continuation: st.continuation,
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
        // NEVER clobber a good saved session with an empty-stopped snapshot. An
        // empty queue with a Stopped deck carries nothing worth persisting, and is
        // exactly the state a failed/aborted restore (backend not yet up) leaves in
        // memory - writing it would permanently delete the on-disk queue. Skipping
        // the write here breaks the "transient backend outage deletes the queue"
        // chain even if resolution stays flaky across several restarts.
        if snap.queue.is_empty() && snap.play_state == ResumePlayState::Stopped {
            tracing::debug!(path = %path.display(), "resume checkpoint skipped (empty stopped deck; preserving saved queue)");
            return;
        }
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
    /// explicit stop survives the rebuild).
    ///
    /// Failure handling distinguishes the TWO reasons a persisted song fails to
    /// re-resolve, by error KIND (not a count heuristic):
    ///
    /// - A TRANSIENT failure ([`SubsonicError::Request`] surfaced as a non-
    ///   NotFound error - the backend not yet reachable at daemon start, a
    ///   transport error) ABORTS the WHOLE restore with Err WITHOUT mutating
    ///   State, leaving resume.toml intact so the next start retries once the
    ///   backend is up. A transient outage must never drop entries and let the
    ///   checkpoint loop clobber the saved session with an empty queue.
    /// - A PERMANENT NotFound ([`SubsonicError::NotFound`], Subsonic API code 70
    ///   - the song was authoritatively deleted from the library) SKIPS just that
    ///   one entry and keeps rebuilding the rest. All-or-nothing here would let a
    ///   single deleted song abort every restart forever, self-perpetuating (the
    ///   empty-stopped checkpoint guard refuses to rewrite resume.toml, so the
    ///   dead id is never pruned), permanently losing the whole saved session.
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
                        Err(SubsonicError::NotFound(e)) => {
                            // The song was authoritatively deleted from the
                            // library (API code 70). Dropping just this entry can
                            // never be confused with a transient outage, so skip
                            // it and keep the rest of the saved session. If this
                            // was the saved current index, playback falls through
                            // to the next surviving entry (or stops if none).
                            tracing::warn!(id, error = %e, "resume: song permanently gone (not found); skipping and keeping the rest of the queue");
                            if Some(i) == s.current {
                                // Point current at the slot the next surviving
                                // entry will occupy; clamped to None after the
                                // loop if nothing follows.
                                new_current = Some(entries.len());
                                current_is_song = false;
                            }
                            continue;
                        }
                        Err(e) => {
                            // A TRANSIENT re-resolution failure (backend not yet
                            // reachable when the daemon restarts before Navidrome
                            // is up, a transport error) MUST NOT drop the song:
                            // dropping entries yields a short/empty queue that the
                            // checkpoint loop then writes back over the good
                            // resume.toml. Abort the WHOLE restore without mutating
                            // State so the on-disk file survives for the next start
                            // (a retry once the backend is up).
                            tracing::warn!(id, error = %e, "resume: song not resolvable (transient); aborting restore to preserve saved queue");
                            return Err(format!("resume: song {id} unresolvable: {e}"));
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

        // If the saved current index was a permanently-deleted song and nothing
        // survives after it, there is no slot to resume into: fall back to no
        // current (playback stays stopped rather than pointing past the end).
        if let Some(c) = new_current {
            if c >= entries.len() {
                new_current = None;
                current_is_song = false;
            }
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
            // Rehydrate the persisted continuation arming (default false for a
            // pre-continuation resume.toml) so the toggle survives a restart.
            st.continuation = s.continuation;
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
            let target_db = mpv_volume_to_db(saved_vol as f64);
            // Smooth-restart ramp-IN: a QUICK deliberate ramp from silence to the
            // user's SAVED level, using restart_fade_secs - the counterpart to the
            // shutdown fade-OUT. NOT the long alarm wake_ramp_secs (8 min), which
            // would leave the music barely audible for minutes after a rebuild.
            let dur = self.clamp_fade_dur(Duration::from_secs(self.fade_cfg.restart_fade_secs));
            // The wake ramp starts SUB-audible: from the synth floor its whole lower
            // stretch is inaudible. LEAD is the wall-clock time that ramp - the EXACT
            // one start_fade_spec spawns below (live_gain_db == synth_floor here, so
            // the two specs are identical) - first crosses AUDIBILITY_DB. Reading it
            // off the REAL (sub-JND-extended) schedule, not the nominal duration, and
            // seeking back by LEAD lands the playhead at the saved elapsed at the
            // first-audible instant: no audible content skipped or replayed under the
            // inaudible head. Falls back to no lead if the spec cannot be built.
            let lead = self
                .wake_ramp_spec(synth_floor, target_db, dur)
                .ok()
                .and_then(|spec| spec.time_to_reach_db(AUDIBILITY_DB))
                .unwrap_or(Duration::ZERO);
            // Silence BEFORE the first buffer: mpv volume 0 persists across the
            // loadfile so the wake ramp owns the rise.
            let _ = self.player.set_volume(0).await;
            if let Err(e) = self.play_index_from_silence(idx).await {
                return Err(e);
            }
            // A library song seeks to (saved elapsed - LEAD), clamped >= 0 so it
            // never seeks before the track start; a raw Stream restarts from 0 (no
            // seek - a live stream has no seekable saved offset).
            if current_is_song && elapsed > 0.0 {
                let target = (elapsed - lead.as_secs_f64()).max(0.0);
                let _ = self.player.seek(target).await;
            }
            let intent = FadeIntent::WakeTo { target_db, vol: saved_vol };
            let _ = self.start_fade_spec(FadeRequest { intent, dur, commit_logical: None }).await;
        } else {
            // Paused/Stopped: restore the baseline volume, leave playback stopped.
            let v = s.volume.min(100);
            self.state.lock().unwrap().set_manual_volume(v);
            let _ = self.player.set_volume(v).await;
        }
        Ok(())
    }

    /// Build the exact wake ramp-in [`FadeSpec`] the restore path spawns via
    /// `start_fade_spec` for a [`FadeIntent::WakeTo`]: sub-JND (extends to honor the
    /// per-step cap), `DbLinear`, from `from_db` up to `target_db`, at the configured
    /// tick. Kept as a single source so the LEAD computed off this spec cannot drift
    /// from the schedule the spawned fade actually runs (they agree today only
    /// because `live_gain_db == synth_floor` at restore; the drift-guard test pins
    /// the shared params). Pure aside from reading the live config.
    fn wake_ramp_spec(
        &self,
        from_db: f64,
        target_db: f64,
        dur: Duration,
    ) -> Result<FadeSpec, FadeError> {
        let tick = Duration::from_millis(self.fade_cfg.tick_ms);
        let bounds = startle_bounds(&self.fade_cfg, true);
        FadeSpec::new(from_db, FadeTarget::Db(target_db), dur, tick, Curve::DbLinear, bounds)
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

    /// Compute the next queue index honoring the `random`/`repeat`/`single`
    /// flags, and apply `consume` (removing the just-finished entry from the
    /// queue and remapping the computed index over the shrink). Returns `Some(idx)`
    /// to play next or `None` to stop. `auto` distinguishes an EOF auto-advance
    /// (where `single` stops after the current track) from a manual `next` gesture
    /// (which always advances; `single` only governs auto-advance in MPD).
    ///
    /// Takes and DROPS the `std` `Mutex<State>` internally with no await inside, so
    /// no lock is held across an `.await` at the call sites. The seeded RNG
    /// ([`State::random_next_index`]) makes the `random` choice deterministic and
    /// unit-testable.
    fn plan_next(&self, auto: bool) -> Option<usize> {
        let mut st = self.state.lock().unwrap();
        // Anchor on the REPORTED current so a manual `next`/`prev` during an
        // in-flight skip steps past the target the user already sees, not the
        // still-loaded old track. On the auto (EOF) path this equals `current` -
        // advance_on_eof early-returns whenever a skip is pending, so pending_skip
        // is always None here for auto advances.
        let cur = st.reported_current()?;
        let len = st.queue.len();
        if len == 0 {
            return None;
        }
        // The next index in PRE-consume terms.
        let mut next: Option<usize> = if auto && st.single {
            // single: stop after the current track, or (with repeat) replay it.
            if st.repeat {
                Some(cur)
            } else {
                None
            }
        } else if st.random {
            st.random_next_index(Some(cur))
        } else if cur + 1 < len {
            Some(cur + 1)
        } else if st.repeat {
            // repeat-all at the end of the queue: wrap to the first entry.
            Some(0)
        } else {
            None
        };
        if st.consume {
            // Remove the just-finished entry, then remap the target index over the
            // shrink: indices AFTER the removed slot shift down by one; a target at
            // or before it is unchanged; anything now out of range stops (or wraps
            // when repeat is set and entries remain).
            if cur < st.queue.len() {
                st.queue.remove(cur);
                st.playlist_version += 1;
            }
            let new_len = st.queue.len();
            next = match next {
                Some(n) if n > cur => Some(n - 1),
                other => other,
            };
            next = match next {
                _ if new_len == 0 => None,
                Some(n) if n >= new_len => {
                    if st.repeat {
                        Some(0)
                    } else {
                        None
                    }
                }
                other => other,
            };
        }
        next
    }

    /// Called by the daemon when the player reports a natural EOF: advance to the
    /// next queue entry (honoring random/repeat/single/consume via
    /// [`Self::plan_next`]), or leave the state stopped at the end of the queue.
    pub async fn advance_on_eof(&self) {
        // A skip dip in flight (pending_skip Some) OWNS the next load: the OLD
        // track keeps playing audibly through the dip and may reach its natural
        // EOF inside that window. Advancing here would load an unrelated track and
        // collide with the pending Terminal::SkipLoad, which still fires afterward
        // and loads the skip target a second time - a spurious load plus an audible
        // double-load glitch. Leave the advance to the skip terminal.
        // Captured under ONE short lock scope, BEFORE plan_next mutates the queue
        // (consume removes the current entry): (a) whether the entry that just ended
        // was itself the active continuation stream - the ONE-SHOT re-entrancy guard
        // - and (b) whether the queue is GENUINELY exhausted (true drain) vs a
        // `single`-mode stop that left tracks queued.
        let (finishing_is_continuation, true_drain) = {
            let mut st = self.state.lock().unwrap();
            if st.pending_skip.is_some() {
                return;
            }
            // Record the finishing track as the recency seed BEFORE the advance
            // repoints `current`. Only a real library Song counts: a live/continuous
            // stream has no id and must never become a similar seed, so a stream EOF
            // leaves `last_finished` at the last real track that ended. The whole Song
            // is captured (not just its id) so the synchronous status render can name
            // the title, and so it survives consume mode evicting the queue entry.
            if let Some(song) = st.current.and_then(|i| st.queue.get(i)).and_then(|it| match &it.entry {
                QueueEntry::Song(s) => Some(s.clone()),
                QueueEntry::Stream { .. } => None,
            }) {
                st.last_finished = Some(song);
            }
            // Does the finishing entry's stable id match the active continuation
            // stream? mpv's loadfile Ok is premature (it fires the moment the load is
            // QUEUED, not when the stream CONNECTS), so a dead/unreachable/404
            // continuation URL surfaces HERE later as an EndFile(Error) -> Eof; a
            // finite/dropped stream likewise re-enters. Either way, if THIS is that
            // stream, we must NOT re-fire continuation.
            let finishing_is_continuation = st.continuation_active.is_some()
                && st.current.and_then(|i| st.queue.get(i)).map(|it| it.id)
                    == st.continuation_active;
            (finishing_is_continuation, st.is_true_drain())
        };
        let next = self.plan_next(true);
        match next {
            Some(idx) => {
                // A natural EOF advance is NOT a user gesture: it must NOT cancel an
                // in-flight fade or snap mpv's gain back to the baseline. A slow ramp
                // (winddown/sleep) has to survive across the track boundary (mpv
                // `volume` persists across loadfile replace), so play WITHOUT the
                // resync that play_index performs for fresh-play gestures.
                let _ = self.play_index_inner(idx, false).await;
            }
            None => {
                // Continuation is a ONE-SHOT flow into radio at a GENUINE end-of-queue
                // drain. Two guards keep it from becoming the old silent-drain bug in a
                // new hat:
                //   - finishing_is_continuation: if the entry that JUST ended was
                //     itself the continuation stream (a dropped/finite stream, or a
                //     dead/unreachable/404 URL whose failure arrived late as
                //     EndFile(Error) after loadfile's premature Ok), do NOT re-fire.
                //     Stop HONESTLY and LOUDLY - a good station plays indefinitely; a
                //     drop is an honest stop, never a retry loop that grows the queue
                //     without bound.
                //   - true_drain: fire ONLY when the queue is genuinely exhausted,
                //     never on a `single`-mode None that left tracks queued (those must
                //     play, not be hijacked by radio).
                // Every disarmed / no-station / unresolvable / cold-load-failure case
                // ALSO falls through to the SAME honest stop - never a silent
                // playing-state, never a retry loop.
                if finishing_is_continuation {
                    tracing::warn!(
                        "continuation stream ended (dropped / finite / unreachable); stopping honestly - one-shot, no re-fire"
                    );
                } else if true_drain && self.try_continuation().await {
                    return;
                }
                let mut st = self.state.lock().unwrap();
                st.current = None;
                // End of queue: no pending pause / skip can survive a stopped deck.
                st.pending_pause = false;
                st.pending_skip = None;
                // No continuation stream is live once the deck stops honestly.
                st.continuation_active = None;
                drop(st);
                self.notify_change();
            }
        }
    }

    /// Resolve the configured continuation station to a raw stream URL. An absolute
    /// `http(s)://` station value is used verbatim; any other value is treated as a
    /// saved Navidrome internet-radio station NAME and resolved via a live
    /// `getInternetRadioStations` fetch (case-insensitive, [`station_url_for_name`]).
    /// Returns `None` for an unresolvable station (network error, or no station by
    /// that name) - the caller then ends stopped, never guessing a random station.
    /// The fetch is an `.await`, so this is ALWAYS called with NO std lock held.
    async fn resolve_continuation_url(&self, station: &str) -> Option<String> {
        if is_stream_uri(station) {
            return Some(station.to_string());
        }
        let stations = self.client.get_internet_radio_stations().await.ok()?;
        station_url_for_name(&stations, station)
    }

    /// End-of-queue CONTINUATION cold-start. Fired ONLY from the
    /// [`Self::advance_on_eof`] None-branch (the drain edge, after single/repeat/
    /// consume were honored). When the runtime toggle is ARMED and a station is
    /// configured, resolve it to a stream URL, append it as a first-class
    /// [`QueueEntry::Stream`], point `current` at it, and cold-load it - publishing
    /// the normal TrackStart/StateChanged (via [`Self::play_index_inner`]) so clients
    /// update. Returns `true` when a continuation stream started playing, `false` in
    /// EVERY inert/failure case (disarmed, no station, unresolvable, or a cold-load
    /// error) so the caller performs the honest stop.
    ///
    /// Invariants preserved: the appended entry is a RAW stream (no song id), so it
    /// never scrobbles, and advance_on_eof already refused to record a stream as
    /// `last_finished` - the continuation stream can never become a similar seed. The
    /// std `Mutex<State>` is released BEFORE the station-resolution await and before
    /// the player load await (the append is a pure mutation under one short lock
    /// scope). A cold-load failure REMOVES the just-appended entry and returns `false`
    /// (honest stop), never a retry loop into a playing-state-with-silence.
    async fn try_continuation(&self) -> bool {
        // 1. Armed? The runtime toggle must be ON (default OFF - never a surprise).
        if !self.state.lock().unwrap().continuation {
            return false;
        }
        // 2. A station configured? (None = feature off.) Cloned out; lock released.
        let Some(station) = self.continuation_station.lock().unwrap().clone() else {
            return false;
        };
        if station.trim().is_empty() {
            return false;
        }
        // 3. Resolve the station to a stream URL - a network call for a Navidrome
        //    station NAME - with NO std lock held. Unresolvable => inert (stop).
        let Some(url) = self.resolve_continuation_url(&station).await else {
            tracing::info!(station = %station, "continuation station unresolvable; ending stopped");
            return false;
        };
        // 4. Append the continuation stream as a first-class queue entry and point
        //    `current` at its index, as ONE pure mutation under a single short lock
        //    scope (never across an await). The title is the configured station label;
        //    the live ICY name (if any) decorates currentsong later via stream_meta.
        let (idx, id) = {
            let mut st = self.state.lock().unwrap();
            let id = st.next_id;
            st.next_id += 1;
            st.queue.push(QueueItem {
                id,
                entry: QueueEntry::Stream { url: url.clone(), title: station.clone() },
            });
            st.playlist_version += 1;
            (st.queue.len() - 1, id)
        };
        self.notify_change();
        // 5. Cold loadfile from the drained deck. Match the natural-advance sibling
        //    (no volume resync) so an in-flight winddown/sleep ramp survives the
        //    handoff. play_index_inner sets `current` + publishes on success.
        if self.play_index_inner(idx, false).await.is_ok() {
            // Latch this stream's id as the ACTIVE continuation stream. loadfile's Ok
            // is premature (the stream may still fail to connect), so this is what lets
            // the NEXT advance_on_eof recognize an EndFile(Error)/Eof on THIS entry as
            // a continuation-stream end and stop honestly instead of re-firing. Set
            // AFTER the load so a cold-load failure (removed below) never leaves it set.
            self.state.lock().unwrap().continuation_active = Some(id);
            return true;
        }
        // Cold-load FAILED: stop LOUDLY. Remove the entry we appended (by stable id, so
        // a concurrent queue edit cannot make us drop the wrong row) and return false;
        // the caller performs the single honest stop (current=None + notify). Never a
        // retry into a silent playing-state - do NOT reintroduce the drain bug.
        tracing::warn!(station = %station, url = %url, "continuation cold-load failed; ending stopped");
        {
            let mut st = self.state.lock().unwrap();
            if let Some(pos) = st.queue.iter().position(|it| it.id == id) {
                st.queue.remove(pos);
                st.playlist_version += 1;
            }
        }
        false
    }

    // ── startle-safe USER skip (skip-fade) ──────────────────────────────────

    /// Resolve a queue item's play args SYNCHRONOUSLY (the Subsonic `stream_url`
    /// is sync), so a caller can hand a sink-level [`ResolvedPlay`] to a fade
    /// terminal that runs under the slot lock (no `&self` handler call there).
    /// Shared by [`Self::play_index_inner`] and [`Self::skip_with_fade`].
    fn resolve_play(&self, item: &QueueItem) -> Result<ResolvedPlay, String> {
        match &item.entry {
            QueueEntry::Song(song) => {
                let url = self.client.stream_url(&song.id).map_err(|e| e.to_string())?;
                Ok(ResolvedPlay {
                    song_id: Some(song.id.clone()),
                    qid: QueueId(item.id),
                    url: url.to_string(),
                })
            }
            QueueEntry::Stream { url, .. } => Ok(ResolvedPlay {
                song_id: None,
                qid: QueueId(item.id),
                url: url.clone(),
            }),
        }
    }

    /// The clamped skip-dip fade duration (`skip_fade_secs` into `[min_slew,
    /// max_dur]`). Mirrors [`Self::pause_fade_dur`]; saturating parse so a
    /// pathological float never panics.
    fn skip_fade_dur(&self) -> Duration {
        let raw = Duration::try_from_secs_f64(self.fade_cfg.skip_fade_secs)
            .unwrap_or_else(|_| Duration::from_millis(self.fade_cfg.min_slew_ms));
        self.clamp_fade_dur(raw)
    }

    /// Whether the CURRENT track is safe to WARM behind (near-EOF guard, finding
    /// 1a). Returns `false` - decline the warm - when the current entry is a live /
    /// continuous stream (no natural end), has an UNKNOWN duration, or is within
    /// [`NEAR_EOF_GUARD_SECS`] of its natural EOF (elapsed read from the lockless
    /// live-media atomic). Only a finite Song with a comfortable margin left warms;
    /// a decline degrades the skip to today's proven trough loadfile-replace, never
    /// worse. Conservative on purpose: any doubt (no current, no duration) declines.
    fn current_can_warm(&self) -> bool {
        let dur_secs = {
            let st = self.state.lock().unwrap();
            let Some(cur) = st.current else { return false };
            let Some(item) = st.queue.get(cur) else { return false };
            match &item.entry {
                // A live/continuous stream never reaches a natural EOF, so appending
                // behind it can never auto-advance - but it also never buffers fully,
                // so the warm has nothing to prefetch and no payoff. Decline.
                QueueEntry::Stream { .. } => return false,
                // A Song with a KNOWN finite duration can be warmed behind, subject to
                // the margin check below. Unknown duration -> decline (cannot bound the
                // EOF distance, so treat as unsafe).
                QueueEntry::Song(song) => match song.duration_secs {
                    Some(d) if d > 0 => d as f64,
                    _ => return false,
                },
            }
        };
        // Live media position (P1 Tick.time_pos) via the lockless atomic.
        let elapsed = self.last_elapsed_secs();
        // The warm sits PARKED behind the current track for the WHOLE dip (from
        // prefetch_warm until the trough switch_warmed ~ the dip's duration), and mpv
        // auto-advances into a parked entry at the current's natural EOF (keep-open does
        // not stop a non-last entry - see MpvPlayer::spawn). So the guard window must
        // exceed the dip duration, not just a fixed constant: only warm when the current
        // has MORE than (dip duration + NEAR_EOF_GUARD_SECS margin) left, so it cannot
        // EOF while a warm is parked. The margin also absorbs the Tick-quantized
        // staleness of `elapsed` and the switch/network overhead.
        let guard = self.skip_fade_dur().as_secs_f64() + NEAR_EOF_GUARD_SECS;
        (dur_secs - elapsed) > guard
    }

    /// Build a DELIBERATE (not sub-JND) fade spec from `from_db` to `target`,
    /// clamping the duration UP to the deliberate-safe minimum (never a hard cut) -
    /// the SAME `eff_dur` math [`Self::start_fade_spec`] applies to a `clamp_dur_up`
    /// intent. Used for BOTH halves of a skip: the dip to silence and the
    /// pre-built ResumeIn back to the baseline.
    fn build_deliberate_spec(
        &self,
        from_db: f64,
        target: FadeTarget,
        dur: Duration,
    ) -> Result<FadeSpec, FadeError> {
        let tick = Duration::from_millis(self.fade_cfg.tick_ms);
        let synth_floor = self.fade_cfg.synth_floor_db;
        let min_slew = Duration::from_millis(self.fade_cfg.min_slew_ms);
        let step_interval = tick.max(min_slew);
        let eff_dur = dur.max(min_deliberate_dur(from_db, target, step_interval, synth_floor));
        let bounds = startle_bounds(&self.fade_cfg, false);
        FadeSpec::new(from_db, target, eff_dur, tick, Curve::DbLinear, bounds)
    }

    /// Route a USER Next/Previous. Fades (dip-through-silence) ONLY when actually
    /// PLAYING with a current track; otherwise (paused / stopped / no-current) it
    /// falls through to the plain [`Self::play_index`] path unchanged. The
    /// autonomous EOF advance does NOT come here - it stays gapless.
    async fn user_skip(&self, idx: usize) -> Result<(), String> {
        let has_current = self.state.lock().unwrap().current.is_some();
        if self.reported_play_state() == PlayState::Playing && has_current {
            self.skip_with_fade(idx).await
        } else {
            self.play_index(idx).await
        }
    }

    /// The skip-fade composition: pre-resolve the target, pre-build the ResumeIn
    /// half, flip the reported current to the target (`pending_skip`) IMMEDIATELY,
    /// then install a deliberate dip-to-silence whose [`Terminal::SkipLoad`] loads
    /// the target from silence and hands off to the ResumeIn follow-on - all
    /// through the ONE active [`FadeSlot`]. A rejected/unresolvable spec degrades
    /// to a plain [`Self::play_index`] so a skip never gets stuck.
    async fn skip_with_fade(&self, idx: usize) -> Result<(), String> {
        // (a) Pre-resolve the target's play args (sync). A resolution failure
        // degrades to the plain path rather than dipping into a dead end.
        let item = {
            let st = self.state.lock().unwrap();
            st.queue.get(idx).cloned()
        };
        let Some(item) = item else { return Err("Bad song index".into()) };
        let play = match self.resolve_play(&item) {
            Ok(p) => p,
            Err(_) => return self.play_index(idx).await,
        };

        // (b) Baseline + the resume target dB.
        let baseline = self.state.lock().unwrap().target_volume;
        let resume_db = mpv_volume_to_db(baseline as f64);
        let dur = self.skip_fade_dur();

        // (d) Pre-build the ResumeIn spec (SKIP_DIP_DB -> baseline), deliberate,
        // clamp-up: the dip bottoms out at the shallow skip floor, so the follow-on
        // rises FROM that floor (not full silence) - which is what keeps the skip
        // short. Built here (from a fixed from_db) so the dip terminal does no
        // handler-side work under the slot lock. A build failure degrades to the
        // plain path.
        let resume_spec =
            match self.build_deliberate_spec(SKIP_DIP_DB, FadeTarget::Db(resume_db), dur) {
                Ok(s) => s,
                Err(_) => return self.play_index(idx).await,
            };

        // (c) Report the TARGET immediately during the dip (WITHOUT mutating
        // `current`): status/MPRIS/currentsong collapse the dip window to the
        // target at once.
        self.state.lock().unwrap().pending_skip = Some(idx);
        self.notify_change();

        // (c2) NEAR-EOF GUARD (finding 1a - the PRIMARY no-bleed defense): a warm
        // appends the target as a 2nd playlist entry, and mpv AUTO-ADVANCES into it the
        // instant the CURRENT track hits its natural EOF (keep-open=always does NOT stop
        // a non-last entry - see MpvPlayer::spawn). So we DECLINE the warm outright
        // whenever the current track has less than (the dip duration + a margin) left,
        // or its duration is unknown, or it is a live/continuous stream with no end (see
        // current_can_warm) - so a warm is NEVER parked in a window where the current
        // could EOF before the trough switch. There the warm's payoff cannot land
        // anyway, and declining keeps the skip on today's proven trough loadfile-replace.
        // On decline we ALSO drop any warm a PRIOR skip parked
        // (a second skip that lands in the guard window must not leave the first skip's
        // warm entry auto-advancing behind the current track - finding 3, second-skip).
        let warm_ok = self.current_can_warm();
        if warm_ok {
            // (c3) WARM the target stream in the BACKGROUND during the dip: mpv opens +
            // demuxes + decodes the appended entry off the audible chain, so the trough
            // switch (Terminal::SkipLoad -> switch_warmed) lands near-instant instead of
            // paying the network first-byte cost at the bottom of the dip - collapsing the
            // moment-of-silence artifact. Purely best-effort and PURE GAIN: the warmed
            // entry is NOT routed to output (no bleed at the shallow duck), and a warm
            // failure just degrades switch_warmed to today's trough loadfile - never worse,
            // never a panic or silence. Errors are ignored here on purpose.
            let _ = self.player.prefetch_warm(&play.url).await;
        } else {
            // Declined: clear any stale warm from an earlier skip so switch_warmed falls
            // back to loadfile-replace and no parked entry can auto-advance.
            let _ = self.player.drop_warm().await;
        }

        // (e) Install the deliberate dip-out to silence -> Terminal::SkipLoad.
        match self.install_skip_dip(dur, idx, play, resume_spec, baseline).await {
            Ok(()) => Ok(()),
            Err(e) => {
                tracing::warn!(error = %e, "skip dip rejected; plain play");
                self.state.lock().unwrap().pending_skip = None;
                self.play_index(idx).await
            }
        }
    }

    /// Install the skip dip via the SAME [`FadeSlot::supersede`] body
    /// [`Self::start_fade_spec`] uses: build a DELIBERATE dip-to-silence from the
    /// live gain paired with a [`Terminal::SkipLoad`], and (once validated) abort
    /// the in-flight fade and spawn it under the slot lock. Install-and-return, so
    /// a second skip can supersede the dip before its terminal loads.
    async fn install_skip_dip(
        &self,
        dur: Duration,
        idx: usize,
        play: ResolvedPlay,
        resume_spec: FadeSpec,
        resume_vol: u8,
    ) -> Result<(), FadeError> {
        let tick = Duration::from_millis(self.fade_cfg.tick_ms);
        let synth_floor = self.fade_cfg.synth_floor_db;
        let min_slew = Duration::from_millis(self.fade_cfg.min_slew_ms);
        let cfg = self.fade_cfg.clone();
        let state_read = self.state.clone();
        let state_task = self.state.clone();
        let changed = self.changed.clone();
        let sink = self.player.clone();
        let slot_for_task = self.fade.clone();

        self.fade
            .supersede(
                move || {
                    // Read the live gain AFTER the outgoing fade is aborted+joined
                    // (validate-before-abort keeps this untouched on rejection).
                    let from_db = state_read.lock().unwrap().live_gain_db;
                    // A shallow duck to SKIP_DIP_DB (not full silence) so the dip
                    // is a few 250ms steps, not ~20 - what keeps a skip snappy.
                    let target = FadeTarget::Db(SKIP_DIP_DB);
                    let step_interval = tick.max(min_slew);
                    let eff_dur =
                        dur.max(min_deliberate_dur(from_db, target, step_interval, synth_floor));
                    let bounds = startle_bounds(&cfg, false);
                    let spec =
                        FadeSpec::new(from_db, target, eff_dur, tick, Curve::DbLinear, bounds)?;
                    let terminal = Terminal::SkipLoad {
                        idx,
                        play,
                        resume_spec,
                        resume_vol,
                        dip_floor_db: SKIP_DIP_DB,
                    };
                    Ok((spec, terminal))
                },
                move |(spec, terminal)| {
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
            .await
    }

    /// Resolve and start playing the queue item at `idx`. Returns an ACK-style
    /// error string on failure. A fresh-play gesture (`next`/`play`/`prev`/Eof
    /// advance) resyncs mpv's gain to the baseline first - see
    /// [`Self::play_index_inner`].
    async fn play_index(&self, idx: usize) -> Result<(), String> {
        self.play_index_inner(idx, true).await
    }

    /// As [`Self::play_index`] but WITHOUT resyncing mpv's gain: the caller
    /// (`wake_now` / `restore`) has deliberately forced start-from-silence
    /// (`live_gain_db = synth_floor`, `player.set_volume(0)`) before the first
    /// buffer and owns the rise via a following wake ramp. Resyncing here would
    /// clobber that silence and defeat the ramp.
    async fn play_index_from_silence(&self, idx: usize) -> Result<(), String> {
        self.play_index_inner(idx, false).await
    }

    /// Resolve and start playing the queue item at `idx`. When `resync_volume` is
    /// set, any in-flight fade is cancelled and mpv's gain re-asserted to the
    /// baseline BEFORE loading, so a fresh-play gesture supersedes a pause/fade in
    /// progress (see the two failure modes documented at the cancel below).
    async fn play_index_inner(&self, idx: usize, resync_volume: bool) -> Result<(), String> {
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
        if resync_volume {
            // A fresh-play gesture supersedes any in-flight fade (e.g. a PauseOut
            // ramp from a pause-then-next gesture, or a plain fade-out): atomically
            // cancel it and settle the baseline, THEN re-assert the real mpv gain to
            // the baseline BEFORE loading. Otherwise a surviving PauseOut fade would
            // drive the freshly started track down to silence and its
            // Terminal::Pause would freeze the deck Paused on the new track; and
            // even an already-completed pause fade leaves mpv's volume at ~0 (it
            // persists across loadfile), so without this the new track would play
            // inaudible while getvol/MPRIS report the baseline. Mirrors the stop
            // path (`stop_playback`).
            self.fade
                .cancel_with(|| {
                    let mut st = self.state.lock().unwrap();
                    let v = st.target_volume;
                    st.set_manual_volume(v);
                    // A fresh-play gesture supersedes any pending pause: the deck is
                    // playing a track now, so the reported state must be Playing, and
                    // a superseded PauseOut fade must never freeze this new track
                    // Paused.
                    st.pending_pause = false;
                })
                .await;
            let baseline = self.state.lock().unwrap().target_volume;
            let _ = self.player.set_volume(baseline).await;
        }
        // Resolve the play args (sync stream_url) then load - the SAME resolution
        // the skip dip pre-computes, factored into one place.
        let play = self.resolve_play(&item)?;
        self.player
            .play_url(play.song_id, Some(play.qid), &play.url)
            .await
            .map_err(|e| e.to_string())?;
        {
            let mut st = self.state.lock().unwrap();
            st.current = Some(idx);
            // A track is now current: the pending fresh-enqueue gesture is consumed /
            // superseded by playback, so the anchor must clear (the NowPlaying branch
            // wins the seed now). This is the universal choke every fresh play, EOF
            // auto-advance, and PlayNow funnels through - the primary clear that keeps a
            // stale anchor from overriding recency on a LATER finish (scenario R).
            st.fresh_enqueue_anchor = None;
        }
        self.notify_change();
        Ok(())
    }

    /// Add an entry by uri. A `song/<id>` uri resolves Subsonic metadata; an
    /// absolute `http://`/`https://` uri is queued as a raw stream (internet
    /// radio) played verbatim, with NO Subsonic call, id, rating, or scrobble -
    /// exactly as MPD's own `add <url>` behaves. Returns the assigned MPD id.
    async fn enqueue_uri(&self, uri: &str) -> Result<u64, String> {
        // `album/<id>` fans the whole album into the queue as ONE atomic push: the
        // songs are resolved BEFORE the std Mutex is taken (never hold it across an
        // await), then every track lands under a single lock with ONE
        // playlist_version bump and ONE notify_change - so idle/MPRIS see one queue
        // change, not a per-song wake burst, and a client cannot observe a
        // half-added album. Returns the FIRST assigned id (MPD addid semantics).
        if let Some(id) = uri.strip_prefix("album/") {
            let songs = self
                .client
                .album_songs(&AlbumId(id.to_string()))
                .await
                .map_err(|e| e.to_string())?;
            if songs.is_empty() {
                return Err(format!("no such album: {uri}"));
            }
            let mut st = self.state.lock().unwrap();
            let first = st.next_id;
            for song in songs {
                let qid = st.next_id;
                st.next_id += 1;
                st.queue.push(QueueItem { id: qid, entry: QueueEntry::Song(song) });
            }
            st.playlist_version += 1;
            drop(st);
            self.notify_change();
            // A fresh idle enqueue outranks a prior finish (see enqueue_songs).
            self.arm_fresh_enqueue_anchor(first);
            return Ok(first);
        }
        // A `station/<name>` uri enqueues a SAVED internet radio station BY NAME:
        // resolve the name to its raw stream URL (case-insensitive) via the live
        // station set, then fall through to the raw-stream push below. The list
        // fetch is an await, so it happens BEFORE the std lock is ever taken (never
        // across it). The URL is the station's identity; the name is recovered live
        // from ICY (stream_meta) or falls back to the URL Title, exactly like any
        // raw stream - so `add station/<name>` then `play` plays it by name.
        let resolved_station_url;
        let uri = if let Some(name) = uri.strip_prefix("station/") {
            let stations = self
                .client
                .get_internet_radio_stations()
                .await
                .map_err(|e| e.to_string())?;
            resolved_station_url = station_url_for_name(&stations, name)
                .ok_or_else(|| format!("no such station: {name}"))?;
            resolved_station_url.as_str()
        } else {
            uri
        };
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
        // A fresh idle enqueue outranks a prior finish (see enqueue_songs).
        self.arm_fresh_enqueue_anchor(id);
        Ok(id)
    }

    /// Append an already-resolved [`Song`] to the queue, returning its MPD id.
    /// This is the shared, INFALLIBLE push path (no network, no parse): it mirrors
    /// [`enqueue_uri`](Self::enqueue_uri)'s id/version/notify bookkeeping. Used by
    /// the plan heuristic enqueue (`plan_enqueue`), which backs BOTH the append-only
    /// `Enqueue` and the autoplay `PlayNow` actions - so it deliberately does NOT
    /// arm the fresh-enqueue anchor (a `PlayNow` track legitimately becomes `current`
    /// and wins the seed anyway; the idle-arm belongs on the non-autoplay batch path,
    /// [`enqueue_songs`](Self::enqueue_songs), and the append-only action seam).
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

/// Overlay a raw stream's LIVE ICY metadata onto its rendered `currentsong` pairs:
/// replace the `Title` (the URL fallback from [`song_pairs`]) with the ICY
/// now-playing line when present, and add a `Name` with the station (icy-name),
/// matching real MPD's convention (Name: = station, Title: = now-playing). A `None`
/// field leaves that pair untouched (the URL Title stays), so a stream advertising
/// only one of the two still surfaces what it has. The caller gates this to a
/// [`QueueEntry::Stream`] whose id matches the stored slot, so a library song can
/// never inherit a station's label.
fn apply_stream_meta(pairs: &mut Vec<(String, String)>, meta: &StreamMeta) {
    if let Some(title) = &meta.title {
        if let Some(slot) = pairs.iter_mut().find(|(k, _)| k == "Title") {
            slot.1 = title.clone();
        } else {
            pairs.push(("Title".to_string(), title.clone()));
        }
    }
    if let Some(name) = &meta.name {
        pairs.push(("Name".to_string(), name.clone()));
    }
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

/// Resets the handler's in-flight `identify` debounce flag on drop (task f7vnd3i),
/// so a recognition that short-circuits, errors, panics, or times out ALWAYS
/// releases the guard - a hung Shazam call can never wedge the trigger forever.
struct RecognizingGuard<'a>(&'a AtomicBool);

impl Drop for RecognizingGuard<'_> {
    fn drop(&mut self) {
        self.0.store(false, Ordering::Release);
    }
}

/// Build the `identify` HIT response as nl_echo-style pairs so a caller sees the
/// recognized track immediately. The recognized `Title` also rides `currentsong`
/// via `stream_meta` (so the dj CLI now-playing card reflects it with no client
/// change); these structured pairs additionally give programmatic callers (dj-gui)
/// the split artist / title / album / cover fields. Only the present fields are
/// emitted (a partial Shazam hit stays honest).
fn identify_hit_response(
    track: &crate::recognize::RecognizedTrack,
    now_playing: Option<&str>,
) -> MpdResponse {
    let mut b = MpdResponse::pairs();
    if let Some(np) = now_playing {
        b = b.pair("identify", np);
    }
    if let Some(a) = &track.artist {
        b = b.pair("identify_artist", a.as_str());
    }
    if let Some(t) = &track.title {
        b = b.pair("identify_title", t.as_str());
    }
    if let Some(al) = &track.album {
        b = b.pair("identify_album", al.as_str());
    }
    if let Some(c) = &track.cover_url {
        b = b.pair("identify_cover", c.as_str());
    }
    b.build()
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
const ACK_ERROR_EXIST: u32 = 56;

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
                let (vol, qlen, cur, ver, random, repeat, single, consume) = {
                    let st = self.state.lock().unwrap();
                    (
                        // Derived from the live gain so status tracks an in-flight
                        // fade and never desyncs from the envelope.
                        st.reported_volume(),
                        st.queue.len(),
                        // The pending-skip-aware reported current: during an
                        // in-flight user skip this is the TARGET, so song/songid/
                        // duration report the target immediately (mirrors the
                        // pending-pause state override).
                        st.reported_current(),
                        st.playlist_version,
                        st.random,
                        st.repeat,
                        st.single,
                        st.consume,
                    )
                };
                let flag = |b: bool| if b { "1" } else { "0" };
                // The pending-pause-aware, idle-guarded reported state (Paused the
                // instant a pause is requested, not only once the fade freezes mpv).
                let state = self.reported_play_state();
                let state_str = match state {
                    PlayState::Playing => "play",
                    PlayState::Paused => "pause",
                    PlayState::Stopped => "stop",
                };
                let mut b = MpdResponse::pairs()
                    .pair("volume", vol.to_string())
                    .pair("repeat", flag(repeat))
                    .pair("random", flag(random))
                    .pair("single", flag(single))
                    .pair("consume", flag(consume))
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
                // Surface the armed human-features (sleep / wind-down / wake) as
                // X- extension pairs, present ONLY when armed so status stays lean.
                for (k, v) in self.armed_feature_pairs() {
                    b = b.pair(k, v);
                }
                // Surface the active latent-field pulls as X- extension pairs, present
                // ONLY when a pull is active so the passive HUD auto-clears at rest.
                for (k, v) in self.field_feature_pairs() {
                    b = b.pair(&k, v);
                }
                // Surface the single most-pertinent context string as the ambient
                // hint, present ONLY for a just-finished or up-next seed (NowPlaying is
                // suppressed here so the pane is never duplicated, None keeps it lean).
                for (k, v) in self.ambient_hint_pairs() {
                    b = b.pair(k, v);
                }
                // Surface the end-of-queue continuation arming (toggle ON + a
                // configured station) so a client renders the standing "then:
                // <station>" queue-tail hint. Present ONLY when armed (lean at rest).
                for (k, v) in self.continuation_status_pairs() {
                    b = b.pair(k, v);
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
                match st.reported_current().and_then(|i| st.queue.get(i).map(|it| (i, it))) {
                    Some((pos, item)) => {
                        let mut pairs = song_pairs(item, pos);
                        // Decorate a RAW STREAM's current row with its LIVE ICY metadata
                        // (station -> Name, now-playing -> Title, replacing the URL
                        // fallback), but ONLY when the stored slot's identity matches this
                        // exact entry - so a library song never inherits a station label
                        // and a stale slot from a prior stream never leaks onto a new one.
                        if matches!(item.entry, QueueEntry::Stream { .. }) {
                            if let Some((qid, meta)) = &st.stream_meta {
                                if *qid == QueueId(item.id) {
                                    apply_stream_meta(&mut pairs, meta);
                                }
                            }
                            // Surface a recognized Shazam cover toward the dj-gui art
                            // pane as an `X-CoverArt` extension, ONLY when the stored
                            // slot's identity matches this exact entry (task f7vnd3i).
                            if let Some((qid, url)) = &st.recognized_cover {
                                if *qid == QueueId(item.id) {
                                    pairs.push(("X-CoverArt".to_string(), url.clone()));
                                }
                            }
                        }
                        MpdResponse::Pairs(pairs)
                    }
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
                // Startle-safe transport: PAUSE fades to silence THEN pauses; RESUME
                // unpauses from silence THEN fades in. set_pause notifies on the
                // transition (the pause edge fires from the fade terminal).
                match self.set_pause(want).await {
                    Ok(()) => MpdResponse::ok(),
                    Err(e) => ack(ACK_ERROR_UNKNOWN, "pause", &e.to_string()),
                }
            }
            MpdCommand::Stop => {
                // Manual wins ATOMICALLY: cancel (abort+join) any fade AND drop the
                // stale live-fade level back to the baseline under the SAME slot
                // lock, so no concurrent `fade` can slip in between (see
                // stop_playback: the stop and the mpv re-assert are sequenced after).
                self.stop_playback().await;
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
            MpdCommand::Plan(cmd) => self.handle_plan(cmd).await,
            MpdCommand::Nl(cmd) => self.handle_nl(cmd).await,
            MpdCommand::Sleep(cmd) => self.handle_sleep(cmd),
            MpdCommand::Winddown(cmd) => self.handle_winddown(cmd),
            MpdCommand::Wake(cmd) => self.handle_wake(cmd),
            MpdCommand::Field(cmd) => self.handle_field(cmd),
            MpdCommand::Identify => self.identify().await,
            MpdCommand::Next => {
                // A manual `next` always advances (single governs only auto-advance);
                // random/repeat/consume are honored via plan_next. The transition
                // itself is startle-safe: user_skip dips through silence when
                // playing, falling back to a plain load when paused/stopped.
                match self.plan_next(false) {
                    Some(idx) => match self.user_skip(idx).await {
                        Ok(()) => MpdResponse::ok(),
                        Err(e) => ack(ACK_ERROR_NO_EXIST, "next", &e),
                    },
                    None => MpdResponse::ok(),
                }
            }
            MpdCommand::Previous => {
                let prev = {
                    let st = self.state.lock().unwrap();
                    // From the REPORTED current (an in-flight skip target), so a
                    // second Previous steps back from IT, not the old track.
                    st.reported_current().and_then(|c| c.checked_sub(1))
                };
                match prev {
                    Some(idx) => match self.user_skip(idx).await {
                        Ok(()) => MpdResponse::ok(),
                        Err(e) => ack(ACK_ERROR_NO_EXIST, "previous", &e),
                    },
                    None => MpdResponse::ok(),
                }
            }
            MpdCommand::Seek { secs, .. } => match self.player.seek(secs).await {
                Ok(()) => MpdResponse::ok(),
                Err(e) => ack(ACK_ERROR_UNKNOWN, "seek", &e.to_string()),
            },
            MpdCommand::SeekCur { secs, relative } => {
                // A relative seek (`seekcur +/-N`) is computed against the live
                // lockless position; the player itself only seeks ABSOLUTELY.
                let target = if relative {
                    (self.last_elapsed_secs() + secs).max(0.0)
                } else {
                    secs
                };
                match self.player.seek(target).await {
                    Ok(()) => {
                        // Advance the lockless position to where we just seeked, so
                        // rapid successive relative scrubs (Space/Backspace held or
                        // tapped between Ticks) accumulate from the new playhead
                        // instead of collapsing onto the same stale Tick base. The
                        // next TimePos Tick corrects any drift.
                        self.note_elapsed_ms((target * 1000.0) as u64);
                        MpdResponse::ok()
                    }
                    Err(e) => ack(ACK_ERROR_UNKNOWN, "seek", &e.to_string()),
                }
            }
            MpdCommand::SeekId { secs, .. } => match self.player.seek(secs).await {
                Ok(()) => MpdResponse::ok(),
                Err(e) => ack(ACK_ERROR_UNKNOWN, "seekid", &e.to_string()),
            },
            MpdCommand::SetVol(v) => {
                // Graduated + humanized: GLIDE to the target through the one
                // FadeSlot (epoch-guarded supersede = manual-wins, last-drag-wins)
                // instead of snapping. See `glide_to_volume`.
                self.glide_to_volume(v).await;
                MpdResponse::ok()
            }
            MpdCommand::Knob(dir) => {
                let _ = self.knob(dir).await;
                MpdResponse::ok()
            }
            MpdCommand::GetVol => {
                let v = self.state.lock().unwrap().reported_volume();
                MpdResponse::pairs().pair("volume", v.to_string()).build()
            }
            MpdCommand::Random(on) => {
                self.state.lock().unwrap().random = on;
                self.notify_change();
                MpdResponse::ok()
            }
            MpdCommand::Repeat(on) => {
                self.state.lock().unwrap().repeat = on;
                self.notify_change();
                MpdResponse::ok()
            }
            MpdCommand::Single(on) => {
                self.state.lock().unwrap().single = on;
                self.notify_change();
                MpdResponse::ok()
            }
            MpdCommand::Consume(on) => {
                self.state.lock().unwrap().consume = on;
                self.notify_change();
                MpdResponse::ok()
            }
            MpdCommand::Continuation(cmd) => self.handle_continuation(cmd).await,

            // ── queue ─────────────────────────────────────────────────────
            MpdCommand::Add(uri) => match self.enqueue_uri(&uri).await {
                // enqueue_uri itself arms the fresh-enqueue anchor on a fresh idle
                // enqueue, so the hint/seed moves to what was just added.
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
                // Advertise the synthetic `Starred` playlist (the star trigger)
                // PLUS every real Navidrome playlist, so a `save`d set is visible
                // and loadable rather than write-only.
                let mut resp = MpdResponse::pairs()
                    .pair("playlist", "Starred")
                    .pair("Last-Modified", "1970-01-01T00:00:00Z");
                match self.client.get_playlists().await {
                    Ok(playlists) => {
                        for p in playlists {
                            resp = resp
                                .pair("playlist", &p.name)
                                .pair("Last-Modified", "1970-01-01T00:00:00Z");
                        }
                        resp.build()
                    }
                    Err(e) => ack(ACK_ERROR_UNKNOWN, "listplaylists", &e.to_string()),
                }
            }
            MpdCommand::ListPlaylistInfo(name) if name == "Starred" => {
                // Starred is NEVER cached (freshness-critical). Record the order
                // so a later position-based playlistdelete maps to a song id.
                match self.starred_songs_recording_order().await {
                    Ok(songs) => {
                        let mut pairs = Vec::new();
                        for s in &songs {
                            pairs.extend(browse_song_pairs(s));
                        }
                        MpdResponse::Pairs(pairs)
                    }
                    Err(e) => ack(ACK_ERROR_UNKNOWN, "listplaylistinfo", &e.to_string()),
                }
            }
            MpdCommand::Load(name) if name == "Starred" => {
                // `load Starred` appends the starred songs to the queue (real MPD
                // `load` semantics), not just echoes them. Record the order too.
                match self.starred_songs_recording_order().await {
                    Ok(songs) => {
                        self.enqueue_songs(songs);
                        MpdResponse::ok()
                    }
                    Err(e) => ack(ACK_ERROR_UNKNOWN, "load", &e.to_string()),
                }
            }
            MpdCommand::ListPlaylistInfo(name) => {
                // A real Navidrome playlist: return its songs so a `save`d set can
                // be inspected. Unknown name is a loud ACK, not a silent empty ok.
                match self.playlist_by_name(&name).await {
                    Ok(Some(pl)) => {
                        let mut pairs = Vec::new();
                        for s in &pl.songs {
                            pairs.extend(browse_song_pairs(s));
                        }
                        MpdResponse::Pairs(pairs)
                    }
                    Ok(None) => {
                        ack(ACK_ERROR_NO_EXIST, "listplaylistinfo", "No such playlist")
                    }
                    Err(e) => ack(ACK_ERROR_UNKNOWN, "listplaylistinfo", &e.to_string()),
                }
            }
            MpdCommand::Load(name) => {
                // `load <name>` appends a real Navidrome playlist's songs to the
                // queue, so a `save`d set round-trips back into the queue.
                match self.playlist_by_name(&name).await {
                    Ok(Some(pl)) => {
                        self.enqueue_songs(pl.songs);
                        MpdResponse::ok()
                    }
                    Ok(None) => ack(ACK_ERROR_NO_EXIST, "load", "No such playlist"),
                    Err(e) => ack(ACK_ERROR_UNKNOWN, "load", &e.to_string()),
                }
            }
            MpdCommand::Save(name) => {
                // `save <name>` persists the CURRENT QUEUE as a new Navidrome
                // playlist (GAP cusq3zaw). `Starred` is reserved to the star
                // path - never save over it (a loud ACK, not a silent clobber).
                if name == "Starred" {
                    return ack(ACK_ERROR_EXIST, "save", "Starred is reserved");
                }
                let song_ids = self.queue_song_ids();
                if song_ids.is_empty() {
                    return ack(ACK_ERROR_UNKNOWN, "save", "queue is empty");
                }
                match self.client.create_playlist(&name, &song_ids).await {
                    Ok(_) => {
                        self.notify_change();
                        MpdResponse::ok()
                    }
                    Err(e) => ack(ACK_ERROR_UNKNOWN, "save", &e.to_string()),
                }
            }
            MpdCommand::PlaylistAdd(name, uri) if name == "Starred" => {
                // The uri PREFIX is the sole routing authority: `song/<id>` stars
                // a song, `album/<id>` an album, `artist/<id>` an artist. Anything
                // else fails LOUD rather than falling to the silent generic arm.
                match Favorite::from_uri(&uri) {
                    Some(fav) => match self.client.star(&fav).await {
                        Ok(()) => {
                            self.bust_star_caches();
                            // Reflect the star on any matching queued entry so the
                            // Now Playing heart appears immediately (before a
                            // re-fetch). Only on a CONFIRMED successful star.
                            if let Some(sid) = song_id_from_uri(&uri) {
                                self.set_queue_starred(&sid, true);
                            }
                            self.notify_change();
                            MpdResponse::ok()
                        }
                        Err(e) => ack(ACK_ERROR_UNKNOWN, "playlistadd", &e.to_string()),
                    },
                    None => ack(ACK_ERROR_NO_EXIST, "playlistadd", "unsupported uri"),
                }
            }
            MpdCommand::PlaylistAdd(name, uri) if name == "Stations" => {
                // `playlistadd Stations <streamUrl>` saves <streamUrl> as a NEW
                // Navidrome internet radio station (task cchte88), mirroring the
                // `Starred` sentinel the codebase already blesses. The default label
                // reuses the LIVE icy-name of the currently-playing stream when it is
                // THIS url (stream_meta from jmrwr99), else falls back to the raw URL
                // (the NTS-mixtape no-ICY case). A non-http uri fails LOUD rather than
                // creating a garbage station.
                let uri = uri.trim();
                if !is_stream_uri(uri) {
                    return ack(ACK_ERROR_NO_EXIST, "playlistadd", "not a stream url");
                }
                // Compute the default name under the std lock, dropping it BEFORE the
                // network await (Mutex-never-across-await), exactly like `save`.
                let station_name = self.default_station_name(uri);
                match self
                    .client
                    .create_internet_radio_station(uri, &station_name, None)
                    .await
                {
                    Ok(()) => {
                        self.notify_change();
                        MpdResponse::ok()
                    }
                    Err(e) => ack(ACK_ERROR_UNKNOWN, "playlistadd", &e.to_string()),
                }
            }
            MpdCommand::PlaylistAdd(name, uri) => {
                // Non-`Starred`: append the resolved song to a REAL Navidrome
                // playlist, create-or-append by name (GAP cusq3zaw). Map the MPD
                // uri back to a SongId exactly as the Starred path resolves a
                // Favorite; a non-`song/` uri fails LOUD (never a silent no-op).
                let id = match song_id_from_uri(&uri) {
                    Some(id) => id,
                    None => return ack(ACK_ERROR_NO_EXIST, "playlistadd", "unsupported uri"),
                };
                match self.playlist_add_song(&name, id).await {
                    Ok(()) => {
                        self.notify_change();
                        MpdResponse::ok()
                    }
                    Err(e) => ack(ACK_ERROR_UNKNOWN, "playlistadd", &e.to_string()),
                }
            }
            MpdCommand::PlaylistDelete(name, pos) if name == "Starred" => {
                // Position-based: map to the song id from the last listed order.
                let target = {
                    let st = self.state.lock().unwrap();
                    st.last_starred_order.get(pos).cloned()
                };
                match target {
                    Some(id) => match self.client.unstar(&Favorite::Song(id.clone())).await {
                        Ok(()) => {
                            self.bust_star_caches();
                            // Symmetrically clear the heart on any matching queued
                            // entry so it disappears LIVE (before a re-fetch). Only
                            // on a CONFIRMED successful unstar.
                            self.set_queue_starred(&id, false);
                            self.notify_change();
                            MpdResponse::ok()
                        }
                        Err(e) => ack(ACK_ERROR_UNKNOWN, "playlistdelete", &e.to_string()),
                    },
                    None => ack(ACK_ERROR_NO_EXIST, "playlistdelete", "Bad song index"),
                }
            }
            MpdCommand::PlaylistDelete(name, pos) => {
                // A real Navidrome playlist: remove the song at `pos` via
                // updatePlaylist(songIndexToRemove). Per MPD semantics an unknown
                // name / out-of-range index is a LOUD ack, never a silent no-op.
                // Removing the last remaining song deletes the whole playlist
                // (deletePlaylist) so an empty stored playlist is not left behind.
                match self.playlist_by_name(&name).await {
                    Ok(Some(pl)) => {
                        if pos >= pl.songs.len() {
                            return ack(ACK_ERROR_NO_EXIST, "playlistdelete", "Bad song index");
                        }
                        let result = if pl.songs.len() == 1 {
                            self.client.delete_playlist(&pl.id).await
                        } else {
                            self.client.remove_from_playlist(&pl.id, pos as u32).await
                        };
                        match result {
                            Ok(()) => {
                                self.notify_change();
                                MpdResponse::ok()
                            }
                            Err(e) => ack(ACK_ERROR_UNKNOWN, "playlistdelete", &e.to_string()),
                        }
                    }
                    Ok(None) => ack(ACK_ERROR_NO_EXIST, "playlistdelete", "No such playlist"),
                    Err(e) => ack(ACK_ERROR_UNKNOWN, "playlistdelete", &e.to_string()),
                }
            }
            MpdCommand::PlaylistClear(name) if name == "Starred" => {
                // Starred is the synthetic star-trigger pseudo-playlist, not a real
                // stored playlist; `playlistclear Starred` must NOT fan out into
                // mass-unstarring. Keep it a well-formed no-op ok (Starred special).
                MpdResponse::ok()
            }
            MpdCommand::PlaylistClear(name) => {
                // Clear a real Navidrome playlist by removing it (deletePlaylist).
                // Unknown name is a LOUD ack; a failed delete surfaces a proper ACK
                // error rather than a silent success.
                match self.playlist_by_name(&name).await {
                    Ok(Some(pl)) => match self.client.delete_playlist(&pl.id).await {
                        Ok(()) => {
                            self.notify_change();
                            MpdResponse::ok()
                        }
                        Err(e) => ack(ACK_ERROR_UNKNOWN, "playlistclear", &e.to_string()),
                    },
                    Ok(None) => ack(ACK_ERROR_NO_EXIST, "playlistclear", "No such playlist"),
                    Err(e) => ack(ACK_ERROR_UNKNOWN, "playlistclear", &e.to_string()),
                }
            }

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
                    "save",
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
        // Mirror the MPD `next` gesture: always advance, honoring
        // random/repeat/consume (single governs only EOF auto-advance).
        if let Some(idx) = self.plan_next(false) {
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
        // Same graduated + humanized glide as the MPD setvol path: a GNOME slider
        // drag = many rapid Glides, each superseding the last (follows the finger).
        self.glide_to_volume(vol).await;
    }

    /// Graduated + humanized absolute volume: GLIDE to `v` through the one FadeSlot
    /// (never snap), with a small SEEDED sub-JND dither so it never lands exactly
    /// on the rung - the human noise of operating a physical knob. Shared by MPD
    /// `setvol` and the MPRIS Volume setter.
    ///
    /// Manual-wins is preserved AS a glide: the Glide rides the epoch-guarded
    /// supersede (validate-before-abort), so a later set / a rapid slider drag
    /// supersedes the in-flight one cleanly (last wins), curing the old
    /// abort+snap-vs-supersede MPRIS-drag race. `setvol 0` lands EXACTLY 0 and
    /// stays Playing (Glide never takes the off-click pause branch). Mid-glide
    /// getvol/status report the in-flight envelope (fading=true) - honest, not the
    /// final u8 until the glide completes.
    async fn glide_to_volume(&self, v: u8) {
        let v = v.min(100);
        // Draw the dither + compute the landing under ONE State lock, dropped
        // BEFORE any await (never hold State across .await).
        let (target_db, landing_vol) = {
            let mut st = self.state.lock().unwrap();
            if v == 0 {
                // A mute / slider-to-0 must land EXACTLY 0 - dithering UP would
                // un-mute. No dither; target the synth floor as a committed
                // baseline (a Db target, NOT Silence/Pause, so playback continues).
                (mpv_volume_to_db(0.0), 0u8)
            } else {
                // 53-bit uniform in [0,1) -> symmetric [-0.7, 0.7] dB dither. 0.7 dB
                // is sub-JND (barely perceptible) - human noise, no exaggeration.
                let d = splitmix64(&mut st.vol_dither_state);
                let frac = (d >> 11) as f64 / (1u64 << 53) as f64;
                let dither_db = (frac * 2.0 - 1.0) * 0.7;
                let raw_db = mpv_volume_to_db(v as f64) + dither_db;
                // HARD post-clamp: the committed u8 lands within [v-1, v+1] and
                // [0, 100], never above 100. NOTE: near the bottom (v < ~15) the
                // dB-domain dither can map to > 1 vol before this clamp, so the
                // effective dither there is ~0 (quiet levels barely change) - that
                // is acceptable, NOT a bug.
                let landing = db_to_mpv_volume(raw_db).round().clamp(0.0, 100.0) as i32;
                let lo = (v as i32 - 1).max(0);
                let hi = (v as i32 + 1).min(100);
                let landing_vol = landing.clamp(lo, hi) as u8;
                // Keep the sub-JND offset in the audible landing target, but the
                // COMMITTED u8 baseline is the rounded landing_vol. Cap at 0 dB so
                // the envelope never pushes the reported bar above 100.
                let target_db = (mpv_volume_to_db(landing_vol as f64) + dither_db).min(0.0);
                (target_db, landing_vol)
            }
        };
        let req = FadeRequest {
            intent: FadeIntent::Glide { target_db, vol: landing_vol },
            dur: self.glide_fade_dur(),
            commit_logical: Some((target_db, landing_vol)),
        };
        if self.start_fade_spec(req).await.is_err() {
            // Defensive: clamp_dur_up should make a rejection impossible, but never
            // let a setvol become a silent no-op - fall back to the old instant
            // cancel_with + set_manual_volume snap (still manual-wins, atomic).
            self.fade
                .cancel_with(|| self.state.lock().unwrap().set_manual_volume(landing_vol))
                .await;
            // This defensive cancel_with also SUPERSEDES a live skip dip (its SkipLoad
            // never runs), so drop any parked warm target - else the still-playing
            // current track's natural EOF would auto-advance into it (finding 3).
            let _ = self.player.drop_warm().await;
            let _ = self.player.set_volume(landing_vol).await;
        }
        self.notify_change();
    }

    /// Await the next change notification (queue/playback/volume/star). The MPRIS
    /// server loops on this to emit `PropertiesChanged`. Shares the SAME `changed`
    /// Notify that wakes MPD `idle`, so both surfaces refresh off one signal.
    pub async fn changed(&self) {
        self.changed.notified().await;
    }

    /// Back `lsinfo` / `listallinfo`. The root lists the artist directories PLUS
    /// the synthetic top-level browse dirs (Genres/Lists/Radio/Starred/Stations).
    /// Drilling into each dispatches to the feature that backs it.
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
                            pairs.push(("X-SongCount".to_string(), al.song_count.to_string()));
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
                                    pairs.push((
                                        "X-SongCount".to_string(),
                                        al.song_count.to_string(),
                                    ));
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

            // ── Stations: saved internet radio (task cchte88) - NEVER cached ─
            // Each station renders as a `file:` row whose value IS the raw stream
            // URL, plus Title + Name = the station name (real MPD radio convention,
            // matching apply_stream_meta). Because the `file:` value is the http(s)
            // stream URL, ncmpcpp add/play of the row sends `add <url>` / `addid
            // <url>`, which funnels straight through enqueue_uri -> QueueEntry::Stream
            // with zero new play plumbing; live ICY then overrides the label. An
            // empty set is a well-formed empty Pairs, never an ACK (same as an empty
            // Starred).
            Some("Stations") => match self.client.get_internet_radio_stations().await {
                Ok(stations) => station_rows(&stations),
                Err(e) => ack(ACK_ERROR_UNKNOWN, "lsinfo", &e.to_string()),
            },

            Some(_) => MpdResponse::ok(),
        }
    }

    /// The root browse view: synthetic top-level dirs + artist dirs (cached).
    async fn lsinfo_root(&self) -> MpdResponse {
        let mut pairs = Vec::new();
        // Synthetic feature dirs first so they sit at the top of ncmpcpp Browse.
        for d in ["Genres", "Lists", "Radio", "Starred", "Stations"] {
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
        // Funnel through enqueue_songs (ONE atomic push + the shared fresh-enqueue
        // anchor arm), so findadd/searchadd cannot forget the fresh-enqueue seed move.
        self.enqueue_songs(matches);
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

    /// Flip the in-memory `starred` flag on every queued entry whose song id
    /// matches, so the Now Playing heart appears (or clears) LIVE before any
    /// re-fetch. Called ONLY after a CONFIRMED successful star / unstar, so the
    /// in-memory flag never desyncs from the real library state. The std Mutex is
    /// taken and released synchronously here - never held across an `.await`.
    fn set_queue_starred(&self, id: &SongId, starred: bool) {
        let mut st = self.state.lock().unwrap();
        for item in st.queue.iter_mut() {
            if let QueueEntry::Song(song) = &mut item.entry {
                if &song.id == id {
                    song.starred = starred;
                }
            }
        }
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

/// Serialize saved internet radio stations as MPD browse rows (task cchte88). An
/// empty slice yields an empty-but-well-formed `Pairs` response, never an ACK.
fn station_rows(stations: &[Station]) -> MpdResponse {
    let mut pairs = Vec::new();
    for s in stations {
        pairs.extend(station_browse_pairs(s));
    }
    MpdResponse::Pairs(pairs)
}

/// Browse pairs for one station: `file:` is the RAW STREAM URL (so add/play of the
/// row funnels through the stream path with no new plumbing), and both `Title:` and
/// `Name:` carry the station name (real MPD radio convention, matching
/// [`apply_stream_meta`]). No Time/duration - a live stream has none.
fn station_browse_pairs(s: &Station) -> Vec<(String, String)> {
    vec![
        ("file".to_string(), s.stream_url.clone()),
        ("Title".to_string(), s.name.clone()),
        ("Name".to_string(), s.name.clone()),
    ]
}

/// Resolve a saved station's raw stream URL by NAME, matching case-insensitively
/// (ASCII), or `None` when no station carries that name. Pure (no network, no lock)
/// so the by-name resolution is unit-testable; the caller does the
/// getInternetRadioStations fetch. Backs `add station/<name>` (and thus `play` of a
/// station by name) via [`enqueue_uri`](HypodjHandler::enqueue_uri).
fn station_url_for_name(stations: &[Station], name: &str) -> Option<String> {
    stations
        .iter()
        .find(|s| s.name.eq_ignore_ascii_case(name))
        .map(|s| s.stream_url.clone())
}

/// The default label for saving `url` as an internet radio station, given the
/// currently-reported queue item and the stored live stream-metadata slot. The
/// fallback chain: the LIVE icy-name (only when `current` is a [`QueueEntry::Stream`]
/// whose url equals `url` AND the `stream_meta` slot is keyed to THAT entry's qid and
/// carries a non-empty name) -> else the raw `url`. The qid gate mirrors the
/// `currentsong` decoration so a stale slot from a prior stream never mislabels the
/// save; a library-song current or a url mismatch also falls back to the url. Pure
/// (no lock, no network) so the save-default logic is unit-testable directly.
fn resolve_station_name(
    url: &str,
    current: Option<&QueueItem>,
    stream_meta: Option<&(QueueId, StreamMeta)>,
) -> String {
    if let Some(item) = current {
        if let QueueEntry::Stream { url: cur_url, .. } = &item.entry {
            if cur_url == url {
                if let Some((qid, meta)) = stream_meta {
                    if *qid == QueueId(item.id) {
                        if let Some(name) = &meta.name {
                            if !name.trim().is_empty() {
                                return name.clone();
                            }
                        }
                    }
                }
            }
        }
    }
    url.to_string()
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
    // A non-standard hint so the clients can show a heart in Now Playing when the
    // current track is a Subsonic favorite. Emitted ONLY when starred (never a
    // `0` line), so the pair stays well-formed and strict MPD clients (ncmpcpp)
    // swallow the unknown song-row key harmlessly.
    if s.starred {
        p.push(("X-Starred".to_string(), "1".to_string()));
    }
    if let Some(a) = &s.artist {
        p.push(("Artist".to_string(), a.clone()));
    }
    if let Some(a) = &s.album {
        p.push(("Album".to_string(), a.clone()));
    }
    // Non-standard hint so the TUI can group queued songs by album for the browse
    // queue markers. libmpdclient swallows unknown song-row pairs, so this is safe
    // for strict clients; emitted only for a library song (a stream has no album).
    if let Some(al) = &s.album_id {
        p.push(("X-AlbumUri".to_string(), format!("album/{}", al.0)));
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
    use crate::model::StationId;
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

    /// Like [`handler_with_null_player`] but wires a probing NullPlayer so a test
    /// can observe the warm-skip commands the handler issues (prefetch / drop).
    /// Same sandbox `None` guard.
    fn handler_with_probe_player() -> Option<(
        HypodjHandler,
        tokio::sync::mpsc::Receiver<PlayerEvent>,
        std::sync::Arc<crate::player::WarmProbe>,
    )> {
        let cfg = ServerConfig {
            url: "http://127.0.0.1:1/never-called".to_string(),
            username: "u".to_string(),
            password: "p".to_string(),
            client_name: "test".to_string(),
        };
        let client = match std::panic::catch_unwind(|| SubsonicClient::connect(&cfg)) {
            Ok(Ok(c)) => Arc::new(c),
            _ => return None,
        };
        let (player, events, probe) = NullPlayer::spawn_with_probe();
        Some((HypodjHandler::new(client, player), events, probe))
    }

    // A minimal library Song for queue/playlist wiring tests (no network).
    fn playlist_test_song(id: &str) -> Song {
        Song {
            id: SongId(id.to_string()),
            title: format!("Song {id}"),
            album: None,
            album_id: None,
            artist: None,
            track: None,
            duration_secs: Some(200),
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

    #[tokio::test]
    async fn queue_song_ids_collects_songs_in_order_skips_streams() {
        let Some((h, _events)) = handler_with_null_player() else { return };
        h.enqueue_song_for_test(playlist_test_song("s-1")).await;
        h.enqueue_stream_for_test(NTS).await; // raw stream: no song id
        h.enqueue_song_for_test(playlist_test_song("s-2")).await;
        let ids = h.queue_song_ids();
        assert_eq!(ids, vec![SongId("s-1".into()), SongId("s-2".into())]);
    }

    // similar_to_current seeds from the CURRENT song, else the FIRST queued song,
    // and yields NO seed (-> honest 0) when nothing playable is queued. Streams are
    // skipped (no library id). This is the id the daemon fills server-side; the
    // model never names it.
    #[tokio::test]
    async fn similar_seed_id_prefers_current_then_first_queued() {
        let Some((h, _events)) = handler_with_null_player() else { return };
        // Empty queue -> nothing to seed from.
        assert_eq!(h.similar_seed_id(), None);
        // A raw stream first (no id) then two songs: with nothing playing, the seed
        // is the FIRST song, skipping the id-less stream.
        h.enqueue_stream_for_test(NTS).await;
        h.enqueue_song_for_test(playlist_test_song("s-1")).await;
        h.enqueue_song_for_test(playlist_test_song("s-2")).await;
        assert_eq!(h.similar_seed_id(), Some(SongId("s-1".into())));
        // Start playback on the second song -> the seed follows the CURRENT track.
        h.handle(MpdCommand::Play(Some(2))).await;
        assert_eq!(h.similar_seed_id(), Some(SongId("s-2".into())));
    }

    // A natural track-end (the advance_on_eof path) records the finishing library
    // song as the recency seed. A user-typed "more like this one" AFTER the track
    // finished then seeds from that just-finished track, NOT an unrelated first-queued
    // song. Task 0ba1lej (the screenshot scenario).
    #[tokio::test]
    async fn last_finished_set_on_natural_eof_and_seeds_recency() {
        let Some((h, _events)) = handler_with_null_player() else { return };
        // A single real song plays; nothing else queued.
        h.enqueue_song_for_test(playlist_test_song("finished")).await;
        {
            let mut st = h.state.lock().unwrap();
            st.current = Some(0);
            assert!(st.last_finished.is_none(), "nothing has finished yet");
        }
        // The track reaches its natural EOF: end of queue -> current becomes None,
        // and the finishing track is latched as the recency seed.
        h.advance_on_eof().await;
        {
            let st = h.state.lock().unwrap();
            assert_eq!(st.current, None, "end of queue stops the deck");
            assert_eq!(
                st.last_finished.as_ref().map(|s| &s.id),
                Some(&SongId("finished".into())),
                "the finishing track is recorded as last_finished"
            );
        }
        // Nothing is playing and nothing is queued, so the seed is the recently
        // finished track (recency), NOT an honest 0.
        assert_eq!(h.similar_seed_id(), Some(SongId("finished".into())));
    }

    // The strict seed preference order: CURRENT playing wins over RECENCY, which wins
    // over the FIRST-QUEUED song, which beats None (honest 0).
    #[tokio::test]
    async fn similar_seed_order_current_beats_recency_beats_queued() {
        let Some((h, _events)) = handler_with_null_player() else { return };
        // Truly nothing: no current, no finished, no queue -> honest 0.
        assert_eq!(h.similar_seed_id(), None);
        // A recently-finished track only (empty queue) -> recency is the seed.
        h.state.lock().unwrap().last_finished = Some(playlist_test_song("recent"));
        assert_eq!(h.similar_seed_id(), Some(SongId("recent".into())));
        // Add a first-queued song: recency STILL wins over first-queued.
        h.enqueue_song_for_test(playlist_test_song("queued")).await;
        assert_eq!(
            h.similar_seed_id(),
            Some(SongId("recent".into())),
            "recency wins over first-queued"
        );
        // Start playback: the CURRENT playing song wins over recency.
        h.handle(MpdCommand::Play(Some(0))).await;
        assert_eq!(
            h.similar_seed_id(),
            Some(SongId("queued".into())),
            "current playing wins over recency"
        );
    }

    // Fix-3 stream-as-current edge: when the current entry is a live stream (no
    // library id), the seed falls to the recently-finished real track, NOT an
    // unrelated first-queued song.
    #[tokio::test]
    async fn stream_current_falls_to_last_finished() {
        let Some((h, _events)) = handler_with_null_player() else { return };
        // A stream is the current entry; a real song sits later in the queue.
        h.enqueue_stream_for_test(NTS).await;
        h.enqueue_song_for_test(playlist_test_song("queued")).await;
        {
            let mut st = h.state.lock().unwrap();
            st.current = Some(0); // the id-less stream is current
            st.last_finished = Some(playlist_test_song("recent"));
        }
        assert_eq!(
            h.similar_seed_id(),
            Some(SongId("recent".into())),
            "stream-as-current falls to last_finished, not the first-queued song"
        );
    }

    // A stream reaching EOF must NEVER become the seed: it has no library id, so
    // advance_on_eof leaves last_finished untouched (it keeps the last real track).
    #[tokio::test]
    async fn stream_eof_never_overwrites_last_finished() {
        let Some((h, _events)) = handler_with_null_player() else { return };
        h.enqueue_stream_for_test(NTS).await;
        {
            let mut st = h.state.lock().unwrap();
            st.current = Some(0);
            st.last_finished = Some(playlist_test_song("real"));
        }
        h.advance_on_eof().await;
        assert_eq!(
            h.state.lock().unwrap().last_finished.as_ref().map(|s| &s.id),
            Some(&SongId("real".into())),
            "a stream EOF does not clobber the last real finished track"
        );
    }

    // ── end-of-queue CONTINUATION radio (slice 1) ────────────────────────────

    // ARMED + a configured http(s) station: the drain-edge None-branch does NOT stop.
    // It appends the station as a first-class raw Stream, points current at it, and
    // cold-loads it (playing). The finishing library song is still latched as the
    // recency seed - the continuation stream never displaces it.
    #[tokio::test]
    async fn continuation_armed_appends_and_plays_station_on_drain() {
        let Some((h, _events)) = handler_with_null_player() else { return };
        // One real song plays to its natural EOF into an empty tail.
        h.enqueue_song_for_test(playlist_test_song("seed")).await;
        h.state.lock().unwrap().current = Some(0);
        // Arm continuation with an absolute stream URL (resolves with NO network).
        h.state.lock().unwrap().continuation = true;
        h.set_continuation_station(Some(NTS.to_string()));

        h.advance_on_eof().await;

        let st = h.state.lock().unwrap();
        assert_eq!(st.queue.len(), 2, "the continuation stream is appended after the drained song");
        assert!(
            matches!(&st.queue[1].entry, QueueEntry::Stream { url, .. } if url == NTS),
            "the appended tail entry is the configured raw stream"
        );
        assert_eq!(st.current, Some(1), "current points at the continuation stream (playing, not stopped)");
        // The just-finished library song is the recency seed; the stream never became one.
        assert_eq!(
            st.last_finished.as_ref().map(|s| &s.id),
            Some(&SongId("seed".into())),
            "the finished library song stays the recency seed"
        );
    }

    // REGRESSION (the whole point vs the old silent-drain): continuation OFF (default)
    // ends the deck STOPPED at end-of-queue exactly as before, even with a station
    // configured. A configured station does NOTHING until the toggle is armed.
    #[tokio::test]
    async fn continuation_off_ends_stopped_regression() {
        let Some((h, _events)) = handler_with_null_player() else { return };
        h.enqueue_song_for_test(playlist_test_song("seed")).await;
        h.state.lock().unwrap().current = Some(0);
        // Station configured but the toggle is OFF (default).
        h.set_continuation_station(Some(NTS.to_string()));
        assert!(!h.state.lock().unwrap().continuation, "continuation defaults OFF");

        h.advance_on_eof().await;

        let st = h.state.lock().unwrap();
        assert_eq!(st.current, None, "disarmed => the deck ends stopped as today");
        assert_eq!(st.queue.len(), 1, "no continuation stream appended when disarmed");
    }

    // Armed but no station configured (feature effectively off): inert - the deck ends
    // stopped exactly as today, never a guessed station.
    #[tokio::test]
    async fn continuation_armed_but_no_station_stays_inert() {
        let Some((h, _events)) = handler_with_null_player() else { return };
        h.enqueue_song_for_test(playlist_test_song("seed")).await;
        h.state.lock().unwrap().current = Some(0);
        h.state.lock().unwrap().continuation = true;
        // No station set (None). Also an empty/whitespace station must be inert.
        h.set_continuation_station(None);

        h.advance_on_eof().await;

        let st = h.state.lock().unwrap();
        assert_eq!(st.current, None, "no station => inert, deck ends stopped");
        assert_eq!(st.queue.len(), 1, "nothing appended");
    }

    // Armed with a station NAME that does not resolve (the offline never-called client
    // yields no stations): inert - the deck ends stopped, the queue is untouched, and
    // NO retry-loop into a playing-state-with-silence. Never guess a random station.
    #[tokio::test]
    async fn continuation_unresolvable_station_stays_inert() {
        let Some((h, _events)) = handler_with_null_player() else { return };
        h.enqueue_song_for_test(playlist_test_song("seed")).await;
        h.state.lock().unwrap().current = Some(0);
        h.state.lock().unwrap().continuation = true;
        // A NAME (not a URL) => a getInternetRadioStations fetch, which fails against
        // the never-called client, so resolution yields None.
        h.set_continuation_station(Some("No Such Station".to_string()));

        h.advance_on_eof().await;

        let st = h.state.lock().unwrap();
        assert_eq!(st.current, None, "unresolvable station => inert, deck ends stopped");
        assert_eq!(st.queue.len(), 1, "unresolvable station appends nothing");
    }

    // A continuation stream must never poison the recency seed nor become a scrobble
    // seed: it is a raw Stream with no library id, so last_finished keeps the real
    // finished song and queue_song_ids (the scrobble/seed id source) excludes it.
    #[tokio::test]
    async fn continuation_stream_does_not_poison_last_finished_or_scrobble() {
        let Some((h, _events)) = handler_with_null_player() else { return };
        h.enqueue_song_for_test(playlist_test_song("seed")).await;
        h.state.lock().unwrap().current = Some(0);
        h.state.lock().unwrap().continuation = true;
        h.set_continuation_station(Some(NTS.to_string()));

        h.advance_on_eof().await; // drains -> continuation stream now current+playing

        // The continuation stream carries no library id, so the id sources that back
        // scrobbling / similar-seeding see ONLY the real song, never the stream.
        assert_eq!(
            h.queue_song_ids(),
            vec![SongId("seed".into())],
            "the continuation stream contributes no song id (never scrobbles / seeds)"
        );
        assert_eq!(
            h.state.lock().unwrap().last_finished.as_ref().map(|s| &s.id),
            Some(&SongId("seed".into())),
            "the continuation stream never displaces the real recency seed"
        );
        // A SECOND drain (the stream 'ends') must still not latch the id-less stream -
        // and, still ARMED, the one-shot guard must stop it honestly WITHOUT re-firing.
        h.advance_on_eof().await;
        assert_eq!(
            h.state.lock().unwrap().last_finished.as_ref().map(|s| &s.id),
            Some(&SongId("seed".into())),
            "a stream EOF never overwrites last_finished, even under continuation"
        );
        let st = h.state.lock().unwrap();
        assert_eq!(st.current, None, "the continuation stream ends the deck stopped, not re-fired");
        assert_eq!(st.queue.len(), 2, "no SECOND continuation stream appended (one-shot)");
        assert_eq!(st.continuation_active, None, "the active-continuation latch clears on the honest stop");
    }

    // CASE 4 (the held merge): a dead / unreachable / 404 continuation URL. mpv's
    // loadfile Ok is PREMATURE (fires when the load is queued, not when the stream
    // connects), so the cold-start "succeeds" and the deck goes Playing, then the
    // open failure arrives LATER. This test covers the HANDLER half of CASE 4: given
    // that the just-ended entry re-enters advance_on_eof, the one-shot re-entrancy
    // guard must catch that it WAS the continuation stream and stop HONESTLY - never
    // re-fire into an unbounded queue-growing retry loop over silence (the forbidden
    // silent-drain bug in a new hat).
    //
    // WHAT NullPlayer CAN simulate: the premature-Ok cold-start (it accepts any URL,
    // so the stream is appended + becomes current + latches continuation_active) and
    // an Eof-style re-entry into advance_on_eof (driven here by a second call). That
    // is enough to exercise the finishing_is_continuation guard end to end.
    //
    // WHAT NullPlayer CANNOT simulate: the REAL mpv failure surface. A dead URL does
    // NOT fail as EndFile(Error)/Eof; on real libmpv the open failure arrives as a
    // TOP-LEVEL wait_event error (Raw(LoadingFailed) / Raw(NothingToPlay) / ...),
    // which the mpv actor classifies (is_active_load_failure) and routes into the
    // honest-stop path so the Eof that reaches advance_on_eof is SYNTHESIZED there.
    // NullPlayer emits no such error, so this test does NOT prove the actor-side
    // classification / latch-clear / Stopped-publish - that half is covered by the
    // #[ignore] live-libmpv test player::tests::live_mpv_dead_url_stops_honestly (and
    // by the isolated-daemon live proof against a real dead URL). This test asserts
    // only the guard given the Eof, not that a dead URL actually produces one.
    #[tokio::test]
    async fn continuation_dead_url_stops_honestly_no_refire() {
        let Some((h, _events)) = handler_with_null_player() else { return };
        h.enqueue_song_for_test(playlist_test_song("seed")).await;
        h.state.lock().unwrap().current = Some(0);
        h.state.lock().unwrap().continuation = true;
        // An absolute URL resolves with NO network; the NullPlayer's premature Ok
        // models mpv accepting the load before the (here, notionally dead) connect.
        h.set_continuation_station(Some(NTS.to_string()));

        // Drain -> continuation cold-starts: the stream is appended + becomes current.
        h.advance_on_eof().await;
        {
            let st = h.state.lock().unwrap();
            assert_eq!(st.queue.len(), 2, "the continuation stream is appended once");
            assert_eq!(st.current, Some(1), "and becomes current (loadfile Ok is premature)");
            assert_eq!(
                st.continuation_active,
                Some(st.queue[1].id),
                "the active-continuation latch records the cold-started stream id"
            );
        }
        // Re-enter advance_on_eof to model the finishing stream's Eof reaching the
        // spine (on real mpv this Eof is synthesized by the actor from the top-level
        // load-failure error; NullPlayer cannot raise that, so we drive the re-entry
        // directly). STILL ARMED, the guard must stop honestly, NOT re-fire.
        h.advance_on_eof().await;
        let st = h.state.lock().unwrap();
        assert_eq!(st.current, None, "a dead continuation stream ends the deck STOPPED (honest)");
        assert_eq!(
            st.queue.len(),
            2,
            "NO second continuation stream appended - the queue does not grow without bound"
        );
        assert_eq!(st.continuation_active, None, "the active-continuation latch clears on the honest stop");
        assert!(st.continuation, "the arming toggle itself is untouched (one-shot is per-stream, not a disarm)");
    }

    // MEDIUM: a `single`-mode stop is NOT a genuine drain. With `single on` and a track
    // still queued, plan_next returns None by design (stop after the current track),
    // but continuation must NOT treat that as exhaustion and hijack the pending track
    // with radio. The true-drain gate distinguishes the two, so the remaining song is
    // preserved and no continuation stream is appended.
    #[tokio::test]
    async fn continuation_single_stop_with_tracks_queued_does_not_fire() {
        let Some((h, _events)) = handler_with_null_player() else { return };
        h.enqueue_song_for_test(playlist_test_song("s0")).await;
        h.enqueue_song_for_test(playlist_test_song("s1")).await;
        h.state.lock().unwrap().current = Some(0);
        // Arm continuation AND turn single on: s0 ends, single stops the deck, but s1
        // is still pending - a mode-stop None, not a true drain.
        h.state.lock().unwrap().continuation = true;
        h.state.lock().unwrap().single = true;
        h.set_continuation_station(Some(NTS.to_string()));

        h.advance_on_eof().await;

        let st = h.state.lock().unwrap();
        assert_eq!(st.current, None, "single stops the deck after the current track (its semantics)");
        assert_eq!(st.queue.len(), 2, "no continuation stream appended - the pending track is preserved");
        assert!(
            st.queue.iter().all(|it| matches!(it.entry, QueueEntry::Song(_))),
            "the queue still holds ONLY the two library songs (radio never hijacked it)"
        );
        assert_eq!(st.continuation_active, None, "no continuation stream was ever cold-started");
    }

    // The continuation INDICATOR pairs ride `status` ONLY when armed (toggle ON AND a
    // station configured), so a client can render the standing "then: <station>" hint.
    // Disarmed or unconfigured => no pairs (a lean status), like the armed/hint HUD.
    #[tokio::test]
    async fn continuation_status_pairs_present_only_when_armed() {
        let Some((h, _events)) = handler_with_null_player() else { return };
        // Disarmed, no station -> no pairs.
        assert!(h.continuation_status_pairs().is_empty(), "disarmed => lean status");
        // Toggle on but still no station -> still no pairs.
        h.state.lock().unwrap().continuation = true;
        assert!(h.continuation_status_pairs().is_empty(), "armed toggle without a station => lean");
        // Toggle on + a configured station -> the two indicator pairs, end to end on status.
        h.set_continuation_station(Some("NTS 1".to_string()));
        assert_eq!(
            h.continuation_status_pairs(),
            vec![
                ("X-hypodj-continuation", "on".to_string()),
                ("X-hypodj-continuation-station", "NTS 1".to_string()),
            ]
        );
        let status = h.handle(MpdCommand::Status).await;
        assert_eq!(pair(&status, "X-hypodj-continuation"), Some("on"));
        assert_eq!(pair(&status, "X-hypodj-continuation-station"), Some("NTS 1"));
        // Disarm again -> the pairs disappear from status.
        h.handle(MpdCommand::Continuation(ContinuationCmd::Off)).await;
        assert!(!h.state.lock().unwrap().continuation, "off toggles the arming");
        let status = h.handle(MpdCommand::Status).await;
        assert_eq!(pair(&status, "X-hypodj-continuation"), None, "disarmed status is lean again");
    }

    // The `continuation on|off` verb flips the persisted runtime toggle; `status`
    // reports the live toggle + configured station honestly.
    #[tokio::test]
    async fn continuation_verb_toggles_and_reports() {
        let Some((h, _events)) = handler_with_null_player() else { return };
        h.set_continuation_station(Some("NTS 1".to_string()));
        // Default off; the report says so.
        let rep = h.handle(MpdCommand::Continuation(ContinuationCmd::Status)).await;
        assert_eq!(pair(&rep, "continuation"), Some("off"));
        assert_eq!(pair(&rep, "continuation_station"), Some("NTS 1"));
        // Arm it.
        h.handle(MpdCommand::Continuation(ContinuationCmd::On)).await;
        assert!(h.state.lock().unwrap().continuation, "on arms the toggle");
        let rep = h.handle(MpdCommand::Continuation(ContinuationCmd::Status)).await;
        assert_eq!(pair(&rep, "continuation"), Some("on"));
    }

    // Helper: the ambient hint's title (if any) names the SAME song the enqueue seed
    // resolves to. The deterministic test title is "Song <id>", so tying the title back
    // to `similar_seed_id`'s id proves the hint can never name a seed the DJ would not
    // enqueue from (the ids-not-strings invariant).
    fn assert_hint_matches_seed(h: &HypodjHandler) {
        let title = h
            .ambient_hint_pairs()
            .into_iter()
            .find(|(k, _)| *k == "X-hypodj-hint-title")
            .map(|(_, v)| v);
        if let Some(t) = title {
            let seed = h.similar_seed_id().expect("a hint present implies a resolvable seed");
            assert_eq!(t, format!("Song {}", seed.0), "hint title names the seed song");
        }
    }

    // The ambient hint is a PURE re-surfacing of the enqueue seed: across the
    // NowPlaying-suppressed / JustFinished / UpNext(anchor) / UpNext(fallback) branches
    // alike, the surfaced title names the SAME song that similar_seed_id returns. One
    // ordering, two readers - never a divergent ranking.
    #[tokio::test]
    async fn ambient_hint_title_names_same_song_as_seed() {
        let Some((h, _events)) = handler_with_null_player() else { return };
        // UpNext(fallback): nothing played, a queued song is the seed and the hint.
        h.enqueue_song_for_test(playlist_test_song("B")).await;
        assert_hint_matches_seed(&h);
        // JustFinished wins over up-next: the recency seed is the hint now.
        h.state.lock().unwrap().last_finished = Some(playlist_test_song("A"));
        assert_hint_matches_seed(&h);
        // UpNext(anchor) - the G-state: a fresh idle enqueue of D arms the anchor, so the
        // hint moves to D even with A still the recency memory. Hint title == seed.
        h.enqueue_songs(vec![playlist_test_song("D")]);
        assert_eq!(h.similar_seed_id(), Some(SongId("D".into())), "G-state seeds the anchored D");
        assert_hint_matches_seed(&h);
        // NowPlaying-suppressed: a library song current emits no hint, and the helper's
        // "title present implies matching seed" holds vacuously (no title to compare).
        h.state.lock().unwrap().current = Some(0);
        assert!(h.ambient_hint_pairs().is_empty(), "NowPlaying suppresses the hint");
        assert_hint_matches_seed(&h);
    }

    // A natural EOF into an empty queue latches the finished track: the Status response
    // then carries the just-finished ambient hint (kind + captured title), end to end.
    #[tokio::test]
    async fn ambient_hint_just_finished_after_natural_eof() {
        let Some((h, _events)) = handler_with_null_player() else { return };
        h.enqueue_song_for_test(playlist_test_song("finished")).await;
        h.state.lock().unwrap().current = Some(0);
        h.advance_on_eof().await; // natural end-of-queue EOF, nothing playing after
        let status = h.handle(MpdCommand::Status).await;
        assert_eq!(pair(&status, "X-hypodj-hint-kind"), Some("just-finished"));
        assert_eq!(pair(&status, "X-hypodj-hint-title"), Some("Song finished"));
    }

    // A library song currently playing is SUPPRESSED at the daemon (the Now Playing
    // pane already shows it): the Status response carries NO hint pair at all.
    #[tokio::test]
    async fn ambient_hint_suppressed_while_library_song_current() {
        let Some((h, _events)) = handler_with_null_player() else { return };
        h.enqueue_song_for_test(playlist_test_song("now")).await;
        h.state.lock().unwrap().current = Some(0); // a library Song is current
        assert!(h.ambient_hint_pairs().is_empty(), "NowPlaying emits no hint pair");
        let status = h.handle(MpdCommand::Status).await;
        assert_eq!(pair(&status, "X-hypodj-hint-kind"), None);
        assert_eq!(pair(&status, "X-hypodj-hint-title"), None);
    }

    // Nothing played yet, songs queued: the hint is up-next the FIRST queued song.
    #[tokio::test]
    async fn ambient_hint_up_next_when_nothing_played() {
        let Some((h, _events)) = handler_with_null_player() else { return };
        h.enqueue_song_for_test(playlist_test_song("B")).await;
        h.enqueue_song_for_test(playlist_test_song("C")).await;
        assert_eq!(
            h.ambient_hint_pairs(),
            vec![
                ("X-hypodj-hint-kind", "up-next".to_string()),
                ("X-hypodj-hint-title", "Song B".to_string()),
            ]
        );
    }

    // Nothing seedable (empty queue, nothing playing, nothing finished): NO hint pair,
    // keeping status lean exactly like the armed/field HUD at rest.
    #[tokio::test]
    async fn ambient_hint_absent_when_nothing_seedable() {
        let Some((h, _events)) = handler_with_null_player() else { return };
        assert!(h.ambient_hint_pairs().is_empty());
        let status = h.handle(MpdCommand::Status).await;
        assert_eq!(pair(&status, "X-hypodj-hint-kind"), None);
        assert_eq!(pair(&status, "X-hypodj-hint-title"), None);
    }

    // Staleness guard (task yyzbyhl), REALISTIC post-EOF flow: track A finishes and
    // LINGERS at queue pos0 (consume off - the deeper root cause), then a fresh user
    // enqueue [D] lands while stopped. The seed AND the hint must move to the newer D,
    // skipping the lingering finished A - NEVER keep naming stale A. Uses the real EOF
    // path (not an artificially emptied queue) so a future artificial-empty-queue test
    // can never again mask this. Fails without the fresh-enqueue anchor + branch-2 skip.
    #[tokio::test]
    async fn ambient_hint_fresh_enqueue_beats_prior_finish() {
        let Some((h, _events)) = handler_with_null_player() else { return };
        // A single real song plays, then reaches natural EOF into an empty queue with
        // consume OFF: A LINGERS at pos0 (state stop, current None, last_finished A).
        h.enqueue_song_for_test(playlist_test_song("A")).await;
        h.state.lock().unwrap().current = Some(0);
        h.advance_on_eof().await;
        {
            let st = h.state.lock().unwrap();
            assert_eq!(st.queue.len(), 1, "A lingers at the queue head (consume off)");
            assert_eq!(st.current, None, "end of queue stops the deck");
            assert_eq!(
                st.last_finished.as_ref().map(|s| &s.id),
                Some(&SongId("A".into())),
                "A is the recency memory"
            );
        }
        assert_eq!(
            h.ambient_hint_pairs(),
            vec![
                ("X-hypodj-hint-kind", "just-finished".to_string()),
                ("X-hypodj-hint-title", "Song A".to_string()),
            ],
            "before a fresh gesture the hint is just-finished A"
        );
        // The user enqueues a fresh selection D while stopped: the fresh gesture arms
        // the anchor, so the seed/hint move to D, skipping the lingering finished A.
        h.enqueue_songs(vec![playlist_test_song("D")]);
        {
            let st = h.state.lock().unwrap();
            assert_eq!(st.queue.len(), 2, "A still lingering at pos0, D appended at pos1");
            assert!(
                matches!(&st.queue[0].entry, QueueEntry::Song(s) if s.id == SongId("A".into())),
                "A is NOT evicted (consume off)"
            );
        }
        assert_eq!(h.similar_seed_id(), Some(SongId("D".into())), "seed skips A to D");
        assert_eq!(
            h.ambient_hint_pairs(),
            vec![
                ("X-hypodj-hint-kind", "up-next".to_string()),
                ("X-hypodj-hint-title", "Song D".to_string()),
            ],
            "the hint honestly reads up-next D, never just-finished A"
        );
    }

    // The fresh-enqueue anchor fix is WIRED into the user Add command: a stream add
    // (offline, no network) while stopped arms the anchor end to end. The stream itself
    // has no library id, but arming still happens; the recency memory last_finished is
    // KEPT (honest across consume eviction), not wiped.
    #[tokio::test]
    async fn add_command_arms_fresh_anchor_when_idle() {
        let Some((h, _events)) = handler_with_null_player() else { return };
        h.state.lock().unwrap().last_finished = Some(playlist_test_song("A"));
        // Deck is stopped/idle (current None). A user Add lands.
        h.handle(MpdCommand::Add(NTS.to_string())).await;
        let st = h.state.lock().unwrap();
        assert!(st.fresh_enqueue_anchor.is_some(), "a fresh idle enqueue arms the anchor");
        assert_eq!(
            st.last_finished.as_ref().map(|s| s.id.clone()),
            Some(SongId("A".into())),
            "the recency memory is kept, not wiped"
        );
    }

    // The arm now lives at the SHARED batch-append seam (enqueue_songs), so every idle
    // enqueue that funnels through it - `load <name>`, `load Starred`, and
    // findadd/searchadd - arms the anchor and the seed moves to the just-appended track.
    // The recency memory (last_finished) is KEPT, not wiped. Offline: enqueue_songs
    // takes already-resolved Songs.
    #[tokio::test]
    async fn enqueue_songs_arms_fresh_anchor_when_idle() {
        let Some((h, _events)) = handler_with_null_player() else { return };
        h.state.lock().unwrap().last_finished = Some(playlist_test_song("A"));
        // The user loads a fresh selection D while stopped (current None).
        h.enqueue_songs(vec![playlist_test_song("D")]);
        {
            let st = h.state.lock().unwrap();
            assert!(st.fresh_enqueue_anchor.is_some(), "the batch-append seam arms the anchor");
            assert_eq!(
                st.last_finished.as_ref().map(|s| s.id.clone()),
                Some(SongId("A".into())),
                "the recency memory is kept, not wiped"
            );
        }
        assert_eq!(h.similar_seed_id(), Some(SongId("D".into())), "seed moves to D");
    }

    // The shared arm is a NO-OP while a track is current: a batch enqueue behind a
    // playing deck must not arm (the current-song branch wins the seed anyway) and must
    // leave last_finished untouched for when playback stops.
    #[tokio::test]
    async fn enqueue_songs_keeps_recency_seed_while_current() {
        let Some((h, _events)) = handler_with_null_player() else { return };
        {
            let mut st = h.state.lock().unwrap();
            st.last_finished = Some(playlist_test_song("A"));
            st.current = Some(0); // a track is playing
        }
        h.enqueue_songs(vec![playlist_test_song("D")]);
        let st = h.state.lock().unwrap();
        assert_eq!(
            st.last_finished.as_ref().map(|s| s.id.clone()),
            Some(SongId("A".into())),
            "a current track keeps the recency seed untouched"
        );
        assert!(st.fresh_enqueue_anchor.is_none(), "no anchor is armed behind a playing deck");
    }

    // The autoplay-shared push helper (enqueue_song, behind plan_enqueue -> PlayNow)
    // must NOT arm the anchor: a played track legitimately becomes `current` and wins the
    // seed on its own, so the idle-arm stays off this path. last_finished stays untouched.
    #[tokio::test]
    async fn enqueue_song_does_not_arm_fresh_anchor() {
        let Some((h, _events)) = handler_with_null_player() else { return };
        h.state.lock().unwrap().last_finished = Some(playlist_test_song("A"));
        // Idle (current None), but the autoplay helper must not arm and must leave
        // last_finished alone.
        h.enqueue_song(playlist_test_song("D")).await;
        let st = h.state.lock().unwrap();
        assert_eq!(
            st.last_finished.as_ref().map(|s| s.id.clone()),
            Some(SongId("A".into())),
            "the autoplay push path never drops the recency seed"
        );
        assert!(st.fresh_enqueue_anchor.is_none(), "the autoplay push path never arms the anchor");
    }

    // The append-only Enqueue ACTION seam (the DJ NL surface: "queue up some jazz")
    // arms the fresh anchor when fresh music lands on an idle deck - the SAME fix the MPD
    // Add/load paths got, now on the feature's most central entry point. Driven at the
    // post-append seam (arm_fresh_enqueue_anchor_on_append) so it needs no network to
    // resolve songs; the real dispatch wiring is proved LIVE by
    // live_enqueue_action_seeds_appended_over_lingering. A finished A lingers, then a
    // fresh selection D is queued while stopped -> the seed moves to D, never stale A,
    // and the recency memory is kept.
    #[tokio::test]
    async fn enqueue_action_arms_fresh_anchor_when_idle() {
        let Some((h, _events)) = handler_with_null_player() else { return };
        h.state.lock().unwrap().last_finished = Some(playlist_test_song("A"));
        // A fresh selection D landed on the idle deck (append-only, current still None).
        // The action seam snapshots the first-appended qid BEFORE the append; mirror it.
        let first = h.enqueue_song_for_test(playlist_test_song("D")).await;
        h.arm_fresh_enqueue_anchor_on_append(first, 1);
        let anchor = h.state.lock().unwrap().fresh_enqueue_anchor;
        assert_eq!(anchor, Some(first), "an append-only Enqueue arms the anchor at the appended music");
        assert_eq!(
            h.state.lock().unwrap().last_finished.as_ref().map(|s| s.id.clone()),
            Some(SongId("A".into())),
            "the recency memory is kept, not wiped"
        );
        assert_eq!(h.similar_seed_id(), Some(SongId("D".into())), "seed moves to D");
    }

    // An append-only Enqueue that resolved NOTHING (honest 0 - no library match) must
    // NOT arm an anchor: with no fresher music to point at, "just finished A" is still
    // the most pertinent seed. Guards the n>0 empty-gate.
    #[tokio::test]
    async fn enqueue_action_keeps_recency_seed_on_zero_match() {
        let Some((h, _events)) = handler_with_null_player() else { return };
        h.state.lock().unwrap().last_finished = Some(playlist_test_song("A"));
        // Idle deck, but the ask resolved 0 songs - nothing fresher landed.
        h.arm_fresh_enqueue_anchor_on_append(0, 0);
        let st = h.state.lock().unwrap();
        assert!(st.fresh_enqueue_anchor.is_none(), "a 0-match enqueue arms no anchor");
        assert_eq!(
            st.last_finished.as_ref().map(|s| s.id.clone()),
            Some(SongId("A".into())),
            "a 0-match enqueue keeps the recency seed (nothing fresher landed)"
        );
    }

    // The append-only arm is a NO-OP while a track is current: an Enqueue action behind
    // a playing deck must not arm (the current-song branch wins the seed anyway) and must
    // leave last_finished untouched. Same idle-gate as
    // enqueue_songs_keeps_recency_seed_while_current.
    #[tokio::test]
    async fn enqueue_action_keeps_recency_seed_while_current() {
        let Some((h, _events)) = handler_with_null_player() else { return };
        {
            let mut st = h.state.lock().unwrap();
            st.last_finished = Some(playlist_test_song("A"));
            st.current = Some(0); // a track is playing
        }
        h.arm_fresh_enqueue_anchor_on_append(7, 1);
        let st = h.state.lock().unwrap();
        assert!(st.fresh_enqueue_anchor.is_none(), "no anchor is armed behind a playing deck");
        assert_eq!(
            st.last_finished.as_ref().map(|s| s.id.clone()),
            Some(SongId("A".into())),
            "a current track keeps the recency seed untouched"
        );
    }

    // THE realistic post-EOF test (scenario G): track A finishes and LINGERS at queue
    // pos0 (consume OFF - the deeper root cause; the finished entry is what would replay),
    // then a fresh idle enqueue [D] lands. The seed AND the hint must skip the lingering
    // finished A and name the freshly-appended D. Drives the REAL EOF path (not an
    // artificially emptied queue), so a future artificial-empty-queue test can never again
    // mask this deeper root cause.
    #[tokio::test]
    async fn seed_skips_lingering_finished_head_after_fresh_enqueue() {
        let Some((h, _events)) = handler_with_null_player() else { return };
        // A single real song plays, then reaches natural EOF: consume off, so A LINGERS.
        h.enqueue_song_for_test(playlist_test_song("A")).await;
        h.state.lock().unwrap().current = Some(0);
        h.advance_on_eof().await;
        {
            let st = h.state.lock().unwrap();
            assert_eq!(st.queue.len(), 1, "A lingers (consume off)");
            assert_eq!(st.current, None, "deck stopped at end of queue");
            assert_eq!(
                st.last_finished.as_ref().map(|s| &s.id),
                Some(&SongId("A".into())),
                "A is the recency memory"
            );
        }
        // Before the gesture the seed/hint is just-finished A.
        assert_eq!(h.similar_seed_id(), Some(SongId("A".into())), "pre-gesture seed is A");
        assert_eq!(
            h.ambient_hint_pairs(),
            vec![
                ("X-hypodj-hint-kind", "just-finished".to_string()),
                ("X-hypodj-hint-title", "Song A".to_string()),
            ]
        );
        // A fresh idle enqueue of D arms the anchor; A is still lingering at pos0.
        h.enqueue_songs(vec![playlist_test_song("D")]);
        assert_eq!(h.state.lock().unwrap().queue.len(), 2, "A lingering at pos0, D at pos1");
        assert_eq!(h.similar_seed_id(), Some(SongId("D".into())), "seed skips A to D");
        assert_eq!(
            h.ambient_hint_pairs(),
            vec![
                ("X-hypodj-hint-kind", "up-next".to_string()),
                ("X-hypodj-hint-title", "Song D".to_string()),
            ],
            "hint is up-next D, never the lingering just-finished A"
        );
    }

    // Scenario R (recency wins, realistic + proves play clears the anchor): [A,B] are
    // enqueued (arming the anchor), A plays and finishes single with NO fresh gesture
    // after the finish. Both linger (consume off). The seed must be the just-finished A
    // (recency), NOT B - because the play-time commit CLEARED the anchor armed at enqueue.
    #[tokio::test]
    async fn recency_wins_when_no_fresh_gesture_after_eof() {
        let Some((h, _events)) = handler_with_null_player() else { return };
        // [A,B] enqueued while idle -> the anchor arms at A.
        h.enqueue_songs(vec![playlist_test_song("A"), playlist_test_song("B")]);
        assert!(h.state.lock().unwrap().fresh_enqueue_anchor.is_some(), "enqueue armed the anchor");
        h.state.lock().unwrap().single = true;
        // A plays: the play-time current-commit clears the anchor.
        h.handle(MpdCommand::Play(Some(0))).await;
        {
            let st = h.state.lock().unwrap();
            assert_eq!(st.current, Some(0), "A is current");
            assert!(st.fresh_enqueue_anchor.is_none(), "play cleared the anchor");
        }
        // A finishes single: deck stops, current None, both lingering, last_finished A.
        h.advance_on_eof().await;
        {
            let st = h.state.lock().unwrap();
            assert_eq!(st.current, None, "single stops after A");
            assert_eq!(st.queue.len(), 2, "both lingering (consume off)");
            assert_eq!(
                st.last_finished.as_ref().map(|s| &s.id),
                Some(&SongId("A".into())),
                "A is the recency memory"
            );
            assert!(st.fresh_enqueue_anchor.is_none(), "no fresh gesture since the finish");
        }
        assert_eq!(h.similar_seed_id(), Some(SongId("A".into())), "recency A wins, NOT B");
    }

    // The play-time current-commit clears a pending fresh-enqueue anchor: an idle enqueue
    // arms it, then starting playback (play_index -> play_index_inner) clears it so the
    // NowPlaying branch owns the seed.
    #[tokio::test]
    async fn anchor_cleared_when_track_becomes_current() {
        let Some((h, _events)) = handler_with_null_player() else { return };
        h.enqueue_songs(vec![playlist_test_song("D")]);
        assert!(h.state.lock().unwrap().fresh_enqueue_anchor.is_some(), "idle enqueue armed the anchor");
        h.play_index(0).await.expect("play the queued song");
        assert!(
            h.state.lock().unwrap().fresh_enqueue_anchor.is_none(),
            "the current-commit cleared the anchor"
        );
    }

    // Latest gesture wins: two successive idle enqueues each OVERWRITE the anchor, so only
    // the newest tail seeds; the older superseded append (D) is intentionally skipped.
    #[tokio::test]
    async fn latest_gesture_supersedes_prior() {
        let Some((h, _events)) = handler_with_null_player() else { return };
        h.enqueue_songs(vec![playlist_test_song("D")]);
        h.enqueue_songs(vec![playlist_test_song("E")]);
        assert_eq!(h.similar_seed_id(), Some(SongId("E".into())), "the newest anchor (E) wins, never D");
    }

    // A dangling anchor self-heals: build the G-state (A lingering, D appended, anchor at
    // D), then DELETE the appended D. The anchor's position lookup fails, branch 2 falls
    // through, and the seed heals back to the recency memory A (just-finished).
    #[tokio::test]
    async fn dangling_anchor_self_heals_to_recency() {
        let Some((h, _events)) = handler_with_null_player() else { return };
        // A lingering at pos0 after a real EOF.
        h.enqueue_song_for_test(playlist_test_song("A")).await;
        h.state.lock().unwrap().current = Some(0);
        h.advance_on_eof().await;
        // Fresh idle enqueue D arms the anchor at D (pos1).
        h.enqueue_songs(vec![playlist_test_song("D")]);
        assert_eq!(h.similar_seed_id(), Some(SongId("D".into())), "sanity: G-state seeds D");
        // Delete the appended D: the anchor now dangles (its qid is gone).
        h.delete_for_test(1);
        assert_eq!(h.similar_seed_id(), Some(SongId("A".into())), "self-heals to recency A");
        assert_eq!(
            h.ambient_hint_pairs(),
            vec![
                ("X-hypodj-hint-kind", "just-finished".to_string()),
                ("X-hypodj-hint-title", "Song A".to_string()),
            ],
            "the hint heals back to just-finished A"
        );
    }

    // Consume-on scenario G: at EOF the finished A is EVICTED (queue empty), but a fresh
    // idle enqueue [D] still arms the anchor and the seed is the appended D. The two
    // mechanisms are orthogonal - the anchor carries the latest gesture regardless of
    // whether consume evicted the head.
    #[tokio::test]
    async fn consume_on_gesture_seeds_appended() {
        let Some((h, _events)) = handler_with_null_player() else { return };
        {
            let mut st = h.state.lock().unwrap();
            st.consume = true;
            // A finished and was evicted (queue empty); recency memory holds A.
            st.last_finished = Some(playlist_test_song("A"));
        }
        h.enqueue_songs(vec![playlist_test_song("D")]);
        assert_eq!(h.similar_seed_id(), Some(SongId("D".into())), "seed the appended D");
        assert_eq!(
            h.ambient_hint_pairs(),
            vec![
                ("X-hypodj-hint-kind", "up-next".to_string()),
                ("X-hypodj-hint-title", "Song D".to_string()),
            ]
        );
    }

    // Consume-on scenario X/R: A finished and was evicted (queue empty), no fresh gesture
    // after the finish. The recency memory carries A across the eviction, so the seed is
    // just-finished A.
    #[tokio::test]
    async fn consume_on_recency_seeds_finished() {
        let Some((h, _events)) = handler_with_null_player() else { return };
        {
            let mut st = h.state.lock().unwrap();
            st.consume = true;
            st.last_finished = Some(playlist_test_song("A"));
        }
        assert_eq!(h.similar_seed_id(), Some(SongId("A".into())), "recency A across eviction");
        assert_eq!(
            h.ambient_hint_pairs(),
            vec![
                ("X-hypodj-hint-kind", "just-finished".to_string()),
                ("X-hypodj-hint-title", "Song A".to_string()),
            ]
        );
    }

    // The append-only Enqueue action arm skips a lingering finished head: with A lingering
    // at pos0, driving arm_fresh_enqueue_anchor_on_append(first_qid, n>0) seeds the
    // appended tail over A; the honest-0 case (n==0) arms nothing and leaves the seed at A.
    #[tokio::test]
    async fn append_only_enqueue_action_arms_anchor_over_lingering_head() {
        let Some((h, _events)) = handler_with_null_player() else { return };
        // A lingering at pos0 after a real EOF.
        h.enqueue_song_for_test(playlist_test_song("A")).await;
        h.state.lock().unwrap().current = Some(0);
        h.advance_on_eof().await;
        // Simulate the append-only action: snapshot the first-appended qid, append the
        // tail, then arm on append. The tail lands at pos1, behind lingering A.
        let first = h.enqueue_song_for_test(playlist_test_song("tail")).await;
        h.arm_fresh_enqueue_anchor_on_append(first, 1);
        assert_eq!(
            h.similar_seed_id(),
            Some(SongId("tail".into())),
            "the arm skips the lingering A to the appended tail"
        );
        // The honest-0 case: n==0 arms no anchor, so the seed stays the finished A.
        let Some((h2, _events2)) = handler_with_null_player() else { return };
        h2.state.lock().unwrap().last_finished = Some(playlist_test_song("A"));
        h2.arm_fresh_enqueue_anchor_on_append(5, 0);
        assert!(h2.state.lock().unwrap().fresh_enqueue_anchor.is_none(), "honest 0 arms nothing");
        assert_eq!(h2.similar_seed_id(), Some(SongId("A".into())), "seed stays A on a 0-match");
    }

    /// LIVE (task ambient-context-hint): the append-only Enqueue ACTION end to end seeds
    /// the freshly-appended music OVER a lingering finished head on an idle deck.
    /// Synthesizes a finished track A LINGERING at queue pos0 (consume off) with the
    /// recency memory set, drives the REAL run_action_outcome(Action::Enqueue {Radio, N})
    /// against a live backend so N>0 real tracks append WITHOUT autoplaying, then proves
    /// the seed moved to the FIRST appended track (skipping the lingering A) while the
    /// recency memory (last_finished) is KEPT - the deeper staleness the hint exists to
    /// prevent, gone at the DJ NL surface. Also proves the autoplay PlayNow action does
    /// NOT strand a stale seed: it starts playback so the now-current track wins. Env:
    /// HYPODJ_LIVE_URL/USER/PASS. Run with
    /// `cargo test -p hypodj-core -- --ignored live_enqueue_action_seeds_appended`.
    #[tokio::test]
    #[ignore = "requires a live backend (HYPODJ_LIVE_URL/USER/PASS)"]
    async fn live_enqueue_action_seeds_appended_over_lingering() {
        let (url, user, pass) = match (
            std::env::var("HYPODJ_LIVE_URL"),
            std::env::var("HYPODJ_LIVE_USER"),
            std::env::var("HYPODJ_LIVE_PASS"),
        ) {
            (Ok(u), Ok(us), Ok(pw)) => (u, us, pw),
            _ => {
                eprintln!("skipping: set HYPODJ_LIVE_URL/USER/PASS to run");
                return;
            }
        };
        let cfg = ServerConfig { url, username: user, password: pass, client_name: "hypodj-live-test".into() };
        let client = Arc::new(SubsonicClient::connect(&cfg).expect("connect"));
        let (player, _events) = NullPlayer::spawn();
        let h = HypodjHandler::new(Arc::clone(&client), player);

        // A finished track A LINGERING at queue pos0 (consume off), stopped, current
        // None: the recency seed is A and the hint would read "just finished A".
        h.enqueue_song_for_test(playlist_test_song("A")).await;
        h.state.lock().unwrap().last_finished = Some(playlist_test_song("A"));
        assert_eq!(h.similar_seed_id(), Some(SongId("A".into())), "seed starts at finished A");

        // The DJ NL surface: an append-only Enqueue lands N>0 real tracks WITHOUT
        // autoplaying (current stays None). The fresh gesture outranks the lingering A.
        let before = h.state.lock().unwrap().queue.len();
        let out = h.run_action_outcome(&Action::Enqueue { selector: Selector::Radio, count: 3 }).await;
        let n = match out {
            PlanOutcome::Added { n, .. } => n,
            other => panic!("expected Added, got {other:?}"),
        };
        assert!(n > 0, "radio must resolve N>0 real tracks");
        assert_eq!(h.state.lock().unwrap().queue.len() - before, n, "append-only delta");
        assert!(h.state.lock().unwrap().current.is_none(), "Enqueue never starts playback");
        assert_eq!(
            h.state.lock().unwrap().last_finished.as_ref().map(|s| s.id.clone()),
            Some(SongId("A".into())),
            "the recency memory is kept (honest across consume eviction), not wiped"
        );
        // The seed is the FIRST appended track (the anchored tail), skipping lingering A.
        let first_appended = match &h.state.lock().unwrap().queue[before].entry {
            QueueEntry::Song(s) => s.id.clone(),
            QueueEntry::Stream { .. } => panic!("radio appends library songs, not streams"),
        };
        let seed = h.similar_seed_id().expect("a seed from the freshly queued music");
        assert_eq!(seed, first_appended, "seed is the first appended track, over lingering A");
        assert_ne!(seed, SongId("A".into()), "seed skips the stale finished A");

        // The autoplay PlayNow action, by contrast, STARTS playback: it must not strand
        // a cleared seed - the now-current track wins seed_source regardless.
        let (player2, _events2) = NullPlayer::spawn();
        let h2 = HypodjHandler::new(Arc::clone(&client), player2);
        h2.state.lock().unwrap().last_finished = Some(playlist_test_song("A"));
        let out2 = h2.run_action_outcome(&Action::PlayNow { selector: Selector::Radio, count: 1 }).await;
        assert!(matches!(out2, PlanOutcome::Played { .. }), "PlayNow reports Played");
        assert!(h2.state.lock().unwrap().current.is_some(), "PlayNow starts playback");
        assert!(
            matches!(h2.seed_source(), Some(SeedSource { kind: SeedKind::NowPlaying, .. })),
            "the now-current PlayNow track wins the seed"
        );
    }

    // Consume mode evicts the finished entry from the queue, but the hint still names
    // it: the title is captured at finish onto last_finished, not looked up in a queue
    // that no longer holds it.
    #[tokio::test]
    async fn ambient_hint_resolves_finished_title_without_queue_entry() {
        let Some((h, _events)) = handler_with_null_player() else { return };
        // last_finished holds the song; the queue is empty (the entry was consumed).
        h.state.lock().unwrap().last_finished = Some(playlist_test_song("gone"));
        assert_eq!(
            h.ambient_hint_pairs(),
            vec![
                ("X-hypodj-hint-kind", "just-finished".to_string()),
                ("X-hypodj-hint-title", "Song gone".to_string()),
            ]
        );
    }

    // A stream can never become the hint: it has no library id. A stream-as-current
    // falls to the finished real track, and a stream-only queue yields no hint.
    #[tokio::test]
    async fn ambient_hint_never_names_a_stream() {
        let Some((h, _events)) = handler_with_null_player() else { return };
        h.enqueue_stream_for_test(NTS).await;
        {
            let mut st = h.state.lock().unwrap();
            st.current = Some(0); // the id-less stream is current
            st.last_finished = Some(playlist_test_song("real"));
        }
        let pairs = h.ambient_hint_pairs();
        assert_eq!(
            pairs,
            vec![
                ("X-hypodj-hint-kind", "just-finished".to_string()),
                ("X-hypodj-hint-title", "Song real".to_string()),
            ]
        );
        assert!(!pairs.iter().any(|(_, v)| v.contains("ntslive")), "never the stream url");
        // A stream-only queue with nothing finished yields no hint at all.
        let Some((h2, _events2)) = handler_with_null_player() else { return };
        h2.enqueue_stream_for_test(NTS).await;
        assert!(h2.ambient_hint_pairs().is_empty());
    }

    // Defensive: a title carrying a newline would tear the status line, so no hint is
    // emitted for it (mirrors the client-side skip-on-torn discipline).
    #[tokio::test]
    async fn ambient_hint_refuses_title_with_newline() {
        let Some((h, _events)) = handler_with_null_player() else { return };
        let mut s = playlist_test_song("x");
        s.title = "line1\nline2".to_string();
        h.state.lock().unwrap().last_finished = Some(s);
        assert!(h.ambient_hint_pairs().is_empty(), "a newline title is refused");
    }

    // "queue more like this" with NOTHING to seed from is an HONEST 0 - it never
    // touches the network (no fabricated pick) and leaves the queue unchanged.
    #[tokio::test]
    async fn plan_enqueue_similar_to_current_honest_zero_when_nothing_queued() {
        let Some((h, _events)) = handler_with_null_player() else { return };
        let before = h.state.lock().unwrap().queue.len();
        let n = h
            .plan_enqueue(&Selector::SimilarToCurrent, 5)
            .await
            .expect("honest 0, not an error");
        let after = h.state.lock().unwrap().queue.len();
        assert_eq!(n, 0, "nothing playing/queued to seed from -> honest 0");
        assert_eq!(before, after, "a no-seed ask leaves the queue unchanged");
    }

    #[test]
    fn push_song_tags_emits_x_starred_only_when_starred() {
        // Not starred -> no X-Starred pair at all (never a `0` line).
        let mut s = playlist_test_song("s-1");
        let pairs = browse_song_pairs(&s);
        assert!(!pairs.iter().any(|(k, _)| k == "X-Starred"));
        // Starred -> exactly one well-formed `X-Starred: 1` pair.
        s.starred = true;
        let pairs = browse_song_pairs(&s);
        let starred: Vec<_> = pairs.iter().filter(|(k, _)| k == "X-Starred").collect();
        assert_eq!(starred.len(), 1);
        assert_eq!(starred[0].1, "1");
    }

    #[tokio::test]
    async fn set_queue_starred_flips_currentsong_heart_live() {
        let Some((h, _events)) = handler_with_null_player() else { return };
        h.enqueue_song_for_test(playlist_test_song("s-1")).await;
        h.handle(MpdCommand::Play(Some(0))).await;
        let cur = |r: MpdResponse| match r {
            MpdResponse::Pairs(p) => p,
            other => panic!("expected Pairs, got {other:?}"),
        };
        // Baseline: not starred, so currentsong carries no heart hint.
        let c0 = cur(h.handle(MpdCommand::CurrentSong).await);
        assert!(!c0.iter().any(|(k, _)| k == "X-Starred"));
        // A confirmed star flips the in-memory entry -> heart appears LIVE.
        h.set_queue_starred(&SongId("s-1".into()), true);
        let c1 = cur(h.handle(MpdCommand::CurrentSong).await);
        assert!(c1.iter().any(|(k, v)| k == "X-Starred" && v == "1"));
        // Symmetric unstar clears it LIVE.
        h.set_queue_starred(&SongId("s-1".into()), false);
        let c2 = cur(h.handle(MpdCommand::CurrentSong).await);
        assert!(!c2.iter().any(|(k, _)| k == "X-Starred"));
    }

    /// Live star/unstar heart flip against a REAL backend. Skipped by default
    /// (`#[ignore]`); run with `--ignored` and env config pointing at a throwaway
    /// backend/song. Stars the given song via the real `playlistadd Starred` path,
    /// asserts `currentsong` gains `X-Starred: 1`, then unstars it (real Subsonic
    /// unstar) and asserts the heart clears. Restores the original star state.
    ///
    /// Env: `HYPODJ_LIVE_URL`, `HYPODJ_LIVE_USER`, `HYPODJ_LIVE_PASS`,
    /// `HYPODJ_LIVE_SONG` (a bare Subsonic song id).
    #[tokio::test]
    #[ignore]
    async fn live_star_unstar_toggles_currentsong_heart() {
        let (url, user, pass, sid) = match (
            std::env::var("HYPODJ_LIVE_URL"),
            std::env::var("HYPODJ_LIVE_USER"),
            std::env::var("HYPODJ_LIVE_PASS"),
            std::env::var("HYPODJ_LIVE_SONG"),
        ) {
            (Ok(u), Ok(us), Ok(pw), Ok(s)) => (u, us, pw, s),
            _ => {
                eprintln!("skipping: set HYPODJ_LIVE_URL/USER/PASS/SONG to run");
                return;
            }
        };
        let cfg = ServerConfig {
            url,
            username: user,
            password: pass,
            client_name: "hypodj-live-test".to_string(),
        };
        let client = Arc::new(SubsonicClient::connect(&cfg).expect("connect"));
        let (player, _events) = NullPlayer::spawn();
        let h = HypodjHandler::new(client, player);

        // Queue a minimal entry for the target song and make it current.
        let mut song = playlist_test_song(&sid);
        song.starred = false;
        h.enqueue_song_for_test(song).await;
        h.handle(MpdCommand::Play(Some(0))).await;

        let cur = |r: MpdResponse| match r {
            MpdResponse::Pairs(p) => p,
            other => panic!("expected Pairs, got {other:?}"),
        };
        let uri = format!("song/{sid}");

        // Star via the real path -> heart appears LIVE.
        let r = h.handle(MpdCommand::PlaylistAdd("Starred".into(), uri.clone())).await;
        assert!(!matches!(r, MpdResponse::Ack { .. }), "star must succeed: {r:?}");
        let c1 = cur(h.handle(MpdCommand::CurrentSong).await);
        assert!(c1.iter().any(|(k, v)| k == "X-Starred" && v == "1"), "heart set: {c1:?}");

        // Unstar via the real Subsonic path -> heart clears LIVE. Restores state.
        h.client.unstar(&Favorite::Song(SongId(sid.clone()))).await.expect("unstar");
        h.set_queue_starred(&SongId(sid.clone()), false);
        let c2 = cur(h.handle(MpdCommand::CurrentSong).await);
        assert!(!c2.iter().any(|(k, _)| k == "X-Starred"), "heart cleared: {c2:?}");
    }

    // ── queue-edit executor (Part B): deterministic remove/move/clear over the
    //    live queue; a no-match is a clean no-op, never a wrong-target delete. ──
    async fn seed_queue(h: &HypodjHandler, n: usize) {
        for i in 1..=n {
            h.enqueue_song_for_test(playlist_test_song(&format!("s-{i}"))).await;
        }
    }

    fn ids(h: &HypodjHandler) -> Vec<String> {
        h.queue_song_ids().into_iter().map(|s| s.0).collect()
    }

    #[tokio::test]
    async fn queue_edit_remove_last_and_range_and_query() {
        use crate::plan::{Action, QueueSelector};
        // remove last 2 -> the tail is gone, order preserved.
        let Some((h, _e)) = handler_with_null_player() else { return };
        seed_queue(&h, 5).await;
        let n = h.plan_queue_edit(&Action::Remove { sel: QueueSelector::Last(2) }).await.unwrap();
        assert_eq!(n, 2);
        assert_eq!(ids(&h), vec!["s-1", "s-2", "s-3"]);

        // remove a 1-based inclusive range (2..=3 -> s-2, s-3).
        let Some((h, _e)) = handler_with_null_player() else { return };
        seed_queue(&h, 5).await;
        let n = h
            .plan_queue_edit(&Action::Remove { sel: QueueSelector::Range { start: 2, end: 3 } })
            .await
            .unwrap();
        assert_eq!(n, 2);
        assert_eq!(ids(&h), vec!["s-1", "s-4", "s-5"]);

        // remove by query match (title contains "s-4").
        let Some((h, _e)) = handler_with_null_player() else { return };
        seed_queue(&h, 5).await;
        let n = h
            .plan_queue_edit(&Action::Remove { sel: QueueSelector::QueryMatch("s-4".into()) })
            .await
            .unwrap();
        assert_eq!(n, 1);
        assert_eq!(ids(&h), vec!["s-1", "s-2", "s-3", "s-5"]);
    }

    #[tokio::test]
    async fn queue_edit_no_match_is_clean_noop() {
        use crate::plan::{Action, QueueSelector};
        // A query that matches nothing removes NOTHING (never a wrong-target delete).
        let Some((h, _e)) = handler_with_null_player() else { return };
        seed_queue(&h, 3).await;
        let n = h
            .plan_queue_edit(&Action::Remove { sel: QueueSelector::QueryMatch("nonesuch".into()) })
            .await
            .unwrap();
        assert_eq!(n, 0);
        assert_eq!(ids(&h), vec!["s-1", "s-2", "s-3"]);
        // An out-of-range position is likewise a clean no-op.
        let n = h
            .plan_queue_edit(&Action::Remove { sel: QueueSelector::Position(99) })
            .await
            .unwrap();
        assert_eq!(n, 0);
        assert_eq!(ids(&h), vec!["s-1", "s-2", "s-3"]);
        // Noop does nothing.
        assert_eq!(h.plan_queue_edit(&Action::Noop).await.unwrap(), 0);
    }

    #[tokio::test]
    async fn queue_edit_clear_scopes() {
        use crate::plan::{Action, ClearScope};
        // clear all -> empty.
        let Some((h, _e)) = handler_with_null_player() else { return };
        seed_queue(&h, 4).await;
        h.plan_queue_edit(&Action::Clear { scope: ClearScope::All }).await.unwrap();
        assert!(ids(&h).is_empty());

        // clear range 2..=3.
        let Some((h, _e)) = handler_with_null_player() else { return };
        seed_queue(&h, 5).await;
        h.plan_queue_edit(&Action::Clear { scope: ClearScope::Range { start: 2, end: 3 } })
            .await
            .unwrap();
        assert_eq!(ids(&h), vec!["s-1", "s-4", "s-5"]);

        // clear after_current with nothing playing -> clean no-op (no surprise wipe).
        let Some((h, _e)) = handler_with_null_player() else { return };
        seed_queue(&h, 3).await;
        let n = h
            .plan_queue_edit(&Action::Clear { scope: ClearScope::AfterCurrent })
            .await
            .unwrap();
        assert_eq!(n, 0);
        assert_eq!(ids(&h), vec!["s-1", "s-2", "s-3"]);
    }

    #[tokio::test]
    async fn queue_edit_move_last_to_top() {
        use crate::plan::{Action, MoveDest, QueueSelector};
        let Some((h, _e)) = handler_with_null_player() else { return };
        seed_queue(&h, 5).await;
        let n = h
            .plan_queue_edit(&Action::Move {
                sel: QueueSelector::Last(1),
                dest: MoveDest::Position(1),
            })
            .await
            .unwrap();
        assert_eq!(n, 1);
        assert_eq!(ids(&h), vec!["s-5", "s-1", "s-2", "s-3", "s-4"]);
    }

    #[tokio::test]
    async fn save_starred_name_is_reserved_and_never_clobbers() {
        // `save Starred` must fail LOUD (reserved) rather than overwrite the
        // synthetic star pseudo-playlist. No network is touched on this path.
        let Some((h, _events)) = handler_with_null_player() else { return };
        h.enqueue_song_for_test(playlist_test_song("s-1")).await;
        let resp = h.handle(MpdCommand::Save("Starred".into())).await;
        match resp {
            MpdResponse::Ack { command, .. } => assert_eq!(command, "save"),
            other => panic!("expected ACK, got {other:?}"),
        }
    }

    #[tokio::test]
    async fn save_empty_queue_acks_rather_than_creating_empty_playlist() {
        // An empty queue must not create an empty Navidrome playlist; it ACKs
        // before any network call.
        let Some((h, _events)) = handler_with_null_player() else { return };
        let resp = h.handle(MpdCommand::Save("Whatever".into())).await;
        assert!(matches!(resp, MpdResponse::Ack { .. }), "empty queue -> ACK");
    }

    #[tokio::test]
    async fn playlistadd_starred_stays_special_unsupported_uri_acks() {
        // The Starred path routes via Favorite::from_uri; a non-favoritable uri
        // fails LOUD (NO_EXIST), proving Starred is still handled specially and
        // never falls through to the real-playlist create path.
        let Some((h, _events)) = handler_with_null_player() else { return };
        let resp = h
            .handle(MpdCommand::PlaylistAdd("Starred".into(), "bogus/x".into()))
            .await;
        match resp {
            MpdResponse::Ack { command, code, .. } => {
                assert_eq!(command, "playlistadd");
                assert_eq!(code, ACK_ERROR_NO_EXIST);
            }
            other => panic!("expected ACK, got {other:?}"),
        }
    }

    #[tokio::test]
    async fn playlistadd_real_playlist_unsupported_uri_acks_not_silent() {
        // Non-Starred playlistadd with a non-`song/` uri must ACK (uri->SongId map
        // fails), never the old silent no-op. No network is touched.
        let Some((h, _events)) = handler_with_null_player() else { return };
        let resp = h
            .handle(MpdCommand::PlaylistAdd("Warm Room".into(), "album/a-1".into()))
            .await;
        match resp {
            MpdResponse::Ack { command, code, .. } => {
                assert_eq!(command, "playlistadd");
                assert_eq!(code, ACK_ERROR_NO_EXIST);
            }
            other => panic!("expected ACK, got {other:?}"),
        }
    }

    #[tokio::test]
    async fn playlistclear_starred_stays_special_noop_ok() {
        // `playlistclear Starred` must NOT touch the real-playlist delete path
        // (which would try to deletePlaylist a non-existent "Starred") and must
        // never fan out into mass-unstarring. It stays a well-formed no-op ok.
        let Some((h, _events)) = handler_with_null_player() else { return };
        let resp = h.handle(MpdCommand::PlaylistClear("Starred".into())).await;
        assert!(
            matches!(resp, MpdResponse::Pairs(ref p) if p.is_empty()),
            "playlistclear Starred must be a well-formed ok, got {resp:?}"
        );
    }

    #[tokio::test]
    async fn playlistdelete_starred_bad_index_acks_and_stays_special() {
        // `playlistdelete Starred <pos>` routes through the star order, NOT the
        // real-playlist path. With no recorded order, pos 0 is a bad index -> a
        // LOUD ack, proving Starred is still handled specially (no network).
        let Some((h, _events)) = handler_with_null_player() else { return };
        let resp = h.handle(MpdCommand::PlaylistDelete("Starred".into(), 0)).await;
        match resp {
            MpdResponse::Ack { command, code, .. } => {
                assert_eq!(command, "playlistdelete");
                assert_eq!(code, ACK_ERROR_NO_EXIST);
            }
            other => panic!("expected ACK, got {other:?}"),
        }
    }

    #[tokio::test]
    async fn playlistdelete_real_playlist_is_wired_not_silent() {
        // A non-Starred `playlistdelete <name> <pos>` is no longer a silent no-op:
        // it reaches Subsonic (get_playlists). With the backend unreachable it must
        // surface a LOUD ACK, never a silent ok that pretends the delete happened.
        let Some((h, _events)) = handler_with_null_player() else { return };
        let resp = h
            .handle(MpdCommand::PlaylistDelete("Warm Room".into(), 0))
            .await;
        match resp {
            MpdResponse::Ack { command, .. } => assert_eq!(command, "playlistdelete"),
            other => panic!("expected ACK (wired to Subsonic), got {other:?}"),
        }
    }

    #[tokio::test]
    async fn playlistclear_real_playlist_is_wired_not_silent() {
        // Same for `playlistclear <name>`: it reaches Subsonic and, with the
        // backend unreachable, surfaces a LOUD ACK rather than the old silent ok.
        let Some((h, _events)) = handler_with_null_player() else { return };
        let resp = h.handle(MpdCommand::PlaylistClear("Warm Room".into())).await;
        match resp {
            MpdResponse::Ack { command, .. } => assert_eq!(command, "playlistclear"),
            other => panic!("expected ACK (wired to Subsonic), got {other:?}"),
        }
    }

    #[tokio::test(start_paused = true)]
    async fn resume_seek_target_is_elapsed_minus_lead() {
        let Some((h, _events)) = handler_with_null_player() else { return };
        let saved_vol = 73u8;
        let target_db = mpv_volume_to_db(saved_vol as f64);
        let synth_floor = h.fade_cfg.synth_floor_db;
        // The EXACT wake ramp restore builds (from the synth floor, sub-JND extended).
        let dur = h.clamp_fade_dur(Duration::from_secs(h.fade_cfg.restart_fade_secs));
        let spec = h.wake_ramp_spec(synth_floor, target_db, dur).unwrap();
        let lead = spec.time_to_reach_db(AUDIBILITY_DB).expect("ramp crosses audibility");
        // A real sub-audible head exists, so the seek-back is strictly positive.
        assert!(lead > Duration::ZERO);
        // At t=LEAD into the ramp the schedule is first >= AUDIBILITY_DB, so seeking
        // back by LEAD lands the playhead at `elapsed` at the first-audible instant.
        // Big elapsed: target = elapsed - LEAD.
        let e = 120.0_f64;
        let target = (e - lead.as_secs_f64()).max(0.0);
        assert!((target - (e - lead.as_secs_f64())).abs() < 1e-9);
        assert!(target > 0.0);
        // elapsed < LEAD clamps to 0 (never seeks before the track start).
        let small = lead.as_secs_f64() / 2.0;
        assert_eq!((small - lead.as_secs_f64()).max(0.0), 0.0);
    }

    #[test]
    fn resume_lead_spec_matches_wake_intent_resolution() {
        // Drift guard: the LEAD spec (wake_ramp_spec) and the fade start_fade_spec
        // actually spawns for a WakeTo must share (from_db, target, sub_jnd, no
        // clamp-up). start_fade_spec resolves the intent and, since WakeTo does NOT
        // clamp_dur_up, uses `dur` verbatim with sub_jnd bounds - exactly what
        // wake_ramp_spec assumes.
        let saved_vol = 73u8;
        let target_db = mpv_volume_to_db(saved_vol as f64);
        let synth_floor = -60.0;
        let intent = FadeIntent::WakeTo { target_db, vol: saved_vol };
        let (target, sub_jnd, _terminal, clamp_dur_up) =
            intent.resolve(synth_floor, -8.0, -45.0);
        assert!(matches!(target, FadeTarget::Db(x) if (x - target_db).abs() < 1e-9));
        assert!(sub_jnd, "wake_ramp_spec builds sub-JND bounds");
        assert!(!clamp_dur_up, "WakeTo never clamps the duration up, so LEAD uses dur verbatim");
    }

    #[test]
    fn skip_dip_is_far_shorter_than_a_full_silence_dip() {
        // A skip ducks to SKIP_DIP_DB, not the -60 dB synth floor. At the startle-
        // safe 250ms minimum step interval, a deliberate fade costs one step per
        // 3 dB, so the cost scales with the dB span: the shallow duck is a handful
        // of steps (~1.5s) versus ~20 steps (~5s) all the way to silence. This is
        // exactly why the skip now feels snappy.
        let step = std::time::Duration::from_millis(250);
        let shallow = min_deliberate_dur(0.0, FadeTarget::Db(SKIP_DIP_DB), step, -60.0);
        let full = min_deliberate_dur(0.0, FadeTarget::Silence, step, -60.0);
        assert!(
            shallow <= std::time::Duration::from_millis(1600),
            "shallow skip dip stays snappy, got {shallow:?}"
        );
        assert!(shallow * 2 < full, "shallow dip is well under the full-silence dip");
    }

    #[tokio::test]
    async fn seekcur_relative_offsets_from_live_position() {
        let Some((handler, mut events)) = handler_with_null_player() else { return };
        // Live position is 30s (the lockless elapsed atomic).
        handler.note_elapsed_ms(30_000);

        // Relative back 10 -> absolute 20.
        handler.handle(MpdCommand::SeekCur { secs: -10.0, relative: true }).await;
        match events.recv().await {
            Some(PlayerEvent::TimePos { pos, .. }) => assert_eq!(pos, 20.0),
            other => panic!("got {other:?}"),
        }

        // Relative forward 10 ACCUMULATES from the new position (20), not the
        // stale Tick base (30): 20 -> 30. This is the rapid-scrub case - no Tick
        // arrived between the two seeks, so the second must build on the first.
        handler.handle(MpdCommand::SeekCur { secs: 10.0, relative: true }).await;
        match events.recv().await {
            Some(PlayerEvent::TimePos { pos, .. }) => assert_eq!(pos, 30.0),
            other => panic!("got {other:?}"),
        }

        // Overshoot below 0 clamps to 0 (from the current 30 - 100).
        handler.handle(MpdCommand::SeekCur { secs: -100.0, relative: true }).await;
        match events.recv().await {
            Some(PlayerEvent::TimePos { pos, .. }) => assert_eq!(pos, 0.0),
            other => panic!("got {other:?}"),
        }

        // An absolute seekcur ignores the live position.
        handler.handle(MpdCommand::SeekCur { secs: 5.0, relative: false }).await;
        match events.recv().await {
            Some(PlayerEvent::TimePos { pos, .. }) => assert_eq!(pos, 5.0),
            other => panic!("got {other:?}"),
        }
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
    async fn add_album_uri_is_routed_to_resolution_not_rejected() {
        // `add album/<id>` must no longer ACK "unsupported uri": it now routes into
        // album_songs resolution. Against the unreachable test server that resolve
        // fails, so the response is a NO_EXIST ACK carrying a network error - the
        // point is that the uri class is HANDLED, not rejected as unsupported, and
        // NO stream/song item leaks into the queue.
        let Some((h, _events)) = handler_with_null_player() else { return };
        let resp = h.handle(MpdCommand::Add("album/whatever".to_string())).await;
        match resp {
            MpdResponse::Ack { message, .. } => {
                assert!(
                    !message.contains("unsupported uri"),
                    "album uri must be resolved, not rejected: {message}"
                );
            }
            other => panic!("expected an ACK from the unreachable resolve, got {other:?}"),
        }
        assert!(h.state.lock().unwrap().queue.is_empty(), "no item leaks on a failed album add");
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

    // A raw stream's LIVE ICY metadata surfaces in currentsong: the station lands in
    // Name: and the now-playing line REPLACES the URL in Title:, while file: stays the
    // raw URL and there is still no Time/duration. Matches real MPD's convention.
    #[tokio::test]
    async fn currentsong_stream_applies_live_name_and_title() {
        let Some((h, _events)) = handler_with_null_player() else { return };
        let qid = h.enqueue_stream_for_test(NTS).await;
        h.play_for_test(0).await;
        // The lossless PlayerEvent::StreamMetadata equivalent: mpv connected and read
        // the station (icy-name) + now-playing (icy-title) off the stream.
        h.set_stream_meta(
            QueueId(qid),
            Some("NTS 1".to_string()),
            Some("Floating Points - Track".to_string()),
        );

        let render = |r: MpdResponse| match r {
            MpdResponse::Pairs(p) => p,
            other => panic!("expected Pairs, got {other:?}"),
        };
        let cur = render(h.handle(MpdCommand::CurrentSong).await);
        assert!(
            cur.iter().any(|(k, v)| k == "Title" && v == "Floating Points - Track"),
            "Title is the live now-playing (icy-title), not the URL: {cur:?}"
        );
        assert!(
            cur.iter().any(|(k, v)| k == "Name" && v == "NTS 1"),
            "Name is the station (icy-name): {cur:?}"
        );
        assert!(cur.iter().any(|(k, v)| k == "file" && v == NTS), "file: stays the raw URL");
        assert!(!cur.iter().any(|(k, v)| k == "Title" && v == NTS), "the URL no longer shows as Title");
        assert!(!cur.iter().any(|(k, _)| k == "Time"), "a live stream still has no Time/duration");
    }

    // The qid gate + library-untouched: metadata stored for the STREAM's qid never
    // decorates a DIFFERENT current entry - here a library song. It keeps its own
    // title and carries no station Name.
    #[tokio::test]
    async fn stream_meta_ignored_for_wrong_qid_and_library_song() {
        let Some((h, _events)) = handler_with_null_player() else { return };
        let qid_stream = h.enqueue_stream_for_test(NTS).await; // qid A (a Stream)
        h.enqueue_song_for_test(playlist_test_song("lib")).await; // qid B (a Song)
        // Store live metadata for the STREAM's qid, but play the LIBRARY song.
        h.set_stream_meta(QueueId(qid_stream), Some("NTS 1".to_string()), Some("Live A".to_string()));
        h.play_for_test(1).await;

        let render = |r: MpdResponse| match r {
            MpdResponse::Pairs(p) => p,
            other => panic!("expected Pairs, got {other:?}"),
        };
        let cur = render(h.handle(MpdCommand::CurrentSong).await);
        assert!(
            cur.iter().any(|(k, v)| k == "Title" && v == "Song lib"),
            "the library song keeps its OWN title: {cur:?}"
        );
        assert!(cur.iter().any(|(k, v)| k == "file" && v == "song/lib"));
        assert!(!cur.iter().any(|(k, _)| k == "Name"), "a library song never inherits a station Name");
    }

    // ── on-demand recognition (identify, task f7vnd3i) ─────────────────────

    #[tokio::test]
    async fn identify_skips_library_song() {
        // A library song already carries metadata, so `identify` short-circuits
        // WITHOUT capturing (no ffmpeg/songrec subprocess) and leaves stream_meta
        // untouched, returning the 'already known' response.
        let Some((h, _events)) = handler_with_null_player() else { return };
        h.enqueue_song_for_test(playlist_test_song("lib")).await;
        h.play_for_test(0).await;

        let resp = h.handle(MpdCommand::Identify).await;
        match resp {
            MpdResponse::Pairs(p) => assert!(
                p.iter().any(|(k, v)| k == "identify" && v.contains("already known")),
                "a library song identifies as already-known: {p:?}"
            ),
            other => panic!("expected Pairs, got {other:?}"),
        }
        // No recognition ran: nothing was surfaced into the stream slots.
        let st = h.state.lock().unwrap();
        assert!(st.stream_meta.is_none(), "no stream_meta set for a library song");
        assert!(st.recognized_cover.is_none(), "no cover set for a library song");
    }

    #[tokio::test]
    async fn identify_nothing_playing_is_a_clean_ack() {
        // Nothing playing / stopped: a clear response, no capture attempted.
        let Some((h, _events)) = handler_with_null_player() else { return };
        let resp = h.handle(MpdCommand::Identify).await;
        match resp {
            MpdResponse::Pairs(p) => assert!(
                p.iter().any(|(k, v)| k == "identify" && v == "nothing playing"),
                "stopped -> nothing to identify: {p:?}"
            ),
            other => panic!("expected Pairs, got {other:?}"),
        }
    }

    #[tokio::test]
    async fn surface_maps_hit_to_stream_meta_and_cover() {
        // The SURFACE step (no live Shazam): a parsed hit's now-playing title rides
        // the same Name/Title path as ICY, and its cover surfaces as X-CoverArt in
        // currentsong - both qid-gated to the current stream entry.
        let Some((h, _events)) = handler_with_null_player() else { return };
        let qid = h.enqueue_stream_for_test(NTS).await;
        h.play_for_test(0).await;

        let track = crate::recognize::RecognizedTrack {
            artist: Some("Calvin Harris & Clementine Douglas".into()),
            title: Some("Blessings".into()),
            album: Some("Blessings".into()),
            cover_url: Some("https://is1.example/hq.jpg".into()),
        };
        let np = crate::recognize::now_playing_title(&track);
        assert_eq!(np.as_deref(), Some("Calvin Harris & Clementine Douglas - Blessings"));
        h.set_recognized_cover(QueueId(qid), track.cover_url.clone().unwrap());
        h.set_stream_meta(QueueId(qid), None, np.clone());

        let cur = match h.handle(MpdCommand::CurrentSong).await {
            MpdResponse::Pairs(p) => p,
            other => panic!("expected Pairs, got {other:?}"),
        };
        assert!(
            cur.iter().any(|(k, v)| k == "Title"
                && v == "Calvin Harris & Clementine Douglas - Blessings"),
            "recognized now-playing rides the Title path: {cur:?}"
        );
        assert!(
            cur.iter().any(|(k, v)| k == "X-CoverArt" && v == "https://is1.example/hq.jpg"),
            "recognized cover surfaces toward the dj-gui pane: {cur:?}"
        );
    }

    #[tokio::test]
    async fn recognized_cover_ignored_for_wrong_qid() {
        // A cover keyed to a DIFFERENT qid must never leak onto the current stream's
        // currentsong (mirrors the stream_meta qid gate).
        let Some((h, _events)) = handler_with_null_player() else { return };
        let qid = h.enqueue_stream_for_test(NTS).await;
        h.play_for_test(0).await;
        // Store a cover under a qid that is NOT the current entry.
        h.set_recognized_cover(QueueId(qid.wrapping_add(999)), "https://x/wrong.jpg".into());

        let cur = match h.handle(MpdCommand::CurrentSong).await {
            MpdResponse::Pairs(p) => p,
            other => panic!("expected Pairs, got {other:?}"),
        };
        assert!(
            !cur.iter().any(|(k, _)| k == "X-CoverArt"),
            "a wrong-qid cover must not surface: {cur:?}"
        );
    }

    // ── saved internet radio stations (task cchte88) ───────────────────────

    #[test]
    fn save_station_defaults_name_to_icy_when_present() {
        // A currently-playing stream whose stream_meta (keyed to its qid) carries an
        // icy-name: saving THAT url defaults the label to the live station name.
        let item = QueueItem {
            id: 7,
            entry: QueueEntry::Stream { url: NTS.to_string(), title: NTS.to_string() },
        };
        let meta = (
            QueueId(7),
            StreamMeta { name: Some("NTS 1".to_string()), title: Some("Floating Points".to_string()) },
        );
        assert_eq!(resolve_station_name(NTS, Some(&item), Some(&meta)), "NTS 1");
    }

    #[test]
    fn save_station_falls_back_to_url_when_no_icy() {
        // The NTS-mixtape case: the stream carries no ICY name, so the default label
        // falls back to the raw URL - both when the slot exists with name None and
        // when there is no stored slot at all.
        let item = QueueItem {
            id: 7,
            entry: QueueEntry::Stream { url: NTS.to_string(), title: NTS.to_string() },
        };
        let no_name = (QueueId(7), StreamMeta { name: None, title: None });
        assert_eq!(resolve_station_name(NTS, Some(&item), Some(&no_name)), NTS);
        assert_eq!(resolve_station_name(NTS, Some(&item), None), NTS);
        // An empty/whitespace icy-name must not become the label either.
        let blank = (QueueId(7), StreamMeta { name: Some("   ".to_string()), title: None });
        assert_eq!(resolve_station_name(NTS, Some(&item), Some(&blank)), NTS);
    }

    #[test]
    fn save_station_name_ignores_wrong_qid_stream_meta() {
        // stream_meta keyed to a DIFFERENT qid must not label this save (mirrors the
        // currentsong qid gate); a library-song current or a url mismatch also falls
        // back to the raw URL.
        let stream = QueueItem {
            id: 7,
            entry: QueueEntry::Stream { url: NTS.to_string(), title: NTS.to_string() },
        };
        let wrong_qid = (QueueId(99), StreamMeta { name: Some("Wrong".to_string()), title: None });
        assert_eq!(resolve_station_name(NTS, Some(&stream), Some(&wrong_qid)), NTS);

        // A library-song current never yields a station name.
        let song = QueueItem { id: 7, entry: QueueEntry::Song(playlist_test_song("lib")) };
        let meta = (QueueId(7), StreamMeta { name: Some("X".to_string()), title: None });
        assert_eq!(resolve_station_name(NTS, Some(&song), Some(&meta)), NTS);

        // A stream playing a DIFFERENT url than the one being saved falls back.
        let other = QueueItem {
            id: 7,
            entry: QueueEntry::Stream {
                url: "https://example.com/other".to_string(),
                title: "x".to_string(),
            },
        };
        assert_eq!(resolve_station_name(NTS, Some(&other), Some(&meta)), NTS);

        // Nothing playing at all -> the url.
        assert_eq!(resolve_station_name(NTS, None, None), NTS);
    }

    #[test]
    fn lsinfo_stations_renders_file_rows() {
        // Each station is a `file:` row = stream url, plus Title + Name = the station
        // name, and the response is a well-formed Pairs (never an ACK). Name -> URL
        // resolution (case-insensitive) for play-by-name is asserted here too.
        let stations = vec![
            Station {
                id: StationId("ir-1".into()),
                name: "NTS 1".into(),
                stream_url: "https://n/1".into(),
                home_page_url: None,
            },
            Station {
                id: StationId("ir-2".into()),
                name: "NTS 2".into(),
                stream_url: "https://n/2".into(),
                home_page_url: Some("https://nts.live".into()),
            },
        ];
        let pairs = match station_rows(&stations) {
            MpdResponse::Pairs(p) => p,
            other => panic!("expected Pairs, got {other:?}"),
        };
        assert!(pairs.iter().any(|(k, v)| k == "file" && v == "https://n/1"));
        assert!(pairs.iter().any(|(k, v)| k == "Title" && v == "NTS 1"));
        assert!(pairs.iter().any(|(k, v)| k == "Name" && v == "NTS 1"));
        assert!(pairs.iter().any(|(k, v)| k == "file" && v == "https://n/2"));
        assert!(pairs.iter().any(|(k, v)| k == "Name" && v == "NTS 2"));
        // A stream row must carry no Time/duration.
        assert!(!pairs.iter().any(|(k, _)| k == "Time"));

        // Name -> URL resolution is case-insensitive.
        assert_eq!(station_url_for_name(&stations, "nts 1").as_deref(), Some("https://n/1"));
        assert_eq!(station_url_for_name(&stations, "NTS 2").as_deref(), Some("https://n/2"));
        assert_eq!(station_url_for_name(&stations, "nope"), None);
    }

    #[test]
    fn lsinfo_stations_empty_is_well_formed_pairs_not_ack() {
        // An empty station set surfaces as an empty-but-well-formed Pairs, never an
        // ACK (same as an empty Starred), so ncmpcpp's blocking lsinfo never breaks.
        match station_rows(&[]) {
            MpdResponse::Pairs(p) => assert!(p.is_empty()),
            other => panic!("expected empty Pairs, got {other:?}"),
        }
    }

    // Stale-clear: a play edge to a DIFFERENT entry drops the stored slot (mirroring
    // the director's clear_stream_meta_except on StateChanged(Playing) with a new
    // qid), so the station label never lingers onto the next stream; a same-qid clear
    // (a mid-stream ICY title change) KEEPS the slot.
    #[tokio::test]
    async fn stream_meta_cleared_on_track_change() {
        let Some((h, _events)) = handler_with_null_player() else { return };
        let other = "https://example.com/other-stream.mp3";
        let qid_a = h.enqueue_stream_for_test(NTS).await;
        let qid_b = h.enqueue_stream_for_test(other).await;
        h.play_for_test(0).await;
        h.set_stream_meta(QueueId(qid_a), Some("NTS 1".to_string()), Some("Live A".to_string()));

        let render = |r: MpdResponse| match r {
            MpdResponse::Pairs(p) => p,
            other => panic!("expected Pairs, got {other:?}"),
        };
        // Sanity: A is decorated with its live line.
        let cur_a = render(h.handle(MpdCommand::CurrentSong).await);
        assert!(cur_a.iter().any(|(k, v)| k == "Title" && v == "Live A"));

        // A same-qid clear (mid-stream title change on A) must KEEP the slot.
        h.clear_stream_meta_except(Some(QueueId(qid_a)));
        let cur_still = render(h.handle(MpdCommand::CurrentSong).await);
        assert!(
            cur_still.iter().any(|(k, v)| k == "Name" && v == "NTS 1"),
            "a same-qid clear keeps the slot: {cur_still:?}"
        );

        // A play edge to a DIFFERENT entry clears the stale slot; B then falls back to
        // its URL Title with no leaked station Name.
        h.clear_stream_meta_except(Some(QueueId(qid_b)));
        h.play_for_test(1).await;
        let cur_b = render(h.handle(MpdCommand::CurrentSong).await);
        assert!(cur_b.iter().any(|(k, v)| k == "file" && v == other));
        assert!(
            cur_b.iter().any(|(k, v)| k == "Title" && v == other),
            "B falls back to its own URL Title: {cur_b:?}"
        );
        assert!(!cur_b.iter().any(|(k, _)| k == "Name"), "no stale station Name leaks onto B");
    }

    // Idle guard: a running daemon with an empty queue and no current song MUST
    // report state:stop, never a phantom play (an idle mpv can report not-paused).
    #[tokio::test]
    async fn status_reports_stop_when_idle() {
        let Some((h, _events)) = handler_with_null_player() else { return };
        // Fresh handler: nothing loaded.
        assert_eq!(pair(&h.handle(MpdCommand::Status).await, "state"), Some("stop"));

        // Force the pathological case the guard exists for: the raw player state
        // is Playing but there is no current song. Status must still say stop.
        h.enqueue_stream_for_test(NTS).await;
        h.play_for_test(0).await;
        assert_eq!(h.player.state(), PlayState::Playing);
        h.state.lock().unwrap().current = None;
        assert_eq!(pair(&h.handle(MpdCommand::Status).await, "state"), Some("stop"));
    }

    // resume_snapshot with no current song records Stopped even if the raw player
    // state is Playing, so a checkpoint can never claim Playing with an empty queue.
    #[tokio::test]
    async fn resume_snapshot_no_current_is_stopped() {
        let Some((h, _events)) = handler_with_null_player() else { return };
        h.enqueue_stream_for_test(NTS).await;
        h.play_for_test(0).await;
        assert_eq!(h.player.state(), PlayState::Playing);
        // Drop the current pointer while the raw state is still Playing.
        h.state.lock().unwrap().current = None;
        let snap = h.resume_snapshot(0.0);
        assert_eq!(snap.play_state, ResumePlayState::Stopped);
        assert_eq!(snap.current, None);
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

    // A `next` issued DURING the pause-out fade supersedes it: the fresh track
    // plays audibly instead of being ramped to silence and frozen Paused by the
    // PauseOut Terminal::Pause. Covers the pause-then-next gesture.
    #[tokio::test(start_paused = true)]
    async fn next_during_pause_fade_plays_the_new_track() {
        let Some((h, _events)) = handler_with_null_player() else { return };
        h.handle(MpdCommand::Add(NTS.to_string())).await;
        h.handle(MpdCommand::Add(NTS.to_string())).await;
        h.handle(MpdCommand::Play(Some(0))).await;
        assert_eq!(h.player.state(), PlayState::Playing);

        // Pause installs a PauseOut fade to silence (mpv still Playing during the
        // ramp). Let a couple of ticks apply so the fade is genuinely in flight.
        h.set_pause(Some(true)).await.unwrap();
        assert!(h.fade_active().await, "pause installs an in-flight fade");
        pump(20, 2).await;

        // `next` mid-fade must cancel the PauseOut fade and start track B audibly.
        h.handle(MpdCommand::Next).await;
        assert_eq!(h.state.lock().unwrap().current, Some(1));
        assert_eq!(h.player.state(), PlayState::Playing, "next must NOT end Paused");
        assert!(!h.fade_active().await, "the PauseOut fade must be superseded");
        assert_eq!(h.state.lock().unwrap().reported_volume(), 100);
        assert_eq!(h.state.lock().unwrap().target_volume, 100);
    }

    // A `play`/`next` issued AFTER the pause-out fade has fully completed (mpv
    // volume stuck at ~0, player Paused) re-asserts the baseline gain, so the new
    // track is audible rather than silent-while-reporting-baseline.
    #[tokio::test(start_paused = true)]
    async fn play_after_completed_pause_fade_restores_audible_gain() {
        let Some((h, _events)) = handler_with_null_player() else { return };
        h.handle(MpdCommand::Add(NTS.to_string())).await;
        h.handle(MpdCommand::Add(NTS.to_string())).await;
        h.handle(MpdCommand::Play(Some(0))).await;

        h.set_pause(Some(true)).await.unwrap();
        h.wait_for_fade().await;
        assert_eq!(h.player.state(), PlayState::Paused);
        // The pause terminal RESTORES mpv's volume to the baseline while paused (F4)
        // so no later play path can start silent - the live gain is back at baseline,
        // not the floor.
        assert!(h.live_gain_db() > SYNTH_FLOOR_DB + 5.0, "paused deck restored to baseline gain");

        // A fresh play (not resume) plays at the baseline so the track is audible.
        h.handle(MpdCommand::Play(Some(1))).await;
        assert_eq!(h.player.state(), PlayState::Playing);
        assert!(!h.fade_active().await);
        assert_eq!(h.state.lock().unwrap().reported_volume(), 100);
        assert!(h.live_gain_db() > SYNTH_FLOOR_DB + 5.0, "gain restored to audible");
    }

    // A manual setvol mid-fade SUPERSEDES the running fade (validate-before-abort)
    // as its OWN graduated glide: manual wins as a glide. The glide commits its
    // landing to target_volume synchronously at install, then animates to it; once
    // it settles the reported volume equals the landing (within the +/-1 dither).
    #[tokio::test(start_paused = true)]
    async fn manual_wins_last() {
        let Some((h, _events)) = handler_with_null_player() else { return };
        h.state.lock().unwrap().vol_dither_state = 0x1234_5678_9ABC_DEF0;
        h.start_fade(fade_args(FadeKind::Out, 60)).await.unwrap();
        assert!(h.fade_active().await);

        // setvol 30 mid-fade: the glide superseded the fade out (committed the
        // landing at install), then run it out.
        h.handle(MpdCommand::SetVol(30)).await;
        assert!(h.fade_active().await, "setvol is itself a glide fade");
        // Landing committed within [29,31] at install, before any tick.
        let committed = h.state.lock().unwrap().target_volume;
        assert!((29..=31).contains(&committed), "landing committed at install: {committed}");
        h.wait_for_fade().await;
        // Post-completion the slot may retain a FINISHED handle; `fading` is the
        // source of truth for "a fade is active" (see the fade_task NOTE).
        assert!(!h.state.lock().unwrap().fading, "the glide settled");
        assert_eq!(h.state.lock().unwrap().reported_volume(), committed);
        assert_eq!(h.state.lock().unwrap().target_volume, committed);
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

    // F1: once a setvol glide SETTLES, a low manual volume is reported as the
    // committed landing u8 VERBATIM, never round-tripped through the cubic dB
    // domain (which would floor <= 10 to 0). The landing is within +/-1 of the
    // request (the human dither); `setvol 0` lands EXACTLY 0 (a mute must not
    // un-mute via dither).
    #[tokio::test(start_paused = true)]
    async fn low_volume_reports_exactly() {
        let Some((h, _events)) = handler_with_null_player() else { return };
        h.state.lock().unwrap().vol_dither_state = 0xDEAD_BEEF_CAFE_1234;
        for v in [0u8, 1, 5, 7, 10, 33, 100] {
            h.handle(MpdCommand::SetVol(v)).await;
            h.wait_for_fade().await;
            let got = match h.handle(MpdCommand::GetVol).await {
                MpdResponse::Pairs(p) => p
                    .iter()
                    .find(|(k, _)| k == "volume")
                    .map(|(_, val)| val.parse::<u8>().unwrap())
                    .unwrap(),
                other => panic!("got {other:?}"),
            };
            if v == 0 {
                assert_eq!(got, 0, "setvol 0 lands exactly 0 (no un-mute dither)");
            } else {
                let lo = v.saturating_sub(1);
                let hi = (v + 1).min(100);
                assert!((lo..=hi).contains(&got), "setvol {v} lands in [{lo},{hi}], got {got}");
                assert!(v < 5 || got > 0, "a low but audible setvol is never floored to 0");
            }
            assert_eq!(h.volume(), got, "MPRIS volume must match the settled getvol");
        }
    }

    // F2: `fade in` from silence ramps UP to the wake ceiling (0 dB == vol 100),
    // never a degenerate no-op. Start muted, fade in, and the reported/baseline
    // volume settles at the ceiling.
    #[tokio::test(start_paused = true)]
    async fn fade_in_ramps_up_from_silence() {
        let Some((h, _events)) = handler_with_null_player() else { return };
        // Start from silence: setvol 0 glides down to the floor, then settles at 0.
        h.handle(MpdCommand::SetVol(0)).await;
        h.wait_for_fade().await;
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
            commit_logical: None,
        })
        .await
        .unwrap();
        assert!(h.fade_active().await);
        h.wait_for_fade().await;
        assert_eq!(h.player.state(), PlayState::Stopped);
    }

    // Issue 1 (pause): a transport PAUSE fades to silence FIRST, THEN pauses mpv -
    // so the audio is already muted at the freeze (no click). Under paused time the
    // fade is driven to completion; at the terminal the player is Paused and the
    // baseline volume is both preserved and re-asserted on mpv (F4), so the live
    // gain sits back at the baseline rather than stuck at the faded-down silence.
    #[tokio::test(start_paused = true)]
    async fn pause_fades_to_silence_then_pauses() {
        let Some((h, _events)) = handler_with_null_player() else { return };
        h.handle(MpdCommand::Add(NTS.to_string())).await;
        h.handle(MpdCommand::Play(Some(0))).await;
        assert_eq!(h.player.state(), PlayState::Playing);

        // Pause: installs the fade-to-silence; the pause runs in its terminal.
        h.handle(MpdCommand::Pause(Some(true))).await;
        assert!(h.fade_active().await, "pause installs a fade to silence");
        // While the fade ramps down the player is STILL playing (fade THEN pause).
        assert_eq!(h.player.state(), PlayState::Playing, "not paused until silence");

        h.wait_for_fade().await;
        // Now paused, baseline preserved for the eventual resume. The pause terminal
        // RESTORES the live gain to the baseline (F4) - the deck is silent because it
        // is paused, not because the volume is stuck at 0 - so a later fresh play is
        // never silent.
        assert_eq!(h.player.state(), PlayState::Paused, "paused after the fade");
        assert!(
            h.live_gain_db() > h.fade_cfg.synth_floor_db + 5.0,
            "paused deck gain restored to baseline, not stuck at silence"
        );
        assert_eq!(h.state.lock().unwrap().target_volume, 100, "baseline preserved");
    }

    // Issue 1 (resume): a transport RESUME unpauses from silence THEN sub-JND ramps
    // the live gain back UP to the pre-pause baseline (monotone, never overshoot).
    #[tokio::test(start_paused = true)]
    async fn resume_fades_in_from_silence() {
        let Some((h, _events)) = handler_with_null_player() else { return };
        h.handle(MpdCommand::Add(NTS.to_string())).await;
        h.handle(MpdCommand::Play(Some(0))).await;
        // Pause and settle to silence.
        h.handle(MpdCommand::Pause(Some(true))).await;
        h.wait_for_fade().await;
        assert_eq!(h.player.state(), PlayState::Paused);

        // Resume: unpauses immediately (Playing edge), then ramps from silence.
        h.handle(MpdCommand::Pause(Some(false))).await;
        assert_eq!(h.player.state(), PlayState::Playing, "unpaused before the ramp");
        assert!(h.fade_active().await, "resume installs a fade-in");
        // The ramp starts from silence (forced), not the prior level.
        assert!(
            h.live_gain_db() <= h.fade_cfg.synth_floor_db + 1e-9,
            "resume ramp starts from silence"
        );

        h.wait_for_fade().await;
        // Ramp restored the pre-pause baseline.
        assert_eq!(h.state.lock().unwrap().target_volume, 100);
        assert_eq!(h.state.lock().unwrap().reported_volume(), 100);
    }

    // The knob steps in EQUAL PERCEPTUAL dB, not linearly: one `knob down` from full
    // (0 dB, vol 100) lands on the -3 dB detent = mpv vol 89, NOT the linear vol 95.
    // This is the cure for "frustrating small increments between volume values".
    #[tokio::test(start_paused = true)]
    async fn knob_down_steps_one_perceptual_detent() {
        let Some((h, _events)) = handler_with_null_player() else { return };
        h.handle(MpdCommand::Add(NTS.to_string())).await;
        h.handle(MpdCommand::Play(Some(0))).await;
        assert_eq!(h.state.lock().unwrap().reported_volume(), 100);

        h.handle(MpdCommand::Knob(KnobDir::Down)).await;
        h.wait_for_fade().await;
        // -3 dB detent: db_to_mpv_volume(-3) = 100*10^(-3/60) ~= 89, committed as
        // the new baseline. A linear step of 5 would have given 95.
        assert_eq!(h.state.lock().unwrap().reported_volume(), 89, "one 3 dB detent");
        assert_eq!(h.state.lock().unwrap().target_volume, 89);
        assert!(!h.state.lock().unwrap().pending_pause, "a normal step never pauses");

        // Successive detents ACCUMULATE and stay monotonic (turning the knob down
        // keeps descending, not plateauing): 89 -> 79 -> 71.
        h.handle(MpdCommand::Knob(KnobDir::Down)).await;
        h.wait_for_fade().await;
        assert_eq!(h.state.lock().unwrap().reported_volume(), 79, "-6 dB detent");
        h.handle(MpdCommand::Knob(KnobDir::Down)).await;
        h.wait_for_fade().await;
        assert_eq!(h.state.lock().unwrap().reported_volume(), 71, "-9 dB detent");

        // Up one detent climbs back (grid: -9 -> -6 dB, vol 79).
        h.handle(MpdCommand::Knob(KnobDir::Up)).await;
        h.wait_for_fade().await;
        assert_eq!(h.state.lock().unwrap().reported_volume(), 79, "climbs back one detent");
    }

    // GRADUATED + HUMANIZED absolute volume: a setvol GLIDES to the target (it does
    // NOT snap) and lands WITHIN +/-1 vol of the request (the seeded human dither).
    // Mid-glide the reported volume tracks the in-flight envelope (honest), and the
    // final landing is byte-for-byte reproducible under a pinned seed.
    #[tokio::test(start_paused = true)]
    async fn glide_lands_within_one_vol() {
        let run_once = || async {
            let Some((h, _events)) = handler_with_null_player() else { return None };
            h.handle(MpdCommand::Add(NTS.to_string())).await;
            h.handle(MpdCommand::Play(Some(0))).await;
            // Start at 70, pin the dither seed, then glide down to 50.
            {
                let mut st = h.state.lock().unwrap();
                st.set_manual_volume(70);
                st.vol_dither_state = 0xF00D_1357_2468_ACE0;
            }
            h.handle(MpdCommand::SetVol(50)).await;
            // It is a GLIDE, not a snap: a fade is in flight and the reported value
            // is tracking the envelope (still near 70, not already 50).
            assert!(h.fade_active().await, "setvol installs a glide, never snaps");
            assert!(h.state.lock().unwrap().fading, "reported tracks the envelope");
            h.wait_for_fade().await;
            let landed = h.state.lock().unwrap().target_volume;
            assert!(!h.state.lock().unwrap().fading, "the glide settled");
            assert!((49..=51).contains(&landed), "landing within +/-1 of 50: {landed}");
            Some(landed)
        };
        let a = run_once().await;
        let b = run_once().await;
        // Same seed -> byte-for-byte reproducible landing.
        assert_eq!(a, b, "a pinned dither seed lands deterministically");
    }

    // The dither is REAL: two DIFFERENT seeds both land within +/-1 of the request
    // but need not be equal (proves it dithers - bounded, never wild).
    #[tokio::test(start_paused = true)]
    async fn glide_dithers_deterministically() {
        let land_with = |seed: u64| async move {
            let Some((h, _events)) = handler_with_null_player() else { return None };
            h.handle(MpdCommand::Add(NTS.to_string())).await;
            h.handle(MpdCommand::Play(Some(0))).await;
            {
                let mut st = h.state.lock().unwrap();
                st.set_manual_volume(90);
                st.vol_dither_state = seed;
            }
            // Draw many landings and collect the set so a seed that happens to
            // agree on one value still shows variation across draws.
            let mut lands = Vec::new();
            for _ in 0..8 {
                h.handle(MpdCommand::SetVol(50)).await;
                h.wait_for_fade().await;
                let v = h.state.lock().unwrap().target_volume;
                assert!((49..=51).contains(&v), "bounded landing: {v}");
                lands.push(v);
            }
            Some(lands)
        };
        // Skip gracefully when there is no client (sandbox: no CA certs), exactly
        // like handler_with_null_player's callers - never unwrap a skipped run.
        let (Some(a), Some(b)) = (
            land_with(0x1111_2222_3333_4444).await,
            land_with(0x9999_8888_7777_6666).await,
        ) else {
            return;
        };
        // Each stream varies (not a constant), and the two seeds differ somewhere.
        let a_varies = a.iter().any(|&v| v != a[0]);
        let b_varies = b.iter().any(|&v| v != b[0]);
        assert!(a_varies || b_varies || a != b, "the dither actually perturbs the landing");
    }

    // setvol 0 (a mute / GNOME slider to 0) must land EXACTLY 0 and stay PLAYING -
    // the Glide never takes the knob off-click / pause branch (only the knob does).
    #[tokio::test(start_paused = true)]
    async fn setvol_0_does_not_pause() {
        let Some((h, _events)) = handler_with_null_player() else { return };
        h.handle(MpdCommand::Add(NTS.to_string())).await;
        h.handle(MpdCommand::Play(Some(0))).await;
        h.mpris_set_volume(0).await;
        h.wait_for_fade().await;
        assert_eq!(h.reported_play_state(), PlayState::Playing, "setvol 0 must NOT pause");
        assert_eq!(h.state.lock().unwrap().reported_volume(), 0, "lands exactly 0, no un-mute");
    }

    // A low but audible setvol is not floored to 0 by the dither/clamp: setvol 5
    // lands in [4,6], never 0 (guards the low-value setvol behavior).
    #[tokio::test(start_paused = true)]
    async fn setvol_low_not_floored() {
        let Some((h, _events)) = handler_with_null_player() else { return };
        h.state.lock().unwrap().vol_dither_state = 0x0BAD_F00D_1234_5678;
        h.handle(MpdCommand::SetVol(5)).await;
        h.wait_for_fade().await;
        let v = h.state.lock().unwrap().reported_volume();
        assert!((4..=6).contains(&v), "setvol 5 lands in [4,6]: {v}");
        assert_ne!(v, 0, "a low audible setvol is never floored to 0");
    }

    // Knob bug (a): from an OFF-GRID level (left by a prior absolute set) ONE knob
    // press moves to exactly the adjacent 3 dB rung - never 1.5-4.5 dB, never skips.
    #[tokio::test(start_paused = true)]
    async fn knob_off_grid_single_rung() {
        let Some((h, _events)) = handler_with_null_player() else { return };
        h.handle(MpdCommand::Add(NTS.to_string())).await;
        h.handle(MpdCommand::Play(Some(0))).await;
        h.state.lock().unwrap().vol_dither_state = 0x2468_1357_9BDF_0ACE;
        // 55 -> ~-15.6 dB, deliberately OFF the 3 dB grid (rungs at -15, -18).
        h.handle(MpdCommand::SetVol(55)).await;
        h.wait_for_fade().await;
        let pre = h.state.lock().unwrap().logical_gain_db;
        // Expected: the largest 3 dB rung STRICTLY BELOW `pre`.
        let expected = (pre / KNOB_STEP_DB).ceil() * KNOB_STEP_DB - KNOB_STEP_DB;
        h.handle(MpdCommand::Knob(KnobDir::Down)).await;
        // The rung is committed to logical_gain_db SYNCHRONOUSLY at install.
        let committed = h.state.lock().unwrap().logical_gain_db;
        assert!((committed - expected).abs() < 1e-9, "adjacent rung {expected}, got {committed}");
        // It is exactly on the 3 dB grid, and the move is one rung (< a full step
        // from an off-grid start, never a skipped rung).
        assert!((committed / KNOB_STEP_DB).fract().abs() < 1e-9, "on the 3 dB grid");
        assert!(pre - committed > 0.0 && pre - committed <= KNOB_STEP_DB + 1e-9, "one detent down");
    }

    // Knob bugs (b)+(c): N rapid knob-downs whose fades SUPERSEDE each other still
    // commit EVERY intermediate rung to the logical target synchronously, so the
    // baseline sits at the Nth rung (not the loud pre-mash level), and a resume
    // ramps back to that committed quiet rung.
    #[tokio::test(start_paused = true)]
    async fn knob_mash_commits_every_rung() {
        let Some((h, _events)) = handler_with_null_player() else { return };
        h.handle(MpdCommand::Add(NTS.to_string())).await;
        h.handle(MpdCommand::Play(Some(0))).await;
        assert_eq!(h.state.lock().unwrap().reported_volume(), 100);
        // Three rapid detents down (each supersedes the last in-flight fade). From
        // 0 dB the rungs are -3, -6, -9; -9 dB = mpv vol 71.
        h.handle(MpdCommand::Knob(KnobDir::Down)).await;
        h.handle(MpdCommand::Knob(KnobDir::Down)).await;
        h.handle(MpdCommand::Knob(KnobDir::Down)).await;
        // The Nth rung is committed to the baseline even though the fades superseded
        // (bug b/c): target_volume already sits at the quiet -9 dB rung.
        assert_eq!(h.state.lock().unwrap().target_volume, 71, "every mashed rung committed");
        h.wait_for_fade().await;
        assert_eq!(h.state.lock().unwrap().reported_volume(), 71);

        // Pause, then resume via knob-up: the resume ramps back FROM the committed
        // quiet rung (the baseline), never the loud pre-mash level.
        h.handle(MpdCommand::Pause(Some(true))).await;
        h.wait_for_fade().await;
        assert_eq!(h.reported_play_state(), PlayState::Paused);
        h.handle(MpdCommand::Knob(KnobDir::Up)).await;
        assert_eq!(h.player.state(), PlayState::Playing, "knob up resumes");
        h.wait_for_fade().await;
        let resumed = h.state.lock().unwrap().reported_volume();
        // Resumed at the committed quiet rung's neighbourhood (one detent up from
        // 71), never back at the loud 100.
        assert!(resumed <= 80, "resume climbs from the committed quiet rung, not loud: {resumed}");
    }

    // The bottom of the knob is a real off-click: a `knob down` from the lowest
    // audible detent (at/below floor_level_db) PAUSES via the exact same pause path
    // as the `p` key. Position at the floor deterministically with `setvol` (which
    // commits live_gain_db) rather than descending 15 detents through the fade slot.
    #[tokio::test(start_paused = true)]
    async fn knob_down_at_floor_off_clicks_to_pause() {
        let Some((h, _events)) = handler_with_null_player() else { return };
        h.handle(MpdCommand::Add(NTS.to_string())).await;
        h.handle(MpdCommand::Play(Some(0))).await;
        // Position the committed logical target AT the floor rung (-45 dB, the
        // lowest audible detent) deterministically. The next `knob down` targets
        // the rung strictly below (-48 dB), which crosses the floor - the
        // off-click. (Direct state write, so the glide/dither never perturbs the
        // starting rung; the glide is covered by its own tests.)
        {
            let mut st = h.state.lock().unwrap();
            st.live_gain_db = -45.0;
            st.logical_gain_db = -45.0;
            st.target_volume = db_to_mpv_volume(-45.0).round() as u8;
            st.fading = false;
        }
        assert_eq!(h.reported_play_state(), PlayState::Playing);

        h.handle(MpdCommand::Knob(KnobDir::Down)).await;
        // Off-click installs the pause fade (pending_pause set immediately).
        assert!(h.state.lock().unwrap().pending_pause, "off-click uses the pause path");
        h.wait_for_fade().await;
        assert_eq!(h.reported_play_state(), PlayState::Paused, "knob past the floor pauses");
    }

    // After a knob detent SETTLES, logical_gain_db is re-derived from the rounded u8
    // volume, so it lands slightly OFF the 3 dB grid (vol 79 = -6.14 dB, just below
    // the -6 line). A knob-up must still climb a FULL detent (-6 -> -3 = vol 89),
    // never plateau on the same rung by nudging only the sub-rung sliver. This is the
    // realistic path the earlier test masks by only ever stepping up from -9.
    #[tokio::test(start_paused = true)]
    async fn knob_up_from_settled_rung_advances_a_full_detent() {
        let Some((h, _events)) = handler_with_null_player() else { return };
        h.handle(MpdCommand::Add(NTS.to_string())).await;
        h.handle(MpdCommand::Play(Some(0))).await;
        // Descend two detents to the -6 dB rung, letting EACH settle so logical is
        // the re-quantized off-grid value (exactly the state the finding hits).
        h.handle(MpdCommand::Knob(KnobDir::Down)).await;
        h.wait_for_fade().await;
        h.handle(MpdCommand::Knob(KnobDir::Down)).await;
        h.wait_for_fade().await;
        assert_eq!(h.state.lock().unwrap().reported_volume(), 79, "-6 dB rung");
        let settled = h.state.lock().unwrap().logical_gain_db;
        assert!(settled < -6.0, "settled just BELOW the -6 grid line: {settled}");
        // Up one detent must reach the -3 dB rung (vol 89), never plateau at 79.
        h.handle(MpdCommand::Knob(KnobDir::Up)).await;
        h.wait_for_fade().await;
        assert_eq!(
            h.state.lock().unwrap().reported_volume(),
            89,
            "climbs a full detent from the off-grid settled rung, no plateau"
        );
    }

    // The off-click fires from the REALISTIC settled bottom rung: after a real settle
    // at the floor the committed logical is the u8-requantized value (vol 18 = -44.68
    // dB, ABOVE the -45 floor line), yet a knob DOWN must still cross the floor and
    // off-click to pause - not plateau just above it. (The other off-click test
    // writes logical = -45.0 EXACTLY, a value a real settle never produces.)
    #[tokio::test(start_paused = true)]
    async fn knob_off_click_from_realistic_settled_floor() {
        let Some((h, _events)) = handler_with_null_player() else { return };
        h.handle(MpdCommand::Add(NTS.to_string())).await;
        h.handle(MpdCommand::Play(Some(0))).await;
        {
            let mut st = h.state.lock().unwrap();
            // The realistic post-settle state at the bottom rung: target_volume is
            // the u8 (vol 18) and logical is its requantized dB (-44.68, above the
            // -45 floor), exactly what setvol+settle leaves - not the -45.0 shortcut.
            let v = db_to_mpv_volume(-45.0).round() as u8;
            st.target_volume = v;
            st.live_gain_db = mpv_volume_to_db(v as f64);
            st.logical_gain_db = st.live_gain_db;
            st.baseline_committed = true;
            st.fading = false;
        }
        let logical = h.state.lock().unwrap().logical_gain_db;
        assert!(logical > -45.0, "settled just ABOVE the floor (off-grid): {logical}");
        h.handle(MpdCommand::Knob(KnobDir::Down)).await;
        assert!(
            h.state.lock().unwrap().pending_pause,
            "crosses the floor from the realistic settled rung and off-clicks to pause"
        );
        h.wait_for_fade().await;
        assert_eq!(h.reported_play_state(), PlayState::Paused);
    }

    // A knob DOWN during a NON-committing fade (a gentle wake ramp climbing from
    // silence) must step from the LIVE in-flight gain, not the STALE pre-sleep
    // baseline still sitting in logical_gain_db. Stepping from the stale loud
    // baseline would compute a target near full loudness and JUMP the volume UP - a
    // startle that defeats the wake.
    #[tokio::test(start_paused = true)]
    async fn knob_down_mid_wake_steps_from_live_not_stale_baseline() {
        let Some((h, _events)) = handler_with_null_player() else { return };
        h.handle(MpdCommand::Add(NTS.to_string())).await;
        h.handle(MpdCommand::Play(Some(0))).await;
        // Simulate mid-wake: stale loud baseline (0 dB), quiet live gain (-30 dB),
        // a non-committing fade in flight - exactly what a WakeTo/In install leaves
        // (commit_logical = None => baseline_committed = false).
        {
            let mut st = h.state.lock().unwrap();
            st.logical_gain_db = 0.0;
            st.live_gain_db = -30.0;
            st.fading = true;
            st.baseline_committed = false;
        }
        h.handle(MpdCommand::Knob(KnobDir::Down)).await;
        // The committed target is one detent BELOW the live -30 dB (a quiet -33),
        // never a jump UP toward -3 dB / full loudness.
        let committed = h.state.lock().unwrap().logical_gain_db;
        assert!(
            committed < -30.0,
            "stepped down from the live quiet gain, not up from the stale baseline: {committed}"
        );
    }

    // A knob-up while paused RESUMES - the same set_pause resume path as any other
    // Play, so there is exactly ONE pause mechanism.
    #[tokio::test(start_paused = true)]
    async fn knob_up_while_paused_resumes() {
        let Some((h, _events)) = handler_with_null_player() else { return };
        h.handle(MpdCommand::Add(NTS.to_string())).await;
        h.handle(MpdCommand::Play(Some(0))).await;
        // Pause via the p-key path, settle to silence.
        h.handle(MpdCommand::Pause(Some(true))).await;
        h.wait_for_fade().await;
        assert_eq!(h.player.state(), PlayState::Paused);

        // Knob up resumes (unpause immediately, then ramp back up).
        h.handle(MpdCommand::Knob(KnobDir::Up)).await;
        assert_eq!(h.player.state(), PlayState::Playing, "knob up resumes from paused");
        h.wait_for_fade().await;
        assert!(!h.state.lock().unwrap().pending_pause, "resumed, no longer pending-pause");
    }

    // Issue 2 (MPRIS): after a pause the reported play state is Paused AND the shared
    // `changed` signal fires on the transition - that signal is exactly what the
    // MPRIS run_property_updates loop awaits before re-emitting PropertiesChanged, so
    // asserting it fires proves the desktop-widget-refresh path is wired. Before the
    // fix the MPRIS pause bypassed notify_change and no signal ever fired.
    #[tokio::test(start_paused = true)]
    async fn pause_reports_paused_and_signals_change() {
        let Some((h, _events)) = handler_with_null_player() else { return };
        let h = Arc::new(h);
        h.handle(MpdCommand::Add(NTS.to_string())).await;
        h.handle(MpdCommand::Play(Some(0))).await;

        // A background subscriber counts change signals (the run_property_updates
        // wake source). Prime it before pausing so we only count the pause edge.
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
        let before = count.load(std::sync::atomic::Ordering::SeqCst);

        // Pause via the MPRIS-facing entry point (same one PlayerInterface::pause
        // calls), then drive the fade to completion so the pause terminal runs.
        h.set_pause(Some(true)).await.unwrap();
        h.wait_for_fade().await;
        tokio::task::yield_now().await;

        // The raw player state is Paused, so effective_play_state (current.is_some())
        // reports Paused -> MPRIS PlaybackStatus = Paused.
        assert_eq!(h.player.state(), PlayState::Paused);
        let has_current = h.current_item().is_some();
        assert_eq!(
            effective_play_state(h.player.state(), has_current),
            PlayState::Paused,
            "MPRIS PlaybackStatus source reports Paused"
        );
        // And a change signal fired on the transition (the PropertiesChanged path).
        let after = count.load(std::sync::atomic::Ordering::SeqCst);
        assert!(after > before, "a change signal must fire on the pause edge");
    }

    // F2: the instant a pause is REQUESTED the reported state flips to Paused -
    // BEFORE the fade to silence completes and freezes mpv. Asserts it at all three
    // outward surfaces (MPD status, the MPRIS/status source, and a resume checkpoint)
    // while mpv is still raw-Playing mid-fade.
    #[tokio::test(start_paused = true)]
    async fn pause_reports_paused_immediately_before_fade_completes() {
        let Some((h, _events)) = handler_with_null_player() else { return };
        h.handle(MpdCommand::Add(NTS.to_string())).await;
        h.handle(MpdCommand::Play(Some(0))).await;

        // Request the pause and let only a tick or two elapse - the fade is still in
        // flight and mpv is still raw-Playing.
        h.set_pause(Some(true)).await.unwrap();
        pump(20, 2).await;
        assert!(h.fade_active().await, "the pause fade is still running");
        assert_eq!(h.player.state(), PlayState::Playing, "mpv not frozen yet");

        // MPD status reports pause immediately.
        assert_eq!(
            pair(&h.handle(MpdCommand::Status).await, "state"),
            Some("pause"),
            "MPD status must report pause the instant it is requested"
        );
        // The MPRIS/status source (reported_play_state) reports Paused immediately.
        assert_eq!(h.reported_play_state(), PlayState::Paused);
        // A checkpoint taken mid-fade persists Paused, never a stale Playing (so a
        // crash-resume would not auto-play).
        let snap = h.resume_snapshot(1.0);
        assert_eq!(snap.play_state, ResumePlayState::Paused);
    }

    // F1: the pause fade is a SHORT DELIBERATE fade (3 dB/step cap), NOT the long
    // ~20s sub-JND fade to silence. From full volume the whole span (0 -> -60 dB)
    // needs ceil(60/3)+1 = 21 steps, not the 80+ sub-JND steps.
    #[tokio::test(start_paused = true)]
    async fn pause_fade_is_short_deliberate_not_sub_jnd() {
        let Some((h, _events)) = handler_with_null_player() else { return };
        h.handle(MpdCommand::Add(NTS.to_string())).await;
        h.handle(MpdCommand::Play(Some(0))).await;

        // Count the distinct live-gain steps the pause fade applies by sampling as it
        // runs. A deliberate fade lands in ~21 steps (5s at 250ms); a sub-JND fade
        // would take 80+ steps (~20s). Assert it completes well under the sub-JND
        // count by driving a bounded number of ticks and checking it finished.
        h.set_pause(Some(true)).await.unwrap();
        // 30 ticks of 250ms = 7.5s: enough for the ~21-step deliberate fade to fully
        // land (and reach the Pause terminal), but far short of a 20s sub-JND fade.
        pump(250, 30).await;
        assert_eq!(
            h.player.state(),
            PlayState::Paused,
            "a deliberate pause fade must have completed and paused within ~7.5s"
        );
    }

    // F5: a Play/Resume issued DURING the pause-out fade window aborts the pause and
    // keeps playing, rather than being dropped or re-pausing. Keys off the pending-
    // pause intent, not the stale raw-Playing state.
    #[tokio::test(start_paused = true)]
    async fn resume_during_pause_fade_keeps_playing() {
        let Some((h, _events)) = handler_with_null_player() else { return };
        h.handle(MpdCommand::Add(NTS.to_string())).await;
        h.handle(MpdCommand::Play(Some(0))).await;

        h.set_pause(Some(true)).await.unwrap();
        pump(20, 2).await;
        assert!(h.fade_active().await);
        assert_eq!(h.reported_play_state(), PlayState::Paused, "pending pause");

        // Resume (MPRIS Play / PlayPause path) mid-fade: must take the resume branch
        // off the pending intent, abort the pause, and stay Playing.
        h.set_pause(Some(false)).await.unwrap();
        assert_eq!(h.player.state(), PlayState::Playing);
        assert_eq!(h.reported_play_state(), PlayState::Playing, "no longer pending");
        h.wait_for_fade().await;
        // The resume ramp restored the baseline; the deck is audibly playing.
        assert_eq!(h.state.lock().unwrap().reported_volume(), 100);
        assert!(h.live_gain_db() > SYNTH_FLOOR_DB + 5.0);
    }

    // F5 (un-dip): resuming while the PauseOut is STILL IN FLIGHT (mpv raw-Playing,
    // the ramp mid-descent well above silence) must ramp UP from the CURRENT gain, NOT
    // snap to silence and fade up. The gain must be monotonic-up: it never dips below
    // the mid-fade level it was at when resume was pressed.
    #[tokio::test(start_paused = true)]
    async fn resume_mid_pause_undips_without_snapping_to_silence() {
        let Some((h, _events)) = handler_with_null_player() else { return };
        h.handle(MpdCommand::Add(NTS.to_string())).await;
        h.handle(MpdCommand::Play(Some(0))).await;

        // Pause, then advance the clock so the PauseOut is roughly halfway down: the
        // gain is well above silence and the deck is STILL raw-Playing (fade THEN
        // pause), so a resume here is the in-flight-abort case, not the settled case.
        h.set_pause(Some(true)).await.unwrap();
        pump(250, 7).await;
        assert!(h.fade_active().await, "pause fade in flight");
        assert_eq!(h.player.state(), PlayState::Playing, "not frozen yet: still ramping");
        let mid = h.live_gain_db();
        assert!(
            mid > SYNTH_FLOOR_DB + 5.0 && mid < -1.0,
            "gain is mid-descent, above silence (was {mid})"
        );

        // Resume mid-fade: it must un-dip from `mid`, never snap to 0 first.
        h.set_pause(Some(false)).await.unwrap();
        assert_eq!(h.reported_play_state(), PlayState::Playing, "no longer pending");
        assert_eq!(h.player.state(), PlayState::Playing, "no spurious re-pause");
        // (a) NO drop to silence: the very next observable gain is at/above `mid`.
        assert!(
            h.live_gain_db() >= mid - 1e-6,
            "resume must not snap below the mid-fade gain (mid={mid}, now={})",
            h.live_gain_db()
        );

        // (b) it ramps UP, monotonically, back toward the baseline: sample across the
        // ramp and assert the gain never dips below `mid`.
        let mut prev = h.live_gain_db();
        for _ in 0..40 {
            pump(20, 1).await;
            let now = h.live_gain_db();
            assert!(now >= mid - 1e-6, "gain dipped below the mid-fade level: {now} < {mid}");
            assert!(now >= prev - 1e-6, "gain must be monotonic up: {now} < {prev}");
            prev = now;
            if !h.fade_active().await {
                break;
            }
        }
        h.wait_for_fade().await;
        // (c) ends Playing at the baseline.
        assert_eq!(h.player.state(), PlayState::Playing);
        assert_eq!(h.state.lock().unwrap().reported_volume(), 100, "back at the baseline");
        assert!(h.live_gain_db() > SYNTH_FLOOR_DB + 5.0);
    }

    // F4: after a pause, a STOP then PLAY (or a fresh queue) starts at the baseline
    // volume, never silent - the pause left mpv restored to the baseline, and the
    // fresh play re-asserts it.
    #[tokio::test(start_paused = true)]
    async fn stop_then_play_after_pause_starts_at_baseline() {
        let Some((h, _events)) = handler_with_null_player() else { return };
        h.handle(MpdCommand::Add(NTS.to_string())).await;
        h.handle(MpdCommand::Add(NTS.to_string())).await;
        h.handle(MpdCommand::Play(Some(0))).await;

        h.set_pause(Some(true)).await.unwrap();
        h.wait_for_fade().await;
        assert_eq!(h.player.state(), PlayState::Paused);

        // Stop clears the pending pause and settles the baseline.
        h.handle(MpdCommand::Stop).await;
        assert_eq!(h.reported_play_state(), PlayState::Stopped);

        // A fresh play starts audible at the baseline, not silent.
        h.handle(MpdCommand::Play(Some(0))).await;
        assert_eq!(h.player.state(), PlayState::Playing);
        assert_eq!(h.reported_play_state(), PlayState::Playing);
        assert_eq!(h.state.lock().unwrap().reported_volume(), 100);
        assert!(h.live_gain_db() > SYNTH_FLOOR_DB + 5.0, "fresh play not silent");
    }

    // A natural EOF advance must NOT cancel an in-flight fade or snap mpv's gain back
    // to the baseline: a slow winddown/sleep ramp has to continue across the track
    // boundary (it is not a user gesture).
    #[tokio::test(start_paused = true)]
    async fn eof_advance_preserves_in_flight_fade() {
        let Some((h, _events)) = handler_with_null_player() else { return };
        h.handle(MpdCommand::Add(NTS.to_string())).await;
        h.handle(MpdCommand::Add(NTS.to_string())).await;
        h.handle(MpdCommand::Play(Some(0))).await;

        // Arm a wind-down (ToFloor) ramp; let it drop the gain a bit.
        h.start_fade(fade_args(FadeKind::ToFloor, 30)).await.unwrap();
        pump(250, 6).await;
        assert!(h.fade_active().await, "winddown running");
        let mid = h.live_gain_db();
        assert!(mid < -1.0, "gain has started descending (was {mid})");

        // Natural EOF advance to the next track: the fade must survive, not be wiped
        // and the gain re-asserted to the baseline.
        h.advance_on_eof().await;
        assert_eq!(h.state.lock().unwrap().current, Some(1), "advanced to next track");
        assert!(h.fade_active().await, "winddown must survive the track boundary");
        assert!(
            h.live_gain_db() <= mid + 1e-6,
            "gain must not snap back to the baseline at EOF"
        );
    }

    // C2: a manual setvol against a running fade SUPERSEDES it (validate-before-
    // abort) as its own glide, then - once the glide settles - leaves NO surviving
    // fade task and reports the committed landing (within +/-1). The atomicity
    // invariant still holds: the slot and the `fading` switch agree at the end.
    #[tokio::test(start_paused = true)]
    async fn setvol_leaves_no_surviving_fade() {
        let Some((h, _events)) = handler_with_null_player() else { return };
        h.state.lock().unwrap().vol_dither_state = 0x00FF_1122_3344_5566;
        h.start_fade(fade_args(FadeKind::Out, 120)).await.unwrap();
        pump(250, 6).await;
        assert!(h.fade_active().await && h.live_gain_db() < 0.0);

        // setvol from a second logical caller: superseded the fade out, committed
        // the landing at install, then run the glide out.
        h.handle(MpdCommand::SetVol(42)).await;
        let committed = h.state.lock().unwrap().target_volume;
        assert!((41..=43).contains(&committed), "landing in [41,43]: {committed}");
        h.wait_for_fade().await;

        let st = h.state.lock().unwrap();
        assert!(!st.fading, "fading switch cleared once the glide settled");
        assert_eq!(st.target_volume, committed);
        assert_eq!(st.reported_volume(), committed);
    }

    // A setvol that supersedes an in-flight PauseOut fade (before its Terminal::Pause
    // freezes mpv) must clear the pending-pause intent: otherwise reported_play_state
    // lies Paused forever while mpv keeps playing at the new volume.
    #[tokio::test(start_paused = true)]
    async fn setvol_during_pause_fade_clears_pending_pause() {
        let Some((h, _events)) = handler_with_null_player() else { return };
        h.handle(MpdCommand::Add(NTS.to_string())).await;
        h.handle(MpdCommand::Play(Some(0))).await;

        h.set_pause(Some(true)).await.unwrap();
        pump(20, 2).await;
        assert!(h.fade_active().await, "pause fade still running");
        assert_eq!(h.player.state(), PlayState::Playing, "mpv not frozen yet");
        assert_eq!(h.reported_play_state(), PlayState::Paused, "pending pause");

        // setvol supersedes the pause fade before it froze mpv. The glide's install
        // clears pending_pause SYNCHRONOUSLY (under the slot lock, like a manual
        // commit), so the deck reports Playing IMMEDIATELY even while the glide is
        // still animating - never stuck Paused with audio audible.
        h.handle(MpdCommand::SetVol(80)).await;
        assert_eq!(h.player.state(), PlayState::Playing, "mpv still playing");
        assert_eq!(h.reported_play_state(), PlayState::Playing);
        let committed = h.state.lock().unwrap().target_volume;
        assert!((79..=81).contains(&committed), "landing in [79,81]: {committed}");
        h.wait_for_fade().await;
        assert!(!h.state.lock().unwrap().fading, "the glide settled");
        assert_eq!(h.state.lock().unwrap().reported_volume(), committed);
    }

    // C2: even when a `fade` from a second logical caller races a setvol, the end
    // state is always consistent - never the corrupt "no fade in the slot yet the
    // reported volume derives from a dead envelope" state. Whichever wins, the
    // slot and the reported volume agree.
    #[tokio::test(start_paused = true)]
    async fn setvol_atomic_against_concurrent_fade() {
        let Some((h, _events)) = handler_with_null_player() else { return };
        let h = Arc::new(h);
        h.state.lock().unwrap().vol_dither_state = 0x7788_99AA_BBCC_DDEE;
        h.start_fade(fade_args(FadeKind::Out, 120)).await.unwrap();
        pump(250, 4).await;

        let h2 = h.clone();
        let fade_fut = tokio::spawn(async move {
            let _ = h2.start_fade(fade_args(FadeKind::To(60), 120)).await;
        });
        h.handle(MpdCommand::SetVol(20)).await;
        let committed = h.state.lock().unwrap().target_volume;
        let _ = fade_fut.await;
        // Run whatever fade owns the slot out to completion so nothing is mid-flight.
        h.wait_for_fade().await;
        pump(250, 2).await;

        let (fading, reported) = {
            let st = h.state.lock().unwrap();
            (st.fading, st.reported_volume())
        };
        // Once everything settles the `fading` switch is cleared (no dead envelope
        // driving the reported volume), and the reported value is the settled
        // baseline of whichever fade landed last: the glide landing (~20) or the
        // To(60) target.
        assert!(!fading, "no in-flight envelope after settle");
        assert!(
            reported == committed || (59..=60).contains(&reported),
            "settled at the glide landing or the To(60) target: {reported}"
        );
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

    // ── free-text Query keyword-split + OR-merge resolution (PURE) ─────────────

    // Owned-String vec from string literals, for QueryKeywords assertions.
    fn svec(xs: &[&str]) -> Vec<String> {
        xs.iter().map(|s| s.to_string()).collect()
    }

    #[test]
    fn query_keywords_strip_filler_and_dedup() {
        // A multi-word mood phrase reduces to its wanted content keywords (filler
        // stripped, nothing excluded).
        assert_eq!(
            query_content_keywords("queue some chill electronic stuff"),
            QueryKeywords { include: svec(&["chill", "electronic"]), exclude: vec![] }
        );
        // A negation ("not") pushes the terms AFTER it into `exclude` so the resolver
        // never searches for the rejected mood - the wrong-song bug.
        assert_eq!(
            query_content_keywords("active but not super head bumping"),
            QueryKeywords { include: svec(&["active"]), exclude: svec(&["head", "bumping"]) }
        );
        assert_eq!(
            query_content_keywords("active but not chill electronic"),
            QueryKeywords { include: svec(&["active"]), exclude: svec(&["chill", "electronic"]) }
        );
        // An all-filler ask yields NO include keywords -> the resolver falls back to a
        // literal whole-phrase search3 (an exact title path stays intact).
        assert!(query_content_keywords("play some music").include.is_empty());
        // Repeated content words collapse (search that keyword once).
        assert_eq!(
            query_content_keywords("funk funk funk"),
            QueryKeywords { include: svec(&["funk"]), exclude: vec![] }
        );
    }

    #[test]
    fn merge_keyword_hits_or_merges_relevance_ordered_and_deduped() {
        // Two keyword result lists: song "b" matches BOTH keywords, so it must lead
        // the OR-merge even though it is not first in either list.
        let chill = vec![mk_song("a"), mk_song("b")];
        let electronic = vec![mk_song("b"), mk_song("c")];
        let merged = merge_keyword_hits(vec![chill, electronic], 10);
        let ids: Vec<&str> = merged.iter().map(|s| s.id.0.as_str()).collect();
        // "b" (2 keywords) leads; "a" and "c" (1 each) follow in first-seen order.
        assert_eq!(ids, vec!["b", "a", "c"]);
    }

    #[test]
    fn merge_keyword_hits_dedups_within_and_across_keywords() {
        // A duplicate id within ONE keyword's hits must not double-count relevance
        // nor appear twice; across keywords the song is deduped to a single entry.
        let kw1 = vec![mk_song("x"), mk_song("x"), mk_song("y")];
        let kw2 = vec![mk_song("x")];
        let merged = merge_keyword_hits(vec![kw1, kw2], 10);
        let ids: Vec<&str> = merged.iter().map(|s| s.id.0.as_str()).collect();
        // "x" appears once (matched 2 distinct keywords -> leads), "y" once.
        assert_eq!(ids, vec!["x", "y"]);
    }

    #[test]
    fn merge_keyword_hits_honest_zero_and_bounded_take() {
        // NO keyword matched anything -> honest empty (no fabrication).
        assert!(merge_keyword_hits(vec![vec![], vec![]], 5).is_empty());
        // The requested count bounds the merged output.
        let hits = vec![mk_song("1"), mk_song("2"), mk_song("3")];
        assert_eq!(merge_keyword_hits(vec![hits], 2).len(), 2);
    }

    // A fake FeatureStore that reads a per-song energy off a lookup table (keyed by
    // song id), so calmer_rerank can be exercised with NO network/model/metadata.
    struct FakeStore(std::collections::HashMap<String, f32>);
    impl crate::intelligence::FeatureStore for FakeStore {
        fn features(&self, song: &Song) -> Option<crate::intelligence::TrackFeatures> {
            self.0.get(&song.id.0).map(|&e| crate::intelligence::TrackFeatures {
                energy: e,
                valence: 0.5,
                embedding: None,
            })
        }
    }

    // Calmer re-rank (PURE, fabricated pool + fake store): seed energy 0.7,
    // candidates {0.2,0.5,0.6,0.9} -> keep {0.2,0.5,0.6} ascending, truncated to 3.
    #[test]
    fn calmer_rerank_keeps_calmer_ascending() {
        let mut e = std::collections::HashMap::new();
        e.insert("seed".to_string(), 0.7);
        e.insert("a".to_string(), 0.2);
        e.insert("b".to_string(), 0.5);
        e.insert("c".to_string(), 0.6);
        e.insert("d".to_string(), 0.9);
        let store = FakeStore(e);
        let seed = mk_song("seed");
        let pool = vec![mk_song("d"), mk_song("b"), mk_song("a"), mk_song("c")];
        let out = calmer_rerank(&store, &seed, pool, 3);
        let ids: Vec<&str> = out.iter().map(|s| s.id.0.as_str()).collect();
        assert_eq!(ids, vec!["a", "b", "c"], "calmer ascending, 0.9 dropped");
    }

    // When NOTHING is calmer than the seed, top-up returns the lowest-energy
    // candidates instead of an empty result (never ramp/enqueue nothing).
    #[test]
    fn calmer_rerank_tops_up_when_none_calmer() {
        let mut e = std::collections::HashMap::new();
        e.insert("seed".to_string(), 0.1); // everything is louder than the seed
        e.insert("a".to_string(), 0.4);
        e.insert("b".to_string(), 0.3);
        e.insert("c".to_string(), 0.9);
        let store = FakeStore(e);
        let seed = mk_song("seed");
        let pool = vec![mk_song("c"), mk_song("a"), mk_song("b")];
        let out = calmer_rerank(&store, &seed, pool, 2);
        let ids: Vec<&str> = out.iter().map(|s| s.id.0.as_str()).collect();
        // Lowest-energy two, ascending: b(0.3), a(0.4).
        assert_eq!(ids, vec!["b", "a"]);
    }

    // FadeIntent::WakeTo resolves to the SAVED perceptual level (not vol 100),
    // sub-JND, committing the saved baseline. Proves a wake restores the user's
    // real volume rather than ramping to full.
    #[test]
    fn wake_to_resolves_to_saved_level_sub_jnd() {
        let saved_vol = 60u8;
        let target = mpv_volume_to_db(saved_vol as f64);
        let intent = FadeIntent::WakeTo { target_db: target, vol: saved_vol };
        let (t, sub_jnd, terminal, _clamp) = intent.resolve(SYNTH_FLOOR_DB, 0.0, -45.0);
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
        let (t, sub_jnd, terminal, _clamp) = FadeIntent::ToFloor.resolve(0.0, 0.0, floor);
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
    #[tokio::test(start_paused = true)]
    async fn resume_snapshot_reflects_queue_and_state() {
        let Some((h, _events)) = handler_with_null_player() else { return };
        h.state.lock().unwrap().vol_dither_state = 0x0102_0304_0506_0708;
        h.enqueue_song_for_test(mk_song("s1")).await;
        h.enqueue_stream_for_test(NTS).await;
        h.enqueue_song_for_test(mk_song("s2")).await;
        h.play_for_test(2).await;
        // A volume set now GLIDES; run it out so the snapshot reads a settled level.
        h.mpris_set_volume(64).await;
        h.wait_for_fade().await;
        let vol = h.state.lock().unwrap().reported_volume();
        assert!((63..=65).contains(&vol), "settled glide landing in [63,65]: {vol}");

        let snap = h.resume_snapshot(31.5);
        assert_eq!(snap.schema_version, RESUME_SCHEMA_VERSION);
        assert_eq!(snap.queue.len(), 3);
        assert_eq!(snap.queue[0], ResumeItem::Song { id: "s1".into() });
        assert!(matches!(snap.queue[1], ResumeItem::Stream { .. }));
        assert_eq!(snap.queue[2], ResumeItem::Song { id: "s2".into() });
        assert_eq!(snap.current, Some(2));
        assert_eq!(snap.volume, vol);
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
            continuation: false,
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
            continuation: false,
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
    #[tokio::test(start_paused = true)]
    async fn checkpoint_writes_loadable_state_to_disk() {
        let Some((h, _events)) = handler_with_null_player() else { return };
        let dir = std::env::temp_dir().join(format!("hypodj-cp-{}", std::process::id()));
        let _ = std::fs::create_dir_all(&dir);
        let path = dir.join("resume.toml");
        h.set_state_path(path.clone());

        h.enqueue_stream_for_test(NTS).await;
        h.play_for_test(0).await;
        // Pause now fades to silence THEN pauses (in the fade terminal); drive the
        // fade to completion under paused time so the Paused edge lands before the
        // checkpoint reads it.
        h.handle(MpdCommand::Pause(Some(true))).await;
        h.wait_for_fade().await;
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

    // BUG 1 (waq4th1) drain regression: a Playing snapshot whose song ids can no
    // longer resolve (the offline client at 127.0.0.1:1 fails every song()) must
    // (a) ABORT restore with Err WITHOUT installing a partial/empty queue, and
    // (b) a checkpoint taken in the resulting empty+stopped state must NOT clobber
    // a pre-written good resume.toml. This reproduces the exact drain (all songs
    // skipped -> empty queue -> checkpoint overwrites the saved file) and proves
    // the two guards fix it.
    #[tokio::test(start_paused = true)]
    async fn restore_abort_and_checkpoint_preserve_saved_queue() {
        let Some((h, _events)) = handler_with_null_player() else { return };
        let s = ResumeState {
            schema_version: RESUME_SCHEMA_VERSION,
            queue: vec![
                ResumeItem::Song { id: "s1".into() },
                ResumeItem::Song { id: "s2".into() },
                ResumeItem::Song { id: "s3".into() },
            ],
            current: Some(0),
            elapsed_secs: 10.0,
            volume: 50,
            play_state: ResumePlayState::Playing,
            playlist_version: 9,
            saved_at_unix: 1,
            continuation: false,
        };
        // (a) restore aborts and leaves State untouched (no drain to empty, no
        // partial install).
        assert!(h.restore(&s).await.is_err(), "an unresolvable song must abort restore");
        assert_eq!(
            h.state.lock().unwrap().queue.len(),
            0,
            "restore must not install a partial/empty queue"
        );

        // (b) a pre-written GOOD file survives a checkpoint taken while the deck is
        // empty + stopped (the failed-restore aftermath).
        let dir = std::env::temp_dir().join(format!("hypodj-drain-{}", std::process::id()));
        let _ = std::fs::create_dir_all(&dir);
        let path = dir.join("resume.toml");
        let good = ResumeState {
            schema_version: RESUME_SCHEMA_VERSION,
            queue: vec![
                ResumeItem::Stream { url: NTS.into(), title: "a".into() },
                ResumeItem::Stream { url: NTS.into(), title: "b".into() },
                ResumeItem::Stream { url: NTS.into(), title: "c".into() },
            ],
            current: Some(1),
            elapsed_secs: 5.0,
            volume: 50,
            play_state: ResumePlayState::Playing,
            playlist_version: 9,
            saved_at_unix: 1,
            continuation: false,
        };
        crate::resume::store_atomic(&path, &good).expect("seed the good file");
        h.set_state_path(path.clone());
        // Empty + Stopped snapshot -> the checkpoint MUST skip the write.
        h.checkpoint(0.0).await;
        let loaded = crate::resume::load(&path).expect("good file still present");
        assert_eq!(
            loaded.queue.len(),
            3,
            "checkpoint must not clobber the saved queue with an empty one"
        );
        let _ = std::fs::remove_dir_all(&dir);
    }

    // BUG 2 (g94y41b): random/repeat/single/consume are HONORED by the advance
    // logic (plan_next). Deterministic - the seeded RNG makes `random` reproducible.
    #[tokio::test]
    async fn advance_honors_playback_mode_flags() {
        let Some((h, _events)) = handler_with_null_player() else { return };
        for _ in 0..3 {
            h.enqueue_stream_for_test(NTS).await;
        }
        let set = |cur: Option<usize>, r: bool, rp: bool, si: bool, co: bool| {
            let mut st = h.state.lock().unwrap();
            st.current = cur;
            st.random = r;
            st.repeat = rp;
            st.single = si;
            st.consume = co;
        };

        // Sequential (all flags off): advance, then stop at the end.
        set(Some(1), false, false, false, false);
        assert_eq!(h.plan_next(true), Some(2));
        set(Some(2), false, false, false, false);
        assert_eq!(h.plan_next(true), None, "end of queue stops when not repeating");

        // repeat-all: wrap from the last entry to the first.
        set(Some(2), false, true, false, false);
        assert_eq!(h.plan_next(true), Some(0), "repeat wraps to the head");

        // single (auto EOF): stop after the current track.
        set(Some(0), false, false, true, false);
        assert_eq!(h.plan_next(true), None, "single stops after the current track");
        // single + repeat: replay the same index.
        set(Some(1), false, true, true, false);
        assert_eq!(h.plan_next(true), Some(1), "single+repeat replays the current track");
        // single is ignored for a manual next (auto == false): it advances.
        set(Some(0), false, false, true, false);
        assert_eq!(h.plan_next(false), Some(1), "manual next ignores single");

        // random: a seeded pick, in range and avoiding an immediate repeat. Also
        // deterministic (same seed -> same pick).
        set(Some(0), true, false, false, false);
        h.state.lock().unwrap().rng_state = 0xDEAD_BEEF_CAFE_F00D;
        let a = h.plan_next(true).expect("random picks an entry");
        assert!(a < 3 && a != 0, "random pick is in range and not an immediate repeat");
        set(Some(0), true, false, false, false);
        h.state.lock().unwrap().rng_state = 0xDEAD_BEEF_CAFE_F00D;
        let b = h.plan_next(true).expect("random picks an entry");
        assert_eq!(a, b, "the seeded RNG is deterministic");
    }

    // consume removes the just-finished entry and remaps the next index over the
    // shrink (the entry that was at old idx 2 is now the target at idx 1).
    #[tokio::test]
    async fn advance_consume_removes_and_reindexes() {
        let Some((h, _events)) = handler_with_null_player() else { return };
        for _ in 0..3 {
            h.enqueue_stream_for_test(NTS).await;
        }
        {
            let mut st = h.state.lock().unwrap();
            st.current = Some(1);
            st.consume = true;
        }
        let next = h.plan_next(true);
        assert_eq!(next, Some(1), "old idx 2 shifts down into idx 1 after removing idx 1");
        assert_eq!(h.state.lock().unwrap().queue.len(), 2, "consume removed the played entry");
    }

    // The flags round-trip through status: a random/repeat/single/consume toggle
    // is reflected truthfully (not the old hardcoded zeros).
    #[tokio::test]
    async fn status_reports_playback_mode_flags() {
        let Some((h, _events)) = handler_with_null_player() else { return };
        h.handle(MpdCommand::Random(true)).await;
        h.handle(MpdCommand::Repeat(true)).await;
        h.handle(MpdCommand::Single(true)).await;
        h.handle(MpdCommand::Consume(true)).await;
        let resp = h.handle(MpdCommand::Status).await;
        assert_eq!(pair(&resp, "random"), Some("1"));
        assert_eq!(pair(&resp, "repeat"), Some("1"));
        assert_eq!(pair(&resp, "single"), Some("1"));
        assert_eq!(pair(&resp, "consume"), Some("1"));
    }

    // The armed human-features surface as X- status pairs ONLY when armed: a fresh
    // handler emits none; arming a sleep timer adds X-hypodj-sleep-remaining with a
    // sensible remaining time; cancelling removes it again (status stays lean).
    #[tokio::test(start_paused = true)]
    async fn status_surfaces_armed_sleep_only_when_armed() {
        let Some((h, _events)) = handler_with_null_player() else { return };
        // Nothing armed: no X- pairs.
        let resp = h.handle(MpdCommand::Status).await;
        assert_eq!(pair(&resp, "X-hypodj-sleep-remaining"), None);

        // Arm a 10-minute sleep timer, then read status.
        h.sleep_set(Duration::from_secs(600)).expect("arm sleep");
        let resp = h.handle(MpdCommand::Status).await;
        let remaining: u64 = pair(&resp, "X-hypodj-sleep-remaining")
            .expect("sleep pair present when armed")
            .parse()
            .expect("digits");
        // Within a tick of 600s (no time advanced).
        assert!(remaining > 590 && remaining <= 600, "sensible remaining: {remaining}");

        // Cancel: the pair disappears.
        assert!(h.sleep_cancel());
        let resp = h.handle(MpdCommand::Status).await;
        assert_eq!(pair(&resp, "X-hypodj-sleep-remaining"), None);
    }

    // Re-arming a single-instance convenience feature must NEVER leave two plans of
    // the same origin: the set_singleton swap is atomic, so status carries EXACTLY
    // one X-hypodj-sleep-remaining key (a duplicate would be malformed MPD status).
    #[tokio::test(start_paused = true)]
    async fn rearm_sleep_keeps_exactly_one_plan_and_pair() {
        let Some((h, _events)) = handler_with_null_player() else { return };
        // Count occurrences of a key across the whole status Pairs response.
        fn count(resp: &MpdResponse, key: &str) -> usize {
            match resp {
                MpdResponse::Pairs(p) => p.iter().filter(|(k, _)| k == key).count(),
                _ => 0,
            }
        }

        h.sleep_set(Duration::from_secs(600)).expect("arm sleep");
        // Re-arm several times; each replaces the previous single instance.
        h.sleep_set(Duration::from_secs(1800)).expect("re-arm sleep");
        h.sleep_set(Duration::from_secs(900)).expect("re-arm sleep again");

        // Exactly one armed plan of the sleep origin remains.
        let armed = h
            .plan_deadlines()
            .into_iter()
            .filter(|(_, origin, _)| origin == ORIGIN_SLEEP)
            .count();
        assert_eq!(armed, 1, "one sleep plan after re-arms, got {armed}");

        // And status surfaces the key exactly once (well-formed).
        let resp = h.handle(MpdCommand::Status).await;
        assert_eq!(count(&resp, "X-hypodj-sleep-remaining"), 1, "no duplicate status key");
        let remaining: u64 =
            pair(&resp, "X-hypodj-sleep-remaining").expect("present").parse().expect("digits");
        // Reflects the LAST re-arm (900s), not a stale earlier one.
        assert!(remaining > 890 && remaining <= 900, "reflects last re-arm: {remaining}");
    }

    // Wind-down and wake each surface their own X- pairs when armed: an immediate
    // wind-down reports active-with-no-remaining; a scheduled wake reports both a
    // remaining and an absolute wake-at epoch.
    #[tokio::test(start_paused = true)]
    async fn status_surfaces_armed_winddown_and_wake() {
        let Some((h, _events)) = handler_with_null_player() else { return };
        h.winddown_set(None).expect("arm immediate winddown");
        let at = chrono::Utc::now() + chrono::Duration::hours(2);
        h.wake_set(at, None, 0).expect("arm wake");

        let resp = h.handle(MpdCommand::Status).await;
        assert_eq!(pair(&resp, "X-hypodj-winddown-active"), Some("1"));
        // Immediate wind-down has no deadline -> no remaining pair.
        assert_eq!(pair(&resp, "X-hypodj-winddown-remaining"), None);
        let wake_rem: u64 = pair(&resp, "X-hypodj-wake-remaining")
            .expect("wake remaining present")
            .parse()
            .unwrap();
        assert!(wake_rem > 7100 && wake_rem <= 7200, "~2h remaining: {wake_rem}");
        assert!(pair(&resp, "X-hypodj-wake-at").is_some(), "wake-at epoch present");
    }

    // ── skip-fade (single-mpv dip-through-silence on a USER Next/Previous) ────

    // A user Next while PLAYING dips to silence, loads the target FROM silence in
    // the SkipLoad terminal, then a follow-on ResumeIn ramps back to the baseline -
    // all through the ONE fade slot. The target is reported current IMMEDIATELY
    // (pending_skip), and the OLD track keeps playing audibly through the dip.
    #[tokio::test(start_paused = true)]
    async fn user_next_dips_loads_target_then_resumes() {
        let Some((h, _events)) = handler_with_null_player() else { return };
        h.handle(MpdCommand::Add(NTS.to_string())).await;
        h.handle(MpdCommand::Add(NTS.to_string())).await;
        h.handle(MpdCommand::Play(Some(0))).await;
        assert_eq!(h.player.state(), PlayState::Playing);

        h.handle(MpdCommand::Next).await;
        // A dip is installed (not an abrupt cut) and mpv still plays the OLD track.
        assert!(h.fade_active().await, "next installs a dip fade");
        assert_eq!(h.player.state(), PlayState::Playing, "old track not cut");
        // status/currentsong report the TARGET (idx 1) immediately via pending_skip,
        // WITHOUT current having moved yet.
        assert_eq!(pair(&h.handle(MpdCommand::Status).await, "song"), Some("1"));
        assert_eq!(h.state.lock().unwrap().current, Some(0), "current not moved yet");
        assert_eq!(h.state.lock().unwrap().pending_skip, Some(1));

        // Drive the dip to its SkipLoad terminal: target committed, pending cleared,
        // a follow-on ResumeIn fade is active, mpv Playing the new track.
        h.wait_for_fade().await;
        assert_eq!(h.state.lock().unwrap().current, Some(1), "target committed");
        assert_eq!(h.state.lock().unwrap().pending_skip, None);
        assert!(h.fade_active().await, "a follow-on ResumeIn fade is active");
        assert_eq!(h.player.state(), PlayState::Playing);

        // Drive the follow-on ResumeIn to completion: back at the baseline, audible.
        h.wait_for_fade().await;
        assert_eq!(h.state.lock().unwrap().reported_volume(), 100);
        assert!(h.live_gain_db() > h.fade_cfg.synth_floor_db + 5.0, "ramped back up");
        assert_eq!(h.state.lock().unwrap().current, Some(1));
    }

    // The warm-skip path is PURE GAIN: a user Next drives dip -> switch_warmed ->
    // ResumeIn with NO pause/unpause anywhere. The deck stays Playing across the whole
    // skip and no Paused state edge is ever emitted - the fix warms the target stream,
    // it never introduces a transport pause (HARD CONSTRAINT 1).
    #[tokio::test(start_paused = true)]
    async fn warm_skip_is_pure_gain_never_pauses() {
        let Some((h, mut events)) = handler_with_null_player() else { return };
        h.handle(MpdCommand::Add(NTS.to_string())).await;
        h.handle(MpdCommand::Add(NTS.to_string())).await;
        h.handle(MpdCommand::Play(Some(0))).await;

        h.handle(MpdCommand::Next).await;
        h.wait_for_fade().await; // dip -> switch_warmed lands the target
        h.wait_for_fade().await; // follow-on ResumeIn back to baseline

        assert_eq!(h.state.lock().unwrap().current, Some(1), "target committed");
        assert_eq!(h.player.state(), PlayState::Playing, "deck never left Playing");

        // Not a single Paused edge crossed the wire during the skip.
        while let Ok(ev) = events.try_recv() {
            if let PlayerEvent::StateChanged(PlayState::Paused, _, _) = ev {
                panic!("a skip must never pause the deck (pure gain violated)");
            }
        }
    }

    // NEAR-EOF GUARD (finding 1a): the warm is DECLINED whenever appending the target
    // behind the current track could let mpv auto-advance into it - i.e. when the
    // current is within NEAR_EOF_GUARD_SECS of its natural end, has an unknown
    // duration, is a live/continuous stream, or there is no current. Only a finite
    // Song with a comfortable margin left warms. Direct unit check of the predicate.
    #[tokio::test(start_paused = true)]
    async fn near_eof_guard_declines_the_warm() {
        let Some((h, _events)) = handler_with_null_player() else { return };
        h.enqueue_song_for_test(playlist_test_song("s-0")).await; // idx 0: 200s
        let mut unknown = playlist_test_song("s-1");
        unknown.duration_secs = None;
        h.enqueue_song_for_test(unknown).await; // idx 1: unknown duration
        h.enqueue_stream_for_test(NTS).await; // idx 2: live stream

        // Mid-track finite song with lots left -> warm OK.
        h.state.lock().unwrap().current = Some(0);
        h.reset_elapsed();
        h.note_elapsed_ms(10_000); // 10s into a 200s track
        assert!(h.current_can_warm(), "mid-track finite song warms");

        // Within the guard window of the natural end -> decline.
        h.note_elapsed_ms(199_000); // 1s remaining of 200s
        assert!(!h.current_can_warm(), "near-EOF declines the warm");

        // Unknown duration -> decline (cannot bound the EOF distance).
        h.state.lock().unwrap().current = Some(1);
        h.note_elapsed_ms(1_000);
        assert!(!h.current_can_warm(), "unknown-duration song declines the warm");

        // Live/continuous stream -> decline (no natural end, nothing to prefetch).
        h.state.lock().unwrap().current = Some(2);
        assert!(!h.current_can_warm(), "live stream declines the warm");

        // No current -> decline (conservative).
        h.state.lock().unwrap().current = None;
        assert!(!h.current_can_warm(), "no current declines the warm");
    }

    // SUPERSEDE CLEARS THE WARM (finding 3): a mid-track skip warms the target, then a
    // PAUSE issued DURING the dip supersedes it - the SkipLoad never runs, so the
    // parked warm MUST be dropped or the still-playing (now paused) old track's natural
    // EOF would later auto-advance into the stale target. Observed via the WarmProbe.
    #[tokio::test(start_paused = true)]
    async fn pause_during_skip_dip_drops_the_warm() {
        use std::sync::atomic::Ordering::Relaxed;
        let Some((h, _events, probe)) = handler_with_probe_player() else { return };
        h.enqueue_song_for_test(playlist_test_song("s-0")).await;
        h.enqueue_song_for_test(playlist_test_song("s-1")).await;
        h.handle(MpdCommand::Play(Some(0))).await;
        assert_eq!(h.player.state(), PlayState::Playing);

        // Next installs a dip and WARMS the target (finite current, fresh elapsed).
        h.handle(MpdCommand::Next).await;
        assert!(h.fade_active().await, "next installs a dip fade");
        assert_eq!(probe.prefetch.load(Relaxed), 1, "a mid-track skip warms the target");
        let drops_before = probe.drop.load(Relaxed);

        // Pause DURING the dip supersedes the skip; the parked warm must be dropped.
        h.handle(MpdCommand::Pause(Some(true))).await;
        assert!(
            probe.drop.load(Relaxed) > drops_before,
            "pause-supersede drops the parked warm target"
        );
    }

    // SUPERSEDE CLEARS THE WARM, NON-COMMITTING branch (findings 1/1b): a mid-track
    // skip warms the target, then a WIND-DOWN `fade to <v>` issued DURING the dip
    // supersedes it. This fade is NON-committing (commit_logical=None) - the exact class
    // the old code left the warm parked for - so it must ALSO clear pending_skip and drop
    // the parked warm, or the still-playing old track's natural EOF would auto-advance
    // into the stale target (audible bleed) and the `warmed` guard would then swallow the
    // EOF and stall the queue. Observed via the WarmProbe.
    #[tokio::test(start_paused = true)]
    async fn winddown_fade_during_skip_dip_drops_the_warm() {
        use std::sync::atomic::Ordering::Relaxed;
        let Some((h, _events, probe)) = handler_with_probe_player() else { return };
        h.enqueue_song_for_test(playlist_test_song("s-0")).await;
        h.enqueue_song_for_test(playlist_test_song("s-1")).await;
        h.handle(MpdCommand::Play(Some(0))).await;
        assert_eq!(h.player.state(), PlayState::Playing);

        // Next installs a dip and WARMS the target (finite current, fresh elapsed).
        h.handle(MpdCommand::Next).await;
        assert!(h.fade_active().await, "next installs a dip fade");
        assert_eq!(probe.prefetch.load(Relaxed), 1, "a mid-track skip warms the target");
        assert_eq!(h.state.lock().unwrap().pending_skip, Some(1), "skip target reported");
        let drops_before = probe.drop.load(Relaxed);

        // A non-committing `fade to 40` DURING the dip supersedes the skip: pending_skip
        // must revert to `current` and the parked warm must be dropped.
        h.start_fade(fade_args(FadeKind::To(40), 60)).await.unwrap();
        assert_eq!(
            h.state.lock().unwrap().pending_skip,
            None,
            "a non-committing wind-down clears the superseded skip"
        );
        assert!(
            probe.drop.load(Relaxed) > drops_before,
            "wind-down supersede drops the parked warm target"
        );
        assert_eq!(h.state.lock().unwrap().current, Some(0), "old track still current");
    }

    // SUPERSEDE CLEARS THE WARM, wake-ramp branch (finding 1): a scheduled/user wake
    // `fade in` (also non-committing, ResumeIn/SetBaseline terminal) that supersedes a
    // live skip dip must clear pending_skip and drop the parked warm just the same.
    #[tokio::test(start_paused = true)]
    async fn wake_ramp_during_skip_dip_drops_the_warm() {
        use std::sync::atomic::Ordering::Relaxed;
        let Some((h, _events, probe)) = handler_with_probe_player() else { return };
        h.enqueue_song_for_test(playlist_test_song("s-0")).await;
        h.enqueue_song_for_test(playlist_test_song("s-1")).await;
        h.handle(MpdCommand::Play(Some(0))).await;

        h.handle(MpdCommand::Next).await;
        assert_eq!(probe.prefetch.load(Relaxed), 1, "a mid-track skip warms the target");
        let drops_before = probe.drop.load(Relaxed);

        h.start_fade(fade_args(FadeKind::In, 30)).await.unwrap();
        assert_eq!(
            h.state.lock().unwrap().pending_skip,
            None,
            "a non-committing wake clears the superseded skip"
        );
        assert!(
            probe.drop.load(Relaxed) > drops_before,
            "wake-ramp supersede drops the parked warm target"
        );
    }

    // A rapid SECOND skip during the dip SUPERSEDES the first: the first SkipLoad
    // terminal is aborted BEFORE it loads, so ONLY the second target is ever loaded.
    #[tokio::test(start_paused = true)]
    async fn double_skip_loads_only_second_target() {
        let Some((h, mut events)) = handler_with_null_player() else { return };
        h.handle(MpdCommand::Add(NTS.to_string())).await;
        h.handle(MpdCommand::Add(NTS.to_string())).await;
        h.handle(MpdCommand::Add(NTS.to_string())).await;
        h.handle(MpdCommand::Play(Some(0))).await;

        h.handle(MpdCommand::Next).await; // dip to idx1
        pump(20, 1).await; // dip in flight
        h.handle(MpdCommand::Next).await; // dip to idx2, supersedes the first

        // Drive the (second) dip then its follow-on to completion.
        h.wait_for_fade().await;
        h.wait_for_fade().await;

        assert_eq!(h.state.lock().unwrap().current, Some(2), "only the 2nd target");
        assert_eq!(h.state.lock().unwrap().reported_volume(), 100);
        assert_eq!(h.player.state(), PlayState::Playing);

        // Drain the player events: track 1's queue_id (QueueId(1)) was NEVER loaded
        // (its SkipLoad was aborted before the load), only 0 (initial) and 2.
        let mut loaded: std::collections::HashSet<u64> = std::collections::HashSet::new();
        while let Ok(ev) = events.try_recv() {
            if let PlayerEvent::StateChanged(PlayState::Playing, _, Some(qid)) = ev {
                loaded.insert(qid.0);
            }
        }
        assert!(!loaded.contains(&1), "the 1st skip target must never load: {loaded:?}");
        assert!(loaded.contains(&2), "the 2nd skip target loads: {loaded:?}");
    }

    // A skip while PAUSED is a plain play (no dip): the deck is not playing, so
    // there is nothing to dip through - it advances and plays at the baseline.
    #[tokio::test(start_paused = true)]
    async fn skip_while_paused_is_plain_play() {
        let Some((h, _events)) = handler_with_null_player() else { return };
        h.handle(MpdCommand::Add(NTS.to_string())).await;
        h.handle(MpdCommand::Add(NTS.to_string())).await;
        h.handle(MpdCommand::Play(Some(0))).await;
        h.handle(MpdCommand::Pause(Some(true))).await;
        h.wait_for_fade().await;
        assert_eq!(h.reported_play_state(), PlayState::Paused);

        h.handle(MpdCommand::Next).await;
        // Plain play_index path: current advanced, audible at baseline immediately,
        // no dip-to-silence and no follow-on skip fade.
        assert_eq!(h.state.lock().unwrap().current, Some(1));
        assert_eq!(h.player.state(), PlayState::Playing);
        assert_eq!(h.state.lock().unwrap().reported_volume(), 100);
        assert!(!h.fade_active().await, "no dip on a paused skip");
        assert_eq!(h.state.lock().unwrap().pending_skip, None);
    }

    // An autonomous EOF advance stays GAPLESS: it must NOT skip-fade (no dip, the
    // envelope/volume is untouched across the track boundary).
    #[tokio::test(start_paused = true)]
    async fn eof_advance_stays_gapless() {
        let Some((h, _events)) = handler_with_null_player() else { return };
        h.handle(MpdCommand::Add(NTS.to_string())).await;
        h.handle(MpdCommand::Add(NTS.to_string())).await;
        h.handle(MpdCommand::Play(Some(0))).await;

        h.advance_on_eof().await;
        assert_eq!(h.state.lock().unwrap().current, Some(1));
        assert!(!h.fade_active().await, "eof advance never dips");
        assert_eq!(h.state.lock().unwrap().reported_volume(), 100);
    }

    // A setvol DURING the dip cleanly SUPERSEDES it as a glide: the skip target is
    // NEVER loaded (the dip's SkipLoad terminal never runs), pending_skip is
    // cleared SYNCHRONOUSLY at the glide install, the OLD track keeps playing, and
    // the glide settles at its landing.
    #[tokio::test(start_paused = true)]
    async fn setvol_during_skip_dip_cancels_cleanly() {
        let Some((h, _events)) = handler_with_null_player() else { return };
        h.state.lock().unwrap().vol_dither_state = 0xABCD_1234_5678_90AB;
        h.handle(MpdCommand::Add(NTS.to_string())).await;
        h.handle(MpdCommand::Add(NTS.to_string())).await;
        h.handle(MpdCommand::Play(Some(0))).await;

        h.handle(MpdCommand::Next).await; // dip to idx1
        pump(20, 1).await;
        h.handle(MpdCommand::SetVol(30)).await;

        // The glide's install already reconciled the deck: skip target abandoned.
        assert_eq!(h.state.lock().unwrap().current, Some(0), "target NOT loaded");
        assert_eq!(h.state.lock().unwrap().pending_skip, None);
        assert_eq!(h.player.state(), PlayState::Playing, "old track still playing");
        let committed = h.state.lock().unwrap().target_volume;
        assert!((29..=31).contains(&committed), "landing in [29,31]: {committed}");
        h.wait_for_fade().await;
        assert!(!h.state.lock().unwrap().fading, "the glide settled");
        assert_eq!(h.state.lock().unwrap().reported_volume(), committed);
    }

    // A stop DURING the dip cleanly cancels it: the target is NEVER loaded, the
    // deck stops, pending_skip is cleared, and the baseline is restored.
    #[tokio::test(start_paused = true)]
    async fn stop_during_skip_dip() {
        let Some((h, _events)) = handler_with_null_player() else { return };
        h.handle(MpdCommand::Add(NTS.to_string())).await;
        h.handle(MpdCommand::Add(NTS.to_string())).await;
        h.handle(MpdCommand::Play(Some(0))).await;

        h.handle(MpdCommand::Next).await; // dip to idx1
        pump(20, 1).await;
        h.handle(MpdCommand::Stop).await;

        assert_eq!(h.player.state(), PlayState::Stopped);
        assert_eq!(h.state.lock().unwrap().current, Some(0), "target NOT loaded");
        assert_eq!(h.state.lock().unwrap().pending_skip, None);
        assert_eq!(h.state.lock().unwrap().reported_volume(), 100, "baseline restored");
    }

    // A natural EOF that lands INSIDE the skip-dip window must NOT auto-advance: the
    // skip owns the next load. Otherwise advance_on_eof would load an unrelated
    // track (current+1) that collides with the pending Terminal::SkipLoad, which
    // still fires and loads the skip target a second time (spurious load + double
    // load glitch). The skip terminal is the sole authority for the next load.
    #[tokio::test(start_paused = true)]
    async fn eof_during_skip_dip_does_not_double_advance() {
        let Some((h, _events)) = handler_with_null_player() else { return };
        h.handle(MpdCommand::Add(NTS.to_string())).await;
        h.handle(MpdCommand::Add(NTS.to_string())).await;
        h.handle(MpdCommand::Add(NTS.to_string())).await;
        h.handle(MpdCommand::Play(Some(0))).await;

        h.handle(MpdCommand::Next).await; // dip: current 0, pending_skip Some(1)
        pump(20, 1).await; // dip in flight
        assert_eq!(h.state.lock().unwrap().pending_skip, Some(1));

        // The OLD track (idx0) reaches its natural EOF mid-dip: must be a no-op, NOT
        // an advance to idx2 (current+1). current + pending_skip stay put.
        h.advance_on_eof().await;
        assert_eq!(h.state.lock().unwrap().current, Some(0), "eof did not advance mid-skip");
        assert_eq!(h.state.lock().unwrap().pending_skip, Some(1), "skip intent intact");

        // The dip's SkipLoad terminal is still the sole authority for the load: it
        // commits the target (idx1), never idx2.
        h.wait_for_fade().await;
        assert_eq!(h.state.lock().unwrap().current, Some(1), "skip target loaded, not eof's idx2");
        assert_eq!(h.state.lock().unwrap().pending_skip, None);
    }

    // A pause DURING the dip supersedes it (SkipLoad never runs, target never
    // loads), so pending_skip must be cleared: the reported current reverts to the
    // OLD track mpv is actually paused on, never stuck on the never-loaded target.
    #[tokio::test(start_paused = true)]
    async fn pause_during_skip_dip_clears_pending_skip() {
        let Some((h, _events)) = handler_with_null_player() else { return };
        h.handle(MpdCommand::Add(NTS.to_string())).await;
        h.handle(MpdCommand::Add(NTS.to_string())).await;
        h.handle(MpdCommand::Play(Some(0))).await;

        h.handle(MpdCommand::Next).await; // dip: current 0, pending_skip Some(1)
        pump(20, 1).await;
        assert_eq!(h.state.lock().unwrap().pending_skip, Some(1));

        h.handle(MpdCommand::Pause(Some(true))).await;
        // The pause aborted the dip before it loaded the target: reported current
        // reverts to the still-loaded idx0, not the never-loaded idx1.
        assert_eq!(h.state.lock().unwrap().pending_skip, None, "skip intent cleared on pause");
        assert_eq!(h.state.lock().unwrap().current, Some(0), "target NOT loaded");
        assert_eq!(pair(&h.handle(MpdCommand::Status).await, "song"), Some("0"));
        h.wait_for_fade().await;
        assert_eq!(h.reported_play_state(), PlayState::Paused);
        // The desync does not survive a resume: still on idx0.
        assert_eq!(h.state.lock().unwrap().current, Some(0));
        assert_eq!(h.state.lock().unwrap().pending_skip, None);
    }

    // A MOOD ask primed as `field set <phrase>` registers a lingering pull with
    // provenance and NEVER mutates the queue (bias-only). A GENRE/explicit ask carries
    // no lexicon direction word, so it sets NO pull (byte-identical to today). This is
    // exactly the daemon side of the client mood-priming seam.
    #[tokio::test(start_paused = true)]
    async fn field_set_from_ask_sets_mood_pull_but_not_genre_and_never_mutates_queue() {
        let Some((h, _events)) = handler_with_null_player() else { return };

        // Empty field to start.
        let resp = h.handle(MpdCommand::Field(FieldCmd::Status)).await;
        assert_eq!(pair(&resp, "field"), Some("no pulls active"));

        // A MOOD ask (as the client primes it): a direction word is felt -> a pull is
        // set, labeled by the matched token, with "from the ask" provenance.
        let resp = h
            .handle(MpdCommand::Field(FieldCmd::Set("play something calmer".into())))
            .await;
        assert_eq!(pair(&resp, "pull_set"), Some("calmer"), "mood ask sets a pull");
        let resp = h.handle(MpdCommand::Field(FieldCmd::Status)).await;
        let line = pair(&resp, "pull").expect("a live pull");
        assert!(line.contains("toward calmer"), "provenance label: {line}");
        assert!(line.contains("from the ask"), "origin marker: {line}");
        // Bias-only: setting a pull never touched the queue.
        assert_eq!(h.state.lock().unwrap().queue.len(), 0, "pull never enqueues");

        // A GENRE ask carries no lexicon direction word -> NO pull is added; the honest
        // "no pull felt" echo, and the field is unchanged beyond the prior mood pull.
        let resp = h
            .handle(MpdCommand::Field(FieldCmd::Set("play some jazz".into())))
            .await;
        assert!(
            pair(&resp, "field").unwrap_or_default().contains("no pull felt"),
            "genre ask sets no pull: {resp:?}"
        );
        // Still exactly one pull (the calmer one), no spurious jazz pull, queue empty.
        let resp = h.handle(MpdCommand::Field(FieldCmd::Status)).await;
        let pulls = match &resp {
            MpdResponse::Pairs(p) => p.iter().filter(|(k, _)| k == "pull").count(),
            _ => 0,
        };
        assert_eq!(pulls, 1, "no spurious pull from a genre ask");
        assert_eq!(h.state.lock().unwrap().queue.len(), 0, "no ask ever mutated the queue");

        // Nudge "less" attenuates the pull (still non-destructive, queue untouched).
        let resp = h.handle(MpdCommand::Field(FieldCmd::Nudge(FieldNudge::Less))).await;
        assert_eq!(pair(&resp, "pull_nudged"), Some("calmer"));
        assert_eq!(h.state.lock().unwrap().queue.len(), 0);
    }

    // The passive field HUD state surfaces as X- status pairs ONLY when a pull is
    // active - a fresh handler emits none; setting a mood pull adds the count/label/
    // strength/age pairs; time decays the surfaced strength; a cleared field removes
    // the pairs again (status stays lean, the HUD auto-clears at rest).
    #[tokio::test(start_paused = true)]
    async fn status_surfaces_field_pull_only_when_active_and_reflects_decay() {
        let Some((h, _events)) = handler_with_null_player() else { return };
        // Nothing active: no field pairs.
        let resp = h.handle(MpdCommand::Status).await;
        assert_eq!(pair(&resp, "X-hypodj-field-count"), None);

        // Set a calmer mood pull -> the field pairs appear.
        h.handle(MpdCommand::Field(FieldCmd::Set("play something calmer".into()))).await;
        let resp = h.handle(MpdCommand::Status).await;
        assert_eq!(pair(&resp, "X-hypodj-field-count"), Some("1"));
        assert_eq!(pair(&resp, "X-hypodj-field-0-label"), Some("calmer"));
        assert_eq!(pair(&resp, "X-hypodj-field-0-age"), Some("0"));
        let hot: u8 = pair(&resp, "X-hypodj-field-0-strength")
            .expect("strength pair present")
            .parse()
            .expect("digits");
        // A fresh lexicon pull is strength 0.6 -> 60 on the basis-of-100 wire scale.
        assert_eq!(hot, 60, "fresh pull surfaces at 60");

        // Advance one half-life: the surfaced strength halves and age climbs, so the
        // HUD ticks down each poll.
        tokio::time::advance(crate::intelligence::PULL_HALF_LIFE).await;
        let resp = h.handle(MpdCommand::Status).await;
        let cold: u8 = pair(&resp, "X-hypodj-field-0-strength").expect("present").parse().unwrap();
        assert!(cold < hot, "decay lowers surfaced strength: {cold} < {hot}");
        let age: u64 = pair(&resp, "X-hypodj-field-0-age").expect("present").parse().unwrap();
        assert!(age >= 10, "age climbs to ~10 min: {age}");

        // Clear the field: the pairs disappear (HUD auto-clears).
        h.handle(MpdCommand::Field(FieldCmd::Clear)).await;
        let resp = h.handle(MpdCommand::Status).await;
        assert_eq!(pair(&resp, "X-hypodj-field-count"), None, "cleared field emits no pairs");
    }

    /// LIVE proof against a REAL backend on a throwaway queue (never touches live
    /// 6600, no secrets): a MOOD ask sets a lingering pull that BIASES a Radio
    /// enqueue calmer-first; a GENRE ask sets NO pull; the pull never mutates the
    /// queue (bias-only). Run with `--ignored` and env pointing at a throwaway
    /// backend.
    ///
    /// Env: `HYPODJ_LIVE_URL`, `HYPODJ_LIVE_USER`, `HYPODJ_LIVE_PASS`.
    // The execute-outcome fix: PlanOutcome::render speaks the REAL count/effect. The
    // silent no-op bug was a plan-time "add 5" shown when 0 resolved; render never
    // does that - 0 says "added 0 - no matches for X", not the asked count.
    #[test]
    fn plan_outcome_render_speaks_real_count() {
        // Zero resolved -> the honest no-match line (NOT "added 5").
        assert_eq!(
            PlanOutcome::Added { n: 0, selector: "\"active but not head bumping\"".into() }.render(),
            "added 0 - no matches for \"active but not head bumping\""
        );
        // N>0 -> the real count, pluralized.
        assert_eq!(
            PlanOutcome::Added { n: 3, selector: "\"jazz\"".into() }.render(),
            "added 3 tracks"
        );
        assert_eq!(
            PlanOutcome::Added { n: 1, selector: "\"jazz\"".into() }.render(),
            "added 1 track"
        );
        // playnow with a resolved track -> "played <title>".
        assert_eq!(
            PlanOutcome::Played {
                n: 1,
                title: Some("\"So What\"".into()),
                selector: "\"so what\"".into(),
            }
            .render(),
            "played \"So What\""
        );
        // playnow that resolved to nothing -> the same honest no-match line.
        assert_eq!(
            PlanOutcome::Played { n: 0, title: None, selector: "\"nope\"".into() }.render(),
            "added 0 - no matches for \"nope\""
        );
        // Queue edits report their real affected count.
        assert_eq!(PlanOutcome::Removed(2).render(), "removed 2 tracks");
        assert_eq!(PlanOutcome::Jumped(1).render(), "jumped to the matching track");
        assert_eq!(PlanOutcome::Jumped(0).render(), "no matching track to play");
    }

    // run_action_outcome for the DETERMINISTIC queue-edit actions (no network): a
    // matching Play jumps (Jumped(1)); a no-match Play is a clean no-op (Jumped(0),
    // "no matching track"); a Remove reports the real removed count; Noop is inert.
    #[tokio::test]
    async fn run_action_outcome_queue_edits_report_real_counts() {
        let Some((h, _events)) = handler_with_null_player() else { return };
        h.enqueue_song_for_test(playlist_test_song("s-1")).await;
        h.enqueue_song_for_test(playlist_test_song("s-2")).await;
        h.enqueue_song_for_test(playlist_test_song("s-3")).await;
        h.play_for_test(0).await;

        // A Play matching "Song s-2" jumps to it.
        let out = h
            .run_action_outcome(&Action::Play {
                sel: crate::plan::QueueSelector::QueryMatch("s-2".into()),
            })
            .await;
        assert_eq!(out, PlanOutcome::Jumped(1));
        assert_eq!(out.render(), "jumped to the matching track");

        // A Play matching nothing is a clean no-op - never a wrong-target jump.
        let out = h
            .run_action_outcome(&Action::Play {
                sel: crate::plan::QueueSelector::QueryMatch("no-such-track".into()),
            })
            .await;
        assert_eq!(out, PlanOutcome::Jumped(0));
        assert_eq!(out.render(), "no matching track to play");

        // A Remove of the last entry reports the real removed count.
        let out = h
            .run_action_outcome(&Action::Remove { sel: crate::plan::QueueSelector::Last(1) })
            .await;
        assert_eq!(out, PlanOutcome::Removed(1));

        // Noop is inert.
        assert_eq!(
            h.run_action_outcome(&Action::Noop).await,
            PlanOutcome::Effect("nothing to do".into())
        );
    }

    // LIVE execute-outcome proof (real server): a mood ask that resolves to 0 real
    // songs reports "added 0 - no matches for X" (NOT the asked count), a real ask
    // reports "added N" with N = the true resolved count, and a playnow reports
    // "played <title>". Run with HYPODJ_LIVE_URL/USER/PASS + HYPODJ_LIVE_GENRE set.
    #[tokio::test]
    #[ignore]
    async fn live_plan_add_reports_real_execute_outcome() {
        let (url, user, pass) = match (
            std::env::var("HYPODJ_LIVE_URL"),
            std::env::var("HYPODJ_LIVE_USER"),
            std::env::var("HYPODJ_LIVE_PASS"),
        ) {
            (Ok(u), Ok(us), Ok(pw)) => (u, us, pw),
            _ => {
                eprintln!("skipping: set HYPODJ_LIVE_URL/USER/PASS to run");
                return;
            }
        };
        let cfg = ServerConfig { url, username: user, password: pass, client_name: "hypodj-live-test".into() };
        let client = Arc::new(SubsonicClient::connect(&cfg).expect("connect"));
        let (player, _events) = NullPlayer::spawn();
        let h = HypodjHandler::new(client, player);

        // (1) A mood query that literal search will not match -> added 0, honest.
        let nonsense = "zzq no such mood xxq";
        let (_id, outcome) = h
            .plan_add_reporting(RawPlan {
                version: 1,
                trigger: RawTrigger::Immediate,
                action: Action::Enqueue { selector: Selector::Query(nonsense.into()), count: 5 },
                once: false,
                origin: "mpd".into(),
            })
            .await
            .expect("arm");
        let line = outcome.expect("immediate outcome").render();
        assert!(line.starts_with("added 0 - no matches for"), "expected 0-match line, got: {line}");

        // (2) A radio ask resolves to real songs -> added N (N>0), the true count.
        let before = h.state.lock().unwrap().queue.len();
        let (_id, outcome) = h
            .plan_add_reporting(RawPlan {
                version: 1,
                trigger: RawTrigger::Immediate,
                action: Action::Enqueue { selector: Selector::Radio, count: 3 },
                once: false,
                origin: "mpd".into(),
            })
            .await
            .expect("arm");
        let after = h.state.lock().unwrap().queue.len();
        let delta = after - before;
        assert_eq!(outcome.expect("outcome").render(), format!("added {delta} {}", tracks_word(delta)));
        assert!(delta > 0, "radio should resolve to real songs");

        // (3) A playnow resolves to a real track -> "played <title>".
        let (_id, outcome) = h
            .plan_add_reporting(RawPlan {
                version: 1,
                trigger: RawTrigger::Immediate,
                action: Action::PlayNow { selector: Selector::Radio, count: 1 },
                once: false,
                origin: "mpd".into(),
            })
            .await
            .expect("arm");
        let line = outcome.expect("outcome").render();
        assert!(line.starts_with("played "), "expected played line, got: {line}");
    }

    /// LIVE proof of the Query keyword-split + OR-merge fix against a REAL backend
    /// (throwaway queue, never touches live 6600, no secrets, NullPlayer = silent).
    ///
    /// Picks a REAL genre token from the library (`list genre` via the client), wraps
    /// it in a natural multi-word phrase that would fail whole-string full-text, and
    /// proves: (a) the OLD whole-phrase `search3` returns 0 songs (so the split is
    /// what fixes it), (b) the NEW keyword-split enqueue resolves N>0 REAL tracks,
    /// (c) a pure-nonsense mood resolves to an HONEST 0 (no fabrication).
    ///
    /// Env: `HYPODJ_LIVE_URL`, `HYPODJ_LIVE_USER`, `HYPODJ_LIVE_PASS`. Run with
    /// `cargo test -p hypodj-core -- --ignored live_query_keyword_split_resolves`.
    #[tokio::test]
    #[ignore = "requires a live backend (HYPODJ_LIVE_URL/USER/PASS)"]
    async fn live_query_keyword_split_resolves_multiword_mood() {
        let (url, user, pass) = match (
            std::env::var("HYPODJ_LIVE_URL"),
            std::env::var("HYPODJ_LIVE_USER"),
            std::env::var("HYPODJ_LIVE_PASS"),
        ) {
            (Ok(u), Ok(us), Ok(pw)) => (u, us, pw),
            _ => {
                eprintln!("skipping: set HYPODJ_LIVE_URL/USER/PASS to run");
                return;
            }
        };
        let cfg = ServerConfig { url, username: user, password: pass, client_name: "hypodj-live-test".into() };
        let client = Arc::new(SubsonicClient::connect(&cfg).expect("connect"));

        // A REAL single-word genre token from the library (never hardcoded), so the
        // per-keyword search3 is guaranteed to have something to match.
        let genres = client.genres().await.expect("list genres");
        let token = genres
            .iter()
            .map(|g| g.name.clone())
            .find(|n| !n.trim().is_empty() && !n.contains(' ') && n.chars().all(|c| c.is_alphanumeric()))
            .expect("a single-word alphanumeric genre token in the library");

        let (player, _events) = NullPlayer::spawn();
        let h = HypodjHandler::new(Arc::clone(&client), player);

        // A natural multi-word phrase embedding the real token. Whole-string search3
        // of this phrase almost never matches a title/artist/album, but the split
        // recovers the token's tracks.
        let phrase = format!("queue some {token} stuff for now");

        // (a) OLD behaviour: the whole-phrase search3 finds 0 songs (the bug).
        let old = client.search3(&phrase).await.expect("search3 whole phrase");
        assert_eq!(
            old.songs.len(),
            0,
            "whole-string search3 of the multi-word phrase should find 0 (that is the bug we fixed)"
        );

        // (b) NEW behaviour: the keyword-split enqueue resolves N>0 REAL tracks.
        let before = h.state.lock().unwrap().queue.len();
        let n = h.plan_enqueue(&Selector::Query(phrase.clone()), 5).await.expect("enqueue");
        let after = h.state.lock().unwrap().queue.len();
        assert!(n > 0, "keyword-split should resolve the token '{token}' to real tracks, got 0");
        assert_eq!(after - before, n, "the reported count equals the real queue delta");

        // (c) Pure nonsense -> honest 0 (no fabrication, queue unchanged).
        let q2 = h.state.lock().unwrap().queue.len();
        let n0 = h
            .plan_enqueue(&Selector::Query("zzq flibber wompus".into()), 5)
            .await
            .expect("enqueue nonsense");
        let q3 = h.state.lock().unwrap().queue.len();
        assert_eq!(n0, 0, "pure nonsense must resolve to an honest 0 (no fabrication)");
        assert_eq!(q2, q3, "a 0-match ask leaves the queue unchanged");
    }

    /// LIVE: "more like what is playing" resolves against a REAL backend. Seeds a
    /// real track as the current song, then proves plan_enqueue(SimilarToCurrent)
    /// (the id-free selector the model emits) fills the seed server-side and appends
    /// N>0 REAL tracks; and that an EMPTY deck yields an HONEST 0 (no fabrication,
    /// queue unchanged). Env: HYPODJ_LIVE_URL/USER/PASS. Run with
    /// `cargo test -p hypodj-core -- --ignored live_similar_to_current`.
    #[tokio::test]
    #[ignore = "requires a live backend (HYPODJ_LIVE_URL/USER/PASS)"]
    async fn live_similar_to_current_resolves_from_the_playing_seed() {
        let (url, user, pass) = match (
            std::env::var("HYPODJ_LIVE_URL"),
            std::env::var("HYPODJ_LIVE_USER"),
            std::env::var("HYPODJ_LIVE_PASS"),
        ) {
            (Ok(u), Ok(us), Ok(pw)) => (u, us, pw),
            _ => {
                eprintln!("skipping: set HYPODJ_LIVE_URL/USER/PASS to run");
                return;
            }
        };
        let cfg = ServerConfig { url, username: user, password: pass, client_name: "hypodj-live-test".into() };
        let client = Arc::new(SubsonicClient::connect(&cfg).expect("connect"));
        let (player, _events) = NullPlayer::spawn();
        let h = HypodjHandler::new(Arc::clone(&client), player);

        // (a) EMPTY deck: nothing to seed from -> honest 0, queue unchanged.
        let n0 = h.plan_enqueue(&Selector::SimilarToCurrent, 5).await.expect("honest 0");
        assert_eq!(n0, 0, "no current/queued song -> honest 0");
        assert_eq!(h.state.lock().unwrap().queue.len(), 0, "no-seed ask leaves queue empty");

        // Seed a REAL track and START it, so there is a current song to be like.
        let seed = client.random_songs(Some(1)).await.expect("random seed");
        let seed = seed.into_iter().next().expect("a real track in the library");
        h.enqueue_song_for_test(seed.clone()).await;
        h.handle(MpdCommand::Play(Some(0))).await;
        assert_eq!(h.similar_seed_id(), Some(seed.id.clone()), "the current track is the seed");

        // (b) "queue more like this" -> resolves via similar(current id) and appends
        // N>0 REAL tracks (append-only delta); the seed itself is never re-appended.
        let before = h.state.lock().unwrap().queue.len();
        let n = h.plan_enqueue(&Selector::SimilarToCurrent, 5).await.expect("similar enqueue");
        let after = h.state.lock().unwrap().queue.len();
        assert!(n > 0, "similar_to_current must resolve the playing seed to real tracks");
        assert_eq!(after - before, n, "reported count equals the real queue delta");
    }

    /// LIVE (task 0ba1lej): "more like this one" after a track FINISHED seeds from
    /// the RECENTLY-FINISHED track against a REAL backend. Plays a real track, drives
    /// it to a natural EOF (nothing playing after), then proves the SimilarToCurrent
    /// selector resolves via the recency seed (last_finished) - N>0 real tracks; and
    /// that a never-played deck yields an HONEST 0. Env: HYPODJ_LIVE_URL/USER/PASS.
    /// Run with `cargo test -p hypodj-core -- --ignored live_similar_recency`.
    #[tokio::test]
    #[ignore = "requires a live backend (HYPODJ_LIVE_URL/USER/PASS)"]
    async fn live_similar_recency_seeds_from_finished_track() {
        let (url, user, pass) = match (
            std::env::var("HYPODJ_LIVE_URL"),
            std::env::var("HYPODJ_LIVE_USER"),
            std::env::var("HYPODJ_LIVE_PASS"),
        ) {
            (Ok(u), Ok(us), Ok(pw)) => (u, us, pw),
            _ => {
                eprintln!("skipping: set HYPODJ_LIVE_URL/USER/PASS to run");
                return;
            }
        };
        let cfg = ServerConfig { url, username: user, password: pass, client_name: "hypodj-live-test".into() };
        let client = Arc::new(SubsonicClient::connect(&cfg).expect("connect"));
        let (player, _events) = NullPlayer::spawn();
        let h = HypodjHandler::new(Arc::clone(&client), player);

        // (2) truly-empty deck (never played anything) -> honest 0.
        let n0 = h.plan_enqueue(&Selector::SimilarToCurrent, 5).await.expect("honest 0");
        assert_eq!(n0, 0, "never-played deck -> honest 0");

        // Play a REAL track, then drive it to a NATURAL EOF so nothing is playing but
        // last_finished holds that track.
        let seed = client.random_songs(Some(1)).await.expect("random seed");
        let seed = seed.into_iter().next().expect("a real track in the library");
        h.enqueue_song_for_test(seed.clone()).await;
        h.handle(MpdCommand::Play(Some(0))).await;
        h.advance_on_eof().await; // natural end-of-queue EOF
        assert_eq!(h.state.lock().unwrap().current, None, "deck stopped after EOF");
        assert_eq!(
            h.similar_seed_id(),
            Some(seed.id.clone()),
            "seed source is the recently-finished track, not first-queued"
        );

        // (1) "queue more like this one" -> resolves via similar(last_finished) and
        // appends N>0 REAL tracks (append-only delta).
        let before = h.state.lock().unwrap().queue.len();
        let n = h.plan_enqueue(&Selector::SimilarToCurrent, 5).await.expect("similar enqueue");
        let after = h.state.lock().unwrap().queue.len();
        assert!(n > 0, "recency seed resolves to real tracks");
        assert_eq!(after - before, n, "reported count equals the real queue delta");
    }

    #[tokio::test]
    #[ignore]
    async fn live_mood_pull_biases_radio_enqueue_and_genre_sets_none() {
        let (url, user, pass) = match (
            std::env::var("HYPODJ_LIVE_URL"),
            std::env::var("HYPODJ_LIVE_USER"),
            std::env::var("HYPODJ_LIVE_PASS"),
        ) {
            (Ok(u), Ok(us), Ok(pw)) => (u, us, pw),
            _ => {
                eprintln!("skipping: set HYPODJ_LIVE_URL/USER/PASS to run");
                return;
            }
        };
        let cfg = ServerConfig {
            url,
            username: user,
            password: pass,
            client_name: "hypodj-live-test".to_string(),
        };
        let client = Arc::new(SubsonicClient::connect(&cfg).expect("connect"));
        let (player, _events) = NullPlayer::spawn();
        let h = HypodjHandler::new(client, player);

        // (1) field shows no pull.
        let resp = h.handle(MpdCommand::Field(FieldCmd::Status)).await;
        assert_eq!(pair(&resp, "field"), Some("no pulls active"), "clean field to start");

        // (2) a MOOD ask (as the client primes it) -> a lingering "calmer" pull WITH
        // provenance.
        let resp = h
            .handle(MpdCommand::Field(FieldCmd::Set("play something calmer".into())))
            .await;
        assert_eq!(pair(&resp, "pull_set"), Some("calmer"), "mood ask registers a pull");
        let resp = h.handle(MpdCommand::Field(FieldCmd::Status)).await;
        let line = pair(&resp, "pull").expect("live pull");
        assert!(line.contains("toward calmer") && line.contains("from the ask"), "{line}");

        // The Radio enqueue is now biased calmer-FIRST: with a live pull active, the
        // resolved random pool is reordered so lower-energy tracks lead the append.
        let n = h.plan_enqueue(&Selector::Radio, 12).await.expect("radio enqueue");
        assert!(n >= 4, "need a few tracks to see the bias, got {n}");
        let energies: Vec<f32> = {
            let st = h.state.lock().unwrap();
            st.queue
                .iter()
                .filter_map(|it| match &it.entry {
                    QueueEntry::Song(s) => Some(crate::intelligence::energy_score(s)),
                    _ => None,
                })
                .collect()
        };
        // Bias-only: the queue is exactly the appended count (never mutated/deleted).
        assert_eq!(energies.len(), n, "queue is a definite array of the appended picks");
        let half = energies.len() / 2;
        let mean = |xs: &[f32]| xs.iter().sum::<f32>() / xs.len().max(1) as f32;
        let front = mean(&energies[..half]);
        let back = mean(&energies[half..]);
        assert!(front <= back + 1e-3, "calmer pull leads with lower-energy picks: {front} <= {back}");

        // (3) a GENRE ask -> NO mood pull is set (no spurious pull). Clear first so we
        // read the genre ask in isolation.
        h.handle(MpdCommand::Field(FieldCmd::Clear)).await;
        let resp = h.handle(MpdCommand::Field(FieldCmd::Set("play some jazz".into()))).await;
        assert!(
            pair(&resp, "field").unwrap_or_default().contains("no pull felt"),
            "genre ask sets no pull: {resp:?}"
        );
        let resp = h.handle(MpdCommand::Field(FieldCmd::Status)).await;
        assert_eq!(pair(&resp, "field"), Some("no pulls active"), "field unchanged by a genre ask");
    }

    // LIVE proof of the enqueue-then-start "play a specific library song NOW" path
    // (PlayNow) and the strictly append-only enqueue, against a REAL backend. Uses a
    // NullPlayer so nothing plays through the speakers (silent by construction);
    // the SubsonicClient is real so search3/random_songs resolve genuine songs and
    // play_index resolves a real stream URL. `#[ignore]` (certless/no-network
    // sandbox skips it): run with
    //   HYPODJ_TEST_URL/USER/PASS set, `cargo test -p hypodj-core -- --ignored
    //   live_play_now_enqueues_and_starts`.
    #[tokio::test]
    #[ignore = "requires a live Navidrome (HYPODJ_TEST_URL/USER/PASS)"]
    async fn live_play_now_enqueues_and_starts() {
        let (url, username, password) = match (
            std::env::var("HYPODJ_TEST_URL"),
            std::env::var("HYPODJ_TEST_USER"),
            std::env::var("HYPODJ_TEST_PASS"),
        ) {
            (Ok(u), Ok(n), Ok(p)) => (u, n, p),
            _ => {
                eprintln!("skipping live play_now: HYPODJ_TEST_URL/USER/PASS not set");
                return;
            }
        };
        let cfg = ServerConfig { url, username, password, client_name: "hypodj-selftest".into() };
        let client = Arc::new(SubsonicClient::connect(&cfg).expect("connect"));
        client.ping().await.expect("ping");
        // A real title to demand NOW (never a hardcoded id).
        let seed = client.random_songs(Some(1)).await.expect("random song");
        let title = seed.first().map(|s| s.title.clone()).expect("at least one song");

        let (player, _events) = NullPlayer::spawn();
        let h = HypodjHandler::new(Arc::clone(&client), player);

        // (0) STRICTLY APPEND-ONLY: enqueue onto a STOPPED, EMPTY deck appends WITHOUT
        // starting playback (the append-only contract the confirm/prompt promise).
        assert!(h.state.lock().unwrap().queue.is_empty());
        assert!(h.state.lock().unwrap().current.is_none(), "deck starts stopped");
        let n0 = h.plan_enqueue(&Selector::Radio, 1).await.expect("append-only enqueue");
        assert!(n0 >= 1, "enqueue appended the resolved song");
        assert!(!h.state.lock().unwrap().queue.is_empty(), "and it was enqueued");
        assert!(
            h.state.lock().unwrap().current.is_none(),
            "append-only enqueue NEVER starts playback, even on an empty stopped deck"
        );

        let (player, _events) = NullPlayer::spawn();
        let h = HypodjHandler::new(client, player);

        // (1) STOPPED empty deck: play_now enqueues the song AND starts on it.
        assert!(h.state.lock().unwrap().queue.is_empty());
        assert!(h.state.lock().unwrap().current.is_none(), "deck starts stopped");
        let n = h
            .plan_play_now(&Selector::Query(title.clone()), 1)
            .await
            .expect("play_now");
        assert!(n >= 1, "play_now enqueued the resolved song");
        assert_eq!(h.state.lock().unwrap().current, Some(0), "playback STARTED on it");
        assert!(!h.state.lock().unwrap().queue.is_empty(), "and it was enqueued");

        // (2) play_now while a track is already playing JUMPS to the new one (never a
        // silent append-and-ignore): the just-enqueued track becomes current.
        let before = h.state.lock().unwrap().queue.len();
        let n = h.plan_play_now(&Selector::Radio, 1).await.expect("play_now radio");
        assert!(n >= 1);
        assert_eq!(
            h.state.lock().unwrap().current,
            Some(before),
            "jumped to the just-enqueued track at the old tail"
        );

        // (3) append-only enqueue onto a NON-empty/live deck does NOT move current.
        let cur = h.state.lock().unwrap().current;
        h.plan_enqueue(&Selector::Radio, 1).await.expect("enqueue");
        assert_eq!(h.state.lock().unwrap().current, cur, "append-only never jumps");
    }
}
