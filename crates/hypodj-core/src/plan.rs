//! P2 pure Plan IR + validator + fire predicate.
//!
//! FOUNDATION (P2), built to LAST. This module is PURE: no clock, no timers, no
//! handler, no mpv, no model inference. Every function here operates on OWNED
//! data ([`RawPlan`], [`QueueSnapshot`], [`DjEvent`], [`PlanBounds`] + an injected
//! `now`/`now_civil`), so the whole trusted path ([`validate`] + [`fires`]) is
//! unit-testable with scripted events and fabricated snapshots - never a real
//! clock or a live model.
//!
//! ## Identities, not indices (the headline invariant)
//!
//! Mirrors [`crate::event`]'s "identities, not indices": the RAW IR (what a DSL
//! or an LLM emits) may hold positions and relative selectors, but an ARMED plan
//! ([`Resolved`]) holds ONLY concrete [`QueueId`]/[`AlbumId`]/absolute [`Instant`]
//! anchors - never a mutable index or a relative selector. "Misfire on the wrong
//! track after a queue mutation" is therefore UNREPRESENTABLE post-arm: it rests
//! on a hard raw-vs-armed TYPE split ([`RawPlan`] -> [`validate`] -> [`Resolved`]).
//!
//! A queue mutation between arm and fire is caught at fire time: [`fires`]
//! re-validates the armed [`QueueId`] against the CURRENT snapshot and returns
//! [`Fire::Stale`] (fail loud, never fire on a neighbor) when the target is gone
//! or has already been passed.

use std::time::Duration;

use serde::{Deserialize, Serialize};
use tokio::time::Instant;

use crate::config::FadeConfig;
use crate::event::{DjEvent, DjEventKind, QueueId, QueueSnapshot, TimerId};
use crate::model::{AlbumId, SongId};

/// Absolute cap on an [`Action::Enqueue`] count, independent of any fade knob.
/// Append-only + count-clamped is the ONLY reason the string [`Selector`]s are an
/// acceptable bounded hole in an otherwise fully-bounded IR.
pub const MAX_ENQUEUE: u32 = 100;

/// Reserved plan origins for the convenience sleep/winddown/wake features. Each
/// names the SINGLE active instance of its feature in the registry, so a
/// `find_by_origin` lookup can replace/cancel it (single-instance control).
pub const ORIGIN_SLEEP: &str = "sleep";
pub const ORIGIN_WINDDOWN: &str = "winddown";
pub const ORIGIN_WAKE: &str = "wake";

fn one() -> u16 {
    1
}

/// A monotonic, never-reused plan identity. Minted by the registry
/// ([`crate::handler`]); a stale cancel/replace can never hit a recycled plan.
#[derive(Clone, Copy, Debug, PartialEq, Eq, Hash, PartialOrd, Ord, Serialize, Deserialize)]
pub struct PlanId(pub u64);

/// The RAW, serde-serializable plan an untrusted producer (DSL / LLM) emits.
///
/// Bounded `#[non_exhaustive]` enums so a hallucinated plan cannot express an
/// unsafe op; versioned so the shape can evolve without a breaking change.
#[derive(Clone, Debug, Serialize, Deserialize)]
pub struct RawPlan {
    #[serde(default = "one")]
    pub version: u16,
    pub trigger: RawTrigger,
    pub action: Action,
    #[serde(default)]
    pub once: bool,
    #[serde(default)]
    pub origin: String,
}

/// The RAW trigger: positions + relative selectors + civil time, resolved to a
/// concrete [`Resolved`] anchor by [`validate`].
#[derive(Clone, Debug, Serialize, Deserialize)]
#[serde(tag = "kind", rename_all = "snake_case")]
#[non_exhaustive]
pub enum RawTrigger {
    /// Execute immediately at add-time (never a stored edge).
    Immediate,
    /// The `n`-th queue position, counted per [`PosBase`].
    QueuePosition { n: usize, base: PosBase },
    /// The single track right after the current one.
    TrackAfterCurrent,
    /// Fire when `secs` remain on `track`.
    TimeRemaining { track: TrackSel, secs: f64 },
    /// Fire in the gap after `track` when the next track is a different album.
    AlbumBoundary { track: TrackSel },
    /// Fire at an absolute civil (wall-clock) instant.
    WallClock { at: chrono::DateTime<chrono::Utc> },
    /// Fire `secs` from now (a monotonic span).
    SpanElapsed { secs: f64 },
}

/// How a [`RawTrigger::QueuePosition`] counts.
#[derive(Clone, Copy, Debug, Serialize, Deserialize)]
#[cfg_attr(feature = "schema", derive(schemars::JsonSchema))]
#[serde(rename_all = "snake_case")]
pub enum PosBase {
    /// `n == 1` is the CURRENT track ("counting current as 1st").
    CurrentIsOne,
    /// `n` is an absolute 1-based queue position.
    Absolute,
}

/// A RAW track selector (raw input only). RENAMED from the spec's `TrackRef` to
/// avoid colliding with [`crate::event::TrackRef`].
#[derive(Clone, Copy, Debug, Serialize, Deserialize)]
#[cfg_attr(feature = "schema", derive(schemars::JsonSchema))]
#[serde(tag = "sel", rename_all = "snake_case")]
pub enum TrackSel {
    /// The current track.
    Current,
    /// `n` tracks relative to the current one (may be negative).
    RelToCurrent(i32),
    /// A concrete stable queue id.
    QueueId(u64),
}

/// A bounded plan action. Mirrors the [`crate::handler::FadeIntent`] seam EXACTLY
/// for fades (no arbitrary curve, no Ramp), so nothing here can express an op the
/// startle-safe primitive would refuse.
#[derive(Clone, Debug, Serialize, Deserialize)]
#[cfg_attr(feature = "schema", derive(schemars::JsonSchema))]
#[serde(tag = "act", rename_all = "snake_case")]
#[non_exhaustive]
pub enum Action {
    Fade(FadeIntentIr),
    Stop,
    Pause,
    SetVolume(u8),
    /// INVARIANT: `Enqueue` is APPEND-ONLY + count-clamped - the only reason the
    /// string [`Selector`]s are an acceptable bounded hole.
    Enqueue { selector: Selector, count: u32 },
    /// A gentle alarm: at the deadline, optionally enqueue `selector` (append-only,
    /// count-clamped), force playback to start from silence, then ramp IN to the
    /// saved comfort level. ONE atomic effect (enqueue -> silence -> play ->
    /// WakeTo), so the ordering cannot be split across independent timers.
    Wake { selector: Option<Selector>, count: u32 },
    /// Remove the entries a [`QueueSelector`] resolves against the LIVE queue. The
    /// selector resolves at EXECUTE time (like [`Action::Enqueue`]'s content
    /// selector), never pre-baked to indices, so a NO-MATCH is a clean no-op -
    /// never a wrong-target delete. Destructive: only reachable through the
    /// echo-before-arm + owner y/N gate.
    Remove { sel: QueueSelector },
    /// Move the selected entries to a destination position (order preserved). A
    /// no-match is a clean no-op. The currently-playing entry is tracked by stable
    /// id across the rebuild, so playback never jumps to a neighbour.
    Move { sel: QueueSelector, dest: MoveDest },
    /// Clear part or all of the queue. Destructive: only through the confirm gate.
    Clear { scope: ClearScope },
    /// Jump playback to the FIRST entry the selector resolves. A no-match is a
    /// clean no-op (playback is left exactly where it was).
    Play { sel: QueueSelector },
    /// Resolve a LIBRARY [`Selector`] (search3/genre/radio, NOT the live-queue
    /// [`QueueSelector`] that [`Action::Play`] uses), APPEND the resolved songs
    /// (append-only + count-clamped, exactly like [`Action::Enqueue`]), then START
    /// playback on the first newly-appended track. This is the honest "play this
    /// specific library song NOW" operation: enqueue-then-start. Non-destructive
    /// (never deletes); the label resolves to ids at EXECUTE time, never from the
    /// model. Distinct from [`Action::Enqueue`], which stays APPEND-ONLY.
    PlayNow { selector: Selector, count: u32 },
    /// No operation: an off-topic / non-music / non-queue / non-playback request
    /// that maps to NO valid action. Emitting this (honest "no action") is how the
    /// model avoids fabricating a wrong enqueue for a request it cannot express.
    Noop,
}

/// Which queue entries a queue-edit action targets. Resolved DETERMINISTICALLY
/// against the LIVE queue at EXECUTE time by [`resolve_selector`]. Positions are
/// 1-based (matching the queue the user sees). A no-match resolves to the empty
/// set - the caller treats that as a clean no-op, never a wrong-target edit.
#[derive(Clone, Debug, Serialize, Deserialize, PartialEq)]
#[cfg_attr(feature = "schema", derive(schemars::JsonSchema))]
#[serde(tag = "qsel", rename_all = "snake_case")]
#[non_exhaustive]
pub enum QueueSelector {
    /// The 1-based queue position `n`.
    Position(usize),
    /// A 1-based inclusive position range `[start, end]`.
    Range { start: usize, end: usize },
    /// Every entry whose title/artist/album contains this (case-insensitive) query.
    QueryMatch(String),
    /// The currently-playing track.
    Current,
    /// The last `n` entries in the queue.
    Last(usize),
}

/// Where an [`Action::Move`] places the selected entries.
#[derive(Clone, Copy, Debug, Serialize, Deserialize, PartialEq)]
#[cfg_attr(feature = "schema", derive(schemars::JsonSchema))]
#[serde(tag = "movedest", rename_all = "snake_case")]
#[non_exhaustive]
pub enum MoveDest {
    /// To the 1-based absolute position among the remaining entries.
    Position(usize),
    /// Relative to the current track's position (may be negative: -1 = just before).
    Relative(i32),
}

/// How much an [`Action::Clear`] removes.
#[derive(Clone, Copy, Debug, Serialize, Deserialize, PartialEq)]
#[cfg_attr(feature = "schema", derive(schemars::JsonSchema))]
#[serde(tag = "clearscope", rename_all = "snake_case")]
#[non_exhaustive]
pub enum ClearScope {
    /// The whole queue.
    All,
    /// Everything AFTER the current track (keep the current track and its history).
    AfterCurrent,
    /// A 1-based inclusive position range.
    Range { start: usize, end: usize },
}

/// PURE. Resolve a [`QueueSelector`] to a sorted set of 0-based queue indices
/// against a queue described by its per-entry searchable text and current index.
/// A no-match returns an EMPTY vec (the caller treats that as a clean no-op).
/// Unit-testable with fabricated text - no queue, no player.
pub fn resolve_selector(sel: &QueueSelector, texts: &[String], current: Option<usize>) -> Vec<usize> {
    let n = texts.len();
    let idx1 = |p: usize| -> Option<usize> {
        if p >= 1 && p <= n { Some(p - 1) } else { None }
    };
    match sel {
        QueueSelector::Position(p) => idx1(*p).into_iter().collect(),
        QueueSelector::Range { start, end } => {
            let (a, b) = if start <= end { (*start, *end) } else { (*end, *start) };
            (a..=b).filter_map(idx1).collect()
        }
        QueueSelector::QueryMatch(q) => {
            let ql = q.trim().to_lowercase();
            if ql.is_empty() {
                return Vec::new();
            }
            texts
                .iter()
                .enumerate()
                .filter(|(_, t)| t.to_lowercase().contains(&ql))
                .map(|(i, _)| i)
                .collect()
        }
        QueueSelector::Current => current.filter(|c| *c < n).into_iter().collect(),
        QueueSelector::Last(k) => {
            if *k == 0 || n == 0 {
                return Vec::new();
            }
            let k = (*k).min(n);
            (n - k..n).collect()
        }
    }
}

/// A local serde mirror of [`crate::handler::FadeIntent`], plus the duration the
/// bare handler intent leaves implicit. `secs` is CLAMPED to `[min_dur, max_dur]`.
#[derive(Clone, Copy, Debug, Serialize, Deserialize)]
#[cfg_attr(feature = "schema", derive(schemars::JsonSchema))]
#[serde(tag = "dir", rename_all = "snake_case")]
pub enum FadeIntentIr {
    /// Ramp to silence, then stop and restore the pre-fade baseline.
    Out { secs: f64 },
    /// Wake ramp up to the comfort ceiling.
    In { secs: f64 },
    /// Deliberate cue to an explicit level, committing `vol` on completion.
    To { target_db: f64, vol: u8, secs: f64 },
    /// Sub-JND wind-down to the configured non-silence floor (`floor_level_db`),
    /// leaving playback running. The floor is read from the LIVE config at spawn
    /// (never baked into the raw plan), so `secs` is the only knob carried here.
    ToFloor { secs: f64 },
    /// Sub-JND wake ramp UP from silence to the SAVED comfort level, committing
    /// `vol` as the restored baseline. Distinct from `In` (which targets the
    /// comfort ceiling / vol 100): a wake restores the exact saved comfort volume.
    WakeTo { target_db: f64, vol: u8, secs: f64 },
}

/// A bounded content selector for [`Action::Enqueue`]. Unimplemented variants
/// return a loud not-yet at EXECUTE time (never silently drop); P4 swaps richer
/// behavior behind the same enum.
#[derive(Clone, Debug, Serialize, Deserialize)]
#[cfg_attr(feature = "schema", derive(schemars::JsonSchema))]
#[serde(tag = "select", rename_all = "snake_case")]
#[non_exhaustive]
pub enum Selector {
    Query(String),
    Exact(Vec<SongId>),
    Similar(SongId),
    /// "More like what is playing." Carries NO id - the MODEL emits this token
    /// (the off-surface-id safety boundary forbids the model naming a library id),
    /// and the daemon fills the seed server-side at EXECUTE time from the current
    /// (or first-queued) song. An honest no-op when nothing is playing/queued.
    SimilarToCurrent,
    Calmer(SongId),
    Genre(String),
    Radio,
}

/// The numeric clamps, sourced from the SAME [`FadeConfig`] knobs the fade
/// primitive normalizes against (one source of truth).
#[derive(Clone, Copy, Debug)]
pub struct PlanBounds {
    pub min_dur: Duration,
    pub max_dur: Duration,
    pub synth_floor_db: f64,
    pub wake_ceiling_db: f64,
    pub max_secs: f64,
    pub max_enqueue: u32,
    pub lead: Duration,
}

impl PlanBounds {
    /// Derive the clamps from the live (already-normalized) fade config.
    pub fn from_fade_config(cfg: &FadeConfig) -> Self {
        Self {
            min_dur: Duration::from_millis(cfg.min_slew_ms),
            max_dur: Duration::from_secs(cfg.max_dur_secs),
            synth_floor_db: cfg.synth_floor_db,
            wake_ceiling_db: cfg.wake_ceiling_db,
            max_secs: cfg.max_dur_secs as f64,
            max_enqueue: MAX_ENQUEUE,
            lead: Duration::from_millis(cfg.min_slew_ms),
        }
    }
}

/// The BOUNDED, concrete, POST-arm resolution: no relative refs, no mutable
/// index. This is the type that makes a wrong-track misfire unrepresentable.
#[derive(Clone, Debug, PartialEq)]
#[non_exhaustive]
pub enum Resolved {
    /// Execute at add-time (the target IS the current track, whose `TrackStart`
    /// already fired), never a stored edge.
    Immediate,
    /// Fire on `TrackStart` of this concrete entry.
    OnTrackStart(QueueId),
    /// Fire on `TrackEnd(last)` when the successor is a different album.
    OnAlbumBoundary { last: QueueId, album: AlbumId },
    /// Fire on the `WallClock` of the timer armed for this absolute deadline.
    OnDeadline(Instant),
    /// Fire on the `WallClock` armed `lead` before `track` ends.
    OnTrackRemaining { track: QueueId, lead: Duration },
}

/// Why a raw plan could not be armed. Maps 1:1 to a fail-loud ACK.
#[derive(Clone, Debug, thiserror::Error, PartialEq)]
pub enum PlanError {
    #[error("target queue slot is gone")]
    Unresolvable,
    #[error("target already played")]
    AlreadyPassed,
    #[error("{field} out of range")]
    OutOfBounds { field: &'static str },
    #[error("no known duration (stream)")]
    NoDuration,
    #[error("no album metadata")]
    NoAlbum,
    #[error("deadline already in the past")]
    PastDeadline,
    #[error("selector not yet supported: {0}")]
    Unsupported(String),
    #[error("bad action for trigger")]
    Untimed,
}

/// The pure ARMED plan: concrete data only, NEVER serialized. The RAII
/// [`crate::timer::TimerGuard`] that disarms a deadline plan is held ALONGSIDE
/// this in the registry, not inside it, so this type stays `Clone` + pure.
#[derive(Clone, Debug)]
pub struct ArmedPlan {
    pub id: PlanId,
    pub raw: RawPlan,
    pub resolved: Resolved,
    pub armed_at: Instant,
    pub once: bool,
    /// The timer this plan fires on, when it is a deadline/remaining plan. `None`
    /// until a [`Resolved::OnTrackRemaining`] plan is lazily armed by the executor.
    pub timer_id: Option<TimerId>,
}

/// Whether an armed plan fires on a given event.
#[derive(Clone, Copy, Debug, PartialEq)]
pub enum Fire {
    /// This event is the armed edge and the target is still valid: execute.
    Yes,
    /// Not this edge.
    No,
    /// The target is gone or already passed: fail loud, never fire on a neighbor.
    Stale,
}

/// Clamp every numeric in a raw action into its safe range (never reject). The
/// trigger numerics are clamped inside [`validate`] (they feed [`Resolved`]);
/// this handles the action side, applied by the registry before storing.
pub fn clamp_action(action: &Action, bounds: &PlanBounds) -> Action {
    let clamp_secs = |s: f64| -> f64 {
        let lo = bounds.min_dur.as_secs_f64();
        let hi = bounds.max_dur.as_secs_f64();
        if s.is_finite() {
            s.clamp(lo, hi)
        } else {
            lo
        }
    };
    match action {
        Action::Fade(FadeIntentIr::Out { secs }) => {
            Action::Fade(FadeIntentIr::Out { secs: clamp_secs(*secs) })
        }
        Action::Fade(FadeIntentIr::In { secs }) => {
            Action::Fade(FadeIntentIr::In { secs: clamp_secs(*secs) })
        }
        Action::Fade(FadeIntentIr::To { target_db, vol, secs }) => {
            let td = if target_db.is_finite() {
                target_db.clamp(bounds.synth_floor_db, bounds.wake_ceiling_db)
            } else {
                bounds.wake_ceiling_db
            };
            Action::Fade(FadeIntentIr::To {
                target_db: td,
                vol: (*vol).min(100),
                secs: clamp_secs(*secs),
            })
        }
        Action::Fade(FadeIntentIr::ToFloor { secs }) => {
            Action::Fade(FadeIntentIr::ToFloor { secs: clamp_secs(*secs) })
        }
        Action::Fade(FadeIntentIr::WakeTo { target_db, vol, secs }) => {
            let td = if target_db.is_finite() {
                target_db.clamp(bounds.synth_floor_db, bounds.wake_ceiling_db)
            } else {
                bounds.wake_ceiling_db
            };
            Action::Fade(FadeIntentIr::WakeTo {
                target_db: td,
                vol: (*vol).min(100),
                secs: clamp_secs(*secs),
            })
        }
        Action::SetVolume(v) => Action::SetVolume((*v).min(100)),
        Action::Enqueue { selector, count } => Action::Enqueue {
            selector: selector.clone(),
            count: (*count).min(bounds.max_enqueue),
        },
        Action::Wake { selector, count } => Action::Wake {
            selector: selector.clone(),
            count: (*count).min(bounds.max_enqueue),
        },
        // PlayNow shares Enqueue's exact bounded-hole justification: a count-clamped
        // string Selector resolved label->id at execute (never an id from the model).
        Action::PlayNow { selector, count } => Action::PlayNow {
            selector: selector.clone(),
            count: (*count).min(bounds.max_enqueue),
        },
        Action::Stop => Action::Stop,
        Action::Pause => Action::Pause,
        // Queue-edit actions carry only a selector/scope/dest that resolves SAFELY
        // at execute time (a huge/absent position simply no-matches), so there is no
        // unbounded numeric to coerce here; pass them through unchanged.
        Action::Remove { .. }
        | Action::Move { .. }
        | Action::Clear { .. }
        | Action::Play { .. }
        | Action::Noop => action.clone(),
    }
}

/// Return a fully-clamped copy of a raw plan (action + trigger numerics), ready
/// to arm. The registry stores THIS, so a hallucinated numeric is coerced once.
pub fn clamp_raw(raw: &RawPlan, bounds: &PlanBounds) -> RawPlan {
    let mut out = raw.clone();
    out.action = clamp_action(&raw.action, bounds);
    match &mut out.trigger {
        RawTrigger::TimeRemaining { secs, .. } => *secs = clamp_secs_field(*secs, bounds),
        RawTrigger::SpanElapsed { secs } => *secs = clamp_span(*secs, bounds),
        _ => {}
    }
    out
}

fn clamp_secs_field(s: f64, bounds: &PlanBounds) -> f64 {
    if s.is_finite() {
        s.clamp(0.0, bounds.max_secs)
    } else {
        0.0
    }
}

fn clamp_span(s: f64, bounds: &PlanBounds) -> f64 {
    let lo = bounds.min_dur.as_secs_f64();
    if s.is_finite() {
        s.clamp(lo, bounds.max_secs)
    } else {
        lo
    }
}

/// The cursor index, or `None` when playback is stopped (no current track).
fn cursor_of(snap: &QueueSnapshot) -> Option<usize> {
    snap.current.as_ref().map(|c| c.index)
}

/// Resolve a [`TrackSel`] to a concrete queue id against the snapshot.
fn resolve_track(sel: &TrackSel, snap: &QueueSnapshot) -> Result<QueueId, PlanError> {
    match sel {
        TrackSel::Current => snap
            .current
            .as_ref()
            .map(|c| c.queue_id)
            .ok_or(PlanError::Unresolvable),
        TrackSel::RelToCurrent(delta) => {
            let cursor = cursor_of(snap).ok_or(PlanError::Unresolvable)? as i64;
            let idx = cursor + *delta as i64;
            if idx < 0 {
                return Err(PlanError::AlreadyPassed);
            }
            let idx = idx as usize;
            if idx >= snap.entries.len() {
                return Err(PlanError::OutOfBounds { field: "track" });
            }
            Ok(snap.entries[idx].queue_id)
        }
        TrackSel::QueueId(id) => {
            let qid = QueueId(*id);
            snap.find(qid).map(|_| qid).ok_or(PlanError::Unresolvable)
        }
    }
}

/// Resolve a position-based trigger to an absolute queue index, guarding the
/// arithmetic so a crafted huge `n` can NEVER overflow `usize` (debug panic /
/// release wrap). Overflow fails loud as [`PlanError::OutOfBounds`]; the
/// past-the-queue bound is enforced by [`classify_position`].
fn resolve_pos_index(cursor: usize, n: usize, base: PosBase) -> Result<usize, PlanError> {
    let idx = match base {
        // n == 1 is the current track; n-1 slots ahead of the cursor. Checked so a
        // near-usize::MAX n fails loud instead of overflowing.
        PosBase::CurrentIsOne => cursor.checked_add(n.saturating_sub(1)),
        PosBase::Absolute => Some(n.saturating_sub(1)),
    };
    idx.ok_or(PlanError::OutOfBounds { field: "position" })
}

/// Resolve a position-based trigger index against the cursor, classifying the
/// result relative to the current track (the shared Immediate/AlreadyPassed/
/// OutOfBounds/OnTrackStart branch).
fn classify_position(idx: usize, cursor: usize, snap: &QueueSnapshot) -> Result<Resolved, PlanError> {
    if idx == cursor {
        // The target IS the current track: its TrackStart already fired and never
        // re-fires, so execute at add-time.
        return Ok(Resolved::Immediate);
    }
    if idx < cursor {
        return Err(PlanError::AlreadyPassed);
    }
    if idx >= snap.entries.len() {
        return Err(PlanError::OutOfBounds { field: "position" });
    }
    Ok(Resolved::OnTrackStart(snap.entries[idx].queue_id))
}

/// PURE. Resolve a raw plan into a concrete [`Resolved`] anchor (or fail loud).
/// Clamps every trigger numeric it consumes; the action numerics are clamped by
/// [`clamp_action`]/[`clamp_raw`] before this is called.
///
/// `now` is the shared monotonic clock instant; `now_civil` is the wall-clock
/// instant used ONLY to reduce a [`RawTrigger::WallClock`] to a monotonic delay
/// (so the timeline never touches civil time - fake-clock deterministic).
pub fn validate(
    raw: &RawPlan,
    snap: &QueueSnapshot,
    now: Instant,
    now_civil: chrono::DateTime<chrono::Utc>,
    bounds: &PlanBounds,
) -> Result<Resolved, PlanError> {
    match &raw.trigger {
        RawTrigger::Immediate => Ok(Resolved::Immediate),

        RawTrigger::QueuePosition { n, base } => {
            let cursor = cursor_of(snap).ok_or(PlanError::Unresolvable)?;
            let idx = resolve_pos_index(cursor, *n, *base)?;
            classify_position(idx, cursor, snap)
        }

        RawTrigger::TrackAfterCurrent => {
            let cursor = cursor_of(snap).ok_or(PlanError::Unresolvable)?;
            let idx = cursor.checked_add(1).ok_or(PlanError::OutOfBounds { field: "position" })?;
            classify_position(idx, cursor, snap)
        }

        RawTrigger::TimeRemaining { track, secs } => {
            let qid = resolve_track(track, snap)?;
            let (_, entry) = snap.find(qid).ok_or(PlanError::Unresolvable)?;
            if entry.duration.is_none() {
                return Err(PlanError::NoDuration);
            }
            let lead = Duration::try_from_secs_f64(clamp_secs_field(*secs, bounds))
                .unwrap_or(Duration::ZERO);
            Ok(Resolved::OnTrackRemaining { track: qid, lead })
        }

        RawTrigger::AlbumBoundary { track } => {
            let qid = resolve_track(track, snap)?;
            let (_, entry) = snap.find(qid).ok_or(PlanError::Unresolvable)?;
            let album = entry.album_id.clone().ok_or(PlanError::NoAlbum)?;
            Ok(Resolved::OnAlbumBoundary { last: qid, album })
        }

        RawTrigger::WallClock { at } => {
            // Reduce civil time to a monotonic delay; the timeline never touches
            // wall-clock. A non-positive delta is already in the past.
            let delta = *at - now_civil;
            match delta.to_std() {
                Ok(delay) => Ok(Resolved::OnDeadline(now + delay)),
                Err(_) => Err(PlanError::PastDeadline),
            }
        }

        RawTrigger::SpanElapsed { secs } => {
            let dur = Duration::try_from_secs_f64(clamp_span(*secs, bounds))
                .unwrap_or(bounds.min_dur);
            Ok(Resolved::OnDeadline(now + dur))
        }
    }
}

/// PURE. Whether `armed` fires on `ev`, re-validating any armed [`QueueId`]
/// against the CURRENT `snap`. Reads `ev.now` internally (no redundant `now`).
///
/// Returns [`Fire::Stale`] the instant the target is gone (`snap.find` is `None`)
/// or has already been passed (its index is behind the cursor) - so a
/// deleted/moved-past target fails loud rather than firing on a neighbor.
pub fn fires(armed: &ArmedPlan, ev: &DjEvent, snap: &QueueSnapshot) -> Fire {
    let cursor = cursor_of(snap).unwrap_or(0);
    match &armed.resolved {
        // Immediate is executed at add-time; it never fires on a stored edge.
        Resolved::Immediate => Fire::No,

        Resolved::OnTrackStart(qid) => match snap.find(*qid) {
            None => Fire::Stale,
            Some((idx, _)) => {
                if idx < cursor {
                    return Fire::Stale;
                }
                match &ev.kind {
                    DjEventKind::TrackStart(t) if t.queue_id == *qid => Fire::Yes,
                    _ => Fire::No,
                }
            }
        },

        Resolved::OnAlbumBoundary { last, album } => match &ev.kind {
            // Fire off the TrackEnd(last) EVENT IDENTITY, never a live-cursor
            // staleness re-read. TrackEnd(last) is published BEFORE advance_on_eof
            // repoints `current`, so a `last.index < cursor` check would race the
            // async advance and silently drop the very boundary this plan exists to
            // catch. The finishing track's identity in the event is authoritative;
            // we locate `last` by stable identity only to peek its successor album.
            DjEventKind::TrackEnd(t) if t.queue_id == *last => match snap.find(*last) {
                None => Fire::Stale,
                Some((idx, _)) => {
                    // A different successor album (or NO successor - the fail-safe
                    // boundary where Stop lands in the gap) means the boundary is real.
                    let succ_album = snap.entries.get(idx + 1).and_then(|e| e.album_id.clone());
                    if succ_album.as_ref() == Some(album) {
                        Fire::No
                    } else {
                        Fire::Yes
                    }
                }
            },
            _ => Fire::No,
        },

        Resolved::OnDeadline(_) => match &ev.kind {
            DjEventKind::WallClock(id) if Some(*id) == armed.timer_id => Fire::Yes,
            _ => Fire::No,
        },

        Resolved::OnTrackRemaining { track, .. } => {
            // The remaining-timer is bound to `track` being the CURRENT track. If it
            // is gone OR no longer current (a skip advanced past it), fail loud - the
            // stale deadline must NEVER fire on the now-playing (wrong) track.
            match snap.find(*track) {
                None => Fire::Stale,
                Some((idx, _)) => {
                    if Some(idx) != cursor_of(snap) {
                        return Fire::Stale;
                    }
                    match &ev.kind {
                        DjEventKind::WallClock(id) if Some(*id) == armed.timer_id => Fire::Yes,
                        _ => Fire::No,
                    }
                }
            }
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::event::{Cursor, EntrySnapshot, TrackRef};

    fn entry(id: u64, album: Option<&str>, dur: Option<u64>) -> EntrySnapshot {
        EntrySnapshot {
            queue_id: QueueId(id),
            song: Some(SongId(format!("s{id}"))),
            album_id: album.map(|a| AlbumId(a.into())),
            duration: dur.map(Duration::from_secs),
        }
    }

    /// A 5-entry queue with the cursor at `cur`, ids 10..15 (so an index is never
    /// mistaken for a queue id).
    fn snap5(cur: usize) -> QueueSnapshot {
        let entries: Vec<_> = (0..5)
            .map(|i| entry(10 + i as u64, Some("A"), Some(200)))
            .collect();
        QueueSnapshot {
            playlist_version: 1,
            current: Some(Cursor {
                index: cur,
                queue_id: entries[cur].queue_id,
            }),
            entries,
        }
    }

    fn bounds() -> PlanBounds {
        PlanBounds::from_fade_config(&FadeConfig::default())
    }

    fn now() -> Instant {
        Instant::now()
    }

    fn civil() -> chrono::DateTime<chrono::Utc> {
        chrono::Utc::now()
    }

    fn raw(trigger: RawTrigger) -> RawPlan {
        RawPlan {
            version: 1,
            trigger,
            action: Action::Stop,
            once: true,
            origin: "test".into(),
        }
    }

    // mapping: CurrentIsOne n=3 -> cursor+2; Absolute n -> n-1; TrackAfterCurrent.
    #[test]
    fn position_mapping_resolves_intended_entry() {
        let snap = snap5(0);
        // n=3 counting current as 1st -> cursor(0)+2 -> entries[2] = QueueId(12).
        let r = validate(
            &raw(RawTrigger::QueuePosition { n: 3, base: PosBase::CurrentIsOne }),
            &snap,
            now(),
            civil(),
            &bounds(),
        )
        .unwrap();
        assert_eq!(r, Resolved::OnTrackStart(QueueId(12)));

        // Absolute n=4 -> index 3 -> QueueId(13).
        let r = validate(
            &raw(RawTrigger::QueuePosition { n: 4, base: PosBase::Absolute }),
            &snap,
            now(),
            civil(),
            &bounds(),
        )
        .unwrap();
        assert_eq!(r, Resolved::OnTrackStart(QueueId(13)));

        // TrackAfterCurrent -> cursor+1 -> QueueId(11).
        let r = validate(&raw(RawTrigger::TrackAfterCurrent), &snap, now(), civil(), &bounds()).unwrap();
        assert_eq!(r, Resolved::OnTrackStart(QueueId(11)));
    }

    // classify: idx==cursor -> Immediate; idx<cursor -> AlreadyPassed; idx>=len.
    #[test]
    fn classify_immediate_passed_oob() {
        let snap = snap5(2);
        // n=1 CurrentIsOne -> idx = cursor -> Immediate.
        let r = validate(
            &raw(RawTrigger::QueuePosition { n: 1, base: PosBase::CurrentIsOne }),
            &snap,
            now(),
            civil(),
            &bounds(),
        )
        .unwrap();
        assert_eq!(r, Resolved::Immediate);

        // Absolute n=1 -> idx 0 < cursor 2 -> AlreadyPassed.
        let e = validate(
            &raw(RawTrigger::QueuePosition { n: 1, base: PosBase::Absolute }),
            &snap,
            now(),
            civil(),
            &bounds(),
        )
        .unwrap_err();
        assert_eq!(e, PlanError::AlreadyPassed);

        // Absolute n=99 -> idx 98 >= len -> OutOfBounds.
        let e = validate(
            &raw(RawTrigger::QueuePosition { n: 99, base: PosBase::Absolute }),
            &snap,
            now(),
            civil(),
            &bounds(),
        )
        .unwrap_err();
        assert!(matches!(e, PlanError::OutOfBounds { .. }));
    }

    // reject untimed: TimeRemaining / AlbumBoundary on missing metadata.
    #[test]
    fn reject_no_duration_and_no_album() {
        let mut snap = snap5(0);
        snap.entries[0].duration = None;
        snap.entries[0].album_id = None;
        let e = validate(
            &raw(RawTrigger::TimeRemaining { track: TrackSel::Current, secs: 10.0 }),
            &snap,
            now(),
            civil(),
            &bounds(),
        )
        .unwrap_err();
        assert_eq!(e, PlanError::NoDuration);

        let e = validate(
            &raw(RawTrigger::AlbumBoundary { track: TrackSel::Current }),
            &snap,
            now(),
            civil(),
            &bounds(),
        )
        .unwrap_err();
        assert_eq!(e, PlanError::NoAlbum);
    }

    // WallClock: past -> PastDeadline; future -> OnDeadline with delay = at-now.
    #[test]
    fn wallclock_past_and_future() {
        let snap = snap5(0);
        let n = now();
        let c = civil();
        let past = c - chrono::Duration::seconds(60);
        let e = validate(&raw(RawTrigger::WallClock { at: past }), &snap, n, c, &bounds()).unwrap_err();
        assert_eq!(e, PlanError::PastDeadline);

        let future = c + chrono::Duration::seconds(120);
        let r = validate(&raw(RawTrigger::WallClock { at: future }), &snap, n, c, &bounds()).unwrap();
        match r {
            Resolved::OnDeadline(inst) => {
                let delay = inst - n;
                // ~120s, within a small slop for the civil->std conversion.
                assert!(delay >= Duration::from_secs(119) && delay <= Duration::from_secs(121));
            }
            other => panic!("expected OnDeadline, got {other:?}"),
        }
    }

    // clamps: target_db to [synth_floor, wake_ceiling], vol/SetVolume to 100,
    // secs/span/count clamped (clamped, never rejected).
    #[test]
    fn clamps_every_numeric() {
        let b = bounds();
        // target_db above the ceiling clamps down; vol clamps to 100.
        let a = clamp_action(
            &Action::Fade(FadeIntentIr::To { target_db: 999.0, vol: 250, secs: 1e9 }),
            &b,
        );
        match a {
            Action::Fade(FadeIntentIr::To { target_db, vol, secs }) => {
                assert!(target_db <= b.wake_ceiling_db && target_db >= b.synth_floor_db);
                assert_eq!(vol, 100);
                assert!(secs <= b.max_dur.as_secs_f64());
            }
            other => panic!("got {other:?}"),
        }
        assert!(matches!(clamp_action(&Action::SetVolume(200), &b), Action::SetVolume(100)));
        match clamp_action(&Action::Enqueue { selector: Selector::Radio, count: 9999 }, &b) {
            Action::Enqueue { count, .. } => assert_eq!(count, MAX_ENQUEUE),
            other => panic!("got {other:?}"),
        }
        // PlayNow clamps count exactly like Enqueue (shared bounded hole).
        match clamp_action(
            &Action::PlayNow { selector: Selector::Query("x".into()), count: 9999 },
            &b,
        ) {
            Action::PlayNow { count, .. } => assert_eq!(count, MAX_ENQUEUE),
            other => panic!("got {other:?}"),
        }
        // SpanElapsed secs clamps into [min_dur, max_dur].
        let snap = snap5(0);
        let r = validate(
            &clamp_raw(&raw(RawTrigger::SpanElapsed { secs: 1e9 }), &b),
            &snap,
            now(),
            civil(),
            &b,
        )
        .unwrap();
        match r {
            Resolved::OnDeadline(_) => {}
            other => panic!("expected OnDeadline, got {other:?}"),
        }
    }

    // The NEW convenience-feature IR variants clamp exactly like their siblings:
    // ToFloor secs into [min,max]; WakeTo target_db into [synth_floor, ceiling],
    // vol -> 100, secs clamped; Action::Wake count -> MAX_ENQUEUE.
    #[test]
    fn clamp_convenience_fade_and_wake_variants() {
        let b = bounds();
        match clamp_action(&Action::Fade(FadeIntentIr::ToFloor { secs: 1e9 }), &b) {
            Action::Fade(FadeIntentIr::ToFloor { secs }) => {
                assert!(secs >= b.min_dur.as_secs_f64() && secs <= b.max_dur.as_secs_f64());
            }
            other => panic!("got {other:?}"),
        }
        match clamp_action(
            &Action::Fade(FadeIntentIr::WakeTo { target_db: 999.0, vol: 250, secs: 1e9 }),
            &b,
        ) {
            Action::Fade(FadeIntentIr::WakeTo { target_db, vol, secs }) => {
                assert!(target_db >= b.synth_floor_db && target_db <= b.wake_ceiling_db);
                assert_eq!(vol, 100);
                assert!(secs <= b.max_dur.as_secs_f64());
            }
            other => panic!("got {other:?}"),
        }
        match clamp_action(
            &Action::Wake { selector: Some(Selector::Query("x".into())), count: 9999 },
            &b,
        ) {
            Action::Wake { count, .. } => assert_eq!(count, MAX_ENQUEUE),
            other => panic!("got {other:?}"),
        }
    }

    // The WallClock horizon is UNCLAMPED: an `at` 1h out (> max_dur = 30min) yields
    // OnDeadline ~now+3600, proving the fade-duration cap never bounds the timer.
    #[test]
    fn wallclock_horizon_unclamped_beyond_max_dur() {
        let snap = snap5(0);
        let n = now();
        let c = civil();
        let future = c + chrono::Duration::seconds(3600); // 1h, well past max_dur
        let r = validate(&raw(RawTrigger::WallClock { at: future }), &snap, n, c, &bounds()).unwrap();
        match r {
            Resolved::OnDeadline(inst) => {
                let delay = inst - n;
                assert!(
                    delay >= Duration::from_secs(3599) && delay <= Duration::from_secs(3601),
                    "horizon not capped at max_dur, got {delay:?}"
                );
            }
            other => panic!("expected OnDeadline, got {other:?}"),
        }
    }

    fn ev(kind: DjEventKind, snap: &QueueSnapshot) -> DjEvent {
        DjEvent {
            kind,
            now: now(),
            seq: 1,
            playlist_version: snap.playlist_version,
            cursor: snap.current.as_ref().map(|c| c.queue_id),
        }
    }

    fn tref(id: u64) -> TrackRef {
        TrackRef {
            queue_id: QueueId(id),
            queue_index: None,
            song: None,
            album_id: None,
            duration: None,
        }
    }

    fn armed(resolved: Resolved) -> ArmedPlan {
        ArmedPlan {
            id: PlanId(1),
            raw: raw(RawTrigger::Immediate),
            resolved,
            armed_at: now(),
            once: true,
            timer_id: None,
        }
    }

    // fires() happy: OnTrackStart(qid) + TrackStart(qid) present -> Yes.
    #[test]
    fn fires_happy_track_start() {
        let snap = snap5(0);
        let a = armed(Resolved::OnTrackStart(QueueId(12)));
        let e = ev(DjEventKind::TrackStart(tref(12)), &snap);
        assert_eq!(fires(&a, &e, &snap), Fire::Yes);
        // A different track's start does not fire.
        let e2 = ev(DjEventKind::TrackStart(tref(11)), &snap);
        assert_eq!(fires(&a, &e2, &snap), Fire::No);
    }

    // fires() Stale (core safety): target gone -> Stale; index<cursor -> Stale;
    // never Yes.
    #[test]
    fn fires_stale_never_yes() {
        // Target deleted: snapshot without QueueId(12).
        let mut snap = snap5(0);
        snap.entries.retain(|e| e.queue_id != QueueId(12));
        let a = armed(Resolved::OnTrackStart(QueueId(12)));
        let e = ev(DjEventKind::TrackStart(tref(12)), &snap);
        assert_eq!(fires(&a, &e, &snap), Fire::Stale);

        // Target behind the cursor: cursor at 3, armed on QueueId(11) (index 1).
        let snap = snap5(3);
        let a = armed(Resolved::OnTrackStart(QueueId(11)));
        let e = ev(DjEventKind::TrackStart(tref(11)), &snap);
        assert_eq!(fires(&a, &e, &snap), Fire::Stale);
    }

    // fires() AlbumBoundary: fires on TrackEnd(last) only when the successor album
    // differs; two album-less tracks -> None successor path is a defined boundary.
    #[test]
    fn fires_album_boundary_defined() {
        // Cursor at 0, album A entries 10..12 then album B at index 2.
        let mut snap = snap5(0);
        snap.entries[2].album_id = Some(AlbumId("B".into()));
        let a = armed(Resolved::OnAlbumBoundary { last: QueueId(11), album: AlbumId("A".into()) });
        // last is index 1 (album A), successor index 2 is album B -> boundary.
        // But cursor must be <= idx: move cursor to 1 so it is not stale.
        let mut snap_b = snap.clone();
        snap_b.current = Some(Cursor { index: 1, queue_id: QueueId(11) });
        let e = ev(DjEventKind::TrackEnd(tref(11)), &snap_b);
        assert_eq!(fires(&a, &e, &snap_b), Fire::Yes);

        // Same-album successor -> not a boundary.
        let a2 = armed(Resolved::OnAlbumBoundary { last: QueueId(10), album: AlbumId("A".into()) });
        let snap_c = snap5(0); // all album A
        let e2 = ev(DjEventKind::TrackEnd(tref(10)), &snap_c);
        assert_eq!(fires(&a2, &e2, &snap_c), Fire::No);

        // Two album-less tracks: successor album None != Some(album) -> boundary.
        let mut snap_d = snap5(0);
        for en in &mut snap_d.entries {
            en.album_id = None;
        }
        // But an album-less last can't arm (NoAlbum); construct the Resolved
        // directly to exercise the None-successor branch of fires().
        let a3 = armed(Resolved::OnAlbumBoundary { last: QueueId(10), album: AlbumId("A".into()) });
        let e3 = ev(DjEventKind::TrackEnd(tref(10)), &snap_d);
        assert_eq!(fires(&a3, &e3, &snap_d), Fire::Yes);
    }

    // F4: AlbumBoundary fires off the TrackEnd(last) event identity even when the
    // async EOF advance already repointed the cursor PAST `last` (so a live-cursor
    // staleness re-read would wrongly drop it). Cursor at index 1, last is index 0.
    #[test]
    fn fires_album_boundary_survives_eof_advance() {
        let mut snap = snap5(0);
        snap.entries[1].album_id = Some(AlbumId("B".into())); // successor differs
        // The advance already moved current to index 1; `last` (index 0, id 10) is
        // now behind the cursor. The old idx<cursor guard would return Stale here.
        snap.current = Some(Cursor { index: 1, queue_id: QueueId(11) });
        let a = armed(Resolved::OnAlbumBoundary { last: QueueId(10), album: AlbumId("A".into()) });
        let e = ev(DjEventKind::TrackEnd(tref(10)), &snap);
        assert_eq!(fires(&a, &e, &snap), Fire::Yes, "boundary not lost to the advance");
    }

    // F2c: an OnTrackRemaining plan whose target is no longer the CURRENT track (a
    // skip advanced past it) is Stale on its WallClock fire - never fires on the
    // wrong, now-playing track.
    #[test]
    fn fires_track_remaining_stale_when_not_current() {
        let snap = snap5(0); // cursor at index 0 (QueueId 10)
        // Target is QueueId(11) (index 1) - present but NOT current.
        let mut a = armed(Resolved::OnTrackRemaining { track: QueueId(11), lead: Duration::ZERO });
        a.timer_id = Some(TimerId(7));
        let e = ev(DjEventKind::WallClock(TimerId(7)), &snap);
        assert_eq!(fires(&a, &e, &snap), Fire::Stale, "never fires on the wrong track");

        // Same plan when the target IS current fires normally.
        let snap_cur = snap5(1); // cursor at index 1 (QueueId 11)
        let e2 = ev(DjEventKind::WallClock(TimerId(7)), &snap_cur);
        assert_eq!(fires(&a, &e2, &snap_cur), Fire::Yes);
    }

    // F5: a crafted huge QueuePosition n fails loud with OutOfBounds - never a
    // usize overflow panic (debug) or wrap (release).
    #[test]
    fn queue_position_huge_n_fails_loud_no_panic() {
        let snap = snap5(2); // cursor at index 2
        let e = validate(
            &raw(RawTrigger::QueuePosition { n: usize::MAX, base: PosBase::CurrentIsOne }),
            &snap,
            now(),
            civil(),
            &bounds(),
        )
        .unwrap_err();
        assert!(matches!(e, PlanError::OutOfBounds { .. }), "got {e:?}");

        // Absolute base with a huge n also fails loud (past the queue), no wrap.
        let e = validate(
            &raw(RawTrigger::QueuePosition { n: usize::MAX, base: PosBase::Absolute }),
            &snap,
            now(),
            civil(),
            &bounds(),
        )
        .unwrap_err();
        assert!(matches!(e, PlanError::OutOfBounds { .. }), "got {e:?}");
    }

    // resolve_selector: every selector variant resolves to the intended 0-based
    // index set, and a no-match (bad position, empty/absent query, empty queue)
    // is a clean EMPTY set - the caller's clean no-op, never a wrong-target edit.
    #[test]
    fn resolve_selector_variants_and_no_match() {
        let texts: Vec<String> = vec![
            "Miles Davis - So What".into(),
            "Bill Evans - Peace Piece".into(),
            "Miles Davis - Blue in Green".into(),
            "Aphex Twin - Rhubarb".into(),
        ];
        let cur = Some(1usize);

        // Position (1-based) -> single index; out-of-range -> empty.
        assert_eq!(resolve_selector(&QueueSelector::Position(1), &texts, cur), vec![0]);
        assert_eq!(resolve_selector(&QueueSelector::Position(4), &texts, cur), vec![3]);
        assert!(resolve_selector(&QueueSelector::Position(9), &texts, cur).is_empty());
        assert!(resolve_selector(&QueueSelector::Position(0), &texts, cur).is_empty());

        // Range inclusive, order-normalized, clamped to the queue.
        assert_eq!(
            resolve_selector(&QueueSelector::Range { start: 2, end: 3 }, &texts, cur),
            vec![1, 2]
        );
        assert_eq!(
            resolve_selector(&QueueSelector::Range { start: 3, end: 2 }, &texts, cur),
            vec![1, 2]
        );
        assert_eq!(
            resolve_selector(&QueueSelector::Range { start: 3, end: 99 }, &texts, cur),
            vec![2, 3]
        );

        // QueryMatch: case-insensitive substring across all matching entries.
        assert_eq!(
            resolve_selector(&QueueSelector::QueryMatch("miles".into()), &texts, cur),
            vec![0, 2]
        );
        assert_eq!(
            resolve_selector(&QueueSelector::QueryMatch("rhubarb".into()), &texts, cur),
            vec![3]
        );
        // No match / blank query -> clean empty set (the no-op).
        assert!(resolve_selector(&QueueSelector::QueryMatch("nonesuch".into()), &texts, cur).is_empty());
        assert!(resolve_selector(&QueueSelector::QueryMatch("   ".into()), &texts, cur).is_empty());

        // Current -> the cursor index; guarded against a stale out-of-range cursor.
        assert_eq!(resolve_selector(&QueueSelector::Current, &texts, cur), vec![1]);
        assert!(resolve_selector(&QueueSelector::Current, &texts, None).is_empty());
        assert!(resolve_selector(&QueueSelector::Current, &texts, Some(99)).is_empty());

        // Last(n) -> the tail; clamped to the queue length; 0 / empty -> empty.
        assert_eq!(resolve_selector(&QueueSelector::Last(2), &texts, cur), vec![2, 3]);
        assert_eq!(resolve_selector(&QueueSelector::Last(99), &texts, cur), vec![0, 1, 2, 3]);
        assert!(resolve_selector(&QueueSelector::Last(0), &texts, cur).is_empty());
        assert!(resolve_selector(&QueueSelector::Position(1), &[], None).is_empty());
    }
}
