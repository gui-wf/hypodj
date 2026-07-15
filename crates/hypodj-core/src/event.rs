//! P1 event/trigger substrate: the typed [`DjEvent`] stream + the queue
//! [`QueueSnapshot`] the enrichment join and resync both read.
//!
//! FOUNDATION (P1), built to LAST. This module is DATA only - the shapes the
//! director ([`crate::director`]) publishes and the future P2 plan-executor
//! consumes. The three traffic classes that carry these shapes are:
//!
//!   1. the LOSSLESS spine (single consumer of the player's `mpsc`), which runs
//!      scrobble + advance inline and never drops an [`crate::player::PlayerEvent`];
//!   2. a LOSSLESS edge-trigger fan-out to the executor (per-subscriber unbounded
//!      `mpsc<DjEvent>`), so a boundary trigger is never lost even under load;
//!   3. a LOSSY observational broadcast (high-rate `Tick` + a mirror of every
//!      edge) plus a level-triggered [`QueueSnapshot`] `watch` for resync.
//!
//! Why identities, not indices: the enrichment join anchors on [`QueueId`] (a
//! stable per-entry handle), NOT the current index, because the current index is
//! mutated OFF the spine by MPD `next`/`prev`/`delete` while buffered `TimePos`
//! frames are still draining. Latch-based, in-FIFO-order attribution is then
//! exact even across an off-spine advance, and a deleted entry disambiguates as
//! "gone" (index `None`) rather than inheriting a neighbor's row.

use std::time::Duration;

use tokio::time::Instant;

use crate::model::{AlbumId, SongId};
use crate::player::PlayState;

/// A stable per-queue-entry identity. This is the SAME integer as an
/// [`crate::handler`] `QueueItem.id` (MPD's monotonic per-song handle), wrapped
/// so it can never be cross-used as an index or an MPD position. The enrichment
/// join is anchored on this, never on the mutable current index.
#[derive(Clone, Copy, Debug, PartialEq, Eq, Hash)]
pub struct QueueId(pub u64);

/// A wall-clock timer identity handed out by [`crate::timer::TimerHandle::arm`].
#[derive(Clone, Copy, Debug, PartialEq, Eq, Hash)]
pub struct TimerId(pub u64);

/// Everything the executor needs to identify a track boundary. Every "unknown"
/// field is an [`Option`] so streams and duration-less songs stay HONEST (a
/// missing duration is `None`, never `0`-as-unknown; a raw stream has no song
/// and no album).
#[derive(Clone, Debug)]
pub struct TrackRef {
    /// The stable identity of the entry this boundary is about.
    pub queue_id: QueueId,
    /// Its position in the queue at publish time, or `None` if the entry has
    /// left the queue (delete/move) - NEVER a shifted neighbor's index.
    pub queue_index: Option<usize>,
    /// The library song id, or `None` for a raw stream.
    pub song: Option<SongId>,
    /// The album id, or `None` for a stream OR a metadata-less song.
    pub album_id: Option<AlbumId>,
    /// The track duration, or `None` when unknown. NEVER `0`-as-unknown.
    pub duration: Option<Duration>,
}

/// The payload of a [`DjEvent`]. `#[non_exhaustive]` so the executor must carry a
/// catch-all arm from day 1 and a later variant is not a breaking change.
#[non_exhaustive]
#[derive(Clone, Debug)]
pub enum DjEventKind {
    /// A periodic position update (high-rate, LOSSY - broadcast only). `time_pos`
    /// is the current media position; `time_remaining` is `Some` only when the
    /// track has a known non-zero duration (never for a stream).
    Tick {
        time_pos: Duration,
        time_remaining: Option<Duration>,
    },
    /// A track began playing (edge, LOSSLESS).
    TrackStart(TrackRef),
    /// A track finished (edge, LOSSLESS). Published BEFORE the queue advances, so
    /// observers always see End-then-Start across a boundary.
    TrackEnd(TrackRef),
    /// A play-state edge other than a track start (Paused/Stopped). Carries the
    /// track it applies to, or `None` on Stop.
    StateChanged(PlayState, Option<TrackRef>),
    /// A wall-clock timer fired (edge, LOSSLESS). `time_pos`/`time_remaining` are
    /// deliberately absent on the [`DjEvent`]: a timer fires independently of
    /// playback, so stamping a stale media position would be a lie.
    WallClock(TimerId),
    /// Emitted on the broadcast after the `watch` snapshot was refreshed, so a
    /// LAGGED observer knows a fresh level-triggered snapshot is available to
    /// re-derive from. Never carries must-not-drop payload.
    Resync,
}

/// One published event. The payload lives INSIDE [`DjEventKind`] (no
/// boolean-blind flat fields); the envelope carries the cross-cutting stamps.
#[derive(Clone, Debug)]
pub struct DjEvent {
    pub kind: DjEventKind,
    /// The shared [`crate::clock::Clock`] instant at publish time - the SAME
    /// monotonic base as the timers and the fade driver (never mixed with media
    /// seconds).
    pub now: Instant,
    /// Monotonic per-stream sequence. A gap in `seq` means a missed event, which
    /// is cheaper to detect than relying on a broadcast `Lagged`.
    pub seq: u64,
    /// The queue "playlist version" at publish time. Always present, so the
    /// `watch` snapshot is a genuine level-triggered source.
    pub playlist_version: u64,
    /// The current track identity at publish time, or `None` when stopped.
    pub cursor: Option<QueueId>,
}

/// One queue entry's join-relevant data. Owned (no borrow of handler state).
#[derive(Clone, Debug)]
pub struct EntrySnapshot {
    pub queue_id: QueueId,
    /// The library song id for this entry, or `None` for a raw stream. Carried so
    /// a LAGGED observer can re-derive which song is current from the snapshot
    /// alone (via the `current` cursor's queue id), without a second lookup.
    pub song: Option<SongId>,
    pub album_id: Option<AlbumId>,
    pub duration: Option<Duration>,
}

/// The current-song pointer inside a [`QueueSnapshot`].
#[derive(Clone, Debug)]
pub struct Cursor {
    pub index: usize,
    pub queue_id: QueueId,
}

/// A whole-queue snapshot: BOTH the enrichment join source and the resync
/// source. Owned data only, built under ONE short state-lock scope and never
/// held across an `.await`.
#[derive(Clone, Debug)]
pub struct QueueSnapshot {
    pub playlist_version: u64,
    pub current: Option<Cursor>,
    pub entries: Vec<EntrySnapshot>,
}

impl QueueSnapshot {
    /// Locate an entry by its stable identity, returning its current index and
    /// row. `None` if the entry has left the queue. This is the resync-side
    /// mirror of [`crate::handler::HypodjHandler::snapshot_by_queue_id`].
    pub fn find(&self, id: QueueId) -> Option<(usize, &EntrySnapshot)> {
        self.entries
            .iter()
            .enumerate()
            .find(|(_, e)| e.queue_id == id)
    }
}
