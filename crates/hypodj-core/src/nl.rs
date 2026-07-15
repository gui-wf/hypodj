//! PURE, model-free natural-language translator SEAM (P3).
//!
//! TRUST BOUNDARY (mirrors [`crate::plan`]'s "identities, not indices"): this
//! module defines ONLY the injected trait + its context/error/outcome types. It
//! holds ZERO model dependency and references only [`RawPlan`] / [`SongId`]. The
//! concrete translators live in the OPTIONAL `hypodj-nl` crate; the daemon injects
//! one into the handler. A translator can NEVER arm a plan: it only EMITS a
//! [`RawPlan`], which the existing P2 [`crate::plan::validate`] gates, and which
//! the handler echoes back for an explicit human confirm before `plan_add`.
//!
//! The model never touches a `QueueId`/`AlbumId`/`Instant`: those stay symbolic in
//! [`RawPlan`]'s raw trigger and are resolved by `validate()` at confirm time
//! against the LIVE snapshot.

use crate::model::SongId;
use crate::plan::RawPlan;

/// The disambiguation context a translator needs to turn a relative utterance
/// ("the 3rd track", "more like this", "at 7") into a raw plan. Built from the
/// LIVE queue snapshot by the handler at translate time; carries owned data only
/// (no borrow of handler state), so a translator can run under `spawn_blocking`.
pub struct NlContext {
    /// The current track's library song id, for `Selector::Calmer`/`Similar`.
    /// `None` when stopped or on a raw stream.
    pub current: Option<SongId>,
    /// The monotonic base (unused by the rules grammar today; carried for parity
    /// with the P2 clock discipline so a future translator never invents a clock).
    pub now: tokio::time::Instant,
    /// The civil (wall-clock) instant, for `WallClock` resolution in the rules.
    pub now_civil: chrono::DateTime<chrono::Utc>,
    /// The daemon-configured local zone offset: "at 7" -> next local 07:00 -> UTC.
    ///
    /// ADAPTATION: a fixed offset (from `chrono`, already a dep) rather than a
    /// `chrono_tz::Tz`, so the default build pulls no extra crate and stays
    /// offline-buildable. A fixed offset is exactly what the wake civil->UTC
    /// reduction needs; a full IANA zone (DST transitions) is a P4 refinement.
    pub tz: chrono::FixedOffset,
    /// The live queue length, for "the last track" and bounds echoes.
    pub queue_len: usize,
}

/// A cheap trust signal surfaced in the echo ("via rules" / "via local model").
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum NlSource {
    Rules,
    Llm,
}

/// A successful translation. `> 1` plan is an ORDERED same-instant batch (e.g. a
/// wake: enqueue then fade-in sharing one resolved deadline), armed in order.
#[derive(Clone, Debug)]
pub struct NlHit {
    pub plans: Vec<RawPlan>,
    pub source: NlSource,
}

/// Why a translation failed. Distinguishes a fall-through miss from a real,
/// specific fail so the hybrid orchestrator knows whether to try the next stage.
#[derive(Clone, Debug, thiserror::Error, PartialEq)]
pub enum NlError {
    /// Not recognized at all: fall through Rules -> Llm -> loud ACK.
    #[error("not understood")]
    NotUnderstood,
    /// Parsed but under-determined; a real, specific fail (do NOT fall through).
    #[error("ambiguous: {0}")]
    Ambiguous(String),
    /// Parsed but nothing to act on (e.g. "more like this" with nothing playing).
    #[error("nothing to act on: {0}")]
    Unresolvable(String),
    /// No translator injected / model file missing.
    #[error("nl translator not available")]
    NotAvailable,
}

/// The injected NL seam. SYNC + `Send + Sync` so the daemon can run it under
/// `spawn_blocking` (a local model can take hundreds of ms): hypodj-core needs no
/// model dep and no `std` Mutex is ever held across an await.
pub trait Translator: Send + Sync {
    fn translate(&self, utterance: &str, ctx: &NlContext) -> Result<NlHit, NlError>;
}
