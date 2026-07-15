//! The OPTIONAL local-model path.
//!
//! The RESTRICTED model surface ([`LlmRawPlan`]) is a subset of the P2 IR with
//! WallClock and Immediate REMOVED (a hallucinated wall-clock or synthetic
//! add-time edge is off the model surface; those stay Rules-only). The subset
//! deserializer + the [`From`] conversion + [`parse_llm_output`] are ALWAYS
//! compiled and model-free (unit-testable with a canned constrained-JSON string,
//! no model, no network).
//!
//! Only the constrained-decode BACKEND ([`LlmBackend`]/[`LlmTranslator`]) and the
//! GBNF derivation from the JSON-Schema are gated behind `feature = "llm"`.

use serde::Deserialize;

use hypodj_core::plan::{Action, PosBase, RawPlan, RawTrigger, TrackSel};

/// The trigger subset the model may emit: RawTrigger MINUS WallClock and
/// Immediate. No `DateTime` target, no synthetic add-time edge on the model
/// surface. Wall-clock and immediate intents stay Rules-only.
#[derive(Clone, Debug, Deserialize)]
#[cfg_attr(feature = "llm", derive(schemars::JsonSchema))]
#[serde(tag = "kind", rename_all = "snake_case")]
pub enum LlmTrigger {
    QueuePosition { n: usize, base: PosBase },
    TrackAfterCurrent,
    TimeRemaining { track: TrackSel, secs: f64 },
    AlbumBoundary { track: TrackSel },
    SpanElapsed { secs: f64 },
}

impl From<LlmTrigger> for RawTrigger {
    fn from(t: LlmTrigger) -> Self {
        match t {
            LlmTrigger::QueuePosition { n, base } => RawTrigger::QueuePosition { n, base },
            LlmTrigger::TrackAfterCurrent => RawTrigger::TrackAfterCurrent,
            LlmTrigger::TimeRemaining { track, secs } => RawTrigger::TimeRemaining { track, secs },
            LlmTrigger::AlbumBoundary { track } => RawTrigger::AlbumBoundary { track },
            LlmTrigger::SpanElapsed { secs } => RawTrigger::SpanElapsed { secs },
        }
    }
}

/// The restricted plan the model may emit (the constrained-decode target). Note
/// `origin` is ABSENT from the surface: the adapter stamps it, never the model.
#[derive(Clone, Debug, Deserialize)]
#[cfg_attr(feature = "llm", derive(schemars::JsonSchema))]
pub struct LlmRawPlan {
    pub trigger: LlmTrigger,
    pub action: Action,
    #[serde(default)]
    pub once: bool,
}

impl From<LlmRawPlan> for RawPlan {
    /// Stamp `version = 1` and leave `origin` empty (the adapter fills
    /// `nl:llm:<model>`, never the model).
    fn from(p: LlmRawPlan) -> Self {
        RawPlan {
            version: 1,
            trigger: p.trigger.into(),
            action: p.action,
            once: p.once,
            origin: String::new(),
        }
    }
}

/// Parse one constrained-JSON string (exactly what a GBNF-constrained decode
/// yields) into a [`RawPlan`]. Model-free + always compiled. A grammar can only
/// produce a valid subset object, but we still parse defensively and surface a
/// readable error rather than panicking.
pub fn parse_llm_output(json: &str) -> Result<RawPlan, String> {
    let plan: LlmRawPlan = serde_json::from_str(json.trim()).map_err(|e| e.to_string())?;
    Ok(plan.into())
}

// ── constrained-decode backend (feature = "llm") ─────────────────────────────

#[cfg(feature = "llm")]
mod backend {
    use hypodj_core::nl::{NlContext, NlError, NlHit, NlSource, Translator};

    /// A local constrained-decode backend. Constrains generation against a GBNF
    /// and returns the raw JSON string the grammar produced.
    pub trait LlmBackend: Send + Sync {
        fn generate(&self, prompt: &str, gbnf: &str) -> Result<String, String>;
    }

    /// The model translator: build a prompt, constrained-decode against the
    /// IR-derived GBNF, parse the output into a [`RawPlan`].
    pub struct LlmTranslator<B: LlmBackend> {
        backend: B,
        gbnf: &'static str,
    }

    impl<B: LlmBackend> LlmTranslator<B> {
        pub fn new(backend: B) -> Self {
            Self { backend, gbnf: crate::gbnf::GBNF }
        }

        fn prompt(utterance: &str, ctx: &NlContext) -> String {
            format!(
                "Translate the DJ request into ONE JSON plan matching the grammar.\n\
                 Queue length: {}. Something is {}playing.\n\
                 Request: {}\nJSON:",
                ctx.queue_len,
                if ctx.current.is_some() { "" } else { "NOT " },
                utterance,
            )
        }
    }

    impl<B: LlmBackend> Translator for LlmTranslator<B> {
        fn translate(&self, utterance: &str, ctx: &NlContext) -> Result<NlHit, NlError> {
            let prompt = Self::prompt(utterance, ctx);
            let out = self
                .backend
                .generate(&prompt, self.gbnf)
                .map_err(|_| NlError::NotUnderstood)?;
            let raw = super::parse_llm_output(&out).map_err(|_| NlError::NotUnderstood)?;
            Ok(NlHit { plans: vec![raw], source: NlSource::Llm })
        }
    }
}

#[cfg(feature = "llm")]
pub use backend::{LlmBackend, LlmTranslator};
