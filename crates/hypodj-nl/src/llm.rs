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

use hypodj_core::plan::{Action, FadeIntentIr, PosBase, RawPlan, RawTrigger, Selector, TrackSel};

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

/// The fade directions the model may emit: `Out`/`In` ONLY. The deliberate-cue
/// (`To`), wind-down (`ToFloor`) and alarm-wake (`WakeTo`) intents are OFF the model
/// surface - a model must never drive those side-effecting ramps. Each carries a
/// NAMED `secs` field so it round-trips through serde (an internally-tagged variant
/// with a struct body, not a primitive newtype).
#[derive(Clone, Debug, Deserialize)]
#[cfg_attr(feature = "llm", derive(schemars::JsonSchema))]
#[serde(tag = "dir", rename_all = "snake_case")]
pub enum LlmFade {
    Out { secs: f64 },
    In { secs: f64 },
}

impl From<LlmFade> for FadeIntentIr {
    fn from(f: LlmFade) -> Self {
        match f {
            LlmFade::Out { secs } => FadeIntentIr::Out { secs },
            LlmFade::In { secs } => FadeIntentIr::In { secs },
        }
    }
}

/// The content selectors the model may emit for an `Enqueue`: free-text `query`,
/// a closed-lexicon `genre`, or `radio`. The identity-bearing selectors
/// (`Exact`/`Similar`/`Calmer`, which carry a `SongId`) stay off the model surface -
/// the model never touches a library id. Each string payload rides in a NAMED field
/// (`q` / `name`) so serde round-trips it (an internally-tagged newtype wrapping a
/// bare `String` will NOT (de)serialize).
#[derive(Clone, Debug, Deserialize)]
#[cfg_attr(feature = "llm", derive(schemars::JsonSchema))]
#[serde(tag = "select", rename_all = "snake_case")]
pub enum LlmSelector {
    Query { q: String },
    Genre { name: String },
    Radio,
}

impl From<LlmSelector> for Selector {
    fn from(s: LlmSelector) -> Self {
        match s {
            LlmSelector::Query { q } => Selector::Query(q),
            LlmSelector::Genre { name } => Selector::Genre(name),
            LlmSelector::Radio => Selector::Radio,
        }
    }
}

/// The action subset the model may emit - the DOCUMENTED trust surface, mirroring
/// only the GBNF-advertised kinds. It is a dedicated type (NOT the full
/// [`Action`]), so the parser can NEVER accept an off-surface action (`Wake`, or a
/// `WakeTo`/`ToFloor` fade) even if a backend ignores the grammar. `SetVolume`
/// carries a NAMED `level` field so it round-trips (an internally-tagged newtype
/// wrapping a bare `u8` will NOT deserialize).
#[derive(Clone, Debug, Deserialize)]
#[cfg_attr(feature = "llm", derive(schemars::JsonSchema))]
#[serde(tag = "act", rename_all = "snake_case")]
pub enum LlmAction {
    Fade(LlmFade),
    Stop,
    Pause,
    SetVolume { level: u8 },
    Enqueue { selector: LlmSelector, count: u32 },
}

impl From<LlmAction> for Action {
    fn from(a: LlmAction) -> Self {
        match a {
            LlmAction::Fade(f) => Action::Fade(f.into()),
            LlmAction::Stop => Action::Stop,
            LlmAction::Pause => Action::Pause,
            LlmAction::SetVolume { level } => Action::SetVolume(level),
            LlmAction::Enqueue { selector, count } => {
                Action::Enqueue { selector: selector.into(), count }
            }
        }
    }
}

/// The restricted plan the model may emit (the constrained-decode target). Note
/// `origin` is ABSENT from the surface: the adapter stamps it, never the model.
/// `action` is the dedicated [`LlmAction`] surface, so an off-surface action can
/// never deserialize even if a backend ignores the GBNF (F7).
#[derive(Clone, Debug, Deserialize)]
#[cfg_attr(feature = "llm", derive(schemars::JsonSchema))]
pub struct LlmRawPlan {
    pub trigger: LlmTrigger,
    pub action: LlmAction,
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
            action: p.action.into(),
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

#[cfg(test)]
mod tests {
    use super::*;
    use hypodj_core::plan::{Action, FadeIntentIr, Selector};

    // F4: EVERY action the GBNF advertises must round-trip through parse_llm_output.
    // enqueue(query/genre/radio) + set_volume were previously REJECTED (serde cannot
    // (de)serialize an internally-tagged newtype wrapping a bare String/u8); the
    // named-field surface fixes that.
    #[test]
    fn parse_round_trips_every_action_kind() {
        // fade out
        let p = parse_llm_output(
            r#"{"trigger":{"kind":"span_elapsed","secs":300.0},"action":{"act":"fade","dir":"out","secs":10.0}}"#,
        )
        .unwrap();
        assert!(matches!(p.action, Action::Fade(FadeIntentIr::Out { .. })));

        // fade in
        let p = parse_llm_output(
            r#"{"trigger":{"kind":"span_elapsed","secs":300.0},"action":{"act":"fade","dir":"in","secs":10.0}}"#,
        )
        .unwrap();
        assert!(matches!(p.action, Action::Fade(FadeIntentIr::In { .. })));

        // stop / pause
        let p = parse_llm_output(
            r#"{"trigger":{"kind":"track_after_current"},"action":{"act":"stop"}}"#,
        )
        .unwrap();
        assert!(matches!(p.action, Action::Stop));
        let p = parse_llm_output(
            r#"{"trigger":{"kind":"track_after_current"},"action":{"act":"pause"}}"#,
        )
        .unwrap();
        assert!(matches!(p.action, Action::Pause));

        // set_volume(level) - previously un-parseable.
        let p = parse_llm_output(
            r#"{"trigger":{"kind":"track_after_current"},"action":{"act":"set_volume","level":42}}"#,
        )
        .unwrap();
        assert!(matches!(p.action, Action::SetVolume(42)));

        // enqueue(query) - previously un-parseable.
        let p = parse_llm_output(
            r#"{"trigger":{"kind":"track_after_current"},"action":{"act":"enqueue","selector":{"select":"query","q":"bon iver"},"count":5}}"#,
        )
        .unwrap();
        match p.action {
            Action::Enqueue { selector: Selector::Query(q), count } => {
                assert_eq!(q, "bon iver");
                assert_eq!(count, 5);
            }
            other => panic!("expected enqueue(query), got {other:?}"),
        }

        // enqueue(genre) - previously un-parseable.
        let p = parse_llm_output(
            r#"{"trigger":{"kind":"track_after_current"},"action":{"act":"enqueue","selector":{"select":"genre","name":"jazz"},"count":3}}"#,
        )
        .unwrap();
        match p.action {
            Action::Enqueue { selector: Selector::Genre(g), count } => {
                assert_eq!(g, "jazz");
                assert_eq!(count, 3);
            }
            other => panic!("expected enqueue(genre), got {other:?}"),
        }

        // enqueue(radio)
        let p = parse_llm_output(
            r#"{"trigger":{"kind":"track_after_current"},"action":{"act":"enqueue","selector":{"select":"radio"},"count":5}}"#,
        )
        .unwrap();
        assert!(matches!(
            p.action,
            Action::Enqueue { selector: Selector::Radio, .. }
        ));
    }

    // F7: an OFF-surface action must be REJECTED even if a backend ignores the
    // grammar. `wake`, and the `wake_to`/`to_floor` fade dirs, are not in LlmAction /
    // LlmFade, so serde fails loud instead of arming an off-surface effect.
    #[test]
    fn parse_rejects_off_surface_actions() {
        // Action::Wake is off-surface.
        assert!(parse_llm_output(
            r#"{"trigger":{"kind":"track_after_current"},"action":{"act":"wake","count":5}}"#,
        )
        .is_err());
        // A wake_to fade is off-surface.
        assert!(parse_llm_output(
            r#"{"trigger":{"kind":"track_after_current"},"action":{"act":"fade","dir":"wake_to","target_db":-10.0,"vol":50,"secs":10.0}}"#,
        )
        .is_err());
        // A to_floor fade is off-surface.
        assert!(parse_llm_output(
            r#"{"trigger":{"kind":"track_after_current"},"action":{"act":"fade","dir":"to_floor","secs":10.0}}"#,
        )
        .is_err());
        // A wall_clock trigger is off the model surface too.
        assert!(parse_llm_output(
            r#"{"trigger":{"kind":"wall_clock","at":"2026-01-01T00:00:00Z"},"action":{"act":"stop"}}"#,
        )
        .is_err());
    }
}
