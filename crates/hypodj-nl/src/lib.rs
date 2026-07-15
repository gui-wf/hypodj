//! hypodj-nl: the OPTIONAL natural-language -> validated P2 Plan IR translator.
//!
//! OFF THE TRUSTED PATH. This crate only ever EMITS a [`hypodj_core::plan::RawPlan`];
//! it cannot arm. Every plan it produces crosses the existing P2
//! [`hypodj_core::plan::validate`] + a human echo + an explicit confirm before the
//! handler's `plan_add`. The model never touches a QueueId/AlbumId/Instant - those
//! stay symbolic in the raw trigger and are resolved by `validate()` at confirm
//! time against the LIVE snapshot.
//!
//! Layering: the [`hypodj_core::nl::Translator`] SEAM (trait + context + error)
//! lives in hypodj-core so the core keeps ZERO model deps; this crate implements
//! it. The deterministic [`RulesTranslator`] + the [`HybridTranslator`] plumbing +
//! the LlmRawPlan <-> RawPlan conversion + the output parse are ALL model-free and
//! always compiled. Only the constrained-decode BACKEND is behind `feature = "llm"`.

#[cfg(feature = "llm")]
pub mod gbnf;
#[cfg(feature = "llm-llama")]
pub mod llama;
pub mod llm;
pub mod rules;

// Echo rendering lives in hypodj-core (the handler renders the echo there and core
// cannot depend on this crate); re-exported for the round-trip test + callers.
pub use hypodj_core::echo::{describe_batch, describe_plan, render_dsl};
pub use llm::{parse_llm_output, LlmRawPlan, LlmTrigger};
pub use rules::RulesTranslator;

#[cfg(feature = "llm")]
pub use llm::{LlmBackend, LlmTranslator};

use hypodj_core::nl::{NlContext, NlError, NlHit, Translator};

/// Tries the deterministic RULES fast-path first; on a plain NotUnderstood miss
/// (and only then) falls through to the optional local model. Ambiguous /
/// Unresolvable are REAL, specific fails and propagate as-is (never fall through -
/// they mean the rules understood the shape but the request is under-determined /
/// unactionable). An Ok result is returned directly.
pub struct HybridTranslator {
    rules: RulesTranslator,
    /// `Some` only under `feature = "llm"` with a loaded model; otherwise `None`,
    /// so the crate degrades to Rules-only + a loud NotUnderstood (the offline /
    /// optional north star).
    llm: Option<Box<dyn Translator>>,
}

impl HybridTranslator {
    /// Build with an optional model translator (the daemon passes `None` when the
    /// feature is off or no model file is present).
    pub fn new(llm: Option<Box<dyn Translator>>) -> Self {
        Self { rules: RulesTranslator, llm }
    }

    /// The deterministic, model-free translator (the default the daemon wires when
    /// no model is configured).
    pub fn rules_only() -> Self {
        Self::new(None)
    }
}

impl Translator for HybridTranslator {
    fn translate(&self, utterance: &str, ctx: &NlContext) -> Result<NlHit, NlError> {
        match self.rules.translate(utterance, ctx) {
            Err(NlError::NotUnderstood) => match &self.llm {
                Some(l) => l.translate(utterance, ctx),
                None => Err(NlError::NotUnderstood),
            },
            other => other,
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use chrono::{FixedOffset, TimeZone, Utc};
    use hypodj_core::model::SongId;
    use hypodj_core::nl::{NlSource, Translator};
    use hypodj_core::plan::{
        Action, FadeIntentIr, PosBase, RawPlan, RawTrigger, Selector, TrackSel,
    };

    /// A fixed civil clock (2026-07-15 05:30 UTC) + a UTC-relative +01:00 zone, so
    /// every wake / wall-clock assertion is deterministic (never wall-clock).
    fn ctx(current: Option<&str>, queue_len: usize) -> NlContext {
        NlContext {
            current: current.map(|s| SongId(s.into())),
            now: tokio::time::Instant::now(),
            now_civil: Utc.with_ymd_and_hms(2026, 7, 15, 5, 30, 0).unwrap(),
            tz: FixedOffset::east_opt(0).unwrap(), // UTC for simple local==UTC math
            queue_len,
        }
    }

    fn tr(u: &str, c: &NlContext) -> Result<Vec<RawPlan>, NlError> {
        rules::translate_rules(u, c)
    }

    fn j(p: &RawPlan) -> serde_json::Value {
        serde_json::to_value(p).unwrap()
    }

    fn raw(trigger: RawTrigger, action: Action) -> RawPlan {
        RawPlan { version: 1, trigger, action, once: true, origin: String::new() }
    }

    // ── CORPUS-AS-TABLE: every phrasing -> exact RawPlan(s) or exact NlError ──

    #[test]
    fn corpus_fade_immediate_and_trigger_gate() {
        let c = ctx(Some("s1"), 5);
        // Bare fade out -> Immediate + Fade(Out{default}).
        assert_eq!(
            j(&tr("fade out", &c).unwrap()[0]),
            j(&raw(RawTrigger::Immediate, Action::Fade(FadeIntentIr::Out { secs: 30.0 })))
        );
        // "fade in" -> Immediate + Fade(In{default}).
        assert_eq!(
            j(&tr("fade in", &c).unwrap()[0]),
            j(&raw(RawTrigger::Immediate, Action::Fade(FadeIntentIr::In { secs: 30.0 })))
        );
        // DISAMBIGUATION: "in <dur>" binds the TRIGGER (fade keeps its default).
        assert_eq!(
            j(&tr("fade out in 20 minutes", &c).unwrap()[0]),
            j(&raw(
                RawTrigger::SpanElapsed { secs: 1200.0 },
                Action::Fade(FadeIntentIr::Out { secs: 30.0 })
            ))
        );
        // DISAMBIGUATION: "over <dur>" binds the FADE secs (trigger stays Immediate).
        assert_eq!(
            j(&tr("fade out over 20 minutes", &c).unwrap()[0]),
            j(&raw(RawTrigger::Immediate, Action::Fade(FadeIntentIr::Out { secs: 1200.0 })))
        );
        // A bare trailing duration after "fade out" binds the FADE secs.
        assert_eq!(
            j(&tr("fade out 20s", &c).unwrap()[0]),
            j(&raw(RawTrigger::Immediate, Action::Fade(FadeIntentIr::Out { secs: 20.0 })))
        );
    }

    #[test]
    fn corpus_fade_track_base_default() {
        let c = ctx(Some("s1"), 5);
        // A bare "the 3rd track" -> Absolute (matches how the queue is displayed).
        assert_eq!(
            j(&tr("fade the 3rd track", &c).unwrap()[0]),
            j(&raw(
                RawTrigger::QueuePosition { n: 3, base: PosBase::Absolute },
                Action::Fade(FadeIntentIr::Out { secs: 30.0 })
            ))
        );
        // "counting current as 1st" -> CurrentIsOne. This is the worked example
        // and MUST equal the P2 fixture RawPlan (QueuePosition{3,CurrentIsOne} +
        // Fade(Out{30})).
        assert_eq!(
            j(&tr("fade the 3rd track counting current as 1st", &c).unwrap()[0]),
            j(&raw(
                RawTrigger::QueuePosition { n: 3, base: PosBase::CurrentIsOne },
                Action::Fade(FadeIntentIr::Out { secs: 30.0 })
            ))
        );
        // Word ordinal + "song" works too.
        assert_eq!(
            j(&tr("fade the fifth song", &c).unwrap()[0]),
            j(&raw(
                RawTrigger::QueuePosition { n: 5, base: PosBase::Absolute },
                Action::Fade(FadeIntentIr::Out { secs: 30.0 })
            ))
        );
        // Outside the closed ordinal set -> NotUnderstood (adapter/model may punt).
        assert_eq!(tr("fade the last track", &c).unwrap_err(), NlError::NotUnderstood);
        assert_eq!(tr("fade the twenty-third track", &c).unwrap_err(), NlError::NotUnderstood);
    }

    #[test]
    fn corpus_stop_pause_and_boundary() {
        let c = ctx(Some("s1"), 5);
        assert_eq!(
            j(&tr("stop", &c).unwrap()[0]),
            j(&raw(RawTrigger::Immediate, Action::Stop))
        );
        assert_eq!(
            j(&tr("pause", &c).unwrap()[0]),
            j(&raw(RawTrigger::Immediate, Action::Pause))
        );
        // "after this album" -> AlbumBoundary{Current}.
        assert_eq!(
            j(&tr("stop after this album", &c).unwrap()[0]),
            j(&raw(
                RawTrigger::AlbumBoundary { track: TrackSel::Current },
                Action::Stop
            ))
        );
        // "after this track" / bare "after this" -> TrackAfterCurrent (a missed
        // "album" keyword must NOT downgrade silently).
        assert_eq!(
            j(&tr("stop after this track", &c).unwrap()[0]),
            j(&raw(RawTrigger::TrackAfterCurrent, Action::Stop))
        );
        assert_eq!(
            j(&tr("stop after this", &c).unwrap()[0]),
            j(&raw(RawTrigger::TrackAfterCurrent, Action::Stop))
        );
    }

    #[test]
    fn unparseable_in_or_at_tail_punts_never_immediate() {
        let c = ctx(Some("s1"), 5);
        // A misheard "in <x>" tail must PUNT (NotUnderstood), NEVER silently
        // downgrade to an Immediate stop.
        assert_eq!(tr("stop in a bit", &c).unwrap_err(), NlError::NotUnderstood);
        // A misheard "at <x>" tail likewise.
        assert_eq!(tr("stop at noonish", &c).unwrap_err(), NlError::NotUnderstood);
        assert_eq!(tr("pause at whenever", &c).unwrap_err(), NlError::NotUnderstood);
        // Sanity: a plain command with no in/at tail is still Immediate, and a
        // WELL-FORMED tail still parses.
        assert_eq!(
            j(&tr("stop", &c).unwrap()[0]),
            j(&raw(RawTrigger::Immediate, Action::Stop))
        );
        assert_eq!(
            j(&tr("stop in 10 minutes", &c).unwrap()[0]),
            j(&raw(RawTrigger::SpanElapsed { secs: 600.0 }, Action::Stop))
        );
    }

    #[test]
    fn unresolved_deferral_punts_never_stops_now() {
        let c = ctx(Some("s1"), 5);
        // A deferral conjunction we could not parse into a known boundary is a
        // deferred intent - PUNT, never a confident Immediate stop-now.
        for u in [
            "stop after the album ends",
            "stop when this song finishes",
            "pause once the track is over",
        ] {
            assert_eq!(tr(u, &c).unwrap_err(), NlError::NotUnderstood, "{u}");
        }
        // Filler after the verb is still Immediate.
        assert_eq!(
            j(&tr("stop now", &c).unwrap()[0]),
            j(&raw(RawTrigger::Immediate, Action::Stop))
        );
    }

    #[test]
    fn with_selector_handles_multibyte_before_with() {
        let c = ctx(Some("s1"), 5);
        // A multibyte char before " with " that changes byte length under
        // lowercasing must NOT garble the selector or slice mid-codepoint (panic).
        // The genre is still resolved correctly from the ORIGINAL text.
        match &tr("wake me at 7 \u{0130} with jazz", &c).unwrap()[0].action {
            Action::Enqueue { selector, .. } => {
                assert_eq!(format!("{selector:?}"), format!("{:?}", Selector::Genre("jazz".into())));
            }
            other => panic!("expected Enqueue, got {other:?}"),
        }
    }

    #[test]
    fn corpus_with_selector_class() {
        let c = ctx(Some("s1"), 5);
        // "with jazz" (in the genre lexicon) -> Genre; via a wake so "with" is set.
        match &tr("wake me at 7 with jazz", &c).unwrap()[0].action {
            Action::Enqueue { selector: Selector::Genre(g), .. } => assert_eq!(g, "jazz"),
            other => panic!("expected Genre, got {other:?}"),
        }
        // "with Bon Iver" (not a genre) -> Query, preserving the original case.
        match &tr("wake me at 7 with Bon Iver", &c).unwrap()[0].action {
            Action::Enqueue { selector: Selector::Query(q), .. } => assert_eq!(q, "Bon Iver"),
            other => panic!("expected Query, got {other:?}"),
        }
    }

    #[test]
    fn corpus_calmer_similar_and_unresolvable() {
        let c = ctx(Some("song-42"), 5);
        // "something calmer" -> Enqueue{Calmer(current)}.
        match &tr("play something calmer", &c).unwrap()[0].action {
            Action::Enqueue { selector: Selector::Calmer(id), .. } => {
                assert_eq!(id, &SongId("song-42".into()))
            }
            other => panic!("got {other:?}"),
        }
        // "more like this" -> Enqueue{Similar(current)}.
        match &tr("more like this", &c).unwrap()[0].action {
            Action::Enqueue { selector: Selector::Similar(id), .. } => {
                assert_eq!(id, &SongId("song-42".into()))
            }
            other => panic!("got {other:?}"),
        }
        // Nothing playing -> Unresolvable (NOT NotUnderstood, NOT a degenerate plan).
        let stopped = ctx(None, 0);
        assert_eq!(
            tr("more like this", &stopped).unwrap_err(),
            NlError::Unresolvable("nothing playing to match".into())
        );
    }

    #[test]
    fn corpus_wake_is_two_plans_sharing_one_instant() {
        let c = ctx(Some("s1"), 5);
        let plans = tr("wake me at 7 with jazz", &c).unwrap();
        assert_eq!(plans.len(), 2, "wake is an ordered pair");
        // Both share ONE resolved WallClock instant.
        let (a1, a2) = match (&plans[0].trigger, &plans[1].trigger) {
            (RawTrigger::WallClock { at: a }, RawTrigger::WallClock { at: b }) => (*a, *b),
            other => panic!("expected two WallClock triggers, got {other:?}"),
        };
        assert_eq!(a1, a2, "same deadline");
        // now_civil is 05:30 UTC; "7" (morning) is 07:00 UTC today (tz = UTC).
        assert_eq!(a1, Utc.with_ymd_and_hms(2026, 7, 15, 7, 0, 0).unwrap());
        // Enqueue BEFORE Fade(In) (insertion order).
        assert!(matches!(plans[0].action, Action::Enqueue { .. }));
        assert!(matches!(plans[1].action, Action::Fade(FadeIntentIr::In { .. })));
    }

    #[test]
    fn corpus_wake_pm_meridian_rolls_to_next_day() {
        let c = ctx(Some("s1"), 5);
        // "at 7 tonight" forces PM -> 19:00 today.
        let plans = tr("wake me at 7 tonight with jazz", &c).unwrap();
        match plans[0].trigger {
            RawTrigger::WallClock { at } => {
                assert_eq!(at, Utc.with_ymd_and_hms(2026, 7, 15, 19, 0, 0).unwrap())
            }
            ref other => panic!("got {other:?}"),
        }
        // now is 05:30; "at 5" (morning) has already passed -> tomorrow 05:00.
        let plans = tr("wake me at 5 with jazz", &c).unwrap();
        match plans[0].trigger {
            RawTrigger::WallClock { at } => {
                assert_eq!(at, Utc.with_ymd_and_hms(2026, 7, 16, 5, 0, 0).unwrap())
            }
            ref other => panic!("got {other:?}"),
        }
    }

    #[test]
    fn empty_and_unknown_fall_through() {
        let c = ctx(Some("s1"), 5);
        assert_eq!(tr("", &c).unwrap_err(), NlError::NotUnderstood);
        assert_eq!(tr("do a barrel roll", &c).unwrap_err(), NlError::NotUnderstood);
    }

    // ── HYBRID orchestration ──────────────────────────────────────────────────

    struct StubLlm;
    impl Translator for StubLlm {
        fn translate(&self, _u: &str, _c: &NlContext) -> Result<NlHit, NlError> {
            Ok(NlHit {
                plans: vec![raw(RawTrigger::Immediate, Action::Stop)],
                source: NlSource::Llm,
            })
        }
    }

    #[test]
    fn hybrid_falls_through_only_on_not_understood() {
        let c = ctx(Some("s1"), 5);
        // Rules-only: an unknown utterance stays NotUnderstood.
        let rules_only = HybridTranslator::rules_only();
        assert_eq!(
            rules_only.translate("do a barrel roll", &c).unwrap_err(),
            NlError::NotUnderstood
        );
        // With an LLM: NotUnderstood falls through to the model.
        let hybrid = HybridTranslator::new(Some(Box::new(StubLlm)));
        let hit = hybrid.translate("do a barrel roll", &c).unwrap();
        assert_eq!(hit.source, NlSource::Llm);
        // But Unresolvable is a real fail and does NOT fall through to the model.
        let stopped = ctx(None, 0);
        assert_eq!(
            hybrid.translate("more like this", &stopped).unwrap_err(),
            NlError::Unresolvable("nothing playing to match".into())
        );
        // And a rules HIT is returned without consulting the model.
        let hit = hybrid.translate("stop", &c).unwrap();
        assert_eq!(hit.source, NlSource::Rules);
    }

    // ── ECHO round-trip: render DSL -> re-parse via parse_plan -> same IR ──────

    #[test]
    fn echo_round_trips_through_plan_dsl() {
        use hypodj_core::mpd::{parse, MpdCommand, PlanCmd};
        let c = ctx(Some("s1"), 5);
        for utterance in [
            "fade the 3rd track counting current as 1st",
            "fade out over 20 minutes",
            "fade out in 20 minutes",
            "stop after this album",
            "stop after this track",
        ] {
            let plans = tr(utterance, &c).unwrap();
            for p in &plans {
                let dsl = render_dsl(p).expect("rules plans are DSL-expressible");
                match parse(&format!("plan add {dsl}")) {
                    MpdCommand::Plan(PlanCmd::Add(reparsed)) => {
                        // The re-parsed plan carries origin "mpd"; compare the
                        // trigger + action + once only (origin is adapter-stamped).
                        assert_eq!(
                            serde_json::to_value(&reparsed.trigger).unwrap(),
                            serde_json::to_value(&p.trigger).unwrap(),
                            "trigger round-trip for {utterance:?}"
                        );
                        assert_eq!(
                            serde_json::to_value(&reparsed.action).unwrap(),
                            serde_json::to_value(&p.action).unwrap(),
                            "action round-trip for {utterance:?}"
                        );
                        assert_eq!(reparsed.once, p.once);
                    }
                    other => panic!("echo did not re-parse for {utterance:?}: {other:?}"),
                }
            }
        }
    }

    // ── LLM subset parse WITHOUT a model (canned constrained JSON) ─────────────

    #[test]
    fn canned_constrained_json_parses_to_raw_plan() {
        // Exactly what a GBNF-constrained decode would emit for the worked example.
        let json = r#"{"trigger":{"kind":"queue_position","n":3,"base":"current_is_one"},"action":{"act":"fade","dir":"out","secs":30.0},"once":true}"#;
        let raw = parse_llm_output(json).unwrap();
        assert_eq!(
            serde_json::to_value(&raw.trigger).unwrap(),
            serde_json::to_value(&RawTrigger::QueuePosition {
                n: 3,
                base: PosBase::CurrentIsOne
            })
            .unwrap()
        );
        match raw.action {
            Action::Fade(FadeIntentIr::Out { secs }) => assert_eq!(secs, 30.0),
            other => panic!("got {other:?}"),
        }
        // The adapter (not the model) owns origin: the model surface leaves it "".
        assert_eq!(raw.origin, "");
        // A malformed / off-surface JSON fails loud, never panics.
        assert!(parse_llm_output("{not json").is_err());
    }

    // ── FEATURE guard: the daemon default build pulls no model runtime ─────────

    #[test]
    fn default_build_is_model_free() {
        // Assert `cargo tree` on the DEFAULT feature set of hypodj-daemon contains
        // no llama-cpp / candle / ort crate. Skips gracefully if cargo is absent.
        let manifest = env!("CARGO_MANIFEST_DIR");
        let ws_root = std::path::Path::new(manifest)
            .parent()
            .and_then(|p| p.parent())
            .expect("workspace root");
        let out = std::process::Command::new("cargo")
            .args(["tree", "-e", "normal", "-p", "hypodj-daemon"])
            .current_dir(ws_root)
            .output();
        let out = match out {
            Ok(o) if o.status.success() => o,
            _ => return, // cargo unavailable / offline resolve failed: skip.
        };
        let tree = String::from_utf8_lossy(&out.stdout).to_lowercase();
        for banned in ["llama-cpp", "candle-core", " ort ", "llama_cpp"] {
            assert!(
                !tree.contains(banned),
                "default daemon build must stay model-free, found {banned:?}"
            );
        }
    }
}
