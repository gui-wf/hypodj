//! The OPTIONAL model (Claude Code / local-model) path.
//!
//! The RESTRICTED model surface ([`LlmRawPlan`]) is the TRUST BOUNDARY: it matches
//! the shape the LIVE Claude 2.1.204 capture proves the CLI emits - a top-level
//! `{"actions":[ {type, ...flat scalars} ]}` of FLAT, internally-tagged objects on
//! a CLOSED string enum. Off-surface intents (an unknown action `type`, an
//! absolute wall-clock instant, an identity-bearing `SongId` selector, or a
//! `Wake`/`WakeTo`/`ToFloor` fade) are UNREPRESENTABLE in this DTO, so they cannot
//! be produced no matter what the backend emits. The DTO + the VALIDATING
//! conversion + [`parse_llm_output`] are ALWAYS compiled and model-free
//! (unit-testable with a canned JSON string, no model, no network).
//!
//! Only the constrained-decode BACKEND ([`LlmBackend`]/[`LlmTranslator`]) and the
//! GBNF derivation from the JSON-Schema are gated behind `feature = "llm"`.

use serde::Deserialize;

use hypodj_core::plan::{
    Action, ClearScope, FadeIntentIr, MoveDest, PosBase, QueueSelector, RawPlan, RawTrigger,
    Selector, TrackSel,
};

/// Closed action lexicon. fade_out/fade_in are SEPARATE variants (no nested dir
/// tag), so To/ToFloor/WakeTo/Wake are unrepresentable. An unknown string fails
/// serde -> rejected. Claude emits this under the key `type` (every live sample);
/// `action`/`act` are drift aliases.
#[derive(Clone, Copy, Debug, Deserialize)]
#[cfg_attr(feature = "llm", derive(schemars::JsonSchema))]
#[serde(rename_all = "snake_case")]
pub enum LlmActionKind {
    FadeOut,
    FadeIn,
    Stop,
    Pause,
    SetVolume,
    Enqueue,
    /// Remove queue entries a selector resolves (destructive; through the gate).
    Remove,
    /// Move selected entries to a destination.
    Move,
    /// Clear part or all of the queue (destructive; through the gate).
    Clear,
    /// Jump playback to the selected track.
    Play,
    /// Resolve a LIBRARY song (query/genre/radio), enqueue it, and START playback
    /// on it: the "play this specific song NOW" path (enqueue-then-start), distinct
    /// from the append-only Enqueue.
    PlayNow,
    /// Honest "no action": an off-topic / non-music / non-queue request. Prevents
    /// fabricating a wrong enqueue for a request that has no valid action.
    Noop,
}

/// Closed queue-selector lexicon (FLAT). An unknown string fails serde -> rejected.
#[derive(Clone, Copy, Debug, Deserialize)]
#[cfg_attr(feature = "llm", derive(schemars::JsonSchema))]
#[serde(rename_all = "snake_case")]
pub enum LlmQSel {
    Current,
    Position,
    Range,
    Query,
    Last,
    /// CONTENT selector (enqueue/play_now only): "more like what is playing".
    /// Carries NO id - the daemon fills the current-track seed server-side, so the
    /// off-surface-id boundary is untouched. Rejected for the queue-edit actions
    /// (remove/move/clear/play) by [`build_qsel`]: it is not a live-queue selector.
    SimilarToCurrent,
}

/// Closed move-destination lexicon (FLAT).
#[derive(Clone, Copy, Debug, Deserialize)]
#[cfg_attr(feature = "llm", derive(schemars::JsonSchema))]
#[serde(rename_all = "snake_case")]
pub enum LlmMoveDest {
    Position,
    Relative,
}

/// Closed clear-scope lexicon (FLAT).
#[derive(Clone, Copy, Debug, Deserialize)]
#[cfg_attr(feature = "llm", derive(schemars::JsonSchema))]
#[serde(rename_all = "snake_case")]
pub enum LlmClearScope {
    All,
    AfterCurrent,
    Range,
}

/// Closed trigger lexicon, FLAT. `Now` is the default (most DJ intents are
/// immediate). No absolute-clock variant and NO DateTime-typed field anywhere ->
/// an absolute civil instant is unrepresentable.
#[derive(Clone, Copy, Debug, Default, Deserialize)]
#[cfg_attr(feature = "llm", derive(schemars::JsonSchema))]
#[serde(rename_all = "snake_case")]
pub enum LlmWhen {
    #[default]
    Now,
    AfterCurrent,
    AfterSecs,
    AlbumBoundary,
    QueuePosition,
    TimeRemaining,
}

/// The FLAT model surface: one object, a required closed discriminator plus
/// OPTIONAL scalars. deny_unknown_fields is deliberately OFF (Claude's payload
/// key names drift run-to-run); load-bearing fields carry aliases for the names
/// observed live, stray keys are ignored. Safety does NOT rest on rejecting
/// stray keys - it rests on the closed action/when enums + presence-only selector
/// inference, which make off-surface intents unrepresentable regardless of noise.
#[derive(Clone, Debug, Deserialize)]
#[cfg_attr(feature = "llm", derive(schemars::JsonSchema))]
pub struct LlmRawPlan {
    /// REQUIRED closed discriminator. PRIMARY key is `type` (Claude's instinct).
    #[serde(rename = "type", alias = "action", alias = "act")]
    pub kind: LlmActionKind,

    /// Flat trigger; DEFAULTS to Now when Claude omits it - the direct fix for the
    /// omnipresent live "missing field `trigger`".
    #[serde(default, alias = "trigger")]
    pub when: LlmWhen,

    // -- action scalars (all optional; cross-checked in TryFrom) --
    #[serde(default, alias = "seconds", alias = "duration")]
    pub secs: Option<f64>,
    #[serde(default, alias = "volume", alias = "vol")]
    pub level: Option<u8>,
    #[serde(default, alias = "limit", alias = "n", alias = "amount")]
    pub count: Option<u32>,

    // -- enqueue selector: inferred by PRESENCE. No source enum, no id field ->
    //    only Query/Genre/Radio are reachable; Exact/Similar/Calmer (SongId) have
    //    nowhere to land. --
    #[serde(default, alias = "q", alias = "value")]
    pub query: Option<String>,
    #[serde(default, alias = "name")]
    pub genre: Option<String>,
    #[serde(default)]
    pub radio: bool,

    // -- `when` scalars --
    #[serde(default, alias = "seconds_remaining")]
    pub when_secs: Option<f64>,
    /// 1-based absolute queue slot for when = queue_position. NOT named `position`:
    /// Claude uses `position` for STRING hints ("end") that we must IGNORE, and a
    /// usize-typed `position` field would hard-fail deserialization on that string.
    #[serde(default, alias = "queue_slot")]
    pub slot: Option<usize>,

    // -- queue-edit scalars (Remove/Move/Clear/Play). The selector reuses `slot`
    //    (position), `count` (last-n), `query` (query match); range + dest + scope
    //    carry their own flat fields. All closed enums, so nothing off-surface. --
    #[serde(default)]
    pub sel: Option<LlmQSel>,
    #[serde(default, alias = "range_from")]
    pub range_start: Option<usize>,
    #[serde(default, alias = "range_to")]
    pub range_end: Option<usize>,
    #[serde(default)]
    pub dest: Option<LlmMoveDest>,
    #[serde(default)]
    pub dest_slot: Option<usize>,
    #[serde(default)]
    pub dest_rel: Option<i32>,
    #[serde(default)]
    pub scope: Option<LlmClearScope>,

    #[serde(default)]
    pub once: bool,
}

/// Build a [`QueueSelector`] from the flat selector fields. Closed `sel` lexicon +
/// presence-checked scalars, so nothing off-surface is constructible.
fn build_qsel(p: &LlmRawPlan) -> Result<QueueSelector, String> {
    match p.sel.ok_or("queue action needs a `sel`")? {
        LlmQSel::Current => Ok(QueueSelector::Current),
        LlmQSel::Position => Ok(QueueSelector::Position(p.slot.ok_or("sel=position needs slot")?)),
        LlmQSel::Last => Ok(QueueSelector::Last(p.count.ok_or("sel=last needs count")? as usize)),
        LlmQSel::Range => Ok(QueueSelector::Range {
            start: p.range_start.ok_or("sel=range needs range_start")?,
            end: p.range_end.ok_or("sel=range needs range_end")?,
        }),
        LlmQSel::Query => Ok(QueueSelector::QueryMatch(
            p.query.clone().ok_or("sel=query needs query")?,
        )),
        // similar_to_current is a CONTENT selector (enqueue/play_now), never a
        // live-queue target - refuse it here so a remove/move/clear/play cannot
        // smuggle it in.
        LlmQSel::SimilarToCurrent => Err("similar_to_current is not a queue selector".into()),
    }
}

/// VALIDATING conversion (fallible: a flat bag needs cross-field checks). Every
/// candidate crosses here on the way to plan.rs validate/clamp. Nothing off-surface
/// is constructible (no Wake action, no To/ToFloor/WakeTo fade, no SongId-bearing
/// selector, no wall-clock). count is clamped to MAX_ENQUEUE downstream in
/// plan::validate; version=1 and origin="" (the adapter stamps nl:cc:<model>).
impl TryFrom<LlmRawPlan> for RawPlan {
    type Error = String;
    fn try_from(p: LlmRawPlan) -> Result<Self, String> {
        // plan.rs validate() clamps fade secs to [min_dur, max_dur], so any
        // default is safe; only supplied so an omitted duration still plans.
        const DEFAULT_FADE_SECS: f64 = 5.0;

        let trigger = match p.when {
            // Claude routinely omits `when` (-> Now) yet still expresses a delay
            // via when_secs/slot alone. Treating that as Immediate would silently
            // drop the delay and fire NOW, so infer the trigger from the timing
            // scalar rather than discarding it: a bare when_secs is a span, a bare
            // slot is a queue position. Only a truly bare Now stays Immediate.
            LlmWhen::Now => match (p.when_secs, p.slot) {
                (Some(secs), _) => RawTrigger::SpanElapsed { secs },
                (None, Some(n)) => RawTrigger::QueuePosition { n, base: PosBase::Absolute },
                (None, None) => RawTrigger::Immediate,
            },
            LlmWhen::AfterCurrent => RawTrigger::TrackAfterCurrent,
            LlmWhen::AfterSecs => RawTrigger::SpanElapsed {
                secs: p.when_secs.ok_or("after_secs needs when_secs")?,
            },
            LlmWhen::AlbumBoundary => RawTrigger::AlbumBoundary { track: TrackSel::Current },
            LlmWhen::QueuePosition => RawTrigger::QueuePosition {
                n: p.slot.ok_or("queue_position needs slot")?,
                base: PosBase::Absolute,
            },
            LlmWhen::TimeRemaining => RawTrigger::TimeRemaining {
                track: TrackSel::Current,
                secs: p.when_secs.ok_or("time_remaining needs when_secs")?,
            },
        };

        let action = match p.kind {
            LlmActionKind::FadeOut => {
                Action::Fade(FadeIntentIr::Out { secs: p.secs.unwrap_or(DEFAULT_FADE_SECS) })
            }
            LlmActionKind::FadeIn => {
                Action::Fade(FadeIntentIr::In { secs: p.secs.unwrap_or(DEFAULT_FADE_SECS) })
            }
            LlmActionKind::Stop => Action::Stop,
            LlmActionKind::Pause => Action::Pause,
            LlmActionKind::SetVolume => Action::SetVolume(p.level.ok_or("set_volume needs level")?),
            LlmActionKind::Enqueue => {
                // similar_to_current wins (an explicit "more like this" token); else
                // presence-only inference: radio, then genre, then query. NO SongId
                // path exists, so this builds only SimilarToCurrent/Query/Genre/Radio
                // (the daemon fills the seed id server-side for SimilarToCurrent).
                let selector = if matches!(p.sel, Some(LlmQSel::SimilarToCurrent)) {
                    Selector::SimilarToCurrent
                } else if p.radio {
                    Selector::Radio
                } else if let Some(g) = p.genre {
                    Selector::Genre(g)
                } else if let Some(q) = p.query {
                    Selector::Query(q)
                } else {
                    return Err("enqueue needs query, genre, radio, or similar_to_current".into());
                };
                Action::Enqueue { selector, count: p.count.unwrap_or(1) }
            }
            LlmActionKind::PlayNow => {
                // Same inference as Enqueue (similar_to_current > radio > genre > query);
                // no SongId path exists, so only SimilarToCurrent/Query/Genre/Radio.
                let selector = if matches!(p.sel, Some(LlmQSel::SimilarToCurrent)) {
                    Selector::SimilarToCurrent
                } else if p.radio {
                    Selector::Radio
                } else if let Some(g) = p.genre {
                    Selector::Genre(g)
                } else if let Some(q) = p.query {
                    Selector::Query(q)
                } else {
                    return Err("play_now needs query, genre, radio, or similar_to_current".into());
                };
                Action::PlayNow { selector, count: p.count.unwrap_or(1) }
            }
            LlmActionKind::Remove => Action::Remove { sel: build_qsel(&p)? },
            LlmActionKind::Play => Action::Play { sel: build_qsel(&p)? },
            LlmActionKind::Noop => Action::Noop,
            LlmActionKind::Clear => {
                let scope = match p.scope.unwrap_or(LlmClearScope::All) {
                    LlmClearScope::All => ClearScope::All,
                    LlmClearScope::AfterCurrent => ClearScope::AfterCurrent,
                    LlmClearScope::Range => ClearScope::Range {
                        start: p.range_start.ok_or("scope=range needs range_start")?,
                        end: p.range_end.ok_or("scope=range needs range_end")?,
                    },
                };
                Action::Clear { scope }
            }
            LlmActionKind::Move => {
                let sel = build_qsel(&p)?;
                let dest = match p.dest.ok_or("move needs a `dest`")? {
                    LlmMoveDest::Position => {
                        MoveDest::Position(p.dest_slot.ok_or("dest=position needs dest_slot")?)
                    }
                    LlmMoveDest::Relative => {
                        MoveDest::Relative(p.dest_rel.ok_or("dest=relative needs dest_rel")?)
                    }
                };
                Action::Move { sel, dest }
            }
        };

        // origin stays empty; the adapter stamps nl:cc:<model>, never the model.
        Ok(RawPlan { version: 1, trigger, action, once: p.once, origin: String::new() })
    }
}

/// Parse Claude's reply into a validated RawPlan. Tolerates the
/// {"actions":[ {...} ]} wrapper Claude gravitates to (live capture) AND a bare
/// flat object. Takes actions[0] (one plan per utterance). Model-free + always
/// compiled - the SINGLE model-free choke point. An off-surface or malformed reply
/// surfaces a readable error rather than panicking or fabricating a plan.
pub fn parse_llm_output(json: &str) -> Result<RawPlan, String> {
    #[derive(Deserialize)]
    struct Wrapper {
        actions: Vec<LlmRawPlan>,
    }

    let t = json.trim();
    let flat: LlmRawPlan = match serde_json::from_str::<Wrapper>(t) {
        Ok(w) => w.actions.into_iter().next().ok_or("empty actions array")?,
        Err(_) => serde_json::from_str::<LlmRawPlan>(t).map_err(|e| e.to_string())?,
    };
    RawPlan::try_from(flat)
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

    // EVERY allowed action AND trigger must round-trip through parse_llm_output on
    // the FLAT surface Claude actually emits (`type` discriminator + flat scalars),
    // including the omitted-trigger case that defaults to Now/Immediate.
    #[test]
    fn parse_round_trips_every_action_kind() {
        // fade out with an explicit span trigger.
        let p = parse_llm_output(
            r#"{"type":"fade_out","secs":10.0,"when":"after_secs","when_secs":300.0}"#,
        )
        .unwrap();
        assert!(matches!(p.action, Action::Fade(FadeIntentIr::Out { .. })));
        assert!(matches!(p.trigger, RawTrigger::SpanElapsed { .. }));

        // fade in.
        let p = parse_llm_output(r#"{"type":"fade_in","secs":10.0}"#).unwrap();
        assert!(matches!(p.action, Action::Fade(FadeIntentIr::In { .. })));

        // OMITTED trigger => Now => Immediate (the direct fix for "missing field
        // `trigger`"); omitted fade secs still plans (clamped downstream).
        let p = parse_llm_output(r#"{"type":"fade_out"}"#).unwrap();
        assert!(matches!(p.action, Action::Fade(FadeIntentIr::Out { .. })));
        assert!(matches!(p.trigger, RawTrigger::Immediate));

        // stop / pause, immediate.
        let p = parse_llm_output(r#"{"type":"stop"}"#).unwrap();
        assert!(matches!(p.action, Action::Stop));
        assert!(matches!(p.trigger, RawTrigger::Immediate));
        let p = parse_llm_output(r#"{"type":"pause"}"#).unwrap();
        assert!(matches!(p.action, Action::Pause));

        // set_volume(level), immediate.
        let p = parse_llm_output(r#"{"type":"set_volume","level":42}"#).unwrap();
        assert!(matches!(p.action, Action::SetVolume(42)));

        // enqueue(query).
        let p =
            parse_llm_output(r#"{"type":"enqueue","query":"bon iver","count":5}"#).unwrap();
        match p.action {
            Action::Enqueue { selector: Selector::Query(q), count } => {
                assert_eq!(q, "bon iver");
                assert_eq!(count, 5);
            }
            other => panic!("expected enqueue(query), got {other:?}"),
        }

        // enqueue(genre).
        let p = parse_llm_output(r#"{"type":"enqueue","genre":"jazz","count":3}"#).unwrap();
        match p.action {
            Action::Enqueue { selector: Selector::Genre(g), count } => {
                assert_eq!(g, "jazz");
                assert_eq!(count, 3);
            }
            other => panic!("expected enqueue(genre), got {other:?}"),
        }

        // enqueue(radio); omitted count defaults to 1.
        let p = parse_llm_output(r#"{"type":"enqueue","radio":true}"#).unwrap();
        assert!(matches!(
            p.action,
            Action::Enqueue { selector: Selector::Radio, count: 1 }
        ));

        // The {"actions":[...]} wrapper Claude gravitates to; first action wins.
        let p = parse_llm_output(r#"{"actions":[{"type":"pause"}]}"#).unwrap();
        assert!(matches!(p.action, Action::Pause));

        // Every FLAT trigger kind maps to its IR trigger.
        let p = parse_llm_output(r#"{"type":"stop","when":"after_current"}"#).unwrap();
        assert!(matches!(p.trigger, RawTrigger::TrackAfterCurrent));
        let p = parse_llm_output(r#"{"type":"stop","when":"album_boundary"}"#).unwrap();
        assert!(matches!(p.trigger, RawTrigger::AlbumBoundary { .. }));
        let p =
            parse_llm_output(r#"{"type":"stop","when":"queue_position","slot":3}"#).unwrap();
        assert!(matches!(
            p.trigger,
            RawTrigger::QueuePosition { n: 3, base: PosBase::Absolute }
        ));
        let p = parse_llm_output(
            r#"{"type":"fade_out","secs":10.0,"when":"time_remaining","when_secs":30.0}"#,
        )
        .unwrap();
        assert!(matches!(p.trigger, RawTrigger::TimeRemaining { .. }));
    }

    // A delay expressed ONLY via when_secs/slot (Claude omitted the `when` tag, so
    // it defaults to Now) must NOT be silently dropped and fired immediately: the
    // timing scalar drives the trigger.
    #[test]
    fn parse_infers_delay_when_when_tag_omitted() {
        // stop "in 300s" with no `when` tag => span, not immediate.
        let p = parse_llm_output(r#"{"type":"stop","when_secs":300.0}"#).unwrap();
        match p.trigger {
            RawTrigger::SpanElapsed { secs } => assert_eq!(secs, 300.0),
            other => panic!("expected SpanElapsed, got {other:?}"),
        }
        // fade_out with when_secs but no `when` tag => span, not immediate.
        let p = parse_llm_output(r#"{"type":"fade_out","secs":10.0,"when_secs":300.0}"#).unwrap();
        assert!(matches!(p.trigger, RawTrigger::SpanElapsed { .. }));
        // slot-only queue_position intent with no `when` tag => queue position.
        let p = parse_llm_output(r#"{"type":"stop","slot":3}"#).unwrap();
        assert!(matches!(
            p.trigger,
            RawTrigger::QueuePosition { n: 3, base: PosBase::Absolute }
        ));
        // Truly bare Now stays Immediate.
        let p = parse_llm_output(r#"{"type":"stop"}"#).unwrap();
        assert!(matches!(p.trigger, RawTrigger::Immediate));
    }

    // The queue-edit actions (remove/move/clear/play) + the noop class round-trip
    // through the FLAT surface Claude emits, building the intended selector/scope/dest.
    #[test]
    fn parse_round_trips_queue_edit_actions() {
        use hypodj_core::plan::{ClearScope, MoveDest, QueueSelector};

        // remove last 3.
        let p = parse_llm_output(r#"{"type":"remove","sel":"last","count":3}"#).unwrap();
        assert!(matches!(
            p.action,
            Action::Remove { sel: QueueSelector::Last(3) }
        ));
        // remove current.
        let p = parse_llm_output(r#"{"type":"remove","sel":"current"}"#).unwrap();
        assert!(matches!(p.action, Action::Remove { sel: QueueSelector::Current }));
        // remove by query.
        let p = parse_llm_output(r#"{"type":"remove","sel":"query","query":"so what"}"#).unwrap();
        match p.action {
            Action::Remove { sel: QueueSelector::QueryMatch(q) } => assert_eq!(q, "so what"),
            other => panic!("got {other:?}"),
        }
        // remove a range.
        let p = parse_llm_output(
            r#"{"type":"remove","sel":"range","range_start":2,"range_end":5}"#,
        )
        .unwrap();
        assert!(matches!(
            p.action,
            Action::Remove { sel: QueueSelector::Range { start: 2, end: 5 } }
        ));
        // clear after_current / all / range.
        assert!(matches!(
            parse_llm_output(r#"{"type":"clear","scope":"after_current"}"#).unwrap().action,
            Action::Clear { scope: ClearScope::AfterCurrent }
        ));
        assert!(matches!(
            parse_llm_output(r#"{"type":"clear","scope":"all"}"#).unwrap().action,
            Action::Clear { scope: ClearScope::All }
        ));
        assert!(matches!(
            parse_llm_output(r#"{"type":"clear"}"#).unwrap().action,
            Action::Clear { scope: ClearScope::All }
        ));
        // move last->top and a relative move.
        assert!(matches!(
            parse_llm_output(
                r#"{"type":"move","sel":"last","count":1,"dest":"position","dest_slot":1}"#
            )
            .unwrap()
            .action,
            Action::Move { sel: QueueSelector::Last(1), dest: MoveDest::Position(1) }
        ));
        assert!(matches!(
            parse_llm_output(
                r#"{"type":"move","sel":"position","slot":4,"dest":"relative","dest_rel":-2}"#
            )
            .unwrap()
            .action,
            Action::Move { sel: QueueSelector::Position(4), dest: MoveDest::Relative(-2) }
        ));
        // play by query / position.
        match parse_llm_output(r#"{"type":"play","sel":"query","query":"blue"}"#).unwrap().action {
            Action::Play { sel: QueueSelector::QueryMatch(q) } => assert_eq!(q, "blue"),
            other => panic!("got {other:?}"),
        }
        assert!(matches!(
            parse_llm_output(r#"{"type":"play","sel":"position","slot":6}"#).unwrap().action,
            Action::Play { sel: QueueSelector::Position(6) }
        ));
        // noop: honest no-action for an off-topic ask (never a fabricated enqueue).
        assert!(matches!(parse_llm_output(r#"{"type":"noop"}"#).unwrap().action, Action::Noop));
    }

    // play_now (enqueue-then-start) parses from the SAME flat query/genre/radio
    // surface as enqueue, but builds the distinct Action::PlayNow. "queue X" stays
    // append-only Action::Enqueue - the two are never conflated.
    #[test]
    fn parse_distinguishes_play_now_from_append_only_enqueue() {
        // "play <specific title>" -> play_now (query), count defaults to 1.
        let p = parse_llm_output(r#"{"type":"play_now","query":"at the door"}"#).unwrap();
        match p.action {
            Action::PlayNow { selector: Selector::Query(q), count } => {
                assert_eq!(q, "at the door");
                assert_eq!(count, 1);
            }
            other => panic!("expected play_now(query), got {other:?}"),
        }
        // play_now by genre / radio also reachable (same presence inference).
        assert!(matches!(
            parse_llm_output(r#"{"type":"play_now","genre":"jazz"}"#).unwrap().action,
            Action::PlayNow { selector: Selector::Genre(_), .. }
        ));
        assert!(matches!(
            parse_llm_output(r#"{"type":"play_now","radio":true}"#).unwrap().action,
            Action::PlayNow { selector: Selector::Radio, .. }
        ));
        // play_now with no selector fails loud (never a fabricated SongId path).
        assert!(parse_llm_output(r#"{"type":"play_now"}"#).is_err());
        assert!(parse_llm_output(r#"{"type":"play_now","id":"song-42"}"#).is_err());

        // The SAME title under "enqueue" stays append-only Enqueue, NOT PlayNow.
        assert!(matches!(
            parse_llm_output(r#"{"type":"enqueue","query":"at the door","count":1}"#)
                .unwrap()
                .action,
            Action::Enqueue { selector: Selector::Query(_), .. }
        ));
    }

    // "more like what is playing": the model emits sel=similar_to_current (NO id,
    // no query/genre) on enqueue OR play_now, and it parses to the id-free
    // Selector::SimilarToCurrent. The daemon fills the seed server-side, so the
    // off-surface-id boundary is untouched.
    #[test]
    fn parse_similar_to_current_carries_no_id() {
        // enqueue "more like this one".
        let p = parse_llm_output(
            r#"{"type":"enqueue","sel":"similar_to_current","count":5}"#,
        )
        .unwrap();
        match p.action {
            Action::Enqueue { selector: Selector::SimilarToCurrent, count } => {
                assert_eq!(count, 5);
            }
            other => panic!("expected enqueue(similar_to_current), got {other:?}"),
        }
        // play_now "play more like what is playing"; omitted count defaults to 1.
        let p = parse_llm_output(r#"{"type":"play_now","sel":"similar_to_current"}"#).unwrap();
        assert!(matches!(
            p.action,
            Action::PlayNow { selector: Selector::SimilarToCurrent, count: 1 }
        ));
        // similar_to_current wins even if the model ALSO (wrongly) attaches an id or
        // query: the id is ignored (no field lands it) and the selector stays id-free.
        let p = parse_llm_output(
            r#"{"type":"enqueue","sel":"similar_to_current","id":"song-42","query":"x","count":3}"#,
        )
        .unwrap();
        assert!(matches!(
            p.action,
            Action::Enqueue { selector: Selector::SimilarToCurrent, .. }
        ));
        // similar_to_current is a CONTENT selector, NOT a live-queue selector: a
        // remove/move/play/clear that tries it is rejected (never a wrong-target op).
        assert!(parse_llm_output(r#"{"type":"remove","sel":"similar_to_current"}"#).is_err());
        assert!(parse_llm_output(r#"{"type":"play","sel":"similar_to_current"}"#).is_err());
    }

    // A queue-edit action with a MISSING required scalar fails loud (no fabricated
    // wrong-target op): a remove with a `sel` but no supporting field is rejected.
    #[test]
    fn parse_rejects_incomplete_queue_edits() {
        assert!(parse_llm_output(r#"{"type":"remove"}"#).is_err());
        assert!(parse_llm_output(r#"{"type":"remove","sel":"last"}"#).is_err());
        assert!(parse_llm_output(r#"{"type":"remove","sel":"query"}"#).is_err());
        assert!(parse_llm_output(r#"{"type":"move","sel":"current"}"#).is_err());
        assert!(parse_llm_output(r#"{"type":"move","sel":"current","dest":"relative"}"#).is_err());
        assert!(parse_llm_output(r#"{"type":"play"}"#).is_err());
    }

    // An OFF-surface intent must be REJECTED even if a backend ignores the schema.
    // The closed `type`/`when` enums + presence-only selector inference make every
    // off-surface effect UNREPRESENTABLE, so serde/TryFrom fails loud instead of
    // arming it.
    #[test]
    fn parse_rejects_off_surface_actions() {
        // An unknown action `type` (Action::Wake and friends) is not in the lexicon.
        assert!(parse_llm_output(r#"{"type":"wake","count":5}"#).is_err());
        // The nested fade dirs (wake_to / to_floor / to) have NO representation:
        // there is no `dir` field and no such action `type`.
        assert!(parse_llm_output(r#"{"type":"wake_to","secs":10.0}"#).is_err());
        assert!(parse_llm_output(r#"{"type":"to_floor","secs":10.0}"#).is_err());
        // A wall_clock trigger is off the `when` lexicon; an absolute datetime has
        // nowhere to land (no variant, no DateTime field).
        assert!(parse_llm_output(r#"{"type":"stop","when":"wall_clock"}"#).is_err());
        assert!(parse_llm_output(
            r#"{"type":"stop","when":"wall_clock","at":"2026-01-01T00:00:00Z"}"#
        )
        .is_err());
        // An identity-bearing selector cannot be built: an `id`/`song_id` field is
        // ignored, and with no query/genre/radio the enqueue conversion fails loud
        // rather than fabricating a SongId selector.
        assert!(parse_llm_output(r#"{"type":"enqueue","id":"song-42"}"#).is_err());
        // A missing discriminator is rejected (no default for the action kind).
        assert!(parse_llm_output(r#"{"when":"now"}"#).is_err());
        // An unknown queue-selector / clear-scope tag is off the closed lexicon.
        assert!(parse_llm_output(r#"{"type":"remove","sel":"everything"}"#).is_err());
        assert!(parse_llm_output(r#"{"type":"clear","scope":"nuke"}"#).is_err());
        assert!(parse_llm_output(r#"{"type":"move","sel":"current","dest":"warp"}"#).is_err());
    }
}
