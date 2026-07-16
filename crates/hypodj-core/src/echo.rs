//! Echo rendering: a [`RawPlan`] back to the keyword `plan add` DSL (so
//! `nl confirm` and `plan add <dsl>` provably converge - the echo re-parses to
//! the same IR), plus a short human sentence for the confirmation prompt.

use crate::nl::NlSource;
use crate::plan::{Action, FadeIntentIr, PosBase, RawPlan, RawTrigger, Selector, TrackSel};

/// Format an f64 seconds value without a trailing `.0` (so `parse_secs` re-reads
/// it): `30.0 -> "30"`, `1200.0 -> "1200"`, `30.5 -> "30.5"`.
fn secs(s: f64) -> String {
    if s.fract() == 0.0 {
        format!("{}", s as i64)
    } else {
        format!("{s}")
    }
}

/// Quote + escape a selector value so a multi-word value ("good vibes", "drum
/// and bass") survives the `plan add` tokenizer as ONE token (it splits on
/// unquoted whitespace and unescapes `\"`/`\\` inside quotes). A single bare word
/// is emitted verbatim; anything with whitespace/quote/backslash is quoted.
///
/// Returns `None` when the value contains a control character (newline, CR, tab,
/// etc.). The `plan add <dsl>` line is framed on the wire by a literal `\n`, and
/// the tokenizer's `\`-escape only unescapes the NEXT char (never re-encodes a
/// literal newline), so a value carrying a newline would smuggle EXTRA command
/// lines past the single-owner confirm gate. Refusing to render it makes the
/// caller fall back to direct arming (or a loud "cannot express" miss) - a
/// control char is nonsense in a music selector anyway.
fn dsl_value(s: &str) -> Option<String> {
    if s.chars().any(|c| c.is_control()) {
        return None;
    }
    if s.is_empty() || s.chars().any(|c| c.is_whitespace() || c == '"' || c == '\\') {
        let mut out = String::with_capacity(s.len() + 2);
        out.push('"');
        for c in s.chars() {
            if c == '"' || c == '\\' {
                out.push('\\');
            }
            out.push(c);
        }
        out.push('"');
        Some(out)
    } else {
        Some(s.to_string())
    }
}

/// Render the trigger portion of the `plan add` DSL.
fn trigger_dsl(t: &RawTrigger) -> Option<String> {
    Some(match t {
        RawTrigger::Immediate => "trigger immediate".into(),
        RawTrigger::QueuePosition { n, base } => {
            let b = match base {
                PosBase::CurrentIsOne => "current",
                PosBase::Absolute => "absolute",
            };
            format!("trigger track {n} base {b}")
        }
        RawTrigger::TrackAfterCurrent => "trigger after".into(),
        RawTrigger::AlbumBoundary { track } => match track {
            TrackSel::Current => "trigger album".into(),
            _ => return None,
        },
        RawTrigger::SpanElapsed { secs: s } => format!("trigger in {}", secs(*s)),
        RawTrigger::WallClock { at } => format!("trigger at {}", at.to_rfc3339()),
        // TimeRemaining is not emitted by the rules grammar; not round-tripped.
        RawTrigger::TimeRemaining { .. } => return None,
    })
}

/// Render the action portion of the `plan add` DSL.
fn action_dsl(a: &Action) -> Option<String> {
    Some(match a {
        Action::Fade(FadeIntentIr::Out { secs: s }) => format!("action fade out {}", secs(*s)),
        Action::Fade(FadeIntentIr::In { secs: s }) => format!("action fade in {}", secs(*s)),
        Action::Fade(FadeIntentIr::To { vol, secs: s, .. }) => {
            format!("action fade to {} {}", vol, secs(*s))
        }
        Action::Stop => "action stop".into(),
        Action::Pause => "action pause".into(),
        Action::SetVolume(v) => format!("action setvol {v}"),
        Action::Enqueue { selector, count } => match selector {
            Selector::Radio => format!("action enqueue radio {count}"),
            Selector::Query(q) => format!("action enqueue query {} {count}", dsl_value(q)?),
            Selector::Genre(g) => format!("action enqueue genre {} {count}", dsl_value(g)?),
            // Exact/Similar/Calmer are not expressible in the keyword DSL.
            _ => return None,
        },
        // ToFloor/WakeTo/Wake are built only by the convenience sleep/winddown/wake
        // commands, not the keyword `plan add` DSL - so they are not round-tripped.
        Action::Fade(FadeIntentIr::ToFloor { .. })
        | Action::Fade(FadeIntentIr::WakeTo { .. })
        | Action::Wake { .. } => return None,
    })
}

/// Render `<trigger> ... action ... [once]` for a `plan add`. `None` when a
/// variant is not keyword-DSL-expressible (Calmer/Similar/TimeRemaining), in which
/// case `nl confirm` still arms it directly - only the DSL round-trip is skipped.
pub fn render_dsl(raw: &RawPlan) -> Option<String> {
    let t = trigger_dsl(&raw.trigger)?;
    let a = action_dsl(&raw.action)?;
    let mut s = format!("{t} {a}");
    if raw.once {
        s.push_str(" once");
    }
    Some(s)
}

fn source_label(src: NlSource) -> &'static str {
    match src {
        NlSource::Rules => "via rules",
        NlSource::Llm => "via local model",
    }
}

/// A short, honest human clause for one plan (the confirmation surface). Spells
/// out a guessed fade direction, flags the append-only + not-yet-at-execute holes.
pub fn describe_plan(raw: &RawPlan) -> String {
    let when = match &raw.trigger {
        RawTrigger::Immediate => "now".to_string(),
        RawTrigger::QueuePosition { n, base } => match base {
            PosBase::CurrentIsOne => format!("when the {} track (counting current as #1) starts", ordinal(*n)),
            PosBase::Absolute => format!("when the {} track in the queue starts", ordinal(*n)),
        },
        RawTrigger::TrackAfterCurrent => "after the current track".to_string(),
        RawTrigger::AlbumBoundary { .. } => "after the current album ends".to_string(),
        RawTrigger::SpanElapsed { secs: s } => format!("in {}", human_dur(*s)),
        RawTrigger::WallClock { at } => format!("at {} UTC", at.format("%Y-%m-%d %H:%M")),
        RawTrigger::TimeRemaining { secs: s, .. } => format!("with {} left on the track", human_dur(*s)),
    };
    let what = match &raw.action {
        Action::Fade(FadeIntentIr::Out { secs: s }) => format!("fade OUT over {}", human_dur(*s)),
        Action::Fade(FadeIntentIr::In { secs: s }) => format!("fade IN over {}", human_dur(*s)),
        Action::Fade(FadeIntentIr::To { vol, secs: s, .. }) => {
            format!("fade to volume {} over {}", vol, human_dur(*s))
        }
        Action::Stop => "stop playback".to_string(),
        Action::Pause => "pause playback".to_string(),
        Action::SetVolume(v) => format!("set volume to {v}"),
        Action::Enqueue { selector, count } => {
            let sel = match selector {
                Selector::Genre(g) => format!("{g} tracks"),
                Selector::Query(q) => format!("tracks matching \"{q}\""),
                Selector::Radio => "random tracks".to_string(),
                Selector::Similar(_) => "similar tracks".to_string(),
                Selector::Calmer(_) => "calmer tracks".to_string(),
                Selector::Exact(_) => "tracks".to_string(),
            };
            format!("add {count} {sel} to the END of the queue (append-only)")
        }
        Action::Fade(FadeIntentIr::ToFloor { secs: s }) => {
            format!("wind DOWN to the quiet floor over {}", human_dur(*s))
        }
        Action::Fade(FadeIntentIr::WakeTo { vol, secs: s, .. }) => {
            format!("wake UP to volume {} over {}", vol, human_dur(*s))
        }
        Action::Wake { selector, count } => match selector {
            Some(sel) => {
                let what = match sel {
                    Selector::Genre(g) => format!("{g} tracks"),
                    Selector::Query(q) => format!("tracks matching \"{q}\""),
                    Selector::Radio => "random tracks".to_string(),
                    _ => "tracks".to_string(),
                };
                format!("wake: enqueue {count} {what}, then ramp up from silence")
            }
            None => "wake: ramp up from silence to the saved comfort level".to_string(),
        },
    };
    format!("{what} {when}")
}

/// The full echo body for a translated batch: the source trust signal, each
/// plan's clause, and the honest wake caveat when it is an Enqueue-only "wake".
pub fn describe_batch(plans: &[RawPlan], src: NlSource) -> String {
    let mut lines: Vec<String> = Vec::new();
    lines.push(source_label(src).to_string());
    for (i, p) in plans.iter().enumerate() {
        lines.push(format!("[{}] {}", i + 1, describe_plan(p)));
    }
    // A wake built only from Enqueue (append-only) + Fade cannot itself START
    // playback; flag that honestly so the user does not confirm a silent no-op.
    let is_wake = plans.len() > 1
        && plans.iter().all(|p| matches!(p.trigger, RawTrigger::WallClock { .. }))
        && plans.iter().any(|p| matches!(p.action, Action::Enqueue { .. }));
    if is_wake {
        lines.push(
            "NOTE: this appends tracks and fades in; a TRUE wake also needs a play/skip action (Enqueue is append-only)".into(),
        );
    }
    // Single line: an MPD pair value must not embed a newline (the wire frames
    // pairs as `key: value\n`). Join the clauses with a visible separator.
    lines.join(" | ")
}

fn ordinal(n: usize) -> String {
    let suf = match (n % 10, n % 100) {
        (1, 11) | (2, 12) | (3, 13) => "th",
        (1, _) => "st",
        (2, _) => "nd",
        (3, _) => "rd",
        _ => "th",
    };
    format!("{n}{suf}")
}

fn human_dur(s: f64) -> String {
    if s >= 60.0 && s % 60.0 == 0.0 {
        let m = (s / 60.0) as i64;
        format!("{m} min")
    } else {
        format!("{}s", secs(s))
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::mpd::{parse, MpdCommand, PlanCmd};

    /// A rendered DSL must re-parse through the `plan add` parser to the SAME plan
    /// IR (modulo `origin`, which the parser defaults). Proves the echoed plan can
    /// be re-armed exactly as described.
    fn assert_round_trip(raw: &RawPlan) {
        let dsl = render_dsl(raw).expect("this plan is keyword-DSL-expressible");
        let parsed = match parse(&format!("plan add {dsl}")) {
            MpdCommand::Plan(PlanCmd::Add(r)) => r,
            other => panic!("re-parse of `{dsl}` failed: {other:?}"),
        };
        // render_dsl captures trigger + action + once (never origin) and is
        // deterministic, so an identical re-render proves the IR round-tripped.
        assert_eq!(
            render_dsl(&parsed).as_deref(),
            Some(dsl.as_str()),
            "round-trip mismatch for `{dsl}`",
        );
    }

    #[test]
    fn render_dsl_round_trips_multiword_selectors() {
        // A multi-word Query value: unquoted spaces would break the plan-add
        // tokenizer; the emitted DSL must quote it so it survives.
        assert_round_trip(&RawPlan {
            version: 1,
            trigger: RawTrigger::TrackAfterCurrent,
            action: Action::Enqueue {
                selector: Selector::Query("good vibes only".into()),
                count: 5,
            },
            once: true,
            origin: String::new(),
        });
        // A multi-word Genre (the closed lexicon includes "drum and bass").
        assert_round_trip(&RawPlan {
            version: 1,
            trigger: RawTrigger::Immediate,
            action: Action::Enqueue {
                selector: Selector::Genre("drum and bass".into()),
                count: 3,
            },
            once: false,
            origin: String::new(),
        });
        // A value carrying a quote + backslash must escape and round-trip.
        assert_round_trip(&RawPlan {
            version: 1,
            trigger: RawTrigger::Immediate,
            action: Action::Enqueue {
                selector: Selector::Query("a \"b\" \\c".into()),
                count: 1,
            },
            once: false,
            origin: String::new(),
        });
        // A single bare word still round-trips (no quoting needed).
        assert_round_trip(&RawPlan {
            version: 1,
            trigger: RawTrigger::Immediate,
            action: Action::Enqueue { selector: Selector::Genre("jazz".into()), count: 2 },
            once: true,
            origin: String::new(),
        });
    }

    #[test]
    fn render_dsl_refuses_control_chars_in_selector() {
        // A selector value carrying a literal newline would, once framed on the
        // wire as `plan add ...\n`, smuggle EXTRA command lines (`clear`, `next`)
        // past the single-owner confirm gate. render_dsl must refuse to emit it.
        let plan = RawPlan {
            version: 1,
            trigger: RawTrigger::Immediate,
            action: Action::Enqueue {
                selector: Selector::Query("chill\nclear\nnext".into()),
                count: 5,
            },
            once: false,
            origin: String::new(),
        };
        assert_eq!(render_dsl(&plan), None, "a newline-bearing selector must not render");
        // Carriage return and tab are control chars too.
        let plan = RawPlan {
            action: Action::Enqueue {
                selector: Selector::Genre("jazz\rvol\t100".into()),
                count: 1,
            },
            ..plan
        };
        assert_eq!(render_dsl(&plan), None);
    }
}
