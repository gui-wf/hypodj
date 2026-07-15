//! The deterministic RULES fast-path: a hand-rolled tokenizer + keyword matcher
//! that turns the common corpus phrasings into a bounded [`RawPlan`]. ZERO model,
//! fully deterministic, table-tested (utterance -> exact RawPlan / exact NlError).
//!
//! Every emitted plan carries `origin: ""`: the adapter (handler) stamps
//! `nl:rules`, never this grammar. WallClock and Immediate ARE allowed here (they
//! are Rules-only; the LLM subset omits them).

use chrono::{TimeZone, Utc};

use hypodj_core::nl::{NlContext, NlError, NlHit, NlSource, Translator};
use hypodj_core::plan::{Action, FadeIntentIr, PosBase, RawPlan, RawTrigger, Selector, TrackSel};

/// The default fade duration a bare "fade out"/"fade in" emits. 30s matches the
/// P2 executor fixture (`fixture_track3_fade_out_on_track_start`), so the worked
/// example RawPlan is byte-identical to the fixture's.
pub const DEFAULT_FADE_SECS: f64 = 30.0;
/// The default track count a bare Enqueue selector ("something calmer", "wake me
/// with jazz") emits. Append-only + count-clamped keeps this a bounded hole.
pub const DEFAULT_ENQUEUE_COUNT: u32 = 5;

/// The closed genre lexicon: "with <X>" maps to [`Selector::Genre`] only when X is
/// one of these; anything else is the safe free-text [`Selector::Query`].
const GENRES: &[&str] = &[
    "jazz", "ambient", "techno", "classical", "house", "drum and bass", "dnb",
    "hip hop", "hip-hop", "rock", "pop", "electronic", "soul", "funk", "reggae",
    "blues", "metal", "disco", "folk",
];

/// The deterministic, stateless rules translator.
pub struct RulesTranslator;

impl Translator for RulesTranslator {
    fn translate(&self, utterance: &str, ctx: &NlContext) -> Result<NlHit, NlError> {
        let plans = translate_rules(utterance, ctx)?;
        Ok(NlHit { plans, source: NlSource::Rules })
    }
}

/// Emit a raw plan with `origin` left empty (the adapter stamps it) and `once`.
fn plan(trigger: RawTrigger, action: Action, once: bool) -> RawPlan {
    RawPlan { version: 1, trigger, action, once, origin: String::new() }
}

/// The single entry point: match the utterance to one of the closed corpus
/// patterns, or fail with a SPECIFIC [`NlError`] (NotUnderstood falls through).
pub fn translate_rules(utterance: &str, ctx: &NlContext) -> Result<Vec<RawPlan>, NlError> {
    let text = utterance.trim();
    if text.is_empty() {
        return Err(NlError::NotUnderstood);
    }
    let low = text.to_lowercase();
    let toks: Vec<String> = low.split_whitespace().map(String::from).collect();
    let tv: Vec<&str> = toks.iter().map(String::as_str).collect();

    // 1. wake ("wake me at 7 with jazz") - an ordered same-instant batch.
    if tv.contains(&"wake") && tv.contains(&"at") {
        return wake(&tv, &low, text, ctx);
    }

    // 2. calmer / similar ("something calmer", "more like this").
    if low.contains("calmer") {
        return one(calmer_similar(&tv, true, ctx)?);
    }
    if low.contains("more like this") || low.contains("something like this") {
        return one(calmer_similar(&tv, false, ctx)?);
    }

    // 3. "fade the Nth track [counting current as 1st]". A positional fade names a
    // "track"/"song"; if the ordinal is outside the closed set, fade_track returns
    // NotUnderstood (it must NOT silently downgrade to a plain immediate fade).
    if tv.contains(&"fade") && (tv.contains(&"track") || tv.contains(&"song")) {
        return one(fade_track(&tv, &low)?);
    }

    // 4. stop / pause (with an optional boundary/time trigger).
    if tv.first() == Some(&"stop") || tv.first() == Some(&"pause") {
        return one(stop_pause(&tv, &low, text, ctx)?);
    }

    // 5. immediate / timed fade ("fade out [over|in] ...").
    if tv.contains(&"fade") {
        return one(fade_immediate(&tv, &low, text, ctx)?);
    }

    Err(NlError::NotUnderstood)
}

fn one(p: RawPlan) -> Result<Vec<RawPlan>, NlError> {
    Ok(vec![p])
}

// ── ordinals ────────────────────────────────────────────────────────────────

/// Closed ordinal lexicon first..tenth (+ the "1st".."10th"/"3rd" digit forms).
/// Anything outside the closed set ("last", roman numerals, "twenty-third") is
/// NOT an ordinal here (the caller returns NotUnderstood so the model may punt).
fn parse_ordinal(w: &str) -> Option<usize> {
    let n = match w {
        "first" => 1, "second" => 2, "third" => 3, "fourth" => 4, "fifth" => 5,
        "sixth" => 6, "seventh" => 7, "eighth" => 8, "ninth" => 9, "tenth" => 10,
        _ => {
            let digits = w
                .trim_end_matches("st")
                .trim_end_matches("nd")
                .trim_end_matches("rd")
                .trim_end_matches("th");
            let v: usize = digits.parse().ok()?;
            if w == digits {
                // A bare number ("3") is NOT an ordinal ("the 3 track" is not a
                // phrasing we accept); require an ordinal suffix or word.
                return None;
            }
            v
        }
    };
    (1..=10).contains(&n).then_some(n)
}


// ── fades ─────────────────────────────────────────────────────────────────────

fn fade_dir(tv: &[&str]) -> FadeIntentIr {
    // Explicit "fade in" -> In; everything else defaults to Out (colloquial
    // "fade [out]"); the echo spells the guessed direction out.
    let fi = tv.iter().position(|t| *t == "fade");
    match fi.and_then(|i| tv.get(i + 1)) {
        Some(&"in") => FadeIntentIr::In { secs: DEFAULT_FADE_SECS },
        _ => FadeIntentIr::Out { secs: DEFAULT_FADE_SECS },
    }
}

fn set_fade_secs(dir: FadeIntentIr, secs: f64) -> FadeIntentIr {
    match dir {
        FadeIntentIr::In { .. } => FadeIntentIr::In { secs },
        FadeIntentIr::Out { .. } => FadeIntentIr::Out { secs },
        other => other,
    }
}

/// "fade the Nth track [counting current as 1st]" -> QueuePosition + Fade.
fn fade_track(tv: &[&str], low: &str) -> Result<RawPlan, NlError> {
    // The ordinal right before "track"/"song".
    let n = tv
        .windows(2)
        .find_map(|w| {
            if w[1] == "track" || w[1] == "song" {
                parse_ordinal(w[0])
            } else {
                None
            }
        })
        .ok_or(NlError::NotUnderstood)?;
    let base = if low.contains("counting current")
        || low.contains("including this")
        || low.contains("from here")
    {
        PosBase::CurrentIsOne
    } else {
        PosBase::Absolute
    };
    let dir = fade_dir(tv);
    Ok(plan(
        RawTrigger::QueuePosition { n, base },
        Action::Fade(dir),
        true,
    ))
}

/// "fade out [in <dur>|at <time>|over <dur>|<dur>]" - the TRIGGER-vs-FADE-DURATION
/// preposition gate. "in/at" bind the TRIGGER; "over/for/across" or a bare trailing
/// duration bind the FADE secs.
fn fade_immediate(
    tv: &[&str],
    low: &str,
    text: &str,
    ctx: &NlContext,
) -> Result<RawPlan, NlError> {
    let fi = tv.iter().position(|t| *t == "fade").ok_or(NlError::NotUnderstood)?;
    // Skip the explicit direction word so "fade in 20 minutes" reads "in" as the
    // DIRECTION, not a trigger preposition.
    let mut rest_start = fi + 1;
    if matches!(tv.get(rest_start), Some(&"out") | Some(&"in")) {
        rest_start += 1;
    }
    let rest = &tv[rest_start.min(tv.len())..];

    let mut dir = fade_dir(tv);
    let mut trigger = RawTrigger::Immediate;
    let mut i = 0;
    while i < rest.len() {
        match rest[i] {
            "over" | "for" | "across" => {
                let (secs, c) = parse_dur(rest, i + 1).ok_or(NlError::NotUnderstood)?;
                dir = set_fade_secs(dir, secs);
                i += 1 + c;
            }
            "in" => {
                let (secs, c) = parse_dur(rest, i + 1).ok_or(NlError::NotUnderstood)?;
                trigger = RawTrigger::SpanElapsed { secs };
                i += 1 + c;
            }
            "at" => {
                let (at, c) = parse_time(rest, i + 1, low, ctx).ok_or(NlError::NotUnderstood)?;
                trigger = RawTrigger::WallClock { at };
                i += 1 + c;
            }
            _ => {
                // A bare trailing duration after "fade out" binds the FADE secs.
                if let Some((secs, c)) = parse_dur(rest, i) {
                    dir = set_fade_secs(dir, secs);
                    i += c;
                } else {
                    i += 1;
                }
            }
        }
    }
    let _ = text;
    Ok(plan(trigger, Action::Fade(dir), true))
}

// ── stop / pause ──────────────────────────────────────────────────────────────

fn stop_pause(
    tv: &[&str],
    low: &str,
    text: &str,
    ctx: &NlContext,
) -> Result<RawPlan, NlError> {
    let action = if tv.first() == Some(&"pause") { Action::Pause } else { Action::Stop };
    let trigger = boundary_or_time(tv, low, text, ctx)?;
    Ok(plan(trigger, action, true))
}

/// Resolve a trailing "after this album|track", "in <dur>", or "at <time>" into a
/// trigger; a bare command with no such tail is Immediate. A missed "album"
/// keyword must NEVER silently downgrade to a track boundary.
fn boundary_or_time(
    tv: &[&str],
    low: &str,
    text: &str,
    ctx: &NlContext,
) -> Result<RawTrigger, NlError> {
    if low.contains("after this album") {
        return Ok(RawTrigger::AlbumBoundary { track: TrackSel::Current });
    }
    if low.contains("after this track")
        || low.contains("after this song")
        || low.contains("after this")
    {
        return Ok(RawTrigger::TrackAfterCurrent);
    }
    // An explicit "in"/"at" clause MUST parse: a misheard duration/time is a
    // NotUnderstood (punt to the model / loud ACK), NEVER a silent Immediate.
    if let Some(p) = tv.iter().position(|t| *t == "in") {
        let (secs, _) = parse_dur(tv, p + 1).ok_or(NlError::NotUnderstood)?;
        return Ok(RawTrigger::SpanElapsed { secs });
    }
    if let Some(p) = tv.iter().position(|t| *t == "at") {
        let (at, _) = parse_time(tv, p + 1, low, ctx).ok_or(NlError::NotUnderstood)?;
        return Ok(RawTrigger::WallClock { at });
    }
    // A DEFERRAL conjunction we did not resolve above ("stop after the album ends",
    // "pause once this song finishes") is a deferred intent we could not parse -
    // punt (NotUnderstood), NEVER silently downgrade to stopping NOW. A bare
    // "stop"/"pause" or only filler words ("stop now") still falls through to
    // Immediate.
    const DEFER_WORDS: &[&str] = &["after", "when", "once", "until", "before"];
    if tv.iter().any(|t| DEFER_WORDS.contains(t)) {
        return Err(NlError::NotUnderstood);
    }
    let _ = text;
    Ok(RawTrigger::Immediate)
}

// ── calmer / similar ──────────────────────────────────────────────────────────

fn calmer_similar(tv: &[&str], calmer: bool, ctx: &NlContext) -> Result<RawPlan, NlError> {
    let id = ctx
        .current
        .clone()
        .ok_or_else(|| NlError::Unresolvable("nothing playing to match".into()))?;
    let count = leading_count(tv).unwrap_or(DEFAULT_ENQUEUE_COUNT);
    let selector = if calmer { Selector::Calmer(id) } else { Selector::Similar(id) };
    Ok(plan(
        RawTrigger::TrackAfterCurrent,
        Action::Enqueue { selector, count },
        true,
    ))
}

/// A leading small count in "play 3 calmer tracks"; None -> use the default.
fn leading_count(tv: &[&str]) -> Option<u32> {
    tv.iter().find_map(|t| t.parse::<u32>().ok())
}

// ── wake ──────────────────────────────────────────────────────────────────────

/// "wake me at 7 with jazz" -> ONE [`Action::Wake`] at the resolved WallClock
/// instant. A single atomic Wake (enqueue -> start-from-silence -> play -> ramp IN)
/// is what actually STARTS playback from a stopped / wound-down deck; a plain
/// Enqueue + Fade(In) pair is append-only + volume-only and would be a silent no-op
/// from silence. Matches the direct `wake` command (`HypodjHandler::wake_set`).
fn wake(tv: &[&str], low: &str, text: &str, ctx: &NlContext) -> Result<Vec<RawPlan>, NlError> {
    let at_pos = tv.iter().position(|t| *t == "at").ok_or(NlError::NotUnderstood)?;
    let (at, _) = parse_time(tv, at_pos + 1, low, ctx)
        .ok_or_else(|| NlError::Ambiguous("could not read the wake time".into()))?;
    // Optional "with <X>" selector; None means wake into the existing queue.
    let selector = with_selector(text);
    Ok(vec![plan(
        RawTrigger::WallClock { at },
        Action::Wake { selector, count: DEFAULT_ENQUEUE_COUNT },
        true,
    )])
}

/// The "with <X>" selector class: Genre only for the closed lexicon, else Query
/// (safe free-text). Uses the ORIGINAL-case text so a Query keeps "Bon Iver".
fn with_selector(text: &str) -> Option<Selector> {
    // Locate " with " in the ORIGINAL text via a case-insensitive ASCII byte-window
    // match. Searching the lowercased copy and slicing the original desyncs when a
    // preceding char changes byte length under lowercasing (garbled selector, or a
    // mid-codepoint slice panic). The needle is pure ASCII, so the match position
    // is a valid char boundary in `text`.
    let needle = b" with ";
    let idx = text
        .as_bytes()
        .windows(needle.len())
        .position(|w| w.eq_ignore_ascii_case(needle))?;
    let raw = text[idx + needle.len()..].trim();
    // Trim trailing punctuation AND a small set of politeness filler words
    // ("please", "thanks", "thank you") so "with jazz please" resolves the closed
    // genre "jazz", not the free-text Query "jazz please".
    let raw = strip_trailing_filler(raw);
    if raw.is_empty() {
        return None;
    }
    let key = raw.to_lowercase();
    if GENRES.contains(&key.as_str()) {
        Some(Selector::Genre(raw.to_string()))
    } else {
        Some(Selector::Query(raw.to_string()))
    }
}

/// Strip trailing punctuation and politeness filler ("please", "thanks", "thank
/// you") from a selector phrase, iterating so "with jazz please." reduces to
/// "jazz". Borrows from the input (no allocation); case-insensitive match on a
/// whole trailing word (never a mid-word suffix).
fn strip_trailing_filler(s: &str) -> &str {
    const FILLER: &[&str] = &["thank you", "thanks", "please"];
    let mut cur = s.trim().trim_end_matches(['.', '!', '?', ',']).trim();
    loop {
        let mut stripped = false;
        for f in FILLER {
            if cur.len() > f.len() && cur.is_char_boundary(cur.len() - f.len()) {
                let split = cur.len() - f.len();
                let head = &cur[..split];
                let tail = &cur[split..];
                // Whole-word boundary: the filler must be preceded by a space so
                // "increase" is never truncated by the "please"... (it is not a
                // suffix here, but the space guard keeps the match honest).
                if tail.eq_ignore_ascii_case(f) && head.ends_with(' ') {
                    cur = head.trim_end().trim_end_matches(['.', '!', '?', ',']).trim_end();
                    stripped = true;
                    break;
                }
            }
        }
        if !stripped {
            return cur;
        }
    }
}

// ── duration / time parsing ───────────────────────────────────────────────────

/// Parse a duration starting at `toks[i]`: a bare number (seconds), a number +
/// unit word ("20 minutes"), or an `s`-suffixed token ("20s"). Returns the
/// seconds and how many tokens were consumed.
fn parse_dur(toks: &[&str], i: usize) -> Option<(f64, usize)> {
    let t = toks.get(i)?;
    if let Some(v) = t.strip_suffix('s') {
        if let Ok(n) = v.parse::<f64>() {
            if n.is_finite() && n >= 0.0 {
                return Some((n, 1));
            }
        }
    }
    let n: f64 = t.parse().ok()?;
    if !n.is_finite() || n < 0.0 {
        return None;
    }
    match toks.get(i + 1).and_then(|u| unit_secs(u)) {
        Some(mult) => Some((n * mult, 2)),
        // A recognized-but-unsupported time unit ("days", "weeks", ...) must NOT
        // collapse to bare seconds - that would emit a confident wrong plan
        // ("in 2 days" -> 2s). Punt so the request falls through to NotUnderstood.
        None if toks.get(i + 1).is_some_and(|u| is_unsupported_time_unit(u)) => None,
        None => Some((n, 1)),
    }
}

fn unit_secs(w: &str) -> Option<f64> {
    match w {
        "second" | "seconds" | "sec" | "secs" | "s" => Some(1.0),
        "minute" | "minutes" | "min" | "mins" | "m" => Some(60.0),
        "hour" | "hours" | "hr" | "hrs" | "h" => Some(3600.0),
        _ => None,
    }
}

/// Time-unit words we deliberately do NOT support. A number followed by one of
/// these is an out-of-range duration request, not a bare-seconds count, so
/// `parse_dur` punts rather than silently dropping the unit.
fn is_unsupported_time_unit(w: &str) -> bool {
    matches!(
        w,
        "day" | "days"
            | "week" | "weeks"
            | "month" | "months"
            | "year" | "years"
            | "millisecond" | "milliseconds" | "ms"
    )
}

/// Parse a clock time at `toks[i]` ("7", "7:30", "7pm", "7:30am") into the NEXT
/// matching absolute UTC instant via `ctx.tz`. A bare hour with no meridian
/// defaults to AM (a wake implies morning); "pm"/"tonight"/"evening" force PM.
fn parse_time(
    toks: &[&str],
    i: usize,
    low: &str,
    ctx: &NlContext,
) -> Option<(chrono::DateTime<Utc>, usize)> {
    let mut raw = toks.get(i)?.to_string();
    let mut consumed = 1;
    let mut meridian: Option<bool> = None; // Some(true) = pm
    if let Some(v) = raw.strip_suffix("pm") {
        meridian = Some(true);
        raw = v.to_string();
    } else if let Some(v) = raw.strip_suffix("am") {
        meridian = Some(false);
        raw = v.to_string();
    }
    let (h, m) = parse_hm(&raw)?;
    if meridian.is_none() {
        match toks.get(i + 1).map(|s| *s) {
            Some("pm") => {
                meridian = Some(true);
                consumed = 2;
            }
            Some("am") => {
                meridian = Some(false);
                consumed = 2;
            }
            _ => {}
        }
    }
    if meridian.is_none() && (low.contains("tonight") || low.contains("evening") || low.contains("pm")) {
        meridian = Some(true);
    }
    let hour24 = apply_meridian(h, meridian.unwrap_or(false))?;
    let at = resolve_next_local(ctx, hour24, m)?;
    Some((at, consumed))
}

fn parse_hm(raw: &str) -> Option<(u32, u32)> {
    match raw.split_once(':') {
        Some((h, m)) => Some((h.parse().ok()?, m.parse().ok()?)),
        None => Some((raw.parse().ok()?, 0)),
    }
}

/// Map a 12-hour clock hour + meridian to a 24-hour hour. `h` must be 1..=12 for a
/// meridian form, but a bare 0..=23 (24h input) is passed through when no meridian
/// was supplied and the hour is already > 12.
fn apply_meridian(h: u32, pm: bool) -> Option<u32> {
    if h > 23 {
        return None;
    }
    if h == 12 {
        return Some(if pm { 12 } else { 0 });
    }
    if h > 12 {
        // Already 24h (e.g. "at 19"); meridian is irrelevant.
        return Some(h);
    }
    Some(if pm { h + 12 } else { h })
}

/// The next local `h:m` (today if still ahead, else tomorrow) as an absolute UTC
/// instant, via the fixed-offset zone. Deterministic under a fixed `ctx.now_civil`.
fn resolve_next_local(ctx: &NlContext, h: u32, m: u32) -> Option<chrono::DateTime<Utc>> {
    let tz = ctx.tz;
    let local_now = ctx.now_civil.with_timezone(&tz);
    let today = local_now.date_naive();
    let naive_today = today.and_hms_opt(h, m, 0)?;
    let dt = match tz.from_local_datetime(&naive_today).single() {
        Some(d) if d > local_now => d,
        _ => {
            let tomorrow = today.succ_opt()?;
            tz.from_local_datetime(&tomorrow.and_hms_opt(h, m, 0)?).single()?
        }
    };
    Some(dt.with_timezone(&Utc))
}
