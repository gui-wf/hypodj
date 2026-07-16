//! Pure NL handshake seams: request quoting/escaping, nl_echo split, token
//! extraction, ACK-reason mapping, and plan_id -> "armed" rendering. All tested
//! with canned strings; no network.

/// Escape a phrase to mirror the daemon tokenizer: FIRST every '\' -> '\\', THEN
/// every '"' -> '\"'. The result is wrapped as a SINGLE quoted argument so the
/// 1-arg `nl "<phrase>"` translate can never be mistaken for the 2-arg
/// keyword_form.
pub fn nl_request(phrase: &str) -> String {
    // Collapse newlines/CR to spaces first (MPD is line-based, no newline
    // escape), then escape backslash + quote.
    let escaped = phrase
        .replace(['\n', '\r'], " ")
        .replace('\\', "\\\\")
        .replace('"', "\\\"");
    format!("nl \"{escaped}\"")
}

/// Quote an arbitrary value as a SINGLE MPD command argument, mirroring the
/// daemon tokenizer (`crates/hypodj-core/src/mpd.rs`): FIRST every '\' -> '\\',
/// THEN every '"' -> '\"', wrapped in double quotes. Without this a value with a
/// space (an album/song uri, a playlist name like `Chill Vibes`) is split by the
/// tokenizer into several args and the server acts on the wrong/truncated value.
/// Newlines/CR collapse to spaces (MPD is line-based, no newline escape).
pub fn quote_arg(value: &str) -> String {
    let escaped = value
        .replace(['\n', '\r'], " ")
        .replace('\\', "\\\\")
        .replace('"', "\\\"");
    format!("\"{escaped}\"")
}

/// Extract the token value (nl-<hex>) from the nl_token pair, if present.
pub fn token_from_pairs(pairs: &[(String, String)]) -> Option<String> {
    pairs.iter().find(|(k, _)| k == "nl_token").map(|(_, v)| v.clone())
}

pub fn echo_from_pairs(pairs: &[(String, String)]) -> Option<String> {
    pairs.iter().find(|(k, _)| k == "nl_echo").map(|(_, v)| v.clone())
}

/// The rendered parts of an nl_echo, split on " | ".
#[derive(Debug, PartialEq, Eq)]
pub struct EchoParts {
    /// The trust footnote: "via rules" or "via local model".
    pub trust: Option<String>,
    /// Each numbered "[n] ..." clause on its own.
    pub steps: Vec<String>,
    /// A "NOTE:" clause (wake caveat) surfaced as a warning above the prompt.
    pub note: Option<String>,
}

/// Split a pipe-joined nl_echo into trust line, numbered steps, and NOTE warning.
pub fn split_echo(echo: &str) -> EchoParts {
    let mut trust = None;
    let mut steps = Vec::new();
    let mut note = None;
    for clause in echo.split(" | ") {
        let c = clause.trim();
        if c == "via rules" || c == "via local model" {
            trust = Some(c.to_string());
        } else if c.starts_with("NOTE:") {
            note = Some(c.to_string());
        } else if !c.is_empty() {
            steps.push(c.to_string());
        }
    }
    EchoParts { trust, steps, note }
}

/// Map a plan_id value to a short human "armed" line (never a raw "plan_id: 3").
pub fn armed_line(plan_id: &str) -> String {
    format!("armed (plan {plan_id})")
}

/// Map an ACK message (extracted after the '}') from an `nl` command to a
/// friendly, actionable string. Keyed on the daemon's NlError Display strings.
pub fn map_ack_reason(msg: &str) -> String {
    let m = msg.trim();
    // Confirm-time token failures.
    if m == "no such nl token" {
        return "that plan expired, run the phrase again".to_string();
    }
    if let Some(rest) = m.strip_prefix("plan no longer valid") {
        // Queue changed since the echo. Surface the reason plainly.
        let reason = rest.trim_start_matches(':').trim();
        if reason.is_empty() {
            return "that plan is no longer valid (the queue changed) - run the phrase again"
                .to_string();
        }
        return format!("that plan is no longer valid: {reason} - run the phrase again");
    }
    // Translate-time NlError reasons (daemon sends the NlError Display string).
    if m == "nl translator not available" {
        return "natural language is not enabled on this server (configure a translator in the daemon)"
            .to_string();
    }
    if m == "not understood" {
        return not_understood_hint();
    }
    if let Some(rest) = m.strip_prefix("ambiguous:") {
        return format!("that request is ambiguous:{rest}");
    }
    if let Some(rest) = m.strip_prefix("nothing to act on:") {
        return format!("nothing to act on:{rest}");
    }
    // Any other ACK: surface plainly.
    m.to_string()
}

/// The friendly miss + supported-shapes hint, also reused by `hjq help`.
pub fn not_understood_hint() -> String {
    let mut s = String::from("I did not understand that. Try things like:\n");
    s.push_str("  - fade out / fade in\n");
    s.push_str("  - fade to a volume (e.g. fade to 30%)\n");
    s.push_str("  - sleep / winddown\n");
    s.push_str("  - stop after this track / stop after this album\n");
    s.push_str("  - wake me at a time with a selector (e.g. wake me at 7 with jazz)\n");
    s.push_str("Or use a control verb: play, pause, stop, next, prev, vol <0-100>, clear, queue, now.");
    s
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn nl_request_escapes_backslash_then_quote() {
        // A phrase with an embedded quote and backslash.
        // input:  a\b"c   -> escape \ first (a\\b"c), then " (a\\b\"c)
        let got = nl_request("a\\b\"c");
        assert_eq!(got, "nl \"a\\\\b\\\"c\"");
    }

    #[test]
    fn quote_arg_wraps_spaces_and_escapes() {
        // A uri/playlist name with a space stays a single tokenizer argument.
        assert_eq!(quote_arg("song/al 1/track 2"), "\"song/al 1/track 2\"");
        assert_eq!(quote_arg("Chill Vibes"), "\"Chill Vibes\"");
        // Backslash escaped before quote, mirroring the daemon tokenizer.
        assert_eq!(quote_arg("a\\b\"c"), "\"a\\\\b\\\"c\"");
    }

    #[test]
    fn nl_request_plain() {
        assert_eq!(nl_request("stop after this album"), "nl \"stop after this album\"");
    }

    #[test]
    fn token_parse() {
        let pairs = vec![
            ("nl_echo".to_string(), "via rules | [1] fade out".to_string()),
            ("nl_token".to_string(), "nl-1a2b3c".to_string()),
        ];
        assert_eq!(token_from_pairs(&pairs), Some("nl-1a2b3c".to_string()));
    }

    #[test]
    fn echo_split_trust_steps_note() {
        let echo = "via rules | [1] fade out over 10s | [2] stop after this track | NOTE: this appends tracks and fades in";
        let parts = split_echo(echo);
        assert_eq!(parts.trust, Some("via rules".to_string()));
        assert_eq!(
            parts.steps,
            vec!["[1] fade out over 10s".to_string(), "[2] stop after this track".to_string()]
        );
        assert_eq!(parts.note, Some("NOTE: this appends tracks and fades in".to_string()));
    }

    #[test]
    fn echo_split_local_model_no_note() {
        let parts = split_echo("via local model | [1] wake at 07:00");
        assert_eq!(parts.trust, Some("via local model".to_string()));
        assert_eq!(parts.steps, vec!["[1] wake at 07:00".to_string()]);
        assert_eq!(parts.note, None);
    }

    #[test]
    fn confirm_plan_id_map() {
        assert_eq!(armed_line("3"), "armed (plan 3)");
    }

    #[test]
    fn ack_reason_map() {
        assert_eq!(
            map_ack_reason("nl translator not available"),
            "natural language is not enabled on this server (configure a translator in the daemon)"
        );
        assert!(map_ack_reason("not understood").contains("fade out"));
        assert!(map_ack_reason("ambiguous: which track").contains("ambiguous"));
        assert!(map_ack_reason("nothing to act on: nothing playing to match")
            .contains("nothing playing to match"));
        assert_eq!(
            map_ack_reason("no such nl token"),
            "that plan expired, run the phrase again"
        );
        assert!(map_ack_reason("plan no longer valid: queue changed").contains("queue changed"));
    }
}
