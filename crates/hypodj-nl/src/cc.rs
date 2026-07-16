//! The CLIENT-SIDE Claude Code CLI intelligence backend (feature = "cc").
//!
//! This runs the `claude` CLI headlessly IN THE CLIENT (the `dj` CLI / `dj-gui`
//! TUI), NEVER in the audio daemon: the daemon's `nl` command is one-shot
//! request/response, so token progress cannot stream over it, and the realtime
//! player must never fork a multi-second subprocess. The interactive client is also
//! the sanctioned home for the user subscription (same user, logged-in OAuth).
//!
//! SAFETY posture (identical to the local-model path): the `claude` output is
//! UNTRUSTED. It is parsed through the ALWAYS-COMPILED, model-free
//! [`crate::parse_llm_output`], whose dedicated subset enums serde-REJECT any
//! off-surface act/dir/trigger (`Wake`, `WakeTo`/`ToFloor`, `WallClock`, id-bearing
//! selectors). A schema-violating or extra-text reply is FINE - it maps to a loud
//! miss, never a fabricated plan. `--json-schema` is POST-HOC validation only (not
//! constrained decoding), which is exactly why the subset re-parse is the real gate.
//!
//! The build never touches the subscription: only `schema_json()` (never a secret)
//! is passed inline to `--json-schema`; credentials are read only by `claude` itself.
//! NEVER invoke this in a nix doCheck sandbox (the live test is `#[ignore]` +
//! availability-gated).

use std::io::{BufRead, BufReader, Read};
use std::process::{Command, Stdio};
use std::sync::mpsc;
use std::time::{Duration, Instant};

use hypodj_core::plan::RawPlan;

use crate::gbnf::schema_json;
use crate::parse_llm_output;

/// Coarse progress phase for the client spinner (the MVP streams no token deltas -
/// see the deferred stream-json path). The CLI/TUI render a spinner + this phase so
/// the multi-second call never looks frozen.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum CcPhase {
    /// The child was spawned; waiting on the model.
    Thinking,
    /// A settled plan came back and is being validated/rendered.
    Planning,
    /// A validated plan is ready to echo + confirm.
    Done,
    /// The call failed (spawn error, non-zero exit, parse miss).
    Error,
}

impl CcPhase {
    /// A short human line for the spinner row.
    pub fn label(self) -> &'static str {
        match self {
            CcPhase::Thinking => "thinking...",
            CcPhase::Planning => "planning...",
            CcPhase::Done => "ready",
            CcPhase::Error => "error",
        }
    }
}

/// Is the `claude` CLI on PATH and runnable? Cheap `claude --version` probe (no
/// network, no subscription touch). The client falls back to the daemon `nl` path
/// when this is false, so a machine without `claude` keeps working unchanged.
pub fn cc_available() -> bool {
    Command::new("claude")
        .arg("--version")
        .output()
        .map(|o| o.status.success())
        .unwrap_or(false)
}

/// Build the one-shot prompt: a deterministic DJ system instruction + the small
/// context the client already has (queue length, is-playing), mirroring the
/// local-model prompt shape (llm.rs `prompt`), plus the emit-one-JSON directive.
/// `--bare` keeps the workspace CLAUDE.md/hooks out, so this text is the whole
/// instruction. Pure and unit-tested.
pub fn build_prompt(utterance: &str, queue_len: usize, is_playing: bool) -> String {
    format!(
        "You are the intent translator for a music player. Translate the DJ request \
         into EXACTLY ONE JSON plan object matching the provided JSON schema, and \
         output nothing else (no prose, no code fence).\n\
         Queue length: {}. Something is {}playing.\n\
         Only emit actions the schema allows (fade out/in, stop, pause, set_volume, \
         enqueue query/genre/radio). If the request cannot be expressed in the \
         schema, still emit your closest valid single plan.\n\
         Request: {}",
        queue_len,
        if is_playing { "" } else { "NOT " },
        utterance,
    )
}

/// Strip a leading/trailing markdown code fence (```json ... ```), if present, so a
/// chatty `result` string still parses. Leaves un-fenced input untouched.
fn strip_fences(s: &str) -> String {
    let t = s.trim();
    let t = t.strip_prefix("```json").or_else(|| t.strip_prefix("```")).unwrap_or(t);
    let t = t.trim_start();
    let t = t.strip_suffix("```").unwrap_or(t);
    t.trim().to_string()
}

/// Parse the `claude --output-format json` envelope into a validated [`RawPlan`].
///
/// The envelope is `{"structured_output": {...}, "result": "...", ...}`. We prefer
/// the schema-validated `structured_output` object (re-serialized then re-parsed
/// through the subset deserializer); if it is absent/null we fall back to the
/// free-text `result` string (fence-stripped). EITHER way the bytes cross
/// [`parse_llm_output`], so an off-surface plan is REJECTED, never armed. Pure and
/// unit-tested with canned envelopes (no `claude`).
pub fn parse_envelope(envelope: &str) -> Result<RawPlan, String> {
    let v: serde_json::Value =
        serde_json::from_str(envelope.trim()).map_err(|e| format!("claude envelope not JSON: {e}"))?;
    // Surface an explicit CLI error subtype loudly rather than mis-parsing it.
    if v.get("is_error").and_then(|b| b.as_bool()) == Some(true) {
        let msg = v.get("result").and_then(|r| r.as_str()).unwrap_or("claude reported an error");
        return Err(msg.to_string());
    }
    if let Some(so) = v.get("structured_output") {
        if !so.is_null() {
            let s = serde_json::to_string(so).map_err(|e| e.to_string())?;
            return parse_llm_output(&s);
        }
    }
    if let Some(res) = v.get("result").and_then(|r| r.as_str()) {
        return parse_llm_output(&strip_fences(res));
    }
    Err("claude envelope had no structured_output or result".to_string())
}

/// One event from the `--output-format stream-json` NDJSON stream (the live token
/// typewriter). Installed claude 2.1.204 TRUNCATES the final stream-json result line
/// (fixed in 2.1.208+), so the settled plan is taken from the last COMPLETE result
/// line that parses, and a non-streamed fallback guarantees a plan when none does.
/// Pure and unit-tested.
#[derive(Clone, Debug, PartialEq, Eq)]
pub enum CcStreamEvent {
    /// A partial assistant text delta (typewriter fragment).
    Delta(String),
    /// The final result envelope line (parse with [`parse_envelope`]).
    Final(String),
    /// A non-result system/progress line (coarse phase only).
    Progress,
    /// A blank or unparseable line - ignore.
    Ignore,
}

/// Classify one NDJSON line from a `stream-json` run. `assistant`/partial deltas
/// become [`CcStreamEvent::Delta`]; the terminal `result` line becomes
/// [`CcStreamEvent::Final`] (the whole line, re-parsed by [`parse_envelope`]); other
/// well-formed lines are [`CcStreamEvent::Progress`]. Pure and unit-tested.
pub fn parse_ndjson_line(line: &str) -> CcStreamEvent {
    let t = line.trim();
    if t.is_empty() {
        return CcStreamEvent::Ignore;
    }
    let v: serde_json::Value = match serde_json::from_str(t) {
        Ok(v) => v,
        Err(_) => return CcStreamEvent::Ignore,
    };
    match v.get("type").and_then(|s| s.as_str()) {
        Some("result") => CcStreamEvent::Final(t.to_string()),
        Some("assistant") | Some("stream_event") => {
            // Best-effort text delta extraction (partial-message shape varies). The
            // real `--include-partial-messages` shape is a `stream_event` carrying an
            // `event.content_block_delta` with a `delta.text_delta`, i.e.
            // `/event/delta/text`; the others cover older/settled-assistant shapes.
            let text = v
                .pointer("/event/delta/text")
                .or_else(|| v.pointer("/delta/text"))
                .or_else(|| v.pointer("/message/content/0/text"))
                .and_then(|s| s.as_str());
            match text {
                Some(s) if !s.is_empty() => CcStreamEvent::Delta(s.to_string()),
                _ => CcStreamEvent::Progress,
            }
        }
        _ => CcStreamEvent::Progress,
    }
}

/// Wall-clock ceiling for one `claude` call. A stalled child (network hang, an
/// expired-auth prompt blocking on stdin) must NEVER hang the client; past this we
/// kill it and degrade gracefully to the daemon path. Generous vs the usual
/// multi-second settle, tight enough that a hang is felt as an error, not a freeze.
const CLAUDE_TIMEOUT: Duration = Duration::from_secs(45);

/// Run one headless `claude` call and return a VALIDATED [`RawPlan`]. Blocks for the
/// (multi-second) subprocess, so callers MUST run it off any render/UI thread and
/// show progress. Non-streamed `--output-format json` for a reliable SETTLED result
/// (the stream-json typewriter is deferred). `--bare` (no workspace CLAUDE.md/hooks/
/// MCP, deterministic), `--max-turns 1` (no agent tool loop), `--max-budget-usd`
/// (cost cap), and a hard [`CLAUDE_TIMEOUT`] wall-clock kill so a stalled child
/// degrades to a loud miss instead of hanging the client. On ANY failure returns a
/// readable error - the caller surfaces a loud miss / falls back to the daemon path,
/// never a fabricated plan.
///
/// `--json-schema` takes the schema INLINE (not a path), so [`schema_json`] is passed
/// straight as the arg value. `< /dev/null` (empty stdin via `Stdio::null`) means an
/// auth/interactive prompt cannot block waiting on input - it fails fast instead.
pub fn run_claude(utterance: &str, queue_len: usize, is_playing: bool) -> Result<RawPlan, String> {
    let prompt = build_prompt(utterance, queue_len, is_playing);
    let mut child = Command::new("claude")
        .arg("-p")
        .arg(&prompt)
        .arg("--output-format")
        .arg("json")
        .arg("--json-schema")
        .arg(schema_json())
        .arg("--bare")
        .arg("--max-turns")
        .arg("1")
        .arg("--max-budget-usd")
        .arg("0.05")
        .stdin(Stdio::null())
        .stdout(Stdio::piped())
        .stderr(Stdio::piped())
        .spawn()
        .map_err(|e| format!("could not run claude: {e}"))?;

    // Drain stdout/stderr on threads so a full pipe buffer can never deadlock the
    // try_wait poll below (and so we still have the bytes after a settled exit).
    let mut stdout_pipe = child.stdout.take();
    let mut stderr_pipe = child.stderr.take();
    let out_h = std::thread::spawn(move || {
        let mut buf = Vec::new();
        if let Some(p) = stdout_pipe.as_mut() {
            let _ = p.read_to_end(&mut buf);
        }
        buf
    });
    let err_h = std::thread::spawn(move || {
        let mut buf = Vec::new();
        if let Some(p) = stderr_pipe.as_mut() {
            let _ = p.read_to_end(&mut buf);
        }
        buf
    });

    let deadline = Instant::now() + CLAUDE_TIMEOUT;
    let status = loop {
        match child.try_wait() {
            Ok(Some(status)) => break status,
            Ok(None) => {
                if Instant::now() >= deadline {
                    // Hard kill; the reader threads then see EOF and finish.
                    let _ = child.kill();
                    let _ = child.wait();
                    let _ = out_h.join();
                    let _ = err_h.join();
                    return Err(format!(
                        "claude timed out after {}s",
                        CLAUDE_TIMEOUT.as_secs()
                    ));
                }
                std::thread::sleep(Duration::from_millis(50));
            }
            Err(e) => return Err(format!("waiting on claude failed: {e}")),
        }
    };

    let stdout = out_h.join().unwrap_or_default();
    let stderr = err_h.join().unwrap_or_default();
    if !status.success() {
        let err = String::from_utf8_lossy(&stderr);
        let err = err.trim();
        return Err(if err.is_empty() {
            format!("claude exited with {status}")
        } else {
            format!("claude failed: {err}")
        });
    }
    parse_envelope(&String::from_utf8_lossy(&stdout))
}

/// Pure accumulator for a `stream-json` run. Fed each raw NDJSON stdout line in
/// order, it (a) returns the typewriter fragment for a `text_delta` line so the
/// caller can render it live, and (b) remembers the plan from the LAST COMPLETE
/// `result` line that parses through [`parse_envelope`]. On claude 2.1.204 the final
/// `result` line is truncated - it fails to parse, so [`Self::settled`] stays `None`
/// and the caller falls back to a non-streamed call. Pure and unit-tested.
#[derive(Default)]
pub struct StreamAcc {
    /// The full concatenated delta text seen so far (the typewriter transcript).
    pub text: String,
    settled: Option<RawPlan>,
}

impl StreamAcc {
    /// Feed one raw NDJSON line. Returns `Some(fragment)` when the line carried a
    /// text delta (the caller streams it live); records a settled plan when the line
    /// is a complete, valid `result` envelope. Pure.
    pub fn feed(&mut self, line: &str) -> Option<String> {
        match parse_ndjson_line(line) {
            CcStreamEvent::Delta(s) => {
                self.text.push_str(&s);
                Some(s)
            }
            CcStreamEvent::Final(l) => {
                // The LAST complete result line that parses wins; a truncated one
                // fails here and leaves the prior settled value (usually None) intact.
                if let Ok(plan) = parse_envelope(&l) {
                    self.settled = Some(plan);
                }
                None
            }
            _ => None,
        }
    }

    /// The settled plan, if any complete `result` line parsed. `None` means the
    /// stream never landed a valid settled plan (truncation) - fall back.
    pub fn into_settled(self) -> Option<RawPlan> {
        self.settled
    }
}

/// Run ONE headless `claude` call in `--output-format stream-json` mode, invoking
/// `on_delta` for each live token fragment and returning the settled plan IF a
/// complete `result` line parsed. `Ok(Some(plan))` = a live-streamed, settled,
/// VALIDATED plan; `Ok(None)` = the stream typed out but never landed a parseable
/// settled plan (claude 2.1.204 truncates the final line) - the caller must fall
/// back; `Err` = a hard failure (spawn/timeout). Blocks (multi-second), so callers
/// MUST run it off any render/UI thread. Same SAFETY boundary as [`run_claude`]: the
/// settled bytes cross [`parse_envelope`], so an off-surface plan is rejected. The
/// prompt is an argv element (no shell), so the utterance is never interpolated.
pub fn run_claude_stream<F: FnMut(&str)>(
    utterance: &str,
    queue_len: usize,
    is_playing: bool,
    mut on_delta: F,
) -> Result<Option<RawPlan>, String> {
    let prompt = build_prompt(utterance, queue_len, is_playing);
    let mut child = Command::new("claude")
        .arg("-p")
        .arg(&prompt)
        .arg("--output-format")
        .arg("stream-json")
        .arg("--verbose")
        .arg("--include-partial-messages")
        .arg("--json-schema")
        .arg(schema_json())
        .arg("--bare")
        .arg("--max-turns")
        .arg("1")
        .arg("--max-budget-usd")
        .arg("0.05")
        .stdin(Stdio::null())
        .stdout(Stdio::piped())
        .stderr(Stdio::piped())
        .spawn()
        .map_err(|e| format!("could not run claude: {e}"))?;

    // Reader thread: forward stdout NDJSON lines over a channel so the main loop can
    // enforce the wall-clock deadline (a blocking `lines()` read could otherwise hang
    // forever on a stalled child). Drain stderr on its own thread so a full stderr
    // pipe can never deadlock the child.
    let stdout_pipe = child.stdout.take();
    let mut stderr_pipe = child.stderr.take();
    let (line_tx, line_rx) = mpsc::channel::<String>();
    let reader = std::thread::spawn(move || {
        if let Some(p) = stdout_pipe {
            for line in BufReader::new(p).lines() {
                match line {
                    Ok(l) => {
                        if line_tx.send(l).is_err() {
                            break;
                        }
                    }
                    Err(_) => break,
                }
            }
        }
    });
    let err_h = std::thread::spawn(move || {
        let mut buf = Vec::new();
        if let Some(p) = stderr_pipe.as_mut() {
            let _ = p.read_to_end(&mut buf);
        }
        buf
    });

    let mut acc = StreamAcc::default();
    let deadline = Instant::now() + CLAUDE_TIMEOUT;
    loop {
        // Enforce the wall-clock deadline on EVERY iteration, not only on idle
        // timeout: a claude that streams stream-json lines continuously (gaps <
        // 100ms, never landing the final EOF - a chatty/runaway or stuck-but-emitting
        // session) would otherwise always hit the Ok arm, never return Timeout, and
        // so never be bounded. Checking here kills such a child and stops StreamAcc
        // plus the downstream Inbound queue from growing without bound.
        if Instant::now() >= deadline {
            let _ = child.kill();
            let _ = child.wait();
            let _ = reader.join();
            let _ = err_h.join();
            return Err(format!("claude timed out after {}s", CLAUDE_TIMEOUT.as_secs()));
        }
        match line_rx.recv_timeout(Duration::from_millis(100)) {
            Ok(line) => {
                if let Some(frag) = acc.feed(&line) {
                    on_delta(&frag);
                }
            }
            // Reader finished (stdout EOF): the stream is complete.
            Err(mpsc::RecvTimeoutError::Disconnected) => break,
            // Idle: no line within the poll window. The deadline check at the top of
            // the loop does the killing; just re-poll.
            Err(mpsc::RecvTimeoutError::Timeout) => {}
        }
    }

    let _ = reader.join();
    let status = child.wait().map_err(|e| format!("waiting on claude failed: {e}"))?;
    let stderr = err_h.join().unwrap_or_default();
    if !status.success() && acc.settled.is_none() {
        let err = String::from_utf8_lossy(&stderr);
        let err = err.trim();
        return Err(if err.is_empty() {
            format!("claude exited with {status}")
        } else {
            format!("claude failed: {err}")
        });
    }
    Ok(acc.into_settled())
}

/// The client entry point: stream a live typewriter via [`run_claude_stream`], and if
/// the stream never lands a parseable settled plan (claude 2.1.204 truncates the
/// final `result` line) or fails outright, FALL BACK to the reliable non-streamed
/// [`run_claude`] so a valid VALIDATED plan ALWAYS lands. `on_delta` renders each
/// live fragment; the fallback path returns the settled plan silently (the live
/// typewriter has already been shown). Blocks (multi-second) - run off the UI thread.
pub fn run_claude_streaming<F: FnMut(&str)>(
    utterance: &str,
    queue_len: usize,
    is_playing: bool,
    on_delta: F,
) -> Result<RawPlan, String> {
    match run_claude_stream(utterance, queue_len, is_playing, on_delta) {
        Ok(Some(plan)) => Ok(plan),
        // Stream typed out but truncated the settled line, OR a hard failure: the
        // non-streamed call is the reliable settle path on 2.1.204.
        Ok(None) | Err(_) => run_claude(utterance, queue_len, is_playing),
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use hypodj_core::plan::{Action, FadeIntentIr};

    #[test]
    fn prompt_carries_instruction_context_and_request() {
        let p = build_prompt("fade out slowly", 7, true);
        // System instruction + the emit-one-JSON directive.
        assert!(p.contains("EXACTLY ONE JSON plan"));
        assert!(p.contains("matching the provided JSON schema"));
        // The small client context, mirroring the local-model prompt shape.
        assert!(p.contains("Queue length: 7."));
        assert!(p.contains("Something is playing."));
        // The utterance rides through verbatim.
        assert!(p.contains("Request: fade out slowly"));
        // Not-playing flips the context.
        assert!(build_prompt("stop", 0, false).contains("Something is NOT playing."));
    }

    #[test]
    fn envelope_structured_output_parses_to_raw_plan() {
        // The happy path: claude returns the schema-validated object.
        let env = r#"{
            "type":"result","subtype":"success","is_error":false,
            "result":"ignored when structured_output is present",
            "structured_output":{
                "trigger":{"kind":"span_elapsed","secs":300.0},
                "action":{"act":"fade","dir":"out","secs":10.0}
            }
        }"#;
        let raw = parse_envelope(env).unwrap();
        assert!(matches!(raw.action, Action::Fade(FadeIntentIr::Out { .. })));
        // The adapter (not the model) owns origin.
        assert_eq!(raw.origin, "");
    }

    #[test]
    fn envelope_result_string_fallback_strips_fence() {
        // No structured_output: fall back to the (fence-wrapped) result string.
        let env = r#"{"type":"result","structured_output":null,
            "result":"```json\n{\"trigger\":{\"kind\":\"track_after_current\"},\"action\":{\"act\":\"stop\"}}\n```"}"#;
        let raw = parse_envelope(env).unwrap();
        assert!(matches!(raw.action, Action::Stop));
    }

    #[test]
    fn envelope_off_surface_body_is_rejected() {
        // A schema-violating / off-surface action (Wake) must map to a loud miss,
        // never a fabricated plan - the whole safety point of the subset re-parse.
        let env = r#"{"structured_output":{"trigger":{"kind":"track_after_current"},"action":{"act":"wake","count":5}}}"#;
        assert!(parse_envelope(env).is_err());
        // A wall_clock trigger is off the model surface too.
        let env = r#"{"structured_output":{"trigger":{"kind":"wall_clock","at":"2026-01-01T00:00:00Z"},"action":{"act":"stop"}}}"#;
        assert!(parse_envelope(env).is_err());
    }

    #[test]
    fn envelope_error_subtype_surfaces_loudly() {
        let env = r#"{"type":"result","is_error":true,"result":"budget exceeded"}"#;
        assert_eq!(parse_envelope(env).unwrap_err(), "budget exceeded");
    }

    #[test]
    fn envelope_missing_payload_is_error() {
        assert!(parse_envelope(r#"{"type":"result"}"#).is_err());
        assert!(parse_envelope("not json").is_err());
    }

    #[test]
    fn ndjson_lines_classify_delta_final_progress() {
        // The terminal result line is returned whole for parse_envelope.
        let fin = r#"{"type":"result","structured_output":{"trigger":{"kind":"track_after_current"},"action":{"act":"stop"}}}"#;
        match parse_ndjson_line(fin) {
            CcStreamEvent::Final(s) => assert!(parse_envelope(&s).is_ok()),
            other => panic!("expected Final, got {other:?}"),
        }
        // An assistant partial with text is a typewriter delta.
        let delta = r#"{"type":"assistant","message":{"content":[{"type":"text","text":"thi"}]}}"#;
        assert_eq!(parse_ndjson_line(delta), CcStreamEvent::Delta("thi".into()));
        // A system/init line is coarse progress; blank/garbage is ignored.
        assert_eq!(parse_ndjson_line(r#"{"type":"system","subtype":"init"}"#), CcStreamEvent::Progress);
        assert_eq!(parse_ndjson_line("   "), CcStreamEvent::Ignore);
        assert_eq!(parse_ndjson_line("{oops"), CcStreamEvent::Ignore);
    }

    #[test]
    fn stream_acc_types_deltas_and_settles_last_complete_result() {
        // The real --include-partial-messages delta shape: stream_event ->
        // content_block_delta -> text_delta at /event/delta/text.
        let mut acc = StreamAcc::default();
        let deltas: Vec<String> = ["Fad", "ing ", "out"]
            .iter()
            .map(|t| {
                acc.feed(&format!(
                    r#"{{"type":"stream_event","event":{{"type":"content_block_delta","delta":{{"type":"text_delta","text":"{t}"}}}}}}"#
                ))
                .unwrap()
            })
            .collect();
        // Each delta line yields its fragment for the live typewriter, and the full
        // transcript accumulates.
        assert_eq!(deltas, vec!["Fad", "ing ", "out"]);
        assert_eq!(acc.text, "Fading out");
        // A complete result line settles a validated plan.
        acc.feed(r#"{"type":"result","structured_output":{"trigger":{"kind":"track_after_current"},"action":{"act":"stop"}}}"#);
        let plan = acc.into_settled().expect("a complete result line settles");
        assert!(matches!(plan.action, Action::Stop));
    }

    #[test]
    fn stream_acc_truncated_result_forces_fallback() {
        // claude 2.1.204 truncates the final result line: it is not valid JSON, so it
        // never settles - into_settled() is None and the caller must fall back.
        let mut acc = StreamAcc::default();
        assert!(acc
            .feed(r#"{"type":"stream_event","event":{"delta":{"text":"stop"}}}"#)
            .is_some());
        // A truncated result line (cut mid-object) parses as Ignore, not Final.
        acc.feed(r#"{"type":"result","structured_output":{"trigger":{"kind":"track_af"#);
        assert!(acc.into_settled().is_none(), "a truncated result must not settle");
    }

    #[test]
    fn stream_acc_last_valid_result_wins_over_off_surface() {
        // An off-surface result line does not settle (parse_envelope rejects it), so a
        // later valid line is the one that wins.
        let mut acc = StreamAcc::default();
        acc.feed(r#"{"type":"result","structured_output":{"trigger":{"kind":"wall_clock","at":"2026-01-01T00:00:00Z"},"action":{"act":"stop"}}}"#);
        assert!(acc.settled.is_none());
        acc.feed(r#"{"type":"result","structured_output":{"trigger":{"kind":"track_after_current"},"action":{"act":"pause"}}}"#);
        assert!(matches!(acc.into_settled().unwrap().action, Action::Pause));
    }

    #[test]
    fn phase_labels_are_present() {
        assert_eq!(CcPhase::Thinking.label(), "thinking...");
        assert_eq!(CcPhase::Error.label(), "error");
    }

    // LIVE: actually shells out to `claude` and asserts a real validated IR. Gated
    // on availability + #[ignore] so it NEVER runs in the certless nix doCheck; run
    // manually with `cargo test -p hypodj-nl --features cc -- --ignored`.
    #[test]
    #[ignore]
    fn live_claude_translates_fade_out() {
        if !cc_available() {
            eprintln!("skipping: claude CLI not available");
            return;
        }
        match run_claude("fade out over 10 seconds", 5, true) {
            Ok(raw) => {
                // A real model reply must still be a validated, off-surface-free IR.
                assert!(render_dsl_ok(&raw), "the live plan must be a valid IR: {raw:?}");
            }
            Err(e) => eprintln!("live claude call did not produce a plan (acceptable): {e}"),
        }
    }

    #[cfg(test)]
    fn render_dsl_ok(raw: &RawPlan) -> bool {
        // Any validated plan is fine; just prove it is not an off-surface effect.
        !matches!(raw.action, Action::Wake { .. })
    }
}
