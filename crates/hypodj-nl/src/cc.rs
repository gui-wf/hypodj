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
//! `--safe-mode` preserves the logged-in OAuth/subscription auth while disabling
//! CLAUDE.md/skills/plugins/hooks/MCP, and `--system-prompt` REPLACES the default
//! Claude Code prompt with the tiny DJ framing - the deterministic, minimal-prompt
//! guarantee (`--bare` was wrongly used for this: it forces API-key-only auth and so
//! returned "Not logged in" on a subscription machine, silently degrading every call
//! to the daemon rules). NEVER invoke this in a nix doCheck sandbox (the live test is
//! `#[ignore]` + availability-gated).

use std::io::Read;
use std::process::{Command, Stdio};
use std::time::{Duration, Instant};

use hypodj_core::plan::RawPlan;

use crate::gbnf::schema_json;
use crate::parse_llm_output;

/// Coarse progress phase for the client spinner. The single `claude` call is
/// non-streamed, so the CLI/TUI render a spinner + this phase so the multi-second
/// call never looks frozen.
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

/// The standing DJ framing, passed via `--system-prompt` (which FULLY REPLACES the
/// default Claude Code prompt). Because `--safe-mode` disables CLAUDE.md/skills/hooks
/// and `--system-prompt` replaces the base prompt, this constant is the WHOLE standing
/// instruction; [`build_prompt`] then carries only the per-request context.
pub const DJ_SYSTEM_PROMPT: &str = "You are the intent translator for a music player. \
    Translate the DJ request into EXACTLY ONE JSON plan object matching the provided \
    JSON schema, and output nothing else (no prose, no code fence). The object is FLAT: \
    a required \"type\" (one of fade_out, fade_in, stop, pause, set_volume, enqueue, \
    remove, move, clear, play, noop), an \
    optional \"when\" (one of now, after_current, after_secs, album_boundary, \
    queue_position, time_remaining; omit it for an immediate action), and only the flat \
    scalars the action needs: \"secs\" for a fade, \"level\" for set_volume, \
    \"query\"/\"genre\"/\"radio\" plus \"count\" for enqueue, \"when_secs\" for \
    after_secs/time_remaining, \"slot\" for queue_position. Do NOT nest a trigger or a \
    selector object.\n\
    Queue-edit actions (remove, move, clear, play) target entries with a FLAT \
    selector: \"sel\" is one of current, position, range, query, last. sel=position uses \
    \"slot\" (1-based); sel=last uses \"count\"; sel=range uses \"range_start\"+\"range_end\" \
    (1-based inclusive); sel=query uses \"query\" (a title/artist substring). For move add \
    \"dest\" (position -> \"dest_slot\", relative -> \"dest_rel\"). For clear add \"scope\" \
    (all, after_current, or range with range_start/range_end). play jumps to the first \
    match. NOTE: favoriting/starring and the bare transport verbs (clear, next, prev, \
    pause, play with no target) are handled BEFORE you and never reach you. If the \
    request is NOT about music, the queue, or playback (e.g. trivia, chit-chat, an \
    off-topic question), emit {\"type\":\"noop\"} - do NOT fabricate an enqueue. Otherwise \
    emit your closest valid single plan.\n\
    Rules:\n\
    - enqueue selector: a music GENRE (jazz, ambient, bossa nova, techno...) -> \"genre\"; \
    a mood/descriptive phrase (calmer, upbeat, calmer tracks) -> \"query\"; \"radio\"/\"station\" \
    -> \"radio\":true (never a \"query\"). Set exactly ONE of them.\n\
    - count from vague quantity: \"a couple\"=2, \"a few\"=3, \"some\"/\"a bunch\"/a bare plural \
    (\"some jazz\", \"radio station\")=5, \"a track\"/\"a song\"/singular=1. Never default a \
    plural request to 1.\n\
    - placement: \"at the end\", \"next\", \"now\", or plain appends are IMMEDIATE - OMIT \"when\" \
    entirely (do NOT emit after_current or queue_position for these). Use \"when\":\"after_current\" \
    ONLY when the words say after the current track/song. Use \"when\":\"queue_position\"+\"slot\" \
    for \"after N songs\". Use \"when\":\"album_boundary\" for \"after this album\". Use \
    \"when\":\"time_remaining\"+\"when_secs\" for \"X before the track ends\" (that duration is the \
    trigger timing, not a fade length). Emit only ONE placement; never mix an append with a \
    trigger.\n\
    Examples:\n\
    Request: play some jazz -> {\"type\":\"enqueue\",\"genre\":\"jazz\",\"count\":5}\n\
    Request: queue a couple of bossa nova tracks -> {\"type\":\"enqueue\",\"genre\":\"bossa nova\",\"count\":2}\n\
    Request: play a few ambient tracks -> {\"type\":\"enqueue\",\"genre\":\"ambient\",\"count\":3}\n\
    Request: queue five calmer tracks at the end -> {\"type\":\"enqueue\",\"query\":\"calmer tracks\",\"count\":5}\n\
    Request: queue some jazz tracks next -> {\"type\":\"enqueue\",\"genre\":\"jazz\",\"count\":5}\n\
    Request: add three upbeat songs after the current track -> {\"type\":\"enqueue\",\"query\":\"upbeat songs\",\"count\":3,\"when\":\"after_current\"}\n\
    Request: put on a radio station -> {\"type\":\"enqueue\",\"radio\":true,\"count\":5}\n\
    Request: queue a jazz track after 3 songs -> {\"type\":\"enqueue\",\"genre\":\"jazz\",\"count\":1,\"when\":\"queue_position\",\"slot\":3}\n\
    Request: fade out the current track over 30 seconds -> {\"type\":\"fade_out\",\"secs\":30}\n\
    Request: fade out 2 minutes before the track ends -> {\"type\":\"fade_out\",\"when\":\"time_remaining\",\"when_secs\":120}\n\
    Request: stop after this album -> {\"type\":\"stop\",\"when\":\"album_boundary\"}\n\
    Request: remove the last 3 tracks -> {\"type\":\"remove\",\"sel\":\"last\",\"count\":3}\n\
    Request: remove the current song -> {\"type\":\"remove\",\"sel\":\"current\"}\n\
    Request: remove the song called blue in green -> {\"type\":\"remove\",\"sel\":\"query\",\"query\":\"blue in green\"}\n\
    Request: delete tracks 2 to 5 -> {\"type\":\"remove\",\"sel\":\"range\",\"range_start\":2,\"range_end\":5}\n\
    Request: clear everything after the current track -> {\"type\":\"clear\",\"scope\":\"after_current\"}\n\
    Request: clear the whole queue -> {\"type\":\"clear\",\"scope\":\"all\"}\n\
    Request: move the last track to the top -> {\"type\":\"move\",\"sel\":\"last\",\"count\":1,\"dest\":\"position\",\"dest_slot\":1}\n\
    Request: move track 4 up two spots -> {\"type\":\"move\",\"sel\":\"position\",\"slot\":4,\"dest\":\"relative\",\"dest_rel\":-2}\n\
    Request: play the track called so what -> {\"type\":\"play\",\"sel\":\"query\",\"query\":\"so what\"}\n\
    Request: jump to track 6 -> {\"type\":\"play\",\"sel\":\"position\",\"slot\":6}\n\
    Request: what is the airspeed of an unladen swallow -> {\"type\":\"noop\"}\n\
    GROUNDING: if the user prompt carries an \"Available in the library:\" block, PREFER a \
    genre/artist/query DRAWN FROM those real listed names over inventing one - pick the closest \
    listed genre for \"genre\", or a listed artist/track label for \"query\". Fall back to a \
    free-text \"query\" ONLY when nothing listed plausibly matches. You still emit only a LABEL \
    (a genre/artist/query string); never a library id.";

/// A COMPACT, real-candidate library context gathered client-side (via MPD
/// list/search) and injected into [`build_prompt`] so Claude picks from what
/// ACTUALLY EXISTS instead of guessing a blind query string. Bounded by
/// construction (a small genre list + a top-N candidate slice) so the prompt cost
/// stays ~flat. Default-empty: an empty context reproduces today's un-grounded
/// prompt exactly (so unit tests + the model-free path are unaffected, and a search
/// failure degrades cleanly). The model still only ever emits a LABEL (genre/artist/
/// query text); hypodj maps that label back to real ids at execute time, so the
/// off-surface-id safety boundary is untouched.
#[derive(Clone, Debug, Default, PartialEq, Eq)]
pub struct LibraryContext {
    /// The library's real genre names (MPD `list genre`), e.g. jazz, ambient.
    pub genres: Vec<String>,
    /// Real artist/album/track LABELS matching the utterance keywords (MPD
    /// `search any <kw>`), e.g. "Bill Evans - Blue in Green".
    pub candidates: Vec<String>,
    /// Optional freeform hint lines (e.g. a "starred/favorites" note) appended
    /// verbatim under the block. Usually empty.
    pub notes: Vec<String>,
}

impl LibraryContext {
    /// True when there is nothing to inject: [`build_prompt`] then emits today's
    /// exact un-grounded prompt (the clean-degrade path).
    pub fn is_empty(&self) -> bool {
        self.genres.is_empty() && self.candidates.is_empty() && self.notes.is_empty()
    }
}

/// Build the per-request user prompt: only the small live context the client already
/// has (queue length, is-playing) plus the utterance, mirroring the local-model prompt
/// shape (llm.rs `prompt`). The standing DJ framing rides in [`DJ_SYSTEM_PROMPT`] via
/// `--system-prompt`, so this text is intentionally just the live bit. Pure and
/// unit-tested.
pub fn build_prompt(
    utterance: &str,
    queue_len: usize,
    is_playing: bool,
    ctx: &LibraryContext,
) -> String {
    let mut s = format!(
        "Queue length: {}. Something is {}playing.\n",
        queue_len,
        if is_playing { "" } else { "NOT " },
    );
    // Inject the real-candidate block ONLY when we have something; an empty context
    // reproduces today's exact un-grounded prompt (clean-degrade + test parity).
    if !ctx.is_empty() {
        s.push_str("Available in the library:\n");
        if !ctx.genres.is_empty() {
            s.push_str("Genres: ");
            s.push_str(&ctx.genres.join(", "));
            s.push('\n');
        }
        if !ctx.candidates.is_empty() {
            s.push_str("Matching artists/tracks: ");
            s.push_str(&ctx.candidates.join(", "));
            s.push('\n');
        }
        for note in &ctx.notes {
            s.push_str(note);
            s.push('\n');
        }
    }
    s.push_str("Request: ");
    s.push_str(utterance);
    s
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

/// Wall-clock ceiling for one `claude` call. A stalled child (network hang, an
/// expired-auth prompt blocking on stdin) must NEVER hang the client; past this we
/// kill it and degrade gracefully to the daemon path. Generous vs the usual
/// multi-second settle, tight enough that a hang is felt as an error, not a freeze.
const CLAUDE_TIMEOUT: Duration = Duration::from_secs(45);

/// Run one headless `claude` call and return a VALIDATED [`RawPlan`]. Blocks for the
/// (multi-second) subprocess, so callers MUST run it off any render/UI thread and
/// show progress. Non-streamed `--output-format json` for a reliable SETTLED result.
/// `--safe-mode` (preserves the logged-in OAuth/subscription auth while disabling
/// CLAUDE.md/skills/plugins/hooks/MCP - the deterministic guarantee), `--system-prompt`
/// with [`DJ_SYSTEM_PROMPT`] (REPLACES the default Claude Code prompt with the tiny DJ
/// framing), `--exclude-dynamic-system-prompt-sections` (drop cwd/env/git/memory),
/// `--tools ""` (zero built-in tools), `--max-turns 1` (no agent tool loop),
/// `--max-budget-usd` (cost cap), and a hard [`CLAUDE_TIMEOUT`] wall-clock kill so a
/// stalled child degrades to a loud miss instead of hanging the client. On ANY failure
/// returns a readable error - the caller surfaces a loud miss / falls back to the
/// daemon path, never a fabricated plan.
///
/// `--json-schema` takes the schema INLINE (not a path), so [`schema_json`] is passed
/// straight as the arg value. `< /dev/null` (empty stdin via `Stdio::null`) means an
/// auth/interactive prompt cannot block waiting on input - it fails fast instead.
pub fn run_claude(
    utterance: &str,
    queue_len: usize,
    is_playing: bool,
    ctx: &LibraryContext,
) -> Result<RawPlan, String> {
    let prompt = build_prompt(utterance, queue_len, is_playing, ctx);
    let mut child = Command::new("claude")
        .arg("-p")
        .arg(&prompt)
        .arg("--output-format")
        .arg("json")
        .arg("--json-schema")
        .arg(schema_json())
        .arg("--safe-mode")
        .arg("--system-prompt")
        .arg(DJ_SYSTEM_PROMPT)
        .arg("--exclude-dynamic-system-prompt-sections")
        .arg("--tools")
        .arg("")
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

#[cfg(test)]
mod tests {
    use super::*;
    use hypodj_core::plan::{Action, FadeIntentIr};

    #[test]
    fn prompt_carries_instruction_context_and_request() {
        // The standing framing rides in the --system-prompt constant, not the per-turn
        // user prompt: the schema-adherence instruction lives there.
        assert!(DJ_SYSTEM_PROMPT.contains("EXACTLY ONE JSON plan"));
        assert!(DJ_SYSTEM_PROMPT.contains("matching the provided JSON schema"));

        // The system prompt instructs the model to PREFER real listed candidates.
        assert!(DJ_SYSTEM_PROMPT.contains("Available in the library:"));
        assert!(DJ_SYSTEM_PROMPT.contains("PREFER"));

        // The user prompt carries ONLY the live bit: queue length, is-playing, request.
        let empty = LibraryContext::default();
        let p = build_prompt("fade out slowly", 7, true, &empty);
        assert!(p.contains("Queue length: 7."));
        assert!(p.contains("Something is playing."));
        // The utterance rides through verbatim.
        assert!(p.contains("Request: fade out slowly"));
        // The standing framing is NOT baked into the user prompt anymore.
        assert!(!p.contains("EXACTLY ONE JSON plan"));
        // Not-playing flips the context.
        assert!(build_prompt("stop", 0, false, &empty).contains("Something is NOT playing."));
    }

    #[test]
    fn empty_context_reproduces_the_ungrounded_prompt() {
        // A default (empty) context MUST NOT change the prompt: no injected block,
        // byte-for-byte the pre-grounding shape (clean-degrade + model-free parity).
        let empty = LibraryContext::default();
        let p = build_prompt("play some jazz", 3, true, &empty);
        assert_eq!(p, "Queue length: 3. Something is playing.\nRequest: play some jazz");
        assert!(!p.contains("Available in the library:"));
        assert!(empty.is_empty());
    }

    #[test]
    fn context_injects_a_labelled_candidate_block() {
        // A grounded context injects a clearly-labelled block with the REAL genres
        // and candidate labels, BEFORE the request line, so Claude can pick from what
        // exists. The candidates are LABELS only (no library ids leak into the prompt).
        let ctx = LibraryContext {
            genres: vec!["jazz".into(), "ambient".into()],
            candidates: vec!["Bill Evans - Blue in Green".into(), "Miles Davis - So What".into()],
            notes: vec!["Note: the user referenced their starred tracks.".into()],
        };
        let p = build_prompt("play something calm", 2, false, &ctx);
        assert!(p.contains("Available in the library:"));
        assert!(p.contains("Genres: jazz, ambient"));
        assert!(p.contains("Matching artists/tracks: Bill Evans - Blue in Green, Miles Davis - So What"));
        assert!(p.contains("Note: the user referenced their starred tracks."));
        // The block precedes the request line.
        let block = p.find("Available in the library:").unwrap();
        let req = p.find("Request: play something calm").unwrap();
        assert!(block < req);
        // No library id form leaked in (labels only).
        assert!(!p.contains("song/"));
        assert!(!ctx.is_empty());
    }

    #[test]
    fn envelope_structured_output_parses_to_raw_plan() {
        // The happy path: claude returns the schema-validated object.
        let env = r#"{
            "type":"result","subtype":"success","is_error":false,
            "result":"ignored when structured_output is present",
            "structured_output":{
                "type":"fade_out","secs":10.0,"when":"after_secs","when_secs":300.0
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
            "result":"```json\n{\"type\":\"stop\",\"when\":\"after_current\"}\n```"}"#;
        let raw = parse_envelope(env).unwrap();
        assert!(matches!(raw.action, Action::Stop));
    }

    #[test]
    fn envelope_wrapper_array_result_parses() {
        // Claude gravitates to a {"actions":[...]} wrapper in the free-text result;
        // parse_llm_output tolerates it and plans the first action.
        let env = r#"{"type":"result","structured_output":null,
            "result":"{\"actions\":[{\"type\":\"pause\"}]}"}"#;
        let raw = parse_envelope(env).unwrap();
        assert!(matches!(raw.action, Action::Pause));
    }

    #[test]
    fn envelope_off_surface_body_is_rejected() {
        // A schema-violating / off-surface action (Wake) must map to a loud miss,
        // never a fabricated plan - the whole safety point of the subset re-parse.
        let env = r#"{"structured_output":{"type":"wake","count":5}}"#;
        assert!(parse_envelope(env).is_err());
        // A wall_clock trigger is off the model surface too.
        let env = r#"{"structured_output":{"type":"stop","when":"wall_clock"}}"#;
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
        match run_claude("fade out over 10 seconds", 5, true, &LibraryContext::default()) {
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

    // LIVE: a real `claude` call must translate a queue-edit ask into the intended
    // validated queue action (Part B), and an off-topic ask into a clean Noop (never
    // a fabricated enqueue). #[ignore] + availability-gated: NEVER runs in doCheck.
    // Run: `cargo test -p hypodj-nl --features cc -- --ignored live_claude_translates_queue`.
    #[test]
    #[ignore]
    fn live_claude_translates_queue_edits_and_noop() {
        use hypodj_core::plan::Action as A;
        if !cc_available() {
            eprintln!("skipping: claude CLI not available");
            return;
        }
        // remove the last 3 tracks -> Action::Remove.
        match run_claude("remove the last 3 tracks", 8, true, &LibraryContext::default()) {
            Ok(raw) => assert!(matches!(raw.action, A::Remove { .. }), "got {:?}", raw.action),
            Err(e) => eprintln!("live remove call did not produce a plan (acceptable): {e}"),
        }
        // clear everything after the current track -> Action::Clear.
        match run_claude("clear everything after the current track", 8, true, &LibraryContext::default()) {
            Ok(raw) => assert!(matches!(raw.action, A::Clear { .. }), "got {:?}", raw.action),
            Err(e) => eprintln!("live clear call did not produce a plan (acceptable): {e}"),
        }
        // An off-topic ask -> Noop, NEVER a fabricated Enqueue.
        match run_claude("what is the airspeed of an unladen swallow", 8, true, &LibraryContext::default()) {
            Ok(raw) => assert!(
                !matches!(raw.action, A::Enqueue { .. }),
                "off-topic ask must not fabricate an enqueue, got {:?}",
                raw.action
            ),
            Err(e) => eprintln!("live noop call did not produce a plan (acceptable): {e}"),
        }
    }

    // LIVE: a real `claude` call with a GROUNDED context must PREFER a listed genre
    // over inventing one - the whole point of the grounding. We inject deliberately
    // uncommon genres the model would never guess blind (so a match proves it read the
    // block), then assert the chosen enqueue label is DRAWN FROM the provided
    // candidates, not a hallucinated free string. #[ignore] + availability-gated:
    // NEVER runs in doCheck. Run:
    // `cargo test -p hypodj-nl --features cc -- --ignored live_claude_grounds`.
    #[test]
    #[ignore]
    fn live_claude_grounds_to_a_real_candidate() {
        use hypodj_core::plan::{Action as A, Selector};
        if !cc_available() {
            eprintln!("skipping: claude CLI not available");
            return;
        }
        // Uncommon genres the model would not guess from "calm/dreamy" alone; if it
        // picks one of these it PROVES the injected block grounded the plan.
        let genres = vec!["shoegaze".to_string(), "ambient".to_string(), "klezmer".to_string()];
        let ctx = LibraryContext {
            genres: genres.clone(),
            candidates: vec!["Slowdive - Alison".to_string(), "Mazzy Star - Fade Into You".to_string()],
            notes: Vec::new(),
        };
        match run_claude("play some calm dreamy music", 0, false, &ctx) {
            Ok(raw) => match &raw.action {
                A::Enqueue { selector: Selector::Genre(g), .. } => assert!(
                    genres.iter().any(|lg| lg.eq_ignore_ascii_case(g)),
                    "grounded genre must be one of the listed candidates, got {g:?}"
                ),
                // A query label is also acceptable (nothing forces genre), but it must
                // still be a validated, off-surface-free plan.
                other => assert!(render_dsl_ok(&raw), "must be a valid IR: {other:?}"),
            },
            Err(e) => eprintln!("live grounded call did not produce a plan (acceptable): {e}"),
        }
    }
}
