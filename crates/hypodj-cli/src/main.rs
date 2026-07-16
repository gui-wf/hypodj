//! dj - the hypodj jukebox CLI. Human-native + natural-language-first: say what
//! you want. A bare control verb (play/pause/stop/next/prev/vol/clear/queue/now)
//! runs directly; anything else is sent to the daemon as `nl "<phrase>"`, echoed,
//! and confirmed y/N. Blocking, one-shot, ONE persistent socket per invocation.

mod render;

use std::io::{BufRead, Write};

use hypodj_client::config::{self, Env};
use hypodj_client::mpd::{MpdConn, MpdError};
use hypodj_client::route::{self, Action};
use hypodj_client::{model, nl};

const HELP: &str = "\
dj - hypodj jukebox

USAGE:
  dj                      show the now-playing card
  dj now | status         show the now-playing card
  dj queue                list the queue
  dj play | pause | stop  playback control
  dj next | prev          skip / go back (also \"next song\", \"skip this\")
  dj fav | favorite       favorite the current track (also \"fav current\")
  dj vol <0-100>          set volume
  dj clear                clear the queue (asks first)
  dj <anything else>      natural language: e.g. \"fade out\", \"stop after this
                           album\", \"wake me at 7 with jazz\" - echoed + confirmed

OPTIONS:
  --host <h>    daemon host (default 127.0.0.1)
  --port <p>    daemon port (default 6600, matches the live deploy; a DEV daemon
                defaults to 6601 - point at it with HYPODJ_PORT=6601)
  -h, --help    this help

CONFIG precedence: flags > HYPODJ_HOST/HYPODJ_PORT > MPD_HOST/MPD_PORT
                   > 127.0.0.1:6600
";

fn main() {
    let raw: Vec<String> = std::env::args().skip(1).collect();
    match run(raw) {
        Ok(()) => {}
        Err(e) => {
            eprintln!("dj: {e}");
            std::process::exit(1);
        }
    }
}

/// Parse leading --host/--port/--help flags, leaving the phrase words.
struct Parsed {
    host: Option<String>,
    port: Option<u16>,
    help: bool,
    words: Vec<String>,
}

fn parse_args(raw: Vec<String>) -> Result<Parsed, String> {
    let mut host = None;
    let mut port = None;
    let mut help = false;
    let mut words = Vec::new();
    let mut it = raw.into_iter();
    while let Some(a) = it.next() {
        match a.as_str() {
            "--host" => host = Some(it.next().ok_or("--host needs a value")?),
            "--port" => {
                let v = it.next().ok_or("--port needs a value")?;
                port = Some(v.parse::<u16>().map_err(|_| format!("bad port: {v}"))?);
            }
            "-h" | "--help" => help = true,
            // Everything after the first non-flag word is part of the phrase.
            _ => {
                words.push(a);
                words.extend(it.by_ref());
                break;
            }
        }
    }
    Ok(Parsed { host, port, help, words })
}

fn run(raw: Vec<String>) -> Result<(), MpdError> {
    let parsed = match parse_args(raw) {
        Ok(p) => p,
        Err(e) => return Err(MpdError::Io(e)),
    };
    if parsed.help {
        print!("{HELP}");
        return Ok(());
    }

    let action = route::route(&parsed.words);
    if let Action::Help = action {
        print!("{HELP}");
        println!("\n{}", nl::not_understood_hint());
        return Ok(());
    }

    let env = Env { get: &|k| std::env::var(k).ok() };
    let (host, port) = config::resolve(parsed.host, parsed.port, &env);
    let mut conn = MpdConn::connect(&host, port)?;

    match action {
        Action::NowPlaying => print_card(&mut conn)?,
        Action::Queue => {
            let pairs = conn.command("playlistinfo")?;
            println!("{}", render::render_queue(&pairs));
        }
        Action::Command(line) => {
            conn.command(&line)?;
            print_card(&mut conn)?;
        }
        Action::ClearConfirm => {
            if confirm("clear the whole queue?") {
                conn.command("clear")?;
                print_card(&mut conn)?;
            } else {
                println!("cancelled");
            }
        }
        Action::FavoriteCurrent => favorite_current(&mut conn)?,
        Action::Nl(phrase) => nl_handshake(&mut conn, &phrase)?,
        Action::Help => unreachable!(),
    }
    Ok(())
}

/// Star the currently playing track. The server exposes only
/// `playlistadd Starred <uri>` (no favorite-current shorthand), so resolve the
/// current song's uri from `currentsong` first. A raw stream has no star surface.
fn favorite_current(conn: &mut MpdConn) -> Result<(), MpdError> {
    let current = conn.command("currentsong")?;
    let np = model::now_playing(&[], &current);
    let uri = match np.file.as_deref() {
        Some(u) => u,
        None => {
            println!("nothing is playing to favorite");
            return Ok(());
        }
    };
    if !uri.starts_with("song/") {
        // A stream (http(s) URL) is not a library song - no Subsonic star.
        println!("the current track is a stream, which can't be favorited");
        return Ok(());
    }
    match conn.command(&format!("playlistadd Starred {uri}")) {
        Ok(_) => {
            let label = np.title.as_deref().unwrap_or(uri);
            println!("favorited: {label}");
            print_card(conn)?;
        }
        Err(MpdError::Ack(msg)) => println!("could not favorite: {msg}"),
        Err(e) => return Err(e),
    }
    Ok(())
}

/// Fetch status + currentsong on the SAME connection and print the card.
fn print_card(conn: &mut MpdConn) -> Result<(), MpdError> {
    let status = conn.command("status")?;
    let current = conn.command("currentsong")?;
    let np = model::now_playing(&status, &current);
    println!("{}", render::render_card(&np));
    Ok(())
}

/// The full NL handshake, all on the one open socket. Under `cc` (and only when the
/// `claude` CLI is present) the phrase is first translated CLIENT-SIDE by Claude
/// Code into a validated Plan IR, echoed + confirmed here, and armed via a normal
/// `plan add <dsl>` (re-clamped + dry-run validated daemon-side, the same trust
/// boundary as `nl confirm`). When `cc` is off, `claude` is absent, or the call
/// fails, it falls through to today's daemon `nl` path unchanged.
fn nl_handshake(conn: &mut MpdConn, phrase: &str) -> Result<(), MpdError> {
    #[cfg(feature = "cc")]
    {
        if hypodj_nl::cc::cc_available() {
            match cc_nl_handshake(conn, phrase)? {
                true => return Ok(()),
                // The CC call failed (spawn/parse/no-DSL); fall through to the daemon.
                false => {}
            }
        }
    }
    let req = nl::nl_request(phrase);
    let pairs = match conn.command(&req) {
        Ok(p) => p,
        // An ACK here is a translate failure: map to a friendly reason.
        Err(MpdError::Ack(msg)) => {
            println!("{}", nl::map_ack_reason(&msg));
            return Ok(());
        }
        Err(e) => return Err(e),
    };

    let token = match nl::token_from_pairs(&pairs) {
        Some(t) => t,
        None => {
            println!("the server did not return a plan to confirm");
            return Ok(());
        }
    };

    if let Some(echo) = nl::echo_from_pairs(&pairs) {
        let parts = nl::split_echo(&echo);
        if let Some(trust) = &parts.trust {
            println!("({trust})");
        }
        for step in &parts.steps {
            println!("{step}");
        }
        // Wake caveat surfaced as a warning ABOVE the prompt.
        if let Some(note) = &parts.note {
            println!("\n! {note}");
        }
    }

    if confirm("confirm?") {
        match conn.command(&format!("nl confirm {token}")) {
            Ok(plan_pairs) => {
                for (k, v) in &plan_pairs {
                    if k == "plan_id" {
                        println!("{}", nl::armed_line(v));
                    }
                }
                print_card(conn)?;
            }
            Err(MpdError::Ack(msg)) => println!("{}", nl::map_ack_reason(&msg)),
            Err(e) => return Err(e),
        }
    } else {
        // Best-effort cancel on the open connection before exiting.
        let _ = conn.command(&format!("nl cancel {token}"));
        println!("cancelled");
    }
    Ok(())
}

/// The Claude Code client-side NL handshake. Reads the small context the client
/// already has (queue length, is-playing) from `status`, then makes ONE non-streamed
/// `claude` call (`--output-format json`): a simple "thinking..." indicator on stderr
/// keeps the multi-second call from looking frozen (stdout stays clean for the echo +
/// confirm), and the settled VALIDATED RawPlan is echoed via describe_plan, confirmed
/// y/N, and armed via `plan add <dsl>`. Returns Ok(true) when it handled the phrase
/// (armed, cancelled, or a loud user-facing miss), Ok(false) to fall through to the
/// daemon `nl` path (spawn/parse failure, or a plan not DSL-expressible).
#[cfg(feature = "cc")]
fn cc_nl_handshake(conn: &mut MpdConn, phrase: &str) -> Result<bool, MpdError> {
    let status = conn.command("status")?;
    let queue_len = status
        .iter()
        .find(|(k, _)| k == "playlistlength")
        .and_then(|(_, v)| v.parse::<usize>().ok())
        .unwrap_or(0);
    let is_playing = status
        .iter()
        .find(|(k, _)| k == "state")
        .map(|(_, v)| v == "play")
        .unwrap_or(false);

    // Simple "thinking..." indicator on stderr (stdout stays clean for the echo +
    // y/N). The blocking multi-second call is fine here - the CLI is a one-shot; the
    // indicator keeps it from ever looking frozen. One non-streamed call returns the
    // settled VALIDATED plan directly (the installed CLI returns the result intact).
    eprint!("Claude Code: thinking...");
    let _ = std::io::stderr().flush();
    let result = hypodj_nl::cc::run_claude(phrase, queue_len, is_playing);
    // Clear the indicator line before any output.
    eprint!("\r\x1b[2K");
    let _ = std::io::stderr().flush();

    let raw = match result {
        Ok(raw) => raw,
        Err(e) => {
            eprintln!("Claude Code could not translate that ({e}); trying the built-in translator.");
            return Ok(false);
        }
    };

    let dsl = match hypodj_nl::render_dsl(&raw) {
        Some(d) => d,
        None => {
            // A validated plan the keyword DSL cannot express (e.g. time_remaining);
            // fall through so the daemon rules can have a go.
            return Ok(false);
        }
    };

    println!("(via Claude Code)");
    println!("{}", hypodj_nl::describe_plan(&raw));

    if confirm("confirm?") {
        match conn.command(&format!("plan add {dsl}")) {
            Ok(plan_pairs) => {
                for (k, v) in &plan_pairs {
                    if k == "plan_id" {
                        println!("{}", nl::armed_line(v));
                    }
                }
                print_card(conn)?;
            }
            Err(MpdError::Ack(msg)) => println!("{}", nl::map_ack_reason(&msg)),
            Err(e) => return Err(e),
        }
    } else {
        println!("cancelled");
    }
    Ok(true)
}

/// A default-No y/N prompt. Only "y"/"yes" (case-insensitive) confirm; bare
/// Enter, "n", EOF (Ctrl-D), all mean No.
fn confirm(question: &str) -> bool {
    print!("{question} [y/N] ");
    let _ = std::io::stdout().flush();
    let mut line = String::new();
    match std::io::stdin().lock().read_line(&mut line) {
        Ok(0) => false, // EOF
        Ok(_) => matches!(line.trim().to_lowercase().as_str(), "y" | "yes"),
        Err(_) => false,
    }
}
