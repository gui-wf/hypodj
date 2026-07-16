//! The blocking-IO worker threads that keep the render loop snappy. ALL network IO
//! lives here, off the render thread:
//!
//! - COMMAND WORKER: sole owner of the persistent command [`MpdConn`]. It FIFO-
//!   consumes [`Req`]s and runs each `conn.command` inline (the 5s IO timeout is
//!   absorbed here, never on the render path). The owner-scoped `nl confirm`/`nl
//!   cancel` handshake runs here, in submit order, on the ONE command socket - the
//!   identical byte sequence to the old inline path, just relocated.
//! - IDLE WORKER: owns a SECOND socket that only ever issues `idle` (with the read
//!   timeout cleared, so it parks for minutes). Each wake becomes an [`Inbound::Wake`].
//! - ART WORKER: fetches cover art off the render thread on a track-uri change.
//!
//! All three post back over ONE merged [`Inbound`] channel (the std substitute for a
//! crossbeam Select), which the render loop drains with `try_recv`.
//!
//! INVARIANT - compound Reqs stay ATOMIC on the worker. [`Req::Enqueue`] and
//! [`Req::Arm`] each run a read-then-write sequence (status len_before -> add ->
//! play; nl confirm reading plan_id) as ONE Req on the FIFO socket. NEVER split them
//! into render-side step chaining: the async boundary would break ordering and the
//! owner_key the `nl` handshake depends on.

use std::net::TcpStream;
use std::sync::atomic::{AtomicBool, Ordering};
use std::sync::mpsc::{self, Receiver, Sender};
use std::sync::Arc;
use std::thread;
use std::time::{Duration, Instant};

use hypodj_client::mpd::{MpdConn, MpdError};
use hypodj_client::model::{now_playing, parse_queue, NowPlaying, QueueItem};
use hypodj_client::nl::{
    armed_line, echo_from_pairs, map_ack_reason, nl_request, quote_arg, split_echo, token_from_pairs,
};

use crate::art::AlbumArt;
use crate::state::{self, BrowseRow, Pending, Screen};

/// Backoff ceiling for a reconnect loop: never busy-loop, never exceed 2s between
/// attempts.
const BACKOFF_START: Duration = Duration::from_millis(250);
const BACKOFF_MAX: Duration = Duration::from_secs(2);

/// A request from the render thread to the command worker. Each is stamped with an
/// implicit receive-order sequence and run one at a time on the one command socket,
/// preserving submit order (and the owner_key the `nl` handshake needs).
pub enum Req {
    /// Re-read status + currentsong; re-fetch the full queue ONLY when the queue
    /// version differs from `known_version` (the version-gated fast path).
    Refresh { known_version: Option<u64> },
    /// Run one MPD command line. On success the render thread sends its own single
    /// batch `Refresh`, so a held-key burst costs N commands + ONE refresh.
    Command(String),
    /// Send the phrase through the NL handshake; on a token, reply with a Confirm.
    Nl(String),
    /// Arm the pending plan (direct command OR `nl confirm <token>`), then refresh.
    Arm(Pending),
    /// Best-effort `nl cancel <token>` on the command socket (render already reset).
    Cancel(Option<String>),
    /// status(len_before) -> add -> play(before) -> refresh, atomically.
    Enqueue { uri: String, play: bool },
    /// `load <name>`, then refresh.
    Load(String),
    /// Translate a DJ View NL query via the CLIENT-SIDE Claude Code CLI on the
    /// DEDICATED CC worker (never the command socket, so a multi-second call can
    /// never head-of-line-block commands). Carries the small context the client
    /// already has. On a settled validated plan the worker posts a Confirm(Pending{
    /// command: Some("plan add <dsl>")}), reusing the exact existing confirm path.
    Cc { phrase: String, queue_len: usize, is_playing: bool },
    /// Run a browse command (`lsinfo <path>` or `listplaylists`), parse rows, reply.
    Browse { target: Screen, command: String, path: String, title: String, restore_sel: Option<usize> },
    /// Drain and stop the worker (quit teardown).
    Shutdown,
}

/// A message from any worker back to the render thread, merged onto ONE channel.
pub enum Inbound {
    /// A response to a `Req`, tagged with the connection epoch for stale-drop.
    Resp { epoch: u64, kind: RespKind },
    /// An `idle` push: the changed subsystems (empty on a bare-OK / catch-up wake).
    Wake(Vec<String>),
    /// A fetched cover for `uri` (None for a stream / missing art / decode failure).
    Art { uri: String, art: Option<AlbumArt> },
    /// The command socket reconnected; epoch bumped. Followed by a catch-up Snapshot.
    Connected { epoch: u64 },
    /// The command socket dropped; the worker is reconnecting.
    Disconnected,
    /// A coarse CC phase line (e.g. "thinking...") from the dedicated CC worker.
    CcProgress(String),
    /// A DISCRETE full DJ View scrollback line (a result summary, a miss notice, or
    /// an error), pushed as its own line.
    CcLine(String),
    /// A settled, validated CC plan ready to confirm. NOT epoch-tagged (the CC worker
    /// owns no socket), so it is never dropped as stale; it drives the standard
    /// confirm popup and arms via the command worker's direct-`command` Arm path.
    /// Only constructed under `feature = "cc"`; the variant stays for match parity.
    #[cfg_attr(not(feature = "cc"), allow(dead_code))]
    CcConfirm(Pending),
}

/// The parsed payload of a command-worker response. The worker parses via the pure
/// hypodj-client fns so the render thread only folds into TuiState (no shared state).
pub enum RespKind {
    Snapshot { now: NowPlaying, queue: Option<Vec<QueueItem>>, version: Option<u64> },
    Confirm(Pending),
    Banner(String),
    /// An old daemon ACKed `unknown command "knob"`; render computes a setvol
    /// fallback from the last-known volume.
    KnobUnknown(String),
    Browse { target: Screen, rows: Vec<BrowseRow>, path: String, title: String, restore_sel: Option<usize> },
}

/// The handles the render thread keeps to talk to (and tear down) the workers.
pub struct Workers {
    pub req_tx: Sender<Req>,
    /// The dedicated CC worker's request channel (Screen::Dj NL queries). Separate
    /// from `req_tx` so a multi-second `claude` call never blocks the command socket.
    pub cc_tx: Sender<Req>,
    pub inbound_rx: Receiver<Inbound>,
    pub art_tx: Sender<String>,
    pub stop: Arc<AtomicBool>,
    /// Cloned command-socket handle: `shutdown(Both)` unblocks a parked read at quit.
    pub cmd_shutdown: TcpStream,
    /// Cloned idle-socket handle (None if the idle socket was not connected at spawn).
    pub idle_shutdown: Option<TcpStream>,
}

/// Connect the command socket (propagating a startup failure), then spawn all three
/// workers. The idle socket is best-effort: if it fails to connect now, the idle
/// worker starts in reconnect-first mode.
pub fn spawn(host: &str, port: u16) -> Result<Workers, MpdError> {
    let cmd_conn = MpdConn::connect(host, port)?;
    let cmd_shutdown =
        cmd_conn.shutdown_handle().map_err(|e| MpdError::Io(e.to_string()))?;

    let (req_tx, req_rx) = mpsc::channel::<Req>();
    let (in_tx, in_rx) = mpsc::channel::<Inbound>();
    let (art_tx, art_rx) = mpsc::channel::<String>();
    let stop = Arc::new(AtomicBool::new(false));

    // Command worker.
    {
        let in_tx = in_tx.clone();
        let host = host.to_string();
        let stop = stop.clone();
        thread::spawn(move || command_worker(cmd_conn, req_rx, in_tx, &host, port, stop));
    }

    // Idle worker: pre-connect the second socket (clearing its read timeout so idle
    // can park), best-effort. On failure the worker reconnects itself first.
    let idle_pre = connect_idle(host, port);
    let idle_shutdown = idle_pre.as_ref().and_then(|c| c.shutdown_handle().ok());
    {
        let in_tx = in_tx.clone();
        let host = host.to_string();
        let stop = stop.clone();
        thread::spawn(move || idle_worker(idle_pre, in_tx, &host, port, stop));
    }

    // Art worker.
    {
        let host = host.to_string();
        let in_tx_art = in_tx.clone();
        thread::spawn(move || art_worker(art_rx, in_tx_art, &host, port));
    }

    // Dedicated CC worker: owns NO socket (it only shells out to `claude` and posts
    // progress/confirm frames), so it can block for seconds without touching the
    // command or idle sockets.
    let (cc_tx, cc_rx) = mpsc::channel::<Req>();
    {
        let stop = stop.clone();
        thread::spawn(move || cc_worker(cc_rx, in_tx, stop));
    }

    Ok(Workers { req_tx, cc_tx, inbound_rx: in_rx, art_tx, stop, cmd_shutdown, idle_shutdown })
}

/// The dedicated CC worker: for each [`Req::Cc`], post a coarse "thinking..." phase,
/// run the client-side Claude Code CLI (feature = "cc"), and on a settled VALIDATED
/// plan post a `Confirm(Pending{ command: Some("plan add <dsl>") })` - reusing the
/// EXACT existing confirm + `Req::Arm` direct-command arm. A miss / disabled build
/// posts a loud CcProgress line, never a fabricated plan. Owns no socket, so a
/// multi-second call cannot block commands.
fn cc_worker(rx: Receiver<Req>, tx: Sender<Inbound>, stop: Arc<AtomicBool>) {
    while let Ok(req) = rx.recv() {
        if stop.load(Ordering::Relaxed) || matches!(req, Req::Shutdown) {
            break;
        }
        let Req::Cc { phrase, queue_len, is_playing } = req else {
            continue;
        };
        if tx.send(Inbound::CcProgress("thinking...".into())).is_err() {
            break;
        }
        #[cfg(feature = "cc")]
        {
            // One non-streamed `claude` call is the single spine: the installed CLI
            // returns the settled result intact, so there is no truncation to work
            // around and no stream-else-fallback dance. The DJ View shows the
            // "thinking..." spinner (already posted above) until this settles.
            let settled = hypodj_nl::cc::run_claude(&phrase, queue_len, is_playing);
            // Drop the "thinking..." spinner.
            let _ = tx.send(Inbound::CcProgress(String::new()));
            match settled {
                Ok(raw) => match hypodj_nl::render_dsl(&raw) {
                    Some(dsl) => {
                        let steps = vec![hypodj_nl::describe_plan(&raw)];
                        let _ = tx.send(Inbound::CcLine(format!("plan: {}", steps[0])));
                        let _ = tx.send(Inbound::CcConfirm(Pending {
                            token: None,
                            command: Some(format!("plan add {dsl}")),
                            trust: Some("via Claude Code".into()),
                            steps,
                            note: None,
                        }));
                    }
                    None => {
                        let _ = tx.send(Inbound::CcLine(
                            "that plan can't be expressed as a DSL plan - try rephrasing".into(),
                        ));
                    }
                },
                Err(e) => {
                    let _ = tx.send(Inbound::CcLine(format!("Claude Code: {e}")));
                }
            }
        }
        #[cfg(not(feature = "cc"))]
        {
            let _ = (&phrase, queue_len, is_playing);
            let _ = tx.send(Inbound::CcProgress(String::new()));
            let _ = tx.send(Inbound::CcLine(
                "Claude Code backend not enabled in this build".into(),
            ));
        }
    }
}

/// Connect a dedicated idle socket and clear its read timeout so `idle` parks
/// indefinitely (mandatory - see [`MpdConn::clear_read_timeout`]).
fn connect_idle(host: &str, port: u16) -> Option<MpdConn> {
    let conn = MpdConn::connect(host, port).ok()?;
    conn.clear_read_timeout().ok()?;
    Some(conn)
}

/// Reconnect with capped backoff, checking `stop` between attempts so quit is prompt.
/// Returns None only when a stop was requested.
fn reconnect(host: &str, port: u16, stop: &AtomicBool, idle: bool) -> Option<MpdConn> {
    let mut backoff = BACKOFF_START;
    loop {
        if stop.load(Ordering::Relaxed) {
            return None;
        }
        let attempt = if idle { connect_idle(host, port) } else { MpdConn::connect(host, port).ok() };
        if let Some(c) = attempt {
            return Some(c);
        }
        thread::sleep(backoff);
        backoff = (backoff * 2).min(BACKOFF_MAX);
    }
}

/// The command worker: sole owner of the command socket. FIFO-consume Reqs; on a
/// transport drop, announce Disconnected, reconnect with backoff, bump the epoch,
/// announce Connected + push one catch-up Snapshot.
fn command_worker(
    mut conn: MpdConn,
    rx: Receiver<Req>,
    tx: Sender<Inbound>,
    host: &str,
    port: u16,
    stop: Arc<AtomicBool>,
) {
    let mut epoch: u64 = 0;
    while let Ok(req) = rx.recv() {
        if stop.load(Ordering::Relaxed) || matches!(req, Req::Shutdown) {
            break;
        }
        let dropped = handle_req(&mut conn, &tx, epoch, req);
        if dropped {
            if tx.send(Inbound::Disconnected).is_err() {
                break;
            }
            match reconnect(host, port, &stop, false) {
                Some(fresh) => {
                    conn = fresh;
                    epoch += 1;
                    if tx.send(Inbound::Connected { epoch }).is_err() {
                        break;
                    }
                    // Catch-up: the state may have moved while we were away.
                    if let Ok(kind) = do_refresh(&mut conn, None) {
                        let _ = tx.send(Inbound::Resp { epoch, kind });
                    }
                }
                None => break,
            }
        }
    }
}

/// Run one Req against the command socket, sending any response(s) on `tx`. Returns
/// true iff a transport drop was seen (caller reconnects). An ACK becomes a friendly
/// Banner (never a drop); the socket stays live.
fn handle_req(conn: &mut MpdConn, tx: &Sender<Inbound>, epoch: u64, req: Req) -> bool {
    let send = |kind: RespKind| tx.send(Inbound::Resp { epoch, kind }).is_ok();
    match req {
        Req::Shutdown => false,
        Req::Refresh { known_version } => match do_refresh(conn, known_version) {
            Ok(kind) => {
                send(kind);
                false
            }
            Err(Fail::Ack(m)) => {
                send(RespKind::Banner(m));
                false
            }
            Err(Fail::Down) => true,
        },
        Req::Command(line) => match conn.command(&line) {
            Ok(_) => false,
            Err(MpdError::Ack(m)) => {
                // Graceful knob -> setvol fallback for an old daemon: hand the render
                // thread the line so it can compute a setvol from the last volume.
                if (line == "knob up" || line == "knob down") && m.contains("unknown command") {
                    send(RespKind::KnobUnknown(line));
                } else {
                    send(RespKind::Banner(map_ack_reason(&m)));
                }
                false
            }
            Err(_) => true,
        },
        Req::Nl(phrase) => match conn.command(&nl_request(&phrase)) {
            Ok(pairs) => {
                match token_from_pairs(&pairs) {
                    Some(token) => {
                        let (trust, steps, note) = match echo_from_pairs(&pairs) {
                            Some(echo) => {
                                let parts = split_echo(&echo);
                                (parts.trust, parts.steps, parts.note)
                            }
                            None => (None, Vec::new(), None),
                        };
                        send(RespKind::Confirm(Pending {
                            token: Some(token),
                            command: None,
                            trust,
                            steps,
                            note,
                        }));
                    }
                    None => {
                        send(RespKind::Banner("the server returned no plan to confirm".into()));
                    }
                }
                false
            }
            Err(MpdError::Ack(m)) => {
                send(RespKind::Banner(map_ack_reason(&m)));
                false
            }
            Err(_) => true,
        },
        Req::Arm(pending) => {
            let result = match (&pending.command, &pending.token) {
                (Some(cmd), _) => conn.command(cmd).map(|_| None),
                (None, Some(token)) => {
                    conn.command(&format!("nl confirm {token}")).map(|pairs| {
                        pairs.iter().find(|(k, _)| k == "plan_id").map(|(_, v)| armed_line(v))
                    })
                }
                (None, None) => Ok(None),
            };
            match result {
                Ok(banner) => {
                    if let Some(b) = banner {
                        send(RespKind::Banner(b));
                    }
                    refresh_after(conn, tx, epoch)
                }
                Err(MpdError::Ack(m)) => {
                    send(RespKind::Banner(map_ack_reason(&m)));
                    false
                }
                Err(_) => true,
            }
        }
        Req::Cancel(token) => {
            if let Some(tok) = token {
                // Best-effort: ignore any error, the render side already reset.
                let _ = conn.command(&format!("nl cancel {tok}"));
            }
            false
        }
        Req::Enqueue { uri, play } => {
            let len_before = if play {
                match cmd(conn, "status") {
                    Ok(status) => Some(
                        status
                            .iter()
                            .find(|(k, _)| k == "playlistlength")
                            .and_then(|(_, v)| v.parse::<usize>().ok())
                            .unwrap_or(0),
                    ),
                    Err(Fail::Ack(m)) => {
                        send(RespKind::Banner(m));
                        return false;
                    }
                    Err(Fail::Down) => return true,
                }
            } else {
                None
            };
            match cmd(conn, &format!("add {}", quote_arg(&uri))) {
                Ok(_) => {}
                Err(Fail::Ack(m)) => {
                    send(RespKind::Banner(m));
                    return false;
                }
                Err(Fail::Down) => return true,
            }
            if let Some(before) = len_before {
                let _ = conn.command(&format!("play {before}"));
            }
            refresh_after(conn, tx, epoch)
        }
        // CC requests are routed to the dedicated CC worker, never here; a stray one
        // on the command socket is a no-op (the socket must not fork a subprocess).
        Req::Cc { .. } => false,
        Req::Load(name) => match conn.command(&format!("load {}", quote_arg(&name))) {
            Ok(_) => refresh_after(conn, tx, epoch),
            Err(MpdError::Ack(m)) => {
                send(RespKind::Banner(map_ack_reason(&m)));
                false
            }
            Err(_) => true,
        },
        Req::Browse { target, command, path, title, restore_sel } => match conn.command(&command) {
            Ok(pairs) => {
                let rows = state::parse_browse(&pairs);
                send(RespKind::Browse { target, rows, path, title, restore_sel });
                false
            }
            Err(MpdError::Ack(m)) => {
                send(RespKind::Banner(map_ack_reason(&m)));
                false
            }
            Err(_) => true,
        },
    }
}

/// A command outcome collapsed to the two render-visible cases.
enum Fail {
    Ack(String),
    Down,
}

/// Run one command, mapping an ACK to a friendly Banner string and any transport
/// error to a drop.
fn cmd(conn: &mut MpdConn, line: &str) -> Result<Vec<(String, String)>, Fail> {
    match conn.command(line) {
        Ok(p) => Ok(p),
        Err(MpdError::Ack(m)) => Err(Fail::Ack(map_ack_reason(&m))),
        Err(_) => Err(Fail::Down),
    }
}

/// Refresh and send the resulting Snapshot after a mutation. Returns true on a drop.
fn refresh_after(conn: &mut MpdConn, tx: &Sender<Inbound>, epoch: u64) -> bool {
    match do_refresh(conn, None) {
        Ok(kind) => {
            let _ = tx.send(Inbound::Resp { epoch, kind });
            false
        }
        Err(Fail::Ack(m)) => {
            let _ = tx.send(Inbound::Resp { epoch, kind: RespKind::Banner(m) });
            false
        }
        Err(Fail::Down) => true,
    }
}

/// The version-gated refresh, run on the worker so status-read-then-decide stays
/// atomic: read status + currentsong, and re-fetch the full queue ONLY when the
/// queue version differs from `known_version`.
fn do_refresh(conn: &mut MpdConn, known_version: Option<u64>) -> Result<RespKind, Fail> {
    let status = cmd(conn, "status")?;
    let current = cmd(conn, "currentsong")?;
    let now = now_playing(&status, &current);
    let version = status
        .iter()
        .find(|(k, _)| k == "playlist")
        .and_then(|(_, v)| v.parse::<u64>().ok());
    if version.is_some() && version == known_version {
        return Ok(RespKind::Snapshot { now, queue: None, version });
    }
    let queue = cmd(conn, "playlistinfo")?;
    Ok(RespKind::Snapshot { now, queue: Some(parse_queue(&queue)), version })
}

/// The idle worker: park in `idle` on the dedicated socket, pushing a Wake on every
/// change. On a transport error, back off + reconnect (never a busy-loop, never a
/// panic), and emit ONE synthetic Wake so the render thread does a catch-up refresh.
fn idle_worker(pre: Option<MpdConn>, tx: Sender<Inbound>, host: &str, port: u16, stop: Arc<AtomicBool>) {
    let mut conn = match pre {
        Some(c) => c,
        None => match reconnect(host, port, &stop, true) {
            Some(c) => c,
            None => return,
        },
    };
    // Backoff for the accept-then-close case: a socket-activated daemon restart can
    // accept the TCP connection (so `reconnect` succeeds instantly) and then drop it,
    // making `idle_once` fail immediately. Without a delay the worker would busy-loop
    // and flood the unbounded Wake channel. We back off whenever a park dies almost
    // instantly and reset once a park lives long enough to be a real connection.
    let mut backoff = BACKOFF_START;
    loop {
        if stop.load(Ordering::Relaxed) {
            break;
        }
        let parked_at = Instant::now();
        match conn.idle_once() {
            Ok(changed) => {
                backoff = BACKOFF_START;
                if tx.send(Inbound::Wake(changed)).is_err() {
                    break;
                }
            }
            Err(_) => {
                if stop.load(Ordering::Relaxed) {
                    break;
                }
                // A park that barely lived is an accept-then-close flap; sleep (with
                // exponential backoff) before reconnecting so we never spin. A park
                // that lived a while is a genuine transient drop - reconnect promptly.
                if parked_at.elapsed() < BACKOFF_MAX {
                    thread::sleep(backoff);
                    backoff = (backoff * 2).min(BACKOFF_MAX);
                } else {
                    backoff = BACKOFF_START;
                }
                match reconnect(host, port, &stop, true) {
                    Some(fresh) => {
                        conn = fresh;
                        // Catch-up: coalesces into the command worker's Connected
                        // refresh, avoiding a double refetch flash.
                        if tx.send(Inbound::Wake(Vec::new())).is_err() {
                            break;
                        }
                    }
                    None => break,
                }
            }
        }
    }
}

/// The art worker: fetch a cover per track-uri change (the last blocking IO off the
/// render thread). A non-`song/` uri (a raw stream) has no cover.
fn art_worker(rx: Receiver<String>, tx: Sender<Inbound>, host: &str, port: u16) {
    while let Ok(uri) = rx.recv() {
        let art = if uri.starts_with("song/") { AlbumArt::load(host, port, &uri) } else { None };
        if tx.send(Inbound::Art { uri, art }).is_err() {
            break;
        }
    }
}
