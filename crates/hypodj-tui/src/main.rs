//! hypodj-tui - a basic full-screen jukebox over the shared hypodj-client. ONE
//! persistent MpdConn for the whole session (so owner-scoped `nl confirm`/`nl
//! cancel` work). This module is the ONLY IO: connect, terminal setup/teardown,
//! and the blocking event loop. The pure logic lives in state.rs; rendering in
//! ui.rs.

mod state;
mod ui;

use std::io::{self, Stdout};
use std::time::{Duration, Instant};

use crossterm::event::{self, Event};
use crossterm::terminal::{
    disable_raw_mode, enable_raw_mode, EnterAlternateScreen, LeaveAlternateScreen, SetTitle,
};
use crossterm::ExecutableCommand;
use ratatui::backend::CrosstermBackend;
use ratatui::Terminal;

use hypodj_client::config::{self, Env};
use hypodj_client::mpd::{MpdConn, MpdError};
use hypodj_client::model::{now_playing, parse_queue};
use hypodj_client::nl::{armed_line, map_ack_reason, nl_request, quote_arg, split_echo, token_from_pairs, echo_from_pairs};

use state::{Intent, Pending, Screen, TuiState};

const POLL: Duration = Duration::from_millis(250);
const REFRESH_EVERY: Duration = Duration::from_secs(1);

fn main() {
    if let Err(e) = run() {
        eprintln!("hypodj-tui: {e}");
        std::process::exit(1);
    }
}

fn run() -> Result<(), MpdError> {
    let env = Env { get: &|k| std::env::var(k).ok() };
    let (host, port) = config::resolve(None, None, &env);
    let mut conn = MpdConn::connect(&host, port)?;

    let mut terminal = setup_terminal().map_err(|e| MpdError::Io(e.to_string()))?;
    let mut state = TuiState::new();
    // Prime the panes before the first draw.
    refresh(&mut conn, &mut state);

    let res = event_loop(&mut terminal, &mut conn, &mut state, &host, port);

    restore_terminal(&mut terminal).ok();
    res
}

fn event_loop(
    terminal: &mut Terminal<CrosstermBackend<Stdout>>,
    conn: &mut MpdConn,
    state: &mut TuiState,
    host: &str,
    port: u16,
) -> Result<(), MpdError> {
    let mut last_refresh = Instant::now();
    // The terminal title last emitted; sync only on change to avoid OSC spam on
    // every tick. Seeded to the setup_terminal() startup title.
    let mut last_title = String::from("HypoDJ");
    sync_title(terminal, state, &mut last_title);
    loop {
        terminal
            .draw(|f| ui::render(f, state))
            .map_err(|e| MpdError::Io(e.to_string()))?;

        if event::poll(POLL).map_err(|e| MpdError::Io(e.to_string()))? {
            if let Event::Key(key) = event::read().map_err(|e| MpdError::Io(e.to_string()))? {
                if key.kind == event::KeyEventKind::Press || key.kind == event::KeyEventKind::Repeat
                {
                    if let Some(intent) = state.handle_key(key) {
                        if matches!(intent, Intent::Quit) {
                            break;
                        }
                        execute(conn, state, intent);
                        sync_title(terminal, state, &mut last_title);
                    }
                }
            }
        }

        // Independently, tick roughly once a second: keep now-playing live, and
        // when disconnected, try to reconnect.
        if last_refresh.elapsed() >= REFRESH_EVERY {
            last_refresh = Instant::now();
            if state.connected {
                refresh(conn, state);
            } else {
                try_reconnect(conn, state, host, port);
            }
            sync_title(terminal, state, &mut last_title);
        }
    }
    Ok(())
}

/// Emit the OSC terminal title for the current now-playing, but only when it
/// differs from `last` (deduped so the 1s tick never spams the tty). Best-effort:
/// a failed write is swallowed - the title is cosmetic and must never break the UI.
fn sync_title(
    terminal: &mut Terminal<CrosstermBackend<Stdout>>,
    state: &TuiState,
    last: &mut String,
) {
    let title = ui::window_title(&state.now);
    if title != *last {
        terminal.backend_mut().execute(SetTitle(&title)).ok();
        *last = title;
    }
}

/// Run one action, then refresh. Any transport error marks the session
/// disconnected; an ACK becomes a friendly banner.
fn execute(conn: &mut MpdConn, state: &mut TuiState, intent: Intent) {
    match intent {
        Intent::Command(line) => match conn.command(&line) {
            Ok(_) => refresh(conn, state),
            Err(MpdError::Ack(m)) => state.status_msg = Some(map_ack_reason(&m)),
            Err(_) => state.mark_disconnected(),
        },
        Intent::Nl(phrase) => match conn.command(&nl_request(&phrase)) {
            Ok(pairs) => match token_from_pairs(&pairs) {
                Some(token) => {
                    let (trust, steps, note) = match echo_from_pairs(&pairs) {
                        Some(echo) => {
                            let parts = split_echo(&echo);
                            (parts.trust, parts.steps, parts.note)
                        }
                        None => (None, Vec::new(), None),
                    };
                    state.enter_confirm(Pending {
                        token: Some(token),
                        command: None,
                        trust,
                        steps,
                        note,
                    });
                }
                None => state.status_msg = Some("the server returned no plan to confirm".into()),
            },
            Err(MpdError::Ack(m)) => state.status_msg = Some(map_ack_reason(&m)),
            Err(_) => state.mark_disconnected(),
        },
        Intent::ConfirmArm => arm(conn, state),
        Intent::ConfirmCancel => {
            if let Some(Pending { token: Some(tok), .. }) = &state.pending {
                // Best-effort cancel on the open connection.
                let _ = conn.command(&format!("nl cancel {tok}"));
            }
            state.pending = None;
            state.mode = state::Mode::Normal;
        }
        Intent::Refresh => refresh(conn, state),
        Intent::ShowScreen(screen) => show_screen(conn, state, screen),
        Intent::BrowseInto(path) => browse_into(conn, state, path),
        Intent::BrowseBack => browse_back(conn, state),
        Intent::Enqueue { uri, play } => enqueue(conn, state, uri, play),
        Intent::LoadPlaylist(name) => match conn.command(&format!("load {}", quote_arg(&name))) {
            Ok(_) => refresh(conn, state),
            Err(MpdError::Ack(m)) => state.status_msg = Some(map_ack_reason(&m)),
            Err(_) => state.mark_disconnected(),
        },
        Intent::Quit => {}
    }
}

/// Run a command for its pair list, mapping an ACK to a banner and a transport
/// error to disconnect (same shape as `refresh`). `None` means "handled the error".
fn run_pairs(conn: &mut MpdConn, state: &mut TuiState, line: &str) -> Option<Vec<(String, String)>> {
    match conn.command(line) {
        Ok(p) => Some(p),
        Err(MpdError::Ack(m)) => {
            state.status_msg = Some(map_ack_reason(&m));
            None
        }
        Err(_) => {
            state.mark_disconnected();
            None
        }
    }
}

/// Switch to a screen; lazily fetch a browse screen's root on first visit. Queue
/// owns its own live refresh, so it just refreshes.
fn show_screen(conn: &mut MpdConn, state: &mut TuiState, screen: Screen) {
    match screen {
        Screen::Queue => refresh(conn, state),
        Screen::Albums => {
            if !state.albums.loaded {
                // Seed Albums from the `newest` smart list (no flat A-Z album index
                // exists server-side yet - see task rglhxv1 server gaps).
                if let Some(pairs) = run_pairs(conn, state, "lsinfo list/newest") {
                    let rows = state::parse_browse(&pairs);
                    state.albums.apply(rows, "list/newest".into(), "Albums (newest)".into());
                }
            }
        }
        Screen::Playlists => {
            if !state.playlists.loaded {
                // The server exposes only the synthetic `Starred` playlist today.
                if let Some(pairs) = run_pairs(conn, state, "listplaylists") {
                    let rows = state::parse_browse(&pairs);
                    state.playlists.apply(rows, String::new(), "Playlists".into());
                }
            }
        }
    }
}

/// Drill into a browse directory: push the current level onto the nav stack, fetch
/// the children, and show them.
fn browse_into(conn: &mut MpdConn, state: &mut TuiState, path: String) {
    let (cur_path, cur_sel) = match state.active_browse() {
        Some(b) => (b.path.clone(), b.selected),
        None => return,
    };
    if let Some(pairs) = run_pairs(conn, state, &format!("lsinfo {}", quote_arg(&path))) {
        let rows = state::parse_browse(&pairs);
        if let Some(b) = state.active_browse() {
            b.stack.push((cur_path, cur_sel));
            let title = browse_title(&path);
            b.apply(rows, path, title);
        }
    }
}

/// Pop one browse level and re-fetch the parent, restoring its cursor.
fn browse_back(conn: &mut MpdConn, state: &mut TuiState) {
    let popped = state.active_browse().and_then(|b| b.stack.pop());
    let Some((path, sel)) = popped else { return };
    if let Some(pairs) = run_pairs(conn, state, &format!("lsinfo {}", quote_arg(&path))) {
        let rows = state::parse_browse(&pairs);
        if let Some(b) = state.active_browse() {
            let title = browse_title(&path);
            b.apply(rows, path, title);
            if !b.rows.is_empty() {
                b.selected = sel.min(b.rows.len() - 1);
            }
        }
    }
}

/// Display title for a browse path (the Albums root gets a friendly label).
fn browse_title(path: &str) -> String {
    if path == "list/newest" {
        "Albums (newest)".to_string()
    } else {
        path.to_string()
    }
}

/// Enqueue a browse uri (`add <uri>`); when `play`, jump to the freshly-added tail
/// by reading the queue length back from a fresh status.
fn enqueue(conn: &mut MpdConn, state: &mut TuiState, uri: String, play: bool) {
    if run_pairs(conn, state, &format!("add {}", quote_arg(&uri))).is_none() {
        return;
    }
    if play {
        if let Some(status) = run_pairs(conn, state, "status") {
            let len = status
                .iter()
                .find(|(k, _)| k == "playlistlength")
                .and_then(|(_, v)| v.parse::<usize>().ok())
                .unwrap_or(0);
            if len > 0 {
                let _ = conn.command(&format!("play {}", len - 1));
            }
        }
    }
    refresh(conn, state);
}

/// Arm the pending plan: a direct command (destructive verb) OR `nl confirm
/// <token>`.
fn arm(conn: &mut MpdConn, state: &mut TuiState) {
    let pending = match state.pending.take() {
        Some(p) => p,
        None => return,
    };
    state.mode = state::Mode::Normal;
    let result = match (&pending.command, &pending.token) {
        (Some(cmd), _) => conn.command(cmd).map(|_| None),
        (None, Some(token)) => conn.command(&format!("nl confirm {token}")).map(|pairs| {
            pairs
                .iter()
                .find(|(k, _)| k == "plan_id")
                .map(|(_, v)| armed_line(v))
        }),
        (None, None) => Ok(None),
    };
    match result {
        Ok(banner) => {
            if let Some(b) = banner {
                state.status_msg = Some(b);
            }
            refresh(conn, state);
        }
        Err(MpdError::Ack(m)) => state.status_msg = Some(map_ack_reason(&m)),
        Err(_) => state.mark_disconnected(),
    }
}

/// Re-read status + currentsong + playlistinfo and apply the snapshot.
fn refresh(conn: &mut MpdConn, state: &mut TuiState) {
    let status = match conn.command("status") {
        Ok(p) => p,
        Err(MpdError::Ack(m)) => {
            state.status_msg = Some(map_ack_reason(&m));
            return;
        }
        Err(_) => return state.mark_disconnected(),
    };
    let current = match conn.command("currentsong") {
        Ok(p) => p,
        Err(_) => return state.mark_disconnected(),
    };
    let queue = match conn.command("playlistinfo") {
        Ok(p) => p,
        Err(_) => return state.mark_disconnected(),
    };
    state.apply_snapshot(now_playing(&status, &current), parse_queue(&queue));
}

/// While disconnected, try to open a fresh socket; on success swap it in.
fn try_reconnect(conn: &mut MpdConn, state: &mut TuiState, host: &str, port: u16) {
    if let Ok(fresh) = MpdConn::connect(host, port) {
        *conn = fresh;
        state.mark_connected();
        refresh(conn, state);
    }
    // On failure keep the banner and keep drawing - keys still work (q quits).
}

fn setup_terminal() -> io::Result<Terminal<CrosstermBackend<Stdout>>> {
    enable_raw_mode()?;
    let mut stdout = io::stdout();
    stdout.execute(EnterAlternateScreen)?;
    stdout.execute(SetTitle("HypoDJ"))?;
    // Restore the terminal even if a later panic unwinds past the normal teardown.
    let prev = std::panic::take_hook();
    std::panic::set_hook(Box::new(move |info| {
        let _ = disable_raw_mode();
        let _ = io::stdout().execute(LeaveAlternateScreen);
        // Clear the stale "HypoDJ - ..." title; the shell repaints its own on the
        // next prompt (VTE/kgx has no title stack to restore from).
        let _ = io::stdout().execute(SetTitle(""));
        prev(info);
    }));
    Terminal::new(CrosstermBackend::new(stdout))
}

fn restore_terminal(terminal: &mut Terminal<CrosstermBackend<Stdout>>) -> io::Result<()> {
    disable_raw_mode()?;
    // Neutral title on clean exit; the shell reclaims its own on the next prompt.
    terminal.backend_mut().execute(SetTitle("")).ok();
    terminal.backend_mut().execute(LeaveAlternateScreen)?;
    terminal.show_cursor()?;
    Ok(())
}
