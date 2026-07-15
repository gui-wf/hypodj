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
    disable_raw_mode, enable_raw_mode, EnterAlternateScreen, LeaveAlternateScreen,
};
use crossterm::ExecutableCommand;
use ratatui::backend::CrosstermBackend;
use ratatui::Terminal;

use hypodj_client::config::{self, Env};
use hypodj_client::mpd::{MpdConn, MpdError};
use hypodj_client::model::{now_playing, parse_queue};
use hypodj_client::nl::{armed_line, map_ack_reason, nl_request, split_echo, token_from_pairs, echo_from_pairs};

use state::{Intent, Pending, TuiState};

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
        }
    }
    Ok(())
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
                    let (steps, note) = match echo_from_pairs(&pairs) {
                        Some(echo) => {
                            let parts = split_echo(&echo);
                            (parts.steps, parts.note)
                        }
                        None => (Vec::new(), None),
                    };
                    state.enter_confirm(Pending { token: Some(token), command: None, steps, note });
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
        Intent::Quit => {}
    }
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
    // Restore the terminal even if a later panic unwinds past the normal teardown.
    let prev = std::panic::take_hook();
    std::panic::set_hook(Box::new(move |info| {
        let _ = disable_raw_mode();
        let _ = io::stdout().execute(LeaveAlternateScreen);
        prev(info);
    }));
    Terminal::new(CrosstermBackend::new(stdout))
}

fn restore_terminal(terminal: &mut Terminal<CrosstermBackend<Stdout>>) -> io::Result<()> {
    disable_raw_mode()?;
    terminal.backend_mut().execute(LeaveAlternateScreen)?;
    terminal.show_cursor()?;
    Ok(())
}
