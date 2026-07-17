//! hypodj-tui - a basic full-screen jukebox over the shared hypodj-client. This
//! module is the RENDER thread: it owns TuiState + the terminal and does the
//! event::poll/read drain+coalesce + draw + title sync, and NEVER touches a socket.
//! All blocking network IO lives on the worker threads (see worker.rs): a COMMAND
//! worker (sole owner of the persistent command socket, so owner-scoped `nl
//! confirm`/`nl cancel` still run in submit order on ONE socket), an IDLE worker (a
//! dedicated second socket that only ever issues `idle` and pushes wakes), and an
//! ART worker. The render thread sends [`worker::Req`]s and folds [`worker::Inbound`]
//! messages into TuiState - no shared mutable state, no Mutex. The pure logic lives
//! in state.rs; rendering in ui.rs.

mod album_color;
mod art;
mod keymap;
mod sigil;
mod state;
mod ui;
mod worker;

use std::io::{self, Stdout};
use std::net::Shutdown;
use std::sync::atomic::Ordering;
use std::sync::mpsc::Sender;
use std::time::{Duration, Instant};

use crossterm::event::{self, Event};
use crossterm::terminal::{
    disable_raw_mode, enable_raw_mode, EnterAlternateScreen, LeaveAlternateScreen, SetTitle,
};
use crossterm::ExecutableCommand;
use ratatui::backend::CrosstermBackend;
use ratatui::Terminal;

use hypodj_client::config::{self, Env};
use hypodj_client::mpd::MpdError;
use hypodj_client::nl::quote_arg;

use state::{coalesce_intents, Intent, Mode, Screen, TuiState};
use worker::{Inbound, Req, RespKind, Workers};

/// Render-loop poll floor. Short enough that an inbound wake/response is folded and
/// drawn within one poll cycle (imperceptible), but event::poll still parks the
/// thread when nothing is happening, so idle CPU stays negligible.
const POLL: Duration = Duration::from_millis(50);
/// The 1s live tick is demoted to a slow safety net: idle-push wakes drive liveness
/// now, so this only catches a missed wake and probes a dropped command socket.
const REFRESH_SAFETY: Duration = Duration::from_secs(5);

/// Volume detent for the knob->setvol fallback, matching the server's step.
const KNOB_STEP: i32 = 5;

fn main() {
    // --version/--help short-circuit before the terminal is taken over. Enriched
    // display version: base semver + commits-since-tag + git short hash on source
    // builds (bare semver otherwise).
    for arg in std::env::args().skip(1) {
        match arg.as_str() {
            "-V" | "--version" => {
                println!(
                    "dj-gui {}",
                    hypodj_build_info::version(
                        env!("CARGO_PKG_VERSION"),
                        option_env!("HYPODJ_BUILD_INFO"),
                    )
                );
                return;
            }
            "-h" | "--help" => {
                println!(
                    "dj-gui - hypodj interactive TUI\n\nUSAGE:\n  dj-gui            launch the jukebox TUI\n\nOPTIONS:\n  -h, --help    this help\n  -V, --version print version and exit"
                );
                return;
            }
            _ => {}
        }
    }
    if let Err(e) = run() {
        eprintln!("hypodj-tui: {e}");
        std::process::exit(1);
    }
}

fn run() -> Result<(), MpdError> {
    let env = Env { get: &|k| std::env::var(k).ok() };
    let (host, port) = config::resolve(None, None, &env);
    let workers = worker::spawn(&host, port)?;

    let mut terminal = setup_terminal().map_err(|e| MpdError::Io(e.to_string()))?;
    let mut state = TuiState::new();
    // Probe the visual-system primitives once at startup (raw mode is on; the OSC 11
    // read is bounded so a non-answering terminal / tmux can never hang us). Capability
    // + truecolor come from the env only (no query). See album_color for the tmux note.
    state.image_protocol = album_color::image_protocol(&env);
    state.truecolor = album_color::truecolor(&env);
    state.term_bg = album_color::probe_bg(
        terminal.backend_mut(),
        &env,
        Duration::from_millis(100),
    );
    // Prime the panes before the first draw: request the initial snapshot (the
    // response lands within the first few poll cycles).
    request_refresh(&workers.req_tx, &mut state);

    let res = event_loop(&mut terminal, &mut state, &workers);

    teardown(&workers);
    restore_terminal(&mut terminal).ok();
    res
}

fn event_loop(
    terminal: &mut Terminal<CrosstermBackend<Stdout>>,
    state: &mut TuiState,
    workers: &Workers,
) -> Result<(), MpdError> {
    let req_tx = &workers.req_tx;
    let mut last_refresh = Instant::now();
    // The terminal title last emitted; sync only on change to avoid OSC spam on
    // every tick. Seeded to the setup_terminal() startup title.
    let mut last_title = String::from("HypoDJ");
    sync_title(terminal, state, &mut last_title);
    // Ambient-visualizer clock: a wall-clock accumulator advanced by the per-frame
    // delta ONLY while playback is `play`, so the idle bottom-bar wave drifts while
    // music flows and freezes flat when paused/stopped. It rides the existing
    // poll(POLL) cadence - one Instant delta per frame, no extra wakeups.
    let mut anim_accum = 0.0f64;
    let mut last_frame = Instant::now();
    loop {
        let frame_now = Instant::now();
        let dt = frame_now.duration_since(last_frame).as_secs_f64();
        if state.now.state.as_deref() == Some("play") {
            anim_accum += dt;
        }
        last_frame = frame_now;
        state.anim_secs = anim_accum;

        // Sample the latest-wins viz slot and run the ballistics envelope at the
        // render dt. When the viz socket is live we draw the REAL post-gain level
        // field; when it is absent (old daemon / refused / no frame yet) we clear
        // viz_active and the renderer falls back to the decorative wave. The slot
        // lock is held only for the brief read, never across IO or the draw.
        update_viz(state, workers, dt);

        terminal
            .draw(|f| ui::render(f, state))
            .map_err(|e| MpdError::Io(e.to_string()))?;

        // Drain ALL key events queued this frame, not just one. Holding a key
        // (autorepeat) floods events faster than one-per-frame handling drains
        // them, so they back up - a felt delay starting, and input still
        // processing after release ("it keeps scrubbing after I let go"). Draining
        // + coalescing makes a held key track the finger and stop the instant it
        // is released: the loop applies the REAL summed action, never a faked UI
        // preview.
        let mut intents: Vec<Intent> = Vec::new();
        let mut quit = false;
        if event::poll(POLL).map_err(|e| MpdError::Io(e.to_string()))? {
            loop {
                if let Event::Key(key) = event::read().map_err(|e| MpdError::Io(e.to_string()))? {
                    if key.kind == event::KeyEventKind::Press
                        || key.kind == event::KeyEventKind::Repeat
                    {
                        if let Some(intent) = state.handle_key(key) {
                            if matches!(intent, Intent::Quit) {
                                quit = true;
                                break;
                            }
                            intents.push(intent);
                        }
                    }
                }
                // Stop once the queue drains; next iteration's poll(POLL) waits.
                if !event::poll(Duration::ZERO).map_err(|e| MpdError::Io(e.to_string()))? {
                    break;
                }
            }
        }
        if quit {
            break;
        }
        if !intents.is_empty() {
            // Convert the coalesced intents into worker Reqs; the worker runs them off
            // the render path so a Subsonic-backed browse/enqueue never blocks input
            // or draw.
            dispatch(req_tx, &workers.cc_tx, state, coalesce_intents(intents));
            sync_title(terminal, state, &mut last_title);
        }

        // Drain the merged inbound channel: fold responses/wakes/art into TuiState.
        // Bounded (try_recv) so the loop never blocks on the worker.
        let mut got_msg = false;
        while let Ok(msg) = workers.inbound_rx.try_recv() {
            apply_inbound(req_tx, state, msg);
            got_msg = true;
        }
        if got_msg {
            sync_title(terminal, state, &mut last_title);
        }
        // Keep album art in step: on a track-uri change, ask the art worker (once).
        request_art(&workers.art_tx, state);
        // Keep the album sigil in step: rebuild only when the album identity changes
        // (static, cached - never regenerated per frame).
        update_sigil(state);

        // Safety net: an idle-push wake normally drives liveness, but a missed wake
        // or a silently-dropped command socket is caught here. request_refresh is
        // gated on the in-flight bool, so this is a cheap status+currentsong at worst.
        if last_refresh.elapsed() >= REFRESH_SAFETY {
            last_refresh = Instant::now();
            request_refresh(req_tx, state);
        }
    }
    Ok(())
}

/// Fold the latest viz level into the render state, running the asymmetric
/// attack/release envelope at the frame `dt`. Connect-or-fallback: if the viz socket
/// is not connected (old daemon / refused) `viz_active` is cleared and the renderer
/// uses the decorative wave. The envelope integrates over frames (persisted in
/// `state.viz_env`) so ~20 fps network frames render as a smooth field and a fade
/// settles on the release tau.
fn update_viz(state: &mut TuiState, workers: &Workers, dt: f64) {
    use std::sync::atomic::Ordering;
    if !workers.viz_connected.load(Ordering::Relaxed) {
        // No live socket: decay the envelope toward rest so a re-connect eases in,
        // and hand the renderer the fallback wave.
        state.viz_active = false;
        state.viz_env = state::envelope_step(state.viz_env, 0.0, dt as f32);
        return;
    }
    // Read the newest sample (lock only for the copy; never across the draw).
    let sample = workers.viz_slot.lock().ok().and_then(|g| *g);
    match sample {
        Some(s) => {
            // The TUI already knows the MPD transport state; treat anything other than
            // "play" as not-playing. This is a belt against a daemon that stops
            // emitting frames on pause/stop without a trailing resting frame (older
            // builds): without it the stale playing=true slot freezes the field lit.
            let transport_playing =
                state.now.state.as_deref().map(|st| st == "play").unwrap_or(true);
            let playing = s.playing && transport_playing;
            let target = state::normalize_level(s.post_gain_db());
            // Only drive up toward the target while playing; a paused/stopped daemon
            // frame (or a non-play transport) targets rest so the field settles to the
            // hairline.
            let target = if playing { target } else { 0.0 };
            state.viz_env = state::envelope_step(state.viz_env, target, dt as f32);
            state.viz_playing = playing;
            state.viz_active = true;
        }
        // Connected but no frame yet: keep the fallback until the first level lands.
        None => {
            state.viz_active = false;
        }
    }
}

/// Convert a frame's coalesced intents into worker Reqs. Command intents are sent as
/// individual Reqs and, since a batch mutates state, ONE trailing Refresh is queued
/// after them (so a held-key burst costs N cheap commands + a single refresh). The
/// confirm/cancel handshake is applied to local state here but relies on the worker
/// running `nl confirm`/`nl cancel` on the ONE command socket, in order.
fn dispatch(tx: &Sender<Req>, cc_tx: &Sender<Req>, state: &mut TuiState, intents: Vec<Intent>) {
    let mut sent_mutation = false;
    for intent in intents {
        match intent {
            Intent::Command(line) => {
                let _ = tx.send(Req::Command(line));
                sent_mutation = true;
            }
            Intent::Nl(phrase) => {
                let _ = tx.send(Req::Nl(phrase));
            }
            Intent::ConfirmArm => {
                let pending = state.pending.take();
                state.mode = Mode::Normal;
                if let Some(p) = pending {
                    let _ = tx.send(Req::Arm(p));
                }
            }
            Intent::ConfirmCancel => {
                let token = state.pending.as_ref().and_then(|p| p.token.clone());
                state.pending = None;
                state.mode = Mode::Normal;
                let _ = tx.send(Req::Cancel(token));
            }
            Intent::Refresh => request_refresh(tx, state),
            Intent::ShowScreen(screen) => show_screen(tx, state, screen),
            Intent::BrowseInto(path) => browse_into(tx, state, path),
            Intent::BrowseBack => browse_back(tx, state),
            Intent::Enqueue { uri, play } => {
                let _ = tx.send(Req::Enqueue { uri, play });
            }
            Intent::LoadPlaylist(name) => {
                let _ = tx.send(Req::Load(name));
            }
            Intent::Cc(phrase) => {
                // Send on the DEDICATED CC channel with the small context the render
                // thread already holds (queue length + is-playing).
                let _ = cc_tx.send(Req::Cc {
                    phrase,
                    queue_len: state.queue.len(),
                    is_playing: state.now.state.as_deref() == Some("play"),
                });
            }
            Intent::Quit => {}
        }
    }
    if sent_mutation {
        request_refresh(tx, state);
    }
}

/// Queue a version-gated refresh, unless one is already in flight (so a wake-storm -
/// or a burst of mutations - collapses to a single refresh). Cleared when the
/// Snapshot lands.
fn request_refresh(tx: &Sender<Req>, state: &mut TuiState) {
    if state.refresh_in_flight {
        // The in-flight refresh may have read the server state before this mutation's
        // effect landed; mark dirty so the gate re-arms one catch-up refresh.
        state.refresh_dirty = true;
        return;
    }
    state.refresh_in_flight = true;
    let _ = tx.send(Req::Refresh { known_version: state.queue_version });
}

/// Clear the in-flight refresh gate as a response lands. If a wake (or a mutation)
/// was suppressed while that refresh was outstanding (`refresh_dirty`), it may not be
/// reflected in the response we just got, so re-arm exactly one catch-up refresh -
/// otherwise the lost change waits for the 5s safety net.
fn clear_refresh_gate(tx: &Sender<Req>, state: &mut TuiState) {
    state.refresh_in_flight = false;
    if state.refresh_dirty {
        state.refresh_dirty = false;
        request_refresh(tx, state);
    }
}

/// Switch to a screen: Queue just refreshes; a browse screen lazily fetches its root
/// on first visit (the worker runs the lsinfo/listplaylists off the render path).
fn show_screen(tx: &Sender<Req>, state: &mut TuiState, screen: Screen) {
    match screen {
        // The DJ pane shows the queue alongside; keep it live-refreshed like Queue.
        Screen::Queue | Screen::Dj => request_refresh(tx, state),
        Screen::Albums => {
            if !state.albums.loaded {
                // Seed Albums from the `newest` smart list (no flat A-Z album index
                // exists server-side yet - see task rglhxv1 server gaps).
                let _ = tx.send(Req::Browse {
                    target: Screen::Albums,
                    command: "lsinfo list/newest".into(),
                    path: "list/newest".into(),
                    title: "Albums (newest)".into(),
                    restore_sel: None,
                });
            }
        }
        Screen::Playlists => {
            if !state.playlists.loaded {
                // The server exposes only the synthetic `Starred` playlist today.
                let _ = tx.send(Req::Browse {
                    target: Screen::Playlists,
                    command: "listplaylists".into(),
                    path: String::new(),
                    title: "Playlists".into(),
                    restore_sel: None,
                });
            }
        }
    }
}

/// Drill into a browse directory: push the current level onto the nav stack now
/// (render-side), and ask the worker to fetch the children. The rows land as a
/// Browse response, applied to the target screen's list.
fn browse_into(tx: &Sender<Req>, state: &mut TuiState, path: String) {
    let target = state.screen;
    let (cur_path, cur_sel) = match state.active_browse() {
        Some(b) => (b.path.clone(), b.selected),
        None => return,
    };
    if let Some(b) = state.active_browse() {
        b.stack.push((cur_path, cur_sel));
    }
    let title = browse_title(&path);
    let _ = tx.send(Req::Browse {
        target,
        command: format!("lsinfo {}", quote_arg(&path)),
        path,
        title,
        restore_sel: None,
    });
}

/// Pop one browse level and ask the worker to re-fetch the parent, restoring its
/// cursor when the rows land (restore_sel).
fn browse_back(tx: &Sender<Req>, state: &mut TuiState) {
    let target = state.screen;
    let popped = state.active_browse().and_then(|b| b.stack.pop());
    let Some((path, sel)) = popped else { return };
    let title = browse_title(&path);
    let _ = tx.send(Req::Browse {
        target,
        command: format!("lsinfo {}", quote_arg(&path)),
        path,
        title,
        restore_sel: Some(sel),
    });
}

/// Display title for a browse path (the Albums root gets a friendly label).
fn browse_title(path: &str) -> String {
    if path == "list/newest" {
        "Albums (newest)".to_string()
    } else {
        path.to_string()
    }
}

/// Fold one inbound worker message into TuiState. Stale responses (an older epoch,
/// from before a reconnect) are dropped; a wake enqueues a coalesced refresh; art is
/// adopted only if it still matches the current track.
fn apply_inbound(tx: &Sender<Req>, state: &mut TuiState, msg: Inbound) {
    match msg {
        Inbound::Resp { epoch, kind } => {
            if state::resp_is_stale(epoch, state.epoch) {
                return;
            }
            apply_resp(tx, state, kind);
        }
        Inbound::Wake(_subsystems) => {
            // Any wake -> a single version-gated refresh (the daemon's idle is a
            // deliberate `changed: player` stub, so liveness rides on any-wake, not
            // on subsystem granularity). Skipped if a refresh is already in flight.
            if state::wake_wants_refresh(state.refresh_in_flight) {
                state.refresh_in_flight = true;
                let _ = tx.send(Req::Refresh { known_version: state.queue_version });
            } else {
                // A wake arrived while a refresh is already in flight: that refresh may
                // have read the server state before this change landed, so dropping the
                // wake outright would leave the now-playing stale until the 5s safety
                // net. Mark dirty; the in-flight Snapshot re-arms one catch-up refresh.
                state.refresh_dirty = true;
            }
        }
        Inbound::Art { uri, art } => {
            // Adopt only if it is still the current track's cover (a late fetch for a
            // since-changed track is discarded).
            if state.now.file.as_deref() == Some(uri.as_str()) {
                state.art = art;
            }
        }
        Inbound::Connected { epoch } => {
            state.epoch = epoch;
            state.mark_connected();
        }
        Inbound::Disconnected => state.mark_disconnected(),
        Inbound::CcProgress(phase) => {
            // An empty phase clears the spinner line (call settled).
            state.dj_phase = if phase.is_empty() { None } else { Some(phase) };
        }
        Inbound::CcLine(line) => {
            // A discrete line (result summary / miss / error).
            state.push_dj_log(line);
        }
        Inbound::CcConfirm(pending) => {
            // The settled, validated plan: drive the standard confirm popup. Arming
            // on `y` runs the direct `plan add <dsl>` on the command worker.
            state.dj_phase = None;
            state.enter_confirm(pending);
        }
    }
}

/// Apply a (non-stale) command-worker response to TuiState.
fn apply_resp(tx: &Sender<Req>, state: &mut TuiState, kind: RespKind) {
    match kind {
        RespKind::Snapshot { now, queue, version } => {
            match queue {
                Some(q) => {
                    state.apply_snapshot(now, q);
                    state.queue_version = version;
                }
                None => state.apply_now(now),
            }
            clear_refresh_gate(tx, state);
        }
        RespKind::Confirm(pending) => state.enter_confirm(pending),
        RespKind::Banner(msg) => {
            // A refresh that ACKed lands here (not as a Snapshot); clear the gate so a
            // rare status/currentsong ACK never permanently wedges wake-driven
            // refreshes. Clearing on an unrelated banner costs at most one extra cheap
            // refresh.
            state.status_msg = Some(msg);
            clear_refresh_gate(tx, state);
        }
        RespKind::KnobUnknown(line) => {
            // Graceful knob -> setvol fallback: an OLD daemon ACKs `unknown command
            // "knob"`. Compute a setvol from the last-known volume so the volume keys
            // work against both old and new daemons; when the volume was never polled
            // there is no base to step from, so show a banner and skip.
            match state.now.volume {
                Some(v) if v >= 0 => {
                    let delta = if line == "knob up" { KNOB_STEP } else { -KNOB_STEP };
                    let new = (v + delta).clamp(0, 100);
                    let _ = tx.send(Req::Command(format!("setvol {new}")));
                    request_refresh(tx, state);
                }
                _ => {
                    state.status_msg =
                        Some("volume unknown - can't step it on this older daemon".into())
                }
            }
        }
        RespKind::Browse { target, rows, path, title, restore_sel } => {
            if let Some(b) = state.browse_for(target) {
                b.apply(rows, path, title);
                if let Some(sel) = restore_sel {
                    if !b.rows.is_empty() {
                        b.selected = sel.min(b.rows.len() - 1);
                    }
                }
            }
        }
    }
}

/// On a track-uri change, ask the art worker to fetch the new cover once. Clears the
/// stale cover immediately so the old art never lingers during the fetch; a stream /
/// nothing-playing clears art. The art worker posts the decoded cover back as an
/// [`Inbound::Art`].
fn request_art(art_tx: &Sender<String>, state: &mut TuiState) {
    match state.now.file.clone() {
        Some(uri) => {
            if state.art_req_uri.as_deref() != Some(uri.as_str()) {
                state.art_req_uri = Some(uri.clone());
                if state.art.as_ref().map(|a| a.uri.as_str()) != Some(uri.as_str()) {
                    state.art = None;
                }
                let _ = art_tx.send(uri);
            }
        }
        None => {
            if state.art_req_uri.is_some() {
                state.art_req_uri = None;
                state.art = None;
            }
        }
    }
}

/// Rebuild the album sigil when the album identity changes, so it is static per album
/// and cached (no per-frame regen). Only meaningful when no inline-image protocol is
/// available (the renderer only draws the sigil in that image-less path), but we build
/// it regardless so a capability change is cheap. The palette comes from the cover the
/// art worker already fetched (DECORATION policy inside the sigil); no cover -> a
/// hash-only identicon over the neutral palette. Nothing playing clears the sigil.
fn update_sigil(state: &mut TuiState) {
    let empty = state.now.title.is_none() && state.now.artist.is_none() && state.now.album.is_none();
    if empty {
        state.sigil = None;
        return;
    }
    let identity = sigil::album_identity(&state.now);
    let has_art = state.art.is_some();
    if let Some(s) = &state.sigil {
        // Up to date: same album, and not still waiting to upgrade to a real palette.
        if s.identity == identity && (s.has_palette || !has_art) {
            return;
        }
    }
    let palette = state.art.as_ref().map(|a| &a.palette);
    state.sigil = Some(sigil::Sigil::build(
        &identity,
        palette,
        state.term_bg,
        state.truecolor,
    ));
}

/// Emit the OSC terminal title for the current now-playing, but only when it
/// differs from `last` (deduped so the tick never spams the tty). Best-effort:
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

/// Quit teardown: tell the command worker to drain, set the shared stop flag, and
/// `shutdown(Both)` the stored socket handles to unblock the parked reads (idle in
/// particular). Best-effort and non-blocking: we do NOT join (a handle may be stale
/// after a worker-side reconnect); process exit reaps the IO-only threads.
fn teardown(workers: &Workers) {
    let _ = workers.req_tx.send(Req::Shutdown);
    let _ = workers.cc_tx.send(Req::Shutdown);
    workers.stop.store(true, Ordering::Relaxed);
    let _ = workers.cmd_shutdown.shutdown(Shutdown::Both);
    if let Some(h) = &workers.idle_shutdown {
        let _ = h.shutdown(Shutdown::Both);
    }
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
