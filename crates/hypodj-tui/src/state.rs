//! The pure, testable core of the jukebox TUI: state, the key -> intent mapping,
//! the command-vs-NL routing reused from hypodj-client, and the confirm state
//! machine. NO terminal, NO network - crossterm KeyEvents come in, Intents go out,
//! and the event loop in main.rs does all the IO.

use std::cell::Cell;

use crossterm::event::{KeyCode, KeyEvent, KeyModifiers};

use hypodj_client::model::{NowPlaying, QueueItem};
use hypodj_client::nl::not_understood_hint;
use hypodj_client::route::{route, Action};

/// Vim-style scrolloff: keep this many rows of context above/below the cursor.
const SCROLLOFF: usize = 3;

/// Scrub step in seconds for Space (forward) / Backspace (back).
const SCRUB_STEP: i32 = 5;

/// Derive the top visible row for a scrolloff viewport. Pure and testable: given
/// the selected row `sel`, the queue length `n`, the viewport height `h`, and the
/// previous offset `prev`, return the new top row.
///
/// - Top-edge exception: when `sel < so` the cursor reaches literal row 0 with no
///   top buffer (falls out of the saturating_sub).
/// - Bottom reachable: the offset is clamped to `n - h`, so the cursor advances
///   into the bottom margin to reach the last row.
/// - Mid-list the cursor pins (at `h-1-so` going down, `so` going up) while the
///   list scrolls underneath.
/// - In a tiny viewport `so` shrinks so the top/bottom margins never overlap.
pub fn scroll_offset(sel: usize, n: usize, h: usize, prev: usize) -> usize {
    if n == 0 || h == 0 {
        return 0;
    }
    let so = SCROLLOFF.min(h.saturating_sub(1) / 2);
    let max_off = n.saturating_sub(h);
    let mut off = prev;
    if sel < off + so {
        off = sel.saturating_sub(so);
    }
    if sel + so >= off + h {
        off = (sel + so + 1).saturating_sub(h);
    }
    off.min(max_off)
}

/// Group a server browse pair list into rows. A `directory:` pair starts a dir row
/// (label refined by a following `Album`/`Artist`/`Genre` value, else the path
/// tail); a `file:` pair starts a song row (label from `Title`, with ` - <artist>`
/// appended); a `playlist:` pair becomes a name row for the Playlists screen. Pure
/// and testable - mirrors the boundary logic of client model.rs::group_blocks.
pub fn parse_browse(pairs: &[(String, String)]) -> Vec<BrowseRow> {
    let mut rows: Vec<BrowseRow> = Vec::new();
    for (k, v) in pairs {
        match k.as_str() {
            "directory" => rows.push(BrowseRow {
                label: path_tail(v).to_string(),
                uri: v.clone(),
                is_dir: true,
            }),
            "file" => rows.push(BrowseRow {
                label: path_tail(v).to_string(),
                uri: v.clone(),
                is_dir: false,
            }),
            "playlist" => rows.push(BrowseRow {
                label: v.clone(),
                uri: v.clone(),
                is_dir: false,
            }),
            "Album" | "Genre" => {
                if let Some(last) = rows.last_mut() {
                    if last.is_dir {
                        last.label = v.clone();
                    }
                }
            }
            "Title" => {
                if let Some(last) = rows.last_mut() {
                    if !last.is_dir {
                        last.label = v.clone();
                    }
                }
            }
            "Artist" => {
                if let Some(last) = rows.last_mut() {
                    if !last.is_dir {
                        last.label = format!("{} - {}", last.label, v);
                    }
                }
            }
            _ => {}
        }
    }
    rows
}

/// The last `/`-separated segment of a browse path, used as a fallback row label.
fn path_tail(p: &str) -> &str {
    p.rsplit('/').next().unwrap_or(p)
}

/// Which input surface has focus.
#[derive(Debug, PartialEq, Eq)]
pub enum Mode {
    /// Keybindings + queue navigation.
    Normal,
    /// The bottom command line (bare verb OR natural-language phrase).
    Command,
    /// The y/N confirm popup for an armed plan (NL echo) or a destructive verb.
    Confirm,
}

/// A plan awaiting confirmation. Either an owner-scoped NL `token` (confirm via
/// `nl confirm <token>`) OR a direct `command` (e.g. destructive `clear`).
#[derive(Debug, Default, PartialEq, Eq)]
pub struct Pending {
    pub token: Option<String>,
    pub command: Option<String>,
    pub steps: Vec<String>,
    pub note: Option<String>,
    /// The "via rules" / "via local model" trust footnote from the nl echo.
    pub trust: Option<String>,
}

/// Which main view is showing. Queue is the live-refreshed default; Albums and
/// Playlists are lazily-fetched browse screens.
#[derive(Debug, PartialEq, Eq, Clone, Copy)]
pub enum Screen {
    Queue,
    Albums,
    Playlists,
}

/// One row in a browse list. `uri` is the server browse path (`album/<id>`,
/// `song/<id>`, `list/<name>`) for Albums, or the playlist NAME for Playlists.
#[derive(Debug, PartialEq, Eq, Clone)]
pub struct BrowseRow {
    pub label: String,
    pub uri: String,
    pub is_dir: bool,
}

/// A self-contained browse list with its own cursor, scroll offset, nav stack, and
/// lazy-fetch guard. One per browse screen so cursors are independent.
#[derive(Debug)]
pub struct Browse {
    pub rows: Vec<BrowseRow>,
    pub selected: usize,
    /// The lsinfo path this list currently shows (root default per screen).
    pub path: String,
    /// Display title for the pane header.
    pub title: String,
    /// (path, cursor) of each ancestor level, for BrowseBack.
    pub stack: Vec<(String, usize)>,
    /// Lazy-fetch guard: false until the first ShowScreen fetch lands.
    pub loaded: bool,
    /// Top visible row for the scrolloff viewport (see [`scroll_offset`]).
    pub offset: Cell<usize>,
}

impl Browse {
    fn new(path: &str, title: &str) -> Self {
        Browse {
            rows: Vec::new(),
            selected: 0,
            path: path.to_string(),
            title: title.to_string(),
            stack: Vec::new(),
            loaded: false,
            offset: Cell::new(0),
        }
    }

    /// Replace the rows for a freshly-fetched level, resetting cursor + scroll.
    pub fn apply(&mut self, rows: Vec<BrowseRow>, path: String, title: String) {
        self.rows = rows;
        self.selected = 0;
        self.offset.set(0);
        self.path = path;
        self.title = title;
        self.loaded = true;
    }

    fn move_selection(&mut self, delta: i32) {
        if self.rows.is_empty() {
            self.selected = 0;
            return;
        }
        let last = self.rows.len() - 1;
        let next = self.selected as i32 + delta;
        self.selected = next.clamp(0, last as i32) as usize;
    }
}

/// The side-effecting request handle_key emits for the event loop to run. IO lives
/// entirely in the loop; the state machine only ever returns one of these.
#[derive(Debug, PartialEq, Eq)]
pub enum Intent {
    /// Run one MPD command line, then refresh.
    Command(String),
    /// Send the phrase through the NL handshake, then enter_confirm on the echo.
    Nl(String),
    /// Re-read status + currentsong + playlistinfo.
    Refresh,
    /// Confirm the pending plan (arm it).
    ConfirmArm,
    /// Cancel the pending plan.
    ConfirmCancel,
    /// Switch the main view; main.rs lazily fetches the screen if not loaded.
    ShowScreen(Screen),
    /// Drill into a browse directory (fetch its children via lsinfo <uri>).
    BrowseInto(String),
    /// Pop one browse level and re-fetch the parent.
    BrowseBack,
    /// Enqueue a browse uri (`add <uri>`), optionally play the new tail.
    Enqueue { uri: String, play: bool },
    /// Load a playlist by name (`load <name>`), appending to the queue.
    LoadPlaylist(String),
    /// Leave the session.
    Quit,
}

pub struct TuiState {
    pub now: NowPlaying,
    pub queue: Vec<QueueItem>,
    pub selected: usize,
    /// The active main view.
    pub screen: Screen,
    /// The Albums browse screen (seeded from the `list/newest` smart list).
    pub albums: Browse,
    /// The Playlists browse screen (server currently exposes only `Starred`).
    pub playlists: Browse,
    /// Top visible queue row, derived in render (where the viewport height is
    /// known) via [`scroll_offset`] and persisted here so scroll state survives
    /// across frames. Interior-mutable so the render (which holds `&TuiState`)
    /// can write the freshly computed offset back.
    pub offset: Cell<usize>,
    pub mode: Mode,
    pub input: String,
    pub pending: Option<Pending>,
    pub status_msg: Option<String>,
    pub connected: bool,
}

impl Default for TuiState {
    fn default() -> Self {
        TuiState {
            now: NowPlaying::default(),
            queue: Vec::new(),
            selected: 0,
            screen: Screen::Queue,
            albums: Browse::new("list/newest", "Albums (newest)"),
            playlists: Browse::new("", "Playlists"),
            offset: Cell::new(0),
            mode: Mode::Normal,
            input: String::new(),
            pending: None,
            status_msg: None,
            connected: true,
        }
    }
}

impl TuiState {
    pub fn new() -> Self {
        Self::default()
    }

    /// Apply a fresh (now-playing, queue) snapshot. Clamps `selected` down when the
    /// queue shrinks so it never dangles past the end.
    pub fn apply_snapshot(&mut self, now: NowPlaying, queue: Vec<QueueItem>) {
        self.now = now;
        self.queue = queue;
        if self.queue.is_empty() {
            self.selected = 0;
        } else if self.selected >= self.queue.len() {
            self.selected = self.queue.len() - 1;
        }
    }

    /// Enter the confirm popup for a pending plan.
    pub fn enter_confirm(&mut self, pending: Pending) {
        self.pending = Some(pending);
        self.mode = Mode::Confirm;
        self.input.clear();
    }

    /// Connection dropped: the token is owner-scoped to the dead socket, so any
    /// pending confirm is void. Fall back to Normal and show the reconnect banner.
    pub fn mark_disconnected(&mut self) {
        self.connected = false;
        self.pending = None;
        self.mode = Mode::Normal;
        // Browse caches were fetched on the dead socket; drop them so a reconnect
        // re-fetches on the next screen visit.
        self.albums.loaded = false;
        self.playlists.loaded = false;
        self.status_msg = Some("connection lost - reconnecting...".to_string());
    }

    /// Reconnected on a fresh socket: any old plan is gone, ask for a re-run.
    pub fn mark_connected(&mut self) {
        self.connected = true;
        self.status_msg = Some("reconnected - re-run the phrase".to_string());
    }

    /// Map a key to an Intent (or pure state change). The dispatch is per-mode.
    pub fn handle_key(&mut self, key: KeyEvent) -> Option<Intent> {
        // Any keypress dismisses a stale banner; the action below may set a new one.
        self.status_msg = None;
        match self.mode {
            Mode::Normal => self.key_normal(key),
            Mode::Command => self.key_command(key),
            Mode::Confirm => self.key_confirm(key),
        }
    }

    fn key_normal(&mut self, key: KeyEvent) -> Option<Intent> {
        // Readline-style CONTROL bindings first, so a plain `p`/`n`/`s` never
        // shadows ctrl+p/ctrl+n (cursor) or ctrl+s (stop).
        if key.modifiers.contains(KeyModifiers::CONTROL) {
            return match key.code {
                KeyCode::Char('n') => {
                    self.move_selection(1);
                    None
                }
                KeyCode::Char('p') => {
                    self.move_selection(-1);
                    None
                }
                KeyCode::Char('s') => Some(Intent::Command("stop".into())),
                _ => None,
            };
        }
        match key.code {
            // Space/Backspace scrub the current track (relative seekcur).
            KeyCode::Char(' ') => Some(Intent::Command(format!("seekcur +{SCRUB_STEP}"))),
            KeyCode::Backspace => Some(Intent::Command(format!("seekcur -{SCRUB_STEP}"))),
            KeyCode::Char('p') => Some(Intent::Command("pause".into())),
            // `<`/`>` arrive as Char with SHIFT; the char value already encodes it.
            KeyCode::Char('<') => Some(Intent::Command("previous".into())),
            KeyCode::Char('>') => Some(Intent::Command("next".into())),
            // Volume is a physical-potentiometer KNOB: each press is one equal-
            // loudness (dB) detent, computed server-side. 0/+/= turn it up, 9/-/_
            // down; turning all the way down is the off-click that pauses, and
            // turning up from there resumes. 0 = louder, 9 = quieter.
            KeyCode::Char('+') | KeyCode::Char('=') | KeyCode::Char('0') => {
                Some(Intent::Command("knob up".into()))
            }
            KeyCode::Char('-') | KeyCode::Char('_') | KeyCode::Char('9') => {
                Some(Intent::Command("knob down".into()))
            }
            KeyCode::Char('j') | KeyCode::Down => {
                self.move_selection(1);
                None
            }
            KeyCode::Char('k') | KeyCode::Up => {
                self.move_selection(-1);
                None
            }
            KeyCode::Char('g') => {
                self.go_top();
                None
            }
            KeyCode::Char('G') => {
                self.go_bottom();
                None
            }
            KeyCode::Char('f') => self.favorite_selected(),
            // Screen switch: 1/2/3 select the view; main.rs lazily fetches it.
            KeyCode::Char('1') => {
                self.screen = Screen::Queue;
                Some(Intent::ShowScreen(Screen::Queue))
            }
            KeyCode::Char('2') => {
                self.screen = Screen::Albums;
                Some(Intent::ShowScreen(Screen::Albums))
            }
            KeyCode::Char('3') => {
                self.screen = Screen::Playlists;
                Some(Intent::ShowScreen(Screen::Playlists))
            }
            // Back out of a browse drill-down (Queue: no-op).
            KeyCode::Char('h') | KeyCode::Left => self.browse_back(),
            KeyCode::Enter => self.enter_action(),
            KeyCode::Char('/') | KeyCode::Char(':') => {
                self.mode = Mode::Command;
                self.input.clear();
                None
            }
            KeyCode::Char('q') => Some(Intent::Quit),
            _ => None,
        }
    }

    /// The active screen's browse list, if the active screen is a browse screen.
    pub fn active_browse(&mut self) -> Option<&mut Browse> {
        match self.screen {
            Screen::Queue => None,
            Screen::Albums => Some(&mut self.albums),
            Screen::Playlists => Some(&mut self.playlists),
        }
    }

    /// Jump the selection to the top of the active list (no-op when empty).
    fn go_top(&mut self) {
        match self.active_browse() {
            Some(b) if !b.rows.is_empty() => b.selected = 0,
            Some(_) => {}
            None => {
                if !self.queue.is_empty() {
                    self.selected = 0;
                }
            }
        }
    }

    /// Jump the selection to the last row of the active list (no-op when empty).
    fn go_bottom(&mut self) {
        match self.active_browse() {
            Some(b) if !b.rows.is_empty() => b.selected = b.rows.len() - 1,
            Some(_) => {}
            None => {
                if !self.queue.is_empty() {
                    self.selected = self.queue.len() - 1;
                }
            }
        }
    }

    /// Enter behaviour per screen: Queue plays the selected row; Albums drills into
    /// a directory or enqueues+plays a song; Playlists loads the selected playlist.
    fn enter_action(&mut self) -> Option<Intent> {
        match self.screen {
            Screen::Queue => self
                .queue
                .get(self.selected)
                .map(|it| Intent::Command(format!("play {}", it.pos))),
            Screen::Albums => {
                let row = self.albums.rows.get(self.albums.selected)?;
                if row.is_dir {
                    Some(Intent::BrowseInto(row.uri.clone()))
                } else {
                    Some(Intent::Enqueue { uri: row.uri.clone(), play: true })
                }
            }
            Screen::Playlists => {
                let row = self.playlists.rows.get(self.playlists.selected)?;
                Some(Intent::LoadPlaylist(row.uri.clone()))
            }
        }
    }

    /// Back out one browse level; only on a browse screen with a non-empty stack.
    fn browse_back(&mut self) -> Option<Intent> {
        match self.active_browse() {
            Some(b) if !b.stack.is_empty() => Some(Intent::BrowseBack),
            _ => None,
        }
    }

    /// Favorite (star) the SELECTED row from its uri (`song/<id>`); mirrors Enter
    /// in acting on the cursor, so any track can be starred without playing it. A
    /// stream row (URL uri) is a friendly status; an empty queue is a silent
    /// no-op.
    fn favorite_selected(&mut self) -> Option<Intent> {
        match self.queue.get(self.selected).and_then(|it| it.uri.as_deref()) {
            Some(uri) if uri.starts_with("song/") => {
                Some(Intent::Command(format!("playlistadd Starred {uri}")))
            }
            Some(_) => {
                self.status_msg = Some("that row is a stream, can't favorite".into());
                None
            }
            None => None,
        }
    }

    /// Move the ACTIVE screen's selection with clamping (no wrap). Queue moves
    /// `self.selected`; browse screens move their own cursor.
    fn move_selection(&mut self, delta: i32) {
        if let Some(b) = self.active_browse() {
            b.move_selection(delta);
            return;
        }
        if self.queue.is_empty() {
            self.selected = 0;
            return;
        }
        let last = self.queue.len() - 1;
        let next = self.selected as i32 + delta;
        self.selected = next.clamp(0, last as i32) as usize;
    }

    fn key_command(&mut self, key: KeyEvent) -> Option<Intent> {
        match key.code {
            KeyCode::Esc => {
                self.mode = Mode::Normal;
                self.input.clear();
                None
            }
            KeyCode::Backspace => {
                self.input.pop();
                None
            }
            KeyCode::Enter => self.submit(),
            KeyCode::Char(c) => {
                self.input.push(c);
                None
            }
            _ => None,
        }
    }

    /// Route the typed line through the SAME client route() the CLI uses, so a bare
    /// verb goes to Command and a phrase goes to NL - one routing source.
    fn submit(&mut self) -> Option<Intent> {
        let words: Vec<String> = self.input.split_whitespace().map(str::to_string).collect();
        let action = route(&words);
        self.mode = Mode::Normal;
        self.input.clear();
        match action {
            Action::NowPlaying | Action::Queue => Some(Intent::Refresh),
            Action::Command(line) => Some(Intent::Command(line)),
            Action::Help => {
                self.status_msg = Some(not_understood_hint());
                None
            }
            Action::ClearConfirm => {
                self.enter_confirm(Pending {
                    command: Some("clear".to_string()),
                    token: None,
                    steps: vec!["clear the whole queue".to_string()],
                    note: None,
                    trust: None,
                });
                None
            }
            Action::FavoriteCurrent => self.favorite_current(),
            Action::Nl(phrase) => Some(Intent::Nl(phrase)),
        }
    }

    /// Favorite (star) the current track from a typed `fav`/`favorite` phrase. Needs
    /// the current song uri (`song/<id>`); a raw stream or nothing playing is a
    /// friendly status, not a command.
    fn favorite_current(&mut self) -> Option<Intent> {
        match self.now.file.as_deref() {
            Some(uri) if uri.starts_with("song/") => {
                Some(Intent::Command(format!("playlistadd Starred {uri}")))
            }
            Some(_) => {
                self.status_msg = Some("the current track is a stream, can't favorite".into());
                None
            }
            None => {
                self.status_msg = Some("nothing is playing to favorite".into());
                None
            }
        }
    }

    fn key_confirm(&mut self, key: KeyEvent) -> Option<Intent> {
        match key.code {
            KeyCode::Char('y') | KeyCode::Char('Y') => Some(Intent::ConfirmArm),
            KeyCode::Char('n') | KeyCode::Char('N') | KeyCode::Esc => Some(Intent::ConfirmCancel),
            _ => None,
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crossterm::event::{KeyCode, KeyEvent, KeyModifiers};

    fn key(c: KeyCode) -> KeyEvent {
        KeyEvent::new(c, KeyModifiers::NONE)
    }

    fn ch(c: char) -> KeyEvent {
        key(KeyCode::Char(c))
    }

    fn item(pos: usize) -> QueueItem {
        QueueItem { pos, title: format!("t{pos}"), artist: None, uri: Some(format!("song/{pos}")) }
    }

    fn ctrl(c: char) -> KeyEvent {
        KeyEvent::new(KeyCode::Char(c), KeyModifiers::CONTROL)
    }

    #[test]
    fn apply_snapshot_sets_and_clamps_selected() {
        let mut s = TuiState::new();
        s.apply_snapshot(NowPlaying::default(), vec![item(0), item(1), item(2)]);
        s.selected = 2;
        // Queue shrinks to 1 item -> selected clamps down to 0.
        s.apply_snapshot(NowPlaying::default(), vec![item(0)]);
        assert_eq!(s.queue.len(), 1);
        assert_eq!(s.selected, 0);
        // Empty queue -> selected 0.
        s.apply_snapshot(NowPlaying::default(), vec![]);
        assert_eq!(s.selected, 0);
    }

    #[test]
    fn normal_transport_keys() {
        let mut s = TuiState::new();
        // Space/Backspace scrub (relative seekcur), NOT pause.
        assert_eq!(s.handle_key(ch(' ')), Some(Intent::Command("seekcur +5".into())));
        assert_eq!(
            s.handle_key(key(KeyCode::Backspace)),
            Some(Intent::Command("seekcur -5".into()))
        );
        // Pause moved to bare `p`.
        assert_eq!(s.handle_key(ch('p')), Some(Intent::Command("pause".into())));
        // `<`/`>` are prev/next.
        assert_eq!(s.handle_key(ch('<')), Some(Intent::Command("previous".into())));
        assert_eq!(s.handle_key(ch('>')), Some(Intent::Command("next".into())));
        // ctrl+s stops.
        assert_eq!(s.handle_key(ctrl('s')), Some(Intent::Command("stop".into())));
        assert_eq!(s.handle_key(ch('q')), Some(Intent::Quit));
        // Bare n/b/s are freed - no transport.
        assert_eq!(s.handle_key(ch('n')), None);
        assert_eq!(s.handle_key(ch('b')), None);
        assert_eq!(s.handle_key(ch('s')), None);
    }

    #[test]
    fn ctrl_np_move_cursor() {
        let mut s = TuiState::new();
        s.apply_snapshot(NowPlaying::default(), vec![item(0), item(1), item(2)]);
        assert_eq!(s.handle_key(ctrl('n')), None);
        assert_eq!(s.selected, 1);
        assert_eq!(s.handle_key(ctrl('n')), None);
        assert_eq!(s.selected, 2);
        assert_eq!(s.handle_key(ctrl('p')), None);
        assert_eq!(s.selected, 1);
    }

    #[test]
    fn g_and_shift_g_jump_and_empty_noop() {
        let mut s = TuiState::new();
        s.apply_snapshot(NowPlaying::default(), vec![item(0), item(1), item(2), item(3)]);
        s.selected = 2;
        s.handle_key(ch('g'));
        assert_eq!(s.selected, 0);
        s.handle_key(ch('G'));
        assert_eq!(s.selected, 3);
        // Empty queue -> both no-op.
        s.apply_snapshot(NowPlaying::default(), vec![]);
        s.handle_key(ch('G'));
        assert_eq!(s.selected, 0);
        s.handle_key(ch('g'));
        assert_eq!(s.selected, 0);
    }

    #[test]
    fn f_favorites_selected_row() {
        let mut s = TuiState::new();
        s.apply_snapshot(NowPlaying::default(), vec![item(6), item(7)]);
        s.selected = 1;
        assert_eq!(
            s.handle_key(ch('f')),
            Some(Intent::Command("playlistadd Starred song/7".into()))
        );
        // A stream row (URL uri) is a friendly status, not a command.
        s.queue[1].uri = Some("http://stream.example/live".into());
        assert_eq!(s.handle_key(ch('f')), None);
        assert!(s.status_msg.is_some());
        // No uri at all -> silent no-op.
        s.queue[1].uri = None;
        assert_eq!(s.handle_key(ch('f')), None);
        // Empty queue -> no-op.
        s.apply_snapshot(NowPlaying::default(), vec![]);
        assert_eq!(s.handle_key(ch('f')), None);
    }

    #[test]
    fn keys_9_and_0_turn_the_knob() {
        let mut s = TuiState::new();
        // The knob is a server-side relative control: the keys emit `knob up|down`
        // regardless of the current (client-side, possibly stale) volume, and the
        // server owns the dB step + the off-click pause. 0/+/= up, 9/-/_ down.
        assert_eq!(s.handle_key(ch('0')), Some(Intent::Command("knob up".into())));
        assert_eq!(s.handle_key(ch('9')), Some(Intent::Command("knob down".into())));
        assert_eq!(s.handle_key(ch('+')), Some(Intent::Command("knob up".into())));
        assert_eq!(s.handle_key(ch('-')), Some(Intent::Command("knob down".into())));
        // No dependence on knowing the current volume.
        s.now.volume = None;
        assert_eq!(s.handle_key(ch('0')), Some(Intent::Command("knob up".into())));
    }

    #[test]
    fn scroll_offset_top_edge_and_bottom_and_tiny() {
        // Top-edge exception: cursor within the top margin -> offset 0 (literal top).
        assert_eq!(scroll_offset(1, 100, 10, 0), 0);
        assert_eq!(scroll_offset(0, 100, 10, 0), 0);
        // Moving down past the bottom margin scrolls: sel 6, h 10, so 3 -> pins at
        // 6 + 3 + 1 - 10 = 0 still (6+3 < 10). sel 7 -> 7+3+1-10 = 1.
        assert_eq!(scroll_offset(7, 100, 10, 0), 1);
        // Mid-list the cursor pins at h-1-so while scrolling: sel 50 -> 50+3+1-10=44.
        assert_eq!(scroll_offset(50, 100, 10, 40), 44);
        // Bottom: last row reachable, offset clamps to n-h = 90.
        assert_eq!(scroll_offset(99, 100, 10, 80), 90);
        // Tiny viewport: so shrinks to (h-1)/2 so margins never overlap.
        // h=2 -> so=0; sel 5 -> off = 5+0+1-2 = 4, clamped to n-h=8 -> 4.
        assert_eq!(scroll_offset(5, 10, 2, 0), 4);
        // Empty queue / zero height -> 0.
        assert_eq!(scroll_offset(0, 0, 10, 5), 0);
        assert_eq!(scroll_offset(3, 100, 0, 5), 0);
    }

    #[test]
    fn enter_plays_selected_and_arrows_move() {
        let mut s = TuiState::new();
        s.apply_snapshot(NowPlaying::default(), vec![item(10), item(11), item(12)]);
        // j/Down and k/Up move within bounds, no wrap.
        s.handle_key(ch('j'));
        assert_eq!(s.selected, 1);
        s.handle_key(key(KeyCode::Down));
        assert_eq!(s.selected, 2);
        s.handle_key(key(KeyCode::Down)); // clamp at last
        assert_eq!(s.selected, 2);
        // Enter plays the SELECTED item's pos (12), not the index.
        assert_eq!(s.handle_key(key(KeyCode::Enter)), Some(Intent::Command("play 12".into())));
        s.handle_key(ch('k'));
        s.handle_key(key(KeyCode::Up));
        assert_eq!(s.selected, 0);
        s.handle_key(key(KeyCode::Up)); // clamp at 0
        assert_eq!(s.selected, 0);
    }

    #[test]
    fn command_mode_edit() {
        let mut s = TuiState::new();
        s.handle_key(ch('/'));
        assert_eq!(s.mode, Mode::Command);
        s.handle_key(ch('a'));
        s.handle_key(ch('b'));
        s.handle_key(key(KeyCode::Backspace));
        assert_eq!(s.input, "a");
        s.handle_key(key(KeyCode::Esc));
        assert_eq!(s.mode, Mode::Normal);
        assert_eq!(s.input, "");
    }

    #[test]
    fn submit_routes_verb_vs_nl() {
        let mut s = TuiState::new();
        s.mode = Mode::Command;
        s.input = "pause".into();
        assert_eq!(s.handle_key(key(KeyCode::Enter)), Some(Intent::Command("pause".into())));
        assert_eq!(s.mode, Mode::Normal);
        s.mode = Mode::Command;
        s.input = "fade out".into();
        assert_eq!(s.handle_key(key(KeyCode::Enter)), Some(Intent::Nl("fade out".into())));
    }

    #[test]
    fn confirm_flow() {
        let mut s = TuiState::new();
        s.enter_confirm(Pending {
            token: Some("nl-1".into()),
            command: None,
            steps: vec!["[1] fade out".into()],
            note: Some("NOTE: caveat".into()),
            trust: None,
        });
        assert_eq!(s.mode, Mode::Confirm);
        assert_eq!(s.pending.as_ref().unwrap().steps, vec!["[1] fade out".to_string()]);
        assert_eq!(s.handle_key(ch('x')), None); // ignored
        assert_eq!(s.handle_key(ch('y')), Some(Intent::ConfirmArm));
        assert_eq!(s.handle_key(ch('n')), Some(Intent::ConfirmCancel));
        assert_eq!(s.handle_key(key(KeyCode::Esc)), Some(Intent::ConfirmCancel));
    }

    #[test]
    fn clear_confirm_path() {
        let mut s = TuiState::new();
        s.mode = Mode::Command;
        s.input = "clear".into();
        assert_eq!(s.handle_key(key(KeyCode::Enter)), None);
        assert_eq!(s.mode, Mode::Confirm);
        let p = s.pending.as_ref().unwrap();
        assert_eq!(p.command, Some("clear".to_string()));
        assert_eq!(p.token, None);
    }

    fn brow(label: &str, uri: &str, is_dir: bool) -> BrowseRow {
        BrowseRow { label: label.into(), uri: uri.into(), is_dir }
    }

    #[test]
    fn screen_switch_keys_set_screen_and_intent() {
        let mut s = TuiState::new();
        assert_eq!(s.handle_key(ch('2')), Some(Intent::ShowScreen(Screen::Albums)));
        assert_eq!(s.screen, Screen::Albums);
        assert_eq!(s.handle_key(ch('3')), Some(Intent::ShowScreen(Screen::Playlists)));
        assert_eq!(s.screen, Screen::Playlists);
        assert_eq!(s.handle_key(ch('1')), Some(Intent::ShowScreen(Screen::Queue)));
        assert_eq!(s.screen, Screen::Queue);
    }

    #[test]
    fn nav_moves_active_screen_cursor() {
        let mut s = TuiState::new();
        s.apply_snapshot(NowPlaying::default(), vec![item(0), item(1), item(2)]);
        s.albums.rows = vec![brow("a", "album/1", true), brow("b", "album/2", true), brow("c", "album/3", true)];
        s.screen = Screen::Albums;
        s.handle_key(ch('j'));
        assert_eq!(s.albums.selected, 1);
        s.handle_key(ch('j'));
        s.handle_key(ch('j')); // clamp, no wrap
        assert_eq!(s.albums.selected, 2);
        s.handle_key(ch('k'));
        assert_eq!(s.albums.selected, 1);
        // Queue cursor untouched.
        assert_eq!(s.selected, 0);
    }

    #[test]
    fn enter_is_contextual_per_screen() {
        let mut s = TuiState::new();
        s.apply_snapshot(NowPlaying::default(), vec![item(4), item(5)]);
        s.selected = 1;
        // Queue: play selected pos.
        assert_eq!(s.handle_key(key(KeyCode::Enter)), Some(Intent::Command("play 5".into())));
        // Albums dir -> BrowseInto; song -> Enqueue.
        s.albums.rows = vec![brow("X", "album/9", true), brow("song", "song/7", false)];
        s.screen = Screen::Albums;
        assert_eq!(s.handle_key(key(KeyCode::Enter)), Some(Intent::BrowseInto("album/9".into())));
        s.albums.selected = 1;
        assert_eq!(
            s.handle_key(key(KeyCode::Enter)),
            Some(Intent::Enqueue { uri: "song/7".into(), play: true })
        );
        // Playlists -> LoadPlaylist(name).
        s.playlists.rows = vec![brow("Starred", "Starred", false)];
        s.screen = Screen::Playlists;
        assert_eq!(s.handle_key(key(KeyCode::Enter)), Some(Intent::LoadPlaylist("Starred".into())));
    }

    #[test]
    fn browse_back_needs_stack_and_screen() {
        let mut s = TuiState::new();
        // Queue: h is a no-op.
        assert_eq!(s.handle_key(ch('h')), None);
        s.screen = Screen::Albums;
        // No stack yet -> no-op.
        assert_eq!(s.handle_key(ch('h')), None);
        s.albums.stack.push(("list/newest".into(), 0));
        assert_eq!(s.handle_key(ch('h')), Some(Intent::BrowseBack));
        assert_eq!(s.handle_key(key(KeyCode::Left)), Some(Intent::BrowseBack));
    }

    #[test]
    fn transport_keys_work_on_every_screen() {
        let mut s = TuiState::new();
        s.screen = Screen::Albums;
        assert_eq!(s.handle_key(ch(' ')), Some(Intent::Command("seekcur +5".into())));
        assert_eq!(s.handle_key(ch('p')), Some(Intent::Command("pause".into())));
        assert_eq!(s.handle_key(ctrl('s')), Some(Intent::Command("stop".into())));
        assert_eq!(s.handle_key(ch('>')), Some(Intent::Command("next".into())));
        assert_eq!(s.handle_key(ch('0')), Some(Intent::Command("knob up".into())));
    }

    #[test]
    fn empty_browse_enter_and_move_noop() {
        let mut s = TuiState::new();
        s.screen = Screen::Albums;
        assert_eq!(s.handle_key(key(KeyCode::Enter)), None);
        s.handle_key(ch('j'));
        assert_eq!(s.albums.selected, 0);
    }

    #[test]
    fn parse_browse_groups_dirs_songs_playlists() {
        let pairs: Vec<(String, String)> = vec![
            ("directory".into(), "album/1".into()),
            ("Album".into(), "X".into()),
            ("file".into(), "song/9".into()),
            ("Title".into(), "T".into()),
            ("Artist".into(), "A".into()),
            ("playlist".into(), "Starred".into()),
        ];
        let rows = parse_browse(&pairs);
        assert_eq!(rows.len(), 3);
        assert_eq!(rows[0], BrowseRow { label: "X".into(), uri: "album/1".into(), is_dir: true });
        assert_eq!(rows[1], BrowseRow { label: "T - A".into(), uri: "song/9".into(), is_dir: false });
        assert_eq!(rows[2], BrowseRow { label: "Starred".into(), uri: "Starred".into(), is_dir: false });
    }

    #[test]
    fn disconnect_clears_pending_reconnect_banner() {
        let mut s = TuiState::new();
        s.enter_confirm(Pending { token: Some("nl-1".into()), ..Default::default() });
        s.mark_disconnected();
        assert!(!s.connected);
        assert!(s.pending.is_none());
        assert_eq!(s.mode, Mode::Normal);
        assert!(s.status_msg.as_ref().unwrap().contains("reconnecting"));
        s.mark_connected();
        assert!(s.connected);
        assert!(s.status_msg.as_ref().unwrap().contains("re-run"));
    }
}
