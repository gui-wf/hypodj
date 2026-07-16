//! The pure, testable core of the jukebox TUI: state, the key -> intent mapping,
//! the command-vs-NL routing reused from hypodj-client, and the confirm state
//! machine. NO terminal, NO network - crossterm KeyEvents come in, Intents go out,
//! and the event loop in main.rs does all the IO.

use std::cell::Cell;
use std::collections::{HashMap, HashSet};

use crossterm::event::{KeyCode, KeyEvent, KeyModifiers};

use hypodj_client::model::{NowPlaying, QueueItem};
use hypodj_client::nl::not_understood_hint;
use hypodj_client::route::{route, Action};

/// Vim-style scrolloff: keep this many rows of context above/below the cursor.
const SCROLLOFF: usize = 3;

/// Scrub step in seconds for ctrl+f (forward) / ctrl+b (back).
const SCRUB_STEP: i32 = 5;

/// Incremental jump-to-match search over an active list. Case-insensitive
/// `contains`, scanning from `origin` forward and wrapping once through the whole
/// list. Pure and testable - no self, no IO.
///
/// - empty query -> `Some(origin)` (cursor stays put)
/// - no match -> `None`
/// - empty list -> `None`
pub fn search_jump(labels: &[&str], query: &str, origin: usize) -> Option<usize> {
    if query.is_empty() {
        return Some(origin);
    }
    search_step(labels, query, origin, true)
}

/// Scan for the next case-insensitive `contains` match starting AT `origin`,
/// stepping forward (`forward`) or backward, wrapping once through the whole list.
/// Pure and testable - the shared engine behind `search_jump` (forward from the
/// origin) and the `n`/`N` repeat-search jumps (which step off the current match).
///
/// - empty query -> `None`
/// - empty list -> `None`
/// - no match -> `None`
pub fn search_step(labels: &[&str], query: &str, origin: usize, forward: bool) -> Option<usize> {
    if query.is_empty() {
        return None;
    }
    let n = labels.len();
    if n == 0 {
        return None;
    }
    let q = query.to_lowercase();
    let start = origin % n;
    for step in 0..n {
        let i = if forward {
            (start + step) % n
        } else {
            (start + n - (step % n)) % n
        };
        if labels[i].to_lowercase().contains(&q) {
            return Some(i);
        }
    }
    None
}

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
                song_count: None,
            }),
            "file" => rows.push(BrowseRow {
                label: path_tail(v).to_string(),
                uri: v.clone(),
                is_dir: false,
                song_count: None,
            }),
            "playlist" => rows.push(BrowseRow {
                label: v.clone(),
                uri: v.clone(),
                is_dir: false,
                song_count: None,
            }),
            "Album" | "Genre" => {
                if let Some(last) = rows.last_mut() {
                    if last.is_dir {
                        last.label = v.clone();
                    }
                }
            }
            "X-SongCount" => {
                if let Some(last) = rows.last_mut() {
                    if last.is_dir {
                        last.song_count = v.parse().ok();
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

/// How much of an album currently sits in the queue, for the browse gutter marker.
#[derive(Debug, PartialEq, Eq, Clone, Copy)]
pub enum QueueMark {
    None,
    Partial,
    Full,
}

/// Classify an album's queue presence from the count of its DISTINCT queued songs
/// and its total track count. Pure and testable.
///
/// - `queued_count == 0` -> [`QueueMark::None`]
/// - a known `song_count > 0` with `queued_count >= song_count` -> [`QueueMark::Full`]
/// - otherwise (some queued, but fewer than the count OR the count is unknown/0)
///   -> [`QueueMark::Partial`]
///
/// The unknown/`0` songCount case degrades to Partial for any queued track - never
/// a false Full. Because the caller counts DISTINCT queued song ids, a duplicated
/// queued track cannot inflate the count past the album size.
pub fn album_mark(queued_count: usize, song_count: Option<u32>) -> QueueMark {
    if queued_count == 0 {
        return QueueMark::None;
    }
    match song_count {
        Some(n) if n > 0 && queued_count >= n as usize => QueueMark::Full,
        _ => QueueMark::Partial,
    }
}

/// The single ASCII gutter glyph for a browse row's queue state (`#` full, `~`
/// partial, ` ` none). ASCII so terminals without good unicode still render it.
pub fn queue_mark_glyph(mark: QueueMark) -> char {
    match mark {
        QueueMark::Full => '#',
        QueueMark::Partial => '~',
        QueueMark::None => ' ',
    }
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
    /// Incremental jump-to-match search over the active list (`/`).
    Search,
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
    /// Total track count for an album dir row, from the daemon's non-standard
    /// `X-SongCount` pair. Drives the full-vs-partial queue marker; `None` when the
    /// listing does not carry it (song rows, playlists, missing count).
    pub song_count: Option<u32>,
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

/// The relative-seek delta of a `seekcur +N` / `seekcur -N` command line, or None
/// if it is not a relative scrub. Used to coalesce a held-scrub burst.
fn seekcur_delta(line: &str) -> Option<i32> {
    let rest = line.strip_prefix("seekcur ")?;
    if rest.starts_with('+') || rest.starts_with('-') {
        rest.parse::<i32>().ok()
    } else {
        None
    }
}

/// Coalesce a frame's drained intents so a burst of held-key autorepeat collapses
/// into ONE real action instead of a backlog. Consecutive relative scrubs
/// (`seekcur +/-N`) SUM into a single seek; everything else passes through in
/// order. This is what makes holding a key track the finger and stop the instant
/// it is released - the loop then applies the REAL summed effect (no faked UI
/// preview, no queued backlog draining after release). Pure and testable.
pub fn coalesce_intents(intents: Vec<Intent>) -> Vec<Intent> {
    let mut out: Vec<Intent> = Vec::new();
    let mut scrub: i32 = 0;
    for it in intents {
        if let Intent::Command(line) = &it {
            if let Some(d) = seekcur_delta(line) {
                scrub = scrub.saturating_add(d);
                continue;
            }
        }
        if scrub != 0 {
            out.push(scrub_intent(scrub));
            scrub = 0;
        }
        out.push(it);
    }
    if scrub != 0 {
        out.push(scrub_intent(scrub));
    }
    out
}

/// Decide whether an inbound `idle` wake should enqueue a `Refresh`, given whether
/// a refresh is already in flight. A wake with nothing in flight starts one (returns
/// true, caller sets the in-flight bool); a wake while one is in flight is dropped
/// (returns false) so a wake-storm - e.g. a fade ramp firing `changed` on every
/// volume step - collapses to a SINGLE version-gated refresh. Pure and testable.
pub fn wake_wants_refresh(refresh_in_flight: bool) -> bool {
    !refresh_in_flight
}

/// Whether an inbound worker response tagged with `resp_epoch` is stale and must be
/// dropped, given the render thread's current `epoch`. The epoch bumps on every
/// reconnect, so a response computed against a since-dead socket (epoch strictly
/// less than current) is discarded rather than folded into a fresh connection's
/// state. Pure and testable.
pub fn resp_is_stale(resp_epoch: u64, current_epoch: u64) -> bool {
    resp_epoch < current_epoch
}

/// Build a single coalesced relative-seek intent (`seekcur +N` keeps the sign).
fn scrub_intent(secs: i32) -> Intent {
    let arg = if secs >= 0 {
        format!("+{secs}")
    } else {
        secs.to_string()
    };
    Intent::Command(format!("seekcur {arg}"))
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
    /// The MPD queue version (`playlist:` in `status`) of the currently-held
    /// `queue`. A refresh re-fetches the (expensive) full `playlistinfo` ONLY when
    /// this changes, so the common actions that never touch the queue (fav, volume,
    /// pause, seek) cost two cheap commands instead of a whole-queue round-trip.
    pub queue_version: Option<u64>,
    /// Decoded cover art for the current track, cached by its uri (fetched on a
    /// dedicated connection when the track changes). `None` for a stream, missing
    /// art, or a fetch/decode failure - the art panel then shows a placeholder.
    pub art: Option<crate::art::AlbumArt>,
    /// The active cursor saved when `/` is pressed, so Esc can restore it after a
    /// non-destructive jump-to-match search.
    pub search_origin: usize,
    /// The last ACCEPTED search query (set on Enter, cleared on a new `/` and on a
    /// screen change). Drives `n`/`N` repeat jumps and the standing substring
    /// highlight while in Normal mode; empty means no standing search.
    pub last_search: String,
    /// True while a `Req::Refresh` is outstanding on the worker (set when one is
    /// sent, cleared when its `Snapshot` lands). Gates wake-driven refreshes so a
    /// wake-storm collapses to one refresh (see [`wake_wants_refresh`]).
    pub refresh_in_flight: bool,
    /// Set when a wake (or a mutation-driven refresh request) is suppressed because a
    /// refresh is already in flight. The outstanding refresh may have read the server
    /// state BEFORE the suppressed change landed, so when its Snapshot (or a Banner
    /// that clears the gate) arrives we re-arm exactly one more refresh to catch up.
    /// Without this a lost wake is not reflected until the 5s safety-net refresh.
    pub refresh_dirty: bool,
    /// The connection epoch, bumped on every worker reconnect. A response tagged
    /// with an older epoch is stale and dropped (see [`resp_is_stale`]).
    pub epoch: u64,
    /// The track uri the art thread was last asked to fetch, so the render thread
    /// only sends one art request per track change.
    pub art_req_uri: Option<String>,
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
            queue_version: None,
            art: None,
            search_origin: 0,
            last_search: String::new(),
            refresh_in_flight: false,
            refresh_dirty: false,
            epoch: 0,
            art_req_uri: None,
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

    /// Update only the now-playing card, leaving the queue (and cursor) untouched.
    /// Used by the fast refresh path when the queue version is unchanged.
    pub fn apply_now(&mut self, now: NowPlaying) {
        self.now = now;
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
        // A refresh outstanding on the dead socket will never land a Snapshot (a
        // Disconnected arrives instead); clear the gate so a post-reconnect wake can
        // drive a fresh refresh.
        self.refresh_in_flight = false;
        // The command worker pushes a catch-up Snapshot on reconnect, so a suppressed
        // wake from the dead socket needs no re-arm; drop the dirty bit.
        self.refresh_dirty = false;
        // Force a full queue re-fetch on reconnect: the queue may have changed while
        // we were away, and the fresh socket's version numbering may differ.
        self.queue_version = None;
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
            Mode::Search => self.key_search(key),
            Mode::Confirm => self.key_confirm(key),
        }
    }

    fn key_normal(&mut self, key: KeyEvent) -> Option<Intent> {
        // Readline-style CONTROL bindings first, so a plain `p`/`n`/`s` never
        // shadows ctrl+p/ctrl+n (cursor), ctrl+f/ctrl+b (scrub), or ctrl+s (fav).
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
                // Scrub the current track (relative seekcur); space vacated scrub.
                KeyCode::Char('f') => Some(Intent::Command(format!("seekcur +{SCRUB_STEP}"))),
                KeyCode::Char('b') => Some(Intent::Command(format!("seekcur -{SCRUB_STEP}"))),
                // Favorite the CURRENT playing track; stop moved to the `:stop` line.
                KeyCode::Char('s') => self.favorite_current(),
                _ => None,
            };
        }
        match key.code {
            // Space ADDS the selected browse row to the queue (no play) and advances
            // the cursor for rapid multi-add; Queue has nothing to add -> no-op.
            KeyCode::Char(' ') => self.enqueue_selected(),
            // Backspace is a safe no-op (browse-back lives on h/Left/Esc).
            KeyCode::Backspace => None,
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
            // `s` stars the SELECTED row; `f` is freed (vim find idiom now on `/`).
            KeyCode::Char('s') => self.favorite_selected(),
            KeyCode::Char('f') => None,
            // `n`/`N` repeat the last accepted search over the active list, stepping
            // OFF the current match (origin +/- 1) so they never re-find in place;
            // no standing search -> no-op.
            KeyCode::Char('n') => {
                self.repeat_search(true);
                None
            }
            KeyCode::Char('N') => {
                self.repeat_search(false);
                None
            }
            // `o` OPENS (drills into) the selected browse directory.
            KeyCode::Char('o') => self.open_selected(),
            // Screen switch: 1/2/3 select the view; main.rs lazily fetches it.
            KeyCode::Char('1') => {
                self.last_search.clear();
                self.screen = Screen::Queue;
                Some(Intent::ShowScreen(Screen::Queue))
            }
            KeyCode::Char('2') => {
                self.last_search.clear();
                self.screen = Screen::Albums;
                Some(Intent::ShowScreen(Screen::Albums))
            }
            KeyCode::Char('3') => {
                self.last_search.clear();
                self.screen = Screen::Playlists;
                Some(Intent::ShowScreen(Screen::Playlists))
            }
            // Back out of a browse drill-down (Queue: no-op).
            KeyCode::Char('h') | KeyCode::Left => self.browse_back(),
            // Esc backs out one browse level; on Queue / a browse root it no-ops.
            KeyCode::Esc => self.browse_back(),
            KeyCode::Enter => self.enter_action(),
            KeyCode::Char(':') => {
                self.mode = Mode::Command;
                self.input.clear();
                None
            }
            KeyCode::Char('/') => {
                self.last_search.clear();
                self.search_origin = self.active_cursor();
                self.input.clear();
                self.mode = Mode::Search;
                None
            }
            KeyCode::Char('q') => Some(Intent::Quit),
            _ => None,
        }
    }

    /// The browse list for a specific target screen, if it is a browse screen. Used
    /// to fold a worker `Browse` response into the right list even if the user has
    /// since switched screens while the fetch was in flight.
    pub fn browse_for(&mut self, target: Screen) -> Option<&mut Browse> {
        match target {
            Screen::Queue => None,
            Screen::Albums => Some(&mut self.albums),
            Screen::Playlists => Some(&mut self.playlists),
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

    /// Enter always PLAYS the selection: Queue plays the selected row; an album/dir
    /// row enqueues the whole album and plays its first track; a song row enqueues
    /// and plays; Playlists loads the selected playlist. Drilling-in moved to `o`.
    fn enter_action(&mut self) -> Option<Intent> {
        match self.screen {
            Screen::Queue => self
                .queue
                .get(self.selected)
                .map(|it| Intent::Command(format!("play {}", it.pos))),
            Screen::Albums => {
                let row = self.albums.rows.get(self.albums.selected)?;
                Some(Intent::Enqueue { uri: row.uri.clone(), play: true })
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

    /// Space: enqueue the selected browse row (no play) and advance the cursor one
    /// row for rapid multi-add. On Playlists a row name is not a file URI, so it
    /// loads via `LoadPlaylist` (mirrors Enter's semantics without playing); an
    /// Albums/dir/song row enqueues with `add <uri>`. Queue has nothing to add ->
    /// no-op.
    fn enqueue_selected(&mut self) -> Option<Intent> {
        let intent = match self.active_browse() {
            Some(b) => {
                let uri = b.rows.get(b.selected).map(|r| r.uri.clone())?;
                match self.screen {
                    Screen::Playlists => Intent::LoadPlaylist(uri),
                    _ => Intent::Enqueue { uri, play: false },
                }
            }
            None => return None,
        };
        self.move_selection(1);
        Some(intent)
    }

    /// `o`: OPEN (drill into) the selected browse directory. A song row or the Queue
    /// screen is a no-op (Enter is the play verb there).
    fn open_selected(&mut self) -> Option<Intent> {
        let b = self.active_browse()?;
        let row = b.rows.get(b.selected)?;
        if row.is_dir {
            Some(Intent::BrowseInto(row.uri.clone()))
        } else {
            None
        }
    }

    /// The labels of the active list, as the eye sees them, for search matching.
    fn active_labels(&self) -> Vec<String> {
        match self.screen {
            Screen::Queue => self
                .queue
                .iter()
                .map(|it| match &it.artist {
                    Some(a) => format!("{} - {}", it.title, a),
                    None => it.title.clone(),
                })
                .collect(),
            Screen::Albums => self.albums.rows.iter().map(|r| r.label.clone()).collect(),
            Screen::Playlists => self.playlists.rows.iter().map(|r| r.label.clone()).collect(),
        }
    }

    /// The active list's current cursor index (Queue or the active browse).
    fn active_cursor(&self) -> usize {
        match self.screen {
            Screen::Queue => self.selected,
            Screen::Albums => self.albums.selected,
            Screen::Playlists => self.playlists.selected,
        }
    }

    /// Set the active list's cursor index (Queue or the active browse).
    fn set_active_cursor(&mut self, i: usize) {
        match self.screen {
            Screen::Queue => self.selected = i,
            Screen::Albums => self.albums.selected = i,
            Screen::Playlists => self.playlists.selected = i,
        }
    }

    /// Re-run the incremental search from `search_origin` against the current input,
    /// moving the active cursor to the match (or sticking at the origin on no match).
    fn run_search(&mut self) {
        let labels = self.active_labels();
        let refs: Vec<&str> = labels.iter().map(String::as_str).collect();
        let i = search_jump(&refs, &self.input, self.search_origin).unwrap_or(self.search_origin);
        self.set_active_cursor(i);
    }

    /// Repeat the last accepted search over the active list, stepping OFF the
    /// current match: forward (`n`) from cursor+1, backward (`N`) from cursor-1,
    /// wrapping once. A no-match or empty standing search leaves the cursor put.
    fn repeat_search(&mut self, forward: bool) {
        if self.last_search.is_empty() {
            return;
        }
        let labels = self.active_labels();
        let refs: Vec<&str> = labels.iter().map(String::as_str).collect();
        let cur = self.active_cursor();
        let origin = if forward { cur + 1 } else { cur.saturating_sub(1) };
        if let Some(i) = search_step(&refs, &self.last_search, origin, forward) {
            self.set_active_cursor(i);
        }
    }

    /// The query currently driving the substring highlight: the live input while in
    /// Search mode, else the standing `last_search` in Normal mode, else empty.
    /// Used by the renderer to underline every matching row.
    pub fn highlight_query(&self) -> &str {
        match self.mode {
            Mode::Search => &self.input,
            Mode::Normal => &self.last_search,
            _ => "",
        }
    }

    /// Map of `album/<id>` -> the set of DISTINCT queued `song/<id>` uris for that
    /// album, folded from the current queue. A set (not a count) so a duplicated
    /// queued track cannot inflate an album past Full. Drives the browse markers.
    pub fn queued_by_album(&self) -> HashMap<String, HashSet<String>> {
        let mut map: HashMap<String, HashSet<String>> = HashMap::new();
        for it in &self.queue {
            if let (Some(al), Some(uri)) = (&it.album_uri, &it.uri) {
                map.entry(al.clone()).or_default().insert(uri.clone());
            }
        }
        map
    }

    /// The set of DISTINCT `song/<id>` uris currently in the queue, so an opened
    /// album's song rows can be marked when they are already queued.
    pub fn queued_uris(&self) -> HashSet<String> {
        self.queue.iter().filter_map(|it| it.uri.clone()).collect()
    }

    /// Incremental jump-to-match search: Char/Backspace re-run the jump, Enter
    /// accepts in place, Esc restores the pre-search cursor. Non-destructive.
    fn key_search(&mut self, key: KeyEvent) -> Option<Intent> {
        match key.code {
            KeyCode::Esc => {
                self.set_active_cursor(self.search_origin);
                self.mode = Mode::Normal;
                self.input.clear();
                None
            }
            KeyCode::Enter => {
                // Keep the accepted query as the standing search for n/N + the
                // post-accept highlight; an empty query (bare Enter) leaves any
                // prior standing search untouched (Esc-like no-op).
                if !self.input.is_empty() {
                    self.last_search = self.input.clone();
                }
                self.mode = Mode::Normal;
                self.input.clear();
                None
            }
            KeyCode::Backspace => {
                self.input.pop();
                self.run_search();
                None
            }
            KeyCode::Char(c) => {
                self.input.push(c);
                self.run_search();
                None
            }
            _ => None,
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
        QueueItem {
            pos,
            title: format!("t{pos}"),
            artist: None,
            uri: Some(format!("song/{pos}")),
            album_uri: None,
        }
    }

    fn cmd(s: &str) -> Intent {
        Intent::Command(s.to_string())
    }

    #[test]
    fn coalesce_sums_a_held_scrub_burst_into_one_seek() {
        // A held Space burst: five +5 scrubs collapse to a single +25 seek, so the
        // player jumps once instead of draining five queued seeks after release.
        let batch = (0..5).map(|_| cmd("seekcur +5")).collect();
        assert_eq!(coalesce_intents(batch), vec![cmd("seekcur +25")]);
        // Mixed directions net out (held back then forward).
        let batch = vec![cmd("seekcur -5"), cmd("seekcur -5"), cmd("seekcur +5")];
        assert_eq!(coalesce_intents(batch), vec![cmd("seekcur -5")]);
        // A net-zero burst emits nothing (no spurious seek).
        assert_eq!(coalesce_intents(vec![cmd("seekcur +5"), cmd("seekcur -5")]), vec![]);
    }

    #[test]
    fn wake_gate_collapses_a_storm_to_one_refresh() {
        // First wake with nothing in flight -> start a refresh.
        assert!(wake_wants_refresh(false));
        // A wake while a refresh is already in flight -> dropped (storm collapses).
        assert!(!wake_wants_refresh(true));
    }

    #[test]
    fn stale_resp_dropped_below_current_epoch() {
        // A response from before a reconnect (older epoch) is stale.
        assert!(resp_is_stale(0, 1));
        // Same-epoch and future-epoch responses are live.
        assert!(!resp_is_stale(1, 1));
        assert!(!resp_is_stale(2, 1));
    }

    #[test]
    fn coalesce_preserves_order_around_non_scrub() {
        // Non-scrub intents pass through in order; a scrub run flushes as one seek
        // before the next distinct action. Absolute setvol is NOT a relative scrub.
        let batch = vec![
            cmd("seekcur +5"),
            cmd("seekcur +5"),
            cmd("knob down"),
            cmd("seekcur -5"),
            cmd("setvol 40"),
        ];
        assert_eq!(
            coalesce_intents(batch),
            vec![cmd("seekcur +10"), cmd("knob down"), cmd("seekcur -5"), cmd("setvol 40")],
        );
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
        // Space is enqueue-selected (Queue: no-op); Backspace is a safe no-op.
        assert_eq!(s.handle_key(ch(' ')), None);
        assert_eq!(s.handle_key(key(KeyCode::Backspace)), None);
        // Pause on bare `p`.
        assert_eq!(s.handle_key(ch('p')), Some(Intent::Command("pause".into())));
        // `<`/`>` are prev/next.
        assert_eq!(s.handle_key(ch('<')), Some(Intent::Command("previous".into())));
        assert_eq!(s.handle_key(ch('>')), Some(Intent::Command("next".into())));
        assert_eq!(s.handle_key(ch('q')), Some(Intent::Quit));
        // Bare b/f are freed - no transport. Bare n/N are the repeat-search jumps:
        // with no standing search they are inert no-ops (return None, cursor put).
        assert_eq!(s.handle_key(ch('n')), None);
        assert_eq!(s.handle_key(ch('N')), None);
        assert_eq!(s.handle_key(ch('b')), None);
        assert_eq!(s.handle_key(ch('f')), None);
    }

    #[test]
    fn n_and_shift_n_repeat_the_accepted_search() {
        let mut s = TuiState::new();
        s.albums.rows = vec![
            brow("Alpha", "album/1", true),
            brow("Beta", "album/2", true),
            brow("Gamma", "album/3", true),
            brow("beta two", "album/4", true),
        ];
        s.screen = Screen::Albums;
        // No standing search -> n/N are inert.
        s.handle_key(ch('n'));
        assert_eq!(s.albums.selected, 0);
        // Accept a `/beta` search: it jumps to the first match (index 1).
        s.handle_key(ch('/'));
        for c in "beta".chars() {
            s.handle_key(ch(c));
        }
        s.handle_key(key(KeyCode::Enter));
        assert_eq!(s.albums.selected, 1);
        assert_eq!(s.last_search, "beta");
        // n steps OFF the current match to the next one (index 3).
        s.handle_key(ch('n'));
        assert_eq!(s.albums.selected, 3);
        // n again wraps back to index 1.
        s.handle_key(ch('n'));
        assert_eq!(s.albums.selected, 1);
        // N steps backward, wrapping to index 3.
        s.handle_key(ch('N'));
        assert_eq!(s.albums.selected, 3);
        // A screen switch clears the standing search (no stale highlight/jump).
        s.handle_key(ch('1'));
        assert_eq!(s.last_search, "");
    }

    #[test]
    fn search_step_directions_wrap_and_case() {
        let labels = ["Alpha", "Beta", "Gamma", "beta two"];
        // Forward from origin 2 finds index 3 (wrapping would reach 1).
        assert_eq!(search_step(&labels, "beta", 2, true), Some(3));
        // Backward from origin 2 finds index 1.
        assert_eq!(search_step(&labels, "beta", 2, false), Some(1));
        // Case-insensitive.
        assert_eq!(search_step(&labels, "GAMMA", 0, true), Some(2));
        // Forward wrap from a late origin.
        assert_eq!(search_step(&labels, "alpha", 3, true), Some(0));
        // Empty query / empty list / no match -> None.
        assert_eq!(search_step(&labels, "", 0, true), None);
        assert_eq!(search_step(&[], "x", 0, true), None);
        assert_eq!(search_step(&labels, "zzz", 0, true), None);
    }

    #[test]
    fn album_mark_none_partial_full() {
        // Nothing queued -> None.
        assert_eq!(album_mark(0, Some(10)), QueueMark::None);
        // All tracks queued (>= count) -> Full.
        assert_eq!(album_mark(10, Some(10)), QueueMark::Full);
        assert_eq!(album_mark(11, Some(10)), QueueMark::Full);
        // Some but not all -> Partial.
        assert_eq!(album_mark(3, Some(10)), QueueMark::Partial);
        // Unknown or zero songCount with queued tracks -> Partial (never false Full).
        assert_eq!(album_mark(3, None), QueueMark::Partial);
        assert_eq!(album_mark(3, Some(0)), QueueMark::Partial);
    }

    #[test]
    fn queued_by_album_dedups_and_groups() {
        let mut s = TuiState::new();
        let it = |pos: usize, uri: &str, al: &str| QueueItem {
            pos,
            title: format!("t{pos}"),
            artist: None,
            uri: Some(uri.into()),
            album_uri: Some(al.into()),
        };
        // Two distinct songs of album/1, plus a DUPLICATE of song/1 (must not
        // double-count), plus one song of album/2.
        s.queue = vec![
            it(0, "song/1", "album/1"),
            it(1, "song/2", "album/1"),
            it(2, "song/1", "album/1"),
            it(3, "song/9", "album/2"),
        ];
        let map = s.queued_by_album();
        assert_eq!(map.get("album/1").map(|s| s.len()), Some(2));
        assert_eq!(map.get("album/2").map(|s| s.len()), Some(1));
        // A full album/1 (count 2) marks Full despite the duplicate row.
        assert_eq!(album_mark(map["album/1"].len(), Some(2)), QueueMark::Full);
        assert!(s.queued_uris().contains("song/9"));
    }

    #[test]
    fn parse_browse_captures_song_count_on_dir() {
        let pairs: Vec<(String, String)> = vec![
            ("directory".into(), "album/1".into()),
            ("Album".into(), "X".into()),
            ("X-SongCount".into(), "12".into()),
            ("file".into(), "song/9".into()),
            ("Title".into(), "T".into()),
            // A stray count on a file row is ignored (not a dir).
            ("X-SongCount".into(), "99".into()),
        ];
        let rows = parse_browse(&pairs);
        assert_eq!(rows[0].song_count, Some(12));
        assert_eq!(rows[1].song_count, None);
    }

    #[test]
    fn ctrl_f_and_b_scrub() {
        let mut s = TuiState::new();
        assert_eq!(s.handle_key(ctrl('f')), Some(Intent::Command("seekcur +5".into())));
        assert_eq!(s.handle_key(ctrl('b')), Some(Intent::Command("seekcur -5".into())));
    }

    #[test]
    fn ctrl_s_favorites_current() {
        let mut s = TuiState::new();
        // Current song row -> star it.
        s.now.file = Some("song/3".into());
        assert_eq!(
            s.handle_key(ctrl('s')),
            Some(Intent::Command("playlistadd Starred song/3".into()))
        );
        // A stream -> friendly status, no command.
        s.now.file = Some("http://stream.example/live".into());
        assert_eq!(s.handle_key(ctrl('s')), None);
        assert!(s.status_msg.is_some());
        // Nothing playing -> friendly status.
        s.now.file = None;
        assert_eq!(s.handle_key(ctrl('s')), None);
        assert!(s.status_msg.is_some());
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
    fn s_favorites_selected_row() {
        let mut s = TuiState::new();
        s.apply_snapshot(NowPlaying::default(), vec![item(6), item(7)]);
        s.selected = 1;
        assert_eq!(
            s.handle_key(ch('s')),
            Some(Intent::Command("playlistadd Starred song/7".into()))
        );
        // A stream row (URL uri) is a friendly status, not a command.
        s.queue[1].uri = Some("http://stream.example/live".into());
        assert_eq!(s.handle_key(ch('s')), None);
        assert!(s.status_msg.is_some());
        // No uri at all -> silent no-op.
        s.queue[1].uri = None;
        assert_eq!(s.handle_key(ch('s')), None);
        // Empty queue -> no-op.
        s.apply_snapshot(NowPlaying::default(), vec![]);
        assert_eq!(s.handle_key(ch('s')), None);
    }

    #[test]
    fn space_enqueues_and_advances_on_browse() {
        let mut s = TuiState::new();
        // Queue: space is a no-op and leaves the cursor put.
        s.apply_snapshot(NowPlaying::default(), vec![item(0), item(1)]);
        assert_eq!(s.handle_key(ch(' ')), None);
        assert_eq!(s.selected, 0);
        // Browse: space enqueues the selected uri (no play) and advances the cursor.
        s.albums.rows = vec![brow("a", "song/1", false), brow("b", "song/2", false)];
        s.screen = Screen::Albums;
        assert_eq!(
            s.handle_key(ch(' ')),
            Some(Intent::Enqueue { uri: "song/1".into(), play: false })
        );
        assert_eq!(s.albums.selected, 1);
        // Playlists: space loads the playlist (a name is not a file uri) and advances.
        s.playlists.rows = vec![brow("Starred", "Starred", false), brow("Chill", "Chill", false)];
        s.screen = Screen::Playlists;
        assert_eq!(s.handle_key(ch(' ')), Some(Intent::LoadPlaylist("Starred".into())));
        assert_eq!(s.playlists.selected, 1);
    }

    #[test]
    fn o_opens_selected_dir() {
        let mut s = TuiState::new();
        s.albums.rows = vec![brow("X", "album/9", true), brow("song", "song/7", false)];
        s.screen = Screen::Albums;
        // A dir row -> BrowseInto.
        assert_eq!(s.handle_key(ch('o')), Some(Intent::BrowseInto("album/9".into())));
        // A song row -> no-op.
        s.albums.selected = 1;
        assert_eq!(s.handle_key(ch('o')), None);
        // Queue -> no-op.
        s.screen = Screen::Queue;
        assert_eq!(s.handle_key(ch('o')), None);
    }

    #[test]
    fn esc_in_normal_backs_out_browse() {
        let mut s = TuiState::new();
        // Queue: Esc is a no-op.
        assert_eq!(s.handle_key(key(KeyCode::Esc)), None);
        s.screen = Screen::Albums;
        // Browse root (empty stack) -> no-op.
        assert_eq!(s.handle_key(key(KeyCode::Esc)), None);
        // Browse sub-level (non-empty stack) -> BrowseBack.
        s.albums.stack.push(("list/newest".into(), 0));
        assert_eq!(s.handle_key(key(KeyCode::Esc)), Some(Intent::BrowseBack));
    }

    #[test]
    fn colon_opens_command_slash_opens_search() {
        let mut s = TuiState::new();
        s.apply_snapshot(NowPlaying::default(), vec![item(0), item(1)]);
        s.selected = 1;
        // `:` -> Command mode.
        assert_eq!(s.handle_key(ch(':')), None);
        assert_eq!(s.mode, Mode::Command);
        s.handle_key(key(KeyCode::Esc));
        // `/` -> Search mode, seeding search_origin from the active cursor.
        assert_eq!(s.handle_key(ch('/')), None);
        assert_eq!(s.mode, Mode::Search);
        assert_eq!(s.search_origin, 1);
    }

    #[test]
    fn search_jump_matches_wraps_and_cases() {
        let labels = ["Alpha", "Beta", "Gamma", "beta two"];
        // Forward match from origin 0.
        assert_eq!(search_jump(&labels, "Beta", 0), Some(1));
        // Case-insensitive.
        assert_eq!(search_jump(&labels, "gamma", 0), Some(2));
        // Wrap-around from a late origin: from 3, "alpha" wraps to index 0.
        assert_eq!(search_jump(&labels, "alpha", 3), Some(0));
        // From origin 2, "beta" finds index 3 first (forward), not 1.
        assert_eq!(search_jump(&labels, "beta", 2), Some(3));
        // Empty query keeps the cursor at origin.
        assert_eq!(search_jump(&labels, "", 2), Some(2));
        // No match -> None.
        assert_eq!(search_jump(&labels, "zzz", 0), None);
        // Empty list -> None.
        assert_eq!(search_jump(&[], "x", 0), None);
    }

    #[test]
    fn search_mode_transitions() {
        let mut s = TuiState::new();
        s.albums.rows = vec![
            brow("Alpha", "song/1", false),
            brow("Beta", "song/2", false),
            brow("Gamma", "song/3", false),
        ];
        s.screen = Screen::Albums;
        s.albums.selected = 0;
        // Enter search, seeding origin.
        s.handle_key(ch('/'));
        assert_eq!(s.search_origin, 0);
        // Typing a matching char jumps the active cursor.
        s.handle_key(ch('g'));
        assert_eq!(s.albums.selected, 2);
        // Enter accepts in place and returns to Normal.
        s.handle_key(key(KeyCode::Enter));
        assert_eq!(s.mode, Mode::Normal);
        assert_eq!(s.albums.selected, 2);
        // A no-match char leaves the cursor at origin (never jumps to 0 blindly).
        s.albums.selected = 1;
        s.handle_key(ch('/'));
        assert_eq!(s.search_origin, 1);
        s.handle_key(ch('z'));
        assert_eq!(s.albums.selected, 1);
        // Esc restores the pre-search cursor (origin 1), even after it moved.
        s.albums.selected = 2;
        s.handle_key(key(KeyCode::Esc));
        assert_eq!(s.mode, Mode::Normal);
        assert_eq!(s.albums.selected, 1);
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
        s.handle_key(ch(':'));
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
        BrowseRow { label: label.into(), uri: uri.into(), is_dir, song_count: None }
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
        // Albums: Enter now PLAYS both a dir (enqueue album + play first) and a song.
        s.albums.rows = vec![brow("X", "album/9", true), brow("song", "song/7", false)];
        s.screen = Screen::Albums;
        assert_eq!(
            s.handle_key(key(KeyCode::Enter)),
            Some(Intent::Enqueue { uri: "album/9".into(), play: true })
        );
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
        assert_eq!(s.handle_key(ctrl('f')), Some(Intent::Command("seekcur +5".into())));
        assert_eq!(s.handle_key(ch('p')), Some(Intent::Command("pause".into())));
        assert_eq!(s.handle_key(ch('>')), Some(Intent::Command("next".into())));
        assert_eq!(s.handle_key(ch('0')), Some(Intent::Command("knob up".into())));
    }

    #[test]
    fn apply_now_keeps_queue_and_cursor() {
        // The fast refresh path (queue version unchanged) updates only now-playing
        // and must NOT touch the held queue or the cursor.
        let mut s = TuiState::new();
        s.apply_snapshot(NowPlaying::default(), vec![item(0), item(1), item(2)]);
        s.selected = 2;
        let mut np = NowPlaying::default();
        np.title = Some("New Track".into());
        s.apply_now(np);
        assert_eq!(s.now.title.as_deref(), Some("New Track"), "now-playing updated");
        assert_eq!(s.queue.len(), 3, "queue untouched");
        assert_eq!(s.selected, 2, "cursor untouched");
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
        assert_eq!(
            rows[0],
            BrowseRow { label: "X".into(), uri: "album/1".into(), is_dir: true, song_count: None }
        );
        assert_eq!(
            rows[1],
            BrowseRow { label: "T - A".into(), uri: "song/9".into(), is_dir: false, song_count: None }
        );
        assert_eq!(
            rows[2],
            BrowseRow {
                label: "Starred".into(),
                uri: "Starred".into(),
                is_dir: false,
                song_count: None,
            }
        );
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
