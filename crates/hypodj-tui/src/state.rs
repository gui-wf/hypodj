//! The pure, testable core of the jukebox TUI: state, the key -> intent mapping,
//! the command-vs-NL routing reused from hypodj-client, and the confirm state
//! machine. NO terminal, NO network - crossterm KeyEvents come in, Intents go out,
//! and the event loop in main.rs does all the IO.

use std::cell::Cell;
use std::collections::{HashMap, HashSet};

use crossterm::event::{KeyCode, KeyEvent};

use hypodj_client::model::{NowPlaying, QueueItem};
use hypodj_client::nl::not_understood_hint;
use hypodj_client::route::{route, Action};

use crate::keymap;

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
/// Playlists are lazily-fetched browse screens; Dj is the Claude Code intelligence
/// pane (right of Queue) that translates a typed NL query into a plan to confirm.
#[derive(Debug, PartialEq, Eq, Clone, Copy)]
pub enum Screen {
    Queue,
    Albums,
    Playlists,
    Dj,
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
    /// Translate a DJ View NL query via the Claude Code backend (on the dedicated CC
    /// worker thread, never the command socket), ending in a Confirm popup.
    Cc(String),
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
    /// The ambient-visualizer clock, in seconds. The render loop advances this by
    /// the wall-clock frame delta ONLY while playback is `play` (so it freezes when
    /// paused/stopped) and writes it here before each draw; the idle bottom-bar wave
    /// reads it as its animation phase. Pure display state - no key/logic meaning.
    pub anim_secs: f64,
    /// Free-running animation clock, advanced EVERY frame regardless of play state
    /// (unlike `anim_secs`, which freezes when paused). Drives the DJ "thinking..."
    /// spinner so it keeps rotating while a CC call is in flight even on a paused
    /// or stopped deck. Pure display state - no key/logic meaning.
    pub spin_secs: f64,
    /// The DJ View "ask>" input line (the NL query being typed on Screen::Dj).
    pub dj_input: String,
    /// The DJ View scrollback: coarse CC progress + result lines, newest at the
    /// bottom. Bounded so a long session never grows without limit.
    pub dj_log: Vec<String>,
    /// The current CC phase line (e.g. "thinking..."), shown next to a spinner while
    /// a call is in flight; `None` when idle.
    pub dj_phase: Option<String>,
    /// Whether the REAL post-gain level wave is live this frame (the viz socket is
    /// connected and a frame has landed). `false` => the render draws the decorative
    /// fallback wave. Set by the render loop from the viz worker's slot.
    pub viz_active: bool,
    /// The smoothed normalized level A in `[0, 1]` (the ballistics envelope output),
    /// persisted across frames so the one-pole attack/release integrates over time.
    pub viz_env: f32,
    /// Whether the daemon reports audio is playing (from the latest viz frame); gates
    /// the level wave between the live field and the resting hairline.
    pub viz_playing: bool,
    /// Whether the `?` help overlay is open. Normal-mode-only modal: while open, only
    /// `?`/Esc/q resolve (toggle-close), everything else is swallowed.
    pub help_open: bool,
    /// The help overlay's vertical scroll offset (rows). Nonzero only when the overlay
    /// is taller than the terminal; nav keys scroll it and the renderer clamps it to the
    /// real max so a short terminal can still reach every binding. Reset when help opens.
    pub help_scroll: u16,
    /// The detected terminal background (OSC 11 at startup / on resize), seeded to the
    /// guaranteed dark default so the visual system always has a bg to contrast against.
    pub term_bg: crate::album_color::TermBg,
    /// The detected inline-image protocol; `None` => the album sigil is drawn in the
    /// album-art slot's image-less path.
    pub image_protocol: crate::album_color::ImageProtocol,
    /// Whether the terminal advertises truecolor (else colors quantize to xterm-256).
    pub truecolor: bool,
    /// The cached album sigil, rebuilt only when the album identity changes (static -
    /// never regenerated per frame).
    pub sigil: Option<crate::sigil::Sigil>,
}

/// Perceptual floor / ceiling (dBFS) for the level normalize. Below the floor is
/// the resting hairline; the loudest music tops out at the ceiling.
pub const VIZ_FLOOR_DB: f32 = -54.0;
pub const VIZ_CEIL_DB: f32 = -6.0;

/// Normalize an audible post-gain level (dBFS) into `[0, 1]` in the perceptual dB
/// domain, with a gentle gamma expand of the quiet range so verses do not flatline.
/// Pure and testable.
pub fn normalize_level(post_gain_db: f32) -> f32 {
    let a = ((post_gain_db - VIZ_FLOOR_DB) / (VIZ_CEIL_DB - VIZ_FLOOR_DB)).clamp(0.0, 1.0);
    a.powf(0.8)
}

/// One asymmetric one-pole envelope step on the normalized level, computed at
/// render `dt` (seconds): quick attack (~60 ms) so a swell feels causal, slow
/// release (~350 ms) so it falls like a needle with gravity and never snaps. A fade
/// (falling target) rides the release tau, so the field settles with the audible
/// sound. Pure and testable (deterministic in `dt`, no wall clock).
pub fn envelope_step(prev: f32, target: f32, dt: f32) -> f32 {
    let tau = if target >= prev { 0.060 } else { 0.350 };
    // alpha = 1 - exp(-dt/tau); guard a zero/negative dt.
    let alpha = if dt <= 0.0 { 0.0 } else { 1.0 - (-dt / tau).exp() };
    prev + (target - prev) * alpha.clamp(0.0, 1.0)
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
            anim_secs: 0.0,
            spin_secs: 0.0,
            dj_input: String::new(),
            dj_log: Vec::new(),
            dj_phase: None,
            viz_active: false,
            viz_env: 0.0,
            viz_playing: false,
            help_open: false,
            help_scroll: 0,
            term_bg: crate::album_color::TermBg::dark_default(),
            image_protocol: crate::album_color::ImageProtocol::None,
            truecolor: false,
            sigil: None,
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

    /// Enter the confirm for a pending plan. On the DJ (chat) screen the echo +
    /// y/N prompt is pushed INLINE into the chat scrollback so it reads as part of
    /// the conversation (ui.rs skips the centered popup for Screen::Dj); on the
    /// other screens the popup carries it.
    pub fn enter_confirm(&mut self, pending: Pending) {
        if self.screen == Screen::Dj {
            if let Some(trust) = &pending.trust {
                self.push_dj_log(trust.clone());
            }
            for step in &pending.steps {
                self.push_dj_log(step.clone());
            }
            if let Some(note) = &pending.note {
                self.push_dj_log(format!("! {note}"));
            }
            self.push_dj_log("confirm? [y/N]".to_string());
        }
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
        // The help overlay is a true modal: while open, ONLY `?`/Esc/q toggle it
        // closed and every other key is swallowed (never leaks to nav/transport).
        if self.help_open {
            // A true modal: `?`/Esc/q close it; j/k/arrows/PgUp/PgDn scroll it (so a
            // short terminal that cannot show the whole table can still reach every
            // binding); everything else is swallowed. The offset is clamped against the
            // real content/viewport in the renderer, so an over-scroll just pins to the
            // last page.
            match key.code {
                KeyCode::Char('?') | KeyCode::Esc | KeyCode::Char('q') => {
                    self.help_open = false;
                    self.help_scroll = 0;
                }
                KeyCode::Char('j') | KeyCode::Down => {
                    self.help_scroll = self.help_scroll.saturating_add(1);
                }
                KeyCode::Char('k') | KeyCode::Up => {
                    self.help_scroll = self.help_scroll.saturating_sub(1);
                }
                KeyCode::PageDown | KeyCode::Char(' ') => {
                    self.help_scroll = self.help_scroll.saturating_add(10);
                }
                KeyCode::PageUp => {
                    self.help_scroll = self.help_scroll.saturating_sub(10);
                }
                _ => {}
            }
            return None;
        }
        // The DJ View captures typing into its own "ask>" line (a DJ query is always
        // NL), so nav/verb keys never shadow the input. `1`/`2`/`3` still tab away is
        // NOT wanted here - Esc leaves the pane. Handled before the shared bindings.
        if self.screen == Screen::Dj {
            return self.key_dj(key);
        }
        // Dispatch is DERIVED from the single-source KEYMAP: resolve the key to its
        // Act via `match_key` (which already encodes the readline-first ordering - a
        // Ctrl chord is a `Ctrl` matcher, so a plain `p`/`n`/`s` never shadows
        // `C-p`/`C-n`/`C-s`) and run it through `apply_act`. Because `apply_act` is an
        // EXHAUSTIVE match on `Act`, a new KEYMAP row (help + dispatch) cannot be added
        // without a compiler error until it is handled here, and an Act cannot be
        // removed from dispatch while a row still advertises it - so help and behavior
        // can never drift. Keys with no row (Backspace, a freed `f`) fall through to a
        // safe no-op.
        if let Some(act) = keymap::match_key(key, self.screen) {
            return self.apply_act(act);
        }
        None
    }

    /// Execute a resolved [`keymap::Act`]. The ONE place a normal-mode binding turns
    /// into an Intent or state change; [`key_normal`] routes every table key here, so
    /// this exhaustive match is the dispatch half of the single-source keymap.
    fn apply_act(&mut self, act: keymap::Act) -> Option<Intent> {
        use keymap::Act;
        match act {
            // Screen switch: main.rs lazily fetches the target view.
            Act::ScreenQueue => self.switch_screen(Screen::Queue),
            Act::ScreenAlbums => self.switch_screen(Screen::Albums),
            Act::ScreenPlaylists => self.switch_screen(Screen::Playlists),
            Act::ScreenDj => self.switch_screen(Screen::Dj),
            Act::Down => {
                self.move_selection(1);
                None
            }
            Act::Up => {
                self.move_selection(-1);
                None
            }
            Act::Top => {
                self.go_top();
                None
            }
            Act::Bottom => {
                self.go_bottom();
                None
            }
            // Shift+P jumps the Queue cursor to the currently-playing song (browse
            // screens have no now-playing row, so it no-ops there).
            Act::JumpCurrent => {
                self.go_current();
                None
            }
            Act::SearchStart => {
                self.last_search.clear();
                self.search_origin = self.active_cursor();
                self.input.clear();
                self.mode = Mode::Search;
                None
            }
            // `n`/`N` repeat the last accepted search over the active list, stepping
            // OFF the current match (origin +/- 1); no standing search -> no-op.
            Act::SearchNext => {
                self.repeat_search(true);
                None
            }
            Act::SearchPrev => {
                self.repeat_search(false);
                None
            }
            Act::CommandLine => {
                self.mode = Mode::Command;
                self.input.clear();
                None
            }
            // Volume is a physical-potentiometer KNOB: each press is one equal-
            // loudness (dB) detent, computed server-side.
            Act::VolumeUp => Some(Intent::Command("knob up".into())),
            Act::VolumeDown => Some(Intent::Command("knob down".into())),
            Act::Pause => Some(Intent::Command("pause".into())),
            Act::Next => Some(Intent::Command("next".into())),
            Act::Prev => Some(Intent::Command("previous".into())),
            // Scrub the current track (relative seekcur).
            Act::ScrubFwd => Some(Intent::Command(format!("seekcur +{SCRUB_STEP}"))),
            Act::ScrubBack => Some(Intent::Command(format!("seekcur -{SCRUB_STEP}"))),
            // `s` stars the SELECTED row; C-s stars the CURRENT playing track.
            Act::FavSelected => self.favorite_selected(),
            Act::FavCurrent => self.favorite_current(),
            Act::PlaySel => self.enter_action(),
            // Space ADDS the selected browse row to the queue (Queue: no-op).
            Act::Enqueue => self.enqueue_selected(),
            // `o` OPENS (drills into) the selected browse directory.
            Act::Open => self.open_selected(),
            // Back out of a browse drill-down (Queue / a browse root: no-op).
            Act::BrowseBack => self.browse_back(),
            // `?` opens the help overlay (a normal-mode modal); the modal intercept at
            // the top of key_normal then handles every key until it is toggled closed.
            Act::HelpToggle => {
                self.help_open = true;
                self.help_scroll = 0;
                None
            }
            Act::Quit => Some(Intent::Quit),
        }
    }

    /// Switch to `screen` (clearing any standing search); main.rs lazily fetches it.
    fn switch_screen(&mut self, screen: Screen) -> Option<Intent> {
        self.last_search.clear();
        self.screen = screen;
        Some(Intent::ShowScreen(screen))
    }

    /// The browse list for a specific target screen, if it is a browse screen. Used
    /// to fold a worker `Browse` response into the right list even if the user has
    /// since switched screens while the fetch was in flight.
    pub fn browse_for(&mut self, target: Screen) -> Option<&mut Browse> {
        match target {
            Screen::Queue | Screen::Dj => None,
            Screen::Albums => Some(&mut self.albums),
            Screen::Playlists => Some(&mut self.playlists),
        }
    }

    /// The active screen's browse list, if the active screen is a browse screen.
    pub fn active_browse(&mut self) -> Option<&mut Browse> {
        match self.screen {
            Screen::Queue | Screen::Dj => None,
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

    /// Jump the Queue cursor to the currently-playing song. Queue only (browse
    /// screens have no now-playing row); no-op when nothing is playing or the
    /// current index is out of range. `now.song` is the 0-based queue index of the
    /// current track; the queue is pos-ordered so it normally equals the row index,
    /// but we match on `pos` and fall back to the index directly to be safe.
    fn go_current(&mut self) {
        if self.screen != Screen::Queue || self.queue.is_empty() {
            return;
        }
        if let Some(song) = self.now.song {
            let idx = self
                .queue
                .iter()
                .position(|it| it.pos == song)
                .unwrap_or(song);
            if idx < self.queue.len() {
                self.selected = idx;
            }
        }
    }

    /// Enter always PLAYS the selection: Queue plays the selected row; an album/dir
    /// row enqueues the whole album and plays its first track; a song row enqueues
    /// and plays; Playlists loads the selected playlist. Drilling-in moved to `o`.
    fn enter_action(&mut self) -> Option<Intent> {
        match self.screen {
            // Dj Enter is handled in key_dj (submit the query), never here.
            Screen::Dj => None,
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
            // The DJ pane has no navigable list to search.
            Screen::Dj => Vec::new(),
        }
    }

    /// The active list's current cursor index (Queue or the active browse).
    fn active_cursor(&self) -> usize {
        match self.screen {
            Screen::Queue | Screen::Dj => self.selected,
            Screen::Albums => self.albums.selected,
            Screen::Playlists => self.playlists.selected,
        }
    }

    /// Set the active list's cursor index (Queue or the active browse).
    fn set_active_cursor(&mut self, i: usize) {
        match self.screen {
            Screen::Queue | Screen::Dj => self.selected = i,
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

    /// DJ View input: printable chars build the "ask>" query, Enter submits it,
    /// Esc leaves back to the Queue screen. A blank Enter is a no-op. Enter routes
    /// the phrase through the SAME client route() the ':' command line uses, so a
    /// bare-favorite phrase ("favorite this song") stars the current track here too
    /// instead of falling to the CC translator that has no favorite capability;
    /// anything else stays a CC translation (a DJ query is otherwise never a bare
    /// verb).
    fn key_dj(&mut self, key: KeyEvent) -> Option<Intent> {
        // The Scope::Global view + help bindings must work here too, and they are
        // resolved through the SINGLE-SOURCE keymap (match_key) - NOT hand-written -
        // so the DJ screen can never drift from KEYMAP. Only the four screen-switch
        // Acts and HelpToggle are honored here; every other Global matcher (j/k/p/vol
        // etc.) falls through to be captured as ask-line input, since a DJ query is
        // always typed text. F-keys are never part of an NL query, so switch outright;
        // `?` opens help ONLY on an empty ask line, so a literal `?` can still be typed
        // mid-phrase ("what should I play?").
        if let Some(act) = keymap::match_key(key, self.screen) {
            use keymap::Act;
            match act {
                Act::ScreenQueue | Act::ScreenAlbums | Act::ScreenPlaylists | Act::ScreenDj => {
                    return self.apply_act(act);
                }
                Act::HelpToggle if self.dj_input.is_empty() => {
                    return self.apply_act(act);
                }
                _ => {}
            }
        }
        match key.code {
            KeyCode::Esc => {
                self.screen = Screen::Queue;
                self.dj_input.clear();
                Some(Intent::ShowScreen(Screen::Queue))
            }
            KeyCode::Backspace => {
                self.dj_input.pop();
                None
            }
            KeyCode::Enter => {
                let phrase = self.dj_input.trim().to_string();
                self.dj_input.clear();
                if phrase.is_empty() {
                    return None;
                }
                self.push_dj_log(format!("> {phrase}"));
                let words: Vec<String> =
                    phrase.split_whitespace().map(str::to_string).collect();
                // Route bare control verbs (favorite/star, play/pause/stop/next/prev,
                // clear) to the DETERMINISTIC client verb path BEFORE Claude - the
                // same spirit as the favorite fix. A queue-manipulation ask that is
                // NOT a bare verb (a fuzzy phrase) still goes to CC. This is the
                // hybrid split: bare verbs never reach the translator (which cannot
                // express favorite/clear and would degrade them to a no-op enqueue).
                match route(&words) {
                    Action::FavoriteCurrent => self.favorite_current(),
                    Action::Command(line) => {
                        self.push_dj_log(format!("ok: {line}"));
                        Some(Intent::Command(line))
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
                    Action::NowPlaying | Action::Queue => Some(Intent::Refresh),
                    Action::Help => {
                        self.push_dj_log(not_understood_hint());
                        None
                    }
                    // A fuzzy phrase (queue-edit ask, fade, enqueue, ...) -> Claude.
                    Action::Nl(phrase) => {
                        self.dj_phase = Some("thinking...".to_string());
                        Some(Intent::Cc(phrase))
                    }
                }
            }
            KeyCode::Char(c) => {
                self.dj_input.push(c);
                None
            }
            _ => None,
        }
    }

    /// Append one line to the DJ scrollback, bounding it so a long session never
    /// grows without limit. Pure (no IO) - folded by the render thread on a CC frame.
    pub fn push_dj_log(&mut self, line: String) {
        const MAX_DJ_LOG: usize = 200;
        self.dj_log.push(line);
        if self.dj_log.len() > MAX_DJ_LOG {
            let drop = self.dj_log.len() - MAX_DJ_LOG;
            self.dj_log.drain(0..drop);
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
    fn dj_screen_confirm_is_inline_in_chat_not_popup() {
        let mut s = TuiState::new();
        s.screen = Screen::Dj;
        s.enter_confirm(Pending {
            steps: vec!["[1] add 5 calmer tracks".into()],
            note: Some("append-only".into()),
            ..Default::default()
        });
        assert_eq!(s.mode, Mode::Confirm);
        // The echo + y/N prompt landed inline in the chat scrollback, not only a popup.
        assert!(s.dj_log.iter().any(|l| l.contains("add 5 calmer tracks")));
        assert!(s.dj_log.iter().any(|l| l == "! append-only"));
        assert!(s.dj_log.iter().any(|l| l == "confirm? [y/N]"));
        // On a non-DJ screen the confirm does NOT touch the chat log (popup carries it).
        let mut q = TuiState::new();
        q.enter_confirm(Pending { steps: vec!["clear".into()], ..Default::default() });
        assert!(q.dj_log.is_empty());
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
    fn dj_view_honors_global_view_and_help_bindings() {
        let mut s = TuiState::new();
        s.screen = Screen::Dj;
        // F-keys switch views even from the DJ screen (Scope::Global, never NL text).
        assert_eq!(s.handle_key(key(KeyCode::F(2))), Some(Intent::ShowScreen(Screen::Albums)));
        assert_eq!(s.screen, Screen::Albums);
        // Back into DJ; `?` on an EMPTY ask line opens help.
        s.screen = Screen::Dj;
        assert!(s.dj_input.is_empty());
        assert_eq!(s.handle_key(ch('?')), None);
        assert!(s.help_open);
        // Close help, then a `?` typed mid-phrase is captured as input, not help.
        s.help_open = false;
        s.screen = Screen::Dj;
        s.handle_key(ch('w'));
        s.handle_key(ch('?'));
        assert!(!s.help_open, "? mid-phrase is text, not a help toggle");
        assert_eq!(s.dj_input, "w?");
    }

    #[test]
    fn dj_screen_all_f_keys_dispatch_via_match_key() {
        // Every F1-F4 switch resolves through the SINGLE-SOURCE keymap (match_key) even
        // from the DJ screen, so the Global view keys are alive on every screen and can
        // never drift from KEYMAP.
        for (fk, want) in [
            (KeyCode::F(1), Screen::Queue),
            (KeyCode::F(2), Screen::Albums),
            (KeyCode::F(3), Screen::Playlists),
            (KeyCode::F(4), Screen::Dj),
        ] {
            let mut s = TuiState::new();
            s.screen = Screen::Dj;
            assert_eq!(s.handle_key(key(fk)), Some(Intent::ShowScreen(want)));
            assert_eq!(s.screen, want);
        }
    }

    #[test]
    fn help_overlay_scrolls_and_resets() {
        let mut s = TuiState::new();
        // `?` opens help at the top.
        assert_eq!(s.handle_key(ch('?')), None);
        assert!(s.help_open);
        assert_eq!(s.help_scroll, 0);
        // j / Down scroll down; k / Up scroll up (clamped at 0). PageDown jumps.
        s.handle_key(ch('j'));
        s.handle_key(key(KeyCode::Down));
        assert_eq!(s.help_scroll, 2);
        s.handle_key(ch('k'));
        assert_eq!(s.help_scroll, 1);
        s.handle_key(key(KeyCode::PageDown));
        assert_eq!(s.help_scroll, 11);
        // Up never underflows.
        s.help_scroll = 0;
        s.handle_key(ch('k'));
        assert_eq!(s.help_scroll, 0);
        // Every other key is swallowed while the modal is open (no transport leak).
        assert_eq!(s.handle_key(ch('p')), None);
        assert!(s.help_open);
        // Closing resets the offset so the next open starts at the top.
        s.help_scroll = 5;
        s.handle_key(key(KeyCode::Esc));
        assert!(!s.help_open);
        assert_eq!(s.help_scroll, 0);
    }

    #[test]
    fn every_keymap_row_is_dispatched() {
        // Drift guard: each KEYMAP matcher must resolve through key_normal's table
        // dispatch (apply_act) to a real effect - never silently fall through. We only
        // assert it does not panic and that the Act round-trips (match_key), which with
        // apply_act's exhaustive match is the single-source contract.
        for b in keymap::KEYMAP {
            let mut s = TuiState::new();
            let ev = match b.matchers[0] {
                keymap::KeyMatch::Char(c) => ch(c),
                keymap::KeyMatch::Ctrl(c) => KeyEvent::new(KeyCode::Char(c), KeyModifiers::CONTROL),
                keymap::KeyMatch::Code(code) => key(code),
            };
            assert_eq!(
                keymap::match_key(ev, Screen::Queue),
                Some(b.act),
                "row {:?} does not resolve to its Act",
                b.keys
            );
            // Exercising dispatch must not panic for any table key.
            let _ = s.handle_key(ev);
        }
    }

    #[test]
    fn shift_p_jumps_queue_cursor_to_current_song() {
        let mut s = TuiState::new();
        let now = NowPlaying {
            song: Some(2),
            ..NowPlaying::default()
        };
        s.apply_snapshot(now, vec![item(0), item(1), item(2), item(3), item(4)]);
        s.selected = 4;
        // Shift+P (Char 'P') moves the cursor to the playing row (index 2).
        assert_eq!(s.handle_key(ch('P')), None);
        assert_eq!(s.selected, 2);
        // Idempotent.
        s.handle_key(ch('P'));
        assert_eq!(s.selected, 2);
        // Nothing playing -> cursor unchanged.
        s.apply_snapshot(NowPlaying::default(), vec![item(0), item(1)]);
        s.selected = 1;
        s.handle_key(ch('P'));
        assert_eq!(s.selected, 1);
        // On a browse screen it no-ops (no now-playing row there).
        s.screen = Screen::Albums;
        s.albums.rows = vec![brow("A", "album/1", true), brow("B", "album/2", true)];
        s.albums.selected = 1;
        s.handle_key(ch('P'));
        assert_eq!(s.albums.selected, 1);
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
        s.handle_key(key(KeyCode::F(1)));
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
        assert_eq!(s.handle_key(key(KeyCode::F(2))), Some(Intent::ShowScreen(Screen::Albums)));
        assert_eq!(s.screen, Screen::Albums);
        assert_eq!(s.handle_key(key(KeyCode::F(3))), Some(Intent::ShowScreen(Screen::Playlists)));
        assert_eq!(s.screen, Screen::Playlists);
        assert_eq!(s.handle_key(key(KeyCode::F(1))), Some(Intent::ShowScreen(Screen::Queue)));
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
    fn key_f4_opens_dj_view() {
        let mut s = TuiState::new();
        assert_eq!(s.handle_key(key(KeyCode::F(4))), Some(Intent::ShowScreen(Screen::Dj)));
        assert_eq!(s.screen, Screen::Dj);
    }

    #[test]
    fn dj_input_builds_and_submits_as_cc() {
        let mut s = TuiState::new();
        s.handle_key(key(KeyCode::F(4)));
        assert_eq!(s.screen, Screen::Dj);
        // Printable chars build the ask> line (never shadowed by nav/verb keys like
        // `p`/`j`/`q`, which would be transport/nav on other screens).
        for c in "pause the 3rd".chars() {
            assert_eq!(s.handle_key(ch(c)), None);
        }
        assert_eq!(s.dj_input, "pause the 3rd");
        // Backspace edits.
        s.handle_key(key(KeyCode::Backspace));
        assert_eq!(s.dj_input, "pause the 3r");
        // Enter submits the whole line as a CC translation (always NL), logs the
        // query, sets the thinking phase, and clears the input.
        s.dj_input = "fade out".into();
        assert_eq!(s.handle_key(key(KeyCode::Enter)), Some(Intent::Cc("fade out".into())));
        assert_eq!(s.dj_input, "");
        assert_eq!(s.dj_phase.as_deref(), Some("thinking..."));
        assert!(s.dj_log.iter().any(|l| l == "> fade out"));
        // A blank Enter is a no-op (no spurious CC call).
        let mut s2 = TuiState::new();
        s2.handle_key(key(KeyCode::F(4)));
        assert_eq!(s2.handle_key(key(KeyCode::Enter)), None);
    }

    #[test]
    fn dj_bare_favorite_phrase_stars_current_track() {
        // A bare-favorite phrase typed in the DJ view routes through the SAME
        // route() the ':' line uses, so it stars the current track instead of
        // falling to the CC translator (which has no favorite capability).
        let mut s = TuiState::new();
        s.now.file = Some("song/7".into());
        s.handle_key(key(KeyCode::F(4)));
        s.dj_input = "favorite this song".into();
        assert_eq!(
            s.handle_key(key(KeyCode::Enter)),
            Some(Intent::Command("playlistadd Starred song/7".into()))
        );
        // No spurious CC thinking phase on the favorite path.
        assert_eq!(s.dj_phase, None);
        assert_eq!(s.dj_input, "");
    }

    #[test]
    fn dj_bare_queue_verb_routes_to_command_not_cc() {
        // A bare control verb typed in the DJ pane must run the DETERMINISTIC verb
        // path (never Claude, which cannot express clear/next and would no-op).
        let mut s = TuiState::new();
        s.handle_key(key(KeyCode::F(4)));
        s.dj_input = "next".into();
        assert_eq!(
            s.handle_key(key(KeyCode::Enter)),
            Some(Intent::Command("next".into()))
        );
        // No CC thinking phase on the verb path.
        assert_eq!(s.dj_phase, None);
        // Feedback is surfaced in the DJ pane scrollback.
        assert!(s.dj_log.iter().any(|l| l == "ok: next"));

        // `clear` opens the destructive default-No confirm, NOT a silent run.
        let mut s = TuiState::new();
        s.handle_key(key(KeyCode::F(4)));
        s.dj_input = "clear".into();
        assert_eq!(s.handle_key(key(KeyCode::Enter)), None);
        assert_eq!(s.mode, Mode::Confirm);
        assert_eq!(s.dj_phase, None);

        // A fuzzy phrase still goes to CC (the translator path).
        let mut s = TuiState::new();
        s.handle_key(key(KeyCode::F(4)));
        s.dj_input = "fade out slowly".into();
        assert_eq!(
            s.handle_key(key(KeyCode::Enter)),
            Some(Intent::Cc("fade out slowly".into()))
        );
        assert_eq!(s.dj_phase.as_deref(), Some("thinking..."));
    }

    #[test]
    fn dj_esc_returns_to_queue() {
        let mut s = TuiState::new();
        s.handle_key(key(KeyCode::F(4)));
        s.dj_input = "half typed".into();
        assert_eq!(s.handle_key(key(KeyCode::Esc)), Some(Intent::ShowScreen(Screen::Queue)));
        assert_eq!(s.screen, Screen::Queue);
        assert_eq!(s.dj_input, "");
    }

    #[test]
    fn dj_log_folds_and_bounds() {
        let mut s = TuiState::new();
        for i in 0..250 {
            s.push_dj_log(format!("line {i}"));
        }
        // Bounded at 200, newest kept.
        assert_eq!(s.dj_log.len(), 200);
        assert_eq!(s.dj_log.last().unwrap(), "line 249");
        assert_eq!(s.dj_log.first().unwrap(), "line 50");
    }

    #[test]
    fn normalize_level_maps_floor_ceiling_and_gamma() {
        // At/below the floor -> 0; at/above the ceiling -> 1.
        assert_eq!(normalize_level(-54.0), 0.0);
        assert_eq!(normalize_level(-90.0), 0.0);
        assert!((normalize_level(-6.0) - 1.0).abs() < 1e-6);
        assert!((normalize_level(0.0) - 1.0).abs() < 1e-6);
        // Midpoint sits above the linear 0.5 because of the <1 gamma (quiet expand).
        let mid = normalize_level(-30.0);
        assert!(mid > 0.5 && mid < 1.0, "gamma lifts the mid: {mid}");
        // Monotone increasing.
        assert!(normalize_level(-40.0) < normalize_level(-20.0));
    }

    #[test]
    fn envelope_attack_faster_than_release() {
        // From rest, a step UP rises quickly; a step DOWN of the same size falls
        // slower - the asymmetric ballistics (attack 60ms vs release 350ms).
        let dt = 0.05; // one ~20fps frame
        let up = envelope_step(0.0, 1.0, dt);
        let down = 1.0 - envelope_step(1.0, 0.0, dt);
        assert!(up > down, "attack ({up}) outpaces release ({down})");
        // A zero dt makes no move; the envelope converges toward the target over
        // repeated steps and never overshoots.
        assert_eq!(envelope_step(0.3, 0.9, 0.0), 0.3);
        let mut a = 0.0f32;
        for _ in 0..200 {
            a = envelope_step(a, 1.0, dt);
        }
        assert!(a > 0.99 && a <= 1.0, "converges to the target without overshoot: {a}");
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
