//! The pure, testable core of the jukebox TUI: state, the key -> intent mapping,
//! the command-vs-NL routing reused from hypodj-client, and the confirm state
//! machine. NO terminal, NO network - crossterm KeyEvents come in, Intents go out,
//! and the event loop in main.rs does all the IO.

use crossterm::event::{KeyCode, KeyEvent};

use hypodj_client::model::{NowPlaying, QueueItem};
use hypodj_client::nl::not_understood_hint;
use hypodj_client::route::{route, Action};

/// Volume step for +/- keys.
const VOL_STEP: i32 = 5;

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
    /// Leave the session.
    Quit,
}

pub struct TuiState {
    pub now: NowPlaying,
    pub queue: Vec<QueueItem>,
    pub selected: usize,
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
        match key.code {
            KeyCode::Char(' ') => Some(Intent::Command("pause".into())),
            KeyCode::Char('n') => Some(Intent::Command("next".into())),
            KeyCode::Char('p') | KeyCode::Char('b') => Some(Intent::Command("previous".into())),
            KeyCode::Char('s') => Some(Intent::Command("stop".into())),
            KeyCode::Char('+') | KeyCode::Char('=') => self.volume_intent(VOL_STEP),
            KeyCode::Char('-') | KeyCode::Char('_') => self.volume_intent(-VOL_STEP),
            KeyCode::Char('j') | KeyCode::Down => {
                self.move_selection(1);
                None
            }
            KeyCode::Char('k') | KeyCode::Up => {
                self.move_selection(-1);
                None
            }
            KeyCode::Enter => self
                .queue
                .get(self.selected)
                .map(|it| Intent::Command(format!("play {}", it.pos))),
            KeyCode::Char('/') | KeyCode::Char(':') => {
                self.mode = Mode::Command;
                self.input.clear();
                None
            }
            KeyCode::Char('q') => Some(Intent::Quit),
            _ => None,
        }
    }

    /// Volume step from the current known volume, clamped 0..=100. No-op (None) when
    /// the volume is unknown (absent or -1).
    fn volume_intent(&self, delta: i32) -> Option<Intent> {
        let cur = self.now.volume?;
        if cur < 0 {
            return None;
        }
        let next = (cur + delta).clamp(0, 100);
        Some(Intent::Command(format!("setvol {next}")))
    }

    /// Move the queue selection with clamping (no wrap).
    fn move_selection(&mut self, delta: i32) {
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
        QueueItem { pos, title: format!("t{pos}"), artist: None }
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
        assert_eq!(s.handle_key(ch(' ')), Some(Intent::Command("pause".into())));
        assert_eq!(s.handle_key(ch('n')), Some(Intent::Command("next".into())));
        assert_eq!(s.handle_key(ch('p')), Some(Intent::Command("previous".into())));
        assert_eq!(s.handle_key(ch('b')), Some(Intent::Command("previous".into())));
        assert_eq!(s.handle_key(ch('s')), Some(Intent::Command("stop".into())));
        assert_eq!(s.handle_key(ch('q')), Some(Intent::Quit));
    }

    #[test]
    fn volume_step_clamped_and_noop_when_unknown() {
        let mut s = TuiState::new();
        s.now.volume = Some(70);
        assert_eq!(s.handle_key(ch('+')), Some(Intent::Command("setvol 75".into())));
        assert_eq!(s.handle_key(ch('-')), Some(Intent::Command("setvol 65".into())));
        s.now.volume = Some(98);
        assert_eq!(s.handle_key(ch('+')), Some(Intent::Command("setvol 100".into())));
        s.now.volume = Some(2);
        assert_eq!(s.handle_key(ch('-')), Some(Intent::Command("setvol 0".into())));
        // Unknown volume -> no-op.
        s.now.volume = None;
        assert_eq!(s.handle_key(ch('+')), None);
        s.now.volume = Some(-1);
        assert_eq!(s.handle_key(ch('-')), None);
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
