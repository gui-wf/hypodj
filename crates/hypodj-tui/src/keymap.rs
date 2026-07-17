//! The SINGLE source-of-truth keymap. [`KEYMAP`] lists every normal-mode binding
//! once: its matcher(s), a display label, a group, a scope, the action it fires, and a
//! one-line help string. The `?` help overlay renders straight from [`grouped`], so
//! it can NEVER drift from the real bindings; [`match_key`] resolves a KeyEvent to its
//! [`Act`] so dispatch can stay in lockstep with the same table.
//!
//! NOTE on lockstep: state.rs `key_normal` DISPATCHES through this table - it calls
//! [`match_key`] to resolve a key to its [`Act`], then runs it through an EXHAUSTIVE
//! `apply_act` match. So a new row (new Act) cannot be added without a compiler error
//! until dispatch handles it, and an Act cannot be dropped from dispatch while a row
//! still advertises it: help (rendered from [`grouped`]) and behavior share one source
//! and cannot drift. The round-trip test additionally guards against matcher typos.

// The scope field + `keys`/`group`/`help` are read by the help overlay and the scope
// gating; some are not consulted by the (minimum) dispatch, so allow dead_code.
#![allow(dead_code)]

use crossterm::event::{KeyCode, KeyEvent, KeyModifiers};

use crate::state::Screen;

/// A logical action a normal-mode key maps to. Mirrors the intents the state machine
/// already produces; the help overlay only ever needs the label + description, but the
/// Act is what makes the round-trip test meaningful.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum Act {
    ScreenQueue,
    ScreenAlbums,
    ScreenPlaylists,
    ScreenDj,
    Down,
    Up,
    Top,
    Bottom,
    JumpCurrent,
    SearchStart,
    SearchNext,
    SearchPrev,
    CommandLine,
    VolumeUp,
    VolumeDown,
    Pause,
    Next,
    Prev,
    ScrubFwd,
    ScrubBack,
    FavSelected,
    FavCurrent,
    PlaySel,
    Enqueue,
    Open,
    BrowseBack,
    HelpToggle,
    Quit,
}

/// One way a binding can match a key event.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum KeyMatch {
    Char(char),
    Ctrl(char),
    Code(KeyCode),
}

impl KeyMatch {
    fn matches(&self, key: KeyEvent) -> bool {
        let ctrl = key.modifiers.contains(KeyModifiers::CONTROL);
        match *self {
            KeyMatch::Char(c) => !ctrl && key.code == KeyCode::Char(c),
            KeyMatch::Ctrl(c) => ctrl && key.code == KeyCode::Char(c),
            KeyMatch::Code(code) => key.code == code,
        }
    }
}

/// The help-overlay group a binding belongs to. Stable order defines the two-column
/// layout order.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum Group {
    View,
    Navigation,
    Playback,
    Volume,
    Search,
    Command,
    Browse,
    Favorites,
    General,
}

impl Group {
    pub fn title(&self) -> &'static str {
        match self {
            Group::View => "View",
            Group::Navigation => "Navigation",
            Group::Playback => "Playback",
            Group::Volume => "Volume",
            Group::Search => "Search",
            Group::Command => "Command",
            Group::Browse => "Browse",
            Group::Favorites => "Favorites",
            Group::General => "General",
        }
    }
}

/// The scope a binding is meaningful in (used for the help label and, in principle,
/// match gating; the dispatch bodies already guard screen-specific no-ops themselves).
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum Scope {
    Global,
    Queue,
    Browse,
}

/// One keymap row.
pub struct Binding {
    pub matchers: &'static [KeyMatch],
    pub keys: &'static str,
    pub group: Group,
    pub scope: Scope,
    pub act: Act,
    pub help: &'static str,
}

/// The stable group order for the overlay.
pub const GROUP_ORDER: [Group; 9] = [
    Group::View,
    Group::Navigation,
    Group::Playback,
    Group::Volume,
    Group::Search,
    Group::Command,
    Group::Browse,
    Group::Favorites,
    Group::General,
];

use Act::*;
use Group::*;
use KeyMatch::{Char, Code, Ctrl};

/// The one table. Every current normal-mode binding lives here exactly once.
pub const KEYMAP: &[Binding] = &[
    // View.
    Binding { matchers: &[Code(KeyCode::F(1))], keys: "F1", group: View, scope: Scope::Global, act: ScreenQueue, help: "Queue screen" },
    Binding { matchers: &[Code(KeyCode::F(2))], keys: "F2", group: View, scope: Scope::Global, act: ScreenAlbums, help: "Albums screen" },
    Binding { matchers: &[Code(KeyCode::F(3))], keys: "F3", group: View, scope: Scope::Global, act: ScreenPlaylists, help: "Playlists screen" },
    Binding { matchers: &[Code(KeyCode::F(4))], keys: "F4", group: View, scope: Scope::Global, act: ScreenDj, help: "DJ (Claude Code) screen" },
    // Navigation.
    Binding { matchers: &[Char('j'), Code(KeyCode::Down), Ctrl('n')], keys: "j / down / C-n", group: Navigation, scope: Scope::Global, act: Down, help: "move cursor down" },
    Binding { matchers: &[Char('k'), Code(KeyCode::Up), Ctrl('p')], keys: "k / up / C-p", group: Navigation, scope: Scope::Global, act: Up, help: "move cursor up" },
    Binding { matchers: &[Char('g')], keys: "g", group: Navigation, scope: Scope::Global, act: Top, help: "jump to top" },
    Binding { matchers: &[Char('G')], keys: "G", group: Navigation, scope: Scope::Global, act: Bottom, help: "jump to bottom" },
    Binding { matchers: &[Char('P')], keys: "P", group: Navigation, scope: Scope::Queue, act: JumpCurrent, help: "jump to the playing song" },
    // Playback.
    Binding { matchers: &[Char('p')], keys: "p", group: Playback, scope: Scope::Global, act: Pause, help: "play / pause" },
    Binding { matchers: &[Char('>')], keys: ">", group: Playback, scope: Scope::Global, act: Next, help: "next track" },
    Binding { matchers: &[Char('<')], keys: "<", group: Playback, scope: Scope::Global, act: Prev, help: "previous track" },
    Binding { matchers: &[Ctrl('f')], keys: "C-f", group: Playback, scope: Scope::Global, act: ScrubFwd, help: "scrub forward 5s" },
    Binding { matchers: &[Ctrl('b')], keys: "C-b", group: Playback, scope: Scope::Global, act: ScrubBack, help: "scrub back 5s" },
    // Volume.
    Binding { matchers: &[Char('0'), Char('+'), Char('=')], keys: "0 / + / =", group: Volume, scope: Scope::Global, act: VolumeUp, help: "turn the knob up" },
    Binding { matchers: &[Char('9'), Char('-'), Char('_')], keys: "9 / - / _", group: Volume, scope: Scope::Global, act: VolumeDown, help: "turn the knob down" },
    // Search.
    Binding { matchers: &[Char('/')], keys: "/", group: Search, scope: Scope::Global, act: SearchStart, help: "incremental search" },
    Binding { matchers: &[Char('n')], keys: "n", group: Search, scope: Scope::Global, act: SearchNext, help: "repeat search forward" },
    Binding { matchers: &[Char('N')], keys: "N", group: Search, scope: Scope::Global, act: SearchPrev, help: "repeat search backward" },
    // Command.
    Binding { matchers: &[Char(':')], keys: ":", group: Command, scope: Scope::Global, act: CommandLine, help: "command / NL line" },
    // Browse.
    Binding { matchers: &[Code(KeyCode::Enter)], keys: "enter", group: Browse, scope: Scope::Global, act: PlaySel, help: "play the selection" },
    Binding { matchers: &[Char(' ')], keys: "space", group: Browse, scope: Scope::Browse, act: Enqueue, help: "enqueue the selected row" },
    Binding { matchers: &[Char('o')], keys: "o", group: Browse, scope: Scope::Browse, act: Open, help: "open (drill into) a directory" },
    Binding { matchers: &[Char('h'), Code(KeyCode::Left), Code(KeyCode::Esc)], keys: "h / left / esc", group: Browse, scope: Scope::Browse, act: BrowseBack, help: "back out one browse level" },
    // Favorites.
    Binding { matchers: &[Char('s')], keys: "s", group: Favorites, scope: Scope::Global, act: FavSelected, help: "star the selected row" },
    Binding { matchers: &[Ctrl('s')], keys: "C-s", group: Favorites, scope: Scope::Global, act: FavCurrent, help: "star the playing track" },
    // General.
    Binding { matchers: &[Char('?')], keys: "?", group: General, scope: Scope::Global, act: HelpToggle, help: "toggle this help" },
    Binding { matchers: &[Char('q')], keys: "q", group: General, scope: Scope::Global, act: Quit, help: "quit" },
];

/// Resolve a key event to its Act, honoring the readline-first ordering (a Ctrl chord
/// is matched as `Ctrl`, so a plain `p`/`n`/`s` never shadows `C-p`/`C-n`/`C-s`). The
/// `_screen` is accepted for scope gating; screen-specific no-ops stay guarded in the
/// dispatch bodies, so this returns the first matching row.
pub fn match_key(key: KeyEvent, _screen: Screen) -> Option<Act> {
    KEYMAP
        .iter()
        .find(|b| b.matchers.iter().any(|m| m.matches(key)))
        .map(|b| b.act)
}

/// The keymap grouped in stable order, for the two-column overlay. Empty groups are
/// omitted.
pub fn grouped() -> Vec<(Group, Vec<&'static Binding>)> {
    GROUP_ORDER
        .iter()
        .filter_map(|g| {
            let rows: Vec<&'static Binding> = KEYMAP.iter().filter(|b| b.group == *g).collect();
            if rows.is_empty() {
                None
            } else {
                Some((*g, rows))
            }
        })
        .collect()
}

#[cfg(test)]
mod tests {
    use super::*;

    fn key(code: KeyCode) -> KeyEvent {
        KeyEvent::new(code, KeyModifiers::NONE)
    }
    fn ch(c: char) -> KeyEvent {
        key(KeyCode::Char(c))
    }
    fn ctrl(c: char) -> KeyEvent {
        KeyEvent::new(KeyCode::Char(c), KeyModifiers::CONTROL)
    }

    #[test]
    fn every_matcher_round_trips_to_its_own_act() {
        // A table typo (wrong char, wrong Ctrl flag) is caught here: each matcher must
        // resolve via match_key back to the Act it is declared on.
        for b in KEYMAP {
            for m in b.matchers {
                let ev = match *m {
                    KeyMatch::Char(c) => ch(c),
                    KeyMatch::Ctrl(c) => ctrl(c),
                    KeyMatch::Code(code) => key(code),
                };
                assert_eq!(
                    match_key(ev, Screen::Queue),
                    Some(b.act),
                    "matcher {m:?} for {:?} did not round-trip",
                    b.act
                );
            }
        }
    }

    #[test]
    fn ctrl_chords_do_not_collide_with_plain_letters() {
        // Plain p is Pause; C-p is Up (cursor). Plain s is FavSelected; C-s FavCurrent.
        assert_eq!(match_key(ch('p'), Screen::Queue), Some(Act::Pause));
        assert_eq!(match_key(ctrl('p'), Screen::Queue), Some(Act::Up));
        assert_eq!(match_key(ch('s'), Screen::Queue), Some(Act::FavSelected));
        assert_eq!(match_key(ctrl('s'), Screen::Queue), Some(Act::FavCurrent));
        assert_eq!(match_key(ctrl('n'), Screen::Queue), Some(Act::Down));
    }

    #[test]
    fn grouped_is_stable_and_non_empty() {
        let g = grouped();
        // Every group present is non-empty and appears in GROUP_ORDER order.
        assert!(!g.is_empty());
        let order: Vec<Group> = g.iter().map(|(grp, _)| *grp).collect();
        let mut expected: Vec<Group> = GROUP_ORDER
            .iter()
            .copied()
            .filter(|grp| KEYMAP.iter().any(|b| b.group == *grp))
            .collect();
        assert_eq!(order, expected);
        for (_, rows) in &g {
            assert!(!rows.is_empty());
        }
        expected.dedup();
        assert_eq!(expected.len(), order.len(), "no duplicate groups");
    }

    #[test]
    fn documented_bindings_all_present() {
        // The set the task calls out must each have a row.
        let want_chars = ['j', 'k', 'g', 'G', 'P', '/', 'n', 'N', ':', 'p', '<', '>', 's', 'o', 'h', 'q', '?', '0', '9', ' '];
        for c in want_chars {
            assert!(
                KEYMAP.iter().any(|b| b.matchers.iter().any(|m| matches!(m, KeyMatch::Char(k) if *k == c))),
                "missing a keymap row for {c:?}"
            );
        }
        for f in 1..=4u8 {
            assert!(
                KEYMAP.iter().any(|b| b.matchers.iter().any(|m| matches!(m, KeyMatch::Code(KeyCode::F(n)) if *n == f))),
                "missing F{f}"
            );
        }
    }
}
