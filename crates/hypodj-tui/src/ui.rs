//! Thin TuiState -> Frame render. No state mutation beyond a scratch ListState for
//! the queue highlight; all decisions come from TuiState.

use ratatui::layout::{Constraint, Layout};
use ratatui::style::{Modifier, Style};
use ratatui::text::{Line, Span};
use ratatui::widgets::{Block, Borders, List, ListItem, ListState, Paragraph};
use ratatui::Frame;

use hypodj_client::model::NowPlaying;

use crate::state::{Mode, TuiState};

/// Draw the full jukebox: now-playing pane, queue list, command/confirm line.
pub fn render(f: &mut Frame, state: &TuiState) {
    let chunks = Layout::vertical([
        Constraint::Length(5),
        Constraint::Min(3),
        Constraint::Length(3),
    ])
    .split(f.area());

    render_now(f, chunks[0], &state.now);
    render_queue(f, chunks[1], state);
    render_command(f, chunks[2], state);
}

fn render_now(f: &mut Frame, area: ratatui::layout::Rect, np: &NowPlaying) {
    let block = Block::default().borders(Borders::ALL).title("Now Playing");
    let stopped = np.state.as_deref() == Some("stop");
    let empty = np.title.is_none() && np.artist.is_none() && np.album.is_none();
    let lines: Vec<Line> = if stopped || empty {
        vec![Line::from("nothing playing")]
    } else {
        let title = np.title.clone().unwrap_or_else(|| "(unknown)".to_string());
        let sub: Vec<&str> = [np.artist.as_deref(), np.album.as_deref()]
            .into_iter()
            .flatten()
            .collect();
        vec![
            Line::from(title),
            Line::from(sub.join(" - ")),
            Line::from(status_line(np)),
        ]
    };
    f.render_widget(Paragraph::new(lines).block(block), area);
}

/// "[playing|paused|stopped] | vol V% | N of M | M:SS" - hide unknown volume,
/// never render elapsed (the server does not emit it).
fn status_line(np: &NowPlaying) -> String {
    let mut bits = Vec::new();
    match np.state.as_deref() {
        Some("play") => bits.push("playing".to_string()),
        Some("pause") => bits.push("paused".to_string()),
        Some("stop") => bits.push("stopped".to_string()),
        Some(other) => bits.push(other.to_string()),
        None => {}
    }
    if let Some(v) = np.volume {
        if v >= 0 {
            bits.push(format!("vol {v}%"));
        }
    }
    if let (Some(song), Some(m)) = (np.song, np.playlistlength) {
        bits.push(format!("{} of {}", song.saturating_add(1), m));
    }
    if let Some(d) = np.duration {
        let total = d as u64;
        bits.push(format!("{}:{:02}", total / 60, total % 60));
    }
    bits.join(" | ")
}

fn render_queue(f: &mut Frame, area: ratatui::layout::Rect, state: &TuiState) {
    let block = Block::default().borders(Borders::ALL).title("Queue");
    let current = state.now.song;
    let items: Vec<ListItem> = state
        .queue
        .iter()
        .map(|it| {
            let base = match &it.artist {
                Some(a) => format!("{}. {} - {}", it.pos + 1, it.title, a),
                None => format!("{}. {}", it.pos + 1, it.title),
            };
            // Mark the current song row.
            let marker = if current == Some(it.pos) { "> " } else { "  " };
            ListItem::new(Line::from(format!("{marker}{base}")))
        })
        .collect();
    let list = List::new(items).block(block).highlight_style(
        Style::default().add_modifier(Modifier::REVERSED),
    );
    let mut ls = ListState::default();
    if !state.queue.is_empty() {
        // Inner list height (area minus the top/bottom border rows).
        let h = area.height.saturating_sub(2) as usize;
        let off = crate::state::scroll_offset(state.selected, state.queue.len(), h, state.offset.get());
        // Persist the derived offset for the next frame, then force it onto the
        // ListState (ratatui would otherwise recompute its own scroll).
        state.offset.set(off);
        *ls.offset_mut() = off;
        ls.select(Some(state.selected));
    }
    f.render_stateful_widget(list, area, &mut ls);
}

fn render_command(f: &mut Frame, area: ratatui::layout::Rect, state: &TuiState) {
    let block = Block::default().borders(Borders::ALL);
    let lines: Vec<Line> = match state.mode {
        Mode::Normal => {
            let hint = "keys: space/bksp=scrub p=pause </>=prev/next ^s=stop j/k=move g/G=top/bot f=fav 9/0=vol enter=play  /=command  q=quit";
            match &state.status_msg {
                Some(msg) => vec![Line::from(msg.replace('\n', " "))],
                None => vec![Line::from(Span::styled(
                    hint,
                    Style::default().add_modifier(Modifier::DIM),
                ))],
            }
        }
        Mode::Command => vec![Line::from(format!("> {}", state.input))],
        Mode::Confirm => {
            let mut ls = Vec::new();
            if let Some(p) = &state.pending {
                if let Some(trust) = &p.trust {
                    ls.push(Line::from(Span::styled(
                        trust.clone(),
                        Style::default().add_modifier(Modifier::DIM),
                    )));
                }
                for step in &p.steps {
                    ls.push(Line::from(step.clone()));
                }
                if let Some(note) = &p.note {
                    ls.push(Line::from(Span::styled(
                        format!("! {note}"),
                        Style::default().add_modifier(Modifier::BOLD),
                    )));
                }
            }
            ls.push(Line::from("confirm? [y/N]"));
            ls
        }
    };
    f.render_widget(Paragraph::new(lines).block(block), area);
}
