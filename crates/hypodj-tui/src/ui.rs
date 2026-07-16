//! Thin TuiState -> Frame render. No state mutation beyond a scratch ListState for
//! the queue highlight; all decisions come from TuiState.

use ratatui::layout::{Alignment, Constraint, Layout, Rect};
use ratatui::style::{Modifier, Style};
use ratatui::text::{Line, Span};
use ratatui::widgets::{Block, Borders, List, ListItem, ListState, Paragraph};
use ratatui::Frame;

use hypodj_client::model::NowPlaying;

use crate::state::{album_mark, queue_mark_glyph, Browse, Mode, Screen, TuiState};

/// Draw the full jukebox: the screen-tab row, the active list
/// (Queue/Albums/Playlists), then the Now Playing pane (album art + up-next
/// preview), and the command/confirm line. Now Playing sits BELOW the list, just
/// above the command box.
pub fn render(f: &mut Frame, state: &TuiState) {
    let chunks = Layout::vertical([
        Constraint::Length(1),  // screen tabs
        Constraint::Min(3),     // active list
        Constraint::Length(12), // Now Playing: album art + up-next preview
        Constraint::Length(3),  // command / status
    ])
    .split(f.area());

    render_tabs(f, chunks[0], state.screen);
    match state.screen {
        Screen::Queue => render_queue(f, chunks[1], state),
        Screen::Albums => render_browse(f, chunks[1], &state.albums, state, true),
        Screen::Playlists => render_browse(f, chunks[1], &state.playlists, state, false),
    }
    render_now(f, chunks[2], state);
    render_command(f, chunks[3], state);
}

/// Split `label` into styled spans, underlining+bolding the FIRST case-insensitive
/// occurrence of `query`. The middle (match) span carries `hit`, the rest `base`.
/// An empty query or no match is a fast path -> a single plain `base` span. The
/// hit style is a MODIFIER (underline+bold), never a fg color: the selected row is
/// already REVERSED, and a fg would invert into a background swatch on that row,
/// whereas a modifier survives the reverse swap and stays legible everywhere. Pure
/// and unit-tested.
fn match_spans(label: &str, query: &str, base: Style, hit: Style) -> Vec<Span<'static>> {
    if query.is_empty() {
        return vec![Span::styled(label.to_string(), base)];
    }
    let needle = query.to_lowercase();
    // Search on the ORIGINAL label and return byte offsets valid in it. We cannot
    // reuse offsets from `label.to_lowercase()`: `to_lowercase` can change byte
    // length (e.g. Turkish dotted-capital-I 'I' -> "i" combining, or German 'SS'
    // -> "ss"), which would put the slice off a char boundary and panic the whole
    // render. `find_ci` walks char boundaries of the original, so every offset is
    // guaranteed valid there.
    match find_ci(label, &needle) {
        Some((start, end)) => {
            let mut spans = Vec::new();
            if start > 0 {
                spans.push(Span::styled(label[..start].to_string(), base));
            }
            spans.push(Span::styled(label[start..end].to_string(), hit));
            if end < label.len() {
                spans.push(Span::styled(label[end..].to_string(), base));
            }
            spans
        }
        None => vec![Span::styled(label.to_string(), base)],
    }
}

/// Find the FIRST case-insensitive occurrence of `needle_lower` (already
/// lowercased) in `label`, returning a `(start, end)` byte range that is always
/// valid in `label` (both ends land on char boundaries of the original). Unlike
/// searching `label.to_lowercase()`, this never yields offsets that fall off a
/// char boundary when the lowercase mapping changes byte length. An empty needle
/// yields no match (callers treat an empty query as the fast path).
fn find_ci(label: &str, needle_lower: &str) -> Option<(usize, usize)> {
    if needle_lower.is_empty() {
        return None;
    }
    let starts: Vec<usize> = label.char_indices().map(|(i, _)| i).collect();
    for &start in &starts {
        // Lowercase the tail one char at a time, comparing against the needle as a
        // growing prefix. Stop as soon as it diverges or matches.
        let mut lowered = String::new();
        let mut end = start;
        for ch in label[start..].chars() {
            for lc in ch.to_lowercase() {
                lowered.push(lc);
            }
            end += ch.len_utf8();
            if lowered.len() >= needle_lower.len() {
                if lowered == needle_lower {
                    return Some((start, end));
                }
                break;
            }
            if !needle_lower.starts_with(&lowered) {
                break;
            }
        }
    }
    None
}

/// The substring-highlight style: underline + bold, composed over the selected
/// row's REVERSED modifier (see [`match_spans`]).
fn hit_style() -> Style {
    Style::default().add_modifier(Modifier::UNDERLINED | Modifier::BOLD)
}

/// A one-line tab strip: the active screen is REVERSED, the rest dim.
fn render_tabs(f: &mut Frame, area: ratatui::layout::Rect, screen: Screen) {
    let labels = [
        (Screen::Queue, "[1]Queue"),
        (Screen::Albums, "[2]Albums"),
        (Screen::Playlists, "[3]Playlists"),
    ];
    let mut spans = Vec::new();
    for (i, (s, label)) in labels.iter().enumerate() {
        if i > 0 {
            spans.push(Span::raw("  "));
        }
        let style = if *s == screen {
            Style::default().add_modifier(Modifier::REVERSED)
        } else {
            Style::default().add_modifier(Modifier::DIM)
        };
        spans.push(Span::styled(*label, style));
    }
    f.render_widget(Paragraph::new(Line::from(spans)), area);
}

/// The terminal window/tab title for the current now-playing snapshot, emitted via
/// OSC (crossterm `SetTitle`). "HypoDJ" when stopped or nothing is playing;
/// "HypoDJ - <artist> - <title>" when both are known; "HypoDJ - <title>" when only
/// the title is. Pure and testable - mirrors the stopped/empty test in render_now.
pub fn window_title(np: &NowPlaying) -> String {
    let stopped = np.state.as_deref() == Some("stop");
    let empty = np.title.is_none() && np.artist.is_none();
    if stopped || empty {
        return "HypoDJ".to_string();
    }
    let artist = np.artist.as_deref().map(sanitize_title);
    let title = np.title.as_deref().map(sanitize_title);
    match (artist, title) {
        (Some(a), Some(t)) => format!("HypoDJ - {a} - {t}"),
        (None, Some(t)) => format!("HypoDJ - {t}"),
        _ => "HypoDJ".to_string(),
    }
}

/// Strip control characters from an MPD tag before it reaches the OSC window
/// title. crossterm `SetTitle` wraps the string as `ESC]0;<title>BEL`, so an
/// embedded BEL (0x07) or ESC (0x1b) in an artist/title tag would terminate the
/// OSC early and let the terminal interpret the trailing bytes as raw
/// output/escape sequences (title injection). Dropping every control char
/// (including newlines/tabs) keeps only printable text.
fn sanitize_title(s: &str) -> String {
    s.chars().filter(|c| !c.is_control()).collect()
}

/// The Now Playing pane: the current track (album art + title/artist/album) on the
/// left, a compact 3-track up-next preview on the right.
fn render_now(f: &mut Frame, area: Rect, state: &TuiState) {
    let cols = Layout::horizontal([Constraint::Percentage(52), Constraint::Percentage(48)])
        .split(area);
    render_current(f, cols[0], state);
    render_next_up(f, cols[1], state);
}

/// Left of Now Playing: the dithered album art with title/artist/album beneath it.
fn render_current(f: &mut Frame, area: Rect, state: &TuiState) {
    let np = &state.now;
    let block = Block::default().borders(Borders::ALL).title("Now Playing");
    let inner = block.inner(area);
    f.render_widget(block, area);
    if inner.width == 0 || inner.height == 0 {
        return;
    }

    let stopped = np.state.as_deref() == Some("stop");
    let empty = np.title.is_none() && np.artist.is_none() && np.album.is_none();
    if stopped || empty {
        f.render_widget(Paragraph::new("nothing playing"), inner);
        return;
    }

    // Reserve up to 4 rows at the bottom for title/artist/album + the playback
    // status line (state | volume fader | position); the rest is art.
    let text_h = 4u16.min(inner.height);
    let art_h = inner.height.saturating_sub(text_h);
    if art_h > 0 {
        // Keep the art roughly square: each cell renders 2 vertical pixels, so a
        // square cover is about (rows*2) columns wide. Clamp to the pane width.
        let art_rows = art_h as usize;
        let art_cols = (art_rows * 2).min(inner.width as usize);
        let art_area = Rect { x: inner.x, y: inner.y, width: art_cols as u16, height: art_h };
        match &state.art {
            Some(a) => f.render_widget(Paragraph::new(a.lines(art_cols, art_rows)), art_area),
            None => f.render_widget(art_placeholder(np), art_area),
        }
    }

    let text_area = Rect {
        x: inner.x,
        y: inner.y + art_h,
        width: inner.width,
        height: text_h,
    };
    let title = np.title.clone().unwrap_or_else(|| "(unknown)".to_string());
    let mut lines = vec![Line::from(Span::styled(
        title,
        Style::default().add_modifier(Modifier::BOLD),
    ))];
    if let Some(artist) = np.artist.as_deref() {
        lines.push(Line::from(artist.to_string()));
    }
    if let Some(album) = np.album.as_deref() {
        lines.push(Line::from(Span::styled(
            album.to_string(),
            Style::default().add_modifier(Modifier::DIM),
        )));
    }
    // Playback status: state | volume fader | N of M | M:SS.
    lines.push(Line::from(Span::styled(
        status_line(np),
        Style::default().add_modifier(Modifier::DIM),
    )));
    f.render_widget(Paragraph::new(lines), text_area);
}

/// A dim placeholder shown when there is no cover art (stream, missing, or a fetch
/// failure): a bordered box with a centered music glyph, so the layout still reads.
fn art_placeholder(_np: &NowPlaying) -> Paragraph<'static> {
    Paragraph::new(vec![Line::from("\u{266B}")])
        .alignment(Alignment::Center)
        .block(Block::default().borders(Borders::ALL))
        .style(Style::default().add_modifier(Modifier::DIM))
}

/// Right of Now Playing: a compact preview of the next 3 queued tracks, each a
/// bold title over a dim artist line - a smaller echo of the current-track card.
fn render_next_up(f: &mut Frame, area: Rect, state: &TuiState) {
    let block = Block::default().borders(Borders::ALL).title("Up Next");
    let inner = block.inner(area);
    f.render_widget(block, area);
    if inner.width == 0 || inner.height == 0 {
        return;
    }
    // The next tracks are those after the current song index, in queue order.
    let start = state.now.song.map(|c| c + 1).unwrap_or(0);
    let mut lines: Vec<Line> = Vec::new();
    let mut count = 0;
    for it in state.queue.iter().skip(start) {
        if count >= 3 {
            break;
        }
        lines.push(Line::from(Span::styled(
            format!("{}. {}", it.pos + 1, it.title),
            Style::default().add_modifier(Modifier::BOLD),
        )));
        if let Some(a) = it.artist.as_deref() {
            lines.push(Line::from(Span::styled(
                format!("   {a}"),
                Style::default().add_modifier(Modifier::DIM),
            )));
        }
        lines.push(Line::from(""));
        count += 1;
    }
    if lines.is_empty() {
        lines.push(Line::from(Span::styled(
            "end of queue",
            Style::default().add_modifier(Modifier::DIM),
        )));
    }
    f.render_widget(Paragraph::new(lines), inner);
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
            bits.push(volume_slider(v.min(100) as u8, 12));
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

/// Render volume as a physical horizontal FADER: `[==#-----] V%`. `width` is the
/// number of inner track cells; the `#` thumb slides across them proportional to
/// `vol` (0..=100). A fader (not a round dial) maps 1:1 to a physical fader's
/// travel, needs only one row, reflows with pane width, and its thumb visibly
/// slides as the reported volume tracks a glide envelope. ASCII-safe glyphs
/// (`[`, `]`, `=`, `#`, `-`) so terminals without good unicode still render it.
/// Pure and deterministic - unit-tested below.
fn volume_slider(vol: u8, width: usize) -> String {
    let vol = vol.min(100);
    // At least one cell so the thumb always has a home.
    let width = width.max(1);
    // Thumb cell index in [0, width-1], proportional to vol.
    let pos = ((vol as f64 / 100.0) * (width as f64 - 1.0)).round() as usize;
    let pos = pos.min(width - 1);
    let filled = "=".repeat(pos);
    let empty = "-".repeat(width - 1 - pos);
    format!("[{filled}#{empty}] {vol}%")
}

#[cfg(test)]
mod tests {
    use super::{hit_style, match_spans, volume_slider, window_title};
    use hypodj_client::model::NowPlaying;
    use ratatui::style::Style;

    #[test]
    fn match_spans_splits_before_match_after() {
        let base = Style::default();
        let hit = hit_style();
        // Case-insensitive middle match splits into (before, MATCH, after).
        let spans = match_spans("Kind of Blue", "of", base, hit);
        assert_eq!(spans.len(), 3);
        assert_eq!(spans[0].content, "Kind ");
        assert_eq!(spans[1].content, "of");
        assert_eq!(spans[1].style, hit);
        assert_eq!(spans[2].content, " Blue");
        assert_eq!(spans[0].style, base);
    }

    #[test]
    fn match_spans_case_insensitive_and_edges() {
        let base = Style::default();
        let hit = hit_style();
        // A match at the very start has no `before` span.
        let spans = match_spans("Beta", "be", base, hit);
        assert_eq!(spans.len(), 2);
        assert_eq!(spans[0].content, "Be");
        assert_eq!(spans[0].style, hit);
        assert_eq!(spans[1].content, "ta");
        // A match at the very end has no `after` span.
        let spans = match_spans("Gamma", "MMA", base, hit);
        assert_eq!(spans.len(), 2);
        assert_eq!(spans[1].content, "mma");
        assert_eq!(spans[1].style, hit);
    }

    #[test]
    fn match_spans_empty_and_no_match_fast_path() {
        let base = Style::default();
        let hit = hit_style();
        // Empty query -> one plain span.
        let spans = match_spans("Alpha", "", base, hit);
        assert_eq!(spans.len(), 1);
        assert_eq!(spans[0].style, base);
        // No match -> one plain span.
        let spans = match_spans("Alpha", "zzz", base, hit);
        assert_eq!(spans.len(), 1);
        assert_eq!(spans[0].content, "Alpha");
        assert_eq!(spans[0].style, base);
    }

    #[test]
    fn match_spans_non_ascii_lowercase_length_change_no_panic() {
        let base = Style::default();
        let hit = hit_style();
        // 'I' (U+0130) lowercases to a 3-byte "i" + combining dot, growing the
        // byte length. Offsets from the lowercased copy would slice off a char
        // boundary of the original and panic; ours stay valid.
        let dotted = "\u{0130}"; // Turkish dotted capital I
        let label = format!("{dotted}a");
        let spans = match_spans(&label, "a", base, hit);
        let joined: String = spans.iter().map(|s| s.content.to_string()).collect();
        assert_eq!(joined, label);
        assert!(spans.iter().any(|s| s.content == "a" && s.style == hit));
        // A query that overshoots into the expanded lowercase must not match the
        // dotted 'I' and must not panic.
        let label = format!("a{dotted}b");
        let spans = match_spans(&label, "b", base, hit);
        let joined: String = spans.iter().map(|s| s.content.to_string()).collect();
        assert_eq!(joined, label);
        assert!(spans.iter().any(|s| s.content == "b" && s.style == hit));
    }

    #[test]
    fn window_title_default_and_stopped_are_plain() {
        // Empty now-playing -> plain product name.
        assert_eq!(window_title(&NowPlaying::default()), "HypoDJ");
        // Explicitly stopped, even with tags, is plain.
        let np = NowPlaying {
            state: Some("stop".into()),
            title: Some("T".into()),
            artist: Some("A".into()),
            ..NowPlaying::default()
        };
        assert_eq!(window_title(&np), "HypoDJ");
    }

    #[test]
    fn window_title_artist_and_title() {
        let np = NowPlaying {
            state: Some("play".into()),
            title: Some("Blue in Green".into()),
            artist: Some("Miles Davis".into()),
            ..NowPlaying::default()
        };
        assert_eq!(window_title(&np), "HypoDJ - Miles Davis - Blue in Green");
    }

    #[test]
    fn window_title_title_only() {
        let np = NowPlaying {
            state: Some("play".into()),
            title: Some("Live Stream".into()),
            ..NowPlaying::default()
        };
        assert_eq!(window_title(&np), "HypoDJ - Live Stream");
    }

    #[test]
    fn window_title_strips_control_chars() {
        // A BEL/ESC in a tag would terminate the OSC title early and inject the
        // trailing bytes into the terminal; they must be dropped.
        let np = NowPlaying {
            state: Some("play".into()),
            artist: Some("Ac\x1bDC".into()),
            title: Some("Foo\x07rm -rf".into()),
            ..NowPlaying::default()
        };
        assert_eq!(window_title(&np), "HypoDJ - AcDC - Foorm -rf");
    }

    #[test]
    fn thumb_hard_left_at_zero() {
        // vol 0 -> thumb at the very first cell, no fill before it.
        assert_eq!(volume_slider(0, 12), "[#-----------] 0%");
    }

    #[test]
    fn thumb_hard_right_at_full() {
        // vol 100 -> thumb at the last cell, everything before it filled.
        assert_eq!(volume_slider(100, 12), "[===========#] 100%");
    }

    #[test]
    fn thumb_centered_at_half() {
        let s = volume_slider(50, 12);
        // 12 inner cells, pos = round(0.5 * 11) = 6 (6 filled, thumb, 5 empty).
        assert_eq!(s, "[======#-----] 50%");
    }

    #[test]
    fn exact_cell_counts_and_percent_suffix() {
        let s = volume_slider(30, 12);
        assert!(s.ends_with(" 30%"), "percent suffix present: {s}");
        let inner = &s[1..s.find(']').unwrap()];
        assert_eq!(inner.chars().count(), 12, "inner track is exactly `width` cells");
        assert_eq!(inner.chars().filter(|&c| c == '#').count(), 1, "exactly one thumb");
    }

    #[test]
    fn thumb_position_monotonic_non_decreasing() {
        let mut last = 0usize;
        for v in 0..=100u8 {
            let s = volume_slider(v, 12);
            let pos = s.find('#').unwrap();
            assert!(pos >= last, "thumb never moves left as vol rises (v={v})");
            last = pos;
        }
    }
}

fn render_queue(f: &mut Frame, area: ratatui::layout::Rect, state: &TuiState) {
    let block = Block::default().borders(Borders::ALL).title("Queue");
    let current = state.now.song;
    let query = state.highlight_query();
    let items: Vec<ListItem> = state
        .queue
        .iter()
        .map(|it| {
            // The searchable label (matches active_labels for the Queue screen).
            let label = match &it.artist {
                Some(a) => format!("{} - {}", it.title, a),
                None => it.title.clone(),
            };
            // Mark the current song row.
            let marker = if current == Some(it.pos) { "> " } else { "  " };
            let mut spans = vec![Span::raw(format!("{marker}{}. ", it.pos + 1))];
            spans.extend(match_spans(&label, query, Style::default(), hit_style()));
            ListItem::new(Line::from(spans))
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

/// Render a browse screen (Albums/Playlists): the active `Browse.rows` in a List,
/// dirs marked with a trailing `/`, driven by the browse's own cursor + scrolloff
/// offset. Reuses the same List+ListState mechanics as render_queue.
fn render_browse(
    f: &mut Frame,
    area: ratatui::layout::Rect,
    browse: &Browse,
    state: &TuiState,
    markers: bool,
) {
    let block = Block::default().borders(Borders::ALL).title(browse.title.clone());
    let query = state.highlight_query();
    // Queue-marker lookups (Albums screen only): album/<id> -> distinct queued song
    // ids for the full/partial gutter, plus the flat queued-uri set for the song
    // rows of an opened album.
    let album_map = if markers { state.queued_by_album() } else { Default::default() };
    let queued_uris = if markers { state.queued_uris() } else { Default::default() };
    let items: Vec<ListItem> = browse
        .rows
        .iter()
        .map(|r| {
            // Gutter glyph: for an album dir row, full/partial from queued vs total;
            // for a song row (opened album), `#` when already queued. Two-char gutter
            // (glyph + space) so it never collides with the REVERSED cursor bar.
            let glyph = if !markers {
                ' '
            } else if r.is_dir {
                let queued = album_map.get(&r.uri).map(|s| s.len()).unwrap_or(0);
                queue_mark_glyph(album_mark(queued, r.song_count))
            } else if queued_uris.contains(&r.uri) {
                '#'
            } else {
                ' '
            };
            let mut spans = vec![Span::raw(format!("{glyph} "))];
            spans.extend(match_spans(&r.label, query, Style::default(), hit_style()));
            if r.is_dir {
                spans.push(Span::raw("/"));
            }
            ListItem::new(Line::from(spans))
        })
        .collect();
    let list = List::new(items)
        .block(block)
        .highlight_style(Style::default().add_modifier(Modifier::REVERSED));
    let mut ls = ListState::default();
    if !browse.rows.is_empty() {
        let h = area.height.saturating_sub(2) as usize;
        let off = crate::state::scroll_offset(browse.selected, browse.rows.len(), h, browse.offset.get());
        browse.offset.set(off);
        *ls.offset_mut() = off;
        ls.select(Some(browse.selected));
    }
    f.render_stateful_widget(list, area, &mut ls);
}

fn render_command(f: &mut Frame, area: ratatui::layout::Rect, state: &TuiState) {
    let block = Block::default().borders(Borders::ALL);
    // Draw a visible caret in the input box only in Command/Search mode; a stray
    // block cursor in Normal/Confirm would sit over the hint line. Inner coords
    // skip the 1-cell border (area.x+1, area.y+1); prompt_len = 2 for "> ", 1 for
    // "/"; chars().count() (not len()) so multibyte input is not mis-placed.
    let caret: Option<(u16, usize)> = match state.mode {
        Mode::Command => Some((2, state.input.chars().count())),
        Mode::Search => Some((1, state.input.chars().count())),
        _ => None,
    };
    if let Some((prompt_len, input_chars)) = caret {
        f.set_cursor_position((
            area.x + 1 + prompt_len + input_chars as u16,
            area.y + 1,
        ));
    }
    let lines: Vec<Line> = match state.mode {
        // Only a status banner here (the key hints were removed); empty otherwise.
        Mode::Normal => match &state.status_msg {
            Some(msg) => vec![Line::from(msg.replace('\n', " "))],
            None => vec![Line::from("")],
        },
        Mode::Command => vec![Line::from(format!("> {}", state.input))],
        Mode::Search => vec![Line::from(format!("/{}", state.input))],
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
