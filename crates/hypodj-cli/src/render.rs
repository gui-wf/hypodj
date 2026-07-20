//! Text formatters for the hjq CLI, expressed on top of the shared parse layer in
//! `hypodj_client::model`. This crate owns ONLY the text card/queue rendering; the
//! NowPlaying + queue parsing lives in the client. A stopped/empty deck renders a
//! friendly "nothing playing".

use hypodj_client::model::{fmt_remaining, parse_queue, ArmedFeatures, FieldState, NowPlaying};

/// Render the now-playing card as plain text (a couple of lines). A stopped or
/// empty deck is "nothing playing" - never a leftover title/artist.
pub fn render_card(np: &NowPlaying) -> String {
    let stopped = np.state.as_deref() == Some("stop");
    let empty = np.title.is_none() && np.artist.is_none() && np.album.is_none();
    let armed = render_armed(&np.armed);
    let field = render_field(&np.field);
    if stopped || empty {
        // A stopped deck can still hold an armed wake alarm or a held pull - surface
        // them so the machine's hold stays visible even with nothing playing. Field,
        // then the ambient hint, are the last, quietest lines.
        let mut lines = vec!["nothing playing".to_string()];
        lines.extend(armed);
        lines.extend(field);
        if let Some(hint) = &np.hint {
            lines.push(hint.phrase());
        }
        // The standing continuation-radio hint is the quietest last line: when armed
        // the deck will flow into this station at end-of-queue instead of stopping.
        if let Some(station) = &np.continuation {
            lines.push(format!("then: {station}"));
        }
        return lines.join("\n");
    }
    let mut lines = Vec::new();
    if let Some(t) = &np.title {
        // A heart marks a Subsonic favorite. U+2665 is East-Asian-Width ambiguous,
        // so it is followed by U+FE0E (text-presentation selector) to force a single
        // cell on emoji-presentation terminals. Prepended only when starred.
        if np.starred {
            lines.push(format!("\u{2665}\u{FE0E} {t}"));
        } else {
            lines.push(t.clone());
        }
    }
    // "Artist - Album" (whichever of the two is present).
    let sub: Vec<&str> = [np.artist.as_deref(), np.album.as_deref()]
        .into_iter()
        .flatten()
        .collect();
    if !sub.is_empty() {
        lines.push(sub.join(" - "));
    }

    // Status line: [playing|paused] | vol V% | N of M | duration.
    let mut status_bits = Vec::new();
    match np.state.as_deref() {
        Some("play") => status_bits.push("playing".to_string()),
        Some("pause") => status_bits.push("paused".to_string()),
        Some(other) => status_bits.push(other.to_string()),
        None => {}
    }
    if let Some(v) = np.volume {
        if v >= 0 {
            status_bits.push(format!("vol {v}%"));
        }
    }
    if let (Some(song), Some(m)) = (np.song, np.playlistlength) {
        status_bits.push(format!("{} of {}", song.saturating_add(1), m));
    }
    if let Some(d) = np.duration {
        status_bits.push(fmt_dur(d));
    }
    if !status_bits.is_empty() {
        lines.push(status_bits.join(" | "));
    }
    if let Some(a) = armed {
        lines.push(a);
    }
    // The field is the last, faintest line of the card - the quietest voice.
    if let Some(fl) = field {
        lines.push(fl);
    }
    // The standing continuation-radio hint rides below the field: what the deck will
    // flow into when the queue drains. Present only when continuation is armed.
    if let Some(station) = &np.continuation {
        lines.push(format!("then: {station}"));
    }
    lines.join("\n")
}

/// The active latent-field pulls as one unobtrusive line, e.g.
/// `toward calmer 0.58 3m | toward warmer 0.41 1m`. `None` when no pull is active,
/// so a resting field renders no line. Reconstructed from the numeric X- pairs; the
/// verbose provenance prose stays reserved for the interactive `field` command.
fn render_field(field: &FieldState) -> Option<String> {
    if !field.active() {
        return None;
    }
    let line = field
        .pulls
        .iter()
        .map(|p| format!("toward {} {:.2} {}m", p.label, p.strength as f32 / 100.0, p.age_mins))
        .collect::<Vec<_>>()
        .join(" | ");
    Some(line)
}

/// The armed human-features as one unobtrusive line, e.g.
/// `sleep 12m | winding down | wake in 7h 00m`. `None` when nothing is armed.
fn render_armed(a: &ArmedFeatures) -> Option<String> {
    if !a.any() {
        return None;
    }
    let mut bits = Vec::new();
    if let Some(s) = a.sleep_remaining {
        bits.push(format!("sleep {}", fmt_remaining(s)));
    }
    if a.winddown_active {
        match a.winddown_remaining {
            Some(s) => bits.push(format!("wind-down in {}", fmt_remaining(s))),
            None => bits.push("winding down".to_string()),
        }
    }
    if let Some(s) = a.wake_remaining {
        bits.push(format!("wake in {}", fmt_remaining(s)));
    }
    Some(bits.join(" | "))
}

fn fmt_dur(secs: f64) -> String {
    let total = secs as u64;
    format!("{}:{:02}", total / 60, total % 60)
}

/// Render the queue from `playlistinfo` pairs, iterating the shared structured
/// parse (`parse_queue`) so the CLI and TUI share one queue model. Prints
/// "<Pos+1>. <Title> - <Artist>" per item. Empty -> "queue is empty".
pub fn render_queue(pairs: &[(String, String)]) -> String {
    let items = parse_queue(pairs);
    if items.is_empty() {
        return "queue is empty".to_string();
    }
    items
        .iter()
        .map(|it| match &it.artist {
            Some(a) => format!("{}. {} - {}", it.pos + 1, it.title, a),
            None => format!("{}. {}", it.pos + 1, it.title),
        })
        .collect::<Vec<_>>()
        .join("\n")
}

#[cfg(test)]
mod tests {
    use super::*;
    use hypodj_client::model::now_playing;

    fn p(pairs: &[(&str, &str)]) -> Vec<(String, String)> {
        pairs.iter().map(|(k, v)| (k.to_string(), v.to_string())).collect()
    }

    #[test]
    fn card_playing() {
        let status = p(&[
            ("volume", "70"),
            ("playlistlength", "12"),
            ("state", "play"),
            ("song", "2"),
            ("duration", "215.000"),
        ]);
        let current = p(&[
            ("file", "song/42"),
            ("Title", "Blue in Green"),
            ("Artist", "Miles Davis"),
            ("Album", "Kind of Blue"),
            ("Pos", "2"),
        ]);
        let card = render_card(&now_playing(&status, &current));
        assert!(card.contains("Blue in Green"));
        assert!(card.contains("Miles Davis - Kind of Blue"));
        assert!(card.contains("playing"));
        assert!(card.contains("vol 70%"));
        assert!(card.contains("3 of 12"));
        assert!(card.contains("3:35"));
        assert!(!card.to_lowercase().contains("elapsed"));
    }

    #[test]
    fn card_prepends_heart_when_starred() {
        let status = p(&[("state", "play")]);
        let current = p(&[
            ("file", "song/42"),
            ("Title", "Blue in Green"),
            ("X-Starred", "1"),
        ]);
        let card = render_card(&now_playing(&status, &current));
        assert!(card.contains("\u{2665}\u{FE0E} Blue in Green"));
        // Without the pair the heart is absent (plain title).
        let plain = render_card(&now_playing(&status, &p(&[
            ("file", "song/42"),
            ("Title", "Blue in Green"),
        ])));
        assert!(!plain.contains('\u{2665}'));
        assert!(plain.contains("Blue in Green"));
    }

    #[test]
    fn card_stopped() {
        let status = p(&[("volume", "50"), ("playlistlength", "3"), ("state", "stop")]);
        assert_eq!(render_card(&now_playing(&status, &[])), "nothing playing");
    }

    #[test]
    fn card_empty_currentsong() {
        let status = p(&[("volume", "50"), ("playlistlength", "0"), ("state", "play")]);
        assert_eq!(render_card(&now_playing(&status, &[])), "nothing playing");
    }

    #[test]
    fn card_shows_ambient_hint_when_stopped() {
        // A stopped deck surfaces the just-finished hint as the quietest last line.
        let status = p(&[
            ("state", "stop"),
            ("X-hypodj-hint-kind", "just-finished"),
            ("X-hypodj-hint-title", "303 (Ninajirachi Remix)"),
        ]);
        let card = render_card(&now_playing(&status, &[]));
        assert!(card.starts_with("nothing playing"));
        assert!(card.contains("just finished 303 (Ninajirachi Remix)"));
        // The hint is the LAST line (quietest voice).
        assert!(card.ends_with("just finished 303 (Ninajirachi Remix)"));
    }

    #[test]
    fn card_up_next_hint_when_empty_deck() {
        let status = p(&[
            ("state", "play"),
            ("playlistlength", "0"),
            ("X-hypodj-hint-kind", "up-next"),
            ("X-hypodj-hint-title", "Blue in Green"),
        ]);
        let card = render_card(&now_playing(&status, &[]));
        assert!(card.contains("up next Blue in Green"));
    }

    #[test]
    fn card_shows_continuation_hint_when_armed() {
        // A stopped deck armed for continuation surfaces the station as the quietest
        // last line: what it will flow into when the queue drains.
        let status = p(&[
            ("state", "stop"),
            ("X-hypodj-continuation", "on"),
            ("X-hypodj-continuation-station", "NTS 1"),
        ]);
        let out = render_card(&now_playing(&status, &[]));
        assert!(out.contains("then: NTS 1"), "continuation hint line present:\n{out}");
        // Disarmed -> no line.
        let np = now_playing(&p(&[("state", "stop")]), &[]);
        assert!(!render_card(&np).contains("then:"));
    }

    #[test]
    fn card_no_hint_line_when_absent() {
        // No hint pair -> no extra line, just "nothing playing".
        let status = p(&[("state", "stop")]);
        assert_eq!(render_card(&now_playing(&status, &[])), "nothing playing");
    }

    #[test]
    fn card_playing_never_renders_hint() {
        // A hint pair should not exist on the wire while a library track plays (the
        // daemon suppresses it), but even if one leaked, the playing card never
        // renders it - the current track is shown by title, never duplicated by a hint.
        let status = p(&[
            ("state", "play"),
            ("song", "0"),
            ("playlistlength", "1"),
            ("X-hypodj-hint-kind", "just-finished"),
            ("X-hypodj-hint-title", "Some Other Track"),
        ]);
        let current = p(&[("file", "song/42"), ("Title", "Now Playing Track")]);
        let card = render_card(&now_playing(&status, &current));
        assert!(card.contains("Now Playing Track"));
        assert!(!card.contains("just finished"));
        assert!(!card.contains("Some Other Track"));
    }

    #[test]
    fn card_shows_armed_features() {
        let status = p(&[
            ("volume", "70"),
            ("playlistlength", "12"),
            ("state", "play"),
            ("song", "0"),
            ("X-hypodj-sleep-remaining", "720"),
            ("X-hypodj-wake-remaining", "25200"),
        ]);
        let current = p(&[("file", "song/1"), ("Title", "X"), ("Artist", "Y")]);
        let card = render_card(&now_playing(&status, &current));
        assert!(card.contains("sleep 12m"), "card: {card}");
        assert!(card.contains("wake in 7h 00m"), "card: {card}");
    }

    #[test]
    fn card_stopped_still_shows_armed_wake() {
        let status = p(&[
            ("volume", "50"),
            ("state", "stop"),
            ("X-hypodj-wake-remaining", "25200"),
        ]);
        let card = render_card(&now_playing(&status, &[]));
        assert!(card.contains("nothing playing"));
        assert!(card.contains("wake in 7h 00m"), "card: {card}");
    }

    #[test]
    fn card_no_armed_line_when_nothing_armed() {
        let status = p(&[("volume", "70"), ("state", "play"), ("song", "0")]);
        let current = p(&[("file", "song/1"), ("Title", "X"), ("Artist", "Y")]);
        let card = render_card(&now_playing(&status, &current));
        assert!(!card.contains("sleep"));
        assert!(!card.contains("wake"));
    }

    #[test]
    fn card_shows_field_pull_line() {
        let status = p(&[
            ("state", "play"),
            ("song", "0"),
            ("X-hypodj-field-count", "2"),
            ("X-hypodj-field-0-label", "calmer"),
            ("X-hypodj-field-0-strength", "58"),
            ("X-hypodj-field-0-age", "3"),
            ("X-hypodj-field-1-label", "warmer"),
            ("X-hypodj-field-1-strength", "41"),
            ("X-hypodj-field-1-age", "1"),
        ]);
        let current = p(&[("file", "song/1"), ("Title", "X"), ("Artist", "Y")]);
        let card = render_card(&now_playing(&status, &current));
        assert!(card.contains("toward calmer 0.58 3m"), "card: {card}");
        assert!(card.contains("toward warmer 0.41 1m"), "card: {card}");
        assert!(card.contains("toward calmer 0.58 3m | toward warmer 0.41 1m"), "card: {card}");
    }

    #[test]
    fn card_no_field_line_at_rest() {
        let status = p(&[("state", "play"), ("song", "0")]);
        let current = p(&[("file", "song/1"), ("Title", "X"), ("Artist", "Y")]);
        let card = render_card(&now_playing(&status, &current));
        assert!(!card.contains("toward"), "no field line at rest: {card}");
    }

    #[test]
    fn card_stopped_still_shows_held_pull() {
        let status = p(&[
            ("state", "stop"),
            ("X-hypodj-field-count", "1"),
            ("X-hypodj-field-0-label", "calmer"),
            ("X-hypodj-field-0-strength", "58"),
            ("X-hypodj-field-0-age", "3"),
        ]);
        let card = render_card(&now_playing(&status, &[]));
        assert!(card.contains("nothing playing"));
        assert!(card.contains("toward calmer 0.58 3m"), "card: {card}");
    }

    #[test]
    fn card_hides_unknown_volume() {
        let status = p(&[("volume", "-1"), ("playlistlength", "1"), ("state", "play"), ("song", "0")]);
        let current = p(&[("file", "song/1"), ("Title", "X"), ("Artist", "Y")]);
        let card = render_card(&now_playing(&status, &current));
        assert!(!card.contains("vol"));
        assert!(card.contains("1 of 1"));
    }

    #[test]
    fn queue_render() {
        let pairs = p(&[
            ("file", "song/1"),
            ("Title", "One"),
            ("Artist", "A"),
            ("Pos", "0"),
            ("file", "song/2"),
            ("Title", "Two"),
            ("Artist", "B"),
            ("Pos", "1"),
        ]);
        assert_eq!(render_queue(&pairs), "1. One - A\n2. Two - B");
    }

    #[test]
    fn queue_empty() {
        assert_eq!(render_queue(&[]), "queue is empty");
    }
}
