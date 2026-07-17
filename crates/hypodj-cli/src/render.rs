//! Text formatters for the hjq CLI, expressed on top of the shared parse layer in
//! `hypodj_client::model`. This crate owns ONLY the text card/queue rendering; the
//! NowPlaying + queue parsing lives in the client. A stopped/empty deck renders a
//! friendly "nothing playing".

use hypodj_client::model::{fmt_remaining, parse_queue, ArmedFeatures, NowPlaying};

/// Render the now-playing card as plain text (a couple of lines). A stopped or
/// empty deck is "nothing playing" - never a leftover title/artist.
pub fn render_card(np: &NowPlaying) -> String {
    let stopped = np.state.as_deref() == Some("stop");
    let empty = np.title.is_none() && np.artist.is_none() && np.album.is_none();
    let armed = render_armed(&np.armed);
    if stopped || empty {
        // A stopped deck can still hold an armed wake alarm - surface it so the
        // machine's hold on the night stays visible even with nothing playing.
        return match armed {
            Some(a) => format!("nothing playing\n{a}"),
            None => "nothing playing".to_string(),
        };
    }
    let mut lines = Vec::new();
    if let Some(t) = &np.title {
        lines.push(t.clone());
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
    lines.join("\n")
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
