//! Pure now-playing + queue rendering from parsed pairs. The server emits only a
//! known subset of keys - there is NO `elapsed` and NO `time` key, so we NEVER
//! render elapsed. Everything is Option-typed; a stopped/empty deck renders a
//! friendly "nothing playing".

/// The now-playing state assembled from `status` + `currentsong` pairs. Fully
/// Option-typed - the server may omit any field.
#[derive(Debug, Default, PartialEq)]
pub struct NowPlaying {
    pub state: Option<String>, // "play" / "pause" / "stop"
    pub volume: Option<i32>,   // -1 or absent => unknown, hidden
    pub playlistlength: Option<usize>,
    pub song: Option<usize>,      // 0-based index of current
    pub duration: Option<f64>,    // library songs only
    pub title: Option<String>,
    pub artist: Option<String>,
    pub album: Option<String>,
}

fn find<'a>(pairs: &'a [(String, String)], key: &str) -> Option<&'a str> {
    pairs.iter().find(|(k, _)| k == key).map(|(_, v)| v.as_str())
}

pub fn now_playing(status: &[(String, String)], current: &[(String, String)]) -> NowPlaying {
    NowPlaying {
        state: find(status, "state").map(str::to_string),
        volume: find(status, "volume").and_then(|v| v.parse::<i32>().ok()),
        playlistlength: find(status, "playlistlength").and_then(|v| v.parse().ok()),
        song: find(status, "song").and_then(|v| v.parse().ok()),
        duration: find(status, "duration").and_then(|v| v.parse().ok()),
        title: find(current, "Title").map(str::to_string),
        artist: find(current, "Artist").map(str::to_string),
        album: find(current, "Album").map(str::to_string),
    }
}

/// Render the now-playing card as plain text (a couple of lines). A stopped or
/// empty deck is "nothing playing" - never a leftover title/artist.
pub fn render_card(np: &NowPlaying) -> String {
    let stopped = np.state.as_deref() == Some("stop");
    let empty = np.title.is_none() && np.artist.is_none() && np.album.is_none();
    if stopped || empty {
        return "nothing playing".to_string();
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
    lines.join("\n")
}

fn fmt_dur(secs: f64) -> String {
    let total = secs as u64;
    format!("{}:{:02}", total / 60, total % 60)
}

/// Render the queue from repeated `playlistinfo` song_pairs blocks. Each block
/// starts at a `file` key; group by that boundary, print "<Pos+1>. <Title> -
/// <Artist>". Empty -> "queue is empty".
pub fn render_queue(pairs: &[(String, String)]) -> String {
    let blocks = group_blocks(pairs);
    if blocks.is_empty() {
        return "queue is empty".to_string();
    }
    let mut lines = Vec::new();
    for (i, b) in blocks.iter().enumerate() {
        let pos = find(b, "Pos")
            .and_then(|v| v.parse::<usize>().ok())
            .map(|p| p + 1)
            .unwrap_or(i + 1);
        let title = find(b, "Title").unwrap_or("(unknown)");
        let artist = find(b, "Artist");
        match artist {
            Some(a) => lines.push(format!("{pos}. {title} - {a}")),
            None => lines.push(format!("{pos}. {title}")),
        }
    }
    lines.join("\n")
}

/// Split a flat pair list into per-song blocks, each beginning at a `file` key.
fn group_blocks(pairs: &[(String, String)]) -> Vec<Vec<(String, String)>> {
    let mut blocks: Vec<Vec<(String, String)>> = Vec::new();
    for (k, v) in pairs {
        if k == "file" {
            blocks.push(Vec::new());
        }
        if let Some(cur) = blocks.last_mut() {
            cur.push((k.clone(), v.clone()));
        }
    }
    blocks
}

#[cfg(test)]
mod tests {
    use super::*;

    fn p(pairs: &[(&str, &str)]) -> Vec<(String, String)> {
        pairs.iter().map(|(k, v)| (k.to_string(), v.to_string())).collect()
    }

    #[test]
    fn nowplaying_playing() {
        // Canned status WITHOUT elapsed/time (the server never emits them).
        let status = p(&[
            ("volume", "70"),
            ("repeat", "0"),
            ("random", "0"),
            ("single", "0"),
            ("consume", "0"),
            ("playlist", "5"),
            ("playlistlength", "12"),
            ("state", "play"),
            ("song", "2"),
            ("songid", "42"),
            ("duration", "215.000"),
        ]);
        let current = p(&[
            ("file", "song/42"),
            ("Title", "Blue in Green"),
            ("Artist", "Miles Davis"),
            ("Album", "Kind of Blue"),
            ("Pos", "2"),
            ("Id", "42"),
        ]);
        let np = now_playing(&status, &current);
        let card = render_card(&np);
        assert!(card.contains("Blue in Green"));
        assert!(card.contains("Miles Davis - Kind of Blue"));
        assert!(card.contains("playing"));
        assert!(card.contains("vol 70%"));
        assert!(card.contains("3 of 12")); // song 2 (0-based) -> 3 of 12
        assert!(card.contains("3:35")); // 215s
        // No elapsed rendered (there is no elapsed key at all).
        assert!(!card.to_lowercase().contains("elapsed"));
    }

    #[test]
    fn nowplaying_stopped() {
        let status = p(&[("volume", "50"), ("playlistlength", "3"), ("state", "stop")]);
        let np = now_playing(&status, &[]);
        assert_eq!(render_card(&np), "nothing playing");
    }

    #[test]
    fn nowplaying_empty_currentsong() {
        // state=play but currentsong returned bare OK (no pairs) -> nothing playing,
        // no leftover title.
        let status = p(&[("volume", "50"), ("playlistlength", "0"), ("state", "play")]);
        let np = now_playing(&status, &[]);
        assert_eq!(render_card(&np), "nothing playing");
    }

    #[test]
    fn nowplaying_hides_unknown_volume() {
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
            ("Id", "1"),
            ("file", "song/2"),
            ("Title", "Two"),
            ("Artist", "B"),
            ("Pos", "1"),
            ("Id", "2"),
        ]);
        let out = render_queue(&pairs);
        assert_eq!(out, "1. One - A\n2. Two - B");
    }

    #[test]
    fn queue_empty() {
        assert_eq!(render_queue(&[]), "queue is empty");
    }
}
