//! Pure parsing of `status` + `currentsong` + `playlistinfo` pairs into structured
//! values. Model-free: just the parse half, no text formatting (the CLI card and
//! queue text formatters live in hypodj-cli/render.rs on top of this). The server
//! emits only a known subset of keys - there is NO `elapsed` and NO `time` key.

/// The now-playing state assembled from `status` + `currentsong` pairs. Fully
/// Option-typed - the server may omit any field.
#[derive(Debug, Default, PartialEq)]
pub struct NowPlaying {
    pub state: Option<String>, // "play" / "pause" / "stop"
    pub volume: Option<i32>,   // -1 or absent => unknown, hidden
    pub playlistlength: Option<usize>,
    pub song: Option<usize>,   // 0-based index of current
    pub duration: Option<f64>, // library songs only
    pub title: Option<String>,
    pub artist: Option<String>,
    pub album: Option<String>,
    /// The current song's uri from `currentsong` `file` (`song/<id>` for a library
    /// track, an `http(s)://...` URL for a raw stream). Needed to favorite the
    /// current track (`playlistadd Starred <uri>`); a stream has no star surface.
    pub file: Option<String>,
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
        file: find(current, "file").map(str::to_string),
    }
}

/// One entry in the queue parsed from a `playlistinfo` block. `pos` is the 0-based
/// MPD `Pos` (fall back to the block index if absent).
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct QueueItem {
    pub pos: usize,
    pub title: String,
    pub artist: Option<String>,
    /// The row's uri from the block `file` key (`song/<id>` for a library track,
    /// an `http(s)://...` URL for a raw stream). Needed to favorite the SELECTED
    /// row (`playlistadd Starred <uri>`); a stream has no star surface.
    pub uri: Option<String>,
    /// The album browse uri (`album/<id>`) from the non-standard `X-AlbumUri` pair
    /// the daemon emits per library song, so the TUI can group the queue by album
    /// for the browse queue markers. `None` for a raw stream (no album).
    pub album_uri: Option<String>,
}

/// Parse the flat `playlistinfo` pair list into structured queue items. Each entry
/// begins at a `file` key; group by that boundary and pull Pos/Title/Artist.
pub fn parse_queue(pairs: &[(String, String)]) -> Vec<QueueItem> {
    group_blocks(pairs)
        .iter()
        .enumerate()
        .map(|(i, b)| QueueItem {
            pos: find(b, "Pos").and_then(|v| v.parse::<usize>().ok()).unwrap_or(i),
            title: find(b, "Title").unwrap_or("(unknown)").to_string(),
            artist: find(b, "Artist").map(str::to_string),
            uri: find(b, "file").map(str::to_string),
            album_uri: find(b, "X-AlbumUri").map(str::to_string),
        })
        .collect()
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
            ("Id", "42"),
        ]);
        let np = now_playing(&status, &current);
        assert_eq!(np.state.as_deref(), Some("play"));
        assert_eq!(np.volume, Some(70));
        assert_eq!(np.playlistlength, Some(12));
        assert_eq!(np.song, Some(2));
        assert_eq!(np.duration, Some(215.0));
        assert_eq!(np.title.as_deref(), Some("Blue in Green"));
        assert_eq!(np.artist.as_deref(), Some("Miles Davis"));
        assert_eq!(np.album.as_deref(), Some("Kind of Blue"));
        assert_eq!(np.file.as_deref(), Some("song/42"));
    }

    #[test]
    fn nowplaying_stopped_empty_current() {
        let status = p(&[("volume", "50"), ("playlistlength", "3"), ("state", "stop")]);
        let np = now_playing(&status, &[]);
        assert_eq!(np.state.as_deref(), Some("stop"));
        assert_eq!(np.title, None);
    }

    #[test]
    fn nowplaying_unknown_volume() {
        let status = p(&[("volume", "-1"), ("playlistlength", "1"), ("state", "play"), ("song", "0")]);
        let np = now_playing(&status, &[]);
        assert_eq!(np.volume, Some(-1));
    }

    #[test]
    fn parse_queue_pos_title_artist() {
        let pairs = p(&[
            ("file", "song/1"),
            ("Title", "One"),
            ("Artist", "A"),
            ("Pos", "0"),
            ("Id", "1"),
            ("file", "song/2"),
            ("Title", "Two"),
            ("Pos", "1"),
            ("Id", "2"),
        ]);
        let q = parse_queue(&pairs);
        assert_eq!(q.len(), 2);
        assert_eq!(
            q[0],
            QueueItem {
                pos: 0,
                title: "One".into(),
                artist: Some("A".into()),
                uri: Some("song/1".into()),
                album_uri: None,
            }
        );
        // Second block has no Artist -> None.
        assert_eq!(
            q[1],
            QueueItem {
                pos: 1,
                title: "Two".into(),
                artist: None,
                uri: Some("song/2".into()),
                album_uri: None,
            }
        );
    }

    #[test]
    fn parse_queue_reads_album_uri() {
        // The daemon's non-standard X-AlbumUri pair groups a queued song by album.
        let pairs = p(&[
            ("file", "song/1"),
            ("Title", "One"),
            ("X-AlbumUri", "album/al-9"),
            ("Pos", "0"),
            ("Id", "1"),
            // A stream row carries no X-AlbumUri -> None.
            ("file", "http://stream.example/live"),
            ("Title", "Live"),
            ("Pos", "1"),
            ("Id", "2"),
        ]);
        let q = parse_queue(&pairs);
        assert_eq!(q[0].album_uri.as_deref(), Some("album/al-9"));
        assert_eq!(q[1].album_uri, None);
    }

    #[test]
    fn parse_queue_empty() {
        assert!(parse_queue(&[]).is_empty());
    }
}
