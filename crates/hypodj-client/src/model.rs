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
    /// True when the current track is a Subsonic favorite (the daemon emits the
    /// non-standard `X-Starred` pair on `currentsong`), so the clients can show a
    /// heart. Parsed from `currentsong`, coexisting with the `armed` status pairs.
    pub starred: bool,
    /// The armed human-features, surfaced by the daemon as X- status pairs and
    /// present ONLY when armed. Startle-safe equals trust only if the machine's
    /// hold on the night is VISIBLE - these back that render.
    pub armed: ArmedFeatures,
    /// The active latent-field pulls, surfaced by the daemon as X- status pairs and
    /// present ONLY while a pull is active. Backs the passive "see the field" HUD:
    /// an inspectable, decaying magnetism map is what makes the nondeterministic
    /// field trustworthy.
    pub field: FieldState,
}

/// One active pull, reconstructed from the daemon's `X-hypodj-field-{i}-*` pairs.
/// `strength` is a basis-of-100 integer (the wire value); render as `strength/100`.
#[derive(Debug, Default, PartialEq, Clone)]
pub struct FieldPull {
    /// The pull label - the matched lexicon token(s), e.g. `calmer` or `less energy`.
    pub label: String,
    /// Decayed strength as an integer 0..=100 (the wire basis-of-100 value).
    pub strength: u8,
    /// Whole minutes since the pull was born/reinforced.
    pub age_mins: u64,
}

/// The active latent-field, parsed from the daemon's `X-hypodj-field-*` status
/// pairs. Empty when no pull is active, so a lean status leaves this empty and the
/// clients render nothing.
#[derive(Debug, Default, PartialEq, Clone)]
pub struct FieldState {
    /// The live pulls in insertion order (most recent last).
    pub pulls: Vec<FieldPull>,
}

impl FieldState {
    /// `true` when at least one pull is active - the HUD render gate.
    pub fn active(&self) -> bool {
        !self.pulls.is_empty()
    }

    fn parse(status: &[(String, String)]) -> Self {
        let count = find(status, "X-hypodj-field-count")
            .and_then(|v| v.parse::<usize>().ok())
            .unwrap_or(0);
        let mut pulls = Vec::new();
        for i in 0..count {
            // Skip any index missing a key (defensive against a torn snapshot).
            let label = match find(status, &format!("X-hypodj-field-{i}-label")) {
                Some(l) => l.to_string(),
                None => continue,
            };
            let strength = match find(status, &format!("X-hypodj-field-{i}-strength"))
                .and_then(|v| v.parse::<u8>().ok())
            {
                Some(s) => s,
                None => continue,
            };
            let age_mins = match find(status, &format!("X-hypodj-field-{i}-age"))
                .and_then(|v| v.parse::<u64>().ok())
            {
                Some(a) => a,
                None => continue,
            };
            pulls.push(FieldPull { label, strength, age_mins });
        }
        FieldState { pulls }
    }
}

/// The armed sleep / wind-down / wake state parsed from the daemon's X- status
/// pairs. Every field is `None`/`false` when nothing is armed, so a lean status
/// leaves this empty and the clients render nothing.
#[derive(Debug, Default, PartialEq, Clone)]
pub struct ArmedFeatures {
    /// Seconds until the sleep fade-to-stop fires (`X-hypodj-sleep-remaining`).
    pub sleep_remaining: Option<u64>,
    /// A wind-down plan is armed (`X-hypodj-winddown-active`).
    pub winddown_active: bool,
    /// Seconds until a scheduled wind-down fires (`X-hypodj-winddown-remaining`);
    /// absent for an immediate wind-down.
    pub winddown_remaining: Option<u64>,
    /// Seconds until the scheduled wake alarm (`X-hypodj-wake-remaining`).
    pub wake_remaining: Option<u64>,
    /// The wake alarm as a unix epoch second (`X-hypodj-wake-at`).
    pub wake_at: Option<u64>,
}

impl ArmedFeatures {
    /// `true` if any feature is armed - the render gate.
    pub fn any(&self) -> bool {
        self.sleep_remaining.is_some()
            || self.winddown_active
            || self.wake_remaining.is_some()
    }

    fn parse(status: &[(String, String)]) -> Self {
        let num = |k: &str| find(status, k).and_then(|v| v.parse::<u64>().ok());
        ArmedFeatures {
            sleep_remaining: num("X-hypodj-sleep-remaining"),
            winddown_active: find(status, "X-hypodj-winddown-active").is_some(),
            winddown_remaining: num("X-hypodj-winddown-remaining"),
            wake_remaining: num("X-hypodj-wake-remaining"),
            wake_at: num("X-hypodj-wake-at"),
        }
    }
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
        starred: find(current, "X-Starred").is_some(),
        armed: ArmedFeatures::parse(status),
        field: FieldState::parse(status),
    }
}

/// Format a `secs` remaining as a compact human-readable string: `Hh MMm`, `MMm`,
/// or `Ss`. Used by both clients so the armed-feature render reads consistently.
pub fn fmt_remaining(secs: u64) -> String {
    let h = secs / 3600;
    let m = (secs % 3600) / 60;
    let s = secs % 60;
    if h > 0 {
        format!("{h}h {m:02}m")
    } else if m > 0 {
        format!("{m}m")
    } else {
        format!("{s}s")
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
        // No armed X- pairs -> nothing armed.
        assert!(!np.armed.any());
        // No X-Starred pair -> not a favorite.
        assert!(!np.starred);
    }

    #[test]
    fn nowplaying_parses_x_starred_coexisting_with_armed() {
        // A starred current track WHILE a sleep timer is armed: the two X- sources
        // (currentsong X-Starred + status X-hypodj-*) must parse independently.
        let status = p(&[
            ("state", "play"),
            ("X-hypodj-sleep-remaining", "600"),
        ]);
        let current = p(&[
            ("file", "song/42"),
            ("Title", "Blue in Green"),
            ("X-Starred", "1"),
        ]);
        let np = now_playing(&status, &current);
        assert!(np.starred);
        assert!(np.armed.any());
        assert_eq!(np.armed.sleep_remaining, Some(600));
        // Absent pair -> not starred, armed untouched.
        let np2 = now_playing(&status, &p(&[("file", "song/7"), ("Title", "X")]));
        assert!(!np2.starred);
        assert!(np2.armed.any());
    }

    #[test]
    fn nowplaying_parses_armed_feature_pairs() {
        let status = p(&[
            ("volume", "70"),
            ("state", "play"),
            ("X-hypodj-sleep-remaining", "720"),
            ("X-hypodj-winddown-active", "1"),
            ("X-hypodj-wake-remaining", "25200"),
            ("X-hypodj-wake-at", "1750000000"),
        ]);
        let np = now_playing(&status, &[]);
        assert!(np.armed.any());
        assert_eq!(np.armed.sleep_remaining, Some(720));
        assert!(np.armed.winddown_active);
        assert_eq!(np.armed.winddown_remaining, None);
        assert_eq!(np.armed.wake_remaining, Some(25200));
        assert_eq!(np.armed.wake_at, Some(1750000000));
    }

    #[test]
    fn armed_absent_when_no_pairs() {
        let np = now_playing(&p(&[("state", "play")]), &[]);
        assert_eq!(np.armed, ArmedFeatures::default());
    }

    #[test]
    fn nowplaying_parses_field_pull_pairs() {
        let status = p(&[
            ("state", "play"),
            ("X-hypodj-field-count", "2"),
            ("X-hypodj-field-0-label", "calmer"),
            ("X-hypodj-field-0-strength", "58"),
            ("X-hypodj-field-0-age", "3"),
            ("X-hypodj-field-1-label", "warmer"),
            ("X-hypodj-field-1-strength", "41"),
            ("X-hypodj-field-1-age", "1"),
        ]);
        let np = now_playing(&status, &[]);
        assert!(np.field.active());
        assert_eq!(np.field.pulls.len(), 2);
        assert_eq!(np.field.pulls[0], FieldPull { label: "calmer".into(), strength: 58, age_mins: 3 });
        assert_eq!(np.field.pulls[1], FieldPull { label: "warmer".into(), strength: 41, age_mins: 1 });
    }

    #[test]
    fn field_absent_when_no_pairs() {
        let np = now_playing(&p(&[("state", "play")]), &[]);
        assert!(!np.field.active());
        assert_eq!(np.field, FieldState::default());
    }

    #[test]
    fn field_skips_torn_index_missing_key() {
        // A count of 2 but the second pull's strength pair is missing (a torn
        // snapshot): the incomplete index is skipped, never a garbage pull.
        let status = p(&[
            ("X-hypodj-field-count", "2"),
            ("X-hypodj-field-0-label", "calmer"),
            ("X-hypodj-field-0-strength", "58"),
            ("X-hypodj-field-0-age", "3"),
            ("X-hypodj-field-1-label", "warmer"),
            ("X-hypodj-field-1-age", "1"),
        ]);
        let np = now_playing(&status, &[]);
        assert_eq!(np.field.pulls.len(), 1);
        assert_eq!(np.field.pulls[0].label, "calmer");
    }

    #[test]
    fn fmt_remaining_reads_human() {
        assert_eq!(fmt_remaining(45), "45s");
        assert_eq!(fmt_remaining(720), "12m");
        assert_eq!(fmt_remaining(25200), "7h 00m");
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
