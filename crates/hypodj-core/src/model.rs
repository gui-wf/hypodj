//! Internal domain model.
//!
//! FOUNDATION. These are *our* types, decoupled from the wire types of the
//! `opensubsonic` crate. The `subsonic` module maps wire -> these. Keeping this
//! boundary means the rest of the daemon (player, mpd server, cache) never
//! depends on the exact shape of a third-party crate's structs.

/// Opaque server-side id for a song/album/artist. Kept as a newtype so we can
/// never accidentally cross-use an album id where a song id is expected.
#[derive(Debug, Clone, PartialEq, Eq, Hash)]
pub struct SongId(pub String);

#[derive(Debug, Clone, PartialEq, Eq, Hash)]
pub struct AlbumId(pub String);

#[derive(Debug, Clone, PartialEq, Eq, Hash)]
pub struct ArtistId(pub String);

#[derive(Debug, Clone)]
pub struct Artist {
    pub id: ArtistId,
    pub name: String,
    /// Number of albums. The wire type (`ArtistId3.album_count`) is
    /// `Option<i64>`; the `subsonic` mapper defaults missing to 0 and saturates
    /// the i64 into u32. This is a deliberate, documented lossy conversion kept
    /// in one place (see `subsonic::i64_to_u32`), not an accidental mismatch.
    pub album_count: u32,
    /// Whether the current user has starred this artist (wire `starred` is an
    /// ISO-8601 timestamp string; we only carry the boolean here).
    pub starred: bool,
    pub cover_art: Option<String>,
}

#[derive(Debug, Clone)]
pub struct Album {
    pub id: AlbumId,
    pub name: String,
    pub artist: String,
    pub artist_id: Option<ArtistId>,
    pub year: Option<u32>,
    pub genre: Option<String>,
    pub cover_art: Option<String>,
    pub song_count: u32,
}

#[derive(Debug, Clone)]
pub struct Song {
    pub id: SongId,
    pub title: String,
    pub album: Option<String>,
    pub album_id: Option<AlbumId>,
    pub artist: Option<String>,
    pub track: Option<u32>,
    pub duration_secs: Option<u32>,
    /// Cover-art id (NOT the song id). Used to resolve `albumart`/`readpicture`.
    /// When absent, the handler falls back to the song id itself (Navidrome and
    /// most servers accept the media id directly for getCoverArt).
    pub cover_art: Option<String>,
    pub starred: bool,
    // ── richer metadata (feature 7) - all Option so absent server data is clean.
    /// MusicBrainz recording/track id (wire `Child.music_brainz_id`).
    pub musicbrainz_id: Option<String>,
    /// Disc number (wire `Child.disc_number`).
    pub disc: Option<u32>,
    /// Release year (wire `Child.year`). Emitted as MPD `Date`.
    pub year: Option<u32>,
    /// Genre name (wire `Child.genre`).
    pub genre: Option<String>,
    /// Bitrate in kbps (wire `Child.bit_rate`).
    pub bitrate: Option<u32>,
    /// Free-form comment (wire `Child.comment`).
    pub comment: Option<String>,
    /// The current user's 0..=5 rating (wire `Child.user_rating`).
    pub user_rating: Option<u8>,
    /// Composer display string (OpenSubsonic). Prefer wire `Child.display_composer`;
    /// fall back to the `Child.contributors` entries whose role is "composer".
    /// Plain-Subsonic servers omit this - `None` then matches nothing (honest).
    pub composer: Option<String>,
    /// Performer display string (OpenSubsonic). There is no `display_performer`
    /// wire field; derived from `Child.contributors` entries whose role is
    /// "performer". Plain-Subsonic servers omit contributors - `None` then.
    pub performer: Option<String>,
}

/// One thing that can sit in the play queue: either a resolved Subsonic [`Song`]
/// or a raw internet-radio / HTTP stream URL added directly (MPD's `add <url>`).
///
/// A raw stream has no Subsonic song id, no rating, and is never scrobbled - it
/// is played by handing its URL straight to the player. Keeping this as an enum
/// (rather than an `Option<SongId>` bolted onto `Song`) means the stream case
/// carries only what it actually has: a URL and a display title.
#[derive(Debug, Clone)]
pub enum QueueEntry {
    /// A library track resolved from Subsonic. Playing it resolves a stream URL
    /// via the client and scrobbles on the usual threshold.
    Song(Song),
    /// A raw HTTP(S) stream (internet radio). `url` is played verbatim by the
    /// player; `title` is what MPD renders (defaults to the URL). No song id,
    /// no scrobble.
    Stream { url: String, title: String },
}

impl QueueEntry {
    /// The MPD `file:` / display title for this entry.
    pub fn title(&self) -> &str {
        match self {
            QueueEntry::Song(s) => &s.title,
            QueueEntry::Stream { title, .. } => title,
        }
    }
}

/// A genre with its song/album counts (wire `data::Genre`; `name` is the
/// renamed `value` field). Backs the `Genres` browse dir and `list genre`.
#[derive(Debug, Clone)]
pub struct Genre {
    pub name: String,
    pub song_count: u32,
    pub album_count: u32,
}
