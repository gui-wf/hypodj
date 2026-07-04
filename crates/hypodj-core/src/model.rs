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
}

/// A genre with its song/album counts (wire `data::Genre`; `name` is the
/// renamed `value` field). Backs the `Genres` browse dir and `list genre`.
#[derive(Debug, Clone)]
pub struct Genre {
    pub name: String,
    pub song_count: u32,
    pub album_count: u32,
}
