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
    pub cover_art: Option<String>,
    pub starred: bool,
}
