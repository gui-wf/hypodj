//! Internal domain model.
//!
//! FOUNDATION. These are *our* types, decoupled from the wire types of the
//! `opensubsonic` crate. The `subsonic` module maps wire -> these. Keeping this
//! boundary means the rest of the daemon (player, mpd server, cache) never
//! depends on the exact shape of a third-party crate's structs.

/// Opaque server-side id for a song/album/artist. Kept as a newtype so we can
/// never accidentally cross-use an album id where a song id is expected.
///
/// `Serialize`/`Deserialize` so the P2 plan IR ([`crate::plan`]) can carry a
/// concrete song id in a `Selector` (append-only enqueue) across the wire.
#[derive(Debug, Clone, PartialEq, Eq, Hash, serde::Serialize, serde::Deserialize)]
#[cfg_attr(feature = "schema", derive(schemars::JsonSchema))]
pub struct SongId(pub String);

#[derive(Debug, Clone, PartialEq, Eq, Hash, serde::Serialize, serde::Deserialize)]
#[cfg_attr(feature = "schema", derive(schemars::JsonSchema))]
pub struct AlbumId(pub String);

#[derive(Debug, Clone, PartialEq, Eq, Hash, serde::Serialize, serde::Deserialize)]
#[cfg_attr(feature = "schema", derive(schemars::JsonSchema))]
pub struct ArtistId(pub String);

/// A single favoritable entity. This is the ONE authority for the favorite uri
/// scheme (`song/<id>` | `album/<id>` | `artist/<id>`), the routing of a star
/// gesture to the right Subsonic wire slice, and (future P4) a serializable
/// listening-intelligence signal.
///
/// The uri PREFIX carries the entity kind, so a `playlistadd Starred <uri>` can
/// never mis-target the wrong bucket: `song/` stars a song, `album/` an album,
/// `artist/` an artist, and anything else parses to `None` (a loud ACK, not a
/// silent no-op).
#[derive(Debug, Clone, PartialEq, Eq, serde::Serialize, serde::Deserialize)]
#[cfg_attr(feature = "schema", derive(schemars::JsonSchema))]
pub enum Favorite {
    Song(SongId),
    Album(AlbumId),
    Artist(ArtistId),
}

impl Favorite {
    /// The browse/gesture uri for this favorite (`song/<id>` etc.). Inverse of
    /// [`Favorite::from_uri`].
    pub fn uri(&self) -> String {
        match self {
            Favorite::Song(id) => format!("song/{}", id.0),
            Favorite::Album(id) => format!("album/{}", id.0),
            Favorite::Artist(id) => format!("artist/{}", id.0),
        }
    }

    /// Parse a favorite uri. The single parse site for star routing: the prefix
    /// is the sole routing authority. An unknown or prefixless uri yields `None`.
    pub fn from_uri(uri: &str) -> Option<Favorite> {
        if let Some(id) = uri.strip_prefix("song/") {
            Some(Favorite::Song(SongId(id.to_string())))
        } else if let Some(id) = uri.strip_prefix("album/") {
            Some(Favorite::Album(AlbumId(id.to_string())))
        } else if let Some(id) = uri.strip_prefix("artist/") {
            Some(Favorite::Artist(ArtistId(id.to_string())))
        } else {
            None
        }
    }
}

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

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn favorite_from_uri_routes_each_kind_by_prefix() {
        assert_eq!(
            Favorite::from_uri("song/so-1"),
            Some(Favorite::Song(SongId("so-1".into())))
        );
        assert_eq!(
            Favorite::from_uri("album/al-1"),
            Some(Favorite::Album(AlbumId("al-1".into())))
        );
        assert_eq!(
            Favorite::from_uri("artist/ar-1"),
            Some(Favorite::Artist(ArtistId("ar-1".into())))
        );
    }

    #[test]
    fn favorite_from_uri_rejects_unknown_and_prefixless() {
        // Unknown prefix, bare id, and empty all yield None -> a loud ACK in the
        // playlistadd Starred arm, never a mis-targeted bucket.
        assert_eq!(Favorite::from_uri("genre/x"), None);
        assert_eq!(Favorite::from_uri("al-1"), None);
        assert_eq!(Favorite::from_uri(""), None);
    }

    #[test]
    fn favorite_uri_round_trips_for_all_variants() {
        for f in [
            Favorite::Song(SongId("so-1".into())),
            Favorite::Album(AlbumId("al-1".into())),
            Favorite::Artist(ArtistId("ar-1".into())),
        ] {
            assert_eq!(Favorite::from_uri(&f.uri()), Some(f));
        }
    }
}
