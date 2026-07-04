//! Thin wrapper over the `opensubsonic` client crate.
//!
//! FOUNDATION. The vertical slice covers, for REAL, against a live server:
//! ping, browse artists (`get_artists`), album list (`get_album_list2`), and
//! stream-URL resolution. The remaining ~75 endpoints are TODO(next-phase) but
//! they all land here as new methods, mapping wire types -> `crate::model`.
//!
//! Rationale for the wrapper: every other module talks to `SubsonicClient`, not
//! to `opensubsonic::Client`. If we ever swap the client crate or the API
//! version, the blast radius is this one file. The upstream error enum is also
//! flattened to a string here, so callers never depend on its shape.
//!
//! Endpoint coverage note (verified against opensubsonic 0.3.0 source, not just
//! docs): every endpoint the 9-feature porting map needs EXISTS with a concrete
//! typed return - `get_artists -> ArtistsId3`, `get_album_list2 -> Vec<AlbumId3>`,
//! `search3 -> SearchResult3`, `scrobble`, `star/unstar`, `get_similar_songs2`
//! (in the browsing module), `get_genres -> Vec<Genre>`, `get_cover_art -> Bytes`.
//! So the honest remaining gap is field-level wire->model mapping, NOT endpoint
//! existence and NOT a get()/raw fallback or a fork.

use crate::config::ServerConfig;
use crate::model::{Album, AlbumId, Artist, ArtistId, Song, SongId};
use opensubsonic::{AlbumListType, data};
use url::Url;

/// Errors surfaced from the Subsonic layer. We flatten the upstream error into
/// a message so callers don't depend on the upstream error enum.
#[derive(Debug, thiserror::Error)]
pub enum SubsonicError {
    #[error("subsonic client init: {0}")]
    Init(String),
    #[error("subsonic request: {0}")]
    Request(String),
}

pub struct SubsonicClient {
    inner: opensubsonic::Client,
}

impl SubsonicClient {
    /// Build the client from config. Uses token auth (MD5(password+salt)+salt),
    /// the recommended OpenSubsonic scheme - the password never crosses the
    /// wire in the clear.
    pub fn connect(cfg: &ServerConfig) -> Result<Self, SubsonicError> {
        let auth = opensubsonic::Auth::token(&cfg.username, &cfg.password);
        let inner = opensubsonic::Client::new(&cfg.url, auth)
            .map_err(|e| SubsonicError::Init(e.to_string()))?;
        Ok(Self { inner })
    }

    /// Liveness + credential check. Vertical-slice step 2.
    pub async fn ping(&self) -> Result<(), SubsonicError> {
        self.inner
            .ping()
            .await
            .map_err(|e| SubsonicError::Request(e.to_string()))
    }

    /// Browse: all artists. Vertical-slice step 3a. REAL: calls `get_artists`
    /// and flattens the `ArtistsId3 { index: Vec<IndexId3 { artist: Vec<..> }> }`
    /// shape into our flat `Vec<Artist>`.
    pub async fn artists(&self) -> Result<Vec<Artist>, SubsonicError> {
        let data = self
            .inner
            .get_artists(None)
            .await
            .map_err(|e| SubsonicError::Request(e.to_string()))?;
        let artists = data
            .index
            .into_iter()
            .flat_map(|idx| idx.artist)
            .map(map_artist)
            .collect();
        Ok(artists)
    }

    /// Browse: a list of albums. Vertical-slice step 3b. REAL: calls
    /// `get_album_list2`.
    ///
    /// Verified signature (opensubsonic 0.3.0, lists.rs:87 - it is a flat 7-arg
    /// call, NOT a tuple): `get_album_list2(list_type: AlbumListType, size,
    /// offset, from_year, to_year, genre, music_folder_id) -> Vec<AlbumId3>`.
    /// `AlbumListType` is re-exported from the crate root. The porting map's
    /// "smart album lists" feature is just varying `list_type` here.
    pub async fn album_list(
        &self,
        list_type: AlbumListType,
        size: Option<i32>,
    ) -> Result<Vec<Album>, SubsonicError> {
        let albums = self
            .inner
            .get_album_list2(list_type, size, None, None, None, None, None)
            .await
            .map_err(|e| SubsonicError::Request(e.to_string()))?;
        Ok(albums.into_iter().map(map_album).collect())
    }

    /// List the songs of an album. REAL: calls `get_album` (returns
    /// `AlbumWithSongsId3`, whose `song: Vec<Child>` are the tracks) and maps
    /// each `Child` into our `Song`. This is the "resolve an album's tracks so
    /// we can queue+stream them" step the play-probe needs to pick a real song
    /// id without guessing.
    pub async fn album_songs(&self, id: &AlbumId) -> Result<Vec<Song>, SubsonicError> {
        let album = self
            .inner
            .get_album(&id.0)
            .await
            .map_err(|e| SubsonicError::Request(e.to_string()))?;
        Ok(album.song.into_iter().map(map_song).collect())
    }

    /// Resolve a playable stream URL for a song. Vertical-slice step 4.
    ///
    /// This is the handoff point to the audio player: hand this URL straight to
    /// mpv. We keep the return type as `url::Url` (not String) so there is no
    /// parse round-trip at the handoff - `libmpv2`/`reqwest` both accept a `Url`,
    /// and MPD serialization can `Display` it. `stream_url` is SYNC upstream (it
    /// only builds a URL, no request), so this method is sync too.
    pub fn stream_url(&self, id: &SongId) -> Result<Url, SubsonicError> {
        self.inner
            .stream_url(&id.0, None, None)
            .map_err(|e| SubsonicError::Request(e.to_string()))
    }
}

// ── wire -> model mapping ──────────────────────────────────────────────────
//
// The wire types use i64/Option heavily; our model prefers non-optional u32 for
// counts. These mappers own that lossy conversion in ONE place (unwrap_or(0),
// then a saturating i64->u32 cast) so no caller has to think about it.

fn map_artist(a: data::ArtistId3) -> Artist {
    Artist {
        id: ArtistId(a.id),
        name: a.name,
        // wire album_count is Option<i64>; default 0, saturate into u32.
        album_count: i64_to_u32(a.album_count.unwrap_or(0)),
        starred: a.starred.is_some(),
        cover_art: a.cover_art,
    }
}

fn map_album(a: data::AlbumId3) -> Album {
    Album {
        id: AlbumId(a.id),
        name: a.name,
        artist: a.artist.unwrap_or_default(),
        artist_id: a.artist_id.map(ArtistId),
        year: a.year.map(|y| y.max(0) as u32),
        genre: a.genre,
        cover_art: a.cover_art,
        song_count: i64_to_u32(a.song_count.unwrap_or(0)),
    }
}

/// Map a wire `Child` (the universal media/song row) into our `Song`. Only the
/// song-relevant fields are carried; video/rating/podcast fields are dropped at
/// this boundary so the model never grows a wire-shaped tail.
fn map_song(c: data::Child) -> Song {
    Song {
        id: SongId(c.id),
        title: c.title,
        album: c.album,
        album_id: c.album_id.map(AlbumId),
        artist: c.artist,
        track: c.track.map(|t| t.max(0) as u32),
        duration_secs: c.duration.map(|d| i64_to_u32(d)),
        cover_art: c.cover_art,
        starred: c.starred.is_some(),
    }
}

/// Saturating i64 -> u32. Negative (never expected for a count) clamps to 0.
fn i64_to_u32(v: i64) -> u32 {
    v.clamp(0, u32::MAX as i64) as u32
}

#[cfg(test)]
mod tests {
    use super::*;

    // These deserialize the EXACT camelCase wire JSON an OpenSubsonic server
    // sends (through the real `opensubsonic::data` structs), then run our
    // wire->model mappers. This exercises the boundary honestly: if the crate's
    // field names/shapes drift, deserialization fails here, not silently in
    // production. It does not need a live server (the wire shape is fixed).

    #[test]
    fn map_artist_flattens_optional_count_and_starred() {
        let wire: data::ArtistId3 = serde_json::from_str(
            r#"{ "id": "ar-1", "name": "Kalabrese", "albumCount": 3,
                 "starred": "2024-01-02T03:04:05Z", "coverArt": "ca-9" }"#,
        )
        .unwrap();
        let a = map_artist(wire);
        assert_eq!(a.id, ArtistId("ar-1".into()));
        assert_eq!(a.name, "Kalabrese");
        assert_eq!(a.album_count, 3);
        assert!(a.starred);
        assert_eq!(a.cover_art.as_deref(), Some("ca-9"));
    }

    #[test]
    fn map_artist_defaults_missing_count_to_zero_and_unstarred() {
        let wire: data::ArtistId3 =
            serde_json::from_str(r#"{ "id": "ar-2", "name": "1300" }"#).unwrap();
        let a = map_artist(wire);
        assert_eq!(a.album_count, 0);
        assert!(!a.starred);
        assert_eq!(a.cover_art, None);
    }

    #[test]
    fn map_album_carries_year_genre_and_song_count() {
        let wire: data::AlbumId3 = serde_json::from_str(
            r#"{ "id": "al-1", "name": "Let Love Rumpel - Part 2",
                 "artist": "Kalabrese", "artistId": "ar-1", "year": 2019,
                 "genre": "Electronic", "songCount": 8 }"#,
        )
        .unwrap();
        let al = map_album(wire);
        assert_eq!(al.id, AlbumId("al-1".into()));
        assert_eq!(al.name, "Let Love Rumpel - Part 2");
        assert_eq!(al.artist, "Kalabrese");
        assert_eq!(al.artist_id, Some(ArtistId("ar-1".into())));
        assert_eq!(al.year, Some(2019));
        assert_eq!(al.genre.as_deref(), Some("Electronic"));
        assert_eq!(al.song_count, 8);
    }

    #[test]
    fn map_album_tolerates_missing_optionals() {
        let wire: data::AlbumId3 =
            serde_json::from_str(r#"{ "id": "al-2", "name": "Untitled" }"#).unwrap();
        let al = map_album(wire);
        assert_eq!(al.artist, ""); // unwrap_or_default
        assert_eq!(al.artist_id, None);
        assert_eq!(al.year, None);
        assert_eq!(al.song_count, 0);
    }

    #[test]
    fn map_song_maps_child_track_duration_and_album_link() {
        let wire: data::Child = serde_json::from_str(
            r#"{ "id": "so-1", "title": "Independent Us", "album": "Let Love Rumpel",
                 "albumId": "al-1", "artist": "Kalabrese", "track": 4,
                 "duration": 372, "coverArt": "ca-1",
                 "starred": "2024-05-01T00:00:00Z", "isDir": false }"#,
        )
        .unwrap();
        let s = map_song(wire);
        assert_eq!(s.id, SongId("so-1".into()));
        assert_eq!(s.title, "Independent Us");
        assert_eq!(s.album.as_deref(), Some("Let Love Rumpel"));
        assert_eq!(s.album_id, Some(AlbumId("al-1".into())));
        assert_eq!(s.artist.as_deref(), Some("Kalabrese"));
        assert_eq!(s.track, Some(4));
        assert_eq!(s.duration_secs, Some(372));
        assert!(s.starred);
    }

    #[test]
    fn i64_to_u32_saturates_and_clamps() {
        assert_eq!(i64_to_u32(-5), 0);
        assert_eq!(i64_to_u32(0), 0);
        assert_eq!(i64_to_u32(42), 42);
        assert_eq!(i64_to_u32(i64::MAX), u32::MAX);
    }
}
