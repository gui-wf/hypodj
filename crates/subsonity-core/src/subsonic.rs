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
use crate::model::{Album, AlbumId, Artist, ArtistId, SongId};
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

/// Saturating i64 -> u32. Negative (never expected for a count) clamps to 0.
fn i64_to_u32(v: i64) -> u32 {
    v.clamp(0, u32::MAX as i64) as u32
}
