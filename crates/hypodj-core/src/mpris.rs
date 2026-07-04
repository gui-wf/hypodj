//! MPRIS (org.mpris.MediaPlayer2) D-Bus surface.
//!
//! Closes the last desktop-parity gap vs mopidy-mpris: exposes now-playing
//! metadata + cover art + two-way media controls to GNOME/KDE/etc over the D-Bus
//! SESSION bus, under the bus name `org.mpris.MediaPlayer2.hypodj`.
//!
//! HARD CONSTRAINT: the bus name is `.hypodj`, deliberately DIFFERENT from
//! mopidy's `.mopidy`, so the two coexist with no conflict on the same session
//! bus. This module never touches audio output - MPRIS is purely the D-Bus
//! metadata/control surface; the actual playback path (and its ao=null headless
//! policy) is entirely the player actor's concern.
//!
//! ## Boundary
//!
//! This stays behind the existing player/handler actor boundary. It holds:
//!   - a [`PlayerHandle`] for the stateless transport controls (play/pause/stop/
//!     seek/volume) - the SAME handle the MPD server drives;
//!   - an `Arc<HypodjHandler>` for queue-aware controls (next/previous) and to
//!     read the current queue item;
//!   - an `Arc<SubsonicClient>` only to BUILD the `mpris:artUrl` getCoverArt URL
//!     (auth baked in like the stream URL); it never fetches bytes here.
//!
//! It subscribes to the handler's single `changed` notification (the same one
//! that wakes MPD `idle`) and re-emits `PropertiesChanged` for PlaybackStatus +
//! Metadata + Volume, so desktops update live. Wire types (mpris_server's
//! Metadata/PlaybackStatus/Time) do not leak: the pure mappers below are the
//! only place model -> wire happens, and they are unit-tested.

use std::sync::Arc;

use mpris_server::{
    zbus::fdo, Metadata, PlaybackStatus, PlayerInterface, Property, RootInterface, Server, Time,
    TrackId,
};

use crate::handler::{CurrentItem, HypodjHandler};
use crate::model::{QueueEntry, Song};
use crate::player::{PlayState, PlayerHandle};
use crate::subsonic::SubsonicClient;

/// The MPRIS implementation object served on the bus. Cheap to hold: three Arc/
/// handle clones.
pub struct HypodjMpris {
    player: PlayerHandle,
    handler: Arc<HypodjHandler>,
    client: Arc<SubsonicClient>,
}

impl HypodjMpris {
    fn playback_status(&self) -> PlaybackStatus {
        state_to_status(self.player.state())
    }

    fn metadata(&self) -> Metadata {
        match self.handler.current_item() {
            Some(item) => item_metadata(&item, &self.client),
            // Nothing playing: an empty-but-valid Metadata (trackid = NO_TRACK).
            None => {
                let mut m = Metadata::new();
                m.set_trackid(Some(TrackId::NO_TRACK));
                m
            }
        }
    }
}

/// Map the player's [`PlayState`] to the MPRIS `PlaybackStatus`. Pure; unit-tested.
pub fn state_to_status(state: PlayState) -> PlaybackStatus {
    match state {
        PlayState::Playing => PlaybackStatus::Playing,
        PlayState::Paused => PlaybackStatus::Paused,
        PlayState::Stopped => PlaybackStatus::Stopped,
    }
}

/// Build an MPRIS object-path `mpris:trackid` for a queue item's MPD id. The
/// path must be a valid D-Bus object path, so we namespace under the hypodj tree.
fn track_id(mpd_id: u64) -> TrackId {
    TrackId::try_from(format!("/blue/skyisnt/hypodj/track/{mpd_id}"))
        .unwrap_or(TrackId::NO_TRACK)
}

/// Map a current queue item to MPRIS Metadata. Pure over (item, client) so it is
/// unit-testable. A library [`Song`] gets full tags + a getCoverArt `mpris:artUrl`
/// (built with auth like the stream URL) when a cover id is known; a raw stream
/// gets only `xesam:title = the URL` and NO art (a live stream has no cover).
pub fn item_metadata(item: &CurrentItem, client: &SubsonicClient) -> Metadata {
    let mut m = Metadata::new();
    m.set_trackid(Some(track_id(item.mpd_id)));
    match &item.entry {
        QueueEntry::Song(song) => song_metadata(&mut m, song, client),
        QueueEntry::Stream { url, title } => {
            // For a raw stream the title is the URL (per the queue model); still
            // set it explicitly and set the xesam:url. No artUrl.
            let shown = if title.is_empty() { url } else { title };
            m.set_title(Some(shown.clone()));
            m.set_url(Some(url.clone()));
        }
    }
    m
}

/// Fill Metadata from a library Song: title/artist/album/length + artUrl.
fn song_metadata(m: &mut Metadata, song: &Song, client: &SubsonicClient) {
    m.set_title(Some(song.title.clone()));
    if let Some(artist) = &song.artist {
        m.set_artist(Some([artist.clone()]));
    }
    if let Some(album) = &song.album {
        m.set_album(Some(album.clone()));
    }
    if let Some(secs) = song.duration_secs {
        m.set_length(Some(Time::from_secs(secs as i64)));
    }
    // mpris:artUrl: the getCoverArt URL for the song's cover id, or the song id
    // itself as a fallback (Navidrome accepts the media id). Auth is baked into
    // the query by the client (same scheme as the stream URL).
    let cover_id = song.cover_art.clone().unwrap_or_else(|| song.id.0.clone());
    if let Ok(url) = client.cover_art_url(&cover_id) {
        m.set_art_url(Some(url.to_string()));
    }
}

/// Spawn the MPRIS server on the session bus. Returns the [`Server`] (which owns
/// the D-Bus connection) plus a driver future that re-emits PropertiesChanged on
/// every handler change. The caller (daemon) spawns the driver and keeps the
/// Server alive for the process lifetime.
///
/// Registers `org.mpris.MediaPlayer2.hypodj`. If there is no session bus (e.g.
/// headless, no `DBUS_SESSION_BUS_ADDRESS`), `Server::new` errors and we bubble
/// it up so the daemon can log-and-skip (never fatal).
pub async fn serve(
    player: PlayerHandle,
    handler: Arc<HypodjHandler>,
    client: Arc<SubsonicClient>,
) -> mpris_server::zbus::Result<Server<HypodjMpris>> {
    let imp = HypodjMpris {
        player,
        handler,
        client,
    };
    Server::new("hypodj", imp).await
}

/// Drive `PropertiesChanged` from the handler's change signal. Loops forever;
/// every wake re-publishes the three properties a desktop cares about. Cheap and
/// conservative (mirrors the MPD `idle` "always report player" policy): the
/// desktop re-reads only what it needs. D-Bus emit errors are logged, never
/// fatal - the daemon and the MPD loop keep running.
pub async fn run_property_updates(server: Server<HypodjMpris>) {
    loop {
        server.imp().handler.changed().await;
        let status = server.imp().playback_status();
        let metadata = server.imp().metadata();
        let volume = server.imp().volume().await.unwrap_or(1.0);
        if let Err(e) = server
            .properties_changed([
                Property::PlaybackStatus(status),
                Property::Metadata(metadata),
                Property::Volume(volume),
            ])
            .await
        {
            tracing::warn!(error = %e, "MPRIS PropertiesChanged emit failed");
        }
    }
}

// ── org.mpris.MediaPlayer2 (root) ───────────────────────────────────────────

impl RootInterface for HypodjMpris {
    async fn raise(&self) -> fdo::Result<()> {
        Ok(())
    }
    async fn quit(&self) -> fdo::Result<()> {
        Ok(())
    }
    async fn can_quit(&self) -> fdo::Result<bool> {
        Ok(false)
    }
    async fn fullscreen(&self) -> fdo::Result<bool> {
        Ok(false)
    }
    async fn set_fullscreen(&self, _: bool) -> mpris_server::zbus::Result<()> {
        Ok(())
    }
    async fn can_set_fullscreen(&self) -> fdo::Result<bool> {
        Ok(false)
    }
    async fn can_raise(&self) -> fdo::Result<bool> {
        Ok(false)
    }
    async fn has_track_list(&self) -> fdo::Result<bool> {
        Ok(false)
    }
    async fn identity(&self) -> fdo::Result<String> {
        Ok("hypodj".to_string())
    }
    async fn desktop_entry(&self) -> fdo::Result<String> {
        // Optional; empty means "no associated .desktop file".
        Ok(String::new())
    }
    async fn supported_uri_schemes(&self) -> fdo::Result<Vec<String>> {
        Ok(vec!["http".into(), "https".into()])
    }
    async fn supported_mime_types(&self) -> fdo::Result<Vec<String>> {
        Ok(vec!["audio/mpeg".into(), "audio/flac".into()])
    }
}

// ── org.mpris.MediaPlayer2.Player ───────────────────────────────────────────

impl PlayerInterface for HypodjMpris {
    async fn next(&self) -> fdo::Result<()> {
        self.handler.mpris_next().await;
        Ok(())
    }
    async fn previous(&self) -> fdo::Result<()> {
        self.handler.mpris_previous().await;
        Ok(())
    }
    async fn pause(&self) -> fdo::Result<()> {
        let _ = self.player.pause().await;
        Ok(())
    }
    async fn play_pause(&self) -> fdo::Result<()> {
        let _ = match self.player.state() {
            PlayState::Playing => self.player.pause().await,
            _ => self.player.resume().await,
        };
        Ok(())
    }
    async fn stop(&self) -> fdo::Result<()> {
        let _ = self.player.stop().await;
        Ok(())
    }
    async fn play(&self) -> fdo::Result<()> {
        let _ = self.player.resume().await;
        Ok(())
    }
    async fn seek(&self, offset: Time) -> fdo::Result<()> {
        // Relative seek: MPRIS Seek is an offset. We have no cheap current
        // position here, so treat the offset as a best-effort absolute nudge from
        // zero when negative-clamped. Absolute positioning is SetPosition.
        let secs = (offset.as_micros() as f64) / 1_000_000.0;
        let _ = self.player.seek(secs.max(0.0)).await;
        Ok(())
    }
    async fn set_position(&self, _track_id: TrackId, position: Time) -> fdo::Result<()> {
        let secs = (position.as_micros() as f64) / 1_000_000.0;
        let _ = self.player.seek(secs.max(0.0)).await;
        Ok(())
    }
    async fn open_uri(&self, _uri: String) -> fdo::Result<()> {
        Ok(())
    }
    async fn playback_status(&self) -> fdo::Result<PlaybackStatus> {
        Ok(HypodjMpris::playback_status(self))
    }
    async fn loop_status(&self) -> fdo::Result<mpris_server::LoopStatus> {
        Ok(mpris_server::LoopStatus::None)
    }
    async fn set_loop_status(&self, _: mpris_server::LoopStatus) -> mpris_server::zbus::Result<()> {
        Ok(())
    }
    async fn rate(&self) -> fdo::Result<f64> {
        Ok(1.0)
    }
    async fn set_rate(&self, _: f64) -> mpris_server::zbus::Result<()> {
        Ok(())
    }
    async fn shuffle(&self) -> fdo::Result<bool> {
        Ok(false)
    }
    async fn set_shuffle(&self, _: bool) -> mpris_server::zbus::Result<()> {
        Ok(())
    }
    async fn metadata(&self) -> fdo::Result<Metadata> {
        Ok(HypodjMpris::metadata(self))
    }
    async fn volume(&self) -> fdo::Result<f64> {
        // MPRIS volume is 0.0..=1.0; our internal volume is 0..=100.
        Ok(self.handler.volume() as f64 / 100.0)
    }
    async fn set_volume(&self, volume: f64) -> mpris_server::zbus::Result<()> {
        let v = (volume.clamp(0.0, 1.0) * 100.0).round() as u8;
        self.handler.mpris_set_volume(v).await;
        Ok(())
    }
    async fn position(&self) -> fdo::Result<Time> {
        // Best-effort: we do not cache the live position here (the player actor
        // owns it and only pushes it out via events). Report zero rather than a
        // stale value; desktops tolerate this.
        Ok(Time::ZERO)
    }
    async fn minimum_rate(&self) -> fdo::Result<f64> {
        Ok(1.0)
    }
    async fn maximum_rate(&self) -> fdo::Result<f64> {
        Ok(1.0)
    }
    async fn can_go_next(&self) -> fdo::Result<bool> {
        Ok(true)
    }
    async fn can_go_previous(&self) -> fdo::Result<bool> {
        Ok(true)
    }
    async fn can_play(&self) -> fdo::Result<bool> {
        Ok(true)
    }
    async fn can_pause(&self) -> fdo::Result<bool> {
        Ok(true)
    }
    async fn can_seek(&self) -> fdo::Result<bool> {
        Ok(true)
    }
    async fn can_control(&self) -> fdo::Result<bool> {
        Ok(true)
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::config::ServerConfig;
    use crate::model::SongId;

    /// Build a non-networked client for the metadata mappers. `connect()` builds
    /// a real reqwest client, which needs system CA certs; a network-isolated
    /// build sandbox has none and the reqwest builder aborts. That is
    /// environmental, not a wiring failure, so return `None` there and the caller
    /// skips (mirrors the existing guard in `subsonic::tests`). In the devshell/CI
    /// with certs this yields a real client and the assertions run.
    fn test_client() -> Option<SubsonicClient> {
        let cfg = ServerConfig {
            url: "http://127.0.0.1:1/never-called".to_string(),
            username: "u".to_string(),
            password: "p".to_string(),
            client_name: "test".to_string(),
        };
        match std::panic::catch_unwind(|| SubsonicClient::connect(&cfg)) {
            Ok(Ok(c)) => Some(c),
            _ => {
                eprintln!("skipping: no CA certs (sandbox); connect() not exercisable here");
                None
            }
        }
    }

    fn a_song() -> Song {
        Song {
            id: SongId("so-1".into()),
            title: "Independent Us".into(),
            album: Some("Let Love Rumpel".into()),
            album_id: None,
            artist: Some("Kalabrese".into()),
            track: Some(4),
            duration_secs: Some(372),
            cover_art: Some("ca-1".into()),
            starred: false,
            musicbrainz_id: None,
            disc: None,
            year: None,
            genre: None,
            bitrate: None,
            comment: None,
            user_rating: None,
        }
    }

    #[test]
    fn state_maps_to_playback_status() {
        assert_eq!(state_to_status(PlayState::Playing), PlaybackStatus::Playing);
        assert_eq!(state_to_status(PlayState::Paused), PlaybackStatus::Paused);
        assert_eq!(state_to_status(PlayState::Stopped), PlaybackStatus::Stopped);
    }

    #[test]
    fn song_metadata_carries_title_artist_album_and_art_url() {
        let Some(client) = test_client() else { return };
        let item = CurrentItem {
            mpd_id: 7,
            entry: QueueEntry::Song(a_song()),
        };
        let m = item_metadata(&item, &client);
        assert_eq!(m.title(), Some("Independent Us"));
        assert_eq!(m.album(), Some("Let Love Rumpel"));
        assert_eq!(m.artist(), Some(vec!["Kalabrese".to_string()]));
        assert_eq!(m.length(), Some(Time::from_secs(372)));
        // trackid present and namespaced under the hypodj tree.
        let tid = m.trackid().expect("trackid present");
        assert!(tid.as_str().contains("/hypodj/track/7"));
        // artUrl is a getCoverArt URL for the cover id, with auth baked in.
        let art = m.art_url().expect("artUrl present for a library song");
        assert!(art.contains("getCoverArt"), "artUrl must be getCoverArt: {art}");
        assert!(art.contains("id=ca-1"), "artUrl must carry the cover id: {art}");
        assert!(art.contains("t=") || art.contains("p="), "artUrl must carry auth: {art}");
    }

    #[test]
    fn song_metadata_falls_back_to_song_id_for_cover() {
        let Some(client) = test_client() else { return };
        let mut song = a_song();
        song.cover_art = None; // no explicit cover id -> fall back to song id
        let item = CurrentItem {
            mpd_id: 1,
            entry: QueueEntry::Song(song),
        };
        let m = item_metadata(&item, &client);
        let art = m.art_url().expect("artUrl present via song-id fallback");
        assert!(art.contains("id=so-1"), "artUrl must fall back to song id: {art}");
    }

    #[test]
    fn raw_stream_metadata_titles_the_url_and_has_no_art() {
        let Some(client) = test_client() else { return };
        let url = "https://stream-mixtape-geo.ntslive.net/mixtape5";
        let item = CurrentItem {
            mpd_id: 3,
            entry: QueueEntry::Stream {
                url: url.to_string(),
                title: url.to_string(),
            },
        };
        let m = item_metadata(&item, &client);
        assert_eq!(m.title(), Some(url));
        assert!(m.art_url().is_none(), "a raw stream must carry no artUrl");
        assert!(m.length().is_none(), "a raw stream has unknown length");
    }
}
