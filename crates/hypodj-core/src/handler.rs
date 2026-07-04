//! The concrete [`MpdHandler`] backing the MPD server with a live Subsonic
//! library + the audio player.
//!
//! Phase 2. This is where MPD command semantics meet OpenSubsonic browse/search
//! and the player actor. State that MPD treats as global (the play queue, the
//! current-song pointer, the volume) lives here behind a `Mutex`, because MPD
//! state is shared across all client connections (see [`MpdHandler`] docs) - the
//! handler is `Arc`-shared and every method takes `&self`.
//!
//! ## URI scheme
//!
//! MPD is path-based; Subsonic is id-based. We bridge them with synthetic URIs:
//!   - `song/<songId>`      - a playable track (what lands in the queue)
//!   - `album/<albumId>`    - an album "directory"
//!   - `artist/<artistId>`  - an artist "directory"
//! The root `lsinfo` lists artist directories; drilling into an artist lists its
//! album directories; drilling into an album lists its song files. `add song/X`
//! / `addid song/X` queue a real track; `play` streams it via the player.

use std::sync::Arc;
use std::sync::Mutex;
use std::time::Duration;

use opensubsonic::AlbumListType;
use tokio::sync::Notify;

use crate::cache::TtlLru;
use crate::model::{AlbumId, ArtistId, Genre, Song, SongId};
use crate::mpd::{MpdCommand, MpdHandler, MpdResponse};
use crate::player::{PlayState, PlayerHandle};
use crate::subsonic::{list_type_from_dirname, SubsonicClient};

/// One queue entry: a resolved song plus its MPD song id (a monotonically
/// increasing integer, MPD's stable per-song handle, distinct from queue pos).
#[derive(Clone)]
struct QueueItem {
    id: u64,
    song: Song,
}

struct State {
    queue: Vec<QueueItem>,
    next_id: u64,
    /// Index into `queue` of the current song, if any.
    current: Option<usize>,
    volume: u8,
    /// Bumped whenever the queue changes (MPD "playlist version").
    playlist_version: u64,
    /// Client-negotiated binary chunk size (ncmpcpp sends `binarylimit`). MPD is
    /// single-stream and this daemon is local single-client, so a shared value
    /// is correct; default 8192.
    binary_limit: usize,
    /// The ordering of the last `listplaylistinfo Starred` response, so a
    /// position-based `playlistdelete Starred <pos>` can map back to a song id
    /// for unstar (MPD playlist deletes are position-based, not uri-based).
    last_starred_order: Vec<SongId>,
}

impl Default for State {
    fn default() -> Self {
        Self {
            queue: Vec::new(),
            next_id: 0,
            current: None,
            volume: 100,
            playlist_version: 0,
            binary_limit: 8192,
            last_starred_order: Vec::new(),
        }
    }
}

pub struct HypodjHandler {
    client: Arc<SubsonicClient>,
    player: PlayerHandle,
    state: Mutex<State>,
    /// Fired when a subsystem changes, to wake `idle`.
    changed: Notify,
    /// Bounded LRU+TTL cache for STABLE listings (artists, albums, genres, smart
    /// lists, similar/top). NEVER holds its lock across an `.await` (see cache
    /// docs): get -> await refill -> put, two separate lock scopes.
    listings: TtlLru<String, Vec<Song>>,
    /// Cache for stable album/artist directory listings (name-bearing rows).
    dir_cache: TtlLru<String, Vec<(String, String)>>,
    /// Decoded cover-art bytes, keyed by cover id. Big win: ncmpcpp requests
    /// albumart in many small offset chunks; caching avoids re-fetching the whole
    /// image per chunk. Longer TTL (art rarely changes).
    cover_cache: TtlLru<String, Vec<u8>>,
}

impl HypodjHandler {
    pub fn new(client: Arc<SubsonicClient>, player: PlayerHandle) -> Self {
        Self {
            client,
            player,
            state: Mutex::new(State::default()),
            changed: Notify::new(),
            listings: TtlLru::new(256, Duration::from_secs(60)),
            dir_cache: TtlLru::new(256, Duration::from_secs(60)),
            cover_cache: TtlLru::new(64, Duration::from_secs(600)),
        }
    }

    /// Shared client handle, so the daemon can also hand it to the scrobbler.
    pub fn client(&self) -> Arc<SubsonicClient> {
        self.client.clone()
    }

    fn notify_change(&self) {
        self.changed.notify_waiters();
    }

    /// Called by the daemon when the player reports a natural EOF: advance to the
    /// next queue entry, or leave the state stopped at the end of the queue.
    pub async fn advance_on_eof(&self) {
        let next = {
            let st = self.state.lock().unwrap();
            st.current.map(|c| c + 1).filter(|&i| i < st.queue.len())
        };
        match next {
            Some(idx) => {
                let _ = self.play_index(idx).await;
            }
            None => {
                self.state.lock().unwrap().current = None;
                self.notify_change();
            }
        }
    }

    /// Resolve and start playing the queue item at `idx`. Returns an ACK-style
    /// error string on failure.
    async fn play_index(&self, idx: usize) -> Result<(), String> {
        let item = {
            let st = self.state.lock().unwrap();
            st.queue.get(idx).cloned()
        };
        let item = match item {
            Some(i) => i,
            None => return Err("Bad song index".into()),
        };
        let url = self
            .client
            .stream_url(&item.song.id)
            .map_err(|e| e.to_string())?;
        self.player
            .play_url(item.song.id.clone(), url.as_str())
            .await
            .map_err(|e| e.to_string())?;
        {
            let mut st = self.state.lock().unwrap();
            st.current = Some(idx);
        }
        self.notify_change();
        Ok(())
    }

    /// Add a song by uri (`song/<id>`), resolving its metadata. Returns the
    /// assigned MPD song id.
    async fn enqueue_uri(&self, uri: &str) -> Result<u64, String> {
        let song_id = uri
            .strip_prefix("song/")
            .ok_or_else(|| format!("unsupported uri: {uri}"))?;
        let song = self
            .client
            .song(&SongId(song_id.to_string()))
            .await
            .map_err(|e| e.to_string())?;
        let mut st = self.state.lock().unwrap();
        let id = st.next_id;
        st.next_id += 1;
        st.queue.push(QueueItem { id, song });
        st.playlist_version += 1;
        drop(st);
        self.notify_change();
        Ok(id)
    }
}

/// Serialize one queued song as MPD `playlistinfo`/`currentsong` pairs.
fn song_pairs(item: &QueueItem, pos: usize) -> Vec<(String, String)> {
    let s = &item.song;
    let mut p = vec![
        ("file".to_string(), format!("song/{}", s.id.0)),
        ("Title".to_string(), s.title.clone()),
    ];
    push_song_tags(&mut p, s);
    p.push(("Pos".to_string(), pos.to_string()));
    p.push(("Id".to_string(), item.id.to_string()));
    p
}

fn ack(code: u32, command: &str, message: &str) -> MpdResponse {
    MpdResponse::Ack {
        code,
        command: command.to_string(),
        message: message.to_string(),
    }
}

// ACK error codes (subset of MPD's ack.h).
const ACK_ERROR_NO_EXIST: u32 = 50;
const ACK_ERROR_UNKNOWN: u32 = 5;

impl MpdHandler for HypodjHandler {
    async fn idle(&self, _subsystems: Vec<String>) -> Option<String> {
        // Minimal-correct: wait for any change notification, report "player".
        // (ncmpcpp re-issues idle after each, and re-reads status/currentsong.)
        self.changed.notified().await;
        Some("player".to_string())
    }

    async fn handle(&self, cmd: MpdCommand) -> MpdResponse {
        match cmd {
            // ── status / metadata ──────────────────────────────────────────
            MpdCommand::Ping => MpdResponse::ok(),

            MpdCommand::Status => {
                let (state, vol, qlen, cur, ver) = {
                    let st = self.state.lock().unwrap();
                    (
                        self.player.state(),
                        st.volume,
                        st.queue.len(),
                        st.current,
                        st.playlist_version,
                    )
                };
                let state_str = match state {
                    PlayState::Playing => "play",
                    PlayState::Paused => "pause",
                    PlayState::Stopped => "stop",
                };
                let mut b = MpdResponse::pairs()
                    .pair("volume", vol.to_string())
                    .pair("repeat", "0")
                    .pair("random", "0")
                    .pair("single", "0")
                    .pair("consume", "0")
                    .pair("playlist", ver.to_string())
                    .pair("playlistlength", qlen.to_string())
                    .pair("state", state_str);
                if let Some(idx) = cur {
                    let st = self.state.lock().unwrap();
                    if let Some(item) = st.queue.get(idx) {
                        b = b
                            .pair("song", idx.to_string())
                            .pair("songid", item.id.to_string());
                        if let Some(d) = item.song.duration_secs {
                            b = b.pair("duration", format!("{d}.000"));
                        }
                    }
                }
                b.build()
            }

            MpdCommand::Stats => {
                // Cheap, honest stats: queue-derived counts (a full library scan
                // would be a Subsonic getScanStatus call - TODO for fidelity).
                let songs = self.state.lock().unwrap().queue.len();
                MpdResponse::pairs()
                    .pair("artists", "0")
                    .pair("albums", "0")
                    .pair("songs", songs.to_string())
                    .pair("uptime", "0")
                    .pair("playtime", "0")
                    .pair("db_playtime", "0")
                    .pair("db_update", "0")
                    .build()
            }

            MpdCommand::CurrentSong => {
                let st = self.state.lock().unwrap();
                match st.current.and_then(|i| st.queue.get(i).map(|it| (i, it))) {
                    Some((pos, item)) => MpdResponse::Pairs(song_pairs(item, pos)),
                    None => MpdResponse::ok(),
                }
            }

            MpdCommand::Idle(_) | MpdCommand::NoIdle => {
                // Handled entirely in the serve loop; never dispatched here.
                MpdResponse::ok()
            }

            // ── playback ──────────────────────────────────────────────────
            MpdCommand::Play(pos) => {
                let idx = pos.unwrap_or_else(|| {
                    self.state.lock().unwrap().current.unwrap_or(0)
                });
                // If already have a current and no explicit pos, resume.
                match self.play_index(idx).await {
                    Ok(()) => MpdResponse::ok(),
                    Err(e) => ack(ACK_ERROR_NO_EXIST, "play", &e),
                }
            }
            MpdCommand::PlayId(id) => {
                let idx = match id {
                    Some(id) => self
                        .state
                        .lock()
                        .unwrap()
                        .queue
                        .iter()
                        .position(|it| it.id == id),
                    None => Some(0),
                };
                match idx {
                    Some(idx) => match self.play_index(idx).await {
                        Ok(()) => MpdResponse::ok(),
                        Err(e) => ack(ACK_ERROR_NO_EXIST, "playid", &e),
                    },
                    None => ack(ACK_ERROR_NO_EXIST, "playid", "No such song"),
                }
            }
            MpdCommand::Pause(want) => {
                let res = match want {
                    Some(true) => self.player.pause().await,
                    Some(false) => self.player.resume().await,
                    None => match self.player.state() {
                        PlayState::Playing => self.player.pause().await,
                        _ => self.player.resume().await,
                    },
                };
                self.notify_change();
                match res {
                    Ok(()) => MpdResponse::ok(),
                    Err(e) => ack(ACK_ERROR_UNKNOWN, "pause", &e.to_string()),
                }
            }
            MpdCommand::Stop => {
                let _ = self.player.stop().await;
                self.notify_change();
                MpdResponse::ok()
            }
            MpdCommand::Next => {
                let next = {
                    let st = self.state.lock().unwrap();
                    st.current.map(|c| c + 1).filter(|&i| i < st.queue.len())
                };
                match next {
                    Some(idx) => match self.play_index(idx).await {
                        Ok(()) => MpdResponse::ok(),
                        Err(e) => ack(ACK_ERROR_NO_EXIST, "next", &e),
                    },
                    None => MpdResponse::ok(),
                }
            }
            MpdCommand::Previous => {
                let prev = {
                    let st = self.state.lock().unwrap();
                    st.current.and_then(|c| c.checked_sub(1))
                };
                match prev {
                    Some(idx) => match self.play_index(idx).await {
                        Ok(()) => MpdResponse::ok(),
                        Err(e) => ack(ACK_ERROR_NO_EXIST, "previous", &e),
                    },
                    None => MpdResponse::ok(),
                }
            }
            MpdCommand::Seek { secs, .. } | MpdCommand::SeekCur(secs) => {
                match self.player.seek(secs).await {
                    Ok(()) => MpdResponse::ok(),
                    Err(e) => ack(ACK_ERROR_UNKNOWN, "seek", &e.to_string()),
                }
            }
            MpdCommand::SeekId { secs, .. } => match self.player.seek(secs).await {
                Ok(()) => MpdResponse::ok(),
                Err(e) => ack(ACK_ERROR_UNKNOWN, "seekid", &e.to_string()),
            },
            MpdCommand::SetVol(v) => {
                let v = v.min(100);
                {
                    self.state.lock().unwrap().volume = v;
                }
                let _ = self.player.set_volume(v).await;
                self.notify_change();
                MpdResponse::ok()
            }
            MpdCommand::GetVol => {
                let v = self.state.lock().unwrap().volume;
                MpdResponse::pairs().pair("volume", v.to_string()).build()
            }

            // ── queue ─────────────────────────────────────────────────────
            MpdCommand::Add(uri) => match self.enqueue_uri(&uri).await {
                Ok(_) => MpdResponse::ok(),
                Err(e) => ack(ACK_ERROR_NO_EXIST, "add", &e),
            },
            MpdCommand::AddId(uri, _pos) => match self.enqueue_uri(&uri).await {
                Ok(id) => MpdResponse::pairs().pair("Id", id.to_string()).build(),
                Err(e) => ack(ACK_ERROR_NO_EXIST, "addid", &e),
            },
            MpdCommand::Clear => {
                {
                    let mut st = self.state.lock().unwrap();
                    st.queue.clear();
                    st.current = None;
                    st.playlist_version += 1;
                }
                let _ = self.player.stop().await;
                self.notify_change();
                MpdResponse::ok()
            }
            MpdCommand::Delete(spec) => {
                let mut st = self.state.lock().unwrap();
                if let Some(pos) = spec.and_then(|s| s.split(':').next().and_then(|p| p.parse::<usize>().ok())) {
                    if pos < st.queue.len() {
                        st.queue.remove(pos);
                        st.playlist_version += 1;
                        if let Some(c) = st.current {
                            if c == pos {
                                st.current = None;
                            } else if c > pos {
                                st.current = Some(c - 1);
                            }
                        }
                    }
                }
                drop(st);
                self.notify_change();
                MpdResponse::ok()
            }
            MpdCommand::PlaylistInfo(_) => {
                let st = self.state.lock().unwrap();
                let mut pairs = Vec::new();
                for (pos, item) in st.queue.iter().enumerate() {
                    pairs.extend(song_pairs(item, pos));
                }
                MpdResponse::Pairs(pairs)
            }
            MpdCommand::PlaylistId(id) => {
                let st = self.state.lock().unwrap();
                let mut pairs = Vec::new();
                for (pos, item) in st.queue.iter().enumerate() {
                    if id.is_none() || id == Some(item.id) {
                        pairs.extend(song_pairs(item, pos));
                    }
                }
                MpdResponse::Pairs(pairs)
            }
            MpdCommand::PlChanges(_) => {
                // Full queue (a correct superset of the diff; ncmpcpp re-reads).
                let st = self.state.lock().unwrap();
                let mut pairs = Vec::new();
                for (pos, item) in st.queue.iter().enumerate() {
                    pairs.extend(song_pairs(item, pos));
                }
                MpdResponse::Pairs(pairs)
            }

            // ── stored playlists + star trigger (feature 3) ─────────────────
            MpdCommand::ListPlaylists => {
                // Advertise the synthetic `Starred` playlist (the star trigger).
                MpdResponse::pairs()
                    .pair("playlist", "Starred")
                    .pair("Last-Modified", "1970-01-01T00:00:00Z")
                    .build()
            }
            MpdCommand::ListPlaylistInfo(name) | MpdCommand::Load(name)
                if name == "Starred" =>
            {
                // Starred is NEVER cached (freshness-critical). Record the order
                // so a later position-based playlistdelete maps to a song id.
                match self.client.starred_songs().await {
                    Ok(songs) => {
                        {
                            let mut st = self.state.lock().unwrap();
                            st.last_starred_order =
                                songs.iter().map(|s| s.id.clone()).collect();
                        }
                        let mut pairs = Vec::new();
                        for s in &songs {
                            pairs.extend(browse_song_pairs(s));
                        }
                        MpdResponse::Pairs(pairs)
                    }
                    Err(e) => ack(ACK_ERROR_UNKNOWN, "listplaylistinfo", &e.to_string()),
                }
            }
            MpdCommand::ListPlaylistInfo(_) | MpdCommand::Load(_) => MpdResponse::ok(),
            MpdCommand::PlaylistAdd(name, uri) if name == "Starred" => {
                // `playlistadd Starred song/<id>` -> star the song server-side.
                match song_id_from_uri(&uri) {
                    Some(id) => match self.client.star_song(&id).await {
                        Ok(()) => {
                            self.bust_star_caches();
                            self.notify_change();
                            MpdResponse::ok()
                        }
                        Err(e) => ack(ACK_ERROR_UNKNOWN, "playlistadd", &e.to_string()),
                    },
                    None => ack(ACK_ERROR_NO_EXIST, "playlistadd", "unsupported uri"),
                }
            }
            MpdCommand::PlaylistAdd(..) => MpdResponse::ok(),
            MpdCommand::PlaylistDelete(name, pos) if name == "Starred" => {
                // Position-based: map to the song id from the last listed order.
                let target = {
                    let st = self.state.lock().unwrap();
                    st.last_starred_order.get(pos).cloned()
                };
                match target {
                    Some(id) => match self.client.unstar_song(&id).await {
                        Ok(()) => {
                            self.bust_star_caches();
                            self.notify_change();
                            MpdResponse::ok()
                        }
                        Err(e) => ack(ACK_ERROR_UNKNOWN, "playlistdelete", &e.to_string()),
                    },
                    None => ack(ACK_ERROR_NO_EXIST, "playlistdelete", "Bad song index"),
                }
            }
            MpdCommand::PlaylistDelete(..) => MpdResponse::ok(),
            MpdCommand::PlaylistClear(_) => MpdResponse::ok(),

            // ── db browse ──────────────────────────────────────────────────
            MpdCommand::LsInfo(path) => self.lsinfo(path.as_deref()).await,
            MpdCommand::ListAllInfo(path) => self.lsinfo(path.as_deref()).await,

            MpdCommand::Find(filters) => self.search_filtered(filters, true).await,
            MpdCommand::Search(filters) => self.search_filtered(filters, false).await,

            MpdCommand::List(spec) => {
                // `list <tag>`: support Artist, Album, Genre.
                let tag = spec.split_whitespace().next().unwrap_or("").to_lowercase();
                match tag.as_str() {
                    "artist" | "albumartist" => match self.client.artists().await {
                        Ok(artists) => {
                            let pairs = artists
                                .into_iter()
                                .map(|a| ("Artist".to_string(), a.name))
                                .collect();
                            MpdResponse::Pairs(pairs)
                        }
                        Err(e) => ack(ACK_ERROR_UNKNOWN, "list", &e.to_string()),
                    },
                    "album" => match self.client.album_list(AlbumListType::AlphabeticalByName, Some(500)).await {
                        Ok(albums) => {
                            let pairs = albums
                                .into_iter()
                                .map(|a| ("Album".to_string(), a.name))
                                .collect();
                            MpdResponse::Pairs(pairs)
                        }
                        Err(e) => ack(ACK_ERROR_UNKNOWN, "list", &e.to_string()),
                    },
                    "genre" => match self.genres().await {
                        Ok(genres) => {
                            let pairs = genres
                                .into_iter()
                                .map(|g| ("Genre".to_string(), g.name))
                                .collect();
                            MpdResponse::Pairs(pairs)
                        }
                        Err(e) => ack(ACK_ERROR_UNKNOWN, "list", &e.to_string()),
                    },
                    _ => MpdResponse::ok(),
                }
            }

            // ── binary cover art (feature 2) ────────────────────────────────
            MpdCommand::AlbumArt(uri, offset) | MpdCommand::ReadPicture(uri, offset) => {
                self.albumart(&uri, offset).await
            }
            MpdCommand::BinaryLimit(n) => {
                // Honor the client's negotiated chunk size (min 64 to stay sane).
                self.state.lock().unwrap().binary_limit = n.max(64);
                MpdResponse::ok()
            }

            // ── capability probes ──────────────────────────────────────────
            MpdCommand::Commands => {
                let cmds = [
                    "add", "addid", "albumart", "binarylimit", "clear",
                    "commands", "currentsong", "delete", "find", "getvol", "idle",
                    "list", "listall", "listallinfo", "listplaylistinfo",
                    "listplaylists", "load", "lsinfo", "next", "noidle",
                    "notcommands", "outputs", "pause", "ping", "play", "playid",
                    "playlistadd", "playlistclear", "playlistdelete", "playlistid",
                    "playlistinfo", "plchanges", "previous", "readpicture",
                    "search", "seek", "seekcur", "seekid", "setvol", "stats",
                    "status", "stop", "tagtypes", "urlhandlers",
                ];
                let pairs = cmds
                    .iter()
                    .map(|c| ("command".to_string(), c.to_string()))
                    .collect();
                MpdResponse::Pairs(pairs)
            }
            MpdCommand::NotCommands => MpdResponse::ok(),
            MpdCommand::TagTypes => {
                let tags = [
                    "Artist", "Album", "Title", "Track", "Genre", "Date", "Disc",
                    "MUSICBRAINZ_TRACKID", "Comment",
                ];
                let pairs = tags
                    .iter()
                    .map(|t| ("tagtype".to_string(), t.to_string()))
                    .collect();
                MpdResponse::Pairs(pairs)
            }
            MpdCommand::Outputs => MpdResponse::pairs()
                .pair("outputid", "0")
                .pair("outputname", "hypodj")
                .pair("outputenabled", "1")
                .build(),
            MpdCommand::Decoders => MpdResponse::ok(),
            MpdCommand::UrlHandlers => MpdResponse::ok(),

            MpdCommand::Unsupported(name) => {
                ack(ACK_ERROR_UNKNOWN, &name, &format!("unknown command \"{name}\""))
            }
        }
    }
}

impl HypodjHandler {
    /// Back `lsinfo` / `listallinfo`. The root lists the artist directories PLUS
    /// the synthetic top-level browse dirs (Genres/Lists/Radio/Starred). Drilling
    /// into each dispatches to the feature that backs it.
    async fn lsinfo(&self, path: Option<&str>) -> MpdResponse {
        match path {
            None | Some("") | Some("/") => self.lsinfo_root().await,

            // ── artist/album drill-down (cached) ────────────────────────────
            Some(p) if p.starts_with("artist/") => {
                let id = p.trim_start_matches("artist/").to_string();
                let key = format!("artist/{id}");
                if let Some(pairs) = self.dir_cache.get(&key) {
                    return MpdResponse::Pairs(pairs);
                }
                match self.client.artist_albums(&ArtistId(id)).await {
                    Ok(albums) => {
                        let mut pairs = Vec::new();
                        for al in &albums {
                            pairs.push(("directory".to_string(), format!("album/{}", al.id.0)));
                            pairs.push(("Album".to_string(), al.name.clone()));
                        }
                        self.dir_cache.put(key, pairs.clone());
                        MpdResponse::Pairs(pairs)
                    }
                    Err(e) => ack(ACK_ERROR_UNKNOWN, "lsinfo", &e.to_string()),
                }
            }
            Some(p) if p.starts_with("album/") => {
                let id = p.trim_start_matches("album/").to_string();
                let key = format!("album/{id}");
                if let Some(songs) = self.listings.get(&key) {
                    return song_rows(&songs);
                }
                match self.client.album_songs(&AlbumId(id)).await {
                    Ok(songs) => {
                        self.listings.put(key, songs.clone());
                        song_rows(&songs)
                    }
                    Err(e) => ack(ACK_ERROR_UNKNOWN, "lsinfo", &e.to_string()),
                }
            }

            // ── Genres (feature 6) ──────────────────────────────────────────
            Some("Genres") => match self.genres().await {
                Ok(genres) => {
                    let mut pairs = Vec::new();
                    for g in &genres {
                        pairs.push(("directory".to_string(), format!("genre/{}", g.name)));
                    }
                    MpdResponse::Pairs(pairs)
                }
                Err(e) => ack(ACK_ERROR_UNKNOWN, "lsinfo", &e.to_string()),
            },
            Some(p) if p.starts_with("genre/") => {
                let name = p.trim_start_matches("genre/").to_string();
                let key = format!("genre/{name}");
                if let Some(songs) = self.listings.get(&key) {
                    return song_rows(&songs);
                }
                match self.client.songs_by_genre(&name).await {
                    Ok(songs) => {
                        self.listings.put(key, songs.clone());
                        song_rows(&songs)
                    }
                    Err(e) => ack(ACK_ERROR_UNKNOWN, "lsinfo", &e.to_string()),
                }
            }

            // ── Lists: smart album lists (feature 5) ────────────────────────
            Some("Lists") => {
                let mut pairs = Vec::new();
                for t in ["frequent", "newest", "recent", "highest", "random"] {
                    pairs.push(("directory".to_string(), format!("list/{t}")));
                }
                MpdResponse::Pairs(pairs)
            }
            Some(p) if p.starts_with("list/") => {
                let name = p.trim_start_matches("list/");
                match list_type_from_dirname(name) {
                    Some(list_type) => {
                        // `random` smart list must stay fresh; others cache.
                        let cached = if name == "random" {
                            None
                        } else {
                            self.dir_cache.get(&format!("list/{name}"))
                        };
                        if let Some(pairs) = cached {
                            return MpdResponse::Pairs(pairs);
                        }
                        match self.client.album_list(list_type, Some(100)).await {
                            Ok(albums) => {
                                let mut pairs = Vec::new();
                                for al in &albums {
                                    pairs.push((
                                        "directory".to_string(),
                                        format!("album/{}", al.id.0),
                                    ));
                                    pairs.push(("Album".to_string(), al.name.clone()));
                                }
                                if name != "random" {
                                    self.dir_cache.put(format!("list/{name}"), pairs.clone());
                                }
                                MpdResponse::Pairs(pairs)
                            }
                            Err(e) => ack(ACK_ERROR_UNKNOWN, "lsinfo", &e.to_string()),
                        }
                    }
                    None => MpdResponse::ok(),
                }
            }

            // ── Radio: random / similar / top (feature 4) ───────────────────
            Some("Radio") => {
                // random is always reachable; similar/top are seeded per song or
                // artist from a browse path (radio/similar/<songId>,
                // radio/top/<artist>). We advertise the random entry plus a hint.
                MpdResponse::pairs()
                    .pair("directory", "radio/random")
                    .build()
            }
            Some("radio/random") => {
                // NEVER cached: randomness is the whole point.
                match self.client.random_songs(Some(50)).await {
                    Ok(songs) => song_rows(&songs),
                    Err(e) => ack(ACK_ERROR_UNKNOWN, "lsinfo", &e.to_string()),
                }
            }
            Some(p) if p.starts_with("radio/similar/") => {
                let id = p.trim_start_matches("radio/similar/").to_string();
                let key = format!("similar/{id}");
                if let Some(songs) = self.listings.get(&key) {
                    return song_rows(&songs);
                }
                match self.client.similar_songs(&SongId(id), Some(50)).await {
                    Ok(songs) => {
                        self.listings.put(key, songs.clone());
                        song_rows(&songs)
                    }
                    Err(e) => ack(ACK_ERROR_UNKNOWN, "lsinfo", &e.to_string()),
                }
            }
            Some(p) if p.starts_with("radio/top/") => {
                let artist = p.trim_start_matches("radio/top/").to_string();
                let key = format!("top/{artist}");
                if let Some(songs) = self.listings.get(&key) {
                    return song_rows(&songs);
                }
                match self.client.top_songs(&artist, Some(50)).await {
                    Ok(songs) => {
                        self.listings.put(key, songs.clone());
                        song_rows(&songs)
                    }
                    Err(e) => ack(ACK_ERROR_UNKNOWN, "lsinfo", &e.to_string()),
                }
            }

            // ── Starred (feature 3) - NEVER cached (freshness) ──────────────
            Some("Starred") => match self.client.starred_songs().await {
                Ok(songs) => {
                    {
                        let mut st = self.state.lock().unwrap();
                        st.last_starred_order = songs.iter().map(|s| s.id.clone()).collect();
                    }
                    song_rows(&songs)
                }
                Err(e) => ack(ACK_ERROR_UNKNOWN, "lsinfo", &e.to_string()),
            },

            Some(_) => MpdResponse::ok(),
        }
    }

    /// The root browse view: synthetic top-level dirs + artist dirs (cached).
    async fn lsinfo_root(&self) -> MpdResponse {
        let mut pairs = Vec::new();
        // Synthetic feature dirs first so they sit at the top of ncmpcpp Browse.
        for d in ["Genres", "Lists", "Radio", "Starred"] {
            pairs.push(("directory".to_string(), d.to_string()));
        }
        if let Some(cached) = self.dir_cache.get(&"artists".to_string()) {
            pairs.extend(cached);
            return MpdResponse::Pairs(pairs);
        }
        match self.client.artists().await {
            Ok(artists) => {
                let mut artist_rows = Vec::new();
                for a in &artists {
                    artist_rows
                        .push(("directory".to_string(), format!("artist/{}", a.id.0)));
                    artist_rows.push(("Artist".to_string(), a.name.clone()));
                }
                self.dir_cache.put("artists".to_string(), artist_rows.clone());
                pairs.extend(artist_rows);
                MpdResponse::Pairs(pairs)
            }
            Err(e) => ack(ACK_ERROR_UNKNOWN, "lsinfo", &e.to_string()),
        }
    }

    /// Genres list, cached in a dedicated slot (stable, benefits from reuse).
    async fn genres(&self) -> Result<Vec<Genre>, crate::subsonic::SubsonicError> {
        // Genres are cheap + stable; cache the resolved names via dir_cache is
        // awkward (different value type), so re-fetch is acceptable, but we keep
        // a tiny cache by reusing the client each call. Left uncached here for
        // simplicity - genres change rarely and the call is cheap.
        self.client.genres().await
    }

    /// Resolve + serve one binary cover-art chunk for `song/<id>` (feature 2).
    /// Resolve chain: song/<id> -> Song.cover_art (or fall back to the song id
    /// itself, which Navidrome accepts) -> cover bytes (cached) -> slice
    /// [offset..offset+binary_limit], clamping the final chunk.
    async fn albumart(&self, uri: &str, offset: usize) -> MpdResponse {
        let song_id = match song_id_from_uri(uri) {
            Some(id) => id,
            None => return ack(ACK_ERROR_NO_EXIST, "albumart", "No file exists"),
        };
        // Resolve the cover id: prefer the song's coverArt, else the song id.
        let cover_id = match self.client.song(&song_id).await {
            Ok(song) => song.cover_art.unwrap_or_else(|| song_id.0.clone()),
            // If we can't resolve the song, still try the id directly.
            Err(_) => song_id.0.clone(),
        };
        // Fetch (cached) the full image bytes.
        let bytes = match self.cover_cache.get(&format!("cover/{cover_id}")) {
            Some(b) => b,
            None => match self.client.cover_art(&cover_id).await {
                Ok(b) if !b.is_empty() => {
                    self.cover_cache.put(format!("cover/{cover_id}"), b.clone());
                    b
                }
                // Empty or errored: gracefully ACK no-exist (never panic).
                _ => return ack(ACK_ERROR_NO_EXIST, "albumart", "No file exists"),
            },
        };
        let total = bytes.len();
        if offset >= total {
            return ack(ACK_ERROR_NO_EXIST, "albumart", "Bad file offset");
        }
        let limit = self.state.lock().unwrap().binary_limit;
        let end = (offset + limit).min(total);
        let chunk = bytes[offset..end].to_vec();
        MpdResponse::Binary { total, chunk }
    }

    /// Full search3 with client-side MPD-tag post-filtering (feature 7). `exact`
    /// (find) matches equality on tags; otherwise (search) case-insensitive
    /// substring. search3 is full-text only, so this filter recovers precision.
    async fn search_filtered(&self, filters: Vec<(String, String)>, exact: bool) -> MpdResponse {
        if filters.is_empty() {
            return MpdResponse::ok();
        }
        // Build the full-text query from all values (search3 is full-text).
        let query = filters
            .iter()
            .map(|(_, v)| v.as_str())
            .collect::<Vec<_>>()
            .join(" ");
        // search3 results are query-specific + ephemeral -> NEVER cached.
        let hits = match self.client.search3(&query).await {
            Ok(h) => h,
            Err(e) => return ack(ACK_ERROR_UNKNOWN, "search", &e.to_string()),
        };
        let matches = |song: &Song| -> bool {
            filters.iter().all(|(tag, val)| tag_matches(song, tag, val, exact))
        };
        let mut pairs = Vec::new();
        for s in hits.songs.iter().filter(|s| matches(s)) {
            pairs.extend(browse_song_pairs(s));
        }
        MpdResponse::Pairs(pairs)
    }

    /// Invalidate cached listings whose starred flag could change after a star.
    fn bust_star_caches(&self) {
        self.dir_cache.invalidate_prefix("album/");
        self.dir_cache.invalidate(&"artists".to_string());
        self.listings.invalidate_prefix("album/");
    }
}

/// Does `song` satisfy the `tag == / contains val` filter? `exact` picks
/// equality vs case-insensitive substring. `any` matches title/artist/album.
fn tag_matches(song: &Song, tag: &str, val: &str, exact: bool) -> bool {
    let cmp = |field: &str| -> bool {
        if exact {
            field == val
        } else {
            field.to_lowercase().contains(&val.to_lowercase())
        }
    };
    match tag {
        "title" => cmp(&song.title),
        "artist" | "albumartist" => song.artist.as_deref().map(cmp).unwrap_or(false),
        "album" => song.album.as_deref().map(cmp).unwrap_or(false),
        "genre" => song.genre.as_deref().map(cmp).unwrap_or(false),
        "any" => {
            cmp(&song.title)
                || song.artist.as_deref().map(cmp).unwrap_or(false)
                || song.album.as_deref().map(cmp).unwrap_or(false)
        }
        // Unknown/unsupported tag: don't exclude (search3 already matched text).
        _ => true,
    }
}

/// Parse a `song/<id>` uri into a `SongId`.
fn song_id_from_uri(uri: &str) -> Option<SongId> {
    uri.strip_prefix("song/").map(|s| SongId(s.to_string()))
}

/// Serialize a slice of songs as browse `file:` rows.
fn song_rows(songs: &[Song]) -> MpdResponse {
    let mut pairs = Vec::new();
    for s in songs {
        pairs.extend(browse_song_pairs(s));
    }
    MpdResponse::Pairs(pairs)
}

/// Serialize a `Song` as a browse `file:` entry (no queue Pos/Id), including the
/// richer metadata tags (feature 7) when present. ncmpcpp reads these directly.
fn browse_song_pairs(s: &Song) -> Vec<(String, String)> {
    let mut p = vec![
        ("file".to_string(), format!("song/{}", s.id.0)),
        ("Title".to_string(), s.title.clone()),
    ];
    push_song_tags(&mut p, s);
    p
}

/// Append the common + richer tags for a song (shared by browse + queue rows).
fn push_song_tags(p: &mut Vec<(String, String)>, s: &Song) {
    if let Some(a) = &s.artist {
        p.push(("Artist".to_string(), a.clone()));
    }
    if let Some(a) = &s.album {
        p.push(("Album".to_string(), a.clone()));
    }
    if let Some(t) = s.track {
        p.push(("Track".to_string(), t.to_string()));
    }
    if let Some(dn) = s.disc {
        p.push(("Disc".to_string(), dn.to_string()));
    }
    if let Some(y) = s.year {
        p.push(("Date".to_string(), y.to_string()));
    }
    if let Some(g) = &s.genre {
        p.push(("Genre".to_string(), g.clone()));
    }
    if let Some(mb) = &s.musicbrainz_id {
        p.push(("MUSICBRAINZ_TRACKID".to_string(), mb.clone()));
    }
    if let Some(c) = &s.comment {
        p.push(("Comment".to_string(), c.clone()));
    }
    if let Some(br) = s.bitrate {
        // ncmpcpp/MPD surface bitrate via the status `bitrate:` line, but a
        // Format hint here is harmless and readable.
        p.push(("Format".to_string(), format!("{}kbps", br)));
    }
    if let Some(d) = s.duration_secs {
        p.push(("Time".to_string(), d.to_string()));
        p.push(("duration".to_string(), format!("{d}.000")));
    }
}
