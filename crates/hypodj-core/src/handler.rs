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

use std::sync::Mutex;

use opensubsonic::AlbumListType;
use tokio::sync::Notify;

use crate::model::{AlbumId, ArtistId, Song, SongId};
use crate::mpd::{MpdCommand, MpdHandler, MpdResponse};
use crate::player::{PlayState, PlayerHandle};
use crate::subsonic::SubsonicClient;

/// One queue entry: a resolved song plus its MPD song id (a monotonically
/// increasing integer, MPD's stable per-song handle, distinct from queue pos).
#[derive(Clone)]
struct QueueItem {
    id: u64,
    song: Song,
}

#[derive(Default)]
struct State {
    queue: Vec<QueueItem>,
    next_id: u64,
    /// Index into `queue` of the current song, if any.
    current: Option<usize>,
    volume: u8,
    /// Bumped whenever the queue changes (MPD "playlist version").
    playlist_version: u64,
}

pub struct HypodjHandler {
    client: SubsonicClient,
    player: PlayerHandle,
    state: Mutex<State>,
    /// Fired when a subsystem changes, to wake `idle`.
    changed: Notify,
}

impl HypodjHandler {
    pub fn new(client: SubsonicClient, player: PlayerHandle) -> Self {
        Self {
            client,
            player,
            state: Mutex::new(State {
                volume: 100,
                ..Default::default()
            }),
            changed: Notify::new(),
        }
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
    if let Some(a) = &s.artist {
        p.push(("Artist".to_string(), a.clone()));
    }
    if let Some(a) = &s.album {
        p.push(("Album".to_string(), a.clone()));
    }
    if let Some(t) = s.track {
        p.push(("Track".to_string(), t.to_string()));
    }
    if let Some(d) = s.duration_secs {
        p.push(("Time".to_string(), d.to_string()));
        p.push(("duration".to_string(), format!("{d}.000")));
    }
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

            // ── stored playlists (must never error -> ncmpcpp hang) ─────────
            MpdCommand::ListPlaylists => MpdResponse::ok(),
            MpdCommand::ListPlaylistInfo(_) => MpdResponse::ok(),
            MpdCommand::Load(_) => MpdResponse::ok(),

            // ── db browse ──────────────────────────────────────────────────
            MpdCommand::LsInfo(path) => self.lsinfo(path.as_deref()).await,
            MpdCommand::ListAllInfo(path) => self.lsinfo(path.as_deref()).await,

            MpdCommand::Find(q) | MpdCommand::Search(q) => {
                if q.trim().is_empty() {
                    return MpdResponse::ok();
                }
                match self.client.search_songs(&q).await {
                    Ok(songs) => {
                        let mut pairs = Vec::new();
                        for s in &songs {
                            pairs.extend(browse_song_pairs(s));
                        }
                        MpdResponse::Pairs(pairs)
                    }
                    Err(e) => ack(ACK_ERROR_UNKNOWN, "search", &e.to_string()),
                }
            }

            MpdCommand::List(spec) => {
                // `list <tag>`: support Artist and Album minimally.
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
                    _ => MpdResponse::ok(),
                }
            }

            // ── binary (not implemented this phase) ─────────────────────────
            MpdCommand::AlbumArt(..) | MpdCommand::ReadPicture(..) => {
                ack(ACK_ERROR_NO_EXIST, "albumart", "No file exists")
            }

            // ── capability probes ──────────────────────────────────────────
            MpdCommand::Commands => {
                let cmds = [
                    "add", "addid", "clear", "commands", "currentsong", "delete",
                    "find", "getvol", "idle", "list", "listall", "listallinfo",
                    "listplaylistinfo", "listplaylists", "load", "lsinfo", "next",
                    "noidle", "notcommands", "outputs", "pause", "ping", "play",
                    "playid", "playlistid", "playlistinfo", "plchanges",
                    "previous", "search", "seek", "seekcur", "seekid", "setvol",
                    "stats", "status", "stop", "tagtypes", "urlhandlers",
                ];
                let pairs = cmds
                    .iter()
                    .map(|c| ("command".to_string(), c.to_string()))
                    .collect();
                MpdResponse::Pairs(pairs)
            }
            MpdCommand::NotCommands => MpdResponse::ok(),
            MpdCommand::TagTypes => {
                let tags = ["Artist", "Album", "Title", "Track"];
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
    /// Back `lsinfo` / `listallinfo`. Root lists artist directories; `artist/<id>`
    /// lists album directories; `album/<id>` lists song files.
    async fn lsinfo(&self, path: Option<&str>) -> MpdResponse {
        match path {
            None | Some("") | Some("/") => match self.client.artists().await {
                Ok(artists) => {
                    let mut pairs = Vec::new();
                    for a in &artists {
                        pairs.push(("directory".to_string(), format!("artist/{}", a.id.0)));
                        // ncmpcpp shows the directory basename; carry the name so
                        // the Browse view is readable.
                        pairs.push(("Artist".to_string(), a.name.clone()));
                    }
                    MpdResponse::Pairs(pairs)
                }
                Err(e) => ack(ACK_ERROR_UNKNOWN, "lsinfo", &e.to_string()),
            },
            Some(p) if p.starts_with("artist/") => {
                let id = p.trim_start_matches("artist/");
                match self.client.artist_albums(&ArtistId(id.to_string())).await {
                    Ok(albums) => {
                        let mut pairs = Vec::new();
                        for al in &albums {
                            pairs.push(("directory".to_string(), format!("album/{}", al.id.0)));
                            pairs.push(("Album".to_string(), al.name.clone()));
                        }
                        MpdResponse::Pairs(pairs)
                    }
                    Err(e) => ack(ACK_ERROR_UNKNOWN, "lsinfo", &e.to_string()),
                }
            }
            Some(p) if p.starts_with("album/") => {
                let id = p.trim_start_matches("album/");
                match self.client.album_songs(&AlbumId(id.to_string())).await {
                    Ok(songs) => {
                        let mut pairs = Vec::new();
                        for s in &songs {
                            pairs.extend(browse_song_pairs(s));
                        }
                        MpdResponse::Pairs(pairs)
                    }
                    Err(e) => ack(ACK_ERROR_UNKNOWN, "lsinfo", &e.to_string()),
                }
            }
            Some(_) => MpdResponse::ok(),
        }
    }
}

/// Serialize a `Song` as a browse `file:` entry (no queue Pos/Id).
fn browse_song_pairs(s: &Song) -> Vec<(String, String)> {
    let mut p = vec![
        ("file".to_string(), format!("song/{}", s.id.0)),
        ("Title".to_string(), s.title.clone()),
    ];
    if let Some(a) = &s.artist {
        p.push(("Artist".to_string(), a.clone()));
    }
    if let Some(a) = &s.album {
        p.push(("Album".to_string(), a.clone()));
    }
    if let Some(t) = s.track {
        p.push(("Track".to_string(), t.to_string()));
    }
    if let Some(d) = s.duration_secs {
        p.push(("Time".to_string(), d.to_string()));
        p.push(("duration".to_string(), format!("{d}.000")));
    }
    p
}
