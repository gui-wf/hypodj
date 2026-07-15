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
use crate::model::{AlbumId, ArtistId, Genre, QueueEntry, Song, SongId};
use crate::subsonic::SubsonicError;
use crate::mpd::{MpdCommand, MpdHandler, MpdResponse, StickerCmd};
use crate::player::{PlayState, PlayerHandle};
use crate::subsonic::{list_type_from_dirname, SubsonicClient};

/// One queue entry: a playable [`QueueEntry`] (Subsonic song OR raw stream) plus
/// its MPD song id (a monotonically increasing integer, MPD's stable per-song
/// handle, distinct from queue pos).
#[derive(Clone)]
struct QueueItem {
    id: u64,
    entry: QueueEntry,
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
        // A library song resolves a Subsonic stream URL and plays under its id
        // (scrobbled). A raw stream plays its URL verbatim with no id (never
        // scrobbled). Either way a bad/unreachable URL surfaces as a player
        // error here and, at worst, an idle/stopped state - never a panic.
        match &item.entry {
            QueueEntry::Song(song) => {
                let url = self
                    .client
                    .stream_url(&song.id)
                    .map_err(|e| e.to_string())?;
                self.player
                    .play_url(Some(song.id.clone()), url.as_str())
                    .await
                    .map_err(|e| e.to_string())?;
            }
            QueueEntry::Stream { url, .. } => {
                self.player
                    .play_url(None, url)
                    .await
                    .map_err(|e| e.to_string())?;
            }
        }
        {
            let mut st = self.state.lock().unwrap();
            st.current = Some(idx);
        }
        self.notify_change();
        Ok(())
    }

    /// Add an entry by uri. A `song/<id>` uri resolves Subsonic metadata; an
    /// absolute `http://`/`https://` uri is queued as a raw stream (internet
    /// radio) played verbatim, with NO Subsonic call, id, rating, or scrobble -
    /// exactly as MPD's own `add <url>` behaves. Returns the assigned MPD id.
    async fn enqueue_uri(&self, uri: &str) -> Result<u64, String> {
        let entry = if is_stream_uri(uri) {
            // Title is the URL (a stream's icy-name is only known once mpv
            // connects; the URL is a sensible, always-available label).
            QueueEntry::Stream {
                url: uri.to_string(),
                title: uri.to_string(),
            }
        } else {
            let song_id = uri
                .strip_prefix("song/")
                .ok_or_else(|| format!("unsupported uri: {uri}"))?;
            let song = self
                .client
                .song(&SongId(song_id.to_string()))
                .await
                .map_err(|e| e.to_string())?;
            QueueEntry::Song(song)
        };
        let mut st = self.state.lock().unwrap();
        let id = st.next_id;
        st.next_id += 1;
        st.queue.push(QueueItem { id, entry });
        st.playlist_version += 1;
        drop(st);
        self.notify_change();
        Ok(id)
    }

    /// Append an already-resolved [`Song`] to the queue, returning its MPD id.
    /// This is the shared, INFALLIBLE push path (no network, no parse): it mirrors
    /// [`enqueue_uri`](Self::enqueue_uri)'s id/version/notify bookkeeping. Used by
    /// `findadd`/`searchadd`, whose matches are already full `Song`s from
    /// `collect_matches`, so re-fetching each via `song/<id>` would be a wasted
    /// round-trip.
    async fn enqueue_song(&self, song: Song) -> u64 {
        let mut st = self.state.lock().unwrap();
        let id = st.next_id;
        st.next_id += 1;
        st.queue.push(QueueItem {
            id,
            entry: QueueEntry::Song(song),
        });
        st.playlist_version += 1;
        drop(st);
        self.notify_change();
        id
    }
}

/// Serialize one queued entry as MPD `playlistinfo`/`currentsong` pairs. A raw
/// stream renders with `file:` = its URL and `Title:` = the URL, and no Time /
/// tags (duration unknown for a live stream) - MPD renders such an entry fine.
fn song_pairs(item: &QueueItem, pos: usize) -> Vec<(String, String)> {
    let mut p = match &item.entry {
        QueueEntry::Song(s) => {
            let mut p = vec![
                ("file".to_string(), format!("song/{}", s.id.0)),
                ("Title".to_string(), s.title.clone()),
            ];
            push_song_tags(&mut p, s);
            p
        }
        QueueEntry::Stream { url, title } => vec![
            ("file".to_string(), url.clone()),
            ("Title".to_string(), title.clone()),
        ],
    };
    p.push(("Pos".to_string(), pos.to_string()));
    p.push(("Id".to_string(), item.id.to_string()));
    p
}

/// Is `uri` an absolute HTTP(S) stream URL (internet radio) rather than a
/// synthetic hypodj `song/`/`album/`/`artist/` path? Such a uri is played
/// directly, bypassing Subsonic resolution - mirroring MPD's `add <url>`.
fn is_stream_uri(uri: &str) -> bool {
    uri.starts_with("http://") || uri.starts_with("https://")
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
        // HONEST LIMITATION: this always reports `changed: player`, regardless of
        // what actually changed or which subsystems the client subscribed to.
        //
        // Reason: there is a SINGLE `changed: Notify` fired for every mutation
        // (queue add/delete/clear, play/pause/stop, volume, star). We do not yet
        // track WHICH subsystem changed, so we cannot honestly emit `playlist`
        // vs `mixer` vs `player` separately, nor filter by the client's
        // `_subsystems` list. We deliberately do NOT claim more than we know:
        // `player` is the one subsystem that a re-read of status/currentsong
        // covers, and ncmpcpp responds to any `changed:` line by re-reading
        // status + currentsong + plchanges, so a single conservative wake still
        // refreshes its whole view. Reporting the true per-subsystem set would
        // mean carrying a changed-subsystem flag alongside the Notify - a real
        // improvement left for when a client needs the granularity.
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
                        // Duration is only known for a library song; a live
                        // stream reports none (unknown length is valid MPD).
                        if let QueueEntry::Song(s) = &item.entry {
                            if let Some(d) = s.duration_secs {
                                b = b.pair("duration", format!("{d}.000"));
                            }
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
            MpdCommand::FindAdd(filters) => self.find_add(filters, true).await,
            MpdCommand::SearchAdd(filters) => self.find_add(filters, false).await,
            MpdCommand::Count(filters) => self.count(filters).await,

            MpdCommand::List { tag, filter } => {
                // `list <tag> [filter]`: support Artist, Album, Genre. When a
                // filter is present it MUST narrow the result - never fall back
                // to the unfiltered library dump (see list_album_by_artist).
                match tag.as_str() {
                    "artist" | "albumartist" => match self.client.artists().await {
                        Ok(artists) => {
                            let pairs = artists
                                .into_iter()
                                .filter(|a| artist_passes_filter(&a.name, &filter))
                                .map(|a| ("Artist".to_string(), a.name))
                                .collect();
                            MpdResponse::Pairs(pairs)
                        }
                        Err(e) => ack(ACK_ERROR_UNKNOWN, "list", &e.to_string()),
                    },
                    "album" => {
                        // A filter constraining the artist restricts to that
                        // artist's albums; any other (or absent) filter lists all.
                        // A bare positional `list album "Tosca"` parses to
                        // filter=[(any,Tosca)], so treat an `any` value as an
                        // artist name too (classic 2-arg `list album <ARTIST>`).
                        if let Some(artist) =
                            filter_value(&filter, &["artist", "albumartist", "any"])
                        {
                            return self.list_albums_by_artist(&artist).await;
                        }
                        // `list album genre X` -> albums of that genre, via
                        // getAlbumList2 type=byGenre (confirmed backend path).
                        // Page it (getAlbumList2 caps `size` at 500 per call) so a
                        // large genre is not silently truncated - same "no silent
                        // caps" contract the search3 paging honors.
                        if let Some(genre) = filter_value(&filter, &["genre"]) {
                            const PAGE: i32 = 500;
                            // Ceiling so a backend that ignores `offset` (returns a
                            // full page forever) cannot spin unboundedly or overflow
                            // the i32 offset. 20 pages = 10000 albums, far beyond any
                            // real genre.
                            const MAX_PAGES: i32 = 20;
                            let mut names: Vec<(String, String)> = Vec::new();
                            let mut offset: i32 = 0;
                            let mut page = 0;
                            loop {
                                match self
                                    .client
                                    .album_list_by_genre(&genre, Some(PAGE), Some(offset))
                                    .await
                                {
                                    Ok(albums) => {
                                        let got = albums.len();
                                        names.extend(
                                            albums.into_iter().map(|a| ("Album".to_string(), a.name)),
                                        );
                                        page += 1;
                                        if (got as i32) < PAGE || page >= MAX_PAGES {
                                            break;
                                        }
                                        offset += PAGE;
                                    }
                                    Err(e) => return ack(ACK_ERROR_UNKNOWN, "list", &e.to_string()),
                                }
                            }
                            return MpdResponse::Pairs(names);
                        }
                        if !filter.is_empty() {
                            // A filter we cannot honor: narrow to nothing rather
                            // than silently dumping the whole library.
                            return MpdResponse::ok();
                        }
                        match self.client.album_list(AlbumListType::AlphabeticalByName, Some(500)).await {
                            Ok(albums) => {
                                let pairs = albums
                                    .into_iter()
                                    .map(|a| ("Album".to_string(), a.name))
                                    .collect();
                                MpdResponse::Pairs(pairs)
                            }
                            Err(e) => ack(ACK_ERROR_UNKNOWN, "list", &e.to_string()),
                        }
                    }
                    "genre" if !filter.is_empty() => {
                        // No Subsonic genre-by-filter path for the genre LIST
                        // itself (a genre filter on `list genre` is meaningless);
                        // narrow to nothing rather than dumping the whole list.
                        // (`list album genre X` is tag=album and handled above.)
                        MpdResponse::ok()
                    }
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

            // ── sticker rating (feature 3, ncmpcpp rating path) ─────────────
            MpdCommand::Sticker(s) => self.sticker(s).await,

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
                    "commands", "count", "currentsong", "delete", "find", "findadd",
                    "getvol", "idle",
                    "list", "listall", "listallinfo", "listplaylistinfo",
                    "listplaylists", "load", "lsinfo", "next", "noidle",
                    "notcommands", "outputs", "pause", "ping", "play", "playid",
                    "playlistadd", "playlistclear", "playlistdelete", "playlistid",
                    "playlistinfo", "plchanges", "previous", "readpicture",
                    "search", "searchadd", "seek", "seekcur", "seekid", "setvol", "stats", "sticker",
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
            MpdCommand::UrlHandlers => MpdResponse::pairs()
                .pair("handler", "http")
                .pair("handler", "https")
                .build(),

            MpdCommand::Unsupported(name) => {
                ack(ACK_ERROR_UNKNOWN, &name, &format!("unknown command \"{name}\""))
            }
        }
    }
}

/// A read-only snapshot of the current queue item, for the MPRIS surface. Holds
/// the MPD song id (stable per-song handle, used to build the `mpris:trackid`
/// object path) plus a clone of the queued [`QueueEntry`] (library Song or raw
/// stream) so the MPRIS module can render Metadata without reaching into the
/// handler's private state or holding its lock.
#[derive(Clone)]
pub struct CurrentItem {
    pub mpd_id: u64,
    pub entry: QueueEntry,
}

impl HypodjHandler {
    /// Snapshot the current queue item (id + entry), or `None` when stopped /
    /// queue empty. Used by the MPRIS server to render now-playing Metadata.
    pub fn current_item(&self) -> Option<CurrentItem> {
        let st = self.state.lock().unwrap();
        let idx = st.current?;
        st.queue.get(idx).map(|it| CurrentItem {
            mpd_id: it.id,
            entry: it.entry.clone(),
        })
    }

    /// Current volume (0..=100), for the MPRIS `Volume` property.
    pub fn volume(&self) -> u8 {
        self.state.lock().unwrap().volume
    }

    /// Advance to the next queue entry (MPRIS `Next` / desktop control). No-op at
    /// the end of the queue.
    pub async fn mpris_next(&self) {
        let next = {
            let st = self.state.lock().unwrap();
            st.current.map(|c| c + 1).filter(|&i| i < st.queue.len())
        };
        if let Some(idx) = next {
            let _ = self.play_index(idx).await;
        }
    }

    /// Go to the previous queue entry (MPRIS `Previous` / desktop control). No-op
    /// at the head of the queue.
    pub async fn mpris_previous(&self) {
        let prev = {
            let st = self.state.lock().unwrap();
            st.current.and_then(|c| c.checked_sub(1))
        };
        if let Some(idx) = prev {
            let _ = self.play_index(idx).await;
        }
    }

    /// Set volume (MPRIS `Volume` setter): mirror it into shared state and push
    /// to the player, same as the MPD `setvol` path.
    pub async fn mpris_set_volume(&self, vol: u8) {
        let v = vol.min(100);
        self.state.lock().unwrap().volume = v;
        let _ = self.player.set_volume(v).await;
        self.notify_change();
    }

    /// Await the next change notification (queue/playback/volume/star). The MPRIS
    /// server loops on this to emit `PropertiesChanged`. Shares the SAME `changed`
    /// Notify that wakes MPD `idle`, so both surfaces refresh off one signal.
    pub async fn changed(&self) {
        self.changed.notified().await;
    }

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
        match self.cached_artists().await {
            Ok(artists) => {
                for (id, name) in artists {
                    pairs.push(("directory".to_string(), format!("artist/{}", id.0)));
                    pairs.push(("Artist".to_string(), name));
                }
                MpdResponse::Pairs(pairs)
            }
            Err(e) => ack(ACK_ERROR_UNKNOWN, "lsinfo", &e.to_string()),
        }
    }

    /// Artist id+name list, served from the shared `dir_cache` "artists" slot
    /// (the `directory`/`Artist` rows) or fetched + cached on a miss. Both
    /// `lsinfo_root` and `list_albums_by_artist` go through here so
    /// `list album artist X` hits the same cache instead of re-fetching.
    async fn cached_artists(&self) -> Result<Vec<(ArtistId, String)>, SubsonicError> {
        if let Some(rows) = self.dir_cache.get(&"artists".to_string()) {
            return Ok(parse_artist_rows(&rows));
        }
        let artists = self.client.artists().await?;
        let rows: Vec<(String, String)> = artists
            .iter()
            .flat_map(|a| {
                [
                    ("directory".to_string(), format!("artist/{}", a.id.0)),
                    ("Artist".to_string(), a.name.clone()),
                ]
            })
            .collect();
        self.dir_cache.put("artists".to_string(), rows);
        Ok(artists.into_iter().map(|a| (a.id, a.name)).collect())
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
        // Thread the true command name into the ACK (mirrors find_add's cmd),
        // so a failing `find` acks as `find`, not a hardcoded `search`.
        let cmd = if exact { "find" } else { "search" };
        let matches = match self.collect_matches(&filters, exact).await {
            Ok(m) => m,
            Err(e) => return ack(ACK_ERROR_UNKNOWN, cmd, &e),
        };
        let mut pairs = Vec::new();
        for s in &matches {
            pairs.extend(browse_song_pairs(s));
        }
        MpdResponse::Pairs(pairs)
    }

    /// `count <filter...>`: the same exact-match search3 + client-side
    /// post-filter as `find`, but instead of listing the songs it returns their
    /// tally and total playtime. MPD's shape is two lines: `songs: <N>` and
    /// `playtime: <total_seconds>` (integer seconds, songs of unknown duration
    /// contributing 0). An empty filter yields a zero tally: we have no
    /// full-library enumeration to count against, so 0 is the honest floor
    /// rather than a fabricated total. On a search3 error, ACK as `count`.
    async fn count(&self, filters: Vec<(String, String)>) -> MpdResponse {
        if filters.is_empty() {
            return MpdResponse::pairs()
                .pair("songs", "0")
                .pair("playtime", "0")
                .build();
        }
        // count is an aggregate: page much further than find/findadd so the tally
        // is honest for large artists/genres (500 pages = 100k songs), still
        // bounded against a backend that ignores offset.
        let matches = match self.collect_matches_capped(&filters, true, 500).await {
            Ok(m) => m,
            Err(e) => return ack(ACK_ERROR_UNKNOWN, "count", &e),
        };
        let songs = matches.len();
        let playtime: u64 = matches
            .iter()
            .map(|s| s.duration_secs.unwrap_or(0) as u64)
            .sum();
        MpdResponse::pairs()
            .pair("songs", songs.to_string())
            .pair("playtime", playtime.to_string())
            .build()
    }

    /// The shared core of find/search/findadd/searchadd: run search3 (full-text)
    /// for the combined filter values, then recover MPD-tag precision with a
    /// client-side post-filter. `exact` (find) matches equality; otherwise
    /// (search) case-insensitive substring. Returns the matching songs so a
    /// caller can either list them (`search_filtered`) or enqueue them
    /// (`find_add`). search3 results are query-specific + ephemeral -> NEVER
    /// cached. On a search3 error, returns the error string for the caller to ACK.
    async fn collect_matches(
        &self,
        filters: &[(String, String)],
        exact: bool,
    ) -> Result<Vec<Song>, String> {
        // find/findadd targets are listings/enqueues: 25 pages (5000 songs) is
        // far beyond any real request. `count` needs an honest total, so it pages
        // further via collect_matches_capped.
        self.collect_matches_capped(filters, exact, 25).await
    }

    /// [`collect_matches`] with an explicit page ceiling. The ceiling exists only
    /// so a backend that ignores `song_offset` (keeps returning a full page)
    /// cannot loop forever, grow the buffer without bound, or overflow the i32
    /// offset. Hitting it is logged (never a silent cap - CLAUDE.md).
    async fn collect_matches_capped(
        &self,
        filters: &[(String, String)],
        exact: bool,
        max_pages: i32,
    ) -> Result<Vec<Song>, String> {
        // Build the full-text query from all values (search3 is full-text).
        let query = filters
            .iter()
            .map(|(_, v)| v.as_str())
            .collect::<Vec<_>>()
            .join(" ");
        // Page search3 so the result is COMPLETE, not silently truncated at the
        // 200-song cap: request 200 at a time, accumulating until a short page
        // (< PAGE) signals exhaustion.
        const PAGE: i32 = 200;
        let mut songs: Vec<Song> = Vec::new();
        // De-dup by song id ACROSS pages. A backend that ignores `song_offset`
        // returns the same page every request; without dedup `count` would sum
        // those repeats into a fabricated total (500 pages * 200 = 100000). Dedup
        // also absorbs a row that overlaps a page boundary on a well-behaved
        // server. `seen` is the source of truth for the tally.
        let mut seen: std::collections::HashSet<String> = std::collections::HashSet::new();
        let mut offset: i32 = 0;
        let mut page = 0;
        loop {
            let hits = self
                .client
                .search3_paged(&query, Some(PAGE), Some(offset))
                .await
                .map_err(|e| e.to_string())?;
            let got = hits.songs.len();
            let mut fresh = 0usize;
            for s in hits.songs {
                if seen.insert(s.id.0.clone()) {
                    songs.push(s);
                    fresh += 1;
                }
            }
            page += 1;
            // Short page -> exhausted. A full page that added NOTHING new means the
            // backend is repeating (ignoring offset) -> stop rather than spin.
            if (got as i32) < PAGE || fresh == 0 {
                break;
            }
            if page >= max_pages {
                tracing::warn!(
                    query = %query,
                    collected = songs.len(),
                    max_pages,
                    "collect_matches hit the page ceiling; result may be incomplete"
                );
                break;
            }
            offset += PAGE;
        }
        let matches = songs
            .into_iter()
            .filter(|s| filters.iter().all(|(tag, val)| tag_matches(s, tag, val, exact)))
            .collect();
        Ok(matches)
    }

    /// Back `findadd`/`searchadd`: collect the matching songs (same path as
    /// find/search) and append every one to the play queue directly (they are
    /// already full `Song`s from `collect_matches`, so no per-song refetch), then
    /// wake idle subscribers. Empty filters is a no-op empty-OK (mirrors
    /// `search_filtered`). A search3 failure ACKs; the per-song push is infallible
    /// so every match is honestly enqueued (nothing is silently dropped).
    async fn find_add(&self, filters: Vec<(String, String)>, exact: bool) -> MpdResponse {
        if filters.is_empty() {
            return MpdResponse::ok();
        }
        let cmd = if exact { "findadd" } else { "searchadd" };
        let matches = match self.collect_matches(&filters, exact).await {
            Ok(m) => m,
            Err(e) => return ack(ACK_ERROR_UNKNOWN, cmd, &e),
        };
        for s in matches {
            self.enqueue_song(s).await;
        }
        self.notify_change();
        MpdResponse::ok()
    }

    /// Back `list album` narrowed by an artist filter: resolve the artist by
    /// (case-insensitive) name, then list that artist's albums. An unknown
    /// artist yields an empty listing - never the full album library (honoring
    /// the "a present filter must narrow" contract).
    async fn list_albums_by_artist(&self, artist: &str) -> MpdResponse {
        let artists = match self.cached_artists().await {
            Ok(a) => a,
            Err(e) => return ack(ACK_ERROR_UNKNOWN, "list", &e.to_string()),
        };
        // Unicode-aware case-insensitive compare (eq_ignore_ascii_case only folds
        // ASCII, so case-differing non-ASCII names would fail to match).
        let wanted = artist.to_lowercase();
        let id = match artists
            .into_iter()
            .find(|(_, name)| name.to_lowercase() == wanted)
        {
            Some((id, _)) => id,
            None => return MpdResponse::ok(),
        };
        match self.client.artist_albums(&id).await {
            Ok(albums) => {
                let pairs = albums
                    .into_iter()
                    .map(|a| ("Album".to_string(), a.name))
                    .collect();
                MpdResponse::Pairs(pairs)
            }
            Err(e) => ack(ACK_ERROR_UNKNOWN, "list", &e.to_string()),
        }
    }

    /// Back the `sticker` command for the `rating` sticker only (ncmpcpp's
    /// rating path), bridging to Subsonic setRating/userRating. Any other
    /// sticker (unknown verb/type/name) answers empty-OK so a probing client
    /// does not hang. A failing Subsonic call ACKs, never panics.
    async fn sticker(&self, cmd: StickerCmd) -> MpdResponse {
        match cmd {
            StickerCmd::Set { uri, value } => {
                let id = match song_id_from_uri(&uri) {
                    Some(id) => id,
                    None => return ack(ACK_ERROR_NO_EXIST, "sticker", "unsupported uri"),
                };
                match self.client.set_rating(&id, value).await {
                    Ok(()) => {
                        self.bust_rating_caches();
                        self.notify_change();
                        MpdResponse::ok()
                    }
                    Err(e) => ack(ACK_ERROR_UNKNOWN, "sticker", &e.to_string()),
                }
            }
            StickerCmd::Delete { uri } => {
                let id = match song_id_from_uri(&uri) {
                    Some(id) => id,
                    None => return ack(ACK_ERROR_NO_EXIST, "sticker", "unsupported uri"),
                };
                // Deleting the rating sticker clears it (setRating 0).
                match self.client.set_rating(&id, 0).await {
                    Ok(()) => {
                        self.bust_rating_caches();
                        self.notify_change();
                        MpdResponse::ok()
                    }
                    Err(e) => ack(ACK_ERROR_UNKNOWN, "sticker", &e.to_string()),
                }
            }
            StickerCmd::Get { uri } => {
                let id = match song_id_from_uri(&uri) {
                    Some(id) => id,
                    None => return ack(ACK_ERROR_NO_EXIST, "sticker", "unsupported uri"),
                };
                match self.client.song(&id).await {
                    // MPD framing: `sticker: <name>=<value>`.
                    Ok(song) => match song.user_rating {
                        Some(r) => MpdResponse::pairs()
                            .pair("sticker", format!("rating={r}"))
                            .build(),
                        // No rating set: MPD returns a "no such sticker" ACK.
                        None => ack(ACK_ERROR_NO_EXIST, "sticker", "no such sticker"),
                    },
                    Err(e) => ack(ACK_ERROR_UNKNOWN, "sticker", &e.to_string()),
                }
            }
            StickerCmd::List { uri } => {
                let id = match song_id_from_uri(&uri) {
                    Some(id) => id,
                    None => return ack(ACK_ERROR_NO_EXIST, "sticker", "unsupported uri"),
                };
                match self.client.song(&id).await {
                    Ok(song) => match song.user_rating {
                        Some(r) => MpdResponse::pairs()
                            .pair("sticker", format!("rating={r}"))
                            .build(),
                        // No stickers set: empty-OK (a valid empty list).
                        None => MpdResponse::ok(),
                    },
                    Err(e) => ack(ACK_ERROR_UNKNOWN, "sticker", &e.to_string()),
                }
            }
            // Unknown sticker verb/type/name: empty-OK, never hang the client.
            StickerCmd::Unsupported => MpdResponse::ok(),
        }
    }

    /// Invalidate cached listings whose user_rating could change after setRating.
    /// Album/genre/list listings carry per-song `user_rating`, so bust them so a
    /// subsequent browse reflects the new rating.
    fn bust_rating_caches(&self) {
        self.listings.invalidate_prefix("album/");
        self.listings.invalidate_prefix("genre/");
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
    // Composer/performer are MPD MULTI-VALUED tags: a track can credit several,
    // and a filter must match on ANY single value (real MPD matches per value).
    // We store them as a ", "-joined display string (from displayComposer /
    // contributors), so split on that delimiter and match any part - otherwise an
    // exact `find performer "Yo-Yo Ma"` never equals "Itzhak Perlman, Yo-Yo Ma".
    let cmp_multi = |field: &Option<String>| -> bool {
        match field {
            Some(s) => s.split(", ").filter(|p| !p.is_empty()).any(cmp),
            None => false,
        }
    };
    match tag {
        "title" => cmp(&song.title),
        "artist" | "albumartist" => song.artist.as_deref().map(cmp).unwrap_or(false),
        "album" => song.album.as_deref().map(cmp).unwrap_or(false),
        "genre" => song.genre.as_deref().map(cmp).unwrap_or(false),
        // Numeric tags the Song carries: compare on the string form (Date is the
        // release year; MPD emits `Date` from `year`).
        "date" => song.year.map(|y| cmp(&y.to_string())).unwrap_or(false),
        "track" => song.track.map(|t| cmp(&t.to_string())).unwrap_or(false),
        "disc" => song.disc.map(|d| cmp(&d.to_string())).unwrap_or(false),
        "comment" => song.comment.as_deref().map(cmp).unwrap_or(false),
        // Composer/performer come from OpenSubsonic metadata (displayComposer /
        // contributors). Absent on plain-Subsonic servers -> None -> no match.
        "composer" => cmp_multi(&song.composer),
        "performer" => cmp_multi(&song.performer),
        // MPD `any` spans EVERY tag - all the ones this Song models, not just
        // title/artist/album (else `any "Techno"` misses a genre-only match).
        "any" => {
            cmp(&song.title)
                || song.artist.as_deref().map(cmp).unwrap_or(false)
                || song.album.as_deref().map(cmp).unwrap_or(false)
                || song.genre.as_deref().map(cmp).unwrap_or(false)
                || song.comment.as_deref().map(cmp).unwrap_or(false)
                || song.year.map(|y| cmp(&y.to_string())).unwrap_or(false)
                || song.track.map(|t| cmp(&t.to_string())).unwrap_or(false)
                || song.disc.map(|d| cmp(&d.to_string())).unwrap_or(false)
                || cmp_multi(&song.composer)
                || cmp_multi(&song.performer)
        }
        // Genuinely unmodeled tag (base, file, modified-since, or unknown): the
        // Song carries no data to satisfy it, so
        // it matches NOTHING rather than passing all. tag_matches is shared by
        // find (list) and findadd (enqueue); passing-all would make findadd
        // over-add on an unsatisfiable constraint. MPD-correct: an unsatisfiable
        // constraint yields no matches.
        _ => false,
    }
}

/// The value of the first filter pair whose tag matches one of `tags`, if any.
/// Used to pull e.g. the `artist` constraint out of a `list album` filter.
fn filter_value(filter: &[(String, String)], tags: &[&str]) -> Option<String> {
    filter
        .iter()
        .find(|(tag, _)| tags.contains(&tag.as_str()))
        .map(|(_, v)| v.clone())
}

/// Does an artist named `name` pass the `list artist`/`list albumartist` filter?
/// An empty filter passes everything. An artist/albumartist constraint matches
/// (case-insensitively) on the name. Any other constraint we cannot honor
/// excludes the row, so a present-but-unhonorable filter narrows to nothing
/// rather than dumping the whole artist list.
fn artist_passes_filter(name: &str, filter: &[(String, String)]) -> bool {
    if filter.is_empty() {
        return true;
    }
    filter.iter().all(|(tag, val)| match tag.as_str() {
        // Unicode-aware fold (eq_ignore_ascii_case only folds ASCII).
        "artist" | "albumartist" => name.to_lowercase() == val.to_lowercase(),
        _ => false,
    })
}

/// Reconstruct `(ArtistId, name)` pairs from the cached `directory`/`Artist`
/// rows that `cached_artists` stores (a `directory: artist/<id>` row followed by
/// its `Artist: <name>` row). Malformed pairs are skipped.
fn parse_artist_rows(rows: &[(String, String)]) -> Vec<(ArtistId, String)> {
    let mut out = Vec::new();
    let mut i = 0;
    while i + 1 < rows.len() {
        if rows[i].0 == "directory" {
            if let (Some(id), true) =
                (rows[i].1.strip_prefix("artist/"), rows[i + 1].0 == "Artist")
            {
                out.push((ArtistId(id.to_string()), rows[i + 1].1.clone()));
            }
        }
        i += 2;
    }
    out
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

#[cfg(test)]
mod tests {
    use super::*;
    use crate::config::ServerConfig;
    use crate::player::{NullPlayer, PlayState, PlayerEvent};
    use crate::scrobble::Scrobbler;

    const NTS: &str = "https://stream-mixtape-geo.ntslive.net/mixtape5";

    /// A handler wired to a NON-networked Subsonic client and a real NullPlayer
    /// actor. The raw-stream path never calls the client, so no server is needed.
    ///
    /// `connect()` builds a real reqwest client, which needs system CA certs; a
    /// network-isolated build sandbox (nix `doCheck`) has none and the reqwest
    /// builder aborts. That is environmental, not a wiring failure, so return
    /// `None` there and the caller skips (same guard as `subsonic::tests`). In the
    /// devshell/CI with certs this yields a real client and the test runs.
    fn handler_with_null_player(
    ) -> Option<(HypodjHandler, tokio::sync::mpsc::Receiver<PlayerEvent>)> {
        let cfg = ServerConfig {
            url: "http://127.0.0.1:1/never-called".to_string(),
            username: "u".to_string(),
            password: "p".to_string(),
            client_name: "test".to_string(),
        };
        let client = match std::panic::catch_unwind(|| SubsonicClient::connect(&cfg)) {
            Ok(Ok(c)) => Arc::new(c),
            _ => {
                eprintln!("skipping: no CA certs (sandbox); connect() not exercisable here");
                return None;
            }
        };
        let (player, events) = NullPlayer::spawn();
        Some((HypodjHandler::new(client, player), events))
    }

    fn sample_song() -> Song {
        Song {
            id: SongId("so-1".into()),
            title: "Independent Us".into(),
            album: Some("Let Love Rumpel".into()),
            album_id: Some(AlbumId("al-1".into())),
            artist: Some("Kalabrese".into()),
            track: Some(4),
            duration_secs: Some(372),
            cover_art: None,
            starred: false,
            musicbrainz_id: None,
            disc: Some(2),
            year: Some(2019),
            genre: Some("Electronic".into()),
            bitrate: None,
            comment: Some("vinyl rip".into()),
            user_rating: None,
            composer: Some("Kalabrese".into()),
            performer: Some("Itzhak Perlman, Yo-Yo Ma".into()),
        }
    }

    #[test]
    fn tag_matches_constrains_date_track_disc_and_comment() {
        let s = sample_song();
        // date -> year; exact + substring both work.
        assert!(tag_matches(&s, "date", "2019", true));
        assert!(!tag_matches(&s, "date", "2020", true));
        assert!(tag_matches(&s, "date", "201", false));
        // track / disc compare on the numeric string form.
        assert!(tag_matches(&s, "track", "4", true));
        assert!(!tag_matches(&s, "track", "5", true));
        assert!(tag_matches(&s, "disc", "2", true));
        // comment.
        assert!(tag_matches(&s, "comment", "vinyl", false));
        assert!(!tag_matches(&s, "comment", "cd", false));
    }

    #[test]
    fn tag_matches_constrains_composer_and_performer() {
        // Composer/performer come from OpenSubsonic metadata; exact + substring
        // both work, and a non-matching value is rejected.
        let s = sample_song();
        assert!(tag_matches(&s, "composer", "Kalabrese", true));
        assert!(tag_matches(&s, "composer", "kala", false));
        assert!(!tag_matches(&s, "composer", "Bach", false));
        assert!(tag_matches(&s, "performer", "Yo-Yo Ma", false));
        assert!(!tag_matches(&s, "performer", "nobody", false));
        // Multi-valued: an EXACT filter on one of several joined performers must
        // match (real MPD treats performer/composer as multi-valued tags).
        assert!(tag_matches(&s, "performer", "Yo-Yo Ma", true));
        assert!(tag_matches(&s, "performer", "Itzhak Perlman", true));
        // The whole joined string is not itself a single value, so it must not
        // match as one under exact.
        assert!(!tag_matches(&s, "performer", "Itzhak Perlman, Yo-Yo Ma", true));
        // `any` spans composer and performer too, not just title/artist/album.
        assert!(tag_matches(&s, "any", "Kalabrese", false));
        assert!(tag_matches(&s, "any", "Yo-Yo Ma", true));
        // Absent metadata (plain-Subsonic) -> no match, never passes-all.
        let mut bare = sample_song();
        bare.composer = None;
        bare.performer = None;
        assert!(!tag_matches(&bare, "composer", "anything", false));
        assert!(!tag_matches(&bare, "performer", "anyone", false));
    }

    #[test]
    fn tag_matches_rejects_unmodeled_tag_rather_than_passing_all() {
        // A genuinely unsupported tag (base/file/modified-since/...) must match
        // NOTHING so findadd never over-adds on an unsatisfiable constraint.
        let s = sample_song();
        assert!(!tag_matches(&s, "modified-since", "2020", false));
        assert!(!tag_matches(&s, "base", "anything", false));
    }

    #[test]
    fn parse_artist_rows_reconstructs_id_and_name() {
        let rows = vec![
            ("directory".to_string(), "artist/ar-1".to_string()),
            ("Artist".to_string(), "Kalabrese".to_string()),
            ("directory".to_string(), "artist/ar-2".to_string()),
            ("Artist".to_string(), "Tosca".to_string()),
        ];
        let out = parse_artist_rows(&rows);
        assert_eq!(
            out,
            vec![
                (ArtistId("ar-1".into()), "Kalabrese".to_string()),
                (ArtistId("ar-2".into()), "Tosca".to_string()),
            ]
        );
    }

    #[tokio::test]
    async fn add_stream_url_produces_stream_queue_item() {
        let Some((h, _events)) = handler_with_null_player() else { return };
        let resp = h.handle(MpdCommand::Add(NTS.to_string())).await;
        // add -> empty-OK (Pairs), never an ACK.
        assert!(matches!(resp, MpdResponse::Pairs(_)), "add stream must succeed");
        let st = h.state.lock().unwrap();
        assert_eq!(st.queue.len(), 1);
        match &st.queue[0].entry {
            QueueEntry::Stream { url, title } => {
                assert_eq!(url, NTS);
                assert_eq!(title, NTS);
            }
            other => panic!("expected Stream, got {other:?}"),
        }
    }

    #[tokio::test]
    async fn play_routes_stream_url_to_player_verbatim() {
        let Some((h, mut events)) = handler_with_null_player() else { return };
        h.handle(MpdCommand::Add(NTS.to_string())).await;
        h.handle(MpdCommand::Play(Some(0))).await;
        // The NullPlayer went to Playing and, crucially, carries NO SongId for a
        // raw stream (so nothing downstream can scrobble it).
        assert_eq!(h.player.state(), PlayState::Playing);
        match events.recv().await.expect("a player event") {
            PlayerEvent::StateChanged(PlayState::Playing, song) => {
                assert!(song.is_none(), "raw stream must carry no scrobble-able id");
            }
            other => panic!("expected Playing StateChanged, got {other:?}"),
        }
    }

    #[tokio::test]
    async fn currentsong_and_playlistinfo_render_stream() {
        let Some((h, _events)) = handler_with_null_player() else { return };
        h.handle(MpdCommand::Add(NTS.to_string())).await;
        h.handle(MpdCommand::Play(Some(0))).await;

        let render = |r: MpdResponse| match r {
            MpdResponse::Pairs(p) => p,
            other => panic!("expected Pairs, got {other:?}"),
        };
        let cur = render(h.handle(MpdCommand::CurrentSong).await);
        assert!(cur.iter().any(|(k, v)| k == "file" && v == NTS));
        assert!(cur.iter().any(|(k, v)| k == "Title" && v == NTS));
        // No Time / duration for a live stream, and it must not have crashed.
        assert!(!cur.iter().any(|(k, _)| k == "Time"));

        let pl = render(h.handle(MpdCommand::PlaylistInfo(None)).await);
        assert!(pl.iter().any(|(k, v)| k == "file" && v == NTS));
        assert!(pl.iter().any(|(k, _)| k == "Pos"));

        // status must render (state: play) without a panic on the unknown-duration
        // stream item.
        let status = render(h.handle(MpdCommand::Status).await);
        assert!(status.iter().any(|(k, v)| k == "state" && v == "play"));
    }

    #[tokio::test]
    async fn scrobbler_skips_raw_stream_item() {
        // A raw stream plays with song=None, so the player emits
        // StateChanged(Playing, None). The scrobbler must not latch/act on it.
        let cfg = ServerConfig {
            url: "http://127.0.0.1:1/never-called".to_string(),
            username: "u".to_string(),
            password: "p".to_string(),
            client_name: "test".to_string(),
        };
        // connect() needs system CA certs; skip in a cert-less build sandbox
        // (same guard as the other client-constructing tests).
        let client = match std::panic::catch_unwind(|| SubsonicClient::connect(&cfg)) {
            Ok(Ok(c)) => Arc::new(c),
            _ => {
                eprintln!("skipping: no CA certs (sandbox); connect() not exercisable here");
                return;
            }
        };
        let scrobbler = Scrobbler::new(client);
        // Feeding the exact event a raw stream produces is a no-op (no id).
        scrobbler.on_event(&PlayerEvent::StateChanged(PlayState::Playing, None));
        scrobbler.on_event(&PlayerEvent::TimePos(120.0));
        // No panic, no submission possible: the scrobbler never latched a song.
        assert!(scrobbler.current_is_none());
    }
}
