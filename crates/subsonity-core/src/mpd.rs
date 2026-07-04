//! MPD-protocol server layer.
//!
//! INTERFACE defined now (FOUNDATION); the wire accept-loop is TODO(next-phase).
//!
//! Why the loop is deferred but the interface is locked: no crate implements the
//! MPD *server* side (only clients: `mpd`, `mpd_client`, and the wire codec
//! `mpd_protocol` - all verified client-side). We hand-roll the server. The
//! protocol is a line-based text protocol over TCP: the client sends a command
//! line, the server replies with `key: value` pairs terminated by `OK\n` (or
//! `ACK [error@cmd_idx] {command} message\n` on error). Command lists wrap in
//! `command_list_begin` / `command_list_end`. `idle` long-polls for change
//! events. Binary payloads (albumart) use a distinct sub-protocol - see
//! [`MpdResponse::Binary`].
//!
//! ## ncmpcpp-critical command set (why the enum is this wide NOW)
//!
//! The persona critique surfaced a real, verified failure mode from the
//! beets/bpd MPD-server port: ncmpcpp does NOT gracefully accept ACK for every
//! unknown command. Specifically:
//!   - if the stored-playlist commands (`listplaylists`, `listplaylistinfo`,
//!     `load`) return an error, ncmpcpp can enter an infinite loop and freeze;
//!   - if `plchanges` returns a shape it dislikes, the playlist view goes blank.
//!
//! So those commands must return a well-formed (even if minimal/empty) response,
//! not `Unsupported`. They are therefore first-class variants of [`MpdCommand`]
//! now, so the dispatch author cannot forget them. `commands`, `tagtypes`,
//! `outputs`, `decoders`, `urlhandlers`, `notcommands` are the capability-probe
//! commands ncmpcpp fires at connect; they too need real (small) replies.

use std::net::SocketAddr;

/// Advertised MPD protocol version in the greeting.
///
/// IMPORTANT contract: the greeting version tells the client which syntax and
/// binary/filter capabilities the server claims. Advertising `0.23.0` promises
/// `albumart`/`readpicture` binary responses and the modern filter syntax. We
/// advertise a version we can actually back. Until the binary + filter surface
/// is implemented (next-phase), keep this at a conservative version that does
/// NOT invite ncmpcpp to request capabilities we would then ACK on. Bump it to
/// `0.23.0` in lockstep with implementing `albumart` + filter parsing.
pub const ADVERTISED_MPD_VERSION: &str = "0.21.0";

/// The command surface, parsed from the wire.
///
/// FOUNDATION: this is the locked shape the dispatch + codec are written
/// against. It intentionally includes the ncmpcpp-blocking commands (see module
/// docs) as explicit variants so they can never silently fall into
/// `Unsupported` and hang the client.
#[derive(Debug, Clone)]
pub enum MpdCommand {
    // ── status / metadata ─────────────────────────────────────────────
    Status,
    Stats,
    CurrentSong,
    /// `idle [subsystems...]` - long-poll until a subsystem changes.
    Idle(Vec<String>),
    /// `noidle` - cancel a pending idle immediately.
    NoIdle,

    // ── playback ───────────────────────────────────────────────────────
    Play(Option<usize>),
    Pause(Option<bool>),
    Stop,
    Next,
    Previous,
    Seek {
        song_pos: usize,
        secs: f64,
    },
    SetVol(u8),

    // ── queue ──────────────────────────────────────────────────────────
    Add(String),
    /// `addid <uri> [pos]` - add and return the assigned song id.
    AddId(String, Option<usize>),
    Clear,
    /// `playlistinfo [pos|range]` - the current queue.
    PlaylistInfo(Option<String>),
    /// `plchanges <version>` - queue diff since a version. MUST return a
    /// well-formed response; a bad shape blanks ncmpcpp's playlist.
    PlChanges(u64),

    // ── stored playlists (ncmpcpp hangs if these error) ────────────────
    ListPlaylists,
    ListPlaylistInfo(String),
    Load(String),

    // ── db browse (backed by Subsonic browse/search3) ──────────────────
    LsInfo(Option<String>),
    ListAllInfo(Option<String>),
    /// `find <filter...>` / `search <filter...>` -> Subsonic search3.
    Find(String),
    Search(String),
    /// `list <tag> [filter]` -> Subsonic list/browse (e.g. `list genre`).
    List(String),

    // ── binary (distinct sub-protocol, see MpdResponse::Binary) ─────────
    /// `albumart <uri> <offset>` - raw cover bytes owned by us (get_cover_art
    /// returns `Bytes`, so we chunk them ourselves).
    AlbumArt(String, usize),
    /// `readpicture <uri> <offset>` - embedded picture, same framing.
    ReadPicture(String, usize),

    // ── capability probe (ncmpcpp fires these at connect) ──────────────
    Commands,
    NotCommands,
    TagTypes,
    Outputs,
    Decoders,
    UrlHandlers,

    /// A command we do not model yet. Dispatch decides ACK vs empty-OK; note
    /// that the ncmpcpp-blocking commands above are deliberately NOT here.
    Unsupported(String),
}

/// What a handler produces for one command.
///
/// Two shapes, because MPD has two: the normal `key: value` pairs terminated by
/// `OK`, and a BINARY response for `albumart`/`readpicture` which is framed as
/// `size: <total>\nbinary: <chunk_len>\n<raw bytes>\nOK\n`, chunked to the
/// negotiated `binarylimit`. Binary is not just another pair - it must be a
/// distinct variant so the codec knows to write raw bytes, not text.
#[derive(Debug)]
pub enum MpdResponse {
    /// Ordered `key: value` lines, serialized then terminated with `OK`.
    Pairs(Vec<(String, String)>),
    /// One chunk of a binary object. `total` is the full object size, `chunk`
    /// is this slice's bytes. The codec writes the `size:`/`binary:` header,
    /// the raw bytes, then `OK`. Repeated calls (with advancing offset in the
    /// command) stream the whole object under `binarylimit`.
    Binary {
        total: usize,
        chunk: Vec<u8>,
    },
    /// A protocol error: `ACK [code@list_idx] {command} message`.
    Ack {
        code: u32,
        command: String,
        message: String,
    },
}

impl MpdResponse {
    /// Convenience builder for a pairs response.
    pub fn pairs() -> PairsBuilder {
        PairsBuilder(Vec::new())
    }

    /// An empty successful response (just `OK`). This is the correct reply for
    /// e.g. an empty `listplaylists` - well-formed, so ncmpcpp does not hang.
    pub fn ok() -> Self {
        MpdResponse::Pairs(Vec::new())
    }
}

/// Small fluent builder so handlers read as `MpdResponse::pairs().pair(..).pair(..).build()`.
#[derive(Debug, Default)]
pub struct PairsBuilder(Vec<(String, String)>);

impl PairsBuilder {
    pub fn pair(mut self, k: &str, v: impl Into<String>) -> Self {
        self.0.push((k.to_string(), v.into()));
        self
    }
    pub fn build(self) -> MpdResponse {
        MpdResponse::Pairs(self.0)
    }
}

/// The trait the daemon implements to serve MPD.
///
/// Note the `&self`: MPD state (queue, current song, volume, idle subscriptions)
/// is SHARED across ALL client connections, not per-connection. So the handler
/// must be shared behind an `Arc` and mutate through interior mutability / an
/// actor, NOT `&mut self` (which would imply per-connection state and force
/// exclusive access the accept loop can't give). The concrete handler owns a
/// clone of the [`crate::player::PlayerHandle`] (player commands) and a
/// `SubsonicClient` (browse/search) - both are themselves `&self`-friendly, so
/// this composes cleanly.
#[allow(async_fn_in_trait)]
pub trait MpdHandler: Send + Sync {
    async fn handle(&self, cmd: MpdCommand) -> MpdResponse;
}

/// Entry point for the deferred server loop. Defined so `main` already
/// references the shape and so the bind address (never 6600 in dev) flows here.
pub struct MpdServer {
    pub bind: SocketAddr,
}

impl MpdServer {
    pub fn new(bind: SocketAddr) -> Self {
        Self { bind }
    }

    /// TODO(next-phase): tokio `TcpListener` accept loop. Per connection:
    ///   1. write `OK MPD {ADVERTISED_MPD_VERSION}\n` greeting;
    ///   2. read lines; parse each via `parse(line) -> MpdCommand`;
    ///   3. dispatch to `handler.handle`, serialize the `MpdResponse`
    ///      (Pairs -> text + `OK`; Binary -> framed bytes; Ack -> `ACK ...`);
    ///   4. support `command_list_begin`/`command_list_ok_begin`/`command_list_end`
    ///      batching, and `idle`/`noidle` long-poll against the shared handler's
    ///      change events.
    ///
    /// The handler is `Arc`-shared across all accepted connections (shared MPD
    /// state), which is exactly why `MpdHandler::handle` takes `&self`.
    pub async fn serve<H>(&self, _handler: std::sync::Arc<H>) -> anyhow::Result<()>
    where
        H: MpdHandler + 'static,
    {
        anyhow::bail!("MPD server loop not implemented yet (next-phase)")
    }
}
