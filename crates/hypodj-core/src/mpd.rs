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
use std::sync::Arc;

use tokio::io::{AsyncBufReadExt, AsyncWriteExt, BufReader};
use tokio::net::TcpListener;

/// Advertised MPD protocol version in the greeting.
///
/// IMPORTANT contract: the greeting version tells the client which syntax and
/// binary/filter capabilities the server claims. Advertising `0.23.0` promises
/// `albumart`/`readpicture` binary responses and the modern filter syntax. We
/// advertise a version we can actually back. As of Phase 3 the binary surface
/// (`albumart`/`readpicture` -> getCoverArt, chunked to `binarylimit`) and the
/// typed find/search tag filter ARE implemented, so `0.23.0` is now honest -
/// bumped in lockstep as the module contract mandates.
pub const ADVERTISED_MPD_VERSION: &str = "0.23.0";

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
    /// `ping` - no-op keepalive.
    Ping,
    /// `idle [subsystems...]` - long-poll until a subsystem changes.
    Idle(Vec<String>),
    /// `noidle` - cancel a pending idle immediately.
    NoIdle,

    // ── playback ───────────────────────────────────────────────────────
    Play(Option<usize>),
    /// `playid <id>` - play the queue entry with this song id.
    PlayId(Option<u64>),
    Pause(Option<bool>),
    Stop,
    Next,
    Previous,
    Seek {
        song_pos: usize,
        secs: f64,
    },
    /// `seekid <id> <secs>`
    SeekId {
        id: u64,
        secs: f64,
    },
    /// `seekcur <secs>` (absolute; leading `+`/`-` for relative is accepted but
    /// treated as absolute for now).
    SeekCur(f64),
    SetVol(u8),
    /// `getvol` - current volume.
    GetVol,

    // ── queue ──────────────────────────────────────────────────────────
    Add(String),
    /// `addid <uri> [pos]` - add and return the assigned song id.
    AddId(String, Option<usize>),
    Clear,
    /// `delete <pos|range>` - remove a queue entry.
    Delete(Option<String>),
    /// `playlistinfo [pos|range]` - the current queue.
    PlaylistInfo(Option<String>),
    /// `playlistid [id]` - the current queue, optionally one entry by id.
    PlaylistId(Option<u64>),
    /// `plchanges <version>` - queue diff since a version. MUST return a
    /// well-formed response; a bad shape blanks ncmpcpp's playlist.
    PlChanges(u64),

    // ── stored playlists (ncmpcpp hangs if these error) ────────────────
    ListPlaylists,
    ListPlaylistInfo(String),
    Load(String),
    /// `playlistadd <name> <uri>` - the `Starred` playlist is our star trigger:
    /// `playlistadd Starred song/<id>` stars the song server-side.
    PlaylistAdd(String, String),
    /// `playlistdelete <name> <pos>` - position-based (MPD has no uri delete).
    /// For `Starred`, the position maps back to a starred song id (re-fetched in
    /// the same order `listplaylistinfo` returned) -> unstar.
    PlaylistDelete(String, usize),
    /// `playlistclear <name>` - clear a stored playlist.
    PlaylistClear(String),

    // ── db browse (backed by Subsonic browse/search3) ──────────────────
    LsInfo(Option<String>),
    ListAllInfo(Option<String>),
    /// `find <filter...>` (exact) / `search <filter...>` (case-insensitive
    /// substring) -> Subsonic search3 + client-side tag post-filter. Carries the
    /// tag->value pairs verbatim (lowercased tag) so the dispatch can filter
    /// precisely; search3 itself is full-text only.
    Find(Vec<(String, String)>),
    Search(Vec<(String, String)>),
    /// `findadd <filter...>` (exact) / `searchadd <filter...>`
    /// (case-insensitive substring) -> the same Subsonic search3 + client-side
    /// tag post-filter as [`MpdCommand::Find`]/[`MpdCommand::Search`], but every
    /// matching song is appended to the play queue instead of listed. Carries
    /// the tag->value pairs verbatim (lowercased tag).
    FindAdd(Vec<(String, String)>),
    SearchAdd(Vec<(String, String)>),
    /// `count <filter...>` -> the same Subsonic search3 + client-side exact
    /// tag post-filter as [`MpdCommand::Find`], but instead of listing the
    /// songs it returns their tally and total playtime (`songs:`/`playtime:`).
    /// Carries the tag->value pairs verbatim (lowercased tag). The `count group
    /// <tag>` form is not modeled here (see the parser note); a plain filter is.
    Count(Vec<(String, String)>),
    /// `list <tag> [filter]` -> Subsonic list/browse (e.g. `list genre`). The
    /// optional filter narrows the listing (e.g. `list album artist "Tosca"` or
    /// the modern `list album "(artist == \"Tosca\")"`); `tag` is the thing to
    /// list, `filter` the tag->value constraints to honor.
    List {
        tag: String,
        filter: Vec<(String, String)>,
    },
    /// `sticker <subcmd> song <uri> [name] [value]` - MPD's per-song key/value
    /// store. We back ONLY the `rating` sticker (ncmpcpp's rating path) onto the
    /// Subsonic 0..=5 `setRating`/`userRating`. See [`StickerCmd`].
    Sticker(StickerCmd),

    // ── binary (distinct sub-protocol, see MpdResponse::Binary) ─────────
    /// `albumart <uri> <offset>` - raw cover bytes owned by us (get_cover_art
    /// returns `Bytes`, so we chunk them ourselves).
    AlbumArt(String, usize),
    /// `readpicture <uri> <offset>` - embedded picture, same framing.
    ReadPicture(String, usize),
    /// `binarylimit <bytes>` - client-negotiated max binary chunk size. ncmpcpp
    /// sends this before `albumart`. Applied per-connection (default 8192).
    BinaryLimit(usize),

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

/// The parsed `sticker` subcommand. MPD's sticker verb is
/// `sticker {get|set|delete|list|find} <type> <uri> [name] [value]`. We model
/// only `type == song` and (for get/set/delete) `name == rating`; anything else
/// dispatch answers with an empty-OK/ACK. `set` carries the parsed 0..=5 value.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum StickerCmd {
    /// `sticker get song <uri> rating`
    Get { uri: String },
    /// `sticker set song <uri> rating <0-5>`
    Set { uri: String, value: u8 },
    /// `sticker delete song <uri> rating` (clears -> setRating 0)
    Delete { uri: String },
    /// `sticker list song <uri>` - list the rating sticker if set.
    List { uri: String },
    /// A sticker verb/type/name we do not model. Dispatch answers empty-OK so a
    /// client probing sticker support does not hang.
    Unsupported,
}

/// Parse the argument vector of a `sticker` command into a [`StickerCmd`]. Only
/// `type == song` and (where a name is required) `name == rating` are honored;
/// everything else maps to [`StickerCmd::Unsupported`].
fn parse_sticker(args: &[String]) -> StickerCmd {
    let a = |i: usize| args.get(i).map(String::as_str);
    let sub = a(0).unwrap_or("").to_lowercase();
    let ty = a(1).unwrap_or("");
    if ty != "song" {
        return StickerCmd::Unsupported;
    }
    let uri = match a(2) {
        Some(u) => u.to_string(),
        None => return StickerCmd::Unsupported,
    };
    let name_is_rating = a(3).map(|n| n.eq_ignore_ascii_case("rating")).unwrap_or(false);
    match sub.as_str() {
        "get" if name_is_rating => StickerCmd::Get { uri },
        "delete" if name_is_rating => StickerCmd::Delete { uri },
        "list" => StickerCmd::List { uri },
        "set" if name_is_rating => match a(4).and_then(|v| v.parse::<u8>().ok()) {
            Some(v) => StickerCmd::Set { uri, value: v.min(5) },
            None => StickerCmd::Unsupported,
        },
        _ => StickerCmd::Unsupported,
    }
}

/// Tokenize an MPD request line, honoring double-quoted arguments (MPD quotes
/// any arg containing spaces; `\"` and `\\` are the only escapes). Returns the
/// bare command name lowercased plus the raw argument vector.
fn tokenize(line: &str) -> Option<(String, Vec<String>)> {
    let mut toks: Vec<String> = Vec::new();
    let mut chars = line.chars().peekable();
    loop {
        // skip whitespace
        while matches!(chars.peek(), Some(c) if c.is_whitespace()) {
            chars.next();
        }
        match chars.peek() {
            None => break,
            Some('"') => {
                chars.next();
                let mut s = String::new();
                while let Some(c) = chars.next() {
                    match c {
                        '"' => break,
                        '\\' => {
                            if let Some(n) = chars.next() {
                                s.push(n);
                            }
                        }
                        _ => s.push(c),
                    }
                }
                toks.push(s);
            }
            Some(_) => {
                let mut s = String::new();
                while let Some(&c) = chars.peek() {
                    if c.is_whitespace() {
                        break;
                    }
                    s.push(c);
                    chars.next();
                }
                toks.push(s);
            }
        }
    }
    if toks.is_empty() {
        return None;
    }
    let name = toks.remove(0).to_lowercase();
    Some((name, toks))
}

/// Parse one request line into an [`MpdCommand`]. Never fails: an unknown or
/// malformed command becomes [`MpdCommand::Unsupported`] (dispatch decides ACK
/// vs empty-OK), so the accept loop never panics on bad input.
pub fn parse(line: &str) -> MpdCommand {
    let (name, args) = match tokenize(line) {
        Some(t) => t,
        None => return MpdCommand::Unsupported(String::new()),
    };
    let arg = |i: usize| args.get(i).cloned();
    match name.as_str() {
        "status" => MpdCommand::Status,
        "stats" => MpdCommand::Stats,
        "currentsong" => MpdCommand::CurrentSong,
        "ping" => MpdCommand::Ping,
        "idle" => MpdCommand::Idle(args.clone()),
        "noidle" => MpdCommand::NoIdle,
        "play" => MpdCommand::Play(arg(0).and_then(|s| s.parse().ok())),
        "playid" => MpdCommand::PlayId(arg(0).and_then(|s| s.parse().ok())),
        "pause" => MpdCommand::Pause(arg(0).and_then(|s| match s.as_str() {
            "1" => Some(true),
            "0" => Some(false),
            _ => None,
        })),
        "stop" => MpdCommand::Stop,
        "next" => MpdCommand::Next,
        "previous" => MpdCommand::Previous,
        "seek" => match (arg(0).and_then(|s| s.parse().ok()), arg(1).and_then(|s| s.parse().ok())) {
            (Some(song_pos), Some(secs)) => MpdCommand::Seek { song_pos, secs },
            _ => MpdCommand::Unsupported(line.to_string()),
        },
        "seekid" => match (arg(0).and_then(|s| s.parse().ok()), arg(1).and_then(|s| s.parse().ok())) {
            (Some(id), Some(secs)) => MpdCommand::SeekId { id, secs },
            _ => MpdCommand::Unsupported(line.to_string()),
        },
        "seekcur" => match arg(0).and_then(|s| s.trim_start_matches(['+', '-']).parse().ok()) {
            Some(secs) => MpdCommand::SeekCur(secs),
            None => MpdCommand::Unsupported(line.to_string()),
        },
        "setvol" => match arg(0).and_then(|s| s.parse().ok()) {
            Some(v) => MpdCommand::SetVol(v),
            None => MpdCommand::Unsupported(line.to_string()),
        },
        "getvol" => MpdCommand::GetVol,
        "add" => MpdCommand::Add(arg(0).unwrap_or_default()),
        "addid" => MpdCommand::AddId(arg(0).unwrap_or_default(), arg(1).and_then(|s| s.parse().ok())),
        "clear" => MpdCommand::Clear,
        "delete" => MpdCommand::Delete(arg(0)),
        "playlistinfo" => MpdCommand::PlaylistInfo(arg(0)),
        "playlistid" => MpdCommand::PlaylistId(arg(0).and_then(|s| s.parse().ok())),
        "plchanges" => MpdCommand::PlChanges(arg(0).and_then(|s| s.parse().ok()).unwrap_or(0)),
        "listplaylists" => MpdCommand::ListPlaylists,
        "listplaylistinfo" => MpdCommand::ListPlaylistInfo(arg(0).unwrap_or_default()),
        "load" => MpdCommand::Load(arg(0).unwrap_or_default()),
        "playlistadd" => MpdCommand::PlaylistAdd(arg(0).unwrap_or_default(), arg(1).unwrap_or_default()),
        "playlistdelete" => MpdCommand::PlaylistDelete(
            arg(0).unwrap_or_default(),
            arg(1).and_then(|s| s.parse().ok()).unwrap_or(0),
        ),
        "playlistclear" => MpdCommand::PlaylistClear(arg(0).unwrap_or_default()),
        "lsinfo" => MpdCommand::LsInfo(arg(0)),
        "listall" | "listallinfo" => MpdCommand::ListAllInfo(arg(0)),
        // find/search take `TAG VALUE ...` filters; keep the tag->value pairs so
        // dispatch can post-filter search3 (full-text) with MPD-tag precision.
        "find" => MpdCommand::Find(parse_filter(&args)),
        "search" => MpdCommand::Search(parse_filter(&args)),
        "findadd" => MpdCommand::FindAdd(parse_filter(&args)),
        "searchadd" => MpdCommand::SearchAdd(parse_filter(&args)),
        // count takes the same `TAG VALUE ...` filters as find, optionally
        // followed by `group <tag>`. We do not tally per-group (that would need
        // one search3 per group value), so a trailing `group <tag>` is dropped
        // and the plain overall count is returned - honest and cheap.
        "count" => MpdCommand::Count(parse_filter(&strip_group(&args))),
        "list" => {
            let tag = args.first().cloned().unwrap_or_default().to_lowercase();
            let filter = parse_list_filter(&args[args.len().min(1)..]);
            MpdCommand::List { tag, filter }
        }
        "sticker" => MpdCommand::Sticker(parse_sticker(&args)),
        "albumart" => MpdCommand::AlbumArt(arg(0).unwrap_or_default(), arg(1).and_then(|s| s.parse().ok()).unwrap_or(0)),
        "readpicture" => MpdCommand::ReadPicture(arg(0).unwrap_or_default(), arg(1).and_then(|s| s.parse().ok()).unwrap_or(0)),
        "binarylimit" => MpdCommand::BinaryLimit(arg(0).and_then(|s| s.parse().ok()).unwrap_or(8192)),
        "commands" => MpdCommand::Commands,
        "notcommands" => MpdCommand::NotCommands,
        "tagtypes" => MpdCommand::TagTypes,
        "outputs" => MpdCommand::Outputs,
        "decoders" => MpdCommand::Decoders,
        "urlhandlers" => MpdCommand::UrlHandlers,
        _ => MpdCommand::Unsupported(name),
    }
}

#[cfg(test)]
mod parse_tests {
    use super::*;

    #[test]
    fn tokenizes_quoted_args() {
        let (name, args) = tokenize(r#"add "song/al 1/track 2""#).unwrap();
        assert_eq!(name, "add");
        assert_eq!(args, vec!["song/al 1/track 2".to_string()]);
    }

    #[test]
    fn parses_core_commands() {
        assert!(matches!(parse("status"), MpdCommand::Status));
        assert!(matches!(parse("ping"), MpdCommand::Ping));
        assert!(matches!(parse("play 3"), MpdCommand::Play(Some(3))));
        assert!(matches!(parse("play"), MpdCommand::Play(None)));
        assert!(matches!(parse("setvol 42"), MpdCommand::SetVol(42)));
        assert!(matches!(parse("pause 1"), MpdCommand::Pause(Some(true))));
        assert!(matches!(parse("playid 7"), MpdCommand::PlayId(Some(7))));
    }

    #[test]
    fn parses_lsinfo_and_add() {
        match parse(r#"lsinfo "artist/ar-9""#) {
            MpdCommand::LsInfo(Some(p)) => assert_eq!(p, "artist/ar-9"),
            other => panic!("got {other:?}"),
        }
        match parse("addid song/so-1") {
            MpdCommand::AddId(uri, None) => assert_eq!(uri, "song/so-1"),
            other => panic!("got {other:?}"),
        }
    }

    #[test]
    fn unknown_command_is_unsupported_not_panic() {
        assert!(matches!(parse("frobnicate x y"), MpdCommand::Unsupported(_)));
        assert!(matches!(parse(""), MpdCommand::Unsupported(_)));
    }

    #[test]
    fn search_filter_keeps_tag_value_pairs() {
        // `search Title foo Artist bar` -> [(title,foo),(artist,bar)] so dispatch
        // can post-filter search3 with MPD-tag precision.
        match parse("search Title foo Artist bar") {
            MpdCommand::Search(pairs) => {
                assert_eq!(
                    pairs,
                    vec![
                        ("title".to_string(), "foo".to_string()),
                        ("artist".to_string(), "bar".to_string()),
                    ]
                );
            }
            other => panic!("got {other:?}"),
        }
    }

    #[test]
    fn bare_search_value_files_under_any() {
        match parse("search kalabrese") {
            MpdCommand::Search(pairs) => {
                assert_eq!(pairs, vec![("any".to_string(), "kalabrese".to_string())]);
            }
            other => panic!("got {other:?}"),
        }
    }

    #[test]
    fn findadd_searchadd_keep_tag_value_pairs() {
        match parse("findadd Artist bar Album baz") {
            MpdCommand::FindAdd(pairs) => assert_eq!(
                pairs,
                vec![
                    ("artist".to_string(), "bar".to_string()),
                    ("album".to_string(), "baz".to_string()),
                ]
            ),
            other => panic!("got {other:?}"),
        }
        match parse("searchadd kalabrese") {
            MpdCommand::SearchAdd(pairs) => {
                assert_eq!(pairs, vec![("any".to_string(), "kalabrese".to_string())]);
            }
            other => panic!("got {other:?}"),
        }
    }

    #[test]
    fn count_keeps_tag_value_pairs() {
        // `count Artist bar Album baz` -> the same filter pairs as find, so
        // dispatch can post-filter search3 and tally the matches.
        match parse("count Artist bar Album baz") {
            MpdCommand::Count(pairs) => assert_eq!(
                pairs,
                vec![
                    ("artist".to_string(), "bar".to_string()),
                    ("album".to_string(), "baz".to_string()),
                ]
            ),
            other => panic!("got {other:?}"),
        }
    }

    #[test]
    fn count_drops_trailing_group_clause() {
        // `count Artist bar group album` -> the `group album` clause is dropped
        // (we tally overall, not per-group), leaving just the filter.
        match parse("count Artist bar group album") {
            MpdCommand::Count(pairs) => {
                assert_eq!(pairs, vec![("artist".to_string(), "bar".to_string())]);
            }
            other => panic!("got {other:?}"),
        }
    }

    #[test]
    fn count_keeps_group_valued_filter() {
        // A filter VALUE that is literally "group" (odd slot) must be preserved,
        // not mistaken for the grouping clause.
        match parse("count Artist group") {
            MpdCommand::Count(pairs) => {
                assert_eq!(pairs, vec![("artist".to_string(), "group".to_string())]);
            }
            other => panic!("got {other:?}"),
        }
    }

    #[test]
    fn list_parses_tag_only() {
        match parse("list album") {
            MpdCommand::List { tag, filter } => {
                assert_eq!(tag, "album");
                assert!(filter.is_empty());
            }
            other => panic!("got {other:?}"),
        }
    }

    #[test]
    fn list_parses_positional_filter() {
        match parse(r#"list album artist "Tosca""#) {
            MpdCommand::List { tag, filter } => {
                assert_eq!(tag, "album");
                assert_eq!(filter, vec![("artist".to_string(), "Tosca".to_string())]);
            }
            other => panic!("got {other:?}"),
        }
    }

    #[test]
    fn list_strips_group_suffix() {
        // `list album group albumartist` -> the `group` clause must be dropped,
        // leaving an EMPTY filter (a whole-library album listing), not a bogus
        // (any, group)/(any, albumartist) filter that would return empty.
        match parse("list album group albumartist") {
            MpdCommand::List { tag, filter } => {
                assert_eq!(tag, "album");
                assert!(filter.is_empty(), "group clause must be stripped, got {filter:?}");
            }
            other => panic!("got {other:?}"),
        }
        // A real filter followed by a group clause keeps only the filter.
        match parse(r#"list album artist "X" group albumartist"#) {
            MpdCommand::List { tag, filter } => {
                assert_eq!(tag, "album");
                assert_eq!(filter, vec![("artist".to_string(), "X".to_string())]);
            }
            other => panic!("got {other:?}"),
        }
        // A filter VALUE literally equal to "group" must be KEPT, not treated as
        // the start of a grouping clause (group only cuts at a tag slot).
        match parse("list album artist group") {
            MpdCommand::List { tag, filter } => {
                assert_eq!(tag, "album");
                assert_eq!(filter, vec![("artist".to_string(), "group".to_string())]);
            }
            other => panic!("got {other:?}"),
        }
    }

    #[test]
    fn list_bare_positional_album_files_under_any() {
        // Classic 2-arg `list album <ARTIST>` files the bare value under `any`;
        // the handler then treats an `any` value as an artist name.
        match parse("list album Tosca") {
            MpdCommand::List { tag, filter } => {
                assert_eq!(tag, "album");
                assert_eq!(filter, vec![("any".to_string(), "Tosca".to_string())]);
            }
            other => panic!("got {other:?}"),
        }
    }

    #[test]
    fn list_parses_expression_filter() {
        match parse(r#"list album "(artist == \"Tosca\")""#) {
            MpdCommand::List { tag, filter } => {
                assert_eq!(tag, "album");
                assert_eq!(filter, vec![("artist".to_string(), "Tosca".to_string())]);
            }
            other => panic!("got {other:?}"),
        }
    }

    #[test]
    fn parses_binarylimit_and_playlistadd() {
        assert!(matches!(parse("binarylimit 4096"), MpdCommand::BinaryLimit(4096)));
        match parse("playlistadd Starred song/so-1") {
            MpdCommand::PlaylistAdd(name, uri) => {
                assert_eq!(name, "Starred");
                assert_eq!(uri, "song/so-1");
            }
            other => panic!("got {other:?}"),
        }
    }

    #[test]
    fn parses_sticker_rating_verbs() {
        match parse("sticker set song song/so-1 rating 4") {
            MpdCommand::Sticker(StickerCmd::Set { uri, value }) => {
                assert_eq!(uri, "song/so-1");
                assert_eq!(value, 4);
            }
            other => panic!("got {other:?}"),
        }
        match parse("sticker get song song/so-1 rating") {
            MpdCommand::Sticker(StickerCmd::Get { uri }) => assert_eq!(uri, "song/so-1"),
            other => panic!("got {other:?}"),
        }
        match parse("sticker delete song song/so-1 rating") {
            MpdCommand::Sticker(StickerCmd::Delete { uri }) => assert_eq!(uri, "song/so-1"),
            other => panic!("got {other:?}"),
        }
        match parse("sticker list song song/so-1") {
            MpdCommand::Sticker(StickerCmd::List { uri }) => assert_eq!(uri, "song/so-1"),
            other => panic!("got {other:?}"),
        }
        // value clamps to 5; a non-song type or non-rating name is Unsupported.
        assert!(matches!(
            parse("sticker set song song/so-1 rating 9"),
            MpdCommand::Sticker(StickerCmd::Set { value: 5, .. })
        ));
        assert!(matches!(
            parse("sticker set song song/so-1 mood happy"),
            MpdCommand::Sticker(StickerCmd::Unsupported)
        ));
        assert!(matches!(
            parse("sticker get playlist foo rating"),
            MpdCommand::Sticker(StickerCmd::Unsupported)
        ));
    }

    #[test]
    fn ack_serialization_shape() {
        let mut buf = Vec::new();
        let ok = write_response(
            &mut buf,
            &MpdResponse::Ack {
                code: 5,
                command: "frob".into(),
                message: "unknown command \"frob\"".into(),
            },
            false,
            0,
        );
        assert!(!ok);
        assert_eq!(
            String::from_utf8(buf).unwrap(),
            "ACK [5@0] {frob} unknown command \"frob\"\n"
        );
    }

    #[test]
    fn pairs_serialization_appends_no_ok_here() {
        let mut buf = Vec::new();
        let ok = write_response(
            &mut buf,
            &MpdResponse::pairs().pair("volume", "50").build(),
            false,
            0,
        );
        assert!(ok);
        assert_eq!(String::from_utf8(buf).unwrap(), "volume: 50\n");
    }
}

/// Known MPD filter tag names (lowercased). A token equal to one of these
/// begins a `TAG VALUE` pair; anything else is treated as a bare value under the
/// `any` tag.
const FILTER_TAGS: &[&str] = &[
    "any", "title", "artist", "album", "albumartist", "track", "genre", "date",
    "composer", "performer", "comment", "disc", "file", "base", "modified-since",
    "albumartistsort", "artistsort",
];

/// Parse a `find`/`search` filter arg list into `(tag, value)` pairs, preserving
/// the tag so dispatch can post-filter with MPD-tag precision (search3 itself is
/// full-text only). `search TITLE foo ARTIST bar` -> `[(title,foo),(artist,bar)]`.
/// A bare leading value (no tag) is filed under `any`.
fn parse_filter(args: &[String]) -> Vec<(String, String)> {
    let mut out: Vec<(String, String)> = Vec::new();
    let mut i = 0;
    while i < args.len() {
        let lower = args[i].to_lowercase();
        if FILTER_TAGS.contains(&lower.as_str()) {
            let value = args.get(i + 1).cloned().unwrap_or_default();
            out.push((lower, value));
            i += 2;
        } else {
            // bare value -> `any`
            out.push(("any".to_string(), args[i].clone()));
            i += 1;
        }
    }
    out
}

/// Drop a trailing `group <tag>` clause from a `count` arg list, returning the
/// filter portion only. Same rule as `parse_list_filter`: `group` begins the
/// clause only when it lands on a tag slot (even index), so a filter VALUE that
/// is literally "group" (odd index) is preserved.
fn strip_group(args: &[String]) -> Vec<String> {
    let cut = args
        .iter()
        .enumerate()
        .position(|(i, t)| i % 2 == 0 && t.eq_ignore_ascii_case("group"));
    match cut {
        Some(pos) => args[..pos].to_vec(),
        None => args.to_vec(),
    }
}

/// Parse the filter remainder of a `list <tag> [filter]` request into
/// `(tag, value)` pairs. Two forms are supported:
///   - classic positional, `list album artist "Tosca"` -> the remainder is
///     `artist "Tosca"`, parsed like a find/search filter;
///   - modern expression, `list album "(artist == \"Tosca\")"` -> the remainder
///     is the single token `(artist == "Tosca")`, parsed here into
///     `[(artist, Tosca)]`.
/// An empty remainder yields no filter (the whole-library listing).
///
/// MPD's `group <tag>` suffix (e.g. `list album group albumartist`) is stripped
/// before filter parsing: the grouping clause always trails the real filter, so
/// everything from `group` onward is dropped rather than mis-parsed into a bogus
/// `(any, group)` / `(any, albumartist)` filter that would return empty.
fn parse_list_filter(rest: &[String]) -> Vec<(String, String)> {
    // Drop a trailing `group <tag>` clause (we do not yet honor grouping, but we
    // must not let it corrupt the filter). MPD grouping trails the `TAG VALUE`
    // filter pairs, so `group` only begins the clause when it lands on a tag slot
    // (even index); a filter VALUE that is literally "group" sits on an odd index
    // and must be kept.
    let cut = rest
        .iter()
        .enumerate()
        .position(|(i, t)| i % 2 == 0 && t.eq_ignore_ascii_case("group"));
    let rest = match cut {
        Some(pos) => &rest[..pos],
        None => rest,
    };
    // Modern single-arg expression form: `(tag == "value")`.
    if rest.len() == 1 && rest[0].contains("==") {
        if let Some(pair) = parse_filter_expression(&rest[0]) {
            return vec![pair];
        }
    }
    parse_filter(rest)
}

/// Parse a single MPD filter expression `(tag == "value")` (also tolerating
/// missing outer parens / quotes) into one `(tag, value)` pair. Only the flat
/// `==` equality form is modeled; anything else yields `None`.
fn parse_filter_expression(expr: &str) -> Option<(String, String)> {
    let inner = expr.trim().trim_start_matches('(').trim_end_matches(')');
    let (tag, value) = inner.split_once("==")?;
    let tag = tag.trim().to_lowercase();
    let value = value.trim().trim_matches('"').to_string();
    if tag.is_empty() {
        return None;
    }
    Some((tag, value))
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
pub trait MpdHandler: Send + Sync {
    fn handle(&self, cmd: MpdCommand) -> impl std::future::Future<Output = MpdResponse> + Send;

    /// Block until one of `subsystems` (empty = all) changes, returning the name
    /// of the changed subsystem, or `None` if it should return with no change.
    /// The serve loop separately races this against the client sending `noidle`
    /// or any other line, so a correct minimal implementation may simply await a
    /// real change event. Default: park forever (relies on the noidle race).
    fn idle(
        &self,
        subsystems: Vec<String>,
    ) -> impl std::future::Future<Output = Option<String>> + Send {
        async move {
            let _ = subsystems;
            std::future::pending::<()>().await;
            None
        }
    }
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

    /// tokio `TcpListener` accept loop. Per connection: write the greeting, read
    /// lines, parse each via [`parse`], dispatch to `handler.handle`, serialize
    /// the [`MpdResponse`]. Supports `command_list_begin` /
    /// `command_list_ok_begin` / `command_list_end` batching and `idle`/`noidle`.
    ///
    /// The handler is `Arc`-shared across all accepted connections (shared MPD
    /// state), which is exactly why [`MpdHandler::handle`] takes `&self`. A bad
    /// command yields an `ACK`, never a panic or a dropped accept loop.
    pub async fn serve<H>(&self, handler: Arc<H>) -> anyhow::Result<()>
    where
        H: MpdHandler + 'static,
    {
        let listener = TcpListener::bind(self.bind).await?;
        tracing::info!(bind = %self.bind, "MPD server listening");
        loop {
            let (sock, peer) = match listener.accept().await {
                Ok(v) => v,
                Err(e) => {
                    tracing::warn!(error = %e, "accept failed");
                    continue;
                }
            };
            let handler = handler.clone();
            tokio::spawn(async move {
                if let Err(e) = serve_conn(sock, handler).await {
                    tracing::debug!(%peer, error = %e, "connection closed");
                }
            });
        }
    }
}

/// Serialize an [`MpdResponse`] for a single (non-list) command into the write
/// buffer, appending the terminating `OK\n` for success. For `Ack`, only the
/// ACK line is written (no `OK`).
fn write_response(buf: &mut Vec<u8>, resp: &MpdResponse, list_ok: bool, idx: usize) -> bool {
    match resp {
        MpdResponse::Pairs(pairs) => {
            for (k, v) in pairs {
                buf.extend_from_slice(format!("{k}: {v}\n").as_bytes());
            }
            if list_ok {
                buf.extend_from_slice(b"list_OK\n");
            }
            true
        }
        MpdResponse::Binary { total, chunk } => {
            buf.extend_from_slice(format!("size: {total}\n").as_bytes());
            buf.extend_from_slice(format!("binary: {}\n", chunk.len()).as_bytes());
            buf.extend_from_slice(chunk);
            buf.push(b'\n');
            if list_ok {
                buf.extend_from_slice(b"list_OK\n");
            }
            true
        }
        MpdResponse::Ack { code, command, message } => {
            buf.extend_from_slice(
                format!("ACK [{code}@{idx}] {{{command}}} {message}\n").as_bytes(),
            );
            false
        }
    }
}

/// Drive one client connection: greeting, then a request loop honoring command
/// lists and idle.
async fn serve_conn<H>(sock: tokio::net::TcpStream, handler: Arc<H>) -> anyhow::Result<()>
where
    H: MpdHandler + 'static,
{
    let (rd, mut wr) = sock.into_split();
    let mut reader = BufReader::new(rd);
    wr.write_all(format!("OK MPD {ADVERTISED_MPD_VERSION}\n").as_bytes())
        .await?;
    wr.flush().await?;

    let mut line = String::new();
    loop {
        line.clear();
        let n = reader.read_line(&mut line).await?;
        if n == 0 {
            break; // client closed
        }
        let trimmed = line.trim_end_matches(['\r', '\n']);

        // ── command list batching ─────────────────────────────────────────
        if trimmed == "command_list_begin" || trimmed == "command_list_ok_begin" {
            let list_ok = trimmed == "command_list_ok_begin";
            let mut cmds: Vec<String> = Vec::new();
            loop {
                let mut l = String::new();
                let n = reader.read_line(&mut l).await?;
                if n == 0 {
                    return Ok(());
                }
                let t = l.trim_end_matches(['\r', '\n']).to_string();
                if t == "command_list_end" {
                    break;
                }
                cmds.push(t);
            }
            let mut buf = Vec::new();
            let mut ok = true;
            for (idx, c) in cmds.iter().enumerate() {
                let cmd = parse(c);
                let resp = handler.handle(cmd).await;
                if !write_response(&mut buf, &resp, list_ok, idx) {
                    ok = false;
                    break; // MPD aborts the list on first error
                }
            }
            if ok {
                buf.extend_from_slice(b"OK\n");
            }
            wr.write_all(&buf).await?;
            wr.flush().await?;
            continue;
        }

        // ── idle: block until a subsystem changes, or noidle ──────────────
        let cmd = parse(trimmed);
        if let MpdCommand::Idle(subsystems) = &cmd {
            // Race a real change event against the client sending another line
            // (typically `noidle`). Whichever wins ends the idle. If the client
            // sends a line, it is consumed here; `noidle` yields no change, any
            // other command is ignored for simplicity (ncmpcpp only sends
            // noidle to break idle).
            let mut peek = String::new();
            let changed = tokio::select! {
                sys = handler.idle(subsystems.clone()) => sys,
                r = reader.read_line(&mut peek) => {
                    match r {
                        Ok(0) => break,
                        Ok(_) => None, // noidle (or any interrupt): no change
                        Err(e) => return Err(e.into()),
                    }
                }
            };
            let mut buf = Vec::new();
            if let Some(sys) = changed {
                buf.extend_from_slice(format!("changed: {sys}\n").as_bytes());
            }
            buf.extend_from_slice(b"OK\n");
            wr.write_all(&buf).await?;
            wr.flush().await?;
            continue;
        }
        if let MpdCommand::NoIdle = cmd {
            wr.write_all(b"OK\n").await?;
            wr.flush().await?;
            continue;
        }

        let resp = handler.handle(cmd).await;
        let mut buf = Vec::new();
        if write_response(&mut buf, &resp, false, 0) {
            buf.extend_from_slice(b"OK\n");
        }
        wr.write_all(&buf).await?;
        wr.flush().await?;
    }
    Ok(())
}
