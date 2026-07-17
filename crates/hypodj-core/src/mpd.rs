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
use std::time::Duration;

use tokio::io::{AsyncBufReadExt, AsyncWriteExt, BufReader};
use tokio::net::TcpListener;

use crate::plan::{
    Action, ClearScope, FadeIntentIr, MoveDest, PlanId, PosBase, QueueSelector, RawPlan,
    RawTrigger, Selector, TrackSel,
};

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
    /// `seekcur <secs>`. A leading `+`/`-` means RELATIVE to the live position
    /// (`relative = true`, `secs` keeps its sign); a bare number is ABSOLUTE.
    SeekCur {
        secs: f64,
        relative: bool,
    },
    SetVol(u8),
    /// `getvol` - current volume.
    GetVol,
    /// `knob up|down` - one equal-loudness (dB) potentiometer detent. A jukebox-only
    /// relative control: down past the audible floor is the off-click that pauses,
    /// up from paused resumes. See [`KnobDir`].
    Knob(KnobDir),
    /// `random <0|1>` - toggle shuffled playback order.
    Random(bool),
    /// `repeat <0|1>` - toggle looping the queue at its end.
    Repeat(bool),
    /// `single <0|1|oneshot>` - stop (or repeat one) after the current track.
    /// `oneshot` is accepted (ncmpcpp sends it) and mapped to `true`.
    Single(bool),
    /// `consume <0|1>` - remove each entry from the queue once it has played.
    Consume(bool),

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
    /// `save <name>` - persist the CURRENT QUEUE as a new named Navidrome
    /// playlist (GAP cusq3zaw). The synthetic `Starred` name stays reserved to
    /// the star path and is never saved over.
    Save(String),
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

    /// `fade out|in|to ...` - drive the startle-safe volume envelope
    /// ([`crate::fade`]). An immediate, cancellable, mid-track fade. See
    /// [`FadeArgs`]. NOT a standard MPD command; a hypodj extension.
    Fade(FadeArgs),

    /// `plan add|list|cancel|replace ...` - the P2 deterministic plan IR
    /// ([`crate::plan`]). NOT a standard MPD command; a hypodj extension. See
    /// [`PlanCmd`] and [`parse_plan`].
    Plan(PlanCmd),

    /// `nl "<request>"` / `nl confirm <token>` / `nl cancel <token>` - the P3
    /// OPTIONAL natural-language surface. Translate EMITS a validated plan echoed
    /// for confirmation; confirm arms it via the P2 registry; cancel drops the
    /// token. NOT a standard MPD command; a hypodj extension. See [`NlCmd`].
    Nl(NlCmd),

    /// `sleep [<dur>|off|cancel]` - the convenience sleep timer (a P2 plan
    /// BUILDER). NOT a standard MPD command; a hypodj extension. See [`SleepCmd`].
    Sleep(SleepCmd),
    /// `winddown [<dur>|off|cancel]` - the convenience wind-down to the non-silence
    /// floor. NOT a standard MPD command; a hypodj extension. See [`WinddownCmd`].
    Winddown(WinddownCmd),
    /// `wake at <time>|in <dur> [with <selector>]` / `wake [list|cancel|off]` - the
    /// convenience wake ramp-in. NOT a standard MPD command; a hypodj extension.
    /// See [`WakeCmd`].
    Wake(WakeCmd),

    /// `field` / `field <word...>` / `field less|more|back` / `field clear` - the
    /// latent-field FIRST SLICE: SET a decaying pull over P4 selection, SEE the live
    /// pulls, one-nudge correct the most-recent one, or clear them. NOT a standard
    /// MPD command; a hypodj extension. Biases candidate ranking ONLY - never
    /// mutates the queue, never arms. See [`FieldCmd`] and [`parse_field`].
    Field(FieldCmd),

    /// A command we do not model yet. Dispatch decides ACK vs empty-OK; note
    /// that the ncmpcpp-blocking commands above are deliberately NOT here.
    Unsupported(String),
}

/// A parsed `field` subcommand (the latent-field first-slice surface).
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum FieldCmd {
    /// `field` - render the live pulls with provenance + decayed strength (SEE).
    Status,
    /// `field <word...>` - SET a pull toward a named direction via the lexicon.
    Set(String),
    /// `field less|back|more` - one-nudge correction of the most-recent pull.
    Nudge(FieldNudge),
    /// `field clear` - drop every pull (correct the system's beliefs, non-destructive).
    Clear,
}

/// The one-nudge correction directions for `field`.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum FieldNudge {
    /// `less` / `back` - attenuate the most-recent pull (halve it).
    Less,
    /// `more` - strengthen the most-recent pull.
    More,
}

/// Parse a `field` request. `field` alone reads the field; the reserved keywords
/// `clear`, `less`, `back`, `more` are the correction/clear verbs; anything else is
/// a SET whose joined text is mapped through the mood lexicon by the handler (an
/// unmapped word yields the honest "no pull felt" echo, never an error here).
fn parse_field(args: &[String], _line: &str) -> MpdCommand {
    if args.is_empty() {
        return MpdCommand::Field(FieldCmd::Status);
    }
    if args.len() == 1 {
        match args[0].to_lowercase().as_str() {
            "clear" => return MpdCommand::Field(FieldCmd::Clear),
            "less" | "back" => return MpdCommand::Field(FieldCmd::Nudge(FieldNudge::Less)),
            "more" => return MpdCommand::Field(FieldCmd::Nudge(FieldNudge::More)),
            _ => {}
        }
    }
    MpdCommand::Field(FieldCmd::Set(args.join(" ")))
}

/// Which way the physical-potentiometer knob turns. One press = one equal-loudness
/// detent; the server owns the dB math and the off-click pause decision.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum KnobDir {
    /// Louder by one detent (or resume, if paused).
    Up,
    /// Quieter by one detent (or the off-click pause, if already at the floor).
    Down,
}

/// A parsed `sleep` subcommand (the convenience sleep-timer surface).
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum SleepCmd {
    /// `sleep` - report the remaining time on the armed sleep timer (or none).
    Status,
    /// `sleep off` / `sleep cancel` - cancel the armed sleep timer.
    Cancel,
    /// `sleep <dur>` - (re)arm a sleep timer firing in `dur`.
    Set(Duration),
}

/// A parsed `winddown` subcommand.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum WinddownCmd {
    /// `winddown off` / `winddown cancel` - cancel the armed wind-down.
    Cancel,
    /// `winddown` (immediate) or `winddown <dur>` (scheduled at now+dur).
    Set(Option<Duration>),
}

/// When a `wake` fires: an absolute civil `h:m` today/tomorrow, or `in <dur>`.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum WakeWhen {
    /// `wake at <time>` - the next future civil `h:m`.
    At { h: u32, m: u32 },
    /// `wake in <dur>` - a monotonic span from now.
    In(Duration),
}

/// A parsed `wake` subcommand (the convenience wake ramp-in surface).
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum WakeCmd {
    /// `wake` / `wake list` - report the remaining time on the armed wake (or none).
    Status,
    /// `wake off` / `wake cancel` - cancel the armed wake.
    Cancel,
    /// `wake at <time>|in <dur> [with <selector>]` - (re)arm the wake.
    Set {
        when: WakeWhen,
        /// The optional `with <text>` query selector (P4 Calmer routing aside).
        selector: Option<String>,
        count: u32,
    },
}

/// Default enqueue count for a `wake ... with <selector>` (append-only, clamped).
const DEFAULT_WAKE_ENQUEUE: u32 = 20;

/// Which direction / target a `fade` drives toward. `To(vol)` fades to an
/// explicit 0..=100 volume; `ToFloor` winds down to the configured non-silence
/// floor (`floor_level_db`) leaving playback running; `Out` ramps to silence
/// (then stops + restores the pre-fade volume); `In` ramps up from the current
/// level to the comfort ceiling.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum FadeKind {
    To(u8),
    /// `fade to floor` - wind down to the configured `floor_level_db`.
    ToFloor,
    Out,
    In,
}

/// A parsed `fade` request: the direction and the RAW, optional duration.
///
/// The parser is deliberately config-free (pure + unit-testable): it does NOT
/// bake in any default duration or clamp bounds. `dur == None` means "the user
/// gave no duration, use the per-kind config default"; `Some(d)` is the raw
/// requested duration (still unclamped). The handler ([`crate::handler`])
/// resolves the default and clamps to `[min_slew, max_dur]` from the live
/// [`crate::config::FadeConfig`], so a user's `[fade]` TOML overrides actually
/// take effect. A present-but-invalid duration (NaN/inf/negative) is rejected by
/// the parser as [`MpdCommand::Unsupported`] before it ever reaches here.
#[derive(Debug, Clone, Copy)]
pub struct FadeArgs {
    pub kind: FadeKind,
    pub dur: Option<Duration>,
}

/// A parsed `plan` subcommand (the P2 registry surface). See [`parse_plan`].
#[derive(Debug, Clone)]
pub enum PlanCmd {
    Add(RawPlan),
    List,
    Cancel(PlanId),
    Replace(PlanId, RawPlan),
}

/// A parsed `nl` subcommand (the P3 natural-language surface). See [`parse_nl`].
///
/// `owner` is the per-connection identity: the confirm/cancel of a pending
/// translation is scoped to the SAME connection that translated it, so one client
/// can never arm another client's echoed-but-unconfirmed plan. It is stamped by
/// the serve loop via [`stamp_nl_owner`] AFTER parsing (the wire text carries no
/// owner); the parser leaves it 0.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum NlCmd {
    /// `nl "<request>"` - translate + echo (does NOT arm).
    Translate { req: String, owner: u64 },
    /// `nl confirm <token>` - arm the echoed plan(s) via the P2 registry.
    Confirm { token: String, owner: u64 },
    /// `nl cancel <token>` - drop a pending token.
    Cancel { token: String, owner: u64 },
}

/// True when `s` has the shape of a minted `nl` token (`nl-<hex>`). Only a
/// token-shaped single argument turns `nl confirm|cancel <x>` into the keyword
/// subcommand; free text ("nl confirm the good vibes only") is a translate.
fn is_nl_token(s: &str) -> bool {
    match s.strip_prefix("nl-") {
        Some(rest) => !rest.is_empty() && rest.bytes().all(|b| b.is_ascii_hexdigit()),
        None => false,
    }
}

/// Stamp the per-connection owner onto a parsed `nl` command. A no-op for any
/// non-`nl` command. Called once by the serve loop after [`parse`].
pub fn stamp_nl_owner(cmd: &mut MpdCommand, owner_key: u64) {
    if let MpdCommand::Nl(nl) = cmd {
        match nl {
            NlCmd::Translate { owner, .. }
            | NlCmd::Confirm { owner, .. }
            | NlCmd::Cancel { owner, .. } => *owner = owner_key,
        }
    }
}

/// Parse an `nl` request. `nl confirm <token>` / `nl cancel <token>` are the two
/// keyword forms, recognized ONLY when the shape matches exactly: the verb plus a
/// single token-shaped argument (`nl-<hex>`). Anything else - including
/// "nl confirm the good vibes only" - is a translate request (the joined argument
/// text). An empty request is [`MpdCommand::Unsupported`] (fail loud, never a
/// panic), matching [`parse_plan`]. `owner` is left 0 here; the serve loop stamps
/// the real per-connection key via [`stamp_nl_owner`].
fn parse_nl(args: &[String], line: &str) -> MpdCommand {
    if args.is_empty() {
        return MpdCommand::Unsupported(line.to_string());
    }
    let keyword_form = |kw: &str| {
        args.len() == 2 && args[0].eq_ignore_ascii_case(kw) && is_nl_token(&args[1])
    };
    if keyword_form("confirm") {
        return MpdCommand::Nl(NlCmd::Confirm { token: args[1].clone(), owner: 0 });
    }
    if keyword_form("cancel") {
        return MpdCommand::Nl(NlCmd::Cancel { token: args[1].clone(), owner: 0 });
    }
    MpdCommand::Nl(NlCmd::Translate { req: args.join(" "), owner: 0 })
}

/// Parse a `plan` request into a [`PlanCmd`]. Grammar (a small keyword DSL that
/// targets the same bounded IR an LLM emits):
///   - `plan list`
///   - `plan cancel <id>`
///   - `plan add trigger <TRIGGER> action <ACTION> [once] [origin <s>]`
///   - `plan replace <id> trigger <TRIGGER> action <ACTION> [once]`
///
/// TRIGGER: `immediate` | `track <n> base current|absolute` | `after` |
///   `remaining <secs> [track ...]` | `album [track ...]` | `in <secs>` |
///   `at <rfc3339>`.
/// ACTION: `fade out|in <secs>` | `fade to <vol> <secs>` | `stop` | `pause` |
///   `setvol <v>` | `enqueue query|genre <text> <count>` | `enqueue radio <count>`.
/// A malformed request is [`MpdCommand::Unsupported`] (a fail-loud ACK), never a
/// panic. The pure validate/arm (and its numeric clamps) happen in the handler.
fn parse_plan(args: &[String], line: &str) -> MpdCommand {
    let unsupported = || MpdCommand::Unsupported(line.to_string());
    match args.first().map(|s| s.to_lowercase()).as_deref() {
        Some("list") => MpdCommand::Plan(PlanCmd::List),
        Some("cancel") => match args.get(1).and_then(|s| s.parse::<u64>().ok()) {
            Some(n) => MpdCommand::Plan(PlanCmd::Cancel(PlanId(n))),
            None => unsupported(),
        },
        Some("add") => match parse_raw_plan(&args[1..]) {
            Some(r) => MpdCommand::Plan(PlanCmd::Add(r)),
            None => unsupported(),
        },
        Some("replace") => {
            let id = args.get(1).and_then(|s| s.parse::<u64>().ok());
            match (id, parse_raw_plan(&args[2.min(args.len())..])) {
                (Some(n), Some(r)) => MpdCommand::Plan(PlanCmd::Replace(PlanId(n), r)),
                _ => unsupported(),
            }
        }
        _ => unsupported(),
    }
}

/// Parse the `trigger ... action ...` body of a `plan add`/`replace`.
fn parse_raw_plan(toks: &[String]) -> Option<RawPlan> {
    let action_pos = toks.iter().position(|t| t.eq_ignore_ascii_case("action"))?;
    // Optional leading `trigger` keyword.
    let t_start = if toks
        .first()
        .map(|t| t.eq_ignore_ascii_case("trigger"))
        .unwrap_or(false)
    {
        1
    } else {
        0
    };
    let trig_toks = &toks[t_start..action_pos];
    let mut act_toks: Vec<String> = toks[action_pos + 1..].to_vec();

    // Strip trailing `once` / `origin <s>` modifiers off the action tokens.
    let mut once = false;
    let mut origin = String::new();
    if let Some(pos) = act_toks.iter().position(|t| t.eq_ignore_ascii_case("origin")) {
        origin = act_toks.get(pos + 1).cloned().unwrap_or_default();
        act_toks.truncate(pos);
    }
    if act_toks.last().map(|t| t.eq_ignore_ascii_case("once")).unwrap_or(false) {
        once = true;
        act_toks.pop();
    }

    let trigger = parse_trigger(trig_toks)?;
    let action = parse_plan_action(&act_toks)?;
    Some(RawPlan {
        version: 1,
        trigger,
        action,
        once,
        origin: if origin.is_empty() { "mpd".into() } else { origin },
    })
}

fn parse_trigger(toks: &[String]) -> Option<RawTrigger> {
    let kw = toks.first()?.to_lowercase();
    match kw.as_str() {
        "immediate" => Some(RawTrigger::Immediate),
        "after" => Some(RawTrigger::TrackAfterCurrent),
        "track" => {
            let n = toks.get(1)?.parse::<usize>().ok()?;
            // `base current|absolute`, defaulting to CurrentIsOne.
            let base = match toks.iter().position(|t| t.eq_ignore_ascii_case("base")) {
                Some(p) => match toks.get(p + 1).map(|s| s.to_lowercase()).as_deref() {
                    Some("absolute") => PosBase::Absolute,
                    _ => PosBase::CurrentIsOne,
                },
                None => PosBase::CurrentIsOne,
            };
            Some(RawTrigger::QueuePosition { n, base })
        }
        "remaining" => {
            let secs = parse_secs(toks.get(1)?)?;
            let track = parse_track_sel(&toks[2.min(toks.len())..]);
            Some(RawTrigger::TimeRemaining { track, secs })
        }
        "album" => Some(RawTrigger::AlbumBoundary {
            track: parse_track_sel(&toks[1..]),
        }),
        "in" => Some(RawTrigger::SpanElapsed {
            secs: parse_secs(toks.get(1)?)?,
        }),
        "at" => {
            let raw = toks.get(1)?;
            let dt = chrono::DateTime::parse_from_rfc3339(raw).ok()?;
            Some(RawTrigger::WallClock {
                at: dt.with_timezone(&chrono::Utc),
            })
        }
        _ => None,
    }
}

/// Parse an optional `[track] current|rel <i>|id <u64>` selector (defaults to
/// the current track when empty).
fn parse_track_sel(toks: &[String]) -> TrackSel {
    let toks = if toks
        .first()
        .map(|t| t.eq_ignore_ascii_case("track"))
        .unwrap_or(false)
    {
        &toks[1..]
    } else {
        toks
    };
    match toks.first().map(|s| s.to_lowercase()).as_deref() {
        Some("rel") => toks
            .get(1)
            .and_then(|s| s.parse::<i32>().ok())
            .map(TrackSel::RelToCurrent)
            .unwrap_or(TrackSel::Current),
        Some("id") => toks
            .get(1)
            .and_then(|s| s.parse::<u64>().ok())
            .map(TrackSel::QueueId)
            .unwrap_or(TrackSel::Current),
        _ => TrackSel::Current,
    }
}

fn parse_plan_action(toks: &[String]) -> Option<Action> {
    match toks.first()?.to_lowercase().as_str() {
        "stop" => Some(Action::Stop),
        "pause" => Some(Action::Pause),
        "setvol" => Some(Action::SetVolume(toks.get(1)?.parse::<u8>().ok()?)),
        "fade" => match toks.get(1)?.to_lowercase().as_str() {
            "out" => Some(Action::Fade(FadeIntentIr::Out {
                secs: parse_secs(toks.get(2)?)?,
            })),
            "in" => Some(Action::Fade(FadeIntentIr::In {
                secs: parse_secs(toks.get(2)?)?,
            })),
            "to" => {
                let vol = toks.get(2)?.parse::<u8>().ok()?;
                let secs = parse_secs(toks.get(3)?)?;
                // The target dB is derived from the requested 0..=100 volume (the
                // same cubic-softvol seam the handler uses); validate clamps it.
                Some(Action::Fade(FadeIntentIr::To {
                    target_db: crate::player::mpv_volume_to_db(vol as f64),
                    vol,
                    secs,
                }))
            }
            _ => None,
        },
        "enqueue" => {
            let sel = match toks.get(1)?.to_lowercase().as_str() {
                "radio" => (Selector::Radio, 2),
                "query" => (Selector::Query(toks.get(2)?.clone()), 3),
                "genre" => (Selector::Genre(toks.get(2)?.clone()), 3),
                _ => return None,
            };
            let count = toks.get(sel.1).and_then(|s| s.parse::<u32>().ok()).unwrap_or(1);
            Some(Action::Enqueue { selector: sel.0, count })
        }
        // ── queue-edit actions (echo-before-arm round-trip; see echo.rs) ──────
        "remove" => Some(Action::Remove { sel: parse_qselector(&toks[1..])? }),
        "play" => Some(Action::Play { sel: parse_qselector(&toks[1..])? }),
        "noop" => Some(Action::Noop),
        "clear" => match toks.get(1)?.to_lowercase().as_str() {
            "all" => Some(Action::Clear { scope: ClearScope::All }),
            "after" => Some(Action::Clear { scope: ClearScope::AfterCurrent }),
            "range" => Some(Action::Clear {
                scope: ClearScope::Range {
                    start: toks.get(2)?.parse().ok()?,
                    end: toks.get(3)?.parse().ok()?,
                },
            }),
            _ => None,
        },
        "move" => {
            let to = toks.iter().position(|t| t.eq_ignore_ascii_case("to"))?;
            let sel = parse_qselector(&toks[1..to])?;
            let dest = parse_movedest(&toks[to + 1..])?;
            Some(Action::Move { sel, dest })
        }
        _ => None,
    }
}

/// Parse a queue selector off the leading tokens: `current` | `pos <n>` |
/// `last <n>` | `range <start> <end>` | `match <query>`. Positions are 1-based.
fn parse_qselector(toks: &[String]) -> Option<QueueSelector> {
    match toks.first()?.to_lowercase().as_str() {
        "current" => Some(QueueSelector::Current),
        "pos" => Some(QueueSelector::Position(toks.get(1)?.parse().ok()?)),
        "last" => Some(QueueSelector::Last(toks.get(1)?.parse().ok()?)),
        "range" => Some(QueueSelector::Range {
            start: toks.get(1)?.parse().ok()?,
            end: toks.get(2)?.parse().ok()?,
        }),
        "match" => Some(QueueSelector::QueryMatch(toks.get(1)?.clone())),
        _ => None,
    }
}

/// Parse a move destination: `pos <n>` (1-based absolute) | `rel <d>` (signed,
/// relative to the current track).
fn parse_movedest(toks: &[String]) -> Option<MoveDest> {
    match toks.first()?.to_lowercase().as_str() {
        "pos" => Some(MoveDest::Position(toks.get(1)?.parse().ok()?)),
        "rel" => Some(MoveDest::Relative(toks.get(1)?.parse().ok()?)),
        _ => None,
    }
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

/// Parse a `fade` request into a [`FadeArgs`]. Grammar:
///   - `fade out [secs]`        -> ramp to silence (then stop + restore)
///   - `fade in  [secs]`        -> ramp up to the comfort ceiling
///   - `fade to  <vol> [secs]`  -> ramp to an explicit 0..=100 volume
///   - `fade to  floor [secs]`  -> wind down to the configured non-silence floor
///
/// `secs` is bare or `s`-suffixed. A MISSING duration parses to `dur: None` (the
/// handler substitutes the per-kind config default); a present-but-non-finite or
/// negative duration is rejected as [`MpdCommand::Unsupported`]. This parser is
/// pure and config-free: it never bakes in a default duration or clamp bound -
/// the handler resolves both from [`crate::config::FadeConfig`] so a user's
/// `[fade]` TOML override actually takes effect. `vol` is clamped to `0..=100`
/// (a pure numeric bound, not a config knob).
fn parse_fade(args: &[String], line: &str) -> MpdCommand {
    let unsupported = || MpdCommand::Unsupported(line.to_string());
    // Parse an optional duration token: absent -> Ok(None) (use the config
    // default), present+valid -> Ok(Some(dur)), present+invalid -> Err.
    let opt_dur = |tok: Option<&String>| -> Result<Option<Duration>, ()> {
        match tok {
            None => Ok(None),
            Some(t) => match parse_secs(t) {
                // try_from_secs_f64 (not from_secs_f64) so a pathological huge
                // token like `fade out 1e20` is rejected as invalid rather than
                // panicking Duration construction on the connection task. A valid
                // duration is still clamped to [min_slew, max_dur] in the handler.
                Some(s) => match Duration::try_from_secs_f64(s) {
                    Ok(d) => Ok(Some(d)),
                    Err(_) => Err(()),
                },
                None => Err(()),
            },
        }
    };
    let sub = match args.first() {
        Some(s) => s.to_lowercase(),
        None => return unsupported(),
    };
    match sub.as_str() {
        "out" | "in" => {
            let kind = if sub == "in" { FadeKind::In } else { FadeKind::Out };
            let dur = match opt_dur(args.get(1)) {
                Ok(d) => d,
                Err(()) => return unsupported(),
            };
            MpdCommand::Fade(FadeArgs { kind, dur })
        }
        "to" => {
            // Second token is `floor` (wind-down to the config floor) or a 0..=100
            // volume. vol is required; reject NaN/inf/negative, clamp to 0..=100.
            let is_floor = args.get(1).map(|t| t.eq_ignore_ascii_case("floor")).unwrap_or(false);
            let kind = if is_floor {
                FadeKind::ToFloor
            } else {
                match args.get(1).and_then(|v| v.parse::<f64>().ok()) {
                    Some(v) if v.is_finite() && v >= 0.0 => FadeKind::To(v.min(100.0) as u8),
                    _ => return unsupported(),
                }
            };
            let dur = match opt_dur(args.get(2)) {
                Ok(d) => d,
                Err(()) => return unsupported(),
            };
            MpdCommand::Fade(FadeArgs { kind, dur })
        }
        _ => unsupported(),
    }
}

/// Parse a `secs` token (bare `30` or `s`-suffixed `30s`) into a finite,
/// non-negative float, or `None` (reject NaN / inf / negative / non-numeric).
fn parse_secs(tok: &str) -> Option<f64> {
    let t = tok.strip_suffix('s').unwrap_or(tok);
    let v: f64 = t.parse().ok()?;
    (v.is_finite() && v >= 0.0).then_some(v)
}

/// Parse a duration token accepting a bare/`s`/`m`/`h` suffix (a small
/// humantime-style extension of [`parse_secs`]): `30`, `30s`, `30m`, `2h`. Rejects
/// NaN / inf / negative / non-numeric / overflow. Pure + config-free.
fn parse_dur_hms(tok: &str) -> Option<Duration> {
    let (num, mult) = if let Some(n) = tok.strip_suffix('h') {
        (n, 3600.0)
    } else if let Some(n) = tok.strip_suffix('m') {
        (n, 60.0)
    } else if let Some(n) = tok.strip_suffix('s') {
        (n, 1.0)
    } else {
        (tok, 1.0)
    };
    let v: f64 = num.parse().ok()?;
    if !(v.is_finite() && v >= 0.0) {
        return None;
    }
    Duration::try_from_secs_f64(v * mult).ok()
}

/// Parse a civil clock token into `(hour, minute)` in 24h space. Accepts `7`,
/// `7:30`, `19:00`, and 12h forms with an `am`/`pm` suffix (`7pm`, `12am`).
/// Rejects out-of-range values. Pure + config-free.
fn parse_civil_time(tok: &str) -> Option<(u32, u32)> {
    let t = tok.to_lowercase();
    let (body, pm, has_mer) = if let Some(x) = t.strip_suffix("pm") {
        (x.trim(), true, true)
    } else if let Some(x) = t.strip_suffix("am") {
        (x.trim(), false, true)
    } else {
        (t.as_str(), false, false)
    };
    let (h, m) = match body.split_once(':') {
        Some((hh, mm)) => (hh.trim().parse::<u32>().ok()?, mm.trim().parse::<u32>().ok()?),
        None => (body.trim().parse::<u32>().ok()?, 0),
    };
    if m > 59 {
        return None;
    }
    let h = if has_mer {
        // 12h -> 24h: 12am == 00, 12pm == 12, else +12 for pm.
        if h == 0 || h > 12 {
            return None;
        }
        if h == 12 {
            if pm { 12 } else { 0 }
        } else if pm {
            h + 12
        } else {
            h
        }
    } else {
        if h > 23 {
            return None;
        }
        h
    };
    Some((h, m))
}

/// Extract an optional `with <selector text>` from the trailing tokens of a wake
/// command, joining everything after `with` into one query string.
fn parse_wake_selector(toks: &[String]) -> Option<String> {
    let pos = toks.iter().position(|t| t.eq_ignore_ascii_case("with"))?;
    let sel = toks[pos + 1..].join(" ");
    (!sel.trim().is_empty()).then(|| sel.trim().to_string())
}

/// Parse a `sleep` request: `sleep` (status), `sleep off|cancel`, `sleep <dur>`.
fn parse_sleep(args: &[String], line: &str) -> MpdCommand {
    match args.first().map(|s| s.to_lowercase()).as_deref() {
        None => MpdCommand::Sleep(SleepCmd::Status),
        Some("off") | Some("cancel") => MpdCommand::Sleep(SleepCmd::Cancel),
        Some(_) => match parse_dur_hms(&args[0]) {
            Some(d) => MpdCommand::Sleep(SleepCmd::Set(d)),
            None => MpdCommand::Unsupported(line.to_string()),
        },
    }
}

/// Parse a `winddown` request: `winddown` (immediate), `winddown <dur>`,
/// `winddown off|cancel`.
fn parse_winddown(args: &[String], line: &str) -> MpdCommand {
    match args.first().map(|s| s.to_lowercase()).as_deref() {
        None => MpdCommand::Winddown(WinddownCmd::Set(None)),
        Some("off") | Some("cancel") => MpdCommand::Winddown(WinddownCmd::Cancel),
        Some(_) => match parse_dur_hms(&args[0]) {
            Some(d) => MpdCommand::Winddown(WinddownCmd::Set(Some(d))),
            None => MpdCommand::Unsupported(line.to_string()),
        },
    }
}

/// Parse a `wake` request: `wake`/`wake list` (status), `wake off|cancel`,
/// `wake at <time> [with <sel>]`, `wake in <dur> [with <sel>]`.
fn parse_wake(args: &[String], line: &str) -> MpdCommand {
    let unsupported = || MpdCommand::Unsupported(line.to_string());
    match args.first().map(|s| s.to_lowercase()).as_deref() {
        None | Some("list") => MpdCommand::Wake(WakeCmd::Status),
        Some("off") | Some("cancel") => MpdCommand::Wake(WakeCmd::Cancel),
        Some("at") => {
            let Some((h, m)) = args.get(1).and_then(|t| parse_civil_time(t)) else {
                return unsupported();
            };
            MpdCommand::Wake(WakeCmd::Set {
                when: WakeWhen::At { h, m },
                selector: parse_wake_selector(&args[2.min(args.len())..]),
                count: DEFAULT_WAKE_ENQUEUE,
            })
        }
        Some("in") => {
            let Some(d) = args.get(1).and_then(|t| parse_dur_hms(t)) else {
                return unsupported();
            };
            MpdCommand::Wake(WakeCmd::Set {
                when: WakeWhen::In(d),
                selector: parse_wake_selector(&args[2.min(args.len())..]),
                count: DEFAULT_WAKE_ENQUEUE,
            })
        }
        _ => unsupported(),
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
        "seekcur" => match arg(0) {
            // f64 parses a leading `+`/`-` itself, so `secs` keeps its sign; the
            // sign presence is what marks the seek relative.
            Some(s) => match s.parse::<f64>() {
                Ok(secs) => MpdCommand::SeekCur {
                    secs,
                    relative: s.starts_with('+') || s.starts_with('-'),
                },
                Err(_) => MpdCommand::Unsupported(line.to_string()),
            },
            None => MpdCommand::Unsupported(line.to_string()),
        },
        "setvol" => match arg(0).and_then(|s| s.parse().ok()) {
            Some(v) => MpdCommand::SetVol(v),
            None => MpdCommand::Unsupported(line.to_string()),
        },
        "getvol" => MpdCommand::GetVol,
        // hypodj-native physical-potentiometer knob: one equal-loudness (dB) detent
        // up or down. Distinct from absolute `setvol` (which MPRIS/GNOME/ncmpcpp hit
        // and which must NEVER auto-pause) - only the jukebox clients send `knob`,
        // and stepping down past the audible floor is the off-click that pauses.
        "knob" => match arg(0).as_deref() {
            Some("up") => MpdCommand::Knob(KnobDir::Up),
            Some("down") => MpdCommand::Knob(KnobDir::Down),
            _ => MpdCommand::Unsupported(line.to_string()),
        },
        // random/repeat/single/consume: `<flag> 1` on, `<flag> 0` off. `single`
        // additionally accepts ncmpcpp's `oneshot` (mapped to on). A missing/bad
        // arg is a no-op-safe Unsupported ACK rather than a silent wrong toggle.
        "random" => match arg(0).as_deref() {
            Some("1") => MpdCommand::Random(true),
            Some("0") => MpdCommand::Random(false),
            _ => MpdCommand::Unsupported(line.to_string()),
        },
        "repeat" => match arg(0).as_deref() {
            Some("1") => MpdCommand::Repeat(true),
            Some("0") => MpdCommand::Repeat(false),
            _ => MpdCommand::Unsupported(line.to_string()),
        },
        "single" => match arg(0).as_deref() {
            Some("1") | Some("oneshot") => MpdCommand::Single(true),
            Some("0") => MpdCommand::Single(false),
            _ => MpdCommand::Unsupported(line.to_string()),
        },
        "consume" => match arg(0).as_deref() {
            Some("1") => MpdCommand::Consume(true),
            Some("0") => MpdCommand::Consume(false),
            _ => MpdCommand::Unsupported(line.to_string()),
        },
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
        // `save <name>` persists the current queue as a new Navidrome playlist.
        // A missing name is a loud (no-op-safe) Unsupported ACK rather than
        // saving an empty-named playlist.
        "save" => match arg(0) {
            Some(name) if !name.is_empty() => MpdCommand::Save(name),
            _ => MpdCommand::Unsupported(line.to_string()),
        },
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
        // count shares the list filter parser so it honors the modern
        // `(tag == "value")` expression form and the same `group` handling.
        "count" => MpdCommand::Count(parse_list_filter(&args)),
        "list" => {
            let tag = args.first().cloned().unwrap_or_default().to_lowercase();
            let filter = parse_list_filter(&args[args.len().min(1)..]);
            MpdCommand::List { tag, filter }
        }
        "fade" => parse_fade(&args, line),
        "plan" => parse_plan(&args, line),
        "nl" => parse_nl(&args, line),
        "sleep" => parse_sleep(&args, line),
        "winddown" => parse_winddown(&args, line),
        "wake" => parse_wake(&args, line),
        "field" => parse_field(&args, line),
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
    fn parses_playback_mode_toggles() {
        assert!(matches!(parse("random 1"), MpdCommand::Random(true)));
        assert!(matches!(parse("random 0"), MpdCommand::Random(false)));
        assert!(matches!(parse("repeat 1"), MpdCommand::Repeat(true)));
        assert!(matches!(parse("repeat 0"), MpdCommand::Repeat(false)));
        assert!(matches!(parse("single 1"), MpdCommand::Single(true)));
        assert!(matches!(parse("single 0"), MpdCommand::Single(false)));
        // ncmpcpp's oneshot maps to on.
        assert!(matches!(parse("single oneshot"), MpdCommand::Single(true)));
        assert!(matches!(parse("consume 1"), MpdCommand::Consume(true)));
        assert!(matches!(parse("consume 0"), MpdCommand::Consume(false)));
        // A missing/garbage arg is an Unsupported ACK, never a silent wrong toggle.
        assert!(matches!(parse("random"), MpdCommand::Unsupported(_)));
        assert!(matches!(parse("repeat blah"), MpdCommand::Unsupported(_)));
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
    fn nl_confirm_only_on_a_token_shaped_argument() {
        // The exact `nl confirm <token>` shape (one token-shaped arg) is a confirm.
        match parse("nl confirm nl-1a2b3c") {
            MpdCommand::Nl(NlCmd::Confirm { token, owner }) => {
                assert_eq!(token, "nl-1a2b3c");
                assert_eq!(owner, 0, "owner is unstamped until the serve loop");
            }
            other => panic!("got {other:?}"),
        }
        match parse("nl cancel nl-deadbeef") {
            MpdCommand::Nl(NlCmd::Cancel { token, .. }) => assert_eq!(token, "nl-deadbeef"),
            other => panic!("got {other:?}"),
        }
        // "nl confirm <freetext>" is a TRANSLATE, not a confirm - the whole line
        // (including "confirm") is the request text.
        match parse("nl confirm the good vibes only") {
            MpdCommand::Nl(NlCmd::Translate { req, .. }) => {
                assert_eq!(req, "confirm the good vibes only");
            }
            other => panic!("expected a translate, got {other:?}"),
        }
        // A single NON-token arg after confirm is also a translate (not a confirm),
        // since it is not token-shaped.
        match parse("nl confirm please") {
            MpdCommand::Nl(NlCmd::Translate { req, .. }) => assert_eq!(req, "confirm please"),
            other => panic!("expected a translate, got {other:?}"),
        }
        // "nl cancel that one" is likewise a translate.
        match parse("nl cancel that one") {
            MpdCommand::Nl(NlCmd::Translate { req, .. }) => assert_eq!(req, "cancel that one"),
            other => panic!("expected a translate, got {other:?}"),
        }
    }

    #[test]
    fn stamp_nl_owner_sets_the_connection_key() {
        let mut cmd = parse("nl confirm nl-abc");
        stamp_nl_owner(&mut cmd, 0xDEAD);
        match cmd {
            MpdCommand::Nl(NlCmd::Confirm { owner, .. }) => assert_eq!(owner, 0xDEAD),
            other => panic!("got {other:?}"),
        }
        // A non-nl command is untouched.
        let mut other = parse("status");
        stamp_nl_owner(&mut other, 0xBEEF);
        assert!(matches!(other, MpdCommand::Status));
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
    fn seekcur_sign_marks_relative() {
        // A bare number is absolute; a leading +/- is a RELATIVE seek that keeps
        // its sign so the handler can offset from the live position.
        match parse("seekcur 30") {
            MpdCommand::SeekCur { secs, relative } => {
                assert_eq!(secs, 30.0);
                assert!(!relative);
            }
            other => panic!("got {other:?}"),
        }
        match parse("seekcur +10") {
            MpdCommand::SeekCur { secs, relative } => {
                assert_eq!(secs, 10.0);
                assert!(relative);
            }
            other => panic!("got {other:?}"),
        }
        match parse("seekcur -10") {
            MpdCommand::SeekCur { secs, relative } => {
                assert_eq!(secs, -10.0);
                assert!(relative);
            }
            other => panic!("got {other:?}"),
        }
    }

    #[test]
    fn parses_knob_up_down() {
        assert!(matches!(parse("knob up"), MpdCommand::Knob(KnobDir::Up)));
        assert!(matches!(parse("knob down"), MpdCommand::Knob(KnobDir::Down)));
        // A missing or unknown direction fails loud (ACK), never a silent wrong turn.
        assert!(matches!(parse("knob"), MpdCommand::Unsupported(_)));
        assert!(matches!(parse("knob sideways"), MpdCommand::Unsupported(_)));
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
    fn count_parses_expression_form() {
        // count shares the list filter parser, so the modern `(tag == "value")`
        // expression works (previously swallowed as a bare `any` value).
        match parse(r#"count "(Artist == \"Tosca\")""#) {
            MpdCommand::Count(pairs) => {
                assert_eq!(pairs, vec![("artist".to_string(), "Tosca".to_string())]);
            }
            other => panic!("got {other:?}"),
        }
    }

    #[test]
    fn count_strips_group_after_a_bare_value() {
        // A bare (any) value before `group <tag>` used to desync the even-index
        // strip and leak the clause. `group` followed by a known tag is the
        // clause start regardless of position.
        match parse("count foo group album") {
            MpdCommand::Count(pairs) => {
                assert_eq!(pairs, vec![("any".to_string(), "foo".to_string())]);
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
    fn parses_save_and_rejects_empty_name() {
        // `save <name>` parses to Save; a quoted multi-word name is preserved.
        match parse("save \"Warm Room\"") {
            MpdCommand::Save(name) => assert_eq!(name, "Warm Room"),
            other => panic!("got {other:?}"),
        }
        match parse("save Set1") {
            MpdCommand::Save(name) => assert_eq!(name, "Set1"),
            other => panic!("got {other:?}"),
        }
        // A bare `save` with no name must NOT silently save an empty playlist.
        assert!(matches!(parse("save"), MpdCommand::Unsupported(_)));
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
    fn parses_fade_commands() {
        // fade out / in with no secs -> dur None (handler substitutes the config
        // default; the parser stays config-free).
        match parse("fade out") {
            MpdCommand::Fade(FadeArgs { kind: FadeKind::Out, dur }) => assert_eq!(dur, None),
            other => panic!("got {other:?}"),
        }
        match parse("fade in") {
            MpdCommand::Fade(FadeArgs { kind: FadeKind::In, dur }) => assert_eq!(dur, None),
            other => panic!("got {other:?}"),
        }
        // Bare and s-suffixed secs both parse to the RAW (unclamped) duration.
        match parse("fade out 30") {
            MpdCommand::Fade(FadeArgs { kind: FadeKind::Out, dur }) => {
                assert_eq!(dur, Some(Duration::from_secs(30)));
            }
            other => panic!("got {other:?}"),
        }
        match parse("fade out 30s") {
            MpdCommand::Fade(FadeArgs { kind: FadeKind::Out, dur }) => {
                assert_eq!(dur, Some(Duration::from_secs(30)));
            }
            other => panic!("got {other:?}"),
        }
        match parse("fade in 300") {
            MpdCommand::Fade(FadeArgs { kind: FadeKind::In, dur }) => {
                assert_eq!(dur, Some(Duration::from_secs(300)));
            }
            other => panic!("got {other:?}"),
        }
        // fade to <vol> <secs>.
        match parse("fade to 40 20") {
            MpdCommand::Fade(FadeArgs { kind: FadeKind::To(40), dur }) => {
                assert_eq!(dur, Some(Duration::from_secs(20)));
            }
            other => panic!("got {other:?}"),
        }
        // fade to floor [secs] -> the wind-down-to-floor kind.
        match parse("fade to floor") {
            MpdCommand::Fade(FadeArgs { kind: FadeKind::ToFloor, dur }) => assert_eq!(dur, None),
            other => panic!("got {other:?}"),
        }
        match parse("fade to floor 45") {
            MpdCommand::Fade(FadeArgs { kind: FadeKind::ToFloor, dur }) => {
                assert_eq!(dur, Some(Duration::from_secs(45)));
            }
            other => panic!("got {other:?}"),
        }
    }

    #[test]
    fn fade_raw_dur_unclamped_and_rejects() {
        // A runaway duration is NOT clamped here (the handler clamps it against
        // config); the parser passes it through raw.
        match parse("fade to 50 999999") {
            MpdCommand::Fade(FadeArgs { kind: FadeKind::To(50), dur }) => {
                assert_eq!(dur, Some(Duration::from_secs(999999)));
            }
            other => panic!("got {other:?}"),
        }
        // vol clamps to 100 (a pure numeric bound, not a config knob).
        assert!(matches!(
            parse("fade to 250 10"),
            MpdCommand::Fade(FadeArgs { kind: FadeKind::To(100), .. })
        ));
        // A zero duration parses raw (Some(0)); the handler lifts it to one slewed
        // step - the parser does not.
        match parse("fade out 0") {
            MpdCommand::Fade(FadeArgs { dur, .. }) => assert_eq!(dur, Some(Duration::ZERO)),
            other => panic!("got {other:?}"),
        }
        // NaN / inf / negative secs are rejected.
        assert!(matches!(parse("fade out nan"), MpdCommand::Unsupported(_)));
        assert!(matches!(parse("fade out inf"), MpdCommand::Unsupported(_)));
        assert!(matches!(parse("fade out -5"), MpdCommand::Unsupported(_)));
        // A finite-but-huge duration overflows Duration; it must be REJECTED at
        // parse (via try_from_secs_f64), never panic the connection task.
        assert!(matches!(parse("fade out 1e20"), MpdCommand::Unsupported(_)));
        assert!(matches!(parse("fade to 40 1e30"), MpdCommand::Unsupported(_)));
        // A bad subcommand / missing vol is unsupported.
        assert!(matches!(parse("fade sideways"), MpdCommand::Unsupported(_)));
        assert!(matches!(parse("fade to"), MpdCommand::Unsupported(_)));
        assert!(matches!(parse("fade"), MpdCommand::Unsupported(_)));
    }

    #[test]
    fn parses_plan_worked_example() {
        // The corpus example: "plan add trigger track 3 base current action fade
        // out 30s" -> a QueuePosition(3, CurrentIsOne) + Fade(Out, 30s), once off.
        match parse("plan add trigger track 3 base current action fade out 30s") {
            MpdCommand::Plan(PlanCmd::Add(raw)) => {
                assert!(matches!(
                    raw.trigger,
                    RawTrigger::QueuePosition { n: 3, base: PosBase::CurrentIsOne }
                ));
                match raw.action {
                    Action::Fade(FadeIntentIr::Out { secs }) => assert_eq!(secs, 30.0),
                    other => panic!("got {other:?}"),
                }
            }
            other => panic!("got {other:?}"),
        }
    }

    #[test]
    fn parses_sleep_winddown_wake_verbs() {
        // sleep: status / cancel / durations with m/h suffixes.
        assert!(matches!(parse("sleep"), MpdCommand::Sleep(SleepCmd::Status)));
        assert!(matches!(parse("sleep off"), MpdCommand::Sleep(SleepCmd::Cancel)));
        assert!(matches!(parse("sleep cancel"), MpdCommand::Sleep(SleepCmd::Cancel)));
        assert!(matches!(
            parse("sleep 30m"),
            MpdCommand::Sleep(SleepCmd::Set(d)) if d == Duration::from_secs(1800)
        ));
        assert!(matches!(
            parse("sleep 1h"),
            MpdCommand::Sleep(SleepCmd::Set(d)) if d == Duration::from_secs(3600)
        ));
        assert!(matches!(parse("sleep banana"), MpdCommand::Unsupported(_)));

        // winddown: immediate (no dur) / scheduled / cancel.
        assert!(matches!(parse("winddown"), MpdCommand::Winddown(WinddownCmd::Set(None))));
        assert!(matches!(
            parse("winddown 20m"),
            MpdCommand::Winddown(WinddownCmd::Set(Some(d))) if d == Duration::from_secs(1200)
        ));
        assert!(matches!(parse("winddown cancel"), MpdCommand::Winddown(WinddownCmd::Cancel)));

        // wake: status / cancel / at <time> [with] / in <dur> [with].
        assert!(matches!(parse("wake"), MpdCommand::Wake(WakeCmd::Status)));
        assert!(matches!(parse("wake list"), MpdCommand::Wake(WakeCmd::Status)));
        assert!(matches!(parse("wake cancel"), MpdCommand::Wake(WakeCmd::Cancel)));
        match parse("wake at 7 with jazz") {
            MpdCommand::Wake(WakeCmd::Set { when, selector, .. }) => {
                assert_eq!(when, WakeWhen::At { h: 7, m: 0 });
                assert_eq!(selector.as_deref(), Some("jazz"));
            }
            other => panic!("got {other:?}"),
        }
        match parse("wake at 7:30pm") {
            MpdCommand::Wake(WakeCmd::Set { when, selector, .. }) => {
                assert_eq!(when, WakeWhen::At { h: 19, m: 30 });
                assert!(selector.is_none());
            }
            other => panic!("got {other:?}"),
        }
        match parse("wake in 2h with deep house") {
            MpdCommand::Wake(WakeCmd::Set { when, selector, .. }) => {
                assert_eq!(when, WakeWhen::In(Duration::from_secs(7200)));
                assert_eq!(selector.as_deref(), Some("deep house"));
            }
            other => panic!("got {other:?}"),
        }
        assert!(matches!(parse("wake at bogus"), MpdCommand::Unsupported(_)));
        assert!(matches!(parse("wake in nope"), MpdCommand::Unsupported(_)));
    }

    #[test]
    fn parses_plan_list_cancel_and_once() {
        assert!(matches!(parse("plan list"), MpdCommand::Plan(PlanCmd::List)));
        match parse("plan cancel 7") {
            MpdCommand::Plan(PlanCmd::Cancel(PlanId(7))) => {}
            other => panic!("got {other:?}"),
        }
        // `once` at the tail marks the plan once:true.
        match parse("plan add trigger after action stop once") {
            MpdCommand::Plan(PlanCmd::Add(raw)) => {
                assert!(raw.once);
                assert!(matches!(raw.trigger, RawTrigger::TrackAfterCurrent));
                assert!(matches!(raw.action, Action::Stop));
            }
            other => panic!("got {other:?}"),
        }
        // A malformed plan is a fail-loud Unsupported, never a panic.
        assert!(matches!(parse("plan add trigger track"), MpdCommand::Unsupported(_)));
        assert!(matches!(parse("plan frobnicate"), MpdCommand::Unsupported(_)));
    }

    #[test]
    fn parses_nl_commands() {
        // A quoted request is one translate argument.
        match parse(r#"nl "fade out in 20 minutes""#) {
            MpdCommand::Nl(NlCmd::Translate { req, .. }) => {
                assert_eq!(req, "fade out in 20 minutes")
            }
            other => panic!("got {other:?}"),
        }
        // An unquoted request joins the tokens.
        match parse("nl fade out") {
            MpdCommand::Nl(NlCmd::Translate { req, .. }) => assert_eq!(req, "fade out"),
            other => panic!("got {other:?}"),
        }
        // Only a token-SHAPED (`nl-<hex>`) argument is a confirm/cancel.
        assert!(matches!(parse("nl confirm nl-abc123"), MpdCommand::Nl(NlCmd::Confirm { token, .. }) if token == "nl-abc123"));
        assert!(matches!(parse("nl cancel nl-abc123"), MpdCommand::Nl(NlCmd::Cancel { token, .. }) if token == "nl-abc123"));
        // A non-token argument after confirm/cancel is a translate, not a confirm.
        assert!(matches!(parse("nl confirm abc123"), MpdCommand::Nl(NlCmd::Translate { .. })));
        // Bare `nl` is a fail-loud Unsupported; `nl confirm` (no token) is a
        // translate of the literal word "confirm".
        assert!(matches!(parse("nl"), MpdCommand::Unsupported(_)));
        assert!(matches!(parse("nl confirm"), MpdCommand::Nl(NlCmd::Translate { .. })));
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
    // MPD grouping (`... group TAG [group TAG]...`) always TRAILS the filter and
    // each `group` is immediately followed by a tag name. Recognize the clause by
    // that shape - a `group` token followed by a known filter tag - rather than by
    // token parity, which desyncs whenever the filter contains a bare (tag-less)
    // `any` value. A filter VALUE literally "group" is not followed by a tag, so
    // it is kept.
    let cut = args.iter().enumerate().position(|(i, t)| {
        t.eq_ignore_ascii_case("group")
            && args
                .get(i + 1)
                .map(|n| FILTER_TAGS.contains(&n.to_lowercase().as_str()))
                .unwrap_or(false)
    });
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
    // Drop a trailing `group <tag>` clause before parsing the filter (shared with
    // count via strip_group).
    let rest = strip_group(rest);
    // Modern single-arg expression form: `(tag == "value")`.
    if rest.len() == 1 && rest[0].contains("==") {
        if let Some(pair) = parse_filter_expression(&rest[0]) {
            return vec![pair];
        }
    }
    parse_filter(&rest)
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

    // A per-connection, unguessable owner key: an `nl` pending translation is
    // confirmable ONLY from the connection that created it. Seeded from a fresh
    // process-random `RandomState` (OS entropy) mixed with a monotonic counter +
    // a wall instant, so it is neither sequential nor guessable across
    // connections.
    let owner_key: u64 = {
        use std::hash::BuildHasher;
        use std::sync::atomic::{AtomicU64, Ordering};
        static CONN_SEQ: AtomicU64 = AtomicU64::new(0);
        let seq = CONN_SEQ.fetch_add(1, Ordering::Relaxed);
        std::collections::hash_map::RandomState::new()
            .hash_one((seq, std::time::SystemTime::now()))
    };

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
                let mut cmd = parse(c);
                stamp_nl_owner(&mut cmd, owner_key);
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

        let mut cmd = cmd;
        stamp_nl_owner(&mut cmd, owner_key);
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
