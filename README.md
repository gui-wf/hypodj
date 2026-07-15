# hypodj - *The DJ Underneath*

A single standalone Rust daemon that (a) speaks the **MPD text protocol** on TCP
so ncmpcpp and every other MPD client keeps working unchanged, and (b) is itself
an **OpenSubsonic client + audio player**.

The name is from the Roman **hypocaust** (Greek *hypo-*, "beneath" + *kaustos*,
"burnt") - the furnace and flue chamber below a bath that heated the room above,
tended out of sight. `hypodj` is the DJ underneath: it does the real work
(browsing, streaming, playing your Navidrome library) hidden below, while your
MPD client lounges in the warm room.

It replaced the `mopidy` + `mopidy-subidy` Python stack entirely - no Python, no
mopidy core, no MPRIS/GStreamer glue. ncmpcpp connects to hypodj exactly as it
connected to mopidy; hypodj translates MPD commands into OpenSubsonic REST calls
(browse / search / star / scrobble) and drives a local audio engine that streams
the resolved URLs.

## Vision (north star)

```
  ncmpcpp ──MPD text protocol──▶ hypodj ──OpenSubsonic REST──▶ Navidrome
   (unchanged)      (TCP)         │  daemon      (browse/search/     (or any
                                  │              stream/scrobble)     OpenSubsonic
                                  ▼                                   server)
                             libmpv audio
                             (streams the resolved URLs)
```

One process. ncmpcpp thinks it is talking to MPD; the daemon browses/searches a
Subsonic library and plays the streams through mpv.

## Phased plan

- **Phase 0 - FOUNDATION. DONE.** Cargo workspace, nix devshell, config, the
  OpenSubsonic client wrapper, the internal domain model, the player **actor
  boundary** (`PlayerHandle`) with a headless `NullPlayer`, and the MPD
  command/response/handler **interface**. The `probe` binary proves the slice
  against a live server: config -> auth/ping -> browse -> resolve stream URL.
- **Phase 1 - MpvPlayer + real browse. DONE.** Real playback behind the same
  `PlayerHandle`: a dedicated thread owning `libmpv2::Mpv`, driven by the
  command channel, pushing `time-pos`/`eof` back out as `PlayerEvent`s (which
  drive queue-advance + scrobble). Browse mapping is real. Proven live +
  headless by the `play-probe` binary.
- **Phase 2 - MPD server loop. DONE.** The tokio TCP accept loop + line parser
  (quoted args) + dispatch, bound to `127.0.0.1:6601` in dev. A `HypodjHandler`
  backs the ncmpcpp-critical command subset with live Subsonic browse/search +
  the player over a shared in-memory queue. Synthetic `artist/<id>` /
  `album/<id>` / `song/<id>` URIs bridge MPD's path model to Subsonic ids;
  `lsinfo` drills root -> artists -> albums -> songs. `command_list[_ok]`
  batching + `idle`/`noidle`. Verified live against Navidrome with a real MPD
  client.
- **Phase 3 - feature parity. DONE.** All 9 shipped Python features ported and
  live-verified against Navidrome (scrobble, cover art, star/love + rating via
  MPD `sticker`, similar/radio/top, smart album lists, genres, search3 with
  typed-tag filtering + richer metadata, TTL+LRU listing cache, OpenSubsonic
  extension negotiation). See the honest feature-status section below.
- **Nix packaging. DONE.** The flake exposes `packages.default` (the daemon,
  libmpv wrapped), `nixosModules.default`, `homeManagerModules.default`, and an
  `overlays.default` - see [Usage](#usage). Password stays out of the store
  (systemd `LoadCredential` + a runtime-rendered config).
- **Phase 4 - cut over. DONE.** hypodj is the daily driver: the deployment binds
  `127.0.0.1:6600` (via `services.hypodj.mpd.bind`) and mopidy + mopidy-subidy are
  retired. Note the in-code `config.rs` default is still `6601`; production sets
  `6600` explicitly.

## Current direction: the smart-server roadmap

Parity is done; the project is now growing hypodj from a protocol proxy into a
music server that understands human intent. The design is a **deterministic
capability core + a typed Plan-IR trigger/executor** over stable primitives, with
an optional natural-language translator that only ever emits a *validated* plan,
and embeddings reserved for content selection. Phases (all now merged):
**P0 fade primitive** -> **P1 event substrate** -> **P2 Plan IR + executor + DSL**
(`plan add ...`) -> **P3 natural-language front-end** (`nl "..."`, rules-first with
an optional local model, echo-before-arm) -> **P4 mood/energy selection** (a local
heuristic + a FeatureStore seam for offline features). Built on top: sleep /
wind-down / wake commands, a graceful smooth-restart (fade-out on SIGTERM,
wake-ramp-in on restart), startle-safe pause/resume, and a first-class favorites
model (songs / albums / artists). Post-parity MPD additions: `findadd`/`searchadd`,
`count`, filtered `list <tag> <filter>`, composer/performer tags, and `fade`.

## Clients (human-native, natural-language-first)

hypodj is the server; driving it is meant to be human-native and
natural-language-first - say what you want in plain words, as simple or complex as
it gets, and the technical complexity is handled server-side. Any MPD client
(ncmpcpp, mpc) works, and hypodj ships thin clients over the MPD protocol + the
`nl` command:

- **`dj`** - a jukebox CLI. Bare `dj` prints a now-playing card; `dj next` /
  `dj pause` / `dj vol 40` are quick verbs; anything else is natural language
  routed through `nl` with an echo-before-arm confirm, e.g.
  `dj "play something calmer"` or `dj "fade the 3rd track"`.
- **`dj-gui`** (product name HypoDJ) - an interactive terminal jukebox:
  now-playing + queue + a command line that takes verbs and natural language,
  the same NL-first surface as the CLI.
- A GNOME Shell search provider (planned) so typing "next song" or "play
  something calmer" into Activities runs the command.

The clients are pure MPD/TCP (no libmpv) and model-free by default; natural
language is translated server-side and always validated + echoed before it arms.

## Phase 3 feature status (honest)

The 9 Python-parity features are ported and reachable through the live MPD serve
loop (the in-code default bind is `6601`; the live deployment binds `6600`):

- **scrobble** - now-playing + threshold-gated completed-play submission, fired
  off the player event loop.
- **cover art** - `albumart`/`readpicture` -> getCoverArt, chunked to
  `binarylimit`, cached.
- **star / love** - a first-class favorites model over the Subsonic star API:
  `playlistadd Starred song/<id>` stars (position-based `playlistdelete` unstars);
  albums and artists are also favoritable and surface as `Starred/Albums` +
  `Starred/Artists` browse dirs.
- **rating** - WIRED via the MPD `sticker` command (ncmpcpp's rating path):
  `sticker set song song/<id> rating <0-5>` -> Subsonic `setRating`;
  `sticker get`/`list` read back `userRating` as `sticker: rating=<n>`;
  `sticker delete` clears it (setRating 0). Proven live against Navidrome: an MPD
  `set rating 4` is confirmed by `getSong` returning `userRating=4`, and cleared
  by `delete`. Only the `rating` sticker is backed (no generic sticker store).
- **similar / radio / top** - `radio/random`, `radio/similar/<songId>`,
  `radio/top/<artist>` browse dirs. LIMITATION: `similar`/`top` return only what
  the server's last.fm-backed data provides; on a server without that data they
  can legitimately be empty (not a client bug).
- **smart album lists** - `Lists/{frequent,newest,recent,highest,random}`.
- **genres** - `Genres` browse dir + `list genre`.
- **search3** - `find`/`search` -> search3 (full-text) + client-side MPD-tag
  post-filter for precision.
- **listing cache** - TTL+LRU over stable listings; freshness-critical listings
  (`Starred`, `random`) are never cached; a rating/star bust invalidates the
  listings whose per-song flags could change.
- **OpenSubsonic extension negotiation** - probed + logged once at connect.
  LIMITATION: the advertised set is currently only recorded (no behaviour is
  gated on it yet, because every shipped feature is core Subsonic).

Known honest limitation in `idle`: it always emits `changed: player` on any
change (single change-notifier; no per-subsystem tracking yet). ncmpcpp re-reads
status/currentsong/plchanges on any `changed:` line, so its view still refreshes.

## Source map

Everything below is built, compiles, and is tested (the full workspace passes
`cargo test`; the feature paths are additionally live-verified against Navidrome
via the probes and a real MPD client). Parity and the cut-over are done; new work
follows the smart-server roadmap above.

- `config.rs` - TOML config; creds read from a file, never hardcoded. Default
  MPD bind `127.0.0.1:6601`; the live deployment overrides this to `6600`.
- `model.rs` - internal domain types (`SongId`/`AlbumId`/`ArtistId` newtypes,
  `Artist`/`Album`/`Song`/`Genre`), decoupled from the `opensubsonic` wire types.
- `subsonic.rs` - `SubsonicClient` over `opensubsonic::Client`: connect (token
  auth, configurable client name), ping, browse (artists/albums/songs), search3,
  scrobble, star/unstar/set_rating, get_starred2, similar/top/random songs,
  album lists by type, genres + songs-by-genre, cover art bytes, and
  OpenSubsonic extension negotiation. Wire->model mapping is unit-tested against
  the real wire structs and exercised live.
- `player.rs` - the player **actor boundary** (`PlayerHandle`) and `MpvPlayer`,
  a real libmpv-backed actor (dedicated thread owning `libmpv2::Mpv`) that pushes
  `time-pos`/`eof` events driving queue-advance + scrobble. `AudioOut` selects
  headless (`Null`/`File`) or `Device` output; init failure falls back to
  `NullPlayer` and never panics the daemon.
- `mpd.rs` + `handler.rs` - the MPD server: accept loop, line parser (quoted
  args), response/binary/ack framing, and dispatch of the ncmpcpp command set
  (status/currentsong/idle, playback, queue, `lsinfo` browse of the synthetic
  Genres/Lists/Radio/Starred dirs, `albumart`/`readpicture`, `sticker` ratings,
  `list`/`search`/`find` with typed-tag filtering, the editable `Starred`
  playlist star trigger).
- `cache.rs` - the bounded TTL+LRU listing cache (freshness-critical listings
  stay uncached; star/rating busts affected listings).
- `scrobble.rs` - now-playing + threshold-gated completed-play submission off the
  player event loop.
- `nix/` + `flake.nix` - `packages.default` (daemon, libmpv wrapped),
  `nixosModules.default`, `homeManagerModules.default`, `overlays.default`, and
  the devshell. See [Usage](#usage).
- `probe.rs` / `play-probe` - the "test with a real server, not mocks" provers.

## Running the vertical slice (`probe`)

The `probe` binary is the "test with a real server, not mocks" proof:

```
nix develop
# create a config with your server creds (see subsonity.toml.example)
cargo run -j2 --bin probe -- ./my-config.toml <song-id>
```

It: (1) loads the TOML config, (2) authenticates + pings, (3) browses (lists
artists + newest albums, prints real counts + a sample name), (4) resolves a
real stream URL for the given song id. Step 4 deliberately stops at the resolved
URL - that is the exact handoff point to `MpvPlayer.play_url(song, url)`.

Verified live against Navidrome: `ping OK`, `89 artists / 20 albums` with real
names, and a resolved stream URL that independently returns `audio/flac`
(HTTP 206, range-capable - what mpv needs for seeking). The stream URL carries
the auth token in its query string, so the probe prints only scheme/host/path
and redacts the query.

## Running the Phase-1 playback proof (`play-probe`)

`play-probe` extends the slice through REAL, headless audio decode:

```
nix develop
cargo run -j2 --bin play-probe -- ./my-config.toml /tmp/out.wav 6
```

It browses -> lists the first album's songs (`get_album`) -> picks a track ->
resolves its stream URL -> hands it to `MpvPlayer` configured with
`AudioOut::File` (mpv encodes decoded PCM to a WAV; **no audio device is ever
opened**) -> plays a few seconds -> stops -> asserts the WAV grew to real size.

Verified live + headless against Navidrome: mpv reached `Playing`, `time-pos`
advanced to ~27s, and a **4.7 MB WAV** was captured. The file independently
re-decodes as `pcm_s16le 2ch 44100 Hz` under mpv, confirming real audio (not a
stub). songrec did not recognize the sample track (niche electronic release,
not in Shazam's DB), so the proof is the bytes-decoded + re-decode check - the
sanctioned fallback. Nothing was ever sent to the speakers.

## Design decisions worth knowing

- **Wire<->model boundary (`model.rs` + `subsonic.rs`).** Nothing outside
  `subsonic.rs` touches the `opensubsonic` crate's structs. Swapping or version-
  bumping the client crate is a one-file blast radius. `opensubsonic` is pinned
  to `=0.3.0` (see "Accepted risks").
- **Player is an actor, not a `&mut self` trait.** `libmpv2::Mpv` is not freely
  `Send`/`Sync` and mpv's event loop is a blocking pull that must be drained.
  The boundary is a cloneable `PlayerHandle` (commands over a channel, state via
  `watch`, events out via a channel). This is settled in Phase 0 - before the
  MPD server is written on top of it - so the trait shape does not have to break
  when the real mpv thread lands in Phase 1. `NullPlayer` exercises the exact
  boundary today.
- **MPD state is shared, so `MpdHandler::handle` is `&self`.** MPD's queue /
  current song / volume / idle subscriptions are shared across all client
  connections. The handler is `Arc`-shared behind interior mutability / the
  player actor, never `&mut self` (which would wrongly imply per-connection
  state).
- **ncmpcpp-blocking commands are first-class.** ncmpcpp does not tolerate ACK
  for every unknown command: if `listplaylists`/`listplaylistinfo`/`load` error
  it can infinite-loop and freeze, and a bad `plchanges` shape blanks its
  playlist (verified from the beets/bpd port). So those, plus the capability
  probes (`commands`/`tagtypes`/`outputs`/`decoders`/`urlhandlers`), are explicit
  `MpdCommand` variants now - the dispatch author cannot forget them.
- **Binary sub-protocol modeled now.** `albumart`/`readpicture` are not
  `key: value` pairs - they are framed `size:`/`binary:`/`<raw bytes>`/`OK`
  chunked to `binarylimit`. `MpdResponse::Binary` represents that in the
  foundation (and `get_cover_art` returns owned `Bytes`, exactly what chunking
  needs).
- **Advertised MPD version tracks the implemented surface.** The greeting version
  (`ADVERTISED_MPD_VERSION`) is `0.23.0` - it promises the binary (`albumart`/
  `readpicture`) and modern filter-expression syntax the server actually backs.
  Bump it in lockstep with the implemented surface, never ahead of it.
- **TLS is rustls, not OpenSSL.** `opensubsonic 0.3` pulls `reqwest 0.13` with
  `default-features = false` + `rustls`. The devshell intentionally does NOT ship
  openssl - nothing would link it.

## Accepted risks (bounded, mitigated)

- **`opensubsonic 0.3.0` is young and single-maintainer** (first published
  Feb 2026, low download count). Mitigation: the `SubsonicClient` wrapper +
  the `SubsonicError`-as-string boundary mean a fallback to hand-rolled `reqwest`
  calls is survivable, and the dep is pinned to `=0.3.0` (with `Cargo.lock`
  committed) so a silent minor bump cannot reshape the wire types this layer maps.
- **libmpv is a C system dependency** (now linked via `libmpv2` 4.1 /
  `libmpv2-sys`). Mitigation: the nix devshell provides it reproducibly
  (`mpv-unwrapped` + `pkg-config`). De-risked: `play-probe` constructs a real
  `libmpv2::Mpv` and decodes a live stream URL to a WAV through the channel
  wrapper. If libmpv is missing at runtime, `MpvPlayer::spawn` logs and falls
  back to `NullPlayer` rather than panicking.
- **The MPD server is hand-rolled** (no server-side crate exists; `mpd`/
  `mpd_client` are clients, `mpd_protocol` is a client codec). The risk is the
  scope of the command set, not per-command difficulty - which is why the
  ncmpcpp-critical surface is enumerated in the interface now.

## Usage

The flake ships a package plus a NixOS module and a Home-Manager module - ONE
shared `services.hypodj` definition. Import the module for your system, point it
at your OpenSubsonic/Navidrome server, and connect ncmpcpp to the configured
`mpd.bind` (the live deployment uses `127.0.0.1:6600`).

The Navidrome password is read from `passwordFile` (or `passwordCommand`) at
service start into a `0600` runtime config under `/run` - it is never written to
the Nix store. Only a template with a `@HYPODJ_PASSWORD@` placeholder lives in
the store; the real password is substituted at start.

### NixOS

```nix
{
  inputs.hypodj.url = "github:FamiliarTools/hypodj";   # or path:/tmp/hypodj while local

  # in your host module (config is in scope here):
  imports = [ hypodj.nixosModules.default ];

  # sops-nix secret (assumed you already run sops-nix); any 0600 file works too.
  sops.secrets."hypodj/password" = { };

  services.hypodj = {
    enable = true;
    server.url = "https://navidrome.example.com";
    server.username = "guilherme";
    server.passwordFile = config.sops.secrets."hypodj/password".path;
    # mpd.bind stays 127.0.0.1:6601 ; audio stays "null" (headless).
  };
}
```

The NixOS module wires `hypodj.overlays.default` automatically, so
`services.hypodj.package` defaults to `pkgs.hypodj`. The system service runs
under `DynamicUser` with `LoadCredential` (credential name `hypodj-password`),
so a root-owned sops secret is read without any ownership juggling: systemd
stages the file into `$CREDENTIALS_DIRECTORY` readable by the (dynamic) service
user, and the pre-start render step reads it there - this works despite
`DynamicUser` + `ProtectHome=true`.

`passwordFile` is typed `str`, not `path`, on purpose: pass a runtime path
string such as `config.sops.secrets."hypodj/password".path`. It is used verbatim
as a runtime path and is never copied into the Nix store.

### Home-Manager

```nix
{
  inputs.hypodj.url = "github:FamiliarTools/hypodj";

  # add the overlay so pkgs.hypodj (the module's default package) resolves:
  nixpkgs.overlays = [ hypodj.overlays.default ];

  imports = [ hypodj.homeManagerModules.default ];

  services.hypodj = {
    enable = true;
    server.url = "https://navidrome.example.com";
    server.username = "guilherme";
    server.passwordFile = "/run/secrets/hypodj-password";  # any 0600 file you own
  };
}
```

### passwordCommand (alternative to passwordFile)

Exactly one of `passwordFile` / `passwordCommand` must be set. The command's
stdout is the password, read at start:

```nix
services.hypodj.server.passwordCommand = [ "pass" "show" "navidrome/guilherme" ];
```

Note: `passwordCommand` runs in `ExecStartPre` with the service's own
privileges and sandbox (on NixOS that means `DynamicUser` + `ProtectHome=true`).
It must not depend on reading anything the hardened service cannot reach. For a
sops secret under a user home or root-owned, prefer `passwordFile`, which is
staged via systemd `LoadCredential` and is readable regardless of the sandbox.

### Connecting a client

```
ncmpcpp -h 127.0.0.1 -p 6601
# or: mpc -h 127.0.0.1 -p 6601 status
```

Set `services.hypodj.audio = "device"` only when you actually want hypodj to own
audio output; the default `"null"` keeps it headless so it never grabs your
speakers (it stays a headless MPD server by default).

## Layout

```
crates/hypodj-core/     library: config, model, subsonic, player, mpd,
                        plan/executor, event/director, fade, clock, resume
crates/hypodj-daemon/   binaries: `hypodj` (daemon) + `probe` (slice prover)
crates/hypodj-nl/       optional NL -> validated Plan IR (rules + feature-gated model)
crates/hypodj-client/   shared client lib (MpdConn, nl handshake, parse/route)
crates/hypodj-cli/      `dj` jukebox CLI (pure MPD/TCP, no libmpv)
crates/hypodj-tui/      `dj-gui` interactive TUI, product HypoDJ (no libmpv)
flake.nix               packages.default, devShell, nixos/home-manager modules
nix/package.nix         buildRustPackage (daemon bin, libmpv wrapped)
nix/hypodj-module.nix   ONE shared services.hypodj module (nixos + home-manager)
nix/overlay.nix         overlays.default -> pkgs.hypodj
```

## Build constraints

Builds are capped at `-j2` (`CARGO_BUILD_JOBS=2`, set in the devshell). Dev
profile uses `opt-level = 0` to keep foundation builds cheap.
```
nix develop --command cargo build -j2
nix develop --command cargo test  -j2
```
