# subsonity

A single standalone Rust daemon that (a) speaks the **MPD text protocol** on TCP
so ncmpcpp and every other MPD client keeps working unchanged, and (b) is itself
an **OpenSubsonic client + audio player**.

It is meant to replace the `mopidy` + `mopidy-subidy` Python stack entirely: no
Python, no mopidy core, no MPRIS/GStreamer glue. ncmpcpp connects to subsonity
exactly as it connects to mopidy today; subsonity translates MPD commands into
OpenSubsonic REST calls (browse / search / star / scrobble) and drives a local
audio engine that streams the resolved URLs.

## Vision (north star)

```
  ncmpcpp ──MPD text protocol──▶ subsonity ──OpenSubsonic REST──▶ Navidrome
   (unchanged)      (TCP)         │  daemon      (browse/search/     (or any
                                  │              stream/scrobble)     OpenSubsonic
                                  ▼                                   server)
                             libmpv audio
                             (streams the resolved URLs)
```

One process. ncmpcpp thinks it is talking to MPD; the daemon browses/searches a
Subsonic library and plays the streams through mpv.

## Phased plan

- **Phase 0 - FOUNDATION (this commit, compiles + real vertical slice).**
  Cargo workspace, nix devshell, config, the OpenSubsonic client wrapper, the
  internal domain model, the player **actor boundary** (`PlayerHandle`) with a
  working headless `NullPlayer`, and the MPD command/response/handler
  **interface**. The `probe` binary proves the slice against a live server:
  config -> auth/ping -> browse -> resolve stream URL.
- **Phase 1 - MpvPlayer.** Real playback behind the same `PlayerHandle`: a
  dedicated thread owning `libmpv2::Mpv`, driven by the command channel, pushing
  `time-pos`/`eof` back out as `PlayerEvent`s (which drive queue-advance +
  scrobble). The devshell already ships libmpv, so adding the dep links.
- **Phase 2 - MPD server loop.** The TCP accept loop + line codec + dispatch
  implementing the ncmpcpp-critical command subset, bound to `127.0.0.1:6601`
  in dev.
- **Phase 3 - feature parity.** Port the 9 shipped Python features (scrobble,
  cover art, star/love, similar/radio, smart album lists, genres, search3,
  listing cache, OpenSubsonic extension negotiation).
- **Phase 4 - cut over.** Flip the bind to `6600` and retire mopidy.

## What is BUILT vs next-phase (honest)

**Built now, real, compiles, tested:**

- `config.rs` - TOML config, creds read from a file (never hardcoded). Default
  MPD bind is `127.0.0.1:6601` on purpose (mopidy owns 6600). Unit-tested.
- `model.rs` - internal domain types (`SongId`/`AlbumId`/`ArtistId` newtypes,
  `Artist`/`Album`/`Song`), decoupled from the `opensubsonic` wire types.
- `subsonic.rs` - `SubsonicClient` wrapping `opensubsonic::Client`. **Real:**
  `connect` (token auth), `ping`, `artists` (real `get_artists` -> flattened
  `Vec<Artist>`), `album_list` (real `get_album_list2` -> `Vec<Album>`),
  `stream_url` (returns `url::Url`, the handoff type to the player). Wire->model
  mapping is exercised by the probe against a live server.
- `player.rs` - the **actor boundary**: `PlayerHandle` (cloneable, `&self`
  command methods over mpsc+oneshot, state via `watch`, events via mpsc), the
  `PlayerEvent` stream, and a genuine `NullPlayer` actor over that boundary.
  Unit-tested through the handle.
- `mpd.rs` - the MPD **interface**: `MpdCommand` (including the ncmpcpp-blocking
  command set), `MpdResponse` (pairs / binary / ack shapes), `MpdHandler` trait
  (shared `&self`), `MpdServer`.
- `probe.rs` - the real vertical-slice prover (see below).
- `flake.nix` - reproducible devshell (rust + pkg-config + libmpv).

**Clearly next-phase (marked `TODO(next-phase)`, not faked as done):**

- `MpvPlayer` - the real libmpv-backed actor. The `NullPlayer` proves the
  boundary works; the mpv thread is the swap-in.
- `MpdServer::serve` - the TCP accept loop + line codec + dispatch. Bails with a
  "next-phase" error today; it does not pretend to serve.
- The remaining ~75 SubsonicClient endpoints (scrobble/star/search3/cover art
  etc.) - each lands as a method on the existing wrapper. The 8 endpoints the
  9-feature parity needs are all verified present in opensubsonic 0.3.0 with
  concrete typed returns; only field-level wire->model mapping remains.
- `cache.rs` / `scrobble.rs` and an `mpd/` submodule split (codec/parse/dispatch)
  once the command surface grows.

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
  (`ADVERTISED_MPD_VERSION`) is a conservative `0.21.0` until the binary +
  filter syntax is implemented, so ncmpcpp is not invited to request
  capabilities the server would then ACK on. Bump it in lockstep with those
  features.
- **TLS is rustls, not OpenSSL.** `opensubsonic 0.3` pulls `reqwest 0.13` with
  `default-features = false` + `rustls`. The devshell intentionally does NOT ship
  openssl - nothing would link it.

## Accepted risks (bounded, mitigated)

- **`opensubsonic 0.3.0` is young and single-maintainer** (first published
  Feb 2026, low download count). Mitigation: the `SubsonicClient` wrapper +
  the `SubsonicError`-as-string boundary mean a fallback to hand-rolled `reqwest`
  calls is survivable, and the dep is pinned to `=0.3.0` (with `Cargo.lock`
  committed) so a silent minor bump cannot reshape the wire types this layer maps.
- **libmpv is a C system dependency** (Phase 1). Mitigation: the nix devshell
  provides it reproducibly (`mpv-unwrapped` + `pkg-config`). The foundation stays
  link-light (mpv is not yet a Cargo dep); Phase 1 must actually construct
  `libmpv2::Mpv` and play one URL through the channel wrapper to fully de-risk.
- **The MPD server is hand-rolled** (no server-side crate exists; `mpd`/
  `mpd_client` are clients, `mpd_protocol` is a client codec). The risk is the
  scope of the command set, not per-command difficulty - which is why the
  ncmpcpp-critical surface is enumerated in the interface now.

## Layout

```
crates/subsonity-core/     library: config, model, subsonic, player, mpd
crates/subsonity-daemon/   binaries: `subsonity` (daemon) + `probe` (slice prover)
flake.nix                  reproducible devshell (rust + pkg-config + libmpv)
```

## Build constraints

Builds are capped at `-j2` (`CARGO_BUILD_JOBS=2`, set in the devshell). Dev
profile uses `opt-level = 0` to keep foundation builds cheap.
```
nix develop --command cargo build -j2
nix develop --command cargo test  -j2
```
