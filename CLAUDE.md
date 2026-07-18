# hypodj

A single Rust daemon that speaks the **MPD text protocol** to clients (ncmpcpp,
mpc) and is itself an **OpenSubsonic client + mpv audio player**. It replaced the
mopidy + mopidy-subidy stack. This is a Cargo workspace: the daemon plus thin
MPD/TCP client tools. See `README.md` for the full story, feature status, and Nix
usage - this file is only what you need to work in the repo.

## Workspace

- `crates/hypodj-core` - the library: config, model, subsonic, player, mpd
  handler, cache, scrobble, fade, clock, plan/executor, event/director, resume.
  Most work happens here.
- `crates/hypodj-daemon` - the `hypodj` daemon binary + the `probe` / `play_probe`
  bins.
- `crates/hypodj-nl` - optional natural-language to validated Plan IR translator
  (deterministic rules + a `feature = "llm"` local-model backend). The DEFAULT
  build is model-free.
- `crates/hypodj-client` - the shared client lib (MpdConn, the `nl` handshake,
  now-playing/queue parsing, verb-vs-NL routing) that both client bins use.
- `crates/hypodj-cli` - the `dj` jukebox CLI (pure MPD/TCP, no libmpv).
- `crates/hypodj-tui` - the `dj-gui` interactive TUI (product name HypoDJ; ratatui
  over `hypodj-client`, no libmpv).

Build or test one crate with `-p <crate>`.

## Build, test, run

Cargo is not on `PATH`; the toolchain, linker, and libmpv come from the flake
devshell. Always build/test inside it:

```
nix develop --command cargo build -j4 --workspace
nix develop --command cargo test  -j4 --workspace
```

- `CARGO_BUILD_JOBS=4`; never exceed 4 cores on this machine (Framework 13 AMD).
  Dev profile is `opt-level = 0`.
- The one `#[ignore]` test, `player::tests::live_mpv_fractional_volume_round_trip`
  in `crates/hypodj-core/src/player.rs`, needs a real libmpv runtime: run it
  manually with `cargo test -p hypodj-core -- --ignored`.
- Prove feature paths against a REAL server - the `probe` / `play_probe` daemon
  bins vs live Navidrome are the sanctioned proof, not mocks; unit tests cover
  parsing / mapping / logic.
- **Time-based code is always fake-clocked.** Fades, timers, and the executor use
  `clock.rs` and `#[tokio::test(start_paused)]`, NEVER wall-clock (flaky). Apply
  the pattern to any new time-dependent logic before writing it.
- **After touching a shared crate (`hypodj-core`/`hypodj-client`) or any
  cross-crate enum/type, build AND test the WHOLE `--workspace`, never just
  `-p <crate>`.** A new enum variant (e.g. an `Action`/`MpdCommand` case) breaks
  another crate's exhaustive `match` (this broke the `dj-gui` build once).
- **A workflow must not merge to master until ALL gates pass: whole-workspace
  build+test green, `nix build .#hypodj`/`.#hypodj-clients` green, the LIVE
  functional proof for the change passes, AND every confirmed critical/high review
  finding is resolved and re-verified.** Gate the integrate phase on these
  explicitly - never merge-then-discover (this shipped a live-broken favorite
  route, a latent cc contract bug, and a critical skip-EOF audible-bleed before
  being caught post-merge). A `let Some(..) = handler_with_null_player()` unit test
  CANNOT prove a real-mpv path (warm-skip, EOF auto-advance, astats) - those need
  an `#[ignore]` live-libmpv test that the gate actually runs.
- **A workflow live-proof that spins up an isolated daemon MUST force
  `audio = null` (e.g. `HYPODJ_AUDIO=null`), never clone the user's real audio
  device.** The runtime config at `/run/user/1000/hypodj/config.toml` binds the
  real speakers, so a test daemon copied from it plays sound through the user's
  room on the alt port. Always override audio to null in the copied config (and
  MPRIS off) so the live proof stays SILENT - your process must leave no sound in
  the human's environment. Also always tear the test daemon down afterward.
- **Devshell `cargo test` green does NOT mean the Nix package builds.**
  `nix/package.nix` and `nix/clients.nix` run `doCheck` with `-p hypodj-core` in a
  CERTLESS, network-less sandbox where `handler_with_null_player()` returns `None`.
  Tests using it MUST skip via `let Some((h, _)) = handler_with_null_player() else
  { return };` - NEVER `.unwrap()` the `Option` (an unwrap panics in the sandbox
  and fails the build while the devshell stays green). Run `nix build .#hypodj` (or
  `nixos-rebuild build`) before calling a change deploy-ready.

## Deploy (human-gated - an agent cannot finish it)

hypodj is the `hypodj` flake input (`github:FamiliarTools/hypodj`) in
`~/os-configurations`. An agent does step 1 and the build; the **switch is the
user's** - agents cannot `sudo` (the sandbox sets no-new-privileges).

1. push `master`, then in `~/os-configurations`: `nix flake update hypodj` and
   commit the bump scoped (`git commit -- flake.lock`), leaving unrelated lock
   churn unstaged.
2. `nixos-rebuild build --flake .#bubble-gum --cores 4 --max-jobs 1`
3. **[user]** `sudo nixos-rebuild switch --flake .#bubble-gum ...`

The live daemon binds `127.0.0.1:6600`. NOTE: the running build can lag `master`
by several merges (the user switches manually) - match the deployed
`crates/.../handler.rs` to a commit before diagnosing a "regression".

## Architecture

A **deterministic capability core** (player + queue + Subsonic select) with a
typed **Plan-IR trigger/executor** over it (P2), an optional NL translator that
only ever emits a *validated* Plan IR (P3), and content-selection embeddings
(P4) - all merged, plus the human features (sleep/wind-down/wake, smooth-restart,
startle-safe pause/resume/skip, first-class favorites). `subsonic.rs` is the ONLY
file that touches `opensubsonic` wire types - a one-file blast radius. The client
tools are thin over the MPD protocol + the `nl` command and never link libmpv.

## Invariants (foundational - do not violate)

Hard-won from the player/fade work; they apply to all player/state code:

- `std::sync::Mutex<State>` is **never held across `.await`**; a tokio async mutex
  (e.g. the fade slot) may be.
- The MPD handler is `Arc`-shared and `&self` (queue/current/volume/idle are
  shared across connections); mutate through interior mutability, never
  `&mut self`.
- A manual volume change (setvol/stop/clear/mpris) must **cancel any in-flight
  fade and apply the state mutation atomically under one slot lock** - releasing
  between cancel and mutate lets a concurrent fade from another connection slip in.
- Fade supersede **validates before aborting** the running fade; the fade
  terminal is **epoch-guarded** under the slot lock. Duration math is
  **saturating**; durations parse via `try_from_secs_f64`; `FadeConfig::normalize`
  must be **total** (no `clamp(lo, hi)` with `lo > hi` on any finite config).
- `Eof` / queue-advance must stay on a **lossless** channel - never a droppable
  broadcast.
- ncmpcpp-blocking commands (`listplaylists`/`listplaylistinfo`/`load`,
  `plchanges`, capability probes) must return well-formed responses, never ACK -
  they are explicit `MpdCommand` variants so dispatch cannot forget them.
- `ADVERTISED_MPD_VERSION` tracks the surface actually implemented; bump it in
  lockstep, never ahead.

## Conventions

- Match the surrounding code style and doc-comment density.
- No emojis, no em dashes, no double hyphens (`--`) anywhere including comments;
  use a single hyphen with spaces for a parenthetical break.
- Roadmap/task state and non-obvious project context live in Memoria (`memo
  recall` before assuming what is done); this file holds only stable facts.
