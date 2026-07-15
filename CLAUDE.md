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
- `crates/hypodj-cli` - the `hjq` jukebox CLI (pure MPD/TCP, does NOT link libmpv).

A shared `hypodj-client` lib and a `dj-gui` TUI are in progress. Binary renames
are planned: `hjq` -> `dj`, the TUI -> `dj-gui` (product name HypoDJ). Build or
test one crate with `-p <crate>`.

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
