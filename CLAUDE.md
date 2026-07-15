# hypodj

A single Rust daemon that speaks the **MPD text protocol** to clients (ncmpcpp,
mpc) and is itself an **OpenSubsonic client + mpv audio player**. It replaced the
mopidy + mopidy-subidy stack. See `README.md` for the full story, feature status,
and Nix usage - this file is only what you need to work in the repo.

## Build, test, run

Cargo is not on `PATH` on its own; the toolchain, linker, and libmpv come from
the flake devshell. Always build/test inside it:

```
nix develop --command cargo build -p hypodj-core
nix develop --command cargo test  -p hypodj-core --lib
nix develop --command cargo build -p hypodj-daemon
```

- The devshell caps jobs (`CARGO_BUILD_JOBS`); never exceed 4 cores on this
  machine. Dev profile is `opt-level = 0`.
- One `#[ignore]` test needs a real libmpv runtime: run it manually with
  `cargo test -- --ignored`.
- "Test against a real server, not mocks." The `probe` / `play_probe` binaries
  and live Navidrome checks are the sanctioned proof for feature paths; unit
  tests cover parsing/mapping/logic. Deterministic time-based code (fades,
  timers, the executor) is tested under a **fake clock** (`clock.rs`,
  `tokio::test(start_paused)`), never wall-clock.

## Deploy

hypodj is packaged in `~/os-configurations` as the `hypodj` flake input
(`github:FamiliarTools/hypodj`). To ship a merged change:

1. push `master`, then in `~/os-configurations`: `nix flake update hypodj`
2. `nixos-rebuild build --flake .#bubble-gum --cores 4 --max-jobs 1`
3. the **user** runs `sudo nixos-rebuild switch ...` (agents cannot `sudo`:
   the sandbox sets "no new privileges")

The live daemon binds `127.0.0.1:6600`. Commit the `flake.lock` hypodj bump
scoped (`git commit -- flake.lock`), leaving unrelated lock churn unstaged.

## Architecture

The direction (see `README.md` "Current direction" + the roadmap) is a
**deterministic capability core** (player + queue + Subsonic select) with a typed
**Plan-IR trigger/executor** layered over it; an optional natural-language
translator only ever emits a *validated* Plan IR; embeddings are for content
selection only. Layout: `crates/hypodj-core` (config, model, subsonic, player,
mpd/handler, cache, scrobble, fade, clock) + `crates/hypodj-daemon` (the `hypodj`
binary + probes). Nothing outside `subsonic.rs` touches the `opensubsonic` wire
types - a one-file blast radius.

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
