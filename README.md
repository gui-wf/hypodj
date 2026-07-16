# hypodj - The DJ Underneath

A single Rust daemon that speaks the **MPD text protocol** to clients (ncmpcpp,
mpc, its own tools) and is itself an **OpenSubsonic client + mpv audio player**.
It replaced a mopidy + mopidy-subidy Python stack in one process: MPD commands in,
OpenSubsonic REST (browse / search / star / scrobble) and mpv-streamed audio out.

The name is from the Roman **hypocaust** (*hypo-* "beneath" + *kaustos* "burnt") -
the furnace under the bath, tended out of sight. hypodj is the DJ underneath: it
does the real work below while your client lounges in the warm room.

```
MPD client ──MPD text/TCP──▶ hypodj daemon ──OpenSubsonic REST──▶ Navidrome
(ncmpcpp, dj, dj-gui)             │                               (or any
                                  ▼                                OpenSubsonic
                             libmpv audio                          server)
```

## Ethos

Driving music should be human-native and **natural-language-first**: say what you
want in plain words ("play something calmer", "fade out in 20 minutes") and the
server handles the complexity. Natural language is translated server-side into a
validated plan and echoed back before it arms - never a surprise. Playback is
**startle-safe**: pause, resume, and skip fade instead of cutting, and volume
moves like a physical fader, not a step function.

## Clients

### HypoDJ (`dj-gui`) - the flagship

A ratatui terminal jukebox, pure MPD/TCP (no libmpv), event-driven via the
daemon's idle-push socket with worker-thread IO so the UI never blocks on network.

- Now-playing card with dithered album-art cover, up-next preview, and a physical
  volume fader
- Three screens: `1`/`2`/`3` = Queue / Albums / Playlists
- Vim-like navigation: `j`/`k`, `g`/`G`, scrolloff
- `/` incremental search with `n`/`N` match cycling and matched-substring highlight
- `:` command line - verbs plus natural language with echo-before-arm confirm
- Physical-potentiometer volume knob: perceptual dB detents, off-click pause
- Queue markers on albums (fully / partially enqueued) and songs

### `dj` - the CLI

Pure MPD/TCP, no libmpv. Bare `dj` prints a now-playing card; `dj next`,
`dj pause`, `dj vol 40` are quick verbs; anything else is natural language with
the same echo-before-arm confirm: `dj "play something calmer"`.

Any stock MPD client (ncmpcpp, mpc) also works unchanged against the daemon.

## What the daemon does

| Area | What |
| --- | --- |
| MPD server | Hand-rolled TCP server: full ncmpcpp command surface, `idle` push, binary `albumart`/`readpicture`, `sticker` ratings, `find`/`search`, playlists |
| Library | OpenSubsonic browse/search3, smart album lists, genres, radio/similar, first-class favorites (songs / albums / artists), scrobbling, TTL+LRU listing cache |
| Playback | libmpv actor; startle-safe fades on pause/resume/skip; graduated + humanized absolute volume; sleep / wind-down / wake; smooth restart |
| Intent | Deterministic capability core + typed Plan IR; the NL front-end (rules-first, optional local model) only ever emits a validated plan |
| Desktop | MPRIS, so GNOME media controls work |

## Install and run (Nix)

```
nix run github:FamiliarTools/hypodj#dj       # the CLI (default app)
nix run github:FamiliarTools/hypodj#dj-gui   # HypoDJ, the TUI
```

The flake ships `packages.hypodj` (the daemon, libmpv wrapped),
`packages.hypodj-clients` (`dj` + `dj-gui`), and one shared `services.hypodj`
module for both NixOS and Home-Manager:

```nix
{
  inputs.hypodj.url = "github:FamiliarTools/hypodj";

  imports = [ hypodj.nixosModules.default ];   # or homeManagerModules.default

  services.hypodj = {
    enable = true;
    server.url = "https://navidrome.example.com";
    server.username = "you";
    server.passwordFile = config.sops.secrets."hypodj/password".path;
  };
}
```

The password is read at service start into a `0600` runtime config - never the
Nix store (`passwordCommand` is the alternative to `passwordFile`). The service
runs headless (`audio = "null"`) by default; set `audio = "device"` when hypodj
should own the speakers. Then point any client at `mpd.bind`
(e.g. `ncmpcpp -h 127.0.0.1 -p 6601`, the default; 6600 is reserved for mopidy,
so use it only if you set `mpd.bind = "127.0.0.1:6600"`).

## Layout

| Crate | What |
| --- | --- |
| `crates/hypodj-core` | The library: config, model, subsonic, player, MPD handler, plan/executor, fade, MPRIS |
| `crates/hypodj-daemon` | `hypodj` daemon binary + `probe`/`play_probe` live provers |
| `crates/hypodj-nl` | Natural language to validated Plan IR (rules + optional local model) |
| `crates/hypodj-client` | Shared client lib (MPD connection, `nl` handshake, routing) |
| `crates/hypodj-cli` | `dj` |
| `crates/hypodj-tui` | `dj-gui` (product name HypoDJ) |

Building, testing, and deploying are covered in [CLAUDE.md](CLAUDE.md).
