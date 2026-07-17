# Waveform visualizer - the real music-responsive HUD wave

Status: draft
Created: 2026-07-16
Updated: 2026-07-16

## Context

The bottom HUD row in `dj-gui` today draws a wave that lies. It is a decorative
wall-clock animation (two incommensurate sines over the terminal width, seeded
per track) with a constant amplitude - it moves whether or not there is sound, it
does not react to loudness, and it keeps rolling through a fade-out or a pause.
The eye learns within seconds that the motion means nothing, and then ignores it.

The goal is a wave that is honestly caused by the audible signal: it swells with
loudness, hits on transients, settles as a fade recedes, and rests as a hairline
when nothing plays. Cause and effect over spectacle.

The data crux is that the client is a thin MPD/TCP terminal with no audio - all
audio lives in the daemon behind libmpv. So the real question is where the
amplitude comes from and how a tiny stream of numbers crosses to the client
without breaking the cross-machine daemon/client model.

Two hard constraints shape everything below:

- libmpv exposes no audio-render or PCM callback. `ao=pcm` / `ao=null` replace
  device output (`AudioOut`, `player.rs:377-392`), so a true PCM tap needs an OS
  monitor (PipeWire/Pulse) - which breaks the daemon-on-another-host model. This
  rules out a client-side FFT spectrum for the first version.
- The HUD is cosmetic. Nothing in this feature may ever be able to kill or
  degrade playback.

The chosen shape is a level meter (RMS + peak in dBFS), not a spectrum. It is the
90%-of-the-feel-at-10%-of-the-cost call, and it is honest: a level envelope drawn
as gapped EQ bars would be a lie, so the wave stays a continuous field.

## Data path

End-to-end chain:

```
mpv astats (in-process lavfi)
  -> mpv actor combines RMS with tracked softvol gain
  -> PlayerEvent::Viz on the existing mpsc (try_send, drop-on-Full)
  -> director spine
  -> dedicated viz broadcast channel
  -> viz TCP socket (MPD_port + 1)
  -> client viz worker
  -> latest-wins slot (Arc<Mutex<Option<VizSample>>>)
  -> renderer ballistics (attack/release envelope at render dt)
  -> bottom HUD row
```

### Amplitude source: labelled lavfi astats

Chosen over a PCM tap. mpv runs an audio-filter chain; `astats` computes RMS and
peak per window in-process at a few flops per sample (the approach Supersonic uses
in `peaks.c`). It is proven live on this exact stack (mpv 0.41.0, libmpv2-4.1):
`lavfi.astats.Overall.RMS_level` and `Overall.Peak_level` surface as node metadata
and the property observe fires.

The filter string must label the astats filter, not the chain. `@name:` labels a
single filter. Set:

```
@viz:astats=metadata=1:reset=1
```

Do NOT use `@viz:asetnsamples=n=1024:p=0,astats=...` - that binds `@viz` to
asetnsamples (which emits no metadata), leaving `af-metadata/viz` an empty node
and the HUD dead. asetnsamples is also dropped entirely: mpv coalesces
af-metadata property dispatch to ~20 Hz regardless, so fixing the sample cadence
buys nothing.

The chain is set non-fatally AFTER mpv construction, never inside
`Mpv::with_initializer` (`player.rs:418-437`). Inside the initializer a `?` on a
filter error (typo, stripped ffmpeg without astats) aborts mpv construction and
collapses the player to `NullPlayer` = no audio at all. Set it as:

```rust
let _ = mpv.set_property("af", "@viz:astats=metadata=1:reset=1");
```

A cosmetic HUD must never be able to silence the deck.

### Reading it

Beside the existing observes (`player.rs:511-513`) add:

```rust
ectx.observe_property("af-metadata/viz", Format::Node, 2)
```

In the `wait_event` loop (`player.rs:534-547`), on
`PropertyChange { name: "af-metadata/viz", .. }`, read the WHOLE node dict and
string-parse the map values for `lavfi.astats.Overall.RMS_level` and
`Overall.Peak_level` (dBFS). Do not observe the sub-key path
`af-metadata/viz/lavfi.astats...` - that path is broken and fires no events (mpv
regression a05b847).

Observe pushes reliably on this build (verified), so no poll fallback is needed.
If a future libmpv build only updates on get (mpv #14464), fall back to a single
`get_property::<Node>("af-metadata/viz")` per wakeup while Playing. The extended
live test (`player.rs:829`) resolves push-vs-poll empirically rather than by
assumption.

### Post-gain honesty

The user af chain runs BEFORE mpv's internal softvol, so astats is measured
pre-gain. The actor already owns the fader: track `cur_vol: f64` on
SetVolume/SetVolumeF64 (`player.rs:697-711`), which the fade drives via
`fade.rs` VolumeSink. Emit:

```
post_gain_db = rms_db + mpv_volume_to_db(cur_vol)   // cubic softvol, player.rs:161
```

This makes a fade-out a genuine descent of the level with zero extra design - the
single strongest "it's real" cue. The pre-vs-post ordering is kept a
runtime-verified flag, not an assumption: if astats turns out already post-softvol
on some build, the addition double-counts and fades read twice as deep, so the
extended live test confirms ordering and the code drops the addition if so.

### Frame and spine

Add `PlayerEvent::Viz { rms_db: f32, peak_db: f32, gain_db: f32, playing: bool }`
(`player.rs:68`), emitted from the mpv actor over the same mpsc via `try_send`
(drop-on-Full, the TimePos discipline at `player.rs:542` - never `blocking_send`).
`NullPlayer` emits no Viz.

Add `DjEventKind::Viz { rms_db, peak_db, gain_db, playing }` (`event.rs:63`; the
enum is `#[non_exhaustive]`, so downstream catch-all arms absorb it).

The spine (`director.rs:384`) matches `PlayerEvent::Viz` and republishes on a
DEDICATED viz broadcast channel, not the shared `DjEvent` broadcast. At ~20 fps,
riding the shared broadcast alongside Tick and edge mirrors raises `Lagged` churn
for every other lossy subscriber (`executor.rs:323`, `director.rs:619/943`),
forcing watch-resyncs. A separate high-rate channel isolates the stream cleanly.
Nothing viz touches the lossless edge/trigger path.

### DSP location: client, not daemon

The daemon ships raw dB numbers (rms/peak/gain). ALL musical shaping runs
client-side, so the wire stays tiny and each client tunes independently. See
Visual + motion design for the renderer math.

### Spectrum (deferred, P3)

A true log-spectrum needs raw PCM for rustfft, which forces the OS-monitor tap and
breaks the cross-machine model. It MUST NOT gate the first real version. astats
level plus the standing-wave texture is the honest P1/P2 shape.

## Side-channel protocol

A dedicated viz socket, chosen over piggybacking MPD. The MPD command socket
carries the owner-scoped `nl` handshake (`client mpd.rs:1-8`) and idle is a
STATE-only one-shot (`mpd.rs:1942`) - neither can carry a 20 fps binary stream.
This mirrors the client's existing dedicated-socket precedent (`idle_worker` and
`art_worker` each own a separate socket, `worker.rs:150-163`).

### Discovery (P1: derive, do not negotiate)

For P1 the client already knows the daemon host and MPD port, so it derives the
viz port directly as `MPD_port + 1` (default 6602; `default_mpd_bind` is 6601,
`config.rs:428`). A connection-refused IS the clean remote degrade signal - no new
MPD command needed. This drops the `vizinfo` capability command, proto versioning,
seq drop-detection, and `sub <bars> <fps>` negotiation from P1; they return in P2
if server-side downsample or rate-cap is wanted. `ADVERTISED_MPD_VERSION`
(`mpd.rs:50`) is untouched - viz is out of band.

### Handshake (per viz connection)

1. Server writes `OK HYPODJ-VIZ 1\n`.
2. Client subscribes a fresh receiver on the viz broadcast channel and streams
   frames until close.
3. Unsubscribe = close socket (the client tears the worker down on leaving the
   HUD/DJ view).

### Frame format

Minimal fixed frame, ~7 bytes at ~20 fps (~220 B/s):

```
[u8 flags: bit0 playing, bit1 post_gain_applied]
[i16 rms_db_centi][i16 peak_db_centi][i16 gain_db_centi]
```

dB as centi-dB `i16`, little-endian (-5400 = -54.00 dBFS) - integer, endian-fixed,
no float wire hazard. A text line is an acceptable P1 alternative for
debuggability. P2 may re-add `[u8 magic][u8 version][u16 seq]` framing when
versioned extension (P3 spectrum bands) is needed; a version byte then gates
decode so v1 clients ignore a tail.

### Backpressure (latest-wins both directions)

- Daemon to wire: the per-conn task owns a `broadcast::Receiver` on the viz
  channel; on `Lagged(n)` it does NOT resubscribe - it continues from newest (viz
  is inherently latest-wins). Writes are best-effort non-blocking; a wedged client
  closes only its own conn.
- Wire to render: the client viz worker decodes each frame into an
  `Arc<Mutex<Option<VizSample>>>` latest-wins slot (std Mutex, locked only for the
  swap, never across await or IO), NOT the merged Inbound channel (`worker.rs:76`)
  - 20 fps onto Inbound would flood the render drain. The render loop samples the
  slot each frame and runs the envelope at render dt, absorbing rate mismatch and
  network jitter in the ballistics.

### Thin-client preservation

The viz socket speaks only the tiny binary frame - no libmpv, no PCM, no FFT
crosses to the client. `dj` / `dj-gui` link zero new audio deps; the viz worker is
pure TCP plus integer decode. The daemon stays the sole audio owner (mpv on its
thread, `player.rs:448-451`). Cross-machine works: the viz port sits at the daemon
host's MPD port + 1.

## Visual + motion design

### The fable

A calm continuous swell that breathes with the actual loudness, hits on
transients, dies with fades, and rests as a hairline. Cause and effect over
spectacle. The per-track spatial texture keeps each song's wave visually its own.

### Placement and prominence

Stays in the existing 1-row borderless bottom bar (`ui.rs` `render_command`,
`chunks[4]`). No new rows, no border. The bar is already the least-prominent
surface; the redesign keeps it that way: styled with the solved OKLCH album color
but ALWAYS with `Modifier::DIM`, and the glyph ramp is capped below full height so
the row never fuses with the content above it.

Breathing room is built into the ramp, not the layout. Vertical range uses space
through `U+2587` (`▇`) only - FULL BLOCK `█` (`U+2588`, `BLOCK_GLYPHS[7]`,
`ui.rs:793`) is BANNED. The loudest possible music tops out at 7/8, leaving a
permanent 1/8 sliver of terminal background above the wave (top breathing room).
The quiet floor is `U+2581` (`▁`), a hairline that keeps the row grounded and
reads as "alive but at rest".

Horizontal: one glyph per column, full bar width, no separators or gaps. P1 data
is a level envelope, not a spectrum, so a continuous field is the honest shape.
Effective bar count = terminal width (80-200 independent column heights).

### Vertical resolution: eighth blocks, not braille

9-step ramp per column: `[' ','▁','▂','▃','▄','▅','▆','▇']`. Index 0 (space) is
reachable only during the pause decay-out; the playing floor is `▁`. Braille
Canvas (2x4 dots per cell) was rejected: at 1 row tall it reads as static, its
dots are far lighter than blocks (fights the DIM + 3:1 contrast budget), and font
coverage is worse over SSH and odd terminals. Eighth blocks are the codebase's
existing ramp (`BLOCK_GLYPHS`, with `ASCII_GLYPHS` fallback `_.:-=+*#` for
non-Unicode terminals).

### Amplitude to height mapping (musical, not jumpy)

Input per frame: `rms_db`, `peak_db` (dBFS, post-gain).

Normalize in the dB domain (perceptual):

```
a = clamp((post_gain_db - FLOOR_DB) / (CEIL_DB - FLOOR_DB), 0, 1)
    FLOOR_DB = -54, CEIL_DB = -6
a = a^0.8                 // gentle gamma; dB is already log, so a light expand of
                          // the quiet range keeps verses from flatlining
```

Column height reuses the existing two-sine standing wave as the SPATIAL texture,
now amplitude-driven instead of constant:

```
h(x) = 1 + round( (6 * A_smooth) * shape(x, t) )
```

`shape(x,t)` is the shipped standing wave (`ui.rs:840-848`, k1=0.35, k2=0.17,
w1=0.9, w2=0.6, seed_phase), remapped `[-1,1] -> [0.15,1.0]` so no column ever
dies to zero while playing. Silence gives a uniform `▁` hairline; a loud chorus
gives a rolling field peaking at `▇`. The temporal drift rates w1/w2 are kept.

Peak accent: when `peak_db - rms_db > 9 dB` (a transient), the 2-3 columns at the
current shape maxima get +1 glyph step for ~120 ms (peak-hold), then fall under
release gravity. Reads as the music hitting without strobing the whole row.

### Smoothing / ballistics (the "alive, not frantic" core)

One-pole asymmetric envelope on `A` (the normalized level), computed per rendered
frame with `dt`:

```
attack tau  = 60 ms    // rise is quick enough to feel causal (~2 frames)
release tau = 350 ms   // falls like a needle with gravity, never snaps
alpha = 1 - exp(-dt / tau)
```

Peak-hold: 600 ms hold, then linear fall at 2.5 glyph-steps/sec.

Viz frames arrive ~20 fps latest-wins; the render loop samples the slot and runs
the envelope at render dt, so network jitter never shows - the envelope IS the
anti-jitter layer.

### Startle-safe fade

Nothing special to draw. Because the level is post-gain, a fade-out IS a falling
`A` - the whole field settles toward the `▁` hairline exactly as the audible sound
recedes. The 350 ms release tau is well under the fade timescale, so the wave
tracks the fade, not its own inertia.

### ASCII mock

Bottom of `dj-gui`, 80 cols. Top rows shown for context; the HUD is the last row.

PLAYING (chorus, loud - field rolls, tops at 7/8, a sliver of bg always above):

```
│  3  Overmono - So U Kno                                    3:12  ██████░░░  │
├─────────────────────────────────────────────────────────────────────────────┤
 ▂▃▅▆▇▆▅▄▃▂▂▃▄▆▇▇▆▄▃▂▁▂▃▅▆▇▆▅▃▂▂▃▅▆▇▇▆▅▃▂▁▂▄▅▇▇▆▅▄▂▂▃▄▆▇▆▅▄▃▂▂▃▅▆▇▆▅▄▃▂▂▃▄▆▇▆▅
```

PLAYING (quiet verse - same shape, low amplitude):

```
 ▁▁▂▂▃▂▂▁▁▁▁▂▂▃▃▂▂▁▁▁▁▂▂▃▂▂▁▁▁▁▂▂▃▃▂▂▁▁▁▁▂▂▃▃▂▂▁▁▁▁▂▂▃▂▂▁▁▁▁▂▂▃▃▂▂▁▁▁▁▂▂▃▂▂▁▁
```

MID-FADE (startle-safe fade lowering gain - field genuinely settling):

```
 ▁▁▂▂▂▂▂▁▁▁▁▁▂▂▂▂▁▁▁▁▁▁▂▂▂▁▁▁▁▁▁▂▂▂▂▁▁▁▁▁▁▂▂▂▂▁▁▁▁▁▁▂▂▂▁▁▁▁▁▁▂▂▂▂▁▁▁▁▁▁▂▂▂▁▁▁
```

PAUSED / IDLE-STOPPED (resting hairline, dim gray, no motion):

```
 ▁▁▁▁▁▁▁▁▁▁▁▁▁▁▁▁▁▁▁▁▁▁▁▁▁▁▁▁▁▁▁▁▁▁▁▁▁▁▁▁▁▁▁▁▁▁▁▁▁
```

### States

- PLAYING: field as above, DIM + solved album color.
- PAUSED: no freeze-frame (a frozen wave looks broken). The envelope target drops
  to 0 and the field decays through release gravity to flat, then eases to a
  resting `▁` row over ~400 ms total; the row stays at `▁` in DIM gray (drop the
  album color) - "held breath". No animation while paused.
- IDLE/STOPPED (nothing loaded): completely flat `▁` row, DIM gray - identical to
  today's frozen baseline. No idle breathing animation: motion must mean audio;
  decorative motion at idle retrains the user to ignore the wave.
- UNSUBSCRIBED / OLD DAEMON / connect refused: fall back to the current decorative
  wall-clock wave, unchanged, still DIM - graceful degrade, never a blank
  regression.

## Color

Not re-derived here. The album swatch color is already solved: query the terminal
background via OSC 11, then pick an OKLCH album-derived hue nudged to clear WCAG
3:1 contrast against that background. That result is a pure `Style` input to the
wave row (`Modifier::DIM` when playing; plain DIM gray when paused/idle/fallback).
The visualizer consumes the swatch; it does not compute it.

## Latency and sync

Target: bars within ~50-100 ms of audible output.

- mpv output buffer (dominant): astats measures the DECODED signal, ahead of the
  DAC by mpv's output pre-buffer (~50-200 ms), so astats slightly LEADS the
  speaker. This is NOT compensated - per-device delay estimation is fragile and
  buys nothing visible for a level meter.
- Network + POLL sampling + envelope release (~350 ms) lag it back the other way.
- Net effect for a level meter: loose but fine for decoration. The honest claim is
  that the wave tracks what is being DECODED (slightly early), not precisely what
  the listener hears. The 350 ms release tau dominates the felt timing anyway.

## Degrade and perf

- No viz socket / connect refused / old daemon: client falls back to the current
  decorative wall-clock wave. Never a blank row.
- astats filter fails to set (stripped ffmpeg): non-fatal `let _`; playback is
  untouched, the daemon simply emits no Viz, and every client degrades to
  fallback.
- CPU: astats is a few flops per sample in-process; the wire is ~220 B/s; the
  client cost is integer decode plus a per-frame one-pole envelope over terminal
  width. All negligible.
- Broadcast isolation: the dedicated viz channel keeps 20 fps churn off the shared
  `DjEvent` subscribers, so no extra watch-resyncs elsewhere.
- Latest-wins everywhere means a slow client or slow render simply drops frames
  and shows the newest - no queue growth, no backpressure into audio.

## Phased plan

Each phase is independently shippable and has its own verification.

### P0 - probe (already done)

Confirmed live on mpv 0.41.0 / libmpv2-4.1 that `@viz:astats=metadata=1:reset=1`
produces `lavfi.astats.Overall.RMS_level` / `Peak_level` and that observe pushes.
Verification: manual live probe (done).

### P1 - real level wave, end to end

Daemon: set the af chain non-fatally post-construction; observe
`af-metadata/viz`; parse RMS/peak; add tracked `cur_vol` and post-gain; emit
`PlayerEvent::Viz`; spine republishes on a dedicated viz broadcast; new
`viz.rs` `serve_viz` TcpListener at MPD_port+1 with greeting + minimal frame
writer + Lagged=continue.
Client: `VizConn::connect(host, port+1)`, `VizSample`, a 4th `viz_worker`
(mirroring idle/art at `worker.rs:150-163`) decoding into the latest-wins slot;
`ui.rs` `wave_row` rewritten to take smoothed level A + post-gain, ramp capped at
index 6 (`▇`), floor `▁`; `render_command` runs the envelope at frame dt when the
viz worker is live, else the unchanged fallback wave.
Verification: extend the `#[ignore]` live test (`player.rs:829`) to assert Viz
events flow with sane dB and resolve push-vs-poll and pre-vs-post-gain
empirically; run `dj-gui` against a real daemon and watch the wave swell on a
chorus, settle on a fade, rest on pause. Build + test the WHOLE `--workspace`
(a new `#[non_exhaustive]` variant must not break exhaustive matches).

### P2 - protocol hardening (optional, only if needed)

Re-add `vizinfo` capability discovery, magic/version/seq framing, and
`sub <bars> <fps>` negotiation for server-side downsample / rate-cap and drop
detection. Bump the frame to version 2 with a version-gated decode.
Verification: old-client/new-daemon and new-client/old-daemon matrix; assert clean
ACK-unknown degrade and seq drop counting.

### P3 - true spectrum (deferred)

Only if the OS-monitor-tap tradeoff is accepted. Client-side rustfft over a PCM
monitor stream, log-band bars, versioned frame tail. Explicitly out of scope for
the honest first version.
Verification: spectral response to a swept sine; graceful fallback where no monitor
exists.

## Risks and the recommended MVP

From the critique:

- FATAL (resolved): the originally proposed filter string
  `@viz:asetnsamples=...,astats=...` labels asetnsamples, not astats, and ships a
  dead HUD. Corrected to `@viz:astats=metadata=1:reset=1`, verified live.
- MAJOR (resolved): setting `af` inside `with_initializer` with `?` can abort mpv
  construction and drop to `NullPlayer` = no audio. Set post-construction with a
  non-fatal `let _`.
- MAJOR (resolved): the transport was over-built for ~220 B/s. P1 drops discovery,
  versioning, seq, and bar/fps negotiation; the client derives port+1 and treats
  connect-refused as the degrade signal.
- MINOR (resolved): putting Viz on the shared `DjEvent` broadcast raises Lagged
  churn for other subscribers. Use a dedicated viz broadcast channel.
- MINOR (acknowledged): cadence is ~20 fps (mpv coalesces af-metadata dispatch),
  not 43 fps; asetnsamples buys nothing and is dropped. The stream is ~220 B/s.
- MINOR (hedged): astats leads the DAC by the output buffer; the "tracks what the
  listener hears" claim is softened to "tracks what is being decoded, slightly
  early". Fine for a level meter.
- MINOR (runtime-verified): pre-vs-post-softvol ordering stays a verified flag, not
  an assumption; drop the gain addition if astats is already post-softvol.

Recommended MVP: ship P1 only. Non-fatal post-construction af chain +
`af-metadata/viz` observe + post-gain + `PlayerEvent::Viz` on a dedicated viz
broadcast + a TcpListener at MPD_port+1 streaming a minimal frame + a 4th client
viz worker with a latest-wins slot + the client-side dB-normalize / gamma /
asymmetric-envelope / standing-wave-texture shaping. That is the whole honest
wave. Everything else (discovery, versioning, true spectrum) is deferred and must
not gate it.

## Open questions

- Push vs poll on other libmpv builds: P1's extended live test decides; the poll
  fallback is specified but should stay unshipped unless a build needs it.
- Pre- vs post-softvol ordering of astats: verified per build via the live test;
  the code carries the `post_gain_applied` flag either way.
- Frame encoding for P1: binary centi-dB (compact) vs a text line (debuggable) -
  either is acceptable; lean text-line for the first cut, switch to binary if the
  debug value stops paying rent.
- Fallback trigger UX: should the client show any subtle marker that it is on the
  decorative fallback (not the real wave), or stay silent? Leaning silent, since
  the wave is cosmetic and a "degraded" marker would draw more attention than the
  feature warrants.
- P2 threshold: what concrete need (rate-cap for battery, many concurrent viz
  clients, drop diagnostics) justifies re-adding the protocol ceremony. Until one
  appears, P1 stands.
