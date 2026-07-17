//! The SHARED visual-system primitives: terminal-background detection (OSC 11) and
//! cover-palette extraction, plus the WCAG contrast math and OKLCH nudges that turn a
//! raw album swatch into a legible waveform color (INFO policy, SC 1.4.11 >= 3:1) or a
//! muted sigil decoration (DECORATION policy, capped below a distraction ceiling).
//!
//! The album sigil and the waveform both read from exactly these two primitives -
//! cover -> [`Palette`] and OSC 11 -> [`TermBg`] - so the two features are one system.
//! Everything here is a PURE function except [`probe_bg`], which does the one-shot
//! stdin read (bounded so a non-answering terminal never hangs the TUI).

use std::io::{Read, Write};
use std::time::Duration;

use ratatui::style::Color;

use hypodj_client::config::Env;

/// The lingua franca to and from `ratatui::style::Color::Rgb`.
pub type Rgb = [u8; 3];

// --------------------------------------------------------------------------
// (a) OSC 11 terminal-background query.
// --------------------------------------------------------------------------

/// Where a resolved terminal background came from, in fallback order.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum BgSource {
    Osc11,
    ColorFgBg,
    // Reserved fallback tier (a TERM/COLORTERM luminance heuristic) between COLORFGBG
    // and the dark default; kept in the documented order even though the current chain
    // jumps straight to the dark default when no signal exists.
    #[allow(dead_code)]
    LuminanceGuess,
    DarkDefault,
}

/// The detected terminal background, with its relative luminance pre-computed and a
/// dark/light classification for the contrast policies.
#[derive(Debug, Clone, Copy, PartialEq)]
pub struct TermBg {
    pub rgb: Rgb,
    pub luminance: f32,
    pub is_dark: bool,
    pub source: BgSource,
}

impl TermBg {
    pub(crate) fn from_rgb(rgb: Rgb, source: BgSource) -> TermBg {
        let luminance = relative_luminance(rgb);
        TermBg { rgb, luminance, is_dark: luminance < 0.5, source }
    }

    /// The guaranteed near-black fallback: used whenever no reliable signal exists so
    /// the rest of the visual system always has a background to contrast against.
    pub fn dark_default() -> TermBg {
        TermBg::from_rgb([0x12, 0x12, 0x12], BgSource::DarkDefault)
    }
}

/// Probe the terminal background. Writes `ESC ]11;? BEL` to `out`, then reads the
/// reply from stdin with a bounded deadline (`timeout`, <= ~100ms in practice) so a
/// terminal or tmux that never answers can NEVER hang the caller. Fallback chain:
/// Osc11 -> `COLORFGBG` (`env`) -> dark default.
///
/// tmux NOTE: tmux swallows OSC 11 unless `allow-passthrough` / DCS wrapping is on;
/// there it simply times out and we fall back, which is the intended safe degrade. The
/// read is single-threaded with a hard deadline (no leaked reader thread), so a
/// non-answering terminal never leaves anything behind to race the event loop or eat
/// keystrokes. Contract: still call it before the crossterm event loop starts
/// consuming input, so the OSC answer is not interleaved with real key input.
pub fn probe_bg(out: &mut impl Write, env: &Env, timeout: Duration) -> TermBg {
    if out.write_all(b"\x1b]11;?\x07").is_ok() && out.flush().is_ok() {
        if let Some(reply) = read_osc_reply(timeout) {
            if let Some(rgb) = parse_osc11(&reply) {
                return TermBg::from_rgb(rgb, BgSource::Osc11);
            }
        }
        // We WROTE the query but got no valid reply within the deadline. A slow terminal
        // (foot/xterm over SSH with RTT+render past the deadline) can still emit its
        // `ESC ]11;rgb:... ESC\` reply AFTER we returned; those bytes would otherwise sit
        // in the tty buffer and be read by the crossterm event loop as spurious key
        // events (a stray escape burst stealing/injecting keystrokes at startup). Drain a
        // late/pending OSC reply so nothing bogus reaches the event loop. Bounded by the
        // same deadline so this can never hang, and gated to only consume a genuine OSC
        // introducer (`ESC ]`) so real typeahead is left untouched.
        drain_pending_osc(timeout);
    }
    if let Some(v) = (env.get)("COLORFGBG") {
        if let Some(rgb) = parse_colorfgbg(&v) {
            return TermBg::from_rgb(rgb, BgSource::ColorFgBg);
        }
    }
    TermBg::dark_default()
}

/// Read one OSC reply from stdin, bounded by `timeout`. Returns `None` on timeout /
/// EOF / error. Runs ENTIRELY on the caller's thread: it `poll(2)`s fd 0 for
/// readability up to the remaining deadline, then reads the ready byte(s), so it never
/// spawns a thread that could outlive the deadline and later race the crossterm event
/// loop for the tty (which would steal the user's keystrokes on a non-answering
/// terminal). A terminal that does not answer simply blocks in `poll` until the
/// deadline and we return `None`. Both OSC terminators are honored: BEL (0x07) AND ST
/// (ESC \), so ST-terminating terminals (foot and friends) deliver their reply.
fn read_osc_reply(timeout: Duration) -> Option<String> {
    use std::os::fd::AsRawFd;
    use std::time::Instant;
    let stdin = std::io::stdin();
    let fd = stdin.as_raw_fd();
    let deadline = Instant::now() + timeout;
    let mut buf: Vec<u8> = Vec::new();
    let mut byte = [0u8; 1];
    let mut prev = 0u8;
    loop {
        let remaining = deadline.checked_duration_since(Instant::now())?;
        if !wait_readable(fd, remaining) {
            // Timeout (or poll error): return whatever we have so a partial-but-valid
            // reply still parses, else None.
            break;
        }
        match stdin.lock().read(&mut byte) {
            Ok(0) => break, // EOF
            Ok(_) => {
                let b = byte[0];
                buf.push(b);
                // Terminators: BEL, or ST (the byte after an ESC is a backslash). Cap
                // the read so a stream of unrelated bytes cannot grow it without bound.
                if b == 0x07 || (b == b'\\' && prev == 0x1b) || buf.len() >= 64 {
                    break;
                }
                prev = b;
            }
            Err(_) => break,
        }
    }
    if buf.is_empty() {
        return None;
    }
    String::from_utf8(buf).ok()
}

/// A pending OSC 11 reply is `ESC ]11;rgb:RRRR/GGGG/BBBB` + terminator - at least ~20
/// bytes, delivered by the terminal in a single write. Ordinary typeahead trickles one
/// key (or a 3-byte arrow) at a time. So a burst of at least this many bytes waiting on
/// the fd is the terminal's late reply, not something the user typed; below it we assume
/// typeahead and never touch the buffer. This is the discriminator that keeps the drain
/// ZERO-steal for real input.
const OSC_REPLY_MIN_BYTES: i32 = 12;

/// Drain a LATE OSC reply that a slow terminal emits after [`read_osc_reply`] already
/// gave up, so the crossterm event loop never reads `ESC ]11;rgb:...` as bogus key
/// events. Runs on the caller's thread, bounded by `grace`. It waits for input to become
/// readable, then only drains when (a) a full OSC-reply-sized BURST is already queued
/// (see [`OSC_REPLY_MIN_BYTES`]) and (b) it actually begins with the OSC introducer
/// `ESC ]`. Ordinary typeahead - a keystroke or two, which is both too short and not an
/// OSC introducer - is left in the buffer untouched. Best-effort: if nothing arrives
/// within `grace`, or what arrives is not the solicited reply, it returns without
/// disturbing the buffer.
fn drain_pending_osc(grace: Duration) {
    use std::os::fd::AsRawFd;
    let fd = std::io::stdin().as_raw_fd();
    drain_pending_osc_fd(fd, grace);
}

/// Bytes currently queued on `fd` (`FIONREAD`), or 0 on error. Lets the drain tell a
/// full OSC-reply burst from a stray keystroke WITHOUT consuming anything.
fn pending_bytes(fd: std::os::fd::RawFd) -> i32 {
    let mut n: libc::c_int = 0;
    let r = unsafe { libc::ioctl(fd, libc::FIONREAD, &mut n) };
    if r == 0 {
        n
    } else {
        0
    }
}

/// The fd-parameterized core of [`drain_pending_osc`], so a pipe can stand in for the
/// tty under test.
fn drain_pending_osc_fd(fd: std::os::fd::RawFd, grace: Duration) {
    use std::time::Instant;
    let deadline = Instant::now() + grace;
    let mut byte = [0u8; 1];
    let read_one = |b: &mut [u8; 1]| -> Option<u8> {
        // A raw fd read; one byte or nothing (EOF / error).
        let n = unsafe { libc::read(fd, b.as_mut_ptr() as *mut libc::c_void, 1) };
        if n == 1 {
            Some(b[0])
        } else {
            None
        }
    };
    // Wait until something is readable, then decide from the QUEUED count alone (no
    // destructive read): only an OSC-reply-sized burst is drainable; anything shorter is
    // treated as typeahead and left alone.
    let Some(remaining) = deadline.checked_duration_since(Instant::now()) else { return };
    if !wait_readable(fd, remaining) {
        return;
    }
    if pending_bytes(fd) < OSC_REPLY_MIN_BYTES {
        return;
    }
    // Big enough to be the reply. Confirm the OSC introducer `ESC ]` before draining; if
    // it is not there (some other large burst, e.g. a paste), stop.
    if read_one(&mut byte) != Some(0x1b) {
        return;
    }
    let Some(remaining) = deadline.checked_duration_since(Instant::now()) else { return };
    if !wait_readable(fd, remaining) || read_one(&mut byte) != Some(b']') {
        return;
    }
    // Confirmed OSC introducer: swallow the rest of the sequence up to its terminator
    // (BEL, or ST = ESC `\`), bounded by the grace deadline and a byte cap.
    let mut prev = 0u8;
    for _ in 0..128 {
        let Some(remaining) = deadline.checked_duration_since(Instant::now()) else { return };
        if !wait_readable(fd, remaining) {
            return;
        }
        let Some(b) = read_one(&mut byte) else { return };
        if b == 0x07 || (b == b'\\' && prev == 0x1b) {
            return;
        }
        prev = b;
    }
}

/// Block until `fd` is readable or `timeout` elapses. `true` => readable, `false` =>
/// timeout or poll error. A `poll(2)` wrapper so the OSC read stays on one thread with
/// a hard deadline and no non-blocking-mode fiddling on the shared tty fd.
fn wait_readable(fd: std::os::fd::RawFd, timeout: Duration) -> bool {
    let mut pfd = libc::pollfd { fd, events: libc::POLLIN, revents: 0 };
    let ms = timeout.as_millis().min(i32::MAX as u128) as i32;
    let r = unsafe { libc::poll(&mut pfd, 1, ms) };
    r > 0 && (pfd.revents & libc::POLLIN) != 0
}

/// Parse an OSC 11 reply body: `...rgb:RRRR/GGGG/BBBB...` or `rgba:.../...`. Each
/// channel is 1-4 hex digits scaled to 8-bit. Returns `None` on anything malformed.
/// Pure and unit-tested.
pub fn parse_osc11(reply: &str) -> Option<Rgb> {
    let tail = reply.split("rgb:").nth(1).or_else(|| reply.split("rgba:").nth(1))?;
    // Stop at the terminator (BEL / ESC / ST-backslash) if present.
    let tail: String = tail
        .chars()
        .take_while(|&c| c != '\x07' && c != '\x1b' && c != '\\')
        .collect();
    let mut it = tail.split('/');
    let r = scale_hex(it.next()?)?;
    let g = scale_hex(it.next()?)?;
    let b = scale_hex(it.next()?)?;
    Some([r, g, b])
}

/// Scale a 1-4 hex-digit channel to 8-bit (e.g. `1c1c` -> 0x1c, `ff` -> 0xff).
fn scale_hex(s: &str) -> Option<u8> {
    let s = s.trim();
    if s.is_empty() || s.len() > 4 || !s.chars().all(|c| c.is_ascii_hexdigit()) {
        return None;
    }
    let v = u32::from_str_radix(s, 16).ok()?;
    let max = (1u32 << (4 * s.len())) - 1;
    Some(((v * 255 + max / 2) / max) as u8)
}

/// Parse `COLORFGBG` (`"15;0"` or `"15;default;0"`) into the background color: the
/// LAST field is the bg index, mapped through the xterm base-16 palette. `None` when
/// the bg field is non-numeric (e.g. `default`). Pure and unit-tested.
pub fn parse_colorfgbg(val: &str) -> Option<Rgb> {
    let last = val.split(';').next_back()?.trim();
    let idx: usize = last.parse().ok()?;
    XTERM16.get(idx).copied()
}

/// The xterm base-16 palette (system colors 0..15), for `COLORFGBG` mapping.
const XTERM16: [Rgb; 16] = [
    [0x00, 0x00, 0x00],
    [0x80, 0x00, 0x00],
    [0x00, 0x80, 0x00],
    [0x80, 0x80, 0x00],
    [0x00, 0x00, 0x80],
    [0x80, 0x00, 0x80],
    [0x00, 0x80, 0x80],
    [0xc0, 0xc0, 0xc0],
    [0x80, 0x80, 0x80],
    [0xff, 0x00, 0x00],
    [0x00, 0xff, 0x00],
    [0xff, 0xff, 0x00],
    [0x00, 0x00, 0xff],
    [0xff, 0x00, 0xff],
    [0x00, 0xff, 0xff],
    [0xff, 0xff, 0xff],
];

// --------------------------------------------------------------------------
// (c) WCAG relative luminance + contrast, and the OKLCH nudge policies.
// --------------------------------------------------------------------------

fn srgb_to_linear(c: u8) -> f32 {
    let c = c as f32 / 255.0;
    if c <= 0.04045 {
        c / 12.92
    } else {
        ((c + 0.055) / 1.055).powf(2.4)
    }
}

fn linear_to_srgb(c: f32) -> u8 {
    let c = c.clamp(0.0, 1.0);
    let v = if c <= 0.0031308 {
        c * 12.92
    } else {
        1.055 * c.powf(1.0 / 2.4) - 0.055
    };
    (v * 255.0).round().clamp(0.0, 255.0) as u8
}

/// WCAG relative luminance in `[0, 1]` (black = 0, white = 1). Pure and unit-tested.
pub fn relative_luminance(c: Rgb) -> f32 {
    let r = srgb_to_linear(c[0]);
    let g = srgb_to_linear(c[1]);
    let b = srgb_to_linear(c[2]);
    0.2126 * r + 0.7152 * g + 0.0722 * b
}

/// WCAG contrast ratio in `1.0..=21.0`, order-free. Pure and unit-tested.
pub fn contrast_ratio(a: Rgb, b: Rgb) -> f32 {
    let la = relative_luminance(a);
    let lb = relative_luminance(b);
    let (hi, lo) = if la >= lb { (la, lb) } else { (lb, la) };
    (hi + 0.05) / (lo + 0.05)
}

/// A color in the OKLCH perceptual space (lightness, chroma, hue-radians).
#[derive(Debug, Clone, Copy, PartialEq)]
pub struct Oklch {
    pub l: f32,
    pub c: f32,
    pub h: f32,
}

/// sRGB -> OKLCH (Ottosson's Oklab, expressed as lightness/chroma/hue).
pub fn srgb_to_oklch(c: Rgb) -> Oklch {
    let r = srgb_to_linear(c[0]);
    let g = srgb_to_linear(c[1]);
    let b = srgb_to_linear(c[2]);
    let l = 0.4122214708 * r + 0.5363325363 * g + 0.0514459929 * b;
    let m = 0.2119034982 * r + 0.6806995451 * g + 0.1073969566 * b;
    let s = 0.0883024619 * r + 0.2817188376 * g + 0.6299787005 * b;
    let l_ = l.cbrt();
    let m_ = m.cbrt();
    let s_ = s.cbrt();
    let ll = 0.2104542553 * l_ + 0.7936177850 * m_ - 0.0040720468 * s_;
    let aa = 1.9779984951 * l_ - 2.4285922050 * m_ + 0.4505937099 * s_;
    let bb = 0.0259040371 * l_ + 0.7827717662 * m_ - 0.8086757660 * s_;
    Oklch { l: ll, c: (aa * aa + bb * bb).sqrt(), h: bb.atan2(aa) }
}

/// OKLCH -> sRGB, gamut-clamped by clamping the linear channels.
pub fn oklch_to_srgb(o: Oklch) -> Rgb {
    let a = o.c * o.h.cos();
    let b = o.c * o.h.sin();
    let l_ = o.l + 0.3963377774 * a + 0.2158037573 * b;
    let m_ = o.l - 0.1055613458 * a - 0.0638541728 * b;
    let s_ = o.l - 0.0894841775 * a - 1.2914855480 * b;
    let l = l_ * l_ * l_;
    let m = m_ * m_ * m_;
    let s = s_ * s_ * s_;
    let r = 4.0767416621 * l - 3.3077115913 * m + 0.2309699292 * s;
    let g = -1.2684380046 * l + 2.6097574011 * m - 0.3413193965 * s;
    let bl = -0.0041960863 * l - 0.7034186147 * m + 1.7076147010 * s;
    [linear_to_srgb(r), linear_to_srgb(g), linear_to_srgb(bl)]
}

/// The INFO contrast target for the waveform: a graphic (SC 1.4.11 non-text) needs
/// >= 3:1 against the background.
const INFO_TARGET: f32 = 3.0;

/// The luminance where contrast to black equals contrast to white:
/// `(L+0.05)^2 = 0.05 * 1.05` => `L ~= 0.179`. BELOW it, pushing lightness UP (toward
/// white) has more contrast headroom; ABOVE it, pushing DOWN (toward black) does. This
/// is the correct nudge pivot - NOT `is_dark`'s 0.5 midpoint, which drives medium-gray
/// backgrounds the low-headroom way and can never reach 3:1.
const INFO_PIVOT: f32 = 0.179;

/// The lightness-nudge direction (+1 up / -1 down) that maximizes contrast headroom
/// against `bg`, pivoting on [`INFO_PIVOT`].
fn info_dir(bg: TermBg) -> f32 {
    if bg.luminance < INFO_PIVOT {
        1.0
    } else {
        -1.0
    }
}

/// The legible neutral for a degenerate fit (swatch ~= bg, or hue pinned in a corner):
/// near-white on darker-than-pivot backgrounds, near-black otherwise.
fn info_neutral(bg: TermBg) -> Rgb {
    if bg.luminance < INFO_PIVOT {
        [0xe0, 0xe0, 0xe0]
    } else {
        [0x20, 0x20, 0x20]
    }
}

/// INFO policy (the waveform): push the swatch's OKLCH lightness AWAY from the
/// background in small steps (re-clamping chroma to gamut each step, preserving hue)
/// until it clears >= 3:1. A swatch already indistinguishable from the bg degenerates
/// to a near-fg neutral. NOTE: this guarantees >= 3:1 in TRUECOLOR; on a 256-color
/// terminal use [`info_color`], which re-checks contrast after quantization. Pure and
/// unit-tested.
pub fn fit_info(swatch: Rgb, bg: TermBg) -> Rgb {
    if contrast_ratio(swatch, bg.rgb) >= INFO_TARGET {
        return swatch;
    }
    let mut o = srgb_to_oklch(swatch);
    let dir = info_dir(bg);
    for _ in 0..50 {
        o.l = (o.l + dir * 0.02).clamp(0.0, 1.0);
        let c = oklch_to_srgb(o);
        if contrast_ratio(c, bg.rgb) >= INFO_TARGET {
            return c;
        }
    }
    // Degenerate (swatch ~= bg, or hue pinned in a corner): a legible neutral.
    info_neutral(bg)
}

/// The INFO policy as an actual ratatui [`Color`], honoring the terminal's color depth.
/// In truecolor this is just `fit_info` -> `Color::Rgb`. On a 256-color terminal the
/// >= 3:1 guarantee that `fit_info` makes in RGB space can be LOST when the color is
/// snapped to the xterm-256 palette, so here we re-check contrast on the REALIZED
/// quantized color and keep nudging (in the same headroom direction) until the on-screen
/// cell itself clears 3:1, falling back to a quantized legible neutral. Pure and
/// unit-tested.
pub fn info_color(swatch: Rgb, bg: TermBg, truecolor: bool) -> Color {
    let fitted = fit_info(swatch, bg);
    if truecolor {
        return Color::Rgb(fitted[0], fitted[1], fitted[2]);
    }
    let (idx, realized) = quantize_256(fitted);
    if contrast_ratio(realized, bg.rgb) >= INFO_TARGET {
        return Color::Indexed(idx);
    }
    // Quantization dropped it below the floor: nudge lightness further away from bg and
    // test the realized cell each step.
    let mut o = srgb_to_oklch(fitted);
    let dir = info_dir(bg);
    for _ in 0..50 {
        o.l = (o.l + dir * 0.02).clamp(0.0, 1.0);
        let (idx, realized) = quantize_256(oklch_to_srgb(o));
        if contrast_ratio(realized, bg.rgb) >= INFO_TARGET {
            return Color::Indexed(idx);
        }
    }
    Color::Indexed(quantize_256(info_neutral(bg)).0)
}

/// The DECORATION distraction ceiling: a sigil may never reach the >= 3:1 INFO floor
/// (that would read as information, not decoration). Kept comfortably below it so even
/// a light-dominant cover on a dark terminal stays a quiet texture.
const DECO_CEILING: f32 = 2.0;

/// DECORATION policy (the album sigil): pull chroma down ~50% and nudge lightness
/// TOWARD the background so the sigil sits BELOW a distraction ceiling - present, but
/// never a 3:1 attention-grabber. A light-dominant cover on a dark bg (or vice versa)
/// can still clear the ceiling after the fixed blend, so we then pull lightness the rest
/// of the way toward the bg in small steps until contrast drops under [`DECO_CEILING`].
/// Pure and unit-tested.
pub fn fit_decoration(swatch: Rgb, bg: TermBg) -> Rgb {
    let mut o = srgb_to_oklch(swatch);
    let bgo = srgb_to_oklch(bg.rgb);
    o.c *= 0.5;
    o.l = o.l * 0.55 + bgo.l * 0.45;
    // Enforce the distraction ceiling: step lightness toward the bg's lightness until
    // the realized contrast sits under the cap (or we have collapsed onto the bg).
    for _ in 0..50 {
        if contrast_ratio(oklch_to_srgb(o), bg.rgb) <= DECO_CEILING {
            break;
        }
        o.l += (bgo.l - o.l) * 0.2;
    }
    oklch_to_srgb(o)
}

// --------------------------------------------------------------------------
// (b) Cover palette extraction.
// --------------------------------------------------------------------------

/// Two-to-three ranked swatches pulled from a cover: `vibrant` (highest-chroma
/// mid-lightness), `muted` (lowest-chroma), and the ranked `swatches` list.
#[derive(Debug, Clone, PartialEq)]
pub struct Palette {
    pub vibrant: Rgb,
    pub muted: Rgb,
    pub swatches: Vec<Rgb>,
}

impl Palette {
    /// A neutral grey palette, the image-less / decode-failure fallback.
    pub fn neutral() -> Palette {
        Palette { vibrant: [0x88, 0x88, 0x88], muted: [0x55, 0x55, 0x55], swatches: vec![[0x88, 0x88, 0x88]] }
    }
}

/// Extract a ranked palette from an already-decoded cover thumbnail: downsample,
/// median-cut into up to four buckets, then rank by OKLCH chroma. `vibrant` is the
/// highest-chroma mid-lightness cluster; `muted` the lowest-chroma. Hand-rolled (no
/// palette crate). Pure and unit-tested.
pub fn extract_palette(img: &image::RgbImage) -> Palette {
    // Downsample: stride the pixels so a 96x96 thumb costs a few hundred samples.
    let (w, h) = img.dimensions();
    if w == 0 || h == 0 {
        return Palette::neutral();
    }
    let step = ((w.max(h) / 48).max(1)) as usize;
    let mut pixels: Vec<Rgb> = Vec::new();
    for y in (0..h as usize).step_by(step) {
        for x in (0..w as usize).step_by(step) {
            let p = img.get_pixel(x as u32, y as u32);
            pixels.push([p[0], p[1], p[2]]);
        }
    }
    if pixels.is_empty() {
        return Palette::neutral();
    }
    let buckets = median_cut(pixels, 4);
    let mut swatches: Vec<Rgb> = buckets;
    // Rank by chroma, high to low.
    swatches.sort_by(|a, b| {
        srgb_to_oklch(*b)
            .c
            .partial_cmp(&srgb_to_oklch(*a).c)
            .unwrap_or(std::cmp::Ordering::Equal)
    });
    // vibrant: highest chroma with a mid lightness (so it is not near-black/white);
    // fall back to the plain highest chroma when none sits in the mid band.
    let vibrant = swatches
        .iter()
        .copied()
        .find(|c| {
            let o = srgb_to_oklch(*c);
            (0.35..=0.85).contains(&o.l)
        })
        .unwrap_or(swatches[0]);
    let muted = *swatches.last().unwrap();
    swatches.truncate(3);
    Palette { vibrant, muted, swatches }
}

/// Median-cut `pixels` into at most `k` buckets, returning each bucket's average
/// color. Splits the bucket with the widest single-channel range at its median.
fn median_cut(pixels: Vec<Rgb>, k: usize) -> Vec<Rgb> {
    let mut buckets: Vec<Vec<Rgb>> = vec![pixels];
    while buckets.len() < k {
        // Pick the bucket with the greatest channel range.
        let mut best = 0usize;
        let mut best_range = -1i32;
        for (i, b) in buckets.iter().enumerate() {
            if b.len() < 2 {
                continue;
            }
            let r = channel_range(b).0;
            if r > best_range {
                best_range = r;
                best = i;
            }
        }
        if best_range < 0 {
            break;
        }
        let bucket = buckets.remove(best);
        let ch = channel_range(&bucket).1;
        let mut sorted = bucket;
        sorted.sort_by_key(|p| p[ch]);
        let mid = sorted.len() / 2;
        let hi = sorted.split_off(mid);
        buckets.push(sorted);
        buckets.push(hi);
    }
    buckets.iter().map(|b| average(b)).collect()
}

/// The widest channel range of a bucket and which channel it is on.
fn channel_range(b: &[Rgb]) -> (i32, usize) {
    let mut best = (-1i32, 0usize);
    for ch in 0..3 {
        let (mut lo, mut hi) = (255u8, 0u8);
        for p in b {
            lo = lo.min(p[ch]);
            hi = hi.max(p[ch]);
        }
        let r = hi as i32 - lo as i32;
        if r > best.0 {
            best = (r, ch);
        }
    }
    best
}

fn average(b: &[Rgb]) -> Rgb {
    if b.is_empty() {
        return [0, 0, 0];
    }
    let mut sum = [0u64; 3];
    for p in b {
        for ch in 0..3 {
            sum[ch] += p[ch] as u64;
        }
    }
    let n = b.len() as u64;
    [(sum[0] / n) as u8, (sum[1] / n) as u8, (sum[2] / n) as u8]
}

// --------------------------------------------------------------------------
// (d) Image-protocol capability + color emission.
// --------------------------------------------------------------------------

/// A terminal inline-image protocol, if any. `None` means the album sigil should be
/// used in the album-art slot's image-less path.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum ImageProtocol {
    Kitty,
    Sixel,
    Iterm2,
    None,
}

/// Detect an inline-image protocol from the environment only (no terminal query, so
/// it can never hang). `None` => use the album sigil. Pure and unit-tested.
pub fn image_protocol(env: &Env) -> ImageProtocol {
    let term = (env.get)("TERM").unwrap_or_default().to_lowercase();
    let prog = (env.get)("TERM_PROGRAM").unwrap_or_default();
    if (env.get)("KITTY_WINDOW_ID").is_some() || term.contains("kitty") || prog == "WezTerm" {
        return ImageProtocol::Kitty;
    }
    if prog == "iTerm.app" {
        return ImageProtocol::Iterm2;
    }
    if term.contains("sixel") || term.contains("mlterm") || term.contains("foot") {
        return ImageProtocol::Sixel;
    }
    ImageProtocol::None
}

/// Whether the terminal advertises truecolor via `COLORTERM`. Pure and unit-tested.
pub fn truecolor(env: &Env) -> bool {
    matches!(
        (env.get)("COLORTERM").as_deref(),
        Some("truecolor") | Some("24bit")
    )
}

/// The xterm 6-level cube axis values.
const CUBE: [u8; 6] = [0, 95, 135, 175, 215, 255];

/// Nearest xterm-256 color: returns the index AND the color it actually realizes, so
/// the caller can re-run [`contrast_ratio`] POST-quantization. Considers the 6x6x6
/// cube (16..231) and the 24-step gray ramp (232..255). Pure and unit-tested.
pub fn quantize_256(c: Rgb) -> (u8, Rgb) {
    let dist = |a: Rgb, b: Rgb| -> i32 {
        (0..3).map(|i| {
            let d = a[i] as i32 - b[i] as i32;
            d * d
        }).sum()
    };
    let mut best_idx = 16u8;
    let mut best_rgb = [0u8; 3];
    let mut best_d = i32::MAX;
    for r in 0..6 {
        for g in 0..6 {
            for b in 0..6 {
                let rgb = [CUBE[r], CUBE[g], CUBE[b]];
                let d = dist(rgb, c);
                if d < best_d {
                    best_d = d;
                    best_rgb = rgb;
                    best_idx = 16 + (36 * r + 6 * g + b) as u8;
                }
            }
        }
    }
    for j in 0..24u8 {
        let v = 8 + j * 10;
        let rgb = [v, v, v];
        let d = dist(rgb, c);
        if d < best_d {
            best_d = d;
            best_rgb = rgb;
            best_idx = 232 + j;
        }
    }
    (best_idx, best_rgb)
}

/// Turn an `Rgb` into a ratatui `Color`: truecolor `Color::Rgb`, else the nearest
/// xterm-256 `Color::Indexed`.
pub fn to_color(c: Rgb, truecolor: bool) -> Color {
    if truecolor {
        Color::Rgb(c[0], c[1], c[2])
    } else {
        Color::Indexed(quantize_256(c).0)
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn env_of(pairs: &[(&'static str, &'static str)]) -> Vec<(String, String)> {
        pairs.iter().map(|(k, v)| (k.to_string(), v.to_string())).collect()
    }

    fn mkenv<'a>(pairs: &'a [(String, String)]) -> Env<'a> {
        Env {
            get: Box::leak(Box::new(move |k: &str| {
                pairs.iter().find(|(pk, _)| pk == k).map(|(_, v)| v.clone())
            })),
        }
    }

    #[test]
    fn parse_osc11_rgb_rgba_and_digit_widths() {
        // Standard 4-hex-digit reply.
        assert_eq!(
            parse_osc11("\x1b]11;rgb:1c1c/1c1c/1c1c\x07"),
            Some([0x1c, 0x1c, 0x1c])
        );
        // White, and an rgba variant.
        assert_eq!(parse_osc11("rgb:ffff/ffff/ffff"), Some([0xff, 0xff, 0xff]));
        assert_eq!(parse_osc11("rgba:0000/0000/0000/ffff"), Some([0, 0, 0]));
        // 2-digit channels.
        assert_eq!(parse_osc11("rgb:ff/80/00"), Some([0xff, 0x80, 0x00]));
        // 1-digit channel (f -> full).
        assert_eq!(parse_osc11("rgb:f/0/0"), Some([0xff, 0x00, 0x00]));
        // ST-terminated reply (ESC backslash) parses identically to BEL-terminated:
        // foot and other ST-replying terminals must be honored, not just BEL.
        assert_eq!(
            parse_osc11("\x1b]11;rgb:1c1c/1c1c/1c1c\x1b\\"),
            Some([0x1c, 0x1c, 0x1c])
        );
        // ST with 2-digit channels too.
        assert_eq!(parse_osc11("rgb:ff/80/00\x1b\\"), Some([0xff, 0x80, 0x00]));
        // Malformed -> None.
        assert_eq!(parse_osc11("garbage"), None);
        assert_eq!(parse_osc11("rgb:zz/00/00"), None);
        assert_eq!(parse_osc11("rgb:1c1c/1c1c"), None);
    }

    #[test]
    fn parse_colorfgbg_forms() {
        assert_eq!(parse_colorfgbg("15;0"), Some([0x00, 0x00, 0x00]));
        assert_eq!(parse_colorfgbg("0;15"), Some([0xff, 0xff, 0xff]));
        assert_eq!(parse_colorfgbg("15;default;0"), Some([0x00, 0x00, 0x00]));
        // Non-numeric bg field -> None.
        assert_eq!(parse_colorfgbg("15;default"), None);
        assert_eq!(parse_colorfgbg(""), None);
    }

    #[test]
    fn luminance_and_contrast_anchors() {
        assert!((relative_luminance([0, 0, 0])).abs() < 1e-6);
        assert!((relative_luminance([255, 255, 255]) - 1.0).abs() < 1e-6);
        // Black vs white is the canonical 21:1.
        assert!((contrast_ratio([0, 0, 0], [255, 255, 255]) - 21.0).abs() < 0.01);
        // Order-free.
        assert_eq!(
            contrast_ratio([10, 20, 30], [200, 200, 200]),
            contrast_ratio([200, 200, 200], [10, 20, 30])
        );
    }

    #[test]
    fn oklch_round_trips() {
        for c in [[120, 60, 200], [255, 0, 0], [10, 200, 90], [200, 200, 200]] {
            let back = oklch_to_srgb(srgb_to_oklch(c));
            for i in 0..3 {
                assert!((back[i] as i32 - c[i] as i32).abs() <= 2, "round trip {c:?} -> {back:?}");
            }
        }
    }

    #[test]
    fn fit_info_clears_three_to_one_on_dark_and_light() {
        let swatch = [90, 70, 60]; // a dim brown, low contrast on both extremes
        let dark = TermBg::dark_default();
        let light = TermBg::from_rgb([0xf0, 0xf0, 0xf0], BgSource::DarkDefault);
        let on_dark = fit_info(swatch, dark);
        let on_light = fit_info(swatch, light);
        assert!(contrast_ratio(on_dark, dark.rgb) >= INFO_TARGET, "clears on dark");
        assert!(contrast_ratio(on_light, light.rgb) >= INFO_TARGET, "clears on light");
        // Hue is roughly preserved (only lightness was nudged).
        let h0 = srgb_to_oklch(swatch).h;
        let h1 = srgb_to_oklch(on_dark).h;
        assert!((h0 - h1).abs() < 0.35, "hue preserved within tolerance");
    }

    #[test]
    fn fit_info_clears_on_medium_gray_backgrounds() {
        // Medium grays (luminance in ~[0.30, 0.50)) classify is_dark=true but have far
        // more contrast headroom toward BLACK. The pivot must drive DOWN, not up.
        let swatch = [220, 40, 40];
        for g in [150u8, 160, 170, 180] {
            let bg = TermBg::from_rgb([g, g, g], BgSource::DarkDefault);
            let fitted = fit_info(swatch, bg);
            assert!(
                contrast_ratio(fitted, bg.rgb) >= INFO_TARGET,
                "clears 3:1 on gray {g}: got {:.2} for {fitted:?}",
                contrast_ratio(fitted, bg.rgb)
            );
        }
    }

    #[test]
    fn info_color_holds_three_to_one_after_quantization() {
        // The finding's failing case and a brute sweep: on a 256-color terminal the
        // REALIZED xterm-256 cell must still clear 3:1, not just the truecolor RGB.
        let bg = TermBg::dark_default();
        let realized = |c: Rgb| -> Rgb {
            match info_color(c, bg, false) {
                Color::Indexed(i) => {
                    // Recover the realized rgb by re-quantizing the fitted color is not
                    // exposed; instead find the cube/gray cell for index i.
                    idx_to_rgb(i)
                }
                _ => unreachable!("non-truecolor path returns Indexed"),
            }
        };
        // Regression anchor from the finding.
        assert!(contrast_ratio(realized([0, 14, 7]), bg.rgb) >= INFO_TARGET);
        // Sweep a spread of swatches; every one must clear post-quantization.
        for r in (0..=255).step_by(51) {
            for g in (0..=255).step_by(51) {
                for b in (0..=255).step_by(51) {
                    let got = realized([r as u8, g as u8, b as u8]);
                    assert!(
                        contrast_ratio(got, bg.rgb) >= INFO_TARGET,
                        "swatch {:?} realized {got:?} only {:.2}:1",
                        [r, g, b],
                        contrast_ratio(got, bg.rgb)
                    );
                }
            }
        }
    }

    /// The rgb an xterm-256 index realizes (test helper mirroring quantize_256's cells).
    fn idx_to_rgb(i: u8) -> Rgb {
        if i >= 232 {
            let v = 8 + (i - 232) * 10;
            [v, v, v]
        } else {
            let i = i - 16;
            [CUBE[(i / 36) as usize], CUBE[((i / 6) % 6) as usize], CUBE[(i % 6) as usize]]
        }
    }

    #[test]
    fn fit_decoration_reduces_chroma_and_contrast() {
        let swatch = [220, 40, 40];
        let bg = TermBg::dark_default();
        let deco = fit_decoration(swatch, bg);
        assert!(
            srgb_to_oklch(deco).c < srgb_to_oklch(swatch).c,
            "chroma pulled down"
        );
        assert!(
            contrast_ratio(deco, bg.rgb) <= contrast_ratio(swatch, bg.rgb),
            "sits below the swatch's own contrast (never a grabber)"
        );
    }

    #[test]
    fn fit_decoration_caps_light_cover_below_ceiling() {
        // A near-white cover on a dark terminal would be a 3:1+ grabber if left alone;
        // the decoration policy must cap it under the distraction ceiling. Checked on
        // dark AND light backgrounds and for a vivid light swatch.
        for bg in [
            TermBg::dark_default(),
            TermBg::from_rgb([0xf4, 0xf4, 0xf4], BgSource::DarkDefault),
        ] {
            for swatch in [[0xf0, 0xf0, 0xf0], [0xff, 0xd0, 0x40], [0x10, 0x10, 0x10]] {
                let deco = fit_decoration(swatch, bg);
                assert!(
                    contrast_ratio(deco, bg.rgb) <= DECO_CEILING + 0.01,
                    "sigil {swatch:?} on bg {:?} exceeds ceiling: {:.2}",
                    bg.rgb,
                    contrast_ratio(deco, bg.rgb)
                );
            }
        }
    }

    #[test]
    fn extract_palette_from_two_tone() {
        // A two-tone image: half vivid red, half dark grey.
        let mut img = image::RgbImage::new(48, 48);
        for y in 0..48 {
            for x in 0..48 {
                let p = if x < 24 { image::Rgb([220, 20, 20]) } else { image::Rgb([40, 40, 40]) };
                img.put_pixel(x, y, p);
            }
        }
        let pal = extract_palette(&img);
        // Vibrant should be the red-ish cluster (higher chroma than the grey).
        assert!(pal.vibrant[0] > pal.vibrant[1] + 40, "vibrant is red-dominant: {:?}", pal.vibrant);
        // Muted is the low-chroma grey.
        let m = pal.muted;
        assert!((m[0] as i32 - m[1] as i32).abs() < 30, "muted is near-grey: {m:?}");
    }

    #[test]
    fn image_protocol_env_fixtures() {
        let e = env_of(&[("KITTY_WINDOW_ID", "1")]);
        assert_eq!(image_protocol(&mkenv(&e)), ImageProtocol::Kitty);
        let e = env_of(&[("TERM_PROGRAM", "iTerm.app")]);
        assert_eq!(image_protocol(&mkenv(&e)), ImageProtocol::Iterm2);
        let e = env_of(&[("TERM", "foot")]);
        assert_eq!(image_protocol(&mkenv(&e)), ImageProtocol::Sixel);
        let e = env_of(&[("TERM", "xterm-256color")]);
        assert_eq!(image_protocol(&mkenv(&e)), ImageProtocol::None);
    }

    #[test]
    fn truecolor_from_colorterm() {
        assert!(truecolor(&mkenv(&env_of(&[("COLORTERM", "truecolor")]))));
        assert!(truecolor(&mkenv(&env_of(&[("COLORTERM", "24bit")]))));
        assert!(!truecolor(&mkenv(&env_of(&[("COLORTERM", "256")]))));
        assert!(!truecolor(&mkenv(&env_of(&[]))));
    }

    #[test]
    fn quantize_256_and_post_recheck() {
        // Pure black maps to index 16 (cube origin) exactly.
        let (idx, rgb) = quantize_256([0, 0, 0]);
        assert_eq!(idx, 16);
        assert_eq!(rgb, [0, 0, 0]);
        // A near-red quantizes to a red-ish cube cell; the realized rgb lets the
        // caller re-check contrast POST-quantization.
        let (_i, realized) = quantize_256([250, 10, 10]);
        let bg = TermBg::dark_default();
        assert!(realized[0] > realized[1], "stays red-dominant after quantization");
        assert!(contrast_ratio(realized, bg.rgb) > 1.0);
    }

    #[test]
    fn to_color_truecolor_vs_indexed() {
        assert_eq!(to_color([10, 20, 30], true), Color::Rgb(10, 20, 30));
        assert!(matches!(to_color([10, 20, 30], false), Color::Indexed(_)));
    }

    /// A unix pipe pre-loaded with `bytes`, with its WRITE end already closed so reads
    /// see EOF after the fed bytes (nothing can block on a still-open writer). Returns
    /// the read fd; the caller closes it.
    fn eof_pipe(bytes: &[u8]) -> std::os::fd::RawFd {
        let mut fds = [0i32; 2];
        assert_eq!(unsafe { libc::pipe(fds.as_mut_ptr()) }, 0);
        let (rd, wr) = (fds[0], fds[1]);
        if !bytes.is_empty() {
            let n = unsafe { libc::write(wr, bytes.as_ptr() as *const libc::c_void, bytes.len()) };
            assert_eq!(n, bytes.len() as isize);
        }
        unsafe { libc::close(wr) };
        rd
    }

    /// Read all remaining bytes on `fd` until EOF.
    fn read_remaining(fd: std::os::fd::RawFd) -> Vec<u8> {
        let mut out = Vec::new();
        let mut b = [0u8; 64];
        loop {
            let n = unsafe { libc::read(fd, b.as_mut_ptr() as *mut libc::c_void, b.len()) };
            if n <= 0 {
                break;
            }
            out.extend_from_slice(&b[..n as usize]);
        }
        out
    }

    #[test]
    fn drain_pending_osc_swallows_late_reply_leaving_following_bytes() {
        // A slow terminal's late ST-terminated OSC 11 reply, followed by a real
        // keystroke the user typed after it. The drain must eat the WHOLE OSC sequence
        // (up to the ST terminator) and leave the trailing keystroke for the event loop.
        let rd = eof_pipe(b"\x1b]11;rgb:1c1c/1c1c/1c1c\x1b\\q");
        drain_pending_osc_fd(rd, Duration::from_millis(200));
        assert_eq!(read_remaining(rd), b"q", "OSC reply drained up to ST, 'q' preserved");
        unsafe { libc::close(rd) };

        // BEL-terminated reply, nothing trailing.
        let rd = eof_pipe(b"\x1b]11;rgb:ffff/ffff/ffff\x07");
        drain_pending_osc_fd(rd, Duration::from_millis(200));
        assert!(read_remaining(rd).is_empty(), "BEL-terminated reply fully drained");
        unsafe { libc::close(rd) };
    }

    #[test]
    fn drain_pending_osc_leaves_non_osc_typeahead() {
        // Ordinary typed keys arrive as a short burst (a key or two), well under an
        // OSC-reply-sized burst, so the count gate returns WITHOUT consuming a single
        // byte. `q` is the classic startle-quit key - it must reach the event loop intact.
        let rd = eof_pipe(b"quit");
        drain_pending_osc_fd(rd, Duration::from_millis(50));
        assert_eq!(read_remaining(rd), b"quit", "plain typeahead is never consumed");
        unsafe { libc::close(rd) };

        // Even a lone ESC-started key sequence (an arrow key, 3 bytes) is below the burst
        // threshold, so it too is left untouched rather than mistaken for a reply.
        let rd = eof_pipe(b"\x1b[A");
        drain_pending_osc_fd(rd, Duration::from_millis(50));
        assert_eq!(read_remaining(rd), b"\x1b[A", "short escape sequence preserved");
        unsafe { libc::close(rd) };
    }

    #[test]
    fn drain_pending_osc_returns_promptly_when_nothing_pending() {
        // A non-answering terminal never sends bytes: the drain waits out the grace on
        // poll(2) and returns, consuming nothing - it must not hang.
        let rd = eof_pipe(b"");
        let started = std::time::Instant::now();
        drain_pending_osc_fd(rd, Duration::from_millis(30));
        assert!(started.elapsed() < Duration::from_millis(2000), "drain must not hang");
        unsafe { libc::close(rd) };
    }

    #[test]
    fn probe_bg_falls_back_without_osc_answer() {
        // A sink that accepts the query; stdin is not fed (a non-answering terminal /
        // tmux without passthrough), so the read must time out FAST on its own thread
        // and fall back - never block the caller and never leave a reader thread behind
        // to later steal the user's keystrokes. The wall-clock bound (generous vs the
        // 30ms deadline) proves the probe returned promptly rather than hanging on fd0.
        let e = env_of(&[("COLORFGBG", "15;0")]);
        let mut sink: Vec<u8> = Vec::new();
        let started = std::time::Instant::now();
        let bg = probe_bg(&mut sink, &mkenv(&e), Duration::from_millis(30));
        assert!(
            started.elapsed() < Duration::from_millis(2000),
            "probe must not block on a non-answering terminal (took {:?})",
            started.elapsed()
        );
        // The query WAS written (so a real terminal would have answered), and we still
        // consumed no stdin - the fallback owns the result.
        assert_eq!(sink, b"\x1b]11;?\x07");
        assert_eq!(bg.source, BgSource::ColorFgBg);
        assert_eq!(bg.rgb, [0, 0, 0]);
        assert!(bg.is_dark);
        // No env at all -> dark default.
        let mut sink2: Vec<u8> = Vec::new();
        let bg2 = probe_bg(&mut sink2, &mkenv(&env_of(&[])), Duration::from_millis(30));
        assert_eq!(bg2.source, BgSource::DarkDefault);
    }
}
