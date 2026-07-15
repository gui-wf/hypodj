//! Daemon configuration, loaded from TOML.
//!
//! FOUNDATION: real, used by the vertical slice.

use serde::Deserialize;
use std::path::{Path, PathBuf};

#[derive(Debug, Clone, Deserialize)]
pub struct Config {
    pub server: ServerConfig,
    #[serde(default)]
    pub mpd: MpdConfig,
    #[serde(default)]
    pub mpris: MprisConfig,
    /// Per-user fade/envelope tunables. The whole `[fade]` section is optional
    /// and every knob defaults from the evidence-based fade research, so a config
    /// with no `[fade]` block still yields a fully startle-safe primitive.
    #[serde(default)]
    pub fade: FadeConfig,
    /// Smooth-restart (resume) tunables. Optional; when the state dir cannot be
    /// resolved (neither this section's `state_dir` nor `$STATE_DIRECTORY` set),
    /// resume is disabled and the daemon simply cold-starts.
    #[serde(default)]
    pub restart: RestartConfig,
}

/// `[restart]` config for the smooth-restart (sleep-fade-out on SIGTERM + resume
/// state + wake-ramp-in) feature. All fields optional.
#[derive(Debug, Clone, Deserialize)]
pub struct RestartConfig {
    /// Where the resume state file (`resume.toml`) lives. When `None` the daemon
    /// reads `$STATE_DIRECTORY` (set by systemd `StateDirectory=`) at startup;
    /// if neither is present, resume is disabled (safe cold start). This is NEVER
    /// the RuntimeDirectory (/run tmpfs is wiped on stop, defeating SIGKILL
    /// resume) - it must be a persistent location.
    #[serde(default)]
    pub state_dir: Option<PathBuf>,
    /// Coarse periodic checkpoint cadence, seconds (refreshes only elapsed while a
    /// track is live). Edge events (track/state/queue changes) checkpoint
    /// immediately regardless.
    #[serde(default = "d_checkpoint_secs")]
    pub checkpoint_secs: u64,
}

pub const DEFAULT_CHECKPOINT_SECS: u64 = 12;

fn d_checkpoint_secs() -> u64 {
    DEFAULT_CHECKPOINT_SECS
}

impl Default for RestartConfig {
    fn default() -> Self {
        Self {
            state_dir: None,
            checkpoint_secs: DEFAULT_CHECKPOINT_SECS,
        }
    }
}

/// Tunable knobs for the volume-envelope (fade) primitive. Defaults are the
/// research-backed constants below; both [`crate::fade::FadeSpec::new`] (via the
/// handler) and the `fade` DSL parser read from this ONE struct, so wiring a
/// per-user TOML override is a one-line change. See the fade-design-spec /
/// config-knobs memories for the rationale behind each default.
#[derive(Debug, Clone, Deserialize)]
pub struct FadeConfig {
    /// Hard floor on any step interval, ms. Startle re-emerges below ~200 ms; the
    /// design floor is 250 ms and it applies to EVERY transition incl user nudges.
    #[serde(default = "d_min_slew_ms")]
    pub min_slew_ms: u64,
    /// Max per-step dB delta for a sub-JND fade (sleep-safe, imperceptible).
    #[serde(default = "d_step_db")]
    pub step_size_db: f64,
    /// The non-zero low floor for a wind-down fade (does NOT reach silence).
    /// Reachable from the DSL as `fade to floor` (see the handler): a deliberate
    /// wind-down to this level that leaves playback running, distinct from
    /// `fade out` which ramps all the way to silence + stops. Normalized into
    /// `(synth_floor_db, wake_ceiling_db)` by [`FadeConfig::normalize`].
    #[serde(default = "d_floor_db")]
    pub floor_level_db: f64,
    /// The perceptual synth floor: at/below it the signal is treated as silence
    /// (mpv volume 0). Keeps the dB domain finite (never -inf).
    #[serde(default = "d_synth_floor")]
    pub synth_floor_db: f64,
    /// The comfort ceiling a wake ramp-in must never overshoot (0 dB == vol 100).
    #[serde(default = "d_ceiling_db")]
    pub wake_ceiling_db: f64,
    /// Tick == step interval, ms (clamped up to `min_slew_ms`).
    #[serde(default = "d_tick_ms")]
    pub tick_ms: u64,
    /// Default duration of a sleep-stop fade to silence, seconds. RESERVED and
    /// consumed by the P1 sleep-timer executor (the scheduled `fade out` a sleep
    /// timer fires); the immediate DSL `fade out` uses `winddown_fade_secs`. It
    /// is not silently ignored: [`FadeConfig::normalize`] reads and clamps it
    /// into `[min_slew, max_dur]` so a bad value is caught at load, not at P1.
    #[serde(default = "d_sleep_fade_s")]
    pub sleep_fade_secs: u64,
    /// Default duration of a wind-down `fade out` / `fade to`, seconds.
    #[serde(default = "d_winddown_s")]
    pub winddown_fade_secs: u64,
    /// Default duration of a wake `fade in` ramp, seconds.
    #[serde(default = "d_wake_s")]
    pub wake_ramp_secs: u64,
    /// Absolute ceiling on any fade duration, seconds (clamps a runaway request).
    #[serde(default = "d_max_dur_s")]
    pub max_dur_secs: u64,
    /// Duration of the DELIBERATE sleep-fade-out run on SIGTERM/SIGINT before the
    /// daemon exits, seconds. This is a deliberate (not sub-JND) fade at the 3
    /// dB/step cap, kept SHORT so it never slows a nixos-rebuild / service
    /// restart; the wake-ramp on the next start uses `wake_ramp_secs`. Normalized
    /// into `[min_slew_s, max_dur_secs]`.
    #[serde(default = "d_shutdown_fade_s")]
    pub shutdown_fade_secs: u64,
    /// Duration of the startle-safe transport pause/resume fade, SECONDS (a float,
    /// unlike the coarse `*_secs` knobs, so a sub-second nominal is expressible).
    /// On PAUSE the transport runs a short sub-JND fade to silence THEN pauses mpv
    /// (silent at the freeze, no click); on RESUME it unpauses from silence THEN
    /// ramps back to the prior level. Kept SHORT so pause feels responsive; the
    /// fade primitive still extends it as far as sub-JND startle safety requires.
    /// Normalized into `[min_slew_s, max_dur_secs]`.
    #[serde(default = "d_pause_fade_s")]
    pub pause_fade_secs: f64,
}

// Research-backed defaults (memory 01kxhjqr). Exposed as `pub const` so the fade
// DSL parser can reference the SAME source of truth as the serde defaults.
pub const DEFAULT_MIN_SLEW_MS: u64 = 250;
pub const DEFAULT_STEP_SIZE_DB: f64 = 0.75;
pub const DEFAULT_FLOOR_LEVEL_DB: f64 = -45.0;
pub const DEFAULT_SYNTH_FLOOR_DB: f64 = -60.0;
pub const DEFAULT_WAKE_CEILING_DB: f64 = 0.0;
pub const DEFAULT_TICK_MS: u64 = 250;
pub const DEFAULT_SLEEP_FADE_SECS: u64 = 480;
pub const DEFAULT_WINDDOWN_FADE_SECS: u64 = 300;
pub const DEFAULT_WAKE_RAMP_SECS: u64 = 480;
pub const DEFAULT_MAX_DUR_SECS: u64 = 1800;
pub const DEFAULT_SHUTDOWN_FADE_SECS: u64 = 6;
pub const DEFAULT_PAUSE_FADE_SECS: f64 = 0.5;

/// Positive minimum for `step_size_db`. A `0` (or negative) step would divide by
/// zero in [`crate::fade::FadeSpec::new`]'s sub-JND path (`range / step_size` ->
/// +inf -> `u64::MAX` steps -> `Vec::with_capacity` / `Duration` overflow panic).
/// Floored to this so the divide is always well defined. Small enough to never
/// coarsen a real (>= default) configured step.
pub const MIN_STEP_SIZE_DB: f64 = 0.05;

/// Minimum headroom, in dB, the wake ceiling must sit above the synth floor. The
/// wind-down floor clamp uses `lo = synth_floor + 1` and `hi = ceiling - 1`; with
/// this margin (>= 2) `hi >= lo` always holds, so no `clamp(lo, hi)` can ever be
/// called with `lo > hi` (which `f64::clamp` panics on).
const CEILING_MIN_MARGIN_DB: f64 = 2.0;

fn d_min_slew_ms() -> u64 { DEFAULT_MIN_SLEW_MS }
fn d_step_db() -> f64 { DEFAULT_STEP_SIZE_DB }
fn d_floor_db() -> f64 { DEFAULT_FLOOR_LEVEL_DB }
fn d_synth_floor() -> f64 { DEFAULT_SYNTH_FLOOR_DB }
fn d_ceiling_db() -> f64 { DEFAULT_WAKE_CEILING_DB }
fn d_tick_ms() -> u64 { DEFAULT_TICK_MS }
fn d_sleep_fade_s() -> u64 { DEFAULT_SLEEP_FADE_SECS }
fn d_winddown_s() -> u64 { DEFAULT_WINDDOWN_FADE_SECS }
fn d_wake_s() -> u64 { DEFAULT_WAKE_RAMP_SECS }
fn d_max_dur_s() -> u64 { DEFAULT_MAX_DUR_SECS }
fn d_shutdown_fade_s() -> u64 { DEFAULT_SHUTDOWN_FADE_SECS }
fn d_pause_fade_s() -> f64 { DEFAULT_PAUSE_FADE_SECS }

impl FadeConfig {
    /// Clamp every knob into its safe range at LOAD time, logging any correction,
    /// so an out-of-range TOML value can never silently produce a startle-unsafe
    /// or degenerate fade downstream. This is the ONE place the invariants are
    /// enforced across the config surface; the handler and [`crate::fade`] then
    /// trust the normalized values.
    ///
    /// Enforced here:
    ///   - `min_slew_ms >= STARTLE_HARD_MIN_SLEW_MS` (200 ms): below it startle
    ///     re-emerges and, historically, `FadeSpec::new` rejected EVERY fade
    ///     (silent no-op). Clamp up, don't reject.
    ///   - `tick_ms >= min_slew_ms` (the tick is the step interval).
    ///   - `synth_floor_db` is pinned to the player's cubic-softvol seam value
    ///     ([`crate::player::SYNTH_FLOOR_DB`]) - it is the SINGLE source of truth
    ///     shared by `db_to_mpv_volume`'s mute threshold and the `FadeSpec`
    ///     Silence ramp, so the final step into silence is always reached by the
    ///     slewed ramp, never a jump. Not independently tunable; the field exists
    ///     for visibility and is normalized to the seam.
    ///   - `floor_level_db` kept strictly inside `(synth_floor_db, wake_ceiling_db)`
    ///     so `fade to floor` is a real, non-degenerate wind-down.
    ///   - the default durations (`sleep_fade_secs`, `winddown_fade_secs`,
    ///     `wake_ramp_secs`) clamped into `[min_slew, max_dur]`.
    pub fn normalize(&mut self) {
        use crate::fade::STARTLE_HARD_MIN_SLEW_MS;
        use crate::player::SYNTH_FLOOR_DB;

        // TOTAL and provably panic-free: every knob is coerced into a range such
        // that NO `f64::clamp` below can ever be called with `min > max` or a NaN
        // bound (either of which panics), and no downstream divide-by-zero /
        // overflow can arise. Sanitize non-finite floats FIRST (TOML permits
        // `nan` / `inf`), so a poisoned value can never reach a comparison.
        if !self.step_size_db.is_finite() {
            self.step_size_db = DEFAULT_STEP_SIZE_DB;
        }
        if !self.floor_level_db.is_finite() {
            self.floor_level_db = DEFAULT_FLOOR_LEVEL_DB;
        }
        if !self.synth_floor_db.is_finite() {
            self.synth_floor_db = DEFAULT_SYNTH_FLOOR_DB;
        }
        if !self.wake_ceiling_db.is_finite() {
            self.wake_ceiling_db = DEFAULT_WAKE_CEILING_DB;
        }

        if self.min_slew_ms < STARTLE_HARD_MIN_SLEW_MS {
            tracing::warn!(
                configured = self.min_slew_ms,
                floor = STARTLE_HARD_MIN_SLEW_MS,
                "min_slew_ms below the 200ms startle hard floor; clamping up"
            );
            self.min_slew_ms = STARTLE_HARD_MIN_SLEW_MS;
        }
        // A tick is at least one startle-slew long. It is NOT upper-bounded here
        // (a large min_slew implies a large tick); FadeSpec::new uses saturating
        // Duration arithmetic so even a degenerate huge tick cannot overflow/panic.
        if self.tick_ms < self.min_slew_ms {
            self.tick_ms = self.min_slew_ms;
        }
        // Floor step_size_db to a positive minimum: a 0 (or negative) step would
        // divide by zero in FadeSpec::new's sub-JND path -> u64::MAX steps ->
        // capacity/Duration overflow panic. This is the config-side guarantee;
        // FadeSpec::new guards the divide too (belt and suspenders).
        if self.step_size_db < MIN_STEP_SIZE_DB {
            tracing::warn!(
                configured = self.step_size_db,
                floor = MIN_STEP_SIZE_DB,
                "step_size_db not positive enough; flooring to the minimum"
            );
            self.step_size_db = MIN_STEP_SIZE_DB;
        }
        // The synth floor is defined by the player's cubic softvol seam. Pin it
        // so the mute threshold and the Silence ramp agree exactly.
        if (self.synth_floor_db - SYNTH_FLOOR_DB).abs() > f64::EPSILON {
            tracing::warn!(
                configured = self.synth_floor_db,
                seam = SYNTH_FLOOR_DB,
                "synth_floor_db is fixed by the cubic softvol seam; pinning to it"
            );
            self.synth_floor_db = SYNTH_FLOOR_DB;
        }
        // Keep the wake ceiling a sane margin above the synth floor so the
        // wind-down-floor clamp bounds (lo = synth_floor + 1, hi = ceiling - 1)
        // always satisfy lo <= hi - this is what makes the clamp below panic-free.
        let min_ceiling = self.synth_floor_db + CEILING_MIN_MARGIN_DB;
        if self.wake_ceiling_db < min_ceiling {
            tracing::warn!(
                configured = self.wake_ceiling_db,
                floor = min_ceiling,
                "wake_ceiling_db too close to (or below) the synth floor; raising"
            );
            self.wake_ceiling_db = min_ceiling;
        }
        // Keep the wind-down floor a real level strictly between silence and the
        // ceiling (read floor_level_db so it is never a dead knob). With the
        // ceiling margin enforced above, lo <= hi always; the max() is a
        // belt-and-suspenders so no clamp can ever see min > max.
        let lo = self.synth_floor_db + 1.0;
        let hi = (self.wake_ceiling_db - 1.0).max(lo);
        if self.floor_level_db <= lo || self.floor_level_db >= hi {
            let clamped = self.floor_level_db.clamp(lo, hi);
            tracing::warn!(
                configured = self.floor_level_db,
                clamped,
                "floor_level_db out of (synth_floor, ceiling); clamping"
            );
            self.floor_level_db = clamped;
        }
        // Clamp every default duration into [min_slew, max_dur]. max_dur must
        // itself be >= the per-second min derived from min_slew, otherwise the
        // clamp(min_s, max_dur) would have min > max and panic.
        let min_s = ((self.min_slew_ms as f64 / 1000.0).ceil() as u64).max(1);
        if self.max_dur_secs < min_s {
            tracing::warn!(
                configured = self.max_dur_secs,
                floor = min_s,
                "max_dur_secs below the per-second min_slew floor; raising"
            );
            self.max_dur_secs = min_s;
        }
        for d in [
            &mut self.sleep_fade_secs,
            &mut self.winddown_fade_secs,
            &mut self.wake_ramp_secs,
            &mut self.shutdown_fade_secs,
        ] {
            *d = (*d).clamp(min_s, self.max_dur_secs);
        }
        // The pause fade is a FLOAT-second knob; a sub-second nominal is allowed
        // down to the per-millisecond min_slew. Sanitize non-finite first (TOML
        // permits nan/inf), then clamp into [min_slew_s, max_dur_secs]. The lower
        // bound uses the exact min_slew in seconds (not the ceil'd `min_s`) so a
        // 0.25s min_slew stays a 0.25s floor, keeping the pause genuinely short.
        if !self.pause_fade_secs.is_finite() {
            self.pause_fade_secs = DEFAULT_PAUSE_FADE_SECS;
        }
        let pause_lo = self.min_slew_ms as f64 / 1000.0;
        self.pause_fade_secs = self.pause_fade_secs.clamp(pause_lo, self.max_dur_secs as f64);
    }
}

impl Default for FadeConfig {
    fn default() -> Self {
        Self {
            min_slew_ms: DEFAULT_MIN_SLEW_MS,
            step_size_db: DEFAULT_STEP_SIZE_DB,
            floor_level_db: DEFAULT_FLOOR_LEVEL_DB,
            synth_floor_db: DEFAULT_SYNTH_FLOOR_DB,
            wake_ceiling_db: DEFAULT_WAKE_CEILING_DB,
            tick_ms: DEFAULT_TICK_MS,
            sleep_fade_secs: DEFAULT_SLEEP_FADE_SECS,
            winddown_fade_secs: DEFAULT_WINDDOWN_FADE_SECS,
            wake_ramp_secs: DEFAULT_WAKE_RAMP_SECS,
            max_dur_secs: DEFAULT_MAX_DUR_SECS,
            shutdown_fade_secs: DEFAULT_SHUTDOWN_FADE_SECS,
            pause_fade_secs: DEFAULT_PAUSE_FADE_SECS,
        }
    }
}

#[derive(Debug, Clone, Deserialize)]
pub struct ServerConfig {
    /// Base URL of the OpenSubsonic server, e.g. https://music.example.com
    pub url: String,
    pub username: String,
    pub password: String,
    /// Client name reported to the server (OpenSubsonic `c` param).
    #[serde(default = "default_client_name")]
    pub client_name: String,
}

#[derive(Debug, Clone, Deserialize)]
pub struct MpdConfig {
    /// Address the MPD-protocol listener binds to.
    ///
    /// Default is 6601 ON PURPOSE: the real mopidy daemon owns 6600 and must
    /// not be disturbed. Production parity flips this to 6600 once mopidy is
    /// retired.
    #[serde(default = "default_mpd_bind")]
    pub bind: String,
}

impl Default for MpdConfig {
    fn default() -> Self {
        Self { bind: default_mpd_bind() }
    }
}

#[derive(Debug, Clone, Deserialize)]
pub struct MprisConfig {
    /// Expose the MPRIS (org.mpris.MediaPlayer2.hypodj) D-Bus server on the
    /// session bus so desktops show now-playing + controls. Default true; set
    /// false to disable. Registered under the `.hypodj` bus name (NOT `.mopidy`),
    /// so it never conflicts with a running mopidy MPRIS server.
    #[serde(default = "default_mpris_enable")]
    pub enable: bool,
    /// Command run by the MPRIS root `Raise()` method when a desktop media widget
    /// is clicked - typically a terminal running the user's music client
    /// (e.g. `["kitty", "ncmpcpp"]`). The first element is the program, the rest
    /// are args. Absent = None = `CanRaise` reports false and `Raise()` is a no-op.
    #[serde(default)]
    pub raise_command: Option<Vec<String>>,
}

impl Default for MprisConfig {
    fn default() -> Self {
        Self { enable: default_mpris_enable(), raise_command: None }
    }
}

fn default_mpris_enable() -> bool {
    true
}

fn default_client_name() -> String {
    "hypodj".to_string()
}

fn default_mpd_bind() -> String {
    "127.0.0.1:6601".to_string()
}

#[derive(Debug, thiserror::Error)]
pub enum ConfigError {
    #[error("reading config {0}: {1}")]
    Io(String, #[source] std::io::Error),
    #[error("parsing config: {0}")]
    Parse(#[from] toml::de::Error),
}

impl Config {
    pub fn load(path: &Path) -> Result<Self, ConfigError> {
        let raw = std::fs::read_to_string(path)
            .map_err(|e| ConfigError::Io(path.display().to_string(), e))?;
        let mut cfg: Config = toml::from_str(&raw)?;
        cfg.fade.normalize();
        Ok(cfg)
    }

    /// Parse from a TOML string (test/embedded use). Kept as an inherent method
    /// with this name for ergonomics; it is not the `FromStr` trait.
    #[allow(clippy::should_implement_trait)]
    pub fn from_str(raw: &str) -> Result<Self, ConfigError> {
        let mut cfg: Config = toml::from_str(raw)?;
        cfg.fade.normalize();
        Ok(cfg)
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn parses_and_defaults_bind_to_6601_not_6600() {
        // No [mpd] section -> the default must be 6601, honoring the hard
        // constraint that mopidy owns 6600.
        let cfg = Config::from_str(
            r#"
            [server]
            url = "https://music.example.com"
            username = "alice"
            password = "s3cr3t"
        "#,
        )
        .expect("valid config");
        assert_eq!(cfg.server.url, "https://music.example.com");
        assert_eq!(cfg.server.username, "alice");
        assert_eq!(cfg.server.client_name, "hypodj");
        assert_eq!(cfg.mpd.bind, "127.0.0.1:6601");
    }

    #[test]
    fn fade_section_defaults_and_overrides() {
        // No [fade] section -> every knob defaults from the research constants.
        let cfg = Config::from_str(
            r#"
            [server]
            url = "https://m"
            username = "a"
            password = "b"
        "#,
        )
        .unwrap();
        assert_eq!(cfg.fade.min_slew_ms, 250);
        assert_eq!(cfg.fade.step_size_db, 0.75);
        assert_eq!(cfg.fade.synth_floor_db, -60.0);
        assert_eq!(cfg.fade.max_dur_secs, 1800);
        assert_eq!(cfg.fade.pause_fade_secs, 0.5, "pause fade defaults to a short 0.5s");

        // A partial [fade] section overrides only the named knobs.
        let cfg = Config::from_str(
            r#"
            [server]
            url = "https://m"
            username = "a"
            password = "b"
            [fade]
            winddown_fade_secs = 120
            step_size_db = 0.5
        "#,
        )
        .unwrap();
        assert_eq!(cfg.fade.winddown_fade_secs, 120);
        assert_eq!(cfg.fade.step_size_db, 0.5);
        // Untouched knobs still default.
        assert_eq!(cfg.fade.min_slew_ms, 250);
    }

    #[test]
    fn normalize_clamps_bad_min_slew_and_pins_synth_floor() {
        // A sub-200ms min_slew (which historically no-op'd every fade) is clamped
        // UP to the startle hard floor, and an off-seam synth_floor is pinned.
        let cfg = Config::from_str(
            r#"
            [server]
            url = "https://m"
            username = "a"
            password = "b"
            [fade]
            min_slew_ms = 50
            synth_floor_db = -45.0
            tick_ms = 60
        "#,
        )
        .unwrap();
        assert_eq!(cfg.fade.min_slew_ms, 200, "min_slew clamped to startle floor");
        assert!(cfg.fade.tick_ms >= cfg.fade.min_slew_ms, "tick >= min_slew");
        assert_eq!(cfg.fade.synth_floor_db, -60.0, "synth floor pinned to the seam");
    }

    #[test]
    fn normalize_clamps_durations_and_floor() {
        let cfg = Config::from_str(
            r#"
            [server]
            url = "https://m"
            username = "a"
            password = "b"
            [fade]
            max_dur_secs = 100
            winddown_fade_secs = 999999
            sleep_fade_secs = 999999
            floor_level_db = 5.0
        "#,
        )
        .unwrap();
        assert_eq!(cfg.fade.winddown_fade_secs, 100, "clamped to max_dur");
        assert_eq!(cfg.fade.sleep_fade_secs, 100, "sleep_fade read + clamped to max_dur");
        // floor pushed back inside (synth_floor, ceiling) = (-60, 0).
        assert!(cfg.fade.floor_level_db > -60.0 && cfg.fade.floor_level_db < 0.0);
    }

    // C1: normalize is TOTAL - degenerate/extreme values that would make an
    // internal clamp have min > max (or divide by zero downstream) must be
    // coerced to safe bounds, NEVER panic.
    #[test]
    fn normalize_never_panics_on_degenerate_values() {
        // max_dur_secs = 0: the duration clamp(min_s, max_dur) would have
        // min_s (>= 1) > 0 and panic without the guard.
        let cfg = Config::from_str(
            r#"
            [server]
            url = "https://m"
            username = "a"
            password = "b"
            [fade]
            max_dur_secs = 0
        "#,
        )
        .unwrap();
        assert!(cfg.fade.max_dur_secs >= 1, "max_dur raised to per-second min");
        assert!(cfg.fade.winddown_fade_secs <= cfg.fade.max_dur_secs);

        // wake_ceiling_db = -60: hi = ceiling - 1 = -61 < lo = synth_floor + 1 =
        // -59, so the floor clamp would be clamp(-59, -61) and panic.
        let cfg = Config::from_str(
            r#"
            [server]
            url = "https://m"
            username = "a"
            password = "b"
            [fade]
            wake_ceiling_db = -60.0
        "#,
        )
        .unwrap();
        assert!(
            cfg.fade.wake_ceiling_db >= cfg.fade.synth_floor_db + CEILING_MIN_MARGIN_DB,
            "ceiling raised to a sane margin above the synth floor"
        );
        assert!(cfg.fade.floor_level_db > cfg.fade.synth_floor_db);
        assert!(cfg.fade.floor_level_db < cfg.fade.wake_ceiling_db);

        // step_size_db = 0: a 0 step divides by zero downstream. Floored positive.
        let cfg = Config::from_str(
            r#"
            [server]
            url = "https://m"
            username = "a"
            password = "b"
            [fade]
            step_size_db = 0.0
        "#,
        )
        .unwrap();
        assert!(cfg.fade.step_size_db >= MIN_STEP_SIZE_DB);
        // And FadeSpec::new over this normalized config must not panic (belt).
        use crate::fade::{Curve, FadeSpec, FadeTarget, StartleBounds};
        let bounds = StartleBounds {
            min_slew: std::time::Duration::from_millis(cfg.fade.min_slew_ms),
            step_size_db: cfg.fade.step_size_db,
            synth_floor_db: cfg.fade.synth_floor_db,
            sub_jnd: true,
        };
        let _ = FadeSpec::new(
            0.0,
            FadeTarget::Db(-45.0),
            std::time::Duration::from_secs(60),
            std::time::Duration::from_millis(cfg.fade.tick_ms),
            Curve::DbLinear,
            bounds,
        );
    }

    // C1: a battery of extreme / non-finite values must all normalize to sane,
    // ordered bounds without panicking.
    #[test]
    fn normalize_extreme_battery_stays_sane() {
        let cases = [
            "min_slew_ms = 0\ntick_ms = 0\nmax_dur_secs = 0\nstep_size_db = 0.0",
            "step_size_db = -5.0\nwake_ceiling_db = -100.0\nfloor_level_db = 999.0",
            "wake_ceiling_db = nan\nstep_size_db = inf\nfloor_level_db = -inf",
            "max_dur_secs = 1\nmin_slew_ms = 999999\ntick_ms = 1",
            "synth_floor_db = 40.0\nwake_ceiling_db = 41.0\nfloor_level_db = 0.0",
        ];
        for extra in cases {
            let raw = format!(
                "[server]\nurl = \"https://m\"\nusername = \"a\"\npassword = \"b\"\n[fade]\n{extra}\n"
            );
            let cfg = Config::from_str(&raw).unwrap_or_else(|e| panic!("parse {extra:?}: {e}"));
            let f = &cfg.fade;
            // All invariants hold: no clamp could have had min > max.
            assert!(f.min_slew_ms >= 200);
            assert!(f.tick_ms >= f.min_slew_ms);
            assert!(f.step_size_db >= MIN_STEP_SIZE_DB);
            assert!(f.step_size_db.is_finite());
            assert_eq!(f.synth_floor_db, -60.0);
            assert!(f.wake_ceiling_db >= f.synth_floor_db + CEILING_MIN_MARGIN_DB);
            assert!(f.wake_ceiling_db.is_finite());
            assert!(f.floor_level_db > f.synth_floor_db && f.floor_level_db < f.wake_ceiling_db);
            let min_s = ((f.min_slew_ms as f64 / 1000.0).ceil() as u64).max(1);
            assert!(f.max_dur_secs >= min_s);
            for d in [f.sleep_fade_secs, f.winddown_fade_secs, f.wake_ramp_secs] {
                assert!(d >= min_s && d <= f.max_dur_secs, "duration {d} out of range in {extra:?}");
            }
        }
    }

    #[test]
    fn explicit_bind_overrides_default() {
        let cfg = Config::from_str(
            r#"
            [server]
            url = "https://m.example.com"
            username = "a"
            password = "b"
            [mpd]
            bind = "127.0.0.1:7000"
        "#,
        )
        .unwrap();
        assert_eq!(cfg.mpd.bind, "127.0.0.1:7000");
    }
}
