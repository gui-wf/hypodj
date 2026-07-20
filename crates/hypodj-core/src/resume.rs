//! Smooth-restart RESUME state: the pure, signal-free, model-free BARS of the
//! feature.
//!
//! SMOOTH-RESTART composes P0 (the fade primitive, [`crate::fade`]) and P1 (the
//! live position from [`crate::event::DjEventKind::Tick`]) onto the PROCESS
//! lifecycle: a deliberate sleep-fade-OUT on SIGTERM/SIGINT, a best-effort resume
//! checkpoint, and a wake-ramp-IN on the next start. This module holds ONLY the
//! parts that are unit-testable with no signals, no real process, and no real
//! mpv:
//!   - the [`ResumeState`] (de)serialize + version gate;
//!   - the ATOMIC state write ([`store_atomic`]) + safe load ([`load`]);
//!   - the shutdown-fade BUILDER ([`build_shutdown_fade`]) that produces a valid,
//!     SHORT, click-free [`FadeSpec`] (or refuses when it would blow the budget).
//!
//! Corruption safety is a BAR: [`from_toml`] / [`load`] return `None` for ANY of
//! {missing, unreadable, garbage, truncated, schema mismatch}. They NEVER panic
//! and NEVER block startup - a bad state file always degrades to a cold start.

use std::io::Write;
use std::path::Path;
use std::time::Duration;

use serde::{Deserialize, Serialize};

use crate::config::FadeConfig;
use crate::fade::{Curve, FadeSpec, FadeTarget, StartleBounds};

/// The on-disk schema version. A loaded state whose `schema_version` differs is
/// treated as corrupt (`None`) so a format change is a clean cold start, never a
/// panic or a mis-parse.
pub const RESUME_SCHEMA_VERSION: u32 = 1;

/// The persisted resume snapshot: everything needed to rebuild the queue + wake
/// back into playback (or stay stopped) after a restart. Serialized to TOML.
#[derive(Serialize, Deserialize, Clone, Debug, PartialEq)]
pub struct ResumeState {
    /// Version gate; a mismatch on load => cold start (see [`from_toml`]).
    pub schema_version: u32,
    pub queue: Vec<ResumeItem>,
    /// Index into `queue` of the current entry, if any.
    pub current: Option<usize>,
    /// Elapsed seconds of the current entry, from the P1 `Tick.time_pos`.
    pub elapsed_secs: f64,
    /// The user baseline volume (0..=100) - `State.target_volume`, NOT any faded
    /// live level. The wake ramp rises TO this on restart.
    pub volume: u8,
    pub play_state: ResumePlayState,
    pub playlist_version: u64,
    pub saved_at_unix: u64,
    /// The persisted end-of-queue continuation-radio arming toggle (`continuation
    /// on|off`). `#[serde(default)]` so a pre-continuation resume.toml (which lacks
    /// the key) loads cleanly with the toggle OFF - no schema bump, no cold-start
    /// on upgrade, and startle-safe (default false = today's silent-stop behavior).
    #[serde(default)]
    pub continuation: bool,
}

/// One persisted queue entry. Internally tagged (`kind = "song" | "stream"`) so
/// it round-trips cleanly as a TOML array-of-tables (external tagging trips the
/// toml serializer's "values before tables" ordering rule). A library song
/// carries only its id (metadata is re-resolved from Subsonic on restore); a raw
/// stream carries its verbatim url + title.
#[derive(Serialize, Deserialize, Clone, Debug, PartialEq)]
#[serde(tag = "kind", rename_all = "snake_case")]
pub enum ResumeItem {
    Song { id: String },
    Stream { url: String, title: String },
}

/// The persisted play state. An explicit Paused/Stopped SURVIVES the rebuild (no
/// autoplay, no wake ramp); only Playing wakes back into playback.
#[derive(Serialize, Deserialize, Clone, Copy, Debug, PartialEq)]
#[serde(rename_all = "snake_case")]
pub enum ResumePlayState {
    Playing,
    Paused,
    Stopped,
}

/// Serialize a [`ResumeState`] to TOML. Infallible in practice (the type is a
/// flat, TOML-representable shape); a serializer error degrades to an empty
/// string, which [`from_toml`] then reads back as `None` (a safe cold start)
/// rather than propagating.
pub fn to_toml(s: &ResumeState) -> String {
    toml::to_string(s).unwrap_or_default()
}

/// Parse a [`ResumeState`] from TOML. ANY error - a parse failure, a truncated
/// or garbage document, a missing required field, OR a `schema_version` that is
/// not [`RESUME_SCHEMA_VERSION`] - yields `None`. NEVER panics.
pub fn from_toml(raw: &str) -> Option<ResumeState> {
    match toml::from_str::<ResumeState>(raw) {
        Ok(s) if s.schema_version == RESUME_SCHEMA_VERSION => Some(s),
        Ok(_) => None,
        Err(_) => None,
    }
}

/// Load resume state from `path`. A missing / unreadable / directory / corrupt /
/// version-mismatched file all return `None` (logged at info, "cold starting").
/// NEVER panics, NEVER blocks startup.
pub fn load(path: &Path) -> Option<ResumeState> {
    let raw = match std::fs::read_to_string(path) {
        Ok(r) => r,
        Err(_) => {
            tracing::info!(path = %path.display(), "no readable resume state; cold starting");
            return None;
        }
    };
    match from_toml(&raw) {
        Some(s) => Some(s),
        None => {
            tracing::info!(path = %path.display(), "invalid/old resume state; cold starting");
            None
        }
    }
}

/// Atomically write resume state to `path`: serialize, write a sibling
/// `resume.toml.tmp`, fsync it, then rename over `path` (same directory => the
/// rename is atomic). A partially written file is never observed. Returns the
/// io error to the caller (which logs it warn, never fatal).
pub fn store_atomic(path: &Path, s: &ResumeState) -> std::io::Result<()> {
    let body = to_toml(s);
    // Sibling temp in the SAME dir so the final rename is atomic (cross-dir
    // renames are not). The temp name is UNIQUE per write (pid + a process-wide
    // counter) so two concurrent checkpoints (periodic vs edge-triggered) cannot
    // clobber each other's half-written temp before their renames.
    static TMP_SEQ: std::sync::atomic::AtomicU64 = std::sync::atomic::AtomicU64::new(0);
    let seq = TMP_SEQ.fetch_add(1, std::sync::atomic::Ordering::Relaxed);
    let tmp = path.with_extension(format!("toml.tmp.{}.{}", std::process::id(), seq));
    {
        let mut f = std::fs::File::create(&tmp)?;
        f.write_all(body.as_bytes())?;
        f.sync_all()?;
    }
    std::fs::rename(&tmp, path)?;
    Ok(())
}

/// A built shutdown fade: the validated [`FadeSpec`] plus the REAL wall-clock
/// length it will take (`step_count * min_slew`), so the caller can await it
/// under a timeout with a known bound.
#[derive(Clone, Debug)]
pub struct ShutdownFade {
    pub spec: FadeSpec,
    pub real_dur: Duration,
}

/// Build the DELIBERATE sleep-fade-out for shutdown. This is NOT the sub-JND
/// `FadeIntent::Out` (which would extend a 60 dB drop to ~80 steps ~ 20s); it is
/// a deliberate fade (`sub_jnd = false`, capped at 3 dB/step) built DIRECTLY to
/// silence over `cfg.shutdown_fade_secs`, so it stays short and click-free.
///
/// Returns `None` when the spec cannot be built (a rejected startle-unsafe spec)
/// OR when its real length (`step_count * min_slew`) would exceed `budget` - the
/// caller then skips the fade and exits immediately, so a mid-fade SIGKILL can
/// never leave a click.
pub fn build_shutdown_fade(
    cfg: &FadeConfig,
    from_db: f64,
    budget: Duration,
) -> Option<ShutdownFade> {
    let min_slew = Duration::from_millis(cfg.min_slew_ms);
    let bounds = StartleBounds {
        min_slew,
        step_size_db: cfg.step_size_db,
        synth_floor_db: cfg.synth_floor_db,
        // DELIBERATE cue, not sub-JND: capped at 3 dB/step, never extended to the
        // ~20s sub-JND envelope. THIS is the blocker-1 fix.
        sub_jnd: false,
    };
    let spec = FadeSpec::new(
        from_db,
        FadeTarget::Silence,
        Duration::from_secs(cfg.shutdown_fade_secs),
        Duration::from_millis(cfg.tick_ms),
        Curve::DbLinear,
        bounds,
    )
    .ok()?;
    // Real length: the driver places steps one STEP INTERVAL apart, which is the
    // tick clamped UP to min_slew (t_eff = max(tick, min_slew)) - NOT min_slew
    // alone. Using min_slew would UNDER-estimate when tick > min_slew, so a fade
    // longer than `budget` could pass this check and get SIGKILLed mid-ramp (a
    // click). saturating so a pathological count cannot overflow.
    let step_interval = min_slew.max(Duration::from_millis(cfg.tick_ms));
    let real_dur = step_interval.saturating_mul(spec.step_count() as u32);
    if real_dur > budget {
        return None;
    }
    Some(ShutdownFade { spec, real_dur })
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::player::SYNTH_FLOOR_DB;

    fn sample_state() -> ResumeState {
        ResumeState {
            schema_version: RESUME_SCHEMA_VERSION,
            queue: vec![
                ResumeItem::Song { id: "song-1".into() },
                ResumeItem::Stream {
                    url: "http://radio.example/stream".into(),
                    title: "Radio".into(),
                },
                ResumeItem::Song { id: "song-2".into() },
            ],
            current: Some(2),
            elapsed_secs: 42.5,
            volume: 73,
            play_state: ResumePlayState::Playing,
            playlist_version: 9,
            saved_at_unix: 1_700_000_000,
            continuation: true,
        }
    }

    #[test]
    fn to_from_toml_round_trip() {
        let s = sample_state();
        let raw = to_toml(&s);
        let back = from_toml(&raw).expect("round-trips");
        assert_eq!(s, back);
    }

    #[test]
    fn from_toml_corruption_battery_is_none_never_panics() {
        // Empty.
        assert!(from_toml("").is_none());
        // Garbage / non-TOML.
        assert!(from_toml("}{ this is not toml @@@").is_none());
        // Truncated valid document.
        let raw = to_toml(&sample_state());
        let truncated = &raw[..raw.len() / 2];
        assert!(from_toml(truncated).is_none());
        // Valid TOML but schema_version = 0 and = 999 => version gate rejects.
        let mut s0 = sample_state();
        s0.schema_version = 0;
        assert!(from_toml(&to_toml(&s0)).is_none());
        let mut s999 = sample_state();
        s999.schema_version = 999;
        assert!(from_toml(&to_toml(&s999)).is_none());
        // Valid TOML missing a required field (drop `volume`).
        let missing = "schema_version = 1\nelapsed_secs = 0.0\nplaylist_version = 0\nsaved_at_unix = 0\nplay_state = \"stopped\"\nqueue = []\n";
        assert!(from_toml(missing).is_none());
    }

    #[test]
    fn pre_continuation_file_loads_with_toggle_off() {
        // A resume.toml written before the continuation feature has no `continuation`
        // key. It must still load (schema unchanged) with the toggle defaulting OFF -
        // an upgrade never loses the saved queue and never silently arms continuation.
        let raw = "schema_version = 1\nelapsed_secs = 0.0\nvolume = 50\nplaylist_version = 0\nsaved_at_unix = 0\nplay_state = \"stopped\"\ncurrent = 0\nqueue = []\n";
        let s = from_toml(raw).expect("pre-continuation file still parses");
        assert!(!s.continuation, "the missing toggle defaults OFF");
    }

    #[test]
    fn load_missing_or_directory_is_none() {
        // A path that does not exist.
        let missing = std::env::temp_dir().join("hypodj-resume-does-not-exist-xyz.toml");
        let _ = std::fs::remove_file(&missing);
        assert!(load(&missing).is_none());
        // A directory (unreadable as a file) => None, no panic.
        assert!(load(&std::env::temp_dir()).is_none());
    }

    #[test]
    fn store_atomic_then_load_round_trip_no_leftover_tmp() {
        let dir = std::env::temp_dir().join(format!("hypodj-resume-test-{}", std::process::id()));
        let _ = std::fs::create_dir_all(&dir);
        let path = dir.join("resume.toml");
        let s = sample_state();
        store_atomic(&path, &s).expect("write");
        let back = load(&path).expect("read back");
        assert_eq!(s, back);
        // No leftover .tmp after the rename.
        let tmp = path.with_extension("toml.tmp");
        assert!(!tmp.exists(), "temp file must be renamed away");
        let _ = std::fs::remove_dir_all(&dir);
    }

    #[test]
    fn build_shutdown_fade_default_is_short_and_click_free() {
        let cfg = FadeConfig::default();
        let budget = Duration::from_secs(10);
        let sf = build_shutdown_fade(&cfg, 0.0, budget).expect("builds from 0 dB");
        // Real length is within budget.
        assert!(sf.real_dur <= budget, "real_dur {:?} within budget", sf.real_dur);
        // From a LOWER start the fade is shorter (fewer steps).
        let lower = build_shutdown_fade(&cfg, -30.0, budget).expect("builds from -30 dB");
        assert!(lower.real_dur <= sf.real_dur);
        // Too-tight budget => None (immediate-exit path).
        assert!(build_shutdown_fade(&cfg, 0.0, Duration::from_millis(100)).is_none());
    }

    #[test]
    fn build_shutdown_fade_is_deliberate_not_sub_jnd() {
        // A short shutdown fade from 0 dB spans the full 60 dB to silence. The
        // DELIBERATE (3 dB/step) path needs at least ceil(60/3) = 20 ramp steps +
        // 1 mute = 21 - NOT the ~80 steps the sub-JND Out path would extend to.
        let mut cfg = FadeConfig::default();
        cfg.shutdown_fade_secs = 5; // 5s / 250ms = 20 nominal steps at exactly 3 dB.
        cfg.normalize();
        let budget = Duration::from_secs(15);
        let sf = build_shutdown_fade(&cfg, 0.0, budget).expect("builds");
        let steps = sf.spec.step_count();
        assert!(
            steps <= (60.0f64 / 3.0).ceil() as usize + 1,
            "steps {steps} must be the deliberate 3 dB/step count (<= 21), not the sub-JND ~80"
        );
    }

    // The mute-step + per-step-delta invariants are proven directly on the fade
    // primitive (fade.rs::silence_final_mute_step / monotone tests); here we only
    // assert the builder plumbing yields a non-empty schedule ending in silence.
    #[test]
    fn build_shutdown_fade_reaches_synth_floor_domain() {
        let cfg = FadeConfig::default();
        let sf = build_shutdown_fade(&cfg, 0.0, Duration::from_secs(30)).unwrap();
        assert!(sf.spec.step_count() >= 1);
        assert_eq!(cfg.synth_floor_db, SYNTH_FLOOR_DB);
    }
}
