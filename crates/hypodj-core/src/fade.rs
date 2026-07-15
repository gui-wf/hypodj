//! The pure volume-envelope (fade) primitive.
//!
//! FOUNDATION (P0): this is the cancellable, superseding fade the whole
//! smart-server plan/executor stack later reuses VERBATIM (wind-down, sleep
//! fade-out, wake ramp-in are the same driver with different targets). So it is
//! built to LAST: zero dependency on the mpv actor or the MPD handler, generic
//! over a [`Clock`](crate::clock::Clock) and a [`VolumeSink`], and fully testable
//! with a fake clock + a recording sink - no real time, no real audio.
//!
//! ## Why stepping, not `afade`
//!
//! ffmpeg's `afade` was rejected: it schedules on ABSOLUTE stream timestamps and
//! cannot be cancelled or reparametrised mid-flight (the exact live-control this
//! primitive exists to provide). We step mpv's `volume` property instead, on an
//! absolute wall-clock deadline schedule that self-corrects the actor's 0.1s poll
//! jitter. mpv softvol is per-process and private, so this never fights PipeWire.
//!
//! ## Why the perceptual dB domain
//!
//! Loudness is compressive (Stevens ~0.67; +10 dB ~ 2x loud), so a LINEAR
//! amplitude ramp feels fast-then-plunge - the startle curve the fade design
//! forbids. We operate in dB: an equal-dB-per-step schedule is
//! exponential-amplitude, perceptually smooth. The sink ([`VolumeSink`]) speaks
//! dB only; the cubic softvol inversion lives strictly BELOW it (in the player).
//!
//! ## Startle safety, enforced in CODE (not just documented)
//!
//! [`FadeSpec::new`] precomputes a validated [`Schedule`] that guarantees:
//!   - every step interval is `>= min_slew` (>= 250 ms; startle vanishes by
//!     ~140-220 ms, so a slewed step never triggers the brainstem);
//!   - each per-step dB delta is `<= step_size_db` (sub-JND, ~0.75 dB) after
//!     clamping - the sub-JND staircase IS the startle mitigation;
//!   - the schedule is MONOTONE toward the target: it never overshoots and never
//!     re-brightens (fresh transients re-flag the autistic/anxious brain);
//!   - it is NEVER a hard cut: even `dur < min_slew` yields one >= 250 ms slewed
//!     step, and true silence is reached via one final slewed mute step from the
//!     -60 dB floor (no click), not an instantaneous drop to zero.
//!
//! The envelope is content-agnostic and CONTINUOUS across track boundaries (mpv
//! `volume` persists across `loadfile ... replace`); the handler does NOT cancel
//! a fade on next/prev/play. End-of-track scheduling and cross-track equal-power
//! crossfades are explicitly OUT of P0 scope (P1/P2).
//!
//! RPITIT (the `impl Future` in [`VolumeSink`]) is always monomorphised at the
//! concrete sink; it is never boxed into a `dyn`.

use std::time::Duration;

use crate::clock::Clock;
use crate::player::{db_to_mpv_volume, PlayerError, PlayerHandle};

/// dB-below-which a `range` is treated as already-arrived (guards from == to).
const RANGE_EPS_DB: f64 = 1e-3;

/// Hard floor on any step interval, in ms. The configured `min_slew` must not
/// dip below this (startle re-emerges below ~200 ms); enforced by both the
/// config normalization pass (clamp-up + log) and [`FadeSpec::new`] (reject).
/// Exposed so [`crate::config::FadeConfig::normalize`] clamps against the SAME
/// value the spec validates against - one source of truth for the hard floor.
pub const STARTLE_HARD_MIN_SLEW_MS: u64 = 200;

/// Hard floor on any step interval as a [`Duration`] (see
/// [`STARTLE_HARD_MIN_SLEW_MS`]).
const STARTLE_HARD_MIN_SLEW: Duration = Duration::from_millis(STARTLE_HARD_MIN_SLEW_MS);

/// Ceiling on a DELIBERATE (`sub_jnd = false`) per-step dB delta. Above this even
/// a "noticeable cue" fade is a startle risk, so [`FadeSpec::new`] rejects it.
const DELIBERATE_STEP_CAP_DB: f64 = 3.0;

/// Hard cap on the number of scheduled steps. Belt-and-suspenders against a
/// pathological (`dur`, `tick`, `step_size`) combination producing an enormous
/// count that would overflow `Vec::with_capacity` or `Duration` multiplication.
/// A real fade never approaches this (max_dur 1800s / 200ms tick = 9000 steps).
const MAX_SCHEDULE_STEPS: u64 = 1_000_000;

/// The interpolation shape across the fade, sampled on `t01` in `[0, 1]`.
///
/// `#[non_exhaustive]`: `DbLinear` (equal-dB-per-tick) is the only variant now;
/// `EqualPower` / `Log` (ease-out tail) are reserved without a breaking change.
#[derive(Clone, Copy, Debug)]
#[non_exhaustive]
pub enum Curve {
    /// Linear in the dB domain: `sample(t01) == t01`. Equal-dB steps, i.e.
    /// exponential amplitude - the perceptually-even default.
    DbLinear,
}

impl Curve {
    /// Fraction of the total dB `range` applied by parameter `t01` in `[0, 1]`.
    pub fn sample(&self, t01: f64) -> f64 {
        match self {
            Curve::DbLinear => t01,
        }
    }
}

/// Where a fade ends.
#[derive(Clone, Copy, Debug)]
pub enum FadeTarget {
    /// A specific perceptual level, in dB (0 dB == mpv volume 100).
    Db(f64),
    /// True silence: ramp to the -60 dB floor, then one final slewed mute step to
    /// mpv volume 0 (no click).
    Silence,
}

/// The startle-safety envelope bounds, sourced from `[fade]` config.
#[derive(Clone, Copy, Debug)]
pub struct StartleBounds {
    /// Minimum interval per step (hard floor 250 ms in config; must be
    /// `>= 200 ms` or [`FadeSpec::new`] returns [`FadeError::SlewTooShort`]).
    pub min_slew: Duration,
    /// Maximum per-step dB delta for a sub-JND fade (~0.75 dB).
    pub step_size_db: f64,
    /// The perceptual floor (-60 dB); at or below it the signal is silence.
    pub synth_floor_db: f64,
    /// Enforce the sub-JND cap by EXTENDING duration when needed (sleep/wind-down
    /// fades). `false` for a deliberate 2-3 dB cue (`fade to`), still slewed.
    pub sub_jnd: bool,
}

/// One scheduled step: apply `gain_db` at absolute offset `at` from the fade's
/// start instant `t0`. A `gain_db` of `f64::NEG_INFINITY` is the final mute step
/// (sink maps it to mpv volume 0 - true silence without a click).
#[derive(Clone, Copy, Debug)]
struct Step {
    at: Duration,
    gain_db: f64,
}

/// The precomputed, validated step schedule. Private: the only way to obtain one
/// is [`FadeSpec::new`], which guarantees the startle invariants.
#[derive(Clone, Debug)]
struct Schedule {
    steps: Vec<Step>,
}

/// A validated fade: an immutable plan the driver replays. Fields are PRIVATE -
/// construction goes through the fallible [`FadeSpec::new`], so an unvalidated
/// (startle-unsafe) spec cannot exist.
#[derive(Clone, Debug)]
pub struct FadeSpec {
    /// The curve the schedule was sampled from. Retained (not read by the driver,
    /// which replays the precomputed steps) for introspection and future
    /// reparam - EqualPower/Log land here without reshaping the type.
    #[allow(dead_code)]
    curve: Curve,
    /// The starting perceptual level (clamped `>= synth_floor_db`), retained for
    /// the degenerate (empty-schedule) report.
    from_db: f64,
    schedule: Schedule,
}

/// Why a [`FadeSpec`] could not be built. All three are reachable and tested.
#[derive(Debug, thiserror::Error, PartialEq)]
pub enum FadeError {
    /// The configured `min_slew` is below the startle hard floor (200 ms).
    #[error("min_slew below the startle hard floor of 200ms")]
    SlewTooShort,
    /// A deliberate (`sub_jnd = false`) fade would exceed the 3 dB/step cap: the
    /// duration is too short even for a noticeable-cue rate.
    #[error("per-step dB delta exceeds the 3dB deliberate-cue cap")]
    StepTooLarge,
    /// `from_db` or a `Db(target)` was NaN / infinite.
    #[error("non-finite dB input")]
    NonFinite,
}

impl FadeSpec {
    /// Build a validated schedule from the fade parameters. Returns an error
    /// rather than ever producing a startle-unsafe plan.
    ///
    /// `from_db` is the LIVE level at spawn (clamped `>= synth_floor_db` so a
    /// resume-from-silence never feeds -inf into the math). `dur` / `tick` are
    /// the nominal duration and step interval; `tick` IS the step interval and is
    /// clamped up to `min_slew`. See the module docs for the invariants.
    pub fn new(
        from_db: f64,
        target: FadeTarget,
        dur: Duration,
        tick: Duration,
        curve: Curve,
        bounds: StartleBounds,
    ) -> Result<FadeSpec, FadeError> {
        // Validate finiteness up front (NaN would poison every comparison below).
        if !from_db.is_finite() {
            return Err(FadeError::NonFinite);
        }
        if let FadeTarget::Db(x) = target {
            if !x.is_finite() {
                return Err(FadeError::NonFinite);
            }
        }
        // The configured slew floor must itself clear the startle hard floor.
        if bounds.min_slew < STARTLE_HARD_MIN_SLEW {
            return Err(FadeError::SlewTooShort);
        }

        // 1. Resolve the target dB + whether a final mute step is needed.
        let (t_db, mute_last) = match target {
            FadeTarget::Db(x) => (x, false),
            FadeTarget::Silence => (bounds.synth_floor_db, true),
        };
        // Clamp the start to the finite floor (no -inf ever enters the math).
        let f = from_db.max(bounds.synth_floor_db);

        // 2. tick is the step interval; never below min_slew. Duration never
        //    yields fewer than one full slewed step.
        let t_eff = tick.max(bounds.min_slew);
        let mut d_eff = dur.max(bounds.min_slew);

        // 3. Degenerate: already at the target. One report, immediate complete.
        //    A Silence target whose start is already at the floor also lands here
        //    (already silent - no mute step needed).
        let range = t_db - f;
        if range.abs() < RANGE_EPS_DB {
            return Ok(FadeSpec {
                curve,
                from_db: f,
                schedule: Schedule { steps: Vec::new() },
            });
        }

        // 4. Nominal step count from the (clamped) duration.
        let t_eff_s = t_eff.as_secs_f64();
        let mut n = ((d_eff.as_secs_f64() / t_eff_s).ceil() as u64).max(1);

        // 5. Sub-JND enforcement: if a step would exceed step_size_db, EXTEND the
        //    duration (more steps) rather than silently violate the cap. Logged,
        //    never silent. `fade to` (sub_jnd = false) skips this for a
        //    deliberate cue but is still capped at 3 dB/step below.
        // Guard the divide: `step_size_db > 0` is guaranteed by config
        // normalization (MIN_STEP_SIZE_DB), but a hand-built StartleBounds could
        // pass 0 / negative, which would yield +inf -> u64::MAX steps -> capacity
        // / Duration overflow panic. Skip the extension in that degenerate case.
        if bounds.sub_jnd
            && bounds.step_size_db > 0.0
            && range.abs() / (n as f64) > bounds.step_size_db
        {
            let needed = (range.abs() / bounds.step_size_db).ceil() as u64;
            n = needed.clamp(1, MAX_SCHEDULE_STEPS);
            d_eff = t_eff.saturating_mul(n as u32);
            tracing::info!(
                extended_secs = d_eff.as_secs_f64(),
                steps = n,
                "fade extended to honor the sub-JND per-step cap"
            );
        }
        // Belt-and-suspenders cap so a pathological nominal count (huge dur / tiny
        // tick) can never overflow the capacity / Duration math below.
        n = n.min(MAX_SCHEDULE_STEPS);
        let _ = d_eff; // d_eff's only role after extension is the log above.

        // 6. Equal-dB steps toward the target on absolute deadlines t0 + k*T_eff.
        let step_db = range / (n as f64);
        // A deliberate cue must still not exceed the 3 dB/step startle ceiling.
        if !bounds.sub_jnd && step_db.abs() > DELIBERATE_STEP_CAP_DB {
            return Err(FadeError::StepTooLarge);
        }
        let mut steps = Vec::with_capacity(n as usize + mute_last as usize);
        for k in 1..=n {
            let t01 = (k as f64) / (n as f64);
            steps.push(Step {
                at: t_eff.saturating_mul(k as u32),
                gain_db: f + curve.sample(t01) * range,
            });
        }

        // 7. Final mute step: one more slewed interval AFTER the floor, dropping
        //    to true silence (NEG_INFINITY -> mpv volume 0) without a click.
        if mute_last {
            steps.push(Step {
                at: t_eff.saturating_mul((n + 1) as u32),
                gain_db: f64::NEG_INFINITY,
            });
        }

        Ok(FadeSpec {
            curve,
            from_db: f,
            schedule: Schedule { steps },
        })
    }
}

/// A dB-only volume sink. The fade driver knows ONLY this: it never learns it is
/// driving mpv, never owns stop/seek. [`PlayerHandle`] implements it by inverting
/// the cubic softvol curve internally (the cube lives strictly below this seam).
pub trait VolumeSink {
    /// Apply a perceptual gain in dB. `f64::NEG_INFINITY` means true silence.
    fn set_gain_db(&self, db: f64) -> impl std::future::Future<Output = Result<(), PlayerError>> + Send;
}

impl VolumeSink for PlayerHandle {
    fn set_gain_db(&self, db: f64) -> impl std::future::Future<Output = Result<(), PlayerError>> + Send {
        // Invert the cube HERE, below the dB seam, then drive the fractional
        // actor path (never the u8 external seam).
        let vol = db_to_mpv_volume(db);
        async move { self.set_volume_f64(vol).await }
    }
}

/// One reported step of progress. `done` is true on the final step (or the single
/// degenerate report). Used by the handler to write the live gain + coalesce
/// change notifications.
#[derive(Clone, Copy, Debug)]
pub struct FadeProgress {
    pub gain_db: f64,
    pub done: bool,
}

/// How a fade run ended. An ABORT (task cancelled) drops the future and returns
/// nothing - so a superseded fade produces no outcome at all, by design.
#[derive(Debug)]
pub enum FadeOutcome {
    /// The schedule ran to completion (or was degenerate).
    Completed,
    /// The sink errored mid-fade; the loop stopped at a known volume (the last
    /// successfully applied step, already reported). No spin, no half-fade stall.
    SinkError(PlayerError),
}

/// The pure fade driver: replay `spec` against `sink` on `clock`'s timeline,
/// reporting each applied step through `report`.
///
/// Generic over the clock and sink so it runs identically under real time (the
/// daemon) and paused fake time (tests). Absolute-deadline scheduling
/// (`t0 + k*tick`) self-corrects sink/poll jitter: a slow step does not push the
/// next one later, so total duration lands within one tick of nominal.
pub async fn run_fade<S: VolumeSink, C: Clock>(
    sink: &S,
    spec: &FadeSpec,
    clock: &C,
    // `+ Send` so the whole driver future stays `Send` and the handler can spawn
    // it onto the multi-thread runtime (the report closure captures only `Send`
    // state: `Arc`s + a `u8`).
    report: &mut (dyn FnMut(FadeProgress) + Send),
) -> FadeOutcome {
    let steps = &spec.schedule.steps;
    // Degenerate schedule (from == to): report the current level once, done.
    if steps.is_empty() {
        report(FadeProgress { gain_db: spec.from_db, done: true });
        return FadeOutcome::Completed;
    }

    let t0 = clock.now();
    let n = steps.len();
    for (i, step) in steps.iter().enumerate() {
        clock.sleep_until(t0 + step.at).await;
        if let Err(e) = sink.set_gain_db(step.gain_db).await {
            // Stop at the last known-good volume (the previous, already-reported
            // step). No further writes, no spin.
            return FadeOutcome::SinkError(e);
        }
        report(FadeProgress { gain_db: step.gain_db, done: i + 1 == n });
    }
    FadeOutcome::Completed
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::clock::TokioClock;
    use crate::player::SYNTH_FLOOR_DB;
    use std::sync::{Arc, Mutex};

    fn bounds(sub_jnd: bool) -> StartleBounds {
        StartleBounds {
            min_slew: Duration::from_millis(250),
            step_size_db: 0.75,
            synth_floor_db: SYNTH_FLOOR_DB,
            sub_jnd,
        }
    }

    // A degenerate huge tick (from a finite-but-absurd config) must not overflow
    // Duration in the sub-JND extension (tick * step_count); saturating arithmetic
    // keeps FadeSpec::new panic-free - it returns, never crashes.
    #[test]
    fn huge_tick_does_not_overflow_or_panic() {
        let huge = Duration::from_millis(20_000_000_000_000_000);
        let b = StartleBounds {
            min_slew: huge,
            step_size_db: 0.75,
            synth_floor_db: SYNTH_FLOOR_DB,
            sub_jnd: true,
        };
        // Just must not panic; either outcome is acceptable for a pathological cfg.
        let _ = FadeSpec::new(0.0, FadeTarget::Silence, huge, huge, Curve::DbLinear, b);
    }

    /// A recording [`VolumeSink`] for tests: captures every applied gain (and the
    /// virtual instant it ran), can inject per-call latency, and can fail on the
    /// Nth call. No real audio, deterministic under paused time.
    #[derive(Clone)]
    struct RecordingSink {
        gains: Arc<Mutex<Vec<f64>>>,
        times: Arc<Mutex<Vec<tokio::time::Instant>>>,
        calls: Arc<Mutex<usize>>,
        fail_on: Option<usize>,
        latency: Duration,
    }

    impl RecordingSink {
        fn new() -> Self {
            Self {
                gains: Arc::new(Mutex::new(Vec::new())),
                times: Arc::new(Mutex::new(Vec::new())),
                calls: Arc::new(Mutex::new(0)),
                fail_on: None,
                latency: Duration::ZERO,
            }
        }
        fn failing_on(n: usize) -> Self {
            Self { fail_on: Some(n), ..Self::new() }
        }
        fn with_latency(latency: Duration) -> Self {
            Self { latency, ..Self::new() }
        }
        fn gains(&self) -> Vec<f64> {
            self.gains.lock().unwrap().clone()
        }
    }

    impl VolumeSink for RecordingSink {
        fn set_gain_db(
            &self,
            db: f64,
        ) -> impl std::future::Future<Output = Result<(), PlayerError>> + Send {
            let gains = self.gains.clone();
            let times = self.times.clone();
            let calls = self.calls.clone();
            let fail_on = self.fail_on;
            let latency = self.latency;
            async move {
                if !latency.is_zero() {
                    tokio::time::sleep(latency).await;
                }
                let n = {
                    let mut c = calls.lock().unwrap();
                    *c += 1;
                    *c
                };
                if Some(n) == fail_on {
                    return Err(PlayerError::Backend("injected sink failure".into()));
                }
                gains.lock().unwrap().push(db);
                times.lock().unwrap().push(tokio::time::Instant::now());
                Ok(())
            }
        }
    }

    async fn drive(spec: &FadeSpec, sink: &RecordingSink) -> (FadeOutcome, Vec<FadeProgress>) {
        let clock = TokioClock;
        let mut prog = Vec::new();
        let outcome = {
            let mut report = |p: FadeProgress| prog.push(p);
            run_fade(sink, spec, &clock, &mut report).await
        };
        (outcome, prog)
    }

    #[test]
    fn monotone_non_increasing_and_sub_jnd() {
        // A wind-down fade-out: gains strictly non-increasing, every per-step
        // delta <= step_size_db after clamp.
        let spec = FadeSpec::new(
            0.0,
            FadeTarget::Db(-45.0),
            Duration::from_secs(60),
            Duration::from_millis(250),
            Curve::DbLinear,
            bounds(true),
        )
        .unwrap();
        let steps = &spec.schedule.steps;
        assert!(!steps.is_empty());
        for w in steps.windows(2) {
            assert!(w[1].gain_db <= w[0].gain_db + 1e-9, "must not re-brighten");
            let d = (w[0].gain_db - w[1].gain_db).abs();
            assert!(d <= 0.75 + 1e-9, "per-step delta {d} exceeds sub-JND cap");
        }
        // Never overshoots the target.
        assert!(steps.last().unwrap().gain_db >= -45.0 - 1e-9);
    }

    #[test]
    fn every_interval_ge_min_slew() {
        let spec = FadeSpec::new(
            0.0,
            FadeTarget::Db(-30.0),
            Duration::from_secs(10),
            // A sub-min_slew tick must be clamped up to 250 ms.
            Duration::from_millis(100),
            Curve::DbLinear,
            bounds(true),
        )
        .unwrap();
        let steps = &spec.schedule.steps;
        assert_eq!(steps[0].at, Duration::from_millis(250));
        for w in steps.windows(2) {
            assert!(w[1].at - w[0].at >= Duration::from_millis(250));
        }
    }

    #[test]
    fn total_duration_extends_not_violates() {
        // 'fade to 0 (from 0 dB) in 5s' spans ~45 dB; 5s / 250ms = 20 steps would
        // be 2.25 dB/step, over the 0.75 cap. It must EXTEND n, not violate.
        let spec = FadeSpec::new(
            0.0,
            FadeTarget::Db(-45.0),
            Duration::from_secs(5),
            Duration::from_millis(250),
            Curve::DbLinear,
            bounds(true),
        )
        .unwrap();
        let steps = &spec.schedule.steps;
        // ceil(45 / 0.75) = 60 steps, not the nominal 20.
        assert_eq!(steps.len(), 60);
        for w in steps.windows(2) {
            assert!((w[0].gain_db - w[1].gain_db).abs() <= 0.75 + 1e-9);
        }
    }

    #[tokio::test(start_paused = true)]
    async fn fade_in_from_silence_finite() {
        // Resume from vol0 -> from_db resolves to the -60 floor; ramp UP to -6 dB.
        let spec = FadeSpec::new(
            SYNTH_FLOOR_DB,
            FadeTarget::Db(-6.0),
            Duration::from_secs(30),
            Duration::from_millis(250),
            Curve::DbLinear,
            bounds(true),
        )
        .unwrap();
        let sink = RecordingSink::new();
        let (outcome, prog) = drive(&spec, &sink).await;
        assert!(matches!(outcome, FadeOutcome::Completed));
        let gains = sink.gains();
        assert!(gains.iter().all(|g| g.is_finite()), "no NaN/inf on a ramp-in");
        // Monotone UP, sub-JND, never overshoot the -6 dB ceiling.
        for w in gains.windows(2) {
            assert!(w[1] >= w[0] - 1e-9);
            assert!((w[1] - w[0]).abs() <= 0.75 + 1e-9);
        }
        assert!(gains.last().unwrap() <= &(-6.0 + 1e-9));
        assert!(prog.last().unwrap().done);
    }

    #[test]
    fn silence_final_mute_step() {
        let spec = FadeSpec::new(
            -6.0,
            FadeTarget::Silence,
            Duration::from_secs(20),
            Duration::from_millis(250),
            Curve::DbLinear,
            bounds(true),
        )
        .unwrap();
        let steps = &spec.schedule.steps;
        let last = steps.last().unwrap();
        let penult = steps[steps.len() - 2];
        // Exactly one NEG_INFINITY step, one min_slew interval after the floor.
        assert!(last.gain_db.is_infinite() && last.gain_db < 0.0);
        assert!((penult.gain_db - SYNTH_FLOOR_DB).abs() < 1e-6);
        assert!(last.at - penult.at >= Duration::from_millis(250));
        // Only the final step is the mute step.
        assert_eq!(steps.iter().filter(|s| s.gain_db.is_infinite()).count(), 1);
    }

    #[tokio::test(start_paused = true)]
    async fn degenerate_from_eq_to() {
        // from == to -> empty schedule, single report, immediate complete.
        let spec = FadeSpec::new(
            -12.0,
            FadeTarget::Db(-12.0),
            Duration::from_secs(10),
            Duration::from_millis(250),
            Curve::DbLinear,
            bounds(true),
        )
        .unwrap();
        assert!(spec.schedule.steps.is_empty());
        let sink = RecordingSink::new();
        let (outcome, prog) = drive(&spec, &sink).await;
        assert!(matches!(outcome, FadeOutcome::Completed));
        assert_eq!(prog.len(), 1);
        assert!(prog[0].done);
        // No sink writes for a degenerate fade.
        assert!(sink.gains().is_empty());
    }

    #[test]
    fn dur_below_min_slew_is_one_slewed_step() {
        // A 50 ms fade must not hard-cut: exactly one >= 250 ms slewed step.
        let spec = FadeSpec::new(
            0.0,
            FadeTarget::Db(-2.0),
            Duration::from_millis(50),
            Duration::from_millis(50),
            Curve::DbLinear,
            bounds(false),
        )
        .unwrap();
        let steps = &spec.schedule.steps;
        assert_eq!(steps.len(), 1);
        assert_eq!(steps[0].at, Duration::from_millis(250));
        assert!((steps[0].gain_db - (-2.0)).abs() < 1e-9);
    }

    #[tokio::test(start_paused = true)]
    async fn sink_error_terminates_with_last_good() {
        let spec = FadeSpec::new(
            0.0,
            FadeTarget::Db(-30.0),
            Duration::from_secs(20),
            Duration::from_millis(250),
            Curve::DbLinear,
            bounds(true),
        )
        .unwrap();
        // Fail on the 5th applied step.
        let sink = RecordingSink::failing_on(5);
        let (outcome, prog) = drive(&spec, &sink).await;
        assert!(matches!(outcome, FadeOutcome::SinkError(_)));
        // 4 good writes recorded, and the loop stopped (no further reports).
        assert_eq!(sink.gains().len(), 4);
        assert_eq!(prog.len(), 4);
        assert!(!prog.last().unwrap().done, "aborted mid-fade, not done");
    }

    #[tokio::test(start_paused = true)]
    async fn absolute_deadline_no_drift() {
        // Inject 50 ms of sink latency per step; absolute deadlines mean the total
        // still lands within one tick of nominal (steps do not accumulate lag).
        let spec = FadeSpec::new(
            0.0,
            FadeTarget::Db(-3.0),
            Duration::from_secs(1),
            Duration::from_millis(250),
            Curve::DbLinear,
            bounds(false),
        )
        .unwrap();
        let n = spec.schedule.steps.len() as u32;
        let sink = RecordingSink::with_latency(Duration::from_millis(50));
        let clock = TokioClock;
        let t0 = clock.now();
        let mut report = |_p: FadeProgress| {};
        let outcome = run_fade(&sink, &spec, &clock, &mut report).await;
        assert!(matches!(outcome, FadeOutcome::Completed));
        let elapsed = clock.now() - t0;
        let nominal = Duration::from_millis(250) * n;
        // Within one tick of nominal despite per-step latency.
        assert!(
            elapsed >= nominal && elapsed <= nominal + Duration::from_millis(250),
            "elapsed {elapsed:?} drifted from nominal {nominal:?}"
        );
    }

    // C1 belt: a pathological StartleBounds (step_size_db = 0) must NOT panic in
    // FadeSpec::new (no divide-by-zero -> u64::MAX steps -> capacity overflow).
    #[test]
    fn zero_step_size_does_not_panic() {
        let bad = StartleBounds { step_size_db: 0.0, ..bounds(true) };
        let spec = FadeSpec::new(
            0.0,
            FadeTarget::Db(-45.0),
            Duration::from_secs(60),
            Duration::from_millis(250),
            Curve::DbLinear,
            bad,
        )
        .expect("zero step_size must build, not panic");
        // The sub-JND extension is skipped, so it falls back to the nominal count.
        assert!(!spec.schedule.steps.is_empty());
        assert!(spec.schedule.steps.len() as u64 <= MAX_SCHEDULE_STEPS);
        // A negative step_size is equally safe.
        let neg = StartleBounds { step_size_db: -1.0, ..bounds(true) };
        assert!(FadeSpec::new(
            0.0,
            FadeTarget::Silence,
            Duration::from_secs(30),
            Duration::from_millis(250),
            Curve::DbLinear,
            neg,
        )
        .is_ok());
    }

    #[test]
    fn rejects_non_finite_and_bad_bounds() {
        // NaN start.
        assert_eq!(
            FadeSpec::new(f64::NAN, FadeTarget::Db(-6.0), Duration::from_secs(5),
                Duration::from_millis(250), Curve::DbLinear, bounds(true)).unwrap_err(),
            FadeError::NonFinite
        );
        // Infinite Db target.
        assert_eq!(
            FadeSpec::new(0.0, FadeTarget::Db(f64::INFINITY), Duration::from_secs(5),
                Duration::from_millis(250), Curve::DbLinear, bounds(true)).unwrap_err(),
            FadeError::NonFinite
        );
        // min_slew below the 200 ms startle hard floor.
        let bad = StartleBounds { min_slew: Duration::from_millis(100), ..bounds(true) };
        assert_eq!(
            FadeSpec::new(0.0, FadeTarget::Db(-6.0), Duration::from_secs(5),
                Duration::from_millis(250), Curve::DbLinear, bad).unwrap_err(),
            FadeError::SlewTooShort
        );
        // A deliberate (sub_jnd=false) fade over a big range in one short step
        // exceeds the 3 dB/step cap.
        assert_eq!(
            FadeSpec::new(0.0, FadeTarget::Db(-45.0), Duration::from_millis(250),
                Duration::from_millis(250), Curve::DbLinear, bounds(false)).unwrap_err(),
            FadeError::StepTooLarge
        );
    }
}
