//! A minimal time abstraction shared across the smart-server stack.
//!
//! FOUNDATION (P0): the fade driver ([`crate::fade::run_fade`]) is generic over
//! this trait so it can run under a fake clock in tests - no real sleeping, no
//! wall-clock flakiness. It is deliberately NOT fade-local: P1's timer source and
//! P2's plan executor reuse the SAME seam so every scheduled behavior shares one
//! fake-time base. Keeping it here (not in `fade`) is what makes that reuse
//! honest rather than a later copy.
//!
//! ## Why absolute deadlines, not durations
//!
//! The trait exposes [`Clock::now`] + [`Clock::sleep_until`] (an absolute
//! deadline), NOT a relative `sleep(dur)`. Absolute-deadline scheduling
//! self-corrects drift: a fade step computes `t0 + k*tick` once and sleeps to
//! that instant, so a slow sink call on step k does not push step k+1 later - the
//! total envelope lands within one tick of its nominal duration regardless of
//! per-step jitter (mpv's 0.1s event poll included).
//!
//! ## Testing
//!
//! Use `#[tokio::test(start_paused = true)]` and `tokio::time::advance(..)` with
//! [`TokioClock`]: paused time makes `sleep_until` resolve instantly when time is
//! advanced past the deadline, so a multi-minute fade runs in microseconds and
//! deterministically. No separate fake-clock type is needed - tokio's own paused
//! clock IS the fake clock, and the production path uses the identical code.

use std::future::Future;

use tokio::time::Instant;

/// A source of "now" and a way to await an absolute deadline. Cloneable and
/// `Send + Sync + 'static` so it can be captured into spawned fade tasks and
/// shared across the timer/executor layers.
pub trait Clock: Clone + Send + Sync + 'static {
    /// The current instant on this clock's timeline.
    fn now(&self) -> Instant;

    /// Resolve once the clock reaches `deadline` (immediately if already past).
    fn sleep_until(&self, deadline: Instant) -> impl Future<Output = ()> + Send;
}

/// The production clock: real (or, under `start_paused`, tokio-virtual) time.
/// Zero-sized, so capturing it into a task costs nothing.
#[derive(Clone, Copy, Default, Debug)]
pub struct TokioClock;

impl Clock for TokioClock {
    fn now(&self) -> Instant {
        Instant::now()
    }

    fn sleep_until(&self, deadline: Instant) -> impl Future<Output = ()> + Send {
        tokio::time::sleep_until(deadline)
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::time::Duration;

    // Under paused time, sleep_until resolves exactly when the clock is advanced
    // past the deadline - the property the fade tests lean on for determinism.
    #[tokio::test(start_paused = true)]
    async fn sleep_until_resolves_on_advance() {
        let clock = TokioClock;
        let start = clock.now();
        let deadline = start + Duration::from_secs(300);

        let sleeper = clock.sleep_until(deadline);
        tokio::pin!(sleeper);

        // Not yet: the deadline is in the (virtual) future.
        assert!(
            futures_poll_ready(&mut sleeper).is_none(),
            "should not be ready before the deadline"
        );
        tokio::time::advance(Duration::from_secs(300)).await;
        // Now the deadline has passed; the sleep resolves.
        sleeper.await;
        assert!(clock.now() >= deadline);
    }

    /// Poll a pinned future once without awaiting; `Some(())` if it was ready.
    /// A tiny helper so the test can assert "not ready yet" without a race.
    fn futures_poll_ready<F: Future<Output = ()>>(
        fut: &mut std::pin::Pin<&mut F>,
    ) -> Option<()> {
        use std::task::{Context, Poll, RawWaker, RawWakerVTable, Waker};
        fn noop(_: *const ()) {}
        fn clone(_: *const ()) -> RawWaker {
            RawWaker::new(std::ptr::null(), &VTABLE)
        }
        static VTABLE: RawWakerVTable = RawWakerVTable::new(clone, noop, noop, noop);
        let raw = RawWaker::new(std::ptr::null(), &VTABLE);
        let waker = unsafe { Waker::from_raw(raw) };
        let mut cx = Context::from_waker(&waker);
        match fut.as_mut().poll(&mut cx) {
            Poll::Ready(()) => Some(()),
            Poll::Pending => None,
        }
    }
}
