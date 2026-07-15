//! P1 wall-clock timer source: a single task that fires [`TimerId`]s at absolute
//! deadlines on the shared [`Clock`] timeline.
//!
//! FOUNDATION (P1). This is the ONLY time base P1/P2 scheduling uses (no direct
//! `tokio::time`), so timers share the SAME `start_paused` fake-time base as the
//! P0 fade driver ([`crate::fade::run_fade`]) - a test advances virtual time once
//! and every scheduled behavior resolves deterministically.
//!
//! ## Cancel / supersede without phantom fires (the `fade_epoch` idiom)
//!
//! Every [`arm`](TimerHandle::arm) mints a FRESH [`TimerId`] and records it in a
//! live-set. [`cancel`](TimerHandle::cancel) removes the id. A heap entry only
//! fires if its id is STILL live when its deadline is reached, so a
//! cancelled-then-rearmed timer whose old deadline fires late simply no-ops (the
//! old id is gone from the live-set; the rearm is a different id). A
//! [`TimerGuard`] cancels on drop, so a leaked handle can never fire.
//!
//! Fires are delivered on the LOSSLESS trigger path (an unbounded `mpsc` the
//! director drains), never only on the lossy broadcast.

use std::cmp::Reverse;
use std::collections::{BinaryHeap, HashSet};
use std::sync::atomic::{AtomicU64, Ordering};
use std::sync::Arc;

use tokio::sync::mpsc;
use tokio::time::Instant;

use crate::clock::Clock;
use crate::event::TimerId;

/// A command to the timer source task.
enum TimerCmd {
    Arm { id: TimerId, deadline: Instant },
    Cancel { id: TimerId },
}

/// The cloneable handle used to arm and cancel wall-clock timers.
#[derive(Clone)]
pub struct TimerHandle {
    cmd_tx: mpsc::UnboundedSender<TimerCmd>,
    next_id: Arc<AtomicU64>,
}

impl TimerHandle {
    /// Arm a timer to fire at the ABSOLUTE `deadline` on the source's clock.
    /// Returns the fresh id plus an RAII [`TimerGuard`] that cancels the timer if
    /// dropped. Precise cues (a crossfade lead-in) are armed as absolute deadlines
    /// (`now + time_remaining - lead`); `Tick`/`time_remaining` is advisory only.
    pub fn arm(&self, deadline: Instant) -> (TimerId, TimerGuard) {
        let id = TimerId(self.next_id.fetch_add(1, Ordering::Relaxed));
        // Unbounded control channel: never drops an arm; errs only if the
        // source is gone, in which case the timer simply never fires.
        let _ = self.cmd_tx.send(TimerCmd::Arm { id, deadline });
        let guard = TimerGuard {
            id,
            tx: Some(self.cmd_tx.clone()),
        };
        (id, guard)
    }

    /// Explicitly cancel a timer by id (idempotent; a stale id is a no-op).
    pub fn cancel(&self, id: TimerId) {
        let _ = self.cmd_tx.send(TimerCmd::Cancel { id });
    }
}

/// RAII cancel-on-drop guard for an armed timer. Holding it keeps the timer
/// eligible to fire; dropping it cancels the timer (so a superseded/forgotten
/// arm cannot fire a phantom [`crate::event::DjEventKind::WallClock`]).
pub struct TimerGuard {
    id: TimerId,
    // Option so disarm_on_drop can drop the sender clone and suppress the cancel
    // WITHOUT mem::forget (which would leak the sender and pin the source task).
    tx: Option<mpsc::UnboundedSender<TimerCmd>>,
}

impl TimerGuard {
    /// The id this guard cancels on drop.
    pub fn id(&self) -> TimerId {
        self.id
    }

    /// Consume the guard WITHOUT cancelling (the caller takes over lifetime via
    /// an explicit [`TimerHandle::cancel`]). Rarely needed; the RAII default is
    /// the safe path.
    pub fn disarm_on_drop(mut self) -> TimerId {
        // Drop the sender clone (releasing it) and suppress the cancel: normal Drop
        // then runs with tx = None and sends nothing. No leak, no double action.
        self.tx = None;
        self.id
    }
}

impl Drop for TimerGuard {
    fn drop(&mut self) {
        if let Some(tx) = &self.tx {
            let _ = tx.send(TimerCmd::Cancel { id: self.id });
        }
    }
}

/// One live timer entry in the deadline heap. `Reverse` so the `BinaryHeap`
/// yields the EARLIEST deadline first (a min-heap on the instant).
type HeapEntry = Reverse<(Instant, u64)>;

/// Spawn the timer source over the shared `clock`. Fired timer ids are delivered
/// on `fires` (an UNBOUNDED sender the director drains on the lossless trigger
/// path). The returned [`TimerHandle`] arms/cancels timers; when it (and all
/// clones + guards) are dropped the command channel closes and the source task
/// exits.
pub fn spawn_timer_source<C: Clock>(
    clock: C,
    fires: mpsc::UnboundedSender<TimerId>,
) -> TimerHandle {
    // Unbounded control channel: arms/cancels are low-rate but must NEVER be
    // dropped (a lost arm means a scheduled cue silently never fires).
    let (cmd_tx, mut cmd_rx) = mpsc::unbounded_channel::<TimerCmd>();
    let next_id = Arc::new(AtomicU64::new(0));

    tokio::spawn(async move {
        let mut heap: BinaryHeap<HeapEntry> = BinaryHeap::new();
        let mut live: HashSet<u64> = HashSet::new();

        loop {
            // Drop any dead heap tops (cancelled ids) so `peek` reflects the next
            // LIVE deadline. A cancelled timer leaves its heap entry behind; we
            // reap it lazily here rather than searching the heap on cancel.
            while let Some(Reverse((_, id))) = heap.peek() {
                if live.contains(id) {
                    break;
                }
                heap.pop();
            }
            let next_deadline = heap.peek().map(|Reverse((d, _))| *d);

            match next_deadline {
                Some(deadline) => {
                    tokio::select! {
                        biased;
                        cmd = cmd_rx.recv() => match cmd {
                            Some(cmd) => apply_cmd(&mut heap, &mut live, cmd),
                            None => break, // all handles dropped: exit.
                        },
                        _ = clock.sleep_until(deadline) => {
                            // The earliest deadline is reached. Fire it if still
                            // live, then reap it. Multiple timers may share an
                            // instant; the loop re-evaluates and fires each.
                            if let Some(Reverse((_, id))) = heap.pop() {
                                if live.remove(&id) {
                                    // Unbounded, lossless: a fire is never dropped.
                                    let _ = fires.send(TimerId(id));
                                }
                            }
                        }
                    }
                }
                None => {
                    // No timers armed: just wait for the next command.
                    match cmd_rx.recv().await {
                        Some(cmd) => apply_cmd(&mut heap, &mut live, cmd),
                        None => break,
                    }
                }
            }
        }
    });

    TimerHandle { cmd_tx, next_id }
}

fn apply_cmd(heap: &mut BinaryHeap<HeapEntry>, live: &mut HashSet<u64>, cmd: TimerCmd) {
    match cmd {
        TimerCmd::Arm { id, deadline } => {
            live.insert(id.0);
            heap.push(Reverse((deadline, id.0)));
        }
        TimerCmd::Cancel { id } => {
            // Remove from the live-set; the heap entry is reaped lazily. A late
            // pop of this id then no-ops (the `fade_epoch` idiom).
            live.remove(&id.0);
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::clock::TokioClock;
    use std::time::Duration;

    // A single armed timer fires exactly once at its deadline under paused time.
    #[tokio::test(start_paused = true)]
    async fn fires_once_at_deadline() {
        let clock = TokioClock;
        let (fire_tx, mut fire_rx) = mpsc::unbounded_channel();
        let timers = spawn_timer_source(clock, fire_tx);

        let (id, _guard) = timers.arm(Instant::now() + Duration::from_secs(10));
        // Not yet.
        assert!(fire_rx.try_recv().is_err());
        tokio::time::advance(Duration::from_secs(11)).await;
        tokio::task::yield_now().await;
        assert_eq!(fire_rx.recv().await, Some(id));
        // Only once.
        tokio::time::advance(Duration::from_secs(60)).await;
        tokio::task::yield_now().await;
        assert!(fire_rx.try_recv().is_err());
    }

    // Cancel-then-rearm: the superseded old timer's late fire no-ops; the rearmed
    // (fresh-id) timer fires. This is the generation/tombstone invariant.
    #[tokio::test(start_paused = true)]
    async fn cancel_then_rearm_old_fire_noops() {
        let clock = TokioClock;
        let (fire_tx, mut fire_rx) = mpsc::unbounded_channel();
        let timers = spawn_timer_source(clock, fire_tx);

        let start = Instant::now();
        let (old_id, old_guard) = timers.arm(start + Duration::from_secs(10));
        timers.cancel(old_id);
        let (new_id, _new_guard) = timers.arm(start + Duration::from_secs(20));
        // Keep the old guard alive so its Drop-cancel is not what silences it -
        // the explicit cancel above is.
        drop(old_guard);

        tokio::time::advance(Duration::from_secs(11)).await;
        tokio::task::yield_now().await;
        // The old deadline passed but was cancelled: no fire.
        assert!(fire_rx.try_recv().is_err());

        tokio::time::advance(Duration::from_secs(10)).await;
        tokio::task::yield_now().await;
        assert_eq!(fire_rx.recv().await, Some(new_id));
    }

    // A dropped guard cancels the timer (RAII), so it never fires.
    #[tokio::test(start_paused = true)]
    async fn guard_drop_cancels() {
        let clock = TokioClock;
        let (fire_tx, mut fire_rx) = mpsc::unbounded_channel();
        let timers = spawn_timer_source(clock, fire_tx);

        let (_id, guard) = timers.arm(Instant::now() + Duration::from_secs(5));
        drop(guard);
        tokio::time::advance(Duration::from_secs(6)).await;
        tokio::task::yield_now().await;
        assert!(fire_rx.try_recv().is_err());
    }
}
