//! P2 plan executor: the effect-owning task layered over the pure IR
//! ([`crate::plan`]).
//!
//! FOUNDATION (P2), built to LAST. The pure core decides WHAT ([`validate`] ->
//! [`Resolved`], [`fires`] -> [`Fire`]); this task owns the effects: it mints
//! [`TimerGuard`]s for deadlines, executes [`Resolved::Immediate`] plans at
//! add-time, and on each event maps a fired [`Action`] onto the existing
//! primitives - [`HypodjHandler::start_fade_spec`] for fades (reused VERBATIM),
//! and synthetic [`MpdCommand`]s for stop/pause/setvol.
//!
//! ## Two inputs, one determinism base
//!
//! The recv loop drains TWO streams:
//!   1. the LOSSLESS trigger `mpsc` (edges + `WallClock` fires) - the fire path;
//!   2. the LOSSY broadcast `Tick` - live position, for lazily arming a
//!      [`Resolved::OnTrackRemaining`] plan at the right moment.
//! All scheduling reuses the shared [`Clock`] + [`TimerHandle`], so a test
//! advances virtual time once and every armed deadline resolves deterministically
//! (the same `start_paused` base as the P0 fade driver).
//!
//! ## Isolation + ordering
//!
//! Fired ids are collected under ONE short registry lock (removing `once:true`
//! plans at selection, mirroring the timer live-set), then executed in ASCENDING
//! [`PlanId`] order. Each action runs in its OWN spawned+joined task, so a
//! panicking or erroring plan is log-and-continue and cannot wedge the loop.

use std::sync::Arc;

use tokio::sync::{broadcast, mpsc};
use tokio::task::JoinHandle;

use crate::clock::Clock;
use crate::event::{DjEvent, DjEventKind, QueueId};
use crate::handler::{FadeIntent, FadeRequest, HypodjHandler};
use crate::mpd::{MpdCommand, MpdHandler};
use crate::plan::{fires, Action, ArmedPlan, FadeIntentIr, Fire, PlanId, Resolved};
use crate::player::PlayState;
use crate::timer::{TimerGuard, TimerHandle};

/// A live-position jump larger than this (a seek, not the second-by-second
/// drift of normal playback) re-arms a TimeRemaining deadline. Normal ticks keep
/// the deadline within a tick of its prior value (now advances as remaining
/// falls), so they never re-arm.
const REARM_JUMP: std::time::Duration = std::time::Duration::from_secs(1);

/// Absolute difference between two monotonic instants (order-free).
fn abs_diff_instant(a: tokio::time::Instant, b: tokio::time::Instant) -> std::time::Duration {
    if a >= b {
        a - b
    } else {
        b - a
    }
}

/// One entry in the shared armed-plan registry: the pure [`ArmedPlan`] plus the
/// RAII [`TimerGuard`] that disarms its timer on drop. Kept OUT of [`ArmedPlan`]
/// so the pure core stays `Clone` + serializable; `plan_cancel`/`plan_replace`
/// drop this to disarm without a phantom `WallClock` fire.
pub struct PendingPlan {
    pub armed: ArmedPlan,
    pub guard: Option<TimerGuard>,
    /// The absolute deadline an [`Resolved::OnTrackRemaining`] plan is currently
    /// armed for (mirrors `guard`/`timer_id`). Held so a fresh Tick whose live
    /// position JUMPED (a seek) is detected and the timer re-armed; `None` when
    /// the plan is not a remaining-plan or is currently disarmed.
    pub remaining_deadline: Option<tokio::time::Instant>,
}

/// The executor task. Generic over the [`Clock`] so it runs identically under
/// real time and paused fake time.
pub struct Executor<C: Clock> {
    handler: Arc<HypodjHandler>,
    timers: TimerHandle,
    #[allow(dead_code)]
    clock: C,
    triggers: mpsc::UnboundedReceiver<DjEvent>,
    ticks: Option<broadcast::Receiver<DjEvent>>,
    immediate: Option<mpsc::UnboundedReceiver<PlanId>>,
    last_version: u64,
}

impl<C: Clock> Executor<C> {
    /// Spawn the executor over the P1 lossless trigger stream + lossy `Tick`
    /// broadcast + the Immediate nudge channel. Returns the task handle.
    pub fn spawn(
        handler: Arc<HypodjHandler>,
        timers: TimerHandle,
        clock: C,
        triggers: mpsc::UnboundedReceiver<DjEvent>,
        ticks: broadcast::Receiver<DjEvent>,
        immediate: mpsc::UnboundedReceiver<PlanId>,
    ) -> JoinHandle<()> {
        let ex = Executor {
            handler,
            timers,
            clock,
            triggers,
            ticks: Some(ticks),
            immediate: Some(immediate),
            last_version: 0,
        };
        tokio::spawn(ex.run())
    }

    async fn run(mut self) {
        loop {
            tokio::select! {
                // The lossless edge/WallClock path is the lifeline: its close means
                // the director spine is gone, so the executor winds down too.
                ev = self.triggers.recv() => match ev {
                    Some(ev) => self.on_event(&ev).await,
                    None => break,
                },
                // Lossy Tick (plus mirrored edges): drives lazy TimeRemaining arming.
                r = recv_bcast(&mut self.ticks) => match r {
                    Some(ev) => self.on_tick(&ev),
                    None => {} // lagged (skip) or closed (branch fused below).
                },
                // Immediate plans handed off by plan_add (their action is async).
                id = recv_mpsc(&mut self.immediate) => match id {
                    Some(id) => self.on_immediate(id),
                    None => {}
                },
            }
        }
    }

    /// An edge (or `WallClock`) arrived: reconcile on a version change, run any
    /// lazy TimeRemaining maintenance, then select + execute the fired plans.
    async fn on_event(&mut self, ev: &DjEvent) {
        if ev.playlist_version != self.last_version {
            self.reconcile();
            self.last_version = ev.playlist_version;
        }
        self.maintain_remaining(ev);

        let snap = self.handler.queue_snapshot();
        let pending = self.handler.plan_pending_handle();
        let mut fired: Vec<(PlanId, Action)> = Vec::new();
        {
            // ONE short lock: collect fired ids + drop once/stale plans, mirroring
            // the timer live-set. No `.await` under this std Mutex.
            let mut g = pending.lock().unwrap();
            g.retain_mut(|pp| match fires(&pp.armed, ev, &snap) {
                Fire::Yes => {
                    fired.push((pp.armed.id, pp.armed.raw.action.clone()));
                    // Every armed anchor here is single-use (a concrete
                    // QueueId/AlbumId/absolute deadline fires exactly once), so a
                    // fired plan is ALWAYS removed at selection (dropping its guard),
                    // regardless of `once` - no un-fireable zombie lingers in the
                    // list. `once` is reserved for future RECURRING triggers; until
                    // one exists every armed plan is fire-once.
                    false
                }
                Fire::Stale => {
                    tracing::warn!(
                        plan = pp.armed.id.0,
                        "plan expired: target queue id gone or already passed"
                    );
                    false
                }
                Fire::No => true,
            });
        }
        // Stable total order: ascending PlanId, reproducible across runs.
        fired.sort_by_key(|(id, _)| *id);
        if !fired.is_empty() {
            self.execute_batch(fired);
        }
    }

    /// A lossy `Tick` (or mirrored edge): maintain the lifecycle of every
    /// TimeRemaining plan (arm/re-arm/disarm) - see [`Self::maintain_remaining`].
    fn on_tick(&self, ev: &DjEvent) {
        self.maintain_remaining(ev);
    }

    /// The TimeRemaining timer lifecycle, bound to the target being the CURRENT
    /// track. On every Tick/edge:
    ///   - DISARM if the target is no longer current (a skip/advance to a different
    ///     queue id, or Stop) or playback paused - a stale deadline must never fire
    ///     on the wrong (now-playing) track;
    ///   - (re-)ARM from a fresh Tick while the target is current: on the first
    ///     Tick, or whenever the live position JUMPED (a seek) so the recomputed
    ///     deadline moved - re-arm to the new deadline.
    /// All timer sends are sync (no `.await` under the std Mutex); the RAII guard
    /// rides in [`PendingPlan`].
    fn maintain_remaining(&self, ev: &DjEvent) {
        let snap = self.handler.queue_snapshot();
        let cur = snap.current.as_ref().map(|c| c.queue_id);
        let paused = matches!(ev.kind, DjEventKind::StateChanged(PlayState::Paused, _));
        let pending = self.handler.plan_pending_handle();
        let mut g = pending.lock().unwrap();
        for pp in g.iter_mut() {
            let (track, lead) = match &pp.armed.resolved {
                Resolved::OnTrackRemaining { track, lead } => (*track, *lead),
                _ => continue,
            };
            // Disarm the moment the target ceases to be current, or on pause. A
            // later Tick while it is current re-arms from the fresh remaining.
            if cur != Some(track) || paused {
                pp.guard = None;
                pp.armed.timer_id = None;
                pp.remaining_deadline = None;
                continue;
            }
            // Current + a fresh position sample: arm when unarmed, or re-arm when the
            // live position jumped (a seek moved the deadline by more than one tick).
            if let DjEventKind::Tick { time_remaining: Some(rem), .. } = &ev.kind {
                let deadline = ev.now + rem.saturating_sub(lead);
                let jumped = match pp.remaining_deadline {
                    None => true,
                    Some(prev) => abs_diff_instant(prev, deadline) > REARM_JUMP,
                };
                if jumped {
                    let (tid, guard) = self.timers.arm(deadline);
                    pp.armed.timer_id = Some(tid);
                    pp.guard = Some(guard);
                    pp.remaining_deadline = Some(deadline);
                }
            }
        }
    }

    /// Execute an Immediate plan handed off at add-time. Its anchor is single-use
    /// (it runs once, now), so it is ALWAYS removed after selection - regardless of
    /// `once` - so a non-once Immediate plan never lingers as an un-fireable zombie.
    fn on_immediate(&self, id: PlanId) {
        let pending = self.handler.plan_pending_handle();
        let action = {
            let mut g = pending.lock().unwrap();
            g.iter().position(|pp| pp.armed.id == id).map(|pos| g.remove(pos).armed.raw.action)
        };
        // Run OFF the select loop (an Immediate Enqueue does network calls): a slow
        // add-time action must not wedge the loop either.
        if let Some(a) = action {
            self.execute_batch(vec![(id, a)]);
        }
    }

    /// DELETE emits no event, so a bumped `playlist_version` triggers this pass:
    /// re-resolve every armed queue id via `snapshot_by_queue_id`; a `None` expires
    /// the plan LOUDLY (dropping its guard), covering the delete gap.
    fn reconcile(&self) {
        let pending = self.handler.plan_pending_handle();
        let mut g = pending.lock().unwrap();
        g.retain(|pp| {
            let qid: Option<QueueId> = match &pp.armed.resolved {
                Resolved::OnTrackStart(q) => Some(*q),
                Resolved::OnAlbumBoundary { last, .. } => Some(*last),
                Resolved::OnTrackRemaining { track, .. } => Some(*track),
                _ => None,
            };
            if let Some(q) = qid {
                if self.handler.snapshot_by_queue_id(q).is_none() {
                    tracing::warn!(
                        plan = pp.armed.id.0,
                        queue_id = q.0,
                        "plan expired on reconcile: target queue id deleted"
                    );
                    return false;
                }
            }
            true
        });
    }

    /// Run a fired batch OFF the recv loop, so a slow/hanging action (an `Enqueue`
    /// does Subsonic network calls) can NEVER wedge the executor: the loop keeps
    /// draining the trigger + tick streams while this runs. The batch is a single
    /// DETACHED task that runs its actions in ascending-PlanId order (stable end
    /// state), each in its OWN spawned+joined task so a panicking or erroring action
    /// is log-and-continue and isolates the rest of the batch. Fades still go
    /// through the single [`HypodjHandler::start_fade_spec`] slot (one envelope).
    fn execute_batch(&self, fired: Vec<(PlanId, Action)>) {
        let h = self.handler.clone();
        tokio::spawn(async move {
            for (id, action) in fired {
                let jh = tokio::spawn(run_action(h.clone(), id, action));
                if let Err(e) = jh.await {
                    tracing::error!(plan = id.0, error = %e, "plan action task panicked; batch continues");
                }
            }
        });
    }
}

/// Await a broadcast receiver that may be closed. `None` = lagged (skip) or closed.
async fn recv_bcast(rx: &mut Option<broadcast::Receiver<DjEvent>>) -> Option<DjEvent> {
    match rx {
        Some(r) => match r.recv().await {
            Ok(ev) => Some(ev),
            Err(broadcast::error::RecvError::Lagged(_)) => None,
            Err(broadcast::error::RecvError::Closed) => {
                *rx = None; // fuse: stop polling a closed broadcast.
                std::future::pending::<()>().await;
                None
            }
        },
        // Fused: park forever so this select branch never busy-spins.
        None => std::future::pending().await,
    }
}

/// Await an mpsc receiver that may be closed, fusing on close so the branch never
/// busy-spins.
async fn recv_mpsc(rx: &mut Option<mpsc::UnboundedReceiver<PlanId>>) -> Option<PlanId> {
    match rx {
        Some(r) => match r.recv().await {
            Some(id) => Some(id),
            None => {
                *rx = None;
                std::future::pending::<()>().await;
                None
            }
        },
        None => std::future::pending().await,
    }
}

/// Map a plan fade intent onto the handler's fade-native [`FadeRequest`].
fn map_fade(ir: &FadeIntentIr) -> FadeRequest {
    let dur = |s: f64| std::time::Duration::try_from_secs_f64(s).unwrap_or(std::time::Duration::from_millis(250));
    match ir {
        FadeIntentIr::Out { secs } => FadeRequest { intent: FadeIntent::Out, dur: dur(*secs) },
        FadeIntentIr::In { secs } => FadeRequest { intent: FadeIntent::In, dur: dur(*secs) },
        FadeIntentIr::To { target_db, vol, secs } => FadeRequest {
            intent: FadeIntent::To { target_db: *target_db, vol: *vol },
            dur: dur(*secs),
        },
        // Wind-down to the configured floor, playback continuing. The floor is
        // resolved from the LIVE config inside `start_fade_spec` (never baked here).
        FadeIntentIr::ToFloor { secs } => FadeRequest {
            intent: FadeIntent::ToFloor,
            dur: dur(*secs),
        },
        FadeIntentIr::WakeTo { target_db, vol, secs } => FadeRequest {
            intent: FadeIntent::WakeTo { target_db: *target_db, vol: *vol },
            dur: dur(*secs),
        },
    }
}

/// Execute one action against the existing primitives. Log-and-continue on error.
async fn run_action(handler: Arc<HypodjHandler>, id: PlanId, action: Action) {
    let r: Result<(), String> = match &action {
        // Fade precedence: start_fade_spec IS the single FadeSlot. It validates
        // before aborting and logs an autonomous takeover; an in-flight fade
        // continues across a TrackStart (the executor never cancels/re-fires it).
        Action::Fade(ir) => handler
            .start_fade_spec(map_fade(ir))
            .await
            .map_err(|e| e.to_string()),
        Action::Stop => {
            handler.handle(MpdCommand::Stop).await;
            Ok(())
        }
        Action::Pause => {
            handler.handle(MpdCommand::Pause(Some(true))).await;
            Ok(())
        }
        Action::SetVolume(v) => {
            handler.handle(MpdCommand::SetVol(*v)).await;
            Ok(())
        }
        Action::Enqueue { selector, count } => {
            handler.plan_enqueue(selector, *count).await.map(|_| ())
        }
        // ONE atomic effect: enqueue? -> start-from-silence -> play -> WakeTo ramp.
        // A single Action (not three timers) is what guarantees this order.
        Action::Wake { selector, count } => {
            handler.wake_now(selector.clone(), *count).await
        }
    };
    if let Err(e) = r {
        tracing::error!(plan = id.0, error = %e, "plan action failed (log-and-continue)");
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::clock::TokioClock;
    use crate::config::ServerConfig;
    use crate::event::{DjEvent, DjEventKind, QueueSnapshot, TrackRef};
    use crate::model::{AlbumId, Song, SongId};
    use crate::plan::{Action, FadeIntentIr, PosBase, RawPlan, RawTrigger, TrackSel};
    use crate::player::NullPlayer;
    use crate::subsonic::SubsonicClient;
    use crate::timer::spawn_timer_source;
    use std::time::Duration;
    use tokio::sync::mpsc;
    use tokio::time::Instant;

    fn song(id: &str, dur: Option<u32>, album: Option<&str>) -> Song {
        Song {
            id: SongId(id.into()),
            title: format!("t-{id}"),
            album: album.map(|a| a.to_string()),
            album_id: album.map(|a| AlbumId(format!("al-{a}"))),
            artist: None,
            track: None,
            duration_secs: dur,
            cover_art: None,
            starred: false,
            musicbrainz_id: None,
            disc: None,
            year: None,
            genre: None,
            bitrate: None,
            comment: None,
            user_rating: None,
            composer: None,
            performer: None,
        }
    }

    fn maybe_client() -> Option<Arc<SubsonicClient>> {
        let cfg = ServerConfig {
            url: "http://127.0.0.1:1/never".to_string(),
            username: "u".into(),
            password: "p".into(),
            client_name: "test".into(),
        };
        match std::panic::catch_unwind(|| SubsonicClient::connect(&cfg)) {
            Ok(Ok(c)) => Some(Arc::new(c)),
            _ => None,
        }
    }

    /// A test rig: a handler with `n` songs queued and index 0 playing, an executor
    /// spawned over test channels (a plain trigger mpsc + a Tick broadcast + the
    /// Immediate nudge), and a manual bridge from the timer-fire channel to a
    /// `WallClock` DjEvent on the trigger stream (what the director does live).
    struct Rig {
        handler: Arc<HypodjHandler>,
        trig_tx: mpsc::UnboundedSender<DjEvent>,
        tick_tx: broadcast::Sender<DjEvent>,
        version: u64,
    }

    /// A SubsonicClient pointed at a local listener that accepts a connection and
    /// NEVER responds - so an `Enqueue` action's network call HANGS forever (not a
    /// fast connection-refused). Used to prove a slow/hanging action cannot wedge
    /// the recv loop. Returns the client + the owning listener (kept alive).
    fn hanging_client() -> Option<(Arc<SubsonicClient>, std::net::TcpListener)> {
        let listener = std::net::TcpListener::bind("127.0.0.1:0").ok()?;
        let port = listener.local_addr().ok()?.port();
        let accept = listener.try_clone().ok()?;
        std::thread::spawn(move || {
            for stream in accept.incoming() {
                // Accept and hold the socket open, never writing a response.
                if let Ok(s) = stream {
                    std::mem::forget(s);
                }
            }
        });
        let cfg = ServerConfig {
            url: format!("http://127.0.0.1:{port}/"),
            username: "u".into(),
            password: "p".into(),
            client_name: "test".into(),
        };
        match std::panic::catch_unwind(|| SubsonicClient::connect(&cfg)) {
            Ok(Ok(c)) => Some((Arc::new(c), listener)),
            _ => None,
        }
    }

    impl Rig {
        async fn new(durs: &[(&str, Option<u32>, Option<&str>)]) -> Option<Rig> {
            Self::new_with_client(maybe_client()?, durs).await
        }

        async fn new_with_client(
            client: Arc<SubsonicClient>,
            durs: &[(&str, Option<u32>, Option<&str>)],
        ) -> Option<Rig> {
            let (player, _events) = NullPlayer::spawn();
            let handler = Arc::new(HypodjHandler::new(client.clone(), player.clone()));
            for (id, dur, album) in durs {
                handler.enqueue_song_for_test(song(id, *dur, *album)).await;
            }
            handler.play_for_test(0).await;

            let (trig_tx, trig_rx) = mpsc::unbounded_channel::<DjEvent>();
            let (tick_tx, tick_rx) = broadcast::channel::<DjEvent>(64);
            let (imm_tx, imm_rx) = mpsc::unbounded_channel::<PlanId>();
            handler.set_plan_immediate_sink(imm_tx);

            // Timer source over the shared clock; bridge its fires into WallClock
            // DjEvents on the trigger stream (the director's live behavior).
            let (fire_tx, mut fire_rx) = mpsc::unbounded_channel();
            let timers = spawn_timer_source(TokioClock, fire_tx);
            handler.set_plan_timers(timers.clone());
            let bridge_tx = trig_tx.clone();
            let bridge_handler = handler.clone();
            tokio::spawn(async move {
                while let Some(id) = fire_rx.recv().await {
                    let snap = bridge_handler.queue_snapshot();
                    let _ = bridge_tx.send(DjEvent {
                        kind: DjEventKind::WallClock(id),
                        now: Instant::now(),
                        seq: 0,
                        playlist_version: snap.playlist_version,
                        cursor: snap.current.as_ref().map(|c| c.queue_id),
                    });
                }
            });

            Executor::spawn(handler.clone(), timers, TokioClock, trig_rx, tick_rx, imm_rx);
            let version = handler.queue_snapshot().playlist_version;
            Some(Rig { handler, trig_tx, tick_tx, version })
        }

        fn snap(&self) -> QueueSnapshot {
            self.handler.queue_snapshot()
        }

        fn event(&self, kind: DjEventKind) -> DjEvent {
            let snap = self.snap();
            DjEvent {
                kind,
                now: Instant::now(),
                seq: 0,
                playlist_version: snap.playlist_version,
                cursor: snap.current.as_ref().map(|c| c.queue_id),
            }
        }

        fn push(&self, kind: DjEventKind) {
            let _ = self.trig_tx.send(self.event(kind));
        }
    }

    fn tref(id: u64) -> TrackRef {
        TrackRef {
            queue_id: QueueId(id),
            queue_index: None,
            song: None,
            album_id: None,
            duration: None,
        }
    }

    async fn settle() {
        for _ in 0..8 {
            tokio::task::yield_now().await;
        }
    }

    // PR11 HAPPY FIXTURE: "plan add trigger track 3 base current action fade out
    // 30s" -> OnTrackStart(3rd entry) -> fires on the scripted TrackStart ->
    // start_fade_spec runs. All under scripted events, no model, no wall time.
    #[tokio::test(start_paused = true)]
    async fn fixture_track3_fade_out_on_track_start() {
        let Some(rig) = Rig::new(&[
            ("s0", Some(200), Some("A")),
            ("s1", Some(200), Some("A")),
            ("s2", Some(200), Some("A")),
            ("s3", Some(200), Some("A")),
            ("s4", Some(200), Some("A")),
        ])
        .await
        else {
            eprintln!("skip: no CA certs");
            return;
        };

        // cursor is index 0 (QueueId 0). n=3 counting current as 1st -> index 2 ->
        // QueueId(2). RawPlan is the worked example.
        let raw = RawPlan {
            version: 1,
            trigger: RawTrigger::QueuePosition { n: 3, base: PosBase::CurrentIsOne },
            action: Action::Fade(FadeIntentIr::Out { secs: 30.0 }),
            once: true,
            origin: "mpd".into(),
        };
        let id = rig.handler.plan_add(raw).expect("valid plan arms");
        assert_eq!(rig.handler.plan_list().len(), 1);
        assert!(!rig.handler.fade_active_for_test().await, "no fade before the edge");

        // Scripted TrackStart for QueueId(2): the resolved OnTrackStart edge.
        rig.push(DjEventKind::TrackStart(tref(2)));
        settle().await;

        assert!(rig.handler.fade_active_for_test().await, "fade started on the resolved edge");
        // once:true was removed at selection.
        assert!(rig.handler.plan_list().is_empty(), "once plan removed after firing");
        let _ = id;
    }

    // PR11 NEGATIVE: delete the target before its TrackStart -> the next edge bumps
    // playlist_version -> reconciliation drops it LOUDLY, no neighbor fade.
    #[tokio::test(start_paused = true)]
    async fn negative_delete_target_expires_no_neighbor_fire() {
        let Some(rig) = Rig::new(&[
            ("s0", Some(200), Some("A")),
            ("s1", Some(200), Some("A")),
            ("s2", Some(200), Some("A")),
        ])
        .await
        else {
            eprintln!("skip: no CA certs");
            return;
        };
        let raw = RawPlan {
            version: 1,
            trigger: RawTrigger::QueuePosition { n: 3, base: PosBase::CurrentIsOne },
            action: Action::Fade(FadeIntentIr::Out { secs: 30.0 }),
            once: true,
            origin: "mpd".into(),
        };
        rig.handler.plan_add(raw).unwrap(); // OnTrackStart(QueueId(2))
        assert_eq!(rig.handler.plan_list().len(), 1);

        // Delete QueueId(2) (index 2). This bumps playlist_version but emits no
        // event; the next edge carries the new version and triggers reconcile.
        rig.handler.delete_for_test(2);
        assert!(rig.handler.snapshot_by_queue_id(QueueId(2)).is_none());

        // A benign edge (TrackStart of the current) bumps the version through the
        // executor -> reconcile expires the orphaned plan.
        rig.push(DjEventKind::TrackStart(tref(0)));
        settle().await;
        assert!(rig.handler.plan_list().is_empty(), "orphaned plan expired on reconcile");
        assert!(!rig.handler.fade_active_for_test().await, "never faded a neighbor");
        let _ = rig.version;
    }

    // SpanElapsed: arm, advance virtual time, assert the fade ran exactly once.
    #[tokio::test(start_paused = true)]
    async fn span_elapsed_fires_after_advance() {
        let Some(rig) = Rig::new(&[("s0", Some(200), Some("A"))]).await else {
            eprintln!("skip: no CA certs");
            return;
        };
        let raw = RawPlan {
            version: 1,
            trigger: RawTrigger::SpanElapsed { secs: 300.0 },
            action: Action::Fade(FadeIntentIr::Out { secs: 30.0 }),
            once: true,
            origin: "mpd".into(),
        };
        rig.handler.plan_add(raw).unwrap();
        assert!(!rig.handler.fade_active_for_test().await);
        // Not yet at 299s.
        tokio::time::advance(Duration::from_secs(299)).await;
        settle().await;
        assert!(!rig.handler.fade_active_for_test().await, "not before the deadline");
        // Cross the deadline: the timer fires -> WallClock edge -> fade.
        tokio::time::advance(Duration::from_secs(2)).await;
        settle().await;
        assert!(rig.handler.fade_active_for_test().await, "fired after the span elapsed");
        assert!(rig.handler.plan_list().is_empty(), "once plan removed");
    }

    // cancel before the deadline -> guard Drop disarms, no fire.
    #[tokio::test(start_paused = true)]
    async fn cancel_before_deadline_no_fire() {
        let Some(rig) = Rig::new(&[("s0", Some(200), Some("A"))]).await else {
            eprintln!("skip: no CA certs");
            return;
        };
        let id = rig
            .handler
            .plan_add(RawPlan {
                version: 1,
                trigger: RawTrigger::SpanElapsed { secs: 300.0 },
                action: Action::Fade(FadeIntentIr::Out { secs: 30.0 }),
                once: true,
                origin: "mpd".into(),
            })
            .unwrap();
        assert!(rig.handler.plan_cancel(id), "cancel drops the plan + its guard");
        tokio::time::advance(Duration::from_secs(400)).await;
        settle().await;
        assert!(!rig.handler.fade_active_for_test().await, "no phantom fire after cancel");
    }

    // deterministic order: two plans (Fade then Stop) on one TrackStart execute in
    // ascending PlanId; Stop (higher id) lands last, so the end state is stopped.
    #[tokio::test(start_paused = true)]
    async fn deterministic_order_fade_then_stop() {
        let Some(rig) = Rig::new(&[("s0", Some(200), Some("A")), ("s1", Some(200), Some("A"))]).await
        else {
            eprintln!("skip: no CA certs");
            return;
        };
        // Both target QueueId(1) TrackStart. Fade added first (lower id), Stop next.
        rig.handler
            .plan_add(RawPlan {
                version: 1,
                trigger: RawTrigger::QueuePosition { n: 2, base: PosBase::CurrentIsOne },
                action: Action::Fade(FadeIntentIr::Out { secs: 30.0 }),
                once: true,
                origin: "mpd".into(),
            })
            .unwrap();
        rig.handler
            .plan_add(RawPlan {
                version: 1,
                trigger: RawTrigger::QueuePosition { n: 2, base: PosBase::CurrentIsOne },
                action: Action::Stop,
                once: true,
                origin: "mpd".into(),
            })
            .unwrap();

        rig.push(DjEventKind::TrackStart(tref(1)));
        settle().await;
        // Stop ran last (ascending id), cancelling the fade: stable end state.
        assert!(!rig.handler.fade_active_for_test().await, "Stop (higher id) applied last");
        assert!(rig.handler.plan_list().is_empty());
    }

    // crash isolation: a plan whose execute ERRORS (Similar selector, no
    // embeddings) does not stop a second plan on the same edge from firing.
    #[tokio::test(start_paused = true)]
    async fn erroring_plan_does_not_block_sibling() {
        let Some(rig) = Rig::new(&[("s0", Some(200), Some("A")), ("s1", Some(200), Some("A"))]).await
        else {
            eprintln!("skip: no CA certs");
            return;
        };
        // Lower id: an Enqueue{Similar} that errors at execute (not-yet).
        rig.handler
            .plan_add(RawPlan {
                version: 1,
                trigger: RawTrigger::QueuePosition { n: 2, base: PosBase::CurrentIsOne },
                action: Action::Enqueue {
                    selector: crate::plan::Selector::Similar(SongId("x".into())),
                    count: 1,
                },
                once: true,
                origin: "mpd".into(),
            })
            .unwrap();
        // Higher id: a Fade that must still fire.
        rig.handler
            .plan_add(RawPlan {
                version: 1,
                trigger: RawTrigger::QueuePosition { n: 2, base: PosBase::CurrentIsOne },
                action: Action::Fade(FadeIntentIr::Out { secs: 30.0 }),
                once: true,
                origin: "mpd".into(),
            })
            .unwrap();

        rig.push(DjEventKind::TrackStart(tref(1)));
        settle().await;
        assert!(rig.handler.fade_active_for_test().await, "sibling fade fired despite the error");
    }

    // TimeRemaining: armed lazily on a live Tick for the current track; paused ->
    // guard dropped (no fire while paused); resume Tick re-arms; advancing past the
    // deadline fires exactly once. Exercises the Tick broadcast path.
    #[tokio::test(start_paused = true)]
    async fn time_remaining_paused_then_resume_fires() {
        let Some(rig) = Rig::new(&[("s0", Some(200), Some("A"))]).await else {
            eprintln!("skip: no CA certs");
            return;
        };
        // Fire when 30s remain on the current track (duration 200s).
        rig.handler
            .plan_add(RawPlan {
                version: 1,
                trigger: RawTrigger::TimeRemaining { track: TrackSel::Current, secs: 30.0 },
                action: Action::Fade(FadeIntentIr::Out { secs: 10.0 }),
                once: true,
                origin: "mpd".into(),
            })
            .unwrap();

        // A Tick at pos 100 (100s remaining) arms the deadline at now + (100 - 30).
        let tick = |rem: u64| DjEventKind::Tick {
            time_pos: Duration::from_secs(200 - rem),
            time_remaining: Some(Duration::from_secs(rem)),
        };
        let _ = rig.tick_tx.send(rig.event(tick(100)));
        settle().await;

        // Pause BEFORE the deadline: the guard is dropped (mirrored edge on ticks).
        let _ = rig.tick_tx.send(rig.event(DjEventKind::StateChanged(PlayState::Paused, None)));
        settle().await;
        // Advance well past the original deadline: no fire while paused.
        tokio::time::advance(Duration::from_secs(120)).await;
        settle().await;
        assert!(!rig.handler.fade_active_for_test().await, "no fire while paused");

        // Resume with a fresh Tick (still 100s remaining): re-arm, then cross it.
        let _ = rig.tick_tx.send(rig.event(tick(100)));
        settle().await;
        tokio::time::advance(Duration::from_secs(80)).await;
        settle().await;
        assert!(rig.handler.fade_active_for_test().await, "fired after resume + re-arm");
    }

    // double-fire guard: once:true, inject the resolved edge TWICE; the plan is
    // removed at selection so it fires exactly once (the second edge is a no-op).
    #[tokio::test(start_paused = true)]
    async fn once_plan_fires_exactly_once() {
        let Some(rig) = Rig::new(&[("s0", Some(200), Some("A")), ("s1", Some(200), Some("A"))]).await
        else {
            eprintln!("skip: no CA certs");
            return;
        };
        rig.handler
            .plan_add(RawPlan {
                version: 1,
                trigger: RawTrigger::QueuePosition { n: 2, base: PosBase::CurrentIsOne },
                action: Action::Fade(FadeIntentIr::Out { secs: 30.0 }),
                once: true,
                origin: "mpd".into(),
            })
            .unwrap();
        rig.push(DjEventKind::TrackStart(tref(1)));
        settle().await;
        assert!(rig.handler.plan_list().is_empty(), "removed at selection");
        // A second identical edge finds no plan: nothing to fire.
        rig.push(DjEventKind::TrackStart(tref(1)));
        settle().await;
        assert!(rig.handler.plan_list().is_empty());
    }

    // F2: a SKIP after arming TimeRemaining must NOT fire on the wrong track. Arm on
    // the current track, arm its deadline via a Tick, then skip to another track:
    // the timer is disarmed (and fires() would be Stale anyway), so crossing the old
    // deadline never fades the now-playing track.
    #[tokio::test(start_paused = true)]
    async fn time_remaining_skip_does_not_fire_wrong_track() {
        let Some(rig) = Rig::new(&[("s0", Some(200), Some("A")), ("s1", Some(200), Some("A"))]).await
        else {
            eprintln!("skip: no CA certs");
            return;
        };
        rig.handler
            .plan_add(RawPlan {
                version: 1,
                trigger: RawTrigger::TimeRemaining { track: TrackSel::Current, secs: 30.0 },
                action: Action::Fade(FadeIntentIr::Out { secs: 10.0 }),
                once: true,
                origin: "mpd".into(),
            })
            .unwrap();
        let tick = |rem: u64| DjEventKind::Tick {
            time_pos: Duration::from_secs(200 - rem),
            time_remaining: Some(Duration::from_secs(rem)),
        };
        // Arm the deadline at now + (100 - 30) = now + 70 on the current track.
        let _ = rig.tick_tx.send(rig.event(tick(100)));
        settle().await;

        // Skip to track 1: current is now QueueId(1); an edge disarms the plan.
        rig.handler.play_for_test(1).await;
        rig.push(DjEventKind::TrackStart(tref(1)));
        settle().await;

        // Cross the OLD deadline (and far beyond): the plan must never fire.
        tokio::time::advance(Duration::from_secs(300)).await;
        settle().await;
        assert!(
            !rig.handler.fade_active_for_test().await,
            "skip disarmed the plan; never fired on the wrong track"
        );
    }

    // F2: a SEEK re-arms the deadline. Arm at 100s remaining (deadline now+70), then
    // a fresh Tick shows a JUMP back to 190s remaining (a seek) -> re-arm to now+160.
    // Crossing the OLD deadline no longer fires (it was cancelled); crossing the NEW
    // one does.
    #[tokio::test(start_paused = true)]
    async fn time_remaining_seek_rearms_deadline() {
        let Some(rig) = Rig::new(&[("s0", Some(200), Some("A"))]).await else {
            eprintln!("skip: no CA certs");
            return;
        };
        rig.handler
            .plan_add(RawPlan {
                version: 1,
                trigger: RawTrigger::TimeRemaining { track: TrackSel::Current, secs: 30.0 },
                action: Action::Fade(FadeIntentIr::Out { secs: 10.0 }),
                once: true,
                origin: "mpd".into(),
            })
            .unwrap();
        let tick = |rem: u64| DjEventKind::Tick {
            time_pos: Duration::from_secs(200 - rem),
            time_remaining: Some(Duration::from_secs(rem)),
        };
        // Arm: deadline now + 70.
        let _ = rig.tick_tx.send(rig.event(tick(100)));
        settle().await;
        // Seek back: remaining jumps to 190 -> re-arm to now + 160 (old cancelled).
        let _ = rig.tick_tx.send(rig.event(tick(190)));
        settle().await;

        // Cross the OLD deadline (70) but not the new (160): no fire.
        tokio::time::advance(Duration::from_secs(80)).await;
        settle().await;
        assert!(
            !rig.handler.fade_active_for_test().await,
            "old deadline was superseded by the re-arm"
        );
        // Cross the NEW deadline: fires.
        tokio::time::advance(Duration::from_secs(100)).await;
        settle().await;
        assert!(rig.handler.fade_active_for_test().await, "fired at the re-armed deadline");
    }

    // F1: a slow/hanging Enqueue action must NOT block a concurrent deadline plan
    // from firing. The enqueue fires on a TrackStart and hangs forever on a network
    // read; a separate SpanElapsed deadline plan must still fade when its timer fires.
    #[tokio::test(start_paused = true)]
    async fn hanging_enqueue_does_not_block_deadline_plan() {
        let Some((client, _listener)) = hanging_client() else {
            eprintln!("skip: no CA certs / no local socket");
            return;
        };
        let Some(rig) = Rig::new_with_client(
            client,
            &[("s0", Some(200), Some("A")), ("s1", Some(200), Some("A"))],
        )
        .await
        else {
            eprintln!("skip: no CA certs");
            return;
        };
        // A hanging enqueue on QueueId(1) TrackStart.
        rig.handler
            .plan_add(RawPlan {
                version: 1,
                trigger: RawTrigger::QueuePosition { n: 2, base: PosBase::CurrentIsOne },
                action: Action::Enqueue { selector: crate::plan::Selector::Query("x".into()), count: 1 },
                once: true,
                origin: "mpd".into(),
            })
            .unwrap();
        // A deadline fade plan (separate event path via the timer).
        rig.handler
            .plan_add(RawPlan {
                version: 1,
                trigger: RawTrigger::SpanElapsed { secs: 300.0 },
                action: Action::Fade(FadeIntentIr::Out { secs: 10.0 }),
                once: true,
                origin: "mpd".into(),
            })
            .unwrap();

        // Fire the enqueue: it hangs in its detached batch task, off the recv loop.
        rig.push(DjEventKind::TrackStart(tref(1)));
        settle().await;
        assert!(!rig.handler.fade_active_for_test().await);

        // The recv loop keeps draining: advance past the deadline -> its WallClock
        // edge is processed and the fade starts, despite the enqueue still hanging.
        tokio::time::advance(Duration::from_secs(301)).await;
        settle().await;
        assert!(
            rig.handler.fade_active_for_test().await,
            "deadline plan fired while the enqueue hangs; loop not wedged"
        );
    }

    // F3: a NON-once Immediate plan is executed at add-time and REMOVED regardless of
    // `once` - no un-fireable zombie lingers in the plan list.
    #[tokio::test(start_paused = true)]
    async fn non_once_immediate_removed_no_zombie() {
        let Some(rig) = Rig::new(&[("s0", Some(200), Some("A"))]).await else {
            eprintln!("skip: no CA certs");
            return;
        };
        rig.handler
            .plan_add(RawPlan {
                version: 1,
                trigger: RawTrigger::Immediate,
                action: Action::Fade(FadeIntentIr::Out { secs: 10.0 }),
                once: false, // NOT once: the old code would leave a zombie.
                origin: "mpd".into(),
            })
            .unwrap();
        settle().await;
        assert!(rig.handler.fade_active_for_test().await, "immediate action ran at add-time");
        assert!(
            rig.handler.plan_list().is_empty(),
            "non-once immediate plan removed; no zombie in the list"
        );
    }

    // F4: an AlbumBoundary plan fires across the EOF advance. The advance has already
    // repointed current PAST `last` when TrackEnd(last) is processed; firing off the
    // event identity (not a live-cursor re-read) means the boundary is never lost.
    #[tokio::test(start_paused = true)]
    async fn album_boundary_fires_across_eof_advance() {
        // Track 0 album A, track 1 album B: a real album boundary after track 0.
        let Some(rig) = Rig::new(&[("s0", Some(200), Some("A")), ("s1", Some(200), Some("B"))]).await
        else {
            eprintln!("skip: no CA certs");
            return;
        };
        rig.handler
            .plan_add(RawPlan {
                version: 1,
                trigger: RawTrigger::AlbumBoundary { track: TrackSel::Current },
                action: Action::Fade(FadeIntentIr::Out { secs: 10.0 }),
                once: true,
                origin: "mpd".into(),
            })
            .unwrap(); // OnAlbumBoundary { last: QueueId(0), album: al-A }

        // The EOF advance already moved current to track 1 (QueueId 1) BEFORE the
        // TrackEnd(last=0) edge is processed - the race the fix defends against.
        rig.handler.play_for_test(1).await;
        rig.push(DjEventKind::TrackEnd(tref(0)));
        settle().await;

        assert!(
            rig.handler.fade_active_for_test().await,
            "album boundary fired across the advance; not lost to the cursor move"
        );
        assert!(rig.handler.plan_list().is_empty(), "fired boundary removed");
    }

    // ── convenience features: sleep / winddown / wake (end-to-end) ────────────

    // SLEEP: `sleep 30m` builds ONE plan (origin sleep); no fire at 1799s; fires at
    // 1801s -> the sub-JND sleep fade-out; once plan removed. The WallClock horizon
    // (1800s > max_dur 1800 boundary) is NOT capped by the fade cap.
    #[tokio::test(start_paused = true)]
    async fn sleep_timer_fires_after_horizon_single_plan() {
        let Some(rig) = Rig::new(&[("s0", Some(200), Some("A"))]).await else {
            eprintln!("skip: no CA certs");
            return;
        };
        rig.handler.sleep_set(Duration::from_secs(1800)).unwrap();
        assert_eq!(rig.handler.plan_list().len(), 1, "exactly one sleep plan");
        tokio::time::advance(Duration::from_secs(1799)).await;
        settle().await;
        assert!(!rig.handler.fade_active_for_test().await, "no fade before the horizon");
        tokio::time::advance(Duration::from_secs(2)).await;
        settle().await;
        assert!(rig.handler.fade_active_for_test().await, "sleep fade fired after the horizon");
        assert!(rig.handler.plan_list().is_empty(), "once sleep plan removed");
    }

    // SLEEP: sleep_remaining counts down from the resolved deadline under the fake
    // clock (arm 1800s, advance 600s, remaining ~1200s).
    #[tokio::test(start_paused = true)]
    async fn sleep_remaining_counts_down() {
        let Some(rig) = Rig::new(&[("s0", Some(200), Some("A"))]).await else {
            eprintln!("skip: no CA certs");
            return;
        };
        rig.handler.sleep_set(Duration::from_secs(1800)).unwrap();
        tokio::time::advance(Duration::from_secs(600)).await;
        settle().await;
        let rem = rig.handler.sleep_remaining().expect("a sleep plan is armed");
        assert!(
            rem >= Duration::from_secs(1198) && rem <= Duration::from_secs(1201),
            "remaining ~1200s, got {rem:?}"
        );
    }

    // SLEEP: `sleep off` cancels (no fire); `sleep 30m` then `sleep 45m` replaces
    // in place (single instance, only the 45m fires - no double-stop plan).
    #[tokio::test(start_paused = true)]
    async fn sleep_off_and_replace_single_instance() {
        let Some(rig) = Rig::new(&[("s0", Some(200), Some("A"))]).await else {
            eprintln!("skip: no CA certs");
            return;
        };
        rig.handler.sleep_set(Duration::from_secs(1800)).unwrap();
        assert!(rig.handler.sleep_cancel(), "sleep off cancels");
        tokio::time::advance(Duration::from_secs(2000)).await;
        settle().await;
        assert!(!rig.handler.fade_active_for_test().await, "no fire after cancel");

        // Re-arm 30m then replace with 45m: exactly one sleep plan, only 45m fires.
        rig.handler.sleep_set(Duration::from_secs(1800)).unwrap();
        rig.handler.sleep_set(Duration::from_secs(2700)).unwrap();
        assert_eq!(rig.handler.plan_list().len(), 1, "single sleep instance after replace");
        tokio::time::advance(Duration::from_secs(1801)).await;
        settle().await;
        assert!(!rig.handler.fade_active_for_test().await, "the old 30m plan was cancelled");
        tokio::time::advance(Duration::from_secs(901)).await; // total ~2702s
        settle().await;
        assert!(rig.handler.fade_active_for_test().await, "the 45m plan fires");
    }

    // WINDDOWN: `winddown` (immediate) installs a ToFloor fade at add-time and is
    // removed (no zombie). `winddown 20m` fires only after its WallClock horizon.
    #[tokio::test(start_paused = true)]
    async fn winddown_immediate_and_scheduled() {
        let Some(rig) = Rig::new(&[("s0", Some(200), Some("A"))]).await else {
            eprintln!("skip: no CA certs");
            return;
        };
        rig.handler.winddown_set(None).unwrap();
        settle().await;
        assert!(rig.handler.fade_active_for_test().await, "immediate winddown fade installed");
        assert!(rig.handler.plan_list().is_empty(), "immediate winddown removed, no zombie");

        // Scheduled winddown fires only past the horizon.
        rig.handler.winddown_set(Some(Duration::from_secs(1200))).unwrap();
        assert_eq!(rig.handler.plan_list().len(), 1, "one scheduled winddown");
        tokio::time::advance(Duration::from_secs(1201)).await;
        settle().await;
        assert!(rig.handler.plan_list().is_empty(), "scheduled winddown fired + removed");
    }

    // WAKE (no selector): `wake in 2h` fires only past 2h. At the deadline wake_now
    // starts from silence (live gain near the synth floor) and installs the sub-JND
    // WakeTo ramp; the once plan is removed.
    #[tokio::test(start_paused = true)]
    async fn wake_in_ramps_from_silence() {
        use crate::player::SYNTH_FLOOR_DB;
        let Some(rig) = Rig::new(&[("s0", Some(200), Some("A"))]).await else {
            eprintln!("skip: no CA certs");
            return;
        };
        let at = chrono::Utc::now() + chrono::Duration::seconds(7200); // 2h
        rig.handler.wake_set(at, None, 0).unwrap();
        assert_eq!(rig.handler.plan_list().len(), 1);
        // Not at 30min (proves the horizon is not capped to max_dur = 30min).
        tokio::time::advance(Duration::from_secs(1800)).await;
        settle().await;
        assert!(!rig.handler.fade_active_for_test().await, "wake in 2h must not fire at 30min");
        // Cross 2h.
        tokio::time::advance(Duration::from_secs(5401)).await;
        settle().await;
        assert!(rig.handler.fade_active_for_test().await, "wake ramp installed at the deadline");
        assert!(
            rig.handler.live_gain_db_for_test() <= SYNTH_FLOOR_DB + 5.0,
            "wake ramp starts near silence"
        );
        assert!(rig.handler.plan_list().is_empty(), "once wake plan removed");
    }

    // WAKE: an enqueue failure ABORTS the ramp - never ramp silence over an empty
    // queue. Selector::Calmer must resolve its seed via client.song(id); against
    // the rig's non-live client that fetch errors, so plan_enqueue returns Err and
    // wake_now aborts before any ramp (the graceful genre/random fallback only runs
    // once the seed is known, so a seed-fetch transport failure still fails loud).
    #[tokio::test(start_paused = true)]
    async fn wake_enqueue_failure_aborts_ramp() {
        let Some(rig) = Rig::new(&[("s0", Some(200), Some("A"))]).await else {
            eprintln!("skip: no CA certs");
            return;
        };
        let at = chrono::Utc::now() + chrono::Duration::seconds(60);
        rig.handler
            .wake_set(at, Some(crate::plan::Selector::Calmer(SongId("x".into()))), 5)
            .unwrap();
        tokio::time::advance(Duration::from_secs(61)).await;
        settle().await;
        assert!(
            !rig.handler.fade_active_for_test().await,
            "no ramp installed when the enqueue fails"
        );
    }

    // WAKE control: wake_remaining reports the countdown; wake_cancel disarms (no
    // fire after advancing past the deadline).
    #[tokio::test(start_paused = true)]
    async fn wake_remaining_and_cancel() {
        let Some(rig) = Rig::new(&[("s0", Some(200), Some("A"))]).await else {
            eprintln!("skip: no CA certs");
            return;
        };
        let at = chrono::Utc::now() + chrono::Duration::seconds(1800);
        rig.handler.wake_set(at, None, 0).unwrap();
        let rem = rig.handler.wake_remaining().expect("a wake is armed");
        assert!(rem >= Duration::from_secs(1798) && rem <= Duration::from_secs(1801), "got {rem:?}");
        assert!(rig.handler.wake_cancel(), "wake cancel disarms");
        tokio::time::advance(Duration::from_secs(2000)).await;
        settle().await;
        assert!(!rig.handler.fade_active_for_test().await, "no fire after wake cancel");
    }
}
