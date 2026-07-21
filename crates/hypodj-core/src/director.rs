//! P1 director: the LOSSLESS event spine + the three-class fan-out.
//!
//! FOUNDATION (P1), built to LAST. This is the single consumer of the player's
//! `mpsc<PlayerEvent>` - it runs `scrobbler.on_event` + `advance_on_eof` INLINE
//! (the network-touching, must-not-drop work), exactly preserving today's
//! main.rs semantics, and re-publishes a strictly-downstream [`DjEvent`] stream
//! that can NEVER gate the spine.
//!
//! ## Three traffic classes
//!
//!   1. LOSSLESS spine - the `while let Some(ev) = player_events.recv()` loop
//!      below. Scrobble + advance are on it; an `Eof` never travels a lossy
//!      channel.
//!   2. LOSSLESS edge fan-out - `TrackStart`/`TrackEnd`/`StateChanged`/`WallClock`
//!      go to every [`DjRuntime::subscribe_triggers`] subscriber on a per-sub
//!      UNBOUNDED `mpsc`. Unbounded is memory-safe (edges are per-boundary,
//!      low-rate) AND never back-pressures the spine (a bounded channel would
//!      wedge advance if the executor stalled; a lossy one would drop the very
//!      trigger P2 exists for).
//!   3. LOSSY observation - high-rate `Tick` (plus a mirror of every edge) on a
//!      `broadcast(512)`, and a `watch<QueueSnapshot>` as the level-triggered
//!      resync source. On broadcast `Lagged`, an observer re-reads the watch
//!      snapshot and re-derives; it NEVER resubscribes and treats `Closed` as
//!      terminal.
//!
//! ## EOF-before-advance ordering
//!
//! On `Eof` the spine (a) builds the finishing [`TrackRef`] from the latched
//! queue id BEFORE advance repoints the current pointer, (b) publishes
//! `TrackEnd(finishing)`, (c) THEN calls `advance_on_eof().await` (wrapped in a
//! timeout so a hung `stream_url` cannot pin the spine). The next track's
//! `StateChanged(Playing)` surfaces as `TrackStart` AFTER, so observers always
//! see End-then-Start.

use std::sync::{Arc, Mutex as StdMutex};
use std::time::Duration;

use tokio::sync::{broadcast, mpsc, watch};
use tokio::task::JoinHandle;

use crate::clock::Clock;
use crate::event::{DjEvent, DjEventKind, QueueId, QueueSnapshot, TrackRef};
use crate::handler::HypodjHandler;
use crate::model::SongId;
use crate::player::{PlayState, PlayerEvent};
use crate::scrobble::Scrobbler;
use crate::subsonic::SubsonicClient;
use crate::timer::{spawn_timer_source, TimerHandle};
use crate::viz::{VizFrame, VIZ_BROADCAST_CAP};

/// Capacity of the lossy observation broadcast. Sized so a briefly-stalled
/// observer of high-rate `Tick`s recovers via the watch snapshot rather than
/// forcing a resubscribe.
const BROADCAST_CAP: usize = 512;

/// How long the spine waits for `advance_on_eof` before giving up. A hung
/// `stream_url` (network) must never pin the spine forever; on timeout the queue
/// simply does not advance and the spine keeps draining.
const ADVANCE_TIMEOUT: Duration = Duration::from_secs(20);

/// The registry of per-subscriber lossless edge-trigger senders. Shared between
/// the spine (fan-out) and [`DjRuntime`] (registration). Closed senders are
/// pruned lazily on publish.
type Triggers = Arc<StdMutex<Vec<mpsc::UnboundedSender<DjEvent>>>>;

/// The named handle the daemon holds and P2 builds on. A struct (not a tuple) so
/// adding a field never breaks the executor wiring.
pub struct DjRuntime {
    /// Lossy observers (UI/metrics): `subscribe()` for a fresh receiver.
    pub events: broadcast::Sender<DjEvent>,
    /// Level-triggered resync source, seeded at construction with a well-formed
    /// snapshot so an immediately-lagging subscriber always resyncs from valid
    /// data (never an `Option::None` sentinel).
    pub snapshot: watch::Receiver<QueueSnapshot>,
    /// Wall-clock timers (shared Clock base). Held for P2 even while unused.
    pub timers: TimerHandle,
    /// The handler, so P2 can call `queue_snapshot()` on resync.
    pub handler: Arc<HypodjHandler>,
    /// The DEDICATED cosmetic-viz broadcast (post-gain audio levels, ~20 fps). Kept
    /// OFF the shared `events` broadcast so its high-rate churn never raises `Lagged`
    /// for the lossy observers. The daemon's viz socket (`viz::serve_viz`) subscribes
    /// a fresh receiver per connection; nothing on the lossless edge path touches it.
    pub viz: broadcast::Sender<VizFrame>,
    triggers: Triggers,
    join: JoinHandle<()>,
}

impl DjRuntime {
    /// Register a LOSSLESS edge-trigger subscriber for the P2 executor. Every
    /// edge (`TrackStart`/`TrackEnd`/`StateChanged`/`WallClock`) is delivered on
    /// this per-subscriber unbounded `mpsc`, so a boundary is never missed even
    /// while the lossy observer path lags.
    pub fn subscribe_triggers(&self) -> mpsc::UnboundedReceiver<DjEvent> {
        let (tx, rx) = mpsc::unbounded_channel();
        self.triggers.lock().unwrap().push(tx);
        rx
    }

    /// The spine task handle. `main` select!s on this in the serve loop: spine
    /// exit (player channel closed) means the process should wind down.
    pub fn join(&mut self) -> &mut JoinHandle<()> {
        &mut self.join
    }
}

/// The publishing primitive: stamps every event with a monotonic `seq`, the
/// shared clock `now`, the cached `playlist_version`, and the current `cursor`
/// (the latch). Owns the fan-out to the three traffic classes.
struct Publisher<C: Clock> {
    clock: C,
    bcast: broadcast::Sender<DjEvent>,
    /// The dedicated viz broadcast (see [`DjRuntime::viz`]). Republished onto from
    /// the `PlayerEvent::Viz` spine arm; isolated from `bcast`.
    viz: broadcast::Sender<VizFrame>,
    watch: watch::Sender<QueueSnapshot>,
    triggers: Triggers,
    handler: Arc<HypodjHandler>,
    seq: u64,
    /// The current track identity, latched from the player's stamped events - the
    /// enrichment join anchor. NEVER derived from the mutable current index.
    latch: Option<QueueId>,
    /// The latched track's duration, cached at `TrackStart` so a per-tick
    /// `time_remaining` needs no state lock (hot-path lock-free).
    latched_duration: Option<Duration>,
    /// Cached playlist version, refreshed on every edge (a boundary) via the
    /// watch update; a plain `Tick` reads it without re-locking state.
    version: u64,
    /// Last play-state observers were shown, so a redundant raw player event
    /// (e.g. a second `Playing` for the same track) does not re-publish a state
    /// edge. Diffed against every incoming event to derive genuine boundaries.
    last_state: PlayState,
    /// When a natural `Eof` advanced the queue, the player emits a TRANSIENT
    /// `Stopped` (the mpv gap) immediately before the next track's `Playing`.
    /// This one-shot flag swallows exactly that Stopped so a gapless advance is
    /// End-then-Start with no spurious `StateChanged(Stopped)`.
    suppress_next_stopped: bool,
}

impl<C: Clock> Publisher<C> {
    fn envelope(&mut self, kind: DjEventKind) -> DjEvent {
        self.seq += 1;
        DjEvent {
            kind,
            now: self.clock.now(),
            seq: self.seq,
            playlist_version: self.version,
            cursor: self.latch,
        }
    }

    /// Publish a LOSSY, broadcast-only event (used for high-rate `Tick`).
    fn broadcast_only(&mut self, kind: DjEventKind) {
        let ev = self.envelope(kind);
        let _ = self.bcast.send(ev);
    }

    /// Publish an EDGE: refresh the level-triggered watch snapshot first (so the
    /// version/cursor stamp and the resync source both reflect this boundary),
    /// then send losslessly to every trigger subscriber AND the lossy broadcast.
    fn edge(&mut self, kind: DjEventKind) {
        self.refresh_watch();
        let ev = self.envelope(kind);
        // Lossless fan-out to the executor(s); prune any closed subscriber.
        {
            let mut subs = self.triggers.lock().unwrap();
            subs.retain(|tx| tx.send(ev.clone()).is_ok());
        }
        // Lossy mirror for passive observers.
        let _ = self.bcast.send(ev);
    }

    /// Re-read the whole-queue snapshot under one short lock scope, update the
    /// cached version, and push it to the level-triggered watch. When the queue
    /// actually changed (new playlist_version), also emit a `Resync` marker on the
    /// lossy broadcast so a lagged observer knows the level state moved and re-reads
    /// the watch snapshot (fulfilling the resync contract; the watch remains the
    /// authoritative source).
    fn refresh_watch(&mut self) {
        let snap = self.handler.queue_snapshot();
        let changed = snap.playlist_version != self.version;
        self.version = snap.playlist_version;
        let _ = self.watch.send(snap);
        if changed {
            let ev = self.envelope(DjEventKind::Resync);
            let _ = self.bcast.send(ev);
        }
    }

    /// Build a [`TrackRef`] for `qid`, joining against the current queue on
    /// IDENTITY. If the entry has left the queue the derived fields are `None`
    /// (never a shifted neighbor's), but the known `song` id is still carried.
    fn track_ref(&self, qid: QueueId, song: Option<SongId>) -> TrackRef {
        match self.handler.snapshot_by_queue_id(qid) {
            Some((idx, e)) => TrackRef {
                queue_id: qid,
                queue_index: Some(idx),
                // Prefer the id the player event carried; fall back to the queue
                // row so an outgoing End (an off-spine skip) still names its song
                // even when the event did not carry one.
                song: song.or(e.song),
                album_id: e.album_id,
                duration: e.duration,
            },
            None => TrackRef {
                queue_id: qid,
                queue_index: None,
                song,
                album_id: None,
                duration: None,
            },
        }
    }

    /// EDGE DERIVATION for a `Playing` player event. Emits `TrackStart(new)` ONLY
    /// when the effective current identity transitions to a genuinely NEW id. A
    /// pause->resume (or a redundant `Playing`) of the SAME latched track emits NO
    /// `TrackStart`. An off-spine next/prev/skip (a `loadfile` replace with no
    /// preceding `Eof`, so the outgoing id is still latched) emits `TrackEnd`
    /// (outgoing) BEFORE `TrackStart(new)`, so a skip is never a bare Start.
    fn on_playing(&mut self, qid: QueueId, song: Option<SongId>) {
        // A Playing edge means the gap (if any) never materialized: clear any pending
        // suppress so it can only ever swallow a Stopped that arrives BEFORE the next
        // Playing. A GAPLESS continuation handoff (a warmed station auto-advancing at
        // EOF) emits NO transient Stopped yet the Eof arm calls mark_advanced (the
        // station qid differs from the finishing qid), which would otherwise leave
        // suppress_next_stopped latched forever and swallow the user's NEXT real stop.
        // Clearing it here on the Playing edge fixes that generally.
        self.suppress_next_stopped = false;
        if self.latch == Some(qid) {
            // Same track already current: a resume or a duplicate Playing. Surface
            // ONLY a real state transition (never a spurious TrackStart).
            if self.last_state != PlayState::Playing {
                self.last_state = PlayState::Playing;
                let tref = self.track_ref(qid, song);
                self.edge(DjEventKind::StateChanged(PlayState::Playing, Some(tref)));
            }
            return;
        }
        // A genuinely new current id. If a DIFFERENT track is still latched, the
        // boundary arrived via an off-spine replace (no Eof cleared it): close it
        // with a TrackEnd first so observers see End-then-Start, not a bare Start.
        if let Some(prev) = self.latch {
            let end = self.track_ref(prev, None);
            self.edge(DjEventKind::TrackEnd(end));
        }
        let tref = self.track_ref(qid, song);
        self.set_latch(qid, tref.duration);
        self.last_state = PlayState::Playing;
        self.edge(DjEventKind::TrackStart(tref));
    }

    /// EDGE DERIVATION for a `Paused` player event: publish a `StateChanged(Paused)`
    /// carrying the track it applies to (song + derived index/album/duration from
    /// the id the event provides), but only on a genuine Playing->Paused edge.
    fn on_paused(&mut self, qid: Option<QueueId>, song: Option<SongId>) {
        // Pause is only meaningful from Playing: a Paused event from Stopped (or a
        // stray/duplicate) must not surface a spurious StateChanged(Paused). This
        // also dedups Paused->Paused (last_state would already be Paused).
        if self.last_state != PlayState::Playing {
            return;
        }
        let tref = qid.map(|q| self.track_ref(q, song));
        self.last_state = PlayState::Paused;
        self.edge(DjEventKind::StateChanged(PlayState::Paused, tref));
    }

    /// EDGE DERIVATION for a `Stopped` player event. Swallows the transient mpv
    /// gap of a gapless natural advance (the one-shot `suppress_next_stopped`);
    /// otherwise derives a REAL stop edge (queue exhausted or explicit stop),
    /// deduplicated against `last_state`.
    fn on_stopped(&mut self) {
        if self.suppress_next_stopped {
            // The transient gap between a natural Eof-advance and the next
            // track's Playing: not a real halt, so publish nothing and keep the
            // latch clear (the incoming Playing re-latches).
            self.suppress_next_stopped = false;
            return;
        }
        self.clear_latch();
        if self.last_state != PlayState::Stopped {
            self.last_state = PlayState::Stopped;
            self.edge(DjEventKind::StateChanged(PlayState::Stopped, None));
        }
    }

    /// Mark that a natural `Eof` advanced to a real next entry, so the trailing
    /// transient `Stopped` is swallowed rather than surfaced as a stop.
    fn mark_advanced(&mut self) {
        self.suppress_next_stopped = true;
    }

    fn set_latch(&mut self, qid: QueueId, duration: Option<Duration>) {
        self.latch = Some(qid);
        self.latched_duration = duration;
    }

    fn clear_latch(&mut self) {
        self.latch = None;
        self.latched_duration = None;
    }

    /// `time_remaining` honesty: `Some((dur - pos).max(0))` only when a known
    /// non-zero duration is latched; `None` for streams and duration-less songs
    /// (never `0`-as-unknown, never negative). A remaining-threshold trigger MUST
    /// NOT match on `None` - this is what stops a crossfade firing instantly over
    /// live radio.
    fn time_remaining(&self, pos: Duration) -> Option<Duration> {
        self.latched_duration.map(|d| d.saturating_sub(pos))
    }
}

/// Convert a raw mpv media-position (seconds, may be slightly negative/NaN at
/// edges) into a non-negative [`Duration`], never panicking.
fn pos_to_duration(pos: f64) -> Duration {
    Duration::try_from_secs_f64(pos.max(0.0)).unwrap_or(Duration::ZERO)
}

/// Launch the director spine over the LOSSLESS `player_events` input (moved out
/// of main.rs). Returns the [`DjRuntime`] handle bundle.
pub fn run<C: Clock>(
    clock: C,
    handler: Arc<HypodjHandler>,
    scrobbler: Arc<Scrobbler>,
    client: Arc<SubsonicClient>,
    player_events: mpsc::Receiver<PlayerEvent>,
) -> DjRuntime {
    let (bcast_tx, _bcast_rx0) = broadcast::channel::<DjEvent>(BROADCAST_CAP);
    // The dedicated viz broadcast: fresh per-connection receivers subscribe off the
    // returned `DjRuntime::viz`; no long-lived receiver is held here (a dropped
    // sender-only channel is fine - `send` just returns Err(no subscribers), ignored).
    let (viz_tx, _viz_rx0) = broadcast::channel::<VizFrame>(VIZ_BROADCAST_CAP);
    // Seed the watch with the current (at startup, empty) well-formed snapshot.
    let (watch_tx, watch_rx) = watch::channel(handler.queue_snapshot());
    // Register the watch with the handler so OFF-spine queue mutations
    // (add/delete/clear/move) republish a fresh snapshot too - a lagged observer
    // then always resyncs to current state, not a stale player-event-edge snapshot.
    handler.set_snapshot_sink(watch_tx.clone());
    let triggers: Triggers = Arc::new(StdMutex::new(Vec::new()));

    // Wall-clock timer source over the SHARED clock. Fires land on the lossless
    // trigger path (drained by the spine's second select arm).
    let (fire_tx, fire_rx) = mpsc::unbounded_channel();
    let timers = spawn_timer_source(clock.clone(), fire_tx);

    let publisher = Publisher {
        clock: clock.clone(),
        bcast: bcast_tx.clone(),
        viz: viz_tx.clone(),
        watch: watch_tx,
        triggers: triggers.clone(),
        handler: handler.clone(),
        seq: 0,
        latch: None,
        latched_duration: None,
        version: 0,
        last_state: PlayState::Stopped,
        suppress_next_stopped: false,
    };

    let join = tokio::spawn(spine(
        handler.clone(),
        scrobbler,
        client,
        player_events,
        fire_rx,
        publisher,
    ));

    DjRuntime {
        events: bcast_tx,
        snapshot: watch_rx,
        timers,
        handler,
        viz: viz_tx,
        triggers,
        join,
    }
}

/// The spine loop: single consumer of `player_events`, plus the timer-fire arm.
/// Preserves today's inline semantics exactly (scrobble FIRST, then advance).
async fn spine<C: Clock>(
    handler: Arc<HypodjHandler>,
    scrobbler: Arc<Scrobbler>,
    client: Arc<SubsonicClient>,
    mut player_events: mpsc::Receiver<PlayerEvent>,
    fire_rx: mpsc::UnboundedReceiver<crate::event::TimerId>,
    mut pubr: Publisher<C>,
) {
    // Fused so a timer source that ENDS before the spine cannot busy-spin the
    // biased select: once `recv` returns `None` we drop the receiver to `None`
    // and the guarded branch stops being polled (the loop parks on the rest).
    let mut fire_rx = Some(fire_rx);
    loop {
        tokio::select! {
            // Bias the player channel: it carries must-not-drop, backpressuring
            // input (mpv blocking_send), so drain it promptly.
            biased;
            ev = player_events.recv() => {
                let Some(ev) = ev else {
                    // Player gone: the spine ends. Dropping `pubr` drops the
                    // broadcast/watch/trigger senders, so observers see Closed.
                    break;
                };
                // Scrobble FIRST, on every event, unchanged from the old main.rs loop.
                scrobbler.on_event(&ev);
                match ev {
                    PlayerEvent::StateChanged(PlayState::Playing, song, qid) => {
                        // Resolve the duration off the spine so the scrobble 50%
                        // threshold can engage (unchanged from main.rs). Only a
                        // library song carries an id to resolve.
                        if let Some(id) = &song {
                            let scrobbler = scrobbler.clone();
                            let client = client.clone();
                            let id = id.clone();
                            tokio::spawn(async move {
                                if let Ok(song) = client.song(&id).await {
                                    scrobbler.set_duration(&id, song.duration_secs);
                                }
                            });
                        }
                        // Derive the boundary by DIFFING against the latch: a NEW
                        // id starts a track (closing any still-latched outgoing one
                        // first), a resume of the SAME id emits no TrackStart. A
                        // Playing with no queue_id is not attributable and dropped.
                        // A GENUINE track-start edge = a qid that is not the one already
                        // latched (a resume of the same qid is NOT a start). Computed BEFORE
                        // `on_playing` mutates the latch, so the station-identity resolver
                        // (task lq54isr) arms once per real start, never on a resume.
                        let new_edge = qid.is_some() && pubr.latch != qid;
                        if let Some(qid) = qid {
                            pubr.on_playing(qid, song);
                        }
                        // Drop any prior stream's live ICY label unless this is the SAME
                        // entry (a mid-stream title change re-lands on the same qid): a
                        // new track must not inherit the outgoing stream's Name/Title.
                        handler.clear_stream_meta_except(qid);
                        // AUTO-IDENTIFY (task bspk8v5): on the stream-becomes-current edge,
                        // arm the ICY grace timer (streams only, idempotent per qid so a
                        // resume does not restart the clock). clear_stream_meta_except just
                        // above already disarmed a stale slot from the outgoing entry; this
                        // only arms. Sync (no await), no poller - the timer wheel drives it.
                        handler.reschedule_auto_identify(qid);
                        // STATION IDENTITY (task lq54isr): on a genuine track-start, spawn
                        // the per-track resolver off the spine (self-filters to a raw stream;
                        // a library song returns without spawning). clear_stream_meta_except
                        // above already dropped a stale slot; this only (re)resolves.
                        if new_edge {
                            if let Some(qid) = qid {
                                handler.spawn_station_identity(qid);
                            }
                        }
                    }
                    PlayerEvent::TimePos { pos, queue_id } => {
                        // Attribute on IDENTITY: a frame whose queue_id does not
                        // match the latch is stale (an off-spine advance drained
                        // late) and is dropped, never mis-enriched.
                        if queue_id.is_some() && queue_id == pubr.latch {
                            let p = pos_to_duration(pos);
                            let rem = pubr.time_remaining(p);
                            pubr.broadcast_only(DjEventKind::Tick {
                                time_pos: p,
                                time_remaining: rem,
                            });
                        }
                    }
                    PlayerEvent::Eof { song, queue_id, continuation_landed } => {
                        if let Some(qid) = queue_id {
                            // (a)+(b): build the finishing ref and publish TrackEnd
                            // BEFORE advance repoints current (End before Start).
                            let finishing = pubr.track_ref(qid, song);
                            pubr.edge(DjEventKind::TrackEnd(finishing));
                        }
                        // The next TrackStart surfaces later from play_index's
                        // StateChanged(Playing); clear the latch now (End is done).
                        pubr.clear_latch();
                        // (c): network, inline, must-not-drop, but bounded so a hung
                        // stream_url cannot pin the spine. `continuation_landed` threads
                        // the actor's warmed-station-auto-advanced signal so the None-branch
                        // ATTRIBUTES the already-playing station (append the Stream row,
                        // repoint current, set the one-shot latch) instead of cold-starting.
                        let _ = tokio::time::timeout(
                            ADVANCE_TIMEOUT,
                            handler.advance_on_eof(continuation_landed),
                        )
                        .await;
                        // Swallow the trailing transient mpv Stopped ONLY if the
                        // queue advanced to a genuinely DIFFERENT entry (a gapless
                        // advance): observers then see End-then-Start with no
                        // intervening StateChanged(Stopped). A FAILED or timed-out
                        // advance leaves `current` at the finishing track (an Err
                        // from play_index never repoints it), and an exhausted queue
                        // leaves it None - both are REAL halts whose Stopped MUST
                        // surface, so is_some() alone is wrong: compare the new
                        // cursor against the finishing id.
                        let advanced = handler
                            .queue_snapshot()
                            .current
                            .map(|c| Some(c.queue_id) != queue_id)
                            .unwrap_or(false);
                        if advanced {
                            pubr.mark_advanced();
                        }
                    }
                    PlayerEvent::StateChanged(PlayState::Stopped, _, _) => {
                        pubr.on_stopped();
                        // Nothing is playing: clear any lingering stream label so a later
                        // currentsong on a re-queued entry never shows a stale now-playing.
                        handler.clear_stream_meta_except(None);
                    }
                    PlayerEvent::Viz { rms_db, peak_db, gain_db, playing } => {
                        // Cosmetic level sample: republish on the DEDICATED viz
                        // broadcast ONLY (never the shared `bcast`, never the lossless
                        // edge path). `send` returns Err when no viz client is
                        // connected - ignored (latest-wins, drop-on-no-subscriber).
                        let _ = pubr.viz.send(VizFrame { rms_db, peak_db, gain_db, playing });
                    }
                    PlayerEvent::StateChanged(PlayState::Paused, song, qid) => {
                        // Paused carries its SongId + queue_id on the player event;
                        // populate the TrackRef so the Paused DjEvent identifies its
                        // track. (The scrobbler already saw it.)
                        pubr.on_paused(qid, song);
                    }
                    PlayerEvent::StreamMetadata { queue_id, name, title } => {
                        // LIVE ICY now-playing for the current raw stream: store it on the
                        // handler keyed by the latched identity so `currentsong` surfaces
                        // the station Name / now-playing Title instead of the raw URL.
                        // set_stream_meta calls notify_change, waking an idling client to
                        // re-read. A None queue_id is unattributable and dropped.
                        if let Some(qid) = queue_id {
                            handler.set_stream_meta(qid, name, title);
                        }
                    }
                }
            }
            fired = async { fire_rx.as_mut().unwrap().recv().await }, if fire_rx.is_some() => {
                // A wall-clock timer fired: deliver on the lossless trigger path
                // (and mirror to broadcast). time_pos/time_remaining are absent on
                // a WallClock DjEvent by construction.
                match fired {
                    Some(id) => {
                        // CONTINUATION-WARM fire hook: a fired timer id may be the
                        // pending continuation warm's. Resolve + issue the background
                        // prefetch INLINE (so the warm is armed against the finishing
                        // track), bounded by ADVANCE_TIMEOUT so a hung station resolve
                        // cannot pin the spine. A stale / non-warm id no-ops. Runs
                        // BEFORE the WallClock edge so a plan timer sharing the id space
                        // still fans out unchanged.
                        let _ = tokio::time::timeout(
                            ADVANCE_TIMEOUT,
                            handler.on_continuation_warm_fire(id),
                        )
                        .await;
                        // AUTO-IDENTIFY fire hook (task bspk8v5): a fired id may be the
                        // pending auto-identify's. It only GATES here (a stale id no-ops)
                        // then tokio::spawns the up-to-40s capture off the spine, so it
                        // returns immediately and never pins the spine (no timeout needed).
                        handler.on_auto_identify_fire(id).await;
                        pubr.edge(DjEventKind::WallClock(id));
                    }
                    // Timer source ended: FUSE the branch (drop the receiver) so it
                    // is no longer polled. A closed fire channel is not a
                    // spine-terminal condition; keep serving player events.
                    None => fire_rx = None,
                }
            }
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::clock::TokioClock;
    use crate::config::ServerConfig;
    use crate::model::{Song, SongId};
    use crate::player::NullPlayer;

    // A Song with a fixed duration and album, enough for the enrichment join.
    fn song(id: &str, dur: Option<u32>, album: Option<&str>) -> Song {
        Song {
            id: SongId(id.into()),
            title: format!("t-{id}"),
            album: album.map(|a| a.to_string()),
            album_id: album.map(|a| crate::model::AlbumId(format!("al-{a}"))),
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

    // A never-reachable client: the director tests that touch the queue use
    // enqueue_song (no network) and NullPlayer (no stream fetch), so `client` is
    // only structurally required. Skips if CA certs are absent (build sandbox).
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

    /// Build a handler on a NullPlayer plus a director over the SAME player event
    /// channel, with two songs already queued. Returns everything a test drives.
    async fn rig(
        durs: &[(&str, Option<u32>, Option<&str>)],
    ) -> Option<(Arc<HypodjHandler>, DjRuntime, crate::player::PlayerHandle)> {
        let client = maybe_client()?;
        let (player, events) = NullPlayer::spawn();
        let handler = Arc::new(HypodjHandler::new(client.clone(), player.clone()));
        for (id, dur, album) in durs {
            handler.enqueue_song_for_test(song(id, *dur, *album)).await;
        }
        let scrobbler = Arc::new(Scrobbler::new(client.clone()));
        let rt = run(TokioClock, handler.clone(), scrobbler, client, events);
        Some((handler, rt, player))
    }

    fn kind_is_track_end(k: &DjEventKind) -> bool {
        matches!(k, DjEventKind::TrackEnd(_))
    }
    fn kind_is_track_start(k: &DjEventKind) -> bool {
        matches!(k, DjEventKind::TrackStart(_))
    }

    /// Drain trigger events until the first `TrackStart`, returning every event
    /// seen up to and including it. Tolerates the intermediate `StateChanged`
    /// edges the player emits around a boundary.
    async fn drain_to_track_start(
        rx: &mut mpsc::UnboundedReceiver<DjEvent>,
    ) -> Vec<DjEventKind> {
        let mut seen = Vec::new();
        loop {
            let ev = rx.recv().await.unwrap();
            let is_start = kind_is_track_start(&ev.kind);
            seen.push(ev.kind);
            if is_start {
                return seen;
            }
        }
    }

    // TOPOLOGY INVARIANT: even with a broadcast subscriber that never reads
    // (forcing Lagged), an Eof advances the queue EXACTLY once and the lossless
    // trigger mpsc delivers TrackEnd(id0) then TrackStart(id1) in order.
    #[tokio::test]
    async fn eof_lossless_under_lagged() {
        let Some((handler, rt, player)) = rig(&[("s0", Some(100), Some("A")), ("s1", Some(100), Some("B"))]).await
        else {
            eprintln!("skip: no CA certs");
            return;
        };
        // A broadcast subscriber we NEVER read: it will Lag, but must not affect
        // the lossless trigger path below.
        let _never_read = rt.events.subscribe();
        let mut triggers = rt.subscribe_triggers();

        // Start playing index 0 (NullPlayer emits StateChanged(Playing, s0, qid0)).
        handler.play_for_test(0).await;
        // TrackStart(s0).
        let first = triggers.recv().await.unwrap();
        assert!(kind_is_track_start(&first.kind), "got {:?}", first.kind);

        // Drive a natural Eof on the latched entry.
        player.test_emit_eof().await.unwrap();

        // The lossless trigger path delivers TrackEnd strictly before TrackStart
        // (End-then-Start), and exactly one of each across the boundary.
        let seen = drain_to_track_start(&mut triggers).await;
        let end_pos = seen.iter().position(kind_is_track_end).expect("a TrackEnd");
        let start_pos = seen.iter().rposition(kind_is_track_start).unwrap();
        assert!(end_pos < start_pos, "End before Start: {seen:?}");
        assert_eq!(
            seen.iter().filter(|k| kind_is_track_end(k)).count(),
            1,
            "exactly one TrackEnd (advanced exactly once)"
        );

        let snap = handler.queue_snapshot();
        assert_eq!(snap.current.as_ref().map(|c| c.index), Some(1), "advanced exactly once");
    }

    // time_remaining honesty: a known-duration song yields Some; a duration-less
    // song and a stream yield None. Also identity-drop of a stale TimePos.
    #[tokio::test]
    async fn time_remaining_and_identity_join() {
        let Some((handler, rt, player)) = rig(&[("s0", Some(100), Some("A")), ("s1", None, None)]).await
        else {
            eprintln!("skip: no CA certs");
            return;
        };
        let mut ticks = rt.events.subscribe();
        let mut triggers = rt.subscribe_triggers();

        handler.play_for_test(0).await;
        let _ts = triggers.recv().await.unwrap();

        // A TimePos for the latched entry (qid 0) yields a Tick with Some remaining.
        player.seek(40.0).await.unwrap();
        let tick = loop {
            let ev = ticks.recv().await.unwrap();
            if let DjEventKind::Tick { time_remaining, .. } = ev.kind {
                break time_remaining;
            }
        };
        assert_eq!(tick, Some(Duration::from_secs(60)), "100 - 40 = 60 remaining");

        // Advance to the duration-less song; its Tick has None remaining.
        player.test_emit_eof().await.unwrap();
        let _ = drain_to_track_start(&mut triggers).await;
        player.seek(5.0).await.unwrap();
        let tick2 = loop {
            let ev = ticks.recv().await.unwrap();
            if let DjEventKind::Tick { time_remaining, .. } = ev.kind {
                break time_remaining;
            }
        };
        assert_eq!(tick2, None, "duration-less song -> honest None remaining");
    }

    // Identity join under off-spine mutation: after an off-spine `next` repoints
    // the current pointer, the finishing entry A still enriches against A's own
    // index/duration, never B's. And a deleted-current yields index None (not a
    // shifted neighbor).
    #[tokio::test]
    async fn identity_join_survives_off_spine_next_and_delete() {
        let Some((handler, _rt, _player)) =
            rig(&[("s0", Some(100), Some("A")), ("s1", Some(200), Some("B"))]).await
        else {
            eprintln!("skip: no CA certs");
            return;
        };
        handler.play_for_test(0).await;

        // Off-spine advance to B.
        handler.next_for_test().await;
        // A (queue_id 0) still resolves to index 0 with A's duration - NOT B's.
        let (idx_a, ea) = handler.snapshot_by_queue_id(QueueId(0)).unwrap();
        assert_eq!(idx_a, 0);
        assert_eq!(ea.duration, Some(Duration::from_secs(100)));
        // B (queue_id 1) resolves to index 1 with B's duration.
        let (idx_b, eb) = handler.snapshot_by_queue_id(QueueId(1)).unwrap();
        assert_eq!(idx_b, 1);
        assert_eq!(eb.duration, Some(Duration::from_secs(200)));

        // Delete the head (queue_id 0). Its identity join now yields None (gone),
        // never a shifted neighbor; B shifts down to index 0 but keeps id 1.
        handler.delete_for_test(0);
        assert!(handler.snapshot_by_queue_id(QueueId(0)).is_none(), "deleted -> None");
        let (idx_b2, _) = handler.snapshot_by_queue_id(QueueId(1)).unwrap();
        assert_eq!(idx_b2, 0, "B shifted down but retains its stable id");
    }

    // Duplicate SongId at two positions disambiguates by the stable queue id.
    #[tokio::test]
    async fn duplicate_song_id_disambiguated_by_queue_id() {
        let Some((handler, _rt, _player)) =
            rig(&[("dup", Some(100), Some("A")), ("dup", Some(100), Some("A"))]).await
        else {
            eprintln!("skip: no CA certs");
            return;
        };
        let (i0, _) = handler.snapshot_by_queue_id(QueueId(0)).unwrap();
        let (i1, _) = handler.snapshot_by_queue_id(QueueId(1)).unwrap();
        assert_eq!(i0, 0);
        assert_eq!(i1, 1);
    }

    // A stream ending mid-queue emits Eof(song=None, queue_id=Some), so the queue
    // ADVANCES and a real TrackEnd(song=None, album=None) is observed instead of
    // a silent halt.
    #[tokio::test]
    async fn stream_end_mid_queue_advances() {
        let Some(client) = maybe_client() else {
            eprintln!("skip: no CA certs");
            return;
        };
        let (player, events) = NullPlayer::spawn();
        let handler = Arc::new(HypodjHandler::new(client.clone(), player.clone()));
        // A raw stream (no network) then a song.
        handler.enqueue_stream_for_test("http://radio.example/stream").await;
        handler.enqueue_song_for_test(song("s1", Some(100), Some("A"))).await;
        let scrobbler = Arc::new(Scrobbler::new(client.clone()));
        let rt = run(TokioClock, handler.clone(), scrobbler, client, events);
        let mut triggers = rt.subscribe_triggers();

        handler.play_for_test(0).await; // play the stream (queue_id 0)
        let start = drain_to_track_start(&mut triggers).await;
        // The stream's TrackStart carries no song.
        assert!(matches!(
            start.last().unwrap(),
            DjEventKind::TrackStart(t) if t.song.is_none()
        ));

        player.test_emit_eof().await.unwrap(); // natural stream end
        let seen = drain_to_track_start(&mut triggers).await;
        // A real TrackEnd for the stream (song None, album None) appeared.
        assert!(seen.iter().any(|k| matches!(
            k,
            DjEventKind::TrackEnd(t) if t.song.is_none() && t.album_id.is_none()
        )));
        // And the queue advanced to the song at index 1.
        assert_eq!(handler.queue_snapshot().current.map(|c| c.index), Some(1));
    }

    // Timer under paused virtual time: an armed deadline fires exactly one
    // WallClock on the LOSSLESS trigger path, stamped on the shared clock.
    #[tokio::test(start_paused = true)]
    async fn timer_fires_on_lossless_path() {
        let Some((_handler, rt, _player)) = rig(&[("s0", Some(100), Some("A"))]).await else {
            eprintln!("skip: no CA certs");
            return;
        };
        let mut triggers = rt.subscribe_triggers();
        let (id, _guard) = rt.timers.arm(tokio::time::Instant::now() + Duration::from_secs(30));
        tokio::time::advance(Duration::from_secs(31)).await;
        tokio::task::yield_now().await;
        let ev = triggers.recv().await.unwrap();
        match ev.kind {
            DjEventKind::WallClock(fired) => assert_eq!(fired, id),
            other => panic!("expected WallClock, got {other:?}"),
        }
    }

    fn is_stopped_state(k: &DjEventKind) -> bool {
        matches!(k, DjEventKind::StateChanged(PlayState::Stopped, _))
    }

    // EDGE DERIVATION (A/B): a pause->resume of the SAME track emits NO TrackStart
    // (only real state edges), and the Paused DjEvent CARRIES its song (B).
    #[tokio::test]
    async fn pause_resume_no_track_start_and_paused_carries_song() {
        let Some((handler, rt, player)) = rig(&[("s0", Some(100), Some("A"))]).await else {
            eprintln!("skip: no CA certs");
            return;
        };
        let mut triggers = rt.subscribe_triggers();

        handler.play_for_test(0).await;
        let start = triggers.recv().await.unwrap();
        assert!(kind_is_track_start(&start.kind), "got {:?}", start.kind);

        // Pause: a real Playing->Paused edge that identifies its track (B).
        player.pause().await.unwrap();
        let paused = triggers.recv().await.unwrap();
        match &paused.kind {
            DjEventKind::StateChanged(PlayState::Paused, Some(t)) => {
                assert_eq!(t.song, Some(SongId("s0".into())), "Paused carries its song");
                assert_eq!(t.queue_id, QueueId(0));
                assert_eq!(t.duration, Some(Duration::from_secs(100)));
            }
            other => panic!("expected StateChanged(Paused, Some), got {other:?}"),
        }

        // Resume the SAME track: a state edge, NEVER a new TrackStart.
        player.resume().await.unwrap();
        let resumed = triggers.recv().await.unwrap();
        assert!(
            matches!(resumed.kind, DjEventKind::StateChanged(PlayState::Playing, _)),
            "resume is a state edge, got {:?}",
            resumed.kind
        );
        assert!(!kind_is_track_start(&resumed.kind), "resume must not re-Start");
    }

    // EDGE DERIVATION (A-b): a GAPLESS natural advance (Eof then next Playing)
    // yields TrackEnd-then-TrackStart with NO intervening StateChanged(Stopped) -
    // the transient mpv gap is suppressed.
    #[tokio::test]
    async fn gapless_advance_has_no_intervening_stopped() {
        let Some((handler, rt, player)) =
            rig(&[("s0", Some(100), Some("A")), ("s1", Some(100), Some("B"))]).await
        else {
            eprintln!("skip: no CA certs");
            return;
        };
        let mut triggers = rt.subscribe_triggers();

        handler.play_for_test(0).await;
        let _ = triggers.recv().await.unwrap(); // TrackStart(s0)

        player.test_emit_eof().await.unwrap();
        let seen = drain_to_track_start(&mut triggers).await;
        assert!(seen.iter().any(kind_is_track_end), "a TrackEnd: {seen:?}");
        assert!(
            !seen.iter().any(is_stopped_state),
            "no transient StateChanged(Stopped) in a gapless advance: {seen:?}"
        );
        let end = seen.iter().position(kind_is_track_end).unwrap();
        let start = seen.iter().rposition(kind_is_track_start).unwrap();
        assert!(end < start, "End before Start: {seen:?}");
    }

    // EDGE DERIVATION (A-b): Eof on the LAST entry exhausts the queue, so the
    // trailing Stopped is a REAL halt and MUST surface as StateChanged(Stopped) -
    // it is NOT swallowed as a gapless-advance gap. Guards the "advanced only when
    // the cursor moved to a different id" fix (is_some() alone would wrongly
    // swallow it on a failed/exhausted advance).
    #[tokio::test]
    async fn exhausted_advance_surfaces_real_stop() {
        let Some((handler, rt, player)) = rig(&[("s0", Some(100), Some("A"))]).await else {
            eprintln!("skip: no CA certs");
            return;
        };
        let mut triggers = rt.subscribe_triggers();
        handler.play_for_test(0).await;
        let _ = triggers.recv().await.unwrap(); // TrackStart(s0)
        player.test_emit_eof().await.unwrap();
        let end = triggers.recv().await.unwrap();
        assert!(kind_is_track_end(&end.kind), "expected TrackEnd, got {end:?}");
        let stop = triggers.recv().await.unwrap();
        assert!(
            is_stopped_state(&stop.kind),
            "exhausted queue must surface a REAL StateChanged(Stopped), got {stop:?}"
        );
    }

    // EDGE DERIVATION (A-c): an OFF-spine next (loadfile replace, no Eof) emits
    // TrackEnd(outgoing) BEFORE TrackStart(new) - a skip is never a bare Start.
    #[tokio::test]
    async fn off_spine_next_is_end_then_start() {
        let Some((handler, rt, _player)) =
            rig(&[("s0", Some(100), Some("A")), ("s1", Some(200), Some("B"))]).await
        else {
            eprintln!("skip: no CA certs");
            return;
        };
        let mut triggers = rt.subscribe_triggers();

        handler.play_for_test(0).await;
        let _ = triggers.recv().await.unwrap(); // TrackStart(s0)

        handler.next_for_test().await; // off-spine skip to s1
        let seen = drain_to_track_start(&mut triggers).await;
        let end = seen.iter().position(kind_is_track_end).expect("TrackEnd(outgoing)");
        let start = seen.iter().rposition(kind_is_track_start).unwrap();
        assert!(end < start, "End before Start on a skip: {seen:?}");
        match &seen[end] {
            DjEventKind::TrackEnd(t) => {
                assert_eq!(t.song, Some(SongId("s0".into())), "End names the outgoing s0");
            }
            other => panic!("expected TrackEnd, got {other:?}"),
        }
        match &seen[start] {
            DjEventKind::TrackStart(t) => {
                assert_eq!(t.song, Some(SongId("s1".into())), "Start names the new s1");
            }
            other => panic!("expected TrackStart, got {other:?}"),
        }
    }

    // SLICE-2 (Part E): a GAPLESS continuation handoff emits no transient Stopped, but
    // the Eof arm calls mark_advanced (the station qid differs from the finishing qid),
    // latching suppress_next_stopped. Without the on_playing clear, that latch would
    // swallow the user's NEXT real stop. The Playing edge (the station starting) must
    // clear it so the later real stop surfaces.
    #[tokio::test]
    async fn playing_edge_clears_suppress_next_stopped() {
        let Some(client) = maybe_client() else {
            eprintln!("skip: no CA certs");
            return;
        };
        let (player, _ev) = NullPlayer::spawn();
        let handler = Arc::new(HypodjHandler::new(client.clone(), player));
        handler.enqueue_song_for_test(song("s0", Some(100), Some("A"))).await;
        let (bcast, _br) = broadcast::channel(16);
        let (viz, _vr) = broadcast::channel(16);
        let (watch_tx, _wr) = watch::channel(handler.queue_snapshot());
        let triggers: Triggers = Arc::new(StdMutex::new(Vec::new()));
        let mut trig = {
            let (tx, rx) = mpsc::unbounded_channel();
            triggers.lock().unwrap().push(tx);
            rx
        };
        let mut pubr = Publisher {
            clock: TokioClock,
            bcast,
            viz,
            watch: watch_tx,
            triggers,
            handler: handler.clone(),
            seq: 0,
            latch: None,
            latched_duration: None,
            version: 0,
            last_state: PlayState::Playing,
            suppress_next_stopped: false,
        };
        // A gapless advance marked advanced: suppress is latched.
        pubr.mark_advanced();
        assert!(pubr.suppress_next_stopped, "mark_advanced latches the suppress");
        // The station's Playing edge arrives (no transient Stopped preceded it): it clears
        // the pending suppress.
        pubr.on_playing(QueueId(0), Some(SongId("s0".into())));
        assert!(!pubr.suppress_next_stopped, "a Playing edge clears the pending suppress");
        // A later REAL stop must now surface (not be swallowed).
        pubr.on_stopped();
        let mut saw_stop = false;
        while let Ok(ev) = trig.try_recv() {
            if is_stopped_state(&ev.kind) {
                saw_stop = true;
            }
        }
        assert!(saw_stop, "the real stop after a cleared suppress surfaces as StateChanged(Stopped)");
    }

    // RESYNC CORRECTNESS (C): an OFF-spine queue mutation (not a player-event
    // edge) is reflected in the level-triggered resync snapshot the instant it
    // happens, so a lagged observer never resyncs to a stale snapshot.
    #[tokio::test]
    async fn queue_mutation_reflected_in_resync_snapshot() {
        let Some((handler, rt, _player)) = rig(&[("s0", Some(100), Some("A"))]).await else {
            eprintln!("skip: no CA certs");
            return;
        };
        // Seeded snapshot: one entry, carrying its song id (C).
        {
            let snap = rt.snapshot.borrow();
            assert_eq!(snap.entries.len(), 1);
            assert_eq!(snap.entries[0].song, Some(SongId("s0".into())));
        }
        // An off-spine enqueue refreshes the watch.
        handler.enqueue_song_for_test(song("s1", Some(100), Some("B"))).await;
        assert_eq!(rt.snapshot.borrow().entries.len(), 2, "enqueue reflected");
        // A delete too.
        handler.delete_for_test(0);
        assert_eq!(rt.snapshot.borrow().entries.len(), 1, "delete reflected");
    }

    // TIMER SELECT FUSE (D): if the timer source ENDS before the spine, the fused
    // select branch stops being polled and the loop parks on the remaining
    // sources - it does NOT busy-spin (which on a current-thread runtime would
    // starve the player task and wedge this test).
    #[tokio::test]
    async fn timer_source_end_does_not_wedge_spine() {
        let Some((handler, rt, _player)) = rig(&[("s0", Some(100), Some("A"))]).await else {
            eprintln!("skip: no CA certs");
            return;
        };
        let mut triggers = rt.subscribe_triggers();
        let handler2 = handler.clone();
        // Dropping the runtime drops the sole TimerHandle: the source task exits
        // and closes the fire channel (recv -> None -> the branch fuses). The spine
        // keeps its own player_events + publisher and runs on.
        drop(rt);
        tokio::task::yield_now().await;
        // The spine still serves player events promptly (no busy-spin starvation).
        handler2.play_for_test(0).await;
        let ev = tokio::time::timeout(Duration::from_secs(5), triggers.recv())
            .await
            .expect("spine not wedged by timer-source end")
            .unwrap();
        assert!(kind_is_track_start(&ev.kind), "got {:?}", ev.kind);
    }

    // Shutdown: dropping the player closes player_events; the spine ends, a
    // broadcast observer sees Closed, and the held JoinHandle completes.
    #[tokio::test]
    async fn shutdown_closes_observers() {
        let Some(client) = maybe_client() else {
            eprintln!("skip: no CA certs");
            return;
        };
        let (player, _null_events) = NullPlayer::spawn();
        let handler = Arc::new(HypodjHandler::new(client.clone(), player.clone()));
        let scrobbler = Arc::new(Scrobbler::new(client.clone()));
        // Drive the spine from a channel we own, so we can close the "player" side
        // directly (the daemon's fail-loud signal: the player event source ended).
        let (evt_tx, evt_rx) = mpsc::channel::<PlayerEvent>(8);
        let mut rt = run(TokioClock, handler, scrobbler, client, evt_rx);
        let mut obs = rt.events.subscribe();

        // Steal the spine handle so we can await it after tearing the runtime down.
        let jh = std::mem::replace(rt.join(), tokio::spawn(async {}));
        // Close the player event source: the spine breaks (no resubscribe spin).
        drop(evt_tx);
        jh.await.unwrap();
        // Dropping the runtime + player releases the last broadcast senders; the
        // observer then sees a terminal Closed.
        drop(rt);
        drop(player);
        loop {
            match obs.recv().await {
                Err(broadcast::error::RecvError::Closed) => break,
                Err(broadcast::error::RecvError::Lagged(_)) => continue,
                Ok(_) => continue,
            }
        }
    }
}
