//! Audio player abstraction.
//!
//! FOUNDATION - and this is the load-bearing foundation decision the persona
//! critiques all converged on, so it is settled NOW rather than after the MPD
//! server is built on top of it.
//!
//! ## Why an actor, not a `&mut self` trait
//!
//! The chosen backend is `libmpv2` (wraps libmpv - see README/deps rationale).
//! `libmpv2::Mpv` is NOT freely `Send`/`Sync`, and mpv's event model is a
//! pull-based blocking `wait_event` loop that must be drained on every wakeup or
//! its ring buffer overflows and drops events. The natural shape is therefore a
//! dedicated OS thread that owns the `Mpv` handle, receiving commands over a
//! channel and pushing events back out.
//!
//! So the public boundary is a cloneable [`PlayerHandle`]:
//!   - commands go over an `mpsc` and get a `oneshot` reply (so callers `&self`,
//!     never `&mut self`; the MPD server holds a clone per connection while the
//!     real state lives behind the actor - MPD state is SHARED across clients);
//!   - playback state is a `watch::Receiver<PlayState>` (cheap, always-current
//!     snapshot, no round-trip);
//!   - an event stream ([`PlayerEvent`]: time-pos / eof / state) flows back out.
//!     This is a FOUNDATION concern, not a later detail: eof + time-pos are
//!     exactly what drives queue-advance and the scrobble trigger (feature 1).
//!
//! [`NullPlayer::spawn`] is a REAL working actor over this exact boundary
//! (headless / tests), which proves the handle contract before mpv lands. The
//! Phase-1 `MpvPlayer` slots in behind the SAME [`PlayerHandle`] by swapping the
//! actor body for the mpv thread; nothing above it changes.

use crate::model::SongId;
use tokio::sync::{mpsc, oneshot, watch};

#[derive(Debug, Clone, Copy, PartialEq, Eq, Default)]
pub enum PlayState {
    #[default]
    Stopped,
    Playing,
    Paused,
}

#[derive(Debug, thiserror::Error)]
pub enum PlayerError {
    #[error("player backend: {0}")]
    Backend(String),
    #[error("player actor is gone")]
    Gone,
}

/// Events pushed OUT of the player actor. In the mpv impl these originate from
/// mpv's `time-pos` / `end-file` observations. The MPD server + a scrobble
/// driver subscribe to this stream.
#[derive(Debug, Clone)]
pub enum PlayerEvent {
    /// Current playback position, seconds. Emitted periodically while playing.
    TimePos(f64),
    /// The current track finished (natural end). Triggers queue-advance +
    /// scrobble submission.
    Eof(SongId),
    /// Play state changed (e.g. paused, stopped).
    StateChanged(PlayState),
}

/// Commands sent INTO the player actor. Each carries a `oneshot` for its reply,
/// so the caller-facing methods on [`PlayerHandle`] are `&self` + `await`.
///
/// `allow(dead_code)`: `PlayUrl.url` and `SetVolume.vol` are part of the locked
/// command contract but the headless `NullPlayer` actor ignores them; the mpv
/// actor (next-phase) reads both (loadfile(url) / set_property("volume", vol)).
#[allow(dead_code)]
enum PlayerCommand {
    PlayUrl {
        song: SongId,
        url: String,
        reply: oneshot::Sender<Result<(), PlayerError>>,
    },
    Pause(oneshot::Sender<Result<(), PlayerError>>),
    Resume(oneshot::Sender<Result<(), PlayerError>>),
    Stop(oneshot::Sender<Result<(), PlayerError>>),
    Seek {
        secs: f64,
        reply: oneshot::Sender<Result<(), PlayerError>>,
    },
    SetVolume {
        vol: u8,
        reply: oneshot::Sender<Result<(), PlayerError>>,
    },
}

/// The cloneable handle every other layer holds. Cheap to clone (just channel
/// senders + a watch receiver). This is the whole public player surface.
#[derive(Clone)]
pub struct PlayerHandle {
    cmd_tx: mpsc::Sender<PlayerCommand>,
    state_rx: watch::Receiver<PlayState>,
}

impl PlayerHandle {
    /// Load a resolved stream URL and begin playback of `song`.
    pub async fn play_url(&self, song: SongId, url: &str) -> Result<(), PlayerError> {
        self.request(|reply| PlayerCommand::PlayUrl {
            song,
            url: url.to_string(),
            reply,
        })
        .await
    }

    pub async fn pause(&self) -> Result<(), PlayerError> {
        self.request(PlayerCommand::Pause).await
    }

    pub async fn resume(&self) -> Result<(), PlayerError> {
        self.request(PlayerCommand::Resume).await
    }

    pub async fn stop(&self) -> Result<(), PlayerError> {
        self.request(PlayerCommand::Stop).await
    }

    /// Seek to absolute position in seconds.
    pub async fn seek(&self, secs: f64) -> Result<(), PlayerError> {
        self.request(|reply| PlayerCommand::Seek { secs, reply }).await
    }

    /// Set volume; `0..=100` to match MPD's range.
    pub async fn set_volume(&self, vol: u8) -> Result<(), PlayerError> {
        self.request(|reply| PlayerCommand::SetVolume { vol, reply })
            .await
    }

    /// Always-current play state snapshot (no round-trip to the actor).
    pub fn state(&self) -> PlayState {
        *self.state_rx.borrow()
    }

    /// Subscribe to state changes (e.g. for `idle player` in the MPD layer).
    pub fn subscribe_state(&self) -> watch::Receiver<PlayState> {
        self.state_rx.clone()
    }

    async fn request<F>(&self, make: F) -> Result<(), PlayerError>
    where
        F: FnOnce(oneshot::Sender<Result<(), PlayerError>>) -> PlayerCommand,
    {
        let (tx, rx) = oneshot::channel();
        self.cmd_tx
            .send(make(tx))
            .await
            .map_err(|_| PlayerError::Gone)?;
        rx.await.map_err(|_| PlayerError::Gone)?
    }
}

/// A no-op player ACTOR over the real [`PlayerHandle`] boundary. Used to exercise
/// the handle contract and the MPD layer without an audio device (headless CI,
/// unit tests). It is a genuine actor (owns state, processes the command loop,
/// updates the `watch`), not a stub that pretends to play. NOT the production
/// backend - that is `MpvPlayer` (next-phase).
pub struct NullPlayer;

impl NullPlayer {
    /// Spawn the actor; returns the handle plus the event stream receiver.
    /// (The event stream is unused by NullPlayer beyond `StateChanged`, but the
    /// wiring is real so the mpv impl inherits an identical spawn contract.)
    pub fn spawn() -> (PlayerHandle, mpsc::Receiver<PlayerEvent>) {
        let (cmd_tx, mut cmd_rx) = mpsc::channel::<PlayerCommand>(32);
        let (state_tx, state_rx) = watch::channel(PlayState::Stopped);
        let (evt_tx, evt_rx) = mpsc::channel::<PlayerEvent>(64);

        tokio::spawn(async move {
            let mut current: Option<SongId> = None;
            while let Some(cmd) = cmd_rx.recv().await {
                match cmd {
                    PlayerCommand::PlayUrl { song, url: _, reply } => {
                        current = Some(song);
                        let _ = state_tx.send(PlayState::Playing);
                        let _ = evt_tx.send(PlayerEvent::StateChanged(PlayState::Playing)).await;
                        let _ = reply.send(Ok(()));
                    }
                    PlayerCommand::Pause(reply) => {
                        let _ = state_tx.send(PlayState::Paused);
                        let _ = evt_tx.send(PlayerEvent::StateChanged(PlayState::Paused)).await;
                        let _ = reply.send(Ok(()));
                    }
                    PlayerCommand::Resume(reply) => {
                        let _ = state_tx.send(PlayState::Playing);
                        let _ = evt_tx.send(PlayerEvent::StateChanged(PlayState::Playing)).await;
                        let _ = reply.send(Ok(()));
                    }
                    PlayerCommand::Stop(reply) => {
                        current = None;
                        let _ = state_tx.send(PlayState::Stopped);
                        let _ = evt_tx.send(PlayerEvent::StateChanged(PlayState::Stopped)).await;
                        let _ = reply.send(Ok(()));
                    }
                    PlayerCommand::Seek { secs, reply } => {
                        let _ = evt_tx.send(PlayerEvent::TimePos(secs)).await;
                        let _ = reply.send(Ok(()));
                    }
                    PlayerCommand::SetVolume { vol: _, reply } => {
                        let _ = reply.send(Ok(()));
                    }
                }
            }
            // Channel closed: nothing more to do. `current` kept only to model
            // what the real actor tracks.
            let _ = current;
        });

        (PlayerHandle { cmd_tx, state_rx }, evt_rx)
    }
}

// TODO(next-phase): MpvPlayer.
//
//   pub struct MpvPlayer;
//   impl MpvPlayer {
//       pub fn spawn() -> (PlayerHandle, mpsc::Receiver<PlayerEvent>) {
//           // 1. mpsc + watch + event channels, IDENTICAL to NullPlayer::spawn.
//           // 2. std::thread::spawn a dedicated OS thread that:
//           //      - constructs libmpv2::Mpv (NOT Send, so it is created and
//           //        stays ON this thread; only channel ends cross threads),
//           //      - loops: select between (a) cmd_rx.blocking_recv() and
//           //        (b) mpv.wait_event(timeout) DRAINED to None each wakeup,
//           //      - maps commands -> loadfile / set_property("pause") / seek /
//           //        set_property("volume"),
//           //      - maps mpv events -> PlayerEvent: PropertyChange("time-pos")
//           //        -> TimePos, EndFile -> Eof(current_song) (drives scrobble +
//           //        queue-advance), and pushes PlayState via the watch.
//           // The PlayerHandle contract above does not change.
//       }
//   }

#[cfg(test)]
mod tests {
    use super::*;
    use crate::model::SongId;

    // Locks the FOUNDATION contract: the NullPlayer actor is drivable purely
    // through the `&self` PlayerHandle, state is observable via the watch, and
    // play/pause/resume/stop transition as expected. This proves the actor
    // boundary is usable headless before the mpv backend exists.
    #[tokio::test]
    async fn null_player_transitions_via_handle() {
        let (player, _events) = NullPlayer::spawn();
        assert_eq!(player.state(), PlayState::Stopped);

        player
            .play_url(SongId("42".into()), "http://example/stream")
            .await
            .unwrap();
        assert_eq!(player.state(), PlayState::Playing);

        player.pause().await.unwrap();
        assert_eq!(player.state(), PlayState::Paused);

        player.resume().await.unwrap();
        assert_eq!(player.state(), PlayState::Playing);

        player.set_volume(80).await.unwrap();
        player.seek(12.5).await.unwrap();

        player.stop().await.unwrap();
        assert_eq!(player.state(), PlayState::Stopped);
    }

    // The handle is cloneable and shared: a second clone sees the same state,
    // which is what the (shared-across-connections) MPD handler relies on.
    #[tokio::test]
    async fn handle_clone_shares_state() {
        let (player, _events) = NullPlayer::spawn();
        let other = player.clone();
        player
            .play_url(SongId("1".into()), "http://x")
            .await
            .unwrap();
        assert_eq!(other.state(), PlayState::Playing);
    }
}
