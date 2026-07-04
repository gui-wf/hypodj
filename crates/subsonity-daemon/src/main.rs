//! subsonity daemon entrypoint.
//!
//! FOUNDATION wiring: load config, connect + ping the Subsonic server, then
//! (TODO next-phase) start the MPD server bound to config.mpd.bind.
//!
//! HARD CONSTRAINT honored: default bind is 127.0.0.1:6601, NOT 6600 - the
//! running mopidy service owns 6600 and must not be disturbed.

use std::net::SocketAddr;
use std::path::PathBuf;

use subsonity_core::config::Config;
use subsonity_core::mpd::MpdServer;
use subsonity_core::subsonic::SubsonicClient;

#[tokio::main(flavor = "multi_thread", worker_threads = 2)]
async fn main() -> anyhow::Result<()> {
    tracing_subscriber::fmt()
        .with_env_filter(
            tracing_subscriber::EnvFilter::try_from_default_env()
                .unwrap_or_else(|_| "info".into()),
        )
        .init();

    let cfg_path = std::env::args()
        .nth(1)
        .map(PathBuf::from)
        .unwrap_or_else(|| PathBuf::from("subsonity.toml"));
    let cfg = Config::load(&cfg_path)?;

    let client = SubsonicClient::connect(&cfg.server)?;
    client.ping().await?;
    tracing::info!("connected to {}", cfg.server.url);

    // Spawn the real mpv-backed player actor behind the same PlayerHandle. It is
    // constructed with AudioOut::Null here: the MPD server loop that would drive
    // real playback is still next-phase, so the daemon must not open the audio
    // device on startup. When the serve loop lands, this flips to
    // AudioOut::Device. If libmpv is missing at runtime, spawn() logs and falls
    // back to a NullPlayer actor rather than panicking.
    use subsonity_core::player::{AudioOut, MpvPlayer};
    let (player, _player_events) = MpvPlayer::spawn(AudioOut::Null);
    let _ = player.state();

    let bind: SocketAddr = cfg.mpd.bind.parse()?;
    let server = MpdServer::new(bind);
    tracing::info!(%bind, "MPD server layer is next-phase; not serving yet");

    // TODO(next-phase): build the shared MpdHandler (holds `player` clone +
    // `client`) and `server.serve(Arc::new(handler)).await`.
    let _ = server;
    Ok(())
}
