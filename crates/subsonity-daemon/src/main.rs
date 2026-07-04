//! subsonity daemon entrypoint.
//!
//! FOUNDATION wiring: load config, connect + ping the Subsonic server, then
//! (TODO next-phase) start the MPD server bound to config.mpd.bind.
//!
//! HARD CONSTRAINT honored: default bind is 127.0.0.1:6601, NOT 6600 - the
//! running mopidy service owns 6600 and must not be disturbed.

use std::net::SocketAddr;
use std::path::PathBuf;
use std::sync::Arc;

use subsonity_core::config::Config;
use subsonity_core::handler::SubsonityHandler;
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

    // Spawn the real mpv-backed player actor behind the same PlayerHandle.
    //
    // AudioOut is chosen from SUBSONITY_AUDIO: "null" (default) keeps playback
    // fully headless (ao=null) - what the Phase-2 dev/validation run uses so the
    // user's speakers are never touched while mopidy still owns real output;
    // "device" opens the real device (Phase-4 cutover). If libmpv is missing at
    // runtime, spawn() logs and falls back to a NullPlayer actor rather than
    // panicking.
    use subsonity_core::player::{AudioOut, MpvPlayer};
    let audio = match std::env::var("SUBSONITY_AUDIO").as_deref() {
        Ok("device") => AudioOut::Device,
        _ => AudioOut::Null,
    };
    let (player, mut player_events) = MpvPlayer::spawn(audio);

    let handler = Arc::new(SubsonityHandler::new(client, player.clone()));

    // Queue-advance: on natural EOF, advance to the next queue entry so playback
    // continues like MPD. (Also keeps `status` honest across track ends.)
    {
        let handler = handler.clone();
        tokio::spawn(async move {
            use subsonity_core::player::PlayerEvent;
            while let Some(ev) = player_events.recv().await {
                if let PlayerEvent::Eof(_) = ev {
                    handler.advance_on_eof().await;
                }
            }
        });
    }

    let bind: SocketAddr = cfg.mpd.bind.parse()?;
    let server = MpdServer::new(bind);
    tracing::info!(%bind, "starting MPD server");
    server.serve(handler).await?;
    Ok(())
}
