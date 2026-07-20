//! hypodj daemon entrypoint.
//!
//! FOUNDATION wiring: load config, connect + ping the Subsonic server, then
//! (TODO next-phase) start the MPD server bound to config.mpd.bind.
//!
//! HARD CONSTRAINT honored: default bind is 127.0.0.1:6601, NOT 6600 - the
//! running mopidy service owns 6600 and must not be disturbed.

use std::net::SocketAddr;
use std::path::PathBuf;
use std::sync::Arc;
use std::time::Duration;

use hypodj_core::config::Config;
use hypodj_core::handler::HypodjHandler;
use hypodj_core::mpd::MpdServer;
use hypodj_core::subsonic::SubsonicClient;

const USAGE: &str = "\
hypodj - MPD-speaking OpenSubsonic client daemon

USAGE:
    hypodj [CONFIG]

ARGS:
    CONFIG    Path to the TOML config (default: hypodj.toml)

ENV:
    HYPODJ_AUDIO    \"null\" (default, headless ao=null) or \"device\" (real output)
    RUST_LOG        tracing filter (default: info)

OPTIONS:
    -h, --help       Print this help and exit
    -V, --version    Print version and exit";

#[tokio::main(flavor = "multi_thread", worker_threads = 2)]
async fn main() -> anyhow::Result<()> {
    // Tiny hand-rolled flag handling: keep argv[1] as the config path, but treat
    // the standard --help/--version flags specially. No arg-parser dependency.
    if let Some(arg) = std::env::args().nth(1) {
        match arg.as_str() {
            "-h" | "--help" => {
                println!("{USAGE}");
                return Ok(());
            }
            "-V" | "--version" => {
                // Enriched display version: base semver + commits-since-tag +
                // git short hash on source builds (bare semver otherwise).
                println!(
                    "hypodj {}",
                    hypodj_build_info::version(
                        env!("CARGO_PKG_VERSION"),
                        option_env!("HYPODJ_BUILD_INFO"),
                    )
                );
                return Ok(());
            }
            _ => {}
        }
    }

    tracing_subscriber::fmt()
        .with_env_filter(
            tracing_subscriber::EnvFilter::try_from_default_env()
                .unwrap_or_else(|_| "info".into()),
        )
        .init();

    let cfg_path = std::env::args()
        .nth(1)
        .map(PathBuf::from)
        .unwrap_or_else(|| PathBuf::from("hypodj.toml"));
    let cfg = Config::load(&cfg_path)?;

    let mut client = SubsonicClient::connect(&cfg.server)?;
    client.ping().await?;
    // Negotiate OpenSubsonic extensions ONCE, while we still hold &mut, before
    // the client is shared into the handler + scrobbler (feature 9).
    client.probe_extensions().await;
    tracing::info!("connected to {}", cfg.server.url);
    let client = std::sync::Arc::new(client);

    // Spawn the real mpv-backed player actor behind the same PlayerHandle.
    //
    // AudioOut is chosen from HYPODJ_AUDIO: "null" (default) keeps playback
    // fully headless (ao=null) - what the Phase-2 dev/validation run uses so the
    // user's speakers are never touched while mopidy still owns real output;
    // "device" opens the real device (Phase-4 cutover). If libmpv is missing at
    // runtime, spawn() logs and falls back to a NullPlayer actor rather than
    // panicking.
    use hypodj_core::player::{AudioOut, MpvPlayer};
    let audio = match std::env::var("HYPODJ_AUDIO").as_deref() {
        Ok("device") => AudioOut::Device,
        // `file:<path>` decodes to a WAV (headless proof path; no device). Any
        // other value (incl. "null"/unset) stays fully headless via ao=null.
        Ok(v) if v.starts_with("file:") => {
            AudioOut::File(PathBuf::from(v.trim_start_matches("file:")))
        }
        _ => AudioOut::Null,
    };
    let (player, player_events) = MpvPlayer::spawn(audio);

    let handler = Arc::new(HypodjHandler::with_fade_config(
        client.clone(),
        player.clone(),
        cfg.fade.clone(),
    ));
    // End-of-queue continuation radio: register the configured station (a Navidrome
    // station name or an http(s) URL), or None to leave the feature off. The runtime
    // `continuation on|off` toggle (default OFF, persisted) still gates whether it fires.
    handler.set_continuation_station(cfg.continuation.station.clone());

    // The background scrobbler (feature 1) shares the SAME client. The director
    // spine feeds it every player event alongside the inline queue-advance.
    let scrobbler = Arc::new(hypodj_core::scrobble::Scrobbler::new(client.clone()));

    // P1 event/trigger substrate. `director::run` consumes the LOSSLESS player
    // event channel as its single spine consumer (scrobble + advance run inline
    // on it), and re-publishes the strictly-downstream DjEvent stream + the
    // level-triggered QueueSnapshot watch + the wall-clock timer source. The
    // returned runtime is held for the whole daemon lifetime; the P2 executor
    // will subscribe to its lossless edge triggers.
    let mut runtime = hypodj_core::director::run(
        hypodj_core::clock::TokioClock,
        handler.clone(),
        scrobbler.clone(),
        client.clone(),
        player_events,
    );

    // P2 plan executor. It consumes the LOSSLESS edge/WallClock trigger stream +
    // the lossy `Tick` broadcast (for live TimeRemaining arming), owns the shared
    // wall-clock timer source, and maps a fired plan action onto the P0 fade
    // primitive + the handler. Registered on the handler so the `plan` MPD command
    // arms/lists/cancels through it. The task runs for the daemon lifetime.
    {
        use hypodj_core::executor::Executor;
        use hypodj_core::plan::PlanId;
        let triggers = runtime.subscribe_triggers();
        let ticks = runtime.events.subscribe();
        let (imm_tx, imm_rx) = tokio::sync::mpsc::unbounded_channel::<PlanId>();
        handler.set_plan_timers(runtime.timers.clone());
        handler.set_plan_immediate_sink(imm_tx);
        Executor::spawn(
            handler.clone(),
            runtime.timers.clone(),
            hypodj_core::clock::TokioClock,
            triggers,
            ticks,
            imm_rx,
        );
        tracing::info!("P2 plan executor started");
    }

    // P3 OPTIONAL natural-language translator. Injected as a `dyn Translator` so
    // hypodj-core stays model-free. With the `llm` feature off (default) or no
    // model file present, the hybrid degrades to the deterministic Rules path +
    // a loud NotUnderstood (the offline / optional north star). If this injection
    // were skipped, `nl` would ACK NotAvailable.
    {
        use hypodj_core::nl::Translator;
        let translator: Arc<dyn Translator> = Arc::new(hypodj_nl::HybridTranslator::rules_only());
        handler.set_translator(translator);
        tracing::info!("P3 NL translator injected (rules fast-path; model backend cfg-gated)");
    }

    // MPRIS (org.mpris.MediaPlayer2.hypodj) on the session bus: desktops get
    // now-playing + cover art + controls. Registered under the `.hypodj` bus name
    // so it NEVER conflicts with a running mopidy's `.mopidy`. If mpris.enable is
    // false, or there is no session bus (headless / no DBUS_SESSION_BUS_ADDRESS),
    // we log and skip - never fatal, and the MPD serve loop is unaffected.
    if cfg.mpris.enable {
        match hypodj_core::mpris::serve(
            player.clone(),
            handler.clone(),
            client.clone(),
            cfg.mpris.raise_command.clone(),
        )
        .await
        {
            Ok(server) => {
                tracing::info!("MPRIS server on org.mpris.MediaPlayer2.hypodj");
                tokio::spawn(hypodj_core::mpris::run_property_updates(server));
            }
            Err(e) => {
                tracing::warn!(error = %e, "MPRIS unavailable (no session bus?); skipping");
            }
        }
    } else {
        tracing::info!("MPRIS disabled by config");
    }

    // SMOOTH-RESTART wiring. Resolve the persistent state dir: an explicit
    // [restart].state_dir wins, else systemd's $STATE_DIRECTORY (set by
    // StateDirectory=), else resume is disabled (safe cold start). This is NEVER
    // the RuntimeDirectory (/run tmpfs is wiped on stop, defeating SIGKILL
    // resume).
    let state_dir: Option<PathBuf> = cfg
        .restart
        .state_dir
        .clone()
        .or_else(|| {
            std::env::var_os("STATE_DIRECTORY").and_then(|v| {
                // STATE_DIRECTORY may be a colon-separated list; take the first.
                let s = v.to_string_lossy();
                let first = s.split(':').next().unwrap_or("");
                if first.is_empty() {
                    None
                } else {
                    Some(PathBuf::from(first))
                }
            })
        });
    let resume_enabled = state_dir.is_some();
    if let Some(dir) = &state_dir {
        let path = dir.join("resume.toml");
        handler.set_state_path(path.clone());
        // Restore from the last checkpoint BEFORE serving: a Playing snapshot wakes
        // back into playback (queue rebuilt, seek, wake-ramp from silence); a
        // Paused/Stopped snapshot restores the queue + baseline volume and stays
        // stopped. A missing / corrupt / old file is a clean cold start (load()
        // returns None, never panics).
        if let Some(rs) = hypodj_core::resume::load(&path) {
            tracing::info!("resume state found; restoring");
            if let Err(e) = handler.restore(&rs).await {
                tracing::warn!(error = %e, "resume restore failed; continuing cold");
            }
        }
        // Best-effort checkpoint task: persists on state-EDGE events + a coarse
        // periodic elapsed refresh, so even an ungraceful SIGKILL resumes from the
        // last checkpoint.
        tokio::spawn(checkpoint_loop(
            handler.clone(),
            runtime.events.subscribe(),
            cfg.restart.checkpoint_secs.max(1),
            player.clone(),
        ));
    }

    let bind: SocketAddr = cfg.mpd.bind.parse()?;

    // COSMETIC VIZ side-channel: a dedicated socket at MPD_port + 1 streaming the
    // post-gain audio levels to any HUD client (dj-gui). Best-effort and fully out
    // of band - a bind failure logs and is ignored (clients degrade to the
    // decorative wave), and nothing here can touch playback or the MPD server.
    {
        let viz_bind = SocketAddr::new(bind.ip(), bind.port().saturating_add(1));
        let viz_tx = runtime.viz.clone();
        tokio::spawn(async move {
            if let Err(e) = hypodj_core::viz::serve_viz(viz_bind, viz_tx).await {
                tracing::warn!(error = %e, %viz_bind, "viz socket unavailable; clients will use the fallback wave");
            }
        });
    }

    let server = MpdServer::new(bind);
    tracing::info!(%bind, "starting MPD server");
    // The internal shutdown-fade budget tracks the configured shutdown_fade_secs
    // plus a small headroom for the up-front persist, so raising the fade knob
    // does not silently make the budget reject the fade (immediate exit). The
    // systemd TimeoutStopSec must stay comfortably ABOVE this (the nix module
    // sizes it from the same knob + margin).
    let shutdown_budget = Duration::from_secs(cfg.fade.shutdown_fade_secs.saturating_add(3));
    let mut sigterm = tokio::signal::unix::signal(tokio::signal::unix::SignalKind::terminate())?;
    // Serve MPD, but also watch the director spine (if it exits/panics, wind down
    // loudly) AND the termination signals (SIGTERM/SIGINT): on a signal, persist
    // resume state, fade out, then exit(0) - a graceful, click-free shutdown.
    tokio::select! {
        r = server.serve(handler.clone()) => r?,
        joined = runtime.join() => match joined {
            Ok(()) => tracing::error!("director spine exited (player gone); shutting down"),
            Err(e) => {
                tracing::error!(error = %e, "director spine panicked; aborting for a clean restart");
                std::process::abort();
            }
        },
        _ = sigterm.recv() => graceful_shutdown(handler.clone(), resume_enabled, shutdown_budget).await,
        _ = tokio::signal::ctrl_c() => graceful_shutdown(handler.clone(), resume_enabled, shutdown_budget).await,
    }
    Ok(())
}

/// The graceful shutdown path on SIGTERM/SIGINT. ORDERING: PERSIST FIRST (the
/// snapshot is fully known the instant the signal arrives; an up-front atomic
/// write is idempotent with the periodic checkpoint and survives a hung fade / a
/// second SIGKILL), THEN run the bounded sleep-fade-out, THEN `exit(0)`. A SECOND
/// signal or the timeout cuts straight to the exit (already persisted), and the
/// inline fade's own `tokio::time::timeout` guarantees a stuck sink can never
/// block the exit.
async fn graceful_shutdown(handler: Arc<HypodjHandler>, resume_enabled: bool, budget: Duration) -> ! {
    tracing::info!("shutdown signal received; persisting resume state then fading out");
    if resume_enabled {
        handler.checkpoint(handler.last_elapsed_secs()).await;
    }
    // A second signal (or the budget) cuts straight to exit.
    let second = async {
        let mut term =
            match tokio::signal::unix::signal(tokio::signal::unix::SignalKind::terminate()) {
                Ok(s) => s,
                Err(_) => {
                    std::future::pending::<()>().await;
                    unreachable!()
                }
            };
        tokio::select! {
            _ = term.recv() => {},
            _ = tokio::signal::ctrl_c() => {},
        }
    };
    tokio::select! {
        _ = handler.shutdown_fade(budget) => {}
        _ = tokio::time::sleep(budget) => {}
        _ = second => tracing::info!("second shutdown signal; exiting immediately"),
    }
    std::process::exit(0);
}

/// The best-effort resume checkpoint loop. Updates the lockless live-elapsed
/// atomic on every high-rate `Tick`, and writes a full checkpoint on state EDGES
/// (track start/end, play-state change, queue resync) plus a coarse periodic
/// refresh while a track is live. NEVER queries mpv; the elapsed comes from the
/// P1 tick stream, so a checkpoint is race-free against shutdown.
async fn checkpoint_loop(
    handler: Arc<HypodjHandler>,
    mut ev: tokio::sync::broadcast::Receiver<hypodj_core::event::DjEvent>,
    checkpoint_secs: u64,
    player: hypodj_core::player::PlayerHandle,
) {
    use hypodj_core::event::DjEventKind;
    use hypodj_core::player::PlayState;
    use tokio::sync::broadcast::error::RecvError;

    let mut interval = tokio::time::interval(Duration::from_secs(checkpoint_secs));
    interval.set_missed_tick_behavior(tokio::time::MissedTickBehavior::Skip);
    loop {
        tokio::select! {
            r = ev.recv() => match r {
                Ok(dj) => match dj.kind {
                    DjEventKind::Tick { time_pos, .. } => {
                        handler.note_elapsed_ms(time_pos.as_millis() as u64);
                    }
                    DjEventKind::TrackStart(_) => {
                        handler.reset_elapsed();
                        handler.checkpoint(handler.last_elapsed_secs()).await;
                    }
                    DjEventKind::TrackEnd(_) => {
                        handler.checkpoint(handler.last_elapsed_secs()).await;
                    }
                    DjEventKind::StateChanged(st, _) => {
                        if st == PlayState::Stopped {
                            handler.reset_elapsed();
                        }
                        handler.checkpoint(handler.last_elapsed_secs()).await;
                    }
                    DjEventKind::Resync => {
                        handler.checkpoint(handler.last_elapsed_secs()).await;
                    }
                    _ => {}
                },
                // A lagged lossy observer just resyncs on the next event; a closed
                // channel means the director is gone, so the loop ends.
                Err(RecvError::Lagged(_)) => continue,
                Err(RecvError::Closed) => break,
            },
            _ = interval.tick() => {
                // Coarse periodic elapsed refresh, debounced: skip when no track is
                // live (nothing changed worth persisting).
                if player.state() == PlayState::Playing {
                    handler.checkpoint(handler.last_elapsed_secs()).await;
                }
            }
        }
    }
}
