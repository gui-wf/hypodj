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
                println!("hypodj {}", env!("CARGO_PKG_VERSION"));
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

    let bind: SocketAddr = cfg.mpd.bind.parse()?;
    let server = MpdServer::new(bind);
    tracing::info!(%bind, "starting MPD server");
    // Serve MPD, but also watch the director spine: if it exits (player channel
    // closed) or panics, wind the daemon down loudly so systemd restarts cleanly
    // rather than leaving a silent event-less zombie.
    tokio::select! {
        r = server.serve(handler) => r?,
        joined = runtime.join() => match joined {
            Ok(()) => tracing::error!("director spine exited (player gone); shutting down"),
            Err(e) => {
                tracing::error!(error = %e, "director spine panicked; aborting for a clean restart");
                std::process::abort();
            }
        },
    }
    Ok(())
}
