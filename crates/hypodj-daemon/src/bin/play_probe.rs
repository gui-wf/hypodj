//! The Phase-1 LIVE + HEADLESS playback prover.
//!
//! Extends the `probe` slice all the way through real audio decode:
//!
//!   1. load config (TOML)
//!   2. authenticate + ping
//!   3. browse: artists + newest albums (REAL wire->model mapping)
//!   4. list the first album's songs (REAL get_album) and pick a track
//!   5. resolve that track's stream URL
//!   6. hand the URL to the REAL MpvPlayer, configured for FILE output
//!      (AudioOut::File -> mpv encodes decoded PCM to a WAV). Play a few
//!      seconds, then stop.
//!   7. assert the WAV grew to a non-trivial size (real bytes decoded) and that
//!      mpv reported Playing.
//!
//! HARD CONSTRAINT honored: audio goes to a FILE, never the speakers. No device
//! is ever opened. mopidy/6600 are untouched.
//!
//! Usage: cargo run -j2 --bin play-probe -- <config.toml> [out.wav] [seconds]

use std::time::Duration;

use opensubsonic::AlbumListType;
use hypodj_core::config::Config;
use hypodj_core::model::{AlbumId, SongId};
use hypodj_core::player::{AudioOut, MpvPlayer, PlayState};
use hypodj_core::subsonic::SubsonicClient;

#[tokio::main(flavor = "multi_thread", worker_threads = 2)]
async fn main() -> anyhow::Result<()> {
    let mut args = std::env::args().skip(1);
    let cfg_path = args.next().unwrap_or_else(|| "hypodj.toml".into());
    let out_path = args.next().unwrap_or_else(|| "/tmp/hypodj-play-probe.wav".into());
    let secs: u64 = args.next().and_then(|s| s.parse().ok()).unwrap_or(6);

    let cfg = Config::load(std::path::Path::new(&cfg_path))?;
    println!("[1/7] config loaded: server={}", cfg.server.url);

    let client = SubsonicClient::connect(&cfg.server)?;
    client.ping().await?;
    println!("[2/7] ping OK (authenticated)");

    let artists = client.artists().await?;
    let albums = client.album_list(AlbumListType::Newest, Some(20)).await?;
    println!(
        "[3/7] browse OK: {} artists, {} albums (newest)",
        artists.len(),
        albums.len()
    );
    if let Some(a) = artists.first() {
        println!("      sample artist: {:?} ({} albums)", a.name, a.album_count);
    }

    // Pick the first album and list its songs (real get_album).
    let album = albums.first().cloned().ok_or_else(|| {
        anyhow::anyhow!("server returned no albums; cannot pick a track to play")
    })?;
    let songs = client.album_songs(&AlbumId(album.id.0.clone())).await?;
    println!(
        "[4/7] album {:?} by {:?} -> {} songs",
        album.name,
        album.artist,
        songs.len()
    );
    let song = songs.first().cloned().ok_or_else(|| {
        anyhow::anyhow!("album {:?} has no listable songs", album.name)
    })?;
    println!(
        "      picked track: {:?}{}",
        song.title,
        song.duration_secs.map(|d| format!(" ({d}s)")).unwrap_or_default()
    );

    // Resolve the stream URL (carries auth in the query; do not print the query).
    let url = client.stream_url(&SongId(song.id.0.clone()))?;
    let safe = format!(
        "{}://{}{}",
        url.scheme(),
        url.host_str().unwrap_or("?"),
        url.path()
    );
    println!("[5/7] stream URL resolved ({safe}?<redacted-auth>)");

    // Real player, FILE output. Nothing hits the speakers.
    let _ = std::fs::remove_file(&out_path);
    let (player, mut events) =
        MpvPlayer::spawn(AudioOut::File(std::path::PathBuf::from(&out_path)));

    player
        .play_url(SongId(song.id.0.clone()), url.as_str())
        .await?;
    println!("[6/7] play_url issued; mpv state = {:?}", player.state());

    // Let it decode for a few seconds. Meanwhile, drain events so we can report
    // a real TimePos observation (proof mpv is actually advancing).
    let mut last_pos = 0.0_f64;
    let deadline = tokio::time::Instant::now() + Duration::from_secs(secs);
    loop {
        tokio::select! {
            _ = tokio::time::sleep_until(deadline) => break,
            ev = events.recv() => {
                match ev {
                    Some(hypodj_core::player::PlayerEvent::TimePos(t)) => last_pos = t,
                    Some(hypodj_core::player::PlayerEvent::Eof(_)) => {
                        println!("      track reached EOF before deadline");
                        break;
                    }
                    Some(_) => {}
                    None => break,
                }
            }
        }
    }

    let playing = player.state() == PlayState::Playing;
    player.stop().await?;
    // Give mpv's encoder a moment to flush + finalize the WAV on stop.
    tokio::time::sleep(Duration::from_millis(500)).await;

    let size = std::fs::metadata(&out_path).map(|m| m.len()).unwrap_or(0);
    println!(
        "[7/7] stopped. mpv was Playing={playing}, last time-pos={last_pos:.2}s, \
         captured WAV = {size} bytes at {out_path}"
    );

    // Proof gate: a WAV header alone is ~44 bytes; anything meaningfully larger
    // means real decoded PCM landed on disk. 6s of 16-bit stereo @44.1k is ~1MB,
    // so require at least 100 KiB to be safe against a stall.
    const MIN_REAL_BYTES: u64 = 100 * 1024;
    if !playing {
        anyhow::bail!("mpv never reached Playing state - playback did NOT work");
    }
    if size < MIN_REAL_BYTES {
        anyhow::bail!(
            "captured WAV is only {size} bytes (< {MIN_REAL_BYTES}); \
             no real audio was decoded"
        );
    }
    println!(
        "PROOF: playback is REAL - {size} bytes of decoded PCM captured headless (no speakers)."
    );
    Ok(())
}
