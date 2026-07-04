//! The REAL vertical slice, proving the foundation end-to-end against a LIVE
//! OpenSubsonic server. This is the "test with real inference, not mocks" bar:
//!
//!   1. load config (TOML)
//!   2. authenticate + ping
//!   3. browse: artists + album list
//!   4. resolve a stream URL for a given song id
//!
//! Usage: cargo run -j2 --bin probe -- <config.toml> <song-id>
//! (No audio device touched; step 4 stops at the resolved URL, which is the
//! handoff point to the mpv player - a thin, clearly-scoped next step.)

use opensubsonic::AlbumListType;
use subsonity_core::config::Config;
use subsonity_core::model::SongId;
use subsonity_core::subsonic::SubsonicClient;

#[tokio::main(flavor = "multi_thread", worker_threads = 2)]
async fn main() -> anyhow::Result<()> {
    let mut args = std::env::args().skip(1);
    let cfg_path = args.next().unwrap_or_else(|| "subsonity.toml".into());
    let song_id = args.next().unwrap_or_else(|| "1".into());

    let cfg = Config::load(std::path::Path::new(&cfg_path))?;
    println!("[1/4] config loaded: server={}", cfg.server.url);

    let client = SubsonicClient::connect(&cfg.server)?;
    client.ping().await?;
    println!("[2/4] ping OK (authenticated)");

    let artists = client.artists().await?;
    let albums = client.album_list(AlbumListType::Newest, Some(20)).await?;
    println!(
        "[3/4] browse OK: {} artists, {} albums (newest)",
        artists.len(),
        albums.len()
    );
    if let Some(a) = artists.first() {
        println!("      sample artist: {:?} ({} albums)", a.name, a.album_count);
    }
    if let Some(al) = albums.first() {
        println!("      sample album:  {:?} by {:?}", al.name, al.artist);
    }

    // The resolved stream URL carries the auth token in its query string, so we
    // deliberately print ONLY that a URL was obtained plus its non-secret
    // scheme+host+path - never the query (which holds salt+token).
    let url = client.stream_url(&SongId(song_id.clone()))?;
    let safe = format!(
        "{}://{}{}",
        url.scheme(),
        url.host_str().unwrap_or("?"),
        url.path()
    );
    println!("[4/4] stream URL for song {song_id}: obtained ({safe}?<redacted-auth>)");

    Ok(())
}
