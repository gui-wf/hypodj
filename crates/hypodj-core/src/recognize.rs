//! On-demand now-playing RECOGNITION for raw streams (task f7vnd3i).
//!
//! Sibling jmrwr99 surfaces a stream's ICY `icy-name`/`icy-title` into MPD
//! `Name`/`Title`. But some real radio streams (the NTS mixtapes) carry NO ICY at
//! all, so the now-playing text must come from OUTSIDE the stream. This module
//! fingerprints a short SIDE-BAND capture of the SAME stream URL with `songrec`
//! (open-source Shazam) and returns the recognized artist / title / album / cover
//! art, station-agnostic and with ZERO interference to the playing libmpv
//! instance.
//!
//! Two honest subprocess steps, both async (`tokio::process`, so the child I/O
//! never blocks the reactor and a timeout can actually KILL the child):
//! 1. `ffmpeg` captures ~11s of the stream URL to a temp mono 16 kHz wav.
//! 2. `songrec recognize --json <wav>` fingerprints the wav, queries Shazam, and
//!    prints ONE line of JSON to stdout (empty stdout + exit 0 = no match).
//!
//! Both tools are put on `PATH` by the nix wrapper (see `nix/package.nix`), so the
//! feature is self-contained. The temp wav is removed in EVERY branch (RAII guard),
//! and every child is `kill_on_drop` so a timeout leaves no orphan process.

use std::path::{Path, PathBuf};
use std::sync::atomic::{AtomicU64, Ordering};
use std::time::Duration;

/// Total wall-clock ceiling for one capture + recognition. Three nested guards keep
/// a hung endpoint from ever WEDGING the `identify` trigger: `ffmpeg -t 11`
/// self-terminates a healthy capture, the `ffmpeg -rw_timeout` (see
/// [`FFMPEG_RW_TIMEOUT_US`]) self-aborts a STALLED stream read well before this
/// bound, and this outer `tokio::time::timeout` is the last resort. On elapse the
/// in-flight child future is dropped, which `kill_on_drop` turns into a real
/// SIGKILL of the `ffmpeg`/`songrec` child (no orphan survives), and the temp wav is
/// cleaned by the RAII guard - only THEN does the async call return and release the
/// caller's in-flight guard, so a later `identify` still runs on a clean slate.
const RECOGNIZE_TIMEOUT: Duration = Duration::from_secs(40);

/// Per-operation I/O ceiling handed to `ffmpeg` as `-rw_timeout` (microseconds): a
/// stream whose socket read/connect stalls for this long self-aborts the capture,
/// so the common "endpoint went silent" case never has to wait for the outer
/// [`RECOGNIZE_TIMEOUT`]. Well under that bound (15s vs 40s) and comfortably above
/// the ~11s a healthy realtime capture takes.
const FFMPEG_RW_TIMEOUT_US: &str = "15000000";

/// A monotonic per-process counter mixed into the temp-file name alongside the pid,
/// so two captures can never collide on the same path (the in-flight guard already
/// serializes them within a process, but the counter is a cheap second belt).
static TEMP_COUNTER: AtomicU64 = AtomicU64::new(0);

/// The fields a recognized track carries into the now-playing surface. Every field
/// is `Option` so a partial Shazam hit (title but no album, say) is honest rather
/// than fabricated. Produced by [`parse_recognize_json`] from the `songrec` output.
#[derive(Debug, Clone, PartialEq, Eq, Default)]
pub struct RecognizedTrack {
    /// The performing artist (`track.subtitle` in the Shazam JSON).
    pub artist: Option<String>,
    /// The track title (`track.title`).
    pub title: Option<String>,
    /// The album name, read from the `SONG` section's `Album` metadata row.
    pub album: Option<String>,
    /// The Shazam/Apple cover-art HTTPS URL (prefers the HQ variant). A remote URL,
    /// not local bytes; surfaced toward the dj-gui art pane as an extension field.
    pub cover_url: Option<String>,
}

/// Why a recognition attempt failed at the SUBPROCESS layer (as opposed to a clean
/// no-match, which is `Ok(None)`). Kept distinct so the handler can ACK an honest
/// error versus a plain "no match".
#[derive(Debug)]
pub enum RecognizeError {
    /// A tool could not be spawned or exec'd (e.g. missing from `PATH`). Carries the
    /// tool name and the underlying io error.
    Spawn(&'static str, std::io::Error),
    /// `ffmpeg` ran but exited non-zero (the stream URL was unreachable / not
    /// capturable, or its `-rw_timeout` fired on a stalled read).
    Capture,
    /// The whole capture+recognition exceeded [`RECOGNIZE_TIMEOUT`]; the child was
    /// killed on the way out.
    Timeout,
}

impl std::fmt::Display for RecognizeError {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            RecognizeError::Spawn(tool, e) => write!(f, "could not run {tool}: {e}"),
            RecognizeError::Capture => write!(f, "stream capture failed"),
            RecognizeError::Timeout => write!(f, "recognition timed out"),
        }
    }
}

impl std::error::Error for RecognizeError {}

/// Removes its temp wav on drop, in EVERY branch (ok / err / panic / timeout), so a
/// recognition never leaves litter in the temp dir. Removal is best-effort (`let _`)
/// because a missing file (ffmpeg never wrote it) is not an error worth surfacing.
struct TempFileGuard(PathBuf);

impl Drop for TempFileGuard {
    fn drop(&mut self) {
        let _ = std::fs::remove_file(&self.0);
    }
}

/// A unique temp path for one capture: `hypodj-songrec-<pid>-<counter>.wav`, in the
/// system temp dir (mirrors the viz-probe temp pattern in `player.rs`).
fn temp_wav_path() -> PathBuf {
    let n = TEMP_COUNTER.fetch_add(1, Ordering::Relaxed);
    std::env::temp_dir().join(format!("hypodj-songrec-{}-{}.wav", std::process::id(), n))
}

/// The subprocess half: capture the stream with `ffmpeg`, then fingerprint the wav
/// with `songrec recognize --json`, returning songrec's raw stdout. Uses
/// `tokio::process` so the child I/O rides the reactor (never blocks it) and both
/// children carry `kill_on_drop(true)` - so if the awaiting future is dropped (the
/// [`RECOGNIZE_TIMEOUT`] path in [`recognize_stream_url`]) the in-flight child is
/// SIGKILLed rather than orphaned. Every subprocess uses `Stdio::null()` for stdin
/// so it can never block waiting on input.
///
/// On a clean no-match, `songrec` exits 0 with EMPTY stdout (it prints "No match"
/// to stderr), so this returns `Ok("")` and the parser maps empty -> `None`. A
/// non-zero songrec exit is NOT treated as a hard error here (an empty/garbage
/// stdout still parses to `None`); only a spawn/exec failure is.
async fn capture_and_recognize(url: &str, wav: &Path) -> Result<String, RecognizeError> {
    use std::process::Stdio;
    use tokio::process::Command;

    // 1. SIDE-BAND capture: re-fetch the SAME stream URL to a bounded temp wav. 11s
    // mono 16 kHz is plenty for a Shazam fingerprint and does not touch the playing
    // libmpv instance. `-nostdin` + null stdin so ffmpeg never waits on the tty;
    // `-rw_timeout` self-aborts a stalled read (see FFMPEG_RW_TIMEOUT_US).
    let capture = Command::new("ffmpeg")
        .args(["-nostdin", "-loglevel", "error", "-rw_timeout", FFMPEG_RW_TIMEOUT_US, "-y", "-i"])
        .arg(url)
        .args(["-t", "11", "-ac", "1", "-ar", "16000", "-f", "wav"])
        .arg(wav)
        .stdin(Stdio::null())
        .stdout(Stdio::null())
        .stderr(Stdio::null())
        .kill_on_drop(true)
        .status()
        .await
        .map_err(|e| RecognizeError::Spawn("ffmpeg", e))?;
    if !capture.success() {
        return Err(RecognizeError::Capture);
    }

    // 2. Headless recognition: songrec prints ONE line of JSON on a match, empty on
    // no-match. Capture stdout; stderr is discarded (the no-match message lives
    // there). Null stdin so it never blocks.
    let out = Command::new("songrec")
        .args(["recognize", "--json"])
        .arg(wav)
        .stdin(Stdio::null())
        .stderr(Stdio::null())
        .kill_on_drop(true)
        .output()
        .await
        .map_err(|e| RecognizeError::Spawn("songrec", e))?;
    Ok(String::from_utf8_lossy(&out.stdout).into_owned())
}

/// Recognize the currently-playing audio at `url` via the side-band capture +
/// `songrec` pipeline. `Ok(None)` is a clean NO MATCH (the honest common case for a
/// niche stream); `Ok(Some(_))` is a hit; `Err(_)` is a subprocess/timeout failure.
///
/// ASYNC/LOCK DISCIPLINE: the caller reads the stream URL under the std state lock
/// and DROPS the lock before calling this (no lock is held across the await here).
/// The heavy work is one async subprocess pair bounded by [`RECOGNIZE_TIMEOUT`] so a
/// hung Shazam call cannot wedge the trigger; on elapse the child future is dropped
/// and `kill_on_drop` reaps the child (no orphan). The temp wav is cleaned in every
/// branch by [`TempFileGuard`].
pub async fn recognize_stream_url(url: String) -> Result<Option<RecognizedTrack>, RecognizeError> {
    let wav = temp_wav_path();
    run_bounded(wav.clone(), RECOGNIZE_TIMEOUT, capture_and_recognize(&url, &wav)).await
}

/// Bound `work` (the capture+recognize future) by `timeout`, cleaning `wav` on EVERY
/// exit via [`TempFileGuard`] - including the timeout branch, where dropping `work`
/// also `kill_on_drop`-reaps the in-flight child. Split out from
/// [`recognize_stream_url`] so the timeout + cleanup wiring is unit-testable with a
/// synthetic `work` future (no real hung stream needed).
async fn run_bounded(
    wav: PathBuf,
    timeout: Duration,
    work: impl std::future::Future<Output = Result<String, RecognizeError>>,
) -> Result<Option<RecognizedTrack>, RecognizeError> {
    // RAII: removes the wav on EVERY exit path below (including the timeout branch,
    // where `work` is dropped - killing its child - but this guard still unlinks it).
    let _guard = TempFileGuard(wav);
    let stdout = match tokio::time::timeout(timeout, work).await {
        Ok(Ok(stdout)) => stdout,
        Ok(Err(e)) => return Err(e),
        Err(_elapsed) => return Err(RecognizeError::Timeout),
    };
    Ok(parse_recognize_json(&stdout))
}

// ── the songrec JSON shape (only the fields the mapper needs) ────────────────

/// The top-level `songrec recognize --json` object. Everything is optional so a
/// reshaped or partial payload degrades to `None` fields rather than a parse error.
#[derive(serde::Deserialize)]
struct RecognizeResponse {
    track: Option<TrackJson>,
}

#[derive(serde::Deserialize)]
struct TrackJson {
    /// The track title.
    title: Option<String>,
    /// The performing artist (Shazam names this `subtitle`).
    subtitle: Option<String>,
    /// Metadata sections; the one whose `type == "SONG"` holds the Album/Label/
    /// Released rows.
    #[serde(default)]
    sections: Vec<SectionJson>,
    /// Cover-art URLs.
    images: Option<ImagesJson>,
}

#[derive(serde::Deserialize)]
struct SectionJson {
    #[serde(rename = "type")]
    section_type: Option<String>,
    #[serde(default)]
    metadata: Vec<MetaJson>,
}

#[derive(serde::Deserialize)]
struct MetaJson {
    /// The row label ("Album" / "Label" / "Released").
    title: Option<String>,
    /// The row value.
    text: Option<String>,
}

#[derive(serde::Deserialize)]
struct ImagesJson {
    coverart: Option<String>,
    coverarthq: Option<String>,
}

/// Trim a value, returning `None` for an empty/whitespace-only string so a blank
/// Shazam field never becomes a visible label.
fn non_blank(s: Option<String>) -> Option<String> {
    s.filter(|v| !v.trim().is_empty())
}

/// Parse one line of `songrec recognize --json` stdout into a [`RecognizedTrack`].
///
/// Returns `None` for the two non-hit cases the daemon must treat identically to a
/// no-match: EMPTY/whitespace stdout (songrec's clean no-match, exit 0), and
/// GARBAGE/malformed JSON (never a panic). A hit requires at least a title or an
/// artist; a `track` object with neither is treated as no-match.
pub fn parse_recognize_json(stdout: &str) -> Option<RecognizedTrack> {
    let trimmed = stdout.trim();
    if trimmed.is_empty() {
        // songrec no-match: exit 0, empty stdout, "No match" on stderr.
        return None;
    }
    // Malformed / unexpected JSON degrades to no-match, never an error or panic.
    let resp: RecognizeResponse = serde_json::from_str(trimmed).ok()?;
    let track = resp.track?;

    // Album = the `text` of the "Album" row inside the SONG-typed section.
    let album = track
        .sections
        .iter()
        .filter(|s| s.section_type.as_deref() == Some("SONG"))
        .flat_map(|s| &s.metadata)
        .find(|m| m.title.as_deref() == Some("Album"))
        .and_then(|m| non_blank(m.text.clone()));

    // Prefer the HQ cover, fall back to the standard one.
    let cover_url = track
        .images
        .and_then(|i| non_blank(i.coverarthq).or_else(|| non_blank(i.coverart)));

    let title = non_blank(track.title);
    let artist = non_blank(track.subtitle);
    if title.is_none() && artist.is_none() {
        // A track object with no usable text is not a real hit.
        return None;
    }
    Some(RecognizedTrack { artist, title, album, cover_url })
}

/// The now-playing `Title` line for a recognized track, mirroring the ICY
/// "Artist - Track" convention so it rides the exact same MPD `Title` surface as a
/// real icy-title (see `apply_stream_meta`). Falls back to whichever half is
/// present; `None` only when neither artist nor title exists.
pub fn now_playing_title(track: &RecognizedTrack) -> Option<String> {
    match (&track.artist, &track.title) {
        (Some(a), Some(t)) => Some(format!("{a} - {t}")),
        (None, Some(t)) => Some(t.clone()),
        (Some(a), None) => Some(a.clone()),
        (None, None) => None,
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    /// A trimmed-down but STRUCTURALLY REAL Shazam payload (the shape verified live
    /// against songrec 0.4.3 during the feasibility investigation): the fields the
    /// mapper reads, in the nesting songrec actually emits.
    const REAL_HIT: &str = r#"{
      "track": {
        "title": "Blessings",
        "subtitle": "Calvin Harris & Clementine Douglas",
        "sections": [
          {
            "type": "SONG",
            "metadata": [
              { "title": "Album", "text": "Blessings" },
              { "title": "Label", "text": "Columbia" },
              { "title": "Released", "text": "2024" }
            ]
          },
          { "type": "LYRICS", "text": ["la la"] }
        ],
        "images": {
          "coverart": "https://is1.example/400x400.jpg",
          "coverarthq": "https://is1.example/hq.jpg"
        },
        "key": "12345",
        "share": { "subject": "Blessings - Calvin Harris & Clementine Douglas" }
      }
    }"#;

    #[test]
    fn parse_recognize_json_extracts_fields() {
        let t = parse_recognize_json(REAL_HIT).expect("a hit");
        assert_eq!(t.title.as_deref(), Some("Blessings"));
        assert_eq!(t.artist.as_deref(), Some("Calvin Harris & Clementine Douglas"));
        assert_eq!(t.album.as_deref(), Some("Blessings"));
        // Prefers the HQ cover URL over the standard one.
        assert_eq!(t.cover_url.as_deref(), Some("https://is1.example/hq.jpg"));
    }

    #[test]
    fn parse_recognize_no_match_is_none() {
        // songrec's clean no-match: exit 0 with empty stdout. Also a whitespace-only
        // line must map to no-match, never an error.
        assert_eq!(parse_recognize_json(""), None);
        assert_eq!(parse_recognize_json("   \n  "), None);
    }

    #[test]
    fn parse_recognize_malformed_is_none() {
        // Garbage stdout must degrade to no-match gracefully, never panic or error.
        assert_eq!(parse_recognize_json("not json at all"), None);
        assert_eq!(parse_recognize_json("{ \"track\": "), None);
        // A well-formed object with no `track` is no-match.
        assert_eq!(parse_recognize_json("{}"), None);
        // A `track` with neither title nor artist is not a real hit.
        assert_eq!(parse_recognize_json(r#"{"track":{"images":{}}}"#), None);
    }

    #[test]
    fn parse_recognize_blank_fields_drop_out() {
        // Whitespace-only Shazam fields must not become visible labels.
        let json = r#"{"track":{"title":"Yelle","subtitle":"   ","images":{"coverarthq":"  "}}}"#;
        let t = parse_recognize_json(json).expect("title alone is a hit");
        assert_eq!(t.title.as_deref(), Some("Yelle"));
        assert_eq!(t.artist, None);
        assert_eq!(t.cover_url, None);
        assert_eq!(t.album, None);
    }

    #[test]
    fn temp_file_guard_unlinks_on_drop() {
        // The RAII guard must remove its wav on drop, in every branch. Write a real
        // file, drop the guard, and confirm it is gone.
        let path = temp_wav_path();
        std::fs::write(&path, b"wav").unwrap();
        assert!(path.exists());
        {
            let _guard = TempFileGuard(path.clone());
        }
        assert!(!path.exists(), "guard must unlink the temp wav on drop");
    }

    #[tokio::test(start_paused = true)]
    async fn run_bounded_timeout_kills_and_cleans() {
        // On timeout, run_bounded must (a) surface RecognizeError::Timeout and (b)
        // still unlink the temp wav via the RAII guard, even though `work` never
        // resolved. A never-completing `work` stands in for a hung stream; the
        // paused clock auto-advances past the timeout without real waiting. (The
        // kill_on_drop of a real child is a tokio guarantee exercised by the live
        // proof; here we pin the wiring: elapse -> Timeout + temp cleaned.)
        let path = temp_wav_path();
        std::fs::write(&path, b"wav").unwrap();
        assert!(path.exists());
        let work = async {
            tokio::time::sleep(Duration::from_secs(3600)).await;
            Ok(String::new())
        };
        let res = run_bounded(path.clone(), Duration::from_secs(40), work).await;
        assert!(matches!(res, Err(RecognizeError::Timeout)));
        assert!(!path.exists(), "temp wav must be cleaned on the timeout path");
    }

    #[tokio::test]
    async fn run_bounded_passes_hit_through() {
        // The success path: a `work` that resolves in time parses into a hit and the
        // temp wav is still cleaned afterward.
        let path = temp_wav_path();
        std::fs::write(&path, b"wav").unwrap();
        let hit = REAL_HIT.to_string();
        let work = async move { Ok(hit) };
        let res = run_bounded(path.clone(), Duration::from_secs(40), work).await;
        let track = res.expect("no error").expect("a hit");
        assert_eq!(track.title.as_deref(), Some("Blessings"));
        assert!(!path.exists(), "temp wav must be cleaned on the success path");
    }

    #[test]
    fn now_playing_title_follows_icy_convention() {
        let full = RecognizedTrack {
            artist: Some("Yelle".into()),
            title: Some("Qui est cette fille?".into()),
            ..Default::default()
        };
        assert_eq!(now_playing_title(&full).as_deref(), Some("Yelle - Qui est cette fille?"));

        let title_only = RecognizedTrack { title: Some("Just A Title".into()), ..Default::default() };
        assert_eq!(now_playing_title(&title_only).as_deref(), Some("Just A Title"));

        let artist_only = RecognizedTrack { artist: Some("Just An Artist".into()), ..Default::default() };
        assert_eq!(now_playing_title(&artist_only).as_deref(), Some("Just An Artist"));

        assert_eq!(now_playing_title(&RecognizedTrack::default()), None);
    }
}
