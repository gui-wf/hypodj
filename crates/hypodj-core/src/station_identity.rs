//! Per-track-start STATION IDENTITY resolution (task lq54isr).
//!
//! The gap: a raw internet-radio [`crate::model::QueueEntry::Stream`] often has no
//! inherent name or cover - it plays fine but every surface (currentsong, the dj-gui
//! now-playing pane, the GNOME MPRIS widget) shows the raw URL and no art. Some
//! streams carry no ICY tags at all (the NTS mixtapes), so the sibling ICY overlay
//! (`jmrwr99`) and the on-demand songrec recognition (`f7vnd3i`) cannot name them.
//!
//! This module resolves a station/show IDENTITY (a display name + a cover image) for
//! such a stream from an OUTSIDE catalogue, keyed by the exact stream URL. It is one
//! of three INDEPENDENT writers into the qid-gated `State.station_identity` slot,
//! merged with the ICY (`stream_meta`) and recognized (`recognized_cover`) slots at
//! READ time - so there is no write race between them.
//!
//! The first (and, in slices P1-P3, only) provider is the NTS MIXTAPES catalogue:
//! `GET https://www.nts.live/api/v2/mixtapes` (no auth) returns every mixtape's
//! `audio_stream_endpoint`, `title`, and cover `media`. A playing URL is matched
//! against the endpoints by STRING EQUALITY (scheme-insensitive), never by parsing
//! the `mixtapeN` suffix - the alias-to-number map is non-sequential (the `poolside`
//! mixtape is `mixtape4`), so any arithmetic on the number would mis-identify.
//!
//! The catalogue is STATIC content, so the caller caches it long-TTL and the match
//! itself ([`match_nts_mixtape`]) is a PURE function over the fetched JSON + the URL,
//! unit-tested offline against a captured fixture.

use serde::Deserialize;

/// The NTS mixtapes catalogue endpoint (no auth). Static content; the caller fetches
/// it once behind a long-TTL cache and re-matches subsequent stream URLs against it.
pub const NTS_MIXTAPES_URL: &str = "https://www.nts.live/api/v2/mixtapes";

/// The cache key under which the caller stores the fetched NTS mixtapes catalogue JSON
/// (a single static document, so one fixed key).
pub const NTS_CATALOGUE_CACHE_KEY: &str = "nts/mixtapes";

/// Where a resolved [`StationIdentity`] came from, for provenance / priority. Only the
/// NTS mixtapes provider exists in slices P1-P3; the live-channel and configured-station
/// providers are clean follow-up seams (P4-P6). The mere PRESENCE of a resolved identity
/// means the URL was CLASSIFIED by a provider, so its name outranks a stream's empty
/// ICY name at read time.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum StationIdentitySource {
    /// Matched against the NTS `/api/v2/mixtapes` catalogue by `audio_stream_endpoint`.
    NtsMixtape,
}

/// A resolved station/show identity for a raw stream: a display `name` and a cover
/// `image_url`, either of which may be absent (a provider might name a stream without a
/// cover, or vice versa). Stored in the qid-gated `State.station_identity` slot and
/// merged with the ICY / recognized slots at read time.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct StationIdentity {
    /// The station/show display name (an NTS mixtape's `title`, e.g. "4 To The Floor"),
    /// or `None` when the provider matched but carried no usable name.
    pub name: Option<String>,
    /// A cover-art image URL (an NTS mixtape's 400x400 `picture_medium`, else
    /// `picture_large`), fetched by the existing bounded remote-cover path. `None` when
    /// the provider matched but carried no image.
    pub image_url: Option<String>,
    /// Which provider resolved this identity (provenance; see [`StationIdentitySource`]).
    pub source: StationIdentitySource,
}

/// The NTS `/api/v2/mixtapes` response envelope. Only `results` is read; unknown fields
/// are ignored by serde so a catalogue-shape addition never breaks the parse.
#[derive(Debug, Clone, Deserialize)]
struct NtsMixtapesResponse {
    results: Vec<NtsMixtape>,
}

/// One NTS mixtape row. Only the fields this matcher needs are named; the rest are
/// ignored. All optional so a partial/renamed row degrades to "no identity", never a
/// parse error that drops the whole catalogue.
#[derive(Debug, Clone, Deserialize)]
struct NtsMixtape {
    title: Option<String>,
    audio_stream_endpoint: Option<String>,
    media: Option<NtsMedia>,
}

/// The cover-art URLs on an NTS mixtape's `media`. Prefer `picture_medium` (400x400,
/// the size the existing 2 MiB-capped fetch handles comfortably), falling back to
/// `picture_large`.
#[derive(Debug, Clone, Deserialize)]
struct NtsMedia {
    picture_medium: Option<String>,
    picture_large: Option<String>,
}

/// Cheap host prefilter: is `url` plausibly an NTS stream, so the mixtapes catalogue is
/// worth fetching for it? The mixtape stream endpoints live on the `ntslive.net` domain
/// (e.g. `stream-mixtape-geo.ntslive.net/mixtape5`), so this gates the network call to
/// NTS-shaped URLs and never fetches the catalogue for an arbitrary stream.
pub fn is_nts_stream_url(url: &str) -> bool {
    url.contains("ntslive.net")
}

/// Match a playing stream `url` against the NTS mixtapes catalogue JSON, returning the
/// resolved [`StationIdentity`] on a hit or `None` on a miss / parse failure. PURE (no
/// network) so the match is unit-tested offline against a captured fixture. Matching is
/// STRING EQUALITY on the full URL (scheme-insensitive, trailing-slash-insensitive),
/// never `mixtapeN`-suffix arithmetic (the alias-to-number map is non-sequential).
pub fn match_nts_mixtape(catalogue_json: &str, url: &str) -> Option<StationIdentity> {
    let parsed: NtsMixtapesResponse = serde_json::from_str(catalogue_json).ok()?;
    let want = normalize_url(url);
    let m = parsed
        .results
        .iter()
        .find(|m| m.audio_stream_endpoint.as_deref().is_some_and(|e| normalize_url(e) == want))?;
    let image_url = m
        .media
        .as_ref()
        .and_then(|md| md.picture_medium.clone().or_else(|| md.picture_large.clone()))
        .filter(|s| !s.trim().is_empty());
    Some(StationIdentity {
        name: m.title.clone().filter(|t| !t.trim().is_empty()),
        image_url,
        source: StationIdentitySource::NtsMixtape,
    })
}

/// Normalize a stream URL for equality: strip an `http(s)://` scheme and a trailing
/// slash so a queued `https://.../mixtape5` matches a catalogue `https://.../mixtape5`
/// even if one side lacks the scheme or carries a trailing slash. Kept minimal - the
/// path is what identifies the mixtape.
fn normalize_url(u: &str) -> &str {
    let u = u.trim();
    let u = u.strip_prefix("https://").or_else(|| u.strip_prefix("http://")).unwrap_or(u);
    u.trim_end_matches('/')
}

#[cfg(test)]
mod tests {
    use super::*;

    /// A trimmed but STRUCTURALLY REAL capture of `GET /api/v2/mixtapes` (two rows,
    /// including the live-verified `mixtape5` -> "4 To The Floor"), so the match test is
    /// offline-deterministic. The `poolside` alias maps to `mixtape4` - a NON-sequential
    /// alias-to-number map that would break any `mixtapeN`-suffix arithmetic.
    const NTS_FIXTURE: &str = r#"{
      "metadata": { "credits": [] },
      "results": [
        {
          "mixtape_alias": "poolside",
          "title": "Poolside",
          "audio_stream_endpoint": "https://stream-mixtape-geo.ntslive.net/mixtape4",
          "media": {
            "picture_medium": "https://media.ntslive.co.uk/resize/400x400/poolside.jpeg",
            "picture_large": "https://media2.ntslive.co.uk/resize/1600x1600/poolside.jpeg"
          }
        },
        {
          "mixtape_alias": "4-to-the-floor",
          "title": "4 To The Floor",
          "audio_stream_endpoint": "https://stream-mixtape-geo.ntslive.net/mixtape5",
          "media": {
            "picture_medium": "https://media.ntslive.co.uk/resize/400x400/fourtothefloor.jpeg",
            "picture_large": "https://media2.ntslive.co.uk/resize/1600x1600/fourtothefloor.jpeg"
          }
        }
      ],
      "links": []
    }"#;

    #[test]
    fn nts_mixtape_url_resolves_name_and_image() {
        let id = match_nts_mixtape(NTS_FIXTURE, "https://stream-mixtape-geo.ntslive.net/mixtape5")
            .expect("mixtape5 must match its catalogue row");
        assert_eq!(id.name.as_deref(), Some("4 To The Floor"));
        assert_eq!(
            id.image_url.as_deref(),
            Some("https://media.ntslive.co.uk/resize/400x400/fourtothefloor.jpeg"),
            "prefers picture_medium (400x400)"
        );
        assert_eq!(id.source, StationIdentitySource::NtsMixtape);
    }

    #[test]
    fn nts_mixtape_match_is_scheme_and_slash_insensitive() {
        // A queued URL without the scheme / with a trailing slash still matches the
        // catalogue's https endpoint - normalization keys on the path.
        let id = match_nts_mixtape(NTS_FIXTURE, "stream-mixtape-geo.ntslive.net/mixtape5/")
            .expect("scheme-less, trailing-slash URL must still match");
        assert_eq!(id.name.as_deref(), Some("4 To The Floor"));
    }

    #[test]
    fn nts_mixtape_match_is_string_equality_not_alias_arithmetic() {
        // `poolside` is mixtape4, "4 To The Floor" is mixtape5: a non-sequential map. A
        // string match on the endpoint returns the RIGHT row (poolside), never one
        // derived by treating the number as an alias index.
        let id = match_nts_mixtape(NTS_FIXTURE, "https://stream-mixtape-geo.ntslive.net/mixtape4")
            .expect("mixtape4 must match the poolside row");
        assert_eq!(id.name.as_deref(), Some("Poolside"));
    }

    #[test]
    fn non_matching_url_yields_no_identity() {
        assert!(match_nts_mixtape(NTS_FIXTURE, "https://example.com/some-other-stream").is_none());
        // A garbage catalogue is a clean miss, never a panic.
        assert!(match_nts_mixtape("not json", "https://stream-mixtape-geo.ntslive.net/mixtape5").is_none());
    }

    #[test]
    fn is_nts_stream_url_gates_the_fetch() {
        assert!(is_nts_stream_url("https://stream-mixtape-geo.ntslive.net/mixtape5"));
        assert!(!is_nts_stream_url("https://ice.somafm.com/groovesalad"));
    }

    /// LIVE proof against the REAL NTS mixtapes catalogue (network; run manually with
    /// `cargo test -p hypodj-core -- --ignored nts_mixtapes_live`). Proves the endpoint
    /// shape this module parses is still current: mixtape5 resolves to a non-empty name
    /// and an https image URL. Ignored by default so the offline suite stays hermetic.
    #[tokio::test]
    #[ignore = "hits the live NTS /api/v2/mixtapes endpoint"]
    async fn nts_mixtapes_live_resolves_mixtape5() {
        let client = reqwest::Client::builder()
            .timeout(std::time::Duration::from_secs(5))
            .build()
            .expect("build client");
        let json = client
            .get(NTS_MIXTAPES_URL)
            .send()
            .await
            .expect("fetch catalogue")
            .text()
            .await
            .expect("read body");
        let id = match_nts_mixtape(&json, "https://stream-mixtape-geo.ntslive.net/mixtape5")
            .expect("mixtape5 must still match the live catalogue");
        assert!(id.name.is_some_and(|n| !n.trim().is_empty()), "live mixtape5 has a name");
        assert!(
            id.image_url.is_some_and(|u| u.starts_with("https://")),
            "live mixtape5 has an https image"
        );
    }
}
