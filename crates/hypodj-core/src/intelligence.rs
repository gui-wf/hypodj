//! P4 content-intelligence: the pure, network-free half of the mood/energy layer.
//!
//! This module is the honest split the P4 design demands:
//!
//! - [`energy_score`] / [`valence_score`] are DOCUMENTED HEURISTICS, not acoustic
//!   measurements. They rank a [`Song`] on the only mood-bearing scalar the current
//!   model carries (genre; year as a weak tiebreak) against a static lexicon. The
//!   `Calmer` selector's re-rank is honest exactly as far as this lexicon is.
//! - [`FeatureStore`] + [`MetadataStore`] + [`cosine_similarity`] are the durable
//!   SEAM. The heavy offline path (Essentia batch feature extraction -> a
//!   precomputed per-song-id feature store returning real embeddings -> a
//!   usearch/hnsw ANN index that re-ranks `Similar` by cosine over embeddings) is an
//!   OPS batch that lives OUTSIDE the daemon sandbox. It slots in behind this trait
//!   WITHOUT touching the wire or the selector logic. It is documented here, not
//!   stubbed inside the daemon.
//!
//! Everything here is PURE: no clock, no I/O, no network, no model, no lock. Fully
//! deterministic and unit-testable.

use crate::model::Song;

/// Per-track mood/energy features. `energy` is the load-bearing v1 signal
/// ([`energy_score`]); `valence` is a second small proxy ([`valence_score`]);
/// `embedding` is `None` for the metadata-only [`MetadataStore`] and `Some(_)`
/// only for the future Essentia-backed store (the ANN re-rank input).
#[derive(Debug, Clone, PartialEq)]
pub struct TrackFeatures {
    /// 0..=1 energy/arousal proxy. Higher = louder/more driving.
    pub energy: f32,
    /// 0..=1 valence/positivity proxy. v1 is a small genre lean; see
    /// [`valence_score`].
    pub valence: f32,
    /// A dense acoustic embedding, present ONLY behind the offline Essentia store.
    /// When `Some`, the `Similar` selector may re-rank by [`cosine_similarity`]
    /// over embeddings instead of the wire call. `None` for metadata-only stores.
    pub embedding: Option<Vec<f32>>,
}

/// The durable P4 hook. A source of per-song [`TrackFeatures`]. The default,
/// in-daemon impl is [`MetadataStore`] (pure metadata heuristics); the offline
/// ops path is an alternate impl that ignores metadata and looks a real embedding
/// up by `song.id`. Selector resolution depends only on THIS trait, so the
/// embedding path swaps in without touching handler or wire code.
pub trait FeatureStore: Send + Sync {
    /// Features for `song`, or `None` when this store has nothing for it. The
    /// metadata store always yields `Some` (it can always score genre); a
    /// precomputed store yields `None` for a song it never extracted.
    fn features(&self, song: &Song) -> Option<TrackFeatures>;
}

/// The default, in-daemon [`FeatureStore`]: derives features purely from the
/// metadata already on the [`Song`] (genre, year), via [`energy_score`] /
/// [`valence_score`]. No embedding (metadata cannot produce one). Always `Some`.
pub struct MetadataStore;

impl FeatureStore for MetadataStore {
    fn features(&self, song: &Song) -> Option<TrackFeatures> {
        Some(TrackFeatures {
            energy: energy_score(song),
            valence: valence_score(song),
            embedding: None,
        })
    }
}

/// A calm genre bucket weight.
const CALM: f32 = 0.20;
/// A mid genre bucket weight.
const MID: f32 = 0.50;
/// An energetic genre bucket weight.
const ENERGETIC: f32 = 0.85;
/// The neutral score for an absent/unknown genre.
const NEUTRAL: f32 = 0.50;

/// The static energy lexicon: `(substring token, bucket weight)`. Matched as a
/// case-insensitive SUBSTRING against the lowercased genre. Ordered longest/most
/// specific first only for readability; matching takes the MAX weight across all
/// hits, so order does not change the result.
const ENERGY_LEXICON: &[(&str, f32)] = &[
    // CALM
    ("ambient", CALM),
    ("classical", CALM),
    ("jazz", CALM),
    ("acoustic", CALM),
    ("folk", CALM),
    ("blues", CALM),
    ("chill", CALM),
    ("downtempo", CALM),
    ("singer-songwriter", CALM),
    ("piano", CALM),
    ("lo-fi", CALM),
    ("lofi", CALM),
    // MID
    ("pop", MID),
    ("rock", MID),
    ("indie", MID),
    ("soul", MID),
    ("r&b", MID),
    ("rnb", MID),
    ("country", MID),
    ("hip hop", MID),
    ("hip-hop", MID),
    ("hiphop", MID),
    ("rap", MID),
    ("funk", MID),
    ("reggae", MID),
    // ENERGETIC
    ("metal", ENERGETIC),
    ("punk", ENERGETIC),
    ("techno", ENERGETIC),
    ("house", ENERGETIC),
    ("edm", ENERGETIC),
    ("dance", ENERGETIC),
    ("drum and bass", ENERGETIC),
    ("dnb", ENERGETIC),
    ("trance", ENERGETIC),
    ("hardcore", ENERGETIC),
    ("breakcore", ENERGETIC),
    ("electro", ENERGETIC),
];

/// A DOCUMENTED energy heuristic in `0..=1` - NOT an acoustic measurement.
///
/// Algorithm:
/// 1. Lowercase the genre. Absent or empty genre -> `0.5` neutral (deterministic).
/// 2. Match every lexicon token as a case-insensitive substring; take the MAX
///    matched bucket weight. When several buckets match ("death metal jazz"), the
///    LOUDER signal wins (energetic > mid > calm) - the documented tie rule.
/// 3. Apply a weak year tiebreak: newer releases nudge up, older down, by AT MOST
///    +/-0.03. This only ever orders songs WITHIN a bucket; it can never cross a
///    bucket boundary (the buckets are >= 0.30 apart). Documented as a tiebreak.
/// 4. Clamp to `0..=1`.
///
/// EXTENSION POINT (documented, not shipped): bpm/loudness are NOT in the current
/// [`Song`] (`map_song` carries no such field). When a backend later surfaces them
/// they fold in HERE as the PRIMARY term and this genre lexicon demotes to a prior.
pub fn energy_score(song: &Song) -> f32 {
    let base = match song.genre.as_deref() {
        Some(g) if !g.trim().is_empty() => {
            let g = g.to_lowercase();
            ENERGY_LEXICON
                .iter()
                .filter(|(tok, _)| g.contains(tok))
                .map(|(_, w)| *w)
                .fold(None, |acc: Option<f32>, w| Some(acc.map_or(w, |a| a.max(w))))
                .unwrap_or(NEUTRAL)
        }
        _ => NEUTRAL,
    };
    (base + year_tiebreak(song.year)).clamp(0.0, 1.0)
}

/// A weak, bounded year nudge in `[-0.03, 0.03]`. A tiebreak ONLY - its magnitude
/// (0.03) is far below the inter-bucket gap (0.30), so it never reorders across
/// buckets. Absent year -> 0.0 (no nudge). Anchored around 1990: pre-1970 clamps
/// to the floor, post-2010 to the ceiling, linear in between.
fn year_tiebreak(year: Option<u32>) -> f32 {
    match year {
        Some(y) => {
            let y = y as f32;
            // Map [1970, 2010] -> [-0.03, 0.03], clamped outside.
            let t = ((y - 1970.0) / 40.0).clamp(0.0, 1.0);
            (t - 0.5) * 0.06
        }
        None => 0.0,
    }
}

/// A DOCUMENTED valence (positivity) heuristic in `0..=1`. v1 is a small genre
/// lean, not a measurement: a few clearly-bright or clearly-dark genres nudge off
/// the `0.5` neutral, everything else stays neutral. Energy is the load-bearing
/// signal; valence is here so the [`TrackFeatures`] seam is complete and a future
/// mood-aware consumer (listening-intelligence) has a field to read.
///
/// TODO(P4+): replace with an acoustic valence when the feature store surfaces one.
pub fn valence_score(song: &Song) -> f32 {
    let base = match song.genre.as_deref() {
        Some(g) if !g.trim().is_empty() => {
            let g = g.to_lowercase();
            let bright = ["pop", "funk", "disco", "reggae", "soul", "house", "dance"];
            let dark = ["metal", "doom", "goth", "industrial", "darkwave", "blues"];
            if bright.iter().any(|t| g.contains(t)) {
                0.70
            } else if dark.iter().any(|t| g.contains(t)) {
                0.30
            } else {
                NEUTRAL
            }
        }
        _ => NEUTRAL,
    };
    base.clamp(0.0, 1.0)
}

/// Cosine similarity in `[-1, 1]` over two equal-length embeddings - the ANN
/// re-rank seam. PURE. Guards the ill-defined cases: unequal lengths or a
/// zero-norm vector return `0.0` (a defined "no signal" value, never a NaN or a
/// panic).
///
/// SEAM: the offline path (Essentia extractor -> per-song-id feature store ->
/// usearch/hnsw index) fills [`TrackFeatures::embedding`]; when embeddings are
/// present the `Similar` selector re-ranks candidates by THIS function over the
/// seed vs candidate embeddings instead of the wire call. That path is out of the
/// daemon sandbox and documented, not stubbed.
pub fn cosine_similarity(a: &[f32], b: &[f32]) -> f32 {
    if a.len() != b.len() || a.is_empty() {
        return 0.0;
    }
    let mut dot = 0.0f32;
    let mut na = 0.0f32;
    let mut nb = 0.0f32;
    for (x, y) in a.iter().zip(b.iter()) {
        dot += x * y;
        na += x * x;
        nb += y * y;
    }
    if na == 0.0 || nb == 0.0 {
        return 0.0;
    }
    dot / (na.sqrt() * nb.sqrt())
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::model::SongId;

    fn song_with(genre: Option<&str>, year: Option<u32>) -> Song {
        Song {
            id: SongId("s1".into()),
            title: "t".into(),
            album: None,
            album_id: None,
            artist: None,
            track: None,
            duration_secs: None,
            cover_art: None,
            starred: false,
            musicbrainz_id: None,
            disc: None,
            year,
            genre: genre.map(|g| g.to_string()),
            bitrate: None,
            comment: None,
            user_rating: None,
            composer: None,
            performer: None,
        }
    }

    #[test]
    fn energy_orders_calm_below_mid_below_energetic() {
        let calm = energy_score(&song_with(Some("ambient"), None));
        let calm2 = energy_score(&song_with(Some("Classical"), None));
        let calm3 = energy_score(&song_with(Some("jazz"), None));
        let mid = energy_score(&song_with(Some("pop"), None));
        let mid2 = energy_score(&song_with(Some("Indie Rock"), None));
        let energetic = energy_score(&song_with(Some("metal"), None));
        let energetic2 = energy_score(&song_with(Some("Techno"), None));
        let energetic3 = energy_score(&song_with(Some("EDM"), None));
        for c in [calm, calm2, calm3] {
            assert!((0.0..=1.0).contains(&c));
            assert!(c < mid, "calm {c} < mid {mid}");
            assert!(c < mid2);
        }
        for m in [mid, mid2] {
            assert!(m < energetic && m < energetic2 && m < energetic3);
        }
        for e in [energetic, energetic2, energetic3] {
            assert!((0.0..=1.0).contains(&e));
        }
    }

    #[test]
    fn absent_or_empty_genre_is_exactly_neutral() {
        assert_eq!(energy_score(&song_with(None, None)), 0.5);
        assert_eq!(energy_score(&song_with(Some(""), None)), 0.5);
        assert_eq!(energy_score(&song_with(Some("   "), None)), 0.5);
        // An unrecognized genre is also neutral.
        assert_eq!(energy_score(&song_with(Some("polka-mystery"), None)), 0.5);
    }

    #[test]
    fn multi_token_takes_max_bucket_and_is_case_insensitive() {
        // "death metal" reads energetic (metal wins over any calmer token).
        let dm = energy_score(&song_with(Some("Death Metal"), None));
        assert_eq!(dm, ENERGETIC);
        // "death metal jazz": louder signal (metal) wins over jazz.
        let dmj = energy_score(&song_with(Some("death METAL jazz"), None));
        assert_eq!(dmj, ENERGETIC);
    }

    #[test]
    fn deterministic_and_year_tiebreak_bounded() {
        let s = song_with(Some("pop"), Some(2005));
        assert_eq!(energy_score(&s), energy_score(&s));
        // Year only nudges within +/-0.03 and never crosses a bucket.
        let old_pop = energy_score(&song_with(Some("pop"), Some(1965)));
        let new_pop = energy_score(&song_with(Some("pop"), Some(2020)));
        assert!((new_pop - old_pop).abs() <= 0.06 + 1e-6);
        // A newest calm track stays strictly below an oldest mid track.
        let newest_calm = energy_score(&song_with(Some("ambient"), Some(2024)));
        let oldest_mid = energy_score(&song_with(Some("pop"), Some(1960)));
        assert!(newest_calm < oldest_mid);
    }

    #[test]
    fn metadata_store_energy_matches_score_and_no_embedding() {
        let s = song_with(Some("techno"), Some(2018));
        let f = MetadataStore.features(&s).unwrap();
        assert_eq!(f.energy, energy_score(&s));
        assert_eq!(f.valence, valence_score(&s));
        assert!(f.embedding.is_none());
    }

    #[test]
    fn cosine_identical_orthogonal_and_guards() {
        let a = [1.0, 2.0, 3.0];
        assert!((cosine_similarity(&a, &a) - 1.0).abs() < 1e-6);
        let x = [1.0, 0.0];
        let y = [0.0, 1.0];
        assert!(cosine_similarity(&x, &y).abs() < 1e-6);
        // Guards: unequal length, empty, zero-norm all -> 0.0, no panic.
        assert_eq!(cosine_similarity(&[1.0, 2.0], &[1.0]), 0.0);
        assert_eq!(cosine_similarity(&[], &[]), 0.0);
        assert_eq!(cosine_similarity(&[0.0, 0.0], &[1.0, 1.0]), 0.0);
    }
}
