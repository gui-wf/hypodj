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

// ── The latent-field FIRST SLICE: a decaying Pull bias over P4 selection ────────
//
// This is `FieldSource` with its wings folded (docs/design/latent-field-interface.md,
// "The first slice"). NO graph, NO field engine, NO CLAP, NO model - just a decaying,
// summable, PURE bias over the same `energy`/`valence` axes the shipped heuristics
// already carry. It degrades to today EXACTLY when no pull is active (the reweight
// funnel is a no-op on an empty field), and gets strictly better - same code - when
// the CLAP sidecar lands and the axis vector grows from length 2 to length 12.

use tokio::time::{Duration, Instant};

/// Default pull half-life (~10 min): a held preference that fades over the next few
/// picks unless reinforced. Config, not a law - the design says these constants are
/// tunable without code change.
pub const PULL_HALF_LIFE: Duration = Duration::from_secs(600);

/// A pull whose decayed strength falls below this is DEAD: pruned from the field and
/// contributing zero bias. "Old pulls fade like memories."
pub const PULL_PRUNE_EPSILON: f32 = 0.05;

/// Cosine threshold above which a new pull MERGES into a live one (same direction)
/// instead of stacking a second ledger entry. "Warmer... warmer" is one stronger
/// pull, not two.
pub const PULL_MERGE_COS: f32 = 0.85;

/// A single decaying magnetic source over the P4 feature axes. PURE: it carries a
/// clock `Instant` but never reads the clock itself - every decay query takes `now`
/// as an argument, so this stays fake-clockable (`#[tokio::test(start_paused)]`) and
/// this module stays I/O-free, lock-free, model-free.
#[derive(Debug, Clone)]
pub struct Pull {
    /// Human provenance, e.g. "calmer" - what the human said, rendered in the echo.
    pub label: String,
    /// The pull DIRECTION, aligned index-for-index with the [`TrackFeatures`]
    /// projection ([energy, valence] in v1; length 12 once CLAP lands, SAME code).
    /// Need not be unit length; [`pull_bonus`] normalizes it.
    pub axes: Vec<f32>,
    /// Strength in `0..=1` AT `set_at` (before decay). Precision is field strength.
    pub strength: f32,
    /// When this pull was born / last reinforced (a fake-clockable clock instant).
    pub set_at: Instant,
    /// The exponential half-life of the decay.
    pub half_life: Duration,
}

impl Pull {
    /// A fresh pull toward `axes` at full-ish `strength`, born `now`, default
    /// half-life. `strength` is clamped to `0..=1`.
    pub fn new(label: impl Into<String>, axes: Vec<f32>, strength: f32, now: Instant) -> Self {
        Pull {
            label: label.into(),
            axes,
            strength: strength.clamp(0.0, 1.0),
            set_at: now,
            half_life: PULL_HALF_LIFE,
        }
    }

    /// The decayed strength at `now`: `strength * 0.5^(dt / half_life)`. PURE.
    /// Saturating dt (a `now` before `set_at` yields the undecayed strength, never a
    /// negative age). A zero half-life is treated as an instant death (returns 0).
    pub fn strength_now(&self, now: Instant) -> f32 {
        let dt = now.saturating_duration_since(self.set_at).as_secs_f32();
        let hl = self.half_life.as_secs_f32();
        if hl <= 0.0 {
            return 0.0;
        }
        self.strength * 0.5f32.powf(dt / hl)
    }

    /// True while the decayed strength is still above the prune epsilon.
    pub fn is_alive(&self, now: Instant) -> bool {
        self.strength_now(now) >= PULL_PRUNE_EPSILON
    }

    /// Whole minutes since this pull was born/reinforced (for the `field` echo).
    pub fn age_mins(&self, now: Instant) -> u64 {
        now.saturating_duration_since(self.set_at).as_secs() / 60
    }
}

/// Project [`TrackFeatures`] onto the v1 field axes `[energy, valence]`. This IS the
/// honest floor the design names: when CLAP lands this becomes the 12-axis readout
/// and everything downstream is unchanged.
pub fn feature_axes(f: &TrackFeatures) -> Vec<f32> {
    vec![f.energy, f.valence]
}

fn dot(a: &[f32], b: &[f32]) -> f32 {
    a.iter().zip(b.iter()).map(|(x, y)| x * y).sum()
}

fn norm(v: &[f32]) -> f32 {
    dot(v, v).sqrt()
}

/// `v` scaled to unit length, or an all-zero vector when `v` has zero norm (a
/// directionless pull contributes no bias, never a NaN).
fn normalize(v: &[f32]) -> Vec<f32> {
    let n = norm(v);
    if n == 0.0 {
        return vec![0.0; v.len()];
    }
    v.iter().map(|x| x / n).collect()
}

/// Cosine between two direction vectors in `[-1, 1]`; `0.0` for a zero-norm or
/// length-mismatched pair (the merge test then simply never fires - safe).
fn cosine(a: &[f32], b: &[f32]) -> f32 {
    if a.len() != b.len() || a.is_empty() {
        return 0.0;
    }
    let (na, nb) = (norm(a), norm(b));
    if na == 0.0 || nb == 0.0 {
        return 0.0;
    }
    dot(a, b) / (na * nb)
}

/// The bias one pull contributes to one candidate, relative to a reference track.
/// PURE, no I/O:
///   `strength_now(now) * dot(normalize(axes), f_candidate - f_reference)`.
/// Positive = the candidate lies further along the pulled direction than the
/// reference (calmer when pulling calmer), so it should rank UP. A dead pull (decayed
/// below epsilon) or an axis-length mismatch contributes exactly `0.0`.
pub fn pull_bonus(
    pull: &Pull,
    now: Instant,
    candidate: &TrackFeatures,
    reference: &TrackFeatures,
) -> f32 {
    let s = pull.strength_now(now);
    if s < PULL_PRUNE_EPSILON {
        return 0.0;
    }
    let dir = normalize(&pull.axes);
    let cand = feature_axes(candidate);
    let refr = feature_axes(reference);
    if dir.len() != cand.len() || cand.len() != refr.len() {
        return 0.0;
    }
    let delta: Vec<f32> = cand.iter().zip(refr.iter()).map(|(c, r)| c - r).collect();
    s * dot(&dir, &delta)
}

/// A small, inspectable set of ACTIVE pulls - the "magnetism map" the human can see.
/// The field is a LIST, never a dense function; this is the whole engine for the
/// first slice. Not `Sync`-shared internally - the handler wraps it in a std `Mutex`
/// with a short, `.await`-free scope.
#[derive(Debug, Default, Clone)]
pub struct PullField {
    pulls: Vec<Pull>,
}

impl PullField {
    pub fn new() -> Self {
        PullField { pulls: Vec::new() }
    }

    /// Drop every pull that has decayed below the prune epsilon. Called before any
    /// read so a dead pull never biases and never renders.
    pub fn prune(&mut self, now: Instant) {
        self.pulls.retain(|p| p.is_alive(now));
    }

    /// True when at least one live pull is present - the reweight funnel's guard.
    /// When this is false the whole feature degrades to today, byte-identical.
    pub fn is_active(&self, now: Instant) -> bool {
        self.pulls.iter().any(|p| p.is_alive(now))
    }

    /// The strongest live pull's decayed strength at `now` in `0..=1` (0.0 for an
    /// empty/all-dead field). This is the field's REWEIGHT AUTHORITY: [`pull_reweight`]
    /// uses it to blend the pulled order against the incoming order, so a fading field
    /// progressively returns candidates toward their original order instead of holding
    /// full reordering power right up to the prune cliff.
    pub fn max_strength(&self, now: Instant) -> f32 {
        self.pulls
            .iter()
            .map(|p| p.strength_now(now))
            .fold(0.0f32, f32::max)
    }

    /// Add a pull, REINFORCING (merge) instead of stacking when a live pull points in
    /// a near-parallel direction (cosine > [`PULL_MERGE_COS`]): strengths add
    /// (clamped to 1.0), `set_at` refreshes to `now`, the label is kept. "Warmer...
    /// warmer" is one stronger, freshly-born pull, not two entries. Prunes first so a
    /// merge never targets a corpse. A merged pull MOVES to the end so insertion order
    /// stays recency order (most-recently-meant last) - the invariant `nudge_recent`,
    /// `describe`, and `snapshot` all rely on.
    pub fn add(&mut self, pull: Pull, now: Instant) {
        self.prune(now);
        if let Some(idx) = self
            .pulls
            .iter()
            .position(|p| cosine(&p.axes, &pull.axes) > PULL_MERGE_COS)
        {
            let mut existing = self.pulls.remove(idx);
            existing.strength = (existing.strength_now(now) + pull.strength).clamp(0.0, 1.0);
            existing.set_at = now;
            existing.half_life = pull.half_life;
            self.pulls.push(existing);
            return;
        }
        self.pulls.push(pull);
    }

    /// Nudge the MOST-RECENT pull (the one the human just meant). `factor` scales its
    /// decayed strength and re-bases it at `now`: `0.5` for "less"/"back"/"too much",
    /// `1.5` for "more". Returns the affected label, or `None` on an empty field.
    /// Below-epsilon after the nudge -> the pull is dropped.
    pub fn nudge_recent(&mut self, factor: f32, now: Instant) -> Option<String> {
        self.prune(now);
        let last = self.pulls.last_mut()?;
        last.strength = (last.strength_now(now) * factor).clamp(0.0, 1.0);
        last.set_at = now;
        let label = last.label.clone();
        self.prune(now);
        Some(label)
    }

    /// Clear every pull (the `field clear` gesture). Non-destructive: it only
    /// corrects the system's BELIEFS, never the queue.
    pub fn clear(&mut self) {
        self.pulls.clear();
    }

    /// A compact, owned snapshot of the live pulls for the passive field HUD:
    /// `(label, strength as 0..=100, age minutes)` per live pull, insertion order
    /// preserved (most recent last). Strength is `round(strength_now * 100)` so the
    /// value is colon-free and re-renderable client-side; a fresh full-strength pull
    /// reads 60 (the lexicon default). Empty when no pull is alive. PURE.
    pub fn snapshot(&self, now: Instant) -> Vec<(String, u8, u64)> {
        self.pulls
            .iter()
            .filter(|p| p.is_alive(now))
            .map(|p| {
                let s = (p.strength_now(now) * 100.0).round().clamp(0.0, 100.0) as u8;
                (p.label.clone(), s, p.age_mins(now))
            })
            .collect()
    }

    /// The summed bias for one candidate over ALL live pulls, relative to a reference
    /// track. `F(candidate) = sum_i pull_bonus_i`. PURE.
    pub fn total_bonus(
        &self,
        now: Instant,
        candidate: &TrackFeatures,
        reference: &TrackFeatures,
    ) -> f32 {
        self.pulls
            .iter()
            .map(|p| pull_bonus(p, now, candidate, reference))
            .sum()
    }

    /// Render the live pulls with provenance + decayed strength for the `field`
    /// read command: `"toward calmer (0.60, from calmer, 3 min ago, fading)"`. Most
    /// recent last (insertion order). Empty when no pull is active.
    pub fn describe(&self, now: Instant) -> Vec<String> {
        self.pulls
            .iter()
            .filter(|p| p.is_alive(now))
            .map(|p| {
                format!(
                    "toward {} ({:.2}, from the ask, {} min ago, fading)",
                    p.label,
                    p.strength_now(now),
                    p.age_mins(now),
                )
            })
            .collect()
    }
}

/// The ONE reweight HOOK. Stable-sort `candidates` DESCENDING by a key that BLENDS
/// the summed pull bias (relative to `reference`) with each candidate's incoming
/// position, weighted by the field's current decayed strength. Tracks further along
/// the pulled direction rank up; ties keep their incoming order. PURE and total:
///   - an EMPTY / all-dead field leaves the vector byte-identical (blend weight 0.0
///     collapses the key to the incoming order) - the "degrades to today exactly"
///     guarantee, though the handler also SKIPS the call entirely when no pull is
///     active;
///   - only reorders CANDIDATE RANKING - it never mutates the queue, never deletes,
///     never arms anything.
///
/// THE FADE IS REAL, not cosmetic. The blend weight `w` is the field's max decayed
/// strength in `0..=1`; the sort key is `w * norm_bonus + (1 - w) * incoming_rank`.
/// Because a single pull's decay is a positive scalar shared by all candidates, it
/// would NOT change a pure-bonus sort order - so a decaying pull used to reorder at
/// FULL power until it snapped to death at the prune cliff. Here, as `w` fades toward
/// zero, the incoming-rank term dominates and candidates progressively return to their
/// original order: a gradual fade, not a binary on/off. At full strength (`w == 1`)
/// the order matches a pure-bonus sort exactly (backwards compatible).
pub fn pull_reweight(
    field: &PullField,
    now: Instant,
    store: &dyn FeatureStore,
    reference: &TrackFeatures,
    candidates: Vec<Song>,
) -> Vec<Song> {
    let n = candidates.len();
    if n <= 1 {
        return candidates;
    }
    let w = field.max_strength(now).clamp(0.0, 1.0);

    // Raw summed bias per candidate (missing features contribute 0.0).
    let bonuses: Vec<f32> = candidates
        .iter()
        .map(|s| match store.features(s) {
            Some(f) => field.total_bonus(now, &f, reference),
            None => 0.0,
        })
        .collect();

    // Min-max normalize the bias into 0..=1 so it is commensurate with the
    // incoming-rank term (an all-equal field yields all-zero, a pure no-op).
    let (mut lo, mut hi) = (f32::INFINITY, f32::NEG_INFINITY);
    for &b in &bonuses {
        lo = lo.min(b);
        hi = hi.max(b);
    }
    let span = hi - lo;

    // Blend key per ORIGINAL index; earlier incoming positions score higher on the
    // rank term so a zero-strength field keeps the incoming order.
    let last = (n - 1) as f32;
    let mut indexed: Vec<(usize, f32)> = (0..n)
        .map(|i| {
            let norm_bonus = if span > 0.0 { (bonuses[i] - lo) / span } else { 0.0 };
            let incoming_rank = (last - i as f32) / last;
            let key = w * norm_bonus + (1.0 - w) * incoming_rank;
            (i, key)
        })
        .collect();

    // Descending by key; ties break on original index (stable ordering).
    indexed.sort_by(|a, b| {
        b.1.partial_cmp(&a.1)
            .unwrap_or(std::cmp::Ordering::Equal)
            .then(a.0.cmp(&b.0))
    });

    let mut src: Vec<Option<Song>> = candidates.into_iter().map(Some).collect();
    indexed
        .into_iter()
        .map(|(i, _)| src[i].take().unwrap())
        .collect()
}

/// Map a small LEXICON of mood words to a pull DIRECTION over `[energy, valence]`.
/// This is a lexicon, NOT a model: a handful of comparatives become a held Direction
/// decaying like Memoria salience. Unknown words yield `None` (the honest "no pull
/// felt from that" echo). Multi-word input is matched token-by-token and the axis
/// contributions SUM, so "more energy" and "calmer warmer" both resolve.
pub fn lexicon_pull(words: &str, strength: f32, now: Instant) -> Option<Pull> {
    // (token, [d_energy, d_valence]). Case-insensitive whole-word match.
    const LEX: &[(&str, [f32; 2])] = &[
        // ENERGY down (calmer / slower / softer feel).
        ("calmer", [-1.0, 0.0]),
        ("calm", [-1.0, 0.0]),
        ("softer", [-1.0, 0.0]),
        ("gentler", [-1.0, 0.0]),
        ("mellower", [-1.0, 0.0]),
        ("slower", [-1.0, 0.0]),
        ("spacier", [-1.0, 0.0]),
        ("chiller", [-1.0, 0.0]),
        // ENERGY up (harder / faster / punchier feel).
        ("energy", [1.0, 0.0]),
        ("energetic", [1.0, 0.0]),
        ("harder", [1.0, 0.0]),
        ("louder", [1.0, 0.0]),
        ("punchier", [1.0, 0.0]),
        ("faster", [1.0, 0.0]),
        ("driving", [1.0, 0.0]),
        ("intense", [1.0, 0.0]),
        // VALENCE up (brighter / warmer / happier feel).
        ("warmer", [0.0, 1.0]),
        ("happier", [0.0, 1.0]),
        ("brighter", [0.0, 1.0]),
        ("dreamier", [0.0, 1.0]),
        ("sweeter", [0.0, 1.0]),
        // VALENCE down (darker / moodier feel).
        ("darker", [0.0, -1.0]),
        ("sadder", [0.0, -1.0]),
        ("moodier", [0.0, -1.0]),
        ("gloomier", [0.0, -1.0]),
        ("bleaker", [0.0, -1.0]),
    ];
    // Negation words INVERT the direction of the term they qualify. Without this a
    // softener/inverter the user adds ("less energy", "not calmer") would be silently
    // dropped and only the base direction would survive - producing a pull toward the
    // OPPOSITE of what was asked. A pending negation flips the sign of the next matched
    // lexicon token, then clears.
    const NEGATORS: &[&str] = &["less", "not", "no", "never", "un", "anti"];
    let lc = words.to_lowercase();
    let mut axes = [0.0f32; 2];
    let mut hit = false;
    let mut neg = false;
    // The matched DIRECTION token(s) become the label, so the `field` echo reads
    // "toward calmer" rather than echoing the whole sentence ("play something
    // calmer"). A leading negator is kept on its term ("less energy").
    let mut label_toks: Vec<String> = Vec::new();
    for tok in lc.split_whitespace() {
        if NEGATORS.contains(&tok) {
            neg = true;
            continue;
        }
        if let Some((_, d)) = LEX.iter().find(|(t, _)| *t == tok) {
            let sign = if neg { -1.0 } else { 1.0 };
            axes[0] += sign * d[0];
            axes[1] += sign * d[1];
            hit = true;
            if neg {
                label_toks.push(format!("less {tok}"));
            } else {
                label_toks.push(tok.to_string());
            }
            neg = false;
        }
    }
    if !hit || (axes[0] == 0.0 && axes[1] == 0.0) {
        return None;
    }
    let label = label_toks.join(" ");
    Some(Pull::new(label, vec![axes[0], axes[1]], strength, now))
}

/// The default strength for a lexicon-set pull (a comparative is a held, mid-band
/// Direction - well below the arm threshold, which this slice never touches anyway).
pub const LEXICON_PULL_STRENGTH: f32 = 0.6;

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

#[cfg(test)]
mod pull_tests {
    use super::*;
    use crate::model::{Song, SongId};

    fn feat(energy: f32, valence: f32) -> TrackFeatures {
        TrackFeatures { energy, valence, embedding: None }
    }

    // A store that reads energy/valence straight off a per-id lookup, so a test can
    // hand-place candidates in the axis plane with no genre lexicon in the way.
    struct FakeStore(std::collections::HashMap<String, (f32, f32)>);
    impl FeatureStore for FakeStore {
        fn features(&self, song: &Song) -> Option<TrackFeatures> {
            self.0.get(&song.id.0).map(|(e, v)| feat(*e, *v))
        }
    }

    fn song(id: &str) -> Song {
        Song {
            id: SongId(id.into()),
            title: id.into(),
            album: None,
            album_id: None,
            artist: None,
            track: None,
            duration_secs: None,
            cover_art: None,
            starred: false,
            musicbrainz_id: None,
            disc: None,
            year: None,
            genre: None,
            bitrate: None,
            comment: None,
            user_rating: None,
            composer: None,
            performer: None,
        }
    }

    // pull_bonus is PURE: a calmer pull rewards lower-energy candidates and the
    // reward scales with the decayed strength.
    #[tokio::test(start_paused = true)]
    async fn pull_bonus_rewards_pulled_direction() {
        let now = Instant::now();
        // "calmer" = -energy. Reference at mid energy.
        let pull = Pull::new("calmer", vec![-1.0, 0.0], 1.0, now);
        let reference = feat(0.5, 0.5);
        let calmer = feat(0.2, 0.5);
        let louder = feat(0.9, 0.5);
        let b_calm = pull_bonus(&pull, now, &calmer, &reference);
        let b_loud = pull_bonus(&pull, now, &louder, &reference);
        assert!(b_calm > 0.0, "a calmer candidate ranks up: {b_calm}");
        assert!(b_loud < 0.0, "a louder candidate ranks down: {b_loud}");
        // The exact value: strength 1.0 * dot([-1,0], [0.2-0.5, 0]) = 0.3.
        assert!((b_calm - 0.3).abs() < 1e-6);
    }

    // Decay is exponential and fake-clocked: one half-life halves the strength, and
    // the pull dies (prunes to zero bias) after enough half-lives.
    #[tokio::test(start_paused = true)]
    async fn decay_halves_each_half_life_and_dies() {
        let now = Instant::now();
        let pull = Pull::new("calmer", vec![-1.0, 0.0], 0.8, now);
        assert!((pull.strength_now(now) - 0.8).abs() < 1e-6);
        tokio::time::advance(PULL_HALF_LIFE).await;
        let t1 = Instant::now();
        assert!((pull.strength_now(t1) - 0.4).abs() < 1e-3);
        assert!(pull.is_alive(t1));
        // 0.8 -> below epsilon (0.05) needs > 4 half-lives (0.8/16 = 0.05); advance 5.
        tokio::time::advance(PULL_HALF_LIFE * 4).await;
        let t5 = Instant::now();
        assert!(!pull.is_alive(t5), "decayed below epsilon: {}", pull.strength_now(t5));
        // A dead pull contributes exactly zero bias.
        assert_eq!(pull_bonus(&pull, t5, &feat(0.1, 0.5), &feat(0.5, 0.5)), 0.0);
    }

    // The reweight funnel ACTUALLY reorders candidates toward the pull: calmer
    // tracks move ahead of louder ones.
    #[tokio::test(start_paused = true)]
    async fn reweight_reorders_toward_pull() {
        let now = Instant::now();
        let mut field = PullField::new();
        field.add(Pull::new("calmer", vec![-1.0, 0.0], 1.0, now), now);
        let store = FakeStore(
            [("loud".into(), (0.9, 0.5)), ("mid".into(), (0.5, 0.5)), ("calm".into(), (0.1, 0.5))]
                .into_iter()
                .collect(),
        );
        let reference = feat(0.5, 0.5);
        // Incoming order is loudest-first; the pull should flip it calm-first.
        let out = pull_reweight(&field, now, &store, &reference, vec![song("loud"), song("mid"), song("calm")]);
        let ids: Vec<&str> = out.iter().map(|s| s.id.0.as_str()).collect();
        assert_eq!(ids, vec!["calm", "mid", "loud"]);
    }

    // The fade is REAL: a strong pull fully reorders; a nearly-dead one (same single
    // half-life) returns candidates toward their incoming order instead of holding
    // full reordering power right up to the prune cliff.
    #[tokio::test(start_paused = true)]
    async fn reweight_fade_weakens_ordering_before_death() {
        let now = Instant::now();
        let mut field = PullField::new();
        field.add(Pull::new("calmer", vec![-1.0, 0.0], 1.0, now), now);
        let store = FakeStore(
            [("loud".into(), (0.9, 0.5)), ("mid".into(), (0.5, 0.5)), ("calm".into(), (0.1, 0.5))]
                .into_iter()
                .collect(),
        );
        let reference = feat(0.5, 0.5);
        let incoming = || vec![song("loud"), song("mid"), song("calm")];

        // Full strength: full reorder, calm-first.
        let hot = pull_reweight(&field, now, &store, &reference, incoming());
        let ids: Vec<&str> = hot.iter().map(|s| s.id.0.as_str()).collect();
        assert_eq!(ids, vec!["calm", "mid", "loud"]);

        // Nearly dead but still alive (~5 half-lives keeps it just above epsilon here:
        // 1.0/32 = 0.031 < 0.05, so use 4 half-lives -> 0.0625, still alive).
        tokio::time::advance(PULL_HALF_LIFE * 4).await;
        let cold = Instant::now();
        assert!(field.is_active(cold), "pull still alive: {}", field.max_strength(cold));
        let faded = pull_reweight(&field, cold, &store, &reference, incoming());
        let faded_ids: Vec<&str> = faded.iter().map(|s| s.id.0.as_str()).collect();
        // The fade has returned the order toward the incoming loudest-first order,
        // i.e. it is NO LONGER the full calm-first reorder.
        assert_ne!(faded_ids, vec!["calm", "mid", "loud"], "decay must weaken the reorder");
        assert_eq!(faded_ids, vec!["loud", "mid", "calm"], "near-dead pull returns to incoming order");
    }

    // Negation words INVERT the term they qualify: "less energy" pulls toward CALMER,
    // never toward more energy (the opposite-of-intent bug).
    #[tokio::test(start_paused = true)]
    async fn lexicon_negation_inverts_direction() {
        let now = Instant::now();
        let less_energy = lexicon_pull("less energy", 0.6, now).expect("negated term still pulls");
        // energy is [1.0, 0.0]; negated -> [-1.0, 0.0] = calmer.
        assert!(less_energy.axes[0] < 0.0, "less energy pulls DOWN in energy: {:?}", less_energy.axes);
        let not_warm = lexicon_pull("not warmer", 0.6, now).expect("negated term still pulls");
        assert!(not_warm.axes[1] < 0.0, "not warmer pulls DOWN in valence: {:?}", not_warm.axes);
        // A lone negator with no known term is still the honest "no pull felt".
        assert!(lexicon_pull("less", 0.6, now).is_none());
    }

    // A no-pull field leaves the candidate list byte-identical (degrades to today).
    #[tokio::test(start_paused = true)]
    async fn empty_field_is_byte_identical() {
        let now = Instant::now();
        let field = PullField::new();
        assert!(!field.is_active(now));
        let store = FakeStore(std::collections::HashMap::new());
        let input = vec![song("a"), song("b"), song("c")];
        let out = pull_reweight(&field, now, &store, &feat(0.5, 0.5), input.clone());
        let before: Vec<&str> = input.iter().map(|s| s.id.0.as_str()).collect();
        let after: Vec<&str> = out.iter().map(|s| s.id.0.as_str()).collect();
        assert_eq!(before, after);
    }

    // A second near-parallel pull REINFORCES (merge) rather than stacking a 2nd entry;
    // an orthogonal pull adds a distinct entry.
    #[tokio::test(start_paused = true)]
    async fn reinforce_merges_not_stacks() {
        let now = Instant::now();
        let mut field = PullField::new();
        field.add(Pull::new("calmer", vec![-1.0, 0.0], 0.5, now), now);
        // "calmer" again (same direction): merge, strengths add.
        field.add(Pull::new("calmer", vec![-1.0, 0.0], 0.4, now), now);
        assert_eq!(field.describe(now).len(), 1, "same direction merges");
        // The merged strength is the sum (0.9), stronger than either alone.
        let b = field.total_bonus(now, &feat(0.0, 0.5), &feat(0.5, 0.5));
        assert!((b - 0.45).abs() < 1e-6, "merged bonus {b}"); // 0.9 * 0.5
        // An orthogonal "warmer" pull is a distinct entry.
        field.add(Pull::new("warmer", vec![0.0, 1.0], 0.5, now), now);
        assert_eq!(field.describe(now).len(), 2, "orthogonal direction adds an entry");
    }

    // The lexicon maps words to directions and rejects unknowns (the honest echo).
    #[tokio::test(start_paused = true)]
    async fn lexicon_maps_words() {
        let now = Instant::now();
        let calmer = lexicon_pull("calmer", 0.6, now).unwrap();
        assert_eq!(calmer.axes, vec![-1.0, 0.0]);
        let energy = lexicon_pull("more energy", 0.6, now).unwrap();
        assert_eq!(energy.axes, vec![1.0, 0.0]);
        let warmer = lexicon_pull("warmer", 0.6, now).unwrap();
        assert_eq!(warmer.axes, vec![0.0, 1.0]);
        // Unknown words -> None (no pull felt from that).
        assert!(lexicon_pull("banana sandwich", 0.6, now).is_none());
        assert!(lexicon_pull("", 0.6, now).is_none());
    }

    // The expanded phrase set maps each new comparative onto the right EXISTING
    // energy/valence axis (no new axes) - punchier/faster/driving pull up in energy,
    // slower/spacier down; dreamier/sweeter up in valence, gloomier/bleaker down.
    #[tokio::test(start_paused = true)]
    async fn expanded_lexicon_maps_to_right_axis() {
        let now = Instant::now();
        // ENERGY up: positive energy, zero valence.
        for w in ["punchier", "faster", "driving", "intense"] {
            let p = lexicon_pull(w, 0.6, now).unwrap_or_else(|| panic!("{w} pulls"));
            assert_eq!(p.axes, vec![1.0, 0.0], "{w} pulls up in energy");
        }
        // ENERGY down: negative energy, zero valence.
        for w in ["slower", "spacier", "chiller"] {
            let p = lexicon_pull(w, 0.6, now).unwrap_or_else(|| panic!("{w} pulls"));
            assert_eq!(p.axes, vec![-1.0, 0.0], "{w} pulls down in energy");
        }
        // VALENCE up: zero energy, positive valence.
        for w in ["dreamier", "sweeter"] {
            let p = lexicon_pull(w, 0.6, now).unwrap_or_else(|| panic!("{w} pulls"));
            assert_eq!(p.axes, vec![0.0, 1.0], "{w} pulls up in valence");
        }
        // VALENCE down: zero energy, negative valence.
        for w in ["gloomier", "bleaker"] {
            let p = lexicon_pull(w, 0.6, now).unwrap_or_else(|| panic!("{w} pulls"));
            assert_eq!(p.axes, vec![0.0, -1.0], "{w} pulls down in valence");
        }
    }

    // The label is the matched DIRECTION token(s), not the whole sentence, so the
    // `field` echo reads "toward calmer" for a fuzzy ask like "play something calmer".
    #[tokio::test(start_paused = true)]
    async fn lexicon_label_is_matched_token_not_whole_phrase() {
        let now = Instant::now();
        let p = lexicon_pull("play something calmer", LEXICON_PULL_STRENGTH, now).unwrap();
        assert_eq!(p.label, "calmer", "label is the matched token, not the sentence");
        assert_eq!(p.strength, LEXICON_PULL_STRENGTH);
        // A negated term keeps its softener in the label.
        let n = lexicon_pull("give me less energy please", 0.6, now).unwrap();
        assert_eq!(n.label, "less energy");
        // The render then reads cleanly with the "from the ask" origin marker.
        let mut field = PullField::new();
        field.add(p, now);
        let line = &field.describe(now)[0];
        assert!(line.contains("toward calmer"), "{line}");
        assert!(line.contains("from the ask"), "{line}");
    }

    // The `field` render carries provenance + decayed strength + age.
    #[tokio::test(start_paused = true)]
    async fn field_render_shows_provenance() {
        let now = Instant::now();
        let mut field = PullField::new();
        field.add(Pull::new("calmer", vec![-1.0, 0.0], 0.6, now), now);
        tokio::time::advance(Duration::from_secs(180)).await; // 3 min
        let t = Instant::now();
        let lines = field.describe(t);
        assert_eq!(lines.len(), 1);
        assert!(lines[0].contains("calmer"), "{}", lines[0]);
        assert!(lines[0].contains("3 min ago"), "{}", lines[0]);
        assert!(lines[0].contains("fading"), "{}", lines[0]);
    }

    // Reinforcing an EARLIER pull makes it the most-recently-meant one: the merge
    // moves it to the end so a following nudge lands on it, not on a later-but-
    // untouched pull. Regression for the in-place-merge recency bug.
    #[tokio::test(start_paused = true)]
    async fn reinforce_updates_recency_order() {
        let now = Instant::now();
        let mut field = PullField::new();
        field.add(Pull::new("calmer", vec![-1.0, 0.0], 0.6, now), now);
        field.add(Pull::new("warmer", vec![0.0, 1.0], 0.6, now), now);
        // Reinforce "calmer" - it is now the just-meant pull, so it must move last.
        field.add(Pull::new("calmer", vec![-1.0, 0.0], 0.2, now), now);
        let lines = field.describe(now);
        assert!(lines.last().unwrap().contains("calmer"), "{:?}", lines);
        // A nudge must attenuate the reinforced "calmer", not the older "warmer".
        let label = field.nudge_recent(0.5, now).unwrap();
        assert_eq!(label, "calmer");
    }

    // "less"/"too much" halves the most-recent pull; "more" strengthens it.
    #[tokio::test(start_paused = true)]
    async fn nudge_attenuates_most_recent() {
        let now = Instant::now();
        let mut field = PullField::new();
        field.add(Pull::new("warmer", vec![0.0, 1.0], 0.8, now), now);
        let label = field.nudge_recent(0.5, now).unwrap();
        assert_eq!(label, "warmer");
        let b = field.total_bonus(now, &feat(0.5, 1.0), &feat(0.5, 0.5));
        assert!((b - 0.2).abs() < 1e-6, "halved bonus {b}"); // 0.4 * 0.5
    }
}
