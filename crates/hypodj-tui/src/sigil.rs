//! The album sigil: a deterministic generative mark drawn in the album-art slot when
//! no inline-image protocol is available (per [`crate::album_color::image_protocol`]).
//! Structure comes from a hash of artist+album (FNV-1a -> splitmix64 PRNG); the form
//! is a Truchet diagonal mosaic; the colors come from the shared [`Palette`] run
//! through the DECORATION policy so the sigil sits BELOW a distraction ceiling (muted,
//! near-background). It is STATIC - regenerated only when the album changes - and the
//! caller caches it by album identity. With no cover it degrades to a hash-only
//! identicon over a neutral palette.

use ratatui::style::{Color, Style};
use ratatui::text::{Line, Span};

use hypodj_client::model::NowPlaying;

use crate::album_color::{fit_decoration, to_color, Palette, Rgb, TermBg};

/// The two Truchet diagonal tiles (light box-drawing diagonals). Each cell shows one,
/// chosen by a PRNG bit, so the grid reads as a woven mosaic.
const TRUCHET: [char; 2] = ['\u{2571}', '\u{2572}']; // BOX DRAWINGS LIGHT DIAGONAL /  \

/// A resolved, static sigil: a fixed tile grid + a per-tile color index, plus the two
/// decoration colors it draws with. Rendering just maps the cached grid onto the cell
/// area, so there is no per-frame regeneration.
#[derive(Debug, Clone)]
pub struct Sigil {
    /// The album identity this sigil was built for (cache key).
    pub identity: String,
    /// Whether this sigil was built from a real cover palette (vs the neutral
    /// fallback). Lets the caller rebuild once the async cover art lands.
    pub has_palette: bool,
    /// Row-major tile glyphs on a fixed GRID x GRID lattice.
    grid: Vec<char>,
    /// Row-major color choice (0 or 1) per tile.
    color_bits: Vec<u8>,
    colors: [Color; 2],
    bg: Color,
}

/// The sigil's internal lattice edge; the render samples this grid into the cell area.
const GRID: usize = 8;

impl Sigil {
    /// Build the static sigil for `identity` with the given palette (or the neutral
    /// fallback when there is no cover) against the detected background.
    pub fn build(identity: &str, palette: Option<&Palette>, bg: TermBg, truecolor: bool) -> Sigil {
        let mut rng = SplitMix64::new(seed_of(identity));
        let mut grid = Vec::with_capacity(GRID * GRID);
        let mut color_bits = Vec::with_capacity(GRID * GRID);
        for _ in 0..GRID * GRID {
            grid.push(TRUCHET[(rng.next_u64() & 1) as usize]);
            color_bits.push((rng.next_u64() & 1) as u8);
        }
        // Two decoration swatches from the palette (vibrant + muted), pulled below the
        // distraction ceiling. No cover -> a neutral palette, still deterministic form.
        let pal = palette.cloned().unwrap_or_else(Palette::neutral);
        let a: Rgb = fit_decoration(pal.vibrant, bg);
        let b: Rgb = fit_decoration(pal.muted, bg);
        Sigil {
            identity: identity.to_string(),
            has_palette: palette.is_some(),
            grid,
            color_bits,
            colors: [to_color(a, truecolor), to_color(b, truecolor)],
            bg: to_color(bg.rgb, truecolor),
        }
    }

    /// Render the sigil into `cols x rows` cells (nearest-neighbour sample of the
    /// internal lattice). Deterministic for a given sigil + size.
    pub fn lines(&self, cols: usize, rows: usize) -> Vec<Line<'static>> {
        if cols == 0 || rows == 0 {
            return Vec::new();
        }
        let mut lines = Vec::with_capacity(rows);
        for row in 0..rows {
            let gy = row * GRID / rows;
            let mut spans = Vec::with_capacity(cols);
            for col in 0..cols {
                let gx = col * GRID / cols;
                let i = gy * GRID + gx;
                let glyph = self.grid[i];
                let color = self.colors[self.color_bits[i] as usize];
                spans.push(Span::styled(
                    glyph.to_string(),
                    Style::default().fg(color).bg(self.bg),
                ));
            }
            lines.push(Line::from(spans));
        }
        lines
    }
}

/// A stable album identity: prefer a stable uri (`album/<id>` derived from the song
/// file, else the file itself), else lowercased "artist\nalbum", else the title. This
/// is the sigil cache key and the PRNG seed source.
pub fn album_identity(np: &NowPlaying) -> String {
    if let (Some(artist), Some(album)) = (np.artist.as_deref(), np.album.as_deref()) {
        return format!("{}\n{}", artist.to_lowercase(), album.to_lowercase());
    }
    if let Some(album) = np.album.as_deref() {
        return album.to_lowercase();
    }
    if let Some(file) = np.file.as_deref() {
        return file.to_string();
    }
    np.title.clone().unwrap_or_default().to_lowercase()
}

/// FNV-1a 64-bit hash of the identity, the PRNG seed.
pub fn seed_of(s: &str) -> u64 {
    let mut hash: u64 = 0xcbf29ce484222325;
    for b in s.as_bytes() {
        hash ^= *b as u64;
        hash = hash.wrapping_mul(0x100000001b3);
    }
    hash
}

/// The splitmix64 PRNG: deterministic, seeded from the album hash, for the tile grid.
struct SplitMix64 {
    state: u64,
}

impl SplitMix64 {
    fn new(seed: u64) -> Self {
        SplitMix64 { state: seed }
    }
    fn next_u64(&mut self) -> u64 {
        self.state = self.state.wrapping_add(0x9e3779b97f4a7bb9);
        let mut z = self.state;
        z = (z ^ (z >> 30)).wrapping_mul(0xbf58476d1ce4e5b9);
        z = (z ^ (z >> 27)).wrapping_mul(0x94d049bb133111eb);
        z ^ (z >> 31)
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::album_color::BgSource;

    fn np(artist: &str, album: &str) -> NowPlaying {
        NowPlaying {
            artist: Some(artist.into()),
            album: Some(album.into()),
            ..NowPlaying::default()
        }
    }

    #[test]
    fn identity_is_stable_and_distinct() {
        let a = album_identity(&np("Miles Davis", "Kind of Blue"));
        assert_eq!(a, album_identity(&np("miles davis", "kind of blue")), "case-insensitive stable");
        assert_ne!(a, album_identity(&np("Miles Davis", "Bitches Brew")), "album changes identity");
    }

    #[test]
    fn sigil_is_deterministic_for_the_same_album() {
        let bg = TermBg::dark_default();
        let s1 = Sigil::build("miles\nkind of blue", None, bg, true);
        let s2 = Sigil::build("miles\nkind of blue", None, bg, true);
        assert_eq!(s1.lines(12, 6), s2.lines(12, 6), "same album -> identical sigil");
        let s3 = Sigil::build("miles\nbitches brew", None, bg, true);
        assert_ne!(s1.lines(12, 6), s3.lines(12, 6), "different album -> different sigil");
    }

    #[test]
    fn sigil_shape_and_glyphs() {
        let bg = TermBg::from_rgb([0x10, 0x10, 0x10], BgSource::DarkDefault);
        let sig = Sigil::build("some\nalbum", None, bg, false);
        let lines = sig.lines(10, 5);
        assert_eq!(lines.len(), 5, "rows");
        for l in &lines {
            assert_eq!(l.spans.len(), 10, "cols");
            for s in &l.spans {
                let c = s.content.chars().next().unwrap();
                assert!(TRUCHET.contains(&c), "only Truchet tiles: {c:?}");
                assert!(s.style.fg.is_some() && s.style.bg.is_some());
            }
        }
        // Degenerate size -> empty.
        assert!(sig.lines(0, 5).is_empty());
        assert!(sig.lines(5, 0).is_empty());
    }

    #[test]
    fn splitmix_is_deterministic() {
        let mut a = SplitMix64::new(seed_of("x"));
        let mut b = SplitMix64::new(seed_of("x"));
        assert_eq!(a.next_u64(), b.next_u64());
        // FNV differs per input.
        assert_ne!(seed_of("a"), seed_of("b"));
    }
}
