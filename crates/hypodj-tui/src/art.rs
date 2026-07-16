//! Album-art fetch + terminal rendering. Cover bytes come from the daemon's MPD
//! `albumart "<uri>" <offset>` command (binary framing: `size:`/`binary:`/raw
//! bytes/`OK`), which the client's text-only [`hypodj_client::mpd::MpdConn`] cannot
//! read - so we fetch on a DEDICATED short-lived connection (the daemon caches
//! decoded covers, so this is cheap and never desyncs the main session socket).
//!
//! Rendering: each terminal cell is split into two vertical pixels via the upper
//! half-block `U+2580`, `fg` = top pixel, `bg` = bottom pixel, so a `cols x rows`
//! cell area shows a `cols x (rows*2)` image. A small ordered (Bayer 4x4) dither is
//! applied per channel to break up banding on the coarse terminal grid - the
//! "dithering to make it look better" the layout calls for.

use std::io::{BufRead, BufReader, Read, Write};
use std::net::TcpStream;
use std::time::Duration;

use image::RgbImage;
use ratatui::style::{Color, Style};
use ratatui::text::{Line, Span};

const ART_TIMEOUT: Duration = Duration::from_secs(3);
/// Decoded-thumbnail edge (px). Fetch+decode once per track; the per-frame
/// downscale from this to the cell grid is cheap.
const THUMB: u32 = 96;

/// A decoded cover thumbnail, cached per track uri. Rendering downscales it to the
/// current cell area every frame (cheap); the expensive fetch+decode happens once.
pub struct AlbumArt {
    pub uri: String,
    img: RgbImage,
}

impl AlbumArt {
    /// Fetch + decode the cover for `uri`; `None` when there is no art or anything
    /// fails (a missing cover must never break the UI).
    pub fn load(host: &str, port: u16, uri: &str) -> Option<AlbumArt> {
        let bytes = fetch_albumart(host, port, uri)?;
        let img = image::load_from_memory(&bytes).ok()?;
        let thumb = img
            .resize_exact(THUMB, THUMB, image::imageops::FilterType::Triangle)
            .to_rgb8();
        Some(AlbumArt { uri: uri.to_string(), img: thumb })
    }

    /// Render the art into `cols x rows` half-block cells (so `cols x rows*2` px).
    pub fn lines(&self, cols: usize, rows: usize) -> Vec<Line<'static>> {
        render_lines(&self.img, cols, rows)
    }
}

/// Read one CRLF/LF-terminated line, stripping the terminator. EOF -> error.
fn read_line(r: &mut impl BufRead) -> std::io::Result<String> {
    let mut s = String::new();
    if r.read_line(&mut s)? == 0 {
        return Err(std::io::Error::new(std::io::ErrorKind::UnexpectedEof, "eof"));
    }
    while s.ends_with('\n') || s.ends_with('\r') {
        s.pop();
    }
    Ok(s)
}

/// MPD binary-safe quoting for the uri argument.
fn quote(uri: &str) -> String {
    format!("\"{}\"", uri.replace('\\', "\\\\").replace('"', "\\\""))
}

/// Fetch the full cover for `uri` over a dedicated connection, looping the
/// `albumart` offset chunks until the reported total is assembled. `None` on no
/// art / ACK / any IO error.
fn fetch_albumart(host: &str, port: u16, uri: &str) -> Option<Vec<u8>> {
    let stream = TcpStream::connect((host, port)).ok()?;
    stream.set_read_timeout(Some(ART_TIMEOUT)).ok()?;
    stream.set_write_timeout(Some(ART_TIMEOUT)).ok()?;
    let mut w = stream.try_clone().ok()?;
    let mut r = BufReader::new(stream);
    if !read_line(&mut r).ok()?.starts_with("OK MPD") {
        return None;
    }
    let mut all: Vec<u8> = Vec::new();
    loop {
        w.write_all(format!("albumart {} {}\n", quote(uri), all.len()).as_bytes())
            .ok()?;
        w.flush().ok()?;
        let mut total = 0usize;
        let mut chunk = 0usize;
        loop {
            let line = read_line(&mut r).ok()?;
            if let Some(v) = line.strip_prefix("size: ") {
                total = v.trim().parse().ok()?;
            } else if let Some(v) = line.strip_prefix("binary: ") {
                let n: usize = v.trim().parse().ok()?;
                // Sanity clamp: never trust a wild length into an allocation.
                if n > 8 * 1024 * 1024 {
                    return None;
                }
                let mut buf = vec![0u8; n];
                r.read_exact(&mut buf).ok()?;
                all.extend_from_slice(&buf);
                chunk = n;
                // The raw payload is followed by a lone `\n` then `OK`; the empty
                // line is consumed (and ignored) by the next read_line.
            } else if line == "OK" {
                break;
            } else if line.starts_with("ACK") {
                return if all.is_empty() { None } else { Some(all) };
            }
        }
        if total == 0 || chunk == 0 || all.len() >= total {
            break;
        }
    }
    if all.is_empty() {
        None
    } else {
        Some(all)
    }
}

/// Bayer 4x4 ordered-dither matrix, centered to roughly [-8, +7].
const BAYER4: [[i16; 4]; 4] = [
    [0, 8, 2, 10],
    [12, 4, 14, 6],
    [3, 11, 1, 9],
    [15, 7, 13, 5],
];

/// Map an image into `cols x rows` upper-half-block cells (top pixel = fg, bottom =
/// bg), nearest-neighbour sampled and ordered-dithered. Pure + unit-tested.
fn render_lines(img: &RgbImage, cols: usize, rows: usize) -> Vec<Line<'static>> {
    let (iw, ih) = img.dimensions();
    if cols == 0 || rows == 0 || iw == 0 || ih == 0 {
        return Vec::new();
    }
    let pw = cols;
    let ph = rows * 2;
    let sample = |px: usize, py: usize| -> [u8; 3] {
        let sx = (px * iw as usize / pw).min(iw as usize - 1);
        let sy = (py * ih as usize / ph).min(ih as usize - 1);
        let p = img.get_pixel(sx as u32, sy as u32);
        [p[0], p[1], p[2]]
    };
    let dither = |c: [u8; 3], x: usize, y: usize| -> Color {
        let t = BAYER4[y % 4][x % 4] - 8;
        let ch = |v: u8| (v as i16 + t).clamp(0, 255) as u8;
        Color::Rgb(ch(c[0]), ch(c[1]), ch(c[2]))
    };
    let mut lines = Vec::with_capacity(rows);
    for row in 0..rows {
        let mut spans = Vec::with_capacity(cols);
        for col in 0..cols {
            let top = sample(col, row * 2);
            let bot = sample(col, row * 2 + 1);
            let fg = dither(top, col, row * 2);
            let bg = dither(bot, col, row * 2 + 1);
            spans.push(Span::styled("\u{2580}", Style::default().fg(fg).bg(bg)));
        }
        lines.push(Line::from(spans));
    }
    lines
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn render_lines_shape_and_halfblock() {
        // A 4x4 solid image -> 3x2 cells = 3 spans/line, 2 lines, all upper-half.
        let img = RgbImage::from_pixel(4, 4, image::Rgb([120, 60, 200]));
        let lines = render_lines(&img, 3, 2);
        assert_eq!(lines.len(), 2, "rows");
        for l in &lines {
            assert_eq!(l.spans.len(), 3, "cols");
            for s in &l.spans {
                assert_eq!(s.content.as_ref(), "\u{2580}", "upper half block");
                assert!(s.style.fg.is_some() && s.style.bg.is_some(), "fg=top bg=bottom");
            }
        }
    }

    #[test]
    fn render_lines_degenerate_is_empty() {
        let img = RgbImage::from_pixel(2, 2, image::Rgb([0, 0, 0]));
        assert!(render_lines(&img, 0, 4).is_empty());
        assert!(render_lines(&img, 4, 0).is_empty());
    }

    #[test]
    fn quote_escapes() {
        assert_eq!(quote("song/1"), "\"song/1\"");
        assert_eq!(quote(r#"a"b\c"#), "\"a\\\"b\\\\c\"");
    }
}
