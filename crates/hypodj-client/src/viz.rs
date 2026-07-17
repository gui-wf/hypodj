//! The thin viz side-channel client: a plain TCP reader for the daemon's dedicated
//! visualizer socket at `MPD_port + 1`. NO libmpv, NO audio, NO FFT crosses here -
//! just a greeting check plus an integer/float line decode (~220 B/s). A daemon
//! that does not expose the socket (old build) refuses the connect, which the
//! caller treats as the clean "use the decorative fallback wave" signal.

use std::io::{BufRead, BufReader};
use std::net::TcpStream;
use std::time::Duration;

const CONNECT_TIMEOUT: Duration = Duration::from_secs(3);

/// The greeting the viz socket writes first. Anything else means it is not a viz
/// endpoint, so the client should fall back to the decorative wave.
pub const VIZ_GREETING_PREFIX: &str = "OK HYPODJ-VIZ";

/// One decoded level sample. `rms_db`/`peak_db` are RAW (pre-softvol) dBFS; the
/// audible POST-GAIN level is `rms_db + gain_db`. `playing` gates the renderer
/// between the live field and the resting hairline.
#[derive(Clone, Copy, Debug, PartialEq)]
pub struct VizSample {
    pub rms_db: f32,
    pub peak_db: f32,
    pub gain_db: f32,
    pub playing: bool,
}

impl VizSample {
    /// The audible post-gain RMS level in dBFS (`rms_db + gain_db`), which is what
    /// the bars track - so a startle-safe fade reads as a genuine descent.
    pub fn post_gain_db(&self) -> f32 {
        self.rms_db + self.gain_db
    }
}

/// Parse one wire line (`<rms> <peak> <gain> <playing>`) into a [`VizSample`].
/// Returns `None` on any malformed line so a garbled frame is skipped, never a
/// panic. Pure and unit-tested; the inverse of the daemon's `encode_frame`.
pub fn decode_frame(line: &str) -> Option<VizSample> {
    let mut it = line.split_whitespace();
    let rms_db = it.next()?.parse::<f32>().ok()?;
    let peak_db = it.next()?.parse::<f32>().ok()?;
    let gain_db = it.next()?.parse::<f32>().ok()?;
    let playing = match it.next()? {
        "1" => true,
        "0" => false,
        _ => return None,
    };
    if it.next().is_some() {
        return None;
    }
    Some(VizSample { rms_db, peak_db, gain_db, playing })
}

/// A connected viz reader. Owns the socket + a buffered reader; each
/// [`next_sample`](Self::next_sample) blocks for the next frame line.
pub struct VizConn {
    reader: BufReader<TcpStream>,
}

impl VizConn {
    /// Connect to the viz socket at `host:port` (the caller passes `MPD_port + 1`),
    /// verify the greeting, and return a reader. `Err` on a refused connect (old
    /// daemon / no viz socket) or a bad greeting - the caller then uses the fallback
    /// wave. Never blocks longer than the connect timeout on setup.
    pub fn connect(host: &str, port: u16) -> std::io::Result<VizConn> {
        let addr = (host, port);
        let sock = std::net::ToSocketAddrs::to_socket_addrs(&addr)?
            .next()
            .ok_or_else(|| std::io::Error::new(std::io::ErrorKind::NotFound, "no address"))?;
        let stream = TcpStream::connect_timeout(&sock, CONNECT_TIMEOUT)?;
        let mut reader = BufReader::new(stream);
        let mut greeting = String::new();
        reader.read_line(&mut greeting)?;
        if !greeting.starts_with(VIZ_GREETING_PREFIX) {
            return Err(std::io::Error::new(
                std::io::ErrorKind::InvalidData,
                "not a viz endpoint",
            ));
        }
        Ok(VizConn { reader })
    }

    /// Block for the next frame line and decode it. `Ok(None)` means a line that did
    /// not decode (skip it); `Err` means the socket closed / errored (reconnect).
    pub fn next_sample(&mut self) -> std::io::Result<Option<VizSample>> {
        let mut line = String::new();
        let n = self.reader.read_line(&mut line)?;
        if n == 0 {
            return Err(std::io::Error::new(
                std::io::ErrorKind::UnexpectedEof,
                "viz socket closed",
            ));
        }
        Ok(decode_frame(line.trim_end()))
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn decode_and_post_gain() {
        let s = decode_frame("-20.50 -12.00 -6.00 1").unwrap();
        assert_eq!(s.rms_db, -20.50);
        assert!(s.playing);
        // post-gain = rms + gain.
        assert!((s.post_gain_db() - (-26.50)).abs() < 1e-4);
    }

    #[test]
    fn decode_rejects_malformed() {
        assert!(decode_frame("").is_none());
        assert!(decode_frame("a b c d").is_none());
        assert!(decode_frame("-1 -2 -3").is_none());
        assert!(decode_frame("-1 -2 -3 9").is_none());
        assert!(decode_frame("-1 -2 -3 0 x").is_none());
        // A paused frame decodes with playing=false.
        assert!(!decode_frame("-54 -54 0 0").unwrap().playing);
    }
}
