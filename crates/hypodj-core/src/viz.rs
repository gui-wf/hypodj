//! The cosmetic HUD visualizer side-channel: a DEDICATED TCP socket at
//! `MPD_port + 1` that streams post-decode audio LEVELS to any subscriber.
//!
//! This mirrors the client's dedicated-socket precedent (the idle-push / albumart
//! sockets): the MPD command socket carries the owner-scoped `nl` handshake and a
//! one-shot `idle`, neither of which can host a ~20 fps level stream. Viz is out of
//! band, so `ADVERTISED_MPD_VERSION` is untouched.
//!
//! ## Hard bar: viz must NEVER disrupt audio
//!
//! The daemon-side level source is a NON-FATAL labelled `astats` af node (see
//! [`crate::player`]); a viz-socket error only ever closes that one connection. The
//! stream rides a DEDICATED `broadcast` channel (NOT the shared `DjEvent`
//! broadcast), so its ~20 fps churn cannot raise `Lagged` for other subscribers.
//!
//! ## Kept deliberately simple (per the design critique)
//!
//! ~220 B/s of levels needs no ceremony: NO capability-command negotiation, NO
//! proto-versioning, NO binary header. A one-line-per-frame text protocol is the P1
//! shape - debuggable (`nc host 6602` prints levels) and endian-free. Discovery is
//! derive-not-negotiate: the client connects to `MPD_port + 1` and treats
//! connection-refused as the clean "old daemon / no viz" degrade signal.

use std::net::SocketAddr;

use tokio::io::AsyncWriteExt;
use tokio::net::TcpListener;
use tokio::sync::broadcast;

/// Capacity of the dedicated viz broadcast. Small: viz is inherently latest-wins,
/// so a briefly-stalled writer just drops to the newest frame on `Lagged` rather
/// than resubscribing. A handful of frames of slack absorbs scheduling jitter.
pub const VIZ_BROADCAST_CAP: usize = 8;

/// The per-connection greeting. A client reads this first and, seeing it, knows it
/// reached a viz-capable daemon; anything else (or a refused connect) means fall
/// back to the decorative wave.
pub const VIZ_GREETING: &str = "OK HYPODJ-VIZ 1";

/// One post-decode level frame published on the viz broadcast. `rms_db`/`peak_db`
/// are RAW (pre-softvol) dBFS; `gain_db` is the daemon's current softvol gain, so a
/// client recovers the audible post-gain level as `rms_db + gain_db`. `playing`
/// gates the client between the live field and the resting hairline.
#[derive(Clone, Copy, Debug, PartialEq)]
pub struct VizFrame {
    pub rms_db: f32,
    pub peak_db: f32,
    pub gain_db: f32,
    pub playing: bool,
}

/// Serialize one frame to a single newline-terminated wire line:
/// `<rms> <peak> <gain> <playing>\n` (dBFS at 2 decimals, playing as 0/1). Pure and
/// unit-tested; the exact inverse of [`decode_frame`].
pub fn encode_frame(f: &VizFrame) -> String {
    format!(
        "{:.2} {:.2} {:.2} {}\n",
        f.rms_db,
        f.peak_db,
        f.gain_db,
        if f.playing { 1 } else { 0 }
    )
}

/// Parse one wire line (without or with a trailing newline) back into a
/// [`VizFrame`]. Returns `None` on any malformed line so a partial/garbled frame is
/// simply skipped, never a panic. Pure and unit-tested.
pub fn decode_frame(line: &str) -> Option<VizFrame> {
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
        return None; // trailing garbage: reject rather than half-trust it.
    }
    Some(VizFrame { rms_db, peak_db, gain_db, playing })
}

/// Serve the viz side-channel: accept connections on `bind` and stream every
/// broadcast frame to each. This is spawned best-effort by the daemon; a bind error
/// is returned (logged by the caller) and simply means no viz socket - playback and
/// the MPD server are entirely unaffected.
pub async fn serve_viz(bind: SocketAddr, frames: broadcast::Sender<VizFrame>) -> anyhow::Result<()> {
    let listener = TcpListener::bind(bind).await?;
    tracing::info!(%bind, "viz socket listening");
    loop {
        let (sock, peer) = match listener.accept().await {
            Ok(v) => v,
            Err(e) => {
                tracing::warn!(error = %e, "viz accept failed");
                continue;
            }
        };
        // A fresh receiver per connection: an unsubscribed/old client never existed
        // here, and a wedged client closes only its own conn.
        let rx = frames.subscribe();
        tokio::spawn(async move {
            if let Err(e) = serve_viz_conn(sock, rx).await {
                tracing::debug!(%peer, error = %e, "viz connection closed");
            }
        });
    }
}

/// Drive one viz connection: write the greeting, then stream frames until the
/// socket closes. On `Lagged` we do NOT resubscribe (viz is latest-wins - continue
/// from the newest frame); on `Closed` the daemon is winding down, so we stop.
async fn serve_viz_conn(
    mut sock: tokio::net::TcpStream,
    mut rx: broadcast::Receiver<VizFrame>,
) -> anyhow::Result<()> {
    sock.write_all(format!("{VIZ_GREETING}\n").as_bytes()).await?;
    sock.flush().await?;
    loop {
        match rx.recv().await {
            Ok(frame) => {
                // Best-effort write; a broken pipe just ends this connection.
                sock.write_all(encode_frame(&frame).as_bytes()).await?;
                sock.flush().await?;
            }
            Err(broadcast::error::RecvError::Lagged(_)) => continue,
            Err(broadcast::error::RecvError::Closed) => break,
        }
    }
    Ok(())
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn frame_round_trips_through_the_wire() {
        let f = VizFrame { rms_db: -20.51, peak_db: -12.30, gain_db: -6.00, playing: true };
        let line = encode_frame(&f);
        assert!(line.ends_with('\n'), "frame is newline terminated");
        let back = decode_frame(line.trim_end()).expect("decodes");
        // 2-decimal wire precision: compare at that resolution.
        assert!((back.rms_db - f.rms_db).abs() < 0.005);
        assert!((back.peak_db - f.peak_db).abs() < 0.005);
        assert!((back.gain_db - f.gain_db).abs() < 0.005);
        assert!(back.playing);
    }

    #[test]
    fn decode_tolerates_and_rejects() {
        // A paused frame decodes with playing=false.
        let f = decode_frame("-54.00 -54.00 0.00 0").unwrap();
        assert!(!f.playing);
        assert_eq!(f.rms_db, -54.00);
        // Malformed lines are rejected (skipped), never a panic.
        assert!(decode_frame("").is_none());
        assert!(decode_frame("garbage").is_none());
        assert!(decode_frame("-1 -2 -3").is_none()); // too few fields
        assert!(decode_frame("-1 -2 -3 2").is_none()); // bad playing flag
        assert!(decode_frame("-1 -2 -3 1 extra").is_none()); // trailing garbage
        assert!(decode_frame("x -2 -3 1").is_none()); // non-numeric
    }

    #[tokio::test]
    async fn negative_infinity_gain_encodes_finite_shape() {
        // A silence frame with a very low gain still round-trips as a finite,
        // parseable line (the wire never carries a NaN token).
        let f = VizFrame { rms_db: -120.0, peak_db: -120.0, gain_db: -60.0, playing: false };
        let back = decode_frame(encode_frame(&f).trim_end()).unwrap();
        assert_eq!(back.playing, false);
        assert!(back.rms_db <= -119.0);
    }
}
