//! Hand-rolled blocking MPD client. ONE persistent TcpStream per hjq invocation:
//! the daemon stamps a per-connection owner_key, and `nl confirm`/`nl cancel`
//! only work on the SAME socket that ran the translate (a connect-per-command
//! client is silently rejected). So open once, verify the greeting, run every
//! command on it, never reconnect. NEVER send `idle` on THIS command socket - the
//! owner-scoped `nl confirm`/`nl cancel` handshake must own the turn. `idle` is
//! allowed ONLY on a dedicated, separate socket (see `idle_once`) that never runs
//! owner-scoped commands.

use std::io::{BufRead, BufReader, Write};
use std::net::TcpStream;
use std::time::Duration;

const CONNECT_TIMEOUT: Duration = Duration::from_secs(3);
const IO_TIMEOUT: Duration = Duration::from_secs(5);

#[derive(Debug, thiserror::Error)]
pub enum MpdError {
    /// Daemon not listening: mapped to a friendly, actionable message.
    #[error("hypodj daemon not running on {0} (start it or set HYPODJ_HOST/PORT)")]
    ConnectionRefused(String),
    /// An `ACK ... {cmd} message` from the server: the message after the last '}'.
    #[error("{0}")]
    Ack(String),
    #[error("io error: {0}")]
    Io(String),
    /// Greeting was not "OK MPD ...": something other than a hypodj/MPD daemon.
    #[error("not an MPD/hypodj server at {0} (unexpected greeting)")]
    BadGreeting(String),
}

pub struct MpdConn {
    stream: TcpStream,
    reader: BufReader<TcpStream>,
    #[allow(dead_code)]
    endpoint: String,
}

impl MpdConn {
    pub fn connect(host: &str, port: u16) -> Result<Self, MpdError> {
        let endpoint = format!("{host}:{port}");
        let addr = (host, port);
        // Resolve then connect with a timeout so a wedged/absent daemon fails fast.
        let sock = std::net::ToSocketAddrs::to_socket_addrs(&addr)
            .map_err(|e| MpdError::Io(e.to_string()))?
            .next()
            .ok_or_else(|| MpdError::Io(format!("could not resolve {endpoint}")))?;
        let stream = TcpStream::connect_timeout(&sock, CONNECT_TIMEOUT).map_err(|e| {
            if e.kind() == std::io::ErrorKind::ConnectionRefused {
                MpdError::ConnectionRefused(endpoint.clone())
            } else {
                MpdError::Io(e.to_string())
            }
        })?;
        stream.set_read_timeout(Some(IO_TIMEOUT)).map_err(|e| MpdError::Io(e.to_string()))?;
        stream.set_write_timeout(Some(IO_TIMEOUT)).map_err(|e| MpdError::Io(e.to_string()))?;
        let reader = BufReader::new(stream.try_clone().map_err(|e| MpdError::Io(e.to_string()))?);
        let mut conn = MpdConn { stream, reader, endpoint: endpoint.clone() };
        let greeting = conn.read_line()?;
        if !greeting.starts_with("OK MPD") {
            return Err(MpdError::BadGreeting(endpoint));
        }
        Ok(conn)
    }

    fn read_line(&mut self) -> Result<String, MpdError> {
        let mut line = String::new();
        let n = self.reader.read_line(&mut line).map_err(|e| MpdError::Io(e.to_string()))?;
        if n == 0 {
            return Err(MpdError::Io("connection closed by server".into()));
        }
        // Strip the trailing newline (and optional CR).
        while line.ends_with('\n') || line.ends_with('\r') {
            line.pop();
        }
        Ok(line)
    }

    /// Send one command line and read the response frame until an exact "OK" or an
    /// "ACK " line. Returns the parsed key/value pairs on success.
    pub fn command(&mut self, line: &str) -> Result<Vec<(String, String)>, MpdError> {
        self.stream
            .write_all(format!("{line}\n").as_bytes())
            .map_err(|e| MpdError::Io(e.to_string()))?;
        self.stream.flush().map_err(|e| MpdError::Io(e.to_string()))?;
        let mut body = Vec::new();
        loop {
            let l = self.read_line()?;
            if l == "OK" {
                break;
            }
            if l.starts_with("ACK ") || l == "ACK" {
                return Err(MpdError::Ack(ack_message(&l)));
            }
            body.push(l);
        }
        Ok(parse_pairs(&body))
    }

    /// Clear the read timeout on this socket (blocking reads park indefinitely).
    ///
    /// The dedicated idle socket MUST call this right after connect: `idle`
    /// legitimately blocks for minutes waiting for a subsystem change, so reusing
    /// the 5s [`IO_TIMEOUT`] would fire spuriously every 5s and demote the push
    /// socket to a 5s poller - the single most likely liveness bug. Never call this
    /// on the command socket (its bounded timeout is what keeps a wedged daemon from
    /// freezing the UI past 5s).
    pub fn clear_read_timeout(&self) -> std::io::Result<()> {
        self.stream.set_read_timeout(None)
    }

    /// A cloned handle on the underlying stream, for `TcpStream::shutdown(Both)` from
    /// another thread to unblock a read parked in [`idle_once`] or `command` at quit.
    /// The clone shares the same OS socket; a shutdown on it tears down the read.
    pub fn shutdown_handle(&self) -> std::io::Result<TcpStream> {
        self.stream.try_clone()
    }

    /// Issue ONE `idle` and block until the daemon reports a change (or a `noidle`
    /// interrupt yields a bare `OK`). Returns the changed subsystems parsed from the
    /// `changed: <sys>` lines (empty on a bare `OK`). This is the ONLY command that
    /// may run on a dedicated non-command socket; it needs no owner_key. Requires
    /// [`clear_read_timeout`] first, else it self-limits to the 5s IO timeout.
    pub fn idle_once(&mut self) -> Result<Vec<String>, MpdError> {
        self.stream.write_all(b"idle\n").map_err(|e| MpdError::Io(e.to_string()))?;
        self.stream.flush().map_err(|e| MpdError::Io(e.to_string()))?;
        let mut body = Vec::new();
        loop {
            let l = self.read_line()?;
            if l == "OK" {
                break;
            }
            if l.starts_with("ACK ") || l == "ACK" {
                return Err(MpdError::Ack(ack_message(&l)));
            }
            body.push(l);
        }
        Ok(parse_changed(&body))
    }
}

/// Parse the changed subsystems from an `idle` response body: each `changed: <sys>`
/// line yields one subsystem. Tolerant by design - 0..N lines, unknown values pass
/// through, a bare `OK` (or any non-`changed:` line) yields an empty Vec. The daemon
/// today always emits exactly `changed: player` (a single conservative wake), but
/// this already exploits per-subsystem granularity if the daemon later carries it.
/// Pure and testable.
pub fn parse_changed(lines: &[String]) -> Vec<String> {
    lines
        .iter()
        .filter_map(|l| l.strip_prefix("changed: ").map(|s| s.trim().to_string()))
        .filter(|s| !s.is_empty())
        .collect()
}

/// Extract the human message from an ACK line: everything after the LAST '}'
/// (the `{command}` field), trimmed. Falls back to the raw line if no brace.
pub fn ack_message(line: &str) -> String {
    match line.rfind('}') {
        Some(i) => line[i + 1..].trim().to_string(),
        None => line.trim_start_matches("ACK").trim().to_string(),
    }
}

/// Split each body line ONCE on the first ": " (colon-space). Bare ':' is NOT a
/// separator (URIs, titles, times embed colons); keys stay case-sensitive.
pub fn parse_pairs(lines: &[String]) -> Vec<(String, String)> {
    lines
        .iter()
        .filter_map(|l| l.split_once(": ").map(|(k, v)| (k.to_string(), v.to_string())))
        .collect()
}

#[cfg(test)]
mod tests {
    use super::*;

    fn v(lines: &[&str]) -> Vec<String> {
        lines.iter().map(|s| s.to_string()).collect()
    }

    #[test]
    fn framing_split_on_first_colon_space_only() {
        // A value that itself contains ": " must be preserved whole.
        let p = parse_pairs(&v(&["file: http://host:8000/stream", "Title: A: B: C"]));
        assert_eq!(p[0], ("file".into(), "http://host:8000/stream".into()));
        assert_eq!(p[1], ("Title".into(), "A: B: C".into()));
    }

    #[test]
    fn framing_ack_message_after_last_brace() {
        assert_eq!(ack_message("ACK [50@0] {nl} no such nl token"), "no such nl token");
        assert_eq!(
            ack_message("ACK [5@0] {nl} plan no longer valid: queue changed"),
            "plan no longer valid: queue changed"
        );
    }

    #[test]
    fn parse_changed_tolerates_zero_n_and_unknown() {
        // Single wake (what the daemon emits today).
        assert_eq!(parse_changed(&v(&["changed: player"])), vec!["player".to_string()]);
        // Multiple subsystems in one idle response.
        assert_eq!(
            parse_changed(&v(&["changed: playlist", "changed: mixer"])),
            vec!["playlist".to_string(), "mixer".to_string()]
        );
        // Bare OK / empty -> no subsystems (an interrupt wake).
        assert!(parse_changed(&v(&["OK"])).is_empty());
        assert!(parse_changed(&v(&[])).is_empty());
        // Unknown subsystem values pass through untouched (forward-compatible).
        assert_eq!(parse_changed(&v(&["changed: foo"])), vec!["foo".to_string()]);
        // Non-changed lines are ignored.
        assert!(parse_changed(&v(&["OK MPD 0.23.0", "list_OK"])).is_empty());
    }

    #[test]
    fn bare_colon_is_not_a_separator() {
        // "key:value" (no space) is not a pair line.
        assert!(parse_pairs(&v(&["notapair:value"])).is_empty());
    }
}
