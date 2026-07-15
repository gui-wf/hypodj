//! Host/port resolution. Precedence: explicit flags > HYPODJ_HOST/HYPODJ_PORT >
//! MPD_HOST/MPD_PORT > 127.0.0.1:6600. The default 6600 matches the LIVE deploy
//! (the running daemon binds 127.0.0.1:6600); a DEV daemon defaults to 6601
//! (mopidy owns 6600 on dev boxes) so point a dev run at it with HYPODJ_PORT=6601.

pub const DEFAULT_HOST: &str = "127.0.0.1";
pub const DEFAULT_PORT: u16 = 6600;

/// Environment lookups threaded as a closure so resolution is a pure, testable
/// function (no real process env in tests).
pub struct Env<'a> {
    pub get: &'a dyn Fn(&str) -> Option<String>,
}

impl<'a> Env<'a> {
    fn v(&self, k: &str) -> Option<String> {
        (self.get)(k)
    }
}

/// Resolve (host, port) from optional flag overrides plus the environment.
/// `flag_host` / `flag_port` come from the CLI parse (highest precedence).
pub fn resolve(
    flag_host: Option<String>,
    flag_port: Option<u16>,
    env: &Env,
) -> (String, u16) {
    // Base host from env chain.
    let (mut host, mut port_from_hostspec) = (None, None);

    if let Some(h) = env.v("HYPODJ_HOST") {
        host = Some(h);
    } else if let Some(h) = env.v("MPD_HOST") {
        // MPD_HOST may carry "[password@]host[:port]". Drop the password prefix
        // (everything up to and including the last '@'), then split host:port.
        let h = h.rsplit('@').next().map(str::to_string).unwrap_or(h);
        if let Some((h, p)) = split_hostspec(&h) {
            host = Some(h);
            port_from_hostspec = p.parse::<u16>().ok();
        } else {
            host = Some(h);
        }
    }

    let mut port = None;
    if let Some(p) = env.v("HYPODJ_PORT").and_then(|s| s.parse::<u16>().ok()) {
        port = Some(p);
    } else if let Some(p) = env.v("MPD_PORT").and_then(|s| s.parse::<u16>().ok()) {
        port = Some(p);
    } else if let Some(p) = port_from_hostspec {
        port = Some(p);
    }

    // Flags win over everything.
    let host = flag_host.or(host).unwrap_or_else(|| DEFAULT_HOST.to_string());
    let port = flag_port.or(port).unwrap_or(DEFAULT_PORT);
    (host, port)
}

/// Split a "host:port" spec on the LAST ':' if the tail parses as a port. A bare
/// host (no colon, or a trailing non-numeric) returns None so it is used as-is.
fn split_hostspec(s: &str) -> Option<(String, String)> {
    let idx = s.rfind(':')?;
    let (h, p) = (&s[..idx], &s[idx + 1..]);
    if !p.is_empty() && p.chars().all(|c| c.is_ascii_digit()) {
        Some((h.to_string(), p.to_string()))
    } else {
        None
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::collections::HashMap;

    fn env_of(pairs: &[(&str, &str)]) -> HashMap<String, String> {
        pairs.iter().map(|(k, v)| (k.to_string(), v.to_string())).collect()
    }

    fn resolve_with(
        flag_host: Option<&str>,
        flag_port: Option<u16>,
        map: HashMap<String, String>,
    ) -> (String, u16) {
        let env = Env { get: &|k| map.get(k).cloned() };
        resolve(flag_host.map(str::to_string), flag_port, &env)
    }

    #[test]
    fn default_is_live_port() {
        assert_eq!(resolve_with(None, None, env_of(&[])), ("127.0.0.1".into(), 6600));
    }

    #[test]
    fn hypodj_env_beats_mpd() {
        let m = env_of(&[
            ("HYPODJ_HOST", "1.2.3.4"),
            ("HYPODJ_PORT", "6601"),
            ("MPD_HOST", "9.9.9.9"),
            ("MPD_PORT", "5000"),
        ]);
        assert_eq!(resolve_with(None, None, m), ("1.2.3.4".into(), 6601));
    }

    #[test]
    fn mpd_hostspec_with_embedded_port() {
        let m = env_of(&[("MPD_HOST", "myhost:6601")]);
        assert_eq!(resolve_with(None, None, m), ("myhost".into(), 6601));
    }

    #[test]
    fn explicit_port_env_beats_hostspec_port() {
        let m = env_of(&[("MPD_HOST", "myhost:6601"), ("MPD_PORT", "7000")]);
        assert_eq!(resolve_with(None, None, m), ("myhost".into(), 7000));
    }

    #[test]
    fn flags_win() {
        let m = env_of(&[("HYPODJ_HOST", "1.2.3.4"), ("HYPODJ_PORT", "6601")]);
        assert_eq!(resolve_with(Some("host"), Some(9999), m), ("host".into(), 9999));
    }

    #[test]
    fn bare_host_no_colon() {
        let m = env_of(&[("MPD_HOST", "localhost")]);
        assert_eq!(resolve_with(None, None, m), ("localhost".into(), 6600));
    }
}
