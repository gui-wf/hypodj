//! Daemon configuration, loaded from TOML.
//!
//! FOUNDATION: real, used by the vertical slice.

use serde::Deserialize;
use std::path::Path;

#[derive(Debug, Clone, Deserialize)]
pub struct Config {
    pub server: ServerConfig,
    #[serde(default)]
    pub mpd: MpdConfig,
    #[serde(default)]
    pub mpris: MprisConfig,
}

#[derive(Debug, Clone, Deserialize)]
pub struct ServerConfig {
    /// Base URL of the OpenSubsonic server, e.g. https://music.example.com
    pub url: String,
    pub username: String,
    pub password: String,
    /// Client name reported to the server (OpenSubsonic `c` param).
    #[serde(default = "default_client_name")]
    pub client_name: String,
}

#[derive(Debug, Clone, Deserialize)]
pub struct MpdConfig {
    /// Address the MPD-protocol listener binds to.
    ///
    /// Default is 6601 ON PURPOSE: the real mopidy daemon owns 6600 and must
    /// not be disturbed. Production parity flips this to 6600 once mopidy is
    /// retired.
    #[serde(default = "default_mpd_bind")]
    pub bind: String,
}

impl Default for MpdConfig {
    fn default() -> Self {
        Self { bind: default_mpd_bind() }
    }
}

#[derive(Debug, Clone, Deserialize)]
pub struct MprisConfig {
    /// Expose the MPRIS (org.mpris.MediaPlayer2.hypodj) D-Bus server on the
    /// session bus so desktops show now-playing + controls. Default true; set
    /// false to disable. Registered under the `.hypodj` bus name (NOT `.mopidy`),
    /// so it never conflicts with a running mopidy MPRIS server.
    #[serde(default = "default_mpris_enable")]
    pub enable: bool,
}

impl Default for MprisConfig {
    fn default() -> Self {
        Self { enable: default_mpris_enable() }
    }
}

fn default_mpris_enable() -> bool {
    true
}

fn default_client_name() -> String {
    "hypodj".to_string()
}

fn default_mpd_bind() -> String {
    "127.0.0.1:6601".to_string()
}

#[derive(Debug, thiserror::Error)]
pub enum ConfigError {
    #[error("reading config {0}: {1}")]
    Io(String, #[source] std::io::Error),
    #[error("parsing config: {0}")]
    Parse(#[from] toml::de::Error),
}

impl Config {
    pub fn load(path: &Path) -> Result<Self, ConfigError> {
        let raw = std::fs::read_to_string(path)
            .map_err(|e| ConfigError::Io(path.display().to_string(), e))?;
        Ok(toml::from_str(&raw)?)
    }

    /// Parse from a TOML string (test/embedded use). Kept as an inherent method
    /// with this name for ergonomics; it is not the `FromStr` trait.
    #[allow(clippy::should_implement_trait)]
    pub fn from_str(raw: &str) -> Result<Self, ConfigError> {
        Ok(toml::from_str(raw)?)
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn parses_and_defaults_bind_to_6601_not_6600() {
        // No [mpd] section -> the default must be 6601, honoring the hard
        // constraint that mopidy owns 6600.
        let cfg = Config::from_str(
            r#"
            [server]
            url = "https://music.example.com"
            username = "alice"
            password = "s3cr3t"
        "#,
        )
        .expect("valid config");
        assert_eq!(cfg.server.url, "https://music.example.com");
        assert_eq!(cfg.server.username, "alice");
        assert_eq!(cfg.server.client_name, "hypodj");
        assert_eq!(cfg.mpd.bind, "127.0.0.1:6601");
    }

    #[test]
    fn explicit_bind_overrides_default() {
        let cfg = Config::from_str(
            r#"
            [server]
            url = "https://m.example.com"
            username = "a"
            password = "b"
            [mpd]
            bind = "127.0.0.1:7000"
        "#,
        )
        .unwrap();
        assert_eq!(cfg.mpd.bind, "127.0.0.1:7000");
    }
}
