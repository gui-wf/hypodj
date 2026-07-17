//! hypodj-client - the single blocking MPD-protocol client shared by the hjq CLI
//! and the hypodj-tui jukebox. ONE persistent socket per session (the daemon
//! stamps a per-connection owner_key, so `nl confirm`/`nl cancel` only work on the
//! SAME socket that ran the translate). Model-free: std::net + thiserror only.

pub mod config;
pub mod grounding;
pub mod model;
pub mod mpd;
pub mod nl;
pub mod route;
pub mod viz;
