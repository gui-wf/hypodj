//! hypodj-core: the reusable library half of the daemon.
//!
//! Module map (FOUNDATION vs next-phase):
//!   config    - FOUNDATION: TOML config load.
//!   model     - FOUNDATION: internal domain types (decoupled from wire).
//!   subsonic  - FOUNDATION: real ping/artists/album_list/stream_url; the other
//!               ~75 endpoints are next-phase methods on the same wrapper.
//!   player    - FOUNDATION: the actor boundary (`PlayerHandle` + `PlayerEvent`)
//!               and a real `NullPlayer` actor over it; `MpvPlayer` next-phase.
//!   mpd       - FOUNDATION: MPD command/response/handler INTERFACE (incl. the
//!               ncmpcpp-blocking command set + binary response shape); the TCP
//!               accept/codec loop is next-phase.

pub mod cache;
pub mod clock;
pub mod config;
pub mod director;
pub mod echo;
pub mod event;
pub mod executor;
pub mod fade;
pub mod handler;
pub mod model;
pub mod mpd;
pub mod mpris;
pub mod nl;
pub mod plan;
pub mod player;
pub mod scrobble;
pub mod subsonic;
pub mod timer;
