//! The management face's address (issue #30): the URL the Tray Menu's
//! Manage item opens. The browser handoff itself is the shared spawn
//! in [`crate::browser`]; this module owns only the address the handoff
//! opens.
//!
//! The face's bind address and port are the daemon's `[management]`
//! config territory (ticket #31's key); the host takes the configured
//! bind from the launcher and opens the address the daemon actually
//! serves ([`management_face_url`]) — the handoff itself is unchanged.

use std::net::SocketAddr;

/// The management face's documented default bind (`127.0.0.1:8081`,
/// ADR-0005) — the fallback when the configured `[management] bind`
/// does not parse.
const DEFAULT_FACE_URL: &str = "http://127.0.0.1:8081";

/// The URL the Manage item opens, from the configured `[management]`
/// bind (config.toml, ticket #31): the address the daemon actually
/// serves. A bind that does not parse falls back to the default — a
/// config typo must not cost the user the one-click handoff (the
/// daemon itself fails loud on the same key at startup; the tray is
/// the quieter half). An unspecified address (the LAN posture's
/// `0.0.0.0:port`) opens at loopback on the same port — the address
/// the machine the tray runs on reaches the face by.
pub fn management_face_url(configured: &str) -> String {
    match configured.parse::<SocketAddr>() {
        Ok(addr) if addr.ip().is_unspecified() => {
            format!("http://127.0.0.1:{}", addr.port())
        }
        Ok(addr) => format!("http://{addr}"),
        Err(_) => DEFAULT_FACE_URL.to_owned(),
    }
}
