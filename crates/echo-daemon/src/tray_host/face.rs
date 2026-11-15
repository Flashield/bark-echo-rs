//! The management face's address and the browser handoff (issue #30):
//! the URL the Tray Menu's Manage item opens, and the one platform-
//! shaped spawn per OS that hands it to the user's default browser.
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

/// Open `url` in the user's default browser — best effort: a failure
/// to spawn the opener is returned, never takes the tray down (the
/// face stays reachable; only the one-click shortcut is missing).
///
/// Platform facts (CODING_STANDARDS): macOS hands a URL to `open`;
/// Linux to `xdg-open`; Windows rides `cmd /C start`, whose first
/// quoted argument is the console title — the empty string placeholder
/// keeps a URL that could parse as a title from being swallowed. The
/// spawned opener is reaped on a throwaway thread: the host runs for
/// days, and an unreaped short-lived child would sit in the process
/// table the whole time.
pub fn open_in_default_browser(url: &str) -> std::io::Result<()> {
    #[cfg(target_os = "macos")]
    let mut command = {
        let mut command = std::process::Command::new("open");
        command.arg(url);
        command
    };
    #[cfg(target_os = "linux")]
    let mut command = {
        let mut command = std::process::Command::new("xdg-open");
        command.arg(url);
        command
    };
    #[cfg(windows)]
    let mut command = {
        use std::os::windows::process::CommandExt;

        // Platform fact: without CREATE_NO_WINDOW the spawned `cmd`
        // flashes an empty console window on every Manage click when
        // the host runs without one of its own (the spawned-detached
        // tray, a Start-menu launch). 0x0800_0000 per Microsoft's
        // process-creation flag table.
        const CREATE_NO_WINDOW: u32 = 0x0800_0000;
        let mut command = std::process::Command::new("cmd");
        command
            .args(["/C", "start", "", url])
            .creation_flags(CREATE_NO_WINDOW);
        command
    };

    command.stdin(std::process::Stdio::null());
    command.stdout(std::process::Stdio::null());
    command.stderr(std::process::Stdio::null());
    let mut child = command.spawn()?;
    // Reap the opener so it never lingers as a zombie for the host's
    // lifetime; its exit status is the OS's business, not ours.
    std::thread::spawn(move || {
        let _ = child.wait();
    });
    Ok(())
}
