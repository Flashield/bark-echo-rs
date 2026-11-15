//! Opening a URL in the user's default browser — the one platform-
//! shaped spawn per OS, shared by the notification click action
//! ([`crate::notify`]) and the Tray Menu's Manage handoff
//! ([`crate::tray_host`]).

/// Open `url` in the user's default browser — best effort: a failure
/// to spawn the opener is returned, never takes the caller down (a
/// missed click action or Manage handoff costs the one URL, not the
/// daemon or the tray).
///
/// Platform facts (CODING_STANDARDS): macOS hands a URL to `open`;
/// Linux to `xdg-open`; Windows rides `cmd /C start`, whose first
/// quoted argument is the console title — the empty string placeholder
/// keeps a URL that could parse as a title from being swallowed. The
/// Windows spawn also carries `CREATE_NO_WINDOW` (0x0800_0000 per
/// Microsoft's process-creation flag table): without it the spawned
/// `cmd` flashes an empty console window — on every Manage click when
/// the tray host runs without a console of its own, and on every
/// notification click from a windowless daemon. The spawned opener is
/// reaped on a throwaway thread: the daemon and the tray host run for
/// days, and an unreaped short-lived child would sit in the process
/// table the whole time. Its exit status is the OS's business, not
/// ours.
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
    std::thread::spawn(move || {
        let _ = child.wait();
    });
    Ok(())
}
