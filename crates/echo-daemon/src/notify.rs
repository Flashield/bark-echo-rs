//! The desktop-notification renderer (ticket #5): the one place that
//! touches the machine's notification system. The seam is the
//! [`NotificationRenderer`] trait — tests inject fakes and assert the
//! *intent stream*; this module is the real glue (notify-rust) and the
//! click actions (spec: rendering itself is out of test, the boundary is
//! the intent).
//!
//! Click actions (protocol §2.2 Ext Params, resolved at the core seam):
//! a `url` push opens the default browser on click; a `copy` push writes
//! the text to the clipboard. The action fires when the user activates
//! the notification ("default" action on the XDG desktop).

use echo_core::intent::{ClickAction, NotificationIntent};

/// Why rendering an intent failed — surfaced to the runtime, which logs
/// it and carries on (a notification hiccup must not stop the streams).
#[derive(Debug, thiserror::Error)]
pub enum RenderError {
    /// The desktop notification could not be shown (no notification
    /// service on a headless machine, say).
    #[error("desktop notification failed: {0}")]
    Show(String),
}

/// Renders notification intents as desktop notifications. Implemented by
/// [`DesktopRenderer`] in production and by test fakes elsewhere.
pub trait NotificationRenderer: Send + Sync {
    /// Render one intent. Called once per processed Push, in delivery
    /// order; must not block for long.
    fn render(&self, intent: &NotificationIntent) -> Result<(), RenderError>;
}

/// The real renderer: notify-rust glue with the click action wired to
/// the notification's default action.
#[derive(Debug, Clone, Copy, Default)]
pub struct DesktopRenderer;

impl NotificationRenderer for DesktopRenderer {
    fn render(&self, intent: &NotificationIntent) -> Result<(), RenderError> {
        let (summary, body) = render_parts(intent);
        let mut notification = notify_rust::Notification::new();
        notification.summary(&summary).body(&body);
        // The click action rides the notification's default action —
        // clicking the notification body itself.
        if intent.click_action.is_some() {
            notification.action("default", action_label(intent));
        }
        // The handle carries the async activation events where the
        // platform has them. Windows toast activations live outside
        // notify-rust's event model — the tray stage owns them
        // (ADR-0002) — so there the handle is not needed (the toast is
        // already shown by show() itself).
        #[cfg(not(target_os = "windows"))]
        let handle = notification
            .show()
            .map_err(|err| RenderError::Show(err.to_string()))?;
        #[cfg(target_os = "windows")]
        notification
            .show()
            .map_err(|err| RenderError::Show(err.to_string()))?;

        // Activation arrives asynchronously; waiting on it must not hold
        // up the stream's rendering, so a detached thread watches for the
        // click. Closed-without-click ends the wait.
        #[cfg(not(target_os = "windows"))]
        if let Some(click) = intent.click_action.clone() {
            std::thread::spawn(move || {
                handle.wait_for_action(|action| {
                    if action == "default" {
                        perform_click(&click);
                    }
                });
            });
        }
        #[cfg(target_os = "windows")]
        if intent.click_action.is_some() {
            // Windows toasts deliver activations outside notify-rust's
            // event model — the tray stage owns click actions (ADR-0002).
            // The action is dropped, loudly enough to debug.
            eprintln!(
                "click action: dropped — windows toast activations need the tray stage (ADR-0002)"
            );
        }
        Ok(())
    }
}

/// The click action's button label.
fn action_label(intent: &NotificationIntent) -> &'static str {
    match intent.click_action {
        Some(ClickAction::OpenUrl { .. }) => "Open",
        Some(ClickAction::Copy { .. }) => "Copy",
        None => "",
    }
}

/// The summary/body split a system notification wants: the title is the
/// summary when it exists, else the subtitle carries the summary and the
/// body stands alone; the subtitle is folded into the body otherwise.
fn render_parts(intent: &NotificationIntent) -> (String, String) {
    if !intent.title.is_empty() {
        let mut body = String::new();
        if !intent.subtitle.is_empty() {
            body.push_str(&intent.subtitle);
            body.push('\n');
        }
        body.push_str(&intent.body);
        (intent.title.clone(), body)
    } else if !intent.subtitle.is_empty() {
        (intent.subtitle.clone(), intent.body.clone())
    } else {
        ("Notification".to_owned(), intent.body.clone())
    }
}

/// What a click on the notification does (spec stories 11 and 12). Only
/// the platforms that watch the notification's activation events call
/// it — on Windows the tray stage owns activations (ADR-0002).
#[cfg(not(target_os = "windows"))]
fn perform_click(action: &ClickAction) {
    match action {
        ClickAction::OpenUrl { url } => {
            if let Err(err) = open_url(url) {
                eprintln!("click action: opening {url} failed: {err}");
            }
        }
        ClickAction::Copy { text } => {
            if let Err(err) = copy_to_clipboard(text) {
                eprintln!("click action: copy to clipboard failed: {err}");
            }
        }
    }
}

/// Open a URL in the default browser.
#[cfg(not(target_os = "windows"))]
fn open_url(url: &str) -> Result<(), String> {
    #[cfg(target_os = "linux")]
    let mut command = {
        let mut command = std::process::Command::new("xdg-open");
        command.arg(url);
        command
    };
    #[cfg(target_os = "macos")]
    let mut command = {
        let mut command = std::process::Command::new("open");
        command.arg(url);
        command
    };
    #[cfg(windows)]
    let mut command = {
        let mut command = std::process::Command::new("cmd");
        command.args(["/C", "start"]).arg(url);
        command
    };
    command.spawn().map(|_| ()).map_err(|err| err.to_string())
}

/// Place text on the system clipboard, by the platform's clipboard
/// command fed over stdin (no clipboard crate).
#[cfg(not(target_os = "windows"))]
fn copy_to_clipboard(text: &str) -> Result<(), String> {
    use std::io::Write;

    #[cfg(target_os = "linux")]
    let mut command = clipboard_command_linux();
    #[cfg(target_os = "macos")]
    let mut command = {
        let mut command = std::process::Command::new("pbcopy");
        command.stdin(std::process::Stdio::piped());
        command
    };
    #[cfg(windows)]
    let mut command = {
        let mut command = std::process::Command::new("clip");
        command.stdin(std::process::Stdio::piped());
        command
    };

    let mut child = command.spawn().map_err(|err| err.to_string())?;
    let mut stdin = child
        .stdin
        .take()
        .ok_or_else(|| "clipboard command lost its stdin".to_owned())?;
    stdin
        .write_all(text.as_bytes())
        .map_err(|err| err.to_string())?;
    drop(stdin);
    // The clipboard command exits once stdin closes; reaping it here
    // keeps the daemon's children from piling up as zombies.
    child.wait().map(|_| ()).map_err(|err| err.to_string())
}

/// The Linux clipboard command, Wayland or X11: `wl-copy` under a
/// Wayland session, `xclip` otherwise.
#[cfg(target_os = "linux")]
fn clipboard_command_linux() -> std::process::Command {
    if std::env::var_os("WAYLAND_DISPLAY").is_some() {
        let mut command = std::process::Command::new("wl-copy");
        command.stdin(std::process::Stdio::piped());
        command
    } else {
        let mut command = std::process::Command::new("xclip");
        command
            .args(["-selection", "clipboard", "-i"])
            .stdin(std::process::Stdio::piped());
        command
    }
}
