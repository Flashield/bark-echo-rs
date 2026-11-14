//! Windows toast activation (ticket #17) — the layer notify-rust's
//! Windows backend cannot express: an AppUserModelID with a Start Menu
//! shortcut plus a COM `INotificationActivationCallback`, so a toast
//! click comes back to the daemon instead of being dropped (research
//! note §10.3: WinRT only routes activation to apps that own an AUMID).
//! The pure decisions live here on every platform and are pinned by
//! `tests/toast_activation.rs`; the WinRT/COM I/O lives in the `windows`
//! submodule and is compiled only for Windows targets, where Windows
//! CI's build + clippy first-party step is the gate (a Linux runner
//! cannot compile — let alone host — a toast).
//!
//! Why a launch argument: toast activation is asynchronous — the click
//! can land minutes after the daemon rendered the toast, and it can be
//! served by a fresh process (the COM local server) that never saw the
//! intent. So the click action rides the toast's `launch` attribute in a
//! self-contained encoding, `bark-echo:action=<kind>&value=<percent-
//! encoded>`, and whoever serves the activation decodes it and performs
//! the same click semantics as macOS/Linux
//! ([`crate::notify::perform_click`]).

use echo_core::intent::{ClickAction, NotificationIntent};

// The Windows-only I/O (registration, COM server, WinRT toast show) and
// its re-exports for the renderer and the binary's local-server entry —
// compiled only for Windows targets.
#[cfg(windows)]
mod windows;
#[cfg(windows)]
pub(crate) use windows::{Activation, register, show_click_toast};
#[cfg(windows)]
pub use windows::{is_activation_process, serve_activation_process};

/// The AppUserModelID (AUMID) this machine's daemon registers: the
/// identity WinRT attaches toasts to. Stable across releases — toasts
/// sitting in the Action Center activate through the same identity after
/// an upgrade.
pub const AUMID: &str = "Flashield.bark-echo";

/// The COM class id of the toast activator (the
/// `INotificationActivationCallback` server). Stable across releases and
/// shared by the Start Menu shortcut, the registry, and this binary — a
/// change would orphan the activation of already-shown toasts.
pub const ACTIVATOR_CLSID: &str = "{6A2AF589-B22E-4C39-8D83-8DA82696FE94}";

/// The name the shell shows for the AUMID (Action Center grouping, the
/// toast's origin line) while the app has no icon asset to point at.
pub const DISPLAY_NAME: &str = "bark-echo";

/// The prefix marking a launch argument as ours (the shell hands the
/// callback whatever the toast carried, foreign strings included).
const LAUNCH_PREFIX: &str = "bark-echo:action=";

/// Encode a click action into the toast's `launch` attribute. Both the
/// toast builder and the activation callback depend on this format
/// staying byte-compatible — see [`decode_launch_argument`].
pub fn encode_launch_argument(action: &ClickAction) -> String {
    let (kind, value) = match action {
        ClickAction::OpenUrl { url } => ("url", url),
        ClickAction::Copy { text } => ("copy", text),
    };
    format!("{LAUNCH_PREFIX}{kind}&value={}", percent_encode(value))
}

/// Decode a toast's launch argument back into the click action. Anything
/// that is not exactly a well-formed argument of ours — a foreign
/// string, a corrupted one, an empty value — decodes to `None`, which
/// the callback treats as "no action", never as a click.
pub fn decode_launch_argument(argument: &str) -> Option<ClickAction> {
    let rest = argument.strip_prefix(LAUNCH_PREFIX)?;
    let (kind, encoded) = rest.split_once("&value=")?;
    let value = percent_decode(encoded)?;
    // Empty values cannot act: the intent seam (echo-core) only resolves
    // non-empty `url`/`copy` Ext Params, so an empty value here means a
    // mangled argument, not an action.
    match kind {
        "url" if !value.is_empty() => Some(ClickAction::OpenUrl { url: value }),
        "copy" if !value.is_empty() => Some(ClickAction::Copy { text: value }),
        _ => None,
    }
}

/// Percent-encode with the RFC 3986 unreserved set: everything that
/// could confuse the surrounding format (`:`, `=`, `&`, `%`, space,
/// non-ASCII) leaves the value byte-reversible and separator-free.
fn percent_encode(value: &str) -> String {
    let mut out = String::with_capacity(value.len());
    for byte in value.bytes() {
        match byte {
            b'A'..=b'Z' | b'a'..=b'z' | b'0'..=b'9' | b'-' | b'_' | b'.' | b'~' => {
                out.push(byte as char)
            }
            _ => out.push_str(&format!("%{byte:02X}")),
        }
    }
    out
}

/// The strict inverse of [`percent_encode`]: unreserved bytes pass
/// through, `%XX` escapes decode, and every other raw byte is rejected —
/// a raw separator would mean the argument was not written by
/// [`encode_launch_argument`].
fn percent_decode(encoded: &str) -> Option<String> {
    let bytes = encoded.as_bytes();
    let mut out = Vec::with_capacity(bytes.len());
    let mut index = 0;
    while index < bytes.len() {
        match bytes[index] {
            b'%' => {
                let hex = bytes.get(index + 1..index + 3)?;
                let high = (hex[0] as char).to_digit(16)?;
                let low = (hex[1] as char).to_digit(16)?;
                out.push((high * 16 + low) as u8);
                index += 3;
            }
            byte if byte.is_ascii_alphanumeric() || matches!(byte, b'-' | b'_' | b'.' | b'~') => {
                out.push(byte);
                index += 1;
            }
            // A raw separator (or any other byte) — not our encoding.
            _ => return None,
        }
    }
    String::from_utf8(out).ok()
}

/// The toast XML for one intent, shown under [`AUMID`] by the Windows
/// renderer. The visual splits title and body exactly like the
/// notify-rust path ([`crate::notify::render_parts`]) so both Windows
/// renderers show the same shape; the launch attribute carries the click
/// action only when there is one. Every dynamic string is XML-escaped —
/// toast bodies are user-controlled Push text.
pub fn toast_xml(intent: &NotificationIntent) -> String {
    let (summary, body) = crate::notify::render_parts(intent);
    let mut xml = String::from("<toast");
    if let Some(action) = &intent.click_action {
        xml.push_str(&format!(
            " launch=\"{}\"",
            xml_escape(&encode_launch_argument(action))
        ));
    }
    xml.push_str("><visual><binding template=\"ToastGeneric\"><text>");
    xml.push_str(&xml_escape(&summary));
    xml.push_str("</text><text>");
    xml.push_str(&xml_escape(&body));
    xml.push_str("</text></binding></visual></toast>");
    xml
}

/// Escape the characters XML markup reads inside an attribute or text
/// node (the toast XML parser hands the callback the unescaped bytes).
fn xml_escape(text: &str) -> String {
    let mut out = String::with_capacity(text.len());
    for character in text.chars() {
        match character {
            '&' => out.push_str("&amp;"),
            '<' => out.push_str("&lt;"),
            '>' => out.push_str("&gt;"),
            '"' => out.push_str("&quot;"),
            other => out.push(other),
        }
    }
    out
}
