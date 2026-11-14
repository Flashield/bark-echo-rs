//! The platform-independent seam of the Windows toast activation (ticket
//! #17): the launch-argument encoding that carries a click action from
//! the rendered toast to the COM `INotificationActivationCallback`, the
//! toast XML the renderer builds under the registered AUMID, and the
//! clipboard byte format the `copy` click feeds to clip.exe. These
//! decisions run on Windows but are pure data — so they are pinned by
//! tests on every platform (the WinRT/COM I/O around them is Windows
//! CI's build + clippy gate; a Linux runner cannot host a toast).

use echo_core::intent::{ClickAction, NotificationIntent};
use echo_daemon::notify::clip_stdin_bytes;
use echo_daemon::toast_activation::{decode_launch_argument, encode_launch_argument, toast_xml};

fn url_action(url: &str) -> ClickAction {
    ClickAction::OpenUrl {
        url: url.to_owned(),
    }
}

fn copy_action(text: &str) -> ClickAction {
    ClickAction::Copy {
        text: text.to_owned(),
    }
}

#[test]
fn a_url_action_round_trips_through_the_launch_argument() {
    let action = url_action("https://ci.example.org/runs/42?status=green");
    let encoded = encode_launch_argument(&action);
    assert_eq!(decode_launch_argument(&encoded), Some(action));
}

#[test]
fn a_copy_action_round_trips_through_the_launch_argument() {
    let action = copy_action("standup in five minutes");
    let encoded = encode_launch_argument(&action);
    assert_eq!(decode_launch_argument(&encoded), Some(action));
}

#[test]
fn url_and_copy_arguments_are_distinguishable() {
    let url = encode_launch_argument(&url_action("https://example.org/a"));
    let copy = encode_launch_argument(&copy_action("https://example.org/a"));
    assert_ne!(url, copy);
    assert_eq!(
        decode_launch_argument(&copy),
        Some(copy_action("https://example.org/a"))
    );
    assert_eq!(
        decode_launch_argument(&url),
        Some(url_action("https://example.org/a"))
    );
}

#[test]
fn characters_that_would_ambiguate_the_encoding_are_escaped() {
    // A URL carrying separators of the launch format itself (`:`, `=`,
    // `&`) and raw bytes (space, CJK) must survive byte-exactly.
    let action = url_action("https://例え.jp/a b?q=1&x=:y");
    assert_eq!(
        decode_launch_argument(&encode_launch_argument(&action)),
        Some(action)
    );
}

#[test]
fn a_multiline_copy_text_round_trips() {
    let action = copy_action("first line\nsecond line\t tabbed 中文");
    assert_eq!(
        decode_launch_argument(&encode_launch_argument(&action)),
        Some(action)
    );
}

#[test]
fn malformed_arguments_decode_to_none_instead_of_acting() {
    // The COM callback receives whatever the toast carried — a foreign
    // or corrupted string must degrade to "no action", never to a click.
    for garbage in [
        "",
        "not-bark-echo",
        "bark-echo:action=open&value=https%3A%2F%2Fexample.org",
        "bark-echo:action=url",           // no value part
        "bark-echo:action=url&value=",    // empty url
        "bark-echo:action=copy&value=",   // empty text
        "bark-echo:action=url&value=%ZZ", // broken escape
        "bark-echo:action=url&value=https%3A%2F%2F&extra=1",
    ] {
        assert_eq!(
            decode_launch_argument(garbage),
            None,
            "argument {garbage:?} must not decode to an action"
        );
    }
}

#[test]
fn the_encoding_never_carries_its_own_separators_raw() {
    // `:`, `=`, and `&` separate the fields; the value's encoding must
    // leave none of them raw, or a crafted value could smuggle a second
    // field past the decoder.
    let encoded = encode_launch_argument(&url_action("a=b&c=d:e"));
    let value = encoded.split_once("&value=").expect("value field").1;
    assert!(
        !value.contains(['&', '=', ':']),
        "raw separator survived in {value:?}"
    );
}

#[test]
fn the_toast_xml_carries_the_action_in_an_escaped_launch_attribute() {
    let intent = NotificationIntent {
        id: 1,
        title: "Build finished".to_owned(),
        subtitle: String::new(),
        body: "branch main is green".to_owned(),
        click_action: Some(url_action("https://ci.example.org/runs/42?a=1&b=2")),
        created_at: 0,
    };
    let xml = toast_xml(&intent);
    assert!(xml.starts_with("<toast"), "unexpected shape: {xml}");
    let launch = xml_launch_attribute(&xml);
    // XML-unescape, then decode: the round trip proves the attribute
    // carries the action intact for the COM callback.
    let unescaped = launch
        .replace("&quot;", "\"")
        .replace("&lt;", "<")
        .replace("&gt;", ">")
        .replace("&amp;", "&");
    assert_eq!(
        decode_launch_argument(&unescaped),
        Some(url_action("https://ci.example.org/runs/42?a=1&b=2"))
    );
}

#[test]
fn the_toast_xml_escapes_text_that_would_break_the_markup() {
    let intent = NotificationIntent {
        id: 1,
        title: "a < b & c > \"d\"".to_owned(),
        subtitle: String::new(),
        body: "<script>alert(\"x\")</script>".to_owned(),
        click_action: Some(copy_action("irrelevant")),
        created_at: 0,
    };
    let xml = toast_xml(&intent);
    assert!(
        xml.contains("a &lt; b &amp; c &gt; &quot;d&quot;"),
        "title unescaped: {xml}"
    );
    assert!(!xml.contains("<script>"), "body escaped: {xml}");
}

#[test]
fn the_toast_xml_splits_title_and_body_like_the_renderer() {
    // The same summary/body split as the notify-rust path (notify.rs's
    // render_parts): title as the first text, subtitle folded above the
    // body in the second.
    let intent = NotificationIntent {
        id: 1,
        title: "Title".to_owned(),
        subtitle: "Subtitle".to_owned(),
        body: "Body".to_owned(),
        click_action: Some(copy_action("x")),
        created_at: 0,
    };
    let xml = toast_xml(&intent);
    assert!(xml.contains("<text>Title</text>"), "{xml}");
    assert!(xml.contains("<text>Subtitle\nBody</text>"), "{xml}");
}

#[test]
fn the_toast_xml_holds_a_launch_attribute_only_for_click_actions() {
    let without = NotificationIntent {
        id: 1,
        title: "Title".to_owned(),
        subtitle: String::new(),
        body: "Body".to_owned(),
        click_action: None,
        created_at: 0,
    };
    assert!(!toast_xml(&without).contains("launch="));
    let mut with = without.clone();
    with.click_action = Some(url_action("https://example.org"));
    assert!(toast_xml(&with).contains("launch="));
}

/// Pull the `launch="…"` attribute value back out of a toast XML — the
/// test-side inverse of the escaping the builder applies.
fn xml_launch_attribute(xml: &str) -> String {
    let marker = "launch=\"";
    let start = xml.find(marker).expect("launch attribute") + marker.len();
    let rest = &xml[start..];
    let end = rest.find('"').expect("closing quote");
    rest[..end].to_owned()
}

#[test]
fn the_clipboard_bytes_carry_a_bom_so_clip_exe_reads_unicode() {
    // Platform fact: clip.exe without a BOM interprets piped bytes in
    // the console codepage, garbling every non-ASCII character — and
    // Bark's primary audience pushes CJK text. With the UTF-16LE BOM
    // (FF FE) it reads the bytes as wide characters.
    let bytes = clip_stdin_bytes("标题 body");
    assert_eq!(&bytes[..2], &[0xFF, 0xFE], "missing UTF-16LE BOM");
    // 标 = U+6807 → LE bytes 07 68.
    assert_eq!(&bytes[2..4], &[0x07, 0x68]);
    // ASCII rides as one byte plus a NUL, exactly wide-char LE: after
    // the BOM and the two CJK units, bytes 6-7 are the space unit and
    // bytes 8-9 the `b` of `body`.
    assert_eq!(&bytes[8..10], &[0x62, 0x00], "b of body");
}

#[test]
fn the_clipboard_bytes_hold_the_whole_text() {
    let text = "first line\nsecond line 中文";
    let bytes = clip_stdin_bytes(text);
    let expected = "FFFE".to_owned()
        + &text
            .encode_utf16()
            .collect::<Vec<_>>()
            .iter()
            .map(|unit| format!("{:02X}{:02X}", unit & 0xFF, unit >> 8))
            .collect::<String>();
    let actual: String = bytes.iter().map(|byte| format!("{byte:02X}")).collect();
    assert_eq!(actual, expected);
}
