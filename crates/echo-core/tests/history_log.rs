//! The History Log (GLOSSARY): one append-only JSONL file per Subscription,
//! appended only after an intent is emitted, whose last line's message id is
//! the reconnect cursor. These tests exercise the public file face directly;
//! the receive loop drives it end-to-end in `receive_loop.rs`.

use echo_core::history::HistoryLog;
use echo_core::intent::{ClickAction, NotificationIntent};

fn intent(id: i64, title: &str) -> NotificationIntent {
    NotificationIntent {
        id,
        title: title.to_owned(),
        subtitle: "sub".to_owned(),
        body: "body".to_owned(),
        click_action: Some(ClickAction::OpenUrl {
            url: "https://example.com/x".to_owned(),
        }),
        created_at: 1_760_000_000 + id,
    }
}

#[test]
fn a_missing_or_empty_log_has_no_cursor() {
    let dir = tempfile::tempdir().expect("temp dir");
    let log = HistoryLog::new(dir.path().join("history.jsonl"));

    assert_eq!(log.cursor().expect("read cursor"), None, "no file, no cursor");
    assert_eq!(log.entries().expect("read entries").len(), 0);

    // An empty file is equally cursor-less.
    std::fs::write(log.path(), b"").expect("create empty log");
    assert_eq!(log.cursor().expect("read cursor"), None, "empty file, no cursor");

    drop(dir);
}

#[test]
fn the_last_lines_id_is_the_cursor() {
    let dir = tempfile::tempdir().expect("temp dir");
    let log = HistoryLog::new(dir.path().join("history.jsonl"));

    log.append(&intent(1, "first")).expect("append 1");
    log.append(&intent(2, "second")).expect("append 2");
    log.append(&intent(3, "third")).expect("append 3");

    assert_eq!(log.cursor().expect("read cursor"), Some(3));

    drop(dir);
}

#[test]
fn appended_entries_round_trip_as_jsonl_lines() {
    let dir = tempfile::tempdir().expect("temp dir");
    let log = HistoryLog::new(dir.path().join("history.jsonl"));
    let entries = [intent(1, "first"), intent(2, "second")];
    for entry in &entries {
        log.append(entry).expect("append");
    }

    // One JSON object per line, exactly the appended intents in order.
    let raw = std::fs::read_to_string(log.path()).expect("read log");
    let lines: Vec<&str> = raw.lines().collect();
    assert_eq!(lines.len(), 2, "one line per append:\n{raw}");
    assert!(raw.ends_with('\n'), "append-only JSONL ends with a newline");

    assert_eq!(log.entries().expect("read entries"), entries);

    drop(dir);
}

#[test]
fn a_truncated_last_line_does_not_become_the_cursor() {
    let dir = tempfile::tempdir().expect("temp dir");
    let log = HistoryLog::new(dir.path().join("history.jsonl"));
    log.append(&intent(1, "first")).expect("append 1");
    log.append(&intent(2, "second")).expect("append 2");

    // A crash mid-write could leave a partial final line; the cursor must
    // stay on the last complete line (a replayed id 2 is suppressed by dedup).
    let mut raw = std::fs::read_to_string(log.path()).expect("read log");
    raw.push_str("{\"id\":3,\"tit");
    std::fs::write(log.path(), raw).expect("write torn log");

    assert_eq!(log.cursor().expect("read cursor"), Some(2));

    drop(dir);
}
