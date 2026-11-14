//! The daemon's runtime state file (ADR-0002 MVP: `status` reads files
//! the runtime writes — no IPC): per-Subscription stream state, written
//! atomically on every change, rendered by `status` alongside the live
//! server statistics.

use echo_daemon::config::Subscription;
use echo_daemon::state::{StateFile, StateWriter, StreamState};

/// One configured Subscription: the writer's input (its public identity
/// is what the state file carries).
fn subscription(id: &str, label: &str) -> Subscription {
    Subscription {
        id: id.to_owned(),
        server: format!("http://{id}.example.test:8080"),
        label: label.to_owned(),
        token_hint: format!("{id}hint..."),
        device_hint: "devhint...".to_owned(),
    }
}

#[test]
fn a_fresh_writer_starts_every_subscription_as_starting() {
    let dir = tempfile::tempdir().expect("temp dir");
    let path = dir.path().join("daemon-state.json");

    let writer = StateWriter::create(
        &path,
        4242,
        &[
            subscription("tok-a", "kitchen"),
            subscription("tok-b", "desktop"),
        ],
    )
    .expect("create state");

    let snapshot = writer.snapshot();
    assert_eq!(snapshot.pid, 4242);
    assert_eq!(
        snapshot.subscriptions["tok-a"].state,
        StreamState::Starting,
        "a fresh runtime has not handshaked yet"
    );
    assert_eq!(snapshot.subscriptions["tok-b"].state, StreamState::Starting);

    // The file is on disk already — `status` in another process reads it.
    let read = StateFile::read(&path).expect("read back").expect("present");
    assert_eq!(read.pid, 4242);
    assert_eq!(read.subscriptions.len(), 2);

    // The public identity rides the file (ticket #13): the Management
    // Window lists it as configured, without reading the config itself.
    assert_eq!(
        read.subscriptions["tok-a"].server,
        "http://tok-a.example.test:8080"
    );
    assert_eq!(read.subscriptions["tok-a"].token_hint, "tok-ahint...");
    assert_eq!(read.subscriptions["tok-b"].label, "desktop");
}

#[test]
fn every_update_persists_atomically_and_reads_back() {
    let dir = tempfile::tempdir().expect("temp dir");
    let path = dir.path().join("daemon-state.json");
    let writer = StateWriter::create(&path, 7, &[subscription("tok", "desktop")]).expect("create");

    writer.set("tok", StreamState::Running).expect("persist");
    let read = StateFile::read(&path).expect("read").expect("present");
    assert_eq!(read.subscriptions["tok"].state, StreamState::Running);

    writer
        .set(
            "tok",
            StreamState::Disconnected {
                detail: "connection closed by the server".to_owned(),
            },
        )
        .expect("persist");
    let read = StateFile::read(&path).expect("read").expect("present");
    assert_eq!(
        read.subscriptions["tok"].state,
        StreamState::Disconnected {
            detail: "connection closed by the server".to_owned()
        }
    );

    // The processed notification's id rides along for status.
    writer.set_last_message_id("tok", 9).expect("persist");
    let read = StateFile::read(&path).expect("read").expect("present");
    assert_eq!(read.subscriptions["tok"].last_message_id, Some(9));
}

#[test]
fn a_missing_state_file_reads_as_none() {
    let dir = tempfile::tempdir().expect("temp dir");
    let absent = dir.path().join("absent.json");
    assert_eq!(
        StateFile::read(&absent).expect("missing is not an error"),
        None
    );
}

#[test]
fn a_torn_state_file_is_an_error_not_silence() {
    let dir = tempfile::tempdir().expect("temp dir");
    let torn = dir.path().join("torn.json");
    std::fs::write(&torn, "{ not json").expect("write torn state");
    assert!(
        StateFile::read(&torn).is_err(),
        "a corrupted state file must not silently read as anything"
    );
}

#[test]
fn state_names_render_in_the_vocabulary_status_uses() {
    assert_eq!(StreamState::Starting.as_str(), "starting");
    assert_eq!(StreamState::Running.as_str(), "running");
    assert_eq!(
        StreamState::Disconnected {
            detail: String::new()
        }
        .as_str(),
        "disconnected"
    );
    assert_eq!(StreamState::TokenInvalid.as_str(), "token invalid");
    assert_eq!(
        StreamState::Failed {
            detail: String::new()
        }
        .as_str(),
        "failed"
    );
}
