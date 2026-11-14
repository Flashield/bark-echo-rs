//! The wire vocabulary (spec story 24): one crate owns the message types
//! so the daemon, the GUI, and the CLI cannot fork the JSON Lines format
//! (ADR-0004). These tests pin the shapes on the wire — including the
//! runtime state file's vocabulary, which the snapshot shares.

use std::collections::BTreeMap;

use echo_ipc::PROTOCOL_VERSION;
use echo_ipc::message::{
    Request, RequestFrame, Response, ResponseFrame, StatusSnapshot, StreamState,
    SubscriptionRuntime,
};

#[test]
fn the_protocol_starts_at_version_one() {
    assert_eq!(PROTOCOL_VERSION, 1);
}

#[test]
fn hello_frame_round_trips_with_its_id_and_type_tag() {
    let frame = RequestFrame {
        id: 7,
        request: Request::Hello {
            version: PROTOCOL_VERSION,
        },
    };
    let json = serde_json::to_string(&frame).expect("serialize");
    assert!(
        json.contains(r#""type":"hello""#),
        "the type tag names the request: {json}"
    );
    assert!(json.contains(r#""id":7"#), "the id rides the frame: {json}");
    let back: RequestFrame = serde_json::from_str(&json).expect("deserialize");
    assert_eq!(back, frame);
}

#[test]
fn status_request_round_trips() {
    let frame = RequestFrame {
        id: 1,
        request: Request::Status,
    };
    let json = serde_json::to_string(&frame).expect("serialize");
    assert!(json.contains(r#""type":"status""#), "{json}");
    let back: RequestFrame = serde_json::from_str(&json).expect("deserialize");
    assert_eq!(back, frame);
}

#[test]
fn snapshot_response_round_trips_with_its_request_id() {
    let frame = ResponseFrame {
        id: 3,
        response: Response::Status {
            snapshot: StatusSnapshot {
                pid: 4242,
                started_at: 1_700_000_000,
                subscriptions: BTreeMap::from([(
                    "tok-a".to_owned(),
                    SubscriptionRuntime {
                        state: StreamState::Running,
                        last_message_id: Some(41),
                        updated_at: 1_700_000_042,
                    },
                )]),
            },
        },
    };
    let json = serde_json::to_string(&frame).expect("serialize");
    assert!(json.contains(r#""type":"status""#), "{json}");
    assert!(json.contains(r#""id":3"#), "{json}");
    let back: ResponseFrame = serde_json::from_str(&json).expect("deserialize");
    assert_eq!(back, frame);
}

#[test]
fn an_error_response_round_trips_with_its_request_id() {
    let frame = ResponseFrame {
        id: 9,
        response: Response::Error {
            message: "protocol 2 speaks a dialect this daemon does not know".to_owned(),
        },
    };
    let json = serde_json::to_string(&frame).expect("serialize");
    assert!(json.contains(r#""type":"error""#), "{json}");
    let back: ResponseFrame = serde_json::from_str(&json).expect("deserialize");
    assert_eq!(back, frame);
}

#[test]
fn stream_state_serializes_in_the_runtime_state_file_vocabulary() {
    // daemon-state.json (ticket #5) already writes this shape; the IPC
    // snapshot is the same words (spec story 24), so a snapshot and a
    // state file read as one vocabulary. The `kind`/`detail` tag+content
    // wrapper is serde's — and therefore the exact bytes the daemon has
    // persisted since before the channel existed.
    assert_eq!(
        serde_json::to_string(&StreamState::Starting).expect("serialize"),
        r#"{"kind":"starting"}"#
    );
    assert_eq!(
        serde_json::to_string(&StreamState::Running).expect("serialize"),
        r#"{"kind":"running"}"#
    );
    assert_eq!(
        serde_json::to_string(&StreamState::Disconnected {
            detail: "connection closed by the server".to_owned(),
        })
        .expect("serialize"),
        r#"{"kind":"disconnected","detail":{"detail":"connection closed by the server"}}"#
    );
    assert_eq!(
        serde_json::to_string(&StreamState::TokenInvalid).expect("serialize"),
        r#"{"kind":"token_invalid"}"#
    );
    assert_eq!(
        serde_json::to_string(&StreamState::Failed {
            detail: "history log write failed".to_owned(),
        })
        .expect("serialize"),
        r#"{"kind":"failed","detail":{"detail":"history log write failed"}}"#
    );
}

#[test]
fn the_snapshot_holds_one_line_per_subscription() {
    let snapshot = StatusSnapshot {
        pid: 1,
        started_at: 0,
        subscriptions: BTreeMap::from([
            (
                "tok-a".to_owned(),
                SubscriptionRuntime {
                    state: StreamState::Starting,
                    last_message_id: None,
                    updated_at: 0,
                },
            ),
            (
                "tok-b".to_owned(),
                SubscriptionRuntime {
                    state: StreamState::Running,
                    last_message_id: Some(7),
                    updated_at: 0,
                },
            ),
        ]),
    };
    assert_eq!(snapshot.subscriptions.len(), 2);
    assert_eq!(snapshot.subscriptions["tok-b"].state, StreamState::Running);
    assert_eq!(snapshot.subscriptions["tok-a"].last_message_id, None);
}
