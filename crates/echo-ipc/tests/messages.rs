//! The wire vocabulary (spec story 24): one crate owns the message types
//! so the daemon, the GUI, and the CLI cannot fork the JSON Lines format
//! (ADR-0004). These tests pin the shapes on the wire — including the
//! runtime state file's vocabulary, which the snapshot shares.

use std::collections::BTreeMap;

use echo_ipc::PROTOCOL_VERSION;
use echo_ipc::message::{
    Event, NotificationIntent, Request, RequestFrame, Response, ResponseFrame, StatusSnapshot,
    StreamState, SubscriptionRuntime,
};

fn plain_intent(id: i64) -> NotificationIntent {
    NotificationIntent {
        id,
        title: "the title".to_owned(),
        subtitle: String::new(),
        body: "the body".to_owned(),
        click_action: None,
        created_at: 1_700_000_000,
    }
}

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
fn shutdown_request_round_trips_with_its_type_tag() {
    // Ticket #12: the Tray Menu's Quit rides the channel like any other
    // request — an id-carrying frame, answered before the stop begins.
    let frame = RequestFrame {
        id: 9,
        request: Request::Shutdown,
    };
    let json = serde_json::to_string(&frame).expect("serialize");
    assert!(
        json.contains(r#""type":"shutdown""#),
        "the type tag names the request: {json}"
    );
    assert!(json.contains(r#""id":9"#), "the id rides the frame: {json}");
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
                        label: "desktop".to_owned(),
                        state: StreamState::Running,
                        last_message_id: Some(41),
                        updated_at: 1_700_000_042,
                        server: "http://127.0.0.1:8080".to_owned(),
                        token_hint: "abcdef123456...".to_owned(),
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
fn the_shutting_down_response_round_trips_with_its_type_tag() {
    // The daemon's acceptance of a Shutdown: the answer leaves first,
    // then the graceful stop begins (ADR-0004's Quit contract, ticket
    // #12) — the channel closing behind it is the stop's announcement.
    let frame = ResponseFrame {
        id: 9,
        response: Response::ShuttingDown,
    };
    let json = serde_json::to_string(&frame).expect("serialize");
    assert!(
        json.contains(r#""type":"shutting_down""#),
        "the type tag names the answer: {json}"
    );
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
                    label: "kitchen".to_owned(),
                    state: StreamState::Starting,
                    last_message_id: None,
                    updated_at: 0,
                    server: "http://127.0.0.1:8080".to_owned(),
                    token_hint: "123456abcdef...".to_owned(),
                },
            ),
            (
                "tok-b".to_owned(),
                SubscriptionRuntime {
                    label: "desktop".to_owned(),
                    state: StreamState::Running,
                    last_message_id: Some(7),
                    updated_at: 0,
                    server: "https://api.day.app".to_owned(),
                    token_hint: "fed654321098...".to_owned(),
                },
            ),
        ]),
    };
    assert_eq!(snapshot.subscriptions.len(), 2);
    assert_eq!(snapshot.subscriptions["tok-b"].state, StreamState::Running);
    assert_eq!(snapshot.subscriptions["tok-a"].last_message_id, None);
    // The label rides the snapshot (ticket #11): a fresh client can name
    // every line without touching the config.
    assert_eq!(snapshot.subscriptions["tok-b"].label, "desktop");
    assert_eq!(snapshot.subscriptions["tok-a"].label, "kitchen");
    // And so does the public identity (ticket #13): server URL and token
    // hint for the Management Window's list.
    assert_eq!(
        snapshot.subscriptions["tok-b"].server,
        "https://api.day.app"
    );
    assert_eq!(
        snapshot.subscriptions["tok-b"].token_hint,
        "fed654321098..."
    );
}

#[test]
fn the_snapshot_line_carries_the_subscriptions_public_identity() {
    // The Management Window (ticket #13) lists label / server URL / token
    // hint per Subscription. These are the config file's public fields
    // (echo-daemon config.rs — the TOML holds non-secret data only), so
    // they ride the snapshot and the GUI never touches a bark-echo file
    // (ADR-0004); the secrets behind the hints stay in the daemon.
    let runtime = SubscriptionRuntime {
        label: "desktop".to_owned(),
        state: StreamState::Running,
        last_message_id: Some(41),
        updated_at: 1_700_000_042,
        server: "http://127.0.0.1:8080".to_owned(),
        token_hint: "abcdef123456...".to_owned(),
    };
    let json = serde_json::to_string(&runtime).expect("serialize");
    assert!(
        json.contains(r#""server":"http://127.0.0.1:8080""#),
        "the server URL rides the line: {json}"
    );
    assert!(
        json.contains(r#""token_hint":"abcdef123456...""#),
        "the token hint rides the line: {json}"
    );
    let back: SubscriptionRuntime = serde_json::from_str(&json).expect("deserialize");
    assert_eq!(back, runtime);
}

#[test]
fn a_state_file_written_before_the_public_identity_still_parses() {
    // The runtime state file is the snapshot's persisted face; files
    // from before ticket #13 carry no server or token hint. Graceful
    // default, not an error — the next daemon run rewrites them. The
    // same rule `label` set in ticket #11.
    let runtime: SubscriptionRuntime =
        serde_json::from_str(r#"{"state":{"kind":"running"},"last_message_id":7,"updated_at":0}"#)
            .expect("parse a pre-identity line");
    assert_eq!(runtime.label, "");
    assert_eq!(
        runtime.server, "",
        "absent identity defaults empty, never invented"
    );
    assert_eq!(runtime.token_hint, "");
    assert_eq!(runtime.state, StreamState::Running);
}

#[test]
fn a_state_file_written_before_labels_still_parses() {
    // The runtime state file is the snapshot's persisted face; files
    // from before ticket #11 lack the label. Graceful default, not an
    // error — the next daemon run rewrites them.
    let runtime: SubscriptionRuntime =
        serde_json::from_str(r#"{"state":{"kind":"running"},"last_message_id":7,"updated_at":0}"#)
            .expect("parse a pre-label line");
    assert_eq!(runtime.label, "");
    assert_eq!(runtime.state, StreamState::Running);
}

#[test]
fn a_stream_state_event_round_trips_without_an_id() {
    // ADR-0004: events carry no id — the client cannot answer them, it
    // just learns. The frame is the bare event, no envelope.
    let event = Event::StreamState {
        subscription_id: "tok".to_owned(),
        label: "desktop".to_owned(),
        state: StreamState::Running,
    };
    let json = serde_json::to_string(&event).expect("serialize");
    assert_eq!(
        json,
        r#"{"type":"stream_state","subscription_id":"tok","label":"desktop","state":{"kind":"running"}}"#,
        "the event names itself, its Subscription, and the state in the runtime's vocabulary"
    );
    let back: Event = serde_json::from_str(&json).expect("deserialize");
    assert_eq!(back, event);
}

#[test]
fn a_history_snapshot_request_round_trips_with_its_subscription_and_tail() {
    // Ticket #15: the viewer's opening content is a bounded tail of one
    // Subscription's History Log, asked over the channel — the GUI never
    // reads the file (ADR-0004). The type tag names the request; the
    // Subscription and the bound ride as fields.
    let frame = RequestFrame {
        id: 5,
        request: Request::HistorySnapshot {
            subscription_id: "tok".to_owned(),
            tail: 200,
        },
    };
    let json = serde_json::to_string(&frame).expect("serialize");
    assert!(
        json.contains(r#""type":"history_snapshot""#),
        "the type tag names the request: {json}"
    );
    assert!(
        json.contains(r#""subscription_id":"tok""#) && json.contains(r#""tail":200"#),
        "the ask carries its Subscription and its bound: {json}"
    );
    let back: RequestFrame = serde_json::from_str(&json).expect("deserialize");
    assert_eq!(back, frame);
}

#[test]
fn a_history_snapshot_response_round_trips_the_log_line_vocabulary() {
    // The answer is the log file's own lines — the HistoryAppended
    // event's vocabulary, oldest last — so the viewer renders snapshot
    // lines and live appends as one thing, all of it channel-borne.
    let frame = ResponseFrame {
        id: 12,
        response: Response::HistorySnapshot {
            entries: vec![plain_intent(40), plain_intent(41)],
        },
    };
    let json = serde_json::to_string(&frame).expect("serialize");
    assert!(
        json.contains(r#""type":"history_snapshot""#) && json.contains(r#""id":12"#),
        "the response names itself and echoes the request's id: {json}"
    );
    assert!(
        json.contains(r#""body":"the body""#) && json.contains(r#""title":"the title""#),
        "the entries ride as the log's own lines: {json}"
    );
    let back: ResponseFrame = serde_json::from_str(&json).expect("deserialize");
    assert_eq!(back, frame);
}

#[test]
fn a_history_appended_event_round_trips_the_log_line_vocabulary() {
    // The History Log's line (GLOSSARY) and the event's entry are one
    // vocabulary: the entry's bytes are what the log file holds, so the
    // GUI renders the event exactly as `log` prints the file.
    let event = Event::HistoryAppended {
        subscription_id: "tok".to_owned(),
        label: "desktop".to_owned(),
        entry: plain_intent(41),
    };
    let json = serde_json::to_string(&event).expect("serialize");
    assert!(
        json.starts_with(r#"{"type":"history_appended","subscription_id":"tok","label":"desktop","entry":{"id":41,"#),
        "the entry rides inside the event: {json}"
    );
    assert!(
        json.contains(r#""title":"the title""#) && json.contains(r#""body":"the body""#),
        "the log line's fields survive: {json}"
    );
    let back: Event = serde_json::from_str(&json).expect("deserialize");
    assert_eq!(back, event);
}
