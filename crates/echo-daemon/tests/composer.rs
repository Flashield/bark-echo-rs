//! The daemon's half of the Push Composer (ticket #16): a SendPush
//! request is answered by the echo-core send path (protocol §2.2) with
//! the daemon's own stored secrets — the request carries no credential,
//! and the answer is the bark server's verdict, verbatim. The embedded
//! server is the real counterpart, so a `Sent` verdict here means the
//! server truly took the Push.

mod common;

use std::sync::Arc;

use echo_daemon::composer::handle_send;
use echo_daemon::config::{Config, ServerEntry, Subscription};
use echo_daemon::secrets::{PlainFileStore, SecretKey, SecretKind, SecretStore};
use echo_ipc::message::{SendPush, SendPushOutcome};

/// A minimal send: ride Subscription `tok`, target `device_key`, no
/// optional fields.
fn a_send(subscription_id: &str, device_key: &str) -> SendPush {
    SendPush {
        subscription_id: subscription_id.to_owned(),
        device_key: device_key.to_owned(),
        title: Some("from the Composer".to_owned()),
        subtitle: None,
        body: Some("through the daemon".to_owned()),
        ext_params: Default::default(),
    }
}

/// One configured Subscription named `tok` on `server`, its Device Key
/// and Stream Token in a plain-file store, plus optional per-server
/// Basic Auth (user in the config, password in the store — story 18).
fn an_env(
    server: &str,
    device_key: &str,
    auth: Option<(&str, &str)>,
) -> (tempfile::TempDir, Config, Arc<dyn SecretStore>) {
    let dir = tempfile::tempdir().expect("temp dir");
    let paths = echo_daemon::paths::Paths::from_dir(dir.path());
    let store: Arc<dyn SecretStore> = Arc::new(PlainFileStore::new(paths.secrets_file()));
    let key = SecretKey::for_subscription(server, "tok");
    store
        .set(&key, SecretKind::DeviceKey, device_key)
        .expect("store device key");
    store
        .set(&key, SecretKind::StreamToken, "stream-token-secret")
        .expect("store stream token");
    let servers = match auth {
        Some((user, password)) => {
            store
                .set(
                    &SecretKey::for_server(server),
                    SecretKind::BasicAuthPassword,
                    password,
                )
                .expect("store basic auth password");
            vec![ServerEntry {
                url: server.to_owned(),
                basic_auth_user: user.to_owned(),
                basic_auth_password_stored: true,
            }]
        }
        None => Vec::new(),
    };
    let config = Config {
        subscriptions: vec![Subscription {
            id: "tok".to_owned(),
            server: server.to_owned(),
            label: "desktop".to_owned(),
            token_hint: "stream-token-...".to_owned(),
            device_hint: format!("{}...", &device_key[..8.min(device_key.len())]),
        }],
        servers,
        management: Default::default(),
    };
    (dir, config, store)
}

#[tokio::test]
async fn the_composer_send_goes_out_with_the_daemons_own_credentials() {
    // A Basic-Auth-protected server (protocol §1.4): the request itself
    // carries no credential — the daemon resolves the stored ones. A
    // `Sent` verdict therefore proves the daemon-side credential path.
    let server = common::TestServer::spawn_with_basic_auth("operator", "hunter2").await;
    // /register is Basic-Auth-exempt (protocol §1.4): no credentials
    // needed to mint the target Device.
    let device = echo_core::server::ServerClient::new(&server.base_url)
        .register_stream_device()
        .await
        .expect("register");
    let (_dir, config, store) = an_env(
        &server.base_url,
        &device.device_key,
        Some(("operator", "hunter2")),
    );
    let send = SendPush {
        ext_params: [
            ("url".to_owned(), "https://example.com".to_owned()),
            ("sound".to_owned(), "bell".to_owned()),
        ]
        .into_iter()
        .collect(),
        ..a_send("tok", &device.device_key)
    };
    let outcome = handle_send(&config, store.as_ref(), send)
        .await
        .expect("the send was attempted");
    assert_eq!(outcome, SendPushOutcome::Sent);
    server.shutdown().await;
}

#[tokio::test]
async fn a_bad_device_key_surfaces_the_servers_verdict_verbatim() {
    // The server's own rejection — a Device Key that addresses no
    // Device answers `400 failed to get device token...` — rides to the
    // window verbatim: code and message (ticket #16's readable rule).
    let server = common::TestServer::spawn().await;
    let device = echo_core::server::ServerClient::new(&server.base_url)
        .register_stream_device()
        .await
        .expect("register");
    let (_dir, config, store) = an_env(&server.base_url, &device.device_key, None);
    let outcome = handle_send(&config, store.as_ref(), a_send("tok", "no-such-device-key"))
        .await
        .expect("the send was attempted, so the verdict answers");
    match outcome {
        SendPushOutcome::Rejected { code, message } => {
            assert_eq!(code, Some(400), "the envelope code rides: {message}");
            assert!(
                message.contains("device token"),
                "the server's own message rides: {message}"
            );
        }
        other => panic!("a bad Device Key is a rejection, got {other:?}"),
    }
    server.shutdown().await;
}

#[tokio::test]
async fn an_unknown_subscription_is_refused_without_a_server_call() {
    // No Subscription named `absent` exists: the daemon refuses the
    // request before any HTTP — the message is the actionable answer.
    // The unroutable server URL proves no send was attempted: reaching
    // it would surface as a transport error, not this message.
    let (_dir, config, store) = an_env("http://127.0.0.1:9", "device-key-1", None);
    let outcome = handle_send(&config, store.as_ref(), a_send("absent", "device-key-1")).await;
    match outcome {
        Err(message) => assert!(
            message.contains("absent") && message.contains("Subscription"),
            "the refusal names the missing Subscription: {message}"
        ),
        Ok(outcome) => panic!("an unknown Subscription is no verdict: {outcome:?}"),
    }
}

#[tokio::test]
async fn a_refused_password_is_the_basic_auth_rejection_verbatim() {
    // Wrong stored password: the server's 418 (protocol §1.4) — the
    // same readable rejection face as any other server refusal.
    let server = common::TestServer::spawn_with_basic_auth("operator", "hunter2").await;
    let device = echo_core::server::ServerClient::new(&server.base_url)
        .register_stream_device()
        .await
        .expect("register");
    let (_dir, config, store) = an_env(
        &server.base_url,
        &device.device_key,
        Some(("operator", "wrong-password")),
    );
    let outcome = handle_send(&config, store.as_ref(), a_send("tok", &device.device_key))
        .await
        .expect("the send was attempted, so the verdict answers");
    match outcome {
        SendPushOutcome::Rejected { code, message } => {
            assert_eq!(code, Some(418), "the Basic Auth refusal is 418: {message}");
        }
        other => panic!("a wrong password is a rejection, got {other:?}"),
    }
    server.shutdown().await;
}
