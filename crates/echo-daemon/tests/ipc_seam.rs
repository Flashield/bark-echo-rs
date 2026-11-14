//! The IPC seam (ticket #10): the real daemon in-process — the way the
//! existing runtime tests drive it, embedded bark-server and all —
//! driven by a real echo-ipc client over the real transport (UDS or
//! named pipe). The channel answers with the daemon's live
//! per-Subscription state, serves a second client concurrently, and
//! survives hostile bytes on one connection without disturbing anyone
//! else.

mod common;

use std::sync::Arc;
use std::time::{Duration, Instant};

use echo_ipc::message::{StatusSnapshot, StreamState};
use echo_ipc::{Client, RequestFrame, connect_raw};

/// A renderer fake that swallows every intent: the seam under test here
/// is the channel, not rendering (mirrors tests/run.rs's fake).
struct SilentRenderer;

impl echo_daemon::notify::NotificationRenderer for SilentRenderer {
    fn render(
        &self,
        _intent: &echo_core::intent::NotificationIntent,
    ) -> Result<(), echo_daemon::notify::RenderError> {
        Ok(())
    }
}

/// One registered Stream Device with its minted Stream Token secret and
/// id — the pieces a Subscription is built from (mirrors tests/run.rs).
async fn register_and_mint(server: &common::TestServer, label: &str) -> (String, String, String) {
    let client = echo_core::server::ServerClient::new(&server.base_url);
    let device = client.register_stream_device().await.expect("register");
    let minted = client
        .mint_stream_token(&device.device_key, label)
        .await
        .expect("mint");
    (device.device_key, minted.secret, minted.token_id)
}

/// Push one V2 JSON Push carrying `body` and assert the server took it.
async fn push_to(server: &common::TestServer, device_key: &str, body: &str) {
    let response = reqwest::Client::new()
        .post(format!("{}/push", server.base_url))
        .json(&serde_json::json!({ "device_key": device_key, "body": body }))
        .send()
        .await
        .expect("POST /push");
    let status = response.status();
    let envelope: serde_json::Value = response.json().await.expect("push envelope");
    assert!(
        status.is_success() && envelope["code"] == 200,
        "push failed: {status} {envelope}"
    );
}

/// A daemon environment in a temp dir: config with `subscription` in it,
/// plain-file secrets holding its Device Key and Stream Token (mirrors
/// tests/run.rs).
struct Env {
    paths: echo_daemon::paths::Paths,
    config: echo_daemon::config::Config,
    store: Arc<dyn echo_daemon::secrets::SecretStore>,
    _dir: tempfile::TempDir,
}

impl Env {
    fn with_subscription(
        server_url: &str,
        device_key: &str,
        stream_token: &str,
        token_id: &str,
        label: &str,
    ) -> Self {
        use echo_daemon::config::{Config, Subscription};
        use echo_daemon::secrets::{PlainFileStore, SecretKey, SecretKind, SecretStore};

        let dir = tempfile::tempdir().expect("temp dir");
        let paths = echo_daemon::paths::Paths::from_dir(dir.path());
        let store: Arc<dyn SecretStore> = Arc::new(PlainFileStore::new(paths.secrets_file()));
        let key = SecretKey {
            server: server_url.to_owned(),
            token_id: token_id.to_owned(),
        };
        store
            .set(&key, SecretKind::DeviceKey, device_key)
            .expect("store device key");
        store
            .set(&key, SecretKind::StreamToken, stream_token)
            .expect("store stream token");
        let config = Config {
            subscriptions: vec![Subscription {
                id: token_id.to_owned(),
                server: server_url.to_owned(),
                label: label.to_owned(),
                token_hint: format!("{}...", &stream_token[..12]),
                device_hint: format!("{}...", &device_key[..8]),
            }],
            servers: Vec::new(),
        };
        Self {
            paths,
            config,
            store,
            _dir: dir,
        }
    }
}

/// Run the real daemon in-process with a fence for shutdown.
async fn running_daemon(
    env: &Env,
) -> (
    tokio::sync::oneshot::Sender<()>,
    tokio::task::JoinHandle<Result<(), echo_daemon::run::RunError>>,
) {
    let (fence_tx, fence_rx) = tokio::sync::oneshot::channel::<()>();
    let spec = echo_daemon::run::DaemonSpec {
        paths: env.paths.clone(),
        config: env.config.clone(),
        store: env.store.clone(),
        renderer: Arc::new(SilentRenderer),
    };
    let runner = tokio::spawn(async move {
        echo_daemon::run::run(spec, async move {
            let _ = fence_rx.await;
        })
        .await
    });
    (fence_tx, runner)
}

/// Poll the channel until `check` accepts the snapshot.
async fn wait_snapshot(
    client: &mut Client,
    what: &str,
    check: impl Fn(&StatusSnapshot) -> bool,
) -> StatusSnapshot {
    let deadline = Instant::now() + Duration::from_secs(15);
    loop {
        match client.status().await {
            Ok(snapshot) if check(&snapshot) => return snapshot,
            Ok(snapshot) => {
                assert!(
                    Instant::now() < deadline,
                    "the snapshot never showed {what}: {snapshot:?}"
                );
                tokio::time::sleep(Duration::from_millis(20)).await;
            }
            Err(err) => {
                assert!(
                    Instant::now() < deadline,
                    "the channel broke while waiting for {what}: {err}"
                );
                tokio::time::sleep(Duration::from_millis(20)).await;
            }
        }
    }
}

#[tokio::test]
async fn the_channel_answers_with_the_live_per_subscription_state() {
    let server = common::TestServer::spawn().await;
    let (device_key, secret, token_id) = register_and_mint(&server, "ipc-test").await;
    let env = Env::with_subscription(&server.base_url, &device_key, &secret, &token_id, "desktop");
    let (fence_tx, runner) = running_daemon(&env).await;

    // The daemon's stream opens; the channel answers from the same
    // in-memory state the receive loop updates — live, not the file's
    // last write.
    common::wait_active_streams(&server, 1).await;
    let endpoint = env.paths.ipc_endpoint();
    let mut client = Client::connect(&endpoint).await.expect("connect");
    let snapshot = wait_snapshot(&mut client, "running", |snapshot| {
        snapshot
            .subscriptions
            .get(&token_id)
            .map(|runtime| runtime.state == StreamState::Running)
            .unwrap_or(false)
    })
    .await;
    assert_eq!(
        snapshot.subscriptions[&token_id].last_message_id, None,
        "nothing has been processed yet"
    );

    // A push lands; the channel's cursor follows the daemon's live
    // state — and agrees with the state file's cursor.
    push_to(&server, &device_key, "through the channel").await;
    let snapshot = wait_snapshot(&mut client, "the processed id", |snapshot| {
        snapshot.subscriptions[&token_id].last_message_id.is_some()
    })
    .await;
    let processed_id = snapshot.subscriptions[&token_id].last_message_id;
    let state_file = echo_daemon::state::StateFile::read(&env.paths.state_file())
        .expect("read state file")
        .expect("state file present");
    assert_eq!(
        state_file.subscriptions[&token_id].last_message_id, processed_id,
        "the channel's answer and the state file describe the same runtime"
    );

    // Shutdown is graceful: the run returns Ok and the stream closes.
    fence_tx.send(()).expect("fence open");
    let outcome = tokio::time::timeout(Duration::from_secs(10), runner)
        .await
        .expect("run ends within 10s")
        .expect("run task joined");
    assert!(outcome.is_ok(), "run must not fail: {outcome:?}");
    common::wait_active_streams(&server, 0).await;

    server.shutdown().await;
}

#[tokio::test]
async fn a_second_client_gets_correct_snapshots_concurrently() {
    let server = common::TestServer::spawn().await;
    let (device_key, secret, token_id) = register_and_mint(&server, "two-clients").await;
    let env = Env::with_subscription(&server.base_url, &device_key, &secret, &token_id, "desktop");
    let (fence_tx, runner) = running_daemon(&env).await;

    common::wait_active_streams(&server, 1).await;
    let endpoint = env.paths.ipc_endpoint();
    // Two clients hold their own connections at the same time.
    let mut client_a = Client::connect(&endpoint).await.expect("connect a");
    let mut client_b = Client::connect(&endpoint).await.expect("connect b");

    // A push lands while both connections are open: each client — in
    // either order — sees the same fresh state.
    push_to(&server, &device_key, "both of us see this").await;
    let seen_a = wait_snapshot(&mut client_a, "the cursor", |snapshot| {
        snapshot.subscriptions[&token_id].last_message_id.is_some()
    })
    .await;
    let seen_b = wait_snapshot(&mut client_b, "the cursor", |snapshot| {
        snapshot.subscriptions[&token_id].last_message_id.is_some()
    })
    .await;
    assert_eq!(seen_a, seen_b, "both connections read the same runtime");
    assert!(seen_a.subscriptions[&token_id].state == StreamState::Running);

    fence_tx.send(()).expect("fence open");
    let outcome = tokio::time::timeout(Duration::from_secs(10), runner)
        .await
        .expect("run ends within 10s")
        .expect("run task joined");
    assert!(outcome.is_ok(), "run must not fail: {outcome:?}");

    server.shutdown().await;
}

#[tokio::test]
async fn malformed_bytes_never_hurt_the_daemon() {
    let server = common::TestServer::spawn().await;
    let (device_key, secret, token_id) = register_and_mint(&server, "hostile-bytes").await;
    let env = Env::with_subscription(&server.base_url, &device_key, &secret, &token_id, "desktop");
    let (fence_tx, runner) = running_daemon(&env).await;

    common::wait_active_streams(&server, 1).await;
    let endpoint = env.paths.ipc_endpoint();

    // Hostile bytes on one raw connection: the connection drops — and
    // the daemon, its stream, and the channel stay healthy.
    let mut hostile = connect_raw(&endpoint).await.expect("raw connect");
    hostile
        .write_raw(b"\x00\x01 not even close to json\n")
        .await
        .expect("garbage");
    let ended: Option<RequestFrame> = hostile.receive().await.expect("eof read");
    assert_eq!(ended, None, "the hostile connection is dropped");

    // A proper client still reads the live state…
    let mut client = Client::connect(&endpoint).await.expect("reconnect");
    let snapshot = wait_snapshot(&mut client, "running", |snapshot| {
        snapshot
            .subscriptions
            .get(&token_id)
            .map(|runtime| runtime.state == StreamState::Running)
            .unwrap_or(false)
    })
    .await;
    assert_eq!(snapshot.pid, std::process::id());

    // …and the daemon's core job carries on: pushes still mirror.
    push_to(&server, &device_key, "still mirroring").await;
    let snapshot = wait_snapshot(&mut client, "the processed id", |snapshot| {
        snapshot.subscriptions[&token_id].last_message_id.is_some()
    })
    .await;
    assert!(snapshot.subscriptions[&token_id].last_message_id.is_some());

    fence_tx.send(()).expect("fence open");
    let outcome = tokio::time::timeout(Duration::from_secs(10), runner)
        .await
        .expect("run ends within 10s")
        .expect("run task joined");
    assert!(outcome.is_ok(), "run must not fail: {outcome:?}");

    server.shutdown().await;
}
