//! The IPC seam (tickets #10 and #14): the real daemon in-process — the
//! way the existing runtime tests drive it, embedded bark-server and
//! all — driven by a real echo-ipc client over the real transport (UDS
//! or named pipe). The channel answers with the daemon's live
//! per-Subscription state, serves a second client concurrently,
//! survives hostile bytes on one connection without disturbing anyone
//! else, and hot-applies Subscription CRUD: an add starts that stream's
//! supervisor immediately, a remove stops it — config, secrets, and
//! snapshot following, all observed from outside the daemon.

mod common;

use std::sync::Arc;
use std::time::{Duration, Instant};

use echo_daemon::secrets::{SecretKey, SecretKind};
use echo_ipc::message::{AddSource, BasicAuthCredentials, StatusSnapshot, StreamState};
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

/// A daemon environment in a temp dir: an empty Subscription list and a
/// plain-file secret store (mirrors tests/run.rs). Subscriptions are
/// added with [`Env::with_subscription`] — or hot, over the channel
/// (ticket #14).
struct Env {
    paths: echo_daemon::paths::Paths,
    config: echo_daemon::config::Config,
    store: Arc<dyn echo_daemon::secrets::SecretStore>,
    _dir: tempfile::TempDir,
}

impl Env {
    fn empty() -> Self {
        use echo_daemon::config::Config;
        use echo_daemon::secrets::{PlainFileStore, SecretStore};

        let dir = tempfile::tempdir().expect("temp dir");
        let paths = echo_daemon::paths::Paths::from_dir(dir.path());
        let store: Arc<dyn echo_daemon::secrets::SecretStore> =
            Arc::new(PlainFileStore::new(paths.secrets_file()));
        Self {
            paths,
            config: Config::default(),
            store,
            _dir: dir,
        }
    }

    fn with_subscription(
        server_url: &str,
        device_key: &str,
        stream_token: &str,
        token_id: &str,
        label: &str,
    ) -> Self {
        use echo_daemon::config::{Config, Subscription};
        use echo_daemon::secrets::{SecretKey, SecretKind, SecretStore};

        let mut env = Self::empty();
        let key = SecretKey {
            server: server_url.to_owned(),
            token_id: token_id.to_owned(),
        };
        env.store
            .set(&key, SecretKind::DeviceKey, device_key)
            .expect("store device key");
        env.store
            .set(&key, SecretKind::StreamToken, stream_token)
            .expect("store stream token");
        env.config = Config {
            subscriptions: vec![Subscription {
                id: token_id.to_owned(),
                server: server_url.to_owned(),
                label: label.to_owned(),
                token_hint: format!("{}...", &stream_token[..12]),
                device_hint: format!("{}...", &device_key[..8]),
            }],
            servers: Vec::new(),
        };
        env
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

    // The line carries the Subscription's public identity (ticket #13):
    // the Management Window lists label / server / token hint matching
    // the persisted config, and the GUI touches no bark-echo files
    // (ADR-0004) — so the channel must deliver them.
    let line = &snapshot.subscriptions[&token_id];
    let config = env
        .config
        .subscriptions
        .iter()
        .find(|subscription| subscription.id == token_id)
        .expect("the configured subscription");
    assert_eq!(line.label, config.label);
    assert_eq!(line.server, config.server);
    assert_eq!(line.token_hint, config.token_hint);

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

/// Connect with a deadline: the daemon binds the channel a beat after
/// the run task starts (lock first, then the endpoint), and a client —
/// the future Tray Menu — starts whenever it starts.
async fn connect_when_listening(endpoint: &echo_ipc::Endpoint) -> Client {
    let deadline = Instant::now() + Duration::from_secs(10);
    loop {
        match Client::connect(endpoint).await {
            Ok(client) => return client,
            Err(_) => {
                assert!(
                    Instant::now() < deadline,
                    "the channel never came up at {endpoint}"
                );
                tokio::time::sleep(Duration::from_millis(20)).await;
            }
        }
    }
}

#[tokio::test]
async fn a_push_lands_as_a_history_event_and_a_server_death_as_a_state_flip() {
    // Ticket #11: the daemon pushes unsolicited events — a History Log
    // append the moment the line is on disk, a stream state flip the
    // moment the runtime records it. A client that never sends a single
    // request still hears both, in the tray's vocabulary (label included).
    let server = common::TestServer::spawn().await;
    let (device_key, secret, token_id) = register_and_mint(&server, "events").await;
    let env = Env::with_subscription(&server.base_url, &device_key, &secret, &token_id, "desktop");
    let (fence_tx, runner) = running_daemon(&env).await;
    let endpoint = env.paths.ipc_endpoint();

    // The event listener never asks for anything: it just listens.
    let mut listener = connect_when_listening(&endpoint).await;
    // A second client drives the snapshot while the listener stays quiet.
    let mut poller = connect_when_listening(&endpoint).await;
    let snapshot = wait_snapshot(&mut poller, "running", |snapshot| {
        snapshot
            .subscriptions
            .get(&token_id)
            .map(|runtime| runtime.state == StreamState::Running)
            .unwrap_or(false)
    })
    .await;
    assert_eq!(
        snapshot.subscriptions[&token_id].label, "desktop",
        "the snapshot names the Subscription so a fresh client can render it"
    );

    // A push lands through the embedded server: the append arrives as an
    // event on the channel — not as a response to anything.
    push_to(&server, &device_key, "pushed as an event").await;
    // Draining past the connect-time stream flips until the append: the
    // listener subscribed before the push, so the append must come.
    let deadline = Instant::now() + Duration::from_secs(10);
    loop {
        let event = tokio::time::timeout(Duration::from_secs(10), listener.next_event())
            .await
            .expect("an event within 10s")
            .expect("the channel stayed open");
        match event {
            echo_ipc::message::Event::HistoryAppended {
                subscription_id,
                label,
                entry,
            } => {
                assert_eq!(subscription_id, token_id);
                assert_eq!(label, "desktop");
                assert_eq!(entry.body, "pushed as an event");
                let appended_id = entry.id;
                let after = wait_snapshot(&mut poller, "the processed id", |snapshot| {
                    snapshot.subscriptions[&token_id].last_message_id.is_some()
                })
                .await;
                assert_eq!(
                    after.subscriptions[&token_id].last_message_id,
                    Some(appended_id),
                    "the event's line is the push the snapshot's cursor follows"
                );
                break;
            }
            other => {
                assert!(
                    Instant::now() < deadline,
                    "the History event never arrived: {other:?}"
                );
            }
        }
    }

    // The embedded server dies: the stream flips to disconnected and the
    // flip is announced — the tray's "never stale silence" rule.
    server.shutdown().await;
    let deadline = Instant::now() + Duration::from_secs(10);
    loop {
        let event = tokio::time::timeout(Duration::from_secs(10), listener.next_event())
            .await
            .expect("an event within 10s")
            .expect("the channel stayed open");
        match event {
            echo_ipc::message::Event::StreamState {
                subscription_id,
                label,
                state,
            } => {
                assert_eq!(subscription_id, token_id);
                assert_eq!(label, "desktop");
                assert!(
                    matches!(state, StreamState::Disconnected { .. }),
                    "the server's death flips the state: {state:?}"
                );
                break;
            }
            other => {
                assert!(
                    Instant::now() < deadline,
                    "the state-flip event never arrived: {other:?}"
                );
            }
        }
    }
    fence_tx.send(()).expect("fence open");
    let outcome = tokio::time::timeout(Duration::from_secs(10), runner)
        .await
        .expect("run ends within 10s")
        .expect("run task joined");
    assert!(outcome.is_ok(), "run must not fail: {outcome:?}");
}

#[tokio::test]
async fn a_daemon_with_zero_subscriptions_stays_healthy_and_actionable() {
    // Ticket #14: hot CRUD means a daemon can reach zero Subscriptions,
    // and the GUI's spawn-if-absent starts one before any exists. Zero
    // streams is a state, not a failure: the channel answers with an
    // empty snapshot and the run stays up for more to come.
    let server = common::TestServer::spawn().await;
    let env = Env::empty();
    let (fence_tx, runner) = running_daemon(&env).await;
    let endpoint = env.paths.ipc_endpoint();

    let mut client = connect_when_listening(&endpoint).await;
    let snapshot = client.status().await.expect("the channel answers");
    assert_eq!(
        snapshot.subscriptions.len(),
        0,
        "no Subscription is configured yet"
    );
    assert_eq!(
        snapshot.pid,
        std::process::id(),
        "the snapshot is this daemon run's"
    );
    // Still alive and answering after the first beat — no crash on the
    // empty list.
    tokio::time::sleep(Duration::from_millis(200)).await;
    let again = client.status().await.expect("the channel still answers");
    assert_eq!(again.subscriptions.len(), 0);

    fence_tx.send(()).expect("fence open");
    let outcome = tokio::time::timeout(Duration::from_secs(10), runner)
        .await
        .expect("run ends within 10s")
        .expect("run task joined");
    assert!(outcome.is_ok(), "run must not fail: {outcome:?}");

    server.shutdown().await;
}

// --- Hot-apply CRUD (ticket #14): add and remove ride the channel and
// --- take effect immediately — asserted from outside the daemon, on
// --- the channel, the server, the config file, and the secret store.

/// Register one Stream Device and return its key — the mirror mode's
/// input (GLOSSARY: mirroring by Device Key).
async fn register_device(server: &common::TestServer) -> String {
    let client = echo_core::server::ServerClient::new(&server.base_url);
    client
        .register_stream_device()
        .await
        .expect("register stream device")
        .device_key
}

/// The config file as the daemon (the single writer) has left it.
fn config_on_disk(env: &Env) -> echo_daemon::config::Config {
    echo_daemon::config::Config::load(&env.paths.config_file()).expect("config file parses")
}

#[tokio::test]
async fn a_hot_own_add_starts_the_stream_and_its_pushes_reach_the_history_log() {
    let server = common::TestServer::spawn().await;
    let env = Env::empty();
    let (fence_tx, runner) = running_daemon(&env).await;
    let endpoint = env.paths.ipc_endpoint();

    // The listener never asks for anything — it just watches, like the
    // Tray Menu; the driver performs the add.
    let mut listener = connect_when_listening(&endpoint).await;
    let mut client = connect_when_listening(&endpoint).await;

    let added = client
        .subscribe_add(&server.base_url, AddSource::Own, "hot-add", None)
        .await
        .expect("the add is answered");
    assert_eq!(added.label, "hot-add");
    assert_eq!(added.server, server.base_url);
    assert_eq!(
        added.device_key.len(),
        22,
        "the push target rides the answer: {}",
        added.device_key
    );

    // The stream runs immediately — the supervisor was up before the
    // answer went out.
    common::wait_active_streams(&server, 1).await;

    // A push lands in the new Subscription's History Log and arrives as
    // an event — the tray's glance and the window's list both learn it
    // without a poll.
    push_to(&server, &added.device_key, "hot off the press").await;
    let deadline = Instant::now() + Duration::from_secs(10);
    let appended = loop {
        let event = tokio::time::timeout(Duration::from_secs(10), listener.next_event())
            .await
            .expect("an event within 10s")
            .expect("the channel stayed open");
        match event {
            echo_ipc::message::Event::StreamState { .. } => {
                assert!(
                    Instant::now() < deadline,
                    "only state flips before the append: no append event"
                );
            }
            echo_ipc::message::Event::HistoryAppended {
                subscription_id,
                label,
                entry,
            } => {
                assert_eq!(subscription_id, added.id);
                assert_eq!(label, "hot-add");
                assert_eq!(entry.body, "hot off the press");
                break entry;
            }
            other => panic!("unexpected event: {other:?}"),
        }
    };
    let log = echo_core::history::HistoryLog::new(env.paths.history_log(&added.id));
    let entries = log.entries().expect("read the new Subscription's log");
    assert_eq!(entries.len(), 1, "the push is durably in the log");
    assert_eq!(entries[0].id, appended.id);

    // The snapshot names the line with its full public identity and the
    // cursor follows the push.
    let snapshot = wait_snapshot(&mut client, "the hot-added line", |snapshot| {
        snapshot
            .subscriptions
            .get(&added.id)
            .map(|line| line.last_message_id == Some(appended.id))
            .unwrap_or(false)
    })
    .await;
    let line = &snapshot.subscriptions[&added.id];
    assert_eq!(line.label, "hot-add");
    assert_eq!(line.server, server.base_url);
    assert_eq!(line.state, StreamState::Running);
    assert_eq!(line.token_hint, added.token_hint);

    // The single writer persisted both faces: the config's non-secret
    // entry and the secrets under the fresh id.
    let config = config_on_disk(&env);
    assert_eq!(config.subscriptions.len(), 1, "the config carries the add");
    let persisted = &config.subscriptions[0];
    assert_eq!(persisted.id, added.id);
    assert_eq!(persisted.label, "hot-add");
    assert_eq!(persisted.token_hint, added.token_hint);
    let key = SecretKey {
        server: server.base_url.clone(),
        token_id: added.id.clone(),
    };
    env.store
        .get(&key, SecretKind::StreamToken)
        .expect("the Stream Token is stored");
    env.store
        .get(&key, SecretKind::DeviceKey)
        .expect("the Device Key is stored");

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
async fn a_hot_mirror_add_mints_a_labelled_token_for_the_existing_device() {
    let server = common::TestServer::spawn().await;
    let env = Env::empty();
    let (fence_tx, runner) = running_daemon(&env).await;
    let endpoint = env.paths.ipc_endpoint();
    let mut client = connect_when_listening(&endpoint).await;

    let device_key = register_device(&server).await;
    let added = client
        .subscribe_add(
            &server.base_url,
            AddSource::DeviceKey {
                device_key: device_key.clone(),
            },
            "the iPhone",
            None,
        )
        .await
        .expect("the mirror add is answered");

    // The stream runs against the mirrored Device, and the token the
    // daemon minted carries the form's label (GLOSSARY: one
    // Subscription mirrors exactly one Device).
    common::wait_active_streams(&server, 1).await;
    let tokens = echo_core::server::ServerClient::new(&server.base_url)
        .list_stream_tokens(&device_key)
        .await
        .expect("list the Device's tokens");
    assert_eq!(tokens.len(), 1, "exactly the token the add minted");
    assert_eq!(tokens[0].client_label, "the iPhone");
    assert_eq!(tokens[0].token_hint, added.token_hint);

    fence_tx.send(()).expect("fence open");
    let outcome = tokio::time::timeout(Duration::from_secs(10), runner)
        .await
        .expect("run ends within 10s")
        .expect("run task joined");
    assert!(outcome.is_ok(), "run must not fail: {outcome:?}");
    server.shutdown().await;
}

#[tokio::test]
async fn a_refused_add_is_an_error_and_leaves_nothing_behind() {
    let server = common::TestServer::spawn().await;
    let env = Env::empty();
    let (fence_tx, runner) = running_daemon(&env).await;
    let endpoint = env.paths.ipc_endpoint();
    let mut client = connect_when_listening(&endpoint).await;

    // A bad Device Key: the server's typed refusal surfaces readably —
    // the window and the CLI print it, the daemon stays untouched.
    let outcome = client
        .subscribe_add(
            &server.base_url,
            AddSource::DeviceKey {
                device_key: "notARealDeviceKey0000".to_owned(),
            },
            "doomed",
            None,
        )
        .await;
    match outcome {
        Err(echo_ipc::IpcError::Rejected(message)) => {
            assert!(
                message.contains("device not found"),
                "the server's refusal surfaces: {message}"
            );
        }
        other => panic!("a bad Device Key is refused, got {other:?}"),
    }

    // Nothing half-applied: no stream, no config entry, no line, and
    // the channel still answers.
    common::wait_active_streams(&server, 0).await;
    let snapshot = client.status().await.expect("the channel still answers");
    assert!(
        snapshot.subscriptions.is_empty(),
        "no line for the refused add"
    );
    assert!(
        config_on_disk(&env).subscriptions.is_empty(),
        "no config entry for the refused add"
    );
    // An empty label is refused before the server is even touched.
    let outcome = client
        .subscribe_add(&server.base_url, AddSource::Own, "   ", None)
        .await;
    assert!(
        matches!(outcome, Err(echo_ipc::IpcError::Rejected(_))),
        "an empty label is refused: {outcome:?}"
    );

    // And the daemon remains actionable: a good add still works after
    // the refusals.
    let added = client
        .subscribe_add(&server.base_url, AddSource::Own, "after-the-refusal", None)
        .await
        .expect("the daemon still applies adds");
    common::wait_active_streams(&server, 1).await;
    assert!(config_on_disk(&env).subscriptions.len() == 1);
    assert_eq!(added.label, "after-the-refusal");

    fence_tx.send(()).expect("fence open");
    let outcome = tokio::time::timeout(Duration::from_secs(10), runner)
        .await
        .expect("run ends within 10s")
        .expect("run task joined");
    assert!(outcome.is_ok(), "run must not fail: {outcome:?}");
    server.shutdown().await;
}

#[tokio::test]
async fn a_hot_remove_stops_the_stream_erases_secrets_and_updates_the_config() {
    let server = common::TestServer::spawn().await;
    let (device_key, secret, token_id) = register_and_mint(&server, "to-remove").await;
    let env = Env::with_subscription(&server.base_url, &device_key, &secret, &token_id, "doomed");
    let (fence_tx, runner) = running_daemon(&env).await;
    let endpoint = env.paths.ipc_endpoint();

    // A watcher hears the removal; the driver performs it.
    let mut listener = connect_when_listening(&endpoint).await;
    let mut client = connect_when_listening(&endpoint).await;
    common::wait_active_streams(&server, 1).await;

    let removed = client
        .subscribe_remove(&token_id)
        .await
        .expect("the remove is answered");
    assert_eq!(removed.id, token_id);
    assert_eq!(removed.label, "doomed");

    // The stream stops immediately.
    common::wait_active_streams(&server, 0).await;
    // The removal is announced — the tray and the window drop the line
    // at once, without a poll.
    let deadline = Instant::now() + Duration::from_secs(10);
    loop {
        let event = tokio::time::timeout(Duration::from_secs(10), listener.next_event())
            .await
            .expect("an event within 10s")
            .expect("the channel stayed open");
        match event {
            echo_ipc::message::Event::SubscriptionRemoved { subscription_id } => {
                assert_eq!(subscription_id, token_id);
                break;
            }
            echo_ipc::message::Event::StreamState { .. } => {
                assert!(
                    Instant::now() < deadline,
                    "the removal event never arrived behind the flips"
                );
            }
            other => panic!("unexpected event: {other:?}"),
        }
    }
    // The snapshot's line is gone and the channel still answers.
    let snapshot = wait_snapshot(&mut client, "the emptied snapshot", |snapshot| {
        snapshot.subscriptions.is_empty()
    })
    .await;
    assert!(snapshot.subscriptions.is_empty());
    // Both secrets are erased from the store.
    let key = SecretKey {
        server: server.base_url.clone(),
        token_id: token_id.clone(),
    };
    assert!(
        env.store.get(&key, SecretKind::StreamToken).is_err(),
        "the Stream Token is gone"
    );
    assert!(
        env.store.get(&key, SecretKind::DeviceKey).is_err(),
        "the Device Key is gone"
    );
    // The config file no longer carries the Subscription.
    assert!(
        config_on_disk(&env).subscriptions.is_empty(),
        "the config no longer carries the removed Subscription"
    );

    fence_tx.send(()).expect("fence open");
    let outcome = tokio::time::timeout(Duration::from_secs(10), runner)
        .await
        .expect("run ends within 10s")
        .expect("run task joined");
    assert!(outcome.is_ok(), "run must not fail: {outcome:?}");
    server.shutdown().await;
}

#[tokio::test]
async fn removing_every_subscription_leaves_the_daemon_healthy_and_actionable() {
    // The acceptance line: zero Subscriptions after removals is a state
    // the daemon holds — no crash, and adds still work.
    let server = common::TestServer::spawn().await;
    let (device_key, secret, token_id) = register_and_mint(&server, "starter").await;
    let env = Env::with_subscription(&server.base_url, &device_key, &secret, &token_id, "starter");
    let (fence_tx, runner) = running_daemon(&env).await;
    let endpoint = env.paths.ipc_endpoint();
    let mut client = connect_when_listening(&endpoint).await;
    common::wait_active_streams(&server, 1).await;

    // A second Subscription rides in hot, then both leave.
    let second = client
        .subscribe_add(&server.base_url, AddSource::Own, "second", None)
        .await
        .expect("the hot add");
    common::wait_active_streams(&server, 2).await;
    client.subscribe_remove(&second.id).await.expect("remove 2");
    client.subscribe_remove(&token_id).await.expect("remove 1");
    common::wait_active_streams(&server, 0).await;

    // Zero held — and the daemon answers, empty and actionable.
    let snapshot = wait_snapshot(&mut client, "the emptied snapshot", |snapshot| {
        snapshot.subscriptions.is_empty()
    })
    .await;
    assert_eq!(snapshot.pid, std::process::id());
    tokio::time::sleep(Duration::from_millis(200)).await;
    let again = client.status().await.expect("still answering at zero");
    assert!(again.subscriptions.is_empty(), "no crash, no residue");

    // Actionable: another add still starts its stream.
    let third = client
        .subscribe_add(&server.base_url, AddSource::Own, "third", None)
        .await
        .expect("the daemon still applies adds");
    common::wait_active_streams(&server, 1).await;
    assert_eq!(third.label, "third");

    fence_tx.send(()).expect("fence open");
    let outcome = tokio::time::timeout(Duration::from_secs(10), runner)
        .await
        .expect("run ends within 10s")
        .expect("run task joined");
    assert!(outcome.is_ok(), "run must not fail: {outcome:?}");
    server.shutdown().await;
}

#[tokio::test]
async fn basic_auth_credentials_land_in_the_store_never_in_the_config() {
    // Story 18's rule over the wire: the form's credentials ride the
    // channel into the daemon, which presents them for minting and
    // keeps the password in the secret store; the config file carries
    // the user and the marker only.
    let server = common::TestServer::spawn_with_basic_auth("operator", "op-secret").await;
    let env = Env::empty();
    let (fence_tx, runner) = running_daemon(&env).await;
    let endpoint = env.paths.ipc_endpoint();
    let mut client = connect_when_listening(&endpoint).await;

    let added = client
        .subscribe_add(
            &server.base_url,
            AddSource::Own,
            "guarded",
            Some(BasicAuthCredentials {
                user: "operator".to_owned(),
                password: "op-secret".to_owned(),
            }),
        )
        .await
        .expect("the add with credentials is answered");
    // The stream runs — the daemon presented the credentials itself
    // (spec story 13: the secrets stay inside the daemon). The /info
    // poll rides the same credentials: the protected server refuses
    // the anonymous observation (§1.4).
    common::wait_active_streams_with(&server, 1, Some(("operator", "op-secret"))).await;

    // The password is in the store under the server's key…
    let stored = env
        .store
        .get(
            &SecretKey::for_server(&server.base_url),
            SecretKind::BasicAuthPassword,
        )
        .expect("the password is stored");
    assert_eq!(stored, "op-secret");
    // …and in no file: the config carries the user and the marker only.
    let raw_config = std::fs::read_to_string(env.paths.config_file()).expect("config file exists");
    assert!(
        !raw_config.contains("op-secret"),
        "the password never enters the config file: {raw_config}"
    );
    let config = config_on_disk(&env);
    assert_eq!(
        config.basic_auth_user(&server.base_url).as_deref(),
        Some("operator")
    );
    assert!(
        config.basic_auth_configured(&server.base_url),
        "the stored-password marker is set"
    );

    fence_tx.send(()).expect("fence open");
    let outcome = tokio::time::timeout(Duration::from_secs(10), runner)
        .await
        .expect("run ends within 10s")
        .expect("run task joined");
    assert!(outcome.is_ok(), "run must not fail: {outcome:?}");
    server.shutdown().await;
}
