//! The daemon runtime end-to-end (ticket #5), against the embedded real
//! server: pushed messages render through the *injected* renderer and
//! land in the History Log, the runtime rides out a server restart, and
//! a revoked Stream Token flags its Subscription only — the others keep
//! working. The renderer is the fake; real rendering is platform glue.

mod common;

use std::sync::{Arc, Mutex};
use std::time::{Duration, Instant};

use echo_core::intent::NotificationIntent;
use echo_daemon::notify::{NotificationRenderer, RenderError};

/// A renderer fake: records every intent it was handed, in order.
#[derive(Clone, Default)]
struct RecordingRenderer {
    rendered: Arc<Mutex<Vec<NotificationIntent>>>,
}

impl RecordingRenderer {
    fn snapshot(&self) -> Vec<NotificationIntent> {
        self.rendered.lock().expect("renderer mutex").clone()
    }
}

impl NotificationRenderer for RecordingRenderer {
    fn render(&self, intent: &NotificationIntent) -> Result<(), RenderError> {
        self.rendered
            .lock()
            .expect("renderer mutex")
            .push(intent.clone());
        Ok(())
    }
}

/// Wait until the fake has rendered at least `n` intents, and return
/// them in order.
async fn wait_rendered(fake: &RecordingRenderer, n: usize) -> Vec<NotificationIntent> {
    let deadline = Instant::now() + Duration::from_secs(10);
    loop {
        let snapshot = fake.snapshot();
        if snapshot.len() >= n {
            return snapshot;
        }
        assert!(
            Instant::now() < deadline,
            "only {}/{} intents rendered in time",
            snapshot.len(),
            n
        );
        tokio::time::sleep(Duration::from_millis(20)).await;
    }
}

/// Wait until the state file shows `state` for `subscription_id` —
/// compared by the state's name, so a `Disconnected`'s free-text detail
/// does not have to match.
async fn wait_state(
    paths: &echo_daemon::paths::Paths,
    subscription_id: &str,
    state: &echo_daemon::state::StreamState,
) {
    use echo_daemon::state::StateFile;
    let deadline = Instant::now() + Duration::from_secs(15);
    loop {
        if let Ok(Some(state_file)) = StateFile::read(&paths.state_file())
            && let Some(runtime) = state_file.subscriptions.get(subscription_id)
            && runtime.state.as_str() == state.as_str()
        {
            return;
        }
        assert!(
            Instant::now() < deadline,
            "subscription {subscription_id} never reached {}: {:?}",
            state.as_str(),
            StateFile::read(&paths.state_file())
        );
        tokio::time::sleep(Duration::from_millis(20)).await;
    }
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

/// One registered Stream Device with its minted Stream Token secret and
/// id — the pieces a Subscription is built from.
async fn register_and_mint(server: &common::TestServer, label: &str) -> (String, String, String) {
    let client = echo_core::server::ServerClient::new(&server.base_url);
    let device = client.register_stream_device().await.expect("register");
    let minted = client
        .mint_stream_token(&device.device_key, label)
        .await
        .expect("mint");
    (device.device_key, minted.secret, minted.token_id)
}

/// A daemon environment in a temp dir: config with `subscription` in it,
/// plain-file secrets holding its Device Key and Stream Token.
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

#[tokio::test]
async fn pushed_messages_render_through_the_injected_renderer_and_reach_the_history_log() {
    let server = common::TestServer::spawn().await;
    let (device_key, secret, token_id) = register_and_mint(&server, "run-test").await;
    let env = Env::with_subscription(&server.base_url, &device_key, &secret, &token_id, "desktop");
    let fake = RecordingRenderer::default();

    let (fence_tx, fence_rx) = tokio::sync::oneshot::channel::<()>();
    let spec = echo_daemon::run::DaemonSpec {
        paths: env.paths.clone(),
        config: env.config.clone(),
        store: env.store.clone(),
        renderer: Arc::new(fake.clone()),
    };
    let runner = tokio::spawn(async move {
        echo_daemon::run::run(spec, async move {
            let _ = fence_rx.await;
        })
        .await
    });

    common::wait_active_streams(&server, 1).await;
    push_to(&server, &device_key, "first push").await;
    push_to(&server, &device_key, "second push").await;

    let rendered = wait_rendered(&fake, 2).await;
    assert_eq!(rendered[0].body, "first push");
    assert_eq!(rendered[1].body, "second push");
    assert!(
        rendered[0].id < rendered[1].id,
        "delivery order equals id order"
    );

    // The History Log holds one line per processed Push — evidence the
    // notification reached the desktop (GLOSSARY).
    let log = echo_core::history::HistoryLog::new(
        env.paths.history_dir().join(format!("{token_id}.jsonl")),
    );
    let entries = log.entries().expect("history entries");
    assert_eq!(
        entries
            .iter()
            .map(|entry| entry.body.as_str())
            .collect::<Vec<_>>(),
        vec!["first push", "second push"]
    );

    // The state file agrees: running, cursor on the last id.
    wait_state(
        &env.paths,
        &token_id,
        &echo_daemon::state::StreamState::Running,
    )
    .await;
    let state = echo_daemon::state::StateFile::read(&env.paths.state_file())
        .expect("read state")
        .expect("state present");
    assert_eq!(
        state.subscriptions[&token_id].last_message_id,
        Some(rendered[1].id)
    );

    // Shutdown is graceful: the run returns Ok and the stream is closed.
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
async fn the_daemon_rides_out_a_server_restart() {
    // Caller-owned scratch and port: the store outlives the first server
    // process and the second rebinds the same address, so the daemon's
    // Subscription never changes across the outage.
    let dir = tempfile::tempdir().expect("temp dir");
    let addr = std::net::TcpListener::bind("127.0.0.1:0")
        .expect("bind port")
        .local_addr()
        .expect("local addr");
    let server = common::TestServer::spawn_on(
        std::net::TcpListener::bind(addr).expect("bind first server"),
        dir.path().to_path_buf(),
        |_| {},
    )
    .await;
    let (device_key, secret, token_id) = register_and_mint(&server, "restart-test").await;
    let env = Env::with_subscription(&server.base_url, &device_key, &secret, &token_id, "desktop");
    let fake = RecordingRenderer::default();

    let (fence_tx, fence_rx) = tokio::sync::oneshot::channel::<()>();
    let spec = echo_daemon::run::DaemonSpec {
        paths: env.paths.clone(),
        config: env.config.clone(),
        store: env.store.clone(),
        renderer: Arc::new(fake.clone()),
    };
    let runner = tokio::spawn(async move {
        echo_daemon::run::run(spec, async move {
            let _ = fence_rx.await;
        })
        .await
    });

    common::wait_active_streams(&server, 1).await;
    push_to(&server, &device_key, "before the close").await;
    wait_rendered(&fake, 1).await;

    // The outage: the server-initiated close drops the stream; the state
    // file shows the Subscription disconnected while the daemon retries.
    server.shutdown().await;
    wait_state(
        &env.paths,
        &token_id,
        &echo_daemon::state::StreamState::Disconnected {
            detail: String::new(),
        },
    )
    .await;

    // The restarted server on the same port and store. Pushes during the
    // outage are retained for replay (§3.7) and surface after the
    // reconnect — no loss, no duplicate.
    let server = common::TestServer::spawn_on(
        std::net::TcpListener::bind(addr).expect("rebind same port"),
        dir.path().to_path_buf(),
        |_| {},
    )
    .await;
    push_to(&server, &device_key, "replayed one").await;
    push_to(&server, &device_key, "replayed two").await;

    let rendered = wait_rendered(&fake, 3).await;
    assert_eq!(rendered[1].body, "replayed one");
    assert_eq!(rendered[2].body, "replayed two");

    // Back to running, and the History Log holds the whole sequence
    // exactly once.
    wait_state(
        &env.paths,
        &token_id,
        &echo_daemon::state::StreamState::Running,
    )
    .await;
    let log = echo_core::history::HistoryLog::new(
        env.paths.history_dir().join(format!("{token_id}.jsonl")),
    );
    assert_eq!(
        log.entries()
            .expect("history entries")
            .iter()
            .map(|entry| entry.body.as_str())
            .collect::<Vec<_>>(),
        vec!["before the close", "replayed one", "replayed two"]
    );

    fence_tx.send(()).expect("fence open");
    let outcome = tokio::time::timeout(Duration::from_secs(15), runner)
        .await
        .expect("run ends within 15s")
        .expect("run task joined");
    assert!(outcome.is_ok());

    server.shutdown().await;
}

#[tokio::test]
async fn a_revoked_token_flags_only_its_subscription() {
    // Two Subscriptions on one server — the multi-machine shape (spec
    // story 3): revoking one machine's token leaves the others untouched.
    // The server restart below forces the reconnect on both, so the
    // store lives in a caller-owned dir and the port is caller-bound.
    let dir = tempfile::tempdir().expect("temp dir");
    let addr = std::net::TcpListener::bind("127.0.0.1:0")
        .expect("bind port")
        .local_addr()
        .expect("local addr");
    let server = common::TestServer::spawn_on(
        std::net::TcpListener::bind(addr).expect("bind first server"),
        dir.path().to_path_buf(),
        |_| {},
    )
    .await;
    let (device_a, secret_a, token_a) = register_and_mint(&server, "machine-a").await;
    let (device_b, secret_b, token_b) = register_and_mint(&server, "machine-b").await;
    let mut env = Env::with_subscription(&server.base_url, &device_a, &secret_a, &token_a, "a");
    env.config
        .subscriptions
        .push(echo_daemon::config::Subscription {
            id: token_b.clone(),
            server: server.base_url.clone(),
            label: "b".to_owned(),
            token_hint: format!("{}...", &secret_b[..12]),
            device_hint: format!("{}...", &device_b[..8]),
        });
    let key_b = echo_daemon::secrets::SecretKey {
        server: server.base_url.clone(),
        token_id: token_b.clone(),
    };
    env.store
        .set(
            &key_b,
            echo_daemon::secrets::SecretKind::DeviceKey,
            &device_b,
        )
        .expect("store b device key");
    env.store
        .set(
            &key_b,
            echo_daemon::secrets::SecretKind::StreamToken,
            &secret_b,
        )
        .expect("store b stream token");
    let fake = RecordingRenderer::default();

    let (fence_tx, fence_rx) = tokio::sync::oneshot::channel::<()>();
    let spec = echo_daemon::run::DaemonSpec {
        paths: env.paths.clone(),
        config: env.config.clone(),
        store: env.store.clone(),
        renderer: Arc::new(fake.clone()),
    };
    let runner = tokio::spawn(async move {
        echo_daemon::run::run(spec, async move {
            let _ = fence_rx.await;
        })
        .await
    });

    common::wait_active_streams(&server, 2).await;

    // Revoke machine-a's token from elsewhere (§3.2) and restart the
    // server to force the reconnect both machines must ride through.
    let client = echo_core::server::ServerClient::new(&server.base_url);
    client
        .revoke_stream_token(&device_a, &token_a)
        .await
        .expect("revoke a");
    server.shutdown().await;
    let server = common::TestServer::spawn_on(
        std::net::TcpListener::bind(addr).expect("rebind same port"),
        dir.path().to_path_buf(),
        |_| {},
    )
    .await;

    // machine-a surfaces the actionable re-subscribe state…
    wait_state(
        &env.paths,
        &token_a,
        &echo_daemon::state::StreamState::TokenInvalid,
    )
    .await;
    // …and never retries on it: machine-b reconnects and keeps working.
    wait_state(
        &env.paths,
        &token_b,
        &echo_daemon::state::StreamState::Running,
    )
    .await;
    push_to(&server, &device_b, "still mirrors b").await;
    let rendered = wait_rendered(&fake, 1).await;
    assert_eq!(rendered[0].body, "still mirrors b");

    fence_tx.send(()).expect("fence open");
    let outcome = tokio::time::timeout(Duration::from_secs(15), runner)
        .await
        .expect("run ends within 15s")
        .expect("run task joined");
    assert!(outcome.is_ok());

    server.shutdown().await;
}

#[tokio::test]
async fn run_refuses_when_another_instance_holds_the_lock() {
    let server = common::TestServer::spawn().await;
    let (device_key, secret, token_id) = register_and_mint(&server, "lock-test").await;
    let env = Env::with_subscription(&server.base_url, &device_key, &secret, &token_id, "desktop");
    let fake = RecordingRenderer::default();

    // Another live instance holds the single-instance lock.
    let _other = echo_daemon::lock::DaemonLock::acquire(env.paths.lock_file()).expect("held");

    let outcome = echo_daemon::run::run(
        echo_daemon::run::DaemonSpec {
            paths: env.paths.clone(),
            config: env.config.clone(),
            store: env.store.clone(),
            renderer: Arc::new(fake.clone()),
        },
        std::future::pending::<()>(),
    )
    .await;
    match outcome {
        Err(echo_daemon::run::RunError::Lock(_)) => {}
        other => panic!("a contested lock must be refused, got {other:?}"),
    }

    server.shutdown().await;
}

#[tokio::test]
async fn run_without_subscriptions_is_an_actionable_error() {
    let dir = tempfile::tempdir().expect("temp dir");
    let outcome = echo_daemon::run::run(
        echo_daemon::run::DaemonSpec {
            paths: echo_daemon::paths::Paths::from_dir(dir.path()),
            config: echo_daemon::config::Config::default(),
            store: Arc::new(echo_daemon::secrets::PlainFileStore::new(
                dir.path().join("secrets.toml"),
            )),
            renderer: Arc::new(RecordingRenderer::default()),
        },
        std::future::pending::<()>(),
    )
    .await;
    match outcome {
        Err(err) => assert!(
            err.to_string().contains("subscribe add"),
            "the error points at the fix: {err}"
        ),
        Ok(()) => panic!("a daemon with nothing to mirror must not start"),
    }
}
