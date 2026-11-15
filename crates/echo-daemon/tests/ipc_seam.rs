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
        use echo_daemon::secrets::PlainFileStore;

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
        Self::with_subscription_and_auth(
            server_url,
            device_key,
            stream_token,
            token_id,
            label,
            None,
        )
    }

    /// [`Env::with_subscription`] plus per-server operator Basic Auth
    /// (protocol §1.4): the user rides the config's public fields, the
    /// password only the daemon's store (spec story 13) — the shape the
    /// Composer's send resolves on the daemon side (ticket #16).
    fn with_subscription_and_auth(
        server_url: &str,
        device_key: &str,
        stream_token: &str,
        token_id: &str,
        label: &str,
        auth: Option<(&str, &str)>,
    ) -> Self {
        use echo_daemon::config::{Config, ServerEntry, Subscription};
        use echo_daemon::secrets::{SecretKey, SecretKind};

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
        let servers = match auth {
            Some((user, password)) => {
                env.store
                    .set(
                        &SecretKey::for_server(server_url),
                        SecretKind::BasicAuthPassword,
                        password,
                    )
                    .expect("store basic auth password");
                vec![ServerEntry {
                    url: server_url.to_owned(),
                    basic_auth_user: user.to_owned(),
                    basic_auth_password_stored: true,
                }]
            }
            None => Vec::new(),
        };
        env.config = Config {
            subscriptions: vec![Subscription {
                id: token_id.to_owned(),
                server: server_url.to_owned(),
                label: label.to_owned(),
                token_hint: format!("{}...", &stream_token[..12]),
                device_hint: format!("{}...", &device_key[..8]),
            }],
            servers,
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
async fn the_history_snapshot_answers_with_the_daemons_own_log_tail() {
    // Ticket #15: the viewer's initial content is the daemon's answer —
    // the daemon reads its daemon-private History Log (ADR-0004) with
    // the CLI's `log --tail N` semantics: all complete entries, oldest
    // last, the last `tail` of them. The test reads the file only as
    // the referee; the viewer itself never will.
    let server = common::TestServer::spawn().await;
    let (device_key, secret, token_id) = register_and_mint(&server, "history-tail").await;
    let env = Env::with_subscription(&server.base_url, &device_key, &secret, &token_id, "desktop");
    let (fence_tx, runner) = running_daemon(&env).await;

    common::wait_active_streams(&server, 1).await;
    let endpoint = env.paths.ipc_endpoint();
    let mut client = connect_when_listening(&endpoint).await;

    // Three pushes land in the Subscription's History Log; the wait is
    // on the daemon's own answer, not the state file's cursor — the log
    // line lands a beat after the cursor, and the channel answer is
    // what the viewer will see.
    for body in ["first", "second", "third"] {
        push_to(&server, &device_key, body).await;
    }
    let deadline = Instant::now() + Duration::from_secs(15);
    let full = loop {
        match client.history_snapshot(&token_id, 100).await {
            Ok(entries) if entries.len() == 3 => break entries,
            Ok(_) | Err(_) => {
                assert!(
                    Instant::now() < deadline,
                    "the daemon's answer never showed three entries"
                );
                tokio::time::sleep(Duration::from_millis(20)).await;
            }
        }
    };
    assert_eq!(full[0].body, "first", "oldest last");
    assert_eq!(full[2].body, "third");

    // The bounded tail is the last N entries, oldest last.
    let tail = client.history_snapshot(&token_id, 2).await.expect("tail");
    assert_eq!(tail.len(), 2);
    assert_eq!(tail[0].body, "second");
    assert_eq!(tail[1].body, "third");

    // The answer is the log file's content verbatim in vocabulary and
    // order: the complete entries' last N — the slice `log --tail 2`
    // prints (the test plays referee by reading the daemon-owned file).
    let log = echo_core::history::HistoryLog::new(env.paths.history_log(&token_id));
    let entries = log.entries().expect("read the daemon's log");
    assert_eq!(
        tail,
        entries[entries.len() - 2..],
        "the channel answer is the log file's own tail"
    );

    // A Subscription the daemon does not hold is refused, not answered
    // with a silently empty log — and so no path outside the daemon's
    // own Subscription logs is ever served.
    let refused = client.history_snapshot("no-such-subscription", 10).await;
    assert!(
        matches!(refused, Err(echo_ipc::IpcError::Rejected(_))),
        "an unknown Subscription is an error answer: {refused:?}"
    );

    fence_tx.send(()).expect("fence open");
    let outcome = tokio::time::timeout(Duration::from_secs(10), runner)
        .await
        .expect("run ends within 10s")
        .expect("run task joined");
    assert!(outcome.is_ok(), "run must not fail: {outcome:?}");

    server.shutdown().await;
}

#[cfg(unix)]
#[tokio::test]
async fn the_viewers_initial_content_is_what_log_tail_prints() {
    // The acceptance line (ticket #15): what the viewer opens with is
    // what `log --tail N` shows for the same Subscription. The CLI
    // child prints the human lines; the daemon answers the entries —
    // and the two agree through the same formatter the CLI uses.
    //
    // Platform fact (unix-only): the CLI subprocess relocates its whole
    // install with HOME/XDG_CONFIG_HOME (tests/common Harness::spawn's
    // unix arm), which is how this test points a real `bark-echo log`
    // child at the same install the in-process daemon serves. Windows
    // relocates via %APPDATA%\bark-echo instead (Harness's windows arm)
    // and has no XDG twin — the same daemon handler answers both
    // platforms, and Windows' channel face stays covered by the other
    // ipc_seam tests plus tests/log_cli.rs.
    let harness = common::Harness::new();
    let paths = harness.paths();

    let server = common::TestServer::spawn().await;
    let (device_key, secret, token_id) = register_and_mint(&server, "log-tail-agrees").await;
    {
        use echo_daemon::secrets::{PlainFileStore, SecretKey, SecretKind, SecretStore};
        let store = PlainFileStore::new(paths.secrets_file());
        let key = SecretKey {
            server: server.base_url.clone(),
            token_id: token_id.clone(),
        };
        store
            .set(&key, SecretKind::DeviceKey, &device_key)
            .expect("store device key");
        store
            .set(&key, SecretKind::StreamToken, &secret)
            .expect("store stream token");
    }
    let config = echo_daemon::config::Config {
        subscriptions: vec![echo_daemon::config::Subscription {
            id: token_id.clone(),
            server: server.base_url.clone(),
            label: "desktop".to_owned(),
            token_hint: format!("{}...", &secret[..12]),
            device_hint: format!("{}...", &device_key[..8]),
        }],
        servers: Vec::new(),
    };
    // The CLI child reads the Subscription list from the files (its
    // headless path); the daemon gets the same list injected.
    config
        .save(&paths.config_file())
        .expect("write the config the CLI will read");

    let (fence_tx, fence_rx) = tokio::sync::oneshot::channel::<()>();
    let daemon_paths = paths.clone();
    let runner = tokio::spawn(async move {
        echo_daemon::run::run(
            echo_daemon::run::DaemonSpec {
                paths: daemon_paths.clone(),
                config,
                store: Arc::new(echo_daemon::secrets::PlainFileStore::new(
                    daemon_paths.secrets_file(),
                )),
                renderer: Arc::new(SilentRenderer),
            },
            async move {
                let _ = fence_rx.await;
            },
        )
        .await
    });

    common::wait_active_streams(&server, 1).await;
    let endpoint = paths.ipc_endpoint();
    let mut client = connect_when_listening(&endpoint).await;
    for body in ["first", "second", "third"] {
        push_to(&server, &device_key, body).await;
    }
    let deadline = Instant::now() + Duration::from_secs(15);
    loop {
        match client.history_snapshot(&token_id, 100).await {
            Ok(entries) if entries.len() == 3 => break,
            Ok(_) | Err(_) => {
                assert!(
                    Instant::now() < deadline,
                    "the daemon's answer never showed three entries"
                );
                tokio::time::sleep(Duration::from_millis(20)).await;
            }
        }
    }

    // The two faces of one fact: the channel answer the viewer opens
    // with, and the lines the CLI prints — `log --tail 2`, same
    // Subscription, same files behind it.
    let stdout = harness
        .run(&["--plain-secrets", "log", "--sub", "desktop", "--tail", "2"])
        .await
        .stdout_or_die("log --tail 2");
    let entries = client
        .history_snapshot(&token_id, 2)
        .await
        .expect("the viewer's tail");
    let lines: Vec<&str> = stdout.lines().collect();
    assert_eq!(
        lines.len(),
        entries.len(),
        "one line per entry: {stdout} vs {entries:?}"
    );
    for (line, entry) in lines.iter().zip(&entries) {
        assert_eq!(
            line,
            &echo_daemon::history::format_entry(entry),
            "the viewer's opening content is what log --tail 2 prints"
        );
    }
    assert_eq!(entries[0].body, "second", "the tail is the newest end");

    fence_tx.send(()).expect("fence open");
    let outcome = tokio::time::timeout(Duration::from_secs(10), runner)
        .await
        .expect("run ends within 10s")
        .expect("run task joined");
    assert!(outcome.is_ok(), "run must not fail: {outcome:?}");

    server.shutdown().await;
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
async fn a_shutdown_request_rides_the_same_graceful_stop_as_the_fence() {
    // Ticket #12: the Tray Menu's Quit asks over the channel, and the
    // daemon answers with its graceful stop — the same path the owner's
    // future (ctrl-C in production, a fence in tests) rides: the
    // channel closes, the streams fence, the run returns Ok, and the
    // lock dies with the run.
    let server = common::TestServer::spawn().await;
    let (device_key, secret, token_id) = register_and_mint(&server, "quit-by-ipc").await;
    let env = Env::with_subscription(&server.base_url, &device_key, &secret, &token_id, "desktop");
    let (fence_tx, runner) = running_daemon(&env).await;

    common::wait_active_streams(&server, 1).await;
    let endpoint = env.paths.ipc_endpoint();
    let mut client = Client::connect(&endpoint).await.expect("connect");
    wait_snapshot(&mut client, "running", |snapshot| {
        snapshot
            .subscriptions
            .get(&token_id)
            .map(|runtime| runtime.state == StreamState::Running)
            .unwrap_or(false)
    })
    .await;

    // The Quit request: accepted before the stop begins.
    client
        .shutdown()
        .await
        .expect("the daemon accepts the shutdown");

    // The run ends the graceful way — the fence never had to speak.
    let outcome = tokio::time::timeout(Duration::from_secs(10), runner)
        .await
        .expect("run ends within 10s")
        .expect("run task joined");
    assert!(outcome.is_ok(), "run must not fail: {outcome:?}");
    // The streams closed with it…
    common::wait_active_streams(&server, 0).await;
    // …and the lock died with the run: nothing stands in the way of the
    // next launch (spawn-if-absent's takeover, ticket #12).
    assert!(
        matches!(
            echo_daemon::lock::DaemonLock::probe(env.paths.lock_file()),
            Ok(false)
        ),
        "the lock is released with the run"
    );

    // The fence is still there; closing it is a no-op now.
    let _ = fence_tx.send(());
    server.shutdown().await;
}

#[tokio::test]
async fn a_launch_never_attempts_a_second_daemon_while_the_lock_is_held() {
    // Ticket #12's attach rule: with a daemon running, the launch shape
    // (spawn-if-absent) probes the lock, answers AlreadyRunning, and
    // spawns nothing — the running daemon keeps serving, untouched.
    let server = common::TestServer::spawn().await;
    let (device_key, secret, token_id) = register_and_mint(&server, "no-second-attempt").await;
    let env = Env::with_subscription(&server.base_url, &device_key, &secret, &token_id, "desktop");
    let (fence_tx, runner) = running_daemon(&env).await;

    common::wait_active_streams(&server, 1).await;

    // The launch shape against a held lock. The program argument is
    // beside the point here — the probe answers before any spawn could
    // read it — but it names a real binary so the production shape is
    // what runs.
    let program = std::path::Path::new(env!("CARGO_BIN_EXE_bark-echo"));
    match echo_daemon::lifecycle::spawn_detached_daemon(program, &env.paths, true)
        .expect("the launch shape probes")
    {
        echo_daemon::lifecycle::SpawnOutcome::AlreadyRunning => {}
        echo_daemon::lifecycle::SpawnOutcome::Spawned { mut child } => {
            let _ = child.kill();
            panic!("a held lock must never spawn a second daemon attempt");
        }
    }

    // The first daemon is untouched and still mirroring.
    let endpoint = env.paths.ipc_endpoint();
    let mut client = Client::connect(&endpoint).await.expect("connect");
    let snapshot = client.status().await.expect("status");
    assert_eq!(
        snapshot.pid,
        std::process::id(),
        "the channel still answers for the one and only daemon"
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

    client
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

// ---- The Push Composer's send (ticket #16) ----

/// Drain `listener` past the connect-time stream flips until the
/// History Log append, and return it.
async fn wait_history_append(
    listener: &mut Client,
    subscription_id: &str,
    body: &str,
) -> echo_ipc::message::NotificationIntent {
    let deadline = Instant::now() + Duration::from_secs(15);
    loop {
        let event = tokio::time::timeout(Duration::from_secs(15), listener.next_event())
            .await
            .expect("an event within 15s")
            .expect("the channel stayed open");
        match event {
            echo_ipc::message::Event::HistoryAppended {
                subscription_id: id,
                entry,
                ..
            } => {
                assert_eq!(id, subscription_id);
                assert_eq!(
                    entry.body, body,
                    "this append is the Composer's Push, mirrored back"
                );
                return entry;
            }
            other => {
                assert!(
                    Instant::now() < deadline,
                    "the append never arrived: {other:?}"
                );
            }
        }
    }
}

#[tokio::test]
async fn a_composer_send_arrives_at_the_target_device_end_to_end() {
    // Ticket #16's acceptance: the send is one IPC request; the daemon
    // performs it; the Push lands at the target Device through the
    // embedded server — here the Subscription's own Device, which the
    // daemon itself mirrors, so the arrival is observable as a History
    // Log append announced on the channel.
    let server = common::TestServer::spawn().await;
    let (device_key, secret, token_id) = register_and_mint(&server, "composer").await;
    let env = Env::with_subscription(&server.base_url, &device_key, &secret, &token_id, "desktop");
    let (fence_tx, runner) = running_daemon(&env).await;

    common::wait_active_streams(&server, 1).await;
    let endpoint = env.paths.ipc_endpoint();
    let mut listener = connect_when_listening(&endpoint).await;
    let mut client = connect_when_listening(&endpoint).await;

    // The send rides the seam: Subscription reference, explicit target
    // Device Key, alert fields, Ext Params — and nothing else.
    let outcome = client
        .send_push(echo_ipc::message::SendPush {
            subscription_id: token_id.clone(),
            device_key: device_key.clone(),
            title: Some("from the Composer".to_owned()),
            subtitle: None,
            body: Some("through the seam".to_owned()),
            ext_params: [
                ("url".to_owned(), "https://example.com".to_owned()),
                ("sound".to_owned(), "bell".to_owned()),
            ]
            .into_iter()
            .collect(),
        })
        .await
        .expect("the send was answered");
    assert_eq!(outcome, echo_ipc::message::SendPushOutcome::Sent);

    // The target Device received it: the daemon's own mirror appended
    // the line — and the Ext Params rode through exactly as the server's
    // V2 JSON route passes them for the CLI, `url` resolving into the
    // intent's click action (the same mapping `push` produces).
    let entry = wait_history_append(&mut listener, &token_id, "through the seam").await;
    assert_eq!(entry.title, "from the Composer");
    assert_eq!(
        entry.click_action,
        Some(echo_core::intent::ClickAction::OpenUrl {
            url: "https://example.com".to_owned(),
        }),
        "the url Ext Param survived the whole ride: {entry:?}"
    );

    // The channel's cursor follows the send, like any other Push.
    let snapshot = wait_snapshot(&mut client, "the processed id", |snapshot| {
        snapshot.subscriptions[&token_id].last_message_id.is_some()
    })
    .await;
    assert_eq!(
        snapshot.subscriptions[&token_id].last_message_id,
        Some(entry.id)
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
async fn a_rejected_composer_send_surfaces_the_servers_verdict_readably() {
    // A bad Device Key: the server's own refusal rides the seam
    // verbatim — the window will show what the server said, never a
    // silent failure (ticket #16's acceptance).
    let server = common::TestServer::spawn().await;
    let (device_key, secret, token_id) = register_and_mint(&server, "composer-reject").await;
    let env = Env::with_subscription(&server.base_url, &device_key, &secret, &token_id, "desktop");
    let (fence_tx, runner) = running_daemon(&env).await;
    let endpoint = env.paths.ipc_endpoint();
    let mut client = connect_when_listening(&endpoint).await;

    let outcome = client
        .send_push(echo_ipc::message::SendPush {
            subscription_id: token_id.clone(),
            device_key: "no-such-device-key".to_owned(),
            title: None,
            subtitle: None,
            body: Some("doomed".to_owned()),
            ext_params: Default::default(),
        })
        .await
        .expect("the verdict answers even for a refusal");
    match outcome {
        echo_ipc::message::SendPushOutcome::Rejected { code, message } => {
            assert_eq!(code, Some(400));
            assert!(
                message.contains("device token"),
                "the server's own message rides: {message}"
            );
        }
        other => panic!("a bad Device Key is a rejection, got {other:?}"),
    }

    fence_tx.send(()).expect("fence open");
    let outcome = tokio::time::timeout(Duration::from_secs(10), runner)
        .await
        .expect("run ends within 10s")
        .expect("run task joined");
    assert!(outcome.is_ok(), "run must not fail: {outcome:?}");
    server.shutdown().await;
}

#[tokio::test]
async fn the_composer_send_works_with_secrets_touched_only_by_the_daemon() {
    // A Basic-Auth-protected server (protocol §1.4): the password lives
    // only in the daemon's store. The IPC client — the GUI's exact
    // position — cannot carry a credential: its request names the
    // Subscription, and the send still goes out. Assert the daemon-side
    // credential path through the seam, arrival included.
    let server = common::TestServer::spawn_with_basic_auth("operator", "hunter2").await;
    // /register is Basic-Auth-exempt (protocol §1.4); minting is not,
    // so setup rides the operator credentials.
    let setup = echo_core::server::ServerClient::new(&server.base_url)
        .with_basic_auth("operator", "hunter2");
    let device = setup.register_stream_device().await.expect("register");
    let minted = setup
        .mint_stream_token(&device.device_key, "composer-secrets")
        .await
        .expect("mint");
    let env = Env::with_subscription_and_auth(
        &server.base_url,
        &device.device_key,
        &minted.secret,
        &minted.token_id,
        "desktop",
        Some(("operator", "hunter2")),
    );
    let (fence_tx, runner) = running_daemon(&env).await;

    let endpoint = env.paths.ipc_endpoint();
    let mut listener = connect_when_listening(&endpoint).await;
    let mut client = connect_when_listening(&endpoint).await;

    let outcome = client
        .send_push(echo_ipc::message::SendPush {
            subscription_id: minted.token_id.clone(),
            device_key: device.device_key.clone(),
            title: None,
            subtitle: None,
            body: Some("credentials stayed home".to_owned()),
            ext_params: Default::default(),
        })
        .await
        .expect("the send was answered");
    assert_eq!(outcome, echo_ipc::message::SendPushOutcome::Sent);

    // The daemon's own stream (its credentials, its connection policy)
    // mirrored the send back: arrival, not just acceptance.
    wait_history_append(&mut listener, &minted.token_id, "credentials stayed home").await;

    fence_tx.send(()).expect("fence open");
    let outcome = tokio::time::timeout(Duration::from_secs(10), runner)
        .await
        .expect("run ends within 10s")
        .expect("run task joined");
    assert!(outcome.is_ok(), "run must not fail: {outcome:?}");
    server.shutdown().await;
}
