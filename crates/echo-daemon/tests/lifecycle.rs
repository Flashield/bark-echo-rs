//! The lifecycle seam (ticket #12): the real `bark-echo` binary, spawned
//! the way the Tray Menu's launch spawns it, driven over the real
//! channel. One test in one binary — and that is a constraint, not a
//! choice: the test points the process environment at a temp config
//! root so the spawned children resolve their files there (the way they
//! will on a desktop), and the environment is process-wide, so a second
//! test here would race it. Everything the launch shape must prove
//! therefore lives in this one flow: spawn-if-absent over an idle lock,
//! attach without a second attempt, the clean exit (code 0, lock
//! released), and the next launch taking the released lock over.

mod common;

use std::path::Path;
use std::time::{Duration, Instant};

use echo_daemon::lifecycle::{SpawnOutcome, spawn_detached_daemon};
use echo_daemon::lock::DaemonLock;
use echo_daemon::paths::Paths;

use echo_ipc::Client;

/// One daemon's whole life here — come up, serve, die on request —
/// inside one CI budget.
const DAEMON_LIFETIME: Duration = Duration::from_secs(15);

/// One registered Stream Device with its minted Stream Token secret and
/// id (mirrors ipc_seam's helper; test binaries share no code).
async fn register_and_mint(server: &common::TestServer, label: &str) -> (String, String, String) {
    let client = echo_core::server::ServerClient::new(&server.base_url);
    let device = client.register_stream_device().await.expect("register");
    let minted = client
        .mint_stream_token(&device.device_key, label)
        .await
        .expect("mint");
    (device.device_key, minted.secret, minted.token_id)
}

/// Connect with a deadline: the spawned daemon binds the channel a beat
/// after the spawn, and the Tray Menu starts whenever it starts.
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

/// Wait for the spawned child's daemon to be reachable and mirroring:
/// the stream must be open on the embedded server, not merely claimed.
async fn wait_stream_open(server: &common::TestServer) {
    common::wait_active_streams(server, 1).await;
}

#[tokio::test]
async fn a_launch_spawns_attaches_quits_and_takes_over() {
    let server = common::TestServer::spawn().await;
    let (device_key, secret, token_id) = register_and_mint(&server, "lifecycle").await;

    // A temp config root this test's process environment points at: the
    // children spawned below resolve their config dir from the
    // environment at startup (paths.rs), exactly as they will on a
    // desktop. Platform facts: Unix reads $XDG_CONFIG_HOME, else
    // $HOME/.config; macOS reads $HOME only; Windows reads %APPDATA%.
    // Desktop users get the same resolution against their real
    // environment. (Rust 2024 fences env mutation; the alternative is
    // an environment seam through the spawn function that production
    // would never use.)
    //
    // The root is deliberately short (common::short_temp_home): the
    // children bind the channel at `Library/Application Support/bark-echo/
    // daemon.ipc` under it, and macOS's 104-byte sun_len cannot fit the
    // per-user temp root's prefix in front of that (ticket #27).
    let root = common::short_temp_home();
    #[cfg(all(unix, not(target_os = "macos")))]
    unsafe {
        std::env::set_var("HOME", root.path());
        std::env::set_var("XDG_CONFIG_HOME", root.path().join(".config"));
    }
    #[cfg(target_os = "macos")]
    unsafe {
        std::env::set_var("HOME", root.path());
    }
    #[cfg(windows)]
    unsafe {
        std::env::set_var("APPDATA", root.path().join("AppData/Roaming"));
    }

    // The config dir the children resolve — through the same pure
    // function the binary uses, so this test cannot drift from it.
    #[cfg(all(unix, not(target_os = "macos")))]
    let config_dir =
        echo_daemon::paths::config_dir_from(Some(root.path()), Some(&root.path().join(".config")))
            .expect("temp root resolves");
    #[cfg(target_os = "macos")]
    let config_dir =
        echo_daemon::paths::config_dir_from(Some(root.path()), None).expect("temp root resolves");
    #[cfg(windows)]
    let config_dir = echo_daemon::paths::config_dir_from(Some(root.path().join("AppData/Roaming")))
        .expect("temp root resolves");
    let paths = Paths::from_dir(config_dir);

    // A real Subscription: config.toml and plain-file secrets the child
    // loads at startup, pointed at the embedded server so its Realtime
    // Stream actually runs.
    {
        use echo_daemon::config::{Config, Subscription};
        use echo_daemon::secrets::{PlainFileStore, SecretKey, SecretKind, SecretStore};
        let store = PlainFileStore::new(paths.secrets_file());
        let key = SecretKey {
            server: server.base_url.clone(),
            token_id: token_id.clone(),
        };
        store
            .set(&key, SecretKind::DeviceKey, &device_key)
            .expect("device key stored");
        store
            .set(&key, SecretKind::StreamToken, &secret)
            .expect("stream token stored");
        let config = Config {
            subscriptions: vec![Subscription {
                id: token_id.clone(),
                server: server.base_url.clone(),
                label: "desktop".to_owned(),
                token_hint: format!("{}...", &secret[..12]),
                device_hint: format!("{}...", &device_key[..8]),
            }],
            servers: Vec::new(),
        };
        config.save(&paths.config_file()).expect("config saved");
    }

    let program = Path::new(env!("CARGO_BIN_EXE_bark-echo"));

    // First launch, lock idle: the daemon is spawned detached.
    let mut child = match spawn_detached_daemon(program, &paths, true).expect("spawn") {
        SpawnOutcome::Spawned { child } => child,
        SpawnOutcome::AlreadyRunning => panic!("the lock was idle — a spawn was due"),
    };
    let spawned_pid = child.id();

    // …it comes up, holds the lock, and its stream actually runs.
    let endpoint = paths.ipc_endpoint();
    let mut client = connect_when_listening(&endpoint).await;
    let snapshot = client.status().await.expect("status");
    assert_eq!(
        snapshot.pid, spawned_pid,
        "the channel answers for the spawned daemon"
    );
    wait_stream_open(&server).await;

    // A launch against the held lock attaches — never a second attempt
    // (two daemons would double-display every Push, ADR-0002).
    match spawn_detached_daemon(program, &paths, true).expect("probe") {
        SpawnOutcome::AlreadyRunning => {}
        SpawnOutcome::Spawned { mut child } => {
            let _ = child.kill();
            panic!("a held lock must not spawn a second daemon");
        }
    }

    // Quit: the Shutdown request; the daemon exits with the clean code
    // no `Restart=on-failure` / `KeepAlive SuccessfulExit=false`
    // supervisor resurrects (docs/autostart.md's samples).
    client
        .shutdown()
        .await
        .expect("the daemon accepts the shutdown");
    let status = tokio::time::timeout(
        DAEMON_LIFETIME,
        tokio::task::spawn_blocking(move || child.wait().expect("child waited")),
    )
    .await
    .expect("the daemon exits in time")
    .expect("wait joined");
    assert_eq!(
        status.code(),
        Some(0),
        "a deliberate Quit exits 0 — the autostart samples do not resurrect a clean stop"
    );
    assert!(
        matches!(DaemonLock::probe(paths.lock_file()), Ok(false)),
        "the lock died with the process"
    );

    // A fresh launch takes the released lock over: spawn-if-absent's
    // whole point, and the acceptance criterion "a fresh GUI launch
    // after Quit works".
    let mut child = match spawn_detached_daemon(program, &paths, true).expect("respawn") {
        SpawnOutcome::Spawned { child } => child,
        SpawnOutcome::AlreadyRunning => panic!("the lock was released — a spawn was due"),
    };
    let mut client = connect_when_listening(&endpoint).await;
    wait_stream_open(&server).await;
    client
        .shutdown()
        .await
        .expect("the takeover daemon accepts the shutdown");
    let status = tokio::time::timeout(
        DAEMON_LIFETIME,
        tokio::task::spawn_blocking(move || child.wait().expect("child waited")),
    )
    .await
    .expect("the daemon exits in time")
    .expect("wait joined");
    assert_eq!(
        status.code(),
        Some(0),
        "the takeover ends as cleanly as it began"
    );

    server.shutdown().await;
}
