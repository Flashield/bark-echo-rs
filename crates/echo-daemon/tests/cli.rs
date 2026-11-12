//! CLI seam (spec Testing Decisions): the `subscribe` subcommands run as
//! a subprocess against a temp `HOME` in plain-secrets mode — CI has no
//! secret service, so this doubles as the headless fallback's end-to-end
//! coverage. Exit codes and stdout/stderr are the contract. Subprocesses
//! are awaited (tokio::process) so the embedded server task keeps running
//! while a CLI run blocks.

mod common;

use echo_core::server::ServerClient;
use tokio::process::Command;

use echo_daemon::config::Config;
use echo_daemon::secrets::{PlainFileStore, SecretError, SecretKey, SecretKind, SecretStore};

/// One CLI run's observable surface.
struct Run {
    success: bool,
    stdout: String,
    stderr: String,
}

impl Run {
    fn stdout_or_die(&self, what: &str) -> String {
        assert!(
            self.success,
            "{what} must exit 0; stderr: {}; stdout: {}",
            self.stderr, self.stdout
        );
        self.stdout.clone()
    }
}

/// A hermetic run environment: a temp `HOME` (so config and the plain
/// secrets file land nowhere else) and the daemon subprocess.
struct Harness {
    home: tempfile::TempDir,
}

impl Harness {
    fn new() -> Self {
        Harness {
            home: tempfile::tempdir().expect("temp home"),
        }
    }

    /// The config locations the daemon resolves inside this harness —
    /// computed with the same pure function the binary uses.
    #[cfg(unix)]
    fn paths(&self) -> echo_daemon::paths::Paths {
        let xdg = self.home.path().join(".config");
        let config_dir = echo_daemon::paths::config_dir_from(Some(self.home.path()), Some(&xdg))
            .expect("temp home resolves");
        echo_daemon::paths::Paths::from_dir(config_dir)
    }

    async fn run(&self, args: &[&str]) -> Run {
        let output = self.spawn(args).output().await.expect("spawn bark-echo");
        Run {
            success: output.status.success(),
            stdout: String::from_utf8_lossy(&output.stdout).into_owned(),
            stderr: String::from_utf8_lossy(&output.stderr).into_owned(),
        }
    }

    #[cfg(unix)]
    fn spawn(&self, args: &[&str]) -> Command {
        let mut command = Command::new(env!("CARGO_BIN_EXE_bark-echo"));
        command
            .args(args)
            .env("HOME", self.home.path())
            .env("XDG_CONFIG_HOME", self.home.path().join(".config"));
        command
    }

    #[cfg(windows)]
    fn spawn(&self, args: &[&str]) -> Command {
        let mut command = Command::new(env!("CARGO_BIN_EXE_bark-echo"));
        command
            .args(args)
            .env("APPDATA", self.home.path().join("AppData/Roaming"));
        command
    }
}

/// A secret service answers this session (mirrors tests/keyring_store.rs's
/// probe): headless-behavior tests must not run where keyring would win.
/// The probe account is unique per call so parallel probes never fight
/// over the same entry.
fn secret_service_reachable() -> bool {
    let probe = format!(
        "echo-daemon-test-probe-{}-{}",
        std::process::id(),
        std::time::SystemTime::now()
            .duration_since(std::time::UNIX_EPOCH)
            .map(|since| since.subsec_nanos())
            .unwrap_or(0)
    );
    let Ok(entry) = keyring::Entry::new(&probe, "probe") else {
        return false;
    };
    match entry.set_password("probe") {
        Ok(()) => entry.delete_credential().is_ok(),
        Err(_) => false,
    }
}

#[tokio::test]
async fn subscribe_add_own_persists_everything_and_prints_a_live_device_key() {
    let server = common::TestServer::spawn().await;
    let harness = Harness::new();

    let stdout = harness
        .run(&[
            "--plain-secrets",
            "subscribe",
            "add",
            "--server",
            &server.base_url,
            "--own",
            "--label",
            "cli-test",
        ])
        .await
        .stdout_or_die("subscribe add --own");

    // The printed Device Key is the real push target: the server knows it.
    let device_key = stdout
        .lines()
        .find_map(|line| line.strip_prefix("device key: "))
        .expect("add prints the Device Key");
    assert_eq!(device_key.len(), 22, "device key shape");
    let tokens = ServerClient::new(&server.base_url)
        .list_stream_tokens(device_key)
        .await
        .expect("printed device key works against the server");
    assert_eq!(
        tokens[0].client_label, "cli-test",
        "the label reached the server's token record"
    );

    // The Subscription survived to the temp HOME's config file.
    let paths = harness.paths();
    let config = Config::load(&paths.config_file()).expect("config loads");
    assert_eq!(config.subscriptions.len(), 1, "one Subscription persisted");
    let subscription = &config.subscriptions[0];
    assert_eq!(subscription.server, server.base_url);
    assert_eq!(subscription.label, "cli-test");

    // Secrets live in the plain file (0600), retrievable by the
    // Subscription's identity — and the hint matches the stored secret.
    #[cfg(unix)]
    {
        use std::os::unix::fs::PermissionsExt;
        let mode = std::fs::metadata(paths.secrets_file())
            .expect("secrets file exists")
            .permissions()
            .mode();
        assert_eq!(mode & 0o777, 0o600, "secrets file must be 0600");
    }
    let store = PlainFileStore::new(paths.secrets_file());
    let key = SecretKey {
        server: server.base_url.clone(),
        token_id: subscription.id.clone(),
    };
    assert_eq!(
        store
            .get(&key, SecretKind::DeviceKey)
            .expect("device key stored"),
        device_key,
        "the stored Device Key is the printed one"
    );
    let token = store
        .get(&key, SecretKind::StreamToken)
        .expect("token stored");
    assert_eq!(
        subscription.token_hint,
        format!("{}...", &token[..12]),
        "the config's hint matches the stored secret"
    );

    server.shutdown().await;
}

#[tokio::test]
async fn subscribe_add_with_device_key_mirrors_and_the_cli_round_trips() {
    let server = common::TestServer::spawn().await;
    let harness = Harness::new();
    let device = ServerClient::new(&server.base_url)
        .register_stream_device()
        .await
        .expect("register the mirrored Device");

    let stdout = harness
        .run(&[
            "--plain-secrets",
            "subscribe",
            "add",
            "--server",
            &server.base_url,
            "--device-key",
            &device.device_key,
            "--label",
            "family-ipad",
        ])
        .await
        .stdout_or_die("subscribe add --device-key");
    assert!(
        stdout.contains(&device.device_key),
        "the mirrored Device's key is echoed: {stdout}"
    );

    // list shows the Subscription with its token hint.
    let paths = harness.paths();
    let config = Config::load(&paths.config_file()).expect("config loads");
    let id = &config.subscriptions[0].id;
    let hint = &config.subscriptions[0].token_hint;
    let listing = harness
        .run(&["--plain-secrets", "subscribe", "list"])
        .await
        .stdout_or_die("subscribe list");
    assert!(
        listing.contains(id.as_str())
            && listing.contains("family-ipad")
            && listing.contains(hint.as_str()),
        "list shows id, label, and token hint, got: {listing}"
    );

    // rm removes it and deletes its secrets.
    harness
        .run(&["--plain-secrets", "subscribe", "rm", id])
        .await
        .stdout_or_die("subscribe rm");
    let config = Config::load(&paths.config_file()).expect("config loads");
    assert!(
        config.subscriptions.is_empty(),
        "the Subscription is gone from the config"
    );
    let store = PlainFileStore::new(paths.secrets_file());
    let key = SecretKey {
        server: server.base_url.clone(),
        token_id: id.clone(),
    };
    assert!(
        matches!(
            store.get(&key, SecretKind::StreamToken),
            Err(SecretError::NotFound { .. })
        ),
        "rm deleted the token secret"
    );
    assert!(
        matches!(
            store.get(&key, SecretKind::DeviceKey),
            Err(SecretError::NotFound { .. })
        ),
        "rm deleted the device key secret"
    );
    let listing = harness
        .run(&["--plain-secrets", "subscribe", "list"])
        .await
        .stdout_or_die("subscribe list after rm");
    assert_eq!(
        listing.trim(),
        "no subscriptions",
        "an empty list says so instead of printing nothing"
    );

    server.shutdown().await;
}

#[tokio::test]
async fn without_a_label_the_machine_name_labels_the_token() {
    let server = common::TestServer::spawn().await;
    let harness = Harness::new();
    let device = ServerClient::new(&server.base_url)
        .register_stream_device()
        .await
        .expect("register the mirrored Device");

    harness
        .run(&[
            "--plain-secrets",
            "subscribe",
            "add",
            "--server",
            &server.base_url,
            "--device-key",
            &device.device_key,
        ])
        .await
        .stdout_or_die("add without --label");

    let config = Config::load(&harness.paths().config_file()).expect("config loads");
    assert!(
        !config.subscriptions[0].label.is_empty(),
        "the default label is the machine's name, never empty"
    );

    server.shutdown().await;
}

#[tokio::test]
async fn adding_needs_exactly_one_of_own_or_device_key() {
    let harness = Harness::new();

    let run = harness
        .run(&[
            "subscribe",
            "add",
            "--server",
            "http://127.0.0.1:9",
            "--own",
            "--device-key",
            "itu4irzixECYJ5Ftd9UygX",
        ])
        .await;
    assert!(
        !run.success && run.stderr.contains("--device-key"),
        "both flags must be rejected, got: {}/{}",
        run.stderr,
        run.stdout
    );

    let run = harness
        .run(&["subscribe", "add", "--server", "http://127.0.0.1:9"])
        .await;
    assert!(
        !run.success && run.stderr.contains("--device-key"),
        "neither flag must be rejected, got: {}/{}",
        run.stderr,
        run.stdout
    );
}

#[tokio::test]
async fn a_failed_add_against_a_dead_server_is_an_actionable_error() {
    let harness = Harness::new();
    // Port 1 on localhost: nothing listens there, so transport fails.
    let dead = "http://127.0.0.1:1";

    let run = harness
        .run(&[
            "--plain-secrets",
            "subscribe",
            "add",
            "--server",
            dead,
            "--own",
            "--label",
            "ghost",
        ])
        .await;

    assert!(
        !run.success,
        "a dead server must fail the add; stdout: {}",
        run.stdout
    );
    assert!(
        run.stderr.contains(dead),
        "the error must name the server, got: {}",
        run.stderr
    );
    assert!(
        !run.stderr.contains("panicked"),
        "never a panic, got: {}",
        run.stderr
    );
}

#[tokio::test]
async fn removing_an_unknown_id_fails_actionably() {
    let harness = Harness::new();

    let run = harness
        .run(&["--plain-secrets", "subscribe", "rm", "deadbeef00000000"])
        .await;

    assert!(!run.success, "removing nothing must fail");
    assert!(
        run.stderr.contains("deadbeef00000000") && run.stderr.contains("subscribe list"),
        "the error must name the id and point at list, got: {}",
        run.stderr
    );
}

#[tokio::test]
async fn a_malformed_config_file_is_an_actionable_error_not_a_panic() {
    let harness = Harness::new();
    let paths = harness.paths();
    std::fs::create_dir_all(&paths.config_dir).expect("config dir");
    std::fs::write(paths.config_file(), "this is not [ toml").expect("write garbage");

    let run = harness.run(&["--plain-secrets", "subscribe", "list"]).await;

    assert!(!run.success, "garbage config must fail the command");
    assert!(
        run.stderr.contains(paths.config_file().to_str().unwrap()),
        "the error must name the file, got: {}",
        run.stderr
    );
    assert!(
        !run.stderr.contains("panicked"),
        "never a panic, got: {}",
        run.stderr
    );
}

#[tokio::test]
async fn without_plain_secrets_on_a_headless_machine_the_error_teaches_the_fallback() {
    if secret_service_reachable() {
        eprintln!("skipping: a secret service is answering, keyring mode would succeed");
        return;
    }
    // The server part must succeed so the failure lands in the secret
    // store, not in transport: the store is where the hint lives.
    let server = common::TestServer::spawn().await;
    let harness = Harness::new();

    let run = harness
        .run(&[
            "subscribe",
            "add",
            "--server",
            &server.base_url,
            "--own",
            "--label",
            "headless",
        ])
        .await;

    assert!(!run.success, "no secret service, no keyring add");
    assert!(
        run.stderr.contains("--plain-secrets"),
        "the error must teach the opt-in fallback, got: {}",
        run.stderr
    );

    server.shutdown().await;
}
