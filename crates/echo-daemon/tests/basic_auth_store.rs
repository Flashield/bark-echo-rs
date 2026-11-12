//! The operator Basic Auth password's home (spec story 18): the secret
//! store — the OS keyring in normal mode, the 0600 plain file in
//! `--plain-secrets` mode — with config.toml carrying only the user and a
//! stored-password marker. Every connection (`push`, `pull`, `token`, the
//! daemon's streams, `status`'s /info observation) rides the stored
//! credentials when the flags are silent; the flags still win.

mod common;

use common::Harness;
use echo_daemon::secrets::{
    KeyringStore, PlainFileStore, SecretError, SecretKey, SecretKind, SecretStore,
};

/// Add one Subscription to a Basic-Auth-protected server, laying the
/// operator credentials in with `--auth-user`/`--auth-password`, and
/// return the Device Key.
async fn subscribe_own_with_auth(harness: &Harness, server: &str) -> String {
    let output = harness
        .run(&[
            "--plain-secrets",
            "subscribe",
            "add",
            "--server",
            server,
            "--own",
            "--label",
            "laptop",
            "--auth-user",
            "operator",
            "--auth-password",
            "hunter2",
        ])
        .await;
    output
        .stdout_or_die("subscribe add behind Basic Auth")
        .lines()
        .find_map(|line| line.strip_prefix("device key: "))
        .expect("the device key is printed first")
        .trim()
        .to_owned()
}

#[tokio::test]
async fn the_password_lives_in_the_store_and_config_toml_stays_secret_free() {
    let server = common::TestServer::spawn_with_basic_auth("operator", "hunter2").await;
    let harness = Harness::new();

    let device_key = subscribe_own_with_auth(&harness, &server.base_url).await;

    // config.toml carries the user and the stored-password marker — and
    // never the password itself. "No secrets here" becomes true.
    let paths = harness.paths();
    let config_raw = std::fs::read_to_string(paths.config_file()).expect("config file");
    assert!(
        config_raw.contains("basic_auth_user = \"operator\""),
        "the user is in the config: {config_raw}"
    );
    assert!(
        config_raw.contains("basic_auth_password_stored = true"),
        "the marker is in the config: {config_raw}"
    );
    assert!(
        !config_raw.contains("hunter2"),
        "the password never lands in config.toml: {config_raw}"
    );

    // The password is in the store, under the server's key.
    let store = PlainFileStore::new(paths.secrets_file());
    assert_eq!(
        store
            .get(
                &SecretKey::for_server(&server.base_url),
                SecretKind::BasicAuthPassword
            )
            .expect("stored password"),
        "hunter2"
    );

    // `push` rides the stored credentials with no flags at all...
    let run = harness
        .run(&[
            "--plain-secrets",
            "push",
            "--server",
            &server.base_url,
            "--device-key",
            &device_key,
            "--title",
            "t",
            "--body",
            "stored creds",
        ])
        .await;
    assert!(
        run.success,
        "push without flags rides the stored credentials: {}",
        run.stderr
    );

    // ...while explicit flags override the store.
    let run = harness
        .run(&[
            "--plain-secrets",
            "push",
            "--server",
            &server.base_url,
            "--device-key",
            &device_key,
            "--body",
            "override",
            "--user",
            "operator",
            "--password",
            "wrong",
        ])
        .await;
    assert!(
        !run.success,
        "a wrong overriding password must fail: {}",
        run.stdout
    );

    server.shutdown().await;
}

#[tokio::test]
async fn add_without_auth_flags_sets_no_credentials_and_minting_refuses() {
    // A protected server demands the operator credentials for minting
    // (§1.4): without the flags the add fails — and lays nothing down.
    let server = common::TestServer::spawn_with_basic_auth("operator", "hunter2").await;
    let harness = Harness::new();

    let run = harness
        .run(&[
            "--plain-secrets",
            "subscribe",
            "add",
            "--server",
            &server.base_url,
            "--own",
            "--label",
            "laptop",
        ])
        .await;
    assert!(
        !run.success,
        "minting behind Basic Auth without credentials must fail: {}",
        run.stdout
    );

    // No flags, no credentials: no marker row, no stored password.
    let paths = harness.paths();
    let config = echo_daemon::config::Config::load(&paths.config_file()).expect("config loads");
    assert!(
        config.servers.is_empty(),
        "no server entry without credentials: {:?}",
        config.servers
    );
    let store = PlainFileStore::new(paths.secrets_file());
    assert!(
        store
            .get(
                &SecretKey::for_server(&server.base_url),
                SecretKind::BasicAuthPassword
            )
            .is_err(),
        "no password is stored"
    );

    server.shutdown().await;
}

/// A scratch keyring entry proves a secret service answers this session
/// (the CI box has none — the plain-file tests above carry those runs).
fn keyring_reachable() -> bool {
    let probe = format!(
        "bark-echo-auth-probe-{}-{}",
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
async fn in_keyring_mode_the_password_lands_in_the_keyring() {
    if !keyring_reachable() {
        eprintln!("skipping: no secret service on this machine");
        return;
    }

    let server = common::TestServer::spawn_with_basic_auth("operator", "hunter2").await;
    let harness = Harness::new();

    // Keyring mode: no --plain-secrets flag.
    harness
        .run(&[
            "subscribe",
            "add",
            "--server",
            &server.base_url,
            "--own",
            "--label",
            "laptop",
            "--auth-user",
            "operator",
            "--auth-password",
            "hunter2",
        ])
        .await
        .stdout_or_die("subscribe add in keyring mode");

    let store = KeyringStore::new();
    assert_eq!(
        store
            .get(
                &SecretKey::for_server(&server.base_url),
                SecretKind::BasicAuthPassword
            )
            .expect("password in the keyring"),
        "hunter2",
        "the password is keyed by server, in the keyring"
    );

    server.shutdown().await;
}

#[tokio::test]
async fn the_daemons_stream_and_status_ride_the_stored_credentials() {
    let server = common::TestServer::spawn_with_basic_auth("operator", "hunter2").await;
    let harness = Harness::new();
    subscribe_own_with_auth(&harness, &server.base_url).await;

    // The daemon's Realtime Stream connect presents the stored password:
    // the only observable is the server's own active-streams count —
    // /info demands the operator credentials here, so the poll carries
    // them too (§1.4: behind Basic Auth every observation needs them).
    let mut daemon = harness
        .spawn(&["--plain-secrets", "run"])
        .spawn()
        .expect("spawn run");
    let deadline = std::time::Instant::now() + std::time::Duration::from_secs(10);
    loop {
        let info: serde_json::Value = reqwest::Client::new()
            .get(format!("{}/info", server.base_url))
            .basic_auth("operator", Some("hunter2"))
            .send()
            .await
            .expect("GET /info")
            .json()
            .await
            .expect("/info json");
        if info["active_streams"] == 1 {
            break;
        }
        assert!(
            std::time::Instant::now() < deadline,
            "the stream never connected on the stored credentials: {info}"
        );
        tokio::time::sleep(std::time::Duration::from_millis(20)).await;
    }
    daemon.kill().await.expect("kill daemon");
    let _ = daemon.wait().await;

    // `status`'s /info observation rides the stored credentials too.
    let run = harness.run(&["--plain-secrets", "status"]).await;
    assert!(
        run.success && run.stdout.contains("server: active_streams"),
        "status rides the stored credentials for /info, got: {}/{}",
        run.stderr,
        run.stdout
    );

    server.shutdown().await;
}

/// A store that must never be touched: for the flag-driven resolution
/// the seam has no business probing (or writing) anything.
struct NeverProbed;

impl SecretStore for NeverProbed {
    fn set(&self, _: &SecretKey, _: SecretKind, _: &str) -> Result<(), SecretError> {
        panic!("the flags carry the whole credential; the store must not be written");
    }

    fn get(&self, _: &SecretKey, _: SecretKind) -> Result<String, SecretError> {
        panic!("the flags carry the whole credential; the store must not be probed");
    }

    fn delete(&self, _: &SecretKey, _: SecretKind) -> Result<(), SecretError> {
        panic!("the flags carry the whole credential; the store must not be touched");
    }
}

/// A store that cannot answer any read — the headless shape (no secret
/// service behind the keyring facade).
struct UnreachableStore;

impl SecretStore for UnreachableStore {
    fn set(&self, _: &SecretKey, _: SecretKind, _: &str) -> Result<(), SecretError> {
        Err(SecretError::Unavailable("no secret service".to_owned()))
    }

    fn get(&self, _: &SecretKey, _: SecretKind) -> Result<String, SecretError> {
        Err(SecretError::Unavailable("no secret service".to_owned()))
    }

    fn delete(&self, _: &SecretKey, _: SecretKind) -> Result<(), SecretError> {
        Err(SecretError::Unavailable("no secret service".to_owned()))
    }
}

#[test]
fn the_flags_carrying_the_whole_credential_silence_the_store() {
    // The resolution seam's first rule: explicit flags win field by
    // field — and when they carry both fields, the store is never
    // probed. An unreachable secret service must not stand between the
    // operator's typed credentials and the connection.
    let auth = echo_daemon::auth::resolve(
        &echo_daemon::config::Config::default(),
        &NeverProbed,
        "http://127.0.0.1:8080",
        Some("operator"),
        Some("hunter2"),
    )
    .expect("the flags resolve without touching the store");
    let auth = auth.expect("the resolved credential");
    assert_eq!(auth.user, "operator");
    assert_eq!(auth.password, "hunter2");
}

#[test]
fn an_unavailable_store_is_the_actionable_error_not_silence() {
    // The credential-required seams (the daemon's streams, the token
    // plane, `pull`, `status`) must not silently drop the stored
    // credentials when the store cannot answer: the error names the
    // store and points at the plain-file fallback. (`push` is the one
    // seam allowed to degrade — its own test pins that.)
    let err = echo_daemon::auth::resolve(
        &echo_daemon::config::Config::default(),
        &UnreachableStore,
        "http://127.0.0.1:8080",
        None,
        None,
    )
    .expect_err("an unreachable store surfaces");
    let text = err.to_string();
    assert!(text.contains("secret store unavailable"), "{text}");
    assert!(text.contains("--plain-secrets"), "{text}");
}
