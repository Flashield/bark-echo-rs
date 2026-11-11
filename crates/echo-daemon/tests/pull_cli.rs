//! The `pull` subcommand (ticket #7): pushes sent while no daemon was
//! running are recovered from the command line — one-shot replay landed
//! silently in the Subscription's History Log, the ingested count
//! reported. Driven against the embedded real server through the CLI's
//! own config/secrets resolution (temp `HOME`, plain-secrets mode).

mod common;

use common::Harness;
use echo_daemon::config::Config;
use echo_daemon::secrets::{PlainFileStore, SecretKey, SecretKind, SecretStore};

/// Push one V2 JSON Push carrying `body` (the daemon is not running —
/// the Push piles up in the server's store-and-forward store), with
/// operator Basic Auth credentials when the server demands them (§1.4).
async fn push(
    server: &common::TestServer,
    device_key: &str,
    body: &str,
    auth: Option<(&str, &str)>,
) {
    let mut request = reqwest::Client::new()
        .post(format!("{}/push", server.base_url))
        .json(&serde_json::json!({ "device_key": device_key, "body": body }));
    if let Some((user, password)) = auth {
        request = request.basic_auth(user, Some(password));
    }
    let response = request.send().await.expect("POST /push");
    let envelope: serde_json::Value = response.json().await.expect("push envelope");
    assert_eq!(envelope["code"], 200, "push accepted: {envelope}");
}

#[tokio::test]
async fn pull_after_offline_pushes_lands_the_history_log_and_reports_the_count() {
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
            "laptop",
        ])
        .await
        .stdout_or_die("subscribe add");
    let device_key = stdout
        .lines()
        .find_map(|line| line.strip_prefix("device key: "))
        .expect("add prints the Device Key")
        .to_owned();

    // No daemon running: the Pushes are retained for replay (§3.7).
    for body in ["first", "second", "third"] {
        push(&server, &device_key, body, None).await;
    }

    // One pull reports the ingested count.
    let run = harness.run(&["--plain-secrets", "pull"]).await;
    assert!(
        run.success && run.stdout.contains("3"),
        "pull reports three ingested, got: {}/{}",
        run.stderr,
        run.stdout
    );

    // The Subscription's History Log holds the three, in id order.
    let paths = harness.paths();
    let config = Config::load(&paths.config_file()).expect("config loads");
    let subscription = &config.subscriptions[0];
    let log = echo_core::history::HistoryLog::new(paths.history_log(&subscription.id));
    let entries = log.entries().expect("history entries");
    assert_eq!(
        entries
            .iter()
            .map(|entry| entry.body.as_str())
            .collect::<Vec<_>>(),
        vec!["first", "second", "third"],
        "the History Log reads in id order"
    );

    // A second pull adds nothing — the dedup path is shared with the
    // live loop (§3.7).
    let run = harness.run(&["--plain-secrets", "pull"]).await;
    assert!(
        run.success && run.stdout.contains("0"),
        "the second pull reports zero, got: {}/{}",
        run.stderr,
        run.stdout
    );
    assert_eq!(
        log.entries().expect("entries").len(),
        3,
        "no duplicates landed"
    );

    server.shutdown().await;
}

#[tokio::test]
async fn pull_sends_the_basic_auth_credentials_configured_for_the_server() {
    // Behind operator Basic Auth (protocol §1.4) the CLI cannot subscribe
    // by itself (minting demands the credentials), so the Subscription is
    // laid out the way an operator would: config + secrets written by the
    // setup tooling — the pull then rides the config's credentials.
    let server = common::TestServer::spawn_with_basic_auth("operator", "hunter2").await;
    let harness = Harness::new();
    let paths = harness.paths();
    std::fs::create_dir_all(&paths.config_dir).expect("config dir");

    // /register is Basic-Auth-exempt (§1.4); minting is not.
    let envelope: serde_json::Value = reqwest::Client::new()
        .post(format!("{}/register", server.base_url))
        .json(&serde_json::json!({ "device_type": "stream" }))
        .send()
        .await
        .expect("register")
        .json()
        .await
        .expect("register envelope");
    let device_key = envelope["data"]["device_key"]
        .as_str()
        .expect("key")
        .to_owned();
    let minted: serde_json::Value = reqwest::Client::new()
        .post(format!(
            "{}/realtime/v1/devices/{}/streams",
            server.base_url, device_key
        ))
        .header("X-Device-Key", &device_key)
        .basic_auth("operator", Some("hunter2"))
        .json(&serde_json::json!({ "client_label": "laptop" }))
        .send()
        .await
        .expect("mint")
        .json()
        .await
        .expect("mint envelope");
    let token = minted["data"]["token"].as_str().expect("token").to_owned();
    let token_id = minted["data"]["token_id"]
        .as_str()
        .expect("token id")
        .to_owned();

    let config = Config {
        subscriptions: vec![echo_daemon::config::Subscription {
            id: token_id.clone(),
            server: server.base_url.clone(),
            label: "laptop".to_owned(),
            token_hint: format!("{}...", &token[..12]),
            device_hint: format!("{}...", &device_key[..8]),
        }],
        servers: vec![echo_daemon::config::ServerEntry {
            url: server.base_url.clone(),
            basic_auth_user: "operator".to_owned(),
            basic_auth_password: "hunter2".to_owned(),
        }],
    };
    config.save(&paths.config_file()).expect("config saved");
    let store = PlainFileStore::new(paths.secrets_file());
    store
        .set(
            &SecretKey {
                server: server.base_url.clone(),
                token_id,
            },
            SecretKind::StreamToken,
            &token,
        )
        .expect("token stored");

    push(
        &server,
        &device_key,
        "behind auth",
        Some(("operator", "hunter2")),
    )
    .await;

    let run = harness.run(&["--plain-secrets", "pull"]).await;
    assert!(
        run.success && run.stdout.contains("1"),
        "the pull rides the config's Basic Auth credentials, got: {}/{}",
        run.stderr,
        run.stdout
    );

    server.shutdown().await;
}

#[tokio::test]
async fn pull_resolves_the_sole_subscription_and_demands_a_label_when_ambiguous() {
    let server = common::TestServer::spawn().await;
    let harness = Harness::new();

    // Nothing held: actionable, pointing at subscribe add.
    let run = harness.run(&["--plain-secrets", "pull"]).await;
    assert!(
        !run.success && run.stderr.contains("subscribe add"),
        "no subscriptions names the way out, got: {}",
        run.stderr
    );

    let stdout = harness
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
        .await
        .stdout_or_die("subscribe add");
    let device_key = stdout
        .lines()
        .find_map(|line| line.strip_prefix("device key: "))
        .expect("device key")
        .to_owned();
    push(&server, &device_key, "anyone?", None).await;

    // An unknown label is actionable, pointing at subscribe list.
    let run = harness
        .run(&["--plain-secrets", "pull", "--sub", "ghost"])
        .await;
    assert!(
        !run.success && run.stderr.contains("ghost") && run.stderr.contains("subscribe list"),
        "unknown label names itself and the way out, got: {}",
        run.stderr
    );

    // With one Subscription held, the bare pull resolves it.
    let run = harness.run(&["--plain-secrets", "pull"]).await;
    assert!(
        run.success && run.stdout.contains("1"),
        "the sole Subscription is addressed without --sub, got: {}/{}",
        run.stderr,
        run.stdout
    );

    // With two held, the bare pull refuses and names --sub.
    harness
        .run(&[
            "--plain-secrets",
            "subscribe",
            "add",
            "--server",
            &server.base_url,
            "--device-key",
            &device_key,
            "--label",
            "desk",
        ])
        .await
        .stdout_or_die("second subscribe add");
    let run = harness.run(&["--plain-secrets", "pull"]).await;
    assert!(
        !run.success && run.stderr.contains("--sub"),
        "several Subscriptions demand --sub, got: {}",
        run.stderr
    );

    server.shutdown().await;
}
