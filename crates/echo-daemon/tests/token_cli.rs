//! The `token` subcommands (protocol §3.2): list and revoke a Device's
//! Stream Tokens from the command line — the lifecycle face of the tokens
//! this CLI mints. Driven against the embedded real server through the
//! CLI's own config/secrets resolution (temp `HOME`, plain-secrets mode).

mod common;

use common::Harness;
use echo_core::server::ServerClient;

/// Add one Subscription to this machine (`--own`), asserting it took and
/// returning the Device Key (the push target `add` prints).
async fn subscribe_own(harness: &Harness, server: &str) -> String {
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
        ])
        .await;
    output
        .stdout_or_die("subscribe add")
        .lines()
        .find_map(|line| line.strip_prefix("device key: "))
        .expect("the device key is printed first")
        .trim()
        .to_owned()
}

/// The Subscription id this machine's `subscribe list` shows first.
async fn first_subscription_id(harness: &Harness) -> String {
    let stdout = harness
        .run(&["--plain-secrets", "subscribe", "list"])
        .await
        .stdout_or_die("subscribe list");
    stdout
        .lines()
        .next()
        .expect("a subscription is listed")
        .split_whitespace()
        .next()
        .expect("the id is the first column")
        .to_owned()
}

#[tokio::test]
async fn token_list_shows_every_token_a_device_holds_and_never_the_secret() {
    let server = common::TestServer::spawn().await;
    let harness = Harness::new();
    let device_key = subscribe_own(&harness, &server.base_url).await;

    // A second token on the same Device (another client's mirror), minted
    // outside the CLI — `token list` must show both.
    let client = ServerClient::new(&server.base_url);
    let second = client
        .mint_stream_token(&device_key, "desk")
        .await
        .expect("second mint");

    let stdout = harness
        .run(&[
            "--plain-secrets",
            "token",
            "list",
            "--server",
            &server.base_url,
            "--device-key",
            &device_key,
        ])
        .await
        .stdout_or_die("token list");

    assert!(
        stdout.contains(&second.token_id) && stdout.contains(&second.client_label),
        "both tokens are listed with their labels: {stdout}"
    );
    assert!(
        stdout.lines().count() >= 2,
        "two tokens are listed as two lines: {stdout}"
    );
    assert!(
        !stdout.contains(&second.secret),
        "the secret is structurally absent from the listing: {stdout}"
    );

    server.shutdown().await;
}

#[tokio::test]
async fn token_revocation_takes_effect_and_a_second_revocation_is_rejected() {
    let server = common::TestServer::spawn().await;
    let harness = Harness::new();
    let device_key = subscribe_own(&harness, &server.base_url).await;
    let token_id = first_subscription_id(&harness).await;

    // Revoking the token this machine holds (spec story 21's scenario,
    // driven from the token face): accepted once.
    let run = harness
        .run(&[
            "--plain-secrets",
            "token",
            "revoke",
            "--server",
            &server.base_url,
            "--device-key",
            &device_key,
            "--token-id",
            &token_id,
        ])
        .await;
    assert!(
        run.success && run.stdout.contains(&token_id),
        "revoke names the token it revoked, got: {}/{}",
        run.stderr,
        run.stdout
    );

    // The listing shows the revoked state — never a secret.
    let stdout = harness
        .run(&[
            "--plain-secrets",
            "token",
            "list",
            "--server",
            &server.base_url,
            "--device-key",
            &device_key,
        ])
        .await
        .stdout_or_die("token list after revoke");
    assert!(
        stdout.contains("revoked"),
        "the revoked state is visible: {stdout}"
    );

    // §3.2: revoking is deliberately not idempotent — a second revoke of
    // the same token is the typed already-revoked error.
    let run = harness
        .run(&[
            "--plain-secrets",
            "token",
            "revoke",
            "--server",
            &server.base_url,
            "--device-key",
            &device_key,
            "--token-id",
            &token_id,
        ])
        .await;
    assert!(
        !run.success && run.stderr.contains("stream token already revoked"),
        "the double revoke is the §3.2 typed error, got: {}",
        run.stderr
    );

    // An unknown token id and an unknown Device Key answer their typed
    // §3.2 errors too.
    let run = harness
        .run(&[
            "--plain-secrets",
            "token",
            "revoke",
            "--server",
            &server.base_url,
            "--device-key",
            &device_key,
            "--token-id",
            "0000000000000000",
        ])
        .await;
    assert!(
        !run.success && run.stderr.contains("stream token not found"),
        "an unknown token id is the §3.2 typed error, got: {}",
        run.stderr
    );
    let run = harness
        .run(&[
            "--plain-secrets",
            "token",
            "revoke",
            "--server",
            &server.base_url,
            "--device-key",
            "zzzzzzzzzzzzzzzzzzzzzz",
            "--token-id",
            &token_id,
        ])
        .await;
    assert!(
        !run.success && run.stderr.contains("device not found"),
        "an unknown device key is the §3.2 typed error, got: {}",
        run.stderr
    );

    server.shutdown().await;
}

#[tokio::test]
async fn token_list_behind_basic_auth_demands_and_accepts_the_operator_credentials() {
    let server = common::TestServer::spawn_with_basic_auth("operator", "hunter2").await;
    let harness = Harness::new();
    // /register is Basic-Auth-exempt (§1.4); the token plane is not.
    let device = ServerClient::new(&server.base_url)
        .register_stream_device()
        .await
        .expect("register stream device");

    // Without credentials the token plane refuses.
    let run = harness
        .run(&[
            "--plain-secrets",
            "token",
            "list",
            "--server",
            &server.base_url,
            "--device-key",
            &device.device_key,
        ])
        .await;
    assert!(
        !run.success && !run.stderr.is_empty(),
        "listing behind Basic Auth without credentials must fail, got: {}",
        run.stderr
    );

    // With the operator's credentials it answers.
    let run = harness
        .run(&[
            "--plain-secrets",
            "token",
            "list",
            "--server",
            &server.base_url,
            "--device-key",
            &device.device_key,
            "--user",
            "operator",
            "--password",
            "hunter2",
        ])
        .await;
    assert!(
        run.success && run.stdout.contains("no stream tokens"),
        "the empty listing answers behind Basic Auth, got: {}/{}",
        run.stderr,
        run.stdout
    );

    server.shutdown().await;
}
