//! The CLI seam for the management credential (issue #31, spec story
//! 21): the real binary in a temp HOME, driving `management token` and
//! `management rotate` as subprocesses against a real daemon — the
//! rotation that rides the channel kills the running face's sessions
//! (issue #24's kill switch), and the direct write covers the
//! no-daemon path.

mod common;

use std::time::{Duration, Instant};

use echo_daemon::config::Config;
use reqwest::header;

/// A free 127.0.0.1 port, picked by binding and dropping a listener.
fn pick_free_port() -> u16 {
    std::net::TcpListener::bind("127.0.0.1:0")
        .expect("pick a port")
        .local_addr()
        .expect("the picked port")
        .port()
}

/// Wait until `management token` answers (the daemon mints the token at
/// startup) and return the printed token.
async fn wait_token(harness: &common::Harness) -> String {
    let deadline = Instant::now() + Duration::from_secs(15);
    loop {
        let run = harness
            .run(&["--plain-secrets", "management", "token"])
            .await;
        if run.success {
            let token = run.stdout_or_die("management token").trim().to_owned();
            assert_eq!(
                token.len(),
                64,
                "the token is 32 random bytes hex-encoded: {token}"
            );
            assert!(
                token.chars().all(|c| c.is_ascii_hexdigit()),
                "hex only: {token}"
            );
            return token;
        }
        assert!(
            Instant::now() < deadline,
            "the daemon never minted a management token: {}",
            run.stderr
        );
        tokio::time::sleep(Duration::from_millis(50)).await;
    }
}

/// `POST /v1/session` with `token`, riding the page's own custom header
/// (the session exchange is a state change: the CSRF gate demands one);
/// returns the cookie name=value on success.
async fn sign_in(base_url: &str, token: &str) -> String {
    let response = reqwest::Client::new()
        .post(format!("{base_url}/v1/session"))
        .header("X-Requested-With", "bark-echo-face")
        .json(&serde_json::json!({ "token": token }))
        .send()
        .await
        .expect("POST /v1/session");
    assert_eq!(
        response.status(),
        reqwest::StatusCode::NO_CONTENT,
        "the token exchanges for a session"
    );
    let set_cookie = response
        .headers()
        .get(header::SET_COOKIE)
        .expect("Set-Cookie")
        .to_str()
        .expect("ascii")
        .to_owned();
    set_cookie.split(';').next().expect("a cookie").to_owned()
}

#[tokio::test]
async fn management_token_prints_what_the_daemon_minted() {
    // No daemon has ever run: there is no token yet, and the error says
    // how to mint one.
    let harness = common::Harness::new();
    let run = harness
        .run(&["--plain-secrets", "management", "token"])
        .await;
    assert!(
        !run.success,
        "no token exists before the first daemon start"
    );
    assert!(
        run.stderr.contains("start the daemon"),
        "the error is actionable: {}",
        run.stderr
    );

    // The daemon's first start with the face mints it (issue #24), and
    // `management token` prints it — no daemon needed for the read.
    let mut daemon = harness
        .spawn(&["--plain-secrets", "run"])
        .spawn()
        .expect("spawn run");
    let token = wait_token(&harness).await;
    daemon.kill().await.expect("kill the daemon");

    // The minted token survives the daemon: the store holds it.
    let run = harness
        .run(&["--plain-secrets", "management", "token"])
        .await;
    assert_eq!(
        run.stdout_or_die("management token").trim(),
        token,
        "the same token, re-printed"
    );
}

#[tokio::test]
async fn management_rotate_without_a_daemon_changes_the_store() {
    let harness = common::Harness::new();
    // Mint one first: the CLI refuses to rotate nothing? No — rotate
    // replaces whatever is stored, minting if absent (the store write
    // is the whole operation when no daemon is running).
    let mut daemon = harness
        .spawn(&["--plain-secrets", "run"])
        .spawn()
        .expect("spawn run");
    let first = wait_token(&harness).await;
    daemon.kill().await.expect("kill the daemon");

    let run = harness
        .run(&["--plain-secrets", "management", "rotate"])
        .await;
    run.stdout_or_die("management rotate");
    let second = harness
        .run(&["--plain-secrets", "management", "token"])
        .await
        .stdout_or_die("management token");
    let second = second.trim();
    assert_ne!(
        first, second,
        "the rotation replaced the token: {first} -> {second}"
    );
}

#[tokio::test]
async fn management_rotate_through_a_running_daemon_kills_its_sessions() {
    // The daemon runs as a subprocess with the face on a port the test
    // picked: the CLI's rotation rides the channel (the daemon is the
    // single writer), and the running face drops every session issued
    // under the old token at once (issue #24's kill switch).
    let port = pick_free_port();
    let harness = common::Harness::new();
    let mut config = Config::load(&harness.paths().config_file()).expect("config loads");
    config.management.bind = format!("127.0.0.1:{port}");
    config
        .save(&harness.paths().config_file())
        .expect("config saved");
    let mut daemon = harness
        .spawn(&["--plain-secrets", "run"])
        .spawn()
        .expect("spawn run");
    let base_url = format!("http://127.0.0.1:{port}");

    let first = wait_token(&harness).await;
    let cookie = sign_in(&base_url, &first).await;

    let client = reqwest::Client::new();
    let cookie_header = cookie;

    // The old token and its session both work before the rotation.
    let response = client
        .get(format!("{base_url}/v1/status"))
        .header("X-Management-Token", &first)
        .send()
        .await
        .expect("status with the old token");
    assert_eq!(response.status(), reqwest::StatusCode::OK);
    let response = client
        .get(format!("{base_url}/v1/status"))
        .header(header::COOKIE, &cookie_header)
        .send()
        .await
        .expect("status with the old session");
    assert_eq!(response.status(), reqwest::StatusCode::OK);

    // The rotation rides the channel: the daemon stores the fresh token
    // and invalidates every session issued under the old one.
    let run = harness
        .run(&["--plain-secrets", "management", "rotate"])
        .await;
    run.stdout_or_die("management rotate");

    let response = client
        .get(format!("{base_url}/v1/status"))
        .header("X-Management-Token", &first)
        .send()
        .await
        .expect("status with the old token");
    assert_eq!(
        response.status(),
        reqwest::StatusCode::UNAUTHORIZED,
        "the old token is dead at once"
    );
    let response = client
        .get(format!("{base_url}/v1/status"))
        .header(header::COOKIE, &cookie_header)
        .send()
        .await
        .expect("status with the old session");
    assert_eq!(
        response.status(),
        reqwest::StatusCode::UNAUTHORIZED,
        "the old session is dead at once"
    );

    // The new token (re-printed by the CLI) works, and exchanges for a
    // fresh session.
    let second = wait_token(&harness).await;
    assert_ne!(first, second, "the rotation replaced the token");
    let response = client
        .get(format!("{base_url}/v1/status"))
        .header("X-Management-Token", &second)
        .send()
        .await
        .expect("status with the new token");
    assert_eq!(response.status(), reqwest::StatusCode::OK);
    sign_in(&base_url, &second).await;

    daemon.kill().await.expect("kill the daemon");
}
