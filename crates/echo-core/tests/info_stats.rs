//! The `/info` face of the server client (protocol §2.3): the live
//! statistics `status` surfaces per Subscription — `active_streams` and
//! `pending_messages` — plus the Basic Auth demand behind a protected
//! server (§1.4: every observation needs the operator credentials).

mod common;

use echo_core::server::ServerClient;

#[tokio::test]
async fn info_reports_the_live_statistics_the_status_face_needs() {
    let server = common::TestServer::spawn().await;
    let client = ServerClient::new(&server.base_url);

    // Fresh server: no streams open, nothing retained.
    let info = client.info().await.expect("info");
    assert_eq!(info.active_streams, 0);
    assert_eq!(info.pending_messages, 0);
    assert!(!info.version.is_empty(), "build identification rides along");

    // A push with no stream open is retained for replay (§3.7): the
    // pending count moves.
    let device = client.register_stream_device().await.expect("register");
    let minted = client
        .mint_stream_token(&device.device_key, "echo-test-client")
        .await
        .expect("mint");
    let response = reqwest::Client::new()
        .post(format!("{}/push", server.base_url))
        .json(&serde_json::json!({ "device_key": device.device_key, "body": "retained" }))
        .send()
        .await
        .expect("POST /push");
    assert!(response.status().is_success());
    let info = client.info().await.expect("info after push");
    assert_eq!(info.pending_messages, 1);

    // An open Realtime Stream moves the active count — the external
    // observation `status` leans on.
    let config = echo_core::stream::StreamConfig {
        base_url: server.base_url.clone(),
        stream_token: minted.secret,
        basic_auth: None,
        history_log: echo_core::history::HistoryLog::new(
            tempfile::tempdir()
                .expect("temp dir")
                .path()
                .join("history.jsonl"),
        ),
        backoff: echo_core::stream::BackoffConfig::default(),
        events: None,
    };
    let (_handle, _intents) = echo_core::stream::RealtimeStream::spawn(config);
    let deadline = std::time::Instant::now() + std::time::Duration::from_secs(10);
    loop {
        let info = client.info().await.expect("info while connecting");
        if info.active_streams == 1 {
            break;
        }
        assert!(
            std::time::Instant::now() < deadline,
            "active_streams never reached 1"
        );
        tokio::time::sleep(std::time::Duration::from_millis(20)).await;
    }

    server.shutdown().await;
}

#[tokio::test]
async fn info_behind_basic_auth_demands_and_accepts_the_operator_credentials() {
    let server = common::TestServer::spawn_with(|config| {
        config.basic_auth_user = "operator".to_owned();
        config.basic_auth_password = "s3cret".to_owned();
    })
    .await;

    // Bare-handed observation is refused (§1.4: the 418 teapot).
    let bare = ServerClient::new(&server.base_url);
    let err = bare.info().await.expect_err("bare /info refused");
    assert!(
        err.to_string().contains("I'm a teapot"),
        "the 418 face names the refusal: {err}"
    );

    // The operator credentials ride along and the stats answer.
    let credentialed = ServerClient::new(&server.base_url).with_basic_auth("operator", "s3cret");
    let info = credentialed.info().await.expect("info with credentials");
    assert_eq!(info.active_streams, 0);

    server.shutdown().await;
}
