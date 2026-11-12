//! The Device/Token plane against the embedded real server: registering
//! Stream Devices (protocol §3.1) and the Stream Token lifecycle (§3.2).

mod common;

use echo_core::server::{ServerClient, ServerError};

/// A stream connect attempt with a Stream Token secret (§3.3): the
/// status alone — reading the stream itself is a later ticket's client.
async fn stream_connect_status(server: &common::TestServer, secret: &str) -> reqwest::StatusCode {
    reqwest::Client::new()
        .get(format!("{}/realtime/v1/stream", server.base_url))
        .header("X-Stream-Token", secret)
        .send()
        .await
        .expect("connect attempt")
        .status()
}

#[tokio::test]
async fn registering_a_stream_device_returns_a_fresh_device_key() {
    let server = common::TestServer::spawn().await;
    let client = ServerClient::new(&server.base_url);

    let device = client.register_stream_device().await.expect("register");

    // A Device Key is a 22-char identifier (protocol §3.2's header note).
    assert_eq!(device.device_key.len(), 22, "device key shape");

    server.shutdown().await;
}

#[tokio::test]
async fn registering_with_an_existing_device_key_refreshes_in_place() {
    let server = common::TestServer::spawn().await;
    let client = ServerClient::new(&server.base_url);

    let device = client.register_stream_device().await.expect("register");
    let refreshed = client
        .refresh_stream_device(&device.device_key)
        .await
        .expect("refresh");

    assert_eq!(
        refreshed.device_key, device.device_key,
        "same-key registration refreshes in place, minting no new key"
    );

    server.shutdown().await;
}

#[tokio::test]
async fn minting_a_stream_token_returns_the_full_secret_with_id_and_hint() {
    let server = common::TestServer::spawn().await;
    let client = ServerClient::new(&server.base_url);
    let device = client.register_stream_device().await.expect("register");

    let minted = client
        .mint_stream_token(&device.device_key, "macbook-reminder")
        .await
        .expect("mint");

    // Stream Token format (protocol §3.2): `st_` + 43 unpadded base64url.
    assert!(minted.secret.starts_with("st_"), "recognition prefix");
    assert_eq!(minted.secret.len(), "st_".len() + 43, "256-bit secret");
    // token_id: 16 lowercase hex chars — the public handle.
    assert_eq!(minted.token_id.len(), 16, "token_id shape");
    assert!(
        minted
            .token_id
            .chars()
            .all(|c| c.is_ascii_digit() || ('a'..='f').contains(&c)),
        "token_id is lowercase hex"
    );
    // token_hint: first 12 characters of the secret + "...".
    assert_eq!(
        minted.token_hint,
        format!("{}...", &minted.secret[..12]),
        "hint matches the secret's first 12 chars"
    );
    assert_eq!(minted.client_label, "macbook-reminder");

    server.shutdown().await;
}

#[tokio::test]
async fn listing_stream_tokens_returns_creation_order_summaries_with_hints() {
    let server = common::TestServer::spawn().await;
    let client = ServerClient::new(&server.base_url);
    let device = client.register_stream_device().await.expect("register");
    let first = client
        .mint_stream_token(&device.device_key, "macbook-reminder")
        .await
        .expect("mint");
    let second = client
        .mint_stream_token(&device.device_key, "android-phone")
        .await
        .expect("mint");

    let tokens = client
        .list_stream_tokens(&device.device_key)
        .await
        .expect("list");

    assert_eq!(tokens.len(), 2, "one entry per minted token");
    // Creation order (protocol §3.2).
    assert_eq!(tokens[0].token_id, first.token_id);
    assert_eq!(tokens[1].token_id, second.token_id);
    // Summaries carry label and hint matching mint time — the summary has
    // no secret field: the full secret left the server exactly once.
    assert_eq!(tokens[0].client_label, "macbook-reminder");
    assert_eq!(tokens[0].token_hint, first.token_hint);
    assert_eq!(tokens[1].client_label, "android-phone");
    assert_eq!(tokens[1].token_hint, second.token_hint);
    assert!(!tokens[0].revoked, "freshly minted token is live");

    server.shutdown().await;
}

#[tokio::test]
async fn revoking_a_stream_token_kills_connects_and_double_revoke_answers_409() {
    let server = common::TestServer::spawn().await;
    let client = ServerClient::new(&server.base_url);
    let device = client.register_stream_device().await.expect("register");
    let minted = client
        .mint_stream_token(&device.device_key, "macbook-reminder")
        .await
        .expect("mint");

    // The live secret may hold a Realtime Stream (protocol §3.3).
    assert_eq!(
        stream_connect_status(&server, &minted.secret).await,
        reqwest::StatusCode::OK
    );

    let revoked = client
        .revoke_stream_token(&device.device_key, &minted.token_id)
        .await
        .expect("revoke");
    assert!(revoked.revoked, "revocation takes effect immediately");
    assert_eq!(revoked.token_id, minted.token_id);
    assert_eq!(revoked.token_hint, minted.token_hint);

    // The same secret's subsequent connect attempts fail auth (§3.2).
    assert_eq!(
        stream_connect_status(&server, &minted.secret).await,
        reqwest::StatusCode::UNAUTHORIZED
    );

    // Revoking an already-revoked token is refused with 409 (§3.2).
    let err = client
        .revoke_stream_token(&device.device_key, &minted.token_id)
        .await
        .expect_err("double revoke must fail");
    assert!(
        matches!(err, ServerError::StreamTokenAlreadyRevoked),
        "wrong error: {err}"
    );

    server.shutdown().await;
}

#[tokio::test]
async fn operating_on_an_unknown_device_answers_device_not_found() {
    let server = common::TestServer::spawn().await;
    let client = ServerClient::new(&server.base_url);
    // A never-registered 22-char key: auth passes (header == path) but
    // the Device does not exist — 404 without revealing later facts (§3.2).
    let unknown_key = "zzzzzzzzzzzzzzzzzzzzzz";

    let err = client
        .mint_stream_token(unknown_key, "macbook-reminder")
        .await
        .expect_err("mint against unknown device");
    assert!(
        matches!(err, ServerError::DeviceNotFound),
        "mint: wrong error: {err}"
    );

    let err = client
        .list_stream_tokens(unknown_key)
        .await
        .expect_err("list against unknown device");
    assert!(
        matches!(err, ServerError::DeviceNotFound),
        "list: wrong error: {err}"
    );

    server.shutdown().await;
}

#[tokio::test]
async fn revoking_a_stream_token_of_another_device_answers_not_found() {
    let server = common::TestServer::spawn().await;
    let client = ServerClient::new(&server.base_url);
    let home = client.register_stream_device().await.expect("register");
    let other = client.register_stream_device().await.expect("register");
    let minted = client
        .mint_stream_token(&home.device_key, "macbook-reminder")
        .await
        .expect("mint");

    // Token ids are Device-scoped (§3.2): presenting another Device's id
    // answers like an unknown id — no leak.
    let err = client
        .revoke_stream_token(&other.device_key, &minted.token_id)
        .await
        .expect_err("cross-device revoke");
    assert!(
        matches!(err, ServerError::StreamTokenNotFound),
        "cross-device: wrong error: {err}"
    );

    let err = client
        .revoke_stream_token(&home.device_key, "0000000000000000")
        .await
        .expect_err("unknown token id");
    assert!(
        matches!(err, ServerError::StreamTokenNotFound),
        "unknown id: wrong error: {err}"
    );

    server.shutdown().await;
}
