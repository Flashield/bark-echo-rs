//! The Device/Token plane against the embedded real server: registering
//! Stream Devices (protocol §3.1) and the Stream Token lifecycle (§3.2).

mod common;

use echo_core::server::ServerClient;

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
