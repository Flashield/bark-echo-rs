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
