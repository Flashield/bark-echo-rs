//! The seam itself: prove the revision-pinned bark-server-rs runs
//! in-process and answers plain HTTP before any echo-core face is tested
//! against it.

mod common;

#[tokio::test]
async fn embedded_bark_server_answers_ping_over_http() {
    let server = common::TestServer::spawn().await;

    let resp = reqwest::Client::new()
        .get(format!("{}/ping", server.base_url))
        .send()
        .await
        .expect("GET /ping");
    assert!(resp.status().is_success(), "ping failed: {resp:?}");

    server.shutdown().await;
}
