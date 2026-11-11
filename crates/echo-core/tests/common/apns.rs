//! A local APNs stand-in: the server's configured `apns_endpoint` pointed
//! here records every `POST /3/device/{token}` — reduced from the server
//! repo's `tests/common/apns_stub.rs` to what the push tests assert (the
//! request path and JSON payload). No request ever leaves the machine.

// common/ is compiled into every test target; helpers are shared per
// target, not all used everywhere.
#![allow(dead_code)]

use std::sync::{Arc, Mutex};

use axum::Json;
use axum::Router;
use axum::extract::{Path, Request, State};
use axum::http::StatusCode;
use axum::routing::{get, post};
use serde_json::{Value, json};
use tokio::sync::oneshot;
use tokio::task::JoinHandle;

/// One recorded APNs push request: the wire facts a test can assert on.
#[derive(Debug, Clone)]
pub struct RecordedPush {
    /// Request path, e.g. `/3/device/PUSHTOKEN1`.
    pub path: String,
    /// The JSON payload body.
    pub payload: Value,
}

/// Handle over a spawned stub: its base URL, the recording buffer, and
/// the shutdown controls.
pub struct ApnsStub {
    /// `https://127.0.0.1:<port>` — feed this to the server config's
    /// `apns_endpoint`.
    pub url: String,
    requests: Arc<Mutex<Vec<RecordedPush>>>,
    shutdown: Option<oneshot::Sender<()>>,
    handle: JoinHandle<()>,
}

impl ApnsStub {
    /// Spawn a stub on an ephemeral port with a self-signed certificate.
    pub async fn spawn() -> ApnsStub {
        // rustls needs a process-level provider; installing ours is a
        // no-op when one is already installed.
        let _ = rustls::crypto::ring::default_provider().install_default();

        let requests: Arc<Mutex<Vec<RecordedPush>>> = Arc::new(Mutex::new(Vec::new()));
        let app = Router::new()
            .route("/3/device/{token}", post(apns_device_push))
            .route("/_health", get(|| async { "ok" }))
            .with_state(requests.clone());

        let certified_key = rcgen::generate_simple_self_signed(vec!["localhost".into()])
            .expect("generate self-signed stub certificate");
        let tls = axum_server::tls_rustls::RustlsConfig::from_pem(
            certified_key.cert.pem().into_bytes(),
            certified_key.signing_key.serialize_pem().into_bytes(),
        )
        .await
        .expect("build stub TLS config");

        // The listener is bound here and handed over — no port race.
        let listener = std::net::TcpListener::bind("127.0.0.1:0").expect("bind ephemeral port");
        let addr = listener.local_addr().expect("local addr");
        let url = format!("https://{addr}");

        let (tx, rx) = oneshot::channel::<()>();
        let axum_handle = axum_server::Handle::new();
        let watcher = axum_handle.clone();
        tokio::spawn(async move {
            let _ = rx.await;
            watcher.shutdown();
        });
        let handle = tokio::spawn(async move {
            axum_server::tls_rustls::from_tcp_rustls(listener, tls)
                .expect("stub takes over the bound listener")
                .handle(axum_handle)
                .serve(app.into_make_service())
                .await
                .expect("APNs stub server error");
        });

        // Readiness: a TLS round-trip against the health route; the
        // certificate is self-signed, so verification is disabled here.
        let probe = reqwest::Client::builder()
            .danger_accept_invalid_certs(true)
            .build()
            .expect("probe client");
        let deadline = tokio::time::Instant::now() + std::time::Duration::from_secs(15);
        loop {
            assert!(
                tokio::time::Instant::now() < deadline,
                "APNs stub at {url} did not become ready in time"
            );
            if let Ok(res) = probe.get(format!("{url}/_health")).send().await
                && res.status().is_success()
            {
                break;
            }
            tokio::time::sleep(std::time::Duration::from_millis(50)).await;
        }

        ApnsStub {
            url,
            requests,
            shutdown: Some(tx),
            handle,
        }
    }

    /// Snapshot of every request received so far, oldest first.
    pub fn pushes(&self) -> Vec<RecordedPush> {
        self.requests
            .lock()
            .unwrap_or_else(|p| p.into_inner())
            .clone()
    }

    pub async fn shutdown(mut self) {
        if let Some(tx) = self.shutdown.take() {
            let _ = tx.send(());
        }
        let _ = self.handle.await;
    }
}

/// Record the request and accept it (200 `{}` — the APNs success body).
async fn apns_device_push(
    State(requests): State<Arc<Mutex<Vec<RecordedPush>>>>,
    Path(token): Path<String>,
    req: Request,
) -> (StatusCode, Json<Value>) {
    let path = req.uri().path().to_string();
    let bytes = axum::body::to_bytes(req.into_body(), 1024 * 1024)
        .await
        .unwrap_or_default();
    let payload = serde_json::from_slice(&bytes).unwrap_or(Value::Null);
    requests
        .lock()
        .unwrap_or_else(|p| p.into_inner())
        .push(RecordedPush { path, payload });
    let _ = token; // the path already carries it
    (StatusCode::OK, Json(json!({})))
}
