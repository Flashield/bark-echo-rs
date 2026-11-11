//! The one test seam: the real bark-server-rs, embedded in-process on a
//! random port (spec Testing Decisions). Mirrors the server repo's own
//! `tests/common/mod.rs` TestServer: bind `127.0.0.1:0` and hand the
//! listener itself to `run_server_on_listener`, poll `/ping` until ready,
//! shut down via a oneshot. No Docker, no subprocess.

pub mod apns;
pub mod sse;

use bark_server_rs::config::Config;

/// An embedded bark server bound to an ephemeral port with its own
/// temporary data directory. Hold the value until the test is done;
/// dropping it deletes the SQLite store with the temp dir.
pub struct TestServer {
    /// `http://127.0.0.1:<port>` — no trailing slash, no url prefix.
    pub base_url: String,
    shutdown: Option<tokio::sync::oneshot::Sender<()>>,
    handle: tokio::task::JoinHandle<anyhow::Result<()>>,
    /// The owned data dir; `None` when the caller owns it (a restart
    /// scenario keeps the store across server processes).
    _dir: Option<tempfile::TempDir>,
}

impl TestServer {
    /// Bind a random port, start the server on the listener's runtime
    /// task, and wait until `/ping` answers, with default settings.
    pub async fn spawn() -> Self {
        Self::spawn_with(|_| {}).await
    }

    /// [`TestServer::spawn`] with operator Basic Auth enabled (protocol
    /// §1.4): every path except `/ping`, `/register` and `/healthz`
    /// demands the credentials.
    // common/ is compiled into every test target; these constructors are
    // shared per target, not used everywhere.
    #[allow(dead_code)]
    pub async fn spawn_with_basic_auth(user: &str, password: &str) -> Self {
        Self::spawn_with(|config| {
            config.basic_auth_user = user.to_string();
            config.basic_auth_password = password.to_string();
        })
        .await
    }

    /// [`TestServer::spawn`] with the APNs endpoint pointed at `endpoint`
    /// (TLS verification off) — the stub route for iOS-type Devices.
    #[allow(dead_code)]
    pub async fn spawn_with_apns(endpoint: &str) -> Self {
        Self::spawn_with(|config| {
            config.apns_endpoint = endpoint.to_string();
            config.apns_insecure_skip_tls_verify = true;
        })
        .await
    }

    /// Like [`TestServer::spawn`], with a tweak applied to the server
    /// `Config` (keep-alive interval, Basic Auth, retention, …).
    pub async fn spawn_with(tweak: impl FnOnce(&mut Config)) -> Self {
        let dir = tempfile::tempdir().expect("temp data dir");
        let listener = std::net::TcpListener::bind("127.0.0.1:0").expect("bind ephemeral port");
        let mut server = Self::spawn_on(listener, dir.path().to_path_buf(), tweak).await;
        server._dir = Some(dir);
        server
    }

    /// Spawn the server on an already-bound listener with a caller-owned
    /// data directory: the store outlives [`TestServer::shutdown`], so a
    /// test can restart the server on the same port and data — the
    /// server-initiated stream close of a shutdown, then a reconnect
    /// against the persisted Device, Stream Token, and message ids.
    #[allow(dead_code)]
    pub async fn spawn_on(
        listener: std::net::TcpListener,
        data_dir: std::path::PathBuf,
        tweak: impl FnOnce(&mut Config),
    ) -> Self {
        let addr = listener.local_addr().expect("local addr");
        let mut config = Config {
            addr: addr.to_string(),
            data_dir,
            ..Config::default()
        };
        tweak(&mut config);
        let base_url = format!("http://{addr}");
        let (tx, rx) = tokio::sync::oneshot::channel::<()>();
        let handle = tokio::spawn(bark_server_rs::run_server_on_listener(
            listener,
            config,
            async move {
                let _ = rx.await;
            },
        ));
        wait_ready(&format!("{base_url}/ping")).await;
        Self {
            base_url,
            shutdown: Some(tx),
            handle,
            _dir: None,
        }
    }

    /// Signal shutdown and await the server task's clean exit.
    pub async fn shutdown(mut self) {
        if let Some(tx) = self.shutdown.take() {
            let _ = tx.send(());
        }
        let _ = self.handle.await;
    }
}

/// A Subscription's server-side identity plus a scratch directory for its
/// History Log: one registered Stream Device, one minted Stream Token.
// common/ is compiled into every test target; helpers are shared per
// target, not all used everywhere.
#[allow(dead_code)]
pub struct Subscribed {
    /// The embedded server.
    pub server: TestServer,
    /// The Stream Device's Device Key (the push address).
    pub device_key: String,
    /// The Stream Token secret (the read credential).
    pub stream_token: String,
    /// Scratch space; keep alive for the test's duration.
    pub dir: tempfile::TempDir,
}

#[allow(dead_code)]
impl Subscribed {
    /// Register a Stream Device, mint it a token, and hand back the harness.
    pub async fn spawn() -> Self {
        let server = TestServer::spawn().await;
        let client = echo_core::server::ServerClient::new(&server.base_url);
        let device = client.register_stream_device().await.expect("register");
        let minted = client
            .mint_stream_token(&device.device_key, "echo-test-client")
            .await
            .expect("mint");
        Self {
            server,
            device_key: device.device_key,
            stream_token: minted.secret,
            dir: tempfile::tempdir().expect("temp dir"),
        }
    }

    /// Push one V2 JSON Push to the Device and assert the server took it.
    /// `body` merges over `{"device_key": …}`.
    pub async fn push(&self, body: serde_json::Value) {
        let mut payload = serde_json::json!({ "device_key": self.device_key });
        if let (Some(obj), Some(extra)) = (payload.as_object_mut(), body.as_object()) {
            for (key, value) in extra {
                obj.insert(key.clone(), value.clone());
            }
        }
        let resp = reqwest::Client::new()
            .post(format!("{}/push", self.server.base_url))
            .json(&payload)
            .send()
            .await
            .expect("POST /push");
        let status = resp.status();
        let envelope: serde_json::Value = resp.json().await.expect("push envelope");
        assert!(
            status.is_success() && envelope["code"] == 200,
            "push failed: {status} {envelope}"
        );
    }

    /// The path of this Subscription's History Log inside the harness dir.
    pub fn history_path(&self) -> std::path::PathBuf {
        self.dir.path().join("history.jsonl")
    }
}

/// Wait until the server's `/info` reports exactly `n` open Realtime
/// Streams (protocol §2.3) — the external observation that a client's
/// stream connection is established before a test pushes.
// common/ is compiled into every test target; helpers are shared per
// target, not all used everywhere.
#[allow(dead_code)]
pub async fn wait_active_streams(server: &TestServer, n: u64) {
    wait_active_streams_with(server, n, None).await
}

/// [`wait_active_streams`] with operator Basic Auth credentials riding on
/// `/info` — behind Basic Auth every observation needs them (§1.4).
// common/ is compiled into every test target; helpers are shared per
// target, not all used everywhere.
#[allow(dead_code)]
pub async fn wait_active_streams_with(server: &TestServer, n: u64, auth: Option<(&str, &str)>) {
    let deadline = std::time::Instant::now() + std::time::Duration::from_secs(10);
    loop {
        let mut request = reqwest::Client::new().get(format!("{}/info", server.base_url));
        if let Some((user, password)) = auth {
            request = request.basic_auth(user, Some(password));
        }
        let info: serde_json::Value = request
            .send()
            .await
            .expect("GET /info")
            .json()
            .await
            .expect("/info json");
        if info["active_streams"] == n {
            return;
        }
        assert!(
            std::time::Instant::now() < deadline,
            "active_streams never reached {n}: {info}"
        );
        tokio::time::sleep(std::time::Duration::from_millis(20)).await;
    }
}

/// Poll until the server answers 2xx (50 ms apart, 15 s budget) — the
/// listener is bound before spawn, so failure means the server task died.
async fn wait_ready(url: &str) {
    let client = reqwest::Client::new();
    let deadline = std::time::Instant::now() + std::time::Duration::from_secs(15);
    loop {
        if let Ok(resp) = client.get(url).send().await
            && resp.status().is_success()
        {
            return;
        }
        assert!(
            std::time::Instant::now() < deadline,
            "embedded bark-server never answered {url}"
        );
        tokio::time::sleep(std::time::Duration::from_millis(50)).await;
    }
}
