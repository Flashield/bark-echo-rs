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
    _dir: tempfile::TempDir,
}

impl TestServer {
    /// Bind a random port, start the server on the listener's runtime
    /// task, and wait until `/ping` answers.
    pub async fn spawn() -> Self {
        Self::spawn_configuring(|_| {}).await
    }

    /// [`TestServer::spawn`] with operator Basic Auth enabled (protocol
    /// §1.4): every path except `/ping`, `/register` and `/healthz`
    /// demands the credentials.
    pub async fn spawn_with_basic_auth(user: &str, password: &str) -> Self {
        Self::spawn_configuring(|config| {
            config.basic_auth_user = user.to_string();
            config.basic_auth_password = password.to_string();
        })
        .await
    }

    /// [`TestServer::spawn`] with the APNs endpoint pointed at `endpoint`
    /// (TLS verification off) — the stub route for iOS-type Devices.
    pub async fn spawn_with_apns(endpoint: &str) -> Self {
        Self::spawn_configuring(|config| {
            config.apns_endpoint = endpoint.to_string();
            config.apns_insecure_skip_tls_verify = true;
        })
        .await
    }

    /// [`TestServer::spawn`] with per-test `Config` overrides applied on
    /// top of the ephemeral-port, temp-data-dir defaults.
    pub async fn spawn_configuring(configure: impl FnOnce(&mut Config)) -> Self {
        let dir = tempfile::tempdir().expect("temp data dir");
        let listener = std::net::TcpListener::bind("127.0.0.1:0").expect("bind ephemeral port");
        let addr = listener.local_addr().expect("local addr");
        let mut config = Config {
            addr: addr.to_string(),
            data_dir: dir.path().to_path_buf(),
            ..Config::default()
        };
        configure(&mut config);
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
            _dir: dir,
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
