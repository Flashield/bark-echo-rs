//! The one test seam: the real bark-server-rs, embedded in-process on a
//! random port (spec Testing Decisions). Mirrors echo-core's
//! `tests/common/mod.rs` TestServer — bind `127.0.0.1:0` and hand the
//! listener itself to `run_server_on_listener`, poll `/ping` until ready,
//! shut down via a oneshot. No Docker, no subprocess.

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
    // common/ is compiled into every test target; these constructors are
    // shared per target, not used everywhere.
    #[allow(dead_code)]
    pub async fn spawn_with_basic_auth(user: &str, password: &str) -> Self {
        Self::spawn_configuring(|config| {
            config.basic_auth_user = user.to_string();
            config.basic_auth_password = password.to_string();
        })
        .await
    }

    /// [`TestServer::spawn`] with per-test `Config` overrides applied on
    /// top of the ephemeral-port, temp-data-dir defaults.
    async fn spawn_configuring(configure: impl FnOnce(&mut Config)) -> Self {
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

    /// Spawn the server on an already-bound listener with a **caller-owned**
    /// data directory: the store outlives [`TestServer::shutdown`], so a
    /// test can restart the server on the same port and data while a
    /// daemon under test rides the outage. The struct's own temp dir is a
    /// scratch value here — the data dir is the caller's to hold.
    #[allow(dead_code)]
    pub async fn spawn_on(
        listener: std::net::TcpListener,
        data_dir: std::path::PathBuf,
        tweak: impl FnOnce(&mut Config),
    ) -> TestServer {
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
        TestServer {
            base_url,
            shutdown: Some(tx),
            handle,
            _dir: tempfile::tempdir().expect("scratch dir"),
        }
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

/// Poll until the server's `/info` reports exactly `n` open Realtime
/// Streams (protocol §2.3) — the external observation that a daemon's
/// stream connection is established before a test pushes.
#[allow(dead_code)]
pub async fn wait_active_streams(server: &TestServer, n: u64) {
    let deadline = std::time::Instant::now() + std::time::Duration::from_secs(10);
    loop {
        let info: serde_json::Value = reqwest::Client::new()
            .get(format!("{}/info", server.base_url))
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

/// One CLI run's observable surface (exit status, stdout, stderr).
// common/ is compiled into every test target; helpers are shared per
// target, not all used everywhere.
#[allow(dead_code)]
pub struct Run {
    pub success: bool,
    pub stdout: String,
    pub stderr: String,
}

impl Run {
    /// Assert the run exited 0 and return its stdout.
    #[allow(dead_code)]
    pub fn stdout_or_die(&self, what: &str) -> String {
        assert!(
            self.success,
            "{what} must exit 0; stderr: {}; stdout: {}",
            self.stderr, self.stdout
        );
        self.stdout.clone()
    }
}

/// A hermetic CLI run environment: a temp `HOME` (so config, secrets, and
/// History Logs land nowhere else) and the daemon subprocess. Mirrors the
/// harness in tests/cli.rs for the pull/log seams (ticket #7).
// common/ is compiled into every test target; helpers are shared per
// target, not all used everywhere.
#[allow(dead_code)]
pub struct Harness {
    home: tempfile::TempDir,
}

#[allow(dead_code)]
impl Harness {
    pub fn new() -> Self {
        Harness {
            home: tempfile::tempdir().expect("temp home"),
        }
    }

    /// The config locations the daemon resolves inside this harness —
    /// computed with the same pure function the binary uses.
    #[cfg(unix)]
    pub fn paths(&self) -> echo_daemon::paths::Paths {
        let xdg = self.home.path().join(".config");
        let config_dir = echo_daemon::paths::config_dir_from(Some(self.home.path()), Some(&xdg))
            .expect("temp home resolves");
        echo_daemon::paths::Paths::from_dir(config_dir)
    }

    pub async fn run(&self, args: &[&str]) -> Run {
        let output = self.spawn(args).output().await.expect("spawn bark-echo");
        Run {
            success: output.status.success(),
            stdout: String::from_utf8_lossy(&output.stdout).into_owned(),
            stderr: String::from_utf8_lossy(&output.stderr).into_owned(),
        }
    }

    /// Spawn (not await) the daemon: for long-lived runs (`log --follow`)
    /// the test reads stdout incrementally and kills the child.
    #[cfg(unix)]
    pub fn spawn(&self, args: &[&str]) -> tokio::process::Command {
        let mut command = Command::new(env!("CARGO_BIN_EXE_bark-echo"));
        command
            .args(args)
            .env("HOME", self.home.path())
            .env("XDG_CONFIG_HOME", self.home.path().join(".config"));
        command
    }

    #[cfg(windows)]
    pub fn spawn(&self, args: &[&str]) -> tokio::process::Command {
        let mut command = Command::new(env!("CARGO_BIN_EXE_bark-echo"));
        command.env("APPDATA", self.home.path().join("AppData/Roaming"));
        command
    }
}

use tokio::process::Command;
