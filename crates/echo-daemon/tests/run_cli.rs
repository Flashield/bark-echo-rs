//! The `run` command over the CLI seam (spec Testing Decisions): a
//! subprocess against a temp `HOME` in plain-secrets mode. The contract:
//! a daemon with nothing to mirror refuses with the actionable fix, a
//! second daemon refuses on the single-instance lock (ADR-0002), and a
//! started daemon holds its Realtime Stream until killed.

mod common;

use std::time::{Duration, Instant};

use tokio::process::Command;

/// One CLI run's observable surface.
struct Run {
    success: bool,
    stdout: String,
    stderr: String,
}

/// A hermetic run environment: a temp `HOME` (config and plain secrets
/// land nowhere else) and the daemon subprocess inside it.
struct Harness {
    home: tempfile::TempDir,
}

impl Harness {
    fn new() -> Self {
        Harness {
            home: tempfile::tempdir().expect("temp home"),
        }
    }

    #[cfg(unix)]
    fn spawn(&self, args: &[&str]) -> Command {
        let mut command = Command::new(env!("CARGO_BIN_EXE_echo-daemon"));
        command
            .args(args)
            .env("HOME", self.home.path())
            .env("XDG_CONFIG_HOME", self.home.path().join(".config"));
        command
    }

    #[cfg(windows)]
    fn spawn(&self, args: &[&str]) -> Command {
        let mut command = Command::new(env!("CARGO_BIN_EXE_echo-daemon"));
        command.env("APPDATA", self.home.path().join("AppData/Roaming"));
        command
    }

    async fn run(&self, args: &[&str]) -> Run {
        let output = self.spawn(args).output().await.expect("spawn echo-daemon");
        Run {
            success: output.status.success(),
            stdout: String::from_utf8_lossy(&output.stdout).into_owned(),
            stderr: String::from_utf8_lossy(&output.stderr).into_owned(),
        }
    }

    /// Add one Subscription to this machine (`--own`), asserting it took.
    async fn subscribe_own(&self, server: &str) {
        let output = self
            .run(&[
                "--plain-secrets",
                "subscribe",
                "add",
                "--server",
                server,
                "--own",
                "--label",
                "run-test",
            ])
            .await;
        assert!(
            output.success,
            "subscribe add must exit 0: {} {}",
            output.stderr, output.stdout
        );
    }

    /// The lock file the daemon resolves inside this harness — computed
    /// with the same pure function the binary uses.
    #[cfg(unix)]
    fn lock_path(&self) -> std::path::PathBuf {
        let xdg = self.home.path().join(".config");
        let config_dir = echo_daemon::paths::config_dir_from(Some(self.home.path()), Some(&xdg))
            .expect("temp home resolves");
        echo_daemon::paths::Paths::from_dir(config_dir).lock_file()
    }
}

/// Poll `/info` until `active_streams` is `n`.
async fn wait_active_streams(server: &common::TestServer, n: u64) {
    let deadline = Instant::now() + Duration::from_secs(10);
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
            Instant::now() < deadline,
            "active_streams never reached {n}: {info}"
        );
        tokio::time::sleep(Duration::from_millis(20)).await;
    }
}

#[tokio::test]
async fn run_without_subscriptions_fails_with_the_actionable_fix() {
    let harness = Harness::new();
    let output = harness.run(&["--plain-secrets", "run"]).await;
    assert!(
        !output.success,
        "a daemon with nothing to mirror must not start"
    );
    assert!(
        output.stderr.contains("subscribe add"),
        "the error points at the fix: {}",
        output.stderr
    );
}

#[tokio::test]
async fn run_refuses_to_start_while_another_instance_holds_the_lock() {
    let server = common::TestServer::spawn().await;
    let harness = Harness::new();
    harness.subscribe_own(&server.base_url).await;

    // Another live process (here: the test itself) holds the lock the
    // subprocess is about to claim.
    let _other = echo_daemon::lock::DaemonLock::acquire(harness.lock_path()).expect("lock taken");

    let output = harness.run(&["--plain-secrets", "run"]).await;
    assert!(
        !output.success,
        "a contested lock must refuse to start; stdout: {}",
        output.stdout
    );
    assert!(
        output.stderr.contains("already running"),
        "the error names the single-instance rule: {}",
        output.stderr
    );
    assert!(
        output.stderr.contains("daemon.lock"),
        "the error names where the lock lives: {}",
        output.stderr
    );

    server.shutdown().await;
}

#[tokio::test]
async fn run_holds_its_stream_until_killed() {
    let server = common::TestServer::spawn().await;
    let harness = Harness::new();
    harness.subscribe_own(&server.base_url).await;

    // The daemon runs as a real subprocess and must connect on its own.
    let mut daemon = harness
        .spawn(&["--plain-secrets", "run"])
        .spawn()
        .expect("spawn run");
    wait_active_streams(&server, 1).await;

    // Killing the daemon closes the stream: the OS releases the lock the
    // process held, so the next run can start without cleanup.
    daemon.kill().await.expect("kill daemon");
    let _ = daemon.wait().await;
    wait_active_streams(&server, 0).await;
    let next = harness.run(&["--plain-secrets", "subscribe", "list"]).await;
    assert!(
        next.success,
        "the config is still intact after the kill: {} {}",
        next.stderr, next.stdout
    );

    server.shutdown().await;
}
