//! The single-instance lock (ADR-0002, spec story 23): while one process
//! holds the daemon lock, a second acquisition is refused with a clear,
//! actionable error — notifications must never double-display. Releasing
//! (process exit included) frees the lock for the next run.

use echo_daemon::lock::{DaemonLock, LockError};

#[test]
fn a_second_acquire_is_refused_while_the_first_holds_the_lock() {
    let dir = tempfile::tempdir().expect("temp dir");
    let path = dir.path().join("daemon.lock");

    let first = DaemonLock::acquire(&path).expect("first acquire");
    // The lock file names its holder for diagnostics.
    let raw = std::fs::read_to_string(&path).expect("lock file readable");
    assert!(
        raw.contains(&std::process::id().to_string()),
        "the lock file records the holding pid: {raw:?}"
    );

    let second = DaemonLock::acquire(&path).expect_err("second acquire refused");
    match &second {
        LockError::Held { path: held } => {
            assert_eq!(held, &path, "the error names the lock file");
            let message = second.to_string();
            assert!(
                message.contains("already running") || message.contains("another"),
                "the message says another instance holds it: {message}"
            );
            assert!(
                message.contains(path.to_string_lossy().as_ref()),
                "the message names where: {message}"
            );
        }
        other => panic!("a contested lock is Held, not {other:?}"),
    }

    // Releasing the first frees the path: the next run can start.
    drop(first);
    DaemonLock::acquire(&path).expect("acquire after release");
}

#[test]
fn locks_on_different_paths_do_not_contend() {
    let dir = tempfile::tempdir().expect("temp dir");
    let _one = DaemonLock::acquire(dir.path().join("one.lock")).expect("first path");
    let _two = DaemonLock::acquire(dir.path().join("two.lock")).expect("second path");
}

#[test]
fn a_probe_never_creates_the_file_and_answers_whether_a_daemon_holds_it() {
    let dir = tempfile::tempdir().expect("temp dir");
    let path = dir.path().join("daemon.lock");

    // Nothing has ever run: absent file, answered as free — and the
    // probe must not have fabricated the file as a side effect.
    assert!(!DaemonLock::probe(&path).expect("probe absent"), "free");
    assert!(!path.exists(), "probing must not create the lock file");

    let held = DaemonLock::acquire(&path).expect("acquire");
    assert!(DaemonLock::probe(&path).expect("probe held"), "held");

    drop(held);
    assert!(
        !DaemonLock::probe(&path).expect("probe released"),
        "free again"
    );
}
