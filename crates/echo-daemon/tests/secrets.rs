//! Secret-store seam: the plain-file fallback (spec story 19) — Stream
//! Tokens and Device Keys in a 0600 TOML file under the config dir, used
//! by every test in CI (no secret service there) and available to users
//! via `--plain-secrets`.

#[cfg(unix)]
use std::os::unix::fs::PermissionsExt;

use echo_daemon::secrets::{PlainFileStore, SecretError, SecretKey, SecretKind, SecretStore};

fn key() -> SecretKey {
    SecretKey {
        server: "http://127.0.0.1:8080".to_owned(),
        token_id: "e9351ee91c837988".to_owned(),
    }
}

#[test]
fn set_then_get_round_trips_both_secret_kinds() {
    let dir = tempfile::tempdir().expect("temp dir");
    let store = PlainFileStore::new(dir.path().join("secrets.toml"));

    store
        .set(&key(), SecretKind::DeviceKey, "itu4irzixECYJ5Ftd9UygK")
        .expect("set device key");
    store
        .set(
            &key(),
            SecretKind::StreamToken,
            "st_31gpMA4IqFoXza0fRcAvSmavSvCuM",
        )
        .expect("set stream token");

    assert_eq!(
        store
            .get(&key(), SecretKind::DeviceKey)
            .expect("get device key"),
        "itu4irzixECYJ5Ftd9UygK"
    );
    assert_eq!(
        store
            .get(&key(), SecretKind::StreamToken)
            .expect("get stream token"),
        "st_31gpMA4IqFoXza0fRcAvSmavSvCuM"
    );
}

#[test]
fn a_reopened_store_sees_what_was_written_before_restart() {
    let dir = tempfile::tempdir().expect("temp dir");
    let path = dir.path().join("secrets.toml");

    PlainFileStore::new(&path)
        .set(
            &key(),
            SecretKind::StreamToken,
            "st_31gpMA4IqFoXza0fRcAvSmavSvCuM",
        )
        .expect("set before restart");

    let reopened = PlainFileStore::new(&path);
    assert_eq!(
        reopened
            .get(&key(), SecretKind::StreamToken)
            .expect("get after restart"),
        "st_31gpMA4IqFoXza0fRcAvSmavSvCuM"
    );
}

#[cfg(unix)]
#[test]
fn the_secrets_file_is_created_with_owner_only_permissions() {
    let dir = tempfile::tempdir().expect("temp dir");
    let path = dir.path().join("secrets.toml");
    let store = PlainFileStore::new(&path);

    store
        .set(&key(), SecretKind::StreamToken, "st_31gpMA4Iq")
        .expect("set");

    let mode = std::fs::metadata(&path)
        .expect("secrets file exists")
        .permissions()
        .mode();
    assert_eq!(
        mode & 0o777,
        0o600,
        "secrets file must be 0600, got {mode:o}"
    );
}

#[test]
fn getting_a_secret_that_was_never_set_is_an_error_naming_it() {
    let dir = tempfile::tempdir().expect("temp dir");
    let store = PlainFileStore::new(dir.path().join("secrets.toml"));

    let err = store
        .get(&key(), SecretKind::StreamToken)
        .expect_err("unset secret");

    assert!(
        matches!(err, SecretError::NotFound { .. }),
        "wrong error: {err}"
    );
    let message = err.to_string();
    assert!(
        message.contains("stream-token") && message.contains("e9351ee91c837988"),
        "error must name the missing secret, got: {message}"
    );
}

#[test]
fn delete_removes_only_its_kind_and_is_idempotent() {
    let dir = tempfile::tempdir().expect("temp dir");
    let store = PlainFileStore::new(dir.path().join("secrets.toml"));
    store
        .set(&key(), SecretKind::DeviceKey, "itu4irzixECYJ5Ftd9UygK")
        .expect("set device key");
    store
        .set(
            &key(),
            SecretKind::StreamToken,
            "st_31gpMA4IqFoXza0fRcAvSmavSvCuM",
        )
        .expect("set stream token");

    store
        .delete(&key(), SecretKind::StreamToken)
        .expect("delete");

    assert!(
        matches!(
            store.get(&key(), SecretKind::StreamToken),
            Err(SecretError::NotFound { .. })
        ),
        "deleted secret is gone"
    );
    assert_eq!(
        store
            .get(&key(), SecretKind::DeviceKey)
            .expect("sibling secret survives"),
        "itu4irzixECYJ5Ftd9UygK",
        "deleting one kind must not take the other"
    );
    store
        .delete(&key(), SecretKind::StreamToken)
        .expect("deleting an absent secret is fine (idempotent)");
}

#[test]
fn a_corrupted_secrets_file_is_an_error_naming_the_file() {
    let dir = tempfile::tempdir().expect("temp dir");
    let path = dir.path().join("secrets.toml");
    std::fs::write(&path, "not [ toml").expect("write garbage");

    let err = PlainFileStore::new(&path)
        .get(&key(), SecretKind::StreamToken)
        .expect_err("corrupted store");

    let message = err.to_string();
    assert!(
        message.contains(path.to_str().unwrap()),
        "error must name the file, got: {message}"
    );
}
