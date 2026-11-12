//! Keyring-store seam: the normal secret home (spec story 18). CI has no
//! secret service, so these tests probe reachability first — with a
//! service they exercise the real keyring round trip; without one they
//! pin the actionable `--plain-secrets` failure.

use echo_daemon::secrets::{KeyringStore, SecretError, SecretKey, SecretKind, SecretStore};

fn key() -> SecretKey {
    SecretKey {
        server: "http://127.0.0.1:8080".to_owned(),
        token_id: "e9351ee91c837988".to_owned(),
    }
}

/// A scratch entry proves a secret service answers this session. The
/// probe account is unique per call so two tests probing in parallel
/// never fight over the same entry.
fn service_reachable() -> bool {
    let probe = format!(
        "echo-daemon-test-probe-{}-{}",
        std::process::id(),
        std::time::SystemTime::now()
            .duration_since(std::time::UNIX_EPOCH)
            .map(|since| since.subsec_nanos())
            .unwrap_or(0)
    );
    let Ok(entry) = keyring::Entry::new(&probe, "probe") else {
        return false;
    };
    match entry.set_password("probe") {
        Ok(()) => entry.delete_credential().is_ok(),
        Err(_) => false,
    }
}

#[test]
fn keyring_store_round_trips_when_a_secret_service_answers() {
    if !service_reachable() {
        eprintln!("skipping: no secret service on this machine");
        return;
    }

    let store = KeyringStore::new();
    store
        .set(
            &key(),
            SecretKind::StreamToken,
            "st_31gpMA4IqFoXza0fRcAvSmavSvCuM",
        )
        .expect("set in keyring");
    assert_eq!(
        store
            .get(&key(), SecretKind::StreamToken)
            .expect("get from keyring"),
        "st_31gpMA4IqFoXza0fRcAvSmavSvCuM"
    );
    store
        .delete(&key(), SecretKind::StreamToken)
        .expect("delete");
    assert!(
        matches!(
            store.get(&key(), SecretKind::StreamToken),
            Err(SecretError::NotFound { .. })
        ),
        "deleted secret is gone from the keyring"
    );
}

#[test]
fn without_a_secret_service_the_error_points_at_the_fallback() {
    if service_reachable() {
        eprintln!("skipping: a secret service is answering");
        return;
    }

    let err = KeyringStore::new()
        .set(&key(), SecretKind::StreamToken, "st_31gpMA4Iq")
        .expect_err("no service, no store");

    assert!(
        matches!(err, SecretError::Unavailable(_)),
        "wrong error: {err}"
    );
    assert!(
        err.to_string().contains("--plain-secrets"),
        "error must teach the fallback, got: {err}"
    );
}
