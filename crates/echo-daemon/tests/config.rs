//! Config seam: the Subscription list (ADR-0001) persists as TOML under
//! non-secret fields only — server URL, label, the Stream Token's public
//! identity, a Device hint — plus per-server Basic Auth entries. Malformed
//! files must surface actionable errors, never panics.

use echo_daemon::config::{Config, ServerEntry, Subscription};

fn subscription(id: &str, label: &str, server: &str) -> Subscription {
    Subscription {
        id: id.to_owned(),
        server: server.to_owned(),
        label: label.to_owned(),
        token_hint: "st_31gpMA4Iq...".to_owned(),
        device_hint: "itu4irzi...".to_owned(),
    }
}

#[test]
fn saving_and_loading_round_trips_subscriptions_and_server_entries() {
    let dir = tempfile::tempdir().expect("temp dir");
    let path = dir.path().join("config.toml");
    let config = Config {
        subscriptions: vec![
            subscription(
                "e9351ee91c837988",
                "macbook-reminder",
                "http://127.0.0.1:8080",
            ),
            subscription(
                "4ec22c61d6e73de0",
                "family-ipad",
                "https://bark.example.org",
            ),
        ],
        servers: vec![ServerEntry {
            url: "https://bark.example.org".to_owned(),
            basic_auth_user: "operator".to_owned(),
            basic_auth_password: "s3cret".to_owned(),
        }],
    };

    config.save(&path).expect("save config");
    let loaded = Config::load(&path).expect("load config");

    assert_eq!(loaded, config, "round trip must preserve every entry");
}

#[test]
fn loading_a_missing_config_yields_an_empty_config() {
    let dir = tempfile::tempdir().expect("temp dir");
    let path = dir.path().join("config.toml");

    let loaded = Config::load(&path).expect("missing config is a fresh install");

    assert_eq!(
        loaded,
        Config::default(),
        "first run starts with no Subscriptions"
    );
}

#[test]
fn malformed_toml_is_an_error_naming_the_file_and_the_spot() {
    let dir = tempfile::tempdir().expect("temp dir");
    let path = dir.path().join("config.toml");
    std::fs::write(&path, "this is not [ toml").expect("write garbage");

    let err = Config::load(&path).expect_err("garbage must not load");

    let message = err.to_string();
    assert!(
        message.contains(path.to_str().unwrap()),
        "error must name the file, got: {message}"
    );
    assert!(
        message.contains("line"),
        "error must point at the parse location, got: {message}"
    );
}

#[test]
fn an_unknown_field_is_an_error_naming_the_field() {
    let dir = tempfile::tempdir().expect("temp dir");
    let path = dir.path().join("config.toml");
    // A label typo: without strict decoding this would silently drop the
    // Subscription's name — worse than failing.
    std::fs::write(
        &path,
        r#"[[subscriptions]]
id = "e9351ee91c837988"
server = "http://127.0.0.1:8080"
lable = "macbook-reminder"
token_hint = "st_31gpMA4Iq..."
device_hint = "itu4irzi..."
"#,
    )
    .expect("write config");

    let err = Config::load(&path).expect_err("typo'd field must not load");

    let message = err.to_string();
    assert!(
        message.contains("lable"),
        "error must name the unknown field, got: {message}"
    );
    assert!(
        message.contains("label"),
        "error must suggest the known field, got: {message}"
    );
}
