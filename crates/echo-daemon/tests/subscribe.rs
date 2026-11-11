//! Subscribe seam (ticket #3): the CLI's add/list/rm as one orchestration
//! over the Subscription-list config (ADR-0001), the secret store
//! (ADR-0001: keyed by server + token id), and the server's Device/Token
//! plane — driven against the real embedded server.

mod common;

use echo_core::server::ServerClient;

use echo_daemon::config::Config;
use echo_daemon::secrets::{PlainFileStore, SecretError, SecretKey, SecretKind, SecretStore};
use echo_daemon::subscribe::{AddSource, add, list, remove};

/// One test's storage: a fresh config value and a plain-file secret store
/// (plain mode is also what every headless run and CLI test uses).
struct Fixture {
    config: Config,
    store: PlainFileStore,
}

impl Fixture {
    async fn new(server: &common::TestServer) -> (Fixture, ServerClient) {
        let dir = tempfile::tempdir().expect("temp dir");
        (
            Fixture {
                config: Config::default(),
                store: PlainFileStore::new(dir.path().join("secrets.toml")),
            },
            ServerClient::new(&server.base_url),
        )
    }

    fn key(&self, server_url: &str, token_id: &str) -> SecretKey {
        SecretKey {
            server: server_url.to_owned(),
            token_id: token_id.to_owned(),
        }
    }
}

#[tokio::test]
async fn adding_an_own_subscription_registers_mints_and_persists_everything() {
    let server = common::TestServer::spawn().await;
    let (mut fixture, client) = Fixture::new(&server).await;

    let outcome = add(
        &mut fixture.config,
        &fixture.store,
        &client,
        AddSource::Own,
        "workstation",
    )
    .await
    .expect("add --own");

    // The Device Key is the real thing — minting a token against it
    // succeeds server-side.
    assert_eq!(outcome.device_key.len(), 22, "device key shape");
    let tokens = client
        .list_stream_tokens(&outcome.device_key)
        .await
        .expect("device key works against the server");
    assert_eq!(tokens.len(), 1, "exactly the token add minted");
    assert_eq!(
        tokens[0].client_label, "workstation",
        "the client label reaches the server's token record"
    );

    // The Subscription list holds exactly one entry: this Stream Token.
    assert_eq!(list(&fixture.config).len(), 1, "one subscription");
    let subscription = &list(&fixture.config)[0];
    assert_eq!(subscription.id, outcome.subscription.id);
    assert_eq!(subscription.label, "workstation");
    assert_eq!(subscription.server, server.base_url, "canonical server URL");

    // Both secrets live in the store under the Subscription's id, and the
    // hints match what was stored — never re-exposed by config.
    let key = fixture.key(&server.base_url, &outcome.subscription.id);
    assert_eq!(
        fixture
            .store
            .get(&key, SecretKind::DeviceKey)
            .expect("device key stored"),
        outcome.device_key,
        "the Device Key secret is stored"
    );
    let token = fixture
        .store
        .get(&key, SecretKind::StreamToken)
        .expect("token stored");
    assert!(token.starts_with("st_"), "stream token shape");
    assert_eq!(
        outcome.subscription.token_hint,
        format!("{}...", &token[..12]),
        "token hint is the stored secret's first 12 characters"
    );
    assert_eq!(
        outcome.subscription.device_hint,
        format!("{}...", &outcome.device_key[..8]),
        "device hint is the Device Key's first 8 characters"
    );

    server.shutdown().await;
}

#[tokio::test]
async fn adding_with_a_device_key_mirrors_the_existing_device() {
    let server = common::TestServer::spawn().await;
    let (mut fixture, client) = Fixture::new(&server).await;
    let device = client
        .register_stream_device()
        .await
        .expect("register device");

    let outcome = add(
        &mut fixture.config,
        &fixture.store,
        &client,
        AddSource::DeviceKey(device.device_key.clone()),
        "family-ipad",
    )
    .await
    .expect("add --device-key");

    assert_eq!(
        outcome.device_key, device.device_key,
        "the mirrored Device is addressed by the caller's key"
    );
    let tokens = client
        .list_stream_tokens(&device.device_key)
        .await
        .expect("list the Device's tokens");
    assert_eq!(tokens.len(), 1);
    assert_eq!(
        tokens[0].client_label, "family-ipad",
        "the mint carries the client's label"
    );
    assert_eq!(
        outcome.subscription.device_hint,
        format!("{}...", &device.device_key[..8]),
        "the hint names the mirrored Device"
    );
    let key = fixture.key(&server.base_url, &outcome.subscription.id);
    let token = fixture
        .store
        .get(&key, SecretKind::StreamToken)
        .expect("token stored");
    assert_eq!(
        outcome.subscription.token_hint,
        format!("{}...", &token[..12]),
        "the Stream Token secret is stored under the fresh Subscription id"
    );

    server.shutdown().await;
}

#[tokio::test]
async fn a_failed_add_leaves_no_partial_state_behind() {
    let server = common::TestServer::spawn().await;
    let (mut fixture, _client) = Fixture::new(&server).await;

    // A 22-char-shaped key the server does not know: minting must fail
    // with the typed error and leave nothing in config or store.
    let err = add(
        &mut fixture.config,
        &fixture.store,
        &ServerClient::new(&server.base_url),
        AddSource::DeviceKey("itu4irzixECYJ5Ftd9UygX".to_owned()),
        "ghost",
    )
    .await
    .expect_err("unknown device key");

    assert!(
        err.to_string().contains("device not found"),
        "the typed server error surfaces, got: {err}"
    );
    assert!(
        list(&fixture.config).is_empty(),
        "no Subscription half-added"
    );
    assert!(
        !fixture.store.path().exists(),
        "no secrets written for the failed add"
    );

    server.shutdown().await;
}

#[tokio::test]
async fn two_subscriptions_to_the_same_device_each_hold_their_own_secrets() {
    let server = common::TestServer::spawn().await;
    let (mut fixture, client) = Fixture::new(&server).await;
    let device = client
        .register_stream_device()
        .await
        .expect("register device");
    let source = AddSource::DeviceKey(device.device_key.clone());
    let first = add(
        &mut fixture.config,
        &fixture.store,
        &client,
        source.clone(),
        "desk",
    )
    .await
    .expect("first add");
    let second = add(
        &mut fixture.config,
        &fixture.store,
        &client,
        source,
        "laptop",
    )
    .await
    .expect("second add");

    assert_ne!(
        first.subscription.id, second.subscription.id,
        "every add mints a fresh token id — the Subscription's unique id"
    );
    let first_key = fixture.key(&server.base_url, &first.subscription.id);
    let second_key = fixture.key(&server.base_url, &second.subscription.id);
    assert_ne!(
        fixture
            .store
            .get(&first_key, SecretKind::StreamToken)
            .expect("first token"),
        fixture
            .store
            .get(&second_key, SecretKind::StreamToken)
            .expect("second token"),
        "two Subscriptions never share a secret"
    );
    assert_eq!(
        list(&fixture.config).len(),
        2,
        "both Subscriptions listed in add order"
    );

    server.shutdown().await;
}

#[tokio::test]
async fn removing_a_subscription_deletes_its_config_entry_and_both_secrets() {
    let server = common::TestServer::spawn().await;
    let (mut fixture, client) = Fixture::new(&server).await;
    let added = add(
        &mut fixture.config,
        &fixture.store,
        &client,
        AddSource::Own,
        "workstation",
    )
    .await
    .expect("add --own");
    let key = fixture.key(&server.base_url, &added.subscription.id);

    let removed =
        remove(&mut fixture.config, &fixture.store, &added.subscription.id).expect("remove");

    assert_eq!(
        removed, added.subscription,
        "remove answers the Subscription it removed"
    );
    assert!(
        list(&fixture.config).is_empty(),
        "the Subscription list is empty again"
    );
    assert!(
        matches!(
            fixture.store.get(&key, SecretKind::DeviceKey),
            Err(SecretError::NotFound { .. })
        ),
        "the Device Key secret is gone"
    );
    assert!(
        matches!(
            fixture.store.get(&key, SecretKind::StreamToken),
            Err(SecretError::NotFound { .. })
        ),
        "the Stream Token secret is gone"
    );

    server.shutdown().await;
}

#[tokio::test]
async fn removing_an_unknown_id_is_an_actionable_error() {
    let server = common::TestServer::spawn().await;
    let (mut fixture, _client) = Fixture::new(&server).await;

    let err = remove(&mut fixture.config, &fixture.store, "deadbeef00000000")
        .expect_err("no such Subscription");

    assert!(
        err.to_string().contains("deadbeef00000000") && err.to_string().contains("subscribe list"),
        "error must name the id and the way out, got: {err}"
    );

    server.shutdown().await;
}
