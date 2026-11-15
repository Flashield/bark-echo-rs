//! The HTTP seam (issue #31): the real daemon in-process — the way the
//! existing runtime tests drive it, embedded bark-server and all —
//! driven over real HTTP to its management face. The security posture
//! is the suite's spine: the token gate refuses everything without a
//! credential, `/v1/session` exchanges the token for the HttpOnly
//! SameSite=Strict cookie the page and SSE ride, CSRF gates refuse
//! cross-site state changes, the brute-force guard engages after 5
//! consecutive failures, a squatted port fails loud naming the config,
//! and the LAN opt-in gates even the HTML page. Issue #24's resolution,
//! observed from outside the daemon.

mod common;

use std::sync::Arc;
use std::time::{Duration, Instant};

use echo_daemon::config::Config;
use echo_daemon::secrets::{SecretKey, SecretKind};
use reqwest::header;

/// A renderer fake that swallows every intent: the seam under test here
/// is the HTTP face, not rendering (mirrors tests/ipc_seam.rs's fake).
struct SilentRenderer;

impl echo_daemon::notify::NotificationRenderer for SilentRenderer {
    fn render(
        &self,
        _intent: &echo_core::intent::NotificationIntent,
    ) -> Result<(), echo_daemon::notify::RenderError> {
        Ok(())
    }
}

/// One registered Stream Device with its minted Stream Token secret and
/// id — the pieces a Subscription is built from (mirrors tests/run.rs).
// common per-target helpers: the CRUD-seam tests use this one; batch-1
// targets do not.
#[allow(dead_code)]
async fn register_and_mint(server: &common::TestServer, label: &str) -> (String, String, String) {
    let client = echo_core::server::ServerClient::new(&server.base_url);
    let device = client.register_stream_device().await.expect("register");
    let minted = client
        .mint_stream_token(&device.device_key, label)
        .await
        .expect("mint");
    (device.device_key, minted.secret, minted.token_id)
}

/// A daemon environment in a temp dir: an empty Subscription list and a
/// plain-file secret store (mirrors tests/ipc_seam.rs). Subscriptions
/// are added with [`Env::with_subscription`] — or hot, over the face.
struct Env {
    paths: echo_daemon::paths::Paths,
    config: echo_daemon::config::Config,
    store: Arc<dyn echo_daemon::secrets::SecretStore>,
    _dir: tempfile::TempDir,
}

impl Env {
    fn empty() -> Self {
        let dir = tempfile::tempdir().expect("temp dir");
        let paths = echo_daemon::paths::Paths::from_dir(dir.path());
        let store: Arc<dyn echo_daemon::secrets::SecretStore> = Arc::new(
            echo_daemon::secrets::PlainFileStore::new(paths.secrets_file()),
        );
        Self {
            paths,
            config: Config::default(),
            store,
            _dir: dir,
        }
    }

    #[allow(dead_code)]
    fn with_subscription(
        server_url: &str,
        device_key: &str,
        stream_token: &str,
        token_id: &str,
        label: &str,
    ) -> Self {
        let mut env = Self::empty();
        let key = SecretKey::for_subscription(server_url, token_id);
        env.store
            .set(&key, SecretKind::DeviceKey, device_key)
            .expect("store device key");
        env.store
            .set(&key, SecretKind::StreamToken, stream_token)
            .expect("store stream token");
        env.config = Config {
            subscriptions: vec![echo_daemon::config::Subscription {
                id: token_id.to_owned(),
                server: server_url.to_owned(),
                label: label.to_owned(),
                token_hint: format!("{}...", &stream_token[..12]),
                device_hint: format!("{}...", &device_key[..8]),
            }],
            servers: Vec::new(),
            management: Default::default(),
        };
        env
    }
}

/// A daemon run serving the management face, with everything a test
/// needs to talk to it: the base URL, the management token (the run
/// minted it into the store at startup), and the stop fence.
struct Running {
    fence: tokio::sync::oneshot::Sender<()>,
    runner: tokio::task::JoinHandle<Result<(), echo_daemon::run::RunError>>,
    base_url: String,
    token: String,
}

impl Running {
    /// Signal the graceful stop and await the run's clean exit.
    async fn stop(self) {
        let _ = self.fence.send(());
        self.runner
            .await
            .expect("the run task does not panic")
            .expect("the run ends clean");
    }
}

/// Run the real daemon in-process with the face on an ephemeral
/// listener; the token is polled out of the store (the run mints it at
/// startup — the first start with the face, issue #24).
async fn running_daemon_with_face(env: &Env, gate_page: bool) -> Running {
    let listener = std::net::TcpListener::bind("127.0.0.1:0").expect("bind ephemeral port");
    let addr = listener.local_addr().expect("local addr");
    let (fence_tx, fence_rx) = tokio::sync::oneshot::channel::<()>();
    let spec = echo_daemon::run::DaemonSpec {
        paths: env.paths.clone(),
        config: env.config.clone(),
        store: env.store.clone(),
        renderer: Arc::new(SilentRenderer),
        management: Some(echo_daemon::run::ManagementFace {
            listener,
            gate_page,
            // The test's face is always on 127.0.0.1; the LAN-simulated
            // runs (gate_page) drop the Host allowlist, like a real LAN
            // bind does.
            local_only: !gate_page,
        }),
    };
    let runner = tokio::spawn(async move {
        echo_daemon::run::run(spec, async move {
            let _ = fence_rx.await;
        })
        .await
    });
    // The token is ensured inside the run — poll it out of the store.
    let deadline = Instant::now() + Duration::from_secs(15);
    let token = loop {
        match echo_daemon::management::stored_token(env.store.as_ref()) {
            Ok(token) => break token,
            Err(echo_daemon::management::ManagementError::NoToken) => {
                assert!(
                    Instant::now() < deadline,
                    "the run never minted the management token"
                );
                tokio::time::sleep(Duration::from_millis(20)).await;
            }
            Err(err) => panic!("the management token is unreadable: {err}"),
        }
    };
    Running {
        fence: fence_tx,
        runner,
        base_url: format!("http://{addr}"),
        token,
    }
}

// --- HTTP helpers (reqwest with the credentials the test chooses) ---

/// GET with the token header and/or the session cookie.
async fn get(
    base_url: &str,
    path: &str,
    token: Option<&str>,
    cookie: Option<&str>,
) -> reqwest::Response {
    let mut request = reqwest::Client::new().get(format!("{base_url}{path}"));
    if let Some(token) = token {
        request = request.header("X-Management-Token", token);
    }
    if let Some(cookie) = cookie {
        request = request.header(header::COOKIE, cookie);
    }
    request.send().await.expect("GET the face")
}

/// POST a JSON body with the token header (the API-client path: the
/// token doubles as the custom header the CSRF gate demands).
async fn post_json(
    base_url: &str,
    path: &str,
    token: Option<&str>,
    body: serde_json::Value,
) -> reqwest::Response {
    let mut request = reqwest::Client::new().post(format!("{base_url}{path}"));
    if let Some(token) = token {
        request = request.header("X-Management-Token", token);
    }
    request.json(&body).send().await.expect("POST the face")
}

/// The stable error shape's body: `{"error":{"code","message"}}`.
async fn error_of(response: reqwest::Response) -> (reqwest::StatusCode, String, String) {
    let status = response.status();
    let body: serde_json::Value = response.json().await.expect("the error body is JSON");
    let error = &body["error"];
    assert!(
        error.is_object(),
        "every refusal carries the stable shape: {body}"
    );
    (
        status,
        error["code"].as_str().expect("code").to_owned(),
        error["message"].as_str().expect("message").to_owned(),
    )
}

/// `POST /v1/session` with `token`, riding the page's own custom header
/// (the session exchange is a state change: the CSRF gate demands one);
/// on success the session cookie line (name=value) is returned, with
/// the full attribute set asserted.
async fn sign_in(base_url: &str, token: &str) -> String {
    let response = reqwest::Client::new()
        .post(format!("{base_url}/v1/session"))
        .header("X-Requested-With", "bark-echo-face")
        .json(&serde_json::json!({ "token": token }))
        .send()
        .await
        .expect("POST /v1/session");
    assert_eq!(
        response.status(),
        reqwest::StatusCode::NO_CONTENT,
        "the token exchanges for a session"
    );
    let set_cookie = response
        .headers()
        .get(header::SET_COOKIE)
        .expect("the session rides Set-Cookie")
        .to_str()
        .expect("cookie is ascii")
        .to_owned();
    assert!(
        set_cookie.contains("HttpOnly"),
        "the cookie is HttpOnly: {set_cookie}"
    );
    assert!(
        set_cookie.contains("SameSite=Strict"),
        "the cookie is SameSite=Strict: {set_cookie}"
    );
    assert!(
        set_cookie.contains("Path=/"),
        "the cookie is scoped to the face: {set_cookie}"
    );
    assert!(
        set_cookie.contains("Max-Age="),
        "the cookie has a short TTL: {set_cookie}"
    );
    let name_value = set_cookie
        .split(';')
        .next()
        .expect("a cookie")
        .trim()
        .to_owned();
    assert!(
        name_value.starts_with("bark_echo_session="),
        "the face's cookie name: {set_cookie}"
    );
    name_value
}

/// One raw HTTP/1.1 request over TCP, for the checks a normal client
/// will not let us fake (a foreign Host header — the DNS-rebinding
/// shape). Returns the status code and the body text.
async fn raw_request(
    addr: std::net::SocketAddr,
    method: &str,
    path: &str,
    host: &str,
    headers: &[(&str, &str)],
    body: Option<&str>,
) -> (u16, String) {
    use tokio::io::{AsyncReadExt, AsyncWriteExt};
    let mut stream = tokio::net::TcpStream::connect(addr).await.expect("connect");
    let mut request = format!("{method} {path} HTTP/1.1\r\nHost: {host}\r\nConnection: close\r\n");
    for (name, value) in headers {
        request.push_str(&format!("{name}: {value}\r\n"));
    }
    if let Some(body) = body {
        request.push_str(&format!("Content-Length: {}\r\n", body.len()));
    }
    request.push_str("\r\n");
    if let Some(body) = body {
        request.push_str(body);
    }
    stream
        .write_all(request.as_bytes())
        .await
        .expect("write the raw request");
    let mut raw = Vec::new();
    stream
        .read_to_end(&mut raw)
        .await
        .expect("read the raw response");
    let text = String::from_utf8_lossy(&raw).into_owned();
    let status_line = text.lines().next().expect("a status line");
    let status: u16 = status_line
        .split_whitespace()
        .nth(1)
        .and_then(|code| code.parse().ok())
        .expect("a status code");
    let body = text.split("\r\n\r\n").nth(1).unwrap_or_default().to_owned();
    (status, body)
}

/// A free 127.0.0.1 port, picked by binding and dropping a listener —
/// the subprocess tests' way to name a port in the config file.
fn pick_free_port() -> u16 {
    std::net::TcpListener::bind("127.0.0.1:0")
        .expect("pick a port")
        .local_addr()
        .expect("the picked port")
        .port()
}

// --- the token gate (issue #24 point 2/3; ticket checklist item 1) ---

#[tokio::test]
async fn the_token_gate_refuses_everything_without_credentials() {
    let env = Env::empty();
    let running = running_daemon_with_face(&env, false).await;

    // No token, no cookie: every /v1 request is refused 401 — reads and
    // state changes and the event stream alike.
    let (status, code, message) =
        error_of(get(&running.base_url, "/v1/status", None, None).await).await;
    assert_eq!(status, reqwest::StatusCode::UNAUTHORIZED);
    assert_eq!(code, "no_session", "the gate's own words: {message}");

    let response = post_json(&running.base_url, "/v1/push", None, serde_json::json!({})).await;
    assert_eq!(response.status(), reqwest::StatusCode::UNAUTHORIZED);

    let response = get(&running.base_url, "/v1/events", None, None).await;
    assert_eq!(response.status(), reqwest::StatusCode::UNAUTHORIZED);

    // At the default localhost bind the HTML shell is anonymous: it
    // renders no data (everything comes from the gated /v1 endpoints),
    // so there is no anonymous *view*.
    let response = get(&running.base_url, "/", None, None).await;
    assert_eq!(response.status(), reqwest::StatusCode::OK);

    running.stop().await;
}

#[tokio::test]
async fn the_token_header_grants_access_and_a_wrong_one_is_refused() {
    let env = Env::empty();
    let running = running_daemon_with_face(&env, false).await;

    // The API-client path: X-Management-Token on every endpoint.
    let response = get(&running.base_url, "/v1/status", Some(&running.token), None).await;
    assert_eq!(response.status(), reqwest::StatusCode::OK);
    let snapshot: serde_json::Value = response.json().await.expect("the snapshot is JSON");
    assert!(
        snapshot["pid"].is_u64() && snapshot["subscriptions"].is_object(),
        "the snapshot is the daemon's own vocabulary: {snapshot}"
    );

    // A wrong token is refused with the guard's words — and, unlike a
    // missing one, it counts as a failed presentation.
    let (status, code, _) =
        error_of(get(&running.base_url, "/v1/status", Some("not-the-token"), None).await).await;
    assert_eq!(status, reqwest::StatusCode::UNAUTHORIZED);
    assert_eq!(code, "invalid_token");

    running.stop().await;
}

// --- the LAN posture (issue #24 point 2; ticket checklist item 1) ---

#[tokio::test]
async fn the_lan_posture_gates_the_page_too() {
    let env = Env::empty();
    let running = running_daemon_with_face(&env, true).await;

    // LAN-bound: every request requires the token — the page included,
    // no anonymous view.
    let (status, code, _) = error_of(get(&running.base_url, "/", None, None).await).await;
    assert_eq!(status, reqwest::StatusCode::UNAUTHORIZED);
    assert_eq!(code, "no_session");
    let (status, _, _) = error_of(get(&running.base_url, "/assets/app.js", None, None).await).await;
    assert_eq!(status, reqwest::StatusCode::UNAUTHORIZED);

    // With the token the page serves (and so does everything else).
    let response = get(&running.base_url, "/", Some(&running.token), None).await;
    assert_eq!(response.status(), reqwest::StatusCode::OK);

    running.stop().await;
}

// --- the session exchange (issue #24 point 3; ticket checklist item 2) ---

#[tokio::test]
async fn the_session_exchange_sets_the_cookie_and_it_grants_access() {
    let env = Env::empty();
    let running = running_daemon_with_face(&env, false).await;

    // sign_in returns the cookie's name=value form, as the Cookie
    // header carries it.
    let cookie_header = sign_in(&running.base_url, &running.token).await;

    // The page path — navigation and the SSE stream (EventSource cannot
    // send custom headers) — rides the cookie.
    let response = get(&running.base_url, "/v1/status", None, Some(&cookie_header)).await;
    assert_eq!(response.status(), reqwest::StatusCode::OK);
    let response = get(&running.base_url, "/v1/events", None, Some(&cookie_header)).await;
    assert_eq!(response.status(), reqwest::StatusCode::OK);

    // A fabricated cookie value is not a session.
    let (status, code, _) = error_of(
        get(
            &running.base_url,
            "/v1/status",
            None,
            Some("bark_echo_session=forged"),
        )
        .await,
    )
    .await;
    assert_eq!(status, reqwest::StatusCode::UNAUTHORIZED);
    assert_eq!(code, "no_session");

    running.stop().await;
}

// --- the brute-force guard (issue #24 point 5; ticket checklist item 7) ---

#[tokio::test]
async fn the_brute_force_guard_engages_after_five_consecutive_failures() {
    let env = Env::empty();
    let running = running_daemon_with_face(&env, false).await;
    let client = reqwest::Client::new();
    let attempt = |token: String| {
        let client = client.clone();
        let base_url = running.base_url.clone();
        async move {
            let started = Instant::now();
            // The custom header clears the CSRF gate so the wrong-token
            // attempts reach the guard — what the page's sign-in does.
            let response = client
                .post(format!("{base_url}/v1/session"))
                .header("X-Requested-With", "bark-echo-face")
                .json(&serde_json::json!({ "token": token }))
                .send()
                .await
                .expect("POST /v1/session");
            (response.status(), started.elapsed())
        }
    };

    // Five consecutive failures answer at once; each failure after that
    // adds 1 s (in-memory only).
    for _ in 0..5 {
        let (status, elapsed) = attempt("wrong".to_owned()).await;
        assert_eq!(status, reqwest::StatusCode::UNAUTHORIZED);
        assert!(
            elapsed < Duration::from_millis(800),
            "the first five failures carry no delay: {elapsed:?}"
        );
    }
    let (status, elapsed) = attempt("wrong".to_owned()).await;
    assert_eq!(status, reqwest::StatusCode::UNAUTHORIZED);
    assert!(
        elapsed >= Duration::from_millis(900),
        "the sixth failure rides the 1 s delay: {elapsed:?}"
    );

    // A success resets the counter — consecutive means consecutive.
    let (status, elapsed) = attempt(running.token.clone()).await;
    assert_eq!(status, reqwest::StatusCode::NO_CONTENT);
    assert!(
        elapsed < Duration::from_millis(800),
        "the correct token is not delayed: {elapsed:?}"
    );
    let (status, elapsed) = attempt("wrong".to_owned()).await;
    assert_eq!(status, reqwest::StatusCode::UNAUTHORIZED);
    assert!(
        elapsed < Duration::from_millis(800),
        "the counter reset on success: {elapsed:?}"
    );

    running.stop().await;
}

// --- the CSRF gates (issue #24 point 3; ticket checklist item 3) ---

#[tokio::test]
async fn the_csrf_gates_refuse_cross_site_state_changes() {
    let env = Env::empty();
    let running = running_daemon_with_face(&env, false).await;
    let addr = running
        .base_url
        .strip_prefix("http://")
        .expect("an http url")
        .to_owned();
    let body = serde_json::json!({ "subscription_id": "x", "device_key": "y" }).to_string();

    // A cross-site Origin (a DNS-rebinding shape): the daemon's own
    // token in the header cannot make the state change legitimate.
    let (status, body_text) = raw_request(
        addr.parse().expect("addr"),
        "POST",
        "/v1/push",
        &addr,
        &[
            ("Content-Type", "application/json"),
            ("X-Management-Token", &running.token),
            ("Origin", "http://evil.example"),
        ],
        Some(&body),
    )
    .await;
    assert_eq!(status, 403, "{body_text}");
    assert!(
        body_text.contains("origin_rejected"),
        "the refusal names the gate: {body_text}"
    );

    // A cross-site Referer alone (older browsers) is refused the same
    // way.
    let (status, body_text) = raw_request(
        addr.parse().expect("addr"),
        "POST",
        "/v1/push",
        &addr,
        &[
            ("Content-Type", "application/json"),
            ("X-Management-Token", &running.token),
            ("Referer", "http://evil.example/some/page"),
        ],
        Some(&body),
    )
    .await;
    assert_eq!(status, 403, "{body_text}");
    assert!(body_text.contains("origin_rejected"), "{body_text}");

    // A foreign Host — the request names some other host than the face
    // is served on (Transmission/qBittorrent-style Host checks).
    let (status, body_text) = raw_request(
        addr.parse().expect("addr"),
        "POST",
        "/v1/push",
        "evil.example:8081",
        &[
            ("Content-Type", "application/json"),
            ("X-Management-Token", &running.token),
        ],
        Some(&body),
    )
    .await;
    assert_eq!(status, 403, "{body_text}");
    assert!(
        body_text.contains("host_rejected"),
        "the refusal names the gate: {body_text}"
    );

    // Same-origin, authenticated by the session cookie, but no custom
    // header: a plain cross-site-style form post is refused — the
    // custom-header requirement is the preflight-proof layer.
    let cookie_header = sign_in(&running.base_url, &running.token).await;
    let (status, body_text) = raw_request(
        addr.parse().expect("addr"),
        "POST",
        "/v1/push",
        &addr,
        &[
            ("Content-Type", "application/json"),
            ("Cookie", &cookie_header),
            ("Origin", &format!("http://{addr}")),
        ],
        Some(&body),
    )
    .await;
    assert_eq!(status, 403, "{body_text}");
    assert!(
        body_text.contains("csrf_rejected"),
        "the refusal names the gate: {body_text}"
    );

    running.stop().await;
}

#[tokio::test]
async fn the_session_exchange_runs_the_csrf_gates() {
    let env = Env::empty();
    let running = running_daemon_with_face(&env, false).await;
    let addr = running
        .base_url
        .strip_prefix("http://")
        .expect("an http url")
        .to_owned();
    let body = serde_json::json!({ "token": &running.token }).to_string();

    // The exchange mints a session — a state change: without the custom
    // header it is refused before the body is even parsed (a wrong
    // token would earn 401; this shape earns 403 first).
    let (status, body_text) = raw_request(
        addr.parse().expect("addr"),
        "POST",
        "/v1/session",
        &addr,
        &[("Content-Type", "application/json")],
        Some(&body),
    )
    .await;
    assert_eq!(status, 403, "{body_text}");
    assert!(
        body_text.contains("csrf_rejected"),
        "the refusal names the gate: {body_text}"
    );

    // The API-client path (the token header doubles as the custom
    // header) with a cross-site Origin: refused.
    let (status, body_text) = raw_request(
        addr.parse().expect("addr"),
        "POST",
        "/v1/session",
        &addr,
        &[
            ("Content-Type", "application/json"),
            ("X-Management-Token", &running.token),
            ("Origin", "http://evil.example"),
        ],
        Some(&body),
    )
    .await;
    assert_eq!(status, 403, "{body_text}");
    assert!(body_text.contains("origin_rejected"), "{body_text}");

    // A foreign Host on the exchange: refused like any state change.
    let (status, body_text) = raw_request(
        addr.parse().expect("addr"),
        "POST",
        "/v1/session",
        "evil.example:8081",
        &[
            ("Content-Type", "application/json"),
            ("X-Management-Token", &running.token),
        ],
        Some(&body),
    )
    .await;
    assert_eq!(status, 403, "{body_text}");
    assert!(body_text.contains("host_rejected"), "{body_text}");

    // Sanity: the page's own shape (X-Requested-With, same origin)
    // exchanges for a session.
    let (status, body_text) = raw_request(
        addr.parse().expect("addr"),
        "POST",
        "/v1/session",
        &addr,
        &[
            ("Content-Type", "application/json"),
            ("X-Requested-With", "bark-echo-face"),
        ],
        Some(&body),
    )
    .await;
    assert_eq!(status, 204, "{body_text}");

    running.stop().await;
}

#[tokio::test]
async fn cross_site_reads_are_refused_and_same_origin_reads_pass() {
    let env = Env::empty();
    let running = running_daemon_with_face(&env, false).await;
    let addr = running
        .base_url
        .strip_prefix("http://")
        .expect("an http url")
        .to_owned();

    // A cross-site Origin on a plain read (a DNS-rebinding shape): the
    // Host/Origin/Referer layer runs on every /v1 request, reads
    // included — the token in the header cannot make it legitimate.
    let response = reqwest::Client::new()
        .get(format!("{}/v1/status", running.base_url))
        .header("X-Management-Token", &running.token)
        .header("Origin", "http://evil.example")
        .send()
        .await
        .expect("GET /v1/status with a foreign Origin");
    assert_eq!(response.status(), reqwest::StatusCode::FORBIDDEN);
    let body: serde_json::Value = response.json().await.expect("the refusal");
    assert_eq!(body["error"]["code"], "origin_rejected", "{body}");

    let response = reqwest::Client::new()
        .get(format!("{}/v1/log/absent", running.base_url))
        .header("X-Management-Token", &running.token)
        .header("Origin", "http://evil.example")
        .send()
        .await
        .expect("GET /v1/log with a foreign Origin");
    assert_eq!(response.status(), reqwest::StatusCode::FORBIDDEN);

    // Same-origin reads pass: the browser names the face's own address.
    let (status, body_text) = raw_request(
        addr.parse().expect("addr"),
        "GET",
        "/v1/status",
        &addr,
        &[
            ("X-Management-Token", &running.token),
            ("Origin", &format!("http://{addr}")),
        ],
        None,
    )
    .await;
    assert_eq!(status, 200, "{body_text}");

    running.stop().await;
}

// --- the bind surface (issue #24 point 2; ticket checklist item 7) ---

#[tokio::test]
async fn the_config_default_binds_localhost_8081() {
    // Issue #24: bark-server owns 8080; the face takes 8081, localhost
    // only, until the explicit LAN opt-in.
    let config = Config::default();
    assert_eq!(config.management.bind, "127.0.0.1:8081");
    assert!(!config.management.bind_lan);
}

#[tokio::test]
async fn a_squatted_management_port_fails_loud_naming_the_config() {
    let port = pick_free_port();
    let squatter = std::net::TcpListener::bind(("127.0.0.1", port)).expect("squat the port");
    let harness = common::Harness::new();
    let mut config = Config::load(&harness.paths().config_file()).expect("config loads");
    config.management.bind = format!("127.0.0.1:{port}");
    config
        .save(&harness.paths().config_file())
        .expect("config saved");

    let output = harness.run(&["--plain-secrets", "run"]).await;
    drop(squatter);
    assert!(
        !output.success,
        "a squatted port must refuse to start; stdout: {}",
        output.stdout
    );
    assert!(
        output.stderr.contains("[management] bind"),
        "the failure names the config key: {}",
        output.stderr
    );
    assert!(
        output.stderr.contains(&format!("127.0.0.1:{port}")),
        "the failure names the address: {}",
        output.stderr
    );
}

#[tokio::test]
async fn the_lan_opt_in_rewrites_the_bind_and_gates_the_page() {
    let port = pick_free_port();
    let harness = common::Harness::new();
    let mut config = Config::load(&harness.paths().config_file()).expect("config loads");
    config.management.bind = format!("127.0.0.1:{port}");
    config.management.bind_lan = true;
    config
        .save(&harness.paths().config_file())
        .expect("config saved");

    // The daemon runs as a real subprocess: the LAN opt-in widens the
    // bind to all interfaces (the configured port kept) and gates every
    // request, the page included.
    let mut daemon = harness
        .spawn(&["--plain-secrets", "run"])
        .spawn()
        .expect("spawn run");
    let base_url = format!("http://127.0.0.1:{port}");

    // The token is minted at startup; `management token` prints it —
    // the CLI face of the same store (spec story 21).
    let deadline = Instant::now() + Duration::from_secs(15);
    let token = loop {
        let run = harness
            .run(&["--plain-secrets", "management", "token"])
            .await;
        if run.success {
            break run.stdout_or_die("management token").trim().to_owned();
        }
        assert!(
            Instant::now() < deadline,
            "the daemon never minted a token: {}",
            run.stderr
        );
        tokio::time::sleep(Duration::from_millis(50)).await;
    };

    // The page is gated: no anonymous view even on the LAN bind.
    let response = get(&base_url, "/", None, None).await;
    assert_eq!(response.status(), reqwest::StatusCode::UNAUTHORIZED);
    let response = get(&base_url, "/", Some(&token), None).await;
    assert_eq!(response.status(), reqwest::StatusCode::OK);

    daemon.kill().await.expect("kill the daemon");
}

// --- the /v1 API face (ticket checklist items 4-6): the CRUD, push,
// --- History Log, and SSE seams against the real daemon and the real
// --- embedded bark server ---

/// One subscriber's read of the face's event stream: raw chunks parsed
/// into the `data:` payloads (axum's SSE frames carry no `event:` line;
/// the `:`-prefixed keep-alives are not events). No total timeout — the
/// stream is long-lived; each read is deadline-bounded instead.
struct FaceEvents {
    res: reqwest::Response,
    buf: Vec<u8>,
}

impl FaceEvents {
    async fn open(base_url: &str, token: &str) -> Self {
        let res = reqwest::Client::new()
            .get(format!("{base_url}/v1/events"))
            .header("X-Management-Token", token)
            .send()
            .await
            .expect("open the event stream");
        assert_eq!(res.status(), reqwest::StatusCode::OK, "the stream opens");
        Self {
            res,
            buf: Vec::new(),
        }
    }

    /// The next event's JSON, or a panic naming `what` past the
    /// deadline — tests must not hang.
    async fn next_event(&mut self, what: &str) -> serde_json::Value {
        let deadline = tokio::time::Instant::now() + Duration::from_secs(15);
        loop {
            let remaining = deadline.saturating_duration_since(tokio::time::Instant::now());
            assert!(!remaining.is_zero(), "timed out waiting for {what}");
            let chunk = tokio::time::timeout(remaining, self.res.chunk())
                .await
                .expect("read deadline hit")
                .expect("stream read error")
                .expect("the stream ended before {what}");
            self.buf.extend_from_slice(&chunk);
            while let Some(pos) = find_frame_end(&self.buf) {
                let frame: Vec<u8> = self.buf.drain(..pos + 2).collect();
                let data: String = String::from_utf8_lossy(&frame)
                    .lines()
                    .filter_map(|line| line.strip_prefix("data: "))
                    .collect::<Vec<_>>()
                    .join("\n");
                if !data.is_empty() {
                    return serde_json::from_str(&data).expect("event data is JSON");
                }
            }
        }
    }
}

/// The index just past the next `\n\n` frame terminator, if any.
fn find_frame_end(buf: &[u8]) -> Option<usize> {
    buf.windows(2).position(|pair| pair == b"\n\n")
}

/// Wait until the event stream yields a `stream_state` for `id` whose
/// state's `kind` is `want`, consuming whatever comes first.
async fn wait_stream_state(events: &mut FaceEvents, id: &str, want: &str, what: &str) {
    let deadline = Instant::now() + Duration::from_secs(15);
    loop {
        let event = events.next_event(what).await;
        if event["type"] == "stream_state"
            && event["subscription_id"] == *id
            && event["state"]["kind"] == want
        {
            return;
        }
        assert!(
            Instant::now() < deadline,
            "the stream never flipped {id} to {want}: last {event}"
        );
    }
}

#[tokio::test]
async fn subscribe_add_hot_applies_and_the_flips_arrive_over_sse() {
    let server = common::TestServer::spawn().await;
    let env = Env::empty();
    let running = running_daemon_with_face(&env, false).await;

    // The stream is open before the add: every announcement lands.
    let mut events = FaceEvents::open(&running.base_url, &running.token).await;

    // The add is the wire's SubscribeAdd vocabulary 1:1: server, source
    // (`kind`-tagged, like the channel frames), label.
    let response = post_json(
        &running.base_url,
        "/v1/subscriptions",
        Some(&running.token),
        serde_json::json!({
            "server": server.base_url,
            "source": { "kind": "own" },
            "label": "phone",
        }),
    )
    .await;
    assert_eq!(response.status(), reqwest::StatusCode::OK);
    let added: serde_json::Value = response.json().await.expect("the added outcome");
    let id = added["id"].as_str().expect("the id").to_owned();
    assert_eq!(added["label"], "phone");
    assert_eq!(added["server"], server.base_url.as_str());
    assert!(
        added["token_hint"].as_str().expect("hint").ends_with("..."),
        "the hint names the token without exposing it: {added}"
    );
    assert!(
        added["device_key"].as_str().expect("key").len() >= 8,
        "the Device Key — the push target — rides the answer: {added}"
    );

    // "Runs immediately" is literal: the Starting flip, then the
    // Running one, arrive over /v1/events.
    wait_stream_state(&mut events, &id, "starting", "the starting flip").await;
    wait_stream_state(&mut events, &id, "running", "the running flip").await;

    // The status snapshot carries the line with its public identity —
    // the same vocabulary the channel answers with.
    let response = get(&running.base_url, "/v1/status", Some(&running.token), None).await;
    let snapshot: serde_json::Value = response.json().await.expect("snapshot");
    let line = &snapshot["subscriptions"][&id];
    assert_eq!(line["label"], "phone");
    assert_eq!(line["server"], server.base_url.as_str());
    assert_eq!(line["state"]["kind"], "running");
    assert_eq!(line["token_hint"], added["token_hint"]);

    // The single writer kept the files agreeing: the config holds the
    // Subscription, the store holds its two secrets.
    let config = Config::load(&env.paths.config_file()).expect("config parses");
    assert_eq!(config.subscriptions.len(), 1);
    assert_eq!(config.subscriptions[0].id, id);
    let key = SecretKey::for_subscription(&server.base_url, &id);
    assert!(
        env.store.get(&key, SecretKind::DeviceKey).is_ok(),
        "the Device Key is in the store"
    );
    assert!(
        env.store.get(&key, SecretKind::StreamToken).is_ok(),
        "the Stream Token is in the store"
    );

    running.stop().await;
    server.shutdown().await;
}

#[tokio::test]
async fn subscribe_remove_hot_applies_and_the_removal_is_announced() {
    let server = common::TestServer::spawn().await;
    let (device_key, secret, token_id) = register_and_mint(&server, "remove-me").await;
    let env = Env::with_subscription(&server.base_url, &device_key, &secret, &token_id, "gone");
    let running = running_daemon_with_face(&env, false).await;
    common::wait_active_streams(&server, 1).await;

    let mut events = FaceEvents::open(&running.base_url, &running.token).await;

    let response = reqwest::Client::new()
        .delete(format!("{}/v1/subscriptions/{token_id}", running.base_url))
        .header("X-Management-Token", &running.token)
        .send()
        .await
        .expect("DELETE the subscription");
    assert_eq!(response.status(), reqwest::StatusCode::OK);
    let removed: serde_json::Value = response.json().await.expect("the removed outcome");
    assert_eq!(removed["id"], token_id.as_str());
    assert_eq!(removed["label"], "gone");

    // The removal is announced unsolicited: watching clients drop the
    // line at once.
    let deadline = Instant::now() + Duration::from_secs(15);
    loop {
        let event = events.next_event("the removal announcement").await;
        if event["type"] == "subscription_removed" && event["subscription_id"] == *token_id {
            break;
        }
        assert!(
            Instant::now() < deadline,
            "the removal was never announced: last {event}"
        );
    }

    // The stream stopped (the server saw it close), the secrets are
    // erased, the config updated.
    common::wait_active_streams(&server, 0).await;
    let config = Config::load(&env.paths.config_file()).expect("config parses");
    assert!(config.subscriptions.is_empty(), "the config has no line");
    let key = SecretKey::for_subscription(&server.base_url, &token_id);
    assert!(
        env.store.get(&key, SecretKind::DeviceKey).is_err(),
        "the Device Key is erased"
    );
    assert!(
        env.store.get(&key, SecretKind::StreamToken).is_err(),
        "the Stream Token is erased"
    );

    running.stop().await;
    server.shutdown().await;
}

#[tokio::test]
async fn refusals_carry_the_daemons_own_words_in_the_stable_shape() {
    let env = Env::empty();
    let running = running_daemon_with_face(&env, false).await;

    // An empty label: refused before the server is touched.
    let (status, code, message) = error_of(
        post_json(
            &running.base_url,
            "/v1/subscriptions",
            Some(&running.token),
            serde_json::json!({
                "server": "http://127.0.0.1:9",
                "source": { "kind": "own" },
                "label": "   ",
            }),
        )
        .await,
    )
    .await;
    assert_eq!(status, reqwest::StatusCode::UNPROCESSABLE_ENTITY);
    assert_eq!(code, "refused");
    assert!(
        message.contains("label"),
        "the daemon's own words: {message}"
    );

    // An add against a dead server: the server plane's typed error
    // rides verbatim.
    let (status, code, message) = error_of(
        post_json(
            &running.base_url,
            "/v1/subscriptions",
            Some(&running.token),
            serde_json::json!({
                "server": "http://127.0.0.1:9",
                "source": { "kind": "own" },
                "label": "dead",
            }),
        )
        .await,
    )
    .await;
    assert_eq!(status, reqwest::StatusCode::UNPROCESSABLE_ENTITY);
    assert_eq!(code, "refused");
    assert!(!message.is_empty(), "the refusal says what happened");

    // A removal naming no held Subscription: 404, the daemon's words.
    let (status, code, message) = error_of(
        reqwest::Client::new()
            .delete(format!("{}/v1/subscriptions/absent", running.base_url))
            .header("X-Management-Token", &running.token)
            .send()
            .await
            .expect("DELETE an unknown id"),
    )
    .await;
    assert_eq!(status, reqwest::StatusCode::NOT_FOUND);
    assert_eq!(code, "refused");
    assert!(
        message.contains("no Subscription with id `absent`"),
        "the daemon's own words: {message}"
    );

    // A History Log tail naming no held Subscription: refused too —
    // no path outside the daemon's own logs is ever served.
    let (status, _, message) = error_of(
        get(
            &running.base_url,
            "/v1/log/absent",
            Some(&running.token),
            None,
        )
        .await,
    )
    .await;
    assert_eq!(status, reqwest::StatusCode::NOT_FOUND);
    assert!(
        message.contains("no Subscription with id `absent`"),
        "the daemon's own words: {message}"
    );

    // A send naming no held Subscription: refused before any HTTP —
    // the Composer's own words.
    let (status, _, message) = error_of(
        post_json(
            &running.base_url,
            "/v1/push",
            Some(&running.token),
            serde_json::json!({
                "subscription_id": "absent",
                "device_key": "any",
                "body": "hello",
            }),
        )
        .await,
    )
    .await;
    assert_eq!(status, reqwest::StatusCode::NOT_FOUND);
    assert!(
        message.contains("no Subscription with id `absent`") && message.contains("Composer"),
        "the daemon's own words: {message}"
    );

    running.stop().await;
}

#[tokio::test]
async fn the_history_log_tail_serves_the_log_files_own_lines_live() {
    let server = common::TestServer::spawn().await;
    let (device_key, secret, token_id) = register_and_mint(&server, "logger").await;
    let env = Env::with_subscription(&server.base_url, &device_key, &secret, &token_id, "logger");
    let running = running_daemon_with_face(&env, false).await;
    common::wait_active_streams(&server, 1).await;

    // The stream is open before the Push lands: the hub does not replay
    // events a subscriber joined too late for.
    let mut events = FaceEvents::open(&running.base_url, &running.token).await;

    // A Push lands through the server plane; the daemon's stream
    // processes it into the History Log, and the append is announced.
    let response = reqwest::Client::new()
        .post(format!("{}/push", server.base_url))
        .json(&serde_json::json!({ "device_key": device_key, "body": "logged-body" }))
        .send()
        .await
        .expect("push to the server");
    let envelope: serde_json::Value = response.json().await.expect("envelope");
    assert_eq!(envelope["code"], 200, "{envelope}");
    let deadline = Instant::now() + Duration::from_secs(15);
    let appended = loop {
        let event = events.next_event("the history append").await;
        if event["type"] == "history_appended" && event["subscription_id"] == *token_id {
            break event;
        }
        assert!(
            Instant::now() < deadline,
            "the append was never announced: last {event}"
        );
    };
    assert_eq!(appended["entry"]["body"], "logged-body");
    assert_eq!(appended["label"], "logger");

    // The tail endpoint answers with the same lines, oldest last —
    // snapshot and live appends are one vocabulary.
    let response = get(
        &running.base_url,
        &format!("/v1/log/{token_id}?tail=10"),
        Some(&running.token),
        None,
    )
    .await;
    assert_eq!(response.status(), reqwest::StatusCode::OK);
    let tail: serde_json::Value = response.json().await.expect("the tail");
    assert_eq!(tail["subscription_id"], token_id.as_str());
    let entries = tail["entries"].as_array().expect("entries");
    assert_eq!(entries.len(), 1);
    assert_eq!(entries[0]["body"], "logged-body");
    assert_eq!(entries[0]["title"], appended["entry"]["title"]);

    running.stop().await;
    server.shutdown().await;
}

#[tokio::test]
async fn push_goes_through_the_subscriptions_stored_credentials_end_to_end() {
    let server = common::TestServer::spawn().await;
    let (device_key, secret, token_id) = register_and_mint(&server, "sender").await;
    let env = Env::with_subscription(&server.base_url, &device_key, &secret, &token_id, "sender");
    let running = running_daemon_with_face(&env, false).await;
    common::wait_active_streams(&server, 1).await;

    let mut events = FaceEvents::open(&running.base_url, &running.token).await;

    // The send rides the Subscription's stored context (ADR-0004): the
    // Subscription names the server, the Device Key is explicit, no
    // credential rides the request.
    let response = post_json(
        &running.base_url,
        "/v1/push",
        Some(&running.token),
        serde_json::json!({
            "subscription_id": token_id,
            "device_key": device_key,
            "title": "From the face",
            "body": "sent-body",
        }),
    )
    .await;
    assert_eq!(response.status(), reqwest::StatusCode::OK);
    let outcome: serde_json::Value = response.json().await.expect("the verdict");
    assert_eq!(outcome["kind"], "sent", "{outcome}");

    // The daemon's own stream receives the Push and logs it — the
    // append is announced like any other processed Push.
    let deadline = Instant::now() + Duration::from_secs(15);
    loop {
        let event = events.next_event("the sent push's append").await;
        if event["type"] == "history_appended"
            && event["subscription_id"] == *token_id
            && event["entry"]["body"] == "sent-body"
        {
            break;
        }
        assert!(
            Instant::now() < deadline,
            "the sent push never landed: last {event}"
        );
    }

    // A send the server refuses: the verdict is verbatim — code and
    // message — never a paraphrase.
    let response = post_json(
        &running.base_url,
        "/v1/push",
        Some(&running.token),
        serde_json::json!({
            "subscription_id": token_id,
            "device_key": "no-such-device-key",
            "body": "lost",
        }),
    )
    .await;
    assert_eq!(response.status(), reqwest::StatusCode::OK);
    let outcome: serde_json::Value = response.json().await.expect("the verdict");
    assert_eq!(outcome["kind"], "rejected", "{outcome}");
    assert_eq!(outcome["code"], 400);
    assert!(
        outcome["message"]
            .as_str()
            .expect("message")
            .contains("device token"),
        "the server's own words: {outcome}"
    );

    running.stop().await;
    server.shutdown().await;
}

#[tokio::test]
async fn a_channel_rotate_kills_the_token_and_every_session_instantly() {
    let env = Env::empty();
    let running = running_daemon_with_face(&env, false).await;
    let cookie = sign_in(&running.base_url, &running.token).await;

    // The rotation rides the daemon's own channel — exactly what the
    // CLI's `management rotate` does when a daemon holds the lock.
    let mut client = echo_ipc::Client::connect(&env.paths.ipc_endpoint())
        .await
        .expect("connect to the channel");
    client.management_rotate().await.expect("rotate");

    // The old token is dead at once; so is the session issued under it.
    let response = get(&running.base_url, "/v1/status", Some(&running.token), None).await;
    assert_eq!(
        response.status(),
        reqwest::StatusCode::UNAUTHORIZED,
        "the old token is dead"
    );
    let response = get(&running.base_url, "/v1/status", None, Some(&cookie)).await;
    assert_eq!(
        response.status(),
        reqwest::StatusCode::UNAUTHORIZED,
        "the old session is dead"
    );

    // The new token — the one the store now holds, the one `management
    // token` would print — works, and exchanges for a fresh session.
    let new_token = match echo_daemon::management::stored_token(env.store.as_ref()) {
        Ok(token) => token,
        Err(err) => panic!("the rotated token is readable: {err}"),
    };
    assert_ne!(new_token, running.token, "the rotation replaced it");
    let response = get(&running.base_url, "/v1/status", Some(&new_token), None).await;
    assert_eq!(response.status(), reqwest::StatusCode::OK);
    sign_in(&running.base_url, &new_token).await;

    running.stop().await;
}

#[tokio::test]
async fn the_event_stream_refuses_a_cross_site_origin() {
    let env = Env::empty();
    let running = running_daemon_with_face(&env, false).await;

    // EventSource rides the cookie — a cross-site Origin on the stream
    // is refused like any other state-adjacent door.
    let response = reqwest::Client::new()
        .get(format!("{}/v1/events", running.base_url))
        .header("X-Management-Token", &running.token)
        .header("Origin", "http://evil.example")
        .send()
        .await
        .expect("GET /v1/events with a foreign Origin");
    assert_eq!(response.status(), reqwest::StatusCode::FORBIDDEN);
    let body: serde_json::Value = response.json().await.expect("the refusal");
    assert_eq!(body["error"]["code"], "origin_rejected", "{body}");

    running.stop().await;
}
