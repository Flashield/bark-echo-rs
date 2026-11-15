//! The `/v1/*` HTTP face: JSON endpoints plus one SSE stream, with the
//! browser-session mechanics from ticket #24's resolution comment —
//! the token is the root credential, `POST /v1/session` exchanges it
//! for an HttpOnly + SameSite=Strict cookie, page navigation and SSE
//! ride the cookie, and state changes require a custom header (cross-site
//! requests cannot pass preflight) on top of SameSite=Strict and
//! Host/Origin validation.
//!
//! The HTML shell at `/` is served unauthenticated: it renders no data
//! (everything comes from gated `/v1/*` endpoints), so there is no
//! anonymous *view* — the token gate lives in the page. The real daemon
//! binds `127.0.0.1:8081` by default and takes an explicit opt-in for
//! LAN; this prototype hard-binds `0.0.0.0:8081` so the driver can open
//! it from a phone, which is acceptable only because every byte it
//! serves is mock data.

use std::collections::BTreeMap;
use std::sync::Arc;
use std::sync::atomic::Ordering;
use std::time::{Duration, Instant, SystemTime, UNIX_EPOCH};

use axum::Json;
use axum::extract::{Path, Query, State};
use axum::http::{HeaderMap, StatusCode, header};
use axum::response::sse::{Event, KeepAlive, Sse};
use axum::response::{Html, IntoResponse, Response};
use futures_core::Stream;
use serde::Deserialize;
use serde_json::json;
use tokio::sync::broadcast;

use crate::mock::{
    LogEntry, MANAGEMENT_TOKEN, MockState, SESSION_COOKIE, SESSION_TTL_SECS, StreamState,
};

/// The stable JSON error shape (ADR-0005's error-shape bullet).
fn err(status: StatusCode, code: &str, message: impl Into<String>) -> Response {
    (
        status,
        Json(json!({ "error": { "code": code, "message": message.into() } })),
    )
        .into_response()
}

/// Constant-time token compare (#24 point 5). Length is not kept
/// secret; for a single fixed token that leaks nothing that matters.
fn ct_eq(a: &[u8], b: &[u8]) -> bool {
    a.len() == b.len() && a.iter().zip(b).fold(0u8, |acc, (x, y)| acc | (x ^ y)) == 0
}

/// Parse one cookie out of the `Cookie` header (one cookie is all the
/// face uses; no cookie-jar dependency).
fn cookie_value(headers: &HeaderMap, name: &str) -> Option<String> {
    let raw = headers.get(header::COOKIE)?.to_str().ok()?;
    raw.split(';').find_map(|pair| {
        let (k, v) = pair.trim().split_once('=')?;
        (k.trim() == name).then(|| v.trim().to_string())
    })
}

/// Host/Origin validation (#24 point 3, also covering SSE — EventSource
/// cannot send custom headers). Non-browser clients send no Origin.
fn origin_matches_host(headers: &HeaderMap) -> bool {
    let Some(host) = headers.get(header::HOST).and_then(|h| h.to_str().ok()) else {
        return false;
    };
    match headers.get(header::ORIGIN).and_then(|o| o.to_str().ok()) {
        None => true,
        Some(origin) => origin
            .strip_prefix("http://")
            .or_else(|| origin.strip_prefix("https://"))
            .is_some_and(|authority| authority == host),
    }
}

/// Authenticate a read: the token header (API-client path) or the
/// session cookie (page path, including EventSource).
fn authorize(state: &Arc<MockState>, headers: &HeaderMap) -> Result<(), Box<Response>> {
    if let Some(token) = headers
        .get("x-management-token")
        .and_then(|v| v.to_str().ok())
    {
        if ct_eq(token.as_bytes(), MANAGEMENT_TOKEN.as_bytes()) {
            return Ok(());
        }
        return Err(Box::new(err(
            StatusCode::UNAUTHORIZED,
            "invalid_token",
            "management token rejected",
        )));
    }
    if let Some(sid) = cookie_value(headers, SESSION_COOKIE) {
        let mut sessions = state.sessions.lock().unwrap();
        match sessions.get(&sid) {
            Some(expiry) if *expiry > Instant::now() => return Ok(()),
            Some(_) => {
                sessions.remove(&sid);
            }
            None => {}
        }
    }
    Err(Box::new(err(
        StatusCode::UNAUTHORIZED,
        "no_session",
        "no valid session cookie or management token",
    )))
}

/// Authenticate a state change: reads-auth plus the CSRF layers —
/// Host/Origin validation and the custom-header requirement (a
/// cross-site fetch with a custom header cannot pass preflight, and
/// SameSite=Strict already keeps the cookie home).
fn authorize_change(state: &Arc<MockState>, headers: &HeaderMap) -> Result<(), Box<Response>> {
    authorize(state, headers)?;
    if !origin_matches_host(headers) {
        return Err(Box::new(err(
            StatusCode::FORBIDDEN,
            "origin_rejected",
            "state change rejected: Origin does not match Host",
        )));
    }
    let has_custom_header =
        headers.contains_key("x-management-token") || headers.contains_key("x-requested-with");
    if !has_custom_header {
        return Err(Box::new(err(
            StatusCode::FORBIDDEN,
            "csrf_rejected",
            "state changes require a custom header (X-Requested-With for the page, X-Management-Token for API clients)",
        )));
    }
    Ok(())
}

// --- assets (embedded; no node build chain, no templating engine) ---

pub async fn index() -> Html<&'static str> {
    Html(include_str!("../assets/index.html"))
}

pub async fn style() -> impl IntoResponse {
    (
        [(header::CONTENT_TYPE, "text/css; charset=utf-8")],
        include_str!("../assets/style.css"),
    )
}

pub async fn script() -> impl IntoResponse {
    (
        [(header::CONTENT_TYPE, "text/javascript; charset=utf-8")],
        include_str!("../assets/app.js"),
    )
}

// --- session (#24 point 3) ---

#[derive(Deserialize)]
pub struct SessionReq {
    pub token: String,
}

static SESSION_COUNTER: std::sync::atomic::AtomicU64 = std::sync::atomic::AtomicU64::new(0);

pub async fn session(State(state): State<Arc<MockState>>, Json(req): Json<SessionReq>) -> Response {
    if !ct_eq(req.token.as_bytes(), MANAGEMENT_TOKEN.as_bytes()) {
        // Brute-force guard: after 5 consecutive failures each failure
        // adds 1s of delay, in-memory only.
        let failures = state.failures.fetch_add(1, Ordering::SeqCst) + 1;
        if failures > 5 {
            tokio::time::sleep(Duration::from_secs((failures - 5).min(5) as u64)).await;
        }
        return err(
            StatusCode::UNAUTHORIZED,
            "invalid_token",
            "management token rejected",
        );
    }
    state.failures.store(0, Ordering::SeqCst);
    let nanos = SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .unwrap_or_default()
        .as_nanos();
    let counter = SESSION_COUNTER.fetch_add(1, Ordering::Relaxed);
    let sid = format!("{nanos:x}-{counter:x}");
    state.sessions.lock().unwrap().insert(
        sid.clone(),
        Instant::now() + Duration::from_secs(SESSION_TTL_SECS),
    );
    let mut headers = HeaderMap::new();
    headers.insert(
        header::SET_COOKIE,
        format!(
            "{SESSION_COOKIE}={sid}; HttpOnly; SameSite=Strict; Path=/; Max-Age={SESSION_TTL_SECS}"
        )
        .parse()
        .expect("cookie header is well-formed"),
    );
    (StatusCode::NO_CONTENT, headers).into_response()
}

// --- status snapshot (ADR-0004's `status`, mirrored 1:1) ---

pub async fn status(State(state): State<Arc<MockState>>, headers: HeaderMap) -> Response {
    if let Err(e) = authorize(&state, &headers) {
        return *e;
    }
    let inner = state.inner.lock().unwrap();
    let subscriptions: BTreeMap<&String, serde_json::Value> = inner
        .subs
        .iter()
        .map(|(id, line)| (id, line.runtime_json()))
        .collect();
    Json(json!({
        "pid": std::process::id(),
        "started_at": state.started_at,
        "subscriptions": subscriptions,
    }))
    .into_response()
}

// --- subscribe_add / subscribe_remove (ADR-0004's CRUD vocabulary) ---

#[derive(Deserialize)]
pub struct SubscribeAddReq {
    pub label: String,
    pub server: String,
    #[serde(default)]
    pub device_key: String,
    #[serde(default)]
    pub stream_token: String,
}

pub async fn subscribe_add(
    State(state): State<Arc<MockState>>,
    headers: HeaderMap,
    Json(req): Json<SubscribeAddReq>,
) -> Response {
    if let Err(e) = authorize_change(&state, &headers) {
        return *e;
    }
    // A refusal is an error response whose message is the reason.
    if req.label.trim().is_empty() {
        return err(
            StatusCode::UNPROCESSABLE_ENTITY,
            "refused",
            "the label is required",
        );
    }
    if req.server.trim().is_empty() {
        return err(
            StatusCode::UNPROCESSABLE_ENTITY,
            "refused",
            "the server URL is required",
        );
    }
    let sub_id = format!("mock-{:04}", state.next_sub.fetch_add(1, Ordering::Relaxed));
    let token_hint = if req.stream_token.is_empty() {
        "tk_generated...".to_string()
    } else {
        let chars: String = req.stream_token.chars().take(12).collect();
        format!("{chars}...")
    };
    let (label, server, device_key, token_hint) = {
        let mut inner = state.inner.lock().unwrap();
        inner.subs.insert(
            sub_id.clone(),
            crate::mock::SubLine {
                label: req.label.clone(),
                server: req.server.clone(),
                token_hint: token_hint.clone(),
                state: StreamState::Starting,
                last_message_id: None,
                updated_at: crate::mock::unix_now(),
                flaky: false,
            },
        );
        inner.logs.insert(sub_id.clone(), Default::default());
        (
            req.label.clone(),
            req.server.clone(),
            req.device_key.clone(),
            token_hint,
        )
    };
    // The add answers once the supervisor is spawned: the line starts at
    // `starting`, its flip to running arrives as the announcement. The
    // announcement is broadcast (a second watching tab grows the line in
    // place on the unknown id), and the acting tab also gets the
    // AddedSubscription fields back.
    state.set_stream_state(&sub_id, StreamState::Starting);
    crate::mock::spawn_handshake(state.clone(), sub_id.clone());
    Json(json!({
        "id": sub_id,
        "label": label,
        "server": server,
        "token_hint": token_hint,
        "device_key": device_key,
    }))
    .into_response()
}

pub async fn subscribe_remove(
    State(state): State<Arc<MockState>>,
    headers: HeaderMap,
    Path(sub_id): Path<String>,
) -> Response {
    if let Err(e) = authorize_change(&state, &headers) {
        return *e;
    }
    let removed = {
        let mut inner = state.inner.lock().unwrap();
        match inner.subs.remove(&sub_id) {
            Some(line) => {
                inner.logs.remove(&sub_id);
                line.label
            }
            None => {
                return err(
                    StatusCode::NOT_FOUND,
                    "refused",
                    format!("no subscription with id {sub_id}"),
                );
            }
        }
    };
    // A removal is announced unsolicited so watching clients drop the
    // line at once.
    state.hub.publish(&json!({
        "type": "subscription_removed",
        "subscription_id": sub_id,
    }));
    Json(json!({ "id": sub_id, "label": removed })).into_response()
}

// --- History Log tail ---

pub async fn log_tail(
    State(state): State<Arc<MockState>>,
    headers: HeaderMap,
    Path(sub_id): Path<String>,
    Query(query): Query<std::collections::HashMap<String, String>>,
) -> Response {
    if let Err(e) = authorize(&state, &headers) {
        return *e;
    }
    let inner = state.inner.lock().unwrap();
    let Some(log) = inner.logs.get(&sub_id) else {
        return err(
            StatusCode::NOT_FOUND,
            "refused",
            format!("no subscription with id {sub_id}"),
        );
    };
    let tail: usize = query
        .get("tail")
        .and_then(|t| t.parse().ok())
        .filter(|t| *t > 0)
        .unwrap_or(50);
    // Entries, oldest last (HistorySnapshot's order).
    let start = log.len().saturating_sub(tail);
    let entries: Vec<&LogEntry> = log.iter().skip(start).collect();
    Json(json!({
        "subscription_id": sub_id,
        "entries": entries,
    }))
    .into_response()
}

// --- Push Composer send (SendPush in, SendPushOutcome out) ---

#[derive(Deserialize)]
pub struct SendPushReq {
    pub subscription_id: String,
    #[serde(default)]
    pub device_key: String,
    pub title: Option<String>,
    pub subtitle: Option<String>,
    pub body: Option<String>,
    #[serde(default)]
    pub ext_params: BTreeMap<String, String>,
}

pub async fn push(
    State(state): State<Arc<MockState>>,
    headers: HeaderMap,
    Json(req): Json<SendPushReq>,
) -> Response {
    if let Err(e) = authorize_change(&state, &headers) {
        return *e;
    }
    // Ext Params resolution at the core seam: `url` wins, `copy` applies
    // only when there is no `url`, everything else is ignored.
    let click_action = match req.ext_params.get("url") {
        Some(url) if !url.is_empty() => {
            Some(crate::mock::ClickAction::OpenUrl { url: url.clone() })
        }
        _ => req
            .ext_params
            .get("copy")
            .filter(|c| !c.is_empty())
            .map(|text| crate::mock::ClickAction::Copy { text: text.clone() }),
    };
    let envelope;
    {
        let mut inner = state.inner.lock().unwrap();
        let Some(line) = inner.subs.get(&req.subscription_id) else {
            drop(inner);
            return err(
                StatusCode::NOT_FOUND,
                "refused",
                format!("no subscription with id {}", req.subscription_id),
            );
        };
        // The server's verdict on the send: a bad Device Key answers
        // 400 with its own words — the mock keeps that rejection shape.
        if req.device_key.trim().is_empty() {
            drop(inner);
            return Json(json!({
                "kind": "rejected",
                "code": 400,
                "message": "failed to get device token: empty device key",
            }))
            .into_response();
        }
        let id = line.last_message_id.unwrap_or(100) + 1;
        // The sent push lands in the log like any other processed Push,
        // with the prototype-only `sent` origin marker.
        let mut entry = line.received_entry(
            id,
            req.title.as_deref().unwrap_or(""),
            req.subtitle.as_deref().unwrap_or(""),
            req.body.as_deref().unwrap_or("(no content)"),
            click_action,
        );
        entry.origin = "sent".to_string();
        let sub_id = req.subscription_id.clone();
        let label = line.label.clone();
        let sub = inner.subs.get_mut(&sub_id).expect("checked above");
        sub.last_message_id = Some(id);
        sub.updated_at = crate::mock::unix_now();
        let log = inner.logs.entry(sub_id.clone()).or_default();
        log.push_back(entry.clone());
        if log.len() > 200 {
            log.pop_front();
        }
        envelope = json!({
            "type": "history_appended",
            "subscription_id": sub_id,
            "label": label,
            "entry": entry,
        });
    }
    state.hub.publish(&envelope);
    Json(json!({ "kind": "sent" })).into_response()
}

// --- the one SSE stream (#24 point 4: all realtime events ride it) ---

/// One subscriber's view of the hub. The hub is a broadcast channel;
/// each SSE connection gets a forwarder task piping into its own mpsc,
/// which this Stream polls — that skips the tokio-stream dependency
/// (`broadcast::Receiver` has no public `poll_recv`, `mpsc::Receiver`
/// does). A lagged client gets a `resync` event (refetch state) instead
/// of a replay — the mock keeps no event store.
struct HubStream {
    rx: tokio::sync::mpsc::Receiver<String>,
}

impl Stream for HubStream {
    type Item = Result<Event, std::convert::Infallible>;

    fn poll_next(
        self: std::pin::Pin<&mut Self>,
        cx: &mut std::task::Context<'_>,
    ) -> std::task::Poll<Option<Self::Item>> {
        let this = self.get_mut();
        match this.rx.poll_recv(cx) {
            std::task::Poll::Ready(Some(data)) => {
                std::task::Poll::Ready(Some(Ok(Event::default().data(data))))
            }
            std::task::Poll::Ready(None) => std::task::Poll::Ready(None),
            std::task::Poll::Pending => std::task::Poll::Pending,
        }
    }
}

pub async fn events(State(state): State<Arc<MockState>>, headers: HeaderMap) -> Response {
    if let Err(e) = authorize(&state, &headers) {
        return *e;
    }
    if !origin_matches_host(&headers) {
        return err(
            StatusCode::FORBIDDEN,
            "origin_rejected",
            "event stream rejected: Origin does not match Host",
        );
    }
    let mut hub_rx = state.hub.subscribe();
    let (tx, rx) = tokio::sync::mpsc::channel::<String>(64);
    // Forward until the page hangs up (dropping the response ends the
    // mpsc receiver; the send error is that hangup).
    tokio::spawn(async move {
        loop {
            match hub_rx.recv().await {
                Ok(data) => {
                    if tx.send(data).await.is_err() {
                        return;
                    }
                }
                Err(broadcast::error::RecvError::Lagged(n)) => {
                    let notice = json!({
                        "type": "resync",
                        "reason": format!("lagged behind by {n} events; refetch state"),
                    });
                    if tx.send(notice.to_string()).await.is_err() {
                        return;
                    }
                }
                Err(broadcast::error::RecvError::Closed) => return,
            }
        }
    });
    Sse::new(HubStream { rx })
        .keep_alive(KeepAlive::default())
        .into_response()
}
