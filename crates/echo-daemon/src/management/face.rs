//! The `/v1` HTTP face (issue #31): the JSON endpoints that mirror the
//! echo-ipc vocabulary 1:1 (ADR-0004's request/response and event
//! shapes — the same types, so the HTTP face cannot fork the channel's
//! semantics, spec story 30), one SSE stream carrying every realtime
//! event, and the HTML shell the web page (ticket #32) rides — with the
//! security gates from issue #24's resolution applied at every door.

use std::collections::HashMap;
use std::convert::Infallible;
use std::pin::Pin;
use std::task::{Context, Poll};

use axum::Json;
use axum::body::Bytes;
use axum::extract::{Path, Query, State};
use axum::http::header;
use axum::http::{HeaderMap, StatusCode};
use axum::response::sse::{Event, KeepAlive, Sse};
use axum::response::{Html, IntoResponse, Response};
use axum::routing::{Router, delete, get, post};
use echo_ipc::message::{AddSource, BasicAuthCredentials, SendPush};
use futures_core::Stream;
use serde::Deserialize;
use serde_json::json;
use tokio::sync::broadcast;

use super::FaceState;
use crate::run::{ApplyError, Manager, SendAttemptError, TailError};
use crate::subscribe::SubscribeError;

/// The face's state: the security state (token, sessions, guard) and the
/// single writer whose operations the endpoints mirror — the same
/// `Manager` the IPC channel's handlers call, so the HTTP face and the
/// channel cannot drift (ADR-0004: the daemon is the single writer).
/// `Clone` because axum's state is cloned per request (both fields are
/// `Arc`s, so the clone shares the one security state and writer).
#[derive(Clone)]
pub(crate) struct Face {
    pub(crate) security: std::sync::Arc<FaceState>,
    pub(crate) manager: std::sync::Arc<Manager>,
}

/// The router: the HTML shell and its assets (ticket #32 fills the
/// contents; the serving shape and the LAN gate live here), the session
/// exchange, and the `/v1` JSON API.
pub(crate) fn router(face: Face) -> Router {
    Router::new()
        .route("/", get(index))
        .route("/assets/style.css", get(style))
        .route("/assets/app.js", get(script))
        .route("/v1/session", post(session))
        .route("/v1/status", get(status))
        .route("/v1/events", get(events))
        .route("/v1/subscriptions", post(subscribe_add))
        .route("/v1/subscriptions/{id}", delete(subscribe_remove))
        .route("/v1/log/{id}", get(log_tail))
        .route("/v1/push", post(push))
        .with_state(face)
}

// --- the stable JSON error shape (ADR-0005's error-shape bullet) ---

/// Every refusal answers `{"error":{"code":"…","message":"…"}}` — the
/// shape a script or the future fleet layer can match on, with the
/// daemon's own words in the message (never a paraphrase).
fn err(status: StatusCode, code: &str, message: impl Into<String>) -> Response {
    (
        status,
        Json(json!({ "error": { "code": code, "message": message.into() } })),
    )
        .into_response()
}

/// Refuse a malformed JSON body with the stable error shape — never
/// axum's plain-text default, and never before the security gates: the
/// handlers take the raw bytes and parse them only after the gates ran,
/// so an unauthenticated or cross-site request is refused for what it
/// is (401/403) even when its body is garbage.
fn bad_request(error: serde_json::Error) -> Response {
    err(StatusCode::BAD_REQUEST, "bad_request", error.to_string())
}

/// Parse a JSON body after the gates: the one door every body-taking
/// handler goes through.
fn parse_body<T: serde::de::DeserializeOwned>(bytes: &[u8]) -> Result<T, Box<Response>> {
    serde_json::from_slice(bytes)
        .map_err(bad_request)
        .map_err(Box::new)
}

// --- the security gates (issue #24) ---

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

/// The `authority` (host[:port]) of an Origin/Referer URL, for the
/// same-origin check. `None` for anything that is not a scheme-prefixed
/// URL — an Origin of `null` (sandboxed documents) matches nothing.
fn authority_of(url: &str) -> Option<&str> {
    let rest = url
        .strip_prefix("http://")
        .or_else(|| url.strip_prefix("https://"))?;
    let end = rest.find(['/', '?', '#']).unwrap_or(rest.len());
    Some(&rest[..end])
}

/// The host part of a Host header authority, port stripped — the port
/// is not checked because an SSH tunnel may map any local port onto the
/// face (spec story 16), and the browser names the tunnel's port, not
/// the daemon's.
fn host_part(authority: &str) -> &str {
    if let Some(rest) = authority.strip_prefix('[') {
        // IPv6: `[::1]:8081` names the host `::1`.
        rest.split_once(']').map(|(host, _)| host).unwrap_or(rest)
    } else {
        authority
            .rsplit_once(':')
            .map(|(host, _)| host)
            .unwrap_or(authority)
    }
}

/// Whether `host_part` names the loopback — the only Host a browser
/// request may name when the face is localhost-bound. A DNS-rebinding
/// request names the attacker's own domain; this is the check that
/// refuses it (the Transmission/qBittorrent Host check, #24 point 3).
fn is_loopback_host(host_part: &str) -> bool {
    matches!(host_part, "localhost" | "127.0.0.1" | "::1")
}

/// The cross-site verdicts of the Host/Origin/Referer validation (#24
/// point 3): a missing Host is foreign; a Host outside the loopback set
/// is foreign while the face is localhost-bound (on a LAN bind the
/// callers may use any name or address — the token gates everything
/// there anyway); an Origin or Referer whose authority disagrees with
/// the Host is cross-site. Non-browser clients send neither Origin nor
/// Referer and pass; same-origin browsers agree. `Some(verdict)` is the
/// refused kind, `None` means the request is same-origin enough.
fn cross_site(security: &FaceState, headers: &HeaderMap) -> Option<&'static str> {
    let Some(host) = headers
        .get(header::HOST)
        .and_then(|host| host.to_str().ok())
    else {
        return Some("host");
    };
    if security.local_only() && !is_loopback_host(host_part(host)) {
        return Some("host");
    }
    if let Some(origin) = headers.get(header::ORIGIN).and_then(|o| o.to_str().ok())
        && authority_of(origin) != Some(host)
    {
        return Some("origin");
    }
    if let Some(referer) = headers.get(header::REFERER).and_then(|r| r.to_str().ok())
        && authority_of(referer) != Some(host)
    {
        return Some("referer");
    }
    None
}

/// The 403 a cross-site verdict earns, phrased for what was refused (a
/// "state change" or the "event stream").
fn cross_site_rejection(verdict: &str, what: &str) -> Response {
    match verdict {
        "host" => err(
            StatusCode::FORBIDDEN,
            "host_rejected",
            format!("{what} rejected: the request names a foreign Host"),
        ),
        which => err(
            StatusCode::FORBIDDEN,
            "origin_rejected",
            format!("{what} rejected: the {which} does not match the Host"),
        ),
    }
}

/// Authenticate a request (issue #24 point 3): the token header (the
/// API-client path — curl, scripts, the future fleet layer) or the
/// session cookie (the page path — including EventSource, which cannot
/// send custom headers). A failed token presentation rides the
/// brute-force guard's delay before the refusal (#24 point 5); a
/// missing-credentials request does not count — every anonymous page
/// visit must not inflate the guard.
async fn authorize(face: &Face, headers: &HeaderMap) -> Result<(), Box<Response>> {
    if let Some(presented) = headers
        .get("x-management-token")
        .and_then(|value| value.to_str().ok())
    {
        let current = face.security.current_token();
        if ct_eq(presented.as_bytes(), current.as_bytes()) {
            face.security.note_success();
            return Ok(());
        }
        face.security.penalize_failure().await;
        return Err(Box::new(err(
            StatusCode::UNAUTHORIZED,
            "invalid_token",
            "management token rejected",
        )));
    }
    if let Some(sid) = cookie_value(headers, super::SESSION_COOKIE)
        && face.security.session_is_live(&sid)
    {
        face.security.note_success();
        return Ok(());
    }
    Err(Box::new(err(
        StatusCode::UNAUTHORIZED,
        "no_session",
        "no valid session cookie or management token",
    )))
}

/// Authenticate a state change: reads-auth plus the CSRF layers —
/// Host/Origin/Referer validation and the custom-header requirement (a
/// cross-site fetch with a custom header cannot pass preflight, and
/// SameSite=Strict already keeps the cookie home; the page sends
/// `X-Requested-With`, API clients' `X-Management-Token` doubles as the
/// custom header).
async fn authorize_change(face: &Face, headers: &HeaderMap) -> Result<(), Box<Response>> {
    authorize(face, headers).await?;
    if let Some(verdict) = cross_site(&face.security, headers) {
        return Err(Box::new(cross_site_rejection(verdict, "state change")));
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

/// Authenticate the HTML shell and its assets: anonymous at the default
/// localhost bind (the shell renders no data — everything comes from the
/// gated `/v1/*` endpoints, so there is no anonymous *view*), but under
/// the LAN opt-in every request requires the token — the page included,
/// no anonymous view (issue #24 point 2).
async fn authorize_page(face: &Face, headers: &HeaderMap) -> Result<(), Box<Response>> {
    if face.security.page_is_gated() {
        return authorize(face, headers).await;
    }
    Ok(())
}

// --- the HTML shell and its assets (embedded; ticket #32 fills the
// --- contents, the serving shape and the gate live here) ---

async fn index(State(face): State<Face>, headers: HeaderMap) -> Response {
    if let Err(response) = authorize_page(&face, &headers).await {
        return *response;
    }
    Html(include_str!("../../assets/index.html")).into_response()
}

async fn style(State(face): State<Face>, headers: HeaderMap) -> Response {
    if let Err(response) = authorize_page(&face, &headers).await {
        return *response;
    }
    (
        [(header::CONTENT_TYPE, "text/css; charset=utf-8")],
        include_str!("../../assets/style.css"),
    )
        .into_response()
}

async fn script(State(face): State<Face>, headers: HeaderMap) -> Response {
    if let Err(response) = authorize_page(&face, &headers).await {
        return *response;
    }
    (
        [(header::CONTENT_TYPE, "text/javascript; charset=utf-8")],
        include_str!("../../assets/app.js"),
    )
        .into_response()
}

// --- the session exchange (issue #24 point 3) ---

/// The sign-in body: the token, as the page's first (and only) hold of
/// it. The response carries no token — the cookie is the session.
#[derive(Deserialize)]
pub struct SessionRequest {
    pub token: String,
}

/// `POST /v1/session`: exchange the token for an HttpOnly +
/// SameSite=Strict session cookie. A wrong token rides the brute-force
/// guard's delay before the refusal; a right one mints a session of the
/// current token generation — rotate invalidates it without a sweep.
async fn session(State(face): State<Face>, bytes: Bytes) -> Response {
    let request: SessionRequest = match parse_body(&bytes) {
        Ok(request) => request,
        Err(response) => return *response,
    };
    let current = face.security.current_token();
    if !ct_eq(request.token.as_bytes(), current.as_bytes()) {
        face.security.penalize_failure().await;
        return err(
            StatusCode::UNAUTHORIZED,
            "invalid_token",
            "management token rejected",
        );
    }
    face.security.note_success();
    let sid = face.security.issue_session();
    let ttl = super::SESSION_TTL_SECS;
    let cookie: axum::http::HeaderValue = format!(
        "{}={sid}; HttpOnly; SameSite=Strict; Path=/; Max-Age={ttl}",
        super::SESSION_COOKIE
    )
    .parse()
    .expect("the cookie header is well-formed");
    ([(header::SET_COOKIE, cookie)], StatusCode::NO_CONTENT).into_response()
}

// --- the status snapshot (ADR-0004's `status`, mirrored 1:1) ---

/// `GET /v1/status`: the live per-Subscription snapshot — the same
/// `StatusSnapshot` the channel answers and the runtime state file
/// holds, one vocabulary (spec story 30). Each Subscription's public
/// identity (server URL, token hint) rides the line; the secrets behind
/// the hints never leave the daemon (spec story 13).
async fn status(State(face): State<Face>, headers: HeaderMap) -> Response {
    if let Err(response) = authorize(&face, &headers).await {
        return *response;
    }
    Json(face.manager.snapshot()).into_response()
}

// --- subscribe_add / subscribe_remove (ADR-0004's CRUD vocabulary) ---

/// The add request's body: `Request::SubscribeAdd`'s fields verbatim —
/// the same `AddSource` and `BasicAuthCredentials` types the channel
/// carries, so the HTTP face mirrors the wire vocabulary 1:1.
#[derive(Deserialize)]
pub struct SubscribeAddRequest {
    /// The bark server base URL, e.g. `http://127.0.0.1:8080`.
    pub server: String,
    /// Where the Subscription's Device comes from.
    pub source: AddSource,
    /// The client label naming the Stream Token — required; the daemon
    /// refuses an empty one before touching the server.
    pub label: String,
    /// The operator Basic Auth credentials (§1.4) to present and store;
    /// omitted rides the already-stored credentials for that server.
    #[serde(default)]
    pub basic_auth: Option<BasicAuthCredentials>,
}

/// Map a hot-apply refusal onto the stable error shape: 404 when the id
/// names nothing, 422 for every other refusal — the message is always
/// the daemon's own words (the channel's error answer, verbatim).
fn refused(error: &ApplyError) -> Response {
    let unknown = matches!(
        error,
        ApplyError::Subscribe(SubscribeError::UnknownSubscription(_))
    );
    err(
        if unknown {
            StatusCode::NOT_FOUND
        } else {
            StatusCode::UNPROCESSABLE_ENTITY
        },
        "refused",
        error.to_string(),
    )
}

/// `POST /v1/subscriptions`: add a Subscription hot — register or mirror
/// a Device, mint a labelled Stream Token, start the Realtime Stream
/// immediately. The answer is the daemon's typed outcome
/// (`Response::SubscriptionAdded`'s fields); the new stream's
/// `Starting` flip arrives over `/v1/events` like any other.
async fn subscribe_add(State(face): State<Face>, headers: HeaderMap, bytes: Bytes) -> Response {
    if let Err(response) = authorize_change(&face, &headers).await {
        return *response;
    }
    let request: SubscribeAddRequest = match parse_body(&bytes) {
        Ok(request) => request,
        Err(response) => return *response,
    };
    match face
        .manager
        .add(
            &request.server,
            request.source,
            &request.label,
            request.basic_auth,
        )
        .await
    {
        Ok(added) => Json(echo_ipc::message::AddedSubscription {
            id: added.subscription.id,
            label: added.subscription.label,
            server: added.subscription.server,
            token_hint: added.subscription.token_hint,
            device_key: added.device_key,
        })
        .into_response(),
        Err(error) => refused(&error),
    }
}

/// `DELETE /v1/subscriptions/{id}`: remove one Subscription hot — its
/// stream stops, its secrets are erased, the config updated; the
/// `subscription_removed` announcement arrives over `/v1/events` for
/// every watching client.
async fn subscribe_remove(
    State(face): State<Face>,
    Path(id): Path<String>,
    headers: HeaderMap,
) -> Response {
    if let Err(response) = authorize_change(&face, &headers).await {
        return *response;
    }
    match face.manager.remove(&id).await {
        Ok(removed) => Json(echo_ipc::message::RemovedSubscription {
            id: removed.id,
            label: removed.label,
        })
        .into_response(),
        Err(error) => refused(&error),
    }
}

// --- the History Log tail (ticket #15's plane, mirrored) ---

/// `GET /v1/log/{id}?tail=N`: the recent tail of one Subscription's
/// History Log, oldest last — the content semantics of the CLI's
/// `log --tail N` and the channel's `HistorySnapshot` (the log file's
/// own line vocabulary, so snapshot and live appends render as one
/// thing). The log stays daemon-private; the id names a Subscription or
/// the request is refused.
async fn log_tail(
    State(face): State<Face>,
    Path(id): Path<String>,
    Query(query): Query<HashMap<String, String>>,
    headers: HeaderMap,
) -> Response {
    if let Err(response) = authorize(&face, &headers).await {
        return *response;
    }
    let tail: u32 = query
        .get("tail")
        .and_then(|tail| tail.parse().ok())
        .filter(|tail| *tail > 0)
        .unwrap_or(50);
    match face.manager.history_tail(&id, tail).await {
        Ok(entries) => Json(json!({ "subscription_id": id, "entries": entries })).into_response(),
        Err(error) => err(
            match error {
                TailError::Unknown(_) => StatusCode::NOT_FOUND,
                TailError::Read(_) => StatusCode::INTERNAL_SERVER_ERROR,
            },
            if matches!(error, TailError::Read(_)) {
                "internal"
            } else {
                "refused"
            },
            error.to_string(),
        ),
    }
}

// --- the Push Composer send (SendPush in, SendPushOutcome out) ---

/// `POST /v1/push`: one send on a Subscription's stored credentials per
/// ADR-0004 — the `SendPush` type the channel carries, verbatim, and
/// the bark server's verdict out. A send the daemon could not even
/// attempt (unknown Subscription, unreachable server) is refused with
/// the daemon's own words — no verdict, no lie.
async fn push(State(face): State<Face>, headers: HeaderMap, bytes: Bytes) -> Response {
    if let Err(response) = authorize_change(&face, &headers).await {
        return *response;
    }
    let send: SendPush = match parse_body(&bytes) {
        Ok(send) => send,
        Err(response) => return *response,
    };
    match face.manager.push(send).await {
        Ok(outcome) => Json(outcome).into_response(),
        Err(error) => {
            let (status, code) = match &error {
                SendAttemptError::Unknown(_) => (StatusCode::NOT_FOUND, "refused"),
                SendAttemptError::Unreachable(_) => (StatusCode::BAD_GATEWAY, "send_failed"),
            };
            err(status, code, error.to_string())
        }
    }
}

// --- the one SSE stream (issue #24 point 4: all realtime events ride it) ---

/// One subscriber's view of the daemon's event hub. The hub is the
/// broadcast channel the runtime's flips and appends are announced on —
/// the same one the IPC channel relays — and each SSE connection gets a
/// forwarder task piping into its own mpsc, which this Stream polls.
/// That skips the tokio-stream dependency (`broadcast::Receiver` has no
/// public `poll_recv`, `mpsc::Receiver` does) — the prototype's
/// validated pattern.
struct HubStream {
    rx: tokio::sync::mpsc::Receiver<String>,
}

impl Stream for HubStream {
    type Item = Result<Event, Infallible>;

    fn poll_next(self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<Option<Self::Item>> {
        let this = self.get_mut();
        match this.rx.poll_recv(cx) {
            Poll::Ready(Some(data)) => Poll::Ready(Some(Ok(Event::default().data(data)))),
            Poll::Ready(None) => Poll::Ready(None),
            Poll::Pending => Poll::Pending,
        }
    }
}

/// `GET /v1/events`: one SSE stream carrying every realtime event —
/// stream state flips, History Log appends, subscription removals —
/// serialized exactly as the channel's event frames (`type`-tagged, one
/// vocabulary). Rides the session cookie (EventSource cannot send
/// custom headers) or the token header; a cross-site Origin is refused.
/// A client lagged behind the hub's capacity gets a `resync` notice —
/// refetch `/v1/status` — instead of a replay.
async fn events(State(face): State<Face>, headers: HeaderMap) -> Response {
    if let Err(response) = authorize(&face, &headers).await {
        return *response;
    }
    if let Some(verdict) = cross_site(&face.security, &headers) {
        return cross_site_rejection(verdict, "event stream");
    }
    let mut hub = face.manager.events().subscribe();
    let (tx, rx) = tokio::sync::mpsc::channel::<String>(64);
    // Forward until the client hangs up (dropping the response ends the
    // mpsc receiver; the send error is that hangup).
    tokio::spawn(async move {
        loop {
            match hub.recv().await {
                Ok(event) => {
                    let data = serde_json::to_string(&event).expect("events serialize");
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
