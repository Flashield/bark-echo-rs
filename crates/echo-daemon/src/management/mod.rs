//! The management face (issue #31): the daemon serves its management
//! surface in-process over HTTP (axum 0.8) — the `/v1` JSON API that
//! mirrors the echo-ipc vocabulary 1:1, one SSE event stream off the
//! daemon's own event hub, and the HTML shell the web page (ticket #32)
//! rides. ADR-0005: no separate face process, no GUI process.
//!
//! Security per issue #24's resolution:
//!
//! 1. **Token lifecycle** — one management token per install,
//!    auto-generated at the daemon's first start with the face, held in
//!    the secret store like every other secret (`--plain-secrets`
//!    fallback as usual). `management token` re-prints it; `management
//!    rotate` replaces it — through the channel when a daemon holds the
//!    lock (the single writer owns the store while it runs), and the
//!    rotation invalidates every session the face issued under the old
//!    token immediately.
//! 2. **Bind** — default `127.0.0.1:8081`, configurable (`[management]
//!    bind`); the LAN bind is an explicit opt-in (`--bind-lan` or
//!    `[management] bind_lan`), and when LAN-bound every request
//!    requires the token — the HTML page included, no anonymous view.
//!    A squatted port fails loud at startup, naming the config key.
//! 3. **Browser sessions** — `POST /v1/session` exchanges the token for
//!    an HttpOnly + SameSite=Strict session cookie (short TTL, killed by
//!    rotate); page navigation and the SSE stream ride the cookie
//!    same-origin, and the page's JS never holds the token again.
//! 4. **CSRF defense in depth** — SameSite=Strict; the custom-header
//!    requirement on state changes, `/v1/session` included (a
//!    cross-site fetch cannot pass preflight with one); and
//!    Host/Origin/Referer validation on every `/v1` request, reads
//!    included (a foreign Host is refused 403, as the prototype
//!    validated by curl).
//! 5. **Brute-force guard** — constant-time token compares; after 5
//!    consecutive failures each failure adds 1 s of delay, in-memory
//!    only.
//!
//! No daemon shutdown endpoint — the tray owns quit (ADR-0005); the
//! channel's graceful stop is not reachable over the face.

use std::collections::HashMap;
use std::sync::Mutex;
use std::sync::atomic::{AtomicU64, Ordering};
use std::time::{Duration, Instant};

use rand::RngCore;

use crate::paths::Paths;
use crate::secrets::{SecretError, SecretKey, SecretKind, SecretStore};

pub mod face;

pub(crate) use face::Face;

/// The browser session's cookie name (issue #24 point 3) — HttpOnly,
/// SameSite=Strict, one cookie is all the face uses.
pub const SESSION_COOKIE: &str = "bark_echo_session";

/// The session TTL (issue #24: a short TTL, killed early by rotate).
/// The prototype's validated value: long enough to stay signed in for a
/// workday, short enough that a stolen cookie ages out on its own.
pub const SESSION_TTL_SECS: u64 = 8 * 3600;

/// The store address the management token lives under (issue #24): one
/// per install, keyed like a per-server secret — the keyring account
/// line reads `management-token::management::`, the plain-file fallback
/// gets a `["management::"]` section. Both stores hold it without new
/// machinery.
fn secret_key() -> SecretKey {
    SecretKey::for_server("management")
}

/// Failures of the management token's CLI face.
#[derive(Debug, thiserror::Error)]
pub enum ManagementError {
    /// No token is stored yet — it is minted at the daemon's first start
    /// with the face (issue #24: no enable command; at the default
    /// localhost bind the auto-generation adds no exposure).
    #[error(
        "no management token is stored yet — start the daemon once (`bark-echo run`) to mint it"
    )]
    NoToken,
    /// The secret store failed underneath.
    #[error(transparent)]
    Secret(#[from] SecretError),
}

/// The token the face answers to: the stored one, or a fresh mint when
/// none exists — the daemon's first start with the face (issue #24
/// point 1). Called once per run, before the face serves.
pub fn ensure_token(store: &dyn SecretStore) -> Result<String, SecretError> {
    match store.get(&secret_key(), SecretKind::ManagementToken) {
        Ok(token) if !token.is_empty() => Ok(token),
        Ok(_) | Err(SecretError::NotFound(_)) => {
            let token = generate_token();
            store.set(&secret_key(), SecretKind::ManagementToken, &token)?;
            Ok(token)
        }
        Err(err) => Err(err),
    }
}

/// The stored management token, read for `bark-echo management token`
/// (spec story 21). No daemon is needed: the CLI reads the same store
/// the daemon wrote, on the same machine. A missing token means no
/// daemon has started with the face yet — the error says so instead of
/// leaking the store's internal account key.
pub fn stored_token(store: &dyn SecretStore) -> Result<String, ManagementError> {
    match store.get(&secret_key(), SecretKind::ManagementToken) {
        Ok(token) if !token.is_empty() => Ok(token),
        Ok(_) | Err(SecretError::NotFound(_)) => Err(ManagementError::NoToken),
        Err(err) => Err(err.into()),
    }
}

/// A fresh management token: 32 random bytes, hex-encoded. The entropy
/// is the whole story — the token is the face's root credential (#24),
/// guessed only by brute force, and the guard slows that.
pub fn generate_token() -> String {
    let mut bytes = [0u8; 32];
    rand::rng().fill_bytes(&mut bytes);
    bytes.iter().map(|byte| format!("{byte:02x}")).collect()
}

/// Replace the management token (`bark-echo management rotate` without a
/// daemon holding the lock): a fresh secret is minted and stored. No
/// sessions exist to invalidate — no face is being served. When a
/// daemon *does* hold the lock, the rotation must ride the channel
/// instead ([`rotate_via_channel`]) so the live face drops its sessions
/// at once (issue #24's kill switch).
pub fn rotate(store: &dyn SecretStore) -> Result<(), ManagementError> {
    apply_rotation(store, None)?;
    Ok(())
}

/// The daemon-side rotation (issue #24's kill switch): a fresh token is
/// minted and stored — the single writer's store — and every session the
/// face issued under the old one dies at once, because the face's cached
/// token is replaced and the session generation moves on. `face` is
/// `None` when this run serves no face (the store write still applies;
/// there is nothing to invalidate).
pub(crate) fn apply_rotation(
    store: &dyn SecretStore,
    face: Option<&FaceState>,
) -> Result<(), SecretError> {
    let token = generate_token();
    store.set(&secret_key(), SecretKind::ManagementToken, &token)?;
    if let Some(face) = face {
        face.rotate(&token);
    }
    Ok(())
}

/// The CLI's `management rotate` when a daemon holds the lock: the
/// rotation rides the channel (the daemon is the single writer of the
/// secret store while it runs, ADR-0004) so the running face drops its
/// sessions with the old token. The same ride the subscribe commands
/// use (deadline, attempt cap, retry — [`crate::channel::ride`]).
pub async fn rotate_via_channel(paths: &Paths) -> Result<(), crate::subscribe::ChannelError> {
    let answer = crate::channel::ride(paths, |mut client| async move {
        client.management_rotate().await
    })
    .await;
    match answer {
        Ok(()) => Ok(()),
        Err(err @ echo_ipc::IpcError::Rejected(_)) => Err(err.into()),
        Err(_) => Err(crate::subscribe::ChannelError::Unreachable),
    }
}

/// One issued browser session: its id, when it expires, and which token
/// generation it was issued under — rotate moves the generation, so
/// every session issued before it is dead without a per-session sweep.
struct Session {
    expires: Instant,
    generation: u64,
}

/// The face's live security state (issue #24): the current token (cached
/// so request handling never touches the keyring — rotate updates cache
/// and store together), the browser sessions it issued, the generation
/// those sessions belong to, and the brute-force guard's consecutive
/// failure count. All in-memory: a daemon restart drops every session,
/// and rotate drops them at once.
pub struct FaceState {
    /// The token the face currently answers to. `RwLock` because rotate
    /// replaces it while requests read it.
    token: std::sync::RwLock<String>,
    /// The live sessions, keyed by id.
    sessions: Mutex<HashMap<String, Session>>,
    /// The current token generation; sessions carry the one they were
    /// issued under. Bumped by rotate.
    generation: AtomicU64,
    /// Consecutive failed token presentations (the brute-force guard,
    /// issue #24 point 5) — reset by any success.
    failures: AtomicU64,
    /// The LAN posture (#24 point 2): when bound to all interfaces,
    /// every request requires the token — the HTML page included.
    gate_page: bool,
    /// Whether the face is localhost-bound and may therefore enforce the
    /// loopback Host allowlist (the DNS-rebinding defense, #24 point 3).
    /// A LAN bind disables it: callers may reach the face by any name or
    /// address, and the token gates everything there anyway.
    local_only: bool,
}

impl FaceState {
    /// The state for one daemon run, seeded with the token
    /// [`ensure_token`] put in the store.
    pub(crate) fn new(token: String, gate_page: bool, local_only: bool) -> Self {
        Self {
            token: std::sync::RwLock::new(token),
            sessions: Mutex::new(HashMap::new()),
            generation: AtomicU64::new(0),
            failures: AtomicU64::new(0),
            gate_page,
            local_only,
        }
    }

    /// The token the face currently answers to — what
    /// `X-Management-Token` and `/v1/session` must match. Handlers
    /// compare constant-time.
    pub(crate) fn current_token(&self) -> String {
        self.token
            .read()
            .expect("management token mutex poisoned")
            .clone()
    }

    /// Rotate (issue #24's kill switch): the new token takes effect at
    /// once and every session issued under the old one is invalidated —
    /// cleared here, and dead by generation either way.
    pub(crate) fn rotate(&self, new_token: &str) {
        *self.token.write().expect("management token mutex poisoned") = new_token.to_owned();
        self.sessions
            .lock()
            .expect("session mutex poisoned")
            .clear();
        self.generation.fetch_add(1, Ordering::SeqCst);
    }

    /// Whether the HTML page is gated (the LAN posture).
    pub(crate) fn page_is_gated(&self) -> bool {
        self.gate_page
    }

    /// Whether the Host allowlist (the loopback set) is enforced.
    pub(crate) fn local_only(&self) -> bool {
        self.local_only
    }

    /// Issue one session: a fresh random id stamped with the current
    /// token generation, expiring after [`SESSION_TTL_SECS`]. The id is
    /// a fresh token's entropy — unguessable, so a stolen id is the only
    /// way in, and HttpOnly keeps it away from the page's JS.
    pub(crate) fn issue_session(&self) -> String {
        let sid = format!("sess-{}", generate_token());
        let session = Session {
            expires: Instant::now() + Duration::from_secs(SESSION_TTL_SECS),
            generation: self.generation.load(Ordering::SeqCst),
        };
        self.sessions
            .lock()
            .expect("session mutex poisoned")
            .insert(sid.clone(), session);
        sid
    }

    /// Whether `sid` names a live session of the current generation.
    /// Expired entries are dropped as they are met — no sweeper task.
    pub(crate) fn session_is_live(&self, sid: &str) -> bool {
        let mut sessions = self.sessions.lock().expect("session mutex poisoned");
        match sessions.get(sid) {
            Some(session)
                if session.expires > Instant::now()
                    && session.generation == self.generation.load(Ordering::SeqCst) =>
            {
                true
            }
            Some(_) => {
                sessions.remove(sid);
                false
            }
            None => false,
        }
    }

    /// Record one failed token presentation and hold the brute-force
    /// guard's delay before the refusal (issue #24 point 5): after 5
    /// consecutive failures each failure adds 1 s, capped at 5 s.
    /// In-memory only — a restart resets it, and so does any success.
    pub(crate) async fn penalize_failure(&self) {
        let count = self.failures.fetch_add(1, Ordering::SeqCst) + 1;
        if count > 5 {
            tokio::time::sleep(Duration::from_secs((count - 5).min(5))).await;
        }
    }

    /// A successful authentication: the brute-force guard's counter
    /// resets — consecutive means consecutive.
    pub(crate) fn note_success(&self) {
        self.failures.store(0, Ordering::SeqCst);
    }
}

/// Serve the face on a pre-bound listener until the daemon's graceful
/// stop aborts the task. The bind itself already happened at the caller
/// (`bark-echo run`, fail-loud on a squatted port); a serve error is
/// logged loudly and the run carries on — mirroring streams is the
/// daemon's core job, and `status` still answers over the channel (the
/// channel's own posture).
pub(crate) async fn serve(listener: tokio::net::TcpListener, face: Face) -> std::io::Result<()> {
    let app = face::router(face);
    axum::serve(listener, app).await
}
