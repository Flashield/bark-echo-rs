//! The channel's transport (ADR-0004): a Unix domain socket beside the
//! daemon's lock on Linux/macOS, a named pipe on Windows — one endpoint
//! type, cfg-switched internals, so every caller (daemon, CLI, GUI,
//! tests) speaks "the channel" without knowing which pipe the OS gave
//! it. The same test suite therefore runs on all three platforms.

use std::fmt;
use std::io;
use std::path::Path;

use crate::connection::Connection;

/// The daemon side of one accepted connection.
#[cfg(unix)]
pub type ServerStream = tokio::net::UnixStream;
/// The client side of one connection.
#[cfg(unix)]
pub type ClientStream = tokio::net::UnixStream;

/// The daemon side of one accepted connection.
#[cfg(windows)]
pub type ServerStream = tokio::net::windows::named_pipe::NamedPipeServer;
/// The client side of one connection.
#[cfg(windows)]
pub type ClientStream = tokio::net::windows::named_pipe::NamedPipeClient;

/// The local channel's address: one per daemon install, derived from the
/// config dir so the daemon and every client of the same install agree
/// without a discovery file ([`Endpoint::in_config_dir`]).
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct Endpoint {
    /// Unix: the socket file's path (a real file beside `daemon.lock`).
    /// Windows: the named pipe's full name (`\\.\pipe\...`).
    address: String,
}

impl Endpoint {
    /// The channel endpoint for a daemon whose files live in
    /// `config_dir` — beside `daemon.lock`, per ADR-0004.
    pub fn in_config_dir(config_dir: impl AsRef<Path>) -> Endpoint {
        #[cfg(unix)]
        {
            // Platform fact: a Unix domain socket is a filesystem entry,
            // so the endpoint can literally sit beside the lock — the
            // config dir plus a fixed name. Desktop users get the
            // socket file's permissions as the channel's access control.
            let path = config_dir.as_ref().join("daemon.ipc");
            Endpoint {
                address: path.to_string_lossy().into_owned(),
            }
        }
        #[cfg(windows)]
        {
            // Platform fact: a Windows named pipe is not a filesystem
            // entry — it has no name beside daemon.lock to inherit. The
            // pipe name is therefore derived from the config dir path,
            // which the daemon and every client resolve identically
            // (%APPDATA%\bark-echo); the pipe's ACL (created from the
            // daemon user's token) is the access control desktop users
            // still get. FNV-1a because the name must stay stable
            // across compiler versions, unlike std's hash defaults.
            let name = config_dir.as_ref().to_string_lossy();
            Endpoint {
                address: format!(r"\\.\pipe\bark-echo-{:016x}", fnv1a(name.as_bytes())),
            }
        }
    }

    /// The endpoint's address string — the socket path on Unix, the
    /// pipe name on Windows.
    pub fn address(&self) -> &str {
        &self.address
    }
}

impl fmt::Display for Endpoint {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.write_str(&self.address)
    }
}

/// FNV-1a 64-bit, written out because the pipe name it feeds must be
/// identical in the daemon and every client of one install — across
/// compiler versions and std releases.
#[cfg(windows)]
fn fnv1a(bytes: &[u8]) -> u64 {
    let mut hash: u64 = 0xcbf2_9ce4_8422_2325;
    for &byte in bytes {
        hash ^= u64::from(byte);
        hash = hash.wrapping_mul(0x0000_0100_0000_01b3);
    }
    hash
}

/// The listening side of the channel. Created under the daemon's
/// single-instance lock; dropped with the daemon's run.
pub struct IpcServer {
    endpoint: Endpoint,
    #[cfg(unix)]
    listener: tokio::net::UnixListener,
    #[cfg(windows)]
    pipe_name: String,
    /// The instance waiting for a client (Windows named pipes serve one
    /// connection per created instance; the next is created as soon as
    /// one connects).
    #[cfg(windows)]
    pending: tokio::net::windows::named_pipe::NamedPipeServer,
}

impl IpcServer {
    /// Bind the channel at `endpoint`. Call only while holding the
    /// daemon's single-instance lock: on Unix a socket file left there
    /// by a dead daemon is removed first — the lock guarantees no live
    /// server stands behind it — and the socket file's mode is forced
    /// to owner-only, the channel's access control (ADR-0004).
    pub fn bind(endpoint: &Endpoint) -> io::Result<IpcServer> {
        #[cfg(unix)]
        {
            let path = endpoint.address();
            match std::fs::remove_file(path) {
                Ok(()) => {}
                // Nothing to clean: no daemon ever bound here.
                Err(err) if err.kind() == io::ErrorKind::NotFound => {}
                Err(source) => return Err(source),
            }
            let listener = tokio::net::UnixListener::bind(path)?;
            // Platform fact: the socket file's mode is the access
            // control (ADR-0004); bind leaves it to the umask, so it is
            // forced to owner-only here — other local users cannot
            // connect to the daemon's channel.
            use std::os::unix::fs::PermissionsExt;
            let mut permissions = std::fs::metadata(path)?.permissions();
            permissions.set_mode(0o600);
            std::fs::set_permissions(path, permissions)?;
            Ok(IpcServer {
                endpoint: endpoint.clone(),
                listener,
            })
        }
        #[cfg(windows)]
        {
            use tokio::net::windows::named_pipe::ServerOptions;
            // first_pipe_instance: two daemons binding the same pipe is
            // a caller bug (the lock forbids it) — fail loudly here too.
            let pending = ServerOptions::new()
                .first_pipe_instance(true)
                .open(&endpoint.address)?;
            Ok(IpcServer {
                endpoint: endpoint.clone(),
                pipe_name: endpoint.address().to_owned(),
                pending,
            })
        }
    }

    /// The endpoint this server listens on.
    pub fn endpoint(&self) -> &Endpoint {
        &self.endpoint
    }

    /// The next client's connection — raw, no handshake (`serve` runs
    /// the protocol; direct callers play their own).
    pub async fn accept(&mut self) -> io::Result<Connection<ServerStream>> {
        #[cfg(unix)]
        {
            let (stream, _peer) = self.listener.accept().await?;
            Ok(Connection::new(stream))
        }
        #[cfg(windows)]
        {
            use tokio::net::windows::named_pipe::ServerOptions;
            match self.pending.connect().await {
                Ok(()) => {}
                // Platform fact (tokio's named pipe contract): a connect
                // future that is dropped mid-wait leaves the instance in
                // the connecting state — the only fix is recreating the
                // instance, which is what a fresh accept attempt does.
                Err(err) if err.kind() == io::ErrorKind::Cancelled => {
                    self.pending = ServerOptions::new().open(&self.pipe_name)?;
                    return self.accept().await;
                }
                Err(source) => return Err(source),
            }
            // This instance now belongs to the connected client; create
            // the next listening instance before anything else so a
            // second client is never refused for a scheduling hiccup.
            let connected = std::mem::replace(
                &mut self.pending,
                ServerOptions::new().open(&self.pipe_name)?,
            );
            Ok(Connection::new(connected))
        }
    }
}

impl Drop for IpcServer {
    fn drop(&mut self) {
        // Best-effort removal of the socket file: the lock is the
        // authority, and the next daemon cleans a leftover anyway, but
        // a tidy shutdown keeps the config dir honest. Windows needs no
        // counterpart — pipe instances die with their handles.
        #[cfg(unix)]
        {
            let _ = std::fs::remove_file(self.endpoint.address());
        }
    }
}

/// A raw client connection — no handshake, no protocol: the surface
/// tests use to play a hostile or confused peer against a real server.
pub async fn connect_raw(endpoint: &Endpoint) -> io::Result<Connection<ClientStream>> {
    #[cfg(unix)]
    {
        let stream = tokio::net::UnixStream::connect(endpoint.address()).await?;
        Ok(Connection::new(stream))
    }
    #[cfg(windows)]
    {
        use tokio::net::windows::named_pipe::ClientOptions;
        // Platform fact: when every pipe instance is busy, open fails
        // with ERROR_PIPE_BUSY (231) — the classic Windows answer is a
        // short retry (the manual equivalent of WaitNamedPipe). A few
        // fast attempts cover the scheduler gap between the daemon
        // accepting one client and offering the next instance; the
        // caller's own retry loop covers everything longer.
        const ERROR_PIPE_BUSY: i32 = 231;
        let mut attempts = 0;
        loop {
            match ClientOptions::new().open(endpoint.address()) {
                Ok(stream) => return Ok(Connection::new(stream)),
                Err(err) if err.raw_os_error() == Some(ERROR_PIPE_BUSY) => {
                    attempts += 1;
                    if attempts >= 5 {
                        return Err(err);
                    }
                    tokio::time::sleep(std::time::Duration::from_millis(10)).await;
                }
                Err(source) => return Err(source),
            }
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn the_endpoint_lives_beside_the_daemon_lock() {
        let dir = tempfile::tempdir().expect("temp dir");
        let endpoint = Endpoint::in_config_dir(dir.path());
        #[cfg(unix)]
        {
            // Platform fact: on Unix the endpoint is a filesystem path,
            // so "beside the lock" is literal.
            assert_eq!(
                endpoint.address(),
                dir.path().join("daemon.ipc").to_string_lossy()
            );
        }
        #[cfg(windows)]
        {
            // Platform fact: on Windows the endpoint is a named pipe in
            // its own namespace — derived from the config dir, not a
            // file beside the lock.
            assert!(
                endpoint.address().starts_with(r"\\.\pipe\bark-echo-"),
                "the pipe is named in the bark-echo namespace: {}",
                endpoint.address()
            );
        }
    }

    #[test]
    fn the_same_config_dir_yields_the_same_endpoint() {
        let dir = tempfile::tempdir().expect("temp dir");
        assert_eq!(
            Endpoint::in_config_dir(dir.path()),
            Endpoint::in_config_dir(dir.path()),
            "daemon and client must agree on the channel's address"
        );
    }

    #[tokio::test]
    async fn connect_to_an_absent_daemon_is_an_error() {
        let dir = tempfile::tempdir().expect("temp dir");
        let endpoint = Endpoint::in_config_dir(dir.path());
        assert!(
            connect_raw(&endpoint).await.is_err(),
            "no server was ever bound here"
        );
    }

    #[tokio::test]
    async fn raw_connections_exchange_bytes_over_the_real_transport() {
        let dir = tempfile::tempdir().expect("temp dir");
        let endpoint = Endpoint::in_config_dir(dir.path());
        let mut server = IpcServer::bind(&endpoint).expect("bind");

        let connecter = tokio::spawn({
            let endpoint = endpoint.clone();
            async move { connect_raw(&endpoint).await }
        });
        let mut server_side = server.accept().await.expect("accept");
        let mut client_side = connecter.await.expect("connect task").expect("connect");

        // A hostile-ish peer writes one raw line; the framed reader on
        // the other side picks it apart and answers a typed frame.
        client_side
            .write_raw(b"{\"word\":\"hello\"}\n")
            .await
            .expect("client write");
        let heard: serde_json::Value = server_side
            .receive()
            .await
            .expect("server receive")
            .expect("one frame");
        assert_eq!(heard["word"], "hello");

        server_side
            .send(&serde_json::json!({ "reply": true }))
            .await
            .expect("server send");
        let back: serde_json::Value = client_side
            .receive()
            .await
            .expect("client receive")
            .expect("one frame");
        assert_eq!(back["reply"], true);
    }

    #[tokio::test]
    async fn a_second_client_is_accepted_while_the_first_is_open() {
        let dir = tempfile::tempdir().expect("temp dir");
        let endpoint = Endpoint::in_config_dir(dir.path());
        let mut server = IpcServer::bind(&endpoint).expect("bind");

        let first = tokio::spawn({
            let endpoint = endpoint.clone();
            async move { connect_raw(&endpoint).await }
        });
        let mut first_server_side = server.accept().await.expect("first accept");
        let mut first_client = first.await.expect("task").expect("first connect");

        let second = tokio::spawn({
            let endpoint = endpoint.clone();
            async move { connect_raw(&endpoint).await }
        });
        let second_server_side = server.accept().await.expect("second accept");
        let mut second_client = second.await.expect("task").expect("second connect");

        // Both connections are alive at once: the first can still talk
        // after the second was accepted.
        first_client
            .write_raw(b"{\"n\":1}\n")
            .await
            .expect("first write");
        second_client
            .write_raw(b"{\"n\":2}\n")
            .await
            .expect("second write");
        let heard: serde_json::Value = first_server_side
            .receive()
            .await
            .expect("first read")
            .expect("one frame");
        assert_eq!(heard["n"], 1);
        drop(second_server_side);
    }

    #[tokio::test]
    async fn rebinding_after_a_dead_daemon_succeeds() {
        // A daemon dies (crash, kill -9): whatever it left at the
        // endpoint must not stop the next daemon — the lock is the
        // authority, so the next holder binds cleanly over the leftover.
        let dir = tempfile::tempdir().expect("temp dir");
        let endpoint = Endpoint::in_config_dir(dir.path());
        {
            let _server = IpcServer::bind(&endpoint).expect("first bind");
        } // dropped: the daemon "died"
        let _server = IpcServer::bind(&endpoint).expect("rebind over the leftover");
    }

    #[tokio::test]
    async fn the_socket_file_is_owner_only_on_unix() {
        // Only meaningful where the endpoint is a filesystem entry.
        #[cfg(unix)]
        {
            use std::os::unix::fs::PermissionsExt;
            let dir = tempfile::tempdir().expect("temp dir");
            let endpoint = Endpoint::in_config_dir(dir.path());
            let _server = IpcServer::bind(&endpoint).expect("bind");
            let mode = std::fs::metadata(endpoint.address())
                .expect("socket file")
                .permissions()
                .mode();
            assert_eq!(
                mode & 0o777,
                0o600,
                "the socket file's mode is the channel's access control (ADR-0004)"
            );
        }
        #[cfg(windows)]
        {
            // Platform fact: on Windows the channel's access control is
            // the pipe's ACL (created from the daemon user's token),
            // not a filesystem mode — nothing to assert without a
            // second user to prove exclusion against.
        }
    }
}
