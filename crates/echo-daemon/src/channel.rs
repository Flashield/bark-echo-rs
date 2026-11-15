//! The CLI faces' ride of the daemon's IPC Channel (ADR-0004): the
//! deadline-timeout-retry loop every live caller shares. One
//! connect-request attempt under a per-attempt cap, retried until the
//! ride's budget is spent — long enough to absorb a starting daemon
//! (the lock is held a beat before the endpoint is bound), short enough
//! to stay snappy, and a wedged daemon cannot wedge the caller.

use std::future::Future;
use std::time::{Duration, Instant};

use echo_ipc::{Client, IpcError};

use crate::paths::Paths;

/// How long a ride tries the channel before giving up.
const CHANNEL_BUDGET: Duration = Duration::from_secs(2);
/// One connect-request attempt's cap.
const CHANNEL_ATTEMPT: Duration = Duration::from_secs(1);
/// The pause between attempts.
const CHANNEL_RETRY: Duration = Duration::from_millis(50);

/// Run `request` against a freshly connected client, retrying until
/// [`CHANNEL_BUDGET`] is spent. The daemon's own refusal
/// ([`IpcError::Rejected`]) is an answer — retrying cannot change it —
/// so it comes back at once; every other failure (a daemon mid-start, a
/// wedged one) is retried, and a spent budget comes back as
/// [`IpcError::Closed`], the sentinel the status fallback reads.
pub(crate) async fn ride<T, F, Fut>(paths: &Paths, request: F) -> Result<T, IpcError>
where
    F: Fn(Client) -> Fut,
    Fut: Future<Output = Result<T, IpcError>>,
{
    let endpoint = paths.ipc_endpoint();
    let deadline = Instant::now() + CHANNEL_BUDGET;
    loop {
        let attempt = tokio::time::timeout(CHANNEL_ATTEMPT, async {
            let client = Client::connect(&endpoint).await?;
            request(client).await
        });
        match attempt.await {
            Ok(Ok(answer)) => return Ok(answer),
            Ok(Err(err @ IpcError::Rejected(_))) => return Err(err),
            _ if Instant::now() >= deadline => return Err(IpcError::Closed),
            _ => tokio::time::sleep(CHANNEL_RETRY).await,
        }
    }
}
