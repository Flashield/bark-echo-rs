//! The daemon's half of the Push Composer (ticket #16, ADR-0004): a
//! SendPush request rides the channel in, and the daemon performs the
//! send itself — the echo-core Push send path (protocol §2.2), aimed at
//! the Subscription's server with the daemon's own stored Basic Auth
//! credentials (story 18). No credential ever rides the request: the
//! Subscription names the server, the target Device is the request's
//! explicit Device Key field, and everything else is the Push's own
//! surface.
//!
//! The answer is the bark server's verdict, verbatim (ADR-0004's one
//! request-response): accepted, or refused with the server's own code
//! and message for the window to show. A send the daemon cannot even
//! attempt — an unknown Subscription, an unreachable secret store, a
//! server that never answers — is the channel's error answer instead:
//! no verdict, no lie.

use echo_core::push::{Push, PushClient, PushError};
use echo_ipc::message::{SendPush, SendPushOutcome};

use crate::config::Config;
use crate::secrets::SecretStore;

/// Perform one Composer send. `Ok` is the bark server's verdict —
/// [`SendPushOutcome::Sent`] or the refusal verbatim; `Err` is the
/// explanation for a send that was never attempted, as the channel's
/// error answer.
pub async fn handle_send(
    config: &Config,
    store: &dyn SecretStore,
    push: SendPush,
) -> Result<SendPushOutcome, String> {
    // The send's context (ADR-0004): one Subscription. Its server is
    // where the Push goes; nothing else about the request names one.
    let subscription = config
        .subscriptions
        .iter()
        .find(|subscription| subscription.id == push.subscription_id)
        .ok_or_else(|| {
            format!(
                "no Subscription with id `{}` is configured — the Composer's send rides one of the daemon's Subscriptions",
                push.subscription_id
            )
        })?;
    // The operator Basic Auth credentials (§1.4) out of the store — the
    // daemon's own (spec story 13). A credential-required seam: unlike
    // the CLI `push` (whose flags or absence speak for themselves), the
    // Composer promised the daemon's connection policy, so an
    // unreachable store is this send's refusal, not a silent degrade.
    let client = PushClient::new(&subscription.server);
    let client = match crate::auth::resolve(config, store, &subscription.server, None, None) {
        Ok(Some(auth)) => client.with_basic_auth(auth.user, auth.password),
        Ok(None) => client,
        Err(err) => return Err(err.to_string()),
    };
    // The Push's own surface (§2.2), mapped one-to-one onto the send
    // module's builder — the same path the `push` command drives, so
    // the wire face cannot fork.
    let mut send = Push::to(&push.device_key);
    if let Some(title) = &push.title {
        send = send.title(title);
    }
    if let Some(subtitle) = &push.subtitle {
        send = send.subtitle(subtitle);
    }
    if let Some(body) = &push.body {
        send = send.body(body);
    }
    for (key, value) in &push.ext_params {
        send = send.ext_param(key, value);
    }
    match client.send(&send).await {
        Ok(()) => Ok(SendPushOutcome::Sent),
        // The server answered with a refusal: its code and message,
        // verbatim — a bad Device Key answers `400 failed to get device
        // token...`, a revoked or wrong credential the 418 teapot (§1.4).
        Err(PushError::Server { code, message }) => Ok(SendPushOutcome::Rejected {
            code: Some(code),
            message,
        }),
        Err(err @ PushError::Teapot) => Ok(SendPushOutcome::Rejected {
            code: Some(418),
            message: err.to_string(),
        }),
        // No verdict exists: the request never completed. The channel's
        // error answer carries what the attempt learned.
        Err(PushError::Transport(err)) => {
            Err(format!("the send could not reach the bark server: {err}"))
        }
    }
}
