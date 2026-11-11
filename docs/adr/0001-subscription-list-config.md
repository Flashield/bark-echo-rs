# Subscription list as the unit of configuration

The client's configuration is a list of Subscriptions — each one server URL +
one Device + one Stream Token — rather than a single account. The protocol
allows any number of concurrent streams, and mirroring an existing Device
(e.g. the user's iPhone, by minting a token with its Device Key) is the only
way to reach the project goal "desktop receives what the iPhone receives".
A single-account model would make that impossible without a config-format
break later.

## Consequences

- Secrets (Stream Tokens, Device Keys) are stored per Subscription, in the
  OS keyring, keyed by server + token id: the Stream Token's public
  `token_id`, minted fresh on every add, is the Subscription's identity.
  Keying by the Device alone would collide when two Subscriptions mirror
  the same Device (the user's iPhone plus a second client, say) — the
  protocol mints a new token per add precisely so each Subscription owns
  its own credential and revocable handle.
- Onboarding is per-Subscription add/remove, not a one-time setup wizard.
