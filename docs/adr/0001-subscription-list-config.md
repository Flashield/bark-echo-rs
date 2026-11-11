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
  OS keyring, keyed by server + device.
- Onboarding is per-Subscription add/remove, not a one-time setup wizard.
