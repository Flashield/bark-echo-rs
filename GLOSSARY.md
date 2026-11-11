# bark-echo-rs

Desktop Realtime Client for bark-server-rs: holds Subscriptions, mirrors
their Pushes as desktop notifications, and can send Pushes. Shared push
vocabulary (Device, Device Key, Push, Message, Ext Params, Realtime
Stream, Stream Token, Stream Device) is defined by the server repo's
`GLOSSARY.md` (bark-server-rs) and used here unchanged.

## Language

**Subscription**:
The client's unit of configuration: one server URL, one Device, and one
Stream Token for it, held open as one Realtime Stream.
_Avoid_: account, profile, connection

**History Log**:
The append-only local record of processed notifications; also the source
of the reconnect cursor.
_Avoid_: cache, archive, inbox

**Pull**:
A one-shot fetch of a Subscription's retained Messages from the server's
store-and-forward store (replay from a chosen cursor), landed in the
History Log without notifications.
_Avoid_: sync, backfill, fetch
