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

**Tray Menu**:
The GUI's always-available system-tray presence: a glance at each
Subscription's stream state plus the quick actions (open the Management
Window, quit).
_Avoid_: status icon, systray app, quick menu

**Management Window**:
The GUI window for managing Subscriptions (add/list/remove), watching
each Realtime Stream's live state, and browsing a Subscription's
History Log.
_Avoid_: dashboard, main window, settings panel

**Push Composer**:
The GUI form that sends a Push, mirroring the `push` command's fields
and its Ext Param passthrough.
_Avoid_: send dialog, push form, quick send
