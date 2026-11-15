//! The tray's model: the echo-ipc client's view of the daemon, reduced
//! to what a Tray Menu shows (spec: the Tray Menu is a glance at each
//! Subscription's stream state plus the quick actions). Pure data — the
//! tray layer derives its muda menu from [`TrayModel::menu_items`],
//! nothing here touches a window system. (Resurrected from the dormant
//! egui face's model — `egui-face-dormant:crates/echo-gui/src/model.rs`
//! — unchanged but for the manage action, which now opens the web face
//! instead of the retired window, ADR-0005.)

use std::collections::BTreeMap;

use echo_ipc::message::{Event, StatusSnapshot, StreamState};

/// The channel's health as the tray shows it — the "never stale
/// silence" rule (ticket #11): the daemon's absence is a state, never
/// an old picture left on screen.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum LinkState {
    /// Reaching for the daemon — the first attempt (or after the model
    /// was reset); no snapshot has arrived yet.
    Connecting,
    /// The channel answered the opening snapshot; events keep the
    /// picture current from here.
    Connected,
    /// A live connection broke (or a connect attempt failed after
    /// having been connected before); the reconnect loop is retrying.
    Reconnecting,
    /// Quit has asked the daemon to stop (ticket #12): the tray's last
    /// picture, under a header naming what is happening. Nothing
    /// updates it further — the process is on its way out.
    Quitting,
}

/// One Subscription's line the tray derives from the model.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct SubscriptionLine {
    /// The Subscription's client label.
    pub label: String,
    /// The stream state as the daemon last reported it.
    pub state: StreamState,
}

/// The tray's whole world: the link's state plus every Subscription's
/// last reported line. The daemon owns the truth; this is its echo.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct TrayModel {
    pub link: LinkState,
    /// Per-Subscription lines keyed by Subscription id (the snapshot's
    /// key), so events update in place.
    pub subscriptions: BTreeMap<String, SubscriptionLine>,
}

/// One row of the tray menu. Subscription rows are informational (the
/// read-only glance of ticket #11); of the two actions, Manage opens
/// the daemon's web face in the default browser (issue #30, ADR-0005 —
/// the face, not a window, is the management surface now), and Quit
/// asks the daemon for a graceful shutdown before leaving (ticket #12,
/// ADR-0004's exchange).
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum MenuItem {
    /// A non-interactive line: the daemon link's state.
    Header(String),
    /// A Subscription's glanceable line.
    Subscription {
        /// The Subscription's id (the snapshot's key).
        id: String,
        /// The rendered text: label and stream state.
        text: String,
    },
    Separator,
    /// Manage — opens the management face in the default browser.
    Manage,
    /// Quit — asks the daemon for a graceful shutdown, waits a bounded
    /// while for its confirmation, then exits (ticket #12).
    Quit,
}

impl TrayModel {
    /// A tray reaching for the daemon for the first time.
    pub fn connecting() -> Self {
        Self {
            link: LinkState::Connecting,
            subscriptions: BTreeMap::new(),
        }
    }

    /// The opening snapshot: the link is up and the lines are whatever
    /// the daemon says — replacing everything previously believed.
    pub fn apply_snapshot(&mut self, snapshot: &StatusSnapshot) {
        self.link = LinkState::Connected;
        self.subscriptions = snapshot
            .subscriptions
            .iter()
            .map(|(id, runtime)| {
                (
                    id.clone(),
                    SubscriptionLine {
                        label: runtime.label.clone(),
                        state: runtime.state.clone(),
                    },
                )
            })
            .collect();
    }

    /// One daemon-pushed event (ticket #11). State flips update their
    /// line in place — no request, no poll; History Log appends are
    /// consumed without effect (the web face's live log view builds on
    /// the same broadcast, ADR-0005).
    pub fn apply_event(&mut self, event: &Event) {
        match event {
            Event::StreamState {
                subscription_id,
                label,
                state,
            } => {
                // Self-contained events: a line unknown so far (a
                // Subscription added while we watch) still gets its row.
                self.subscriptions.insert(
                    subscription_id.clone(),
                    SubscriptionLine {
                        label: label.clone(),
                        state: state.clone(),
                    },
                );
            }
            Event::HistoryAppended { .. } => {}
            // Hot-apply (ticket #14): a Subscription removed while the
            // tray watches leaves the menu at once — a removed name must
            // not linger as a stale line until the next snapshot.
            Event::SubscriptionRemoved { subscription_id } => {
                self.subscriptions.remove(subscription_id);
            }
        }
    }

    /// The channel broke. The lines stay (they are the Subscription
    /// list's identity) but lose their claim on freshness: the menu
    /// derives "unknown" for them while the header names the truth.
    pub fn connection_lost(&mut self) {
        self.link = LinkState::Reconnecting;
    }

    /// Quit clicked (ticket #12): the header names the stop while the
    /// Quit exchange waits its bounded while, and every line's
    /// last-known state loses its freshness claim. Idempotent — a
    /// second Quit click is the same instruction, not a new one.
    pub fn quitting(&mut self) {
        self.link = LinkState::Quitting;
    }

    /// The menu the tray renders, derived fresh on every call.
    pub fn menu_items(&self) -> Vec<MenuItem> {
        let mut items = vec![MenuItem::Header(self.header_text())];
        // Sorted by label: the daemon's ids are hex token ids — no
        // display order lives in them, and a stable menu must not jump.
        let mut lines: Vec<(&String, &SubscriptionLine)> = self.subscriptions.iter().collect();
        lines.sort_by(|a, b| a.1.label.cmp(&b.1.label));
        for (id, line) in lines {
            items.push(MenuItem::Subscription {
                id: id.clone(),
                text: format!("{} — {}", line.label, self.state_text(line)),
            });
        }
        items.push(MenuItem::Separator);
        items.push(MenuItem::Manage);
        items.push(MenuItem::Quit);
        items
    }

    /// The header's words, per link state.
    fn header_text(&self) -> String {
        match self.link {
            LinkState::Connecting => "Daemon: connecting…".to_owned(),
            LinkState::Connected => "Daemon: connected".to_owned(),
            LinkState::Reconnecting => "Daemon: reconnecting…".to_owned(),
            LinkState::Quitting => "Daemon: shutting down…".to_owned(),
        }
    }

    /// The state's words for one line: while the channel is up, the
    /// daemon's own vocabulary (GLOSSARY / `status`); while it is not,
    /// the line says so — a last-known "running" must never pose as
    /// current.
    fn state_text(&self, line: &SubscriptionLine) -> String {
        match self.link {
            LinkState::Connected => line.state.as_str().to_owned(),
            LinkState::Connecting | LinkState::Reconnecting | LinkState::Quitting => {
                "unknown (daemon away)".to_owned()
            }
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use echo_ipc::message::NotificationIntent;

    fn snapshot_with(id: &str, label: &str, state: StreamState) -> StatusSnapshot {
        StatusSnapshot {
            pid: 4242,
            started_at: 1_700_000_000,
            subscriptions: BTreeMap::from([(
                id.to_owned(),
                echo_ipc::message::SubscriptionRuntime {
                    label: label.to_owned(),
                    state,
                    last_message_id: None,
                    updated_at: 1_700_000_042,
                    server: "http://127.0.0.1:8080".to_owned(),
                    token_hint: "abcdef123456...".to_owned(),
                },
            )]),
        }
    }

    fn flip(id: &str, label: &str, state: StreamState) -> Event {
        Event::StreamState {
            subscription_id: id.to_owned(),
            label: label.to_owned(),
            state,
        }
    }

    fn appended(id: i64) -> Event {
        Event::HistoryAppended {
            subscription_id: "tok".to_owned(),
            label: "desktop".to_owned(),
            entry: NotificationIntent {
                id,
                title: String::new(),
                subtitle: String::new(),
                body: "the push".to_owned(),
                click_action: None,
                created_at: 1_700_000_000,
            },
        }
    }

    #[test]
    fn a_fresh_tray_reaches_for_the_daemon_and_shows_no_lines() {
        let model = TrayModel::connecting();
        assert_eq!(
            model.menu_items(),
            vec![
                MenuItem::Header("Daemon: connecting…".to_owned()),
                MenuItem::Separator,
                MenuItem::Manage,
                MenuItem::Quit,
            ],
            "no subscription picture exists before the snapshot"
        );
    }

    #[test]
    fn the_opening_snapshot_paints_the_lines_and_connects() {
        let mut model = TrayModel::connecting();
        model.apply_snapshot(&snapshot_with("tok", "desktop", StreamState::Running));
        assert_eq!(
            model.menu_items(),
            vec![
                MenuItem::Header("Daemon: connected".to_owned()),
                MenuItem::Subscription {
                    id: "tok".to_owned(),
                    text: "desktop — running".to_owned(),
                },
                MenuItem::Separator,
                MenuItem::Manage,
                MenuItem::Quit,
            ]
        );
    }

    #[test]
    fn an_event_flips_the_visible_state_without_any_user_action() {
        let mut model = TrayModel::connecting();
        model.apply_snapshot(&snapshot_with("tok", "desktop", StreamState::Running));
        model.apply_event(&flip(
            "tok",
            "desktop",
            StreamState::Disconnected {
                detail: "connection closed by the server".to_owned(),
            },
        ));
        let items = model.menu_items();
        assert_eq!(
            items[1],
            MenuItem::Subscription {
                id: "tok".to_owned(),
                text: "desktop — disconnected".to_owned(),
            },
            "the flip is on the menu, not at the next poll"
        );
    }

    #[test]
    fn a_history_append_is_consumed_without_touching_the_menu() {
        let mut model = TrayModel::connecting();
        model.apply_snapshot(&snapshot_with("tok", "desktop", StreamState::Running));
        let before = model.menu_items();
        model.apply_event(&appended(41));
        assert_eq!(
            model.menu_items(),
            before,
            "appends are the web face's food (ADR-0005)"
        );
    }

    #[test]
    fn losing_the_connection_shows_reconnecting_and_dethrones_stale_states() {
        let mut model = TrayModel::connecting();
        model.apply_snapshot(&snapshot_with("tok", "desktop", StreamState::Running));
        model.connection_lost();
        let items = model.menu_items();
        assert_eq!(
            items[0],
            MenuItem::Header("Daemon: reconnecting…".to_owned()),
            "the daemon's absence is named"
        );
        assert_eq!(
            items[1],
            MenuItem::Subscription {
                id: "tok".to_owned(),
                text: "desktop — unknown (daemon away)".to_owned(),
            },
            "a last-known running never poses as current"
        );
    }

    #[test]
    fn quitting_names_the_state_and_dethrones_stale_lines() {
        // Ticket #12: during Quit's bounded wait the tray names what is
        // happening — and the lines lose their freshness claim, the way
        // they do when the daemon is away. Nothing updates the tray
        // after this: the process is on its way out.
        let mut model = TrayModel::connecting();
        model.apply_snapshot(&snapshot_with("tok", "desktop", StreamState::Running));
        model.quitting();
        let items = model.menu_items();
        assert_eq!(
            items[0],
            MenuItem::Header("Daemon: shutting down…".to_owned()),
            "the header names the stop while the exchange waits"
        );
        assert_eq!(
            items[1],
            MenuItem::Subscription {
                id: "tok".to_owned(),
                text: "desktop — unknown (daemon away)".to_owned(),
            },
            "a last-known running never poses as current"
        );
        // Idempotent: a second click is the same instruction.
        model.quitting();
        assert_eq!(model.link, LinkState::Quitting);
    }

    #[test]
    fn a_subscription_removed_while_watched_leaves_the_menu_at_once() {
        // Hot-apply (ticket #14): the daemon's `subscription_removed`
        // event drops the row immediately — a removed name must not
        // linger as a stale line until the next snapshot.
        let mut model = TrayModel::connecting();
        model.apply_snapshot(&snapshot_with("tok", "desktop", StreamState::Running));
        model.apply_event(&Event::SubscriptionRemoved {
            subscription_id: "tok".to_owned(),
        });
        let items = model.menu_items();
        assert!(
            !items
                .iter()
                .any(|item| matches!(item, MenuItem::Subscription { .. })),
            "the removed Subscription's line is gone without a restart"
        );
        assert_eq!(
            items[0],
            MenuItem::Header("Daemon: connected".to_owned()),
            "the removal did not disturb the link's health"
        );
    }
}
