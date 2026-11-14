//! The tray's model: the echo-ipc client's view of the daemon, reduced
//! to what a Tray Menu shows (spec: "GUI logic is an echo-ipc client
//! state machine covered by the IPC seam; the egui rendering layer stays
//! thin"). Pure data — the tray layer derives its muda menu from
//! [`TrayModel::menu_items`], nothing here touches a window system.

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
/// read-only tray of ticket #11); the two actions are placeholders —
/// "open Management Window" is wired by ticket #13, and Quit only exits
/// the GUI process here (the daemon shutdown is ticket #12).
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum MenuItem {
    /// A non-interactive line: the daemon link's state.
    Header(String),
    /// A Subscription's glanceable line.
    Subscription {
        id: String,
        text: String,
    },
    Separator,
    /// "Open Management Window" (GLOSSARY) — placeholder.
    OpenManagementWindow,
    /// Quit — this ticket: exits the GUI process.
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
    /// consumed without effect here (the Management Window's live log
    /// view builds on them, tickets #13/#15).
    pub fn apply_event(&mut self, event: &Event) {
        match event {
            Event::StreamState {
                subscription_id,
                label,
                state,
            } => {
                // Self-contained events: a line unknown so far (a
                // Subscription added while we watch, tickets ahead) still
                // gets its row.
                self.subscriptions.insert(
                    subscription_id.clone(),
                    SubscriptionLine {
                        label: label.clone(),
                        state: state.clone(),
                    },
                );
            }
            Event::HistoryAppended { .. } => {}
        }
    }

    /// The channel broke. The lines stay (they are the Subscription
    /// list's identity) but lose their claim on freshness: the menu
    /// derives "unknown" for them while the header names the truth.
    pub fn connection_lost(&mut self) {
        self.link = LinkState::Reconnecting;
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
        items.push(MenuItem::OpenManagementWindow);
        items.push(MenuItem::Quit);
        items
    }

    /// The header's words, per link state.
    fn header_text(&self) -> String {
        match self.link {
            LinkState::Connecting => "Daemon: connecting…".to_owned(),
            LinkState::Connected => "Daemon: connected".to_owned(),
            LinkState::Reconnecting => "Daemon: reconnecting…".to_owned(),
        }
    }

    /// The state's words for one line: while the channel is up, the
    /// daemon's own vocabulary (GLOSSARY / `status`); while it is not,
    /// the line says so — a last-known "running" must never pose as
    /// current.
    fn state_text(&self, line: &SubscriptionLine) -> String {
        match self.link {
            LinkState::Connected => line.state.as_str().to_owned(),
            LinkState::Connecting | LinkState::Reconnecting => "unknown (daemon away)".to_owned(),
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
                MenuItem::OpenManagementWindow,
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
                MenuItem::OpenManagementWindow,
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
            "appends are later tickets' food"
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
    fn the_tray_heals_itself_when_the_daemon_returns() {
        let mut model = TrayModel::connecting();
        model.apply_snapshot(&snapshot_with("tok", "desktop", StreamState::Running));
        model.connection_lost();
        // The daemon restarted; the next snapshot is fresh authority.
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
                MenuItem::OpenManagementWindow,
                MenuItem::Quit,
            ],
            "no residue of the outage"
        );
    }

    #[test]
    fn an_event_for_an_unknown_subscription_still_gets_its_row() {
        // Events are self-contained: a Subscription the snapshot never
        // named (one added while the tray watches — later tickets' hot
        // apply) still gets its row from the flip alone. Events only
        // arrive on a connected link, so the model is connected here.
        let mut model = TrayModel::connecting();
        model.apply_snapshot(&StatusSnapshot {
            pid: 1,
            started_at: 0,
            subscriptions: BTreeMap::new(),
        });
        model.apply_event(&flip("tok-new", "newcomer", StreamState::Starting));
        assert_eq!(
            model.menu_items()[1],
            MenuItem::Subscription {
                id: "tok-new".to_owned(),
                text: "newcomer — starting".to_owned(),
            }
        );
    }

    #[test]
    fn lines_sort_by_label_for_a_stable_menu() {
        let mut model = TrayModel::connecting();
        let mut snapshot = StatusSnapshot {
            pid: 1,
            started_at: 0,
            subscriptions: BTreeMap::new(),
        };
        // Inserted in an id order that means nothing for display.
        for (id, label) in [("tok-b", "zebra"), ("tok-a", "alpha")] {
            snapshot.subscriptions.insert(
                id.to_owned(),
                echo_ipc::message::SubscriptionRuntime {
                    label: label.to_owned(),
                    state: StreamState::Running,
                    last_message_id: None,
                    updated_at: 0,
                    server: "http://127.0.0.1:8080".to_owned(),
                    token_hint: "abcdef123456...".to_owned(),
                },
            );
        }
        model.apply_snapshot(&snapshot);
        let items = model.menu_items();
        assert_eq!(
            items[1],
            MenuItem::Subscription {
                id: "tok-a".to_owned(),
                text: "alpha — running".to_owned(),
            }
        );
        assert_eq!(
            items[2],
            MenuItem::Subscription {
                id: "tok-b".to_owned(),
                text: "zebra — running".to_owned(),
            }
        );
    }
}
