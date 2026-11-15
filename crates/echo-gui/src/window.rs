//! The Management Window (GLOSSARY, tickets #13 and #15): the model
//! holds the echo-ipc client's view of the daemon reduced to what the
//! window lists — each Subscription's label, server URL, and token hint
//! (the snapshot's public identity, never the secrets behind it) with
//! its Realtime Stream's live state — and the render function lays that
//! picture out, with the picked Subscription's History Log view (ticket
//! #15) beneath the list: its label is the pick, the click becomes one
//! ask over the channel. The same thin split as the tray (model owns
//! every word, paint owns none): the data arrives as the connection
//! loop's snapshot-then-events stream — the opening snapshot rides every
//! (re)connect and the daemon's events keep the rows current whether
//! the window is open or not, so showing it reveals the current picture
//! — and a broken link is named outright instead of left as a stale
//! picture or a silent blank.

use std::collections::BTreeMap;
use std::sync::mpsc::Sender;

use echo_ipc::message::{Event, StatusSnapshot, StreamState};

use crate::connection::ClientAsk;
use crate::history_view::{self, HistoryView};
use crate::model::LinkState;

/// One Subscription's row: the snapshot's public identity plus the
/// stream's live state, keyed by Subscription id so events update in
/// place.
#[derive(Debug, Clone, PartialEq, Eq)]
struct SubscriptionRow {
    label: String,
    server: String,
    token_hint: String,
    state: StreamState,
}

/// One row of the window, derived fresh on every [`WindowModel::rows`]
/// call — the words already chosen, ready to paint.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct RowView {
    /// The Subscription's id (its Stream Token's `token_id`) — a future
    /// ticket's remove/inspect action will ride it.
    pub id: String,
    pub label: String,
    /// The server base URL, as the config persisted it.
    pub server: String,
    /// First characters of the Stream Token + `...` — the hint that
    /// names the token without re-exposing it; the secret never leaves
    /// the daemon (spec story 13).
    pub token_hint: String,
    /// The state's words, honest about staleness while the link is down.
    pub state: String,
}

/// The window's whole world: the link's state plus every Subscription's
/// last reported row. The daemon owns the truth; this is its echo — the
/// tray model keeps the glanceable line, this one the full identity.
pub struct WindowModel {
    link: LinkState,
    rows: BTreeMap<String, SubscriptionRow>,
}

impl WindowModel {
    /// A window reaching for the daemon for the first time (or after
    /// the model was reset — which never happens; the shell owns one
    /// for its lifetime).
    pub fn connecting() -> Self {
        Self {
            link: LinkState::Connecting,
            rows: BTreeMap::new(),
        }
    }

    /// The opening snapshot (the connection loop takes one on every
    /// (re)connect): the link is up and the rows are whatever the daemon
    /// says — replacing everything previously believed.
    pub fn apply_snapshot(&mut self, snapshot: &StatusSnapshot) {
        self.link = LinkState::Connected;
        self.rows = snapshot
            .subscriptions
            .iter()
            .map(|(id, runtime)| {
                (
                    id.clone(),
                    SubscriptionRow {
                        label: runtime.label.clone(),
                        server: runtime.server.clone(),
                        token_hint: runtime.token_hint.clone(),
                        state: runtime.state.clone(),
                    },
                )
            })
            .collect();
    }

    /// One daemon-pushed event. State flips update their row in place —
    /// no request, no reopen; History Log appends are consumed without
    /// effect here (the live History Log view builds on them, ticket
    /// #15).
    pub fn apply_event(&mut self, event: &Event) {
        match event {
            Event::StreamState {
                subscription_id,
                label,
                state,
            } => {
                // Self-contained events: a row unknown so far (a
                // Subscription added while we watch — a later ticket's
                // hot apply) still gets its row, its identity fields
                // empty until the next snapshot describes them.
                let row =
                    self.rows
                        .entry(subscription_id.clone())
                        .or_insert_with(|| SubscriptionRow {
                            label: String::new(),
                            server: String::new(),
                            token_hint: String::new(),
                            state: state.clone(),
                        });
                row.label = label.clone();
                row.state = state.clone();
            }
            Event::HistoryAppended { .. } => {}
            // Hot-apply (ticket #14): a Subscription removed while the
            // window watches leaves the list at once — a removed name
            // must not linger as a stale row until the next snapshot.
            Event::SubscriptionRemoved { subscription_id } => {
                self.rows.remove(subscription_id);
            }
        }
    }

    /// The channel broke. The rows stay (the Subscription list's
    /// identity — never a blank window) but lose their claim on
    /// freshness: the daemon banner names the absence and every state
    /// reads "unknown".
    pub fn connection_lost(&mut self) {
        self.link = LinkState::Reconnecting;
    }

    /// Quit clicked (ticket #12): the banner names the stop — the tray
    /// header's own word, one shared state (`LinkState::Quitting`) —
    /// and every row's last-known state loses its freshness claim,
    /// exactly as when the link broke. Idempotent.
    pub fn quitting(&mut self) {
        self.link = LinkState::Quitting;
    }

    /// The link's state, for the render's color choice.
    pub fn link(&self) -> LinkState {
        self.link.clone()
    }

    /// The daemon banner's words, per link state — the tray header's
    /// vocabulary, so both surfaces say the same thing.
    pub fn status_line(&self) -> String {
        match self.link {
            LinkState::Connecting => "Daemon: connecting…".to_owned(),
            LinkState::Connected => "Daemon: connected".to_owned(),
            LinkState::Reconnecting => "Daemon: reconnecting…".to_owned(),
            LinkState::Quitting => "Daemon: shutting down…".to_owned(),
        }
    }

    /// The rows the window paints, sorted by label: the daemon's ids
    /// are hex token ids — no display order lives in them, and a stable
    /// list must not jump.
    pub fn rows(&self) -> Vec<RowView> {
        let mut rows: Vec<(&String, &SubscriptionRow)> = self.rows.iter().collect();
        rows.sort_by(|a, b| a.1.label.cmp(&b.1.label));
        rows.into_iter()
            .map(|(id, row)| RowView {
                id: id.clone(),
                label: row.label.clone(),
                server: row.server.clone(),
                token_hint: row.token_hint.clone(),
                state: self.state_words(&row.state),
            })
            .collect()
    }

    /// The state's words for one row: while the channel is up, the
    /// daemon's own vocabulary — the same words `status` prints; while
    /// it is not, the row says so — a last-known "running" must never
    /// pose as current.
    fn state_words(&self, state: &StreamState) -> String {
        match self.link {
            LinkState::Connected => match state {
                StreamState::Starting => "starting".to_owned(),
                StreamState::Running => "running".to_owned(),
                StreamState::Disconnected { detail } => format!("disconnected — {detail}"),
                // Actionable, not retryable (spec story 21) — the same
                // advice `status` gives.
                StreamState::TokenInvalid => {
                    "token invalid — the Stream Token was revoked or replaced; \
                     re-subscribe this Subscription (`subscribe add`)"
                        .to_owned()
                }
                StreamState::Failed { detail } => format!("stopped — {detail}"),
            },
            LinkState::Connecting | LinkState::Reconnecting | LinkState::Quitting => {
                "unknown (daemon away)".to_owned()
            }
        }
    }
}

/// The Management Window's face: the model's picture laid out, with the
/// picked Subscription's History Log view beneath the list (ticket #15).
/// Thin by contract — the words come from the models; this only
/// arranges them. A click on a Subscription's label becomes one ask on
/// `asks` (the connection loop serves it over its own client); the
/// viewer's content is the answer, never a file read. Returns the ids
/// of the Subscriptions whose Remove button was clicked this frame —
/// the shell hands them to the CRUD model, whose confirmation asks
/// before anything is sent (ticket #14). Called by the shell's `ui`
/// for the visible window; while the window is hidden eframe runs no
/// paint pass at all (App::logic only), so nothing here costs anything
/// until the user opens it.
pub fn render(
    ui: &mut egui::Ui,
    model: &WindowModel,
    history: &mut HistoryView,
    asks: &Sender<ClientAsk>,
) -> Vec<String> {
    // The daemon banner: the link's truth first, colored when it is not
    // the quiet "connected" — the absence is a state, never a blank.
    let mut header = egui::RichText::new(model.status_line()).strong();
    header = match model.link() {
        LinkState::Connected => header,
        // Transitional states get the warm color; the absence gets red.
        // Quitting is transitional too — the stop is underway, and the
        // window is on its way out with the process.
        LinkState::Connecting | LinkState::Quitting => header.color(egui::Color32::YELLOW),
        LinkState::Reconnecting => header.color(egui::Color32::LIGHT_RED),
    };
    ui.label(header);
    ui.separator();

    let mut remove_clicked = Vec::new();
    // The Subscription list: bounded height, so the History Log view
    // below always keeps its ground even with several Subscriptions.
    egui::ScrollArea::vertical()
        .max_height(160.0)
        .show(ui, |ui| {
            let rows = model.rows();
            if rows.is_empty() {
                // Worth saying only when the daemon had its say; while
                // the link is down the banner above already names the
                // absence.
                if model.link() == LinkState::Connected {
                    ui.label("No Subscriptions yet — add one below.");
                }
                return;
            }
            for row in rows {
                ui.add_space(4.0);
                // The label is the pick: clicking it opens that
                // Subscription's History Log below. Re-picking the one
                // already followed asks for nothing (the view says so).
                let picked = history.selected_id() == Some(row.id.as_str());
                if ui
                    .selectable_label(picked, egui::RichText::new(&row.label).strong())
                    .clicked()
                    && let Some(id) = history.select(&row.id, &row.label)
                {
                    let _ = asks.send(ClientAsk::HistorySnapshot {
                        subscription_id: id,
                    });
                }
                ui.label(format!("Server: {}", row.server));
                ui.label(format!("Token hint: {}", row.token_hint));
                ui.label(format!("Realtime Stream: {}", row.state));
                if ui.button("Remove").clicked() {
                    remove_clicked.push(row.id.clone());
                }
                ui.separator();
            }
        });
    // The picked Subscription's History Log (ticket #15): its own
    // scroll, newest last, stuck to the bottom.
    history_view::render(ui, history);
    remove_clicked
}

#[cfg(test)]
mod tests {
    use super::*;
    use echo_ipc::message::{NotificationIntent, SubscriptionRuntime};

    fn empty_snapshot() -> StatusSnapshot {
        StatusSnapshot {
            pid: 1,
            started_at: 0,
            subscriptions: BTreeMap::new(),
        }
    }

    fn snapshot_with_state(state: StreamState) -> StatusSnapshot {
        StatusSnapshot {
            pid: 4242,
            started_at: 1_700_000_000,
            subscriptions: BTreeMap::from([(
                "tok".to_owned(),
                SubscriptionRuntime {
                    label: "desktop".to_owned(),
                    state,
                    last_message_id: None,
                    updated_at: 1_700_000_042,
                    server: "http://127.0.0.1:8080".to_owned(),
                    token_hint: "abcdef123456...".to_owned(),
                },
            )]),
        }
    }

    fn flip(state: StreamState) -> Event {
        Event::StreamState {
            subscription_id: "tok".to_owned(),
            label: "desktop".to_owned(),
            state,
        }
    }

    #[test]
    fn a_fresh_window_reaches_for_the_daemon_and_lists_nothing() {
        let model = WindowModel::connecting();
        assert!(
            model.rows().is_empty(),
            "no subscription picture exists before the snapshot"
        );
        assert_eq!(model.status_line(), "Daemon: connecting…");
    }

    #[test]
    fn the_opening_snapshot_paints_every_row_as_configured() {
        let mut model = WindowModel::connecting();
        model.apply_snapshot(&snapshot_with_state(StreamState::Running));
        let rows = model.rows();
        assert_eq!(rows.len(), 1, "one row per Subscription");
        assert_eq!(rows[0].label, "desktop");
        assert_eq!(
            rows[0].server, "http://127.0.0.1:8080",
            "the server URL as the config persisted it"
        );
        assert_eq!(
            rows[0].token_hint, "abcdef123456...",
            "the hint, never the secret itself"
        );
        assert_eq!(rows[0].state, "running");
        assert_eq!(model.status_line(), "Daemon: connected");
    }

    #[test]
    fn an_event_flips_the_row_in_place_without_a_reopen() {
        let mut model = WindowModel::connecting();
        model.apply_snapshot(&snapshot_with_state(StreamState::Running));
        model.apply_event(&flip(StreamState::Disconnected {
            detail: "connection closed by the server".to_owned(),
        }));
        let rows = model.rows();
        assert_eq!(
            rows[0].state, "disconnected — connection closed by the server",
            "the flip is on screen while the window sits open"
        );
        assert_eq!(
            rows[0].server, "http://127.0.0.1:8080",
            "the identity fields survive the flip"
        );
        assert_eq!(rows[0].token_hint, "abcdef123456...");
    }

    #[test]
    fn a_history_append_is_consumed_without_touching_the_rows() {
        let mut model = WindowModel::connecting();
        model.apply_snapshot(&snapshot_with_state(StreamState::Running));
        let before = model.rows();
        model.apply_event(&Event::HistoryAppended {
            subscription_id: "tok".to_owned(),
            label: "desktop".to_owned(),
            entry: NotificationIntent {
                id: 41,
                title: String::new(),
                subtitle: String::new(),
                body: "the push".to_owned(),
                click_action: None,
                created_at: 1_700_000_000,
            },
        });
        assert_eq!(
            model.rows(),
            before,
            "appends are the History Log view's food"
        );
    }

    #[test]
    fn losing_the_link_names_the_absence_and_dethrones_stale_states() {
        let mut model = WindowModel::connecting();
        model.apply_snapshot(&snapshot_with_state(StreamState::Running));
        model.connection_lost();
        assert_eq!(
            model.status_line(),
            "Daemon: reconnecting…",
            "the daemon's absence is named, not left blank"
        );
        let rows = model.rows();
        assert_eq!(
            rows[0].state, "unknown (daemon away)",
            "a last-known running never poses as current"
        );
        assert_eq!(
            rows[0].label, "desktop",
            "the list keeps its identity while the link is down"
        );
        assert_eq!(rows[0].server, "http://127.0.0.1:8080");
        assert_eq!(rows[0].token_hint, "abcdef123456...");
    }

    #[test]
    fn quitting_names_the_stop_and_dethrones_stale_states() {
        // Ticket #12: during Quit's bounded wait the banner names what
        // is happening — the tray header's own word, one shared state —
        // and the rows lose their freshness claim the same way they do
        // when the link breaks. Nothing updates the window after this.
        let mut model = WindowModel::connecting();
        model.apply_snapshot(&snapshot_with_state(StreamState::Running));
        model.quitting();
        assert_eq!(
            model.status_line(),
            "Daemon: shutting down…",
            "the banner names the stop while the exchange waits"
        );
        let rows = model.rows();
        assert_eq!(
            rows[0].state, "unknown (daemon away)",
            "a last-known running never poses as current during the stop"
        );
    }

    #[test]
    fn the_window_heals_when_the_daemon_returns() {
        let mut model = WindowModel::connecting();
        model.apply_snapshot(&snapshot_with_state(StreamState::Running));
        model.connection_lost();
        // The daemon restarted; the next snapshot is fresh authority.
        model.apply_snapshot(&snapshot_with_state(StreamState::Starting));
        let rows = model.rows();
        assert_eq!(model.status_line(), "Daemon: connected");
        assert_eq!(rows[0].state, "starting", "no residue of the outage");
    }

    #[test]
    fn an_event_for_an_unknown_subscription_still_gets_its_row() {
        // Events are self-contained for label and state; a Subscription
        // the snapshot never named (added while we watch — a later
        // ticket's hot apply) still gets its row, its identity fields
        // empty until the next snapshot describes them.
        let mut model = WindowModel::connecting();
        model.apply_snapshot(&empty_snapshot());
        model.apply_event(&flip(StreamState::Starting));
        let rows = model.rows();
        assert_eq!(rows.len(), 1);
        assert_eq!(rows[0].label, "desktop");
        assert_eq!(rows[0].state, "starting");
        assert_eq!(rows[0].server, "", "no identity was described yet");
        assert_eq!(rows[0].token_hint, "");
    }

    #[test]
    fn a_removal_event_drops_the_row_at_once() {
        // Ticket #14: the daemon announces a hot removal; the row
        // leaves without any poll or reopen — the other rows stand.
        let mut model = WindowModel::connecting();
        let mut snapshot = snapshot_with_state(StreamState::Running);
        snapshot.subscriptions.insert(
            "tok-b".to_owned(),
            SubscriptionRuntime {
                label: "beta".to_owned(),
                state: StreamState::Running,
                last_message_id: None,
                updated_at: 0,
                server: "http://127.0.0.1:8081".to_owned(),
                token_hint: "fed654321098...".to_owned(),
            },
        );
        model.apply_snapshot(&snapshot);
        model.apply_event(&Event::SubscriptionRemoved {
            subscription_id: "tok".to_owned(),
        });
        let rows = model.rows();
        assert_eq!(rows.len(), 1, "the removed Subscription's row is gone");
        assert_eq!(rows[0].label, "beta", "the surviving row stands");
    }

    #[test]
    fn rows_sort_by_label_for_a_stable_window() {
        let mut model = WindowModel::connecting();
        let mut snapshot = empty_snapshot();
        // Inserted in an id order that means nothing for display.
        for (id, label) in [("tok-b", "zebra"), ("tok-a", "alpha")] {
            snapshot.subscriptions.insert(
                id.to_owned(),
                SubscriptionRuntime {
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
        let rows = model.rows();
        assert_eq!(rows[0].label, "alpha");
        assert_eq!(rows[1].label, "zebra");
    }
}
