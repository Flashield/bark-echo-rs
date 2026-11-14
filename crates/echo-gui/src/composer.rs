//! The Push Composer's model (GLOSSARY, ticket #16): the echo-ipc
//! client's view reduced to what the form needs — the Subscription
//! picker's lines (the snapshot's public identity, never secrets), the
//! editable fields mirroring the `push` command's surface (title /
//! subtitle / body plus Ext Params), and the send's lifecycle. The
//! same thin split as the tray and window models: the model owns every
//! word and decision; the render layer only binds widgets to it.
//!
//! The send itself never happens here: [`ComposerModel::request_send`]
//! produces the one [`SendPush`] the shell forwards to the connection
//! loop, and the verdict comes back through
//! [`ComposerModel::apply_send_result`]. Secrets never enter this
//! process (spec story 13) — the send's credentials are the daemon's.

use std::collections::BTreeMap;

use echo_ipc::message::{Event, SendPush, SendPushOutcome, StatusSnapshot};

/// One Subscription line the picker offers: the snapshot's public
/// identity (ticket #13).
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct ComposerSubscription {
    /// The Subscription's id (its Stream Token's `token_id`) — what a
    /// send rides.
    pub id: String,
    /// The Subscription's client label.
    pub label: String,
    /// The server base URL, as the config persisted it.
    pub server: String,
}

/// The send's lifecycle as the form shows it: nothing is silent —
/// the verdict (or the absence) is always named.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum SendState {
    /// Nothing sent (yet).
    Idle,
    /// The send is on its way through the daemon.
    Sending,
    /// The bark server accepted the Push.
    Sent,
    /// The send did not go out or was refused: what happened, readable.
    Failed(String),
}

/// The Composer's whole world: the picker's lines, the form's fields,
/// and the last send's fate. Pure data — the render binds to it.
pub struct ComposerModel {
    subscriptions: Vec<ComposerSubscription>,
    /// The selected Subscription's id, if one is picked.
    selected: Option<String>,
    device_key: String,
    title: String,
    subtitle: String,
    body: String,
    /// The named Ext Params the `push` command names most often.
    url: String,
    group: String,
    sound: String,
    /// Arbitrary Ext Param rows: any other key-value pair, added and
    /// removed on the form.
    ext_rows: Vec<(String, String)>,
    state: SendState,
    /// What blocks a Send click right now — the validation answer,
    /// never a silent dead button.
    notice: Option<String>,
}

impl ComposerModel {
    /// A Composer with nothing picked and nothing typed.
    pub fn new() -> Self {
        Self {
            subscriptions: Vec::new(),
            selected: None,
            device_key: String::new(),
            title: String::new(),
            subtitle: String::new(),
            body: String::new(),
            url: String::new(),
            group: String::new(),
            sound: String::new(),
            ext_rows: Vec::new(),
            state: SendState::Idle,
            notice: None,
        }
    }

    /// The opening snapshot (the connection loop takes one on every
    /// (re)connect): the picker's lines are whatever the daemon says —
    /// sorted by label like the window's rows, a stable list that does
    /// not jump. The selection survives when its Subscription still
    /// exists; otherwise the first line is picked, and an empty
    /// Subscription list picks nothing.
    pub fn apply_snapshot(&mut self, snapshot: &StatusSnapshot) {
        let mut lines: Vec<ComposerSubscription> = snapshot
            .subscriptions
            .iter()
            .map(|(id, runtime)| ComposerSubscription {
                id: id.clone(),
                label: runtime.label.clone(),
                server: runtime.server.clone(),
            })
            .collect();
        lines.sort_by(|a, b| a.label.cmp(&b.label));
        self.selected = self
            .selected
            .take()
            .filter(|selected| lines.iter().any(|line| &line.id == selected))
            .or_else(|| lines.first().map(|line| line.id.clone()));
        self.subscriptions = lines;
    }

    /// One daemon-pushed event. A state flip for a Subscription the
    /// snapshot never named still gets its picker row (the window's
    /// rule); History Log appends are consumed without effect.
    pub fn apply_event(&mut self, event: &Event) {
        if let Event::StreamState {
            subscription_id,
            label,
            ..
        } = event
        {
            if !self
                .subscriptions
                .iter()
                .any(|line| line.id == *subscription_id)
            {
                self.subscriptions.push(ComposerSubscription {
                    id: subscription_id.clone(),
                    label: label.clone(),
                    server: String::new(),
                });
            }
        }
    }

    /// The channel broke. A send on its way can no longer be answered
    /// by this link — it is failed with the absence named, never left
    /// hanging on Sending. (The connection loop answers the queued
    /// request too; whichever arrives first wins, both say the truth.)
    pub fn connection_lost(&mut self) {
        if self.state == SendState::Sending {
            self.state = SendState::Failed("the daemon went away — the Push did not go out".into());
        }
    }

    /// The send's answer (ticket #16): the bark server's verdict, or
    /// why the send never went out. Only a send that is on its way can
    /// consume an answer — a stale result for a form that is not
    /// sending is ignored.
    pub fn apply_send_result(&mut self, result: &Result<SendPushOutcome, String>) {
        if self.state != SendState::Sending {
            return;
        }
        self.state = match result {
            Ok(SendPushOutcome::Sent) => SendState::Sent,
            Ok(SendPushOutcome::Rejected { code, message }) => SendState::Failed(match code {
                Some(code) => format!("the bark server refused the Push (code {code}): {message}"),
                None => format!("the bark server refused the Push: {message}"),
            }),
            Err(message) => SendState::Failed(message.clone()),
        };
    }

    /// Build the one request a Send click produces: the Subscription
    /// reference, the explicit target Device Key, the alert fields
    /// (empty means absent), and the Ext Params — the named fields
    /// first, then the arbitrary rows, so a row sharing a named field's
    /// name has the last word, as on the `push` command line. The
    /// fields pass through verbatim: the server's verdict is the
    /// honest answer to whatever was typed.
    ///
    /// `None` when the click cannot send: the state names what is
    /// missing. One send at a time — the form is busy while Sending.
    pub fn request_send(&mut self) -> Option<SendPush> {
        self.notice = None;
        if self.state == SendState::Sending {
            return None;
        }
        let subscription_id = match &self.selected {
            Some(id) => id.clone(),
            None => {
                self.notice = Some("choose a Subscription to send through first".to_owned());
                return None;
            }
        };
        if self.device_key.is_empty() {
            self.notice = Some("the target Device Key is required".to_owned());
            return None;
        }
        let mut ext_params = BTreeMap::new();
        for (name, field) in [
            ("url", &self.url),
            ("group", &self.group),
            ("sound", &self.sound),
        ] {
            if !field.is_empty() {
                ext_params.insert(name.to_owned(), field.clone());
            }
        }
        for (key, value) in &self.ext_rows {
            if !key.is_empty() {
                ext_params.insert(key.clone(), value.clone());
            }
        }
        self.state = SendState::Sending;
        Some(SendPush {
            subscription_id,
            device_key: self.device_key.clone(),
            title: (!self.title.is_empty()).then(|| self.title.clone()),
            subtitle: (!self.subtitle.is_empty()).then(|| self.subtitle.clone()),
            body: (!self.body.is_empty()).then(|| self.body.clone()),
            ext_params,
        })
    }

    /// The picker's words for the chosen Subscription — its label and
    /// server, the snapshot's public identity; the honest nothing when
    /// no Subscription exists to pick.
    fn selected_text(&self) -> String {
        match self
            .selected
            .as_deref()
            .and_then(|id| self.subscriptions.iter().find(|line| line.id == id))
        {
            Some(line) => format!("{} — {}", line.label, line.server),
            None => "choose a Subscription…".to_owned(),
        }
    }

    /// The send's words for under the Send button, with the color the
    /// render paints them in. Nothing is silent: a verdict and an
    /// absence are both named.
    fn state_line(&self) -> (String, egui::Color32) {
        match &self.state {
            SendState::Idle => (String::new(), egui::Color32::GRAY),
            SendState::Sending => ("Sending…".to_owned(), egui::Color32::YELLOW),
            SendState::Sent => (
                "the bark server accepted the Push".to_owned(),
                egui::Color32::LIGHT_GREEN,
            ),
            SendState::Failed(message) => (message.clone(), egui::Color32::LIGHT_RED),
        }
    }
}

/// The Push Composer's face: the form laid out. Thin by contract — the
/// model owns every word and decision; this binds widgets to its
/// fields and returns the one request a Send click produces (`None`
/// when the click could not send — the notice under the button says
/// why). Called by the shell's `ui` for the visible window; while the
/// window is hidden eframe runs no paint pass at all, and the form's
/// state persists untouched.
///
/// The text fields are egui's own: every glyph resolves through the
/// startup CJK fallback (ADR-0003's red line, fonts.rs), so Chinese —
/// any IME input — renders in the Composer like everywhere else.
pub fn render(ui: &mut egui::Ui, model: &mut ComposerModel) -> Option<SendPush> {
    let mut requested = None;
    ui.strong("Push Composer");

    // The send's context: one Subscription. The picker's lines are the
    // snapshot's public identity — label and server, never secrets.
    egui::ComboBox::from_id_salt("composer_subscription")
        .selected_text(model.selected_text())
        .show_ui(ui, |ui| {
            for line in &model.subscriptions {
                ui.selectable_value(
                    &mut model.selected,
                    Some(line.id.clone()),
                    format!("{} — {}", line.label, line.server),
                );
            }
        });

    // The target Device: the Composer's explicit field (GLOSSARY) —
    // any Device the user can name, mirroring `push --device-key`.
    ui.horizontal(|ui| {
        ui.label("Device Key:");
        ui.add(
            egui::TextEdit::singleline(&mut model.device_key)
                .hint_text("the target Device's Device Key")
                .desired_width(f32::INFINITY),
        );
    });
    ui.horizontal(|ui| {
        ui.label("Title:");
        ui.add(egui::TextEdit::singleline(&mut model.title).desired_width(f32::INFINITY));
    });
    ui.horizontal(|ui| {
        ui.label("Subtitle:");
        ui.add(egui::TextEdit::singleline(&mut model.subtitle).desired_width(f32::INFINITY));
    });
    ui.horizontal(|ui| {
        ui.label("Body:");
        ui.add(
            egui::TextEdit::multiline(&mut model.body)
                .desired_width(f32::INFINITY)
                .desired_rows(2),
        );
    });

    // The Ext Params (GLOSSARY): the named ones the `push` command
    // names most often, plus arbitrary key-value rows added and
    // removed on the form. All of it passes through verbatim.
    ui.strong("Ext Params");
    ui.horizontal(|ui| {
        ui.label("url:");
        ui.add(egui::TextEdit::singleline(&mut model.url).desired_width(200.0));
        ui.label("group:");
        ui.add(egui::TextEdit::singleline(&mut model.group).desired_width(120.0));
        ui.label("sound:");
        ui.add(egui::TextEdit::singleline(&mut model.sound).desired_width(120.0));
    });
    let mut remove = None;
    for (index, (key, value)) in model.ext_rows.iter_mut().enumerate() {
        ui.horizontal(|ui| {
            ui.add(
                egui::TextEdit::singleline(key)
                    .hint_text("key")
                    .desired_width(140.0),
            );
            ui.add(
                egui::TextEdit::singleline(value)
                    .hint_text("value")
                    .desired_width(240.0),
            );
            if ui.button("Remove").clicked() {
                remove = Some(index);
            }
        });
    }
    if let Some(index) = remove {
        model.ext_rows.remove(index);
    }
    if ui.button("Add Ext Param").clicked() {
        model.ext_rows.push((String::new(), String::new()));
    }

    // Send: one click, one request — and the state under it, so the
    // click's outcome (or the reason it could not happen) is always
    // on the form, never silent.
    ui.add_space(4.0);
    ui.horizontal(|ui| {
        let busy = model.state == SendState::Sending;
        if ui.add_enabled(!busy, egui::Button::new("Send")).clicked() {
            requested = model.request_send();
        }
        let (words, color) = model.state_line();
        if !words.is_empty() {
            ui.label(egui::RichText::new(words).color(color));
        }
    });
    if let Some(notice) = &model.notice {
        ui.label(egui::RichText::new(notice).color(egui::Color32::YELLOW));
    }
    requested
}

#[cfg(test)]
mod tests {
    use super::*;
    use echo_ipc::message::{NotificationIntent, SubscriptionRuntime};

    fn snapshot_with(id: &str, label: &str, server: &str) -> StatusSnapshot {
        StatusSnapshot {
            pid: 4242,
            started_at: 1_700_000_000,
            subscriptions: BTreeMap::from([(
                id.to_owned(),
                SubscriptionRuntime {
                    label: label.to_owned(),
                    state: echo_ipc::message::StreamState::Running,
                    last_message_id: None,
                    updated_at: 1_700_000_042,
                    server: server.to_owned(),
                    token_hint: "abcdef123456...".to_owned(),
                },
            )]),
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
    fn a_fresh_composer_offers_no_subscription_and_cannot_send() {
        let mut model = ComposerModel::new();
        assert!(model.request_send().is_none());
        assert_eq!(
            model.notice.as_deref(),
            Some("choose a Subscription to send through first"),
            "the dead button explains itself"
        );
    }

    #[test]
    fn the_snapshot_fills_the_picker_sorted_by_label_and_picks_the_first() {
        let mut model = ComposerModel::new();
        let mut snapshot = StatusSnapshot {
            pid: 1,
            started_at: 0,
            subscriptions: BTreeMap::new(),
        };
        for (id, label, server) in [
            ("tok-b", "zebra", "http://b.example"),
            ("tok-a", "alpha", "http://a.example"),
        ] {
            snapshot.subscriptions.insert(id.to_owned(), {
                SubscriptionRuntime {
                    label: label.to_owned(),
                    state: echo_ipc::message::StreamState::Running,
                    last_message_id: None,
                    updated_at: 0,
                    server: server.to_owned(),
                    token_hint: "abcdef123456...".to_owned(),
                }
            });
        }
        model.apply_snapshot(&snapshot);
        assert_eq!(
            model
                .subscriptions
                .iter()
                .map(|line| line.label.clone())
                .collect::<Vec<_>>(),
            vec!["alpha", "zebra"],
            "a stable picker, like the window's rows"
        );
        assert_eq!(model.selected.as_deref(), Some("tok-a"));
    }

    #[test]
    fn a_full_form_builds_the_exact_request_and_busy_state() {
        let mut model = ComposerModel::new();
        model.apply_snapshot(&snapshot_with("tok", "desktop", "http://127.0.0.1:8080"));
        model.device_key = "device-key-1".to_owned();
        model.title = "你好".to_owned();
        model.subtitle = "the subtitle".to_owned();
        model.body = "the body".to_owned();
        model.url = "https://example.com".to_owned();
        model.group = "notes".to_owned();
        model.sound = "bell".to_owned();
        model.ext_rows = vec![("level".to_owned(), "active".to_owned())];

        let send = model.request_send().expect("the form can send");
        assert_eq!(
            send,
            SendPush {
                subscription_id: "tok".to_owned(),
                device_key: "device-key-1".to_owned(),
                title: Some("你好".to_owned()),
                subtitle: Some("the subtitle".to_owned()),
                body: Some("the body".to_owned()),
                ext_params: BTreeMap::from([
                    ("url".to_owned(), "https://example.com".to_owned()),
                    ("group".to_owned(), "notes".to_owned()),
                    ("sound".to_owned(), "bell".to_owned()),
                    ("level".to_owned(), "active".to_owned()),
                ]),
            },
            "the push command's surface, exactly"
        );
        assert_eq!(model.state, SendState::Sending, "the form is busy");
        assert_eq!(model.notice, None, "no complaint on a valid form");
    }

    #[test]
    fn empty_fields_ride_absent_and_blank_rows_are_skipped() {
        let mut model = ComposerModel::new();
        model.apply_snapshot(&snapshot_with("tok", "desktop", "http://127.0.0.1:8080"));
        model.device_key = "device-key-1".to_owned();
        model.ext_rows = vec![
            (String::new(), "no key, no ride".to_owned()),
            ("kept".to_owned(), String::new()),
        ];
        let send = model.request_send().expect("the form can send");
        assert_eq!(send.title, None);
        assert_eq!(send.subtitle, None);
        assert_eq!(send.body, None);
        assert_eq!(
            send.ext_params,
            BTreeMap::from([("kept".to_owned(), String::new())]),
            "an empty value rides; a blank key names nothing"
        );
    }

    #[test]
    fn an_ext_row_sharing_a_named_field_has_the_last_word() {
        let mut model = ComposerModel::new();
        model.apply_snapshot(&snapshot_with("tok", "desktop", "http://127.0.0.1:8080"));
        model.device_key = "device-key-1".to_owned();
        model.url = "https://first.example".to_owned();
        model.ext_rows = vec![("url".to_owned(), "https://second.example".to_owned())];
        let send = model.request_send().expect("the form can send");
        assert_eq!(
            send.ext_params.get("url").map(String::as_str),
            Some("https://second.example"),
            "the last word wins, as on the `push` command line"
        );
    }

    #[test]
    fn a_blank_device_key_is_refused_with_the_reason() {
        let mut model = ComposerModel::new();
        model.apply_snapshot(&snapshot_with("tok", "desktop", "http://127.0.0.1:8080"));
        assert!(model.request_send().is_none());
        assert_eq!(
            model.notice.as_deref(),
            Some("the target Device Key is required"),
        );
        assert_eq!(model.state, SendState::Idle, "nothing went out");
    }

    #[test]
    fn the_verdict_paints_readably_and_the_form_frees_up() {
        let mut model = ComposerModel::new();
        model.apply_snapshot(&snapshot_with("tok", "desktop", "http://127.0.0.1:8080"));
        model.device_key = "device-key-1".to_owned();
        let _send = model.request_send().unwrap();

        model.apply_send_result(&Ok(SendPushOutcome::Sent));
        assert_eq!(model.state, SendState::Sent, "the acceptance is named");

        // The form is free again: the next click sends.
        assert!(model.request_send().is_some());
    }

    #[test]
    fn a_rejection_shows_the_servers_words_verbatim() {
        let mut model = ComposerModel::new();
        model.apply_snapshot(&snapshot_with("tok", "desktop", "http://127.0.0.1:8080"));
        model.device_key = "device-key-1".to_owned();
        let _send = model.request_send().unwrap();
        model.apply_send_result(&Ok(SendPushOutcome::Rejected {
            code: Some(400),
            message: "failed to get [key] device token from database".to_owned(),
        }));
        assert_eq!(
            model.state,
            SendState::Failed(
                "the bark server refused the Push (code 400): failed to get [key] device token from database"
                    .to_owned()
            ),
            "code and message verbatim, never a paraphrase"
        );
    }

    #[test]
    fn a_send_that_never_went_out_names_why() {
        let mut model = ComposerModel::new();
        model.apply_snapshot(&snapshot_with("tok", "desktop", "http://127.0.0.1:8080"));
        model.device_key = "device-key-1".to_owned();
        let _send = model.request_send().unwrap();
        model.apply_send_result(&Err(
            "the daemon is away — the Push did not go out".to_owned()
        ));
        assert_eq!(
            model.state,
            SendState::Failed("the daemon is away — the Push did not go out".to_owned()),
        );
    }

    #[test]
    fn a_stale_result_is_ignored() {
        // Only a send on its way can consume an answer: a result that
        // arrives when the form is not sending changes nothing.
        let mut model = ComposerModel::new();
        model.apply_send_result(&Ok(SendPushOutcome::Sent));
        assert_eq!(model.state, SendState::Idle);
        model.apply_send_result(&Err("late failure".to_owned()));
        assert_eq!(model.state, SendState::Idle);
    }

    #[test]
    fn losing_the_link_fails_only_a_send_on_its_way() {
        let mut model = ComposerModel::new();
        model.apply_snapshot(&snapshot_with("tok", "desktop", "http://127.0.0.1:8080"));
        model.connection_lost();
        assert_eq!(model.state, SendState::Idle, "nothing was in flight");

        model.device_key = "device-key-1".to_owned();
        let _send = model.request_send().unwrap();
        model.connection_lost();
        assert_eq!(
            model.state,
            SendState::Failed("the daemon went away — the Push did not go out".to_owned()),
            "the in-flight send is failed, never left hanging"
        );
        // And a late answer for it changes nothing.
        model.apply_send_result(&Err(
            "the daemon is away — the Push did not go out".to_owned()
        ));
        assert_eq!(
            model.state,
            SendState::Failed("the daemon went away — the Push did not go out".to_owned())
        );
    }

    #[test]
    fn an_event_for_an_unknown_subscription_still_gets_its_picker_row() {
        let mut model = ComposerModel::new();
        model.apply_snapshot(&StatusSnapshot {
            pid: 1,
            started_at: 0,
            subscriptions: BTreeMap::new(),
        });
        model.apply_event(&Event::StreamState {
            subscription_id: "tok-new".to_owned(),
            label: "newcomer".to_owned(),
            state: echo_ipc::message::StreamState::Starting,
        });
        assert_eq!(
            model.subscriptions,
            vec![ComposerSubscription {
                id: "tok-new".to_owned(),
                label: "newcomer".to_owned(),
                server: String::new(),
            }],
            "self-contained events feed the picker, the window's rule"
        );
    }

    #[test]
    fn a_history_append_is_consumed_without_touching_the_picker() {
        let mut model = ComposerModel::new();
        model.apply_snapshot(&snapshot_with("tok", "desktop", "http://127.0.0.1:8080"));
        let before = model.subscriptions.clone();
        model.apply_event(&appended(41));
        assert_eq!(model.subscriptions, before);
    }
}
