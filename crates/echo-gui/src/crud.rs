//! The Management Window's CRUD face (ticket #14): the add form — a
//! fresh Stream Device for this desktop or an existing Device mirrored
//! by its Device Key, the label required, the operator Basic Auth
//! optional — and the remove confirmation, as one testable state
//! machine. The egui layer paints it and forwards the composed
//! commands; every decision (required fields, the compose, the
//! confirm) lives here, the same thin split as the tray and the
//! window: the model owns every word, the paint owns none.
//!
//! The composed commands ride the connection loop (the loop owns the
//! client, ADR-0004); the model never talks to the daemon itself, and
//! the secrets typed into the form leave for the daemon at once — the
//! GUI process keeps no copy (spec story 13).

use crate::connection::CrudCommand;
use echo_ipc::message::{AddSource, BasicAuthCredentials};

/// Which Device a new Subscription is built on (GLOSSARY.md: one
/// Subscription mirrors exactly one Device).
#[derive(Debug, Clone, Copy, Default, PartialEq, Eq)]
pub enum AddMode {
    /// Register a fresh Stream Device for this desktop.
    #[default]
    Own,
    /// Mirror an existing Device by its Device Key.
    Mirror,
}

/// The add form's fields as the user left them — kept verbatim so a
/// refused submit comes back with the values intact.
#[derive(Debug, Clone, PartialEq, Eq, Default)]
pub struct AddForm {
    /// Own Device or mirror (GLOSSARY).
    pub mode: AddMode,
    /// The bark server base URL, e.g. `http://127.0.0.1:8080`.
    pub server: String,
    /// The Device Key to mirror — used only in mirror mode.
    pub mirror_device_key: String,
    /// The client label naming the Stream Token (GLOSSARY) — required.
    pub label: String,
    /// Whether the operator Basic Auth (§1.4) fields are in play.
    pub use_basic_auth: bool,
    /// The operator Basic Auth user.
    pub auth_user: String,
    /// The operator Basic Auth password — shown typed-over, sent to the
    /// daemon on submit, never kept.
    pub auth_password: String,
}

/// The model's stages: the idle list, the add form, or a removal
/// waiting for its confirmation.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum CrudState {
    /// The list view — the add button is the only ask.
    Idle,
    /// The add form is open, collecting [`AddForm`].
    Adding(AddForm),
    /// A removal was asked for this Subscription (id and its label as
    /// the list shows it) and awaits the confirmation.
    ConfirmingRemove { id: String, label: String },
}

/// The CRUD model: which stage, whether an action is in flight, and
/// the last refusal's words — shown where the form can be fixed.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct CrudModel {
    state: CrudState,
    submitting: bool,
    error: Option<String>,
}

impl Default for CrudModel {
    fn default() -> Self {
        Self::new()
    }
}

impl CrudModel {
    /// The idle model the shell starts with.
    pub fn new() -> Self {
        Self {
            state: CrudState::Idle,
            submitting: false,
            error: None,
        }
    }

    /// The model's stage — the render's switch.
    pub fn state(&self) -> &CrudState {
        &self.state
    }

    /// Whether an action is riding the channel — submit buttons lock.
    pub fn is_submitting(&self) -> bool {
        self.submitting
    }

    /// The last refusal's words, if any.
    pub fn error(&self) -> Option<&str> {
        self.error.as_deref()
    }

    /// Open the add form (an empty own-mode form).
    pub fn open_add(&mut self) {
        if self.submitting {
            return;
        }
        self.state = CrudState::Adding(AddForm {
            mode: AddMode::Own,
            ..AddForm::default()
        });
        self.error = None;
    }

    /// Ask to remove the named Subscription: the confirmation names it
    /// before anything is sent.
    pub fn begin_remove(&mut self, id: &str, label: &str) {
        if self.submitting {
            return;
        }
        self.state = CrudState::ConfirmingRemove {
            id: id.to_owned(),
            label: label.to_owned(),
        };
        self.error = None;
    }

    /// Leave the form or the confirmation without doing anything.
    pub fn cancel(&mut self) {
        if self.submitting {
            return;
        }
        self.state = CrudState::Idle;
        self.error = None;
    }

    /// Submit the add form: validate, then compose the command. A
    /// refusal of validation stays in the form with the reason; a
    /// valid submit locks the model until [`CrudModel::action_finished`].
    pub fn submit_add(&mut self) -> Option<CrudCommand> {
        let CrudState::Adding(form) = self.state.clone() else {
            return None;
        };
        if self.submitting {
            return None;
        }
        match validate_add(&form) {
            Ok(command) => {
                self.submitting = true;
                self.error = None;
                Some(command)
            }
            Err(message) => {
                self.error = Some(message);
                None
            }
        }
    }

    /// Confirm the removal: compose the command and lock until
    /// [`CrudModel::action_finished`].
    pub fn confirm_remove(&mut self) -> Option<CrudCommand> {
        let CrudState::ConfirmingRemove { id, .. } = self.state.clone() else {
            return None;
        };
        if self.submitting {
            return None;
        }
        self.submitting = true;
        self.error = None;
        Some(CrudCommand::Remove { id })
    }

    /// The action's outcome came back. Success closes everything — the
    /// fresh snapshot that follows repaints the rows. A refusal keeps
    /// the stage editable with the daemon's words showing, so the
    /// values can be fixed and submitted again.
    pub fn action_finished(&mut self, outcome: Result<(), String>) {
        self.submitting = false;
        match outcome {
            Ok(()) => {
                self.state = CrudState::Idle;
                self.error = None;
            }
            Err(message) => {
                self.error = Some(message);
            }
        }
    }
}

/// The add form's validation: the words that must be there before the
/// daemon is asked for anything. Every message is for the user and
/// speaks GLOSSARY.
fn validate_add(form: &AddForm) -> Result<CrudCommand, String> {
    let server = form.server.trim();
    if server.is_empty() {
        return Err("Server URL is required — e.g. http://127.0.0.1:8080.".to_owned());
    }
    let label = form.label.trim();
    if label.is_empty() {
        return Err("A label is required — the Stream Token is minted with it.".to_owned());
    }
    let source = match form.mode {
        AddMode::Own => AddSource::Own,
        AddMode::Mirror => {
            let device_key = form.mirror_device_key.trim();
            if device_key.is_empty() {
                return Err("A Device Key is required to mirror an existing Device.".to_owned());
            }
            AddSource::DeviceKey {
                device_key: device_key.to_owned(),
            }
        }
    };
    let basic_auth = if form.use_basic_auth {
        let user = form.auth_user.trim();
        let password = form.auth_password.trim();
        if user.is_empty() && password.is_empty() {
            return Err("Basic Auth needs a user or a password.".to_owned());
        }
        // An empty field means "not given" — a server may enable the
        // check on user and password independently (§1.4).
        Some(BasicAuthCredentials {
            user: user.to_owned(),
            password: password.to_owned(),
        })
    } else {
        None
    };
    Ok(CrudCommand::Add {
        server: server.to_owned(),
        source,
        label: label.to_owned(),
        basic_auth,
    })
}

/// The CRUD face's paint: the add button when idle, the form when
/// open, the confirmation when a removal was asked. Thin by contract —
/// the model owns every word and decision; this arranges them and
/// hands back the one command composed this frame, if any.
pub fn render(ui: &mut egui::Ui, crud: &mut CrudModel) -> Option<CrudCommand> {
    let mut composed = None;
    match crud.state().clone() {
        CrudState::Idle => {
            ui.add_space(4.0);
            let locked = crud.is_submitting();
            ui.add_enabled_ui(!locked, |ui| {
                if ui.button("Add Subscription…").clicked() {
                    crud.open_add();
                }
            });
            show_error(ui, crud);
        }
        CrudState::Adding(_) => {
            ui.add_space(4.0);
            ui.strong("Add Subscription");
            // The fields edit the model's form in place — the typing
            // must survive across frames — while the buttons only
            // record their clicks: the model calls run after the
            // borrow ends.
            let locked = crud.is_submitting();
            let mut add_clicked = false;
            let mut cancel_clicked = false;
            if let CrudState::Adding(form) = &mut crud.state {
                ui.add_enabled_ui(!locked, |ui| {
                    egui::Grid::new("crud_add_form")
                        .num_columns(2)
                        .show(ui, |ui| {
                            ui.label("Server URL:");
                            ui.add(
                                egui::TextEdit::singleline(&mut form.server)
                                    .hint_text("http://127.0.0.1:8080"),
                            );
                            ui.end_row();
                            ui.label("Device:");
                            ui.horizontal(|ui| {
                                ui.radio_value(
                                    &mut form.mode,
                                    AddMode::Own,
                                    "This desktop (own Stream Device)",
                                );
                                ui.radio_value(&mut form.mode, AddMode::Mirror, "Mirror a Device");
                            });
                            ui.end_row();
                            if form.mode == AddMode::Mirror {
                                ui.label("Device Key:");
                                ui.add(
                                    egui::TextEdit::singleline(&mut form.mirror_device_key)
                                        .hint_text("the key of the Device to mirror"),
                                );
                                ui.end_row();
                            }
                            ui.label("Label:");
                            ui.add(
                                egui::TextEdit::singleline(&mut form.label)
                                    .hint_text("names the Stream Token"),
                            );
                            ui.end_row();
                            ui.label("Basic Auth:");
                            ui.checkbox(
                                &mut form.use_basic_auth,
                                "the server demands operator credentials (§1.4)",
                            );
                            ui.end_row();
                            if form.use_basic_auth {
                                ui.label("User:");
                                ui.add(egui::TextEdit::singleline(&mut form.auth_user));
                                ui.end_row();
                                ui.label("Password:");
                                ui.add(
                                    egui::TextEdit::singleline(&mut form.auth_password)
                                        .password(true),
                                );
                                ui.end_row();
                            }
                        });
                    ui.horizontal(|ui| {
                        if ui.button("Add").clicked() {
                            add_clicked = true;
                        }
                        if ui.button("Cancel").clicked() {
                            cancel_clicked = true;
                        }
                        if locked {
                            ui.label("Sending…");
                        }
                    });
                });
            }
            if add_clicked {
                composed = crud.submit_add();
            }
            if cancel_clicked {
                crud.cancel();
            }
            show_error(ui, crud);
        }
        CrudState::ConfirmingRemove { id, label } => {
            ui.add_space(4.0);
            ui.strong(format!("Remove Subscription \"{label}\"?"));
            ui.label(
                "Its Realtime Stream stops immediately and its secrets are erased. \
                 This cannot be undone.",
            );
            let locked = crud.is_submitting();
            ui.add_enabled_ui(!locked, |ui| {
                ui.horizontal(|ui| {
                    if ui.button("Remove").clicked() {
                        composed = crud.confirm_remove();
                    }
                    if ui.button("Keep it").clicked() {
                        crud.cancel();
                    }
                    if locked {
                        ui.label("Sending…");
                    }
                });
            });
            show_error(ui, crud);
            // The id rides the command above; nothing paints it.
            let _ = id;
        }
    }
    composed
}

/// The last refusal's words, in the alarm color — shown where the
/// form can be fixed.
fn show_error(ui: &mut egui::Ui, crud: &CrudModel) {
    if let Some(error) = crud.error() {
        ui.label(egui::RichText::new(error).color(egui::Color32::LIGHT_RED));
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn own_form() -> AddForm {
        AddForm {
            mode: AddMode::Own,
            server: "http://127.0.0.1:8080".to_owned(),
            label: "desktop".to_owned(),
            ..AddForm::default()
        }
    }

    #[test]
    fn an_idle_model_opens_the_add_form_and_cancels_back() {
        let mut crud = CrudModel::new();
        assert_eq!(crud.state(), &CrudState::Idle);
        crud.open_add();
        assert_eq!(
            crud.state(),
            &CrudState::Adding(AddForm {
                mode: AddMode::Own,
                ..AddForm::default()
            }),
            "the form opens empty, own mode first"
        );
        crud.cancel();
        assert_eq!(crud.state(), &CrudState::Idle);
    }

    #[test]
    fn the_add_form_composes_the_own_command_when_the_required_words_are_there() {
        let mut crud = CrudModel::new();
        crud.open_add();
        if let CrudState::Adding(form) = &mut crud.state {
            *form = own_form();
        }
        let command = crud
            .submit_add()
            .expect("a complete own form composes the command");
        assert_eq!(
            command,
            CrudCommand::Add {
                server: "http://127.0.0.1:8080".to_owned(),
                source: AddSource::Own,
                label: "desktop".to_owned(),
                basic_auth: None,
            }
        );
        assert!(crud.is_submitting(), "the submit locks until the outcome");
    }

    #[test]
    fn the_add_form_refuses_to_compose_without_the_required_words() {
        // Each missing word gets its own sentence, and the form stays
        // open and unlocked for fixing.
        let cases: Vec<(AddForm, &str)> = vec![
            (
                AddForm {
                    mode: AddMode::Own,
                    server: String::new(),
                    label: "desktop".to_owned(),
                    ..AddForm::default()
                },
                "Server URL is required",
            ),
            (
                AddForm {
                    mode: AddMode::Own,
                    server: "http://127.0.0.1:8080".to_owned(),
                    label: "   ".to_owned(),
                    ..AddForm::default()
                },
                "A label is required",
            ),
            (
                AddForm {
                    mode: AddMode::Mirror,
                    server: "http://127.0.0.1:8080".to_owned(),
                    label: "the iPhone".to_owned(),
                    mirror_device_key: String::new(),
                    ..AddForm::default()
                },
                "A Device Key is required",
            ),
            (
                AddForm {
                    mode: AddMode::Own,
                    server: "http://127.0.0.1:8080".to_owned(),
                    label: "desktop".to_owned(),
                    use_basic_auth: true,
                    ..AddForm::default()
                },
                "Basic Auth needs a user or a password",
            ),
        ];
        for (form, expected) in cases {
            let mut crud = CrudModel::new();
            crud.state = CrudState::Adding(form);
            let command = crud.submit_add();
            assert!(command.is_none(), "an incomplete form composes nothing");
            let error = crud.error().expect("the refusal names the fix");
            assert!(
                error.contains(expected),
                "the words point at the fix: {error}"
            );
            assert!(!crud.is_submitting(), "a refused submit does not lock");
        }
    }

    #[test]
    fn the_add_form_composes_the_mirror_command_with_its_labelled_token_request() {
        let mut crud = CrudModel::new();
        crud.state = CrudState::Adding(AddForm {
            mode: AddMode::Mirror,
            server: "https://api.day.app".to_owned(),
            mirror_device_key: "  AbCdEf1234567890GhIjKl ".to_owned(),
            label: "the iPhone".to_owned(),
            use_basic_auth: true,
            auth_user: "operator".to_owned(),
            auth_password: "op-secret".to_owned(),
        });
        let command = crud.submit_add().expect("a complete mirror form composes");
        assert_eq!(
            command,
            CrudCommand::Add {
                server: "https://api.day.app".to_owned(),
                source: AddSource::DeviceKey {
                    device_key: "AbCdEf1234567890GhIjKl".to_owned(),
                },
                label: "the iPhone".to_owned(),
                basic_auth: Some(BasicAuthCredentials {
                    user: "operator".to_owned(),
                    password: "op-secret".to_owned(),
                }),
            },
            "the fields are trimmed, the credentials ride to the daemon only"
        );
    }

    #[test]
    fn a_removal_asks_for_confirmation_then_composes_the_remove_command() {
        let mut crud = CrudModel::new();
        crud.begin_remove("tok-1", "desktop");
        assert_eq!(
            crud.state(),
            &CrudState::ConfirmingRemove {
                id: "tok-1".to_owned(),
                label: "desktop".to_owned(),
            },
            "nothing is sent before the confirmation"
        );
        // Cancel first: nothing left the model.
        crud.cancel();
        assert_eq!(crud.state(), &CrudState::Idle);
        crud.begin_remove("tok-1", "desktop");
        let command = crud.confirm_remove().expect("the confirmation composes");
        assert_eq!(
            command,
            CrudCommand::Remove {
                id: "tok-1".to_owned()
            }
        );
        assert!(crud.is_submitting());
    }

    #[test]
    fn a_finished_action_closes_and_a_refusal_keeps_the_stage_editable() {
        let mut crud = CrudModel::new();
        crud.open_add();
        if let CrudState::Adding(form) = &mut crud.state {
            *form = own_form();
        }
        let _ = crud.submit_add().expect("composes");
        crud.action_finished(Ok(()));
        assert_eq!(
            crud.state(),
            &CrudState::Idle,
            "success closes the form — the fresh snapshot repaints"
        );
        assert!(!crud.is_submitting());
        assert_eq!(crud.error(), None);

        // A refusal: the form stays with its values, the daemon's words
        // show, and a resubmit is possible.
        crud.open_add();
        if let CrudState::Adding(form) = &mut crud.state {
            *form = own_form();
        }
        let _ = crud.submit_add().expect("composes");
        crud.action_finished(Err(
            "the daemon refused the request: device not found".to_owned()
        ));
        assert!(
            matches!(crud.state(), CrudState::Adding(_)),
            "the refused add keeps the form"
        );
        assert_eq!(
            crud.error(),
            Some("the daemon refused the request: device not found"),
            "the daemon's words show where the form can be fixed"
        );
        assert!(!crud.is_submitting(), "editable again");
        assert!(
            crud.submit_add().is_some(),
            "the user can submit again without retyping"
        );
    }

    #[test]
    fn nothing_composes_while_an_action_is_in_flight() {
        let mut crud = CrudModel::new();
        crud.state = CrudState::Adding(own_form());
        let _ = crud.submit_add().expect("composes");
        assert!(crud.submit_add().is_none(), "the lock holds");
        crud.begin_remove("tok-1", "desktop");
        assert!(
            !matches!(crud.state(), CrudState::ConfirmingRemove { .. }),
            "no stage change mid-flight"
        );
        crud.open_add();
        assert!(
            matches!(crud.state(), CrudState::Adding(_)),
            "no form reset mid-flight"
        );
        // The outcome settles it either way.
        crud.action_finished(Ok(()));
        assert_eq!(crud.state(), &CrudState::Idle);
    }
}
