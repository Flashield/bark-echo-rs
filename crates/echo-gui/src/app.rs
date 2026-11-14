//! The eframe application shell (ADR-0003: egui/eframe, glow renderer):
//! one window that lives hidden while the Tray Menu is the whole
//! surface, and shows the Management Window (#13) when the tray item
//! asks. The shell is thin by contract: it drains the connection loop's
//! messages into the two models (tray and window — siblings, each its
//! own state machine), hands tray changes to the tray, dispatches menu
//! clicks, and draws the window. Every nontrivial decision lives in a
//! model, every transport rule in the connection loop.
//!
//! The window's close button hides it, it never ends the app (the tray
//! persists; reopening works repeatedly) — eframe 0.37's close contract,
//! read off its source: a close of the root viewport (the X button, or
//! our own `ViewportCommand::Close`) ends the application unless the
//! pass that sees the request sends `ViewportCommand::CancelClose` —
//! the check runs in both paths, `update` for a visible window and
//! `update_logic_only` for a hidden one. So the shell distinguishes the
//! two asks by its `quitting` flag: a close while not quitting is
//! cancelled and answered with `Visible(false)`; a close after Quit is
//! let through. Quit itself (ticket #12) is an exchange, not a
//! click-through: the click starts it (`quit_in_progress` — the
//! reconnect loop stands down, the shutdown request goes out, a
//! window close still means hide), and the exit close is only sent —
//! with `quitting` set to let it pass — once the exchange concludes,
//! whichever way it concluded.

use std::sync::Arc;
use std::sync::atomic::{AtomicBool, Ordering};
use std::sync::mpsc::{Receiver, Sender};
use std::time::Duration;

use echo_ipc::Endpoint;

use crate::composer::ComposerModel;
use crate::connection::{GuiRequest, UiMsg, spawn_connection_loop};
use crate::fonts;
use crate::model::TrayModel;
use crate::shutdown::{QUIT_BUDGET, QuitOutcome, spawn_quit};
use crate::tray::TrayMenu;
use crate::window::{WindowModel, render};

/// How soon the shell wakes again when nothing happens: menu clicks and
/// channel messages both surface through the polled channels, and this
/// cadence keeps both responsive next to a hidden window that produces
/// no input events of its own.
const WAKE_UP: Duration = Duration::from_millis(100);

/// Why the GUI could not start at all. A daemon that is not there is
/// not an error (the tray shows the reaching state); a window-system
/// failure is.
#[derive(Debug, thiserror::Error)]
pub enum GuiError {
    #[error("the GUI could not start: {0}")]
    Start(String),
}

/// Run the tray-only GUI until the user quits. Called on the main
/// thread (the `gui` subcommand arranges that): eframe/winit's event
/// loop must own the main thread, and on macOS the tray must be created
/// on it too. The channel work runs on its own thread (see
/// [`spawn_connection_loop`]) and arrives here over the mpsc receiver.
pub fn run(endpoint: Endpoint) -> Result<(), GuiError> {
    let (sender, receiver) = std::sync::mpsc::channel();
    // The GUI's own asks (the Composer's send, ticket #16) travel the
    // other way: the shell hands them here, the connection loop
    // performs them on the one client.
    let (requests, request_receiver) = std::sync::mpsc::channel();
    let stop = Arc::new(AtomicBool::new(false));
    // The endpoint and sender ride on: the Quit exchange (ticket #12)
    // opens its own one-shot connection and paints through the same
    // queue the connection loop uses.
    spawn_connection_loop(
        endpoint.clone(),
        sender.clone(),
        request_receiver,
        stop.clone(),
    );

    let options = eframe::NativeOptions {
        // The one window starts hidden (the Tray Menu is the app's
        // always-available face) and becomes the Management Window when
        // the tray item asks; `run_native`'s app id stays "bark-echo"
        // (the persistence key), while the runtime title is set on every
        // show. The size is the Management Window's opening shape —
        // the Push Composer's form above the Subscription list (ticket
        // #16); the hidden shell never paints at it.
        viewport: egui::ViewportBuilder::default()
            .with_title("bark-echo")
            .with_inner_size([640.0, 700.0])
            .with_visible(false),
        renderer: eframe::Renderer::Glow,
        event_loop_builder: Some(Box::new(|builder| {
            // Platform fact (macOS, spec story 17): a Tray Menu behaves
            // like a menu-bar app — the Accessory activation policy
            // keeps the Dock icon off. Set here, before the loop starts,
            // through winit's own extension (eframe's event-loop hook).
            #[cfg(target_os = "macos")]
            {
                use winit::platform::macos::EventLoopBuilderExtMacOS;
                builder.with_activation_policy(winit::platform::macos::ActivationPolicy::Accessory);
            }
            #[cfg(not(target_os = "macos"))]
            let _ = builder;
        })),
        ..Default::default()
    };

    eframe::run_native(
        "bark-echo",
        options,
        Box::new(move |creation_context| {
            // The CJK red line's foundation (ADR-0003): the system font
            // is loaded before any UI exists, so the very first frame
            // of user-defined text renders.
            creation_context
                .egui_ctx
                .set_fonts(fonts::font_definitions());
            Ok(Box::new(TrayApp::new(
                receiver, requests, sender, endpoint, stop,
            )))
        }),
    )
    .map_err(|err| GuiError::Start(err.to_string()))
}

/// The shell: the models' owner on the UI thread.
struct TrayApp {
    receiver: Receiver<UiMsg>,
    /// The asks' door to the connection loop — the Composer's send
    /// goes through it (ticket #16); no other component holds a client.
    requests: Sender<GuiRequest>,
    /// The queue Quit's exchange answers into — the same one the
    /// connection loop paints from.
    sender: Sender<UiMsg>,
    /// The channel endpoint, kept for the Quit exchange's one-shot
    /// connection (the connection loop's connection belongs to its
    /// watching job, not to Quit's).
    endpoint: Endpoint,
    stop: Arc<AtomicBool>,
    model: TrayModel,
    /// The Management Window's picture, fed the same connection stream
    /// as the tray model — whether the window is open or not. Keeping
    /// it current while hidden is what makes opening the window show a
    /// live picture instead of a fetch: the connection loop's snapshot
    /// rides every (re)connect and the events keep it current, so no
    /// on-open request is needed (and none exists to send — the
    /// connection loop owns the client).
    window: WindowModel,
    /// The Push Composer's form (ticket #16), fed the same stream:
    /// the picker's lines from the snapshot, its send's verdicts from
    /// the loop's answers. Its fields persist while the window hides.
    composer: ComposerModel,
    /// Set by the first Quit click (ticket #12): the exchange runs, the
    /// reconnect loop is told to stand down, further clicks and channel
    /// news are drained without effect. Not the close-means-exit flag —
    /// during the exchange a window close still means hide.
    quit_in_progress: bool,
    /// Set once the Quit exchange concluded (any outcome — ticket #12):
    /// the exit close the shell then sends is let through by the close
    /// interception below, instead of being cancelled and hidden.
    quitting: bool,
    /// Built on the first update, not in the constructor: macOS wants
    /// the tray created while the event loop is already running
    /// (tray-icon's platform note; winit's earliest safe point), and
    /// the first update is the first moment that is true. Windows and
    /// the Linux-KSNI backend accept the same arrangement.
    tray: Option<TrayMenu>,
}

impl TrayApp {
    fn new(
        receiver: Receiver<UiMsg>,
        requests: Sender<GuiRequest>,
        sender: Sender<UiMsg>,
        endpoint: Endpoint,
        stop: Arc<AtomicBool>,
    ) -> Self {
        Self {
            receiver,
            requests,
            sender,
            endpoint,
            stop,
            model: TrayModel::connecting(),
            window: WindowModel::connecting(),
            composer: ComposerModel::new(),
            quit_in_progress: false,
            quitting: false,
            tray: None,
        }
    }
}

impl Drop for TrayApp {
    fn drop(&mut self) {
        // The tray drops with the app — the icon leaves with the
        // process (tray-icon's contract). The channel loop winds down
        // behind the stop flag.
        self.stop.store(true, Ordering::Relaxed);
        self.tray = None;
    }
}

impl eframe::App for TrayApp {
    // The shell's whole beat runs in `logic`: eframe calls it before
    // every repaint and — the reason it is the right hook here — also
    // while the window is hidden, which it is whenever the Tray Menu is
    // the only surface. No painting happens in it; painting is `ui`'s.
    fn logic(&mut self, ctx: &egui::Context, _frame: &mut eframe::Frame) {
        if self.tray.is_none() {
            // The tray is built here, not in the constructor: macOS
            // wants the tray created while the event loop is already
            // running (tray-icon's platform note; winit's earliest safe
            // point), and the first logic beat is the first moment that
            // is true. Windows and the Linux-KSNI backend accept the
            // same arrangement.
            match TrayMenu::new(&self.model) {
                Ok(tray) => self.tray = Some(tray),
                Err(err) => eprintln!("the tray could not be built: {err}"),
            }
        }

        // The channel's news, all of it, into the models — the tray's
        // glance, the window's list, and the Composer's picker eat the
        // same snapshot-then-events stream (they all stay current while
        // hidden); the Composer's send answers arrive here too — or
        // everything is drained without effect once Quit is underway:
        // the connection loop is winding down behind the stop flag, and
        // nothing it says can matter to a shell on its way out.
        let mut dirty = false;
        while let Ok(msg) = self.receiver.try_recv() {
            match msg {
                UiMsg::ShutdownOutcome(outcome) => {
                    // Quit's last word — honest about what the daemon
                    // said, and the GUI exits either way (ticket #12:
                    // a stuck or dead daemon must not hold the tray
                    // hostage). The conclusion flips the close gate:
                    // the exit close this sends is the one close the
                    // interception lets through.
                    match outcome {
                        QuitOutcome::Confirmed => {}
                        QuitOutcome::DaemonAbsent => {
                            eprintln!("no daemon answered the quit request; the Tray Menu exits")
                        }
                        QuitOutcome::Unconfirmed => eprintln!(
                            "the daemon did not confirm a graceful shutdown within {:?}; \
                             the Tray Menu exits anyway",
                            QUIT_BUDGET
                        ),
                    }
                    self.quitting = true;
                    ctx.send_viewport_cmd(egui::ViewportCommand::Close);
                    return;
                }
                _ if self.quit_in_progress => {}
                UiMsg::Snapshot(snapshot) => {
                    self.model.apply_snapshot(&snapshot);
                    self.window.apply_snapshot(&snapshot);
                    self.composer.apply_snapshot(&snapshot);
                    dirty = true;
                }
                UiMsg::Event(event) => {
                    self.model.apply_event(&event);
                    self.window.apply_event(&event);
                    self.composer.apply_event(&event);
                    dirty = true;
                }
                UiMsg::SendFinished(result) => {
                    self.composer.apply_send_result(&result);
                    dirty = true;
                }
                UiMsg::LinkDown => {
                    self.model.connection_lost();
                    self.window.connection_lost();
                    self.composer.connection_lost();
                    dirty = true;
                }
            }
        }
        if dirty
            && let Some(tray) = &mut self.tray
            && let Err(err) = tray.refresh(&self.model)
        {
            eprintln!("the tray menu could not refresh: {err}");
        }

        // The user's clicks, all of them. The tray's ids are read
        // through short per-event borrows — a Quit click needs the tray
        // mutable right after, for the shutting-down refresh.
        while let Ok(event) = tray_icon::menu::MenuEvent::receiver().try_recv() {
            let is_quit = self
                .tray
                .as_ref()
                .map(|tray| tray.is_quit(&event))
                .unwrap_or(false);
            let is_open_management = self
                .tray
                .as_ref()
                .map(|tray| tray.is_open_management(&event))
                .unwrap_or(false);
            if is_quit {
                // Quit (ticket #12): ask the daemon for a graceful
                // shutdown, wait a bounded while for its confirmation,
                // then exit. The exchange runs on its own thread with
                // its own one-shot connection — this thread only paints
                // the state and refuses re-entry: a second Quit click
                // is the same instruction, not a new one. The exit
                // close itself is sent when the exchange concludes
                // (any outcome); quitting then lets it through.
                if !self.quit_in_progress {
                    self.quit_in_progress = true;
                    // Both surfaces name the stop — one shared
                    // `LinkState::Quitting` each, the tray's header and
                    // the window's banner.
                    self.model.quitting();
                    self.window.quitting();
                    if let Some(tray) = &mut self.tray
                        && let Err(err) = tray.refresh(&self.model)
                    {
                        eprintln!("the tray menu could not refresh: {err}");
                    }
                    // The reconnect loop stands down before the
                    // exchange starts: its job is watching a daemon
                    // that is on its way out, and a retry behind Quit's
                    // back would re-attach to it and paint noise over
                    // the shutting-down tray.
                    self.stop.store(true, Ordering::Relaxed);
                    spawn_quit(self.endpoint.clone(), self.sender.clone());
                }
            } else if is_open_management {
                // The Management Window (ticket #13): reveal the one
                // viewport as itself. The title names the surface
                // (GLOSSARY) — `run_native`'s app id stays "bark-echo",
                // so persistence is untouched. Focus raises an existing
                // window; egui 0.37 notes it has no effect on Wayland
                // (compositor's call there), which is fine — showing
                // still works.
                ctx.send_viewport_cmd(egui::ViewportCommand::Title(
                    "bark-echo — Management Window".to_owned(),
                ));
                ctx.send_viewport_cmd(egui::ViewportCommand::Visible(true));
                ctx.send_viewport_cmd(egui::ViewportCommand::Focus);
            }
        }

        // The close button asks to close; the app answers. A close
        // that is not a Quit is the Management Window being dismissed:
        // cancel the exit (eframe 0.37: the app ends unless this pass
        // sends CancelClose — both the visible and the hidden path
        // check for it) and hide the viewport instead. The tray, the
        // connection loop, and both models keep running; reopening is
        // another Visible(true).
        let close_requested = ctx.input(|i| i.viewport().close_requested());
        if close_requested && !self.quitting {
            ctx.send_viewport_cmd(egui::ViewportCommand::CancelClose);
            ctx.send_viewport_cmd(egui::ViewportCommand::Visible(false));
        }

        ctx.request_repaint_after(WAKE_UP);
    }

    // The Management Window's paint pass, reached only while the
    // viewport is visible (eframe runs no paint at all while hidden).
    // Thin by contract: the models own every word; this lays them out —
    // the Push Composer's form on top (ticket #16), the Subscription
    // list filling the rest.
    fn ui(&mut self, ui: &mut egui::Ui, _frame: &mut eframe::Frame) {
        if let Some(send) = crate::composer::render(ui, &mut self.composer) {
            // The send rides the connection loop's client; the shell
            // only forwards it (the model's busy state keeps the clicks
            // one at a time).
            let _ = self.requests.send(GuiRequest::SendPush(send));
        }
        ui.separator();
        render(ui, &self.window);
    }
}
