//! The eframe application shell (ADR-0003: egui/eframe, glow renderer):
//! a tray-only app — the event loop runs without a visible window (the
//! Management Window is #13) and everything the user touches is the
//! Tray Menu. The shell is thin by contract: it drains the connection
//! loop's messages into the model, hands model changes to the tray, and
//! dispatches menu clicks. Every nontrivial decision lives in the
//! model, every transport rule in the connection loop.

use std::sync::Arc;
use std::sync::atomic::{AtomicBool, Ordering};
use std::sync::mpsc::{Receiver, Sender};
use std::time::Duration;

use echo_ipc::Endpoint;

use crate::connection::{UiMsg, spawn_connection_loop};
use crate::fonts;
use crate::model::TrayModel;
use crate::shutdown::{QUIT_BUDGET, QuitOutcome, spawn_quit};
use crate::tray::TrayMenu;

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
    let stop = Arc::new(AtomicBool::new(false));
    spawn_connection_loop(endpoint.clone(), sender.clone(), stop.clone());

    let options = eframe::NativeOptions {
        // Tray-only: no window yet — the Tray Menu is the whole surface
        // of ticket #11. The title stays (it names the process to the
        // window system and becomes eframe's persistence id).
        viewport: egui::ViewportBuilder::default()
            .with_title("bark-echo")
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
            Ok(Box::new(TrayApp::new(receiver, sender, endpoint, stop)))
        }),
    )
    .map_err(|err| GuiError::Start(err.to_string()))
}

/// The shell: the model's owner on the UI thread.
struct TrayApp {
    receiver: Receiver<UiMsg>,
    /// The queue Quit's exchange answers into — the same one the
    /// connection loop paints from.
    sender: Sender<UiMsg>,
    /// The channel endpoint, kept for the Quit exchange's one-shot
    /// connection (the connection loop's connection belongs to its
    /// watching job, not to Quit's).
    endpoint: Endpoint,
    stop: Arc<AtomicBool>,
    model: TrayModel,
    /// Set by the first Quit click: the exchange runs, the reconnect
    /// loop is told to stand down, further clicks and channel news are
    /// drained without effect.
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
        sender: Sender<UiMsg>,
        endpoint: Endpoint,
        stop: Arc<AtomicBool>,
    ) -> Self {
        Self {
            receiver,
            sender,
            endpoint,
            stop,
            model: TrayModel::connecting(),
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
    // while the window is hidden, which this tray-only window always
    // is. No painting happens in it; the window has no UI to show.
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

        // The channel's news, all of it, into the model — or drained
        // without effect once Quit is underway: the connection loop is
        // winding down behind the stop flag, and nothing it says can
        // matter to a tray on its way out.
        let mut dirty = false;
        while let Ok(msg) = self.receiver.try_recv() {
            match msg {
                UiMsg::ShutdownOutcome(outcome) => {
                    // Quit's last word — honest about what the daemon
                    // said, and the GUI exits either way (ticket #12:
                    // a stuck or dead daemon must not hold the tray
                    // hostage).
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
                    ctx.send_viewport_cmd(egui::ViewportCommand::Close);
                    return;
                }
                _ if self.quitting => {}
                UiMsg::Snapshot(snapshot) => {
                    self.model.apply_snapshot(&snapshot);
                    dirty = true;
                }
                UiMsg::Event(event) => {
                    self.model.apply_event(&event);
                    dirty = true;
                }
                UiMsg::LinkDown => {
                    self.model.connection_lost();
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
                // is the same instruction, not a new one.
                if !self.quitting {
                    self.quitting = true;
                    self.model.quitting();
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
                // Placeholder: the Management Window is ticket #13.
            }
        }

        ctx.request_repaint_after(WAKE_UP);
    }

    // Required by the trait, empty by design: the window is invisible
    // (tray-only), so there is nothing to draw. Every user-visible
    // thing is the native tray menu, driven from `logic`.
    fn ui(&mut self, _ui: &mut egui::Ui, _frame: &mut eframe::Frame) {}
}
