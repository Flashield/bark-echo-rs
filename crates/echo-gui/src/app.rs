//! The eframe application shell (ADR-0003: egui/eframe, glow renderer):
//! a tray-only app — the event loop runs without a visible window (the
//! Management Window is #13) and everything the user touches is the
//! Tray Menu. The shell is thin by contract: it drains the connection
//! loop's messages into the model, hands model changes to the tray, and
//! dispatches menu clicks. Every nontrivial decision lives in the
//! model, every transport rule in the connection loop.

use std::sync::Arc;
use std::sync::atomic::{AtomicBool, Ordering};
use std::sync::mpsc::Receiver;
use std::time::Duration;

use echo_ipc::Endpoint;

use crate::connection::{UiMsg, spawn_connection_loop};
use crate::fonts;
use crate::model::TrayModel;
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
    spawn_connection_loop(endpoint, sender, stop.clone());

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
            Ok(Box::new(TrayApp::new(receiver, stop)))
        }),
    )
    .map_err(|err| GuiError::Start(err.to_string()))
}

/// The shell: the model's owner on the UI thread.
struct TrayApp {
    receiver: Receiver<UiMsg>,
    stop: Arc<AtomicBool>,
    model: TrayModel,
    /// Built on the first update, not in the constructor: macOS wants
    /// the tray created while the event loop is already running
    /// (tray-icon's platform note; winit's earliest safe point), and
    /// the first update is the first moment that is true. Windows and
    /// the Linux-KSNI backend accept the same arrangement.
    tray: Option<TrayMenu>,
}

impl TrayApp {
    fn new(receiver: Receiver<UiMsg>, stop: Arc<AtomicBool>) -> Self {
        Self {
            receiver,
            stop,
            model: TrayModel::connecting(),
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

        // The channel's news, all of it, into the model.
        let mut dirty = false;
        while let Ok(msg) = self.receiver.try_recv() {
            match msg {
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

        // The user's clicks, all of them.
        if let Some(tray) = &self.tray {
            while let Ok(event) = tray_icon::menu::MenuEvent::receiver().try_recv() {
                if tray.is_quit(&event) {
                    // Ticket #11's Quit: exit the GUI process. Asking
                    // the daemon for a graceful shutdown is #12.
                    ctx.send_viewport_cmd(egui::ViewportCommand::Close);
                } else if tray.is_open_management(&event) {
                    // Placeholder: the Management Window is ticket #13.
                }
            }
        }

        ctx.request_repaint_after(WAKE_UP);
    }

    // Required by the trait, empty by design: the window is invisible
    // (tray-only), so there is nothing to draw. Every user-visible
    // thing is the native tray menu, driven from `logic`.
    fn ui(&mut self, _ui: &mut egui::Ui, _frame: &mut eframe::Frame) {}
}
