//! The eframe application shell (ADR-0003: egui/eframe, glow renderer):
//! one window that lives hidden while the Tray Menu is the whole
//! surface, and shows the Management Window (#13) when the tray item
//! asks. The shell is thin by contract: it drains the connection loop's
//! messages into the models (tray and window and the History Log
//! viewer — siblings, each its own state machine), hands tray changes
//! to the tray, dispatches menu clicks, and draws the window. Every
//! nontrivial decision lives in a model, every transport rule in the
//! connection loop.
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
//! let through.

use std::sync::Arc;
use std::sync::atomic::{AtomicBool, Ordering};
use std::sync::mpsc::{Receiver, Sender};
use std::time::Duration;

use echo_ipc::Endpoint;

use crate::connection::{ClientAsk, UiMsg, spawn_connection_loop};
use crate::fonts;
use crate::history_view::HistoryView;
use crate::model::TrayModel;
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
    // The window's asks ride the reverse direction: a History Log pick
    // (ticket #15) becomes one request the connection loop serves over
    // its own client — the surfaces never touch the channel themselves.
    let (ask_sender, ask_receiver) = std::sync::mpsc::channel();
    let stop = Arc::new(AtomicBool::new(false));
    spawn_connection_loop(endpoint, sender, ask_receiver, stop.clone());

    let options = eframe::NativeOptions {
        // The one window starts hidden (the Tray Menu is the app's
        // always-available face) and becomes the Management Window when
        // the tray item asks; `run_native`'s app id stays "bark-echo"
        // (the persistence key), while the runtime title is set on every
        // show. The size is the Management Window's opening shape — the
        // hidden shell never paints at it.
        viewport: egui::ViewportBuilder::default()
            .with_title("bark-echo")
            .with_inner_size([560.0, 420.0])
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
            Ok(Box::new(TrayApp::new(receiver, ask_sender, stop)))
        }),
    )
    .map_err(|err| GuiError::Start(err.to_string()))
}

/// The shell: the models' owner on the UI thread.
struct TrayApp {
    receiver: Receiver<UiMsg>,
    /// The window's asks out to the connection loop (a History Log pick,
    /// ticket #15).
    asks: Sender<ClientAsk>,
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
    /// The picked Subscription's History Log viewer (ticket #15): fed
    /// the same stream, its one request going out over `asks`.
    history: HistoryView,
    /// Set by the tray's Quit so the close interception below knows
    /// this close means exit, not hide.
    quitting: bool,
    /// Built on the first update, not in the constructor: macOS wants
    /// the tray created while the event loop is already running
    /// (tray-icon's platform note; winit's earliest safe point), and
    /// the first update is the first moment that is true. Windows and
    /// the Linux-KSNI backend accept the same arrangement.
    tray: Option<TrayMenu>,
}

impl TrayApp {
    fn new(receiver: Receiver<UiMsg>, asks: Sender<ClientAsk>, stop: Arc<AtomicBool>) -> Self {
        Self {
            receiver,
            asks,
            stop,
            model: TrayModel::connecting(),
            window: WindowModel::connecting(),
            history: HistoryView::default(),
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
        // glance, the window's list, and the History Log viewer eat the
        // same snapshot-then-events stream (each stays current while
        // hidden). The viewer re-asks on every fresh authority: its
        // request goes out over the ask channel and the answer comes
        // back here as a message.
        let mut dirty = false;
        while let Ok(msg) = self.receiver.try_recv() {
            match msg {
                UiMsg::Snapshot(snapshot) => {
                    self.model.apply_snapshot(&snapshot);
                    self.window.apply_snapshot(&snapshot);
                    if let Some(id) = self.history.daemon_back() {
                        let _ = self.asks.send(ClientAsk::HistorySnapshot {
                            subscription_id: id,
                        });
                    }
                    dirty = true;
                }
                UiMsg::Event(event) => {
                    self.model.apply_event(&event);
                    self.window.apply_event(&event);
                    self.history.apply_append(&event);
                    dirty = true;
                }
                UiMsg::LinkDown => {
                    self.model.connection_lost();
                    self.window.connection_lost();
                    self.history.connection_lost();
                    dirty = true;
                }
                UiMsg::HistoryLoaded {
                    subscription_id,
                    entries,
                } => {
                    self.history.snapshot_loaded(&subscription_id, &entries);
                    dirty = true;
                }
                UiMsg::HistoryUnavailable {
                    subscription_id,
                    reason,
                } => {
                    self.history.snapshot_failed(&subscription_id, &reason);
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
                    // the daemon for a graceful shutdown is #12. The
                    // flag tells the close handling below to let this
                    // one through.
                    self.quitting = true;
                    ctx.send_viewport_cmd(egui::ViewportCommand::Close);
                } else if tray.is_open_management(&event) {
                    // The Management Window (ticket #13): reveal the
                    // one viewport as itself. The title names the
                    // surface (GLOSSARY) — `run_native`'s app id stays
                    // "bark-echo", so persistence is untouched. Focus
                    // raises an existing window; egui 0.37 notes it has
                    // no effect on Wayland (compositor's call there),
                    // which is fine — showing still works.
                    ctx.send_viewport_cmd(egui::ViewportCommand::Title(
                        "bark-echo — Management Window".to_owned(),
                    ));
                    ctx.send_viewport_cmd(egui::ViewportCommand::Visible(true));
                    ctx.send_viewport_cmd(egui::ViewportCommand::Focus);
                }
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
    // Thin by contract: the models own every word; this lays them out.
    fn ui(&mut self, ui: &mut egui::Ui, _frame: &mut eframe::Frame) {
        render(ui, &self.window, &mut self.history, &self.asks);
    }
}
