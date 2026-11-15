//! The Tray Menu host (issue #30, ADR-0005): a bare winit event loop
//! with no windows — the tray icon and its muda menu are the whole
//! native face. The daemon's management surface is the web page the
//! daemon serves (ADR-0005); this host is its desktop presence:
//! spawn-if-absent inherited from the old GUI (ADR-0004), a glance at
//! each Subscription's stream state, Manage handing off to the default
//! browser at the face's address, and Quit composing the ADR-0004
//! graceful-shutdown exchange.
//!
//! Threading facts (tray-icon's platform notes, CODING_STANDARDS: name
//! the platform reality): macOS runs the event loop on the main thread
//! and the tray must be created there — the Accessory activation
//! policy set before the loop starts hides the Dock icon; Windows
//! needs the event loop on the tray's creating thread. Both hold when
//! one loop owns the main thread and the tray is built from the
//! handler's `resumed`, with the loop already running. Linux's KSNI
//! backend serves the icon from its own worker thread and needs none
//! of this; the same arrangement keeps one shape on all three.
//!
//! The channel work lives on its own thread
//! ([`connection::spawn_connection_loop`]) and reaches the loop
//! through the winit proxy — every message wakes it. Two pumps feed
//! the proxy: the connection loop's [`connection::UiMsg`] stream and
//! muda's global menu-event queue. Quit's exchange runs on its own
//! thread with its own one-shot runtime
//! ([`shutdown::spawn_quit`]) — Quit must work whether the loop's
//! connection happens to be up, retrying, or gone.

pub mod connection;
pub mod face;
pub mod model;
pub mod shutdown;
pub mod tray;

use std::sync::Arc;
use std::sync::atomic::{AtomicBool, Ordering};
use std::sync::mpsc::{Receiver, Sender};

use winit::application::ApplicationHandler;
use winit::event::WindowEvent;
use winit::event_loop::{ActiveEventLoop, EventLoop, EventLoopProxy};

use echo_ipc::Endpoint;

use crate::lifecycle::{SpawnOutcome, spawn_if_absent};
use crate::paths::Paths;
use crate::tray_host::connection::{UiMsg, spawn_connection_loop};
use crate::tray_host::face::{management_face_url, open_in_default_browser};
use crate::tray_host::model::TrayModel;
use crate::tray_host::shutdown::{QUIT_BUDGET, QuitOutcome, spawn_quit};
use crate::tray_host::tray::TrayMenu;

/// Run the tray host until Quit: bring the daemon up if it is absent
/// (probe-then-spawn, ADR-0004), watch the channel, serve the Tray
/// Menu. `management_bind` is the configured `[management] bind`
/// (config.toml, ticket #31's key) the launcher passes along — the
/// Manage item opens the face at the address the daemon actually
/// serves. Returns when the user quits — with a clean exit code riding
/// on the caller (`Ok`), or the error that kept the host from ever
/// serving (a tray that could not be built, a spawn that could not
/// happen). The daemon's own stop is the Quit exchange's business; a
/// daemon silent past its budget never holds this host hostage.
pub fn run(
    paths: &Paths,
    plain_secrets: bool,
    management_bind: &str,
) -> Result<(), Box<dyn std::error::Error>> {
    // Spawn-if-absent (ADR-0004): probe the single-instance lock, spawn
    // a detached `bark-echo run` beside this binary when it is idle,
    // attach when held — never a second daemon attempt. The
    // probe-then-spawn gap is arbitrated by the child's own lock take:
    // a loser exits with the standard lock refusal while the winner
    // serves.
    match spawn_if_absent(paths, plain_secrets)? {
        SpawnOutcome::Spawned { mut child } => {
            eprintln!("bark-echo: daemon spawned (pid {})", child.id());
            // The detached daemon is this process's child until one of
            // the two exits: reap it when it stops, or it would sit in
            // the process table as a zombie for the host's whole
            // lifetime. If the host quits first, the daemon reparents
            // to init, which reaps — either order stays clean.
            std::thread::spawn(move || {
                let _ = child.wait();
            });
        }
        SpawnOutcome::AlreadyRunning => {
            eprintln!("bark-echo: daemon already running");
        }
    }

    let (ui_tx, ui_rx) = std::sync::mpsc::channel();
    let stop = Arc::new(AtomicBool::new(false));
    spawn_connection_loop(paths.ipc_endpoint(), ui_tx.clone(), stop.clone());

    // The loop on the main thread; macOS sets the Accessory policy
    // before it starts (the Dock icon stays hidden — a menu-bar-only
    // app; the policy only takes effect before `run_app`).
    let mut builder = EventLoop::<HostMsg>::with_user_event();
    #[cfg(target_os = "macos")]
    {
        use winit::platform::macos::{ActivationPolicy, EventLoopBuilderExtMacOS};

        builder.with_activation_policy(ActivationPolicy::Accessory);
    }
    let event_loop = builder.build()?;

    let mut host = TrayHost {
        proxy: event_loop.create_proxy(),
        model: TrayModel::connecting(),
        tray: None,
        ui_rx: Some(ui_rx),
        ui_tx,
        endpoint: paths.ipc_endpoint(),
        face_url: management_face_url(management_bind),
        stop,
        quitting: false,
        dirty: false,
        failure: None,
    };
    event_loop.run_app(&mut host)?;
    if let Some(failure) = host.failure {
        return Err(failure.into());
    }
    Ok(())
}

/// What woke the loop: a message from the channel's pumps.
#[derive(Debug)]
enum HostMsg {
    /// A [`UiMsg`] the connection loop (or the Quit exchange) sent.
    Channel(UiMsg),
    /// A click on a menu row.
    MenuClicked(muda::MenuEvent),
}

/// The host's state: the tray, the model it renders, the channel's
/// messages, and the quit flow. No windows exist (ADR-0005: the face
/// is the web page) — the `window_event` hook is the shape
/// `ApplicationHandler` requires, not a surface.
struct TrayHost {
    /// Wakes the loop when a pump finds a message.
    proxy: EventLoopProxy<HostMsg>,
    /// The daemon's echo — the truth the tray renders.
    model: TrayModel,
    /// The icon and menu. `None` until `resumed` builds it (the event
    /// loop must be running first — the platform facts above); held
    /// for the process lifetime, since dropping it removes the icon.
    tray: Option<TrayMenu>,
    /// The connection loop's (and Quit exchange's) messages. `resumed`
    /// moves the receiver into the channel pump — the loop reads
    /// everything through the proxy from then on.
    ui_rx: Option<Receiver<UiMsg>>,
    /// The ui channel's sender — Quit's exchange thread lands its
    /// outcome here.
    ui_tx: Sender<UiMsg>,
    /// The daemon endpoint, for Quit's own one-shot connection.
    endpoint: Endpoint,
    /// The management face's URL, from the `[management]` bind the
    /// launcher read out of config — where Manage opens the browser.
    face_url: String,
    /// Stops the connection loop once Quit has been clicked — the
    /// watcher's job ends with the process.
    stop: Arc<AtomicBool>,
    /// Set at the first Quit click: the exchange is spawned once, the
    /// model stops taking updates (the process is on its way out), and
    /// a second click is the same instruction, not a new one.
    quitting: bool,
    /// Whether the model changed since the menu was last rebuilt.
    dirty: bool,
    /// Why the host could not serve at all (a tray that could not be
    /// built): the loop exits and `run` reports it — the process's
    /// answer to "can the user reach me at all?".
    failure: Option<String>,
}

impl ApplicationHandler<HostMsg> for TrayHost {
    fn resumed(&mut self, event_loop: &ActiveEventLoop) {
        // The tray is created here — the event loop is running on the
        // main thread (tray-icon's macOS requirement; Windows needs the
        // loop on the creating thread) — and lives as long as the
        // process does.
        self.tray = match TrayMenu::new(&self.model) {
            Ok(tray) => Some(tray),
            Err(err) => {
                self.failure = Some(err.to_string());
                event_loop.exit();
                return;
            }
        };

        // Pump 1: the channel's messages. Blocking recv — a queued
        // message wakes at once, a new one when it lands; the proxy is
        // the only path into the loop's thread. The receiver moves into
        // the pump and never comes back: the loop reads everything
        // through the proxy from here on.
        let proxy = self.proxy.clone();
        if let Some(ui_rx) = self.ui_rx.take() {
            std::thread::spawn(move || {
                while let Ok(msg) = ui_rx.recv() {
                    if proxy.send_event(HostMsg::Channel(msg)).is_err() {
                        return;
                    }
                }
            });
        }

        // Pump 2: muda's global menu-event queue (clicks land there on
        // every platform — the ksni backend's worker thread included).
        let proxy = self.proxy.clone();
        std::thread::spawn(move || {
            let receiver = muda::MenuEvent::receiver();
            while let Ok(event) = receiver.recv() {
                if proxy.send_event(HostMsg::MenuClicked(event)).is_err() {
                    return;
                }
            }
        });
    }

    fn window_event(
        &mut self,
        _event_loop: &ActiveEventLoop,
        _window_id: winit::window::WindowId,
        _event: WindowEvent,
    ) {
        // The host runs no windows (ADR-0005): the face is the web
        // page, the tray is the glance. This hook exists because the
        // ApplicationHandler shape requires it.
    }

    fn user_event(&mut self, event_loop: &ActiveEventLoop, msg: HostMsg) {
        match msg {
            HostMsg::Channel(msg) => self.on_channel(event_loop, msg),
            HostMsg::MenuClicked(event) => self.on_menu_click(&event),
        }
        if self.dirty {
            self.dirty = false;
            self.refresh_tray();
        }
    }
}

impl TrayHost {
    /// One message from the channel. While quitting, the model's
    /// picture is frozen — the tray names the stop and nothing updates
    /// it further; only Quit's own outcome is heard.
    fn on_channel(&mut self, event_loop: &ActiveEventLoop, msg: UiMsg) {
        if self.quitting {
            if let UiMsg::ShutdownOutcome(outcome) = msg {
                self.on_quit_outcome(event_loop, outcome);
            }
            return;
        }
        match msg {
            UiMsg::Snapshot(snapshot) => {
                self.model.apply_snapshot(&snapshot);
                self.dirty = true;
            }
            UiMsg::Event(event) => {
                self.model.apply_event(&event);
                self.dirty = true;
            }
            UiMsg::LinkDown => {
                self.model.connection_lost();
                self.dirty = true;
            }
            UiMsg::ShutdownOutcome(outcome) => self.on_quit_outcome(event_loop, outcome),
        }
    }

    /// A click on a menu row. Only the two actions carry ids; the
    /// glance rows are disabled and never emit.
    fn on_menu_click(&mut self, event: &muda::MenuEvent) {
        if self.quitting {
            return;
        }
        let clicked_quit = self.tray.as_ref().is_some_and(|tray| tray.is_quit(event));
        let clicked_manage = self.tray.as_ref().is_some_and(|tray| tray.is_manage(event));
        if clicked_quit {
            self.begin_quit();
        } else if clicked_manage {
            // Best effort: a failed handoff is named, never fatal —
            // the face stays reachable, only the shortcut is missing.
            if let Err(err) = open_in_default_browser(&self.face_url) {
                eprintln!("bark-echo: cannot open the browser: {err}");
            }
        }
    }

    /// Quit clicked (ticket #12, ADR-0004): name the stop on the tray,
    /// end the watcher, and ask the daemon for a graceful shutdown on
    /// Quit's own one-shot connection. The host exits when the
    /// outcome lands — confirmed or not.
    fn begin_quit(&mut self) {
        if self.quitting {
            return;
        }
        self.quitting = true;
        self.model.quitting();
        self.dirty = true;
        self.stop.store(true, Ordering::Relaxed);
        spawn_quit(self.endpoint.clone(), self.ui_tx.clone());
    }

    /// The exchange answered — honestly, whatever it says — and the
    /// host leaves: `run_app` returns, `run` returns `Ok`, and the
    /// process ends with the clean exit code service managers read as
    /// "no resurrection" (ADR-0004).
    fn on_quit_outcome(&mut self, event_loop: &ActiveEventLoop, outcome: QuitOutcome) {
        match outcome {
            QuitOutcome::Confirmed => {}
            QuitOutcome::DaemonAbsent => {
                eprintln!("bark-echo: the daemon was already gone");
            }
            QuitOutcome::Unconfirmed => {
                eprintln!(
                    "bark-echo: the daemon did not confirm its shutdown within {}s — quitting anyway",
                    QUIT_BUDGET.as_secs()
                );
            }
        }
        event_loop.exit();
    }

    /// Rebuild the menu from the model. A refresh failure is named on
    /// stderr and the host keeps running — the last menu stands.
    fn refresh_tray(&mut self) {
        if let Some(tray) = &mut self.tray
            && let Err(err) = tray.refresh(&self.model)
        {
            eprintln!("bark-echo: {err}");
        }
    }
}
