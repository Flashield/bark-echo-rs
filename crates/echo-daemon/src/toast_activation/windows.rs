//! The Windows half of toast activation (ticket #17), compiled only for
//! Windows targets — a Linux runner cannot compile (let alone host) a
//! toast, so Windows CI's build + clippy first-party step is this
//! module's gate.
//!
//! Three moving parts, following Microsoft's
//! ToastNotificationManagerCompat path for unpackaged Win32 apps
//! (Windows 10 build 19041+):
//!
//! 1. Registration ([`register`], once per daemon startup, runtime
//!    only): the AppUserModelID is anchored by a Start Menu shortcut
//!    carrying the `System.AppUserModel.ID` and
//!    `System.AppUserModel.ToastActivatorCLSID` properties, plus a
//!    per-user registry entry naming this executable as the activator's
//!    COM server. No installer, no elevation.
//! 2. The COM server ([`ToastActivator`]): the
//!    `INotificationActivationCallback` that fires when the user clicks
//!    a toast. While the daemon runs, an in-process class factory
//!    ([`FactoryThread`]) serves activations directly; when the daemon
//!    is gone, the shell launches the registered LocalServer32 command —
//!    this same executable with [`ACTIVATION_ARG`]
//!    ([`serve_activation_process`]) — which serves one activation and
//!    exits.
//! 3. The toast itself ([`show_click_toast`]): click-action toasts
//!    render through WinRT under the AUMID, because notify-rust's
//!    Windows backend cannot express the launch argument the callback
//!    decodes.
//!
//! Every step degrades: a registration failure means toasts fire
//! exactly as before this ticket (notify-rust) with an actionable log —
//! never a lost notification.

use std::path::{Path, PathBuf};
use std::sync::mpsc;

use echo_core::intent::NotificationIntent;
use windows::core::{
    BOOL, Error, GUID, HSTRING, IUnknown, Interface, PCWSTR, PWSTR, Ref, implement,
};

use super::{ACTIVATOR_CLSID, AUMID, DISPLAY_NAME};
use windows::Data::Xml::Dom::XmlDocument;
use windows::UI::Notifications::{ToastNotification, ToastNotificationManager};
use windows::Win32::Foundation::{
    CLASS_E_NOAGGREGATION, E_POINTER, ERROR_SUCCESS, HANDLE, LPARAM, RPC_E_CHANGED_MODE, S_FALSE,
    WAIT_OBJECT_0, WPARAM,
};
use windows::Win32::Storage::EnhancedStorage::{
    PKEY_AppUserModel_ID, PKEY_AppUserModel_ToastActivatorCLSID,
};
use windows::Win32::System::Com::StructuredStorage::{
    PROPVARIANT, PROPVARIANT_0, PROPVARIANT_0_0, PROPVARIANT_0_0_0,
};
use windows::Win32::System::Com::{
    CLSCTX_INPROC_SERVER, CLSCTX_LOCAL_SERVER, COINIT_APARTMENTTHREADED, CoCreateInstance,
    CoInitializeEx, CoRegisterClassObject, CoRevokeClassObject, CoTaskMemAlloc, CoTaskMemFree,
    CoUninitialize, IClassFactory, IClassFactory_Impl, IPersistFile, REGCLS_MULTIPLEUSE,
    REGCLS_SINGLEUSE,
};
use windows::Win32::System::Registry::{
    HKEY, HKEY_CURRENT_USER, KEY_WRITE, REG_CREATE_KEY_DISPOSITION, REG_OPTION_NON_VOLATILE,
    REG_SZ, RegCloseKey, RegCreateKeyExW, RegSetValueExW,
};
use windows::Win32::System::Threading::{CreateEventW, GetCurrentThreadId, SetEvent};
use windows::Win32::System::Variant::{VT_CLSID, VT_LPWSTR};
use windows::Win32::UI::Notifications::{
    INotificationActivationCallback, INotificationActivationCallback_Impl,
    NOTIFICATION_USER_INPUT_DATA,
};
use windows::Win32::UI::Shell::PropertiesSystem::IPropertyStore;
use windows::Win32::UI::Shell::{
    FOLDERID_Programs, IShellLinkW, KNOWN_FOLDER_FLAG, SHGetKnownFolderPath, ShellLink,
};
use windows::Win32::UI::WindowsAndMessaging::{
    DispatchMessageW, GetMessageW, MSG, MWMO_INPUTAVAILABLE, MsgWaitForMultipleObjectsEx,
    PM_NOREMOVE, PM_REMOVE, PeekMessageW, PostThreadMessageW, QS_ALLINPUT, WM_QUIT,
};

/// The command-line flag the LocalServer32 registration carries. COM
/// appends its own `-Embedding` when the shell launches the server.
const ACTIVATION_ARG: &str = "--toast-activation";

/// How long the one-shot activation server waits for the shell to
/// connect before giving up: the shell connects within moments of
/// launching us, so a longer wait only delays a dead process.
const ACTIVATION_TIMEOUT_MS: u32 = 5_000;

/// The Start Menu shortcut's file name beside the Programs folder.
const SHORTCUT_FILE_NAME: &str = "bark-echo.lnk";

/// What registration produced for the renderer.
#[derive(Debug, Default)]
pub(crate) enum Activation {
    /// The AUMID and the activator are registered: click-action toasts
    /// render through [`show_click_toast`] and clicks come back through
    /// the COM callback.
    Ready(Ready),
    /// Registration failed — already logged, actionable. Toasts render
    /// through notify-rust exactly as they did before ticket #17.
    #[default]
    Degraded,
}

/// A ready activation: keeps the daemon's factory thread alive (its
/// `Drop` unwinds the thread when the daemon exits).
#[derive(Debug)]
pub(crate) struct Ready {
    _factory: Option<FactoryThread>,
}

/// Attempt the runtime registration (AUMID + COM activator + factory).
/// Never fails outward: a failure is logged actionably and answered
/// with [`Activation::Degraded`] — toasts must never depend on
/// registration succeeding (ticket #17).
pub(crate) fn register() -> Activation {
    match register_inner() {
        Ok(factory) => Activation::Ready(Ready {
            _factory: Some(factory),
        }),
        Err(err) => {
            eprintln!(
                "toast activation: registration failed — toasts will show, but clicks do nothing until this is fixed: {err}"
            );
            Activation::Degraded
        }
    }
}

fn register_inner() -> Result<FactoryThread, String> {
    let exe = std::env::current_exe().map_err(|err| format!("locate the executable: {err}"))?;
    write_registry(&exe)?;
    write_start_menu_shortcut(&exe)?;
    // The factory is best-effort: if it cannot run, activations still
    // work through the registered LocalServer32 — a fresh
    // `--toast-activation` process serving one click.
    FactoryThread::start()
}

/// Show one click-action toast under the AUMID (the Windows renderer's
/// Ready path). The visual uses the same summary/body split as the
/// notify-rust path; the launch argument carries the click action to
/// the callback. The `Ready` handle is the precondition: only a
/// registered activation may show here.
pub(crate) fn show_click_toast(_ready: &Ready, intent: &NotificationIntent) -> Result<(), String> {
    // WinRT factory calls need an initialized apartment on the calling
    // thread — the renderer runs on tokio workers, which own none.
    let balance = initialize_apartment()?;
    let result = show_initialized(intent);
    if balance {
        unsafe { CoUninitialize() };
    }
    result
}

fn show_initialized(intent: &NotificationIntent) -> Result<(), String> {
    // The WinRT surface here is all safe functions; the only unsafe
    // concern (apartment initialization) is handled by the caller.
    let document =
        XmlDocument::new().map_err(|err| format!("create the toast XML document: {err}"))?;
    document
        .LoadXml(&HSTRING::from(super::toast_xml(intent)))
        .map_err(|err| format!("load the toast XML: {err}"))?;
    let toast = ToastNotification::CreateToastNotification(&document)
        .map_err(|err| format!("create the toast: {err}"))?;
    let notifier = ToastNotificationManager::CreateToastNotifierWithId(&HSTRING::from(AUMID))
        .map_err(|err| format!("open the toast notifier for the AUMID: {err}"))?;
    notifier
        .Show(&toast)
        .map_err(|err| format!("show the toast: {err}"))
}

/// Is this process the toast-activation server the shell launched to
/// serve one click? Checked by `main` before the CLI parses its
/// arguments (which the extra flag would fail).
pub fn is_activation_process() -> bool {
    std::env::args().any(|argument| argument == ACTIVATION_ARG)
}

/// The one-shot COM local server: what the shell runs
/// (`<exe> --toast-activation`) when a click lands while no daemon
/// holds the class object. Registers the class object for a single
/// use, waits bounded for the shell's `CreateInstance` and the
/// `Activate` call, then returns the process exit code.
pub fn serve_activation_process() -> i32 {
    let balance = match initialize_apartment() {
        Ok(balance) => balance,
        Err(err) => {
            eprintln!("toast activation server: {err}");
            return 1;
        }
    };
    // Manual-reset event the callback sets when the click is served.
    let served = unsafe { CreateEventW(None, true, false, PCWSTR::null()) };
    let served = match served {
        Ok(event) => event,
        Err(err) => {
            eprintln!("toast activation server: create the completion event: {err}");
            if balance {
                unsafe { CoUninitialize() };
            }
            return 1;
        }
    };
    let factory: IClassFactory = ActivatorFactory {
        completion: Some(served),
    }
    .into();
    let registered = unsafe {
        CoRegisterClassObject(
            &activator_clsid(),
            &factory,
            CLSCTX_LOCAL_SERVER,
            REGCLS_SINGLEUSE,
        )
    };
    match registered {
        Ok(cookie) => {
            wait_for_activation(served);
            // REGCLS_SINGLEUSE retires the class object after the first
            // connection; revoking here is the belt to that suspenders
            // (a stale cookie failing to revoke is fine to ignore).
            let _ = unsafe { CoRevokeClassObject(cookie) };
        }
        Err(err) => {
            eprintln!("toast activation server: register the class object: {err}");
        }
    }
    if balance {
        unsafe { CoUninitialize() };
    }
    0
}

/// Wait for the activation to be served (the callback sets the event),
/// pumping messages while we wait — the shell's `CreateInstance` can
/// only be delivered to our STA while it pumps. Bounded: a shell that
/// never connects cannot wedge the process.
fn wait_for_activation(event: HANDLE) {
    loop {
        let wait = unsafe {
            MsgWaitForMultipleObjectsEx(
                Some(&[event]),
                ACTIVATION_TIMEOUT_MS,
                QS_ALLINPUT,
                MWMO_INPUTAVAILABLE,
            )
        };
        if wait == WAIT_OBJECT_0 {
            // The callback ran; the click is performed.
            return;
        }
        if wait.0 == WAIT_OBJECT_0.0 + 1 {
            // Input to dispatch — keep the STA serviceable and wait on.
            let mut message = MSG::default();
            while unsafe { PeekMessageW(&mut message, None, 0, 0, PM_REMOVE) }.as_bool() {
                unsafe { DispatchMessageW(&message) };
            }
            continue;
        }
        // Timeout or a wait failure: give up. A click arriving later
        // finds no server — toasts are transient, and the daemon (which
        // keeps a live factory) is the normal case.
        return;
    }
}

/// Initialize COM on the calling thread; answers whether the matching
/// `CoUninitialize` is owed. The apartment facts: S_FALSE means the
/// thread already runs this apartment kind and still needs the
/// balancing call; RPC_E_CHANGED_MODE means it already runs the other
/// kind, which WinRT and class-object registration accept as-is.
fn initialize_apartment() -> Result<bool, String> {
    let initialized = unsafe { CoInitializeEx(None, COINIT_APARTMENTTHREADED) };
    if initialized.is_ok() || initialized == S_FALSE {
        Ok(true)
    } else if initialized == RPC_E_CHANGED_MODE {
        Ok(false)
    } else {
        Err(format!("initialize COM: {initialized}"))
    }
}

/// The registry side of registration: the activator's class id resolves
/// to this executable (the LocalServer32 command the shell runs when no
/// live daemon holds the class object), and the AUMID gets its display
/// name. Per-user (HKCU) — no elevation, no installer.
fn write_registry(exe: &Path) -> Result<(), String> {
    let server = format!("\"{}\" {ACTIVATION_ARG}", exe.display());
    set_registry_string(
        &format!(r"Software\Classes\CLSID\{ACTIVATOR_CLSID}\LocalServer32"),
        "",
        &server,
    )?;
    set_registry_string(
        &format!(r"Software\Classes\AppUserModelId\{AUMID}"),
        "DisplayName",
        DISPLAY_NAME,
    )
}

/// Write one REG_SZ value under HKCU, creating the key when missing.
fn set_registry_string(subkey: &str, value: &str, data: &str) -> Result<(), String> {
    unsafe {
        let subkey_w = wide(subkey);
        let value_w = wide(value);
        let data_w = wide(data);
        let mut key = HKEY::default();
        let mut disposition = REG_CREATE_KEY_DISPOSITION::default();
        let status = RegCreateKeyExW(
            HKEY_CURRENT_USER,
            PCWSTR::from_raw(subkey_w.as_ptr()),
            None,
            PCWSTR::null(),
            REG_OPTION_NON_VOLATILE,
            KEY_WRITE,
            None,
            &mut key,
            Some(&mut disposition),
        );
        if status != ERROR_SUCCESS {
            return Err(format!("open HKCU\\{subkey}: {}", status.0));
        }
        // A wide string in the registry is its UTF-16 bytes, NUL
        // included (`wide` already added it).
        let data_bytes = std::slice::from_raw_parts(
            data_w.as_ptr().cast::<u8>(),
            data_w.len() * size_of::<u16>(),
        );
        let status = RegSetValueExW(
            key,
            PCWSTR::from_raw(value_w.as_ptr()),
            Some(0),
            REG_SZ,
            Some(data_bytes),
        );
        let _ = RegCloseKey(key);
        if status != ERROR_SUCCESS {
            return Err(format!("write HKCU\\{subkey}\\{value}: {}", status.0));
        }
    }
    Ok(())
}

/// The Start Menu shortcut side of registration (the
/// ToastNotificationManagerCompat anchor): a `.lnk` to this executable
/// carrying the AUMID as `System.AppUserModel.ID` and the activator's
/// class id as `System.AppUserModel.ToastActivatorCLSID` — the pair the
/// shell resolves a toast's identity through. Rewritten every startup,
/// so a moved or updated executable self-heals.
fn write_start_menu_shortcut(exe: &Path) -> Result<(), String> {
    unsafe {
        let programs = known_folder(&FOLDERID_Programs)?;
        let link_path = programs.join(SHORTCUT_FILE_NAME);

        let link: IShellLinkW =
            CoCreateInstance(&ShellLink, None::<&IUnknown>, CLSCTX_INPROC_SERVER)
                .map_err(|err| format!("create the shortcut COM object: {err}"))?;
        let exe_w = wide(&exe.display().to_string());
        link.SetPath(PCWSTR::from_raw(exe_w.as_ptr()))
            .map_err(|err| format!("set the shortcut target: {err}"))?;
        link.SetDescription(&HSTRING::from(DISPLAY_NAME))
            .map_err(|err| format!("set the shortcut description: {err}"))?;

        let store: IPropertyStore = link
            .cast()
            .map_err(|err| format!("open the shortcut properties: {err}"))?;
        store
            .SetValue(&PKEY_AppUserModel_ID, &propvariant_string(AUMID)?)
            .map_err(|err| format!("set the AppUserModelID property: {err}"))?;
        store
            .SetValue(
                &PKEY_AppUserModel_ToastActivatorCLSID,
                &propvariant_clsid()?,
            )
            .map_err(|err| format!("set the toast activator property: {err}"))?;
        store
            .Commit()
            .map_err(|err| format!("commit the shortcut properties: {err}"))?;

        let file: IPersistFile = link
            .cast()
            .map_err(|err| format!("open the shortcut file writer: {err}"))?;
        let link_path_w = wide(&link_path.display().to_string());
        file.Save(PCWSTR::from_raw(link_path_w.as_ptr()), true)
            .map_err(|err| format!("write {}: {err}", link_path.display()))
    }
}

/// One known folder's path, freed with `CoTaskMemFree` per the API's
/// contract.
fn known_folder(folder: &GUID) -> Result<PathBuf, String> {
    unsafe {
        let path = SHGetKnownFolderPath(folder, KNOWN_FOLDER_FLAG(0), None).map_err(|err| {
            format!("locate the Start Menu Programs folder ({FOLDERID_Programs:?}): {err}")
        })?;
        let text = path
            .to_string()
            .map_err(|err| format!("the Start Menu Programs folder is not valid Unicode: {err}"));
        CoTaskMemFree(Some(path.as_ptr().cast()));
        text.map(PathBuf::from)
    }
}

/// A null-terminated UTF-16 buffer for a Windows string parameter.
fn wide(text: &str) -> Vec<u16> {
    text.encode_utf16().chain(std::iter::once(0)).collect()
}

/// A VT_LPWSTR variant owning its buffer the way the property system
/// expects: CoTaskMemAlloc'd, so the `PropVariantClear` that runs when
/// it drops can free it.
fn propvariant_string(value: &str) -> Result<PROPVARIANT, String> {
    let buffer = wide(value);
    let raw = alloc_task_memory(buffer.len() * size_of::<u16>())? as *mut u16;
    unsafe { std::ptr::copy_nonoverlapping(buffer.as_ptr(), raw, buffer.len()) };
    Ok(PROPVARIANT {
        Anonymous: PROPVARIANT_0 {
            Anonymous: std::mem::ManuallyDrop::new(PROPVARIANT_0_0 {
                vt: VT_LPWSTR,
                wReserved1: 0,
                wReserved2: 0,
                wReserved3: 0,
                Anonymous: PROPVARIANT_0_0_0 {
                    pwszVal: PWSTR(raw),
                },
            }),
        },
    })
}

/// A VT_CLSID variant holding a CoTaskMemAlloc'd copy of the activator's
/// class id (freed by `PropVariantClear` when it drops).
fn propvariant_clsid() -> Result<PROPVARIANT, String> {
    let raw = alloc_task_memory(size_of::<GUID>())? as *mut GUID;
    unsafe { std::ptr::write(raw, activator_clsid()) };
    Ok(PROPVARIANT {
        Anonymous: PROPVARIANT_0 {
            Anonymous: std::mem::ManuallyDrop::new(PROPVARIANT_0_0 {
                vt: VT_CLSID,
                wReserved1: 0,
                wReserved2: 0,
                wReserved3: 0,
                Anonymous: PROPVARIANT_0_0_0 { puuid: raw },
            }),
        },
    })
}

fn alloc_task_memory(bytes: usize) -> Result<*mut std::ffi::c_void, String> {
    let raw = unsafe { CoTaskMemAlloc(bytes) };
    if raw.is_null() {
        return Err("out of memory".to_owned());
    }
    Ok(raw)
}

/// The activator's class id as a GUID — [`ACTIVATOR_CLSID`] without its
/// braces.
fn activator_clsid() -> GUID {
    GUID::try_from(ACTIVATOR_CLSID.trim_matches(|c| c == '{' || c == '}'))
        .expect("ACTIVATOR_CLSID is a braced GUID literal")
}

/// The COM object the shell instantiates for a toast click. Holds no
/// per-toast state — the launch argument carries the whole message, so
/// any process serving the callback acts identically.
#[implement(INotificationActivationCallback)]
struct ToastActivator {
    /// Set when the activation has been served — wakes the one-shot
    /// server ([`serve_activation_process`]); `None` in the daemon's
    /// long-lived factory.
    completion: Option<HANDLE>,
}

impl INotificationActivationCallback_Impl for ToastActivator_Impl {
    fn Activate(
        &self,
        _appusermodelid: &PCWSTR,
        invokedargs: &PCWSTR,
        _data: *const NOTIFICATION_USER_INPUT_DATA,
        _count: u32,
    ) -> windows::core::Result<()> {
        // The click can arrive minutes after the toast was shown, from
        // this daemon or from the one-shot server — the launch argument
        // is the whole message; nothing else is consulted.
        let argument = if invokedargs.is_null() {
            None
        } else {
            // The shell owns the string for the duration of the call.
            Some(unsafe { invokedargs.to_string() }.unwrap_or_default())
        };
        match argument.as_deref().and_then(super::decode_launch_argument) {
            Some(action) => crate::notify::perform_click(&action),
            None => eprintln!(
                "toast click: no usable launch argument ({argument:?}) — taking no action"
            ),
        }
        if let Some(event) = self.completion {
            // Tell the one-shot server this activation is served.
            let _ = unsafe { SetEvent(event) };
        }
        Ok(())
    }
}

/// The COM class factory for [`ToastActivator`] — what
/// `CoRegisterClassObject` hands out.
#[implement(IClassFactory)]
struct ActivatorFactory {
    /// Handed to every fresh activator: the one-shot server's completion
    /// event, `None` in the daemon's factory.
    completion: Option<HANDLE>,
}

impl IClassFactory_Impl for ActivatorFactory_Impl {
    fn CreateInstance(
        &self,
        punkouter: Ref<IUnknown>,
        riid: *const GUID,
        ppvobject: *mut *mut std::ffi::c_void,
    ) -> windows::core::Result<()> {
        if !punkouter.is_null() {
            // This object does not aggregate (and the shell never asks
            // it to).
            return Err(Error::from_hresult(CLASS_E_NOAGGREGATION));
        }
        if ppvobject.is_null() {
            return Err(Error::from_hresult(E_POINTER));
        }
        unsafe { *ppvobject = std::ptr::null_mut() };
        let activator: INotificationActivationCallback = ToastActivator {
            completion: self.completion,
        }
        .into();
        // Hand the caller whichever interface it asked for (the query
        // rides IUnknown's vtable slot — every interface starts there).
        unsafe {
            (activator.assume_vtable::<IUnknown>().QueryInterface)(
                activator.as_raw(),
                riid,
                ppvobject,
            )
            .ok()
        }
    }

    fn LockServer(&self, _flock: BOOL) -> windows::core::Result<()> {
        // Nothing to lock: the factory holds no shared state.
        Ok(())
    }
}

/// The daemon's live class object: while it runs, the shell hands a
/// toast click straight to this process (REGCLS_MULTIPLEUSE) — no
/// process spawn per click. Dropping it posts WM_QUIT so the thread
/// unwinds (revoke + `CoUninitialize`) before the daemon exits.
#[derive(Debug)]
struct FactoryThread {
    thread_id: u32,
    join: Option<std::thread::JoinHandle<()>>,
}

impl FactoryThread {
    /// Spawn the factory thread and wait until its class object is
    /// registered (or the attempt failed). No completion event here:
    /// the daemon serves clicks indefinitely.
    fn start() -> Result<FactoryThread, String> {
        let (ready, registered) = mpsc::channel::<Result<u32, String>>();
        let join = std::thread::Builder::new()
            .name("toast-activation".to_owned())
            .spawn(move || factory_thread_body(ready))
            .map_err(|err| format!("spawn the toast-activation thread: {err}"))?;
        match registered.recv() {
            Ok(Ok(thread_id)) => Ok(FactoryThread {
                thread_id,
                join: Some(join),
            }),
            Ok(Err(err)) => {
                let _ = join.join();
                Err(err)
            }
            Err(_) => {
                let _ = join.join();
                Err("the toast-activation thread died before registering".to_owned())
            }
        }
    }
}

impl Drop for FactoryThread {
    fn drop(&mut self) {
        // WM_QUIT ends the pump; the thread then revokes the class
        // object and balances CoUninitialize before exiting.
        unsafe {
            let _ = PostThreadMessageW(self.thread_id, WM_QUIT, WPARAM(0), LPARAM(0));
        }
        if let Some(join) = self.join.take() {
            let _ = join.join();
        }
    }
}

/// The factory thread: an STA with a message pump — that is what lets
/// the shell marshal a click into this process. The message queue is
/// created before readiness is reported, so a WM_QUIT posted right
/// after can never fall on the floor (PostThreadMessageW's documented
/// trap: messages to a thread without a queue are lost).
fn factory_thread_body(ready: mpsc::Sender<Result<u32, String>>) {
    let thread_id = unsafe { GetCurrentThreadId() };
    let initialized = unsafe { CoInitializeEx(None, COINIT_APARTMENTTHREADED) };
    // Platform facts (see `initialize_apartment`): S_FALSE — already an
    // STA here, balance it anyway; RPC_E_CHANGED_MODE — already an MTA,
    // which class-object registration accepts (the message pump is what
    // matters, not the apartment kind).
    let balance = initialized.is_ok() || initialized == S_FALSE;
    if initialized.is_err() && initialized != RPC_E_CHANGED_MODE {
        let _ = ready.send(Err(format!(
            "initialize COM on the toast-activation thread: {initialized}"
        )));
        return;
    }

    // Create the message queue now (PeekMessage with PM_NOREMOVE is the
    // documented way to force it); the peek itself is deliberately
    // result-free.
    let mut message = MSG::default();
    let _ = unsafe { PeekMessageW(&mut message, None, 0, 0, PM_NOREMOVE) };

    let factory: IClassFactory = ActivatorFactory { completion: None }.into();
    match unsafe {
        CoRegisterClassObject(
            &activator_clsid(),
            &factory,
            CLSCTX_LOCAL_SERVER,
            REGCLS_MULTIPLEUSE,
        )
    } {
        Ok(cookie) => {
            let _ = ready.send(Ok(thread_id));
            pump_until_quit();
            // The daemon is exiting; release the class object.
            let _ = unsafe { CoRevokeClassObject(cookie) };
        }
        Err(err) => {
            let _ = ready.send(Err(format!(
                "register the toast activator class object: {err}"
            )));
        }
    }
    if balance {
        unsafe { CoUninitialize() };
    }
}

/// The STA's message pump: dispatch everything until WM_QUIT.
fn pump_until_quit() {
    let mut message = MSG::default();
    loop {
        // GetMessageW answers 0 for WM_QUIT and -1 for an error; both
        // end the pump.
        let received = unsafe { GetMessageW(&mut message, None, 0, 0) };
        if received.0 <= 0 {
            break;
        }
        unsafe { DispatchMessageW(&message) };
    }
}
