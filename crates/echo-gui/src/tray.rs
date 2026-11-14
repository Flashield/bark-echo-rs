//! The tray layer: the muda menu derived from the model, rebuilt when
//! the model changes, and the icon that lives for the process lifetime.
//! Rendering here is native (the OS draws the menu — CJK labels are the
//! OS's business); egui enters later with the Management Window (#13).
//!
//! Threading facts (tray-icon's own platform notes, CODING_STANDARDS:
//! name the platform reality): macOS requires the tray created on the
//! main thread while the event loop is already running — the caller
//! builds it from the GUI's first update; Windows and the Linux-KSNI
//! backend are content with the same arrangement (Windows needs its
//! event loop on the creating thread — eframe's is; KSNI runs its D-Bus
//! service on its own thread and needs no display event loop at all).

use crate::model::{MenuItem, TrayModel};

/// The tray icon and its menu. Hold for the process lifetime — dropping
/// it removes the icon (tray-icon's contract, and the tray-only shape:
/// Quit's exit removes it with the process).
pub struct TrayMenu {
    tray: tray_icon::TrayIcon,
    /// The interactive rows' ids from the current menu build — the map
    /// from a [`tray_icon::menu::MenuEvent`] back to an action.
    open_management_id: tray_icon::menu::MenuId,
    quit_id: tray_icon::menu::MenuId,
}

/// Why the tray could not be built or refreshed. The GUI keeps running
/// on a refresh failure (the last menu stands); a construction failure
/// is the process's answer to "can the user reach me at all?".
#[derive(Debug, thiserror::Error)]
pub enum TrayError {
    #[error("the tray menu failed: {0}")]
    Menu(#[from] tray_icon::menu::Error),
    #[error("the tray icon failed: {0}")]
    Icon(String),
}

impl TrayMenu {
    /// Build the tray: the icon (drawn in code — one binary, no asset
    /// files) and the menu for `model`'s current picture.
    pub fn new(model: &TrayModel) -> Result<TrayMenu, TrayError> {
        // Placeholder ids: the real ones land in the first `refresh`,
        // which runs right here.
        let mut tray_menu = TrayMenu {
            tray: tray_icon::TrayIconBuilder::new()
                .with_id("bark-echo")
                .with_icon(tray_icon())
                .with_tooltip("bark-echo")
                .build()
                .map_err(|err| TrayError::Icon(err.to_string()))?,
            open_management_id: tray_icon::menu::MenuId::new("open-management"),
            quit_id: tray_icon::menu::MenuId::new("quit"),
        };
        tray_menu.refresh(model)?;
        Ok(tray_menu)
    }

    /// Rebuild the menu from the model. The whole menu is replaced —
    /// rows are few, flips are rare, and a rebuild is the only way a
    /// native menu's labels change.
    pub fn refresh(&mut self, model: &TrayModel) -> Result<(), TrayError> {
        // muda's Menu is internally mutable — appending takes &self.
        let menu = tray_icon::menu::Menu::new();
        for item in model.menu_items() {
            match item {
                // Informational rows: not clickable, the menu shows
                // them greyed — the glance is read-only.
                MenuItem::Header(text) | MenuItem::Subscription { text, .. } => {
                    menu.append(&tray_icon::menu::MenuItem::new(text, false, None))?;
                }
                MenuItem::Separator => {
                    menu.append(&tray_icon::menu::PredefinedMenuItem::separator())?;
                }
                // The two actions. Quit exits the GUI process in this
                // ticket (the daemon shutdown is #12); the Management
                // Window row is #13's placeholder.
                MenuItem::OpenManagementWindow => {
                    let item = tray_icon::menu::MenuItem::new("Open Management Window", true, None);
                    self.open_management_id = item.id().clone();
                    menu.append(&item)?;
                }
                MenuItem::Quit => {
                    let item = tray_icon::menu::MenuItem::new("Quit", true, None);
                    self.quit_id = item.id().clone();
                    menu.append(&item)?;
                }
            }
        }
        self.tray.set_menu(Some(Box::new(menu)));
        Ok(())
    }

    /// Whether this click was on the Quit row.
    pub fn is_quit(&self, event: &tray_icon::menu::MenuEvent) -> bool {
        event.id() == &self.quit_id
    }

    /// Whether this click was on the "Open Management Window" row.
    pub fn is_open_management(&self, event: &tray_icon::menu::MenuEvent) -> bool {
        event.id() == &self.open_management_id
    }
}

/// The tray icon, drawn in code: a rounded teal tile with the white
/// notification dot — no asset files ride in the binary.
fn tray_icon() -> tray_icon::Icon {
    const SIZE: u32 = 32;
    let radius = i64::from(SIZE) / 4;
    let mut rgba = Vec::with_capacity((SIZE * SIZE * 4) as usize);
    for y in 0..i64::from(SIZE) {
        for x in 0..i64::from(SIZE) {
            // A rounded-corner mask: a pixel is inside the tile when it
            // sits in no corner's cut-out square beyond the arc.
            let corner_distance = |corner: (i64, i64)| {
                let (dx, dy) = (x - corner.0, y - corner.1);
                (dx * dx + dy * dy) <= radius * radius
            };
            let in_corner_cut = |corner: (i64, i64)| {
                let (cx, cy) = corner;
                let (near_x, near_y) = ((x - cx).abs() <= radius, (y - cy).abs() <= radius);
                near_x && near_y && !corner_distance(corner)
            };
            let corners = [
                (radius, radius),
                (i64::from(SIZE) - 1 - radius, radius),
                (radius, i64::from(SIZE) - 1 - radius),
                (i64::from(SIZE) - 1 - radius, i64::from(SIZE) - 1 - radius),
            ];
            let inside = !corners.iter().any(|&corner| in_corner_cut(corner));
            if inside {
                // The notification dot: a white disc at the tile's heart.
                let center = i64::from(SIZE) / 2;
                let dx = x - center;
                let dy = y - center;
                if dx * dx + dy * dy <= 6 * 6 {
                    rgba.extend_from_slice(&[255, 255, 255, 255]);
                } else {
                    rgba.extend_from_slice(&[31, 111, 105, 255]);
                }
            } else {
                rgba.extend_from_slice(&[0, 0, 0, 0]);
            }
        }
    }
    tray_icon::Icon::from_rgba(rgba, SIZE, SIZE).expect("the hand-built 32x32 RGBA icon parses")
}
