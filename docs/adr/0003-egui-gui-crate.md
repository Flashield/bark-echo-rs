# The GUI is an egui/eframe crate (glow renderer), not Tauri or Slint

The GUI stage (Tray Menu, Management Window, Push Composer — GLOSSARY) is
one Rust crate built on egui/eframe with the glow renderer. ADR-0002's
placeholder named Tauri; the deciding constraints rank differently: the
"One binary, no installer" promise rules out GTK-based stacks outright and
makes every WebView stack carry a runtime dependency (webkit2gtk on
Linux); the MIT project wants a zero-burden license (Slint's royalty-free
license is workable but adds a permanent attribution obligation and an
app-shape restriction); AccessKit accessibility is on by default under
eframe on Windows (UIA) and macOS; glow gives the smallest binary
(~2.5 MB) with OpenGL fallback for old machines, VMs, and remote desktops;
and the tray stack this project depends on (tray-icon + muda) exercises
eframe in its own dev-deps. Slint is the named runner-up — its trigger
conditions (full-marks accessibility, a declarative + hot-reload
preference) were evaluated and not met. GPUI's standalone crate was
stalled for a year with no external stability contract. Research:
`.notes/rust-gui-frameworks-2026.md` (2026-11-14).

## Consequences

- Linux builds must explicitly enable `x11`/`wayland` features (winit
  0.30 fails otherwise), and glow is opted into over eframe's wgpu
  default (`default-features = false`).
- egui ships no CJK glyphs by default: system fonts are loaded
  explicitly, and typing Chinese into the Push Composer is a first-week
  acceptance test on all three platforms (Linux Wayland IME has a known
  regression, egui#5544 — dev boxes run X11/XWayland). Since the choice
  was made, eframe 0.37 grew on-demand system font loading per missing
  glyph (the `system_fonts` feature, `system_font_fallback: true`) —
  the explicit startup load stays the deterministic foundation (the
  first frame renders user-defined Subscription labels from a font the
  GUI chose), with the on-demand loader covering what one font file
  misses. The feature is enabled (without it the loader is a warning
  stub, and machines whose CJK font sits outside the startup
  candidates' paths — Debian's `fonts-droid-fallback`, for one — would
  show tofu). It costs no system-library dependency: its Linux font
  discovery (fontique) reaches fontconfig through `dlopen`, so a
  machine without fontconfig degrades to the startup font, never to a
  binary that does not start.
- The tray/notification stack is tray-icon + muda + notify-rust
  regardless of this choice; the Windows toast click action remains an
  AUMID + COM activation registration work item (windows-rs), not a
  library swap.
- The ADR-0002 phrase "Tauri app" refers to this crate.
