//! One atomic file write for every persisted file — the Subscription
//! config, the plain-file secrets, the daemon state: a sibling temp file
//! first, then a rename into place, so a crash never leaves a
//! half-written file for a reader to trip on.

use std::path::Path;

/// Atomically replace `path` with `contents`: the payload is written to a
/// sibling temp file (the path's own extension plus `.tmp`), optionally
/// restricted to the owner (0600 on unix — the secrets file's rule), and
/// renamed over `path`. Parent directories are created as needed.
pub(crate) fn write(path: &Path, contents: &str, owner_only: bool) -> std::io::Result<()> {
    if let Some(parent) = path.parent() {
        std::fs::create_dir_all(parent)?;
    }
    let tmp = match path.extension() {
        Some(ext) => path.with_extension(format!("{}.tmp", ext.to_string_lossy())),
        None => path.with_extension("tmp"),
    };
    std::fs::write(&tmp, contents)?;
    #[cfg(unix)]
    if owner_only {
        use std::os::unix::fs::PermissionsExt;
        std::fs::set_permissions(&tmp, std::fs::Permissions::from_mode(0o600))?;
    }
    #[cfg(not(unix))]
    let _ = owner_only;
    std::fs::rename(&tmp, path)
}
