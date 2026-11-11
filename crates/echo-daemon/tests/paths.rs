//! Config-location seam: the Subscription list lives in the standard
//! per-OS config dir (ADR-0001), resolved from the environment so a temp
//! `HOME` hermetically relocates every CLI test (spec Testing Decisions).

use std::path::{Path, PathBuf};

use echo_daemon::paths::{PathsError, config_dir_from};

fn home() -> PathBuf {
    PathBuf::from("/home/ada")
}

#[cfg(unix)]
#[test]
fn the_config_dir_is_the_standard_per_os_dir_plus_the_app_name() {
    let dir = config_dir_from(Some(&home()), None).expect("home present");

    assert_eq!(
        dir,
        std::path::Path::new("/home/ada")
            .join(default_suffix())
            .join("echo-daemon"),
        "standard config dir + the app's own directory"
    );
}

/// The per-OS standard location under the user's home — the platform's
/// own convention, so the test stays honest about what "standard" means.
#[cfg(all(unix, not(target_os = "macos")))]
fn default_suffix() -> PathBuf {
    PathBuf::from(".config")
}

#[cfg(target_os = "macos")]
fn default_suffix() -> PathBuf {
    PathBuf::from("Library/Application Support")
}

#[cfg(unix)]
#[test]
fn xdg_config_home_overrides_the_home_default() {
    let xdg = Path::new("/hermetic/config");

    let dir = config_dir_from(Some(&home()), Some(xdg)).expect("home present");

    assert_eq!(dir, xdg.join("echo-daemon"), "$XDG_CONFIG_HOME wins");
}

#[cfg(unix)]
#[test]
fn xdg_config_home_stands_alone_without_a_home() {
    let xdg = Path::new("/hermetic/config");

    let dir = config_dir_from(None, Some(xdg)).expect("xdg present");

    assert_eq!(dir, xdg.join("echo-daemon"), "$XDG_CONFIG_HOME suffices");
}

#[cfg(windows)]
#[test]
fn the_config_dir_is_roaming_appdata_plus_the_app_name() {
    let appdata = Path::new(r"C:\Users\ada\AppData\Roaming");

    let dir = config_dir_from(Some(appdata)).expect("appdata present");

    assert_eq!(dir, appdata.join("echo-daemon"));
}

#[cfg(unix)]
#[test]
fn without_home_or_xdg_the_error_names_what_to_set() {
    let err = config_dir_from(None, None).expect_err("nothing to resolve from");

    let message = err.to_string();
    assert!(
        matches!(err, PathsError::NoConfigRoot),
        "wrong error: {err}"
    );
    assert!(
        message.contains("HOME") && message.contains("XDG_CONFIG_HOME"),
        "error must name the missing variables, got: {message}"
    );
}

#[cfg(windows)]
#[test]
fn without_appdata_the_error_names_what_to_set() {
    let err = config_dir_from(None).expect_err("nothing to resolve from");

    assert!(
        matches!(err, PathsError::NoConfigRoot),
        "wrong error: {err}"
    );
    assert!(
        err.to_string().contains("APPDATA"),
        "error must name the missing variable, got: {err}"
    );
}

#[cfg(unix)]
#[test]
fn config_and_secrets_files_live_in_the_app_dir() {
    use echo_daemon::paths::Paths;

    let paths = Paths::from_dir(config_dir_from(Some(&home()), None).expect("home present"));

    assert_eq!(
        paths.config_file(),
        config_dir_from(Some(&home()), None)
            .expect("home present")
            .join("config.toml"),
        "the Subscription list is a TOML file in the app dir"
    );
    assert_eq!(
        paths.secrets_file(),
        config_dir_from(Some(&home()), None)
            .expect("home present")
            .join("secrets.toml"),
        "the plain fallback keeps secrets beside the config"
    );
}
