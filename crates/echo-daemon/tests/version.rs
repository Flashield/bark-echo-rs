//! CLI seam: until the daemon grows subcommands (later tickets), the
//! version flag is its entire observable surface.

use std::process::Command;

#[test]
fn version_flag_prints_the_daemon_name_and_version() {
    let output = Command::new(env!("CARGO_BIN_EXE_echo-daemon"))
        .arg("--version")
        .output()
        .expect("spawn echo-daemon");

    assert!(
        output.status.success(),
        "--version must exit 0, got {output:?}"
    );
    let stdout = String::from_utf8(output.stdout).expect("utf-8 stdout");
    assert_eq!(
        stdout.trim(),
        format!("echo-daemon {}", env!("CARGO_PKG_VERSION")),
        "--version must print name and version"
    );
}
