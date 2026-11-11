//! CLI seam: the version flag pins the product identity — the binary is
//! named after the repository (bark-echo), whatever the crate is called.

use std::process::Command;

#[test]
fn version_flag_prints_the_binary_name_and_version() {
    let output = Command::new(env!("CARGO_BIN_EXE_bark-echo"))
        .arg("--version")
        .output()
        .expect("spawn bark-echo");

    assert!(
        output.status.success(),
        "--version must exit 0, got {output:?}"
    );
    let stdout = String::from_utf8(output.stdout).expect("utf-8 stdout");
    assert_eq!(
        stdout.trim(),
        format!("bark-echo {}", env!("CARGO_PKG_VERSION")),
        "--version must print the product binary name and version"
    );
}
