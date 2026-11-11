//! echo-daemon: the desktop Realtime Client binary. An empty shell until
//! its first functional ticket; the version flag is the whole CLI surface.

fn main() {
    let mut args = std::env::args().skip(1);
    match args.next().as_deref() {
        Some("--version") | Some("-V") => {
            println!("echo-daemon {}", env!("CARGO_PKG_VERSION"));
        }
        _ => {
            eprintln!("nothing to do yet; try --version");
            std::process::exit(1);
        }
    }
}
