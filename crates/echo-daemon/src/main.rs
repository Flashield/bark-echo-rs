//! echo-daemon: the desktop Realtime Client binary. Its first functional
//! ticket is the `push` subcommand — the CLI face of echo-core's Push
//! send module (issue #6); receiving subcommands arrive with later
//! tickets.

use echo_core::push::{Push, PushClient};

fn main() {
    tokio::runtime::Builder::new_multi_thread()
        .enable_all()
        .build()
        .expect("tokio runtime")
        .block_on(run());
}

async fn run() {
    let args: Vec<String> = std::env::args().skip(1).collect();
    match args.first().map(String::as_str) {
        Some("--version") | Some("-V") => {
            println!("echo-daemon {}", env!("CARGO_PKG_VERSION"));
        }
        Some("push") => run_push(&args[1..]).await,
        _ => {
            eprintln!("nothing to do yet; try --version");
            std::process::exit(1);
        }
    }
}

/// The `push` subcommand: `push --server URL --device-key KEY --title …`
/// targets any Device, mapping its flags onto the send module's Push
/// (protocol §2.2). Failures exit non-zero.
async fn run_push(args: &[String]) {
    let mut server = None;
    let mut device_key = None;
    let mut title = None;
    let mut subtitle = None;
    let mut body = None;
    let mut user = None;
    let mut password = None;
    let mut ext_params: Vec<(String, String)> = Vec::new();

    let mut fields = args.iter();
    while let Some(flag) = fields.next() {
        let value = fields.next().unwrap_or_else(|| {
            die(&format!("push: flag {flag} needs a value"));
        });
        match flag.as_str() {
            "--server" => server = Some(value.clone()),
            "--device-key" => device_key = Some(value.clone()),
            "--title" => title = Some(value.clone()),
            "--subtitle" => subtitle = Some(value.clone()),
            "--body" => body = Some(value.clone()),
            // Per-server Basic Auth credentials (protocol §1.4) — either
            // field alone is enough; servers enable the check on each
            // independently.
            "--user" => user = Some(value.clone()),
            "--password" => password = Some(value.clone()),
            // Anything else is an Ext Param (url, group, sound, …): passed
            // through exactly like the server's V2 JSON route (§2.2).
            _ => ext_params.push((flag.trim_start_matches('-').to_owned(), value.clone())),
        }
    }

    let Some(server) = server else {
        die("push: --server URL is required");
    };
    let Some(device_key) = device_key else {
        die("push: --device-key KEY is required");
    };

    let mut push = Push::to(device_key);
    if let Some(title) = title {
        push = push.title(title);
    }
    if let Some(subtitle) = subtitle {
        push = push.subtitle(subtitle);
    }
    if let Some(body) = body {
        push = push.body(body);
    }
    for (key, value) in &ext_params {
        push = push.ext_param(key, value);
    }

    // Failures surface the server's message and code (issue #6): the
    // send module's error display is verbatim, the exit is non-zero.
    let client = PushClient::new(&server);
    let client = if user.is_some() || password.is_some() {
        client.with_basic_auth(user.unwrap_or_default(), password.unwrap_or_default())
    } else {
        client
    };
    if let Err(err) = client.send(&push).await {
        die(&err.to_string());
    }
}

/// Print a usage failure and exit non-zero.
fn die(message: &str) -> ! {
    eprintln!("{message}");
    std::process::exit(1);
}
