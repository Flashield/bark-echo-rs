//! echo-daemon: the desktop Realtime Client binary. This shell holds the
//! CLI surface — ticket #3's `subscribe` subcommands over the config, the
//! secret store, and the server's Device/Token plane; ticket #6's `push`
//! subcommand over echo-core's send module; the daemon runtime (streams,
//! notifications) arrives with later tickets (ADR-0002).

use clap::{Args, Parser, Subcommand};
use echo_core::pull::PullConfig;
use echo_core::push::{Push, PushClient};
use echo_core::server::ServerClient;

use echo_daemon::config::Config;
use echo_daemon::history;
use echo_daemon::paths::Paths;
use echo_daemon::secrets::{KeyringStore, PlainFileStore, SecretStore};
use echo_daemon::subscribe::{self, AddSource};

/// bark-echo-rs desktop Realtime Client: hold Subscriptions, mirror
/// their Pushes as desktop notifications, send Pushes.
#[derive(Parser)]
#[command(name = "echo-daemon", version, about)]
struct Cli {
    /// Keep secrets in a 0600 plain file under the config dir instead of
    /// the OS keyring — the explicit opt-in for headless machines without
    /// a secret service (spec story 19).
    #[arg(long, global = true)]
    plain_secrets: bool,

    #[command(subcommand)]
    command: Command,
}

#[derive(Subcommand)]
enum Command {
    /// Send one Push to a Device by its Device Key — the CLI face of the
    /// core send module (issue #6). The future scheduler's entry point.
    Push {
        /// The flags, verbatim: `--server URL`, `--device-key KEY`, the
        /// alert fields (`--title`, `--subtitle`, `--body`), the per-server
        /// Basic Auth credentials (`--user`, `--password`), and every other
        /// `--key value` pair riding through as an Ext Param (`--url`,
        /// `--group`, `--sound`, …) exactly like the server's V2 JSON
        /// route (protocol §2.2).
        #[arg(trailing_var_arg = true, allow_hyphen_values = true)]
        args: Vec<String>,
    },
    /// Recover Messages missed while offline: one-shot replay from the
    /// server's store-and-forward store, landed silently in the
    /// Subscription's History Log (GLOSSARY: Pull) — no notifications.
    Pull {
        /// The Subscription's label; omitted when exactly one Subscription
        /// is held.
        #[arg(long)]
        sub: Option<String>,
        /// The replay cursor: 0 replays everything the server still
        /// retains (protocol §3.5); ids the History Log already holds are
        /// suppressed by dedup.
        #[arg(long, default_value_t = 0)]
        cursor: u64,
    },
    /// Read a Subscription's History Log back for auditing (GLOSSARY).
    Log {
        /// The Subscription's label; omitted when exactly one Subscription
        /// is held.
        #[arg(long)]
        sub: Option<String>,
        /// Keep streaming new lines as they land — visible live while the
        /// daemon runs. Stops on Ctrl-C.
        #[arg(long)]
        follow: bool,
        /// Print only the last N entries instead of the whole log.
        #[arg(long)]
        tail: Option<usize>,
    },
    /// Manage this machine's Subscriptions.
    Subscribe {
        #[command(subcommand)]
        command: SubscribeCommand,
    },
}

#[derive(Subcommand)]
enum SubscribeCommand {
    /// Register (or reuse) a Device, mint a labelled Stream Token, and
    /// persist the Subscription across restarts (ADR-0001). Prints the
    /// Device Key — the push target for scripts.
    Add {
        /// The bark server base URL, e.g. `http://127.0.0.1:8080`.
        #[arg(long)]
        server: String,
        #[command(flatten)]
        source: AddSourceArgs,
        /// The client label naming this machine's Stream Token
        /// (default: this machine's hostname).
        #[arg(long)]
        label: Option<String>,
    },
    /// List Subscriptions with their token hints.
    List,
    /// Remove one Subscription by id (as `subscribe list` shows) and
    /// delete its secrets from the secret store.
    Rm {
        /// The Subscription's id (its Stream Token's `token_id`).
        id: String,
    },
}

/// Exactly one of `--own` / `--device-key` names the Subscription's
/// Device (GLOSSARY.md: one Subscription mirrors exactly one Device).
#[derive(Args)]
#[group(required = true, multiple = false)]
struct AddSourceArgs {
    /// Register a fresh Stream Device for this desktop (spec story 2).
    #[arg(long)]
    own: bool,
    /// Mirror an existing Device by its Device Key — e.g. an iPhone or
    /// another family machine (spec story 4).
    #[arg(long)]
    device_key: Option<String>,
}

#[tokio::main]
async fn main() {
    let cli = Cli::parse();
    if let Err(err) = run(cli).await {
        eprintln!("{err}");
        std::process::exit(1);
    }
}

async fn run(cli: Cli) -> Result<(), Box<dyn std::error::Error>> {
    let paths = Paths::resolve()?;
    let mut config = Config::load(&paths.config_file())?;
    let store: Box<dyn SecretStore> = if cli.plain_secrets {
        Box::new(PlainFileStore::new(paths.secrets_file()))
    } else {
        Box::new(KeyringStore::new())
    };

    match cli.command {
        Command::Push { args } => run_push(&args).await?,
        Command::Pull { sub, cursor } => {
            run_pull(&paths, &config, store.as_ref(), sub.as_deref(), cursor).await?
        }
        Command::Log { sub, follow, tail } => {
            run_log(&paths, &config, sub.as_deref(), follow, tail).await?
        }
        Command::Subscribe { command } => match command {
            SubscribeCommand::Add {
                server,
                source,
                label,
            } => {
                // The arg group guarantees exactly one source; the else
                // arm only keeps this panic-free should that ever change.
                let source = if source.own {
                    AddSource::Own
                } else if let Some(device_key) = source.device_key {
                    AddSource::DeviceKey(device_key)
                } else {
                    return Err("either --own or --device-key KEY is required".into());
                };
                let label = label.unwrap_or_else(default_label);
                let client = ServerClient::new(&server);
                let added =
                    subscribe::add(&mut config, store.as_ref(), &client, source, &label).await?;
                config.save(&paths.config_file())?;
                println!("device key: {}", added.device_key);
                println!(
                    "subscription {} added (label: {}, token hint: {})",
                    added.subscription.id, added.subscription.label, added.subscription.token_hint
                );
            }
            SubscribeCommand::List => {
                let subscriptions = subscribe::list(&config);
                if subscriptions.is_empty() {
                    println!("no subscriptions");
                }
                for subscription in subscriptions {
                    println!(
                        "{}  {}  {}  {}",
                        subscription.id,
                        subscription.server,
                        subscription.label,
                        subscription.token_hint
                    );
                }
            }
            SubscribeCommand::Rm { id } => {
                let removed = subscribe::remove(&mut config, store.as_ref(), &id)?;
                config.save(&paths.config_file())?;
                println!(
                    "subscription {} removed (label: {})",
                    removed.id, removed.label
                );
            }
        },
    }
    Ok(())
}

/// The `pull` subcommand: one-shot replay of the Subscription's retained
/// Messages into its History Log (GLOSSARY: Pull) — silent, no intents,
/// no notifications — reporting the ingested count. The Stream Token comes
/// from the secret store; the per-server Basic Auth credentials (§1.4)
/// ride along when the config carries them.
async fn run_pull(
    paths: &Paths,
    config: &Config,
    store: &dyn SecretStore,
    label: Option<&str>,
    cursor: u64,
) -> Result<(), Box<dyn std::error::Error>> {
    let subscription = history::resolve_subscription(config, label)?;
    let token = history::stream_token(store, &subscription)?;
    let mut pull_config = PullConfig::new(
        subscription.server.clone(),
        token,
        history::history_log(paths, &subscription),
    );
    pull_config.cursor = cursor as i64;
    pull_config.basic_auth = history::basic_auth(config, &subscription);
    let report = echo_core::pull::pull(pull_config).await?;
    println!("pulled {} messages", report.ingested);
    Ok(())
}

/// The `log` subcommand: read the Subscription's History Log back
/// (GLOSSARY), newest last; `--tail N` prints only the last N entries and
/// `--follow` keeps streaming new lines as they land (the daemon's live
/// appends and pulled backfills alike).
async fn run_log(
    paths: &Paths,
    config: &Config,
    label: Option<&str>,
    follow: bool,
    tail: Option<usize>,
) -> Result<(), Box<dyn std::error::Error>> {
    let subscription = history::resolve_subscription(config, label)?;
    let log = history::history_log(paths, &subscription);
    let entries = log.entries()?;
    let start = entries.len().saturating_sub(tail.unwrap_or(entries.len()));
    for entry in &entries[start..] {
        println!("{}", history::format_entry(entry));
    }
    if follow {
        let mut seen = entries.len();
        loop {
            seen = history::print_new_entries(&log, seen)?;
            tokio::time::sleep(history::FOLLOW_POLL).await;
        }
    }
    Ok(())
}

/// The `push` subcommand: `push --server URL --device-key KEY --title …`
/// targets any Device, mapping its flags onto the send module's Push
/// (protocol §2.2). The flags are parsed here rather than by clap's typed
/// args so that flag order never matters and unknown `--key value` pairs
/// ride through as Ext Params verbatim. Failures surface the server's
/// message and code; `run` prints them and exits non-zero.
async fn run_push(args: &[String]) -> Result<(), Box<dyn std::error::Error>> {
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
        let value = match fields.next() {
            Some(value) => value,
            None => return Err(format!("push: flag {flag} needs a value").into()),
        };
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
        return Err("push: --server URL is required".into());
    };
    let Some(device_key) = device_key else {
        return Err("push: --device-key KEY is required".into());
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

    let client = PushClient::new(&server);
    let client = if user.is_some() || password.is_some() {
        client.with_basic_auth(user.unwrap_or_default(), password.unwrap_or_default())
    } else {
        client
    };
    client.send(&push).await?;
    Ok(())
}

/// The default client label: this machine's name (spec story 3), from
/// the environment's hostname variables, falling back to the hostname
/// file and finally a stable name.
fn default_label() -> String {
    if let Some(name) = std::env::var_os("HOSTNAME").filter(|value| !value.is_empty()) {
        return name.to_string_lossy().into_owned();
    }
    #[cfg(windows)]
    if let Some(name) = std::env::var_os("COMPUTERNAME").filter(|value| !value.is_empty()) {
        return name.to_string_lossy().into_owned();
    }
    if let Ok(hostname) = std::fs::read_to_string("/etc/hostname") {
        let hostname = hostname.trim();
        if !hostname.is_empty() {
            return hostname.to_owned();
        }
    }
    "desktop".to_owned()
}
