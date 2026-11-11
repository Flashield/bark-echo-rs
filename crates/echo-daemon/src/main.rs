//! echo-daemon: the desktop Realtime Client binary. This shell holds the
//! CLI surface — ticket #3's `subscribe` subcommands over the config, the
//! secret store, and the server's Device/Token plane; the daemon runtime
//! (streams, notifications) arrives with later tickets (ADR-0002).

use clap::{Args, Parser, Subcommand};
use echo_core::server::ServerClient;

use echo_daemon::config::Config;
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
