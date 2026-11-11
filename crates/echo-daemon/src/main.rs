//! The `bark-echo` binary (this crate's product face, named after the
//! repo): the desktop Realtime Client. It holds the CLI surface — ticket
//! #3's `subscribe` subcommands over the config, the secret store, and
//! the server's Device/Token plane; ticket #6's `push` subcommand over
//! echo-core's send module; the daemon runtime (streams, notifications)
//! arrives with later tickets (ADR-0002).

use std::sync::Arc;

use clap::{Args, Parser, Subcommand};
use echo_core::pull::PullConfig;
use echo_core::push::{Push, PushClient};
use echo_core::server::ServerClient;

use echo_daemon::auth;
use echo_daemon::config::Config;
use echo_daemon::history;
use echo_daemon::notify::DesktopRenderer;
use echo_daemon::paths::Paths;
use echo_daemon::run::{self, DaemonSpec};
use echo_daemon::secrets::{KeyringStore, PlainFileStore, SecretStore};
use echo_daemon::status;
use echo_daemon::subscribe::{self, AddSource};

/// bark-echo-rs desktop Realtime Client: hold Subscriptions, mirror
/// their Pushes as desktop notifications, send Pushes.
#[derive(Parser)]
#[command(name = "bark-echo", version, about)]
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
        /// The operator Basic Auth user (§1.4) — overrides the stored
        /// credentials.
        #[arg(long)]
        user: Option<String>,
        /// The operator Basic Auth password (§1.4) — overrides the stored
        /// credentials.
        #[arg(long)]
        password: Option<String>,
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
    /// Inspect and revoke a Device's Stream Tokens on the server (protocol
    /// §3.2) — the lifecycle face of the tokens `subscribe add` mints.
    Token {
        #[command(subcommand)]
        command: TokenCommand,
    },
    /// Hold every Subscription's Realtime Stream open and render each
    /// Push as a desktop notification — the daemon runtime (ADR-0002).
    /// Stops on ctrl-C; a second run refuses on the single-instance lock.
    Run,
    /// Per-Subscription stream state plus the server's live statistics
    /// (/info): delivery health at a glance.
    Status,
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
        /// The operator Basic Auth user (§1.4) for a protected server —
        /// stored (with the password) so every later connection rides
        /// it; without these flags no credentials are set.
        #[arg(long)]
        auth_user: Option<String>,
        /// The operator Basic Auth password (§1.4) — stored in the secret
        /// store under the server's key, never in config.toml.
        #[arg(long)]
        auth_password: Option<String>,
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

/// The `token` subcommands (protocol §3.2). Both address one Device by
/// its Device Key and present the operator Basic Auth credentials (§1.4)
/// when the server demands them.
#[derive(Subcommand)]
enum TokenCommand {
    /// List a Device's Stream Token summaries: token id, hint, label, and
    /// revoked state — the secret is structurally absent (§3.2).
    List {
        /// The bark server base URL, e.g. `http://127.0.0.1:8080`.
        #[arg(long)]
        server: String,
        /// The Device Key whose Stream Tokens are listed.
        #[arg(long)]
        device_key: String,
        #[command(flatten)]
        auth: BasicAuthArgs,
    },
    /// Revoke one Stream Token by its public token id (§3.2): takes
    /// effect immediately — its connect attempts fail auth from here on.
    /// Deliberately not idempotent (§3.2): revoking a revoked token is an
    /// error.
    Revoke {
        /// The bark server base URL, e.g. `http://127.0.0.1:8080`.
        #[arg(long)]
        server: String,
        /// The Device Key the token was minted for.
        #[arg(long)]
        device_key: String,
        /// The Stream Token's public id (16 hex chars, as `token list`
        /// shows).
        #[arg(long)]
        token_id: String,
        #[command(flatten)]
        auth: BasicAuthArgs,
    },
}

/// The operator Basic Auth credentials (protocol §1.4) a server may
/// demand on its token plane.
#[derive(Args)]
struct BasicAuthArgs {
    /// The operator Basic Auth user.
    #[arg(long)]
    user: Option<String>,
    /// The operator Basic Auth password.
    #[arg(long)]
    password: Option<String>,
}

impl BasicAuthArgs {
    /// The flags as the resolution seam takes them: `(user, password)`,
    /// either `None` when its flag is absent (the stored credentials fill
    /// it then).
    fn parts(&self) -> (Option<&str>, Option<&str>) {
        (self.user.as_deref(), self.password.as_deref())
    }
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
    let store: Arc<dyn SecretStore> = if cli.plain_secrets {
        Arc::new(PlainFileStore::new(paths.secrets_file()))
    } else {
        Arc::new(KeyringStore::new())
    };

    match cli.command {
        Command::Push { args } => {
            run_push(&config, store.as_ref(), &args).await?;
        }
        Command::Pull {
            sub,
            cursor,
            user,
            password,
        } => {
            run_pull(
                &paths,
                &config,
                store.as_ref(),
                sub.as_deref(),
                cursor,
                user.as_deref(),
                password.as_deref(),
            )
            .await?
        }
        Command::Log { sub, follow, tail } => {
            run_log(&paths, &config, sub.as_deref(), follow, tail).await?
        }
        Command::Subscribe { command } => match command {
            SubscribeCommand::Add {
                server,
                source,
                label,
                auth_user,
                auth_password,
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
                // Operator Basic Auth (§1.4, story 18): the client presents
                // the credentials for minting; `add` then stores the
                // password and lays the user + marker into the config.
                let client = ServerClient::new(&server);
                let client = match (&auth_user, &auth_password) {
                    (None, None) => client,
                    (user, password) => client.with_basic_auth(
                        user.clone().unwrap_or_default(),
                        password.clone().unwrap_or_default(),
                    ),
                };
                let auth = (auth_user.is_some() || auth_password.is_some()).then(|| {
                    echo_core::stream::BasicAuth {
                        user: auth_user.unwrap_or_default(),
                        password: auth_password.unwrap_or_default(),
                    }
                });
                let added =
                    subscribe::add(&mut config, store.as_ref(), &client, source, &label, auth)
                        .await?;
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
        Command::Token { command } => match command {
            TokenCommand::List {
                server,
                device_key,
                auth: auth_flags,
            } => {
                let client = token_client(&config, store.as_ref(), &server, auth_flags)?;
                let tokens = client.list_stream_tokens(&device_key).await?;
                if tokens.is_empty() {
                    println!("no stream tokens");
                }
                for token in tokens {
                    let state = if token.revoked { "  revoked" } else { "" };
                    println!(
                        "{}  {}  {}{}",
                        token.token_id, token.token_hint, token.client_label, state
                    );
                }
            }
            TokenCommand::Revoke {
                server,
                device_key,
                token_id,
                auth: auth_flags,
            } => {
                let client = token_client(&config, store.as_ref(), &server, auth_flags)?;
                let token = client.revoke_stream_token(&device_key, &token_id).await?;
                println!(
                    "stream token {} revoked (label: {})",
                    token.token_id, token.client_label
                );
            }
        },
        Command::Run => {
            let spec = DaemonSpec {
                paths: paths.clone(),
                config,
                store,
                renderer: Arc::new(DesktopRenderer),
            };
            run::run(spec, async {
                let _ = tokio::signal::ctrl_c().await;
            })
            .await?;
        }
        Command::Status => {
            let report = status::report(&paths, &config, store.as_ref()).await?;
            print!("{}", report.render());
        }
    }
    Ok(())
}

/// The `pull` subcommand: one-shot replay of the Subscription's retained
/// Messages into its History Log (GLOSSARY: Pull) — silent, no intents,
/// no notifications — reporting the ingested count. The Stream Token comes
/// from the secret store; the per-server Basic Auth credentials (§1.4)
/// are the stored ones (story 18) unless the flags override them.
async fn run_pull(
    paths: &Paths,
    config: &Config,
    store: &dyn SecretStore,
    label: Option<&str>,
    cursor: u64,
    flag_user: Option<&str>,
    flag_password: Option<&str>,
) -> Result<(), Box<dyn std::error::Error>> {
    let subscription = history::resolve_subscription(config, label)?;
    let token = history::stream_token(store, &subscription)?;
    let mut pull_config = PullConfig::new(
        subscription.server.clone(),
        token,
        history::history_log(paths, &subscription),
    );
    pull_config.cursor = cursor as i64;
    pull_config.basic_auth = auth::resolve(
        config,
        store,
        &subscription.server,
        flag_user,
        flag_password,
    )?;
    let report = echo_core::pull::pull(pull_config).await?;
    println!("pulled {} messages", report.ingested);
    Ok(())
}

/// The `log` subcommand: read the Subscription's History Log back
/// (GLOSSARY), newest last; `--tail N` prints only the last N entries and
/// `--follow` keeps streaming new lines as they land (the daemon's live
/// appends and pulled Messages alike).
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
/// ride through as Ext Params verbatim. The Basic Auth credentials (§1.4)
/// are the stored ones (story 18) unless the flags override them.
/// Failures surface the server's message and code; `run` prints them and
/// exits non-zero.
async fn run_push(
    config: &Config,
    store: &dyn SecretStore,
    args: &[String],
) -> Result<(), Box<dyn std::error::Error>> {
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
    let client = match auth::resolve(config, store, &server, user.as_deref(), password.as_deref())?
    {
        Some(auth) => client.with_basic_auth(auth.user, auth.password),
        None => client,
    };
    client.send(&push).await?;
    Ok(())
}

/// The server client the `token` subcommands talk through, carrying the
/// operator Basic Auth credentials (§1.4) — the stored ones (story 18)
/// unless the flags override them.
fn token_client(
    config: &Config,
    store: &dyn SecretStore,
    server: &str,
    auth_flags: BasicAuthArgs,
) -> Result<ServerClient, Box<dyn std::error::Error>> {
    let client = ServerClient::new(server);
    Ok(
        match auth::resolve(
            config,
            store,
            server,
            auth_flags.parts().0,
            auth_flags.parts().1,
        )? {
            Some(auth) => client.with_basic_auth(auth.user, auth.password),
            None => client,
        },
    )
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
