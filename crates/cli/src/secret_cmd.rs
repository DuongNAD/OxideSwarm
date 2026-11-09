//! Cluster secret handling for the CLI: where the secret comes from, the `secret generate` /
//! `secret show` subcommands, and the Master's policy for running without one.
//!
//! Only the CLI layer resolves secrets; the library types (`ServerConfig`, `WorkerConfig`) take
//! the secret explicitly. Resolution order (first hit wins; a source that is present but invalid
//! is an error, never a silent fall-through to a weaker setup):
//!
//! 1. `--secret <hex>` (discouraged: visible in the process list)
//! 2. `--secret-file <path>`
//! 3. `OXIDESWARM_SECRET`
//! 4. `OXIDESWARM_SECRET_FILE`
//! 5. the `secret` / `secret_file` config-file keys (see [`crate::config::ConfigFile::secret_keys`])
//! 6. the default file `~/.oxideswarm/cluster.secret`, if it exists
//!
//! Tiers 1-4 and 6 are `ClusterSecret::resolve` from `rusty_grid_core`; the config file slots in
//! between the environment and the default file, matching "flags > environment > config file >
//! defaults" used by every other setting.

use std::path::{Path, PathBuf};
use std::process::ExitCode;

use clap::{Parser, Subcommand};

use rusty_grid_core::cluster_auth::{ClusterSecret, ENV_SECRET, ENV_SECRET_FILE};
use rusty_grid_core::error::{GridError, GridResult};
use rusty_grid_core::noise_auth::{default_node_key_path, NodeKeypair, PublicKey, ENV_MASTER_KEY};

use crate::config::{resolve_bool, ConfigFile, SecretSection, SecretText};
use crate::connect::{
    cli_master_key_flag, master_requires_secret, set_default_client_v3_auth, AUTH_EXIT_CODE,
};

/// The secret-related command line flags, as given.
#[derive(Debug, Clone, Default)]
pub struct SecretOpts {
    /// `--secret <hex>`.
    pub value: Option<SecretText>,
    /// `--secret-file <path>`.
    pub file: Option<PathBuf>,
}

impl SecretOpts {
    /// Resolves the cluster secret for a command whose config-file keys live in `section`,
    /// reading the real environment and the real default file location.
    pub fn resolve(
        &self,
        config: Option<&ConfigFile>,
        section: SecretSection,
    ) -> GridResult<Option<ClusterSecret>> {
        self.resolve_with(
            config,
            section,
            // `var_os` + lossy, like `ClusterSecret::resolve`: a non-Unicode value is present
            // (and invalid), not absent.
            |name| std::env::var_os(name).map(|v| v.to_string_lossy().into_owned()),
            ClusterSecret::default_path(),
        )
    }

    /// [`SecretOpts::resolve`] with the environment and the default file location injected.
    pub fn resolve_with(
        &self,
        config: Option<&ConfigFile>,
        section: SecretSection,
        env: impl Fn(&str) -> Option<String>,
        default_file: Option<PathBuf>,
    ) -> GridResult<Option<ClusterSecret>> {
        // Tiers 1-4. The default file is withheld here: the config file outranks it.
        if let Some(secret) = ClusterSecret::resolve_with(
            self.value.as_ref().map(SecretText::as_str),
            self.file.as_deref(),
            &env,
            None,
        )
        .map_err(|e| explain_missing_file(e, self.file.as_deref()))?
        {
            return Ok(Some(secret));
        }

        // Tier 5: config file keys (a value beats a file, like the flags do).
        if let Some(config) = config {
            let (value, file) = config.secret_keys(section);
            if let Some(value) = value {
                return ClusterSecret::from_hex(&value)
                    .map(Some)
                    .map_err(|e| config_key_error("secret", &e));
            }
            if let Some(file) = file {
                let path = PathBuf::from(file.trim());
                return ClusterSecret::load_file(&path)
                    .map(Some)
                    .map_err(|e| explain_missing_file(e, Some(&path)));
            }
        }

        // Tier 6: the default file, if present.
        ClusterSecret::resolve_with(None, None, |_| None, default_file)
    }

    /// Resolves the pinned master X25519 public key (`--master-key` > `RUSTY_GRID_MASTER_KEY` >
    /// config `master_key` > `None`).
    pub fn resolve_master_key(
        &self,
        config: Option<&ConfigFile>,
        section: SecretSection,
    ) -> GridResult<Option<PublicKey>> {
        let flag = cli_master_key_flag();
        self.resolve_master_key_with(flag.as_deref(), config, section, |name| {
            std::env::var_os(name).map(|v| v.to_string_lossy().into_owned())
        })
    }

    /// [`SecretOpts::resolve_master_key`] with the CLI flag and environment lookup injected.
    pub fn resolve_master_key_with(
        &self,
        flag: Option<&str>,
        config: Option<&ConfigFile>,
        section: SecretSection,
        env: impl Fn(&str) -> Option<String>,
    ) -> GridResult<Option<PublicKey>> {
        if let Some(flag_val) = flag {
            let trimmed = flag_val.trim();
            if trimmed.is_empty() {
                return Err(GridError::Config("--master-key is set but empty".into()));
            }
            return PublicKey::from_hex(trimmed)
                .map(Some)
                .map_err(|e| GridError::Config(format!("--master-key: {e}")));
        }
        if let Some(env_val) = env(ENV_MASTER_KEY) {
            let trimmed = env_val.trim();
            if trimmed.is_empty() {
                return Err(GridError::Config(format!(
                    "{ENV_MASTER_KEY} is set but empty"
                )));
            }
            return PublicKey::from_hex(trimmed)
                .map(Some)
                .map_err(|e| GridError::Config(format!("{ENV_MASTER_KEY}: {e}")));
        }
        if let Some(cfg_hex) = config.and_then(|c| c.master_key(section)) {
            return PublicKey::from_hex(&cfg_hex)
                .map(Some)
                .map_err(|e| config_key_error("master_key", &e));
        }
        Ok(None)
    }

    /// Resolves the OXS-AUTH v3 credential pair `(node_key, pinned_master_key)` for `section`:
    /// when a `master_key` is configured, loads (or generates on first run) the private key at
    /// `~/.oxideswarm/node.key`.
    pub fn resolve_v3_auth(
        &self,
        config: Option<&ConfigFile>,
        section: SecretSection,
    ) -> GridResult<Option<(NodeKeypair, PublicKey)>> {
        let Some(master_pub) = self.resolve_master_key(config, section)? else {
            return Ok(None);
        };
        let Some(node_key_path) = default_node_key_path() else {
            return Err(GridError::Config(
                "cannot determine the home directory for ~/.oxideswarm/node.key".into(),
            ));
        };
        let (node_key, _) = NodeKeypair::load_or_generate_file(&node_key_path)?;
        Ok(Some((node_key, master_pub)))
    }
}

fn config_key_error(key: &str, err: &GridError) -> GridError {
    let detail = match err {
        GridError::Config(msg) => msg.clone(),
        other => other.to_string(),
    };
    GridError::Config(format!("config file key '{key}': {detail}"))
}

/// Where the cluster secret would be visible to a confined task, or `None` when it is not.
///
/// On macOS no sandbox rule stops a task from reading the environment and the command line of
/// the other processes of its user (`sysctl KERN_PROCARGS2`). A secret handed to the worker in
/// `OXIDESWARM_SECRET` or `--secret` is therefore readable by every task, whatever its isolation
/// level (design 0033, Security). A secret file path is not exposed that way, but the file itself
/// must be unreadable to tasks: see [`secret_files_to_protect`]. Returns the description of the
/// exposed channel.
pub fn secret_exposed_to_tasks(
    opts: &SecretOpts,
    env: impl Fn(&str) -> Option<String>,
) -> Option<&'static str> {
    if opts.value.is_some() {
        return Some("--secret, which is visible in the process list");
    }
    if env(ENV_SECRET).is_some_and(|v| !v.trim().is_empty()) {
        return Some("the OXIDESWARM_SECRET environment variable, which other processes of this user can read");
    }
    None
}

/// Every file that holds or may hold the cluster secret for a worker: the file named by
/// `--secret-file`, `OXIDESWARM_SECRET_FILE` or a config key, the default file, and the config file
/// in use (it can hold `secret =`). A worker with isolation passes these to the sandbox policy so
/// that a task cannot read them even when they live outside the denied home (design 0033).
pub fn secret_files_to_protect(
    opts: &SecretOpts,
    config: Option<&ConfigFile>,
    env: impl Fn(&str) -> Option<String>,
) -> Vec<PathBuf> {
    let mut files: Vec<PathBuf> = Vec::new();
    files.extend(opts.file.clone());
    files.extend(
        env(ENV_SECRET_FILE)
            .filter(|v| !v.trim().is_empty())
            .map(PathBuf::from),
    );
    if let Some(config) = config {
        for section in [SecretSection::Worker, SecretSection::Master] {
            if let (_, Some(file)) = config.secret_keys(section) {
                files.push(PathBuf::from(file.trim()));
            }
        }
        files.extend(config.source_path.0.clone());
    }
    files.extend(ClusterSecret::default_path());
    files.extend(default_node_key_path());
    files.sort();
    files.dedup();
    files
}

/// The refusal a worker with `--isolation fs|fs+net` prints for an exposed secret.
pub fn exposed_secret_error(channel: &str) -> String {
    format!(
        "--isolation is on but the cluster secret is given through {channel}, and a task running as this user could read it. Put the secret in a file and pass --secret-file PATH or {ENV_SECRET_FILE}=PATH (or use the default file ~/.oxideswarm/cluster.secret), and unset {ENV_SECRET}"
    )
}

/// Adds the "how do I create it" hint when an explicitly named secret file does not exist.
fn explain_missing_file(err: GridError, file: Option<&Path>) -> GridError {
    match (&err, file) {
        (GridError::Config(msg), Some(path)) if msg.starts_with("cannot read cluster secret file") && !path.exists() => {
            GridError::Config(format!(
                "cluster secret file '{}' does not exist; create it with `oxideswarm secret generate --out {}` (or copy the file from the Master machine)",
                path.display(),
                path.display()
            ))
        }
        _ => err,
    }
}

// ---------------------------------------------------------------------------------------------
// `oxideswarm secret ...`
// ---------------------------------------------------------------------------------------------

/// Manage the shared cluster secret.
#[derive(Parser, Debug, Clone)]
pub struct SecretCliArgs {
    #[command(subcommand)]
    pub action: SecretAction,
}

#[derive(Subcommand, Debug, Clone)]
pub enum SecretAction {
    /// Generate a new random cluster secret and save it to a private file
    Generate(SecretGenerateArgs),
    /// Print the cluster secret (or the derived dashboard token) this machine would use
    Show(SecretShowArgs),
    /// Print a join token (master address, optional P2P ticket and the cluster secret) for
    /// `oxideswarm join` on another machine. The token is as sensitive as the secret.
    JoinToken(JoinTokenArgs),
}

#[derive(Parser, Debug, Clone)]
pub struct JoinTokenArgs {
    #[arg(
        long,
        value_name = "HOST:PORT",
        help = "Address other machines use to reach this master's grid port (default: the [master] listen address of the config file, unless it is a wildcard such as 0.0.0.0)"
    )]
    pub master: Option<String>,

    #[arg(
        long,
        value_name = "TICKET",
        conflicts_with = "p2p_ticket_file",
        help = "Also carry this P2P connection ticket"
    )]
    pub p2p_ticket: Option<String>,

    #[arg(
        long,
        value_name = "PATH",
        help = "Also carry the P2P ticket stored in this file (default: the [master] p2p_ticket_file of the config file, if it exists)"
    )]
    pub p2p_ticket_file: Option<PathBuf>,
}

#[derive(Parser, Debug, Clone)]
pub struct SecretGenerateArgs {
    #[arg(
        long,
        value_name = "PATH",
        help = "Where to write the secret (default: ~/.oxideswarm/cluster.secret)"
    )]
    pub out: Option<PathBuf>,

    #[arg(long, help = "Replace the file if it already exists")]
    pub force: bool,
}

#[derive(Parser, Debug, Clone)]
pub struct SecretShowArgs {
    #[arg(
        long,
        help = "Print the dashboard Admin token derived from the secret instead of the secret itself"
    )]
    pub dashboard_token: bool,
}

/// Creates a new secret file. Returns the path written. Never overwrites without `force`.
///
/// `default_path` is where the file goes when `out` is not given (the caller passes
/// `ClusterSecret::default_path()`).
pub fn generate_secret_file(
    out: Option<&Path>,
    force: bool,
    default_path: Option<PathBuf>,
) -> GridResult<PathBuf> {
    let path = match (out, default_path) {
        (Some(out), _) => out.to_path_buf(),
        (None, Some(default)) => default,
        (None, None) => return Err(GridError::Config(
            "cannot determine the home directory for ~/.oxideswarm/cluster.secret; pass --out PATH"
                .into(),
        )),
    };
    ClusterSecret::generate()?.save_file(&path, force)?;
    Ok(path)
}

/// The text `secret show` prints: the secret as hex, or the dashboard token derived from it.
pub fn render_secret(secret: &ClusterSecret, dashboard_token: bool) -> String {
    if dashboard_token {
        rusty_grid_master::dashboard_admin_token(secret)
    } else {
        secret.to_hex()
    }
}

/// How other machines obtain the secret, in one line (shown after generating a file).
pub fn share_instruction(path: &Path) -> String {
    format!(
        "To let other machines join: copy {} to ~/.oxideswarm/cluster.secret there (mode 600), or run them with --secret-file PATH or {ENV_SECRET}=<secret> (see `oxideswarm secret show`)",
        path.display()
    )
}

/// The secret this machine would use, whichever command it is for: the flags, environment and
/// default file are the same for every command; only the config file has per-command sections,
/// so they are tried in turn (the first one that yields a secret wins).
fn resolve_for_show(
    opts: &SecretOpts,
    config: Option<&ConfigFile>,
) -> GridResult<Option<ClusterSecret>> {
    for section in [
        SecretSection::Master,
        SecretSection::Worker,
        SecretSection::Submit,
        SecretSection::Status,
    ] {
        if let Some(secret) = opts.resolve(config, section)? {
            return Ok(Some(secret));
        }
    }
    Ok(None)
}

/// Runs `oxideswarm secret ...`.
pub fn run_secret(args: SecretCliArgs, opts: &SecretOpts, config: Option<&ConfigFile>) -> ExitCode {
    match args.action {
        SecretAction::Generate(gen) => {
            match generate_secret_file(gen.out.as_deref(), gen.force, ClusterSecret::default_path())
            {
                Ok(path) => {
                    println!("Cluster secret written to {}", path.display());
                    println!("{}", share_instruction(&path));
                    if gen.force {
                        println!("The previous secret was replaced: every machine must receive the new one.");
                    }
                    ExitCode::SUCCESS
                }
                Err(e) => {
                    eprintln!("{e}");
                    ExitCode::FAILURE
                }
            }
        }
        SecretAction::JoinToken(args) => match resolve_for_show(opts, config)
            .and_then(|secret| {
                secret.ok_or_else(|| {
                    GridError::Config(
                        "No cluster secret found to put in a token. Create one with `oxideswarm secret generate`.".into(),
                    )
                })
            })
            .and_then(|secret| build_join_token(&args, secret, config))
        {
            Ok(token) => {
                eprintln!("This token contains the cluster secret: anyone who has it can join and drive your cluster. Send it over a channel you trust and do not paste it into chat, e-mail, tickets or logs. On the new machine run: oxideswarm join  (token on standard input)");
                println!("{}", token.encode());
                ExitCode::SUCCESS
            }
            Err(e) => {
                eprintln!("{e}");
                ExitCode::FAILURE
            }
        },
        SecretAction::Show(show) => match resolve_for_show(opts, config) {
            Ok(Some(secret)) => {
                println!("{}", render_secret(&secret, show.dashboard_token));
                ExitCode::SUCCESS
            }
            Ok(None) => {
                eprintln!(
                    "No cluster secret found (checked --secret, --secret-file, {ENV_SECRET}, {ENV_SECRET_FILE}, the config file and ~/.oxideswarm/cluster.secret). Create one with `oxideswarm secret generate`."
                );
                ExitCode::FAILURE
            }
            Err(e) => {
                eprintln!("{e}");
                ExitCode::FAILURE
            }
        },
    }
}

/// Builds the join token for `secret`. The address comes from `--master`, else from the config
/// file's `[master] listen` when other machines could dial it (not a wildcard, not port 0); the
/// ticket from `--p2p-ticket`, `--p2p-ticket-file` or the config's `[master] p2p_ticket_file`.
pub fn build_join_token(
    args: &JoinTokenArgs,
    secret: ClusterSecret,
    config: Option<&ConfigFile>,
) -> GridResult<crate::join::JoinToken> {
    let master_cfg = config.and_then(|c| c.master.as_ref());
    let ticket = match (&args.p2p_ticket, &args.p2p_ticket_file) {
        (Some(t), _) => Some(t.trim().to_string()),
        (None, Some(file)) => Some(
            std::fs::read_to_string(file)
                .map_err(|e| {
                    GridError::Config(format!(
                        "cannot read P2P ticket file '{}': {e}",
                        file.display()
                    ))
                })?
                .trim()
                .to_string(),
        ),
        (None, None) => master_cfg
            .and_then(|m| m.p2p_ticket_file.as_deref())
            .and_then(|f| std::fs::read_to_string(f).ok())
            .map(|t| t.trim().to_string()),
    }
    .filter(|t| !t.is_empty());

    let master = match &args.master {
        Some(m) => Some(crate::connect::normalize_master_addr(m)),
        None => master_cfg
            .and_then(|m| m.listen.as_deref())
            .and_then(|l| l.parse::<std::net::SocketAddr>().ok())
            .filter(|a| !a.ip().is_unspecified() && a.port() != 0)
            .map(|a| a.to_string()),
    }
    .filter(|m| !m.is_empty());
    if master.is_none() && ticket.is_none() {
        return Err(GridError::Config(
            "no address to put in the token: pass --master HOST:PORT (the address other machines use to reach this master, for example 192.168.1.5:8088), or --p2p-ticket-file".into(),
        ));
    }
    Ok(crate::join::JoinToken {
        master,
        ticket,
        secret,
    })
}

// ---------------------------------------------------------------------------------------------
// Master policy
// ---------------------------------------------------------------------------------------------

/// What the Master decided about its cluster secret at startup.
#[derive(Debug, Clone)]
pub enum MasterSecretOutcome {
    /// A secret was resolved (flag, environment, config file or default file): authentication on.
    Configured(ClusterSecret),
    /// Nothing resolved on an exposed master, so a new secret was written to `path`.
    Generated {
        secret: ClusterSecret,
        path: PathBuf,
    },
    /// No secret, and none needed: the master is reachable from this machine only.
    OpenLoopbackOnly,
    /// No secret on an exposed master because `--insecure` was given.
    OpenInsecure,
}

impl MasterSecretOutcome {
    /// The secret to install on the server, if any.
    pub fn secret(&self) -> Option<&ClusterSecret> {
        match self {
            Self::Configured(secret) | Self::Generated { secret, .. } => Some(secret),
            Self::OpenLoopbackOnly | Self::OpenInsecure => None,
        }
    }
}

/// Applies the Master's secret policy.
///
/// * a resolved secret is always used (even with `--insecure`, which only waives the *requirement*);
/// * no secret and `exposed == false` (loopback-only, no P2P): fine, nothing to protect from;
/// * no secret and `exposed`, with `--insecure`: allowed, the caller must warn loudly;
/// * no secret and `exposed`, without `--insecure`: a new secret is generated at `default_path`
///   (an existing file there was already tried by the resolution, so this never overwrites one).
///
/// `exposed` is true when the grid port listens beyond loopback **or** the P2P listener is on: an
/// iroh ticket only addresses the node, it is not a credential, so a P2P master is reachable from
/// the internet whatever its TCP listen address says.
pub fn decide_master_secret(
    resolved: Option<ClusterSecret>,
    exposed: bool,
    insecure: bool,
    default_path: Option<PathBuf>,
) -> GridResult<MasterSecretOutcome> {
    if let Some(secret) = resolved {
        return Ok(MasterSecretOutcome::Configured(secret));
    }
    if !exposed {
        return Ok(MasterSecretOutcome::OpenLoopbackOnly);
    }
    if insecure {
        return Ok(MasterSecretOutcome::OpenInsecure);
    }
    let Some(path) = default_path else {
        return Err(GridError::Config(
            "this master is reachable beyond localhost and needs a cluster secret, but the home directory for ~/.oxideswarm/cluster.secret is unknown; pass --secret-file PATH, or --insecure to run without authentication (not recommended)"
                .into(),
        ));
    };
    let (secret, created) = ClusterSecret::load_or_generate_file(&path)?;
    if created {
        Ok(MasterSecretOutcome::Generated { secret, path })
    } else {
        // Someone created the file between resolution and now (two masters starting together).
        Ok(MasterSecretOutcome::Configured(secret))
    }
}

// ---------------------------------------------------------------------------------------------
// Worker / client policy
// ---------------------------------------------------------------------------------------------

/// Which side of the grid is asking, for the wording of the refusal.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum PeerRole {
    /// `worker`: would obey whatever commands the master it connects to sends.
    Worker,
    /// `submit` / `status` / `workers` / `mapreduce`: would hand its requests to the master.
    Client,
}

/// What a worker or client decided about its cluster secret before connecting.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum PeerSecretOutcome {
    /// A secret is configured: the master must prove it holds it (and the peer proves the same).
    Authenticated,
    /// No secret, and none needed: the master is on this machine.
    OpenLocalOnly,
    /// No secret although the master is remote/discovered/P2P, because `--insecure` was given.
    OpenInsecure,
}

/// Applies the worker/client policy: authentication is **fail-closed**.
///
/// * a resolved secret is always used (even with `--insecure`, which only waives the *requirement*);
/// * no secret and a master on this machine ([`master_requires_secret`] is false): fine;
/// * no secret and a non-loopback, `auto`-discovered or P2P-ticket master, with `--insecure`:
///   allowed, the caller must warn loudly;
/// * no secret and such a master, without `--insecure`: refused with `GridError::AuthRequired`
///   (exit code 6). Without a secret the peer cannot tell the real master from any host that
///   answers a discovery probe or sits on the path, and a rogue master can run arbitrary commands
///   on a worker or read everything a client submits.
///
/// A secret that is *configured but blank* never reaches this function as "no secret":
/// `ClusterSecret::resolve` reports it as a configuration error.
pub fn decide_peer_secret(
    secret: Option<&ClusterSecret>,
    master_addr: &str,
    via_p2p_ticket: bool,
    insecure: bool,
    role: PeerRole,
) -> GridResult<PeerSecretOutcome> {
    decide_peer_credential(
        secret.is_some(),
        master_addr,
        via_p2p_ticket,
        insecure,
        role,
    )
}

/// Like [`decide_peer_secret`], accepting any authentication credential (a shared cluster secret
/// or a pinned OXS-AUTH v3 `master_key` enrolment).
pub fn decide_peer_credential(
    has_credential: bool,
    master_addr: &str,
    via_p2p_ticket: bool,
    insecure: bool,
    role: PeerRole,
) -> GridResult<PeerSecretOutcome> {
    let via_p2p_ticket = via_p2p_ticket || crate::connect::is_p2p_ticket(master_addr);
    if has_credential {
        return Ok(PeerSecretOutcome::Authenticated);
    }
    if !master_requires_secret(master_addr, via_p2p_ticket) {
        return Ok(PeerSecretOutcome::OpenLocalOnly);
    }
    if insecure {
        return Ok(PeerSecretOutcome::OpenInsecure);
    }

    let target = if via_p2p_ticket {
        "a master reached through a P2P ticket (a ticket only addresses a node; it does not prove who runs it)".to_string()
    } else if master_addr.trim().is_empty()
        || master_addr.trim().to_ascii_lowercase().starts_with("auto")
    {
        "a master found by LAN auto-discovery (whichever host answers first)".to_string()
    } else {
        format!("the master at {}", master_addr.trim())
    };
    let risk = match role {
        PeerRole::Worker => {
            "any host posing as the master could then run arbitrary commands on this machine"
        }
        PeerRole::Client => {
            "the command, environment and input of your tasks would go to whichever host answers"
        }
    };
    Err(GridError::AuthRequired(format!(
        "refusing to connect to {target} without a cluster secret: {risk}. Provide the secret (--secret-file PATH, {ENV_SECRET}=<secret>, or copy ~/.oxideswarm/cluster.secret from the master machine), or pass --insecure to continue without authentication (not recommended)"
    )))
}

/// Prints the loud warning for [`PeerSecretOutcome::OpenInsecure`] to stderr, once per start
/// (a worker also logs it, like the master does). Silent for the other outcomes.
///
/// Clients deliberately do not log it: the tracing subscriber writes to stdout, which `--json`
/// consumers parse. Never prints the secret.
pub fn announce_peer_secret(outcome: PeerSecretOutcome, role: PeerRole, master_addr: &str) {
    if outcome != PeerSecretOutcome::OpenInsecure {
        return;
    }
    let who = match role {
        PeerRole::Worker => "this worker will obey",
        PeerRole::Client => "this client will send its requests to",
    };
    let banner = format!(
        "WARNING: --insecure: no cluster secret, so the master ({}) is not authenticated and {who} any host that answers as the master. Use this only on a network you fully control.",
        if master_addr.trim().is_empty() { "auto" } else { master_addr.trim() }
    );
    if role == PeerRole::Worker {
        tracing::warn!("{banner}");
    }
    eprintln!("!!! {banner}");
}

/// Resolves `--insecure` for a worker or client command: the flag, then `RUSTY_GRID_INSECURE`,
/// then the command's config-file key, else off.
pub fn peer_insecure(flag: bool, config_value: Option<bool>) -> bool {
    resolve_bool(
        if flag { Some(true) } else { None },
        &["RUSTY_GRID_INSECURE"],
        config_value,
        false,
    )
}

/// Resolves the cluster secret for a client command and applies the fail-closed policy for
/// `master_addr` (see [`decide_peer_secret`]), printing the error on failure: a configuration
/// problem exits with 1, a refusal to talk to an unauthenticated remote / `auto` master without
/// `--insecure` with the authentication exit code (6).
pub fn client_secret(
    secret_opts: &SecretOpts,
    config_file: Option<&ConfigFile>,
    section: SecretSection,
    master_addr: &str,
    insecure: bool,
) -> Result<Option<ClusterSecret>, ExitCode> {
    let v3_auth = secret_opts
        .resolve_v3_auth(config_file, section)
        .map_err(|e| {
            eprintln!("{e}");
            ExitCode::FAILURE
        })?;
    let secret = secret_opts.resolve(config_file, section).map_err(|e| {
        eprintln!("{e}");
        ExitCode::FAILURE
    })?;
    match decide_peer_credential(
        secret.is_some() || v3_auth.is_some(),
        master_addr,
        false,
        insecure,
        PeerRole::Client,
    ) {
        Ok(outcome) => {
            set_default_client_v3_auth(v3_auth);
            announce_peer_secret(outcome, PeerRole::Client, master_addr);
            Ok(secret)
        }
        Err(e) => {
            eprintln!("{e}");
            Err(ExitCode::from(AUTH_EXIT_CODE))
        }
    }
}

/// Prints what the Master decided, once at startup. Never prints the secret itself.
pub fn announce_master_secret(outcome: &MasterSecretOutcome, listen: &str, insecure_flag: bool) {
    match outcome {
        MasterSecretOutcome::Configured(_) => {
            println!(
                "Cluster authentication: enabled (workers and clients must hold the cluster secret)"
            );
            if insecure_flag {
                println!("Note: --insecure ignored because a cluster secret is configured");
            }
        }
        MasterSecretOutcome::Generated { path, .. } => {
            println!(
                "Cluster authentication: enabled. Generated a new cluster secret at {}",
                path.display()
            );
            println!("{}", share_instruction(path));
        }
        MasterSecretOutcome::OpenLoopbackOnly => {}
        MasterSecretOutcome::OpenInsecure => {
            let banner = format!(
                "WARNING: --insecure: this master ({listen}) is reachable beyond this machine and runs WITHOUT a cluster secret. Anyone who can reach it can run arbitrary commands on every worker. Use this only on a network you fully control."
            );
            tracing::warn!("{banner}");
            eprintln!("!!! {banner}");
        }
    }
}

#[cfg(test)]
mod exposure_tests {
    use super::*;

    #[test]
    fn test_secret_files_to_protect_lists_every_place_the_secret_may_be() {
        let opts = SecretOpts {
            file: Some(PathBuf::from("/x/flag.secret")),
            ..SecretOpts::default()
        };
        let config = ConfigFile {
            source_path: crate::config::SourcePath(Some(PathBuf::from("/x/rusty-grid.toml"))),
            secret_file: Some("/x/config.secret".into()),
            ..ConfigFile::default()
        };
        let env = |n: &str| (n == ENV_SECRET_FILE).then(|| "/x/env.secret".to_string());
        let files = secret_files_to_protect(&opts, Some(&config), env);
        for expected in [
            "/x/flag.secret",
            "/x/env.secret",
            "/x/config.secret",
            "/x/rusty-grid.toml",
        ] {
            assert!(
                files.contains(&PathBuf::from(expected)),
                "{expected} in {files:?}"
            );
        }
        assert!(
            secret_files_to_protect(&SecretOpts::default(), None, |_| None)
                .iter()
                .all(|p| p.ends_with("cluster.secret") || p.ends_with("node.key"))
        );
    }

    #[test]
    fn test_secret_exposure_is_flag_or_env_value_but_not_a_file() {
        let none = |_: &str| None;
        let mut opts = SecretOpts::default();
        assert_eq!(secret_exposed_to_tasks(&opts, none), None);
        let env = |n: &str| (n == ENV_SECRET).then(|| "ab".repeat(32));
        assert!(secret_exposed_to_tasks(&opts, env)
            .unwrap()
            .contains("OXIDESWARM_SECRET"));
        let blank = |n: &str| (n == ENV_SECRET).then(|| "  ".to_string());
        assert_eq!(secret_exposed_to_tasks(&opts, blank), None);
        let file_env = |n: &str| (n == ENV_SECRET_FILE).then(|| "/x/secret".to_string());
        assert_eq!(secret_exposed_to_tasks(&opts, file_env), None);
        opts.value = Some(SecretText("ab".repeat(32)));
        assert!(secret_exposed_to_tasks(&opts, none)
            .unwrap()
            .contains("--secret"));
        let msg = exposed_secret_error("--secret");
        assert!(
            msg.contains("--secret-file") && msg.contains("OXIDESWARM_SECRET_FILE"),
            "{msg}"
        );
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::config::{MasterFileConfig, SubmitFileConfig, WorkerFileConfig};
    use std::collections::HashMap;

    const HEX_A: &str = "aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa";
    const HEX_B: &str = "bbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbb";
    const HEX_C: &str = "cccccccccccccccccccccccccccccccccccccccccccccccccccccccccccccccc";
    const HEX_D: &str = "dddddddddddddddddddddddddddddddddddddddddddddddddddddddddddddddd";
    const HEX_E: &str = "eeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeee";
    const HEX_F: &str = "ffffffffffffffffffffffffffffffffffffffffffffffffffffffffffffffff";

    fn secret_of(hex: &str) -> ClusterSecret {
        ClusterSecret::from_hex(hex).unwrap()
    }

    fn env_of(pairs: &[(&str, &str)]) -> impl Fn(&str) -> Option<String> {
        let map: HashMap<String, String> = pairs
            .iter()
            .map(|(k, v)| (k.to_string(), v.to_string()))
            .collect();
        move |name| map.get(name).cloned()
    }

    fn write_secret_file(dir: &Path, name: &str, hex: &str) -> PathBuf {
        let path = dir.join(name);
        std::fs::write(&path, format!("{hex}\n")).unwrap();
        path
    }

    /// Config with every tier populated, so each test peels one tier off.
    fn full_config(dir: &Path) -> ConfigFile {
        ConfigFile {
            secret: Some(SecretText(HEX_E.into())),
            secret_file: Some(
                write_secret_file(dir, "cfg-top.secret", HEX_F)
                    .display()
                    .to_string(),
            ),
            ..Default::default()
        }
    }

    // --- resolution precedence --------------------------------------------------------------

    #[test]
    fn test_resolve_precedence_walks_every_tier_in_order() {
        let dir = tempfile::tempdir().unwrap();
        let cli_file = write_secret_file(dir.path(), "cli.secret", HEX_B);
        let env_file = write_secret_file(dir.path(), "env.secret", HEX_D);
        let default_file = write_secret_file(dir.path(), "default.secret", HEX_A);
        let config = ConfigFile {
            secret: Some(SecretText(HEX_E.into())),
            ..Default::default()
        };
        let env_all = env_of(&[
            (ENV_SECRET, HEX_C),
            (ENV_SECRET_FILE, &env_file.display().to_string()),
        ]);
        let empty_env = env_of(&[]);
        let env_file_only = env_of(&[(ENV_SECRET_FILE, &env_file.display().to_string())]);
        let resolve =
            |opts: &SecretOpts, cfg: Option<&ConfigFile>, env: &dyn Fn(&str) -> Option<String>| {
                opts.resolve_with(cfg, SecretSection::Worker, env, Some(default_file.clone()))
                    .unwrap()
            };

        // 1. --secret beats everything.
        let opts = SecretOpts {
            value: Some(SecretText(HEX_A.replace('a', "1"))),
            file: Some(cli_file.clone()),
        };
        assert_eq!(
            resolve(&opts, Some(&config), &env_all),
            Some(secret_of(&"1".repeat(64)))
        );
        // 2. --secret-file beats the environment, config and default file.
        let opts = SecretOpts {
            value: None,
            file: Some(cli_file),
        };
        assert_eq!(
            resolve(&opts, Some(&config), &env_all),
            Some(secret_of(HEX_B))
        );
        // 3. OXIDESWARM_SECRET beats OXIDESWARM_SECRET_FILE, config and default file.
        let none = SecretOpts::default();
        assert_eq!(
            resolve(&none, Some(&config), &env_all),
            Some(secret_of(HEX_C))
        );
        // 4. OXIDESWARM_SECRET_FILE beats config and default file.
        assert_eq!(
            resolve(&none, Some(&config), &env_file_only),
            Some(secret_of(HEX_D))
        );
        // 5. The config file beats the default file.
        assert_eq!(
            resolve(&none, Some(&config), &empty_env),
            Some(secret_of(HEX_E))
        );
        // 6. The default file is the last resort ...
        assert_eq!(resolve(&none, None, &empty_env), Some(secret_of(HEX_A)));
        // ... and without it there is no secret.
        assert_eq!(
            none.resolve_with(None, SecretSection::Worker, env_of(&[]), None)
                .unwrap(),
            None
        );
    }

    #[test]
    fn test_config_value_beats_config_file_and_sections_beat_top_level() {
        let dir = tempfile::tempdir().unwrap();
        let none = SecretOpts::default();

        // Top level: `secret` beats `secret_file`.
        let config = full_config(dir.path());
        assert_eq!(
            none.resolve_with(Some(&config), SecretSection::Master, env_of(&[]), None)
                .unwrap(),
            Some(secret_of(HEX_E))
        );

        // Only `secret_file` set: it is used.
        let config = ConfigFile {
            secret_file: config.secret_file.clone(),
            ..Default::default()
        };
        assert_eq!(
            none.resolve_with(Some(&config), SecretSection::Master, env_of(&[]), None)
                .unwrap(),
            Some(secret_of(HEX_F))
        );

        // A section's own key wins over the top level, only for its own command.
        let config = ConfigFile {
            secret: Some(SecretText(HEX_E.into())),
            worker: Some(WorkerFileConfig {
                secret: Some(SecretText(HEX_C.into())),
                ..Default::default()
            }),
            submit: Some(SubmitFileConfig {
                secret: Some(SecretText(HEX_D.into())),
                ..Default::default()
            }),
            master: Some(MasterFileConfig::default()),
            ..Default::default()
        };
        let resolve = |section| {
            none.resolve_with(Some(&config), section, env_of(&[]), None)
                .unwrap()
        };
        assert_eq!(resolve(SecretSection::Worker), Some(secret_of(HEX_C)));
        assert_eq!(resolve(SecretSection::Submit), Some(secret_of(HEX_D)));
        assert_eq!(resolve(SecretSection::Master), Some(secret_of(HEX_E)));
        assert_eq!(resolve(SecretSection::Status), Some(secret_of(HEX_E)));
    }

    #[test]
    fn test_invalid_source_is_an_error_not_a_silent_fall_through() {
        let dir = tempfile::tempdir().unwrap();
        let default_file = write_secret_file(dir.path(), "default.secret", HEX_A);

        // A malformed --secret must not fall through to the default file.
        let bad = SecretOpts {
            value: Some(SecretText("not-hex".into())),
            file: None,
        };
        assert!(bad
            .resolve_with(
                None,
                SecretSection::Worker,
                env_of(&[]),
                Some(default_file.clone())
            )
            .is_err());

        // Same for a bad env var and a bad config value.
        let none = SecretOpts::default();
        assert!(none
            .resolve_with(
                None,
                SecretSection::Worker,
                env_of(&[(ENV_SECRET, "zz")]),
                Some(default_file.clone())
            )
            .is_err());
        let config = ConfigFile {
            secret: Some(SecretText("short".into())),
            ..Default::default()
        };
        let err = none
            .resolve_with(
                Some(&config),
                SecretSection::Worker,
                env_of(&[]),
                Some(default_file),
            )
            .unwrap_err();
        assert!(
            err.to_string().contains("config file key 'secret'"),
            "{err}"
        );
        // The value itself is never echoed.
        assert!(!err.to_string().contains("short"), "{err}");
    }

    #[test]
    fn test_blank_secret_sources_are_errors_not_an_open_cluster() {
        let dir = tempfile::tempdir().unwrap();
        let default_file = write_secret_file(dir.path(), "default.secret", HEX_A);

        // `--secret ""` (an empty shell expansion) must not fall through to the default file or
        // to "no secret".
        for blank in ["", "   "] {
            let opts = SecretOpts {
                value: Some(SecretText(blank.into())),
                file: None,
            };
            let err = opts
                .resolve_with(
                    None,
                    SecretSection::Worker,
                    env_of(&[]),
                    Some(default_file.clone()),
                )
                .unwrap_err();
            assert!(
                err.to_string().contains("--secret is set but empty"),
                "{err}"
            );
        }

        // Same for blank OXIDESWARM_SECRET / OXIDESWARM_SECRET_FILE.
        let none = SecretOpts::default();
        for var in [ENV_SECRET, ENV_SECRET_FILE] {
            let err = none
                .resolve_with(
                    None,
                    SecretSection::Submit,
                    env_of(&[(var, "")]),
                    Some(default_file.clone()),
                )
                .unwrap_err();
            assert!(err.to_string().contains(var), "{var}: {err}");
        }
    }

    #[test]
    fn test_missing_explicit_secret_file_explains_how_to_create_it() {
        let dir = tempfile::tempdir().unwrap();
        let missing = dir.path().join("nope.secret");
        let opts = SecretOpts {
            value: None,
            file: Some(missing.clone()),
        };
        let err = opts
            .resolve_with(None, SecretSection::Submit, env_of(&[]), None)
            .unwrap_err()
            .to_string();
        assert!(err.contains("does not exist"), "{err}");
        assert!(err.contains("oxideswarm secret generate --out"), "{err}");
        assert!(err.contains(&missing.display().to_string()), "{err}");
    }

    #[test]
    fn test_secret_text_never_prints_its_value() {
        let text = SecretText(HEX_A.into());
        assert!(!format!("{text:?}").contains(HEX_A));
        let opts = SecretOpts {
            value: Some(text),
            file: None,
        };
        assert!(!format!("{opts:?}").contains(HEX_A));
        let config = ConfigFile {
            secret: Some(SecretText(HEX_B.into())),
            ..Default::default()
        };
        assert!(!format!("{config:?}").contains(HEX_B));
    }

    #[test]
    fn test_resolve_master_key_precedence() {
        let config = ConfigFile {
            master_key: Some(HEX_E.into()),
            worker: Some(WorkerFileConfig {
                master_key: Some(HEX_C.into()),
                ..Default::default()
            }),
            ..Default::default()
        };
        let env_set = env_of(&[(ENV_MASTER_KEY, HEX_B)]);
        let empty_env = env_of(&[]);
        let none = SecretOpts::default();

        // 1. --master-key beats env and config.
        assert_eq!(
            none.resolve_master_key_with(
                Some(HEX_A),
                Some(&config),
                SecretSection::Worker,
                &env_set
            )
            .unwrap(),
            Some(PublicKey::from_hex(HEX_A).unwrap())
        );

        // 2. RUSTY_GRID_MASTER_KEY beats config.
        assert_eq!(
            none.resolve_master_key_with(None, Some(&config), SecretSection::Worker, &env_set)
                .unwrap(),
            Some(PublicKey::from_hex(HEX_B).unwrap())
        );

        // 3. Section config beats top-level config.
        assert_eq!(
            none.resolve_master_key_with(None, Some(&config), SecretSection::Worker, &empty_env)
                .unwrap(),
            Some(PublicKey::from_hex(HEX_C).unwrap())
        );
        assert_eq!(
            none.resolve_master_key_with(None, Some(&config), SecretSection::Submit, &empty_env)
                .unwrap(),
            Some(PublicKey::from_hex(HEX_E).unwrap())
        );

        // 4. Default is None.
        assert_eq!(
            none.resolve_master_key_with(None, None, SecretSection::Worker, &empty_env)
                .unwrap(),
            None
        );
    }

    // --- secret generate / show -------------------------------------------------------------

    #[test]
    fn test_generate_writes_a_private_loadable_file_and_refuses_to_overwrite() {
        let dir = tempfile::tempdir().unwrap();
        let path = dir.path().join("nested").join("cluster.secret");

        let written = generate_secret_file(Some(&path), false, None).unwrap();
        assert_eq!(written, path);
        let first = ClusterSecret::load_file(&path).unwrap();
        assert_eq!(first.to_hex().len(), 64);
        #[cfg(unix)]
        {
            use std::os::unix::fs::PermissionsExt;
            let mode = std::fs::metadata(&path).unwrap().permissions().mode() & 0o777;
            assert_eq!(mode, 0o600, "secret files must be private");
        }

        // A second generate without --force must leave the file untouched.
        let err = generate_secret_file(Some(&path), false, None).unwrap_err();
        assert!(err.to_string().contains("--force"), "{err}");
        assert_eq!(ClusterSecret::load_file(&path).unwrap(), first);

        // --force replaces it with a different secret.
        generate_secret_file(Some(&path), true, None).unwrap();
        assert_ne!(ClusterSecret::load_file(&path).unwrap(), first);
    }

    #[test]
    fn test_generate_defaults_to_the_default_path_and_needs_one() {
        let dir = tempfile::tempdir().unwrap();
        let default = dir.path().join(".oxideswarm").join("cluster.secret");
        let written = generate_secret_file(None, false, Some(default.clone())).unwrap();
        assert_eq!(written, default);
        assert!(default.is_file());

        let err = generate_secret_file(None, false, None).unwrap_err();
        assert!(err.to_string().contains("--out"), "{err}");
    }

    #[test]
    fn test_show_renders_the_secret_or_the_derived_dashboard_token() {
        let secret = secret_of(HEX_A);
        assert_eq!(render_secret(&secret, false), HEX_A);

        let token = render_secret(&secret, true);
        assert_eq!(token, rusty_grid_master::dashboard_admin_token(&secret));
        assert_ne!(
            token, HEX_A,
            "the dashboard token must not be the secret itself"
        );
        assert_eq!(token.len(), 64);
    }

    #[test]
    fn test_share_instruction_names_the_path_and_both_ways_to_share() {
        let text = share_instruction(Path::new("/home/me/.oxideswarm/cluster.secret"));
        assert!(text.contains("/home/me/.oxideswarm/cluster.secret"));
        assert!(text.contains("--secret-file"));
        assert!(text.contains(ENV_SECRET));
        assert!(!text.contains('\n'), "one line");
    }

    // --- master policy ----------------------------------------------------------------------

    #[test]
    fn test_policy_uses_a_resolved_secret_even_when_loopback_or_insecure() {
        for (exposed, insecure) in [(false, false), (true, false), (true, true), (false, true)] {
            let out =
                decide_master_secret(Some(secret_of(HEX_A)), exposed, insecure, None).unwrap();
            assert!(matches!(out, MasterSecretOutcome::Configured(_)));
            assert_eq!(out.secret(), Some(&secret_of(HEX_A)));
        }
    }

    #[test]
    fn test_policy_loopback_master_needs_no_secret_and_writes_nothing() {
        let dir = tempfile::tempdir().unwrap();
        let default = dir.path().join(".oxideswarm").join("cluster.secret");
        let out = decide_master_secret(None, false, false, Some(default.clone())).unwrap();
        assert!(matches!(out, MasterSecretOutcome::OpenLoopbackOnly));
        assert!(out.secret().is_none());
        assert!(
            !default.exists(),
            "a loopback master must not create a secret file"
        );
        assert!(!default.parent().unwrap().exists());
    }

    #[test]
    fn test_policy_exposed_master_generates_the_default_secret_file() {
        let dir = tempfile::tempdir().unwrap();
        let default = dir.path().join(".oxideswarm").join("cluster.secret");
        let out = decide_master_secret(None, true, false, Some(default.clone())).unwrap();
        match &out {
            MasterSecretOutcome::Generated { secret, path } => {
                assert_eq!(path, &default);
                assert_eq!(&ClusterSecret::load_file(&default).unwrap(), secret);
            }
            other => panic!("expected Generated, got {other:?}"),
        }
        assert!(out.secret().is_some());

        // The next start resolves that file instead of generating another one.
        let resolved = SecretOpts::default()
            .resolve_with(
                None,
                SecretSection::Master,
                env_of(&[]),
                Some(default.clone()),
            )
            .unwrap();
        assert_eq!(resolved.as_ref(), out.secret());
    }

    #[test]
    fn test_policy_insecure_flag_allows_an_exposed_master_without_creating_files() {
        let dir = tempfile::tempdir().unwrap();
        let default = dir.path().join(".oxideswarm").join("cluster.secret");
        let out = decide_master_secret(None, true, true, Some(default.clone())).unwrap();
        assert!(matches!(out, MasterSecretOutcome::OpenInsecure));
        assert!(out.secret().is_none());
        assert!(!default.exists());
    }

    // --- worker / client policy -------------------------------------------------------------

    fn peer(
        secret: Option<&ClusterSecret>,
        master: &str,
        ticket: bool,
        insecure: bool,
    ) -> GridResult<PeerSecretOutcome> {
        decide_peer_secret(secret, master, ticket, insecure, PeerRole::Worker)
    }

    #[test]
    fn test_peer_policy_uses_a_resolved_secret_everywhere() {
        let secret = secret_of(HEX_A);
        for master in ["127.0.0.1:8088", "10.0.0.7:8088", "auto", ""] {
            for ticket in [false, true] {
                for insecure in [false, true] {
                    assert_eq!(
                        peer(Some(&secret), master, ticket, insecure).unwrap(),
                        PeerSecretOutcome::Authenticated,
                        "{master:?} ticket={ticket} insecure={insecure}"
                    );
                }
            }
        }
    }

    #[test]
    fn test_peer_policy_a_master_on_this_machine_needs_no_secret() {
        for master in ["127.0.0.1:8088", "localhost", "[::1]:8088", "127.0.0.1:0"] {
            assert_eq!(
                peer(None, master, false, false).unwrap(),
                PeerSecretOutcome::OpenLocalOnly,
                "{master:?}"
            );
        }
    }

    #[test]
    fn test_peer_policy_is_fail_closed_for_remote_auto_and_p2p_masters() {
        for (master, ticket) in [
            ("192.0.2.10:8088", false),
            ("192.0.2.10", false),
            ("mac-studio.local:8088", false),
            ("auto", false),
            ("auto:8090", false),
            ("", false),
            ("127.0.0.1:8088", true), // a ticket outranks the loopback --master next to it
        ] {
            let err = peer(None, master, ticket, false)
                .expect_err(&format!("{master:?} ticket={ticket} must be refused"));
            assert!(matches!(err, GridError::AuthRequired(_)), "{err:?}");
            assert_eq!(err.exit_code(), 6);
            let text = err.to_string();
            assert!(text.contains("--insecure"), "{text}");
            assert!(text.contains("--secret-file"), "{text}");
            assert!(text.contains(ENV_SECRET), "{text}");
        }

        // The refusal names what is being trusted, and the role changes the stakes described.
        let remote = peer(None, "10.0.0.7:8088", false, false)
            .unwrap_err()
            .to_string();
        assert!(remote.contains("the master at 10.0.0.7:8088"), "{remote}");
        assert!(
            remote.contains("run arbitrary commands on this machine"),
            "{remote}"
        );
        let auto = peer(None, "auto", false, false).unwrap_err().to_string();
        assert!(auto.contains("auto-discovery"), "{auto}");
        let p2p = peer(None, "127.0.0.1:8088", true, false)
            .unwrap_err()
            .to_string();
        assert!(p2p.contains("P2P ticket"), "{p2p}");
        let client = decide_peer_secret(None, "10.0.0.7:8088", false, false, PeerRole::Client)
            .unwrap_err()
            .to_string();
        assert!(
            client.contains("environment and input of your tasks"),
            "{client}"
        );
    }

    #[test]
    fn test_peer_policy_insecure_flag_opts_in_explicitly() {
        for (master, ticket) in [
            ("10.0.0.7:8088", false),
            ("auto", false),
            ("127.0.0.1:1", true),
        ] {
            assert_eq!(
                peer(None, master, ticket, true).unwrap(),
                PeerSecretOutcome::OpenInsecure,
                "{master:?}"
            );
        }
    }

    #[test]
    fn test_peer_announce_handles_every_outcome() {
        // Only the insecure outcome prints (to stderr); the others must be silent no-ops.
        for outcome in [
            PeerSecretOutcome::Authenticated,
            PeerSecretOutcome::OpenLocalOnly,
            PeerSecretOutcome::OpenInsecure,
        ] {
            for master in ["10.0.0.7:8088", "", "auto"] {
                announce_peer_secret(outcome, PeerRole::Client, master);
                announce_peer_secret(outcome, PeerRole::Worker, master);
            }
        }
    }

    #[test]
    fn test_policy_exposed_master_without_a_home_directory_fails_clearly() {
        let err = decide_master_secret(None, true, false, None)
            .unwrap_err()
            .to_string();
        assert!(err.contains("--secret-file"), "{err}");
        assert!(err.contains("--insecure"), "{err}");
    }
}
