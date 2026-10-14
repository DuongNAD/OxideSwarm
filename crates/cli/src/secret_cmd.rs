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

use crate::config::{ConfigFile, SecretSection, SecretText};
use crate::connect::master_requires_secret;

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
}

fn config_key_error(key: &str, err: &GridError) -> GridError {
    let detail = match err {
        GridError::Config(msg) => msg.clone(),
        other => other.to_string(),
    };
    GridError::Config(format!("config file key '{key}': {detail}"))
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
    if secret.is_some() {
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
            ("192.168.1.144:8088", false),
            ("192.168.1.144", false),
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
