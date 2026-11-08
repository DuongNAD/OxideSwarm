//! Join a machine to a cluster with one token (JOIN-1, design record
//! `docs/design/0048-join-token.md`).
//!
//! `oxideswarm secret join-token` (master side) prints one string that carries the master's address,
//! optionally its P2P ticket, and the cluster secret. `oxideswarm join` (new machine) reads that
//! string from standard input, stores the secret in `~/.oxideswarm/cluster.secret`, saves the
//! address (and ticket) in `~/.oxideswarm/rusty-grid.toml`, and runs the `doctor` checks.
//!
//! The token is as sensitive as the secret. Nothing in this module logs, prints or formats a token
//! or a secret: errors describe what is wrong, never the text.

use std::io::{IsTerminal, Read};
use std::path::{Path, PathBuf};
use std::process::ExitCode;
use std::time::Duration;

use base64::engine::general_purpose::URL_SAFE_NO_PAD;
use base64::Engine;
use clap::Parser;
use serde::{Deserialize, Serialize};

use rusty_grid_core::cluster_auth::ClusterSecret;
use rusty_grid_core::error::{GridError, GridResult};

use crate::config::default_oxideswarm_dir;
use crate::connect::{normalize_master_addr, AUTH_EXIT_CODE};
use crate::doctor::{run_doctor, DoctorOptions};

/// Every join token starts with this prefix (`oxs1` = version 1 of the OxideSwarm secret formats).
pub const TOKEN_PREFIX: &str = "oxs1-join:";
/// Label of the integrity tag: `HMAC-SHA256(secret, TAG_LABEL || payload text)`.
const TAG_LABEL: &str = "oxs1-join-token:";
/// Bytes of the tag kept in the token (rendered as hex).
const TAG_BYTES: usize = 16;
/// The longest token `oxideswarm join` reads from standard input.
const MAX_TOKEN_BYTES: u64 = 8 * 1024;
/// Longest address or ticket a token may carry.
const MAX_FIELD_LEN: usize = 2048;
/// File the address is saved in, inside `~/.oxideswarm`.
const CONFIG_FILE_NAME: &str = "rusty-grid.toml";

/// What a token carries.
pub struct JoinToken {
    /// Address of the master's grid port (`host:port`), if the token has one.
    pub master: Option<String>,
    /// P2P connection ticket of the master, if the token has one.
    pub ticket: Option<String>,
    pub secret: ClusterSecret,
}

impl std::fmt::Debug for JoinToken {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("JoinToken")
            .field("master", &self.master)
            .field("ticket", &self.ticket.as_ref().map(|_| "<present>"))
            .field("secret", &"<redacted>")
            .finish()
    }
}

#[derive(Serialize, Deserialize)]
struct Payload {
    /// Master address.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    m: Option<String>,
    /// P2P ticket.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    t: Option<String>,
    /// Secret as hex.
    s: String,
}

/// Why a token was refused. The texts never contain the token.
#[derive(Debug, PartialEq, Eq)]
pub enum TokenError {
    NotAToken,
    Corrupt,
    Invalid(&'static str),
}

impl std::fmt::Display for TokenError {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            TokenError::NotAToken => write!(f, "this is not a join token (it must start with {TOKEN_PREFIX})"),
            TokenError::Corrupt => f.write_str(
                "the join token is damaged: its integrity check failed (a character was changed, lost or added while copying). Create it again with `oxideswarm secret join-token` and copy it whole",
            ),
            TokenError::Invalid(why) => write!(f, "the join token is invalid: {why}"),
        }
    }
}

fn tag_of(secret: &ClusterSecret, payload_text: &str) -> String {
    let full = secret.derive(&format!("{TAG_LABEL}{payload_text}"));
    full[..TAG_BYTES]
        .iter()
        .map(|b| format!("{b:02x}"))
        .collect()
}

fn field_ok(value: &str) -> bool {
    !value.is_empty() && value.len() <= MAX_FIELD_LEN && !value.chars().any(char::is_whitespace)
}

impl JoinToken {
    /// The token text: `oxs1-join:<base64url payload>.<hex tag>`.
    pub fn encode(&self) -> String {
        let payload = Payload {
            m: self.master.clone(),
            t: self.ticket.clone(),
            s: self.secret.to_hex(),
        };
        let json = serde_json::to_vec(&payload).expect("a payload of strings serializes");
        let text = URL_SAFE_NO_PAD.encode(json);
        let tag = tag_of(&self.secret, &text);
        format!("{TOKEN_PREFIX}{text}.{tag}")
    }

    /// Parses and verifies a token. Surrounding whitespace is ignored.
    pub fn decode(token: &str) -> Result<Self, TokenError> {
        let body = token
            .trim()
            .strip_prefix(TOKEN_PREFIX)
            .ok_or(TokenError::NotAToken)?;
        let (text, tag) = body.rsplit_once('.').ok_or(TokenError::Corrupt)?;
        let json = URL_SAFE_NO_PAD
            .decode(text)
            .map_err(|_| TokenError::Corrupt)?;
        let payload: Payload = serde_json::from_slice(&json).map_err(|_| TokenError::Corrupt)?;
        let secret = ClusterSecret::from_hex(&payload.s).map_err(|_| TokenError::Corrupt)?;
        // Not a secret-dependent branch worth hiding (the key is inside the token), but compare
        // without early exit anyway.
        let expected = tag_of(&secret, text);
        let same = expected.len() == tag.len()
            && expected
                .bytes()
                .zip(tag.bytes())
                .fold(0u8, |acc, (a, b)| acc | (a ^ b))
                == 0;
        if !same {
            return Err(TokenError::Corrupt);
        }
        if payload.m.is_none() && payload.t.is_none() {
            return Err(TokenError::Invalid(
                "it has neither a master address nor a P2P ticket",
            ));
        }
        if payload.m.as_deref().is_some_and(|m| !field_ok(m)) {
            return Err(TokenError::Invalid("the master address is malformed"));
        }
        if payload.t.as_deref().is_some_and(|t| !field_ok(t)) {
            return Err(TokenError::Invalid("the P2P ticket is malformed"));
        }
        Ok(Self {
            master: payload.m,
            ticket: payload.t,
            secret,
        })
    }
}

/// Reads at most [`MAX_TOKEN_BYTES`] from `reader` and returns the trimmed text. Never echoes it.
pub fn read_token(reader: impl Read) -> GridResult<String> {
    let mut text = String::new();
    reader
        .take(MAX_TOKEN_BYTES + 1)
        .read_to_string(&mut text)
        .map_err(|e| {
            GridError::Config(format!(
                "cannot read the join token from standard input: {e}"
            ))
        })?;
    if text.len() as u64 > MAX_TOKEN_BYTES {
        return Err(GridError::Config(
            "standard input is too long to be a join token".into(),
        ));
    }
    let text = text.trim().to_string();
    if text.is_empty() {
        return Err(GridError::Config(
            "standard input is empty: pipe the join token into this command".into(),
        ));
    }
    Ok(text)
}

/// Saves the master address and ticket in the TOML config file `path`, keeping its other keys
/// (comments are not kept). Returns whether the file changed. A file that is not valid TOML is
/// refused and left alone.
///
/// * `[worker] master`, `[submit] master` and `[status] master` get the address.
/// * `[worker] p2p_ticket` gets the ticket; without one the key is removed, because a stale ticket
///   from an earlier master would otherwise win over the new address.
pub fn save_target(path: &Path, master: Option<&str>, ticket: Option<&str>) -> GridResult<bool> {
    let cfg_err = |what: &str, e: &dyn std::fmt::Display| {
        GridError::Config(format!("{what} '{}': {e}", path.display()))
    };
    let existing = match std::fs::read_to_string(path) {
        Ok(text) => Some(text),
        Err(e) if e.kind() == std::io::ErrorKind::NotFound => None,
        Err(e) => return Err(cfg_err("cannot read config file", &e)),
    };
    let mut table: toml::Table = match &existing {
        Some(text) => toml::from_str(text).map_err(|e| {
            GridError::Config(format!(
                "config file '{}' is not valid TOML ({e}); fix or move it, then join again",
                path.display()
            ))
        })?,
        None => toml::Table::new(),
    };
    let before = table.clone();
    for name in ["worker", "submit", "status"] {
        section_of(&mut table, name, path)?;
    }
    if let Some(master) = master {
        for name in ["worker", "submit", "status"] {
            section_of(&mut table, name, path)?
                .insert("master".into(), toml::Value::String(master.to_string()));
        }
    }
    let worker = section_of(&mut table, "worker", path)?;
    match ticket {
        Some(t) => {
            worker.insert("p2p_ticket".into(), toml::Value::String(t.to_string()));
        }
        None => {
            worker.remove("p2p_ticket");
        }
    }
    if table == before && existing.is_some() {
        return Ok(false);
    }
    if let Some(parent) = path.parent().filter(|p| !p.as_os_str().is_empty()) {
        std::fs::create_dir_all(parent)
            .map_err(|e| cfg_err("cannot create the directory of", &e))?;
    }
    let text =
        toml::to_string_pretty(&table).map_err(|e| cfg_err("cannot render config file", &e))?;
    let tmp = path.with_extension(format!("toml.{}.tmp", std::process::id()));
    std::fs::write(&tmp, text)
        .and_then(|()| std::fs::rename(&tmp, path))
        .map_err(|e| {
            let _ = std::fs::remove_file(&tmp);
            cfg_err("cannot write config file", &e)
        })?;
    Ok(true)
}

fn section_of<'a>(
    table: &'a mut toml::Table,
    name: &str,
    path: &Path,
) -> GridResult<&'a mut toml::Table> {
    table
        .entry(name)
        .or_insert_with(|| toml::Value::Table(toml::Table::new()))
        .as_table_mut()
        .ok_or_else(|| {
            GridError::Config(format!(
                "config file '{}' has a `{name}` key that is not a table",
                path.display()
            ))
        })
}

/// What [`store_secret`] did.
#[derive(Debug, PartialEq, Eq)]
pub enum SecretStored {
    Created,
    AlreadyPresent,
    Replaced,
}

/// Puts `secret` into the secret file `path`. A different existing secret (or an unreadable file)
/// is replaced only with `replace`.
pub fn store_secret(
    path: &Path,
    secret: &ClusterSecret,
    replace: bool,
) -> GridResult<SecretStored> {
    if !path.exists() {
        secret.save_file(path, false)?;
        return Ok(SecretStored::Created);
    }
    if ClusterSecret::load_file(path).is_ok_and(|existing| existing.to_hex() == secret.to_hex()) {
        return Ok(SecretStored::AlreadyPresent);
    }
    if !replace {
        return Err(GridError::Config(format!(
            "'{}' already holds a different cluster secret (or one that cannot be read). Joining would cut this machine off from the cluster it uses now. If that is what you want, run `oxideswarm join --replace-secret`",
            path.display()
        )));
    }
    secret.save_file(path, true)?;
    Ok(SecretStored::Replaced)
}

/// `oxideswarm join`: reads a join token from standard input.
#[derive(Parser, Debug, Clone)]
pub struct JoinArgs {
    #[arg(
        long,
        help = "Replace a different cluster secret that this machine already has"
    )]
    pub replace_secret: bool,

    #[arg(
        long,
        value_name = "N",
        default_value = "0",
        help = "The doctor check fails unless at least N workers are connected (default 0: this machine's worker usually starts after joining)"
    )]
    pub expect_workers: usize,

    #[arg(
        long,
        help = "Save the secret and the address but do not run the doctor checks"
    )]
    pub no_doctor: bool,

    /// Catches a token typed as an argument so that it can be refused without being echoed back
    /// (clap would print an unexpected argument in its error message).
    #[arg(hide = true, num_args = 0.., value_name = "TOKEN")]
    pub refused_argument: Vec<String>,
}

/// Runs `oxideswarm join`.
pub async fn run_join(args: JoinArgs) -> ExitCode {
    if !args.refused_argument.is_empty() {
        eprintln!("A join token must not be given as an argument: it would stay in your shell history and be visible in the process list. Pipe it in instead, for example `pbpaste | oxideswarm join`, or `ssh master oxideswarm secret join-token | oxideswarm join`.");
        return ExitCode::FAILURE;
    }
    let stdin = std::io::stdin();
    if stdin.is_terminal() {
        eprintln!("Standard input is a terminal, where a pasted token would be shown on screen. Pipe the token in instead, for example `pbpaste | oxideswarm join`.");
        return ExitCode::FAILURE;
    }
    let text = match read_token(stdin.lock()) {
        Ok(text) => text,
        Err(e) => {
            eprintln!("{e}");
            return ExitCode::FAILURE;
        }
    };
    let token = match JoinToken::decode(&text) {
        Ok(token) => token,
        Err(e) => {
            eprintln!("{e}");
            return ExitCode::FAILURE;
        }
    };
    drop(text);

    let Some(secret_path) = ClusterSecret::default_path() else {
        eprintln!("cannot determine the home directory for ~/.oxideswarm/cluster.secret");
        return ExitCode::FAILURE;
    };
    let config_path = default_oxideswarm_dir()
        .map(|d| d.join(CONFIG_FILE_NAME))
        .unwrap_or_else(|| PathBuf::from(CONFIG_FILE_NAME));
    let master = token.master.as_deref().map(normalize_master_addr);

    // Settle the secret first: a refusal must leave the machine untouched.
    match store_secret(&secret_path, &token.secret, args.replace_secret) {
        Ok(SecretStored::Created) => println!("Cluster secret saved to {}", secret_path.display()),
        Ok(SecretStored::AlreadyPresent) => println!(
            "{} already holds this cluster's secret; left as it is",
            secret_path.display()
        ),
        Ok(SecretStored::Replaced) => println!(
            "The cluster secret in {} was replaced",
            secret_path.display()
        ),
        Err(e) => {
            eprintln!("{e}");
            return ExitCode::FAILURE;
        }
    }
    match save_target(&config_path, master.as_deref(), token.ticket.as_deref()) {
        Ok(true) => println!("Master address saved to {}", config_path.display()),
        Ok(false) => println!("{} already names this master", config_path.display()),
        Err(e) => {
            eprintln!("{e}");
            return ExitCode::FAILURE;
        }
    }
    if let Some(master) = &master {
        println!("Master: {master}");
    }
    if token.ticket.is_some() {
        println!("The master's P2P ticket was saved as the worker's `p2p_ticket`.");
    }
    println!("Start a worker here with: oxideswarm worker");

    let Some(master) = master else {
        println!(
            "The token has no master address, so the doctor checks were skipped (they need one)."
        );
        return ExitCode::SUCCESS;
    };
    if args.no_doctor {
        return ExitCode::SUCCESS;
    }
    println!("Checking the cluster ...");
    let options = DoctorOptions {
        expect_workers: args.expect_workers,
        probe_timeout: Duration::from_secs(60),
        ..DoctorOptions::default()
    };
    let report = run_doctor(&master, Some(&token.secret), &options).await;
    print!("{}", report.render());
    if report.auth_failed {
        ExitCode::from(AUTH_EXIT_CODE)
    } else if report.failed() {
        ExitCode::FAILURE
    } else {
        ExitCode::SUCCESS
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn secret() -> ClusterSecret {
        ClusterSecret::from_hex(&"ab".repeat(32)).unwrap()
    }

    fn token() -> JoinToken {
        JoinToken {
            master: Some("192.168.1.5:8088".into()),
            ticket: Some("ticketvalue".into()),
            secret: secret(),
        }
    }

    #[test]
    fn test_round_trip_keeps_every_field() {
        let text = token().encode();
        assert!(text.starts_with(TOKEN_PREFIX));
        assert!(
            !text.contains(&secret().to_hex()),
            "the hex is not visible in the token"
        );
        let back = JoinToken::decode(&format!("  {text}\n")).unwrap();
        assert_eq!(back.master.as_deref(), Some("192.168.1.5:8088"));
        assert_eq!(back.ticket.as_deref(), Some("ticketvalue"));
        assert_eq!(back.secret.to_hex(), secret().to_hex());
    }

    #[test]
    fn test_every_single_character_change_is_refused() {
        let text = token().encode();
        let start = TOKEN_PREFIX.len();
        for i in 0..text.len() {
            let mut bytes = text.clone().into_bytes();
            bytes[i] = if bytes[i] == b'A' { b'B' } else { b'A' };
            let changed = String::from_utf8(bytes).unwrap();
            let result = JoinToken::decode(&changed);
            if i < start {
                assert_eq!(result.err(), Some(TokenError::NotAToken), "position {i}");
            } else {
                assert!(result.is_err(), "a change at position {i} was accepted");
            }
        }
        assert!(
            JoinToken::decode(&text[..text.len() - 1]).is_err(),
            "truncated"
        );
        assert!(JoinToken::decode(&format!("{text}0")).is_err(), "extended");
    }

    #[test]
    fn test_a_token_without_a_target_or_with_a_bad_field_is_invalid() {
        let mut t = token();
        t.master = None;
        t.ticket = None;
        assert!(matches!(
            JoinToken::decode(&t.encode()),
            Err(TokenError::Invalid(_))
        ));
        let mut t = token();
        t.master = Some("has space:8088".into());
        assert!(matches!(
            JoinToken::decode(&t.encode()),
            Err(TokenError::Invalid(_))
        ));
        let mut t = token();
        t.master = None;
        assert!(
            JoinToken::decode(&t.encode()).is_ok(),
            "a ticket alone is a target"
        );
    }

    #[test]
    fn test_errors_and_debug_never_show_the_token_or_secret() {
        let text = token().encode();
        let shown = format!("{:?}", token());
        assert!(!shown.contains(&secret().to_hex()) && !shown.contains("ticketvalue"));
        let mut changed = text.clone().into_bytes();
        let last = changed.len() - 1;
        changed[last] = if changed[last] == b'0' { b'1' } else { b'0' };
        let err = JoinToken::decode(&String::from_utf8(changed).unwrap())
            .unwrap_err()
            .to_string();
        assert!(!err.contains(&text[TOKEN_PREFIX.len()..]) && !err.contains(&secret().to_hex()));
    }

    #[test]
    fn test_read_token_trims_and_bounds_its_input() {
        assert_eq!(read_token("  abc\n".as_bytes()).unwrap(), "abc");
        assert!(read_token("  \n".as_bytes()).is_err());
        let long = "x".repeat(MAX_TOKEN_BYTES as usize + 1);
        assert!(read_token(long.as_bytes()).is_err());
    }

    #[test]
    fn test_store_secret_creates_keeps_and_refuses_to_replace() {
        let dir = tempfile::tempdir().unwrap();
        let path = dir.path().join("sub").join("cluster.secret");
        assert_eq!(
            store_secret(&path, &secret(), false).unwrap(),
            SecretStored::Created
        );
        assert_eq!(
            store_secret(&path, &secret(), false).unwrap(),
            SecretStored::AlreadyPresent
        );
        let other = ClusterSecret::from_hex(&"cd".repeat(32)).unwrap();
        let err = store_secret(&path, &other, false).unwrap_err().to_string();
        assert!(err.contains("--replace-secret"));
        assert_eq!(
            ClusterSecret::load_file(&path).unwrap().to_hex(),
            secret().to_hex()
        );
        assert_eq!(
            store_secret(&path, &other, true).unwrap(),
            SecretStored::Replaced
        );
        assert_eq!(
            ClusterSecret::load_file(&path).unwrap().to_hex(),
            other.to_hex()
        );
    }

    #[test]
    fn test_save_target_merges_and_drops_a_stale_ticket() {
        let dir = tempfile::tempdir().unwrap();
        let path = dir.path().join("rusty-grid.toml");
        std::fs::write(
            &path,
            "[worker]\nname = \"box\"\np2p_ticket = \"old\"\n\n[master]\nlisten = \"0.0.0.0:8088\"\n",
        )
        .unwrap();
        assert!(save_target(&path, Some("10.0.0.2:8088"), None).unwrap());
        let cfg: crate::config::ConfigFile =
            toml::from_str(&std::fs::read_to_string(&path).unwrap()).unwrap();
        let worker = cfg.worker.unwrap();
        assert_eq!(worker.master.as_deref(), Some("10.0.0.2:8088"));
        assert_eq!(worker.name.as_deref(), Some("box"));
        assert_eq!(
            worker.p2p_ticket, None,
            "a stale ticket would win over the new address"
        );
        assert_eq!(cfg.submit.unwrap().master.as_deref(), Some("10.0.0.2:8088"));
        assert_eq!(cfg.status.unwrap().master.as_deref(), Some("10.0.0.2:8088"));
        assert_eq!(cfg.master.unwrap().listen.as_deref(), Some("0.0.0.0:8088"));
        assert!(
            !save_target(&path, Some("10.0.0.2:8088"), None).unwrap(),
            "unchanged"
        );
        assert!(save_target(&path, Some("10.0.0.2:8088"), Some("tk")).unwrap());
    }

    #[test]
    fn test_save_target_refuses_a_file_that_is_not_toml() {
        let dir = tempfile::tempdir().unwrap();
        let path = dir.path().join("rusty-grid.toml");
        std::fs::write(&path, "this is = = not toml").unwrap();
        assert!(save_target(&path, Some("h:1"), None).is_err());
        assert_eq!(
            std::fs::read_to_string(&path).unwrap(),
            "this is = = not toml"
        );
    }
}
