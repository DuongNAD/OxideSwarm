//! 4-tier Hierarchical Configuration Engine for `rusty-grid`.
//!
//! Precedence order (highest to lowest):
//! 1. CLI Arguments & Flags
//! 2. Environment Variables (`RUSTY_GRID_*`)
//! 3. Configuration File (`--config <path>`, `RUSTY_GRID_CONFIG`, or `./rusty-grid.{toml,json}`)
//! 4. Hardcoded System Defaults

use rusty_grid_core::error::{GridError, GridResult};
use serde::{Deserialize, Serialize};
use std::path::{Path, PathBuf};

/// Top-level configuration file schema supporting TOML and JSON.
#[derive(Debug, Clone, Default, Serialize, Deserialize, PartialEq)]
pub struct ConfigFile {
    #[serde(default)]
    pub master: Option<MasterFileConfig>,
    #[serde(default)]
    pub worker: Option<WorkerFileConfig>,
    #[serde(default)]
    pub submit: Option<SubmitFileConfig>,
    #[serde(default)]
    pub status: Option<StatusFileConfig>,
}

#[derive(Debug, Clone, Default, Serialize, Deserialize, PartialEq)]
pub struct MasterFileConfig {
    pub listen: Option<String>,
    pub port_file: Option<String>,
    pub heartbeat_timeout_secs: Option<u64>,
    pub reaper_interval_secs: Option<u64>,
    pub heartbeat_interval_secs: Option<u64>,
    pub max_queue_size: Option<usize>,
    pub default_retry_max: Option<u32>,
    pub scheduler_policy: Option<String>,
    pub max_host_cpu_pct: Option<f32>,
    pub preserve_gpu: Option<bool>,
    pub p2p: Option<bool>,
    pub p2p_ticket_file: Option<String>,
    pub p2p_key_file: Option<String>,
    pub wire_codec: Option<String>,
    pub dashboard_port: Option<u16>,
    pub dashboard_port_file: Option<String>,
}

#[derive(Debug, Clone, Default, Serialize, Deserialize, PartialEq)]
pub struct WorkerHardwareConfig {
    pub cores: Option<usize>,
    pub ram_mb: Option<u64>,
    pub gpu: Option<bool>,
    pub no_gpu: Option<bool>,
    pub simulate_gpu: Option<bool>,
    pub gpu_name: Option<String>,
}

#[derive(Debug, Clone, Default, Serialize, Deserialize, PartialEq)]
pub struct WorkerFileConfig {
    pub master: Option<String>,
    pub p2p_ticket: Option<String>,
    pub name: Option<String>,
    pub heartbeat_interval_secs: Option<u64>,
    pub max_concurrency: Option<usize>,
    pub sandbox_base_dir: Option<String>,
    pub keep_sandboxes: Option<bool>,
    pub cores: Option<usize>,
    pub ram_mb: Option<u64>,
    pub gpu: Option<bool>,
    pub no_gpu: Option<bool>,
    pub simulate_gpu: Option<bool>,
    pub gpu_name: Option<String>,
    pub wire_codec: Option<String>,
    #[serde(default)]
    pub hardware: Option<WorkerHardwareConfig>,
}

#[derive(Debug, Clone, Default, Serialize, Deserialize, PartialEq)]
pub struct SubmitFileConfig {
    pub master: Option<String>,
    pub timeout_secs: Option<u64>,
}

#[derive(Debug, Clone, Default, Serialize, Deserialize, PartialEq)]
pub struct StatusFileConfig {
    pub master: Option<String>,
}

/// Returns the platform-specific default OxideSwarm directory:
/// Unix / macOS: ~/.oxideswarm
/// Windows: %USERPROFILE%\.oxideswarm
pub fn default_oxideswarm_dir() -> Option<PathBuf> {
    #[cfg(windows)]
    {
        std::env::var_os("USERPROFILE")
            .or_else(|| std::env::var_os("HOME"))
            .map(PathBuf::from)
            .map(|h| h.join(".oxideswarm"))
    }
    #[cfg(not(windows))]
    {
        std::env::var_os("HOME")
            .map(PathBuf::from)
            .map(|h| h.join(".oxideswarm"))
    }
}

/// Attempts to load configuration file from explicit path, environment variable, or standard locations.
pub fn load_config_file(explicit_path: Option<&Path>) -> GridResult<Option<ConfigFile>> {
    let path_to_load = if let Some(p) = explicit_path {
        Some(p.to_path_buf())
    } else if let Ok(env_path) = std::env::var("RUSTY_GRID_CONFIG") {
        Some(PathBuf::from(env_path))
    } else if Path::new("rusty-grid.toml").exists() {
        Some(PathBuf::from("rusty-grid.toml"))
    } else if Path::new("rusty-grid.json").exists() {
        Some(PathBuf::from("rusty-grid.json"))
    } else {
        default_oxideswarm_dir()
            .map(|d| d.join("rusty-grid.toml"))
            .filter(|user_config| user_config.exists())
    };

    let path = match path_to_load {
        Some(p) => p,
        None => return Ok(None),
    };

    if !path.exists() {
        return Err(GridError::Config(format!(
            "Configuration file not found at: {}",
            path.display()
        )));
    }

    let content = std::fs::read_to_string(&path).map_err(|e| {
        GridError::Config(format!(
            "Failed to read configuration file '{}': {e}",
            path.display()
        ))
    })?;

    let is_json = path.extension().and_then(|s| s.to_str()) == Some("json");
    if is_json {
        let parsed: ConfigFile = serde_json::from_str(&content).map_err(|e| {
            GridError::Config(format!(
                "Failed to parse JSON configuration file '{}': {e}",
                path.display()
            ))
        })?;
        Ok(Some(parsed))
    } else {
        // Try TOML first, then fallback to JSON
        match toml::from_str::<ConfigFile>(&content) {
            Ok(parsed) => Ok(Some(parsed)),
            Err(toml_err) => match serde_json::from_str::<ConfigFile>(&content) {
                Ok(parsed) => Ok(Some(parsed)),
                Err(_) => Err(GridError::Config(format!(
                    "Failed to parse configuration file '{}': {toml_err}",
                    path.display()
                ))),
            },
        }
    }
}

/// Generic precedence resolver across CLI > Environment Variables > Config File > Default.
pub fn resolve_field<T: Clone>(
    cli_val: Option<T>,
    env_names: &[&str],
    file_val: Option<T>,
    default_val: T,
    parse_env: impl Fn(&str) -> Option<T>,
) -> T {
    if let Some(v) = cli_val {
        return v;
    }
    for env_name in env_names {
        if let Ok(val_str) = std::env::var(env_name) {
            if let Some(v) = parse_env(&val_str) {
                return v;
            }
        }
    }
    if let Some(v) = file_val {
        return v;
    }
    default_val
}

/// Resolves an optional field (no default value) across CLI > Env > Config.
pub fn resolve_opt_field<T: Clone>(
    cli_val: Option<T>,
    env_names: &[&str],
    file_val: Option<T>,
    parse_env: impl Fn(&str) -> Option<T>,
) -> Option<T> {
    if let Some(v) = cli_val {
        return Some(v);
    }
    for env_name in env_names {
        if let Ok(val_str) = std::env::var(env_name) {
            if let Some(v) = parse_env(&val_str) {
                return Some(v);
            }
        }
    }
    file_val
}

pub fn resolve_string(
    cli: Option<String>,
    env_names: &[&str],
    file_val: Option<String>,
    default: &str,
) -> String {
    resolve_field(cli, env_names, file_val, default.to_string(), |s| {
        Some(s.trim().to_string())
    })
}

pub fn resolve_opt_string(
    cli: Option<String>,
    env_names: &[&str],
    file_val: Option<String>,
) -> Option<String> {
    resolve_opt_field(cli, env_names, file_val, |s| {
        let trimmed = s.trim();
        if trimmed.is_empty() {
            None
        } else {
            Some(trimmed.to_string())
        }
    })
}

pub fn resolve_u64(
    cli: Option<u64>,
    env_names: &[&str],
    file_val: Option<u64>,
    default: u64,
) -> u64 {
    resolve_field(cli, env_names, file_val, default, |s| s.trim().parse().ok())
}

pub fn resolve_u32(
    cli: Option<u32>,
    env_names: &[&str],
    file_val: Option<u32>,
    default: u32,
) -> u32 {
    resolve_field(cli, env_names, file_val, default, |s| s.trim().parse().ok())
}

pub fn resolve_opt_u32(cli: Option<u32>, env_names: &[&str], file_val: Option<u32>) -> Option<u32> {
    resolve_opt_field(cli, env_names, file_val, |s| s.trim().parse().ok())
}

pub fn resolve_usize(
    cli: Option<usize>,
    env_names: &[&str],
    file_val: Option<usize>,
    default: usize,
) -> usize {
    resolve_field(cli, env_names, file_val, default, |s| s.trim().parse().ok())
}

pub fn resolve_opt_usize(
    cli: Option<usize>,
    env_names: &[&str],
    file_val: Option<usize>,
) -> Option<usize> {
    resolve_opt_field(cli, env_names, file_val, |s| s.trim().parse().ok())
}

pub fn resolve_opt_u64(cli: Option<u64>, env_names: &[&str], file_val: Option<u64>) -> Option<u64> {
    resolve_opt_field(cli, env_names, file_val, |s| s.trim().parse().ok())
}

pub fn resolve_opt_u16(cli: Option<u16>, env_names: &[&str], file_val: Option<u16>) -> Option<u16> {
    resolve_opt_field(cli, env_names, file_val, |s| s.trim().parse().ok())
}

pub fn resolve_f32(
    cli: Option<f32>,
    env_names: &[&str],
    file_val: Option<f32>,
    default: f32,
) -> f32 {
    resolve_field(cli, env_names, file_val, default, |s| s.trim().parse().ok())
}

pub fn resolve_bool(
    cli: Option<bool>,
    env_names: &[&str],
    file_val: Option<bool>,
    default: bool,
) -> bool {
    resolve_field(cli, env_names, file_val, default, |s| {
        match s.trim().to_lowercase().as_str() {
            "1" | "true" | "yes" | "on" => Some(true),
            "0" | "false" | "no" | "off" => Some(false),
            _ => None,
        }
    })
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_4_tier_precedence_resolution() {
        let env_key = "RUSTY_GRID_TEST_VAR_PRECEDENCE";
        std::env::remove_var(env_key);

        // Tier 4: Default value applies when nothing else set
        let res1 = resolve_string(None, &[env_key], None, "default_val");
        assert_eq!(res1, "default_val");

        // Tier 3: Config file overrides Default
        let res2 = resolve_string(
            None,
            &[env_key],
            Some("config_val".to_string()),
            "default_val",
        );
        assert_eq!(res2, "config_val");

        // Tier 2: Environment variable overrides Config file
        std::env::set_var(env_key, "env_val");
        let res3 = resolve_string(
            None,
            &[env_key],
            Some("config_val".to_string()),
            "default_val",
        );
        assert_eq!(res3, "env_val");

        // Tier 1: CLI flag overrides Environment variable
        let res4 = resolve_string(
            Some("cli_val".to_string()),
            &[env_key],
            Some("config_val".to_string()),
            "default_val",
        );
        assert_eq!(res4, "cli_val");

        std::env::remove_var(env_key);
    }

    #[test]
    fn test_parse_toml_and_json_config() {
        let toml_str = r#"
[master]
listen = "127.0.0.1:9090"
port_file = "/tmp/port.txt"
heartbeat_interval_secs = 5

[worker]
master = "127.0.0.1:9090"
cores = 4
simulate_gpu = true

[worker.hardware]
cores = 8
gpu_name = "Mock RTX"
"#;
        let parsed_toml: ConfigFile = toml::from_str(toml_str).expect("parse toml");
        let master = parsed_toml.master.unwrap();
        assert_eq!(master.listen.as_deref(), Some("127.0.0.1:9090"));
        assert_eq!(master.heartbeat_interval_secs, Some(5));

        let worker = parsed_toml.worker.unwrap();
        assert_eq!(worker.simulate_gpu, Some(true));
        assert_eq!(
            worker.hardware.unwrap().gpu_name.as_deref(),
            Some("Mock RTX")
        );

        let json_str = r#"{
  "master": {
    "listen": "0.0.0.0:8888",
    "preserve_gpu": false,
    "dashboard_port": 8080,
    "dashboard_port_file": "dashboard.port"
  }
}"#;
        let parsed_json: ConfigFile = serde_json::from_str(json_str).expect("parse json");
        assert_eq!(
            parsed_json.master.as_ref().unwrap().preserve_gpu,
            Some(false)
        );
        assert_eq!(
            parsed_json.master.as_ref().unwrap().dashboard_port,
            Some(8080)
        );
        assert_eq!(
            parsed_json
                .master
                .as_ref()
                .unwrap()
                .dashboard_port_file
                .as_deref(),
            Some("dashboard.port")
        );
    }

    #[test]
    fn test_resolve_opt_u16_cli_precedence() {
        let env_key = "TEST_DASHBOARD_PORT_CLI_PREC";
        std::env::set_var(env_key, "8081");
        let res = resolve_opt_u16(Some(8080), &[env_key], Some(8082));
        std::env::remove_var(env_key);
        assert_eq!(res, Some(8080));
    }

    #[test]
    fn test_resolve_opt_u16_env_precedence() {
        let env_key = "TEST_DASHBOARD_PORT_ENV_PREC";
        std::env::set_var(env_key, "8081");
        let res = resolve_opt_u16(None, &[env_key], Some(8082));
        std::env::remove_var(env_key);
        assert_eq!(res, Some(8081));
    }

    #[test]
    fn test_resolve_opt_u16_oxide_swarm_env_fallback() {
        let env_rusty = "RUSTY_GRID_DASH_TEST";
        let env_oxide = "OXIDE_SWARM_DASH_TEST";
        std::env::remove_var(env_rusty);
        std::env::set_var(env_oxide, "8083");
        let res = resolve_opt_u16(None, &[env_rusty, env_oxide], Some(8082));
        std::env::remove_var(env_oxide);
        assert_eq!(res, Some(8083));
    }

    #[test]
    fn test_resolve_opt_u16_file_fallback() {
        let env_key = "TEST_DASHBOARD_PORT_FILE_PREC";
        std::env::remove_var(env_key);
        let res = resolve_opt_u16(None, &[env_key], Some(8082));
        assert_eq!(res, Some(8082));
    }

    #[test]
    fn test_resolve_opt_u16_none_default() {
        let env_key = "TEST_DASHBOARD_PORT_NONE_PREC";
        std::env::remove_var(env_key);
        let res = resolve_opt_u16(None, &[env_key], None);
        assert_eq!(res, None);
    }
}
