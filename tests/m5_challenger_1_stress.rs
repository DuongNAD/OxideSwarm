//! Adversarial Stress and Verification Suite for Milestone 5.
//!
//! Authored by Challenger 1 to aggressively and empirically verify:
//! 1. 4-Tier Configuration Precedence Permutations:
//!    - CLI flag (Tier 1) > Env Var (Tier 2) > Config File (Tier 3) > Hardcoded Default (Tier 4).
//!    - Multi-field matrix across String, Numeric (u64, usize), Float (f32), and Boolean types.
//!    - Primary vs secondary environment variable fallbacks.
//!    - Resilient fallback on malformed environment variables (no crashes).
//! 2. TOML and JSON Configuration Parsing & Syntax Errors:
//!    - Semantic parity between TOML and JSON formats.
//!    - Malformed syntax rejection with descriptive errors (unclosed quotes, broken JSON).
//!    - Missing files, empty files (0-byte), and forward compatibility with unknown fields.
//! 3. Extreme & Edge Hardware Capability Overrides:
//!    - Zero cores (`--cores 0`) safety fallback to system parallelism (never 0).
//!    - Massive cores (`--cores 128`, `--cores 1024`) and concurrency permits.
//!    - Extreme RAM (`--ram-mb 1` vs `0` vs `u64::MAX`) and task requirement satisfaction.
//!    - Strict domination of `--no-gpu` over physical, simulated, and custom GPU configs.
//!    - Worker concurrency throttle overrides (`--max-concurrency`).
//! 4. Error Handling, Malformed P2P Tickets & CLI Parameter Validation:
//!    - Invalid master listen and connect socket addresses.
//!    - Corrupted and malformed P2P tickets.
//!    - Missing required CLI subcommands and parameters (exit code 2).
//! 5. Live End-to-End Config-Driven Cluster Verification:
//!    - Master running entirely from TOML configuration with ephemeral port.
//!    - Worker running entirely from JSON configuration with hardware overrides.
//!    - Successful registration, capability advertisement, and GPU workload execution.

use std::fs;
use std::path::PathBuf;
use std::process::Command;
use std::sync::atomic::{AtomicUsize, Ordering};
use std::time::Duration;

use tempfile::TempDir;
use tokio::sync::watch;
use uuid::Uuid;

use rusty_grid_core::capabilities::{HardwareOverrides, WorkerCapabilities};
use rusty_grid_core::error::GridError;
use rusty_grid_core::task::{Task, TaskRequirements, TaskSpec};
use rusty_grid_master::registry::WorkerStatus;
use rusty_grid_master::{MasterServer, ReaperConfig, SchedulerConfig, ServerConfig};
use rusty_grid_worker::client::{WorkerClient, WorkerConfig};

#[allow(dead_code)]
#[path = "../crates/cli/src/config.rs"]
mod config;

use config::{
    load_config_file, resolve_bool, resolve_f32, resolve_opt_field, resolve_opt_string,
    resolve_opt_u64, resolve_opt_usize, resolve_string, resolve_u64, resolve_usize, ConfigFile,
};

static ENV_COUNTER: AtomicUsize = AtomicUsize::new(1);

/// Generates an isolated unique environment variable key to prevent cross-test interference.
fn unique_env_key(prefix: &str) -> String {
    let id = ENV_COUNTER.fetch_add(1, Ordering::SeqCst);
    format!("{prefix}_{id}_{}", Uuid::new_v4().simple())
}

/// RAII guard to safely remove an environment variable upon test completion or panic.
struct EnvGuard(String);

impl EnvGuard {
    fn set(key: impl Into<String>, val: &str) -> Self {
        let k = key.into();
        std::env::set_var(&k, val);
        Self(k)
    }
}

impl Drop for EnvGuard {
    fn drop(&mut self) {
        std::env::remove_var(&self.0);
    }
}

// =========================================================================
// PART 1: 4-Tier Configuration Precedence Permutations & Fallbacks
// =========================================================================

#[test]
fn test_adversarial_4_tier_string_precedence_full_ladder() {
    let primary_env = unique_env_key("RUSTY_GRID_PRIMARY");
    let secondary_env = unique_env_key("RUSTY_GRID_SECONDARY");

    let env_keys: Vec<&str> = vec![&primary_env, &secondary_env];

    // Tier 4: Default value applies when nothing is set
    let val_tier4 = resolve_string(None, &env_keys, None, "tier4_default");
    assert_eq!(val_tier4, "tier4_default");

    // Tier 3: Config file value overrides default
    let val_tier3 = resolve_string(
        None,
        &env_keys,
        Some("tier3_config".to_string()),
        "tier4_default",
    );
    assert_eq!(val_tier3, "tier3_config");

    // Tier 2: Secondary environment variable overrides config file
    let _g2 = EnvGuard::set(&secondary_env, "tier2_secondary_env");
    let val_tier2_sec = resolve_string(
        None,
        &env_keys,
        Some("tier3_config".to_string()),
        "tier4_default",
    );
    assert_eq!(val_tier2_sec, "tier2_secondary_env");

    // Tier 2: Primary environment variable overrides secondary environment variable
    let _g1 = EnvGuard::set(&primary_env, "tier2_primary_env");
    let val_tier2_pri = resolve_string(
        None,
        &env_keys,
        Some("tier3_config".to_string()),
        "tier4_default",
    );
    assert_eq!(val_tier2_pri, "tier2_primary_env");

    // Tier 1: CLI flag strictly overrides ALL environment variables and config files
    let val_tier1 = resolve_string(
        Some("tier1_cli".to_string()),
        &env_keys,
        Some("tier3_config".to_string()),
        "tier4_default",
    );
    assert_eq!(val_tier1, "tier1_cli");
}

#[test]
fn test_adversarial_optional_field_resolvers() {
    let env_str = unique_env_key("RUSTY_GRID_OPT_STR");
    let env_num = unique_env_key("RUSTY_GRID_OPT_NUM");

    // All None -> None
    assert_eq!(resolve_opt_string(None, &[&env_str], None), None);
    assert_eq!(resolve_opt_usize(None, &[&env_num], None), None);
    assert_eq!(resolve_opt_u64(None, &[&env_num], None), None);
    assert_eq!(
        resolve_opt_field(None, &[&env_str], None, |s| Some(s.to_string())),
        None
    );

    // Config Some -> Some
    assert_eq!(
        resolve_opt_string(None, &[&env_str], Some("cfg_val".into())),
        Some("cfg_val".into())
    );
    assert_eq!(resolve_opt_usize(None, &[&env_num], Some(42)), Some(42));
    assert_eq!(resolve_opt_u64(None, &[&env_num], Some(100)), Some(100));

    // Env Some -> overrides Config
    {
        let _g = EnvGuard::set(&env_str, "env_val");
        let _g2 = EnvGuard::set(&env_num, "99");
        assert_eq!(
            resolve_opt_string(None, &[&env_str], Some("cfg_val".into())),
            Some("env_val".into())
        );
        assert_eq!(resolve_opt_usize(None, &[&env_num], Some(42)), Some(99));
        assert_eq!(resolve_opt_u64(None, &[&env_num], Some(100)), Some(99));
    }

    // CLI Some -> overrides Env and Config
    {
        let _g = EnvGuard::set(&env_str, "env_val");
        assert_eq!(
            resolve_opt_string(Some("cli_val".into()), &[&env_str], Some("cfg_val".into())),
            Some("cli_val".into())
        );
    }
}

#[test]
fn test_adversarial_4_tier_numeric_precedence_and_parsing_fallbacks() {
    let env_key = unique_env_key("RUSTY_GRID_NUM_VAR");

    // Tier 4: Default integer
    assert_eq!(resolve_u64(None, &[&env_key], None, 42), 42);
    assert_eq!(resolve_usize(None, &[&env_key], None, 8), 8);

    // Tier 3: Config file integer
    assert_eq!(resolve_u64(None, &[&env_key], Some(100), 42), 100);
    assert_eq!(resolve_usize(None, &[&env_key], Some(16), 8), 16);

    // Tier 2: Environment variable with leading/trailing whitespace
    {
        let _g = EnvGuard::set(&env_key, "   512   ");
        assert_eq!(resolve_u64(None, &[&env_key], Some(100), 42), 512);
        assert_eq!(resolve_usize(None, &[&env_key], Some(16), 8), 512);
    }

    // Tier 2 Adversarial: Malformed / non-numeric environment variable MUST NOT crash
    // and must fall back gracefully to Tier 3 (config file) or Tier 4 (default)
    {
        let _g = EnvGuard::set(&env_key, "not_a_valid_number");
        assert_eq!(
            resolve_u64(None, &[&env_key], Some(100), 42),
            100,
            "Must fall back to config file when env is garbage"
        );
        assert_eq!(
            resolve_u64(None, &[&env_key], None, 42),
            42,
            "Must fall back to default when config is None and env is garbage"
        );
    }

    // Tier 1: CLI argument strictly dominates even when valid env is present
    {
        let _g = EnvGuard::set(&env_key, "2048");
        assert_eq!(
            resolve_u64(Some(9999), &[&env_key], Some(100), 42),
            9999,
            "CLI argument must override environment variable"
        );
    }
}

#[test]
fn test_adversarial_4_tier_float_precedence_thresholds() {
    let env_key = unique_env_key("RUSTY_GRID_CPU_PCT");

    // Default fallback
    assert_eq!(resolve_f32(None, &[&env_key], None, 85.0), 85.0);

    // Config override
    assert_eq!(resolve_f32(None, &[&env_key], Some(70.5), 85.0), 70.5);

    // Env override
    {
        let _g = EnvGuard::set(&env_key, " 92.5 ");
        assert_eq!(resolve_f32(None, &[&env_key], Some(70.5), 85.0), 92.5);
    }

    // Env garbage fallback
    {
        let _g = EnvGuard::set(&env_key, "maximum_load_override");
        assert_eq!(
            resolve_f32(None, &[&env_key], Some(70.5), 85.0),
            70.5,
            "Float resolver must fall back cleanly on invalid float strings"
        );
    }

    // CLI override
    assert_eq!(resolve_f32(Some(50.0), &[&env_key], Some(70.5), 85.0), 50.0);
}

#[test]
fn test_adversarial_4_tier_boolean_truthy_falsy_permutations() {
    let env_key = unique_env_key("RUSTY_GRID_BOOL_FLAG");

    // Matrix of truthy representations: "1", "true", "yes", "on", case-insensitive
    for truthy in &["1", "true", "TRUE", "True", "yes", "YES", "on", "ON"] {
        let _g = EnvGuard::set(&env_key, truthy);
        assert!(
            resolve_bool(None, &[&env_key], None, false),
            "Expected '{truthy}' to evaluate to true"
        );
    }

    // Matrix of falsy representations: "0", "false", "no", "off", case-insensitive
    for falsy in &["0", "false", "FALSE", "False", "no", "NO", "off", "OFF"] {
        let _g = EnvGuard::set(&env_key, falsy);
        assert!(
            !resolve_bool(None, &[&env_key], None, true),
            "Expected '{falsy}' to evaluate to false"
        );
    }

    // Invalid string must fall back to default/config rather than panic
    {
        let _g = EnvGuard::set(&env_key, "maybe_uncertain");
        assert!(
            resolve_bool(None, &[&env_key], None, true),
            "Invalid boolean string must fall back to default"
        );
        assert!(
            !resolve_bool(None, &[&env_key], None, false),
            "Invalid boolean string must fall back to default"
        );
    }

    // CLI flag overrides environment variable
    {
        let _g = EnvGuard::set(&env_key, "false");
        assert!(
            resolve_bool(Some(true), &[&env_key], None, false),
            "CLI Some(true) must override env 'false'"
        );
    }
}

#[test]
fn test_adversarial_nested_config_hierarchy_precedence() {
    let toml_content = r#"
[worker]
cores = 12
ram_mb = 16384
gpu = false

[worker.hardware]
cores = 24
ram_mb = 32768
gpu = true
gpu_name = "Embedded Tensor Unit"
"#;

    let parsed: ConfigFile = toml::from_str(toml_content).expect("Valid TOML");
    let worker = parsed.worker.expect("worker config exists");
    let hw = worker.hardware.clone().expect("hardware section exists");

    // Test top-level worker.cores taking priority over worker.hardware.cores
    let effective_cores = worker.cores.or(hw.cores);
    assert_eq!(
        effective_cores,
        Some(12),
        "Top-level worker.cores must take precedence over worker.hardware.cores"
    );

    // Test top-level worker.ram_mb taking priority over worker.hardware.ram_mb
    let effective_ram = worker.ram_mb.or(hw.ram_mb);
    assert_eq!(
        effective_ram,
        Some(16384),
        "Top-level worker.ram_mb must take precedence over worker.hardware.ram_mb"
    );

    // Test fallback when top-level is omitted: gpu_name is only in [worker.hardware]
    let effective_gpu_name = worker.gpu_name.or(hw.gpu_name);
    assert_eq!(
        effective_gpu_name.as_deref(),
        Some("Embedded Tensor Unit"),
        "Nested hardware gpu_name must apply when top-level is omitted"
    );
}

// =========================================================================
// PART 2: TOML and JSON Configuration Parsing & Syntax Errors
// =========================================================================

#[test]
fn test_adversarial_config_file_valid_toml_and_json_parity() {
    let temp_dir = TempDir::new().expect("create temp dir");
    let toml_path = temp_dir.path().join("config.toml");
    let json_path = temp_dir.path().join("config.json");

    let toml_str = r#"
[master]
listen = "127.0.0.1:7777"
port_file = "/tmp/port_m5.txt"
heartbeat_interval_secs = 4
heartbeat_timeout_secs = 12
reaper_interval_secs = 2
max_host_cpu_pct = 80.0
preserve_gpu = true
p2p = true

[worker]
master = "127.0.0.1:7777"
name = "parity-worker"
cores = 8
ram_mb = 16384
simulate_gpu = true
max_concurrency = 4
keep_sandboxes = true

[submit]
master = "127.0.0.1:7777"
timeout_secs = 45

[status]
master = "127.0.0.1:7777"
"#;

    let json_str = r#"{
  "master": {
    "listen": "127.0.0.1:7777",
    "port_file": "/tmp/port_m5.txt",
    "heartbeat_interval_secs": 4,
    "heartbeat_timeout_secs": 12,
    "reaper_interval_secs": 2,
    "max_host_cpu_pct": 80.0,
    "preserve_gpu": true,
    "p2p": true
  },
  "worker": {
    "master": "127.0.0.1:7777",
    "name": "parity-worker",
    "cores": 8,
    "ram_mb": 16384,
    "simulate_gpu": true,
    "max_concurrency": 4,
    "keep_sandboxes": true
  },
  "submit": {
    "master": "127.0.0.1:7777",
    "timeout_secs": 45
  },
  "status": {
    "master": "127.0.0.1:7777"
  }
}"#;

    fs::write(&toml_path, toml_str).expect("write toml");
    fs::write(&json_path, json_str).expect("write json");

    let loaded_toml = load_config_file(Some(&toml_path))
        .expect("load toml ok")
        .expect("config present");
    let loaded_json = load_config_file(Some(&json_path))
        .expect("load json ok")
        .expect("config present");

    assert_eq!(
        loaded_toml, loaded_json,
        "TOML and JSON representations must produce identical ConfigFile structures"
    );

    assert_eq!(
        loaded_toml.master.unwrap().listen.as_deref(),
        Some("127.0.0.1:7777")
    );
    assert_eq!(loaded_json.worker.unwrap().simulate_gpu, Some(true));
}

#[test]
fn test_adversarial_config_file_syntax_error_rejection() {
    let temp_dir = TempDir::new().expect("create temp dir");

    // 1. Broken TOML: unclosed quote and invalid assignment
    let bad_toml_path = temp_dir.path().join("broken.toml");
    fs::write(&bad_toml_path, "[master\nlisten = \"unclosed string\n").expect("write bad toml");

    let res_toml = load_config_file(Some(&bad_toml_path));
    assert!(res_toml.is_err(), "Must return Err on malformed TOML");
    let err_str = res_toml.unwrap_err().to_string();
    assert!(
        err_str.contains("Failed to parse configuration file"),
        "Error message must clearly identify parsing failure: {err_str}"
    );

    // 2. Broken JSON: missing closing bracket and invalid comma
    let bad_json_path = temp_dir.path().join("broken.json");
    fs::write(
        &bad_json_path,
        "{\n  \"master\": {\n    \"listen\": 12345,\n",
    )
    .expect("write bad json");

    let res_json = load_config_file(Some(&bad_json_path));
    assert!(res_json.is_err(), "Must return Err on malformed JSON");
    let json_err_str = res_json.unwrap_err().to_string();
    assert!(
        json_err_str.contains("Failed to parse JSON configuration file"),
        "Error message must clearly identify JSON parse failure: {json_err_str}"
    );
}

#[test]
fn test_adversarial_config_file_missing_and_empty_files() {
    // 1. Non-existent file path
    let missing_path = PathBuf::from("/nonexistent/rusty_grid_test_path/config.toml");
    let res_missing = load_config_file(Some(&missing_path));
    assert!(res_missing.is_err());
    let err_msg = res_missing.unwrap_err().to_string();
    assert!(
        err_msg.contains("Configuration file not found"),
        "Expected not found error, got: {err_msg}"
    );

    // 2. Empty config file (0 bytes)
    let temp_dir = TempDir::new().expect("temp dir");
    let empty_path = temp_dir.path().join("empty.toml");
    fs::write(&empty_path, "").expect("write empty file");

    let res_empty = load_config_file(Some(&empty_path));
    assert!(res_empty.is_ok(), "0-byte config file must not crash");
    let cfg = res_empty.unwrap().expect("ConfigFile generated");
    assert_eq!(cfg, ConfigFile::default());
}

#[test]
fn test_adversarial_config_file_partial_and_missing_sections() {
    let temp_dir = TempDir::new().expect("temp dir");
    let partial_path = temp_dir.path().join("partial.toml");

    // Only submit section, everything else omitted
    fs::write(
        &partial_path,
        r#"
[submit]
master = "127.0.0.1:9090"
"#,
    )
    .expect("write partial");

    let res = load_config_file(Some(&partial_path))
        .expect("load ok")
        .unwrap();
    assert!(res.master.is_none());
    assert!(res.worker.is_none());
    assert!(res.status.is_none());
    assert_eq!(
        res.submit.unwrap().master.as_deref(),
        Some("127.0.0.1:9090")
    );
}

// =========================================================================
// PART 3: Extreme & Edge Hardware Capability Overrides
// =========================================================================

#[test]
fn test_adversarial_hardware_overrides_cores_zero_and_boundary() {
    // 1. Passing cores = 0: MUST fall back safely to system available parallelism (never 0)
    let overrides_zero = HardwareOverrides {
        cores: Some(0),
        ..Default::default()
    };
    let caps_zero = WorkerCapabilities::detect_with_overrides(overrides_zero);
    assert!(
        caps_zero.cpu_cores >= 1,
        "Cores=0 override must safely clamp/fallback to at least 1 core, got {}",
        caps_zero.cpu_cores
    );

    // 2. Massive cores override (128 and 1024)
    let overrides_128 = HardwareOverrides {
        cores: Some(128),
        ..Default::default()
    };
    let caps_128 = WorkerCapabilities::detect_with_overrides(overrides_128);
    assert_eq!(caps_128.cpu_cores, 128);

    let overrides_1024 = HardwareOverrides {
        cores: Some(1024),
        ..Default::default()
    };
    let caps_1024 = WorkerCapabilities::detect_with_overrides(overrides_1024);
    assert_eq!(caps_1024.cpu_cores, 1024);

    // 3. Semaphore permits match the configured cores
    let cfg = WorkerConfig::new("127.0.0.1:8080").with_cores(128);
    let client = WorkerClient::new(cfg);
    assert_eq!(client.concurrency_semaphore().available_permits(), 128);
}

#[test]
fn test_adversarial_hardware_overrides_ram_extreme_boundaries() {
    // 1. Extreme minimum: 1 MB RAM override
    let overrides_1mb = HardwareOverrides {
        ram_mb: Some(1),
        ..Default::default()
    };
    let caps_1mb = WorkerCapabilities::detect_with_overrides(overrides_1mb);
    assert_eq!(caps_1mb.ram_mb, 1);

    // A task requiring 2 MB RAM must be rejected
    let mut req_2mb = TaskRequirements::generic(1, 60);
    req_2mb.ram_mb = 2;
    assert!(
        !caps_1mb.satisfies(&req_2mb),
        "1MB RAM worker must NOT satisfy a 2MB requirement"
    );

    // A task requiring 1 MB RAM must be accepted
    let mut req_1mb = TaskRequirements::generic(1, 60);
    req_1mb.ram_mb = 1;
    assert!(
        caps_1mb.satisfies(&req_1mb),
        "1MB RAM worker must satisfy a 1MB requirement"
    );

    // 2. RAM override of 0: falls back to system detected RAM (> 0 MB)
    let overrides_0mb = HardwareOverrides {
        ram_mb: Some(0),
        ..Default::default()
    };
    let caps_0mb = WorkerCapabilities::detect_with_overrides(overrides_0mb);
    assert!(
        caps_0mb.ram_mb > 0,
        "RAM=0 override must safely fallback to system RAM, got {}",
        caps_0mb.ram_mb
    );

    // 3. Boundary maximum RAM
    let overrides_max = HardwareOverrides {
        ram_mb: Some(u64::MAX),
        ..Default::default()
    };
    let caps_max = WorkerCapabilities::detect_with_overrides(overrides_max);
    assert_eq!(caps_max.ram_mb, u64::MAX);
}

#[test]
fn test_adversarial_hardware_overrides_no_gpu_strict_domination() {
    // When no_gpu is true, it MUST strictly dominate all other GPU flags:
    // simulate_gpu: true, gpu: Some(true), and custom gpu_name
    let conflicting_overrides = HardwareOverrides {
        name: Some("strict-no-gpu-node".into()),
        no_gpu: true,
        simulate_gpu: true,
        gpu: Some(true),
        gpu_name: Some("Overridden RTX 4090".into()),
        ..Default::default()
    };

    let caps = WorkerCapabilities::detect_with_overrides(conflicting_overrides);

    assert_eq!(caps.name, "strict-no-gpu-node");
    assert!(
        !caps.has_gpu,
        "no_gpu MUST strictly disable has_gpu regardless of other flags"
    );
    assert!(
        !caps.is_simulated_gpu,
        "no_gpu MUST strictly disable is_simulated_gpu regardless of other flags"
    );
    assert!(
        caps.gpu_device_name.is_none(),
        "no_gpu MUST strip gpu_device_name"
    );
    assert!(
        !caps.can_execute_gpu(),
        "can_execute_gpu() must return false when no_gpu is set"
    );

    // Strict requirement gating: GPU tasks MUST be rejected
    let gpu_req = TaskRequirements::gpu(60);
    assert!(
        !caps.satisfies(&gpu_req),
        "no_gpu worker must NEVER satisfy GPU task requirements"
    );
}

#[test]
fn test_adversarial_hardware_overrides_gpu_and_gpu_name_customization() {
    // 1. Explicit physical GPU toggle
    let overrides_gpu = HardwareOverrides {
        gpu: Some(true),
        gpu_name: Some("NVIDIA H100 SXM5".into()),
        ..Default::default()
    };
    let caps_gpu = WorkerCapabilities::detect_with_overrides(overrides_gpu);
    assert!(caps_gpu.has_gpu);
    assert!(!caps_gpu.is_simulated_gpu);
    assert_eq!(
        caps_gpu.gpu_device_name.as_deref(),
        Some("NVIDIA H100 SXM5")
    );
    assert!(caps_gpu.can_execute_gpu());
    assert!(caps_gpu.satisfies(&TaskRequirements::gpu(60)));

    // 2. Simulated GPU toggle with default device name
    let overrides_sim = HardwareOverrides {
        simulate_gpu: true,
        ..Default::default()
    };
    let caps_sim = WorkerCapabilities::detect_with_overrides(overrides_sim);
    assert!(caps_sim.has_gpu);
    assert!(caps_sim.is_simulated_gpu);
    assert_eq!(
        caps_sim.gpu_device_name.as_deref(),
        Some("Simulated Virtual GPU (Matrix Engine)")
    );
    assert!(caps_sim.can_execute_gpu());
}

#[test]
fn test_adversarial_worker_concurrency_semaphore_override() {
    // Setting max_concurrency: Some(2) on an 8-core worker must restrict permits to 2
    let cfg = WorkerConfig::new("127.0.0.1:8080")
        .with_cores(8)
        .with_max_concurrency(2);
    let client = WorkerClient::new(cfg);

    assert_eq!(
        client.capabilities().cpu_cores,
        8,
        "CPU core count remains advertised as 8"
    );
    assert_eq!(
        client.concurrency_semaphore().available_permits(),
        2,
        "Concurrency semaphore permits must be restricted to max_concurrency (2)"
    );

    // Setting max_concurrency: Some(0) must safely clamp to 1 permit to prevent deadlock
    let cfg_zero = WorkerConfig::new("127.0.0.1:8080")
        .with_cores(8)
        .with_max_concurrency(0);
    let client_zero = WorkerClient::new(cfg_zero);
    assert_eq!(
        client_zero.concurrency_semaphore().available_permits(),
        1,
        "max_concurrency=0 must safely clamp to at least 1 permit"
    );
}

// =========================================================================
// PART 4: Invalid Master Addresses, Malformed P2P Tickets & Parameter Validation
// =========================================================================

#[test]
fn test_adversarial_master_listen_address_validation() {
    // 1. Completely invalid strings
    let invalid_addrs = [
        "not_an_ip_or_port",
        "999.999.999.999:80",
        "127.0.0.1:99999", // Port exceeds u16::MAX
        "::1:invalid_port",
        "",
    ];

    for addr in &invalid_addrs {
        let res = ServerConfig::from_addr(addr);
        assert!(
            res.is_err(),
            "ServerConfig::from_addr must return error for '{addr}'"
        );
        match res.unwrap_err() {
            GridError::Config(msg) => {
                assert!(
                    msg.contains("Failed to parse server bind address"),
                    "Expected bind address error, got: {msg}"
                );
            }
            other => panic!("Expected GridError::Config, got {other:?}"),
        }
    }

    // 2. Verify CLI binary exits with failure (code 1) when given an invalid listen address
    let bin_path = env!("CARGO_BIN_EXE_rusty-grid");
    let output = Command::new(bin_path)
        .arg("master")
        .arg("--listen")
        .arg("invalid_socket_address:abc")
        .output()
        .expect("invoke CLI binary");

    assert!(
        !output.status.success(),
        "CLI master must exit with failure code on invalid listen address"
    );
    let stderr = String::from_utf8_lossy(&output.stderr);
    assert!(
        stderr.contains("Invalid listen address"),
        "Expected invalid listen address error in stderr: {stderr}"
    );
}

#[test]
fn test_adversarial_p2p_ticket_malformed_rejection() {
    let malformed_tickets = [
        "not_a_json_ticket",
        "{\"unexpected_key\": 123}",
        "",
        "   ",
        "[1, 2, 3]",
        "{\"node_id\": \"invalid_hex\"}",
    ];

    for ticket in &malformed_tickets {
        let res = rusty_grid_core::transport::parse_p2p_ticket(ticket);
        assert!(
            res.is_err(),
            "parse_p2p_ticket must reject malformed ticket: '{ticket}'"
        );
    }
}

#[test]
fn test_adversarial_cli_missing_and_invalid_arguments() {
    let bin_path = env!("CARGO_BIN_EXE_rusty-grid");

    // 1. Invoking CLI with zero arguments: missing subcommand
    let output_no_args = Command::new(bin_path).output().expect("invoke CLI");
    assert_eq!(
        output_no_args.status.code(),
        Some(2),
        "Clap must exit with code 2 on missing required subcommand"
    );

    // 2. Invoking CLI with non-existent subcommand
    let output_bad_cmd = Command::new(bin_path)
        .arg("nonexistent_command")
        .output()
        .expect("invoke CLI");
    assert_eq!(
        output_bad_cmd.status.code(),
        Some(2),
        "Clap must exit with code 2 on unrecognized subcommand"
    );

    // 3. Invoking mapreduce without required --input argument
    let output_mr = Command::new(bin_path)
        .arg("mapreduce")
        .output()
        .expect("invoke CLI");
    assert_eq!(
        output_mr.status.code(),
        Some(2),
        "Clap must exit with code 2 on missing required --input parameter"
    );
    let stderr_mr = String::from_utf8_lossy(&output_mr.stderr);
    assert!(stderr_mr.contains("--input"));

    // 4. Submitting task to closed/non-listening port
    let output_submit_closed = Command::new(bin_path)
        .arg("submit")
        .arg("--master")
        .arg("127.0.0.1:1") // Closed port
        .arg("--type")
        .arg("generic")
        .output()
        .expect("invoke CLI");
    assert_eq!(
        output_submit_closed.status.code(),
        Some(1),
        "Submit to closed port must return ExitCode::FAILURE (1)"
    );
    let stderr_submit = String::from_utf8_lossy(&output_submit_closed.stderr);
    assert!(
        stderr_submit.contains("Failed to connect to Master"),
        "Expected connection error message, got: {stderr_submit}"
    );
}

// =========================================================================
// PART 5: Live End-to-End Config-Driven Cluster Verification
// =========================================================================

#[tokio::test]
async fn test_adversarial_live_cluster_configured_via_toml_and_json_files() {
    let temp_dir = TempDir::new().expect("create temp dir");
    let master_toml_path = temp_dir.path().join("master_config.toml");
    let worker_json_path = temp_dir.path().join("worker_config.json");
    let port_file_path = temp_dir.path().join("master_port.txt");

    // 1. Write Master configuration to TOML file
    let master_toml = format!(
        r#"
[master]
listen = "127.0.0.1:0"
port_file = '{}'
heartbeat_interval_secs = 2
heartbeat_timeout_secs = 6
preserve_gpu = true
"#,
        port_file_path.display()
    );
    fs::write(&master_toml_path, master_toml).expect("write master toml");

    // Load master configuration using the 4-tier engine
    let loaded_master_file = load_config_file(Some(&master_toml_path))
        .expect("load ok")
        .expect("master config present");
    let master_cfg = loaded_master_file.master.unwrap();

    let listen_addr = master_cfg.listen.unwrap();
    let port_file = master_cfg.port_file.unwrap();
    let heartbeat_interval = master_cfg.heartbeat_interval_secs.unwrap();
    let heartbeat_timeout = master_cfg.heartbeat_timeout_secs.unwrap();
    let preserve_gpu = master_cfg.preserve_gpu.unwrap();

    let server_config = ServerConfig::from_addr(&listen_addr)
        .expect("parse addr")
        .with_port_file(port_file)
        .with_heartbeat_interval(heartbeat_interval);

    let sched_config = SchedulerConfig {
        preserve_gpu_for_gpu_tasks: preserve_gpu,
        ..Default::default()
    };

    let reaper_config = ReaperConfig::new(
        Duration::from_secs(1),
        Duration::from_secs(heartbeat_timeout),
    );

    // Spawn Master server
    let master_handle = MasterServer::spawn_with_config(server_config, sched_config, reaper_config)
        .await
        .expect("spawn master server");

    let bound_addr = master_handle.server_addr();
    let bound_port = bound_addr.port();
    assert!(bound_port > 0, "Bound port must be allocated dynamically");

    // 2. Write Worker configuration to JSON file with hardware overrides
    let worker_json = format!(
        r#"{{
  "worker": {{
    "master": "{}",
    "name": "adversarial-cfg-worker",
    "cores": 6,
    "ram_mb": 12288,
    "simulate_gpu": true,
    "gpu_name": "Configured Virtual Tensor Unit",
    "max_concurrency": 6,
    "heartbeat_interval_secs": 2
  }}
}}"#,
        bound_addr
    );
    fs::write(&worker_json_path, worker_json).expect("write worker json");

    // Load worker configuration using the 4-tier engine
    let loaded_worker_file = load_config_file(Some(&worker_json_path))
        .expect("load ok")
        .expect("worker config present");
    let worker_cfg = loaded_worker_file.worker.unwrap();

    let worker_config = WorkerConfig::new(worker_cfg.master.unwrap())
        .with_name(worker_cfg.name.unwrap())
        .with_cores(worker_cfg.cores.unwrap())
        .with_ram_mb(worker_cfg.ram_mb.unwrap())
        .with_simulate_gpu(worker_cfg.simulate_gpu.unwrap())
        .with_gpu_name(worker_cfg.gpu_name.unwrap())
        .with_max_concurrency(worker_cfg.max_concurrency.unwrap());

    let mut worker_client = WorkerClient::new(worker_config);
    let worker_id = worker_client.worker_id();

    // Verify worker client capabilities reflect the JSON configuration overrides
    assert_eq!(worker_client.capabilities().name, "adversarial-cfg-worker");
    assert_eq!(worker_client.capabilities().cpu_cores, 6);
    assert_eq!(worker_client.capabilities().ram_mb, 12288);
    assert!(worker_client.capabilities().is_simulated_gpu);
    assert_eq!(
        worker_client.capabilities().gpu_device_name.as_deref(),
        Some("Configured Virtual Tensor Unit")
    );

    // Spawn worker in background
    let (shutdown_tx, shutdown_rx) = watch::channel(false);
    let worker_join = tokio::spawn(async move {
        let _ = worker_client.run(shutdown_rx).await;
    });

    // Wait for worker to register with Master
    let mut registered = false;
    for _ in 0..50 {
        tokio::time::sleep(Duration::from_millis(50)).await;
        if let Some(info) = master_handle.registry().get_worker(worker_id).await {
            if info.status == WorkerStatus::Connected {
                assert_eq!(info.capabilities.name, "adversarial-cfg-worker");
                assert_eq!(info.capabilities.cpu_cores, 6);
                assert_eq!(info.capabilities.ram_mb, 12288);
                assert!(info.capabilities.has_gpu);
                assert!(info.capabilities.is_simulated_gpu);
                registered = true;
                break;
            }
        }
    }
    assert!(
        registered,
        "Worker configured via JSON must successfully register with Master"
    );

    // 3. Submit a GPU task to verify the config-driven cluster executes work properly
    let gpu_task = Task::new(
        TaskSpec::GpuCompute {
            kernel_name: "config_test_kernel".into(),
            input_data: vec![10, 20, 30],
            work_group_size: 16,
            simulated_matrix_dim: 32,
            compute_intensity: 5,
        },
        TaskRequirements::gpu(30),
    );

    let task_id = master_handle
        .submit_task(gpu_task)
        .await
        .expect("submit task");
    let res = master_handle
        .wait_task(task_id, Some(Duration::from_secs(5)))
        .await
        .expect("Task executed and waited successfully");

    assert_eq!(res.exit_code, 0);
    assert!(res.is_gpu_executed);
    assert!(res.stdout.contains("[GPU COMPUTE SIMULATOR]"));

    // 4. Clean shutdown
    let _ = shutdown_tx.send(true);
    let _ = worker_join.await;
    let _ = master_handle.shutdown();
}
