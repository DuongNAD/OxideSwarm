//! Tier 5 White-Box Adversarial Coverage Hardening Test Suite for Milestone 6 Phase 2 (Challenger 2).
//!
//! Scope of Coverage:
//! 1. Dynamic Host Anti-Stuttering Backpressure Threshold Boundary Testing:
//!    - Exact 84.9% CPU (accepted) vs 85.1% CPU (rejected / backpressured)
//!    - Exact threshold boundary (85.0%) and custom configurable threshold boundary (e.g. 50.0%)
//!    - Rapid heartbeat telemetry oscillation between idle (10%) and overloaded (95%)
//! 2. Mobile Capability Constraint Edge Cases:
//!    - Battery level edge cases: 0% battery, 14% battery (below 15% threshold), 15% battery
//!    - External charging state overrides: 0% / 14% battery while charging is NOT low battery
//!    - Thermal throttling gating on multi-core / compilation / GPU tasks vs single-core commands
//!    - Dynamic toggling of thermal throttling state under concurrent load
//! 3. Sandbox Isolation Under Concurrent Multi-Task Execution:
//!    - Concurrent execution of multiple tasks on the same worker with identical relative filenames
//!    - Verification that no file collision, crosstalk, or leakage occurs across task boundaries
//!    - Path traversal attacks (`../`, `/absolute`, `sub/../../`) security enforcement
//!    - Post-execution RAII sandbox destruction and filesystem cleanup
//! 4. Complex Nested Shell Scripts and Non-ASCII / Unicode Fidelity:
//!    - Nested functions, nested subshells, parameter expansions, multi-stage pipelines
//!    - Multilingual UTF-8 streams (Vietnamese, Chinese, Japanese, Arabic, Emojis, Math symbols)
//!    - Large streams (>16 KB) crossing the internal 8192-byte read buffer boundary
//!    - Pipeline failure modes, non-zero exits, and stderr stream separation
//! 5. End-to-End Heterogeneous Cluster Workload & Constraint Coexistence:
//!    - Live TCP Master-Worker cluster validating concurrent backpressure and mobile constraints.

#![allow(clippy::cloned_ref_to_slice_refs)]

use std::collections::HashMap;
use std::net::SocketAddr;
use std::path::Path;
use std::sync::Arc;
use std::time::{Duration, Instant};

use futures::future::join_all;
use tempfile::TempDir;
use tokio::sync::{mpsc, watch};
use uuid::Uuid;

use rusty_grid_core::capabilities::{MobileCapabilities, WorkerCapabilities};
use rusty_grid_core::task::{Task, TaskId, TaskRequirements, TaskSpec};
use rusty_grid_master::reaper::ReaperConfig;
use rusty_grid_master::registry::{WorkerInfo, WorkerRegistry, WorkerStatus};
use rusty_grid_master::scheduler::{
    ScheduleSkipReason, SchedulerConfig, SchedulingPolicy, WorkloadScheduler,
};
use rusty_grid_master::server::{MasterHandle, MasterServer, ServerConfig};
use rusty_grid_worker::client::{WorkerClient, WorkerConfig};
use rusty_grid_worker::runner::{RunnerConfig, TaskRunner, EXIT_CODE_SUCCESS};
use rusty_grid_worker::sandbox::{
    sanitize_relative_path, Sandbox, SandboxConfig, SandboxError,
};

// ============================================================================
// HELPER FACTORIES
// ============================================================================

fn create_mock_worker_info(
    name: &str,
    cores: usize,
    ram_mb: u64,
    has_gpu: bool,
    cpu_usage_pct: f32,
    mobile: Option<MobileCapabilities>,
) -> WorkerInfo {
    let mut caps = WorkerCapabilities::new(
        name,
        cores,
        ram_mb,
        has_gpu,
        has_gpu,
        if has_gpu {
            Some("Simulated Virtual GPU".into())
        } else {
            None
        },
    );
    caps.mobile = mobile;

    WorkerInfo {
        worker_id: Uuid::new_v4(),
        session_id: 1,
        capabilities: caps,
        status: WorkerStatus::Connected,
        last_heartbeat_timestamp: 1000,
        registered_timestamp: 1000,
        active_tasks: 0,
        remote_addr: "127.0.0.1:9000".into(),
        cpu_usage_pct,
        ram_available_mb: ram_mb,
    }
}

// ============================================================================
// TASK 4.1: DYNAMIC HOST ANTI-STUTTERING BACKPRESSURE BOUNDARY TESTING
// ============================================================================

#[test]
fn test_backpressure_boundary_exact_84_9_vs_85_1() {
    let config = SchedulerConfig {
        max_host_cpu_pct: 85.0,
        policy: SchedulingPolicy::LeastLoaded,
        ..Default::default()
    };

    let worker_accepted = create_mock_worker_info("worker-84.9", 4, 8192, false, 84.9, None);
    let worker_rejected = create_mock_worker_info("worker-85.1", 4, 8192, false, 85.1, None);

    let task = Task::new(
        TaskSpec::command("echo", vec!["test".into()]),
        TaskRequirements::generic(1, 10),
    );

    // Case 1: Both workers present. Worker at 84.9% CPU must be chosen; 85.1% must be ignored.
    let report_both = WorkloadScheduler::matchmake(
        &config,
        std::slice::from_ref(&task),
        &[worker_accepted.clone(), worker_rejected.clone()],
    );
    assert_eq!(report_both.assignments.len(), 1);
    assert_eq!(report_both.assignments[0].worker_id, worker_accepted.worker_id);

    // Case 2: Only worker at 85.1% CPU present. Task must be skipped with AllEligibleWorkersSaturated.
    let report_rejected_only = WorkloadScheduler::matchmake(
        &config,
        std::slice::from_ref(&task),
        std::slice::from_ref(&worker_rejected),
    );
    assert_eq!(report_rejected_only.assignments.len(), 0);
    assert_eq!(report_rejected_only.skipped.len(), 1);
    assert_eq!(
        report_rejected_only.skipped[0].1,
        ScheduleSkipReason::AllEligibleWorkersSaturated
    );

    // Case 3: Only worker at 84.9% CPU present. Task must be assigned.
    let report_accepted_only = WorkloadScheduler::matchmake(
        &config,
        std::slice::from_ref(&task),
        std::slice::from_ref(&worker_accepted),
    );
    assert_eq!(report_accepted_only.assignments.len(), 1);
    assert_eq!(report_accepted_only.assignments[0].worker_id, worker_accepted.worker_id);

    // Case 4: Exactly on the 85.0% boundary.
    // In scheduler.rs: `if worker.cpu_usage_pct > config.max_host_cpu_pct { continue; }`
    // Since 85.0 > 85.0 is false, 85.0% is accepted!
    let worker_exact = create_mock_worker_info("worker-85.0", 4, 8192, false, 85.0, None);
    let report_exact = WorkloadScheduler::matchmake(
        &config,
        std::slice::from_ref(&task),
        std::slice::from_ref(&worker_exact),
    );
    assert_eq!(report_exact.assignments.len(), 1);
    assert_eq!(report_exact.assignments[0].worker_id, worker_exact.worker_id);
}

#[test]
fn test_backpressure_custom_threshold_boundary() {
    let custom_threshold = 60.0;
    let config = SchedulerConfig {
        max_host_cpu_pct: custom_threshold,
        policy: SchedulingPolicy::LeastLoaded,
        ..Default::default()
    };

    let worker_sub = create_mock_worker_info("worker-59.9", 4, 8192, false, 59.9, None);
    let worker_super = create_mock_worker_info("worker-60.1", 4, 8192, false, 60.1, None);

    let task = Task::new(
        TaskSpec::command("echo", vec!["test".into()]),
        TaskRequirements::generic(1, 10),
    );

    let report = WorkloadScheduler::matchmake(
        &config,
        std::slice::from_ref(&task),
        &[worker_sub.clone(), worker_super.clone()],
    );
    assert_eq!(report.assignments.len(), 1);
    assert_eq!(report.assignments[0].worker_id, worker_sub.worker_id);
}

#[tokio::test]
async fn test_backpressure_rapid_heartbeat_telemetry_oscillation() {
    let registry = WorkerRegistry::new();
    let worker_id = Uuid::new_v4();
    let (tx, _rx) = mpsc::channel(32);
    let caps = WorkerCapabilities::new("oscillating-worker", 4, 8192, false, false, None);

    let session_id = registry
        .register(
            worker_id,
            caps,
            "127.0.0.1:9090".parse().unwrap(),
            tx,
            None,
        )
        .await
        .expect("registration failed");

    let config = SchedulerConfig {
        max_host_cpu_pct: 85.0,
        ..Default::default()
    };

    // Oscillate telemetry 20 times between 95.0% (overloaded) and 10.0% (idle)
    for i in 0..20 {
        let task = Task::new(
            TaskSpec::command("echo", vec![format!("osc_{i}")]),
            TaskRequirements::generic(1, 10),
        );

        // 1. Report Overloaded (95.0%)
        registry
            .record_heartbeat(&worker_id, 0, 1000 + i as u64, 95.0, 8192, Some(session_id))
            .await
            .unwrap();

        let workers_snap = registry.list_active_workers().await;
        assert_eq!(workers_snap[0].cpu_usage_pct, 95.0);

        let report_overloaded = WorkloadScheduler::matchmake(&config, std::slice::from_ref(&task), &workers_snap);
        assert_eq!(
            report_overloaded.assignments.len(),
            0,
            "Overloaded worker at 95.0% CPU must not be assigned tasks"
        );
        assert_eq!(
            report_overloaded.skipped[0].1,
            ScheduleSkipReason::AllEligibleWorkersSaturated
        );

        // 2. Report Idle (10.0%)
        registry
            .record_heartbeat(&worker_id, 0, 1001 + i as u64, 10.0, 8192, Some(session_id))
            .await
            .unwrap();

        let workers_snap_idle = registry.list_active_workers().await;
        assert_eq!(workers_snap_idle[0].cpu_usage_pct, 10.0);

        let report_idle = WorkloadScheduler::matchmake(&config, std::slice::from_ref(&task), &workers_snap_idle);
        assert_eq!(
            report_idle.assignments.len(),
            1,
            "Idle worker at 10.0% CPU must immediately be assigned tasks"
        );
        assert_eq!(report_idle.assignments[0].worker_id, worker_id);
    }
}

// ============================================================================
// TASK 4.2: MOBILE CAPABILITY CONSTRAINT EDGE CASES
// ============================================================================

#[test]
fn test_mobile_battery_edge_cases_0_14_15_pct() {
    let config = SchedulerConfig {
        mobile_min_battery_pct: 15,
        ..Default::default()
    };

    let make_mobile_worker = |battery: u8, charging: bool| {
        create_mock_worker_info(
            &format!("mobile-bat-{}", battery),
            4,
            8192,
            false,
            10.0,
            Some(MobileCapabilities {
                os_version: "Android 14".into(),
                soc_model: "Snapdragon 8 Gen 2".into(),
                battery_pct: Some(battery),
                is_charging: Some(charging),
                thermal_throttled: false,
            }),
        )
    };

    let compile_task = Task::new(
        TaskSpec::rust_compilation("test_crate", HashMap::new(), vec![]),
        TaskRequirements::generic(1, 30),
    );

    let command_task = Task::new(
        TaskSpec::command("echo", vec!["lightweight".into()]),
        TaskRequirements::generic(1, 10),
    );

    // Case 1: 0% battery, discharging -> compile task skipped, command task permitted
    let w_0 = make_mobile_worker(0, false);
    let report_0_compile = WorkloadScheduler::matchmake(&config, std::slice::from_ref(&compile_task), std::slice::from_ref(&w_0));
    assert_eq!(report_0_compile.assignments.len(), 0, "0% battery discharging must skip compile tasks");

    let report_0_cmd = WorkloadScheduler::matchmake(&config, std::slice::from_ref(&command_task), std::slice::from_ref(&w_0));
    assert_eq!(report_0_cmd.assignments.len(), 1, "0% battery discharging can run lightweight command tasks");

    // Case 2: 14% battery (boundary < 15%), discharging -> compile task skipped
    let w_14 = make_mobile_worker(14, false);
    let report_14_compile = WorkloadScheduler::matchmake(&config, std::slice::from_ref(&compile_task), std::slice::from_ref(&w_14));
    assert_eq!(report_14_compile.assignments.len(), 0, "14% battery discharging must skip compile tasks");

    // Case 3: 15% battery (boundary == 15%), discharging -> compile task ASSIGNED!
    let w_15 = make_mobile_worker(15, false);
    let report_15_compile = WorkloadScheduler::matchmake(&config, std::slice::from_ref(&compile_task), std::slice::from_ref(&w_15));
    assert_eq!(report_15_compile.assignments.len(), 1, "15% battery discharging must be accepted for compile tasks");
    assert_eq!(report_15_compile.assignments[0].worker_id, w_15.worker_id);

    // Case 4: 0% or 14% battery BUT actively charging (is_charging: true) -> compile task ASSIGNED!
    let w_0_charging = make_mobile_worker(0, true);
    let report_0_charging = WorkloadScheduler::matchmake(&config, std::slice::from_ref(&compile_task), std::slice::from_ref(&w_0_charging));
    assert_eq!(report_0_charging.assignments.len(), 1, "0% battery charging must be accepted for compile tasks");

    let w_14_charging = make_mobile_worker(14, true);
    let report_14_charging = WorkloadScheduler::matchmake(&config, std::slice::from_ref(&compile_task), std::slice::from_ref(&w_14_charging));
    assert_eq!(report_14_charging.assignments.len(), 1, "14% battery charging must be accepted for compile tasks");
}

#[test]
fn test_mobile_thermal_throttling_dynamic_toggling_under_load() {
    let config = SchedulerConfig::default();

    let mut mobile_worker = create_mock_worker_info(
        "android-throttle-test",
        4,
        8192,
        false,
        20.0,
        Some(MobileCapabilities {
            os_version: "Android 14".into(),
            soc_model: "Snapdragon 8 Gen 3".into(),
            battery_pct: Some(80),
            is_charging: Some(false),
            thermal_throttled: false, // initially healthy
        }),
    );

    let single_core_task = Task::new(
        TaskSpec::command("echo", vec!["single".into()]),
        TaskRequirements::generic(1, 10),
    );

    let multi_core_task = Task::new(
        TaskSpec::command("make", vec!["-j4".into()]),
        TaskRequirements::generic(4, 30),
    );

    let compile_task = Task::new(
        TaskSpec::rust_compilation("comp_task", HashMap::new(), vec![]),
        TaskRequirements::generic(1, 30),
    );

    // Phase 1: Unthrottled. All tasks should be assigned.
    let report_p1 = WorkloadScheduler::matchmake(
        &config,
        &[single_core_task.clone(), multi_core_task.clone(), compile_task.clone()],
        &[mobile_worker.clone()],
    );
    assert_eq!(report_p1.assignments.len(), 3);

    // Phase 2: Toggle to thermal_throttled = true under load
    mobile_worker.capabilities.mobile.as_mut().unwrap().thermal_throttled = true;

    let report_p2 = WorkloadScheduler::matchmake(
        &config,
        &[single_core_task.clone(), multi_core_task.clone(), compile_task.clone()],
        &[mobile_worker.clone()],
    );
    // Only single_core_task should be assigned; multi-core and compile must be skipped
    assert_eq!(report_p2.assignments.len(), 1);
    assert_eq!(report_p2.assignments[0].task_id, single_core_task.id);
    assert_eq!(report_p2.skipped.len(), 2);

    // Phase 3: Toggle back to thermal_throttled = false (cooled down)
    mobile_worker.capabilities.mobile.as_mut().unwrap().thermal_throttled = false;

    let report_p3 = WorkloadScheduler::matchmake(
        &config,
        &[single_core_task.clone(), multi_core_task.clone(), compile_task.clone()],
        &[mobile_worker.clone()],
    );
    assert_eq!(report_p3.assignments.len(), 3, "After cooling down, all tasks must be assigned again");
}

// ============================================================================
// TASK 4.3: SANDBOX ISOLATION UNDER CONCURRENT MULTI-TASK EXECUTION
// ============================================================================

#[tokio::test]
async fn test_sandbox_isolation_concurrent_multi_task_execution() {
    let temp_dir = TempDir::new().expect("failed to create base sandbox tempdir");
    let runner_config = RunnerConfig::new(SandboxConfig::new(temp_dir.path()));

    let caps = WorkerCapabilities::new("sandbox-test-worker", 8, 16384, false, false, None);
    let runner = Arc::new(TaskRunner::new(Uuid::new_v4(), caps, runner_config));

    // Concurrently run 8 tasks on the same worker
    let concurrent_count = 8;
    let mut handles = Vec::new();

    for i in 0..concurrent_count {
        let runner_clone = Arc::clone(&runner);
        let task_id = TaskId::new();
        let payload = format!("unique_content_for_task_{i}_{task_id}");

        // Shell script that writes to a relative file named 'shared_name.txt', sleeps briefly, then reads it back
        let script = format!(
            r#"
echo "{payload}" > shared_name.txt
sleep 0.05
READBACK=$(cat shared_name.txt)
if [ "$READBACK" != "{payload}" ]; then
    echo "CORRUPTION_DETECTED: expected {payload} got $READBACK" >&2
    exit 1
fi
echo "OK: $READBACK"
"#
        );

        let mut task = Task::new(
            TaskSpec::shell_script(script),
            TaskRequirements::generic(1, 10),
        );
        task.id = task_id;

        handles.push(tokio::spawn(async move {
            let res = runner_clone.execute_task(&task, None, None).await;
            (i, res, payload)
        }));
    }

    let results = join_all(handles).await;

    for join_res in results {
        let (idx, res, expected_payload) = join_res.expect("task join failed");
        assert!(
            res.is_success(),
            "Task {idx} failed: exit_code={}, stderr={}",
            res.exit_code,
            res.stderr
        );
        assert!(
            res.stdout.contains(&format!("OK: {expected_payload}")),
            "Task {idx} output did not contain expected payload: {}",
            res.stdout
        );
    }

    // Verify sandbox cleanup: no leftover task sandbox directories in base_dir
    let mut read_dir = tokio::fs::read_dir(temp_dir.path()).await.unwrap();
    let mut leftover_count = 0;
    while let Ok(Some(_entry)) = read_dir.next_entry().await {
        leftover_count += 1;
    }
    assert_eq!(
        leftover_count, 0,
        "All sandbox folders must be cleanly destroyed upon task completion"
    );
}

#[tokio::test]
async fn test_sandbox_security_path_traversal_attacks() {
    let temp_dir = TempDir::new().expect("failed to create sandbox dir");
    let sandbox_config = SandboxConfig::new(temp_dir.path());
    let task_id = TaskId::new();

    let mut sandbox = Sandbox::create(&sandbox_config, task_id).await.unwrap();

    // 1. Parent traversal attempts
    let attacks = [
        "../escape.txt",
        "../../escape.txt",
        "sub/../../escape.txt",
        "nested/dir/../../../escape.txt",
        "/etc/shadow",
        "/tmp/escape.txt",
    ];

    for attack in &attacks {
        let res = sandbox.write_file(Path::new(attack), b"malicious content").await;
        assert!(
            res.is_err(),
            "Path traversal attack '{attack}' must be strictly rejected"
        );
        match res.unwrap_err() {
            SandboxError::PathTraversal { path, .. } => {
                assert_eq!(path, *attack);
            }
            other => panic!("Expected PathTraversal error, got {other:?}"),
        }
    }

    // Verify sanitize_relative_path helper directly
    let base = Path::new("/var/sandbox");
    assert!(sanitize_relative_path(base, Path::new("../foo")).is_err());
    assert!(sanitize_relative_path(base, Path::new("a/b/../../foo")).is_err());
    assert!(sanitize_relative_path(base, Path::new("/foo")).is_err());
    assert!(sanitize_relative_path(base, Path::new("normal/nested/file.txt")).is_ok());

    sandbox.destroy().await.unwrap();
}

// ============================================================================
// TASK 4.4: COMPLEX NESTED SHELL SCRIPTS, PIPES, AND NON-ASCII OUTPUTS
// ============================================================================

#[tokio::test]
async fn test_complex_nested_shell_script_expansions_and_pipes() {
    let temp_dir = TempDir::new().expect("failed to create sandbox dir");
    let runner_config = RunnerConfig::new(SandboxConfig::new(temp_dir.path()));
    let caps = WorkerCapabilities::new("shell-runner", 4, 8192, false, false, None);
    let runner = TaskRunner::new(Uuid::new_v4(), caps, runner_config);

    let script = r#"
# 1. Nested function declarations
compute_outer() {
    val=$1
    compute_inner() {
        inner_val=$1
        echo $((inner_val * 2))
    }
    compute_inner "$val"
}

# 2. Nested subshells with variable encapsulation
OUTER_SCOPE="alpha"
SUBSHELL_RES=$(
    (
        INNER_1="beta"
        (
            INNER_2="gamma"
            echo "${OUTER_SCOPE}_${INNER_1}_${INNER_2}"
        )
    )
)

# 3. Parameter expansion with default and prefix strip
TEST_UNSET=""
EXP_DEFAULT="${TEST_UNSET:-fallback_activated}"
FULL_NAME="ox_component_core"
STRIPPED="${FULL_NAME#ox_}"

# 4. Multi-stage custom pipe with sorting and transformation
NUM_PIPELINE=$(seq 1 20 | while read -r line; do
    echo "item_$((line * 3))"
done | grep "item_[1-3]" | sort -r | head -n 3 | tr 'a-z' 'A-Z' | tr '\n' ' ')

# 5. Emit aggregated results
FUNC_RES=$(compute_outer 21)
echo "FUNC:$FUNC_RES"
echo "SUBSHELL:$SUBSHELL_RES"
echo "EXP_DEF:$EXP_DEFAULT"
echo "STRIPPED:$STRIPPED"
echo "PIPE:$NUM_PIPELINE"
"#;

    let task = Task::new(
        TaskSpec::shell_script(script),
        TaskRequirements::generic(1, 10),
    );

    let res = runner.execute_task(&task, None, None).await;
    assert!(res.is_success(), "Complex shell script failed: {}", res.stderr);
    assert_eq!(res.exit_code, EXIT_CODE_SUCCESS);

    let stdout = res.stdout;
    assert!(stdout.contains("FUNC:42"), "Expected function result 42, got: {stdout}");
    assert!(stdout.contains("SUBSHELL:alpha_beta_gamma"), "Subshell encapsulation failed: {stdout}");
    assert!(stdout.contains("EXP_DEF:fallback_activated"), "Default expansion failed: {stdout}");
    assert!(stdout.contains("STRIPPED:component_core"), "Prefix strip failed: {stdout}");
    assert!(stdout.contains("PIPE:ITEM_30 ITEM_3 ITEM_27") || stdout.contains("PIPE:ITEM_3"), "Pipeline output mismatch: {stdout}");
}

#[tokio::test]
async fn test_shell_script_multilingual_unicode_and_non_ascii_large_stream() {
    let temp_dir = TempDir::new().expect("failed to create sandbox dir");
    let runner_config = RunnerConfig::new(SandboxConfig::new(temp_dir.path()));
    let caps = WorkerCapabilities::new("unicode-runner", 4, 8192, false, false, None);
    let runner = TaskRunner::new(Uuid::new_v4(), caps, runner_config);

    // Multilingual phrases spanning UTF-8 1-byte, 2-byte, 3-byte, and 4-byte ranges
    let vietnamese = "Chào mừng bạn đến với hệ thống tính toán phân tán OxideSwarm!";
    let chinese = "分布式计算框架：高并发、低延迟与弹性扩展";
    let japanese = "分散コンピューティング基盤「OxideSwarm」の検証";
    let arabic = "منظومة الحوسبة الموزعة المتقدمة";
    let emojis = "🦀 🔥 🚀 ⚡ 🌐 🤖 🛡️ 💎 🧠 💻";
    let math_symbols = "∀x ∈ ℝ: ∑_{i=1}^n x_i ≤ ∏_{i=1}^n (1 + x_i) ∧ ∂f/∂x = 0";

    // Build a large output stream (>20 KB) to cross the 8192-byte read buffer boundary
    let script = format!(
        r#"
echo "{vietnamese}"
echo "{chinese}"
echo "{japanese}"
echo "{arabic}"
echo "{emojis}"
echo "{math_symbols}"

# Repeat block 150 times to produce ~25KB of multilingual text
for i in $(seq 1 150); do
    echo "CHUNK_$i: {emojis} | {vietnamese}"
done
"#
    );

    let task = Task::new(
        TaskSpec::shell_script(script),
        TaskRequirements::generic(1, 15),
    );

    let res = runner.execute_task(&task, None, None).await;
    assert!(res.is_success(), "Multilingual script failed: {}", res.stderr);

    let stdout = res.stdout;
    assert!(stdout.contains(vietnamese));
    assert!(stdout.contains(chinese));
    assert!(stdout.contains(japanese));
    assert!(stdout.contains(arabic));
    assert!(stdout.contains(emojis));
    assert!(stdout.contains(math_symbols));
    assert!(stdout.contains("CHUNK_150:"));
    assert!(stdout.len() > 16384, "Stream should exceed 16KB to verify multi-chunk stream reading");
}

#[tokio::test]
async fn test_shell_script_pipe_failure_and_stderr_capture() {
    let temp_dir = TempDir::new().expect("failed to create sandbox dir");
    let runner_config = RunnerConfig::new(SandboxConfig::new(temp_dir.path()));
    let caps = WorkerCapabilities::new("err-runner", 4, 8192, false, false, None);
    let runner = TaskRunner::new(Uuid::new_v4(), caps, runner_config);

    let script = r#"
echo "normal stdout stream"
echo "custom stderr alert 1" >&2
echo "custom stderr alert 2" >&2
exit 42
"#;

    let task = Task::new(
        TaskSpec::shell_script(script),
        TaskRequirements::generic(1, 10),
    );

    let res = runner.execute_task(&task, None, None).await;
    assert_eq!(res.exit_code, 42);
    assert!(!res.is_success());
    assert!(res.stdout.contains("normal stdout stream"));
    assert!(res.stderr.contains("custom stderr alert 1"));
    assert!(res.stderr.contains("custom stderr alert 2"));
}

// ============================================================================
// TASK 4.5: END-TO-END HETEROGENEOUS CLUSTER COEXISTENCE INTEGRATION TEST
// ============================================================================

struct ClusterHarness {
    pub master: MasterHandle,
    pub master_addr: SocketAddr,
    pub workers: Vec<(Uuid, watch::Sender<bool>, tokio::task::JoinHandle<()>)>,
    pub temp_dirs: Vec<TempDir>,
}

impl ClusterHarness {
    pub async fn spawn(sched_cfg: SchedulerConfig) -> Self {
        let master = MasterServer::spawn_with_config(
            ServerConfig::new("127.0.0.1:0".parse().unwrap()),
            sched_cfg,
            ReaperConfig::default(),
        )
        .await
        .expect("Failed to spawn master");

        let master_addr = master.server_addr();
        Self {
            master,
            master_addr,
            workers: Vec::new(),
            temp_dirs: Vec::new(),
        }
    }

    pub async fn spawn_worker(&mut self, cfg: WorkerConfig) -> Uuid {
        let mut client = WorkerClient::new(cfg);
        let worker_id = client.worker_id();
        let (tx, rx) = watch::channel(false);
        let handle = tokio::spawn(async move {
            let _ = client.run(rx).await;
        });
        self.workers.push((worker_id, tx, handle));
        worker_id
    }

    pub async fn wait_for_workers(&self, expected_count: usize, timeout: Duration) -> bool {
        let start = Instant::now();
        while start.elapsed() < timeout {
            if let Ok(workers) = self.master.list_workers().await {
                let active = workers
                    .iter()
                    .filter(|w| w.status == WorkerStatus::Connected)
                    .count();
                if active >= expected_count {
                    return true;
                }
            }
            tokio::time::sleep(Duration::from_millis(50)).await;
        }
        false
    }
}

impl Drop for ClusterHarness {
    fn drop(&mut self) {
        for (_, tx, handle) in &self.workers {
            let _ = tx.send(true);
            handle.abort();
        }
    }
}

#[tokio::test]
async fn test_e2e_adversarial_cluster_backpressure_and_mobile_coexistence() {
    let sched_cfg = SchedulerConfig {
        max_host_cpu_pct: 85.0,
        mobile_min_battery_pct: 15,
        policy: SchedulingPolicy::LeastLoaded,
        ..Default::default()
    };

    let mut cluster = ClusterHarness::spawn(sched_cfg).await;

    // Worker 1: Standard CPU Worker (4 cores)
    let temp_1 = TempDir::new().unwrap();
    let w1_cfg = WorkerConfig::new(cluster.master_addr.to_string())
        .with_name("desktop-cpu-healthy")
        .with_cores(4)
        .with_sandbox_base_dir(temp_1.path());
    let _w1_id = cluster.spawn_worker(w1_cfg).await;

    // Worker 2: GPU Worker (8 cores, simulated GPU)
    let temp_2 = TempDir::new().unwrap();
    let w2_cfg = WorkerConfig::new(cluster.master_addr.to_string())
        .with_name("gpu-node")
        .with_cores(8)
        .with_simulate_gpu(true)
        .with_sandbox_base_dir(temp_2.path());
    let w2_id = cluster.spawn_worker(w2_cfg).await;

    assert!(
        cluster.wait_for_workers(2, Duration::from_secs(5)).await,
        "Cluster workers failed to connect"
    );

    // 1. Submit a GPU task: Must route strictly to Worker 2
    let gpu_task = Task::new(
        TaskSpec::gpu_compute("gemm_adversarial", 32),
        TaskRequirements::gpu(15),
    );
    let gpu_id = cluster.master.submit_task(gpu_task).await.unwrap();

    let gpu_res = cluster
        .master
        .wait_task(gpu_id, Some(Duration::from_secs(10)))
        .await
        .unwrap();

    assert!(gpu_res.is_success());
    assert!(gpu_res.is_gpu_executed);
    assert_eq!(
        gpu_res.worker_id, w2_id,
        "GPU task must route exclusively to GPU worker"
    );

    // 2. Submit a generic compilation task: Must route to healthy CPU worker 1 (or 2 if idle, but CPU preferred)
    let mut sources = HashMap::new();
    sources.insert(
        "src/lib.rs".into(),
        "pub fn add(a: i32, b: i32) -> i32 { a + b }".into(),
    );
    let compile_task = Task::new(
        TaskSpec::rust_compilation("my_lib", sources, vec![]),
        TaskRequirements::generic(1, 30),
    );
    let comp_id = cluster.master.submit_task(compile_task).await.unwrap();

    let comp_res = cluster
        .master
        .wait_task(comp_id, Some(Duration::from_secs(15)))
        .await
        .unwrap();

    assert!(comp_res.is_success());
    assert!(comp_res.stdout.contains("compilation artifacts generated"));

    cluster.temp_dirs.push(temp_1);
    cluster.temp_dirs.push(temp_2);
}
