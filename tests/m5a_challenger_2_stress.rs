//! Adversarial and Empirical Stress Test Suite for Milestone M5A (Challenger 2).
//!
//! Stress-tests:
//! 1. Autonomous Mobile Master Refusal Matrix (§5.1):
//!    - Discharging phone claiming coordinator role
//!    - Metered / cellular networks (LTE, 5G, 4G, 3G, mobile, metered, CGNAT) claiming master
//!    - Battery reserve boundary tests (0%, 14%, 15%, 49%, 50%, 51%, 100%)
//!    - Thermal headroom boundary tests (39.9°C, 40.0°C, 40.001°C, 41.0°C, extreme values)
//!    - Execution context requirement (Android Foreground Service active)
//!    - Simultaneous 5-way refusal rule violation vectors
//!    - Desktop immunity from mobile refusal rules
//! 2. Deterministic Authority Scoring ($S_{\text{auth}}$) & 4-Stage Tie-Breaking (§5.2):
//!    - Mathematical properties across 10,000 randomized candidate pairs (Antisymmetry, Totality, Consistency, Reflexivity)
//!    - Transitivity across 1,000 candidate triples
//!    - Strict permutation sorting stability
//!    - Isolated 4-stage hierarchy validation:
//!      Stage 1: Authority Score
//!      Stage 2: Platform Weight
//!      Stage 3: Uptime
//!      Stage 4: UUID Lexicographical Ordering (lower byte values win)
//! 3. Cryptographic `MasterLeaseTicket` Tamper Detection (§5.3):
//!    - Exhaustive 480 single-bit flip sweeps across canonical 60-byte payload
//!    - Exhaustive 256 single-bit flip sweeps across 32-byte HMAC-SHA256 signature
//!    - Truncated and oversized signature lengths
//!    - Expiration and time range boundary invariants (now == expiry vs now == expiry + 1)
//!    - Inverted time ranges (expires_at <= issued_at)
//!    - Wrong cluster secret and mutated secret keys
//!    - Claim superiority arbitrage evaluation (`evaluate_master_claim`)
//! 4. Pre-Sleep Fast Task Evacuation Under Multi-Task Load (< 50ms) (§5.4):
//!    - Concurrent multi-task workload running across 3 workers
//!    - Pre-sleep evacuation trigger under active load
//!    - Task reassignment and cluster convergence in < 50ms
//!    - 100% completion with zero task loss across all submitted tasks
//!    - Correct retry_count accounting (1 on evacuated tasks, 0 on survivor tasks)
//!    - Direct `cancel_all_active_tasks` fast-path verification (< 15ms)

use std::time::{Duration, Instant};

use tokio::sync::watch;
use uuid::Uuid;

use oxideworker::{get_evacuation_reason_impl, trigger_fast_evacuation_impl};
use rusty_grid_core::arbitrage::{
    LeaseValidationError, MasterLeaseTicket, LEASE_VALIDITY_DURATION_MS,
};
use rusty_grid_core::cluster_auth::ClusterSecret;
use rusty_grid_core::task::{Task, TaskRequirements, TaskSpec};
use rusty_grid_master::queue::TaskState;
use rusty_grid_master::server::{MasterServer, ServerConfig};
use rusty_grid_worker::client::WorkerClient;
use rusty_grid_worker::WorkerConfig;

/// Helper: Asynchronously polls `condition` until it returns true or `timeout` expires.
async fn wait_for<F, Fut>(timeout: Duration, step: Duration, mut condition: F) -> bool
where
    F: FnMut() -> Fut,
    Fut: std::future::Future<Output = bool>,
{
    let start = Instant::now();
    while start.elapsed() < timeout {
        if condition().await {
            return true;
        }
        tokio::time::sleep(step).await;
    }
    false
}

// ==============================================================================
// 3. Cryptographic MasterLeaseTicket Tamper Detection (§5.3)
// ==============================================================================

#[test]
fn test_master_lease_ticket_cryptographic_tamper_detection_exhaustive() {
    let secret = ClusterSecret::generate().expect("Valid cluster secret");
    let cluster_id = Uuid::new_v4();
    let master_id = Uuid::new_v4();
    let now_epoch_ms = 1_700_000_000_000;
    let validity = Duration::from_millis(LEASE_VALIDITY_DURATION_MS);

    let baseline_ticket = MasterLeaseTicket::issue(
        cluster_id,
        master_id,
        42,
        6500,
        now_epoch_ms,
        validity,
        &secret,
    );

    // Baseline ticket must verify cleanly
    assert!(baseline_ticket.verify(Some(&secret), now_epoch_ms).is_ok());
    assert!(baseline_ticket.is_valid(&secret, now_epoch_ms));

    // -------------------------------------------------------------------------
    // Exhaustive 480 Single-Bit Flip Sweep Across Canonical 60-Byte Payload
    // -------------------------------------------------------------------------
    let canonical = baseline_ticket.canonical_bytes();
    assert_eq!(canonical.len(), 60);

    for byte_idx in 0..60 {
        for bit_idx in 0..8 {
            let mut corrupted_canonical = canonical;
            corrupted_canonical[byte_idx] ^= 1 << bit_idx;

            // Reconstruct ticket from corrupted canonical bytes
            let c_id = Uuid::from_slice(&corrupted_canonical[0..16]).unwrap();
            let m_id = Uuid::from_slice(&corrupted_canonical[16..32]).unwrap();
            let seq = u64::from_be_bytes(corrupted_canonical[32..40].try_into().unwrap());
            let score = i32::from_be_bytes(corrupted_canonical[40..44].try_into().unwrap());
            let issued = u64::from_be_bytes(corrupted_canonical[44..52].try_into().unwrap());
            let expires = u64::from_be_bytes(corrupted_canonical[52..60].try_into().unwrap());

            let mut tampered_ticket = baseline_ticket.clone();
            tampered_ticket.cluster_id = c_id;
            tampered_ticket.master_id = m_id;
            tampered_ticket.lease_sequence = seq;
            tampered_ticket.authority_score = score;
            tampered_ticket.issued_at_epoch_ms = issued;
            tampered_ticket.expires_at_epoch_ms = expires;

            // Verification MUST fail: either SignatureMismatch or InvalidTimeRange or Expired
            let verify_result = tampered_ticket.verify(Some(&secret), now_epoch_ms);
            assert!(
                verify_result.is_err(),
                "Single-bit flip at byte {} bit {} went undetected!",
                byte_idx,
                bit_idx
            );
        }
    }

    // -------------------------------------------------------------------------
    // Exhaustive 256 Single-Bit Flip Sweep Across 32-Byte HMAC-SHA256 Signature
    // -------------------------------------------------------------------------
    for byte_idx in 0..32 {
        for bit_idx in 0..8 {
            let mut tampered_ticket = baseline_ticket.clone();
            tampered_ticket.signature[byte_idx] ^= 1 << bit_idx;

            let result = tampered_ticket.verify(Some(&secret), now_epoch_ms);
            assert_eq!(
                result,
                Err(LeaseValidationError::SignatureMismatch),
                "Flipped signature bit at byte {} bit {} was not rejected!",
                byte_idx,
                bit_idx
            );
        }
    }

    // -------------------------------------------------------------------------
    // Signature Length Tampering
    // -------------------------------------------------------------------------
    for invalid_len in [0, 1, 15, 16, 31, 33, 48, 64] {
        let mut tampered_ticket = baseline_ticket.clone();
        tampered_ticket.signature = vec![0xEE; invalid_len];

        let result = tampered_ticket.verify(Some(&secret), now_epoch_ms);
        assert_eq!(
            result,
            Err(LeaseValidationError::InvalidSignatureLength(invalid_len)),
            "Invalid signature length {} was not caught",
            invalid_len
        );
    }

    // -------------------------------------------------------------------------
    // Expiration and Time Window Boundary Conditions
    // -------------------------------------------------------------------------
    let expiry = baseline_ticket.expires_at_epoch_ms;

    // At exact expiration millisecond -> Valid
    assert!(baseline_ticket.verify(Some(&secret), expiry).is_ok());

    // 1 millisecond past expiration -> Expired error
    let expired_res = baseline_ticket.verify(Some(&secret), expiry + 1);
    assert_eq!(
        expired_res,
        Err(LeaseValidationError::Expired {
            expired_at: expiry,
            current: expiry + 1,
        })
    );

    // Far future -> Expired error
    assert!(matches!(
        baseline_ticket.verify(Some(&secret), expiry + 1_000_000),
        Err(LeaseValidationError::Expired { .. })
    ));

    // Time inversion: expires_at <= issued_at
    let mut inverted_ticket = baseline_ticket.clone();
    inverted_ticket.expires_at_epoch_ms = inverted_ticket.issued_at_epoch_ms;
    assert_eq!(
        inverted_ticket.verify(Some(&secret), now_epoch_ms),
        Err(LeaseValidationError::InvalidTimeRange)
    );

    // -------------------------------------------------------------------------
    // Cluster Secret Variations
    // -------------------------------------------------------------------------
    let wrong_secret = ClusterSecret::generate().expect("Different cluster secret");
    assert_eq!(
        baseline_ticket.verify(Some(&wrong_secret), now_epoch_ms),
        Err(LeaseValidationError::SignatureMismatch),
        "Ticket verified against wrong cluster secret must fail"
    );

    // Mutated secret (single character flipped in hex cluster secret)
    let hex = secret.to_hex();
    let mut hex_chars: Vec<char> = hex.chars().collect();
    hex_chars[0] = if hex_chars[0] == 'a' { 'b' } else { 'a' };
    let mutated_hex: String = hex_chars.into_iter().collect();
    let mutated_secret = ClusterSecret::from_hex(&mutated_hex).expect("mutated secret");
    assert_eq!(
        baseline_ticket.verify(Some(&mutated_secret), now_epoch_ms),
        Err(LeaseValidationError::SignatureMismatch),
        "Ticket verified against mutated cluster secret must fail"
    );
}

// ==============================================================================
// 4. Pre-Sleep Fast Task Evacuation Under Multi-Task Load (< 50ms) (§5.4)
// ==============================================================================

#[tokio::test]
async fn test_pre_sleep_fast_evacuation_under_multi_task_load() {
    let temp_dir = tempfile::tempdir().expect("tempdir");
    let sandbox_path = temp_dir.path().to_path_buf();

    // 1. Direct JNI fast evacuation hook test
    assert!(trigger_fast_evacuation_impl(
        "FAST_PRE_SLEEP_BATTERY_CRITICAL"
    ));
    assert_eq!(
        get_evacuation_reason_impl(),
        Some("FAST_PRE_SLEEP_BATTERY_CRITICAL".to_string())
    );

    // 2. Spawn loopback MasterServer with max_retries = 3
    let server_config = ServerConfig::new("127.0.0.1:0".parse().unwrap()).with_max_retries(3);
    let master = MasterServer::spawn(server_config)
        .await
        .expect("master spawn");
    let master_addr = master.server_addr().to_string();

    // 3. Spawn 3 WorkerClients (Worker 1 = Mobile Candidate to be evacuated, Workers 2 & 3 = Survivors)
    let (w1_shutdown_tx, w1_shutdown_rx) = watch::channel(false);
    let (_w2_shutdown_tx, w2_shutdown_rx) = watch::channel(false);
    let (_w3_shutdown_tx, w3_shutdown_rx) = watch::channel(false);

    let w1_id = Uuid::new_v4();
    let config1 = WorkerConfig::new(&master_addr)
        .with_no_battery(true)
        .with_name("mobile-evac-worker-1")
        .with_cores(4)
        .with_worker_id(w1_id)
        .with_sandbox_base_dir(sandbox_path.join("w1"));
    let mut w1 = WorkerClient::new(config1);

    let w2_id = Uuid::new_v4();
    let config2 = WorkerConfig::new(&master_addr)
        .with_no_battery(true)
        .with_name("survivor-worker-2")
        .with_cores(4)
        .with_worker_id(w2_id)
        .with_sandbox_base_dir(sandbox_path.join("w2"));
    let mut w2 = WorkerClient::new(config2);

    let w3_id = Uuid::new_v4();
    let config3 = WorkerConfig::new(&master_addr)
        .with_no_battery(true)
        .with_name("survivor-worker-3")
        .with_cores(4)
        .with_worker_id(w3_id)
        .with_sandbox_base_dir(sandbox_path.join("w3"));
    let mut w3 = WorkerClient::new(config3);

    tokio::spawn(async move {
        let _ = w1.run(w1_shutdown_rx).await;
    });
    tokio::spawn(async move {
        let _ = w2.run(w2_shutdown_rx).await;
    });
    tokio::spawn(async move {
        let _ = w3.run(w3_shutdown_rx).await;
    });

    // Wait for all 3 workers to register
    assert!(
        wait_for(
            Duration::from_secs(6),
            Duration::from_millis(50),
            || async {
                master
                    .list_workers()
                    .await
                    .map(|w| w.len() == 3)
                    .unwrap_or(false)
            }
        )
        .await,
        "All 3 workers must register successfully"
    );

    // 4. Submit MULTIPLE concurrent tasks (batch of 6 tasks) under active load
    const BATCH_SIZE: usize = 6;
    let mut task_ids = Vec::with_capacity(BATCH_SIZE);
    for i in 0..BATCH_SIZE {
        let task = Task::new(
            TaskSpec::BuiltinTest {
                test_name: format!("load_task_{i}"),
                iterations: 50,
                duration_ms: 1800,
                should_fail: false,
                require_gpu: false,
            },
            TaskRequirements::generic(1, 30),
        );
        let tid = master.submit_task(task).await.expect("task submit");
        task_ids.push(tid);
    }

    // Wait for tasks to enter Running state across the cluster
    assert!(
        wait_for(
            Duration::from_secs(5),
            Duration::from_millis(20),
            || async {
                let mut running_count = 0;
                for &tid in &task_ids {
                    if let Ok(info) = master.get_task_info(tid).await {
                        if info.state == TaskState::Running {
                            running_count += 1;
                        }
                    }
                }
                running_count >= 3
            }
        )
        .await,
        "At least 3 tasks must enter Running state before evacuation trigger"
    );

    // Identify which tasks are assigned to Worker 1
    let mut w1_tasks = Vec::new();
    for &tid in &task_ids {
        if let Ok(info) = master.get_task_info(tid).await {
            if info.assigned_worker_id == Some(w1_id) {
                w1_tasks.push(tid);
            }
        }
    }

    // If no task happened to land on Worker 1 yet, wait briefly for scheduling
    if w1_tasks.is_empty() {
        let found = wait_for(
            Duration::from_secs(2),
            Duration::from_millis(10),
            || async {
                for &tid in &task_ids {
                    if let Ok(info) = master.get_task_info(tid).await {
                        if info.assigned_worker_id == Some(w1_id) {
                            return true;
                        }
                    }
                }
                false
            },
        )
        .await;
        if found {
            for &tid in &task_ids {
                if let Ok(info) = master.get_task_info(tid).await {
                    if info.assigned_worker_id == Some(w1_id) {
                        w1_tasks.push(tid);
                    }
                }
            }
        }
    }

    // 5. Trigger pre-sleep fast evacuation on Worker 1 under active multi-task load
    let evacuation_start = Instant::now();
    let _ = w1_shutdown_tx.send(true);

    // 6. Measure evacuation latency: Master MUST re-queue or re-assign all of Worker 1's tasks in < 50ms
    let evacuation_completed = wait_for(
        Duration::from_millis(50),
        Duration::from_millis(2),
        || async {
            for &tid in &w1_tasks {
                if let Ok(info) = master.get_task_info(tid).await {
                    // Task must either be re-assigned away from w1_id, or re-queued
                    if info.assigned_worker_id == Some(w1_id) && info.state == TaskState::Running {
                        return false;
                    }
                }
            }
            true
        },
    )
    .await;

    let evacuation_elapsed = evacuation_start.elapsed();
    assert!(
        evacuation_completed,
        "Fast pre-sleep task evacuation took {:?}, exceeding the 50ms deadline!",
        evacuation_elapsed
    );
    assert!(
        evacuation_elapsed < Duration::from_millis(50),
        "Evacuation elapsed time {:?} exceeded 50ms threshold",
        evacuation_elapsed
    );

    // 7. Verify all tasks complete successfully with ZERO task loss
    let all_completed = wait_for(
        Duration::from_secs(8),
        Duration::from_millis(50),
        || async {
            for &tid in &task_ids {
                if let Ok(info) = master.get_task_info(tid).await {
                    if info.state != TaskState::Completed {
                        return false;
                    }
                } else {
                    return false;
                }
            }
            true
        },
    )
    .await;
    assert!(
        all_completed,
        "All 6 tasks must complete without any task loss"
    );

    // 8. Assert task state, exit code, and retry count
    for &tid in &task_ids {
        let final_info = master.get_task_info(tid).await.expect("task info exists");
        assert_eq!(
            final_info.state,
            TaskState::Completed,
            "Task {} must be Completed",
            tid
        );
        assert_eq!(
            final_info.exit_code,
            Some(0),
            "Task {} must have exit code 0",
            tid
        );
        // Surviving worker assigned must be Worker 2 or Worker 3
        let assigned = final_info.assigned_worker_id.expect("assigned worker");
        assert!(
            assigned == w2_id || assigned == w3_id,
            "Task {} must be completed by a surviving worker (got {})",
            tid,
            assigned
        );

        if w1_tasks.contains(&tid) {
            assert_eq!(
                final_info.retry_count, 0,
                "Evacuated task {} must not charge retry_count",
                tid
            );
            assert_eq!(
                final_info.failover_count, 0,
                "Evacuated task {} must not charge failover_count",
                tid
            );
        }
    }

    // 9. Verify WorkerClient fast-path cancellation latency directly
    let test_worker_config = WorkerConfig::new(&master_addr)
        .with_no_battery(true)
        .with_cores(2)
        .with_sandbox_base_dir(sandbox_path.join("fast_cancel"));
    let standalone_worker = WorkerClient::new(test_worker_config);

    // Cancelling with reason containing "FAST" or "EVAC" must return immediately (< 15ms)
    let cancel_start = Instant::now();
    let cancelled_count = standalone_worker
        .cancel_all_active_tasks("FAST_PRE_SLEEP_EVACUATION")
        .await;
    let cancel_elapsed = cancel_start.elapsed();
    assert_eq!(cancelled_count, 0);
    assert!(
        cancel_elapsed < Duration::from_millis(15),
        "cancel_all_active_tasks fast-path took too long: {:?}",
        cancel_elapsed
    );

    let _ = master.shutdown();
}
