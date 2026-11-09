//! Wire layout guard for the protocol messages of [`crate::protocol`].
//!
//! `WorkerMessage`, `MasterMessage`, `ClientMessage`, `ClientResponse` and the `InboundMessage`
//! wrapper are serialized through hand-written `Human*` (tagged JSON) and `Binary*` (bincode,
//! positional) mirror enums. This module builds one canonical instance of every variant, every
//! field set to a fixed, non-default value, and checks
//!
//! * that each instance comes back unchanged from both codecs and from the raw JSON read path, so
//!   a field that one mirror or one `From` impl drops or mangles fails here (the values are
//!   non-default because the JSON mirrors fill a missing field from `#[serde(default)]`), and
//! * that the bincode encoding of each instance of the four message enums is the one recorded in
//!   [`WIRE_LAYOUT_FILE`]. Bincode is positional: a field inserted before existing ones, a moved
//!   variant or a changed field type makes an older peer misread the frame, and it changes the
//!   recorded bytes. `InboundMessage` is only round-tripped: no peer sends the wrapped form.
//!
//! A new variant does not compile until it has an arm in the exhaustive `match` of its enum's
//! instance list, and each list is checked against the variants its `Binary*` mirror declares, so
//! a variant cannot be left without a canonical instance either. Every `TaskSpec` variant must
//! travel in one of the messages as well.
//!
//! Non-default values never trip a `skip_serializing_if` predicate, so this module cannot see one
//! on a bincode type; `skip_serializing_if_is_kept_off_bincode_types` in `lib.rs` scans every
//! `crates/core/src/*.rs` for that, so no sparse twins of these instances are needed.

use std::collections::{BTreeMap, BTreeSet, HashMap};
use std::fmt::{Debug, Display, Write as _};
use std::path::{Path, PathBuf};

use serde::de::{DeserializeOwned, Visitor};
use serde::Serialize;
use uuid::Uuid;

use crate::arbitrage::MasterLeaseTicket;
use crate::blob::{BlobFile, BlobId, InputSource, TaskInput};
use crate::blob_transfer::{BlobReply, BlobRequest};
use crate::capabilities::{BatteryTier, MobileCapabilities, ThermalTier, WorkerCapabilities};
use crate::mapreduce::{MapFunctionSpec, MapReduceJobSpec, MapReduceResult, ReduceFunctionSpec};
use crate::protocol::{
    deserialize_message, serialize_message, ClientMessage, ClientResponse, InboundMessage,
    MasterMessage, ProtocolCapabilities, WireCodec, WorkerMessage, PROTOCOL_VERSION_CURRENT,
    PROTOCOL_VERSION_MIN_COMPATIBLE, WIRE_FORMAT_BINCODE, WIRE_FORMAT_JSON,
};
use crate::task::{
    AutoRigSpec, Bytes, CheckpointData, Generative3DSpec, OutputArtifact, StagedFile, Task, TaskId,
    TaskRequirements, TaskResult, TaskSpec, TaskStatus, WorkspaceAffinity, WorkspaceRef,
    WorkspaceSource,
};

/// The recorded encodings, relative to the crate root.
const WIRE_LAYOUT_FILE: &str = "tests/data/wire_layout.txt";
/// Set to `1` to rewrite [`WIRE_LAYOUT_FILE`] from the current encodings instead of comparing.
const UPDATE_ENV: &str = "UPDATE_WIRE_LAYOUT";
/// Optional git ref to compare [`WIRE_LAYOUT_FILE`] against in CI (`push` compares with
/// `github.event.before`, `pull_request` compares with `github.base_ref`).
const WIRE_LAYOUT_BASELINE_REF_ENV: &str = "WIRE_LAYOUT_BASELINE_REF";
/// Dedicated ref fetched by CI before running the wire-layout baseline check.
const CI_WIRE_LAYOUT_BASELINE_REF: &str = "refs/remotes/ci/wire-layout-base";
/// The command that rewrites [`WIRE_LAYOUT_FILE`], for a POSIX shell and for PowerShell.
const REGENERATE: &str =
    "    UPDATE_WIRE_LAYOUT=1 cargo test -p rusty_grid_core --lib wire_layout\n    \
     $env:UPDATE_WIRE_LAYOUT=1; cargo test -p rusty_grid_core --lib wire_layout   (PowerShell)";

const HEADER: &str = "\
# Bincode wire layout of the protocol messages (crates/core/src/protocol.rs): one canonical
# instance per variant of WorkerMessage, MasterMessage, ClientMessage and ClientResponse, built in
# crates/core/src/wire_layout_tests.rs, as `<Enum>::<Variant> <hex>` (the frame payload after the
# 0x02 format byte), sorted by name. Read the test's failure message, then regenerate with
#   UPDATE_WIRE_LAYOUT=1 cargo test -p rusty_grid_core --lib wire_layout
#   $env:UPDATE_WIRE_LAYOUT=1; cargo test -p rusty_grid_core --lib wire_layout   (PowerShell)
";

// ---------------------------------------------------------------------------------------------
// Fixed values. Within a message, fields of the same type (nested ones included) hold different
// values, so a mirror or `From` impl that reads one in place of another fails the round trip or
// changes the recorded bytes. A bool has two values only: see `worker_capabilities`.
// ---------------------------------------------------------------------------------------------

/// 2026-01-01T00:00:00Z in seconds since the Unix epoch.
const EPOCH_SECS: u64 = 1_767_225_600;

/// Not `PROTOCOL_VERSION_CURRENT`, which JSON fills in for a missing `protocol_version`.
const PROTOCOL_VERSION: u32 = 0x0a0b_0c0d;

/// A fixed version-4 UUID per `n`.
fn fixed_uuid(n: u8) -> Uuid {
    Uuid::from_u128(0x7769_7265_0000_4000_8000_0000_0000_0000 | u128::from(n))
}

/// A fixed 32-byte value (blob id, digest, signature) per `seed`.
fn fixed_bytes32(seed: u8) -> [u8; 32] {
    std::array::from_fn(|i| seed.wrapping_add(i as u8))
}

fn worker_id() -> Uuid {
    fixed_uuid(0x01)
}

fn task_id() -> TaskId {
    TaskId(fixed_uuid(0x02))
}

fn job_id() -> Uuid {
    fixed_uuid(0x05)
}

fn blob_id(seed: u8) -> BlobId {
    BlobId(fixed_bytes32(seed))
}

/// Not `ProtocolCapabilities::default()` (every supported bit), which JSON fills in for a missing
/// `features`. The bit values are wire format too, so they are pinned with the rest.
fn features() -> ProtocolCapabilities {
    ProtocolCapabilities::MOBILE_TELEMETRY | ProtocolCapabilities::BLOB_TRANSFER
}

/// Capabilities `n` (0, 1 or 2). The mirrors carry its four bools side by side and a bool has two
/// values only, so the three instances take turns: each bool is `true` in one of them (a field a
/// mirror drops falls back to `false`), and every pair of bools differs in one of them.
fn worker_capabilities(n: usize) -> WorkerCapabilities {
    // [has_gpu, is_simulated_gpu, has_blender, has_cuda]
    const BOOLS: [[bool; 4]; 3] = [
        [true, true, true, true],
        [true, true, false, false],
        [true, false, true, false],
    ];
    let [has_gpu, is_simulated_gpu, has_blender, has_cuda] = BOOLS[n];
    WorkerCapabilities {
        name: format!("wire-worker-{n}"),
        cpu_cores: 12,
        ram_mb: 32_768,
        has_gpu,
        is_simulated_gpu,
        gpu_device_name: Some("Adreno 740".into()),
        tags: vec!["zone:lab".into(), "fast-disk".into()],
        mobile: Some(MobileCapabilities {
            os_version: "Android 14 (API 34)".into(),
            soc_model: "Snapdragon 8 Gen 2".into(),
            battery_pct: Some(67),
            is_charging: Some(true),
            thermal_throttled: true,
        }),
        has_blender,
        blender_version: Some("Blender 4.5.4 LTS".into()),
        vram_mb: Some(8_192),
        has_cuda,
    }
}

/// Checkpoint `n`, built literally: `CheckpointData::new` reads the clock.
fn checkpoint(n: u8) -> CheckpointData {
    CheckpointData {
        sequence: 100 + u64::from(n),
        // Not UTF-8, so JSON carries it as an array rather than a string.
        delta_state: Bytes::from(vec![0x00, b'd', n, 0xff]),
        timestamp: EPOCH_SECS + 1_000 + u64::from(n),
    }
}

/// A task with every field set, `n` giving it its own id and checkpoint. Built literally:
/// `Task::new` draws a random id and reads the clock.
fn task(n: u8, spec: TaskSpec) -> Task {
    Task {
        id: TaskId(fixed_uuid(0x10 + n)),
        spec,
        requirements: TaskRequirements {
            cpu_cores: 3,
            ram_mb: 2_048,
            // Not `true`: `ClientMessage::SubmitTask` carries one of these tasks next to its
            // `wait: true`, and `TaskRequirements` has no mirror that could drop the field.
            gpu_required: false,
            timeout_secs: 90,
            max_retries: Some(4),
        },
        created_at_utc: EPOCH_SECS,
        tags: vec!["require:os=linux".into(), "batch-7".into()],
        latest_checkpoint: Some(checkpoint(n)),
        staged_files: vec![StagedFile {
            path: "data/input.csv".into(),
            size: 8,
            sha256: fixed_bytes32(0x20),
            executable: true,
            data: Bytes::from_static(b"a,b\n1,2\n"),
        }],
        expected_outputs: vec!["out/report.txt".into()],
        inputs: vec![
            TaskInput {
                path: "src".into(),
                source: InputSource::Tree {
                    manifest: blob_id(0x40),
                    manifest_size: 512,
                },
            },
            TaskInput {
                path: "bin/tool".into(),
                source: InputSource::File {
                    id: blob_id(0x60),
                    size: 4_096,
                    executable: true,
                },
            },
        ],
        output_globs: vec!["target/**/*.rlib".into()],
        workspace: Some(WorkspaceRef {
            name: "oxideswarm-agent.1".into(),
            source: Some(WorkspaceSource::Tree {
                manifest: blob_id(0x70),
                manifest_size: 2_048,
            }),
        }),
    }
}

// One `TaskSpec` per variant, spread over the messages that carry a `Task`, so the nested
// `HumanTaskSpec` / `BinaryTaskSpec` mirrors are covered as well. Every `HashMap` has one entry:
// iteration order is random per process, and the encoding must not be.

fn command_spec() -> TaskSpec {
    TaskSpec::Command {
        program: "cargo".into(),
        args: vec!["build".into(), "--release".into()],
        env: HashMap::from([("RUST_LOG".to_string(), "debug".to_string())]),
        working_dir: Some(PathBuf::from("/srv/oxide/work")),
        stdin: Some(vec![0x01, 0x02, 0x03]),
    }
}

fn shell_spec() -> TaskSpec {
    TaskSpec::ShellScript {
        script: "echo \"$GREETING\" > out/report.txt".into(),
        interpreter: Some("bash".into()),
        env: HashMap::from([("GREETING".to_string(), "hello".to_string())]),
    }
}

fn rust_spec() -> TaskSpec {
    TaskSpec::RustCompilation {
        crate_name: "wire_probe".into(),
        source_files: HashMap::from([(
            "src/lib.rs".to_string(),
            "pub fn probe() -> u32 { 7 }".to_string(),
        )]),
        compiler_flags: vec!["--crate-type".into(), "lib".into()],
        target_dir: Some(PathBuf::from("target/wire")),
    }
}

fn gpu_spec() -> TaskSpec {
    TaskSpec::GpuCompute {
        kernel_name: "gemm".into(),
        input_data: vec![9, 8, 7, 6],
        // The JSON defaults are 16 and 64.
        work_group_size: 32,
        simulated_matrix_dim: 256,
    }
}

/// Two bools side by side, like [`worker_capabilities`]: the instances `(true, true)` and
/// `(false, true)` set each of them and tell them apart.
fn builtin_spec(should_fail: bool, require_gpu: bool) -> TaskSpec {
    TaskSpec::BuiltinTest {
        // The JSON defaults are "hash_compute" and 100_000.
        test_name: "sleep_spin".into(),
        iterations: 1_234,
        duration_ms: 50,
        should_fail,
        require_gpu,
    }
}

fn generative_spec() -> TaskSpec {
    TaskSpec::Generative3D(Generative3DSpec {
        prompt: Some("a low-poly lantern".into()),
        image_data: Some(Bytes::from_static(b"\x89PNG\r\n\x1a\n")),
        // The JSON defaults are "standard" and `true`.
        quality: "high".into(),
        engine_preference: Some("sdf".into()),
        export_texture: false,
    })
}

fn auto_rig_spec() -> TaskSpec {
    TaskSpec::AutoRig(AutoRigSpec {
        input_mesh: Bytes::from_static(b"glTF\x02\x00\x00\x00\xff"),
        // The JSON defaults are "biped" and ["idle", "walk", "turnaround"].
        rig_preset: "quadruped".into(),
        embed_animations: vec!["run".into()],
        preferred_provider: Some("blender".into()),
    })
}

/// Its `task_id` is not [`task_id`]: `ClientResponse::TaskCompleted` carries both.
fn task_result() -> TaskResult {
    TaskResult {
        worker_id: worker_id(),
        task_id: TaskId(fixed_uuid(0x06)),
        exit_code: -3,
        stdout: Bytes::from_static(b"compiled 3 crates\n"),
        stderr: Bytes::from_static(b"warning: \xff unused\n"),
        execution_time_ms: 1_250,
        is_gpu_executed: true,
        device_name: Some("Adreno 740".into()),
        error: Some("exit status -3".into()),
        asset_bytes: Some(Bytes::from_static(b"glb\x00asset")),
        artifacts: vec![OutputArtifact {
            path: "out/report.txt".into(),
            size: 6,
            sha256: fixed_bytes32(0x80),
            data: Bytes::from_static(b"report"),
        }],
        outputs: vec![BlobFile {
            path: "target/release/libwire_probe.rlib".into(),
            id: blob_id(0xa0),
            size: 2_048,
            executable: true,
        }],
    }
}

fn lease_ticket() -> MasterLeaseTicket {
    MasterLeaseTicket {
        cluster_id: fixed_uuid(0x03),
        master_id: fixed_uuid(0x04),
        lease_sequence: 41,
        authority_score: 8_500,
        issued_at_epoch_ms: EPOCH_SECS * 1_000,
        expires_at_epoch_ms: EPOCH_SECS * 1_000 + 30_000,
        signature: fixed_bytes32(0x90).to_vec(),
    }
}

/// Built literally: `MapReduceJobSpec::new` draws a random job id.
fn map_reduce_job() -> MapReduceJobSpec {
    MapReduceJobSpec {
        job_id: job_id(),
        name: "word-count".into(),
        input_data: vec!["apple banana".into(), "banana cherry".into()],
        mapper: MapFunctionSpec::Command {
            program: "wc".into(),
            args: vec!["-w".into()],
        },
        reducer: ReduceFunctionSpec::Builtin {
            operator: "sum".into(),
        },
        partition_count: 4,
        reducer_count: 2,
        timeout_secs: 120,
    }
}

fn map_reduce_result() -> MapReduceResult {
    MapReduceResult {
        job_id: job_id(),
        status: "Completed".into(),
        output: HashMap::from([("banana".to_string(), serde_json::json!(2))]),
        map_tasks_total: 4,
        map_tasks_completed: 3,
        reduce_tasks_total: 2,
        reduce_tasks_completed: 1,
        execution_time_ms: 1_500,
        error: Some("1 map task retried".into()),
    }
}

// ---------------------------------------------------------------------------------------------
// One canonical instance per variant, in declaration order. Where a field is itself an enum, the
// instance uses its last variant when that costs nothing (an index shift before it then shows).
// ---------------------------------------------------------------------------------------------

fn worker_messages() -> Vec<(&'static str, WorkerMessage)> {
    let all = vec![
        (
            "Register",
            WorkerMessage::Register {
                worker_id: worker_id(),
                capabilities: worker_capabilities(0),
                protocol_version: PROTOCOL_VERSION,
                features: features(),
            },
        ),
        (
            "Heartbeat",
            WorkerMessage::Heartbeat {
                worker_id: worker_id(),
                timestamp: EPOCH_SECS + 5,
                active_tasks: 3,
                // Exact in binary, so JSON gives back the same `f32`.
                cpu_usage_pct: 42.5,
                ram_available_mb: 6_144,
            },
        ),
        (
            "TaskProgress",
            WorkerMessage::TaskProgress {
                worker_id: worker_id(),
                task_id: task_id(),
                status: TaskStatus::Running,
            },
        ),
        // Through `From<TaskResult>`, which is how the worker sends its results.
        ("TaskResult", WorkerMessage::from(task_result())),
        (
            "Checkpoint",
            WorkerMessage::Checkpoint {
                task_id: task_id(),
                sequence: 9,
                delta_state: Bytes::from_static(b"\x00\x02state\xfe"),
            },
        ),
        (
            "Disconnecting",
            WorkerMessage::Disconnecting {
                worker_id: worker_id(),
                reason: "maintenance window".into(),
            },
        ),
        (
            "MobileTelemetry",
            WorkerMessage::MobileTelemetry {
                worker_id: worker_id(),
                battery_pct: Some(14),
                is_charging: Some(false),
                thermal_throttled: true,
                battery_temperature: Some(38.25),
                network_type: Some("cellular".into()),
                battery_tier: Some(BatteryTier::Critical),
                thermal_tier: Some(ThermalTier::Critical),
            },
        ),
        (
            "ClaimMasterRole",
            WorkerMessage::ClaimMasterRole {
                ticket: lease_ticket(),
            },
        ),
        (
            "UnknownVariant",
            WorkerMessage::UnknownVariant {
                tag: 4_242,
                raw_bytes: vec![0xde, 0xad, 0xbe, 0xef],
            },
        ),
        (
            "BlobRequest",
            WorkerMessage::BlobRequest {
                request_id: 77,
                request: BlobRequest::Read {
                    id: blob_id(0xc0),
                    offset: 4_194_304,
                    max_len: 65_536,
                },
            },
        ),
        (
            "ResumeSession",
            WorkerMessage::ResumeSession {
                worker_id: worker_id(),
                running: vec![task_id(), TaskId(fixed_uuid(0x06))],
            },
        ),
    ];
    check_instances("WorkerMessage", &all, |msg| match msg {
        WorkerMessage::Register { .. } => "Register",
        WorkerMessage::Heartbeat { .. } => "Heartbeat",
        WorkerMessage::TaskProgress { .. } => "TaskProgress",
        WorkerMessage::TaskResult { .. } => "TaskResult",
        WorkerMessage::Checkpoint { .. } => "Checkpoint",
        WorkerMessage::Disconnecting { .. } => "Disconnecting",
        WorkerMessage::MobileTelemetry { .. } => "MobileTelemetry",
        WorkerMessage::ClaimMasterRole { .. } => "ClaimMasterRole",
        WorkerMessage::UnknownVariant { .. } => "UnknownVariant",
        WorkerMessage::BlobRequest { .. } => "BlobRequest",
        WorkerMessage::ResumeSession { .. } => "ResumeSession",
    });
    all
}

fn master_messages() -> Vec<(&'static str, MasterMessage)> {
    let all = vec![
        (
            "RegisterAck",
            MasterMessage::RegisterAck {
                accepted: true,
                worker_id: worker_id(),
                heartbeat_interval_secs: 4,
                protocol_version: PROTOCOL_VERSION,
                features: features(),
                message: Some("features=mobile-telemetry,blobs".into()),
            },
        ),
        (
            "HeartbeatAck",
            MasterMessage::HeartbeatAck {
                timestamp: EPOCH_SECS + 6,
            },
        ),
        (
            "AssignTask",
            MasterMessage::AssignTask {
                task: task(1, command_spec()),
            },
        ),
        (
            "AssignTaskWithCheckpoint",
            MasterMessage::AssignTaskWithCheckpoint {
                task: task(2, shell_spec()),
                // Not the task's own `checkpoint(2)`, so neither can stand in for the other.
                latest_checkpoint: Some(checkpoint(0x20)),
            },
        ),
        (
            "CancelTask",
            MasterMessage::CancelTask {
                task_id: task_id(),
                reason: Some("cancelled by the operator".into()),
            },
        ),
        (
            "Shutdown",
            MasterMessage::Shutdown {
                reason: "cluster evacuation".into(),
                grace_period_secs: Some(15),
            },
        ),
        (
            "RoleHandoverAck",
            MasterMessage::RoleHandoverAck {
                accepted: true,
                reason: Some("lease 41 accepted".into()),
                task_queue_snapshot: vec![
                    task(3, gpu_spec()),
                    task(4, builtin_spec(true, true)),
                    task(5, generative_spec()),
                    task(6, auto_rig_spec()),
                    task(8, builtin_spec(false, true)),
                ],
                connected_workers: vec![worker_capabilities(1)],
            },
        ),
        (
            "UnknownVariant",
            MasterMessage::UnknownVariant {
                tag: 4_343,
                raw_bytes: vec![0xca, 0xfe],
            },
        ),
        (
            "BlobReply",
            MasterMessage::BlobReply {
                request_id: 77,
                reply: BlobReply::Chunk {
                    id: blob_id(0xc0),
                    offset: 4_194_304,
                    total_size: 4_259_840,
                    data: Bytes::from_static(b"\x00chunk\xff"),
                },
            },
        ),
    ];
    check_instances("MasterMessage", &all, |msg| match msg {
        MasterMessage::RegisterAck { .. } => "RegisterAck",
        MasterMessage::HeartbeatAck { .. } => "HeartbeatAck",
        MasterMessage::AssignTask { .. } => "AssignTask",
        MasterMessage::AssignTaskWithCheckpoint { .. } => "AssignTaskWithCheckpoint",
        MasterMessage::CancelTask { .. } => "CancelTask",
        MasterMessage::Shutdown { .. } => "Shutdown",
        MasterMessage::RoleHandoverAck { .. } => "RoleHandoverAck",
        MasterMessage::UnknownVariant { .. } => "UnknownVariant",
        MasterMessage::BlobReply { .. } => "BlobReply",
    });
    all
}

fn client_messages() -> Vec<(&'static str, ClientMessage)> {
    let all = vec![
        (
            "SubmitTask",
            ClientMessage::SubmitTask {
                task: task(7, rust_spec()),
                // The JSON default is `false`.
                wait: true,
            },
        ),
        (
            "GetTaskStatus",
            ClientMessage::GetTaskStatus { task_id: task_id() },
        ),
        (
            "CancelTask",
            ClientMessage::CancelTask { task_id: task_id() },
        ),
        ("ClusterStatus", ClientMessage::ClusterStatus),
        ("ListWorkers", ClientMessage::ListWorkers),
        (
            "SubmitMapReduce",
            ClientMessage::SubmitMapReduce {
                job: map_reduce_job(),
            },
        ),
        (
            "GetTaskResult",
            ClientMessage::GetTaskResult { task_id: task_id() },
        ),
        (
            "BlobRequest",
            ClientMessage::BlobRequest {
                request: BlobRequest::Write {
                    id: blob_id(0xe0),
                    total_size: 10,
                    offset: 4,
                    data: Bytes::from_static(b"\x00\x9fwire"),
                },
            },
        ),
        ("Hello", ClientMessage::Hello),
        ("ListWorkspaces", ClientMessage::ListWorkspaces),
        (
            "Enroll",
            ClientMessage::Enroll {
                invite_id: "0102030405060708090a0b0c0d0e0f10".into(),
                proof: crate::noise_auth::RedactedBytes32([0xab; 32]),
                name: "dev-box".into(),
                os: "linux".into(),
                arch: "x86_64".into(),
            },
        ),
        (
            "CreateInvite",
            ClientMessage::CreateInvite {
                roles: vec![
                    crate::noise_auth::MemberRole::Worker,
                    crate::noise_auth::MemberRole::Client,
                ],
                max_uses: 3,
                expires_in_secs: 86_400,
                require_approval: true,
            },
        ),
        ("ListInvites", ClientMessage::ListInvites),
        (
            "RevokeInvite",
            ClientMessage::RevokeInvite {
                invite_id: "01020304".into(),
            },
        ),
        ("ListMembers", ClientMessage::ListMembers),
        (
            "ListPendingEnrollments",
            ClientMessage::ListPendingEnrollments,
        ),
        (
            "ApproveEnrollment",
            ClientMessage::ApproveEnrollment {
                pending_id: "aabbccdd".into(),
            },
        ),
        (
            "RejectEnrollment",
            ClientMessage::RejectEnrollment {
                pending_id: "aabbccdd".into(),
            },
        ),
        (
            "RevokeMember",
            ClientMessage::RevokeMember {
                target: "dev-box".into(),
            },
        ),
        (
            "RenameMember",
            ClientMessage::RenameMember {
                target: "dev-box".into(),
                new_name: "gpu-rig".into(),
            },
        ),
    ];
    check_instances("ClientMessage", &all, |msg| match msg {
        ClientMessage::SubmitTask { .. } => "SubmitTask",
        ClientMessage::GetTaskStatus { .. } => "GetTaskStatus",
        ClientMessage::CancelTask { .. } => "CancelTask",
        ClientMessage::ClusterStatus => "ClusterStatus",
        ClientMessage::ListWorkers => "ListWorkers",
        ClientMessage::SubmitMapReduce { .. } => "SubmitMapReduce",
        ClientMessage::GetTaskResult { .. } => "GetTaskResult",
        ClientMessage::BlobRequest { .. } => "BlobRequest",
        ClientMessage::Hello => "Hello",
        ClientMessage::ListWorkspaces => "ListWorkspaces",
        ClientMessage::Enroll { .. } => "Enroll",
        ClientMessage::CreateInvite { .. } => "CreateInvite",
        ClientMessage::ListInvites => "ListInvites",
        ClientMessage::RevokeInvite { .. } => "RevokeInvite",
        ClientMessage::ListMembers => "ListMembers",
        ClientMessage::ListPendingEnrollments => "ListPendingEnrollments",
        ClientMessage::ApproveEnrollment { .. } => "ApproveEnrollment",
        ClientMessage::RejectEnrollment { .. } => "RejectEnrollment",
        ClientMessage::RevokeMember { .. } => "RevokeMember",
        ClientMessage::RenameMember { .. } => "RenameMember",
    });
    all
}

fn client_responses() -> Vec<(&'static str, ClientResponse)> {
    let all = vec![
        (
            "TaskSubmitted",
            ClientResponse::TaskSubmitted { task_id: task_id() },
        ),
        (
            "TaskCompleted",
            ClientResponse::TaskCompleted {
                task_id: task_id(),
                result: task_result(),
            },
        ),
        (
            "TaskStatusInfo",
            ClientResponse::TaskStatusInfo {
                task_id: task_id(),
                status: TaskStatus::Cancelled,
                state_name: "Cancelled".into(),
                assigned_worker: Some(worker_id()),
                error: Some("cancelled by the operator".into()),
            },
        ),
        (
            "TaskCancelled",
            ClientResponse::TaskCancelled {
                task_id: task_id(),
                success: true,
            },
        ),
        (
            "ClusterStatus",
            ClientResponse::ClusterStatus {
                total_tasks: 11,
                pending_tasks: 3,
                running_tasks: 2,
                completed_tasks: 5,
                failed_tasks: 1,
                workers: vec![worker_capabilities(2)],
                active_bytes: 601_029,
                max_active_bytes: 268_435_456,
                store_error: Some("cannot sync journal.bin: Input/output error".into()),
            },
        ),
        (
            "WorkerList",
            ClientResponse::WorkerList {
                workers: vec![worker_capabilities(1), worker_capabilities(2)],
            },
        ),
        (
            "MapReduceSubmitted",
            ClientResponse::MapReduceSubmitted { job_id: job_id() },
        ),
        (
            "MapReduceCompleted",
            ClientResponse::MapReduceCompleted {
                result: map_reduce_result(),
            },
        ),
        (
            "Error",
            ClientResponse::Error {
                message: "unknown task id".into(),
            },
        ),
        (
            "BlobReply",
            ClientResponse::BlobReply {
                reply: BlobReply::Written {
                    id: blob_id(0xe0),
                    received: 10,
                    complete: true,
                },
            },
        ),
        (
            "Hello",
            ClientResponse::Hello {
                min_compatible_version: PROTOCOL_VERSION_MIN_COMPATIBLE,
            },
        ),
        (
            "Workspaces",
            ClientResponse::Workspaces {
                workspaces: vec![WorkspaceAffinity {
                    name: "oxideswarm-agent.1".into(),
                    worker: worker_id(),
                    last_used_utc: EPOCH_SECS + 42,
                }],
            },
        ),
        (
            "EnrollPending",
            ClientResponse::EnrollPending {
                pending_id: "0011223344556677".into(),
            },
        ),
        ("EnrollKeepalive", ClientResponse::EnrollKeepalive),
        (
            "Enrolled",
            ClientResponse::Enrolled {
                member_id: "0123456789abcdef".into(),
                roles: vec![crate::noise_auth::MemberRole::Worker],
            },
        ),
        (
            "EnrollRejected",
            ClientResponse::EnrollRejected {
                message: "invite has expired".into(),
            },
        ),
        (
            "InviteCreated",
            ClientResponse::InviteCreated {
                invite_id: "0102030405060708090a0b0c0d0e0f10".into(),
                invite_secret: crate::noise_auth::RedactedBytes32([0x5a; 32]),
                roles: vec![crate::noise_auth::MemberRole::Worker],
                max_uses: 1,
                expires_at: EPOCH_SECS + 3600,
                require_approval: true,
                master_key: crate::noise_auth::PublicKey::from_bytes([0x11; 32]),
                p2p_ticket: Some("node-ticket-example".into()),
            },
        ),
        (
            "InvitesList",
            ClientResponse::InvitesList {
                invites: vec![crate::noise_auth::InviteSummary {
                    invite_id: "0102030405060708090a0b0c0d0e0f10".into(),
                    roles: vec![crate::noise_auth::MemberRole::Worker],
                    uses_remaining: 1,
                    max_uses: 1,
                    expires_at: EPOCH_SECS + 3600,
                    require_approval: true,
                    revoked: false,
                }],
            },
        ),
        (
            "InviteRevoked",
            ClientResponse::InviteRevoked {
                invite_id: "0102030405060708090a0b0c0d0e0f10".into(),
            },
        ),
        (
            "MembersList",
            ClientResponse::MembersList {
                members: vec![crate::noise_auth::MemberEntry {
                    id: "0123456789abcdef".into(),
                    name: "dev-box".into(),
                    public_key: crate::noise_auth::PublicKey::from_bytes([0x22; 32]),
                    roles: vec![crate::noise_auth::MemberRole::Worker],
                    enrolled_at: Some(EPOCH_SECS),
                    last_seen: Some(EPOCH_SECS + 10),
                    via_invite: Some("01020304".into()),
                }],
            },
        ),
        (
            "PendingEnrollmentsList",
            ClientResponse::PendingEnrollmentsList {
                pending: vec![crate::noise_auth::PendingEnrollmentInfo {
                    pending_id: "0011223344556677".into(),
                    member_id: "0123456789abcdef".into(),
                    public_key: crate::noise_auth::PublicKey::from_bytes([0x22; 32]),
                    name: "dev-box".into(),
                    os: "linux".into(),
                    arch: "x86_64".into(),
                    roles: vec![crate::noise_auth::MemberRole::Worker],
                    invite_id: "0102030405060708090a0b0c0d0e0f10".into(),
                    pairing_code: crate::noise_auth::RedactedString("123456".into()),
                    requested_at: EPOCH_SECS,
                    expires_at: EPOCH_SECS + 600,
                }],
            },
        ),
        (
            "EnrollmentApproved",
            ClientResponse::EnrollmentApproved {
                pending_id: "0011223344556677".into(),
                member_id: "0123456789abcdef".into(),
                name: "dev-box".into(),
                roles: vec![crate::noise_auth::MemberRole::Worker],
            },
        ),
        (
            "EnrollmentRejected",
            ClientResponse::EnrollmentRejected {
                pending_id: "0011223344556677".into(),
            },
        ),
        (
            "MemberRevoked",
            ClientResponse::MemberRevoked {
                member_id: "0123456789abcdef".into(),
                disconnected_sessions: 2,
            },
        ),
        (
            "MemberRenamed",
            ClientResponse::MemberRenamed {
                member_id: "0123456789abcdef".into(),
                name: "gpu-rig".into(),
            },
        ),
    ];
    check_instances("ClientResponse", &all, |msg| match msg {
        ClientResponse::TaskSubmitted { .. } => "TaskSubmitted",
        ClientResponse::TaskCompleted { .. } => "TaskCompleted",
        ClientResponse::TaskStatusInfo { .. } => "TaskStatusInfo",
        ClientResponse::TaskCancelled { .. } => "TaskCancelled",
        ClientResponse::ClusterStatus { .. } => "ClusterStatus",
        ClientResponse::WorkerList { .. } => "WorkerList",
        ClientResponse::MapReduceSubmitted { .. } => "MapReduceSubmitted",
        ClientResponse::MapReduceCompleted { .. } => "MapReduceCompleted",
        ClientResponse::Error { .. } => "Error",
        ClientResponse::BlobReply { .. } => "BlobReply",
        ClientResponse::Hello { .. } => "Hello",
        ClientResponse::Workspaces { .. } => "Workspaces",
        ClientResponse::EnrollPending { .. } => "EnrollPending",
        ClientResponse::EnrollKeepalive => "EnrollKeepalive",
        ClientResponse::Enrolled { .. } => "Enrolled",
        ClientResponse::EnrollRejected { .. } => "EnrollRejected",
        ClientResponse::InviteCreated { .. } => "InviteCreated",
        ClientResponse::InvitesList { .. } => "InvitesList",
        ClientResponse::InviteRevoked { .. } => "InviteRevoked",
        ClientResponse::MembersList { .. } => "MembersList",
        ClientResponse::PendingEnrollmentsList { .. } => "PendingEnrollmentsList",
        ClientResponse::EnrollmentApproved { .. } => "EnrollmentApproved",
        ClientResponse::EnrollmentRejected { .. } => "EnrollmentRejected",
        ClientResponse::MemberRevoked { .. } => "MemberRevoked",
        ClientResponse::MemberRenamed { .. } => "MemberRenamed",
    });
    all
}

fn inbound_messages() -> Vec<(&'static str, InboundMessage)> {
    let all = vec![
        // What a worker opens its connection with, and the most common client request.
        (
            "Worker",
            InboundMessage::Worker(canonical(worker_messages(), "Register")),
        ),
        (
            "Client",
            InboundMessage::Client(canonical(client_messages(), "SubmitTask")),
        ),
    ];
    check_instances("InboundMessage", &all, |msg| match msg {
        InboundMessage::Worker(_) => "Worker",
        InboundMessage::Client(_) => "Client",
    });
    all
}

/// Every worker and client message inside an `InboundMessage`, labelled `Worker(<variant>)` and
/// `Client(<variant>)`.
fn wrapped_inbound_messages() -> Vec<(String, InboundMessage)> {
    let workers = worker_messages()
        .into_iter()
        .map(|(variant, msg)| (format!("Worker({variant})"), InboundMessage::Worker(msg)));
    let clients = client_messages()
        .into_iter()
        .map(|(variant, msg)| (format!("Client({variant})"), InboundMessage::Client(msg)));
    workers.chain(clients).collect()
}

/// The canonical instance labelled `variant`.
fn canonical<M>(instances: Vec<(&'static str, M)>, variant: &str) -> M {
    instances
        .into_iter()
        .find(|(label, _)| *label == variant)
        .map(|(_, msg)| msg)
        .unwrap_or_else(|| panic!("no canonical instance labelled {variant}"))
}

// ---------------------------------------------------------------------------------------------
// Coverage: an instance list must name exactly the variants of the enum.
// ---------------------------------------------------------------------------------------------

/// Checks an instance list against its enum: the labels are exactly the variants the `Binary*`
/// mirror declares, in declaration order; each instance is the variant its label names (per the
/// exhaustive `match` in `variant_of`); and its bincode encoding starts with its position as the
/// variant index.
fn check_instances<M: Serialize + DeserializeOwned>(
    enum_name: &str,
    instances: &[(&'static str, M)],
    variant_of: fn(&M) -> &'static str,
) {
    let labels: Vec<&str> = instances.iter().map(|(label, _)| *label).collect();
    assert_eq!(
        labels,
        binary_variant_names::<M>(),
        "{enum_name}: the canonical instances must be one per variant, in declaration order; \
         add an instance for every new variant"
    );
    for (index, (label, msg)) in instances.iter().enumerate() {
        assert_eq!(
            variant_of(msg),
            *label,
            "{enum_name}::{label}: the instance is another variant"
        );
        let frame = bincode_frame(&format!("{enum_name}::{label}"), msg);
        let encoded_index = u32::from_le_bytes(frame[1..5].try_into().expect("4 bytes"));
        assert_eq!(
            encoded_index as usize, index,
            "{enum_name}::{label}: encoded with another bincode variant index"
        );
    }
}

/// The variant names a `Binary*` mirror declares, in declaration (that is, bincode index) order.
/// The mirrors are private to `protocol.rs`, so this asks the public `Deserialize` impl: given a
/// deserializer that is not human readable, it hands over to the mirror's derived impl, which
/// passes its variant names to `deserialize_enum`.
fn binary_variant_names<M: DeserializeOwned>() -> Vec<&'static str> {
    let mut probe = VariantNameProbe(&[]);
    // The probe decodes nothing, so this always fails, after the names are recorded.
    let _ = M::deserialize(&mut probe);
    probe.0.to_vec()
}

struct VariantNameProbe(&'static [&'static str]);

impl<'de> serde::Deserializer<'de> for &mut VariantNameProbe {
    type Error = serde::de::value::Error;

    fn is_human_readable(&self) -> bool {
        false
    }

    fn deserialize_enum<V: Visitor<'de>>(
        self,
        _name: &'static str,
        variants: &'static [&'static str],
        _visitor: V,
    ) -> Result<V::Value, Self::Error> {
        self.0 = variants;
        Err(serde::de::Error::custom("variant names recorded"))
    }

    fn deserialize_any<V: Visitor<'de>>(self, _visitor: V) -> Result<V::Value, Self::Error> {
        Err(serde::de::Error::custom(
            "the probe only reads enum variant names",
        ))
    }

    serde::forward_to_deserialize_any! {
        bool i8 i16 i32 i64 i128 u8 u16 u32 u64 u128 f32 f64 char str string bytes byte_buf
        option unit unit_struct newtype_struct seq tuple tuple_struct map struct identifier
        ignored_any
    }
}

/// Every `Task` the canonical messages carry.
fn canonical_tasks() -> Vec<Task> {
    let mut tasks = Vec::new();
    for (_, msg) in master_messages() {
        match msg {
            MasterMessage::AssignTask { task }
            | MasterMessage::AssignTaskWithCheckpoint { task, .. } => tasks.push(task),
            MasterMessage::RoleHandoverAck {
                task_queue_snapshot,
                ..
            } => tasks.extend(task_queue_snapshot),
            _ => {}
        }
    }
    for (_, msg) in client_messages() {
        if let ClientMessage::SubmitTask { task, .. } = msg {
            tasks.push(task);
        }
    }
    tasks
}

/// The variant name of `spec`. No `_` arm: a new `TaskSpec` variant does not compile here.
fn task_spec_variant(spec: &TaskSpec) -> &'static str {
    match spec {
        TaskSpec::Command { .. } => "Command",
        TaskSpec::ShellScript { .. } => "ShellScript",
        TaskSpec::RustCompilation { .. } => "RustCompilation",
        TaskSpec::GpuCompute { .. } => "GpuCompute",
        TaskSpec::BuiltinTest { .. } => "BuiltinTest",
        TaskSpec::Generative3D(_) => "Generative3D",
        TaskSpec::AutoRig(_) => "AutoRig",
    }
}

// ---------------------------------------------------------------------------------------------
// Round trips.
// ---------------------------------------------------------------------------------------------

/// What the JSON read paths do with an instance, keyed `<Enum>::<label>`.
enum JsonOutcome {
    /// It decodes back to the same message.
    RoundTrips,
    /// It is refused, on purpose.
    Refused,
}

fn json_outcome(key: &str) -> JsonOutcome {
    match key {
        // `UnknownVariant` stands for a bincode frame of a variant this build does not know. The
        // JSON mirrors mark it `skip_deserializing`, so no JSON peer can claim to send one; its
        // bincode form round-trips and is recorded like every other variant.
        "WorkerMessage::UnknownVariant"
        | "MasterMessage::UnknownVariant"
        | "InboundMessage::Worker(UnknownVariant)" => JsonOutcome::Refused,
        _ => JsonOutcome::RoundTrips,
    }
}

/// The `serialize_message` bincode frame of `msg`: the 0x02 format byte, then the encoding.
fn bincode_frame<M: Serialize>(key: &str, msg: &M) -> Vec<u8> {
    let frame = serialize_message(msg, WireCodec::Bincode)
        .unwrap_or_else(|e| panic!("{key}: bincode encoding failed: {e}"));
    assert_eq!(frame[0], WIRE_FORMAT_BINCODE, "{key}: bincode format byte");
    frame.to_vec()
}

/// Every instance must come back unchanged from the bincode frame, the 0x01 JSON frame, a legacy
/// raw JSON frame and a raw JSON frame that starts with whitespace (the format is detected from
/// the first byte), unless [`json_outcome`] says otherwise for JSON.
fn assert_round_trips<L, M>(enum_name: &str, instances: &[(L, M)])
where
    L: Display,
    M: Serialize + DeserializeOwned + PartialEq + Debug + 'static,
{
    for (label, msg) in instances {
        let key = format!("{enum_name}::{label}");

        let frame = bincode_frame(&key, msg);
        let (back, codec) = deserialize_message::<M>(&frame)
            .unwrap_or_else(|e| panic!("{key}: bincode decoding failed: {e}"));
        assert_eq!(codec, WireCodec::Bincode, "{key}: detected codec");
        assert_eq!(
            &back, msg,
            "{key}: changed by the bincode round trip: a `Binary*` mirror or one of its `From` \
             impls drops or mangles a field"
        );

        let frame = serialize_message(msg, WireCodec::Json)
            .unwrap_or_else(|e| panic!("{key}: JSON encoding failed: {e}"));
        assert_eq!(frame[0], WIRE_FORMAT_JSON, "{key}: JSON format byte");
        let raw = &frame[1..];
        let reads = [
            ("JSON frame", frame.to_vec()),
            ("raw JSON frame", raw.to_vec()),
            (
                "raw JSON frame after whitespace",
                [b"\n".as_slice(), raw].concat(),
            ),
        ];
        for (path, bytes) in reads {
            let decoded = deserialize_message::<M>(&bytes);
            match json_outcome(&key) {
                JsonOutcome::RoundTrips => {
                    let (back, codec) = decoded.unwrap_or_else(|e| {
                        panic!(
                            "{key}: {path} decoding failed: {e}\n{}",
                            String::from_utf8_lossy(raw)
                        )
                    });
                    assert_eq!(codec, WireCodec::Json, "{key}: {path}: detected codec");
                    assert_eq!(
                        &back, msg,
                        "{key}: changed by the {path} round trip: a `Human*` mirror or one of \
                         its `From` impls drops or mangles a field"
                    );
                }
                JsonOutcome::Refused => {
                    if let Ok((back, _)) = decoded {
                        panic!("{key}: the {path} must be refused, it decoded to {back:?}");
                    }
                }
            }
        }
    }
}

// ---------------------------------------------------------------------------------------------
// The recorded layout.
// ---------------------------------------------------------------------------------------------

fn wire_layout_path() -> PathBuf {
    Path::new(env!("CARGO_MANIFEST_DIR")).join(WIRE_LAYOUT_FILE)
}

/// `<Enum>::<Variant>` -> hex of the bincode encoding (the frame after its 0x02 format byte) of
/// every canonical instance of the four message enums.
fn current_wire_layout() -> BTreeMap<String, String> {
    fn record<M: Serialize>(
        layout: &mut BTreeMap<String, String>,
        enum_name: &str,
        instances: Vec<(&'static str, M)>,
    ) {
        for (variant, msg) in instances {
            let key = format!("{enum_name}::{variant}");
            let hex = to_hex(&bincode_frame(&key, &msg)[1..]);
            assert!(layout.insert(key, hex).is_none(), "duplicate {variant}");
        }
    }
    let mut layout = BTreeMap::new();
    record(&mut layout, "WorkerMessage", worker_messages());
    record(&mut layout, "MasterMessage", master_messages());
    record(&mut layout, "ClientMessage", client_messages());
    record(&mut layout, "ClientResponse", client_responses());
    layout
}

fn to_hex(bytes: &[u8]) -> String {
    let mut hex = String::with_capacity(bytes.len() * 2);
    for byte in bytes {
        let _ = write!(hex, "{byte:02x}");
    }
    hex
}

fn render_wire_layout(layout: &BTreeMap<String, String>) -> String {
    let mut text = String::from(HEADER);
    let _ = writeln!(text, "# protocol_version: {PROTOCOL_VERSION_CURRENT}");
    let _ = writeln!(
        text,
        "# protocol_version_min_compatible: {PROTOCOL_VERSION_MIN_COMPATIBLE}"
    );
    for (key, hex) in layout {
        let _ = writeln!(text, "{key} {hex}");
    }
    text
}

/// Parses `# protocol_version: <N>` and `# protocol_version_min_compatible: <M>` from the header
/// comments of `source`. Both header lines are mandatory.
fn try_parse_wire_layout_versions_from(text: &str, source: &str) -> Result<(u32, u32), String> {
    let mut version: Option<u32> = None;
    let mut min_compatible: Option<u32> = None;
    for line in text.lines() {
        let line = line.trim();
        if let Some(rest) = line.strip_prefix("# protocol_version:") {
            let parsed = rest
                .trim()
                .parse::<u32>()
                .map_err(|e| format!("invalid `# protocol_version:` header in {source}: {e}"))?;
            version = Some(parsed);
        } else if let Some(rest) = line.strip_prefix("# protocol_version_min_compatible:") {
            let parsed = rest.trim().parse::<u32>().map_err(|e| {
                format!("invalid `# protocol_version_min_compatible:` header in {source}: {e}")
            })?;
            min_compatible = Some(parsed);
        }
    }
    let version =
        version.ok_or_else(|| format!("missing `# protocol_version: <N>` header in {source}"))?;
    let min_compatible = min_compatible.ok_or_else(|| {
        format!("missing `# protocol_version_min_compatible: <M>` header in {source}")
    })?;
    Ok((version, min_compatible))
}

fn try_parse_wire_layout_versions(text: &str) -> Result<(u32, u32), String> {
    try_parse_wire_layout_versions_from(text, WIRE_LAYOUT_FILE)
}

fn parse_wire_layout_versions(text: &str) -> (u32, u32) {
    try_parse_wire_layout_versions(text).unwrap_or_else(|e| panic!("{e}"))
}

/// Validates that [`WIRE_LAYOUT_FILE`] already exists on disk, has both `# protocol_version`
/// headers, and satisfies [`verify_version_bump_for_layout_change`] before `UPDATE_WIRE_LAYOUT=1`
/// is allowed to overwrite it.
fn validate_wire_layout_update_from_existing(
    existing_text: Result<String, std::io::Error>,
    current_version: u32,
    current_min_compatible: u32,
    current: &BTreeMap<String, String>,
) -> Result<(), String> {
    let existing_text = existing_text.map_err(|e| {
        format!(
            "cannot update {WIRE_LAYOUT_FILE}: existing file is missing or unreadable ({e}); \
             restore {WIRE_LAYOUT_FILE} from git before running with {UPDATE_ENV}=1"
        )
    })?;
    let (recorded_ver, recorded_min) = try_parse_wire_layout_versions(&existing_text)?;
    let recorded_layout = parse_wire_layout(&existing_text);
    if recorded_layout.is_empty() {
        return Err(format!(
            "cannot update {WIRE_LAYOUT_FILE}: existing file contains no recorded message encodings"
        ));
    }
    verify_version_bump_for_layout_change(
        WIRE_LAYOUT_FILE,
        recorded_ver,
        recorded_min,
        &recorded_layout,
        current_version,
        current_min_compatible,
        current,
    )
}

/// Compares the working tree's [`WIRE_LAYOUT_FILE`] against `baseline_source` (for example
/// `origin/master:crates/core/tests/data/wire_layout.txt`), requiring the recorded
/// `# protocol_version` header to increase whenever the body changed.
fn verify_wire_layout_file_against_baseline(
    current_text: &str,
    baseline_text: &str,
    baseline_source: &str,
) -> Result<(), String> {
    let (current_ver, current_min) = try_parse_wire_layout_versions(current_text)?;
    let current_layout = parse_wire_layout(current_text);
    let has_baseline_headers = baseline_text
        .lines()
        .any(|l| l.trim().starts_with("# protocol_version"));
    let (base_ver, base_min) = if has_baseline_headers {
        try_parse_wire_layout_versions_from(baseline_text, baseline_source)?
    } else {
        (2, 2)
    };
    let base_layout = parse_wire_layout_from(baseline_text, baseline_source);
    verify_version_bump_for_layout_change(
        baseline_source,
        base_ver,
        base_min,
        &base_layout,
        current_ver,
        current_min,
        &current_layout,
    )
}

/// Enforces that any wire layout change relative to `recorded_source` bumps
/// `PROTOCOL_VERSION_CURRENT`, and bumps `PROTOCOL_VERSION_MIN_COMPATIBLE` to `current_version`
/// whenever an existing variant's encoding changed or was removed.
fn verify_version_bump_for_layout_change(
    recorded_source: &str,
    recorded_version: u32,
    recorded_min_compatible: u32,
    recorded: &BTreeMap<String, String>,
    current_version: u32,
    current_min_compatible: u32,
    current: &BTreeMap<String, String>,
) -> Result<(), String> {
    let differences = layout_differences(recorded, current);
    if differences.is_empty() {
        if current_version < recorded_version {
            return Err(format!(
                "PROTOCOL_VERSION_CURRENT ({current_version}) regressed below the recorded \
                 protocol_version ({recorded_version}) in {recorded_source}"
            ));
        }
        if current_min_compatible > current_version {
            return Err(format!(
                "PROTOCOL_VERSION_MIN_COMPATIBLE ({current_min_compatible}) exceeds \
                 PROTOCOL_VERSION_CURRENT ({current_version})"
            ));
        }
        return Ok(());
    }

    if current_version <= recorded_version {
        return Err(format!(
            "The bincode encoding of {} canonical message(s) differs from {recorded_source}, \
             but PROTOCOL_VERSION_CURRENT is still {current_version} (recorded: \
             {recorded_version}). Bump PROTOCOL_VERSION_CURRENT in crates/core/src/protocol.rs \
             before regenerating {WIRE_LAYOUT_FILE}:\n\n    {}",
            differences.len(),
            differences.join("\n    ")
        ));
    }

    let existing_variant_changed_or_removed = recorded
        .iter()
        .any(|(key, before)| current.get(key) != Some(before));
    if existing_variant_changed_or_removed
        && (current_min_compatible <= recorded_min_compatible
            || current_min_compatible < current_version)
    {
        return Err(format!(
            "An existing canonical message encoding changed or was removed relative to \
             {recorded_source} (recorded protocol_version {recorded_version}, \
             min_compatible {recorded_min_compatible}), so older peers cannot decode the new \
             wire layout. Bump PROTOCOL_VERSION_MIN_COMPATIBLE to {current_version} (got \
             {current_min_compatible}) in crates/core/src/protocol.rs."
        ));
    }

    Ok(())
}

/// Reads the recorded layout: blank lines and `#` comments are skipped, every other line is
/// `<Enum>::<Variant> <hex>`, sorted and unique. CRLF line ends (a Windows checkout) are fine.
fn parse_wire_layout_from(text: &str, source: &str) -> BTreeMap<String, String> {
    let mut layout = BTreeMap::new();
    let mut previous: Option<&str> = None;
    for (index, line) in text.lines().enumerate() {
        let line = line.trim();
        if line.is_empty() || line.starts_with('#') {
            continue;
        }
        let line_no = index + 1;
        let (key, hex) = line
            .split_once(' ')
            .unwrap_or_else(|| panic!("{source}:{line_no}: expected `<Enum>::<Variant> <hex>`"));
        if let Some(previous) = previous {
            assert!(
                previous < key,
                "{source}:{line_no}: `{key}` is out of order or listed twice; \
                 regenerate the file with\n{REGENERATE}"
            );
        }
        previous = Some(key);
        layout.insert(key.to_string(), hex.trim().to_string());
    }
    layout
}

fn parse_wire_layout(text: &str) -> BTreeMap<String, String> {
    parse_wire_layout_from(text, WIRE_LAYOUT_FILE)
}

/// One entry per variant whose recorded and current encodings differ.
fn layout_differences(
    recorded: &BTreeMap<String, String>,
    current: &BTreeMap<String, String>,
) -> Vec<String> {
    let mut differences = Vec::new();
    for (key, now) in current {
        match recorded.get(key) {
            None => differences.push(format!("{key}: no recorded encoding (a new variant?)")),
            Some(before) if before != now => differences.push(describe_change(key, before, now)),
            Some(_) => {}
        }
    }
    for key in recorded.keys().filter(|key| !current.contains_key(*key)) {
        differences.push(format!(
            "{key}: recorded, but no such variant is encoded any more (removed or renamed?)"
        ));
    }
    differences
}

/// Where the two hex encodings part, with a few bytes from there on.
fn describe_change(key: &str, before: &str, now: &str) -> String {
    let (before_len, now_len) = (before.len() / 2, now.len() / 2);
    let first = before
        .as_bytes()
        .chunks(2)
        .zip(now.as_bytes().chunks(2))
        .position(|(a, b)| a != b)
        .unwrap_or(before_len.min(now_len));
    let appended = if now.starts_with(before) {
        " (the current encoding only appends bytes to the recorded one)"
    } else {
        ""
    };
    format!(
        "{key}: {before_len} bytes recorded, {now_len} now, first difference at byte \
         {first}{appended}\n      recorded: {}\n      current:  {}",
        excerpt(before, first),
        excerpt(now, first)
    )
}

/// Up to 24 bytes of `hex` from byte `from` on.
fn excerpt(hex: &str, from: usize) -> String {
    let start = (2 * from).min(hex.len());
    let end = (start + 48).min(hex.len());
    let before = if start > 0 { "..." } else { "" };
    let after = if end < hex.len() { "..." } else { "" };
    format!("{before}{}{after}", &hex[start..end])
}

// ---------------------------------------------------------------------------------------------
// Tests.
// ---------------------------------------------------------------------------------------------

#[test]
fn every_variant_has_one_canonical_instance() {
    // Each list checks itself against the variants of its `Binary*` mirror as it is built.
    worker_messages();
    master_messages();
    client_messages();
    client_responses();
    inbound_messages();
}

/// The `TaskSpec` mirrors only travel inside a `Task`, so a new `TaskSpec` variant needs a task in
/// one of the canonical messages.
#[test]
fn every_task_spec_variant_travels_in_a_canonical_message() {
    let carried: BTreeSet<&str> = canonical_tasks()
        .iter()
        .map(|task| task_spec_variant(&task.spec))
        .collect();
    let declared: BTreeSet<&str> = binary_variant_names::<TaskSpec>().into_iter().collect();
    assert_eq!(
        carried, declared,
        "the TaskSpec variants the canonical messages carry (left) must be all that the binary \
         mirror declares (right): give a task in master_messages() or client_messages() each \
         missing spec"
    );
}

#[test]
fn canonical_instances_round_trip_through_both_codecs() {
    assert_round_trips("WorkerMessage", &worker_messages());
    assert_round_trips("MasterMessage", &master_messages());
    assert_round_trips("ClientMessage", &client_messages());
    assert_round_trips("ClientResponse", &client_responses());
    assert_round_trips("InboundMessage", &inbound_messages());
}

/// The Master reads the first frame of a connection as an `InboundMessage`, while peers send a
/// bare message: a worker its `Register`, a client any `ClientMessage`. Both must be routed to the
/// right side, and every worker and client message must survive being wrapped.
#[test]
fn inbound_message_routes_bare_first_frames_and_wraps_every_message() {
    let register = canonical(worker_messages(), "Register");
    for codec in [WireCodec::Bincode, WireCodec::Json] {
        let first_frames = std::iter::once((
            "WorkerMessage::Register".to_string(),
            serialize_message(&register, codec),
            InboundMessage::Worker(register.clone()),
        ))
        .chain(client_messages().into_iter().map(|(variant, msg)| {
            (
                format!("ClientMessage::{variant}"),
                serialize_message(&msg, codec),
                InboundMessage::Client(msg),
            )
        }));
        for (key, frame, expected) in first_frames {
            let frame = frame.unwrap_or_else(|e| panic!("{key}: {codec} encoding failed: {e}"));
            let (routed, detected) = deserialize_message::<InboundMessage>(&frame)
                .unwrap_or_else(|e| panic!("{key}: a bare {codec} first frame is refused: {e}"));
            assert_eq!(detected, codec, "{key}: detected codec");
            assert_eq!(
                routed, expected,
                "{key}: a bare {codec} first frame is misrouted"
            );
        }
    }
    assert_round_trips("InboundMessage", &wrapped_inbound_messages());
}

#[test]
fn bincode_encoding_matches_the_recorded_wire_layout() {
    let current = current_wire_layout();
    // Fixed inputs only: no random id, no clock, no `HashMap` with more than one entry (each new
    // map gets new hash keys, so two constructions would list it in different orders).
    assert_eq!(
        current,
        current_wire_layout(),
        "two constructions of the canonical instances encode differently"
    );

    let path = wire_layout_path();
    if std::env::var_os(UPDATE_ENV).is_some_and(|value| value == "1") {
        if let Err(reason) = validate_wire_layout_update_from_existing(
            std::fs::read_to_string(&path),
            PROTOCOL_VERSION_CURRENT,
            PROTOCOL_VERSION_MIN_COMPATIBLE,
            &current,
        ) {
            panic!("{reason}");
        }
        let dir = path
            .parent()
            .expect("the layout file has a parent directory");
        std::fs::create_dir_all(dir).unwrap_or_else(|e| panic!("{}: {e}", dir.display()));
        let rendered = render_wire_layout(&current);
        std::fs::write(&path, &rendered).unwrap_or_else(|e| panic!("{}: {e}", path.display()));
        check_wire_layout_against_git_baseline(&rendered);
        eprintln!(
            "{UPDATE_ENV}=1: wrote {} encodings (protocol_version={PROTOCOL_VERSION_CURRENT}, \
             min_compatible={PROTOCOL_VERSION_MIN_COMPATIBLE}) to {}",
            current.len(),
            path.display()
        );
        return;
    }

    let text = std::fs::read_to_string(&path).unwrap_or_else(|e| {
        panic!(
            "cannot read {}: {e}\nGenerate it with\n{REGENERATE}",
            path.display()
        )
    });
    let (recorded_ver, recorded_min) = parse_wire_layout_versions(&text);
    let recorded_layout = parse_wire_layout(&text);
    if let Err(reason) = verify_version_bump_for_layout_change(
        WIRE_LAYOUT_FILE,
        recorded_ver,
        recorded_min,
        &recorded_layout,
        PROTOCOL_VERSION_CURRENT,
        PROTOCOL_VERSION_MIN_COMPATIBLE,
        &current,
    ) {
        panic!("{reason}");
    }
    let differences = layout_differences(&recorded_layout, &current);
    assert!(
        differences.is_empty(),
        "The bincode encoding of {} canonical message(s) differs from {WIRE_LAYOUT_FILE}:\n\n    \
         {}\n\nIf a message type changed (a field or variant added, removed, moved or retyped, in \
         the message or in a type it carries), this is a wire format change: bincode is \
         positional, so new fields go after the existing ones, and peers built before the change \
         must still decode every frame (see \"Wire protocol\" in CLAUDE.md). Bump \
         PROTOCOL_VERSION_CURRENT (and PROTOCOL_VERSION_MIN_COMPATIBLE if breaking) and record it \
         in a design record (docs/design/NNNN-<slug>.md).\n\nThen \
         regenerate the file with\n{REGENERATE}\n(PowerShell keeps the variable for the session: \
         `Remove-Item Env:{UPDATE_ENV}` afterwards.)\n",
        differences.len(),
        differences.join("\n    ")
    );
    assert_eq!(
        (recorded_ver, recorded_min),
        (PROTOCOL_VERSION_CURRENT, PROTOCOL_VERSION_MIN_COMPATIBLE),
        "{WIRE_LAYOUT_FILE} records protocol_version ({recorded_ver}, min {recorded_min}), \
         but code has ({PROTOCOL_VERSION_CURRENT}, min {PROTOCOL_VERSION_MIN_COMPATIBLE}); \
         regenerate with\n{REGENERATE}"
    );
}

#[test]
fn changed_task_layout_fails_without_version_bump() {
    let baseline = current_wire_layout();

    // Simulate changing `Task`'s wire layout (e.g. adding a `priority: u32` field to `Task`),
    // which alters the bincode bytes of every message carrying a `Task`.
    #[derive(Serialize)]
    struct MutatedTask {
        base: Task,
        added_field: u32,
    }
    #[derive(Serialize)]
    #[allow(dead_code, clippy::large_enum_variant)]
    enum MutatedMasterMessage {
        RegisterAck,
        HeartbeatAck,
        AssignTask { task: MutatedTask },
    }
    let mutated_assign = MutatedMasterMessage::AssignTask {
        task: MutatedTask {
            base: task(1, command_spec()),
            added_field: 0xdead_beef,
        },
    };
    let mutated_assign_hex = to_hex(&bincode_frame("MutatedAssignTask", &mutated_assign)[1..]);

    let mut mutated = baseline.clone();
    mutated.insert("MasterMessage::AssignTask".to_string(), mutated_assign_hex);

    let diffs = layout_differences(&baseline, &mutated);
    assert_eq!(diffs.len(), 1);
    assert!(
        diffs[0].contains("MasterMessage::AssignTask"),
        "expected AssignTask difference, got: {:?}",
        diffs
    );

    // 1. Without bumping PROTOCOL_VERSION_CURRENT, the guard rejects the layout change even if
    // someone tries to regenerate wire_layout.txt.
    let err_no_bump = verify_version_bump_for_layout_change(
        WIRE_LAYOUT_FILE,
        PROTOCOL_VERSION_CURRENT,
        PROTOCOL_VERSION_MIN_COMPATIBLE,
        &baseline,
        PROTOCOL_VERSION_CURRENT,
        PROTOCOL_VERSION_MIN_COMPATIBLE,
        &mutated,
    )
    .unwrap_err();
    assert!(
        err_no_bump.contains("Bump PROTOCOL_VERSION_CURRENT"),
        "unexpected error: {err_no_bump}"
    );

    // 2. Bumping PROTOCOL_VERSION_CURRENT without bumping PROTOCOL_VERSION_MIN_COMPATIBLE is also
    // rejected because an existing message variant changed its wire bytes.
    let err_min_not_bumped = verify_version_bump_for_layout_change(
        WIRE_LAYOUT_FILE,
        PROTOCOL_VERSION_CURRENT,
        PROTOCOL_VERSION_MIN_COMPATIBLE,
        &baseline,
        PROTOCOL_VERSION_CURRENT + 1,
        PROTOCOL_VERSION_MIN_COMPATIBLE,
        &mutated,
    )
    .unwrap_err();
    assert!(
        err_min_not_bumped.contains("Bump PROTOCOL_VERSION_MIN_COMPATIBLE"),
        "unexpected error: {err_min_not_bumped}"
    );

    // 3. Bumping both PROTOCOL_VERSION_CURRENT and PROTOCOL_VERSION_MIN_COMPATIBLE succeeds.
    verify_version_bump_for_layout_change(
        WIRE_LAYOUT_FILE,
        PROTOCOL_VERSION_CURRENT,
        PROTOCOL_VERSION_MIN_COMPATIBLE,
        &baseline,
        PROTOCOL_VERSION_CURRENT + 1,
        PROTOCOL_VERSION_CURRENT + 1,
        &mutated,
    )
    .expect("bumping both current and min_compatible version must allow the layout update");

    // 4. Deleting wire_layout.txt or stripping its `# protocol_version` headers in update mode
    // (`UPDATE_WIRE_LAYOUT=1`) is rejected rather than falling back to version 2.
    let err_missing_file = validate_wire_layout_update_from_existing(
        Err(std::io::Error::new(
            std::io::ErrorKind::NotFound,
            "file not found",
        )),
        PROTOCOL_VERSION_CURRENT,
        PROTOCOL_VERSION_MIN_COMPATIBLE,
        &mutated,
    )
    .unwrap_err();
    assert!(
        err_missing_file.contains("existing file is missing"),
        "unexpected error for missing file: {err_missing_file}"
    );

    let full_text = render_wire_layout(&baseline);
    let stripped_headers: String = full_text
        .lines()
        .filter(|l| !l.trim().starts_with("# protocol_version"))
        .map(|l| format!("{l}\n"))
        .collect();
    let err_missing_headers = validate_wire_layout_update_from_existing(
        Ok(stripped_headers),
        PROTOCOL_VERSION_CURRENT,
        PROTOCOL_VERSION_MIN_COMPATIBLE,
        &mutated,
    )
    .unwrap_err();
    assert!(
        err_missing_headers.contains("missing `# protocol_version"),
        "unexpected error for missing headers: {err_missing_headers}"
    );

    // 5. A branch that modifies the wire layout body and regenerates wire_layout.txt without
    // bumping the header version above the baseline copy fails the baseline comparison check, and
    // names the baseline source rather than the working tree's file.
    let mutated_file_without_bump = render_wire_layout(&mutated);
    let baseline_source = "origin/master:crates/core/tests/data/wire_layout.txt";
    let err_baseline = verify_wire_layout_file_against_baseline(
        &mutated_file_without_bump,
        &full_text,
        baseline_source,
    )
    .unwrap_err();
    assert!(
        err_baseline.contains("Bump PROTOCOL_VERSION_CURRENT"),
        "unexpected baseline comparison error: {err_baseline}"
    );
    assert!(
        err_baseline.contains("differs from origin/master:crates/core/tests/data/wire_layout.txt"),
        "baseline error must name the baseline source, got: {err_baseline}"
    );

    // 6. Baseline ref selection prefers explicit WIRE_LAYOUT_BASELINE_REF, then the dedicated CI
    // ref (`refs/remotes/ci/wire-layout-base`), and falls back to `origin/master`.
    assert_eq!(
        select_wire_layout_baseline_ref(Some("refs/remotes/ci/custom"), false, true),
        Some("refs/remotes/ci/custom".to_string())
    );
    assert_eq!(
        select_wire_layout_baseline_ref(None, true, true),
        Some(CI_WIRE_LAYOUT_BASELINE_REF.to_string())
    );
    assert_eq!(
        select_wire_layout_baseline_ref(None, false, true),
        Some("origin/master".to_string())
    );
    assert_eq!(select_wire_layout_baseline_ref(None, false, false), None);
}

fn git_ref_exists(manifest_dir: &Path, git_ref: &str) -> bool {
    std::process::Command::new("git")
        .current_dir(manifest_dir)
        .args(["rev-parse", "--verify", "--quiet", git_ref])
        .output()
        .is_ok_and(|o| o.status.success())
}

fn select_wire_layout_baseline_ref(
    explicit_env_ref: Option<&str>,
    has_ci_baseline_ref: bool,
    has_origin_master: bool,
) -> Option<String> {
    if let Some(env_ref) = explicit_env_ref.map(str::trim).filter(|s| !s.is_empty()) {
        return Some(env_ref.to_string());
    }
    if has_ci_baseline_ref {
        return Some(CI_WIRE_LAYOUT_BASELINE_REF.to_string());
    }
    if has_origin_master {
        return Some("origin/master".to_string());
    }
    None
}

fn resolve_wire_layout_baseline_ref(manifest_dir: &Path) -> Option<String> {
    let explicit = std::env::var(WIRE_LAYOUT_BASELINE_REF_ENV).ok();
    let has_ci_ref = git_ref_exists(manifest_dir, CI_WIRE_LAYOUT_BASELINE_REF);
    let has_origin_master = git_ref_exists(manifest_dir, "origin/master");
    let chosen =
        select_wire_layout_baseline_ref(explicit.as_deref(), has_ci_ref, has_origin_master)?;
    if explicit
        .as_deref()
        .map(str::trim)
        .is_some_and(|s| !s.is_empty())
    {
        assert!(
            git_ref_exists(manifest_dir, &chosen),
            "{WIRE_LAYOUT_BASELINE_REF_ENV}={chosen} does not exist in git"
        );
    }
    Some(chosen)
}

fn check_wire_layout_against_git_baseline(current_text: &str) {
    let manifest_dir = Path::new(env!("CARGO_MANIFEST_DIR"));
    let Some(baseline_ref) = resolve_wire_layout_baseline_ref(manifest_dir) else {
        return;
    };
    let baseline_spec = format!("{baseline_ref}:crates/core/{WIRE_LAYOUT_FILE}");
    let output = std::process::Command::new("git")
        .current_dir(manifest_dir)
        .args(["show", &baseline_spec])
        .output()
        .unwrap_or_else(|e| panic!("run git show {baseline_spec}: {e}"));
    assert!(
        output.status.success(),
        "git show {baseline_spec} failed: {}",
        String::from_utf8_lossy(&output.stderr)
    );
    let baseline_text =
        String::from_utf8(output.stdout).expect("baseline wire_layout.txt is UTF-8");
    if let Err(reason) =
        verify_wire_layout_file_against_baseline(current_text, &baseline_text, &baseline_spec)
    {
        panic!("{reason}");
    }
}

#[test]
fn wire_layout_file_bumps_version_when_body_differs_from_origin_master() {
    // In update mode (`UPDATE_WIRE_LAYOUT=1`), `bincode_encoding_matches_the_recorded_wire_layout`
    // rewrites `tests/data/wire_layout.txt` and then runs `check_wire_layout_against_git_baseline`
    // after the write finishes; skip here so a concurrent test thread never reads a half-written file.
    if std::env::var_os(UPDATE_ENV).is_some_and(|value| value == "1") {
        return;
    }
    let path = wire_layout_path();
    let current_text = std::fs::read_to_string(&path)
        .unwrap_or_else(|e| panic!("cannot read {}: {e}", path.display()));
    check_wire_layout_against_git_baseline(&current_text);
}
