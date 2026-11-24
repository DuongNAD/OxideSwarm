use std::collections::{HashMap, HashSet};
use std::sync::Arc;
use std::time::Duration;

use tokio::sync::{mpsc, watch, Notify};
use uuid::Uuid;

use rusty_grid_core::capabilities::WorkerCapabilities;
use rusty_grid_core::placement::OPT_REPEATABLE_TAG;
use rusty_grid_core::protocol::{MasterMessage, ProtocolCapabilities};
use rusty_grid_core::task::{
    AutoRigSpec, Bytes, Generative3DSpec, Task, TaskRequirements, TaskResult, TaskSpec,
};
use rusty_grid_master::queue::{TaskQueue, TaskState};
use rusty_grid_master::reaper::{spawn_reaper_with_queue, ReaperConfig};
use rusty_grid_master::registry::{WorkerInfo, WorkerRegistry, WorkerStatus};
use rusty_grid_master::scheduler::{ScheduleSkipReason, SchedulerConfig, WorkloadScheduler};
use rusty_grid_master::speed::{mutate_shared_speed_book, SpeedBook, TaskKey};

fn make_worker(
    name: &str,
    cores: usize,
    ram_mb: u64,
    gpu: bool,
    simulated_gpu: bool,
    vram_mb: Option<u64>,
    has_blender: bool,
) -> WorkerInfo {
    let caps = WorkerCapabilities::new(name, cores, ram_mb, gpu, simulated_gpu, None)
        .with_vram_mb(vram_mb)
        .with_blender(has_blender, has_blender.then(|| "4.2.0".to_string()));
    let mut info = WorkerInfo::new(
        Uuid::new_v4(),
        1,
        caps,
        WorkerStatus::Connected,
        0,
        0,
        0,
        "127.0.0.1:0".into(),
    );
    info.features = ProtocolCapabilities::all_supported();
    info
}

fn autorig_task(preferred_provider: Option<&str>) -> Task {
    Task::new(
        TaskSpec::AutoRig(AutoRigSpec {
            input_mesh: Bytes::from(b"glTF\x02\x00\x00\x00".to_vec()),
            rig_preset: "biped".into(),
            embed_animations: vec!["idle".into()],
            preferred_provider: preferred_provider.map(str::to_string),
        }),
        TaskRequirements::generic(1, 512),
    )
}

fn gen3d_task(engine: Option<&str>, quality: &str, req: TaskRequirements) -> Task {
    let mut spec = Generative3DSpec::new_prompt("robot").with_quality(quality);
    if let Some(eng) = engine {
        spec = spec.with_engine(eng);
    }
    Task::new(TaskSpec::Generative3D(spec), req)
}

#[test]
fn blender_preferred_provider_routes_only_to_blender_worker_and_preserves_builtin_shape() {
    let config = SchedulerConfig::default().without_host_cpu_backpressure();
    let no_blender = make_worker("no-blender", 4, 8192, true, false, Some(8192), false);
    let blender = make_worker("has-blender", 4, 8192, true, false, Some(8192), true);

    // Case-insensitive and whitespace-trimmed "blender" must land exclusively on `blender`.
    let t_blender = autorig_task(Some(" Blender "));
    let t_builtin = autorig_task(Some("builtin"));
    let report = WorkloadScheduler::matchmake(
        &config,
        &[t_blender.clone(), t_builtin.clone()],
        &[no_blender.clone(), blender.clone()],
    );
    let assigned: HashMap<_, _> = report
        .assignments
        .iter()
        .map(|a| (a.task_id, a.worker_id))
        .collect();
    assert_eq!(
        assigned.get(&t_blender.id).copied(),
        Some(blender.worker_id),
        "preferred_provider='blender' must route strictly to has_blender=true worker"
    );
    assert!(assigned.contains_key(&t_builtin.id));

    // When only `no_blender` exists and fail_unfulfillable_tasks is false:
    // t_blender skips with ClusterLacksCapability("... Blender ..."), while t_builtin behind it
    // STILL places onto `no_blender` (Shape isolation).
    let t_blender2 = autorig_task(Some("blender"));
    let t_builtin2 = autorig_task(Some("builtin"));
    let skip_report = WorkloadScheduler::matchmake(
        &config,
        &[t_blender2.clone(), t_builtin2.clone()],
        std::slice::from_ref(&no_blender),
    );
    assert_eq!(skip_report.assignments.len(), 1);
    assert_eq!(skip_report.assignments[0].task_id, t_builtin2.id);
    assert_eq!(skip_report.assignments[0].worker_id, no_blender.worker_id);
    assert!(matches!(
        &skip_report.skipped[..],
        [(id, ScheduleSkipReason::ClusterLacksCapability(msg))]
            if *id == t_blender2.id && msg.contains("Blender")
    ));

    // With fail_unfulfillable_tasks = true:
    let strict_cfg = config.clone().with_fail_unfulfillable_tasks(true);
    let fail_report = WorkloadScheduler::matchmake(
        &strict_cfg,
        &[t_blender2.clone(), t_builtin2.clone()],
        std::slice::from_ref(&no_blender),
    );
    assert_eq!(fail_report.assignments.len(), 1);
    assert_eq!(fail_report.assignments[0].task_id, t_builtin2.id);
    assert_eq!(fail_report.unfulfillable.len(), 1);
    assert_eq!(fail_report.unfulfillable[0].0, t_blender2.id);
    assert!(fail_report.unfulfillable[0].1.contains("Blender"));

    // When a Blender worker exists with insufficient CPU cores, the diagnostic mentions blender=true.
    let small_blender = make_worker("small-blender", 1, 8192, false, false, None, true);
    let heavy_blender = Task::new(t_blender.spec.clone(), TaskRequirements::generic(8, 512));
    let heavy_plain = Task::new(t_builtin.spec.clone(), TaskRequirements::generic(8, 512));
    let diag_report = WorkloadScheduler::matchmake(
        &strict_cfg,
        &[heavy_blender.clone(), heavy_plain.clone()],
        &[small_blender],
    );
    let reasons: HashMap<_, _> = diag_report.unfulfillable.into_iter().collect();
    let blender_reason = reasons.get(&heavy_blender.id).unwrap();
    let plain_reason = reasons.get(&heavy_plain.id).unwrap();
    assert!(
        blender_reason.contains("blender=true"),
        "expected blender=true in reason, got: {blender_reason}"
    );
    assert!(
        !plain_reason.contains("blender="),
        "non-Blender task reason must omit blender=, got: {plain_reason}"
    );
}

#[test]
fn preset_vram_high_skips_2gb_4gb_6143mb_and_preserves_bug3_cpu_defaults() {
    let config = SchedulerConfig::default().without_host_cpu_backpressure();

    let w_cpu = make_worker("cpu", 8, 16384, false, false, None, false);
    let w_2g = make_worker("gpu-2g", 8, 16384, true, false, Some(2048), false);
    let w_4g = make_worker("gpu-4g", 8, 16384, true, false, Some(4096), false);
    let w_6143 = make_worker("gpu-6143", 8, 16384, true, false, Some(6143), false);
    let w_6144 = make_worker("gpu-6144", 8, 16384, true, false, Some(6144), false);
    let w_sim = make_worker("gpu-sim-none", 8, 16384, true, true, None, false);

    let cluster = vec![
        w_cpu.clone(),
        w_2g.clone(),
        w_4g.clone(),
        w_6143.clone(),
        w_6144.clone(),
        w_sim.clone(),
    ];

    // `auto` always runs the CPU SDF tier, so even quality="high" needs no VRAM and runs on a CPU
    // worker (it must never wait for a GPU it will not use).
    let t_auto_high = gen3d_task(None, "high", TaskRequirements::generic(2, 60));
    assert_eq!(config.required_vram_mb(&t_auto_high), 0);
    let auto_report = WorkloadScheduler::matchmake(
        &config,
        std::slice::from_ref(&t_auto_high),
        std::slice::from_ref(&w_cpu),
    );
    assert_eq!(
        auto_report.assignments.len(),
        1,
        "auto high must run on CPU"
    );

    // `triposr` with quality="high" (even without --gpu/--vram-mb) requires 6144 MB.
    {
        let eng = Some("triposr");
        let t_high = gen3d_task(eng, "high", TaskRequirements::generic(2, 60));
        assert_eq!(config.required_vram_mb(&t_high), 6144);
        let report = WorkloadScheduler::matchmake(&config, std::slice::from_ref(&t_high), &cluster);
        assert_eq!(report.assignments.len(), 1, "engine={eng:?}");
        assert_eq!(
            report.assignments[0].worker_id, w_6144.worker_id,
            "high task (engine={eng:?}) must skip CPU/2GB/4GB/6143MB/unknown-VRAM and land on 6144MB"
        );
    }

    // Even with ONLY CPU + 2GB workers, a `high` task must skip the 2GB worker (never fall back).
    let t_high_only_2g = gen3d_task(Some("triposr"), "high", TaskRequirements::generic(1, 60));
    let skip_2g =
        WorkloadScheduler::matchmake(&config, &[t_high_only_2g], &[w_cpu.clone(), w_2g.clone()]);
    assert!(
        skip_2g.assignments.is_empty(),
        "high task must not land on CPU or 2GB GPU worker"
    );

    // `sdf` with quality="high" remains CPU-capable (0 MB VRAM unless explicitly requested).
    let t_sdf_high = gen3d_task(Some("sdf"), "high", TaskRequirements::generic(1, 60));
    assert_eq!(config.required_vram_mb(&t_sdf_high), 0);
    let sdf_report =
        WorkloadScheduler::matchmake(&config, &[t_sdf_high], &[w_cpu.clone(), w_6144.clone()]);
    assert_eq!(sdf_report.assignments[0].worker_id, w_cpu.worker_id);

    // BUG-3 defaults: `auto` and `triposr` with quality="standard" and gpu_required=false have 0 VRAM.
    for eng in [None, Some("triposr")] {
        let t_std = gen3d_task(eng, "standard", TaskRequirements::generic(1, 60));
        assert_eq!(config.required_vram_mb(&t_std), 0);
        let std_report =
            WorkloadScheduler::matchmake(&config, &[t_std], std::slice::from_ref(&w_cpu));
        assert_eq!(std_report.assignments.len(), 1);
        assert_eq!(std_report.assignments[0].worker_id, w_cpu.worker_id);
    }

    // `auto` with `--gpu` + `fast` gets 2048 MB: skips 1024 MB and lands on 2048 MB.
    let w_1g = make_worker("gpu-1g", 8, 16384, true, false, Some(1024), false);
    let mut gpu_fast_req = TaskRequirements::generic(1, 60);
    gpu_fast_req.gpu_required = true;
    let t_fast_gpu = gen3d_task(Some("triposr"), "fast", gpu_fast_req);
    assert_eq!(config.required_vram_mb(&t_fast_gpu), 2048);
    let fast_report = WorkloadScheduler::matchmake(&config, &[t_fast_gpu], &[w_1g, w_2g.clone()]);
    assert_eq!(fast_report.assignments[0].worker_id, w_2g.worker_id);
}

#[test]
fn preset_vram_shape_isolation_and_custom_thresholds() {
    let mut config = SchedulerConfig::default()
        .without_host_cpu_backpressure()
        .with_fail_unfulfillable_tasks(true);
    config.min_vram_high_mb = 12288;
    config.min_vram_fast_mb = 2048;

    let w_8g = make_worker("gpu-8g", 4, 16384, true, false, Some(8192), false);
    let t_high = gen3d_task(Some("triposr"), "high", TaskRequirements::generic(1, 60));
    let mut req_fast = TaskRequirements::generic(1, 60);
    req_fast.gpu_required = true;
    let t_fast = gen3d_task(Some("triposr"), "fast", req_fast);

    let report = WorkloadScheduler::matchmake(
        &config,
        &[t_high.clone(), t_fast.clone()],
        std::slice::from_ref(&w_8g),
    );
    assert_eq!(report.unfulfillable.len(), 1);
    assert_eq!(report.unfulfillable[0].0, t_high.id);
    assert!(
        report.unfulfillable[0].1.contains("vram_mb=12288"),
        "expected vram_mb=12288 in reason, got: {}",
        report.unfulfillable[0].1
    );
    assert_eq!(report.assignments.len(), 1);
    assert_eq!(report.assignments[0].task_id, t_fast.id);
    assert_eq!(report.assignments[0].worker_id, w_8g.worker_id);
}

#[tokio::test]
async fn async_schedule_batch_reserves_preset_vram_and_holds_back() {
    let registry = WorkerRegistry::new();
    let queue = TaskQueue::new();
    let notify = Arc::new(Notify::new());

    let w_8g = Uuid::new_v4();
    let (tx_8g, mut rx_8g) = mpsc::channel(16);
    let mut caps_8g = WorkerCapabilities::new("gpu-8g", 4, 16384, true, false, None)
        .with_vram_mb(Some(8192))
        .with_tags(vec!["slots:4".into()]);
    caps_8g.has_gpu = true;
    let reg_8g = registry
        .register_session(
            w_8g,
            caps_8g,
            ProtocolCapabilities::all_supported(),
            "127.0.0.1:9701".parse().unwrap(),
            tx_8g,
            None,
        )
        .await
        .unwrap();

    // Submit two `high` tasks (each needs 6144 MB, so 8192 MB fits only one at a time)
    // plus a trailing generic task that could otherwise steal slots on `w_8g`.
    let t1 = gen3d_task(Some("triposr"), "high", TaskRequirements::generic(1, 60));
    let t2 = gen3d_task(Some("triposr"), "high", TaskRequirements::generic(1, 60));
    let t_cpu = Task::new(
        TaskSpec::command("echo", vec!["filler".into()]),
        TaskRequirements::generic(1, 60),
    );
    let id1 = queue.submit(t1).await.unwrap();
    let id2 = queue.submit(t2).await.unwrap();
    let id_cpu = queue.submit(t_cpu).await.unwrap();

    let scheduler = WorkloadScheduler::new(
        SchedulerConfig::default().without_host_cpu_backpressure(),
        registry.clone(),
        queue.clone(),
        Arc::clone(&notify),
    );

    // Pass 1: t1 placed, t2 skips and holds back w_8g, so t_cpu does NOT steal w_8g.
    let r1 = scheduler.schedule_batch(16).await.unwrap();
    assert_eq!(r1.assignments.len(), 1);
    assert_eq!(r1.assignments[0].task_id, id1);
    assert!(matches!(
        rx_8g.recv().await,
        Some(MasterMessage::AssignTask { task }) if task.id == id1
    ));
    assert_eq!(queue.get_state(&id2).await, Some(TaskState::Queued));
    assert_eq!(queue.get_state(&id_cpu).await, Some(TaskState::Queued));

    // Pass 2 (while t1 is still Scheduled/Running): assigned_loads_with must still reserve 6144 MB.
    let r2 = scheduler.schedule_batch(16).await.unwrap();
    assert!(
        r2.assignments.is_empty(),
        "second high task must not overcommit 8192 MB worker across passes"
    );

    // Finish t1 -> Pass 3 places t2 and then t_cpu.
    queue
        .record_result_from(
            w_8g,
            Some(reg_8g.session_id),
            rusty_grid_core::task::TaskResult::success(w_8g, id1, "done", 10, false),
        )
        .await
        .unwrap();
    let r3 = scheduler.schedule_batch(16).await.unwrap();
    let placed_ids: Vec<_> = r3.assignments.iter().map(|a| a.task_id).collect();
    assert_eq!(placed_ids, vec![id2, id_cpu]);
}

#[tokio::test]
async fn hold_back_does_not_steal_stage6_gpu_reserve_slots() {
    // PROBE 1a: w_gpu (4 cores, 3/4 active -> 1 free slot == gpu_reserved_slots=1),
    // w_cpu (2 cores, 2/2 active). Queue: [t_wide_cpu(cores=2), t_gpu(cores=1, vram=1024)].
    // Non-GPU hold_back must NOT reserve w_gpu and starve the waiting GPU task!
    for use_gen3d_high in [false, true] {
        let registry = WorkerRegistry::new();
        let queue = TaskQueue::new();
        let notify = Arc::new(Notify::new());

        let w_gpu = Uuid::new_v4();
        let w_cpu = Uuid::new_v4();
        let (tx_gpu, mut rx_gpu) = mpsc::channel(16);
        let (tx_cpu, mut rx_cpu) = mpsc::channel(16);

        let caps_gpu = WorkerCapabilities::new("w_gpu", 4, 16384, true, false, None)
            .with_vram_mb(Some(8192))
            .with_tags(vec!["slots:4".into()]);
        let reg_gpu = registry
            .register_session(
                w_gpu,
                caps_gpu,
                ProtocolCapabilities::all_supported(),
                "127.0.0.1:9721".parse().unwrap(),
                tx_gpu,
                None,
            )
            .await
            .unwrap();

        let caps_cpu = WorkerCapabilities::new("w_cpu", 2, 16384, false, false, None)
            .with_tags(vec!["slots:2".into()]);
        let reg_cpu = registry
            .register_session(
                w_cpu,
                caps_cpu,
                ProtocolCapabilities::all_supported(),
                "127.0.0.1:9722".parse().unwrap(),
                tx_cpu,
                None,
            )
            .await
            .unwrap();

        // Saturate w_gpu to 3/4 slots and w_cpu to 2/2 slots.
        let gpu_holder = Task::new(
            TaskSpec::command("hold-gpu", vec![]),
            TaskRequirements::generic(3, 60),
        );
        let cpu_holder = Task::new(
            TaskSpec::command("hold-cpu", vec![]),
            TaskRequirements::generic(2, 60),
        );
        let id_gh = queue.submit(gpu_holder).await.unwrap();
        let id_ch = queue.submit(cpu_holder).await.unwrap();
        queue
            .schedule_tasks_batch_in_sessions(&[
                (id_gh, w_gpu, reg_gpu.session_id),
                (id_ch, w_cpu, reg_cpu.session_id),
            ])
            .await;
        queue
            .mark_running_from(w_gpu, Some(reg_gpu.session_id), &id_gh)
            .await
            .unwrap();
        queue
            .mark_running_from(w_cpu, Some(reg_cpu.session_id), &id_ch)
            .await
            .unwrap();

        let t_wide_cpu = Task::new(
            TaskSpec::command("wide-cpu", vec![]),
            TaskRequirements::generic(2, 60),
        );
        let t_gpu = if use_gen3d_high {
            gen3d_task(Some("triposr"), "high", TaskRequirements::generic(1, 60))
        } else {
            Task::new(
                TaskSpec::new_gpu_compute("gpu-1", 32),
                TaskRequirements {
                    gpu_required: true,
                    ..TaskRequirements::generic(1, 60)
                }
                .with_vram_mb(1024),
            )
        };
        let id_wide = queue.submit_with_priority(t_wide_cpu, 10).await.unwrap();
        let id_gpu = queue.submit_with_priority(t_gpu, 1).await.unwrap();

        let cfg = SchedulerConfig {
            preserve_gpu_for_gpu_tasks: true,
            gpu_reserved_slots: 1,
            ..SchedulerConfig::default().without_host_cpu_backpressure()
        };
        let scheduler = WorkloadScheduler::new(cfg, registry.clone(), queue.clone(), notify);
        let report = scheduler.schedule_batch(16).await.unwrap();

        assert_eq!(
            report.assignments.len(),
            1,
            "use_gen3d_high={use_gen3d_high}: report={report:?}"
        );
        assert_eq!(report.assignments[0].task_id, id_gpu);
        assert_eq!(report.assignments[0].worker_id, w_gpu);
        assert!(matches!(
            rx_gpu.recv().await,
            Some(MasterMessage::AssignTask { task }) if task.id == id_gpu
        ));
        assert!(rx_cpu.try_recv().is_err());
        assert_eq!(queue.get_state(&id_wide).await, Some(TaskState::Queued));
    }

    // PROBE 1b: w_gpu2 (4 cores, 2/4 used, gpu_reserved_slots=2) + w_cpu2 (4 cores, 2/4 used).
    // Queue: [t_wide3(cores=3), t_gpu2(cores=1, vram=1024)].
    // Even with a tie in slots_to_free, hold_back must skip w_gpu2 and let t_gpu2 place onto w_gpu2.
    {
        let registry = WorkerRegistry::new();
        let queue = TaskQueue::new();
        let notify = Arc::new(Notify::new());

        let w_gpu2 = Uuid::from_u128(1); // Lower UUID than w_cpu2 so index/id tie favors w_gpu2 if unfiltered.
        let w_cpu2 = Uuid::from_u128(2);
        let (tx_gpu2, mut rx_gpu2) = mpsc::channel(16);
        let (tx_cpu2, _rx_cpu2) = mpsc::channel(16);

        let caps_gpu2 = WorkerCapabilities::new("w_gpu2", 4, 16384, true, false, None)
            .with_vram_mb(Some(8192))
            .with_tags(vec!["slots:4".into()]);
        let reg_gpu2 = registry
            .register_session(
                w_gpu2,
                caps_gpu2,
                ProtocolCapabilities::all_supported(),
                "127.0.0.1:9723".parse().unwrap(),
                tx_gpu2,
                None,
            )
            .await
            .unwrap();
        let caps_cpu2 = WorkerCapabilities::new("w_cpu2", 4, 16384, false, false, None)
            .with_tags(vec!["slots:4".into()]);
        let reg_cpu2 = registry
            .register_session(
                w_cpu2,
                caps_cpu2,
                ProtocolCapabilities::all_supported(),
                "127.0.0.1:9724".parse().unwrap(),
                tx_cpu2,
                None,
            )
            .await
            .unwrap();

        let g_hold = Task::new(
            TaskSpec::command("g2-hold", vec![]),
            TaskRequirements::generic(2, 60),
        );
        let c_hold = Task::new(
            TaskSpec::command("c2-hold", vec![]),
            TaskRequirements::generic(2, 60),
        );
        let id_gh = queue.submit(g_hold).await.unwrap();
        let id_ch = queue.submit(c_hold).await.unwrap();
        queue
            .schedule_tasks_batch_in_sessions(&[
                (id_gh, w_gpu2, reg_gpu2.session_id),
                (id_ch, w_cpu2, reg_cpu2.session_id),
            ])
            .await;
        queue
            .mark_running_from(w_gpu2, Some(reg_gpu2.session_id), &id_gh)
            .await
            .unwrap();
        queue
            .mark_running_from(w_cpu2, Some(reg_cpu2.session_id), &id_ch)
            .await
            .unwrap();

        let t_wide3 = Task::new(
            TaskSpec::command("wide-3", vec![]),
            TaskRequirements::generic(3, 60),
        );
        let t_gpu2 = Task::new(
            TaskSpec::new_gpu_compute("gpu-2", 32),
            TaskRequirements {
                gpu_required: true,
                ..TaskRequirements::generic(1, 60)
            }
            .with_vram_mb(1024),
        );
        let id_wide3 = queue.submit_with_priority(t_wide3, 10).await.unwrap();
        let id_gpu2 = queue.submit_with_priority(t_gpu2, 1).await.unwrap();

        let cfg = SchedulerConfig {
            preserve_gpu_for_gpu_tasks: true,
            gpu_reserved_slots: 2,
            ..SchedulerConfig::default().without_host_cpu_backpressure()
        };
        let scheduler = WorkloadScheduler::new(cfg, registry, queue.clone(), notify);
        let report = scheduler.schedule_batch(16).await.unwrap();

        assert_eq!(report.assignments.len(), 1, "report={report:?}");
        assert_eq!(report.assignments[0].task_id, id_gpu2);
        assert_eq!(report.assignments[0].worker_id, w_gpu2);
        assert!(matches!(
            rx_gpu2.recv().await,
            Some(MasterMessage::AssignTask { task }) if task.id == id_gpu2
        ));
        assert_eq!(queue.get_state(&id_wide3).await, Some(TaskState::Queued));
    }
}

#[tokio::test]
async fn reregister_capability_downgrade_fails_queued_3d_tasks() {
    let registry = WorkerRegistry::new();
    let queue = TaskQueue::new();
    let notify = Arc::new(Notify::new());

    let w_3d = Uuid::new_v4();
    let (tx, _rx) = mpsc::channel(16);
    let caps_initial = WorkerCapabilities::new("w-3d", 4, 16384, true, false, None)
        .with_vram_mb(Some(8192))
        .with_blender(true, Some("4.5.4".into()))
        .with_tags(vec!["slots:4".into()]);
    let reg = registry
        .register_session(
            w_3d,
            caps_initial,
            ProtocolCapabilities::all_supported(),
            "127.0.0.1:9725".parse().unwrap(),
            tx.clone(),
            None,
        )
        .await
        .unwrap();

    // Saturate w_3d so queued 3D tasks remain Queued until the worker re-registers.
    let blocker = Task::new(
        TaskSpec::command("busy", vec![]),
        TaskRequirements::generic(4, 60),
    );
    let id_busy = queue.submit(blocker).await.unwrap();
    queue
        .schedule_tasks_batch_in_sessions(&[(id_busy, w_3d, reg.session_id)])
        .await;

    let id_blender = queue.submit(autorig_task(Some("blender"))).await.unwrap();
    let id_high = queue
        .submit(gen3d_task(
            Some("triposr"),
            "high",
            TaskRequirements::generic(1, 60),
        ))
        .await
        .unwrap();

    // Re-register w_3d with downgraded capabilities (has_blender=false, vram_mb=2048).
    let caps_downgraded = WorkerCapabilities::new("w-3d", 4, 16384, true, false, None)
        .with_vram_mb(Some(2048))
        .with_blender(false, None)
        .with_tags(vec!["slots:4".into()]);
    registry
        .register_session(
            w_3d,
            caps_downgraded,
            ProtocolCapabilities::all_supported(),
            "127.0.0.1:9725".parse().unwrap(),
            tx,
            None,
        )
        .await
        .unwrap();

    let cfg = SchedulerConfig::default()
        .without_host_cpu_backpressure()
        .with_fail_unfulfillable_tasks(true);
    let scheduler = WorkloadScheduler::new(cfg, registry, queue.clone(), notify);
    let report = scheduler.schedule_batch(16).await.unwrap();

    assert!(report.assignments.is_empty());
    let unfulfillable: HashMap<_, _> = report.unfulfillable.into_iter().collect();
    assert!(
        unfulfillable
            .get(&id_blender)
            .is_some_and(|r| r.contains("Blender")),
        "expected Blender unfulfillable reason: {unfulfillable:?}"
    );
    assert!(
        unfulfillable
            .get(&id_high)
            .is_some_and(|r| r.contains("vram_mb=6144")),
        "expected vram_mb=6144 unfulfillable reason: {unfulfillable:?}"
    );
    assert_eq!(queue.get_state(&id_blender).await, Some(TaskState::Failed));
    assert_eq!(queue.get_state(&id_high).await, Some(TaskState::Failed));
}

#[tokio::test]
async fn unregister_last_worker_keeps_queued() {
    let registry = WorkerRegistry::new();
    let queue = TaskQueue::new();
    let notify = Arc::new(Notify::new());

    let w_3d = Uuid::new_v4();
    let (tx, _rx) = mpsc::channel(8);
    let caps = WorkerCapabilities::new("w-3d", 4, 16384, true, false, None)
        .with_vram_mb(Some(8192))
        .with_blender(true, Some("4.5.4".into()));
    let _reg = registry
        .register_session(
            w_3d,
            caps,
            ProtocolCapabilities::all_supported(),
            "127.0.0.1:9726".parse().unwrap(),
            tx,
            None,
        )
        .await
        .unwrap();

    assert!(registry.unregister(&w_3d, None).await.unwrap());
    let id_blender = queue.submit(autorig_task(Some("blender"))).await.unwrap();

    let cfg = SchedulerConfig::default()
        .without_host_cpu_backpressure()
        .with_fail_unfulfillable_tasks(true);
    let scheduler = WorkloadScheduler::new(cfg, registry, queue.clone(), notify);
    let report = scheduler.schedule_batch(16).await.unwrap();

    assert!(report.unfulfillable.is_empty(), "{report:?}");
    assert!(matches!(
        &report.skipped[..],
        [(id, ScheduleSkipReason::NoEligibleWorkers)] if *id == id_blender
    ));
    assert_eq!(queue.get_state(&id_blender).await, Some(TaskState::Queued));
}

#[tokio::test]
async fn reaper_straggler_respects_requires_blender_and_preset_vram() {
    let registry = WorkerRegistry::new();
    let queue = TaskQueue::new();
    let notify = Arc::new(Notify::new());
    let _sched = WorkloadScheduler::new(
        SchedulerConfig::default().without_host_cpu_backpressure(),
        registry.clone(),
        queue.clone(),
        Arc::clone(&notify),
    );
    let sb = _sched.speed_book();

    let w_primary = Uuid::new_v4();
    let w_2g_noblender = Uuid::new_v4();
    let w_8g_blender = Uuid::new_v4();
    let (tx_p, _rx_p) = mpsc::channel(16);
    let (tx_2g, mut rx_2g) = mpsc::channel(16);
    let (tx_8g, mut rx_8g) = mpsc::channel(16);

    let reg_p = registry
        .register_session(
            w_primary,
            WorkerCapabilities::new("primary", 8, 16384, true, false, None)
                .with_vram_mb(Some(8192))
                .with_blender(true, Some("4.5.4".into())),
            ProtocolCapabilities::all_supported(),
            "127.0.0.1:9727".parse().unwrap(),
            tx_p,
            None,
        )
        .await
        .unwrap();
    let _reg_2g = registry
        .register_session(
            w_2g_noblender,
            WorkerCapabilities::new("gpu-2g-noblender", 8, 16384, true, false, None)
                .with_vram_mb(Some(2048))
                .with_blender(false, None),
            ProtocolCapabilities::all_supported(),
            "127.0.0.1:9728".parse().unwrap(),
            tx_2g,
            None,
        )
        .await
        .unwrap();
    let _reg_8g = registry
        .register_session(
            w_8g_blender,
            WorkerCapabilities::new("gpu-8g-blender", 8, 16384, true, false, None)
                .with_vram_mb(Some(8192))
                .with_blender(true, Some("4.5.4".into())),
            ProtocolCapabilities::all_supported(),
            "127.0.0.1:9729".parse().unwrap(),
            tx_8g,
            None,
        )
        .await
        .unwrap();

    let mut blender_task = autorig_task(Some("blender"));
    blender_task.tags.push(OPT_REPEATABLE_TAG.to_string());
    let mut high_task = gen3d_task(Some("triposr"), "high", TaskRequirements::generic(1, 60));
    high_task.tags.push(OPT_REPEATABLE_TAG.to_string());

    let b_key = TaskKey::for_task(&blender_task);
    let h_key = TaskKey::for_task(&high_task);
    let dummy_b = TaskResult::success(w_2g_noblender, blender_task.id, "ok", 10, false);
    let dummy_h = TaskResult::success(w_2g_noblender, high_task.id, "ok", 10, true);
    mutate_shared_speed_book(sb, |book| {
        book.record_result(w_2g_noblender, b_key, 60, &dummy_b);
        book.record_result(w_8g_blender, b_key, 60, &dummy_b);
        book.record_result(w_2g_noblender, h_key, 60, &dummy_h);
        book.record_result(w_8g_blender, h_key, 60, &dummy_h);
    });

    let id_b = queue.submit(blender_task).await.unwrap();
    let id_h = queue.submit(high_task).await.unwrap();
    queue
        .schedule_tasks_batch_in_sessions(&[
            (id_b, w_primary, reg_p.session_id),
            (id_h, w_primary, reg_p.session_id),
        ])
        .await;
    queue
        .mark_running_from(w_primary, Some(reg_p.session_id), &id_b)
        .await
        .unwrap();
    queue
        .mark_running_from(w_primary, Some(reg_p.session_id), &id_h)
        .await
        .unwrap();

    let (shutdown_tx, shutdown_rx) = watch::channel(false);
    let reaper_cfg = ReaperConfig::default()
        .with_scan_interval(Duration::from_millis(20))
        .with_timeout(Duration::from_secs(30))
        .with_straggler_min_elapsed(Duration::from_millis(20))
        .with_straggler_multiplier(1.5)
        .with_scheduler_config(SchedulerConfig::default().without_host_cpu_backpressure());
    let reaper_handle =
        spawn_reaper_with_queue(registry, queue, notify, None, reaper_cfg, shutdown_rx);

    let mut got_8g = Vec::new();
    let deadline = tokio::time::Instant::now() + Duration::from_secs(3);
    while got_8g.len() < 2 && tokio::time::Instant::now() < deadline {
        if let Ok(Some(MasterMessage::AssignTask { task })) =
            tokio::time::timeout(Duration::from_millis(100), rx_8g.recv()).await
        {
            if !got_8g.contains(&task.id) {
                got_8g.push(task.id);
            }
        }
    }
    let _ = shutdown_tx.send(true);
    let _ = tokio::time::timeout(Duration::from_secs(2), reaper_handle).await;

    assert!(
        rx_2g.try_recv().is_err(),
        "2GB non-Blender worker must never receive blender or high-VRAM straggler copies"
    );
    assert!(
        got_8g.contains(&id_b) && got_8g.contains(&id_h),
        "expected both blender and high-VRAM straggler copies on 8GB Blender worker, got {got_8g:?}"
    );
}

#[test]
fn speedbook_honors_prefer_worker_and_isolates_avoid() {
    // PROBE 2a: prefer:worker=slow_node overrides SpeedBook when fast_node is busy.
    let w_fast = make_worker("fast_node", 2, 8192, false, false, None, false);
    let w_slow = make_worker("slow_node", 2, 8192, false, false, None, false);

    let pref_task = Task::new(
        TaskSpec::command("bench", vec!["1".into()]),
        TaskRequirements::generic(1, 60),
    )
    .with_tags(vec![format!("prefer:worker={}", w_slow.worker_id)]);
    let key = TaskKey::for_task(&pref_task);

    let mut sb = SpeedBook::new();
    let ok_fast = TaskResult::success(w_fast.worker_id, pref_task.id, "ok", 10, false);
    let ok_slow = TaskResult::success(w_slow.worker_id, pref_task.id, "ok", 500, false);
    for _ in 0..3 {
        sb.record_result(w_fast.worker_id, key, 60, &ok_fast);
        sb.record_result(w_slow.worker_id, key, 60, &ok_slow);
    }

    let mut cfg = SchedulerConfig::default().without_host_cpu_backpressure();
    cfg.speed_hold_factor = 2.0;
    cfg.speed_min_samples = 1;

    let mut fast_busy = w_fast.clone();
    fast_busy.active_tasks = 2; // w_fast is full (2/2)
    let mut running = HashMap::new();
    running.insert(fast_busy.worker_id, vec![(key, Duration::from_millis(5))]);

    let report_pref = WorkloadScheduler::matchmake_with_speed(
        &cfg,
        std::slice::from_ref(&pref_task),
        &[fast_busy.clone(), w_slow.clone()],
        &HashMap::new(),
        &HashMap::new(),
        &HashSet::new(),
        &sb,
        &running,
    );
    assert_eq!(report_pref.assignments.len(), 1, "{report_pref:?}");
    assert_eq!(
        report_pref.assignments[0].worker_id, w_slow.worker_id,
        "prefer:worker=slow_node must place onto slow_node even with fast_node in SpeedBook"
    );

    // PROBE 2b: T1 with avoid=[w_fast] places onto w_slow without poisoning dead_speed for T2.
    let t1 = Task::new(
        TaskSpec::command("bench", vec!["1".into()]),
        TaskRequirements::generic(1, 60),
    );
    let t2 = Task::new(
        TaskSpec::command("bench", vec!["1".into()]),
        TaskRequirements::generic(1, 60),
    );
    let mut avoid = HashMap::new();
    avoid.insert(t1.id, vec![w_fast.worker_id]);

    let report_avoid = WorkloadScheduler::matchmake_with_speed(
        &cfg,
        &[t1.clone(), t2.clone()],
        &[w_fast.clone(), w_slow.clone()],
        &avoid,
        &HashMap::new(),
        &HashSet::new(),
        &sb,
        &HashMap::new(),
    );
    let placed: HashMap<_, _> = report_avoid
        .assignments
        .iter()
        .map(|a| (a.task_id, a.worker_id))
        .collect();
    assert_eq!(placed.get(&t1.id).copied(), Some(w_slow.worker_id));
    assert_eq!(
        placed.get(&t2.id).copied(),
        Some(w_fast.worker_id),
        "T1's avoid list must not poison T2 from placing on w_fast: {report_avoid:?}"
    );
}
