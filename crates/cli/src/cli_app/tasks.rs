//! `oxideswarm status`, `cancel`, `result`, and `workers` client commands.

use std::path::Path;
use std::process::ExitCode;
use std::time::Duration;

use rusty_grid_core::protocol::{ClientMessage, ClientResponse};
use rusty_grid_core::task::TaskId;

use crate::config::{resolve_string, ConfigFile, SecretSection, CLIENT_MASTER_ENV_VARS};
use crate::connect::{connect_master, report_master_failure, DEFAULT_MASTER_ADDR};
use crate::secret_cmd::{client_secret, peer_insecure, SecretOpts};

use super::{exchange, resolve_relay_url, CancelArgs, ResultArgs, StatusArgs, WorkersArgs};

pub(super) async fn run_status(
    args: StatusArgs,
    config_file: Option<ConfigFile>,
    secret_opts: SecretOpts,
) -> ExitCode {
    let relay_url = match resolve_relay_url(
        args.relay_url.clone(),
        config_file.as_ref().and_then(|c| c.status_relay_url()),
        crate::connect::client_p2p_minimal_requested(),
    ) {
        Ok(url) => url,
        Err(e) => {
            eprintln!("{e}");
            return ExitCode::FAILURE;
        }
    };
    crate::connect::with_client_relay_url(
        relay_url,
        run_status_inner(args, config_file, secret_opts),
    )
    .await
}

async fn run_status_inner(
    args: StatusArgs,
    config_file: Option<ConfigFile>,
    secret_opts: SecretOpts,
) -> ExitCode {
    let status_cfg = config_file.as_ref().and_then(|c| c.status.clone());
    let master_addr = resolve_string(
        args.master,
        CLIENT_MASTER_ENV_VARS,
        status_cfg.as_ref().and_then(|s| s.target_master()),
        DEFAULT_MASTER_ADDR,
    );
    let secret = match client_secret(
        &secret_opts,
        config_file.as_ref(),
        SecretSection::Status,
        &master_addr,
        peer_insecure(args.insecure, status_cfg.as_ref().and_then(|s| s.insecure)),
    ) {
        Ok(secret) => secret,
        Err(code) => return code,
    };

    let mut transport = match connect_master(&master_addr, secret.as_ref()).await {
        Ok(t) => t,
        Err(e) => {
            return report_master_failure(
                &master_addr,
                secret.is_some(),
                "Failed to connect to Master",
                &e,
            )
        }
    };
    let had_secret = secret.is_some();

    if let Some(task_uuid) = args.task_id {
        let task_id = TaskId(task_uuid);
        let msg = ClientMessage::GetTaskStatus { task_id };
        let response = match exchange(
            &mut transport,
            &master_addr,
            had_secret,
            &msg,
            "Failed to query task status",
        )
        .await
        {
            Ok(response) => response,
            Err(code) => return code,
        };
        match response {
            ClientResponse::TaskStatusInfo {
                task_id,
                status,
                state_name,
                assigned_worker,
                error,
            } => {
                if args.json {
                    println!(
                        "{}",
                        serde_json::json!({
                            "task_id": task_id.to_string(),
                            "status": format!("{:?}", status),
                            "state": state_name,
                            "assigned_worker": assigned_worker,
                            "error": error
                        })
                    );
                } else {
                    println!("Task ID:        {task_id}");
                    println!("Status:         {:?}", status);
                    println!("State:          {state_name}");
                    if let Some(w) = assigned_worker {
                        println!("Worker:         {w}");
                    }
                    if let Some(err) = error {
                        println!("Error:          {err}");
                    }
                }
                ExitCode::SUCCESS
            }
            ClientResponse::Error { message } => {
                eprintln!("Error: {message}");
                ExitCode::FAILURE
            }
            other => {
                eprintln!("Unexpected response: {other:?}");
                ExitCode::FAILURE
            }
        }
    } else if args.workers {
        let msg = ClientMessage::ListWorkers;
        let response = match exchange(
            &mut transport,
            &master_addr,
            had_secret,
            &msg,
            "Failed to list workers",
        )
        .await
        {
            Ok(response) => response,
            Err(code) => return code,
        };
        match response {
            ClientResponse::WorkerList { workers } => {
                if args.json {
                    let enriched_workers: Vec<serde_json::Value> = workers
                        .iter()
                        .map(|w| {
                            let link = parse_worker_link(w);
                            let speed = parse_worker_speed(w);
                            let mut val = serde_json::to_value(w).unwrap_or_default();
                            if let Some(obj) = val.as_object_mut() {
                                if let Some(l) = link {
                                    obj.insert("link".to_string(), serde_json::Value::String(l));
                                }
                                if let Some(s) = speed {
                                    if let Some(num) = serde_json::Number::from_f64(s) {
                                        obj.insert(
                                            "speed".to_string(),
                                            serde_json::Value::Number(num),
                                        );
                                    }
                                }
                            }
                            val
                        })
                        .collect();
                    println!(
                        "{}",
                        serde_json::to_string_pretty(&enriched_workers).unwrap()
                    );
                } else {
                    let connected = workers.iter().filter(|w| worker_is_connected(w)).count();
                    println!(
                        "Workers ({} connected, {} registered):",
                        connected,
                        workers.len()
                    );
                    for w in &workers {
                        let state = match worker_state(w) {
                            Some(state) if state != "connected" => format!(" [{state}]"),
                            _ => String::new(),
                        };
                        let speed_suffix = parse_worker_speed_str(w)
                            .map(|s| format!(", Speed: {s}x"))
                            .unwrap_or_default();
                        println!(
                            "- Worker [{}] (Cores: {}, RAM: {}MB, GPU: {}, Simulated: {}{}){}",
                            w.name,
                            w.cpu_cores,
                            w.ram_mb,
                            w.has_gpu,
                            w.is_simulated_gpu,
                            speed_suffix,
                            state,
                        );
                        if let Some(link) = parse_worker_link(w) {
                            println!("  └─ {}", link);
                        }
                    }
                }
                ExitCode::SUCCESS
            }
            other => {
                eprintln!("Unexpected response: {other:?}");
                ExitCode::FAILURE
            }
        }
    } else {
        let msg = ClientMessage::ClusterStatus;
        let response = match exchange(
            &mut transport,
            &master_addr,
            had_secret,
            &msg,
            "Failed to get cluster status",
        )
        .await
        {
            Ok(response) => response,
            Err(code) => return code,
        };
        match response {
            ClientResponse::ClusterStatus {
                total_tasks,
                pending_tasks,
                running_tasks,
                completed_tasks,
                failed_tasks,
                active_bytes,
                max_active_bytes,
                store_error,
                workers,
            } => {
                if args.json {
                    let enriched_workers: Vec<serde_json::Value> = workers
                        .iter()
                        .map(|w| {
                            let link = parse_worker_link(w);
                            let speed = parse_worker_speed(w);
                            let mut val = serde_json::to_value(w).unwrap_or_default();
                            if let Some(obj) = val.as_object_mut() {
                                if let Some(l) = link {
                                    obj.insert("link".to_string(), serde_json::Value::String(l));
                                }
                                if let Some(s) = speed {
                                    if let Some(num) = serde_json::Number::from_f64(s) {
                                        obj.insert(
                                            "speed".to_string(),
                                            serde_json::Value::Number(num),
                                        );
                                    }
                                }
                            }
                            val
                        })
                        .collect();
                    let mut out = serde_json::json!({
                        "total_tasks": total_tasks,
                        "pending_tasks": pending_tasks,
                        "running_tasks": running_tasks,
                        "completed_tasks": completed_tasks,
                        "failed_tasks": failed_tasks,
                        "active_bytes": active_bytes,
                        "max_active_bytes": max_active_bytes,
                        "worker_count": workers.len(),
                        "workers": enriched_workers
                    });
                    if let Some(ref err) = store_error {
                        out["store_error"] = serde_json::Value::String(err.clone());
                    }
                    println!("{}", out);
                } else {
                    println!("=== RustyGrid Cluster Status ===");
                    println!(
                        "Connected Workers: {} ({} registered)",
                        workers.iter().filter(|w| worker_is_connected(w)).count(),
                        workers.len()
                    );
                    println!("Total Tasks:      {}", total_tasks);
                    println!("Pending Tasks:    {}", pending_tasks);
                    println!("Running Tasks:    {}", running_tasks);
                    println!("Completed Tasks:  {}", completed_tasks);
                    println!("Failed Tasks:     {}", failed_tasks);
                    println!(
                        "Active Budget:    {} / {} bytes ({:.2} / {:.2} MiB)",
                        active_bytes,
                        max_active_bytes,
                        active_bytes as f64 / (1024.0 * 1024.0),
                        max_active_bytes as f64 / (1024.0 * 1024.0),
                    );
                    if let Some(ref err) = store_error {
                        println!(
                            "Task Store Error: {} (durability is gone until restart)",
                            err
                        );
                    }
                    if !workers.is_empty() {
                        println!("\nWorkers:");
                        for w in &workers {
                            let speed_suffix = parse_worker_speed_str(w)
                                .map(|s| format!(", Speed: {s}x"))
                                .unwrap_or_default();
                            println!(
                                "  • [{}] (Cores: {}, RAM: {}MB, GPU: {}{})",
                                w.name, w.cpu_cores, w.ram_mb, w.has_gpu, speed_suffix
                            );
                            if let Some(link) = parse_worker_link(w) {
                                println!("    └─ {}", link);
                            }
                        }
                    }
                }
                ExitCode::SUCCESS
            }
            other => {
                eprintln!("Unexpected response: {other:?}");
                ExitCode::FAILURE
            }
        }
    }
}

pub async fn run_cancel(
    args: CancelArgs,
    config_file: Option<ConfigFile>,
    secret_opts: SecretOpts,
) -> ExitCode {
    let relay_url = match resolve_relay_url(
        args.relay_url.clone(),
        config_file.as_ref().and_then(|c| c.status_relay_url()),
        crate::connect::client_p2p_minimal_requested(),
    ) {
        Ok(url) => url,
        Err(e) => {
            eprintln!("{e}");
            return ExitCode::FAILURE;
        }
    };
    crate::connect::with_client_relay_url(
        relay_url,
        run_cancel_inner(args, config_file, secret_opts),
    )
    .await
}

async fn run_cancel_inner(
    args: CancelArgs,
    config_file: Option<ConfigFile>,
    secret_opts: SecretOpts,
) -> ExitCode {
    let status_cfg = config_file.as_ref().and_then(|c| c.status.clone());
    let master_addr = resolve_string(
        args.master,
        CLIENT_MASTER_ENV_VARS,
        status_cfg.as_ref().and_then(|s| s.target_master()),
        DEFAULT_MASTER_ADDR,
    );
    let secret = match client_secret(
        &secret_opts,
        config_file.as_ref(),
        SecretSection::Status,
        &master_addr,
        peer_insecure(args.insecure, status_cfg.as_ref().and_then(|s| s.insecure)),
    ) {
        Ok(secret) => secret,
        Err(code) => return code,
    };

    let mut transport = match connect_master(&master_addr, secret.as_ref()).await {
        Ok(t) => t,
        Err(e) => {
            return report_master_failure(
                &master_addr,
                secret.is_some(),
                "Failed to connect to Master",
                &e,
            )
        }
    };
    let had_secret = secret.is_some();
    let task_id = TaskId(args.task_id);
    let msg = ClientMessage::CancelTask { task_id };
    let response = match exchange(
        &mut transport,
        &master_addr,
        had_secret,
        &msg,
        "Failed to cancel task",
    )
    .await
    {
        Ok(response) => response,
        Err(code) => return code,
    };

    match response {
        ClientResponse::TaskCancelled { task_id, success } => {
            if args.json {
                println!(
                    "{}",
                    serde_json::json!({
                        "task_id": task_id.to_string(),
                        "cancelled": success,
                    })
                );
            } else if success {
                println!("Task {task_id} cancelled.");
            } else {
                eprintln!("Task {task_id} could not be cancelled (not found or already finished).");
            }
            if success {
                ExitCode::SUCCESS
            } else {
                ExitCode::FAILURE
            }
        }
        ClientResponse::Error { message } => {
            if args.json {
                println!("{}", serde_json::json!({ "error": message }));
            } else {
                eprintln!("Error: {message}");
            }
            ExitCode::FAILURE
        }
        other => {
            eprintln!("Unexpected response: {other:?}");
            ExitCode::FAILURE
        }
    }
}

pub async fn run_result(
    args: ResultArgs,
    config_file: Option<ConfigFile>,
    secret_opts: SecretOpts,
) -> ExitCode {
    let relay_url = match resolve_relay_url(
        args.relay_url.clone(),
        config_file.as_ref().and_then(|c| c.status_relay_url()),
        crate::connect::client_p2p_minimal_requested(),
    ) {
        Ok(url) => url,
        Err(e) => {
            eprintln!("{e}");
            return ExitCode::FAILURE;
        }
    };
    crate::connect::with_client_relay_url(
        relay_url,
        run_result_inner(args, config_file, secret_opts),
    )
    .await
}

async fn run_result_inner(
    args: ResultArgs,
    config_file: Option<ConfigFile>,
    secret_opts: SecretOpts,
) -> ExitCode {
    let status_cfg = config_file.as_ref().and_then(|c| c.status.clone());
    let master_addr = resolve_string(
        args.master,
        CLIENT_MASTER_ENV_VARS,
        status_cfg.as_ref().and_then(|s| s.target_master()),
        DEFAULT_MASTER_ADDR,
    );
    let secret = match client_secret(
        &secret_opts,
        config_file.as_ref(),
        SecretSection::Status,
        &master_addr,
        peer_insecure(args.insecure, status_cfg.as_ref().and_then(|s| s.insecure)),
    ) {
        Ok(secret) => secret,
        Err(code) => return code,
    };

    let mut transport = match connect_master(&master_addr, secret.as_ref()).await {
        Ok(t) => t,
        Err(e) => {
            return report_master_failure(
                &master_addr,
                secret.is_some(),
                "Failed to connect to Master",
                &e,
            )
        }
    };
    let had_secret = secret.is_some();
    let task_id = TaskId(args.task_id);
    let msg = ClientMessage::GetTaskResult { task_id };

    loop {
        let response = match exchange(
            &mut transport,
            &master_addr,
            had_secret,
            &msg,
            "Failed to fetch task result",
        )
        .await
        {
            Ok(response) => response,
            Err(code) => return code,
        };

        match response {
            ClientResponse::TaskCompleted { task_id, result } => {
                let output_dir = args.output_dir.as_deref().unwrap_or(Path::new("."));
                let mut output_download_error: Option<String> = None;
                let saved_outputs = if result.outputs.is_empty() {
                    None
                } else {
                    let downloaded =
                        match crate::blobs::BlobConnection::connect(&master_addr, secret.as_ref())
                            .await
                        {
                            Ok(connection) => {
                                let res = crate::blobs::download_outputs(
                                    &connection,
                                    &result.outputs,
                                    output_dir,
                                )
                                .await;
                                connection.close().await;
                                res
                            }
                            Err(e) => Err(e.to_string()),
                        };
                    match downloaded {
                        Ok(saved) => Some(saved),
                        Err(e) => {
                            output_download_error = Some(e);
                            None
                        }
                    }
                };
                let mut artifact_save_error: Option<String> = None;
                let saved_artifacts = if !result.artifacts.is_empty() {
                    match crate::connect::save_output_artifacts(&result.artifacts, output_dir) {
                        Ok(saved) => Some(saved),
                        Err(e) => {
                            artifact_save_error = Some(e);
                            None
                        }
                    }
                } else {
                    None
                };

                if args.json {
                    let mut value = serde_json::to_value(&result).unwrap_or_default();
                    if let Some(obj) = value.as_object_mut() {
                        if let Some(ref saved) = saved_artifacts {
                            let saved_strs: Vec<String> =
                                saved.iter().map(|p| p.display().to_string()).collect();
                            obj.insert(
                                "saved_artifacts".to_string(),
                                serde_json::json!(saved_strs),
                            );
                        }
                        if let Some(ref err) = artifact_save_error {
                            obj.insert("artifact_save_error".to_string(), serde_json::json!(err));
                        }
                        if let Some(ref saved) = saved_outputs {
                            let saved_strs: Vec<String> =
                                saved.iter().map(|p| p.display().to_string()).collect();
                            obj.insert("saved_outputs".to_string(), serde_json::json!(saved_strs));
                        }
                        if let Some(ref err) = output_download_error {
                            obj.insert("output_download_error".to_string(), serde_json::json!(err));
                        }
                    }
                    println!("{}", serde_json::to_string_pretty(&value).unwrap());
                } else {
                    if result.is_success() {
                        println!("Task {task_id} completed successfully.");
                    } else if result.exit_code == 130 {
                        println!("Task {task_id} was cancelled.");
                    } else {
                        println!("Task {task_id} failed.");
                    }
                    if !result.worker_id.is_nil() {
                        println!("Worker: {}", result.worker_id);
                    }
                    println!("Exit Code: {}", result.exit_code);
                    println!("Execution Time: {}ms", result.execution_time_ms);
                    if let Some(ref saved) = saved_artifacts {
                        println!(
                            "Artifacts ({} saved to {}):",
                            saved.len(),
                            output_dir.display()
                        );
                        for (artifact, path) in result.artifacts.iter().zip(saved.iter()) {
                            println!(
                                "  - {} ({} bytes, sha256: {}) -> {}",
                                artifact.path,
                                artifact.size,
                                artifact.sha256_hex(),
                                path.display()
                            );
                        }
                    }
                    if let Some(ref saved) = saved_outputs {
                        println!(
                            "Output files ({} saved to {}):",
                            saved.len(),
                            output_dir.display()
                        );
                        for (output, path) in result.outputs.iter().zip(saved.iter()) {
                            println!(
                                "  - {} ({} bytes, blake3: {}) -> {}",
                                output.path,
                                output.size,
                                output.id.short(),
                                path.display()
                            );
                        }
                    }
                    if !result.stdout.is_empty() {
                        println!("Stdout:\n{}", result.stdout_str().trim());
                    }
                    if !result.stderr.is_empty() {
                        eprintln!("Stderr:\n{}", result.stderr_str().trim());
                    }
                    if let Some(ref err) = result.error {
                        eprintln!("Error: {err}");
                    }
                }
                if let Some(err) = output_download_error {
                    eprintln!("Downloading the output files failed: {err}");
                    return ExitCode::FAILURE;
                }
                if let Some(err) = artifact_save_error {
                    eprintln!("Failed to save output artifacts: {err}");
                    return ExitCode::FAILURE;
                }
                return if result.is_success() {
                    ExitCode::SUCCESS
                } else {
                    let code = result.exit_code.clamp(1, 255) as u8;
                    ExitCode::from(if code == 3 { 1 } else { code })
                };
            }
            ClientResponse::TaskStatusInfo {
                task_id,
                status,
                state_name,
                assigned_worker,
                error,
            } => {
                let is_terminal = matches!(
                    status,
                    rusty_grid_core::TaskStatus::Completed
                        | rusty_grid_core::TaskStatus::Failed
                        | rusty_grid_core::TaskStatus::Cancelled
                );
                if args.wait && !is_terminal {
                    tokio::time::sleep(Duration::from_millis(100)).await;
                    continue;
                }
                if args.json {
                    println!(
                        "{}",
                        serde_json::json!({
                            "task_id": task_id.to_string(),
                            "status": format!("{:?}", status),
                            "state": state_name,
                            "assigned_worker": assigned_worker,
                            "error": error,
                        })
                    );
                } else {
                    println!("Task ID:        {task_id}");
                    println!("Status:         {:?}", status);
                    println!("State:          {state_name}");
                    if let Some(w) = assigned_worker {
                        println!("Worker:         {w}");
                    }
                    if let Some(err) = error {
                        eprintln!("Error:          {err}");
                    }
                }
                return if status == rusty_grid_core::TaskStatus::Completed {
                    ExitCode::SUCCESS
                } else if !is_terminal {
                    ExitCode::from(3)
                } else {
                    ExitCode::FAILURE
                };
            }
            ClientResponse::Error { message } => {
                if args.json {
                    println!("{}", serde_json::json!({ "error": message }));
                } else {
                    eprintln!("Error: {message}");
                }
                return ExitCode::FAILURE;
            }
            other => {
                eprintln!("Unexpected response: {other:?}");
                return ExitCode::FAILURE;
            }
        }
    }
}

/// The `state:` tag the master adds to each worker in `ListWorkers` and `ClusterStatus`
/// (`connected`, `disconnected`, ...), or `None` when the master sent none.
pub(super) fn worker_state(w: &rusty_grid_core::WorkerCapabilities) -> Option<&str> {
    w.tags.iter().find_map(|tag| tag.strip_prefix("state:"))
}

/// Whether a listed worker is connected. A worker without a `state:` tag counts as connected,
/// which is how such a listing was meant before the master added the tag.
pub(super) fn worker_is_connected(w: &rusty_grid_core::WorkerCapabilities) -> bool {
    worker_state(w).is_none_or(|state| state != "disconnected")
}

pub(super) fn parse_worker_speed_str(w: &rusty_grid_core::WorkerCapabilities) -> Option<&str> {
    rusty_grid_core::placement::tag_value(&w.tags, rusty_grid_core::placement::SPEED_TAG_PREFIX)
        .filter(|v| v.parse::<f64>().is_ok_and(|f| f.is_finite() && f > 0.0))
}

pub(super) fn parse_worker_speed(w: &rusty_grid_core::WorkerCapabilities) -> Option<f64> {
    parse_worker_speed_str(w).and_then(|v| v.parse::<f64>().ok())
}

pub(super) fn parse_worker_link(w: &rusty_grid_core::WorkerCapabilities) -> Option<String> {
    for tag in &w.tags {
        if let Some(rest) = tag.strip_prefix("link:") {
            if let Some((kind, rtt)) = rest.split_once(':') {
                return Some(format!("Link: {} | RTT: {}", kind, rtt));
            } else {
                return Some(format!("Link: {}", rest));
            }
        }
    }
    Some("Link: TCP/LAN".to_string())
}

pub(super) async fn run_workers(
    args: WorkersArgs,
    config_file: Option<ConfigFile>,
    secret_opts: SecretOpts,
) -> ExitCode {
    let status_args = StatusArgs {
        master: args.master,
        relay_url: args.relay_url,
        task_id: None,
        workers: true,
        json: args.json,
        insecure: args.insecure,
    };
    run_status(status_args, config_file, secret_opts).await
}
