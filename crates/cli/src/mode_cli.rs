//! Command-line interface and interactive menu runner for OxideSwarm Workflow Modes.

use std::collections::HashMap;
use std::fs;
use std::io::{self, Write};
use std::path::{Path, PathBuf};
use std::process::{Command, Stdio};
use std::time::Instant;

use clap::Parser;
use serde::{Deserialize, Serialize};

use rusty_grid_core::mode::{
    execute_mode_action, run_flaky_detection, ModeExecutionResult, WorkflowMode,
};

// ANSI terminal colors for modern, elegant CLI output
const CLR_RESET: &str = "\x1b[0m";
const CLR_BOLD: &str = "\x1b[1m";
const CLR_CYAN: &str = "\x1b[36m";
const CLR_GREEN: &str = "\x1b[32m";
const CLR_YELLOW: &str = "\x1b[33m";
const CLR_RED: &str = "\x1b[31m";
const CLR_MAGENTA: &str = "\x1b[35m";
const CLR_DIM: &str = "\x1b[2m";

#[derive(Parser, Debug, Clone, Serialize, Deserialize)]
#[command(
    name = "ox-mode",
    about = "OxideSwarm Workflow Modes & Presets: test, dev, doc, research",
    version
)]
pub struct ModeCliArgs {
    /// Workflow mode: test (kiểm tra), dev (phát triển), doc (tài liệu), research (nghiên cứu)
    #[arg(index = 1)]
    pub mode: Option<String>,

    /// Mode action (e.g. quick, full, lint, flaky, fast, cluster, status, stop, build, verify, export, profile, bench, distributed, report)
    #[arg(index = 2)]
    pub action: Option<String>,

    /// Specific action override flag
    #[arg(long, short = 'a')]
    pub action_opt: Option<String>,

    /// Number of iterations for flaky test detection (default: 5)
    #[arg(long, default_value = "5")]
    pub iterations: usize,

    /// Format output as JSON
    #[arg(long)]
    pub json: bool,

    /// Open interactive menu
    #[arg(short = 'i', long)]
    pub interactive: bool,
}

/// Helper to locate the workspace root directory reliably.
pub fn find_workspace_root() -> PathBuf {
    if let Ok(cargo_dir) = std::env::var("CARGO_MANIFEST_DIR") {
        let p = PathBuf::from(cargo_dir);
        if p.join("../../Cargo.toml").exists() {
            return p.join("../..");
        }
        if p.join("Cargo.toml").exists() && p.join("crates").exists() {
            return p;
        }
    }

    let mut cur = std::env::current_dir().unwrap_or_else(|_| PathBuf::from("."));
    for _ in 0..10 {
        if cur.join("Cargo.toml").exists() && cur.join("crates").exists() {
            return cur;
        }
        if let Some(parent) = cur.parent() {
            cur = parent.to_path_buf();
        } else {
            break;
        }
    }
    std::env::current_dir().unwrap_or_else(|_| PathBuf::from("."))
}

/// Executes the mode CLI workflow.
pub async fn run_mode_cli(args: ModeCliArgs) -> i32 {
    let workspace_root = find_workspace_root();

    // If no mode is supplied or interactive flag is specified, open interactive TUI menu
    if args.mode.is_none() || args.interactive {
        return run_interactive_menu(&workspace_root).await;
    }

    let mode_str = args.mode.as_deref().unwrap_or("");
    let parsed_mode = match WorkflowMode::from_str_loose(mode_str) {
        Some(m) => m,
        None => {
            eprintln!(
                "{}Error: Unknown workflow mode '{}'. Available: test, dev, doc, research.{}",
                CLR_RED, mode_str, CLR_RESET
            );
            eprintln!("Run `ox-mode --help` or `ox-mode -i` for the interactive menu.");
            return 1;
        }
    };

    let action_str = args
        .action
        .or(args.action_opt)
        .unwrap_or_else(|| match parsed_mode {
            WorkflowMode::Test => "quick".to_string(),
            WorkflowMode::Dev => "fast".to_string(),
            WorkflowMode::Doc => "verify".to_string(),
            WorkflowMode::Research => "profile".to_string(),
        });

    let result =
        execute_single_action(parsed_mode, &action_str, args.iterations, &workspace_root).await;

    if args.json {
        if let Ok(json) = serde_json::to_string_pretty(&result) {
            println!("{}", json);
        }
    } else {
        render_execution_result(&result);
    }

    if result.success {
        0
    } else {
        result.exit_code.unwrap_or(1)
    }
}

/// Dispatches an action with special handling for cluster launch/stop and live-reload.
pub async fn execute_single_action(
    mode: WorkflowMode,
    action: &str,
    iterations: usize,
    workspace_root: &Path,
) -> ModeExecutionResult {
    let action_clean = action.trim().to_lowercase();

    // Special custom actions in CLI
    if mode == WorkflowMode::Test && action_clean == "flaky" {
        return run_flaky_detection(workspace_root, iterations);
    }

    if mode == WorkflowMode::Dev && (action_clean == "watch" || action_clean == "reload") {
        return run_live_reload_watch(workspace_root).await;
    }

    if mode == WorkflowMode::Dev && action_clean == "cluster" {
        return launch_local_dev_cluster(workspace_root).await;
    }

    if mode == WorkflowMode::Dev && action_clean == "stop" {
        return stop_local_dev_cluster(workspace_root).await;
    }

    // Otherwise dispatch to core mode engine
    execute_mode_action(mode, &action_clean, workspace_root)
}

/// Spawns local dev cluster: 1 Master with Web UI + 2 Workers in background with PID tracking.
pub async fn launch_local_dev_cluster(workspace_root: &Path) -> ModeExecutionResult {
    let mut res = ModeExecutionResult::new(WorkflowMode::Dev, "cluster");
    let start = Instant::now();

    let pid_file = workspace_root.join(".dev_cluster.pids");
    let json_file = workspace_root.join(".dev_cluster.json");
    if pid_file.exists() || json_file.exists() {
        let _ = stop_local_dev_cluster(workspace_root).await;
    }

    let target_bin = workspace_root.join("target/debug/rusty-grid");
    if !target_bin.exists() {
        // Build first if missing
        let build = Command::new("cargo")
            .args(["build", "--bin", "rusty-grid"])
            .current_dir(workspace_root)
            .output();
        if let Err(e) = build {
            res.success = false;
            res.stderr = format!("Failed to build rusty-grid binary: {}", e);
            return res;
        }
    }

    let master_tcp_port = 9090u16;
    let web_ui_port = 8088u16; // Use port 8088 to avoid colliding with default 8080

    let mut spawned_pids = Vec::new();

    // 1. Spawn Master with both TCP protocol listener and HTTP Web UI Dashboard
    let master_res = Command::new(&target_bin)
        .args([
            "master",
            "--listen",
            &format!("127.0.0.1:{}", master_tcp_port),
            "--web-ui-addr",
            &format!("127.0.0.1:{}", web_ui_port),
        ])
        .current_dir(workspace_root)
        .stdout(Stdio::null())
        .stderr(Stdio::null())
        .spawn();

    let master_pid = match master_res {
        Ok(child) => {
            let pid = child.id();
            spawned_pids.push(format!("master:{}", pid));
            pid
        }
        Err(e) => {
            res.success = false;
            res.stderr = format!("Failed to spawn Master: {}", e);
            return res;
        }
    };

    // Wait a brief moment for Master to bind
    tokio::time::sleep(std::time::Duration::from_millis(1000)).await;

    // 2. Spawn Worker 1 (CPU compute node)
    let w1_res = Command::new(&target_bin)
        .args([
            "worker",
            "--master",
            &format!("127.0.0.1:{}", master_tcp_port),
            "--name",
            "dev-worker-cpu-1",
            "--cores",
            "4",
            "--ram",
            "8192",
        ])
        .current_dir(workspace_root)
        .stdout(Stdio::null())
        .stderr(Stdio::null())
        .spawn();

    if let Ok(child) = w1_res {
        spawned_pids.push(format!("worker_cpu:{}", child.id()));
    }

    // 3. Spawn Worker 2 (Simulated GPU compute node)
    let w2_res = Command::new(&target_bin)
        .args([
            "worker",
            "--master",
            &format!("127.0.0.1:{}", master_tcp_port),
            "--name",
            "dev-worker-gpu-2",
            "--cores",
            "8",
            "--ram",
            "16384",
            "--simulate-gpu",
            "--gpu-name",
            "NVIDIA RTX 4090 (Dev Sim)",
        ])
        .current_dir(workspace_root)
        .stdout(Stdio::null())
        .stderr(Stdio::null())
        .spawn();

    if let Ok(child) = w2_res {
        spawned_pids.push(format!("worker_gpu:{}", child.id()));
    }

    // Write PID tracking files (.dev_cluster.pids and structured .dev_cluster.json)
    let _ = fs::write(&pid_file, spawned_pids.join("\n"));
    let cluster_json = serde_json::json!({
        "master_pid": master_pid,
        "master_tcp": format!("127.0.0.1:{}", master_tcp_port),
        "web_ui_port": web_ui_port,
        "web_ui_url": format!("http://127.0.0.1:{}", web_ui_port),
        "pids": spawned_pids,
    });
    if let Ok(json_str) = serde_json::to_string_pretty(&cluster_json) {
        let _ = fs::write(&json_file, json_str);
    }

    res.duration_ms = start.elapsed().as_millis() as u64;
    res.success = true;
    res.stdout = format!(
        "=== Local Development Cluster Launched Successfully ===\nMaster Coordinator : tcp://127.0.0.1:{} (PID: {})\nWeb UI Dashboard   : http://127.0.0.1:{}\nWorker 1 (CPU Node) : dev-worker-cpu-1 (4 Cores, 8 GB RAM)\nWorker 2 (GPU Node) : dev-worker-gpu-2 (8 Cores, 16 GB RAM, RTX 4090)\nPID tracking files : {}\n                     {}\n\nRun `ox-mode dev status` to inspect cluster health.\nRun `ox-mode dev stop` to shut down the cluster cleanly.\n",
        master_tcp_port, master_pid,
        web_ui_port,
        pid_file.display(),
        json_file.display()
    );

    res = res.with_metric("master_pid", master_pid);
    res = res.with_metric("master_tcp", format!("127.0.0.1:{}", master_tcp_port));
    res = res.with_metric("web_ui_url", format!("http://127.0.0.1:{}", web_ui_port));
    res = res.with_metric("web_ui_port", web_ui_port);

    res
}

/// Stops running dev cluster child processes cleanly with cross-platform termination.
pub async fn stop_local_dev_cluster(workspace_root: &Path) -> ModeExecutionResult {
    let mut res = ModeExecutionResult::new(WorkflowMode::Dev, "stop");
    let start = Instant::now();

    let pid_file = workspace_root.join(".dev_cluster.pids");
    let json_file = workspace_root.join(".dev_cluster.json");

    if !pid_file.exists() && !json_file.exists() {
        res.success = true;
        res.stdout = "No running dev cluster tracking files found.".to_string();
        return res;
    }

    let mut pids_to_kill = Vec::new();

    if let Ok(content) = fs::read_to_string(&pid_file) {
        for line in content.lines() {
            if let Some((_role, pid_str)) = line.split_once(':') {
                if let Ok(pid) = pid_str.trim().parse::<u32>() {
                    pids_to_kill.push(pid);
                }
            }
        }
    }

    if let Ok(content) = fs::read_to_string(&json_file) {
        if let Ok(val) = serde_json::from_str::<serde_json::Value>(&content) {
            if let Some(master_pid) = val.get("master_pid").and_then(|p| p.as_u64()) {
                pids_to_kill.push(master_pid as u32);
            }
            if let Some(arr) = val.get("pids").and_then(|p| p.as_array()) {
                for item in arr {
                    if let Some(line) = item.as_str() {
                        if let Some((_, pid_str)) = line.split_once(':') {
                            if let Ok(pid) = pid_str.trim().parse::<u32>() {
                                pids_to_kill.push(pid);
                            }
                        }
                    }
                }
            }
        }
    }

    pids_to_kill.sort();
    pids_to_kill.dedup();

    let mut stopped = 0;
    for pid in pids_to_kill {
        if terminate_process(pid) {
            stopped += 1;
        }
    }

    let _ = fs::remove_file(&pid_file);
    let _ = fs::remove_file(&json_file);

    res.duration_ms = start.elapsed().as_millis() as u64;
    res.success = true;
    res.stdout = format!("Stopped {} dev cluster process(es) cleanly.", stopped);
    res = res.with_metric("stopped_count", stopped);

    res
}

fn terminate_process(pid: u32) -> bool {
    #[cfg(unix)]
    {
        let out = Command::new("kill")
            .args(["-TERM", &pid.to_string()])
            .output();
        out.map(|o| o.status.success()).unwrap_or(false)
    }
    #[cfg(windows)]
    {
        let out = Command::new("taskkill")
            .args(["/PID", &pid.to_string(), "/F"])
            .output();
        out.map(|o| o.status.success()).unwrap_or(false)
    }
}

/// Runs continuous live-reload watcher loop in the terminal.
pub async fn run_live_reload_watch(workspace_root: &Path) -> ModeExecutionResult {
    let mut res = ModeExecutionResult::new(WorkflowMode::Dev, "watch");
    let start = Instant::now();

    println!(
        "{}{}================================================================================",
        CLR_BOLD, CLR_CYAN
    );
    println!("       🔄 OXIDESWARM LIVE-RELOAD (WATCH MODE) CONTROLLER");
    println!(
        "================================================================================{}",
        CLR_RESET
    );
    println!("Monitoring workspace Rust files (`crates/`, `tests/`, `Cargo.toml`)...");
    println!("Auto-triggering fast compiler check on file modification.");
    println!(
        "Press {}Ctrl+C{} to exit live-reload mode.\n",
        CLR_BOLD, CLR_RESET
    );

    // Initial check
    println!(
        "{}--> Executing initial compiler check...{}",
        CLR_DIM, CLR_RESET
    );
    let init_res = execute_mode_action(WorkflowMode::Dev, "fast", workspace_root);
    render_execution_result(&init_res);

    let mut last_mtimes = collect_source_mtimes(workspace_root);
    let mut check_count = 0usize;

    // Check loop with cancellation handling
    loop {
        tokio::select! {
            _ = tokio::signal::ctrl_c() => {
                println!("\n{}Live-reload terminated by user (Ctrl+C).{}", CLR_YELLOW, CLR_RESET);
                break;
            }
            _ = tokio::time::sleep(tokio::time::Duration::from_millis(500)) => {}
        }

        let current_mtimes = collect_source_mtimes(workspace_root);
        let mut changed_file = None;

        for (path, mtime) in &current_mtimes {
            if let Some(prev) = last_mtimes.get(path) {
                if mtime > prev {
                    changed_file = Some(path.clone());
                    break;
                }
            } else {
                changed_file = Some(path.clone());
                break;
            }
        }

        if let Some(path) = changed_file {
            check_count += 1;
            last_mtimes = current_mtimes;
            let rel_path = path.strip_prefix(workspace_root).unwrap_or(&path);
            println!(
                "\n{}[LIVE-RELOAD #{}] Change detected in: {}{}",
                CLR_YELLOW,
                check_count,
                rel_path.display(),
                CLR_RESET
            );
            println!("Triggering incremental compilation check...");
            let check_res = execute_mode_action(WorkflowMode::Dev, "fast", workspace_root);
            render_execution_result(&check_res);
        }
    }

    res.duration_ms = start.elapsed().as_millis() as u64;
    res.success = true;
    res.stdout = format!(
        "Live-reload watch session completed after {} change trigger(s).",
        check_count
    );
    res = res.with_metric("reloads_count", check_count);
    res
}

fn collect_source_mtimes(workspace_root: &Path) -> HashMap<PathBuf, std::time::SystemTime> {
    let mut mtimes = HashMap::new();
    let scan_dirs = ["crates", "tests", "src"];
    for dir in &scan_dirs {
        scan_dir_mtimes(&workspace_root.join(dir), &mut mtimes);
    }
    let toml = workspace_root.join("Cargo.toml");
    if let Ok(meta) = fs::metadata(&toml) {
        if let Ok(mod_time) = meta.modified() {
            mtimes.insert(toml, mod_time);
        }
    }
    mtimes
}

fn scan_dir_mtimes(dir: &Path, mtimes: &mut HashMap<PathBuf, std::time::SystemTime>) {
    if let Ok(entries) = fs::read_dir(dir) {
        for entry in entries.flatten() {
            let path = entry.path();
            if path.is_dir() {
                if let Some(name) = path.file_name().and_then(|n| n.to_str()) {
                    if name == "target" || name.starts_with('.') {
                        continue;
                    }
                }
                scan_dir_mtimes(&path, mtimes);
            } else if path.is_file() {
                if let Some(ext) = path.extension().and_then(|e| e.to_str()) {
                    if ext == "rs" || ext == "toml" {
                        if let Ok(meta) = entry.metadata() {
                            if let Ok(mtime) = meta.modified() {
                                mtimes.insert(path, mtime);
                            }
                        }
                    }
                }
            }
        }
    }
}

/// Formats and renders execution result to stdout with ANSI decorations.
pub fn render_execution_result(res: &ModeExecutionResult) {
    let status_str = if res.success {
        format!("{}[OK - SUCCESS]{}", CLR_GREEN, CLR_RESET)
    } else {
        format!("{}[FAILED]{}", CLR_RED, CLR_RESET)
    };

    println!();
    println!(
        "{}────────────────────────────────────────────────────────────────────────────────{}",
        CLR_DIM, CLR_RESET
    );
    println!(
        "{}Mode:{} {:<12} {}Action:{} {:<12} {}Status:{} {} ({} ms)",
        CLR_BOLD,
        CLR_RESET,
        res.mode.as_str().to_uppercase(),
        CLR_BOLD,
        CLR_RESET,
        res.action,
        CLR_BOLD,
        CLR_RESET,
        status_str,
        res.duration_ms
    );
    println!(
        "{}────────────────────────────────────────────────────────────────────────────────{}",
        CLR_DIM, CLR_RESET
    );

    if !res.stdout.trim().is_empty() {
        println!("{}", res.stdout.trim());
    }

    if !res.stderr.trim().is_empty() {
        eprintln!("{}Error / Diagnostic Output:{}", CLR_YELLOW, CLR_RESET);
        eprintln!("{}", res.stderr.trim());
    }

    println!(
        "{}────────────────────────────────────────────────────────────────────────────────{}",
        CLR_DIM, CLR_RESET
    );
    println!();
}

/// Runs interactive terminal menu loop.
pub async fn run_interactive_menu(workspace_root: &Path) -> i32 {
    loop {
        println!();
        println!(
            "{}{}================================================================================",
            CLR_BOLD, CLR_CYAN
        );
        println!("       🚀 OXIDESWARM WORKFLOW MODES & PRESETS CONTROL CONSOLE (ox-mode)");
        println!(
            "================================================================================{}",
            CLR_RESET
        );
        println!("  Select a workflow mode:");
        println!(
            "    {}1.{} {}🧪 Chế độ Test / Kiểm tra{}  (Unit tests, Clippy lints, Flaky detector)",
            CLR_BOLD, CLR_RESET, CLR_CYAN, CLR_RESET
        );
        println!(
            "    {}2.{} {}⚡ Chế độ Phát triển{}      (Fast check, Local cluster dev, Health probe)",
            CLR_BOLD, CLR_RESET, CLR_YELLOW, CLR_RESET
        );
        println!(
            "    {}3.{} {}📚 Chế độ Viết tài liệu{}    (Cargo doc build, Spec verify, Export architecture)",
            CLR_BOLD, CLR_RESET, CLR_MAGENTA, CLR_RESET
        );
        println!(
            "    {}4.{} {}🔬 Chế độ Nghiên cứu{}       (Hardware profile, Micro-benchmarks, Distributed)",
            CLR_BOLD, CLR_RESET, CLR_GREEN, CLR_RESET
        );
        println!("    {}0.{} Thoát (Exit)", CLR_BOLD, CLR_RESET);
        println!(
            "{}--------------------------------------------------------------------------------{}",
            CLR_DIM, CLR_RESET
        );
        print!("Nhập lựa chọn của bạn [0-4]: ");
        io::stdout().flush().unwrap();

        let mut input = String::new();
        if io::stdin().read_line(&mut input).is_err() {
            return 0;
        }

        let choice = input.trim();
        match choice {
            "1" => {
                run_test_menu(workspace_root).await;
            }
            "2" => {
                run_dev_menu(workspace_root).await;
            }
            "3" => {
                run_doc_menu(workspace_root).await;
            }
            "4" => {
                run_research_menu(workspace_root).await;
            }
            "0" | "q" | "exit" => {
                println!("Tạm biệt!");
                return 0;
            }
            _ => {
                println!(
                    "{}Lựa chọn không hợp lệ. Vui lòng chọn từ 0 đến 4.{}",
                    CLR_YELLOW, CLR_RESET
                );
            }
        }
    }
}

async fn run_test_menu(workspace_root: &Path) {
    println!();
    println!(
        "{}=== 🧪 CHẾ ĐỘ TEST & KIỂM TRA CHẤT LƯỢNG ==={}",
        CLR_BOLD, CLR_RESET
    );
    println!("  [1] Quick Unit Tests (cargo test --workspace --lib)");
    println!("  [2] Lint Checks & Formatting (cargo clippy & fmt)");
    println!("  [3] Flaky Test Detector (5 iterations test loop)");
    println!("  [4] Full Integration & E2E Test Suite (cargo test --workspace)");
    println!("  [0] Quay lại Menu chính");
    print!("Lựa chọn [0-4]: ");
    io::stdout().flush().unwrap();

    let mut input = String::new();
    let _ = io::stdin().read_line(&mut input);
    match input.trim() {
        "1" => {
            println!("Đang chạy Quick Unit Tests...");
            let res = execute_single_action(WorkflowMode::Test, "quick", 5, workspace_root).await;
            render_execution_result(&res);
        }
        "2" => {
            println!("Đang chạy Lint & Format checks...");
            let res = execute_single_action(WorkflowMode::Test, "lint", 5, workspace_root).await;
            render_execution_result(&res);
        }
        "3" => {
            println!("Đang chạy Flaky Test Detector (5 lần lặp)...");
            let res = execute_single_action(WorkflowMode::Test, "flaky", 5, workspace_root).await;
            render_execution_result(&res);
        }
        "4" => {
            println!("Đang chạy Full Test Suite...");
            let res = execute_single_action(WorkflowMode::Test, "full", 5, workspace_root).await;
            render_execution_result(&res);
        }
        _ => {}
    }
}

async fn run_dev_menu(workspace_root: &Path) {
    println!();
    println!(
        "{}=== ⚡ CHẾ ĐỘ PHÁT TRIỂN & CỤM DEV LOCAL ==={}",
        CLR_BOLD, CLR_RESET
    );
    println!("  [1] Fast Incremental Check (cargo check --workspace)");
    println!("  [2] Live-Reload Watch Mode (Tự động biên dịch khi file thay đổi)");
    println!("  [3] Khởi chạy Local Dev Cluster (Master + 2 Workers)");
    println!("  [4] Kiểm tra sức khỏe cụm (Cluster Health Status)");
    println!("  [5] Dừng Local Dev Cluster (Stop cluster)");
    println!("  [0] Quay lại Menu chính");
    print!("Lựa chọn [0-5]: ");
    io::stdout().flush().unwrap();

    let mut input = String::new();
    let _ = io::stdin().read_line(&mut input);
    match input.trim() {
        "1" => {
            let res = execute_single_action(WorkflowMode::Dev, "fast", 5, workspace_root).await;
            render_execution_result(&res);
        }
        "2" => {
            let res = execute_single_action(WorkflowMode::Dev, "watch", 5, workspace_root).await;
            render_execution_result(&res);
        }
        "3" => {
            let res = execute_single_action(WorkflowMode::Dev, "cluster", 5, workspace_root).await;
            render_execution_result(&res);
        }
        "4" => {
            let res = execute_single_action(WorkflowMode::Dev, "status", 5, workspace_root).await;
            render_execution_result(&res);
        }
        "5" => {
            let res = execute_single_action(WorkflowMode::Dev, "stop", 5, workspace_root).await;
            render_execution_result(&res);
        }
        _ => {}
    }
}

async fn run_doc_menu(workspace_root: &Path) {
    println!();
    println!(
        "{}=== 📚 CHẾ ĐỘ VIẾT TÀI LIỆU & ĐẶC TẢ KIẾN TRÚC ==={}",
        CLR_BOLD, CLR_RESET
    );
    println!("  [1] Kiểm tra tính toàn vẹn tài liệu Markdown (Verify Docs & Specs)");
    println!("  [2] Xuất toàn văn Đặc tả Kiến trúc thống nhất (Export Architecture Spec)");
    println!("  [3] Biên dịch tài liệu mã nguồn (cargo doc --workspace --no-deps)");
    println!("  [0] Quay lại Menu chính");
    print!("Lựa chọn [0-3]: ");
    io::stdout().flush().unwrap();

    let mut input = String::new();
    let _ = io::stdin().read_line(&mut input);
    match input.trim() {
        "1" => {
            let res = execute_single_action(WorkflowMode::Doc, "verify", 5, workspace_root).await;
            render_execution_result(&res);
        }
        "2" => {
            let res = execute_single_action(WorkflowMode::Doc, "export", 5, workspace_root).await;
            render_execution_result(&res);
        }
        "3" => {
            let res = execute_single_action(WorkflowMode::Doc, "build", 5, workspace_root).await;
            render_execution_result(&res);
        }
        _ => {}
    }
}

async fn run_research_menu(workspace_root: &Path) {
    println!();
    println!(
        "{}=== 🔬 CHẾ ĐỘ NGHIÊN CỨU & ĐO KIỂM HIỆU NĂNG ==={}",
        CLR_BOLD, CLR_RESET
    );
    println!("  [1] Phân tích & đo lường tài nguyên phần cứng (CPU/RAM Profiler)");
    println!("  [2] Micro-Benchmarks (Serde, Queue & Codec Throughput)");
    println!("  [3] Distributed Scheduling Simulation Benchmark");
    println!("  [4] Tạo Báo cáo Nghiên cứu hoàn chỉnh (Export Research JSON Report)");
    println!("  [0] Quay lại Menu chính");
    print!("Lựa chọn [0-4]: ");
    io::stdout().flush().unwrap();

    let mut input = String::new();
    let _ = io::stdin().read_line(&mut input);
    match input.trim() {
        "1" => {
            let res =
                execute_single_action(WorkflowMode::Research, "profile", 5, workspace_root).await;
            render_execution_result(&res);
        }
        "2" => {
            let res =
                execute_single_action(WorkflowMode::Research, "bench", 5, workspace_root).await;
            render_execution_result(&res);
        }
        "3" => {
            let res =
                execute_single_action(WorkflowMode::Research, "distributed", 5, workspace_root)
                    .await;
            render_execution_result(&res);
        }
        "4" => {
            let res =
                execute_single_action(WorkflowMode::Research, "report", 5, workspace_root).await;
            render_execution_result(&res);
        }
        _ => {}
    }
}
