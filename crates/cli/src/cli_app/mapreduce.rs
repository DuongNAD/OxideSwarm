//! `oxideswarm mapreduce` command implementation and script helpers.

use std::path::Path;
use std::process::ExitCode;

use rusty_grid_core::mapreduce::{MapFunctionSpec, MapReduceJobSpec, ReduceFunctionSpec};
use rusty_grid_core::protocol::{ClientMessage, ClientResponse};

use crate::config::{
    resolve_string, resolve_u64, ConfigFile, SecretSection, CLIENT_MASTER_ENV_VARS,
};
use crate::connect::{connect_master, report_master_failure, DEFAULT_MASTER_ADDR};
use crate::secret_cmd::{client_secret, peer_insecure, SecretOpts};

use super::{exchange, resolve_relay_url, MapReduceArgs};

pub(super) const MAX_MAPREDUCE_SCRIPT_BYTES: usize = 64 * 1024;

fn is_builtin_mapper(s: &str) -> bool {
    matches!(s, "word_count" | "line_count" | "identity")
}

fn is_builtin_reducer(s: &str) -> bool {
    matches!(s, "sum" | "count" | "max" | "min" | "concat")
}

fn looks_like_script_path(s: &str) -> bool {
    s.contains('/') || s.contains('\\') || s.ends_with(".sh")
}

pub(super) fn read_mapreduce_script(
    kind: &str,
    cmd_flag: &str,
    path_str: &str,
) -> Result<String, String> {
    let path = Path::new(path_str);
    let meta = std::fs::metadata(path)
        .map_err(|e| format!("Failed to read {kind} script '{path_str}': {e}"))?;
    if meta.len() > MAX_MAPREDUCE_SCRIPT_BYTES as u64 {
        return Err(format!(
            "Failed to read {kind} script '{path_str}': script size ({} bytes) exceeds maximum inline MapReduce script size ({} bytes); use {cmd_flag} or stage the script onto workers",
            meta.len(),
            MAX_MAPREDUCE_SCRIPT_BYTES
        ));
    }
    let script = std::fs::read_to_string(path)
        .map_err(|e| format!("Failed to read {kind} script '{path_str}': {e}"))?;
    if script.len() > MAX_MAPREDUCE_SCRIPT_BYTES {
        return Err(format!(
            "Failed to read {kind} script '{path_str}': script size ({} bytes) exceeds maximum inline MapReduce script size ({} bytes); use {cmd_flag} or stage the script onto workers",
            script.len(),
            MAX_MAPREDUCE_SCRIPT_BYTES
        ));
    }
    Ok(script)
}

pub(super) fn split_command_line(cmd: &str) -> Result<(String, Vec<String>), String> {
    let mut tokens = Vec::new();
    let mut current = String::new();
    let mut in_single = false;
    let mut in_double = false;
    let mut token_started = false;
    let mut chars = cmd.chars().peekable();

    while let Some(ch) = chars.next() {
        match ch {
            '\\' if !in_single => {
                let should_escape = if in_double {
                    matches!(chars.peek(), Some('"' | '\\'))
                } else {
                    chars.peek().is_some_and(|next| {
                        *next == '"' || *next == '\'' || *next == '\\' || next.is_whitespace()
                    })
                };
                if should_escape {
                    if let Some(escaped_ch) = chars.next() {
                        current.push(escaped_ch);
                    }
                } else {
                    current.push('\\');
                }
                token_started = true;
            }
            '\'' if !in_double => {
                in_single = !in_single;
                token_started = true;
            }
            '"' if !in_single => {
                in_double = !in_double;
                token_started = true;
            }
            c if c.is_whitespace() && !in_single && !in_double => {
                if token_started {
                    tokens.push(std::mem::take(&mut current));
                    token_started = false;
                }
            }
            c => {
                current.push(c);
                token_started = true;
            }
        }
    }
    if in_single || in_double {
        return Err("unclosed quote in command string".to_string());
    }
    if token_started {
        tokens.push(current);
    }
    let mut iter = tokens.into_iter();
    let program = iter
        .next()
        .filter(|p| !p.is_empty())
        .ok_or_else(|| "command cannot be empty".to_string())?;
    Ok((program, iter.collect()))
}

pub(super) async fn run_mapreduce(
    args: MapReduceArgs,
    config_file: Option<ConfigFile>,
    secret_opts: SecretOpts,
) -> ExitCode {
    let relay_url = match resolve_relay_url(
        args.relay_url.clone(),
        config_file.as_ref().and_then(|c| c.submit_relay_url()),
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
        run_mapreduce_inner(args, config_file, secret_opts),
    )
    .await
}

async fn run_mapreduce_inner(
    args: MapReduceArgs,
    config_file: Option<ConfigFile>,
    secret_opts: SecretOpts,
) -> ExitCode {
    if args.chunks == 0 {
        eprintln!("--chunks must be at least 1");
        return ExitCode::FAILURE;
    }
    let submit_cfg = config_file.as_ref().and_then(|c| c.submit.clone());
    let master_addr = resolve_string(
        args.master,
        CLIENT_MASTER_ENV_VARS,
        submit_cfg.as_ref().and_then(|s| s.target_master()),
        DEFAULT_MASTER_ADDR,
    );
    let secret = match client_secret(
        &secret_opts,
        config_file.as_ref(),
        SecretSection::Submit,
        &master_addr,
        peer_insecure(args.insecure, submit_cfg.as_ref().and_then(|s| s.insecure)),
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

    // Read input data from files or treat arguments directly as data
    let mut chunks: Vec<String> = Vec::new();
    for inp in &args.input {
        let p = Path::new(inp);
        if p.exists() {
            match std::fs::read_to_string(p) {
                Ok(content) => {
                    // Split file content into N roughly equal line chunks
                    let lines: Vec<&str> = content.lines().collect();
                    if lines.is_empty() {
                        chunks.push(String::new());
                    } else {
                        let chunk_size = lines.len().div_ceil(args.chunks);
                        for chunk_slice in lines.chunks(chunk_size.max(1)) {
                            chunks.push(chunk_slice.join("\n"));
                        }
                    }
                }
                Err(e) => {
                    eprintln!("Failed to read input file '{}': {e}", p.display());
                    return ExitCode::FAILURE;
                }
            }
        } else {
            chunks.push(inp.clone());
        }
    }

    let mapper_spec = if let Some(ref cmd) = args.map_cmd {
        match split_command_line(cmd) {
            Ok((program, cmd_args)) => MapFunctionSpec::Command {
                program,
                args: cmd_args,
            },
            Err(e) => {
                eprintln!("Invalid --map-cmd '{cmd}': {e}");
                return ExitCode::FAILURE;
            }
        }
    } else if is_builtin_mapper(&args.mapper) {
        MapFunctionSpec::Builtin {
            operator: args.mapper,
        }
    } else if looks_like_script_path(&args.mapper) {
        match read_mapreduce_script("mapper", "--map-cmd", &args.mapper) {
            Ok(script) => MapFunctionSpec::ShellScript { script },
            Err(msg) => {
                eprintln!("{msg}");
                return ExitCode::FAILURE;
            }
        }
    } else {
        MapFunctionSpec::Builtin {
            operator: args.mapper,
        }
    };

    let reducer_spec = if let Some(ref cmd) = args.reduce_cmd {
        match split_command_line(cmd) {
            Ok((program, cmd_args)) => ReduceFunctionSpec::Command {
                program,
                args: cmd_args,
            },
            Err(e) => {
                eprintln!("Invalid --reduce-cmd '{cmd}': {e}");
                return ExitCode::FAILURE;
            }
        }
    } else if is_builtin_reducer(&args.reducer) {
        ReduceFunctionSpec::Builtin {
            operator: args.reducer,
        }
    } else if looks_like_script_path(&args.reducer) {
        match read_mapreduce_script("reducer", "--reduce-cmd", &args.reducer) {
            Ok(script) => ReduceFunctionSpec::ShellScript { script },
            Err(msg) => {
                eprintln!("{msg}");
                return ExitCode::FAILURE;
            }
        }
    } else {
        ReduceFunctionSpec::Builtin {
            operator: args.reducer,
        }
    };

    let timeout_secs = resolve_u64(
        args.timeout_secs,
        &[],
        submit_cfg.as_ref().and_then(|s| s.timeout_secs),
        60,
    )
    .max(1);
    let job = MapReduceJobSpec::new(
        "cli_mapreduce_job",
        chunks,
        mapper_spec,
        reducer_spec,
        args.chunks,
        1,
        timeout_secs,
    );

    let msg = ClientMessage::SubmitMapReduce { job };
    let response = match exchange(
        &mut transport,
        &master_addr,
        secret.is_some(),
        &msg,
        "Failed to dispatch MapReduce job to Master",
    )
    .await
    {
        Ok(response) => response,
        Err(code) => return code,
    };

    match response {
        ClientResponse::MapReduceCompleted { result } => {
            // The master answers a job that failed (a mapper exited non-zero, output was cut off,
            // a reducer failed) with a result whose status is not "Completed": report it as a failure.
            let succeeded = result.status == "Completed";
            if args.json {
                println!("{}", serde_json::to_string_pretty(&result).unwrap());
            } else if !succeeded {
                eprintln!("=== MapReduce Job FAILED ===");
                eprintln!("Job ID:           {}", result.job_id);
                eprintln!("Status:           {}", result.status);
                eprintln!(
                    "Map Tasks:        {}/{}",
                    result.map_tasks_completed, result.map_tasks_total
                );
                eprintln!(
                    "Reduce Tasks:     {}/{}",
                    result.reduce_tasks_completed, result.reduce_tasks_total
                );
                if let Some(error) = &result.error {
                    eprintln!("Error:            {error}");
                }
            } else {
                println!("=== MapReduce Job Completed ===");
                println!("Job ID:           {}", result.job_id);
                println!("Status:           {}", result.status);
                println!(
                    "Map Tasks:        {}/{}",
                    result.map_tasks_completed, result.map_tasks_total
                );
                println!(
                    "Reduce Tasks:     {}/{}",
                    result.reduce_tasks_completed, result.reduce_tasks_total
                );
                println!("Execution Time:   {}ms", result.execution_time_ms);
                println!(
                    "\nAggregated Results ({} unique keys):",
                    result.output.len()
                );
                for (key, val) in &result.output {
                    println!("  {key}: {val}");
                }
            }
            if succeeded {
                ExitCode::SUCCESS
            } else {
                ExitCode::FAILURE
            }
        }
        ClientResponse::Error { message } => {
            eprintln!("MapReduce execution error: {message}");
            ExitCode::FAILURE
        }
        other => {
            eprintln!("Unexpected Master response: {other:?}");
            ExitCode::FAILURE
        }
    }
}
