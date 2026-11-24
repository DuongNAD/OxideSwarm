//! `oxideswarm submit` specification builder, validation, and dispatch.

use std::collections::HashMap;
use std::path::{Path, PathBuf};
use std::process::ExitCode;
use std::time::Duration;

use uuid::Uuid;

use rusty_grid_core::error::GridError;
use rusty_grid_core::placement::{
    listed_supports_workspaces, listed_worker_id, tag_value, IsolationLevel, Placement,
    REQUIRE_PREFIX, STATE_TAG_PREFIX,
};
use rusty_grid_core::protocol::{ClientMessage, ClientResponse, MessageTransport};
use rusty_grid_core::task::{Task, TaskRequirements, TaskSpec};
use rusty_grid_core::transport::GridStream;
use rusty_grid_core::WorkerCapabilities;

use crate::config::{
    resolve_string, resolve_u64, ConfigFile, SecretSection, CLIENT_MASTER_ENV_VARS,
};
use crate::connect::{connect_master, report_master_failure, DEFAULT_MASTER_ADDR};
use crate::secret_cmd::{client_secret, peer_insecure, SecretOpts};

use super::{exchange, resolve_relay_url, SubmitArgs, MIB};

/// Largest file `submit --stdin-file` / `--script-file` sends: well below the 64 MiB frame limit
/// (`MAX_FRAME_SIZE`), which also has to carry the rest of the task.
pub(super) const MAX_TASK_INPUT_BYTES: u64 = 32 * 1024 * 1024;

/// Validates that a binary buffer has a well-formed glTF 2.0 binary (`.glb`) header and JSON chunk 0.
pub fn validate_glb_header(raw: &[u8]) -> Result<(), String> {
    if raw.len() < 20 {
        return Err(format!(
            "Asset buffer too small ({} bytes < 20 bytes minimum for glTF 2.0 binary)",
            raw.len()
        ));
    }
    if &raw[0..4] != b"glTF" {
        return Err(format!(
            "Invalid glTF magic number: expected b\"glTF\", got {:?}",
            &raw[0..4]
        ));
    }
    let version = u32::from_le_bytes(raw[4..8].try_into().unwrap());
    if version != 2 {
        return Err(format!(
            "Unsupported glTF binary version: {version} (expected 2)"
        ));
    }
    let total_length = u32::from_le_bytes(raw[8..12].try_into().unwrap()) as usize;
    if total_length > raw.len() || total_length < 20 {
        return Err(format!(
            "Corrupted glTF binary: header length ({total_length}) exceeds buffer ({})",
            raw.len()
        ));
    }
    let chunk0_length = u32::from_le_bytes(raw[12..16].try_into().unwrap()) as usize;
    if 20usize.saturating_add(chunk0_length) > raw.len() {
        return Err(format!(
            "Corrupted glTF chunk 0: length ({chunk0_length}) exceeds buffer ({})",
            raw.len()
        ));
    }
    let chunk0_type = u32::from_le_bytes(raw[16..20].try_into().unwrap());
    if chunk0_type != 0x4E4F534A {
        return Err(format!(
            "Invalid glTF chunk 0 type: 0x{chunk0_type:08X} (expected 0x4E4F534A 'JSON')"
        ));
    }
    let json_slice = &raw[20..20 + chunk0_length];
    let json_str = std::str::from_utf8(json_slice)
        .map_err(|e| format!("glTF chunk 0 is not valid UTF-8: {e}"))?;
    let json_val: serde_json::Value = serde_json::from_str(json_str)
        .map_err(|e| format!("Failed to parse glTF chunk 0 JSON: {e}"))?;
    let asset_ver = json_val
        .get("asset")
        .and_then(|a| a.get("version"))
        .and_then(|v| v.as_str());
    if asset_ver != Some("2.0") {
        return Err(format!(
            "Invalid glTF asset version: {:?} (expected Some(\"2.0\"))",
            asset_ver
        ));
    }
    Ok(())
}

/// Validates and normalizes `--cwd` / `workspace.cwd` relative to the workspace root.
/// `"."` (or `"./"`, `".\\"`) resolves to `None` (the workspace root itself).
pub(crate) fn normalize_workspace_cwd(raw: &str) -> Result<Option<PathBuf>, String> {
    let trimmed = raw.trim();
    if trimmed.is_empty() {
        return Err("Working directory inside workspace cannot be empty".to_string());
    }
    let slash_norm = trimmed.replace('\\', "/");
    if slash_norm == "." || slash_norm.trim_matches('/') == "." {
        return Ok(None);
    }
    let validated = rusty_grid_core::task::validate_relative_sandbox_path(trimmed)?;
    Ok(Some(PathBuf::from(validated)))
}

/// Builds the task spec `submit` sends from its flags, reading `--stdin-file` / `--script-file`;
/// an error names the flag at fault. Nothing is sent before this succeeds.
///
/// * `--command PROG [-- ARGS...]`: run `PROG` with exactly `ARGS` (possibly none).
/// * `-- PROG [ARGS...]` without `--command`: the first trailing token is the program.
/// * neither: the default demo `echo "Hello from RustyGrid"`.
///
/// An explicit program with no arguments runs with no arguments; the demo greeting is only a
/// default for a completely empty command line.
pub fn build_task_spec(args: &SubmitArgs) -> Result<TaskSpec, String> {
    let mut kind = args.task_type.to_lowercase().replace('-', "_");
    if (kind == "generic" || kind == "command" || kind.is_empty())
        && (args.prompt.is_some() || args.image.is_some())
    {
        kind = "3d".to_string();
    } else if (kind == "generic" || kind == "command" || kind.is_empty())
        && args.input_mesh.is_some()
    {
        kind = "autorig".to_string();
    }
    let is_command = !matches!(
        kind.as_str(),
        "gpu"
            | "compile"
            | "shell"
            | "3d"
            | "gen3d"
            | "3d_generate"
            | "generative_3d"
            | "autorig"
            | "auto_rig"
            | "rig"
            | "builtin_test"
            | "builtintest"
    );
    let only_for = |flag: &str, kinds: &str| {
        Err(format!(
            "{flag} applies to {kinds} tasks, not to `--type {}`",
            args.task_type
        ))
    };
    if args.stdin_file.is_some() && !is_command {
        return only_for("--stdin-file", "command");
    }
    if kind != "shell" {
        if args.script_file.is_some() {
            return only_for("--script-file", "shell");
        }
        if args.interpreter.is_some() {
            return only_for("--interpreter", "shell");
        }
    }
    if !args.env.is_empty() && !is_command && kind != "shell" {
        return only_for("--env", "command and shell");
    }
    if kind != "gpu" {
        if args.matrix_dim.is_some() {
            return only_for("--matrix-dim", "gpu");
        }
        if args.work_group_size.is_some() {
            return only_for("--work-group-size", "gpu");
        }
    }
    let is_compile = kind == "compile";
    if !args.sources.is_empty() && !is_compile {
        return only_for("--source", "compile");
    }
    if !args.inputs.is_empty() && !is_command && kind != "shell" && !is_compile {
        return only_for("--input (or --file)", "command and shell");
    }
    if !args.outputs.is_empty() && !is_command && kind != "shell" && !is_compile {
        if matches!(
            kind.as_str(),
            "3d" | "gen3d" | "3d_generate" | "generative_3d" | "autorig" | "auto_rig" | "rig"
        ) {
            let target = if args.task_type == "generic" || args.task_type.is_empty() {
                "3d or rigging".to_string()
            } else {
                args.task_type.clone()
            };
            return Err(format!(
                "--output applies to command and shell tasks, not to {target} tasks (use --output-asset or -o for 3D/rigging assets)"
            ));
        }
        return only_for("--output", "command and shell");
    }
    if args.output_dir.is_some() && !is_command && kind != "shell" && !is_compile {
        return only_for("--output-dir", "command and shell");
    }
    if args.output_dir.is_some() && args.outputs.is_empty() && !is_compile {
        return Err("--output-dir requires at least one --output pattern".to_string());
    }
    if args.sync.is_some() && args.workspace.is_none() {
        return Err("--sync requires --workspace <NAME>".to_string());
    }
    if args.no_sync && args.workspace.is_none() {
        return Err("--no-sync requires --workspace <NAME>".to_string());
    }
    if args.sync.is_some() && args.no_sync {
        return Err("--sync cannot be combined with --no-sync".to_string());
    }
    if args.cwd.is_some() && args.workspace.is_none() {
        return Err("--cwd requires --workspace <NAME>".to_string());
    }
    if args.cwd.is_some() && !is_command {
        return only_for("--cwd", "command");
    }
    if args.workspace_max_files == Some(0) {
        return Err("--workspace-max-files must be at least 1 (got 0)".to_string());
    }
    if args.workspace_max_mb == Some(0) {
        return Err("--workspace-max-mb must be at least 1 MiB (got 0)".to_string());
    }
    if let Some(ref ws_name) = args.workspace {
        if !is_command && kind != "shell" {
            return only_for("--workspace", "command and shell");
        }
        rusty_grid_core::task::validate_workspace_name(ws_name)?;
        if !args.inputs.is_empty() {
            return Err(
                "--workspace cannot be combined with --input (or --file); sync the files into the workspace instead"
                    .to_string(),
            );
        }
    }
    let working_dir = match &args.cwd {
        Some(raw_cwd) => normalize_workspace_cwd(raw_cwd).map_err(|e| format!("--cwd: {e}"))?,
        None => None,
    };
    let env = parse_env_pairs(&args.env)?;

    match kind.as_str() {
        "3d" | "gen3d" | "3d_generate" | "generative_3d" => {
            let prompt = match &args.prompt {
                Some(p) => {
                    let trimmed = p.trim();
                    if trimmed.is_empty() {
                        return Err("--prompt cannot be empty or whitespace".to_string());
                    }
                    Some(trimmed.to_string())
                }
                None => None,
            };
            let image_data = match &args.image {
                Some(img_path) => {
                    let meta = std::fs::metadata(img_path)
                        .map_err(|e| format!("--image {}: {e}", img_path.display()))?;
                    if !meta.is_file() {
                        return Err(format!(
                            "--image {}: path is not a regular file",
                            img_path.display()
                        ));
                    }
                    if meta.len() == 0 {
                        return Err(format!(
                            "--image {}: file is empty (0 bytes)",
                            img_path.display()
                        ));
                    }
                    let bytes = read_task_input(img_path, "--image")?;
                    Some(rusty_grid_core::task::Bytes::from(bytes))
                }
                None => None,
            };
            if prompt.is_none() && image_data.is_none() {
                return Err("3D generation requires either --prompt or --image".to_string());
            }
            let quality = args
                .quality
                .clone()
                .unwrap_or_else(|| "standard".to_string())
                .to_lowercase();
            let engine_preference = args
                .engine
                .as_deref()
                .map(str::parse::<rusty_grid_core::task::Engine3D>)
                .transpose()?
                .map(|e| e.as_str().to_string());
            let _ = args.export_texture;
            let export_texture = !args.no_export_texture;
            let spec = TaskSpec::Generative3D(rusty_grid_core::task::Generative3DSpec {
                prompt,
                image_data,
                quality,
                engine_preference,
                export_texture,
            });
            spec.validate()?;
            Ok(spec)
        }
        "autorig" | "auto_rig" | "rig" => {
            let mesh_path = args
                .input_mesh
                .as_ref()
                .ok_or_else(|| "Auto-rigging requires --input-mesh <PATH>".to_string())?;
            let meta = std::fs::metadata(mesh_path)
                .map_err(|e| format!("--input-mesh {}: {e}", mesh_path.display()))?;
            if !meta.is_file() {
                return Err(format!(
                    "--input-mesh {}: path is not a regular file",
                    mesh_path.display()
                ));
            }
            if meta.len() == 0 {
                return Err(format!(
                    "--input-mesh {}: file is empty (0 bytes)",
                    mesh_path.display()
                ));
            }
            let bytes = read_task_input(mesh_path, "--input-mesh")?;
            let rig_preset = args
                .rig_preset
                .clone()
                .unwrap_or_else(|| "biped".to_string());
            let spec = TaskSpec::AutoRig(rusty_grid_core::task::AutoRigSpec {
                input_mesh: rusty_grid_core::task::Bytes::from(bytes),
                rig_preset,
                embed_animations: args.embed_animations.clone(),
                preferred_provider: args.preferred_provider.clone(),
            });
            spec.validate()?;
            Ok(spec)
        }
        "gpu" => {
            let dim = args.matrix_dim.unwrap_or(64);
            let tile = args.work_group_size.unwrap_or(16);
            if dim == 0 || tile == 0 {
                return Err("--matrix-dim and --work-group-size must be at least 1".to_string());
            }
            Ok(TaskSpec::gpu_compute_with_params(
                args.command
                    .clone()
                    .unwrap_or_else(|| "matrix_multiply".into()),
                vec![1, 2, 3, 4],
                tile,
                dim,
            ))
        }
        "compile" => {
            let (crate_name, source_files) = collect_compile_sources(args)?;
            let spec = TaskSpec::RustCompilation {
                crate_name,
                source_files,
                compiler_flags: args.args.clone(),
                target_dir: None,
            };
            spec.validate()?;
            Ok(spec)
        }
        "shell" => {
            let script = match &args.script_file {
                Some(path) => {
                    if args.command.is_some() || !args.args.is_empty() {
                        return Err(
                            "--script-file cannot be combined with --command or words after --"
                                .to_string(),
                        );
                    }
                    String::from_utf8(read_task_input(path, "--script-file")?).map_err(|_| {
                        format!(
                            "--script-file {}: the file is not UTF-8 text",
                            path.display()
                        )
                    })?
                }
                None => match (&args.command, args.args.is_empty()) {
                    // `--command 'ls' -- -la`: the trailing words extend the script instead of
                    // being dropped.
                    (Some(script), _) => std::iter::once(script.as_str())
                        .chain(args.args.iter().map(String::as_str))
                        .collect::<Vec<_>>()
                        .join(" "),
                    (None, false) => args.args.join(" "),
                    (None, true) => "echo 'Hello from shell task'".into(),
                },
            };
            Ok(TaskSpec::ShellScript {
                script,
                interpreter: args.interpreter.clone(),
                env,
            })
        }
        _ => {
            let (program, cmd_args) = match (&args.command, args.args.split_first()) {
                (Some(program), _) => (program.clone(), args.args.clone()),
                (None, Some((program, rest))) => (program.clone(), rest.to_vec()),
                (None, None) => ("echo".to_string(), vec!["Hello from RustyGrid".to_string()]),
            };
            let stdin = match &args.stdin_file {
                Some(path) => Some(read_task_input(path, "--stdin-file")?),
                None => None,
            };
            Ok(TaskSpec::Command {
                program,
                args: cmd_args,
                env,
                working_dir,
                stdin,
            })
        }
    }
}

/// Reads a file `submit` sends with the task, refusing one above [`MAX_TASK_INPUT_BYTES`].
pub(super) fn read_task_input(path: &Path, flag: &str) -> Result<Vec<u8>, String> {
    let len = std::fs::metadata(path)
        .map_err(|e| format!("{flag} {}: {e}", path.display()))?
        .len();
    if len > MAX_TASK_INPUT_BYTES {
        return Err(format!(
            "{flag} {}: {len} bytes, more than the {} MiB a task may carry",
            path.display(),
            MAX_TASK_INPUT_BYTES / (1024 * 1024)
        ));
    }
    std::fs::read(path).map_err(|e| format!("{flag} {}: {e}", path.display()))
}

/// Collects Rust source files and crate metadata from `--source` (or `--input` when `--source`
/// is omitted) for `submit --type compile`.
fn collect_compile_sources(args: &SubmitArgs) -> Result<(String, HashMap<String, String>), String> {
    let (raw_specs, flag_name) = if !args.sources.is_empty() {
        (&args.sources, "--source")
    } else if !args.inputs.is_empty() {
        (&args.inputs, "--input")
    } else {
        return Err(
            "compile tasks require --source <PATH> (a Rust source file or Cargo crate directory)"
                .to_string(),
        );
    };

    let mut source_files = HashMap::new();
    let mut first_local_path: Option<PathBuf> = None;

    for raw in raw_specs {
        let (local_path, explicit_dest) =
            crate::blobs::parse_input_spec(raw).map_err(|e| e.replace("--input", flag_name))?;
        if first_local_path.is_none() {
            first_local_path = Some(local_path.clone());
        }
        let meta = std::fs::metadata(&local_path)
            .map_err(|e| format!("{flag_name} {}: {e}", local_path.display()))?;
        if meta.is_dir() {
            let prefix = match explicit_dest {
                Some(dest) => {
                    let parts: Vec<&str> = dest
                        .split('/')
                        .filter(|p| !p.is_empty() && *p != ".")
                        .collect();
                    if parts.is_empty() {
                        String::new()
                    } else {
                        let joined = parts.join("/");
                        rusty_grid_core::blob::validate_blob_path(&joined)
                            .map_err(|e| format!("{flag_name}: sandbox path '{dest}': {e}"))?;
                        joined
                    }
                }
                None => String::new(),
            };
            collect_compile_dir(
                &local_path,
                &local_path,
                &prefix,
                flag_name,
                &mut source_files,
            )?;
        } else if meta.is_file() {
            let sandbox_rel = match explicit_dest {
                Some(dest) => {
                    let parts: Vec<&str> = dest
                        .split('/')
                        .filter(|p| !p.is_empty() && *p != ".")
                        .collect();
                    if parts.is_empty() {
                        return Err(format!(
                            "{flag_name}: '{dest}' is the sandbox root; a file needs a file name"
                        ));
                    }
                    let joined = parts.join("/");
                    rusty_grid_core::blob::validate_blob_path(&joined)
                        .map_err(|e| format!("{flag_name}: sandbox path '{dest}': {e}"))?;
                    joined
                }
                None => default_compile_file_dest(&local_path, flag_name)?,
            };
            let bytes = read_task_input(&local_path, flag_name)?;
            let text = String::from_utf8(bytes).map_err(|_| {
                format!(
                    "{flag_name} {}: the file is not UTF-8 text",
                    local_path.display()
                )
            })?;
            source_files.insert(sandbox_rel, text);
        } else {
            return Err(format!(
                "{flag_name} {}: path is neither a regular file nor a directory",
                local_path.display()
            ));
        }
    }

    if source_files.is_empty() {
        return Err(format!("{flag_name}: no source files found"));
    }

    let crate_name = if let Some(cmd) = &args.command {
        let trimmed = cmd.trim();
        if trimmed.is_empty() {
            "crate".to_string()
        } else {
            trimmed.to_string()
        }
    } else if let Some(manifest) = source_files.get("Cargo.toml") {
        parse_cargo_package_name(manifest).unwrap_or_else(|| "crate".to_string())
    } else if let Some(path) = first_local_path {
        path.file_stem()
            .or_else(|| path.file_name())
            .and_then(|s| s.to_str())
            .map(|s| s.replace('-', "_"))
            .filter(|s| !s.trim().is_empty())
            .unwrap_or_else(|| "crate".to_string())
    } else {
        "crate".to_string()
    };

    Ok((crate_name, source_files))
}

fn collect_compile_dir(
    root: &Path,
    current: &Path,
    prefix: &str,
    flag_name: &str,
    out: &mut HashMap<String, String>,
) -> Result<(), String> {
    let mut entries: Vec<std::fs::DirEntry> = std::fs::read_dir(current)
        .map_err(|e| format!("{flag_name} {}: {e}", current.display()))?
        .collect::<Result<Vec<_>, _>>()
        .map_err(|e| format!("{flag_name} {}: {e}", current.display()))?;
    entries.sort_by_key(|e| e.file_name());

    for entry in entries {
        let file_type = entry
            .file_type()
            .map_err(|e| format!("{flag_name} {}: {e}", entry.path().display()))?;
        if file_type.is_symlink() {
            continue;
        }
        let name = entry.file_name();
        let Some(name_str) = name.to_str() else {
            continue;
        };
        if file_type.is_dir() {
            if name_str == ".git" || name_str == "target" {
                continue;
            }
            collect_compile_dir(root, &entry.path(), prefix, flag_name, out)?;
        } else if file_type.is_file() {
            let rel = entry
                .path()
                .strip_prefix(root)
                .map_err(|e| format!("{flag_name} {}: {e}", entry.path().display()))?
                .components()
                .filter_map(|c| match c {
                    std::path::Component::Normal(s) => s.to_str(),
                    _ => None,
                })
                .collect::<Vec<_>>()
                .join("/");
            if rel.is_empty() {
                continue;
            }
            let sandbox_rel = if prefix.is_empty() {
                rel
            } else {
                format!("{prefix}/{rel}")
            };
            if rusty_grid_core::blob::validate_blob_path(&sandbox_rel).is_err() {
                continue;
            }
            let bytes = read_task_input(&entry.path(), flag_name)?;
            let text = String::from_utf8(bytes).map_err(|_| {
                format!(
                    "{flag_name} {}: the file is not UTF-8 text",
                    entry.path().display()
                )
            })?;
            out.insert(sandbox_rel, text);
        }
    }
    Ok(())
}

fn default_compile_file_dest(local: &Path, flag_name: &str) -> Result<String, String> {
    let mut parts = Vec::new();
    let mut relative = true;
    for component in local.components() {
        match component {
            std::path::Component::Normal(part) => match part.to_str() {
                Some(s) => parts.push(s.to_string()),
                None => relative = false,
            },
            std::path::Component::CurDir => {}
            _ => relative = false,
        }
    }
    if relative && !parts.is_empty() {
        let joined = parts.join("/");
        if rusty_grid_core::blob::validate_blob_path(&joined).is_ok() {
            return Ok(joined);
        }
    }
    if let (Some(file_name), Some(parent)) =
        (local.file_name().and_then(|n| n.to_str()), local.parent())
    {
        if parent.file_name().and_then(|n| n.to_str()) == Some("src") {
            let candidate = format!("src/{file_name}");
            if rusty_grid_core::blob::validate_blob_path(&candidate).is_ok() {
                return Ok(candidate);
            }
        }
        if rusty_grid_core::blob::validate_blob_path(file_name).is_ok() {
            return Ok(file_name.to_string());
        }
    }
    Err(format!(
        "{flag_name} '{}': cannot derive a sandbox path; name one with PATH:SANDBOX_PATH",
        local.display()
    ))
}

fn parse_cargo_package_name(manifest: &str) -> Option<String> {
    let parsed: toml::Value = toml::from_str(manifest).ok()?;
    parsed
        .get("package")?
        .get("name")?
        .as_str()
        .map(|s| s.trim().to_string())
        .filter(|s| !s.is_empty())
}

/// `--env KEY=VALUE` pairs as a map; the first `=` ends the key, which must not be empty.
fn parse_env_pairs(pairs: &[String]) -> Result<HashMap<String, String>, String> {
    let mut env = HashMap::new();
    for pair in pairs {
        match pair.split_once('=') {
            Some((key, value)) if !key.is_empty() => {
                env.insert(key.to_string(), value.to_string());
            }
            _ => return Err(format!("--env {pair}: expected KEY=VALUE")),
        }
    }
    Ok(env)
}

/// The placement constraints of `submit --os / --arch / --worker / --require-isolation / --require-tag / --require-tool`, checked and
/// normalised exactly as the master will read them.
pub(super) fn build_placement(args: &SubmitArgs) -> Result<Placement, String> {
    let mut tags = Vec::new();
    if let Some(os) = &args.os {
        tags.push(format!("{REQUIRE_PREFIX}os={os}"));
    }
    if let Some(arch) = &args.arch {
        tags.push(format!("{REQUIRE_PREFIX}arch={arch}"));
    }
    if let Some(worker) = &args.worker {
        tags.push(format!("{REQUIRE_PREFIX}worker={worker}"));
    }
    if let Some(level) = &args.require_isolation {
        let level = IsolationLevel::parse_required(level)
            .map_err(|e| format!("--require-isolation: {e}"))?;
        tags.push(format!("{REQUIRE_PREFIX}iso={level}"));
    }
    for tag in &args.require_tags {
        tags.push(format!("{REQUIRE_PREFIX}tag={tag}"));
    }
    for tool in &args.require_tools {
        tags.push(format!("{REQUIRE_PREFIX}tool={tool}"));
    }
    Placement::from_task_tags(&tags)
}

/// For `TaskSpec::RustCompilation`, automatically adds `require:tool=cargo` when `source_files`
/// contains `Cargo.toml` (or `require:tool=rustc` otherwise), unless the caller already specified
/// a `--require-tool` requirement.
pub(super) fn apply_default_spec_placement(spec: &TaskSpec, placement: &mut Placement) {
    if let TaskSpec::RustCompilation { source_files, .. } = spec {
        if placement.tools.is_empty() {
            let tool = if source_files.contains_key("Cargo.toml") {
                "cargo"
            } else {
                "rustc"
            };
            placement.tools.push(tool.to_string());
        }
    }
}
/// (see `rusty_grid_core::placement`). `None` when the master did not answer with a list.
async fn fetch_workers(
    transport: &mut MessageTransport<GridStream>,
    master_addr: &str,
    had_secret: bool,
) -> Option<Vec<WorkerCapabilities>> {
    match exchange(
        transport,
        master_addr,
        had_secret,
        &ClientMessage::ListWorkers,
        "Failed to list the workers",
    )
    .await
    {
        Ok(ClientResponse::WorkerList { workers }) => Some(workers),
        _ => None,
    }
}

/// Whether a listed worker is connected (the master marks the others `state:disconnected`).
fn listed_as_connected(caps: &WorkerCapabilities) -> bool {
    tag_value(&caps.tags, STATE_TAG_PREFIX) != Some("disconnected")
}

/// True when at least one connected GPU worker advertises `>= req_vram_mb` MB of VRAM.
pub(super) fn any_connected_worker_satisfies_vram(
    workers: &[WorkerCapabilities],
    req_vram_mb: u64,
) -> bool {
    workers.iter().any(|w| {
        listed_as_connected(w) && w.can_execute_gpu() && w.vram_mb.is_some_and(|v| v >= req_vram_mb)
    })
}

/// True when at least one connected worker advertises Blender capability.
pub(super) fn any_connected_worker_has_blender(workers: &[WorkerCapabilities]) -> bool {
    workers
        .iter()
        .any(|w| listed_as_connected(w) && w.has_blender)
}

/// Effective VRAM (MB) used by master `SchedulerConfig` defaults when `--vram-mb` is omitted.
pub(super) fn preflight_required_vram_mb(
    spec: &TaskSpec,
    gpu_required: bool,
    explicit_vram_mb: Option<u64>,
) -> Option<u64> {
    if let Some(vram) = explicit_vram_mb.filter(|&v| v > 0) {
        return Some(vram);
    }
    let TaskSpec::Generative3D(gen) = spec else {
        return None;
    };
    // `auto` always runs the CPU SDF tier on the worker (3D-1 D3), so it needs no VRAM.
    if matches!(
        gen.engine(),
        rusty_grid_core::task::Engine3D::Sdf | rusty_grid_core::task::Engine3D::Auto
    ) {
        return None;
    }
    let q = gen.quality.trim();
    if gpu_required || q.eq_ignore_ascii_case("high") {
        if q.eq_ignore_ascii_case("fast") {
            Some(2048)
        } else if q.eq_ignore_ascii_case("high") {
            Some(6144)
        } else {
            Some(4096)
        }
    } else {
        None
    }
}

/// The name of the listed worker with this id, if the list has it.
fn listed_worker_name(workers: &[WorkerCapabilities], worker_id: Uuid) -> Option<String> {
    workers
        .iter()
        .find(|w| listed_worker_id(w) == Some(worker_id))
        .map(|w| w.name.clone())
}

/// `--verify`: runs the task again on `replicas` other workers and compares the outputs with the
/// first run's (`first`).
#[allow(clippy::too_many_arguments)]
async fn verify_on_other_workers(
    master_addr: &str,
    secret: Option<&rusty_grid_core::cluster_auth::ClusterSecret>,
    template: &Task,
    placement: &Placement,
    first: &rusty_grid_core::task::TaskResult,
    first_name: Option<String>,
    replicas: usize,
    verify_file: Option<&str>,
    any_system: bool,
) -> Result<crate::verify::Report, String> {
    let connect = || async {
        connect_master(master_addr, secret)
            .await
            .map_err(|e| format!("could not connect to the master: {e}"))
    };
    let mut transport = connect().await?;
    let workers = fetch_workers(&mut transport, master_addr, secret.is_some())
        .await
        .ok_or("could not list the workers")?;
    drop(transport);
    crate::verify::verify_on_workers(
        crate::verify::Request {
            workers: &workers,
            template,
            placement,
            first,
            first_name,
            replicas,
            file: verify_file,
            any_system,
        },
        connect,
    )
    .await
}

/// Public entry point for programmatic/test submission with default secret options.
pub async fn run_submit(args: SubmitArgs, config_file: Option<ConfigFile>) -> ExitCode {
    run_submit_with_secret(args, config_file, SecretOpts::default()).await
}

pub(super) async fn run_submit_with_secret(
    args: SubmitArgs,
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
        run_submit_with_secret_inner(args, config_file, secret_opts),
    )
    .await
}

async fn run_submit_with_secret_inner(
    args: SubmitArgs,
    config_file: Option<ConfigFile>,
    secret_opts: SecretOpts,
) -> ExitCode {
    // Everything wrong with the arguments is reported before connecting.
    let (spec, placement) = match build_task_spec(&args).and_then(|spec| {
        let mut placement = build_placement(&args)?;
        apply_default_spec_placement(&spec, &mut placement);
        Ok((spec, placement))
    }) {
        Ok(built) => built,
        Err(e) => {
            eprintln!("{e}");
            return ExitCode::FAILURE;
        }
    };

    let matrix_cells = if args.matrix.is_empty() {
        None
    } else {
        if args.verify.is_some() {
            eprintln!("--matrix cannot be combined with --verify");
            return ExitCode::FAILURE;
        }
        if args.output.is_some() {
            eprintln!(
                "--matrix cannot be combined with -o/--output-asset; use --output and --output-dir"
            );
            return ExitCode::FAILURE;
        }
        let max_cells = args
            .max_matrix_cells
            .unwrap_or(crate::matrix::DEFAULT_MAX_MATRIX_CELLS);
        let matrix_spec = match crate::matrix::MatrixSpec::parse(&args.matrix, max_cells) {
            Ok(s) => s,
            Err(e) => {
                eprintln!("{e}");
                return ExitCode::FAILURE;
            }
        };
        let allow_env = matches!(
            spec,
            TaskSpec::Command { .. } | TaskSpec::ShellScript { .. }
        );
        match crate::matrix::expand_cells(&matrix_spec, &placement, allow_env) {
            Ok(cells) => Some(cells),
            Err(e) => {
                eprintln!("{e}");
                return ExitCode::FAILURE;
            }
        }
    };

    // Inputs are hashed (and directories listed) before connecting, so a typo in a path never
    // reaches the master. When `--type compile` was given without `--source`, `args.inputs` was
    // already loaded into `TaskSpec::RustCompilation.source_files`.
    let is_compile = matches!(spec, TaskSpec::RustCompilation { .. });
    let inputs = if is_compile && args.sources.is_empty() {
        crate::blobs::PreparedInputs::default()
    } else {
        match crate::blobs::scan_inputs(&args.inputs) {
            Ok(inputs) => inputs,
            Err(e) => {
                eprintln!("{e}");
                return ExitCode::FAILURE;
            }
        }
    };
    for skipped in &inputs.skipped {
        eprintln!("Warning: --input skips {skipped}");
    }

    let (workspace_ref, workspace_scan) = match &args.workspace {
        Some(ws_name) => {
            if args.no_sync {
                (
                    Some(rusty_grid_core::task::WorkspaceRef::new(ws_name.clone())),
                    None,
                )
            } else {
                let limits = crate::blobs::WorkspaceLimits {
                    max_files: args
                        .workspace_max_files
                        .unwrap_or(crate::blobs::DEFAULT_WORKSPACE_MAX_FILES),
                    max_bytes: args
                        .workspace_max_mb
                        .map(|mb| mb.saturating_mul(MIB))
                        .unwrap_or(crate::blobs::DEFAULT_WORKSPACE_MAX_BYTES),
                };
                let sync_dir = args.sync.as_deref().unwrap_or(Path::new("."));
                let scan = match crate::blobs::scan_workspace_source(sync_dir, limits) {
                    Ok(s) => s,
                    Err(e) => {
                        eprintln!("{e}");
                        return ExitCode::FAILURE;
                    }
                };
                for skipped in &scan.prepared.skipped {
                    eprintln!("Warning: --workspace skips {skipped}");
                }
                let ws_ref = rusty_grid_core::task::WorkspaceRef::new(ws_name.clone())
                    .with_tree(scan.manifest_id, scan.manifest_len);
                (Some(ws_ref), Some(scan))
            }
        }
        None => {
            if args.workspace_max_files.is_some() || args.workspace_max_mb.is_some() {
                eprintln!(
                    "Warning: --workspace-max-files and --workspace-max-mb have no effect without --workspace"
                );
            }
            (None, None)
        }
    };

    let mut output_globs = if !args.outputs.is_empty() {
        if let Err(e) = rusty_grid_core::blob::validate_output_patterns(&args.outputs) {
            eprintln!("--output: {e}");
            return ExitCode::FAILURE;
        }
        args.outputs.clone()
    } else {
        Vec::new()
    };
    if let Some(replicas) = args.verify {
        if let Err(e) = crate::verify::check_options(
            replicas,
            args.verify_file.as_deref(),
            !output_globs.is_empty(),
            args.worker.is_some(),
        ) {
            eprintln!("{e}");
            return ExitCode::FAILURE;
        }
        if let Some(file) = &args.verify_file {
            if !output_globs.contains(file) {
                output_globs.push(file.clone());
            }
        }
    }
    if (!inputs.inputs.is_empty() || !output_globs.is_empty())
        && !matches!(
            spec,
            TaskSpec::Command { .. }
                | TaskSpec::ShellScript { .. }
                | TaskSpec::RustCompilation { .. }
        )
    {
        eprintln!(
            "--input and --output work with command and shell tasks, not --type {}",
            args.task_type
        );
        return ExitCode::FAILURE;
    }

    let submit_cfg = config_file.as_ref().and_then(|c| c.submit.clone());
    let master_addr = resolve_string(
        args.master.clone(),
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

    let submit_started = std::time::Instant::now();

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
    let vram_mb = args.vram_mb.filter(|&v| v > 0);
    let is_gpu = args.gpu
        || vram_mb.is_some()
        || args.task_type.eq_ignore_ascii_case("gpu")
        || spec.requires_gpu();
    let default_timeout_secs = if args.workspace.is_some() { 1800 } else { 60 };
    let timeout_secs = resolve_u64(
        args.timeout_secs,
        &[],
        submit_cfg.as_ref().and_then(|s| s.timeout_secs),
        default_timeout_secs,
    )
    .max(1);
    let requirements = TaskRequirements {
        cpu_cores: args.cores.max(1),
        ram_mb: args.ram_mb,
        gpu_required: is_gpu,
        timeout_secs,
        max_retries: args.max_retries,
        vram_mb,
        extensions: Vec::new(),
    };

    if let Some(cells) = matrix_cells {
        return run_matrix_submit(
            &args,
            cells,
            spec,
            placement,
            requirements,
            inputs,
            workspace_ref,
            workspace_scan,
            output_globs,
            &master_addr,
            secret.as_ref(),
            had_secret,
            submit_started,
            transport,
        )
        .await;
    }

    let requires_blender = matches!(
        &spec,
        TaskSpec::AutoRig(rig)
            if rig
                .preferred_provider
                .as_deref()
                .is_some_and(|p| p.trim().eq_ignore_ascii_case("blender"))
    );
    let preflight_vram_mb = preflight_required_vram_mb(&spec, is_gpu, vram_mb);

    // A constrained or workspace task nobody can take would wait silently; say so (it is still
    // submitted, a matching worker may connect later).
    if !placement.is_empty()
        || workspace_ref.is_some()
        || preflight_vram_mb.is_some()
        || requires_blender
    {
        if let Some(workers) = fetch_workers(&mut transport, &master_addr, had_secret).await {
            if workspace_ref.is_some()
                && !workers
                    .iter()
                    .any(|w| listed_as_connected(w) && listed_supports_workspaces(w))
            {
                eprintln!(
                    "Warning: no connected worker supports workspaces yet; the task will wait"
                );
            }
            if !placement.is_empty()
                && !workers
                    .iter()
                    .any(|w| listed_as_connected(w) && placement.matches_listed(w))
            {
                eprintln!(
                    "Warning: no connected worker matches the placement constraints ({placement}); the task waits in the queue until one does"
                );
            }
            if let Some(req_vram) = preflight_vram_mb {
                if !any_connected_worker_satisfies_vram(&workers, req_vram) {
                    eprintln!(
                        "Warning: no connected GPU worker advertises >= {req_vram} MB VRAM; the task waits in the queue until one does"
                    );
                }
            }
            if requires_blender && !any_connected_worker_has_blender(&workers) {
                eprintln!(
                    "Warning: no connected worker has Blender available; the task waits in the queue until one does"
                );
            }
        }
    }

    // Only the input blobs the master lacks are sent, on a connection of their own.
    let upload = if let Some(ref scan) = workspace_scan {
        let uploaded =
            match crate::blobs::BlobConnection::connect(&master_addr, secret.as_ref()).await {
                Ok(connection) => crate::blobs::upload_inputs(&connection, &scan.prepared).await,
                Err(e) => Err(e.to_string()),
            };
        match uploaded {
            Ok(report) => {
                if scan.prepared.skipped.is_empty() {
                    eprintln!(
                        "Workspace '{}': {} files ({}); sent {} of {} blobs ({}), {} already on the master",
                        args.workspace.as_deref().unwrap_or(""),
                        scan.prepared.files,
                        rusty_grid_worker::staging::human_bytes(scan.prepared.bytes),
                        report.uploaded,
                        report.blobs,
                        rusty_grid_worker::staging::human_bytes(report.bytes),
                        report.blobs - report.uploaded
                    );
                } else {
                    eprintln!(
                        "Workspace '{}': {} files ({}), {} skipped ({}); sent {} of {} blobs ({}), {} already on the master",
                        args.workspace.as_deref().unwrap_or(""),
                        scan.prepared.files,
                        rusty_grid_worker::staging::human_bytes(scan.prepared.bytes),
                        scan.prepared.skipped.len(),
                        scan.prepared.skipped.join("; "),
                        report.uploaded,
                        report.blobs,
                        rusty_grid_worker::staging::human_bytes(report.bytes),
                        report.blobs - report.uploaded
                    );
                }
                Some(report)
            }
            Err(e) => {
                eprintln!("Uploading the workspace files failed: {e}");
                return ExitCode::FAILURE;
            }
        }
    } else if inputs.inputs.is_empty() {
        None
    } else {
        let uploaded =
            match crate::blobs::BlobConnection::connect(&master_addr, secret.as_ref()).await {
                Ok(connection) => crate::blobs::upload_inputs(&connection, &inputs).await,
                Err(e) => Err(e.to_string()),
            };
        match uploaded {
            Ok(report) => {
                eprintln!(
                    "Inputs: {} files ({}); sent {} of {} blobs ({}), {} already on the master",
                    inputs.files,
                    rusty_grid_worker::staging::human_bytes(inputs.bytes),
                    report.uploaded,
                    report.blobs,
                    rusty_grid_worker::staging::human_bytes(report.bytes),
                    report.blobs - report.uploaded
                );
                Some(report)
            }
            Err(e) => {
                eprintln!("Uploading the input files failed: {e}");
                return ExitCode::FAILURE;
            }
        }
    };

    let mut task = Task::new(spec, requirements)
        .with_placement(&placement)
        .with_inputs(inputs.inputs)
        .with_output_globs(output_globs.clone());
    if args.repeatable {
        task.tags
            .push(rusty_grid_core::placement::OPT_REPEATABLE_TAG.to_string());
    }
    if let Some(ws) = workspace_ref {
        task = task.with_workspace(ws);
    }
    let task_id = task.id;
    let verify_template = args.verify.map(|_| task.clone());
    let wait = args.wait || args.output.is_some() || !output_globs.is_empty() || is_compile;
    let submit_msg = ClientMessage::SubmitTask { task, wait };

    let response = if wait {
        if let Err(e) = transport.send_msg(&submit_msg).await {
            return report_master_failure(
                &master_addr,
                had_secret,
                "Failed to send task to Master",
                &GridError::from(e),
            );
        }
        match tokio::time::timeout(
            Duration::from_secs(timeout_secs),
            transport.recv_msg::<ClientResponse>(),
        )
        .await
        {
            Ok(Ok(Some(response))) => response,
            Ok(Ok(None)) => {
                return report_master_failure(
                    &master_addr,
                    had_secret,
                    "Master closed the connection before replying",
                    &GridError::ConnectionClosed,
                );
            }
            Ok(Err(e)) => {
                return report_master_failure(
                    &master_addr,
                    had_secret,
                    "Communication error with Master",
                    &GridError::from(e),
                );
            }
            Err(_) => {
                let msg = format!(
                    "Timed out after {timeout_secs}s waiting for task {task_id} to complete"
                );
                eprintln!("{msg}");
                if args.json {
                    println!(
                        "{}",
                        serde_json::json!({
                            "status": "timeout",
                            "task_id": task_id.to_string(),
                            "error": msg,
                        })
                    );
                }
                return ExitCode::FAILURE;
            }
        }
    } else {
        match exchange(
            &mut transport,
            &master_addr,
            had_secret,
            &submit_msg,
            "Failed to send task to Master",
        )
        .await
        {
            Ok(response) => response,
            Err(code) => return code,
        }
    };

    match response {
        ClientResponse::TaskSubmitted { task_id } => {
            if args.json {
                println!(
                    "{}",
                    serde_json::json!({
                        "status": "submitted",
                        "task_id": task_id.to_string()
                    })
                );
            } else {
                println!("Task submitted successfully. Task ID: {task_id}");
            }
            ExitCode::SUCCESS
        }
        ClientResponse::TaskCompleted { task_id, result } => {
            let worker_name = fetch_workers(&mut transport, &master_addr, had_secret)
                .await
                .and_then(|workers| listed_worker_name(&workers, result.worker_id));

            let asset_output_path = args.output.clone();

            if let (Some(ref out_path), Some(ref asset_bytes)) =
                (&asset_output_path, &result.asset_bytes)
            {
                if let Err(e) = validate_glb_header(asset_bytes.as_slice()) {
                    eprintln!("Asset validation failed before writing to disk: {e}");
                    return ExitCode::FAILURE;
                }
                if let Some(parent) = out_path.parent() {
                    if !parent.as_os_str().is_empty() {
                        if let Err(e) = std::fs::create_dir_all(parent) {
                            eprintln!(
                                "Failed to create output directory '{}': {e}",
                                parent.display()
                            );
                            return ExitCode::FAILURE;
                        }
                    }
                }
                if let Err(e) = std::fs::write(out_path, asset_bytes.as_slice()) {
                    eprintln!("Failed to write asset to '{}': {e}", out_path.display());
                    return ExitCode::FAILURE;
                }
            }

            let output_dir = args.output_dir.as_deref().unwrap_or(Path::new("."));
            let mut download_failed = false;
            let saved_outputs = if result.outputs.is_empty() {
                None
            } else {
                let downloaded = match crate::blobs::BlobConnection::connect(
                    &master_addr,
                    secret.as_ref(),
                )
                .await
                {
                    Ok(connection) => {
                        crate::blobs::download_outputs(&connection, &result.outputs, output_dir)
                            .await
                    }
                    Err(e) => Err(e.to_string()),
                };
                match downloaded {
                    Ok(saved) => {
                        if is_compile && result.asset_bytes.is_none() {
                            if let (Some(ref out_path), Some(first)) =
                                (&asset_output_path, saved.first())
                            {
                                if let Some(parent) = out_path.parent() {
                                    if !parent.as_os_str().is_empty() {
                                        let _ = std::fs::create_dir_all(parent);
                                    }
                                }
                                if let Err(e) = std::fs::copy(first, out_path) {
                                    eprintln!(
                                        "Failed to write compiled output to '{}': {e}",
                                        out_path.display()
                                    );
                                    download_failed = true;
                                }
                            }
                        }
                        Some(saved)
                    }
                    Err(e) => {
                        eprintln!("Downloading the output files failed: {e}");
                        download_failed = true;
                        None
                    }
                }
            };
            let waited_ms = submit_started.elapsed().as_millis().min(u64::MAX as u128) as u64;
            let summary = crate::run_summary::RunSummary::from_results(
                std::slice::from_ref(&result),
                waited_ms,
            );
            let verify_outcome = match (args.verify, &verify_template) {
                (Some(replicas), Some(template)) if result.is_success() => Some(
                    verify_on_other_workers(
                        &master_addr,
                        secret.as_ref(),
                        template,
                        &placement,
                        &result,
                        worker_name.clone(),
                        replicas,
                        args.verify_file.as_deref(),
                        args.verify_any_system,
                    )
                    .await,
                ),
                _ => None,
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
                    obj.insert("worker_name".to_string(), serde_json::json!(worker_name));
                    if let Some(ref saved) = saved_artifacts {
                        let saved_strs: Vec<String> =
                            saved.iter().map(|p| p.display().to_string()).collect();
                        obj.insert("saved_artifacts".to_string(), serde_json::json!(saved_strs));
                    }
                    if let Some(ref err) = artifact_save_error {
                        obj.insert("artifact_save_error".to_string(), serde_json::json!(err));
                    }
                    match &verify_outcome {
                        Some(Ok(report)) => {
                            obj.insert("verify".to_string(), report.to_json());
                        }
                        Some(Err(e)) => {
                            obj.insert(
                                "verify".to_string(),
                                serde_json::json!({"status": "error", "error": e}),
                            );
                        }
                        None => {}
                    }
                    if let Some(ref saved) = saved_outputs {
                        let saved_strs: Vec<String> =
                            saved.iter().map(|p| p.display().to_string()).collect();
                        obj.insert("saved_outputs".to_string(), serde_json::json!(saved_strs));
                    }
                    if let Some(report) = upload {
                        obj.insert("input_upload".to_string(), serde_json::json!(report));
                    }
                    summary.insert_json_fields(obj);
                }
                println!("{}", serde_json::to_string_pretty(&value).unwrap());
            } else {
                if result.is_success() {
                    println!("Task {task_id} completed successfully.");
                } else {
                    println!("Task {task_id} failed.");
                }
                match &worker_name {
                    Some(name) => println!("Worker: {name} ({})", result.worker_id),
                    None => println!("Worker: {}", result.worker_id),
                }
                println!("Exit Code: {}", result.exit_code);
                println!("Execution Time: {}ms", result.execution_time_ms);
                println!("{}", summary.tasks_line());
                println!("{}", summary.waited_line());
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
                if let Some(ref error) = result.error {
                    eprintln!("Error: {error}");
                }
                if let Some(Ok(report)) = &verify_outcome {
                    println!("{}", report.text());
                }
            }
            if let Some(Err(e)) = &verify_outcome {
                eprintln!("Verification failed: {e}");
            }
            if let Some(err) = artifact_save_error {
                eprintln!("Failed to save output artifacts: {err}");
                return ExitCode::FAILURE;
            }
            if download_failed {
                ExitCode::FAILURE
            } else if result.is_success() {
                match verify_outcome {
                    Some(Ok(report)) if !report.verified() => {
                        ExitCode::from(crate::verify::MISMATCH_EXIT_CODE)
                    }
                    Some(Err(_)) => ExitCode::FAILURE,
                    _ => ExitCode::SUCCESS,
                }
            } else if result.exit_code != 0 {
                ExitCode::from(result.exit_code.min(255) as u8)
            } else {
                ExitCode::FAILURE
            }
        }
        ClientResponse::Error { message } => {
            if args.json {
                println!("{}", serde_json::json!({"error": message}));
            } else {
                eprintln!("Master error: {message}");
            }
            ExitCode::FAILURE
        }
        other => {
            eprintln!("Unexpected master response: {other:?}");
            ExitCode::FAILURE
        }
    }
}

#[allow(clippy::too_many_arguments)]
async fn run_matrix_submit(
    args: &SubmitArgs,
    cells: Vec<crate::matrix::MatrixCell>,
    spec: TaskSpec,
    placement: Placement,
    requirements: TaskRequirements,
    inputs: crate::blobs::PreparedInputs,
    workspace_ref: Option<rusty_grid_core::task::WorkspaceRef>,
    workspace_scan: Option<crate::blobs::WorkspaceScan>,
    output_globs: Vec<String>,
    master_addr: &str,
    secret: Option<&rusty_grid_core::cluster_auth::ClusterSecret>,
    _had_secret: bool,
    submit_started: std::time::Instant,
    mut transport: MessageTransport<GridStream>,
) -> ExitCode {
    let rpc_timeout = Duration::from_secs(requirements.timeout_secs.clamp(2, 10));
    let workers = match tokio::time::timeout(
        rpc_timeout,
        fetch_workers(&mut transport, master_addr, secret.is_some()),
    )
    .await
    {
        Ok(Some(workers)) => workers,
        Ok(None) => {
            if args.json {
                println!(
                    "{}",
                    serde_json::json!({"error": "Failed to list connected workers from Master"})
                );
            }
            return ExitCode::FAILURE;
        }
        Err(_) => {
            let msg = format!(
                "Timed out after {}s listing workers from Master",
                rpc_timeout.as_secs()
            );
            eprintln!("{msg}");
            if args.json {
                println!("{}", serde_json::json!({"error": msg}));
            }
            return ExitCode::FAILURE;
        }
    };
    transport.close().await;

    let placeability =
        crate::matrix::check_placeability(&cells, &workers, &requirements, workspace_ref.is_some());
    let placeable_count = placeability.iter().filter(|r| r.is_none()).count();

    let upload = if placeable_count == 0 {
        None
    } else if let Some(ref scan) = workspace_scan {
        let uploaded = match crate::blobs::BlobConnection::connect(master_addr, secret).await {
            Ok(connection) => crate::blobs::upload_inputs(&connection, &scan.prepared).await,
            Err(e) => Err(e.to_string()),
        };
        match uploaded {
            Ok(report) => {
                if scan.prepared.skipped.is_empty() {
                    eprintln!(
                        "Workspace '{}': {} files ({}); sent {} of {} blobs ({}), {} already on the master",
                        args.workspace.as_deref().unwrap_or(""),
                        scan.prepared.files,
                        rusty_grid_worker::staging::human_bytes(scan.prepared.bytes),
                        report.uploaded,
                        report.blobs,
                        rusty_grid_worker::staging::human_bytes(report.bytes),
                        report.blobs - report.uploaded
                    );
                } else {
                    eprintln!(
                        "Workspace '{}': {} files ({}), {} skipped ({}); sent {} of {} blobs ({}), {} already on the master",
                        args.workspace.as_deref().unwrap_or(""),
                        scan.prepared.files,
                        rusty_grid_worker::staging::human_bytes(scan.prepared.bytes),
                        scan.prepared.skipped.len(),
                        scan.prepared.skipped.join("; "),
                        report.uploaded,
                        report.blobs,
                        rusty_grid_worker::staging::human_bytes(report.bytes),
                        report.blobs - report.uploaded
                    );
                }
                Some(report)
            }
            Err(e) => {
                eprintln!("Uploading the workspace files failed: {e}");
                return ExitCode::FAILURE;
            }
        }
    } else if inputs.inputs.is_empty() {
        None
    } else {
        let uploaded = match crate::blobs::BlobConnection::connect(master_addr, secret).await {
            Ok(connection) => crate::blobs::upload_inputs(&connection, &inputs).await,
            Err(e) => Err(e.to_string()),
        };
        match uploaded {
            Ok(report) => {
                eprintln!(
                    "Inputs: {} files ({}); sent {} of {} blobs ({}), {} already on the master",
                    inputs.files,
                    rusty_grid_worker::staging::human_bytes(inputs.bytes),
                    report.uploaded,
                    report.blobs,
                    rusty_grid_worker::staging::human_bytes(report.bytes),
                    report.blobs - report.uploaded
                );
                Some(report)
            }
            Err(e) => {
                eprintln!("Uploading the input files failed: {e}");
                return ExitCode::FAILURE;
            }
        }
    };

    let mut template = Task::new(spec, requirements.clone())
        .with_placement(&placement)
        .with_inputs(inputs.inputs)
        .with_output_globs(output_globs);
    if args.repeatable {
        template
            .tags
            .push(rusty_grid_core::placement::OPT_REPEATABLE_TAG.to_string());
    }
    if let Some(ws) = workspace_ref {
        template = template.with_workspace(ws);
    }

    let addr_arc = std::sync::Arc::new(master_addr.to_string());
    let secret_arc = std::sync::Arc::new(secret.cloned());
    let connect = std::sync::Arc::new(move || {
        let addr = std::sync::Arc::clone(&addr_arc);
        let sec = std::sync::Arc::clone(&secret_arc);
        async move {
            connect_master(&addr, sec.as_ref().as_ref())
                .await
                .map_err(|e| format!("could not connect to the master: {e}"))
        }
    });

    let mut executions = crate::matrix::execute_matrix(
        cells,
        &workers,
        placeability,
        &template,
        Duration::from_secs(requirements.timeout_secs),
        None,
        None,
        connect,
    )
    .await;

    let base_out_dir = args.output_dir.as_deref().unwrap_or(Path::new("."));
    let any_outputs = executions
        .iter()
        .any(|e| e.result.as_ref().is_some_and(|r| !r.outputs.is_empty()));
    if any_outputs {
        match crate::blobs::BlobConnection::connect(master_addr, secret).await {
            Ok(connection) => {
                for exec in &mut executions {
                    let Some(ref result) = exec.result else {
                        continue;
                    };
                    if result.outputs.is_empty() {
                        continue;
                    }
                    let cell_dir = match crate::matrix::ensure_non_symlink_cell_dir(
                        base_out_dir,
                        &exec.cell.slug,
                    ) {
                        Ok(dir) => dir,
                        Err(e) => {
                            eprintln!(
                                "Downloading output files for cell '{}' failed: {e}",
                                exec.cell.label
                            );
                            exec.download_error = Some(e);
                            continue;
                        }
                    };
                    match crate::blobs::download_outputs(&connection, &result.outputs, &cell_dir)
                        .await
                    {
                        Ok(paths) => exec.saved_outputs = Some(paths),
                        Err(e) => {
                            eprintln!(
                                "Downloading output files for cell '{}' failed: {e}",
                                exec.cell.label
                            );
                            exec.download_error = Some(format!("output download failed: {e}"));
                        }
                    }
                }
                connection.close().await;
            }
            Err(e) => {
                for exec in &mut executions {
                    if exec.result.as_ref().is_some_and(|r| !r.outputs.is_empty()) {
                        exec.download_error =
                            Some(format!("failed to connect for output download: {e}"));
                    }
                }
                eprintln!("Downloading matrix output files failed: {e}");
            }
        }
    }

    for exec in &mut executions {
        let Some(ref result) = exec.result else {
            continue;
        };
        if !result.artifacts.is_empty() {
            let cell_dir =
                match crate::matrix::ensure_non_symlink_cell_dir(base_out_dir, &exec.cell.slug) {
                    Ok(dir) => dir,
                    Err(e) => {
                        eprintln!(
                            "Saving artifacts for cell '{}' failed: {e}",
                            exec.cell.label
                        );
                        exec.download_error.get_or_insert(e);
                        continue;
                    }
                };
            match crate::connect::save_output_artifacts(&result.artifacts, &cell_dir) {
                Ok(paths) => exec.saved_artifacts = Some(paths),
                Err(e) => {
                    eprintln!(
                        "Saving artifacts for cell '{}' failed: {e}",
                        exec.cell.label
                    );
                    exec.download_error
                        .get_or_insert_with(|| format!("artifact save failed: {e}"));
                }
            }
        }
    }

    let workspace_summary = args.workspace.as_ref().map(|ws_name| {
        let (files, skipped, skipped_entries) = match &workspace_scan {
            Some(scan) => (
                scan.prepared.files,
                scan.prepared.skipped.len(),
                scan.prepared.skipped.clone(),
            ),
            None => (0, 0, Vec::new()),
        };
        let uploaded_bytes = upload.as_ref().map_or(0, |u| u.bytes);
        let mut ws = serde_json::json!({
            "name": ws_name,
            "files": files,
            "uploaded_bytes": uploaded_bytes,
            "skipped": skipped,
        });
        if !skipped_entries.is_empty() {
            ws["skipped_entries"] = serde_json::json!(skipped_entries);
        }
        ws
    });

    let waited_ms = submit_started.elapsed().as_millis().min(u64::MAX as u128) as u64;
    let report = crate::matrix::MatrixReport::from_executions(
        executions,
        waited_ms,
        crate::matrix::DEFAULT_CELL_OUTPUT_BYTES,
        upload,
        workspace_summary,
    );

    if args.json {
        println!(
            "{}",
            serde_json::to_string_pretty(&report.to_json()).unwrap()
        );
    } else {
        println!("{}", report.text());
    }

    if report.any_failed() {
        ExitCode::FAILURE
    } else {
        ExitCode::SUCCESS
    }
}
