//! Core protocol wire types, framing codecs, capabilities, task schemas, and error hierarchy for `rusty_grid`.

#![deny(unsafe_code)]

pub mod arbitrage;
pub mod blob;
pub mod blob_transfer;
// LV-4: cfg(windows) GetSystemPowerStatus battery/thermal probe and cfg(unix/windows) GPU/device probes.
#[allow(unsafe_code)]
pub mod capabilities;
pub mod cluster_auth;
pub mod discovery;
pub mod error;
pub mod mapreduce;
pub mod mode;
// LV-4: zeroize::ZeroizeOnDrop FFI buffer cleanup / secret keypair guard tests and Unix FFI helpers.
#[allow(unsafe_code)]
pub mod noise_auth;
pub mod placement;
// LV-4: cfg(windows) / cfg(unix) ACL, descriptor, and file-identity syscalls for private key files.
#[allow(unsafe_code)]
pub mod private_file;
pub mod protocol;
pub mod task;
pub mod transport;

#[cfg(test)]
mod window_tests;
#[cfg(test)]
mod wire_layout_tests;

pub use arbitrage::{LeaseValidationError, MasterLeaseTicket, LEASE_VALIDITY_DURATION_MS};
pub use capabilities::{
    calculate_effective_concurrency, calculate_keepalive_interval, is_task_eligible, BatteryTier,
    MobileCapabilities, MobileLiveStatus, ThermalTier, WorkerCapabilities,
};
pub use cluster_auth::{client_handshake, server_handshake, ClusterSecret};
pub use discovery::{
    discover_master, discover_master_with_secret, MasterBeacon, DEFAULT_DISCOVERY_PORT,
    DISCOVERY_MAGIC_REQUEST,
};
pub use error::{sandbox_base_refusal, GridError, GridResult, SANDBOX_BASE_REFUSAL_PREFIX};
pub use mode::{ModeExecutionResult, WorkflowMode};
pub use placement::Placement;
pub use protocol::{
    default_codec, deserialize_message, serialize_message, MasterMessage, MessageReader,
    MessageTransport, MessageWriter, ProtocolError, WireCodec, WorkerMessage, LENGTH_FIELD_BYTES,
    MAX_FRAME_SIZE, WIRE_FORMAT_AUTH, WIRE_FORMAT_BINCODE, WIRE_FORMAT_JSON,
};
pub use task::{
    Task, TaskId, TaskRequirements, TaskResult, TaskSpec, TaskStatus, WorkspaceAffinity,
    WorkspaceRef, WorkspaceSource,
};

#[cfg(test)]
mod tests {
    use std::path::Path;

    fn is_allowed_skip_serializing_if_type(type_name: &str) -> bool {
        type_name.starts_with("Human") || matches!(type_name, "MasterBeacon" | "RendezvousLease")
    }

    fn strip_line_comment(line: &str) -> &str {
        line.split("//").next().unwrap_or("")
    }

    fn parse_type_decl(line: &str) -> Option<&str> {
        let mut tokens = line.split_whitespace().peekable();
        if let Some(&vis) = tokens.peek() {
            if vis == "pub" {
                tokens.next();
                if tokens.peek().is_some_and(|t| t.starts_with('(')) {
                    for tok in tokens.by_ref() {
                        if tok.ends_with(')') {
                            break;
                        }
                    }
                }
            } else if vis.starts_with("pub(") {
                for tok in tokens.by_ref() {
                    if tok.ends_with(')') {
                        break;
                    }
                }
            }
        }
        let kw = tokens.next()?;
        if !matches!(kw, "struct" | "enum") {
            return None;
        }
        let raw_name = tokens.next()?;
        let name = raw_name
            .trim_end_matches(|c: char| !c.is_alphanumeric() && c != '_')
            .split('<')
            .next()?;
        (!name.is_empty()).then_some(name)
    }

    fn find_disallowed_skip_serializing_if(file_label: &str, src: &str) -> Vec<String> {
        let mut violations = Vec::new();
        let mut current_type: Option<(String, i32)> = None;
        let mut brace_depth: i32 = 0;

        for (idx, raw_line) in src.lines().enumerate() {
            let line_no = idx + 1;
            let code = strip_line_comment(raw_line);
            let trimmed = code.trim();
            if trimmed == "mod tests {" || trimmed.starts_with("mod tests {") {
                break;
            }
            if current_type.is_none() {
                if let Some(type_name) = parse_type_decl(code.trim_start()) {
                    current_type = Some((type_name.to_string(), brace_depth));
                }
            }
            if code.contains("skip_serializing_if") {
                match &current_type {
                    Some((type_name, _)) if is_allowed_skip_serializing_if_type(type_name) => {}
                    Some((type_name, _)) => {
                        violations.push(format!(
                            "{file_label}:{line_no}: `{type_name}` must not carry `skip_serializing_if` (bincode is positional)"
                        ));
                    }
                    None => {
                        violations.push(format!(
                            "{file_label}:{line_no}: `skip_serializing_if` outside an allowed type"
                        ));
                    }
                }
            }
            for ch in code.chars() {
                match ch {
                    '{' => brace_depth += 1,
                    '}' => {
                        brace_depth -= 1;
                        if let Some((_, start_depth)) = &current_type {
                            if brace_depth <= *start_depth {
                                current_type = None;
                            }
                        }
                    }
                    ';' => {
                        if let Some((_, start_depth)) = &current_type {
                            if brace_depth == *start_depth {
                                current_type = None;
                            }
                        }
                    }
                    _ => {}
                }
            }
        }

        violations
    }

    fn collect_non_test_rs_files(dir: &Path, out: &mut Vec<std::path::PathBuf>) {
        for entry in std::fs::read_dir(dir).expect("read rust source dir") {
            let entry = entry.expect("dir entry");
            let path = entry.path();
            if path.is_dir() {
                collect_non_test_rs_files(&path, out);
                continue;
            }
            if path.extension().and_then(|e| e.to_str()) != Some("rs") {
                continue;
            }
            let name = path.file_name().and_then(|n| n.to_str()).unwrap_or("");
            if name == "tests.rs" || (name.ends_with("_tests.rs") && name != "wire_layout_tests.rs")
            {
                continue;
            }
            out.push(path);
        }
    }

    #[test]
    #[cfg_attr(miri, ignore = "Miri: too slow under the interpreter")]
    fn skip_serializing_if_is_kept_off_bincode_types() {
        let src_dir = Path::new(env!("CARGO_MANIFEST_DIR")).join("src");
        let mut violations = Vec::new();
        let mut files = Vec::new();
        collect_non_test_rs_files(&src_dir, &mut files);
        files.sort();
        let checked_files = files.len();

        for path in &files {
            let label = path
                .strip_prefix(&src_dir)
                .unwrap_or(path)
                .to_string_lossy()
                .replace('\\', "/");
            let content = std::fs::read_to_string(path).expect("read rust source file");
            violations.extend(find_disallowed_skip_serializing_if(&label, &content));
        }

        assert!(checked_files > 0, "expected to scan crates/core/src/*.rs");
        assert!(
            violations.is_empty(),
            "Found disallowed `skip_serializing_if` in crates/core:\n{}",
            violations.join("\n")
        );

        let rejected = find_disallowed_skip_serializing_if(
            "blob.rs",
            "pub struct TaskInput {\n    #[serde(skip_serializing_if = \"String::is_empty\")]\n    pub path: String,\n}\n",
        );
        assert_eq!(rejected.len(), 1);
        assert!(rejected[0].contains("TaskInput"));

        let allowed = find_disallowed_skip_serializing_if(
            "task.rs",
            "struct HumanTaskResult {\n    #[serde(skip_serializing_if = \"Option::is_none\")]\n    pub error: Option<String>,\n}\n",
        );
        assert!(allowed.is_empty());

        let after_cfg_test_fn_and_tuple_struct = find_disallowed_skip_serializing_if(
            "protocol.rs",
            "#[cfg(test)]\nfn starting_at() {}\nstruct HumanTuple(u8);\nenum BinaryWorkerMessage {\n    Register {\n        #[serde(skip_serializing_if = \"Uuid::is_nil\")]\n        worker_id: Uuid,\n    },\n}\n",
        );
        assert_eq!(after_cfg_test_fn_and_tuple_struct.len(), 1);
        assert!(after_cfg_test_fn_and_tuple_struct[0].contains("BinaryWorkerMessage"));

        let pub_super_bincode = find_disallowed_skip_serializing_if(
            "protocol/worker_master_mirrors.rs",
            "pub(super) enum BinaryWorkerMessage {\n    Register {\n        #[serde(skip_serializing_if = \"Uuid::is_nil\")]\n        worker_id: Uuid,\n    },\n}\n",
        );
        assert_eq!(pub_super_bincode.len(), 1);
        assert!(pub_super_bincode[0].contains("BinaryWorkerMessage"));

        let pub_super_human = find_disallowed_skip_serializing_if(
            "protocol/client_msg_mirrors.rs",
            "pub(super) enum HumanClientMessage {\n    Submit {\n        #[serde(skip_serializing_if = \"Option::is_none\")]\n        note: Option<String>,\n    },\n}\n",
        );
        assert!(pub_super_human.is_empty());
    }

    #[test]
    fn workspace_and_all_crates_inherit_msrv_1_91() {
        let workspace_root = Path::new(env!("CARGO_MANIFEST_DIR"))
            .parent()
            .and_then(Path::parent)
            .expect("workspace root");
        let root_toml =
            std::fs::read_to_string(workspace_root.join("Cargo.toml")).expect("read Cargo.toml");
        assert!(
            root_toml
                .lines()
                .any(|l| l.trim() == r#"rust-version = "1.91""#),
            "workspace Cargo.toml must declare rust-version = \"1.91\""
        );

        let crates_dir = workspace_root.join("crates");
        let mut checked_crates = 0usize;
        for entry in std::fs::read_dir(&crates_dir).expect("read crates/") {
            let entry = entry.expect("dir entry");
            let manifest = entry.path().join("Cargo.toml");
            if !manifest.is_file() {
                continue;
            }
            checked_crates += 1;
            let content = std::fs::read_to_string(&manifest).expect("read crate Cargo.toml");
            assert!(
                content
                    .lines()
                    .any(|l| l.trim() == "rust-version.workspace = true"),
                "{} must inherit rust-version.workspace = true",
                manifest.display()
            );
        }
        assert!(
            checked_crates >= 6,
            "expected at least 6 workspace crates, found {checked_crates}"
        );
    }
}
