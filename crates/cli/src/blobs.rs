//! The client side of task input and output files (`submit --input`, `--output`,
//! `--output-dir`): hashing local files and directories into blobs, uploading the blobs the Master
//! lacks, and downloading a task's output files. Design record:
//! `docs/design/0006-content-addressed-data-plane.md`.
//!
//! * A directory becomes a `TreeManifest` of its regular files. Symbolic links, special files and
//!   names that cannot be a portable relative path are skipped and listed in
//!   [`PreparedInputs::skipped`], so nothing outside the named directory is ever sent.
//! * Only the blobs the Master does not hold are uploaded: resubmitting the same inputs costs one
//!   `Missing` round trip.
//! * Blob transfers use a connection of their own ([`BlobConnection`]), apart from the one that
//!   submits the task and waits for it.
//! * Output files are written to a temporary file, checked against their blob id, and renamed into
//!   place; a path that would lead through a symbolic link in the destination is refused.

use std::collections::HashMap;
use std::io::Read;
use std::path::{Component, Path, PathBuf};
use std::time::SystemTime;

use tokio::net::TcpStream;
use tokio::sync::Mutex;

use rusty_grid_core::blob::{
    hash_reader, open_regular_file_nofollow, validate_blob_path, validate_task_inputs, BlobFile,
    BlobId, TaskInput, TreeManifest, SANDBOX_ROOT,
};
use rusty_grid_core::blob_transfer::{
    download_blob, for_each_concurrently, missing_blobs, upload_blob, BlobChannel, BlobError,
    BlobReply, BlobRequest,
};
use rusty_grid_core::cluster_auth::ClusterSecret;
use rusty_grid_core::error::GridResult;
use rusty_grid_core::protocol::{ClientMessage, ClientResponse, MessageTransport};

use crate::connect::connect_master;

/// Default maximum number of regular files staged in one bounded (`scan_input_paths_with_limits`)
/// scan call (used by the MCP server).
pub const DEFAULT_MAX_INPUT_FILES: usize = 1_000;
/// Default maximum aggregate byte size of regular files staged in one bounded
/// (`scan_input_paths_with_limits`) scan call (256 MiB, used by the MCP server).
pub const DEFAULT_MAX_INPUT_BYTES: u64 = 256 * 1024 * 1024;

/// Caps on the number and total size of regular files scanned into [`PreparedInputs`].
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct InputLimits {
    pub max_files: usize,
    pub max_bytes: u64,
}

impl Default for InputLimits {
    fn default() -> Self {
        Self {
            max_files: DEFAULT_MAX_INPUT_FILES,
            max_bytes: DEFAULT_MAX_INPUT_BYTES,
        }
    }
}

/// Metadata snapshot recorded on the open file handle at scan time and re-checked when the file
/// is re-opened for upload so neither open file handles nor symlink/content swaps leak across
/// the scan-to-upload window.
#[derive(Debug, Clone, PartialEq, Eq)]
struct FileFingerprint {
    len: u64,
    modified: Option<SystemTime>,
    #[cfg(unix)]
    dev: u64,
    #[cfg(unix)]
    ino: u64,
    nofollow: bool,
}

impl FileFingerprint {
    fn from_metadata(meta: &std::fs::Metadata, nofollow: bool) -> Self {
        #[cfg(unix)]
        use std::os::unix::fs::MetadataExt;
        Self {
            len: meta.len(),
            modified: meta.modified().ok(),
            #[cfg(unix)]
            dev: meta.dev(),
            #[cfg(unix)]
            ino: meta.ino(),
            nofollow,
        }
    }

    fn matches(&self, meta: &std::fs::Metadata) -> bool {
        #[cfg(unix)]
        {
            use std::os::unix::fs::MetadataExt;
            if self.dev != meta.dev() || self.ino != meta.ino() {
                return false;
            }
        }
        self.len == meta.len() && self.modified == meta.modified().ok()
    }
}

/// Where the content of a blob to upload comes from.
#[derive(Clone)]
enum LocalSource {
    File {
        path: PathBuf,
        fingerprint: FileFingerprint,
    },
    Bytes(Vec<u8>),
}

#[derive(Clone)]
struct LocalBlob {
    size: u64,
    source: LocalSource,
}

/// Local files and directories turned into task inputs, ready to upload.
#[derive(Default)]
pub struct PreparedInputs {
    /// The inputs to put into `Task::inputs`.
    pub inputs: Vec<TaskInput>,
    blobs: HashMap<BlobId, LocalBlob>,
    /// Regular files sent.
    pub files: usize,
    /// Their total size in bytes.
    pub bytes: u64,
    /// Entries left out of directories, with the reason.
    pub skipped: Vec<String>,
}

impl PreparedInputs {
    /// The distinct blobs the inputs need (files and directory manifests).
    pub fn blob_count(&self) -> usize {
        self.blobs.len()
    }

    /// True if `id` is among the prepared blobs.
    pub fn has_blob(&self, id: &BlobId) -> bool {
        self.blobs.contains_key(id)
    }
}

/// What an upload did.
#[derive(Debug, Clone, Copy, Default, PartialEq, Eq, serde::Serialize)]
pub struct UploadReport {
    /// Distinct blobs the inputs need.
    pub blobs: usize,
    /// Blobs that had to be sent (the Master lacked them).
    pub uploaded: usize,
    /// Bytes sent.
    pub bytes: u64,
}

/// Splits `LOCAL[:SANDBOX]` (a Windows drive letter such as `C:\x` is part of LOCAL). The
/// sandbox path is checked later, once it is known whether LOCAL is a file or a directory.
pub fn parse_input_spec(spec: &str) -> Result<(PathBuf, Option<String>), String> {
    let spec = spec.trim();
    if spec.is_empty() {
        return Err("--input: the path cannot be empty".to_string());
    }
    let bytes = spec.as_bytes();
    let has_drive = bytes.len() >= 2 && bytes[1] == b':' && bytes[0].is_ascii_alphabetic();
    let search_from = if has_drive { 2 } else { 0 };
    match spec[search_from..].find(':').map(|i| i + search_from) {
        Some(pos) => {
            let (local, dest) = (spec[..pos].trim(), spec[pos + 1..].trim());
            if local.is_empty() || dest.is_empty() {
                return Err(format!(
                    "--input '{spec}': expected LOCAL or LOCAL:SANDBOX_PATH"
                ));
            }
            Ok((PathBuf::from(local), Some(dest.replace('\\', "/"))))
        }
        None => Ok((PathBuf::from(spec), None)),
    }
}

/// The sandbox path an input lands at when the user named none: the local path itself when it is
/// relative and stays below the current directory (`data/in.csv`, `./src`), else its last
/// component; the sandbox root for a directory like `.`.
fn default_destination(local: &Path, is_dir: bool) -> Result<String, String> {
    let mut parts = Vec::new();
    let mut relative = true;
    for component in local.components() {
        match component {
            Component::Normal(part) => match part.to_str() {
                Some(part) => parts.push(part.to_string()),
                None => relative = false,
            },
            Component::CurDir => {}
            _ => relative = false,
        }
    }
    if relative && !parts.is_empty() {
        let joined = parts.join("/");
        if validate_blob_path(&joined).is_ok() {
            return Ok(joined);
        }
    }
    match local.file_name().and_then(|name| name.to_str()) {
        Some(name) if validate_blob_path(name).is_ok() => Ok(name.to_string()),
        _ if is_dir => Ok(SANDBOX_ROOT.to_string()),
        _ => Err(format!(
            "--input '{}': cannot derive a sandbox path; name one with LOCAL:SANDBOX_PATH",
            local.display()
        )),
    }
}

fn normalize_destination(dest: &str, is_dir: bool) -> Result<String, String> {
    let parts: Vec<&str> = dest
        .split('/')
        .filter(|part| !part.is_empty() && *part != ".")
        .collect();
    if parts.is_empty() {
        return if is_dir {
            Ok(SANDBOX_ROOT.to_string())
        } else {
            Err(format!(
                "--input: '{dest}' is the sandbox itself; a file needs a file name"
            ))
        };
    }
    let joined = parts.join("/");
    validate_blob_path(&joined).map_err(|e| format!("--input: sandbox path '{dest}': {e}"))?;
    Ok(joined)
}

fn is_executable(meta: &std::fs::Metadata) -> bool {
    #[cfg(unix)]
    {
        use std::os::unix::fs::PermissionsExt;
        meta.permissions().mode() & 0o111 != 0
    }
    #[cfg(not(unix))]
    {
        let _ = meta;
        false
    }
}

/// Hashes the files and directories named by `specs` (`LOCAL[:SANDBOX]`) into task inputs.
/// Used by CLI `submit --input` (no file-count or byte-size limit; top-level symlinks are
/// followed while symlinks inside directories are skipped).
pub fn scan_inputs(specs: &[String]) -> Result<PreparedInputs, String> {
    let mut parsed = Vec::with_capacity(specs.len());
    for spec in specs {
        parsed.push(parse_input_spec(spec)?);
    }
    scan_input_paths(&parsed)
}

/// Hashes the pre-parsed `(local_path, optional_sandbox_dest)` pairs into task inputs without
/// file-count or byte-size caps (used by CLI callers).
pub fn scan_input_paths(specs: &[(PathBuf, Option<String>)]) -> Result<PreparedInputs, String> {
    scan_input_paths_inner(specs, None)
}

/// Hashes the pre-parsed `(local_path, optional_sandbox_dest)` pairs into task inputs, enforcing
/// `limits` on the number of regular files and their aggregate byte size and refusing symbolic
/// links at every level (`O_NOFOLLOW`). Used by the MCP server.
pub fn scan_input_paths_with_limits(
    specs: &[(PathBuf, Option<String>)],
    limits: InputLimits,
) -> Result<PreparedInputs, String> {
    scan_input_paths_inner(specs, Some(limits))
}

fn scan_input_paths_inner(
    specs: &[(PathBuf, Option<String>)],
    limits: Option<InputLimits>,
) -> Result<PreparedInputs, String> {
    let nofollow = limits.is_some();
    let mut prepared = PreparedInputs::default();
    for (local, dest) in specs {
        let meta = if nofollow {
            let symlink_meta = std::fs::symlink_metadata(local)
                .map_err(|e| format!("--input {}: {e}", local.display()))?;
            if symlink_meta.file_type().is_symlink() {
                return Err(format!(
                    "--input {}: symbolic links are not sent",
                    local.display()
                ));
            }
            symlink_meta
        } else {
            std::fs::metadata(local).map_err(|e| format!("--input {}: {e}", local.display()))?
        };
        let is_dir = meta.is_dir();
        let dest = match dest {
            Some(dest) => normalize_destination(dest, is_dir)?,
            None => default_destination(local, is_dir)?,
        };
        if meta.is_file() {
            let (id, size, executable) = prepared.add_file_from_path(local, limits)?;
            prepared
                .inputs
                .push(TaskInput::file(&dest, id, size, executable));
        } else if is_dir {
            let files = prepared.scan_directory(local, limits)?;
            let manifest = TreeManifest::new(files)
                .map_err(|e| format!("--input {}: {e}", local.display()))?;
            let encoded = manifest.encode();
            let id = BlobId::of(&encoded);
            prepared
                .inputs
                .push(TaskInput::tree(&dest, id, encoded.len() as u64));
            prepared.blobs.insert(
                id,
                LocalBlob {
                    size: encoded.len() as u64,
                    source: LocalSource::Bytes(encoded),
                },
            );
        } else {
            return Err(format!(
                "--input {}: not a regular file or directory",
                local.display()
            ));
        }
    }
    validate_task_inputs(&prepared.inputs).map_err(|e| format!("--input: {e}"))?;
    Ok(prepared)
}

impl PreparedInputs {
    /// Opens `path` (with `O_NOFOLLOW` when `limits` or directory traversal requires it), records
    /// its `(dev, ino, len, mtime)` fingerprint via `fstat`, hashes from the open handle, and
    /// closes the file immediately so scanning thousands of files never exhausts `RLIMIT_NOFILE`.
    fn add_file_from_path(
        &mut self,
        path: &Path,
        limits: Option<InputLimits>,
    ) -> Result<(BlobId, u64, bool), String> {
        self.add_file_from_path_with_mode(path, limits, limits.is_some())
    }

    fn add_file_from_path_with_mode(
        &mut self,
        path: &Path,
        limits: Option<InputLimits>,
        nofollow: bool,
    ) -> Result<(BlobId, u64, bool), String> {
        if let Some(lim) = limits {
            if self.files >= lim.max_files {
                return Err(format!(
                    "--input {}: input count exceeds the limit of {} files",
                    path.display(),
                    lim.max_files
                ));
            }
        }
        let (mut file, meta) = if nofollow {
            open_regular_file_nofollow(path)
                .map_err(|e| format!("--input {}: {e}", path.display()))?
        } else {
            let f = std::fs::File::open(path)
                .map_err(|e| format!("--input {}: {e}", path.display()))?;
            let m = f
                .metadata()
                .map_err(|e| format!("--input {}: {e}", path.display()))?;
            if !m.is_file() {
                return Err(format!("--input {}: not a regular file", path.display()));
            }
            (f, m)
        };
        let fingerprint = FileFingerprint::from_metadata(&meta, nofollow);
        let (id, size) = if let Some(lim) = limits {
            let remaining = lim.max_bytes.saturating_sub(self.bytes);
            if meta.len() > remaining {
                return Err(format!(
                    "--input {}: total input size ({} bytes) exceeds the limit of {} bytes",
                    path.display(),
                    self.bytes.saturating_add(meta.len()),
                    lim.max_bytes
                ));
            }
            let (id, size) = hash_reader((&mut file).take(remaining.saturating_add(1)))
                .map_err(|e| format!("--input {}: {e}", path.display()))?;
            if size > remaining {
                return Err(format!(
                    "--input {}: total input size ({} bytes) exceeds the limit of {} bytes",
                    path.display(),
                    self.bytes.saturating_add(size),
                    lim.max_bytes
                ));
            }
            (id, size)
        } else {
            hash_reader(&mut file).map_err(|e| format!("--input {}: {e}", path.display()))?
        };
        drop(file);
        let executable = is_executable(&meta);
        self.files += 1;
        self.bytes += size;
        self.blobs.entry(id).or_insert_with(|| LocalBlob {
            size,
            source: LocalSource::File {
                path: path.to_path_buf(),
                fingerprint,
            },
        });
        Ok((id, size, executable))
    }

    /// The regular files below `root`, with paths relative to it.
    fn scan_directory(
        &mut self,
        root: &Path,
        limits: Option<InputLimits>,
    ) -> Result<Vec<BlobFile>, String> {
        let mut files = Vec::new();
        let mut stack = vec![(root.to_path_buf(), String::new())];
        while let Some((dir, rel_dir)) = stack.pop() {
            let entries =
                std::fs::read_dir(&dir).map_err(|e| format!("--input {}: {e}", dir.display()))?;
            for entry in entries {
                let entry = entry.map_err(|e| format!("--input {}: {e}", dir.display()))?;
                let path = entry.path();
                let Some(name) = entry.file_name().to_str().map(str::to_string) else {
                    self.skipped
                        .push(format!("{} (name is not UTF-8)", path.display()));
                    continue;
                };
                let rel = if rel_dir.is_empty() {
                    name
                } else {
                    format!("{rel_dir}/{name}")
                };
                if let Err(e) = validate_blob_path(&rel) {
                    self.skipped.push(format!("{} ({e})", path.display()));
                    continue;
                }
                let file_type = entry
                    .file_type()
                    .map_err(|e| format!("--input {}: {e}", path.display()))?;
                if file_type.is_dir() {
                    stack.push((path, rel));
                } else if file_type.is_file() {
                    let (id, size, executable) =
                        self.add_file_from_path_with_mode(&path, limits, true)?;
                    files.push(BlobFile {
                        path: rel,
                        id,
                        size,
                        executable,
                    });
                } else if file_type.is_symlink() {
                    self.skipped
                        .push(format!("{} (symbolic links are not sent)", path.display()));
                } else {
                    self.skipped
                        .push(format!("{} (not a regular file)", path.display()));
                }
            }
        }
        Ok(files)
    }
}

/// A connection to the Master used only for blob requests. Requests are answered in order, one
/// at a time.
pub struct BlobConnection {
    transport: Mutex<MessageTransport<TcpStream>>,
}

impl BlobConnection {
    pub fn new(transport: MessageTransport<TcpStream>) -> Self {
        Self {
            transport: Mutex::new(transport),
        }
    }

    /// Opens a new authenticated connection to the Master at `addr` for blob transfers.
    pub async fn connect(addr: &str, secret: Option<&ClusterSecret>) -> GridResult<Self> {
        Ok(Self::new(connect_master(addr, secret).await?))
    }
}

/// What a master that predates blob transfer does with a `BlobRequest`: it cannot decode it and
/// drops the connection.
const OLD_MASTER_HINT: &str = "if the master runs an older version, restart it with this one \
     (it cannot move files and closes the connection)";

impl BlobChannel for BlobConnection {
    async fn call(&self, request: BlobRequest) -> Result<BlobReply, BlobError> {
        let mut transport = self.transport.lock().await;
        transport
            .send_msg(&ClientMessage::BlobRequest { request })
            .await
            .map_err(|e| BlobError::Transport(format!("sending to the master failed: {e}")))?;
        match transport.recv_msg::<ClientResponse>().await {
            Ok(Some(ClientResponse::BlobReply { reply })) => Ok(reply),
            Ok(Some(ClientResponse::Error { message })) => Err(BlobError::Refused(message)),
            Ok(Some(other)) => Err(BlobError::Transport(format!(
                "unexpected answer from the master: {other:?}"
            ))),
            Ok(None) => Err(BlobError::Transport(format!(
                "the master closed the connection; {OLD_MASTER_HINT}"
            ))),
            Err(e) => Err(BlobError::Transport(format!(
                "reading from the master failed: {e}; {OLD_MASTER_HINT}"
            ))),
        }
    }
}

/// Uploads the blobs of `prepared` that the Master lacks.
pub async fn upload_inputs<C: BlobChannel>(
    channel: &C,
    prepared: &PreparedInputs,
) -> Result<UploadReport, String> {
    let ids: Vec<BlobId> = prepared.blobs.keys().copied().collect();
    let missing = missing_blobs(channel, &ids)
        .await
        .map_err(|e| e.to_string())?;
    let uploaded = missing.len();
    let jobs: Vec<(BlobId, LocalBlob)> = missing
        .into_iter()
        .map(|id| (id, prepared.blobs[&id].clone()))
        .collect();
    let bytes = for_each_concurrently(jobs, |(id, blob)| async move {
        match blob.source {
            LocalSource::File { path, fingerprint } => {
                let (std_file, meta) = if fingerprint.nofollow {
                    open_regular_file_nofollow(&path)?
                } else {
                    let f = std::fs::File::open(&path)?;
                    let m = f.metadata()?;
                    (f, m)
                };
                if !fingerprint.matches(&meta) {
                    return Err(BlobError::Io(std::io::Error::new(
                        std::io::ErrorKind::InvalidData,
                        format!(
                            "input file '{}' changed on disk between scan and upload",
                            path.display()
                        ),
                    )));
                }
                let tokio_file = tokio::fs::File::from_std(std_file);
                upload_blob(channel, id, blob.size, tokio_file).await
            }
            LocalSource::Bytes(bytes) => {
                upload_blob(channel, id, blob.size, std::io::Cursor::new(bytes)).await
            }
        }
    })
    .await
    .map_err(|e| e.to_string())?;
    Ok(UploadReport {
        blobs: ids.len(),
        uploaded,
        bytes,
    })
}

/// Creates the directories of `rel` below `root`, refusing to go through a symbolic link or a
/// file, and returns the path the file goes to.
pub(crate) fn prepare_destination(root: &Path, rel: &str) -> Result<PathBuf, String> {
    validate_blob_path(rel).map_err(|e| format!("output '{rel}': {e}"))?;
    #[cfg(windows)]
    rusty_grid_core::task::validate_relative_sandbox_path(rel)
        .map_err(|e| format!("output '{rel}': {e}"))?;
    let parts: Vec<&str> = rel.split('/').collect();
    let mut dir = root.to_path_buf();
    for part in &parts[..parts.len() - 1] {
        dir.push(part);
        loop {
            match std::fs::symlink_metadata(&dir) {
                Ok(meta) if meta.is_dir() => break,
                Ok(meta) if meta.file_type().is_symlink() => {
                    return Err(format!(
                        "output '{rel}': '{}' is a symbolic link; refusing to write through it",
                        dir.display()
                    ))
                }
                Ok(_) => {
                    return Err(format!(
                        "output '{rel}': '{}' exists and is not a directory",
                        dir.display()
                    ))
                }
                Err(e) if e.kind() == std::io::ErrorKind::NotFound => {
                    match std::fs::create_dir(&dir) {
                        Ok(()) => break,
                        // Created meanwhile by a parallel download: look again.
                        Err(e) if e.kind() == std::io::ErrorKind::AlreadyExists => continue,
                        Err(e) => return Err(format!("output '{rel}': {e}")),
                    }
                }
                Err(e) => return Err(format!("output '{rel}': {e}")),
            }
        }
    }
    let target = dir.join(parts[parts.len() - 1]);
    if let Ok(meta) = std::fs::symlink_metadata(&target) {
        if meta.file_type().is_symlink() {
            return Err(format!(
                "output '{rel}': '{}' is a symbolic link; refusing to write through it",
                target.display()
            ));
        }
        if meta.is_dir() {
            return Err(format!(
                "output '{rel}': '{}' is a directory",
                target.display()
            ));
        }
    }
    Ok(target)
}

/// Downloads `outputs` into `dest_dir` (created if needed) and returns where each one went, in
/// the same order.
pub async fn download_outputs<C: BlobChannel>(
    channel: &C,
    outputs: &[BlobFile],
    dest_dir: &Path,
) -> Result<Vec<PathBuf>, String> {
    if let Ok(meta) = std::fs::symlink_metadata(dest_dir) {
        if meta.file_type().is_symlink() {
            return Err(format!(
                "cannot use output directory '{}': it is a symbolic link",
                dest_dir.display()
            ));
        }
        if !meta.is_dir() {
            return Err(format!(
                "cannot use output directory '{}': exists and is not a directory",
                dest_dir.display()
            ));
        }
    }
    std::fs::create_dir_all(dest_dir).map_err(|e| {
        format!(
            "cannot create the output directory '{}': {e}",
            dest_dir.display()
        )
    })?;
    let mut targets = Vec::with_capacity(outputs.len());
    for output in outputs {
        targets.push(prepare_destination(dest_dir, &output.path)?);
    }
    let jobs: Vec<(BlobFile, PathBuf)> = outputs
        .iter()
        .cloned()
        .zip(targets.iter().cloned())
        .collect();
    for_each_concurrently(jobs, |(output, target)| async move {
        let name = target
            .file_name()
            .and_then(|n| n.to_str())
            .unwrap_or("output");
        let tmp = target.with_file_name(format!(".{name}.{}.part", uuid::Uuid::new_v4()));
        let outcome = async {
            let mut file = tokio::fs::File::create(&tmp).await?;
            let bytes = download_blob(channel, output.id, Some(output.size), &mut file).await?;
            drop(file);
            rusty_grid_worker::sandbox::set_permissions(
                &tmp,
                if output.executable { 0o755 } else { 0o644 },
            )?;
            tokio::fs::rename(&tmp, &target).await?;
            Ok::<u64, BlobError>(bytes)
        }
        .await;
        if outcome.is_err() {
            let _ = tokio::fs::remove_file(&tmp).await;
        }
        outcome.map_err(|e| match e {
            BlobError::NotFound(id) => BlobError::Refused(format!(
                "output '{}' (blob {id}) is no longer on the master; it was evicted from the \
                 blob store (run the task again, or raise `master --blob-store-mb`)",
                output.path
            )),
            other => other,
        })
    })
    .await
    .map_err(|e| e.to_string())?;
    Ok(targets)
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn input_specs_split_on_the_colon_but_keep_drive_letters() {
        assert_eq!(
            parse_input_spec("data.csv").unwrap(),
            (PathBuf::from("data.csv"), None)
        );
        assert_eq!(
            parse_input_spec("local/dir:remote/dir").unwrap(),
            (PathBuf::from("local/dir"), Some("remote/dir".to_string()))
        );
        assert_eq!(
            parse_input_spec("C:\\proj:src").unwrap(),
            (PathBuf::from("C:\\proj"), Some("src".to_string()))
        );
        assert_eq!(
            parse_input_spec(".:.").unwrap(),
            (PathBuf::from("."), Some(".".to_string()))
        );
        assert!(parse_input_spec("").is_err());
        assert!(parse_input_spec("data:").is_err(), "an empty sandbox path");
        assert_eq!(
            parse_input_spec("x:").unwrap().0,
            PathBuf::from("x:"),
            "a bare drive letter is a local path"
        );
    }

    #[test]
    fn destinations_default_to_the_relative_path_or_the_last_component() {
        assert_eq!(
            default_destination(Path::new("./data/in.csv"), false).unwrap(),
            "data/in.csv"
        );
        assert_eq!(default_destination(Path::new("src/"), true).unwrap(), "src");
        assert_eq!(
            default_destination(Path::new("/abs/path/proj"), true).unwrap(),
            "proj"
        );
        assert_eq!(
            default_destination(Path::new("../sibling"), true).unwrap(),
            "sibling"
        );
        assert_eq!(
            default_destination(Path::new("."), true).unwrap(),
            SANDBOX_ROOT
        );
        assert!(default_destination(Path::new("."), false).is_err());
        assert_eq!(normalize_destination("./a//b/", false).unwrap(), "a/b");
        assert_eq!(normalize_destination(".", true).unwrap(), SANDBOX_ROOT);
        assert!(normalize_destination(".", false).is_err());
        assert!(normalize_destination("../x", true).is_err());
    }

    #[cfg(unix)]
    #[test]
    fn scanning_a_directory_skips_links_and_hashes_every_file() {
        let dir = tempfile::tempdir().unwrap();
        let proj = dir.path().join("proj");
        std::fs::create_dir_all(proj.join("src")).unwrap();
        std::fs::write(proj.join("Cargo.toml"), b"[package]").unwrap();
        std::fs::write(proj.join("src/main.rs"), b"fn main() {}").unwrap();
        std::fs::write(proj.join("src/copy.rs"), b"fn main() {}").unwrap();
        std::os::unix::fs::symlink("/etc/passwd", proj.join("passwd")).unwrap();

        let spec = format!("{}:work", proj.display());
        let prepared = scan_inputs(&[spec]).unwrap();
        assert_eq!(prepared.inputs.len(), 1);
        assert_eq!(prepared.inputs[0].path, "work");
        assert_eq!(prepared.files, 3);
        assert_eq!(prepared.bytes, 9 + 12 + 12);
        // Two identical files are one blob, plus the manifest and Cargo.toml.
        assert_eq!(prepared.blob_count(), 3);
        assert_eq!(prepared.skipped.len(), 1);
        assert!(prepared.skipped[0].contains("symbolic link"));

        // The same tree scanned again has the same id: nothing would be uploaded twice.
        let again = scan_inputs(&[format!("{}:work", proj.display())]).unwrap();
        assert_eq!(again.inputs, prepared.inputs);
    }

    #[cfg(unix)]
    #[tokio::test]
    async fn open_once_handle_prevents_symlink_swap_between_scan_and_upload() {
        struct RecordingChannel {
            received: std::sync::Mutex<Vec<u8>>,
        }
        impl BlobChannel for RecordingChannel {
            async fn call(&self, request: BlobRequest) -> Result<BlobReply, BlobError> {
                match request {
                    BlobRequest::Missing { ids } => Ok(BlobReply::Missing { ids }),
                    BlobRequest::Write {
                        id,
                        total_size,
                        offset,
                        data,
                    } => {
                        let received = offset + data.len() as u64;
                        self.received
                            .lock()
                            .unwrap()
                            .extend_from_slice(data.as_bytes());
                        Ok(BlobReply::Written {
                            id,
                            received,
                            complete: received >= total_size,
                        })
                    }
                    other => panic!("unexpected {other:?}"),
                }
            }
        }

        let dir = tempfile::tempdir().unwrap();
        let file_path = dir.path().join("input.txt");
        let secret_path = dir.path().join("secret.txt");
        std::fs::write(&file_path, b"original-safe-bytes").unwrap();
        std::fs::write(&secret_path, b"swapped-secret-bytes").unwrap();

        // Direct symlink is refused at scan time when using scan_input_paths_with_limits (MCP).
        let link_path = dir.path().join("direct_link.txt");
        std::os::unix::fs::symlink(&secret_path, &link_path).unwrap();
        let direct_err = scan_input_paths_with_limits(
            &[(link_path, Some("link.txt".to_string()))],
            InputLimits::default(),
        )
        .err()
        .expect("direct symlink must be refused");
        assert!(direct_err.contains("symbolic link"), "{direct_err}");

        // Scan regular file, then swap the path on disk with a symlink to `secret.txt` before uploading.
        let prepared = scan_input_paths_with_limits(
            &[(file_path.clone(), Some("input.txt".to_string()))],
            InputLimits::default(),
        )
        .expect("regular file scan succeeds");
        std::fs::remove_file(&file_path).unwrap();
        std::os::unix::fs::symlink(&secret_path, &file_path).unwrap();

        let channel = RecordingChannel {
            received: std::sync::Mutex::new(Vec::new()),
        };
        let swap_err = upload_inputs(&channel, &prepared)
            .await
            .expect_err("re-opening a swapped-in symlink with O_NOFOLLOW must fail");
        assert!(swap_err.contains("symbolic link"), "{swap_err}");
        assert!(
            channel.received.lock().unwrap().is_empty(),
            "swapped-in symlink target must never be uploaded"
        );

        // Also verify that swapping for another regular file is caught by the (dev, ino, len, mtime) fingerprint check.
        std::fs::remove_file(&file_path).unwrap();
        std::fs::write(&file_path, b"different-regular-file").unwrap();
        let repl_err = upload_inputs(&channel, &prepared)
            .await
            .expect_err("re-opening a replaced regular file must fail fingerprint check");
        assert!(repl_err.contains("changed on disk"), "{repl_err}");
    }

    #[cfg(unix)]
    #[test]
    fn cli_scan_inputs_allows_over_256mib_and_top_level_symlinked_directory() {
        let dir = tempfile::tempdir().unwrap();

        // 1. CLI `scan_inputs` follows a top-level symlinked directory (`--input ./data`).
        let real_data = dir.path().join("real_data");
        std::fs::create_dir_all(&real_data).unwrap();
        std::fs::write(real_data.join("item.txt"), b"hello-cli").unwrap();
        let symlink_data = dir.path().join("data");
        std::os::unix::fs::symlink(&real_data, &symlink_data).unwrap();

        let sym_prepared = scan_inputs(&[format!("{}:data", symlink_data.display())])
            .expect("CLI scan_inputs must allow a top-level symlinked directory");
        assert_eq!(sym_prepared.files, 1);
        assert_eq!(sym_prepared.inputs[0].path, "data");

        // 2. CLI `scan_inputs` allows a single file over 256 MiB (300 MiB sparse file).
        let big_path = dir.path().join("big.bin");
        let big_file = std::fs::File::create(&big_path).unwrap();
        let three_hundred_mib = 300 * 1024 * 1024;
        big_file.set_len(three_hundred_mib).unwrap();
        drop(big_file);

        let big_prepared = scan_inputs(&[format!("{}:big.bin", big_path.display())])
            .expect("CLI scan_inputs must allow files > 256 MiB");
        assert_eq!(big_prepared.files, 1);
        assert_eq!(big_prepared.bytes, three_hundred_mib);
    }

    #[cfg(unix)]
    #[test]
    fn scan_and_upload_1000_files_under_ulimit_n_256() {
        const CHILD_ENV: &str = "OXIDESWARM_TEST_ULIMIT_256_CHILD";
        if std::env::var_os(CHILD_ENV).is_some() {
            let rt = tokio::runtime::Builder::new_current_thread()
                .enable_all()
                .build()
                .unwrap();
            rt.block_on(async {
                struct SinkChannel;
                impl BlobChannel for SinkChannel {
                    async fn call(&self, request: BlobRequest) -> Result<BlobReply, BlobError> {
                        match request {
                            BlobRequest::Missing { ids } => Ok(BlobReply::Missing { ids }),
                            BlobRequest::Write {
                                id,
                                total_size,
                                offset,
                                data,
                            } => {
                                let received = offset + data.len() as u64;
                                Ok(BlobReply::Written {
                                    id,
                                    received,
                                    complete: received >= total_size,
                                })
                            }
                            other => panic!("unexpected {other:?}"),
                        }
                    }
                }

                let dir = tempfile::tempdir().unwrap();
                for i in 0..1_000usize {
                    std::fs::write(
                        dir.path().join(format!("f_{i:04}.txt")),
                        format!("file-content-{i}").as_bytes(),
                    )
                    .unwrap();
                }

                let prepared = scan_input_paths_with_limits(
                    &[(dir.path().to_path_buf(), Some(".".to_string()))],
                    InputLimits::default(),
                )
                .expect("1,000 files must scan under ulimit -n 256");
                assert_eq!(prepared.files, 1_000);

                let report = upload_inputs(&SinkChannel, &prepared)
                    .await
                    .expect("1,000 files must upload under ulimit -n 256");
                // 1,000 distinct file blobs + 1 tree manifest blob
                assert_eq!(report.uploaded, 1_001);
            });
            return;
        }

        let current_exe = std::env::current_exe().unwrap();
        let status = std::process::Command::new("/bin/sh")
            .args([
                "-c",
                "ulimit -n 256 && exec \"$1\" --exact blobs::tests::scan_and_upload_1000_files_under_ulimit_n_256 --nocapture",
                "sh",
            ])
            .arg(&current_exe)
            .env(CHILD_ENV, "1")
            .status()
            .expect("spawn /bin/sh with ulimit -n 256");
        assert!(status.success(), "ulimit -n 256 child process failed");
    }

    #[test]
    fn input_limits_cap_file_count_and_aggregate_bytes() {
        let dir = tempfile::tempdir().unwrap();
        std::fs::write(dir.path().join("a.txt"), b"0123456789").unwrap();
        std::fs::write(dir.path().join("b.txt"), b"0123456789").unwrap();

        let specs = vec![
            (dir.path().join("a.txt"), Some("a.txt".to_string())),
            (dir.path().join("b.txt"), Some("b.txt".to_string())),
        ];

        let err_count = scan_input_paths_with_limits(
            &specs,
            InputLimits {
                max_files: 1,
                max_bytes: 1024,
            },
        )
        .err()
        .expect("exceeding max_files must fail");
        assert!(err_count.contains("limit of 1 files"), "{err_count}");

        let err_bytes = scan_input_paths_with_limits(
            &specs,
            InputLimits {
                max_files: 10,
                max_bytes: 15,
            },
        )
        .err()
        .expect("exceeding max_bytes must fail");
        assert!(err_bytes.contains("limit of 15 bytes"), "{err_bytes}");
    }

    #[cfg(unix)]
    #[test]
    fn downloads_never_write_through_a_symbolic_link() {
        let dir = tempfile::tempdir().unwrap();
        let out = dir.path().join("out");
        let elsewhere = dir.path().join("elsewhere");
        std::fs::create_dir_all(&out).unwrap();
        std::fs::create_dir_all(&elsewhere).unwrap();
        std::os::unix::fs::symlink(&elsewhere, out.join("reports")).unwrap();
        let err = prepare_destination(&out, "reports/summary.txt").unwrap_err();
        assert!(err.contains("symbolic link"), "{err}");
        assert!(prepare_destination(&out, "../escape.txt").is_err());
        let ok = prepare_destination(&out, "a/b/c.txt").unwrap();
        assert_eq!(ok, out.join("a").join("b").join("c.txt"));
        assert!(out.join("a/b").is_dir());
    }
}
