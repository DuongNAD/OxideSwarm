//! The client side of task input and output files (`submit --input`, `--output`,
//! `--output-dir`): hashing local files and directories into blobs, uploading the blobs the Master
//! lacks, and downloading a task's output files. Design record:
//! `docs/design/0005-content-addressed-data-plane.md`.
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
use std::path::{Component, Path, PathBuf};

use tokio::net::TcpStream;
use tokio::sync::Mutex;

use rusty_grid_core::blob::{
    hash_file, validate_blob_path, validate_task_inputs, BlobFile, BlobId, TaskInput, TreeManifest,
    SANDBOX_ROOT,
};
use rusty_grid_core::blob_transfer::{
    download_blob, for_each_concurrently, missing_blobs, upload_blob, BlobChannel, BlobError,
    BlobReply, BlobRequest,
};
use rusty_grid_core::cluster_auth::ClusterSecret;
use rusty_grid_core::error::GridResult;
use rusty_grid_core::protocol::{ClientMessage, ClientResponse, MessageTransport};

use crate::connect::connect_master;

/// Where the content of a blob to upload comes from.
enum LocalSource {
    File(PathBuf),
    Bytes(Vec<u8>),
}

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
pub fn scan_inputs(specs: &[String]) -> Result<PreparedInputs, String> {
    let mut prepared = PreparedInputs::default();
    for spec in specs {
        let (local, dest) = parse_input_spec(spec)?;
        let meta =
            std::fs::metadata(&local).map_err(|e| format!("--input {}: {e}", local.display()))?;
        let dest = match dest {
            Some(dest) => normalize_destination(&dest, meta.is_dir())?,
            None => default_destination(&local, meta.is_dir())?,
        };
        if meta.is_file() {
            let (id, size) =
                hash_file(&local).map_err(|e| format!("--input {}: {e}", local.display()))?;
            prepared
                .inputs
                .push(TaskInput::file(&dest, id, size, is_executable(&meta)));
            prepared.add_file(id, size, local);
        } else if meta.is_dir() {
            let files = prepared.scan_directory(&local)?;
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
    fn add_file(&mut self, id: BlobId, size: u64, path: PathBuf) {
        self.files += 1;
        self.bytes += size;
        self.blobs.entry(id).or_insert(LocalBlob {
            size,
            source: LocalSource::File(path),
        });
    }

    /// The regular files below `root`, with paths relative to it.
    fn scan_directory(&mut self, root: &Path) -> Result<Vec<BlobFile>, String> {
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
                    let meta = entry
                        .metadata()
                        .map_err(|e| format!("--input {}: {e}", path.display()))?;
                    let (id, size) =
                        hash_file(&path).map_err(|e| format!("--input {}: {e}", path.display()))?;
                    files.push(BlobFile {
                        path: rel,
                        id,
                        size,
                        executable: is_executable(&meta),
                    });
                    self.add_file(id, size, path);
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
    let bytes = for_each_concurrently(missing, |id| async move {
        let blob = &prepared.blobs[&id];
        match &blob.source {
            LocalSource::File(path) => {
                let file = tokio::fs::File::open(path).await?;
                upload_blob(channel, id, blob.size, file).await
            }
            LocalSource::Bytes(bytes) => upload_blob(channel, id, blob.size, &bytes[..]).await,
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
fn prepare_destination(root: &Path, rel: &str) -> Result<PathBuf, String> {
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
    if std::fs::symlink_metadata(&target).is_ok_and(|m| m.is_dir()) {
        return Err(format!(
            "output '{rel}': '{}' is a directory",
            target.display()
        ));
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
    let jobs: Vec<(&BlobFile, &PathBuf)> = outputs.iter().zip(targets.iter()).collect();
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
            tokio::fs::rename(&tmp, target).await?;
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
