//! Content-addressed blobs: the data plane behind task inputs and output files.
//!
//! A blob is the content of one file, named by its BLAKE3 hash ([`BlobId`]). The Master keeps
//! blobs in a bounded on-disk store, every worker keeps a bounded cache, and a blob that a peer
//! already holds is never sent to it again. Design record:
//! `docs/design/0005-content-addressed-data-plane.md`.
//!
//! * **Directories** travel as a [`TreeManifest`] (relative path -> blob id, size, executable
//!   bit), which is itself stored as a blob, plus the blobs of its files.
//! * **Inputs.** A task names them with [`TaskInput`]s (`Task::inputs`): a file blob or a tree
//!   manifest, and the sandbox path to place it at. The worker fetches the blobs it lacks from the
//!   Master before the run ([`crate::blob_transfer`]).
//! * **Outputs.** A task names the files to bring back with output patterns (`Task::output_globs`,
//!   see [`OutputPattern`]). The worker stores the files that match as blobs, uploads the ones the
//!   Master lacks, and lists them in `TaskResult::outputs` as [`BlobFile`]s.
//!
//! Paths in manifests, inputs and outputs are relative, `/`-separated and canonical
//! ([`validate_blob_path`]). Whoever creates a file from one checks it again on its own platform.

use std::collections::HashSet;
use std::fmt;
use std::io::Read;
use std::path::Path;
use std::str::FromStr;

use serde::{Deserialize, Serialize};

use crate::cluster_auth::{hex_decode, hex_encode};

/// Largest encoded [`TreeManifest`] accepted, in bytes.
pub const MAX_MANIFEST_BYTES: u64 = 64 * 1024 * 1024;
/// Most files one [`TreeManifest`] may list.
pub const MAX_TREE_FILES: usize = 500_000;
/// Most entries in `Task::inputs`.
pub const MAX_TASK_INPUTS: usize = 1024;
/// Most entries in `Task::output_globs`.
pub const MAX_OUTPUT_PATTERNS: usize = 256;
/// Longest blob path or output pattern, in bytes.
pub const MAX_BLOB_PATH_BYTES: usize = 4096;
/// Longest single path segment, in bytes (the common file-name limit).
pub const MAX_PATH_SEGMENT_BYTES: usize = 255;
/// The [`TaskInput::path`] of a tree placed at the sandbox root itself.
pub const SANDBOX_ROOT: &str = ".";

/// First bytes of every encoded [`TreeManifest`].
const MANIFEST_MAGIC: &[u8] = b"oxideswarm-tree/1\n";

/// BLAKE3 hash of a blob's content, its name everywhere. Text form: 64 lowercase hex digits.
#[derive(Clone, Copy, PartialEq, Eq, Hash, PartialOrd, Ord)]
pub struct BlobId(pub [u8; 32]);

impl BlobId {
    /// The id of `data`.
    pub fn of(data: &[u8]) -> Self {
        Self(*blake3::hash(data).as_bytes())
    }

    /// The 64-digit lowercase hex form.
    pub fn to_hex(&self) -> String {
        hex_encode(&self.0)
    }

    /// The first 12 hex digits, enough to tell blobs apart in a log line.
    pub fn short(&self) -> String {
        let mut hex = self.to_hex();
        hex.truncate(12);
        hex
    }

    /// Parses the 64-digit hex form (either case).
    pub fn from_hex(text: &str) -> Result<Self, String> {
        let text = text.trim();
        match hex_decode(text) {
            Some(bytes) if bytes.len() == 32 => {
                let mut id = [0u8; 32];
                id.copy_from_slice(&bytes);
                Ok(Self(id))
            }
            _ => Err(format!(
                "'{text}' is not a blob id (64 hexadecimal digits expected)"
            )),
        }
    }
}

impl fmt::Display for BlobId {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.write_str(&self.to_hex())
    }
}

impl fmt::Debug for BlobId {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(f, "BlobId({})", self.short())
    }
}

impl FromStr for BlobId {
    type Err = String;

    fn from_str(s: &str) -> Result<Self, Self::Err> {
        Self::from_hex(s)
    }
}

// JSON carries the hex form, bincode the 32 raw bytes.
impl Serialize for BlobId {
    fn serialize<S: serde::Serializer>(&self, serializer: S) -> Result<S::Ok, S::Error> {
        if serializer.is_human_readable() {
            serializer.serialize_str(&self.to_hex())
        } else {
            self.0.serialize(serializer)
        }
    }
}

impl<'de> Deserialize<'de> for BlobId {
    fn deserialize<D: serde::Deserializer<'de>>(deserializer: D) -> Result<Self, D::Error> {
        if deserializer.is_human_readable() {
            let text = String::deserialize(deserializer)?;
            Self::from_hex(&text).map_err(serde::de::Error::custom)
        } else {
            <[u8; 32]>::deserialize(deserializer).map(Self)
        }
    }
}

/// Incremental BLAKE3 hashing of a blob that arrives in pieces.
#[derive(Default, Clone)]
pub struct BlobHasher(blake3::Hasher);

impl BlobHasher {
    pub fn new() -> Self {
        Self::default()
    }

    pub fn update(&mut self, data: &[u8]) {
        self.0.update(data);
    }

    /// The id of everything passed to [`BlobHasher::update`] so far.
    pub fn finish(&self) -> BlobId {
        BlobId(*self.0.finalize().as_bytes())
    }
}

/// Hashes everything `reader` yields: its blob id and its length.
pub fn hash_reader<R: Read>(mut reader: R) -> std::io::Result<(BlobId, u64)> {
    let mut hasher = BlobHasher::new();
    let mut buf = vec![0u8; 256 * 1024];
    let mut len = 0u64;
    loop {
        let n = match reader.read(&mut buf) {
            Ok(0) => break,
            Ok(n) => n,
            Err(e) if e.kind() == std::io::ErrorKind::Interrupted => continue,
            Err(e) => return Err(e),
        };
        hasher.update(&buf[..n]);
        len += n as u64;
    }
    Ok((hasher.finish(), len))
}

/// Hashes the file at `path`: its blob id and its length.
pub fn hash_file(path: &Path) -> std::io::Result<(BlobId, u64)> {
    hash_reader(std::fs::File::open(path)?)
}

/// Checks that `path` is a canonical relative blob path: `/`-separated segments that are not
/// empty, `.` or `..`, and no backslash, colon or NUL anywhere. Rules that only one platform has
/// (Windows device names such as `CON`) are checked where the file is created, not here, so a
/// Unix project with a `src/aux.rs` can still be sent to a Unix worker.
pub fn validate_blob_path(path: &str) -> Result<(), String> {
    if path.is_empty() {
        return Err("a path cannot be empty".to_string());
    }
    if path.len() > MAX_BLOB_PATH_BYTES {
        return Err(format!(
            "path '{}…' is longer than {MAX_BLOB_PATH_BYTES} bytes",
            truncate_for_message(path)
        ));
    }
    if path.starts_with('/') {
        return Err(format!("path '{path}' is absolute; it must be relative"));
    }
    if let Some(c) = path.chars().find(|c| matches!(c, '\\' | ':' | '\0')) {
        return Err(format!(
            "path '{}' contains {:?}, which is not allowed in a relative path",
            path.escape_debug(),
            c
        ));
    }
    for segment in path.split('/') {
        match segment {
            "" => return Err(format!("path '{path}' has an empty segment")),
            "." | ".." => {
                return Err(format!(
                    "path '{path}' contains '{segment}'; it must be canonical and relative"
                ))
            }
            s if s.len() > MAX_PATH_SEGMENT_BYTES => {
                return Err(format!(
                    "path '{path}' has a segment longer than {MAX_PATH_SEGMENT_BYTES} bytes"
                ))
            }
            _ => {}
        }
    }
    Ok(())
}

fn truncate_for_message(text: &str) -> &str {
    let mut end = text.len().min(64);
    while !text.is_char_boundary(end) {
        end -= 1;
    }
    &text[..end]
}

/// Joins a [`TaskInput::path`] and a path inside a tree into one sandbox-relative path.
pub fn join_blob_path(base: &str, path: &str) -> String {
    if base == SANDBOX_ROOT {
        path.to_string()
    } else {
        format!("{base}/{path}")
    }
}

/// One file of a [`TreeManifest`] or of a task's outputs.
#[derive(Debug, Clone, PartialEq, Eq, Hash, Serialize, Deserialize)]
pub struct BlobFile {
    /// Relative, canonical path ([`validate_blob_path`]).
    pub path: String,
    /// The file's content.
    pub id: BlobId,
    /// Length of the content in bytes.
    pub size: u64,
    /// Whether the file is created executable (mode 0755 instead of 0644 on Unix).
    #[serde(default)]
    pub executable: bool,
}

/// A directory tree as a list of its regular files, sorted by path. Directories exist only as
/// the parents of files (an empty directory is not represented) and symbolic links are not
/// represented at all.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct TreeManifest {
    files: Vec<BlobFile>,
}

impl TreeManifest {
    /// Builds a manifest from `files` in any order. Fails on an invalid or duplicate path, on a
    /// file that is also the parent directory of another file, and on more than
    /// [`MAX_TREE_FILES`] files.
    pub fn new(mut files: Vec<BlobFile>) -> Result<Self, String> {
        files.sort_by(|a, b| a.path.cmp(&b.path));
        let manifest = Self { files };
        manifest.validate()?;
        Ok(manifest)
    }

    pub fn files(&self) -> &[BlobFile] {
        &self.files
    }

    pub fn into_files(self) -> Vec<BlobFile> {
        self.files
    }

    /// Total size of the files, in bytes.
    pub fn total_size(&self) -> u64 {
        self.files.iter().map(|f| f.size).sum()
    }

    /// The canonical encoding (a fixed header, then bincode), which is what the manifest's blob
    /// id names.
    pub fn encode(&self) -> Vec<u8> {
        let mut out = MANIFEST_MAGIC.to_vec();
        bincode::serialize_into(&mut out, &self.files)
            .expect("serializing a manifest into memory cannot fail");
        out
    }

    /// The blob id of [`TreeManifest::encode`].
    pub fn id(&self) -> BlobId {
        BlobId::of(&self.encode())
    }

    /// Reads an encoded manifest and checks it like [`TreeManifest::new`] does, and that it is
    /// in canonical order. Anything that arrived over the network goes through here.
    pub fn decode(bytes: &[u8]) -> Result<Self, String> {
        if bytes.len() as u64 > MAX_MANIFEST_BYTES {
            return Err(format!(
                "manifest of {} bytes exceeds the limit of {MAX_MANIFEST_BYTES} bytes",
                bytes.len()
            ));
        }
        let body = bytes
            .strip_prefix(MANIFEST_MAGIC)
            .ok_or_else(|| "not a directory manifest (unknown header)".to_string())?;
        use bincode::Options;
        let files: Vec<BlobFile> = bincode::DefaultOptions::new()
            .with_fixint_encoding()
            .with_limit(body.len() as u64)
            .deserialize(body)
            .map_err(|e| format!("corrupt directory manifest: {e}"))?;
        if files.windows(2).any(|w| w[0].path >= w[1].path) {
            return Err("directory manifest is not sorted by path".to_string());
        }
        let manifest = Self { files };
        manifest.validate()?;
        Ok(manifest)
    }

    fn validate(&self) -> Result<(), String> {
        if self.files.len() > MAX_TREE_FILES {
            return Err(format!(
                "directory has {} files; at most {MAX_TREE_FILES} can be sent",
                self.files.len()
            ));
        }
        let mut paths = HashSet::with_capacity(self.files.len());
        for file in &self.files {
            validate_blob_path(&file.path)?;
            if !paths.insert(file.path.as_str()) {
                return Err(format!("path '{}' is listed twice", file.path));
            }
        }
        for file in &self.files {
            for (pos, _) in file.path.match_indices('/') {
                let parent = &file.path[..pos];
                if paths.contains(parent) {
                    return Err(format!(
                        "'{parent}' is listed as a file and as the directory of '{}'",
                        file.path
                    ));
                }
            }
        }
        Ok(())
    }
}

/// Where a [`TaskInput`] comes from.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub enum InputSource {
    /// One file: blob `id` of `size` bytes.
    File {
        id: BlobId,
        size: u64,
        executable: bool,
    },
    /// A directory: the [`TreeManifest`] stored as blob `manifest`, `manifest_size` bytes long.
    Tree {
        manifest: BlobId,
        manifest_size: u64,
    },
}

/// Content placed into the task sandbox before the run.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct TaskInput {
    /// Where it goes, relative to the sandbox: the file's own path, or the directory a tree's
    /// files are placed in ([`SANDBOX_ROOT`] for the sandbox itself).
    pub path: String,
    pub source: InputSource,
}

impl TaskInput {
    pub fn file(path: impl Into<String>, id: BlobId, size: u64, executable: bool) -> Self {
        Self {
            path: path.into(),
            source: InputSource::File {
                id,
                size,
                executable,
            },
        }
    }

    pub fn tree(path: impl Into<String>, manifest: BlobId, manifest_size: u64) -> Self {
        Self {
            path: path.into(),
            source: InputSource::Tree {
                manifest,
                manifest_size,
            },
        }
    }

    /// The blob this input names directly: the file, or the tree's manifest.
    pub fn blob_id(&self) -> BlobId {
        match self.source {
            InputSource::File { id, .. } => id,
            InputSource::Tree { manifest, .. } => manifest,
        }
    }

    /// Checks the destination path ([`SANDBOX_ROOT`] is only allowed for a tree).
    pub fn validate(&self) -> Result<(), String> {
        let is_tree = matches!(self.source, InputSource::Tree { .. });
        if is_tree && self.path == SANDBOX_ROOT {
            return Ok(());
        }
        validate_blob_path(&self.path).map_err(|e| format!("input destination: {e}"))
    }
}

/// Checks a task's inputs: at most [`MAX_TASK_INPUTS`], valid destinations, no destination used
/// twice.
pub fn validate_task_inputs(inputs: &[TaskInput]) -> Result<(), String> {
    if inputs.len() > MAX_TASK_INPUTS {
        return Err(format!(
            "a task can have at most {MAX_TASK_INPUTS} inputs, this one has {}",
            inputs.len()
        ));
    }
    let mut seen = HashSet::with_capacity(inputs.len());
    for input in inputs {
        input.validate()?;
        if !seen.insert(input.path.as_str()) {
            return Err(format!("input destination '{}' is used twice", input.path));
        }
    }
    Ok(())
}

/// One segment of an [`OutputPattern`].
#[derive(Debug, Clone, PartialEq, Eq)]
enum PatternSegment {
    /// Matches exactly this name.
    Literal(String),
    /// A name with `*` (any run of characters, possibly empty) and `?` (one character).
    Wildcard(String),
    /// `**`: any number of directories, including none.
    AnyDepth,
}

/// A sandbox-relative pattern naming output files (`Task::output_globs`).
///
/// Segments are separated by `/` (a `\` counts as `/`). In a segment `*` matches any run of
/// characters and `?` one character, `/` excepted; a segment that is exactly `**` matches any
/// number of directories. `*` also matches names that start with a dot. A pattern selects every
/// file it matches **and every file inside a directory it matches**, so `target/release` brings
/// back that whole directory and `logs/*.txt` only the text files directly in `logs`.
///
/// Patterns are relative: a leading `/`, a drive letter or `:`, NUL and `..` are refused.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct OutputPattern {
    text: String,
    segments: Vec<PatternSegment>,
}

impl OutputPattern {
    pub fn parse(pattern: &str) -> Result<Self, String> {
        let text = pattern.trim();
        if text.is_empty() {
            return Err("an output pattern cannot be empty".to_string());
        }
        if text.len() > MAX_BLOB_PATH_BYTES {
            return Err(format!(
                "output pattern '{}…' is longer than {MAX_BLOB_PATH_BYTES} bytes",
                truncate_for_message(text)
            ));
        }
        if text.contains('\0') {
            return Err("an output pattern cannot contain NUL".to_string());
        }
        let normalized = text.replace('\\', "/");
        if normalized.starts_with('/') {
            return Err(format!(
                "output pattern '{text}' is absolute; it must be relative to the task sandbox"
            ));
        }
        if normalized.contains(':') {
            return Err(format!(
                "output pattern '{text}' contains ':' (drive letters are not allowed)"
            ));
        }
        let mut segments = Vec::new();
        for raw in normalized.split('/') {
            match raw {
                "" | "." => continue,
                ".." => {
                    return Err(format!(
                        "output pattern '{text}' contains '..'; it must stay in the sandbox"
                    ))
                }
                "**" => {
                    // `**/**` matches what one `**` matches.
                    if segments.last() != Some(&PatternSegment::AnyDepth) {
                        segments.push(PatternSegment::AnyDepth);
                    }
                }
                s if s.contains(['*', '?']) => segments.push(PatternSegment::Wildcard(s.into())),
                s => segments.push(PatternSegment::Literal(s.into())),
            }
        }
        if segments.is_empty() {
            return Err(format!(
                "output pattern '{text}' names the sandbox itself; name the files or directories to bring back"
            ));
        }
        Ok(Self {
            text: text.to_string(),
            segments,
        })
    }

    /// The pattern as given.
    pub fn as_str(&self) -> &str {
        &self.text
    }

    /// True when the pattern has no wildcard: it names one file or directory.
    pub fn is_literal(&self) -> bool {
        self.segments
            .iter()
            .all(|s| matches!(s, PatternSegment::Literal(_)))
    }

    /// The leading segments without wildcards, joined with `/` (empty when the first segment has
    /// one): the directory below which every selected file lies.
    pub fn literal_prefix(&self) -> String {
        let mut prefix = Vec::new();
        for segment in &self.segments {
            match segment {
                PatternSegment::Literal(name) => prefix.push(name.as_str()),
                _ => break,
            }
        }
        prefix.join("/")
    }

    /// Whether the canonical relative file path `path` is selected: matched by the pattern
    /// itself, or inside a directory the pattern matches.
    pub fn selects(&self, path: &str) -> bool {
        let parts: Vec<&str> = path.split('/').collect();
        (1..=parts.len()).any(|len| match_segments(&self.segments, &parts[..len]))
    }
}

impl fmt::Display for OutputPattern {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.write_str(&self.text)
    }
}

/// Checks a task's output patterns: at most [`MAX_OUTPUT_PATTERNS`], each one valid.
pub fn validate_output_patterns(patterns: &[String]) -> Result<(), String> {
    if patterns.len() > MAX_OUTPUT_PATTERNS {
        return Err(format!(
            "a task can have at most {MAX_OUTPUT_PATTERNS} output patterns, this one has {}",
            patterns.len()
        ));
    }
    for pattern in patterns {
        OutputPattern::parse(pattern)?;
    }
    Ok(())
}

fn match_segments(pattern: &[PatternSegment], path: &[&str]) -> bool {
    match pattern.split_first() {
        None => path.is_empty(),
        Some((PatternSegment::AnyDepth, rest)) => {
            (0..=path.len()).any(|skip| match_segments(rest, &path[skip..]))
        }
        Some((segment, rest)) => match path.split_first() {
            Some((name, tail)) => match_segment(segment, name) && match_segments(rest, tail),
            None => false,
        },
    }
}

fn match_segment(segment: &PatternSegment, name: &str) -> bool {
    match segment {
        PatternSegment::Literal(literal) => literal == name,
        PatternSegment::Wildcard(wildcard) => wildcard_match(wildcard, name),
        PatternSegment::AnyDepth => true,
    }
}

/// `*` / `?` matching of one name, with the usual single backtracking point.
fn wildcard_match(pattern: &str, name: &str) -> bool {
    let pattern: Vec<char> = pattern.chars().collect();
    let name: Vec<char> = name.chars().collect();
    let (mut p, mut n) = (0, 0);
    let mut star: Option<(usize, usize)> = None;
    while n < name.len() {
        if p < pattern.len() && (pattern[p] == '?' || pattern[p] == name[n]) {
            p += 1;
            n += 1;
        } else if p < pattern.len() && pattern[p] == '*' {
            star = Some((p, n));
            p += 1;
        } else if let Some((star_p, star_n)) = star {
            p = star_p + 1;
            n = star_n + 1;
            star = Some((star_p, star_n + 1));
        } else {
            return false;
        }
    }
    pattern[p..].iter().all(|&c| c == '*')
}

#[cfg(test)]
mod tests {
    use super::*;

    fn file(path: &str, content: &[u8]) -> BlobFile {
        BlobFile {
            path: path.to_string(),
            id: BlobId::of(content),
            size: content.len() as u64,
            executable: false,
        }
    }

    #[test]
    fn blob_id_is_blake3_and_round_trips_through_hex() {
        let id = BlobId::of(b"abc");
        assert_eq!(
            id.to_hex(),
            "6437b3ac38465133ffb63b75273a8db548c558465d79db03fd359c6cd5bd9d85",
            "BLAKE3 test vector for \"abc\""
        );
        assert_eq!(BlobId::from_hex(&id.to_hex()).unwrap(), id);
        assert_eq!(BlobId::from_hex(&id.to_hex().to_uppercase()).unwrap(), id);
        assert_eq!(id.short().len(), 12);
        assert!(BlobId::from_hex("abcd").is_err());
        assert!(BlobId::from_hex(&"g".repeat(64)).is_err());
    }

    #[test]
    fn blob_id_is_hex_in_json_and_32_raw_bytes_in_bincode() {
        let id = BlobId::of(b"payload");
        let json = serde_json::to_string(&id).unwrap();
        assert_eq!(json, format!("\"{}\"", id.to_hex()));
        assert_eq!(serde_json::from_str::<BlobId>(&json).unwrap(), id);

        let bin = bincode::serialize(&id).unwrap();
        assert_eq!(bin.len(), 32);
        assert_eq!(bincode::deserialize::<BlobId>(&bin).unwrap(), id);
    }

    #[test]
    fn streaming_hash_matches_one_shot_hash() {
        let data: Vec<u8> = (0..1_000_000u32).map(|i| (i % 251) as u8).collect();
        let (id, len) = hash_reader(std::io::Cursor::new(&data)).unwrap();
        assert_eq!(id, BlobId::of(&data));
        assert_eq!(len, data.len() as u64);

        let mut hasher = BlobHasher::new();
        for piece in data.chunks(7919) {
            hasher.update(piece);
        }
        assert_eq!(hasher.finish(), id);
    }

    #[test]
    fn blob_paths_must_be_canonical_and_relative() {
        for good in [
            "a",
            "src/main.rs",
            "dir/sub/file.tar.gz",
            ".hidden",
            "src/aux.rs",
        ] {
            assert!(validate_blob_path(good).is_ok(), "{good}");
        }
        for bad in [
            "",
            "/etc/passwd",
            "a//b",
            "a/",
            "./a",
            "a/./b",
            "../a",
            "a/../b",
            "a\\b",
            "C:/x",
            "a:b",
            "nul\0byte",
        ] {
            assert!(validate_blob_path(bad).is_err(), "{bad:?} must be refused");
        }
        assert!(validate_blob_path(&"x".repeat(MAX_PATH_SEGMENT_BYTES + 1)).is_err());
        let long = vec!["seg"; MAX_BLOB_PATH_BYTES / 4 + 1].join("/");
        assert!(validate_blob_path(&long).is_err());
    }

    #[test]
    fn manifest_round_trips_and_its_id_names_the_canonical_encoding() {
        let a = TreeManifest::new(vec![
            file("src/main.rs", b"fn main() {}"),
            file("Cargo.toml", b"[package]"),
            file("src/lib/mod.rs", b""),
        ])
        .unwrap();
        // Input order does not matter: the manifest is sorted, so its id is stable.
        let b = TreeManifest::new(vec![
            file("src/lib/mod.rs", b""),
            file("src/main.rs", b"fn main() {}"),
            file("Cargo.toml", b"[package]"),
        ])
        .unwrap();
        assert_eq!(a, b);
        assert_eq!(a.id(), b.id());
        assert_eq!(a.files()[0].path, "Cargo.toml");
        assert_eq!(a.total_size(), 12 + 9);

        let decoded = TreeManifest::decode(&a.encode()).unwrap();
        assert_eq!(decoded, a);

        let changed = TreeManifest::new(vec![
            file("src/main.rs", b"fn main() { }"),
            file("Cargo.toml", b"[package]"),
            file("src/lib/mod.rs", b""),
        ])
        .unwrap();
        assert_ne!(changed.id(), a.id(), "a changed file changes the tree id");
    }

    #[test]
    fn manifest_refuses_duplicates_escapes_and_file_directory_clashes() {
        assert!(TreeManifest::new(vec![file("a", b"1"), file("a", b"2")]).is_err());
        assert!(TreeManifest::new(vec![file("../a", b"1")]).is_err());
        assert!(TreeManifest::new(vec![file("/a", b"1")]).is_err());
        let clash = TreeManifest::new(vec![file("a", b"1"), file("a/b", b"2")]).unwrap_err();
        assert!(clash.contains("directory"), "{clash}");
        // `a.txt` sorts between `a` and `a/b`; the clash must still be found.
        assert!(TreeManifest::new(vec![
            file("a", b"1"),
            file("a.txt", b"3"),
            file("a/b", b"2")
        ])
        .is_err());
        assert!(TreeManifest::new(vec![file("a.txt", b"3"), file("a/b", b"2")]).is_ok());
    }

    #[test]
    fn manifest_decode_refuses_tampered_input() {
        let manifest = TreeManifest::new(vec![file("x", b"1"), file("y", b"2")]).unwrap();
        let encoded = manifest.encode();

        assert!(TreeManifest::decode(b"not a manifest").is_err());
        assert!(TreeManifest::decode(&encoded[..encoded.len() - 3]).is_err());

        // Unsorted lists are refused, so one tree has exactly one id.
        let mut unsorted = MANIFEST_MAGIC.to_vec();
        bincode::serialize_into(
            &mut unsorted,
            &vec![manifest.files()[1].clone(), manifest.files()[0].clone()],
        )
        .unwrap();
        assert!(TreeManifest::decode(&unsorted)
            .unwrap_err()
            .contains("sorted"));

        // A path that escapes is refused even when it arrives encoded.
        let mut escaping = MANIFEST_MAGIC.to_vec();
        bincode::serialize_into(&mut escaping, &vec![file("../../etc/passwd", b"x")]).unwrap();
        assert!(TreeManifest::decode(&escaping).is_err());
    }

    #[test]
    fn inputs_validate_destinations() {
        let id = BlobId::of(b"x");
        assert!(TaskInput::file("data/in.csv", id, 1, false)
            .validate()
            .is_ok());
        assert!(TaskInput::tree(SANDBOX_ROOT, id, 1).validate().is_ok());
        assert!(TaskInput::tree("project", id, 1).validate().is_ok());
        assert!(
            TaskInput::file(SANDBOX_ROOT, id, 1, false)
                .validate()
                .is_err(),
            "a file cannot be the sandbox root"
        );
        assert!(TaskInput::file("../x", id, 1, false).validate().is_err());
        assert!(TaskInput::tree("/abs", id, 1).validate().is_err());

        let dup = vec![
            TaskInput::file("a", id, 1, false),
            TaskInput::tree("a", id, 1),
        ];
        assert!(validate_task_inputs(&dup).unwrap_err().contains("twice"));
        assert_eq!(join_blob_path(SANDBOX_ROOT, "x/y"), "x/y");
        assert_eq!(join_blob_path("proj", "x/y"), "proj/x/y");
    }

    #[test]
    fn output_patterns_select_files_and_whole_directories() {
        let p = OutputPattern::parse("out.txt").unwrap();
        assert!(p.is_literal());
        assert!(p.selects("out.txt"));
        assert!(!p.selects("out.txt.bak"));
        assert!(!p.selects("sub/out.txt"));

        let dir = OutputPattern::parse("target/release/").unwrap();
        assert!(dir.selects("target/release/app"));
        assert!(dir.selects("target/release/deps/libx.rlib"));
        assert!(!dir.selects("target/debug/app"));
        assert_eq!(dir.literal_prefix(), "target/release");

        let star = OutputPattern::parse("logs/*.txt").unwrap();
        assert!(!star.is_literal());
        assert_eq!(star.literal_prefix(), "logs");
        assert!(star.selects("logs/a.txt"));
        assert!(star.selects("logs/.hidden.txt"));
        assert!(!star.selects("logs/a.log"));
        assert!(!star.selects("logs/sub/a.txt"), "* does not cross '/'");

        let deep = OutputPattern::parse("**/*.rlib").unwrap();
        assert_eq!(deep.literal_prefix(), "");
        assert!(deep.selects("x.rlib"));
        assert!(deep.selects("target/debug/deps/libfoo.rlib"));
        assert!(!deep.selects("target/debug/libfoo.so"));

        let mid = OutputPattern::parse("a/**/b?.bin").unwrap();
        assert!(mid.selects("a/b1.bin"));
        assert!(mid.selects("a/x/y/b2.bin"));
        assert!(!mid.selects("a/x/b10.bin"));

        let win = OutputPattern::parse("out\\*.csv").unwrap();
        assert!(win.selects("out/r.csv"));
    }

    #[test]
    fn output_patterns_refuse_escapes() {
        for bad in [
            "",
            "   ",
            "/etc/*",
            "\\abs",
            "../x",
            "a/../../x",
            "C:\\x",
            "c:x",
            ".",
            "./",
            "a\0b",
        ] {
            assert!(
                OutputPattern::parse(bad).is_err(),
                "{bad:?} must be refused"
            );
        }
        let too_many: Vec<String> = (0..=MAX_OUTPUT_PATTERNS).map(|i| format!("f{i}")).collect();
        assert!(validate_output_patterns(&too_many).is_err());
        assert!(validate_output_patterns(&["a".into(), "b/**".into()]).is_ok());
    }

    #[test]
    fn wildcard_matching_backtracks_correctly() {
        assert!(wildcard_match("*", ""));
        assert!(wildcard_match("a*b*c", "aXbYbZc"));
        assert!(!wildcard_match("a*b*c", "aXbYbZ"));
        assert!(wildcard_match("*.tar.gz", "x.tar.gz"));
        assert!(wildcard_match("??.rs", "ab.rs"));
        assert!(!wildcard_match("??.rs", "abc.rs"));
        assert!(wildcard_match("ü*", "über"), "matching is per character");
    }
}
