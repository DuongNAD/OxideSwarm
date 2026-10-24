//! Persistent worker identity.
//!
//! A worker that picks a fresh UUID on every start shows up as a brand-new machine after each
//! restart, reboot or service respawn: the old entry lingers as a ghost, per-machine history is
//! lost and nobody can address "the GPU PC" by a stable id. The id is therefore stored in a small
//! file and reused.
//!
//! * Created with mode `0600` (the id is an identity, not a secret, but it is also not for other
//!   local users to rewrite), inside a `0700` directory that is created on demand.
//! * Race-safe: several processes starting at once agree on one id, because the file is created
//!   with `O_EXCL` semantics and every loser reads what the winner wrote.
//! * Never fatal: if the file cannot be read or written the worker warns and carries on with an id
//!   that lives only as long as the process.

use std::io::Write;
use std::path::{Path, PathBuf};
use std::time::Duration;

use tracing::{info, warn};
use uuid::Uuid;

/// Environment variable naming the id file (takes precedence over the default location).
pub const WORKER_ID_FILE_ENV: &str = "RUSTY_GRID_WORKER_ID_FILE";

/// How long a reader waits for a concurrent writer to finish writing the id.
const READ_RETRIES: usize = 40;
const READ_RETRY_DELAY: Duration = Duration::from_millis(25);

/// The user's home directory (`HOME`, or `USERPROFILE` on Windows).
fn home_dir() -> Option<PathBuf> {
    ["HOME", "USERPROFILE"]
        .iter()
        .filter_map(std::env::var_os)
        .find(|v| !v.is_empty())
        .map(PathBuf::from)
}

/// Whether a worker should keep its id across restarts.
///
/// Only a worker that is told which machine it is: an explicit `name` (`--name`,
/// `RUSTY_GRID_WORKER_NAME` or the config file; its id lives in `worker_id.<name>`), or an id file
/// named in `RUSTY_GRID_WORKER_ID_FILE`. An unnamed worker would use `~/.oxideswarm/worker_id`,
/// shared by every unnamed worker under the same home directory (two terminals, an NFS home, a
/// copied `~/.oxideswarm`); the master takes the same id for one machine that reconnected and
/// closes the live session of the first, so they would kick each other forever. Those get a fresh
/// id per process instead.
pub fn persistence_wanted(name: Option<&str>, id_file_env: Option<&std::ffi::OsStr>) -> bool {
    name.is_some_and(|n| !sanitize_name(n).is_empty()) || id_file_env.is_some_and(|p| !p.is_empty())
}

/// Reduces a worker name to a file-name-safe suffix (`[A-Za-z0-9._-]`, at most 64 bytes).
fn sanitize_name(name: &str) -> String {
    name.trim()
        .chars()
        .map(|c| {
            if c.is_ascii_alphanumeric() || matches!(c, '.' | '_' | '-') {
                c
            } else {
                '_'
            }
        })
        .take(64)
        .collect()
}

/// Where the id of a worker is stored when nothing says otherwise.
///
/// `RUSTY_GRID_WORKER_ID_FILE` wins if set. Otherwise it is `~/.oxideswarm/worker_id`; a worker
/// started with an explicit `name` gets `~/.oxideswarm/worker_id.<name>`, so that several named
/// workers on one machine (a local test cluster, a GPU and a CPU worker side by side) each keep
/// their own identity instead of all collapsing into one. Returns `None` if there is neither the
/// variable nor a home directory.
pub fn default_worker_id_path(name: Option<&str>) -> Option<PathBuf> {
    if let Some(path) = std::env::var_os(WORKER_ID_FILE_ENV).filter(|p| !p.is_empty()) {
        return Some(PathBuf::from(path));
    }
    let dir = if let Some(custom) = std::env::var_os("OXIDESWARM_HOME").filter(|s| !s.is_empty()) {
        PathBuf::from(custom)
    } else {
        home_dir()?.join(".oxideswarm")
    };
    let file = match name.map(sanitize_name).filter(|n| !n.is_empty()) {
        Some(n) => format!("worker_id.{n}"),
        None => "worker_id".to_string(),
    };
    Some(dir.join(file))
}

fn parse_id(text: &str) -> Option<Uuid> {
    Uuid::parse_str(text.trim()).ok().filter(|id| !id.is_nil())
}

/// Reads the id stored at `path`, waiting briefly for a concurrent creator to finish writing it.
fn read_id(path: &Path) -> Option<Uuid> {
    for attempt in 0..READ_RETRIES {
        match std::fs::read_to_string(path) {
            Ok(text) => {
                if let Some(id) = parse_id(&text) {
                    return Some(id);
                }
            }
            Err(e) if e.kind() == std::io::ErrorKind::NotFound => return None,
            Err(_) => return None,
        }
        if attempt + 1 < READ_RETRIES {
            std::thread::sleep(READ_RETRY_DELAY);
        }
    }
    None
}

#[cfg(unix)]
fn create_exclusive(path: &Path) -> std::io::Result<std::fs::File> {
    use std::os::unix::fs::OpenOptionsExt;
    std::fs::OpenOptions::new()
        .write(true)
        .create_new(true)
        .mode(0o600)
        .open(path)
}

#[cfg(not(unix))]
fn create_exclusive(path: &Path) -> std::io::Result<std::fs::File> {
    std::fs::OpenOptions::new()
        .write(true)
        .create_new(true)
        .open(path)
}

fn create_parent_dir(path: &Path) -> std::io::Result<()> {
    let Some(parent) = path.parent().filter(|p| !p.as_os_str().is_empty()) else {
        return Ok(());
    };
    if parent.is_dir() {
        return Ok(());
    }
    std::fs::create_dir_all(parent)?;
    #[cfg(unix)]
    {
        use std::os::unix::fs::PermissionsExt;
        // Best effort: only the directory this call created is ours to tighten, and only when it
        // is the dedicated `.oxideswarm` directory (never a shared parent such as /tmp).
        if parent.file_name().is_some_and(|n| n == ".oxideswarm") {
            let _ = std::fs::set_permissions(parent, std::fs::Permissions::from_mode(0o700));
        }
    }
    Ok(())
}

/// Replaces a corrupt id file atomically (write a temp file, rename over it).
fn replace_corrupt(path: &Path, id: Uuid) -> std::io::Result<()> {
    let tmp = path.with_extension(format!("tmp{}", std::process::id()));
    let mut file = create_exclusive(&tmp)?;
    let written = file
        .write_all(id.to_string().as_bytes())
        .and_then(|_| file.sync_all());
    drop(file);
    if let Err(e) = written.and_then(|_| std::fs::rename(&tmp, path)) {
        let _ = std::fs::remove_file(&tmp);
        return Err(e);
    }
    Ok(())
}

/// Loads the worker id stored at `path`, creating it (with a fresh random id) if it does not
/// exist. Returns `None` if the file can neither be read nor written; the caller then warns and
/// falls back to an id that is not persisted.
pub fn load_or_create_worker_id(path: &Path) -> Option<Uuid> {
    if let Some(id) = read_id(path) {
        return Some(id);
    }
    if let Err(e) = create_parent_dir(path) {
        warn!(path = %path.display(), error = %e, "Cannot create the worker id directory");
        return None;
    }

    let fresh = Uuid::new_v4();
    match create_exclusive(path) {
        Ok(mut file) => {
            let written = file
                .write_all(fresh.to_string().as_bytes())
                .and_then(|_| file.sync_all());
            if let Err(e) = written {
                // Do not leave an empty file behind: it would look like a corrupt id to the next start.
                drop(file);
                let _ = std::fs::remove_file(path);
                warn!(path = %path.display(), error = %e, "Cannot write the worker id file");
                return None;
            }
            info!(path = %path.display(), worker_id = %fresh, "Created persistent worker id");
            Some(fresh)
        }
        Err(e) if e.kind() == std::io::ErrorKind::AlreadyExists => {
            // Another process created it between our read and our create: use its id.
            if let Some(id) = read_id(path) {
                return Some(id);
            }
            // Still unreadable after waiting: the file is corrupt (or truncated). Replace it.
            match replace_corrupt(path, fresh) {
                Ok(()) => {
                    warn!(path = %path.display(), worker_id = %fresh, "Replaced a corrupt worker id file");
                    Some(fresh)
                }
                Err(e) => {
                    warn!(path = %path.display(), error = %e, "Cannot replace the corrupt worker id file");
                    None
                }
            }
        }
        Err(e) => {
            warn!(path = %path.display(), error = %e, "Cannot create the worker id file");
            None
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::sync::{Arc, Barrier};

    #[test]
    fn test_only_a_named_worker_or_an_explicit_id_file_keeps_its_id() {
        use std::ffi::OsStr;
        assert!(persistence_wanted(Some("gpu-pc"), None));
        assert!(persistence_wanted(None, Some(OsStr::new("/var/lib/ox/id"))));
        assert!(persistence_wanted(
            Some("gpu-pc"),
            Some(OsStr::new("/var/lib/ox/id"))
        ));
        // Nothing says which machine this is: it would share ~/.oxideswarm/worker_id with the others.
        assert!(!persistence_wanted(None, None));
        assert!(!persistence_wanted(Some(""), None));
        assert!(!persistence_wanted(Some("   "), Some(OsStr::new(""))));
    }

    #[test]
    fn test_id_is_created_once_and_reused() {
        let dir = tempfile::tempdir().unwrap();
        let path = dir.path().join("nested").join("worker_id");
        let first = load_or_create_worker_id(&path).expect("created");
        assert!(path.is_file());
        assert_eq!(
            std::fs::read_to_string(&path).unwrap().trim(),
            first.to_string()
        );
        assert_eq!(
            load_or_create_worker_id(&path),
            Some(first),
            "second start reuses it"
        );
        assert_eq!(load_or_create_worker_id(&path), Some(first));
    }

    #[cfg(unix)]
    #[test]
    fn test_id_file_is_private() {
        use std::os::unix::fs::PermissionsExt;
        let dir = tempfile::tempdir().unwrap();
        let path = dir.path().join(".oxideswarm").join("worker_id");
        load_or_create_worker_id(&path).unwrap();
        let file_mode = std::fs::metadata(&path).unwrap().permissions().mode() & 0o777;
        assert_eq!(file_mode, 0o600, "id file mode");
        let dir_mode = std::fs::metadata(path.parent().unwrap())
            .unwrap()
            .permissions()
            .mode()
            & 0o777;
        assert_eq!(dir_mode, 0o700, "directory mode");
    }

    #[test]
    fn test_concurrent_starts_agree_on_one_id() {
        let dir = tempfile::tempdir().unwrap();
        let path = Arc::new(dir.path().join("worker_id"));
        let threads = 16;
        let barrier = Arc::new(Barrier::new(threads));
        let handles: Vec<_> = (0..threads)
            .map(|_| {
                let (path, barrier) = (Arc::clone(&path), Arc::clone(&barrier));
                std::thread::spawn(move || {
                    barrier.wait();
                    load_or_create_worker_id(&path)
                })
            })
            .collect();
        let ids: Vec<Option<Uuid>> = handles.into_iter().map(|h| h.join().unwrap()).collect();
        let first = ids[0].expect("an id");
        assert!(
            ids.iter().all(|id| *id == Some(first)),
            "every concurrent starter must see the same id, got {ids:?}"
        );
        assert_eq!(
            parse_id(&std::fs::read_to_string(&*path).unwrap()),
            Some(first)
        );
    }

    #[test]
    fn test_corrupt_file_is_replaced_and_then_stable() {
        let dir = tempfile::tempdir().unwrap();
        let path = dir.path().join("worker_id");
        std::fs::write(&path, "this is not a uuid").unwrap();
        let id = load_or_create_worker_id(&path).expect("replaced");
        assert_eq!(parse_id(&std::fs::read_to_string(&path).unwrap()), Some(id));
        assert_eq!(load_or_create_worker_id(&path), Some(id));

        // The nil UUID is not an identity either.
        std::fs::write(&path, Uuid::nil().to_string()).unwrap();
        let replaced = load_or_create_worker_id(&path).expect("replaced nil");
        assert_ne!(replaced, Uuid::nil());
    }

    #[test]
    fn test_unwritable_location_falls_back_without_panicking() {
        let dir = tempfile::tempdir().unwrap();
        // A regular file where a directory is needed: neither readable nor creatable.
        let blocker = dir.path().join("blocker");
        std::fs::write(&blocker, "x").unwrap();
        assert_eq!(load_or_create_worker_id(&blocker.join("worker_id")), None);
        // A directory in place of the file.
        let as_dir = dir.path().join("is_a_dir");
        std::fs::create_dir(&as_dir).unwrap();
        assert_eq!(load_or_create_worker_id(&as_dir), None);
    }

    #[test]
    fn test_default_path_is_per_name_and_sanitised() {
        // The env override is process-global; this test only inspects the name handling when it
        // is unset, which is the case for every test in this crate.
        if std::env::var_os(WORKER_ID_FILE_ENV).is_some() {
            return;
        }
        let Some(plain) = default_worker_id_path(None) else {
            return; // no home directory in this environment
        };
        assert!(
            plain.ends_with(".oxideswarm/worker_id") || plain.ends_with(".oxideswarm\\worker_id")
        );
        let named = default_worker_id_path(Some("gpu node/1")).unwrap();
        assert_eq!(named.file_name().unwrap(), "worker_id.gpu_node_1");
        let other = default_worker_id_path(Some("cpu-2")).unwrap();
        assert_ne!(named, other, "different names must not share an id file");
        assert_eq!(default_worker_id_path(Some("   ")).unwrap(), plain);
    }
}
