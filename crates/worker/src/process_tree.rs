//! Supervision of a task's whole process tree, and the priority task processes run at.
//!
//! A task is rarely one process: `cargo build`, `make -j8` or a shell script fan out into
//! grandchildren, and a careless one leaves a daemon behind. Taking such a tree down by walking a
//! snapshot of the process table is racy (anything forked after the snapshot, or already
//! re-parented to init, is missed) and can hit an unrelated process that reuses a PID. So each
//! task process starts in a container the operating system itself tracks:
//!
//! * **Unix**: the child becomes the leader of a new process group (`process_group(0)`), and
//!   `pre_exec` double-forks a tiny detached supervisor that holds the read end of a per-task
//!   lifeline pipe (`FD_CLOEXEC`) from the worker. The group id is the leader's PID, stays
//!   reserved while *any* member is alive, and `killpg` reaches every member in one call,
//!   grandchildren included. Stopping is `SIGTERM` to the group, a short grace, then `SIGKILL` to
//!   the group, followed by a cgroup v2 leaf kill (on Linux when delegated) and a per-attempt
//!   `RUSTY_GRID_RUN_TOKEN` environment sweep for `setsid()` survivors. When the worker crashes
//!   (`SIGKILL`, OOM kill), the kernel closes the worker's write end of the lifeline pipe with
//!   zero bytes written and the supervisor runs the same teardown on EOF.
//! * **Windows**: the child is assigned to a Job Object created with
//!   `JOB_OBJECT_LIMIT_KILL_ON_JOB_CLOSE`. `TerminateJobObject` ends every process in the job, and
//!   closing the last handle to it (worker exit, or a worker crash: the OS closes the handle) does
//!   the same. The assignment happens right after `CreateProcess` (`CREATE_SUSPENDED`), before the
//!   child's primary thread runs; if the assignment fails altogether the caller falls back to
//!   `taskkill /F /T`.
//!
//! Every live tree is registered, so [`terminate_all_task_process_trees`] can take them all down
//! from a signal handler. Dropping a task's `ProcessTree` kills what is left of it, which is what makes
//! an aborted task future (or a runtime that is shut down) clean up after itself.
//!
//! Memory and disk quotas are not enforced here (see [`ProcessPriority`] for CPU priority).

use std::str::FromStr;
use std::sync::atomic::{AtomicBool, Ordering};
use std::sync::{Arc, Mutex, MutexGuard};

use tracing::warn;

/// Length of `"RUSTY_GRID_RUN_TOKEN=<36-char hyphenated UUID>\0"`.
#[cfg(any(unix, test))]
pub(crate) const TASK_ENV_NEEDLE_LEN: usize = 21 + 36 + 1;

/// Fixed-size stack buffer holding `"RUSTY_GRID_RUN_TOKEN=<uuid>\0"`.
#[cfg(any(unix, test))]
pub(crate) type TaskEnvNeedle = [u8; TASK_ENV_NEEDLE_LEN];

/// Formats `"RUSTY_GRID_RUN_TOKEN=<uuid>\0"` into a fixed-size stack array with no heap allocation.
#[cfg(any(unix, test))]
pub(crate) fn build_task_env_needle(run_token: uuid::Uuid) -> TaskEnvNeedle {
    let mut out = [0u8; TASK_ENV_NEEDLE_LEN];
    out[..21].copy_from_slice(b"RUSTY_GRID_RUN_TOKEN=");
    let mut uuid_buf = [0u8; 36];
    let hyphenated = run_token.hyphenated().encode_lower(&mut uuid_buf);
    out[21..57].copy_from_slice(hyphenated.as_bytes());
    out[57] = 0;
    out
}

/// Checks whether a `\0`-delimited environment buffer chunk contains `needle`.
///
/// When `allow_at_start` is `true`, a match at byte offset `0` is accepted; otherwise `needle`
/// must be immediately preceded by a `\0` separator byte.
#[cfg(any(target_os = "linux", target_os = "android", target_os = "macos", test))]
pub(crate) fn env_block_contains_needle_chunk(
    buf: &[u8],
    needle: &TaskEnvNeedle,
    allow_at_start: bool,
) -> bool {
    if needle[0] == 0 || buf.len() < TASK_ENV_NEEDLE_LEN {
        return false;
    }
    if allow_at_start && &buf[..TASK_ENV_NEEDLE_LEN] == needle.as_slice() {
        return true;
    }
    let limit = buf.len() - TASK_ENV_NEEDLE_LEN;
    let mut i = 0;
    while i < limit {
        if buf[i] == 0 && &buf[i + 1..i + 1 + TASK_ENV_NEEDLE_LEN] == needle.as_slice() {
            return true;
        }
        i += 1;
    }
    false
}

/// Checks whether a `\0`-delimited environment buffer contains the exact
/// `RUSTY_GRID_RUN_TOKEN=<uuid>\0` entry.
#[cfg(any(target_os = "macos", test))]
pub(crate) fn env_block_contains_needle(buf: &[u8], needle: &TaskEnvNeedle) -> bool {
    env_block_contains_needle_chunk(buf, needle, true)
}

/// Formats `val` as ASCII decimal into `out` and returns the number of bytes written.
#[cfg(any(target_os = "linux", target_os = "android", test))]
fn format_u32_decimal(mut val: u32, out: &mut [u8; 16]) -> usize {
    if val == 0 {
        out[0] = b'0';
        return 1;
    }
    let mut tmp = [0u8; 16];
    let mut len = 0usize;
    while val > 0 {
        tmp[len] = b'0' + (val % 10) as u8;
        val /= 10;
        len += 1;
    }
    let mut i = 0;
    while i < len {
        out[i] = tmp[len - 1 - i];
        i += 1;
    }
    len
}

/// CPU priority task processes run at, so a build or a compute job submitted to the owner's
/// machine does not compete with what the owner is doing on it.
///
/// Mapping: `Normal` leaves the priority alone, `BelowNormal` is `nice +10` on Unix and
/// `BELOW_NORMAL_PRIORITY_CLASS` on Windows, `Idle` is `nice +19` and `IDLE_PRIORITY_CLASS`.
/// Lowering the priority needs no privilege; children inherit it. This is the *only* resource
/// limit applied to task processes: memory and disk use are **not** limited.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Default)]
pub enum ProcessPriority {
    /// Same priority as the worker itself.
    Normal,
    /// Yields to interactive work (the default).
    #[default]
    BelowNormal,
    /// Runs only with otherwise idle CPU time.
    Idle,
}

impl ProcessPriority {
    /// Environment variable that selects the priority when none is configured explicitly.
    pub const ENV_VAR: &'static str = "RUSTY_GRID_TASK_PRIORITY";

    /// The value an environment variable or a config file spells this priority as.
    pub fn as_str(self) -> &'static str {
        match self {
            ProcessPriority::Normal => "normal",
            ProcessPriority::BelowNormal => "below-normal",
            ProcessPriority::Idle => "idle",
        }
    }

    /// Reads [`Self::ENV_VAR`]. An unset or empty variable is `None`; an unknown value is
    /// reported and ignored (the default stays in force) rather than silently becoming `Normal`.
    pub fn from_env() -> Option<Self> {
        let raw = std::env::var(Self::ENV_VAR).ok()?;
        if raw.trim().is_empty() {
            return None;
        }
        match raw.parse() {
            Ok(priority) => Some(priority),
            Err(e) => {
                warn!("{}: {e}; keeping the default task priority", Self::ENV_VAR);
                None
            }
        }
    }

    /// The `nice` increment applied to task processes on Unix.
    #[cfg(unix)]
    fn unix_nice(self) -> i32 {
        match self {
            ProcessPriority::Normal => 0,
            ProcessPriority::BelowNormal => 10,
            ProcessPriority::Idle => 19,
        }
    }

    /// The Windows priority-class creation flag (0 = leave the default class).
    #[cfg(windows)]
    fn windows_class_flag(self) -> u32 {
        const BELOW_NORMAL_PRIORITY_CLASS: u32 = 0x0000_4000;
        const IDLE_PRIORITY_CLASS: u32 = 0x0000_0040;
        match self {
            ProcessPriority::Normal => 0,
            ProcessPriority::BelowNormal => BELOW_NORMAL_PRIORITY_CLASS,
            ProcessPriority::Idle => IDLE_PRIORITY_CLASS,
        }
    }
}

impl FromStr for ProcessPriority {
    type Err = String;

    fn from_str(s: &str) -> Result<Self, Self::Err> {
        match s.trim().to_ascii_lowercase().replace('_', "-").as_str() {
            "normal" => Ok(ProcessPriority::Normal),
            "below-normal" | "below" | "low" => Ok(ProcessPriority::BelowNormal),
            "idle" => Ok(ProcessPriority::Idle),
            other => Err(format!(
                "unknown task priority '{other}' (expected normal, below-normal or idle)"
            )),
        }
    }
}

/// A spawned task child process: on Windows, either a standard [`tokio::process::Child`] or an
/// AppContainer child process spawned via `CreateProcessW` + `STARTUPINFOEXW`.
#[cfg(not(windows))]
pub(crate) type SpawnedChild = tokio::process::Child;

/// A spawned task child process: either a standard [`tokio::process::Child`] or a Windows
/// AppContainer child process spawned via `CreateProcessW` + `STARTUPINFOEXW`.
#[cfg(windows)]
pub(crate) enum SpawnedChild {
    Tokio(tokio::process::Child),
    AppContainer(crate::isolation::windows::AppContainerChild),
}

#[cfg(windows)]
impl SpawnedChild {
    pub(crate) fn id(&self) -> Option<u32> {
        match self {
            Self::Tokio(c) => c.id(),
            Self::AppContainer(c) => c.id(),
        }
    }

    pub(crate) fn raw_handle(&self) -> Option<*mut core::ffi::c_void> {
        match self {
            Self::Tokio(c) => c.raw_handle(),
            Self::AppContainer(c) => c.raw_handle(),
        }
    }

    pub(crate) fn take_stdin(
        &mut self,
    ) -> Option<std::pin::Pin<Box<dyn tokio::io::AsyncWrite + Send>>> {
        match self {
            Self::Tokio(c) => c
                .stdin
                .take()
                .map(|s| Box::pin(s) as std::pin::Pin<Box<dyn tokio::io::AsyncWrite + Send>>),
            Self::AppContainer(c) => c
                .stdin
                .take()
                .map(|s| Box::pin(s) as std::pin::Pin<Box<dyn tokio::io::AsyncWrite + Send>>),
        }
    }

    pub(crate) fn take_stdout(
        &mut self,
    ) -> Option<std::pin::Pin<Box<dyn tokio::io::AsyncRead + Send>>> {
        match self {
            Self::Tokio(c) => c
                .stdout
                .take()
                .map(|s| Box::pin(s) as std::pin::Pin<Box<dyn tokio::io::AsyncRead + Send>>),
            Self::AppContainer(c) => c
                .stdout
                .take()
                .map(|s| Box::pin(s) as std::pin::Pin<Box<dyn tokio::io::AsyncRead + Send>>),
        }
    }

    pub(crate) fn take_stderr(
        &mut self,
    ) -> Option<std::pin::Pin<Box<dyn tokio::io::AsyncRead + Send>>> {
        match self {
            Self::Tokio(c) => c
                .stderr
                .take()
                .map(|s| Box::pin(s) as std::pin::Pin<Box<dyn tokio::io::AsyncRead + Send>>),
            Self::AppContainer(c) => c
                .stderr
                .take()
                .map(|s| Box::pin(s) as std::pin::Pin<Box<dyn tokio::io::AsyncRead + Send>>),
        }
    }

    pub(crate) async fn wait(&mut self) -> std::io::Result<std::process::ExitStatus> {
        match self {
            Self::Tokio(c) => c.wait().await,
            Self::AppContainer(c) => c.wait().await,
        }
    }

    pub(crate) fn start_kill(&mut self) -> std::io::Result<()> {
        match self {
            Self::Tokio(c) => c.start_kill(),
            Self::AppContainer(c) => c.start_kill(),
        }
    }
}

/// State prepared by [`configure_command`] before spawning the task process.
pub(crate) struct ConfiguredCommand {
    #[cfg(unix)]
    unix: imp::PreparedUnix,
    #[cfg(windows)]
    run_token: uuid::Uuid,
    #[cfg(windows)]
    windows_creation_flags: u32,
    #[cfg(windows)]
    app_container: Option<crate::isolation::windows::WindowsAppContainerGuard>,
}

impl ConfiguredCommand {
    /// Returns the per-attempt run token generated by [`configure_command`] (FEAT-2).
    #[cfg(windows)]
    pub(crate) fn run_token(&self) -> uuid::Uuid {
        self.run_token
    }

    /// Attaches a per-task Windows AppContainer guard to be used during [`Self::spawn`] and
    /// cleaned up when the [`ProcessTree`] is dropped.
    #[cfg(windows)]
    pub(crate) fn set_app_container(
        &mut self,
        guard: crate::isolation::windows::WindowsAppContainerGuard,
    ) {
        self.app_container = Some(guard);
    }

    /// Spawns `cmd` (holding `SPAWN_LOCK` on macOS so no concurrent `fork` observes an
    /// un-`CLOEXEC`ed lifeline descriptor) and closes the parent's copy of the lifeline read end.
    pub(crate) fn spawn(
        &mut self,
        cmd: &mut tokio::process::Command,
    ) -> std::io::Result<SpawnedChild> {
        #[cfg(unix)]
        {
            imp::spawn_command(&mut self.unix, cmd)
        }
        #[cfg(windows)]
        {
            if let Some(ref guard) = self.app_container {
                guard
                    .spawn_in_appcontainer(cmd, self.windows_creation_flags)
                    .map(SpawnedChild::AppContainer)
            } else {
                cmd.spawn().map(SpawnedChild::Tokio)
            }
        }
        #[cfg(not(any(unix, windows)))]
        {
            cmd.spawn()
        }
    }

    /// Attaches the freshly spawned `child` to its [`ProcessTree`].
    pub(crate) fn attach(self, child: &SpawnedChild) -> ProcessTree {
        ProcessTree::attach(child, self)
    }
}

/// Prepares `cmd` so that the process it spawns leads its own process group with a lifeline
/// supervisor (Unix) or starts suspended for Job Object attachment (Windows), and runs at
/// `priority`. Must be called before [`ConfiguredCommand::spawn`].
pub(crate) fn configure_command(
    cmd: &mut tokio::process::Command,
    priority: ProcessPriority,
    task_id: rusty_grid_core::TaskId,
) -> ConfiguredCommand {
    #[cfg(unix)]
    {
        let _ = task_id;
        ConfiguredCommand {
            unix: imp::configure_command(cmd, priority),
        }
    }

    #[cfg(windows)]
    {
        let _ = task_id;
        let run_token = uuid::Uuid::new_v4();
        cmd.env("RUSTY_GRID_RUN_TOKEN", run_token.to_string());
        let flag =
            priority.windows_class_flag() | rusty_grid_core::capabilities::WINDOWS_CREATE_SUSPENDED;
        cmd.creation_flags(flag);
        ConfiguredCommand {
            run_token,
            windows_creation_flags: flag,
            app_container: None,
        }
    }

    #[cfg(not(any(unix, windows)))]
    {
        let _ = (cmd, priority, task_id);
        ConfiguredCommand {}
    }
}

#[cfg(unix)]
mod imp {
    use std::os::unix::io::{AsRawFd, FromRawFd, OwnedFd};
    #[cfg(not(any(target_os = "linux", target_os = "android")))]
    use std::sync::Mutex;
    use tokio::process::Child;

    #[cfg(target_os = "macos")]
    use super::env_block_contains_needle;
    #[cfg(any(target_os = "linux", target_os = "android", test))]
    use super::TASK_ENV_NEEDLE_LEN;
    use super::{build_task_env_needle, ProcessPriority, TaskEnvNeedle};
    #[cfg(any(target_os = "linux", target_os = "android"))]
    use super::{env_block_contains_needle_chunk, format_u32_decimal};

    /// Serialises `pipe` + `fcntl(FD_CLOEXEC)` and `cmd.spawn()` on platforms without `pipe2`
    /// (macOS) so no concurrent task `fork` can inherit a lifeline pipe descriptor before
    /// `FD_CLOEXEC` is set on it.
    #[cfg(not(any(target_os = "linux", target_os = "android")))]
    static SPAWN_LOCK: Mutex<()> = Mutex::new(());

    #[cfg(target_os = "linux")]
    pub(super) const CGROUP_PATH_CAP: usize = 256;

    #[cfg(target_os = "linux")]
    pub(super) struct CgroupLeaf {
        pub(super) path_cstr: [u8; CGROUP_PATH_CAP],
        pub(super) procs_fd: OwnedFd,
        pub(super) kill_fd: Option<OwnedFd>,
    }

    pub(super) struct PreparedUnix {
        lifeline_read: Option<OwnedFd>,
        pub(super) lifeline_write: Option<OwnedFd>,
        worker_pid: libc::pid_t,
        needle: TaskEnvNeedle,
        #[cfg(target_os = "macos")]
        spawn_tvsec: u64,
        #[cfg(target_os = "linux")]
        pub(super) cgroup_leaf: Option<CgroupLeaf>,
    }

    /// Metadata of an active Unix task tree stored in [`super::LIVE`] so signal handlers and
    /// [`super::ProcessTree::kill`] can terminate both the process group and any `setsid()`
    /// survivors without heap allocation.
    #[derive(Clone, Copy)]
    pub(super) struct RawId {
        pub(super) pgid: libc::pid_t,
        worker_pid: libc::pid_t,
        needle: TaskEnvNeedle,
        #[cfg(target_os = "macos")]
        spawn_tvsec: u64,
        #[cfg(target_os = "linux")]
        cgroup_path_cstr: [u8; CGROUP_PATH_CAP],
        #[cfg(target_os = "linux")]
        cgroup_procs_fd: libc::c_int,
        #[cfg(target_os = "linux")]
        cgroup_kill_fd: libc::c_int,
    }

    impl PartialEq for RawId {
        fn eq(&self, other: &Self) -> bool {
            self.pgid == other.pgid
        }
    }

    impl Eq for RawId {}

    impl RawId {
        #[cfg(test)]
        pub(super) fn for_pgid_only(pgid: libc::pid_t) -> Self {
            Self {
                pgid,
                worker_pid: 0,
                needle: [0u8; TASK_ENV_NEEDLE_LEN],
                #[cfg(target_os = "macos")]
                spawn_tvsec: 0,
                #[cfg(target_os = "linux")]
                cgroup_path_cstr: [0u8; CGROUP_PATH_CAP],
                #[cfg(target_os = "linux")]
                cgroup_procs_fd: -1,
                #[cfg(target_os = "linux")]
                cgroup_kill_fd: -1,
            }
        }
    }

    /// Creates an anonymous lifeline pipe `(read_fd, write_fd)` with `FD_CLOEXEC` set on both ends.
    pub(super) fn create_lifeline_pipe() -> std::io::Result<(OwnedFd, OwnedFd)> {
        #[cfg(not(any(target_os = "linux", target_os = "android")))]
        let _guard = SPAWN_LOCK
            .lock()
            .unwrap_or_else(|poisoned| poisoned.into_inner());

        let mut fds: [libc::c_int; 2] = [-1, -1];
        #[cfg(any(target_os = "linux", target_os = "android"))]
        {
            let rc = unsafe { libc::pipe2(fds.as_mut_ptr(), libc::O_CLOEXEC) };
            if rc != 0 {
                return Err(std::io::Error::last_os_error());
            }
        }
        #[cfg(not(any(target_os = "linux", target_os = "android")))]
        {
            let rc = unsafe { libc::pipe(fds.as_mut_ptr()) };
            if rc != 0 {
                return Err(std::io::Error::last_os_error());
            }
            for &fd in &fds {
                let flags = unsafe { libc::fcntl(fd, libc::F_GETFD) };
                if flags < 0
                    || unsafe { libc::fcntl(fd, libc::F_SETFD, flags | libc::FD_CLOEXEC) } < 0
                {
                    let err = std::io::Error::last_os_error();
                    unsafe {
                        let _ = libc::close(fds[0]);
                        let _ = libc::close(fds[1]);
                    }
                    return Err(err);
                }
            }
        }
        let read_fd = unsafe { OwnedFd::from_raw_fd(fds[0]) };
        let write_fd = unsafe { OwnedFd::from_raw_fd(fds[1]) };
        Ok((read_fd, write_fd))
    }

    #[cfg(target_os = "linux")]
    fn delegated_cgroup_base() -> Option<&'static std::path::Path> {
        static BASE: std::sync::OnceLock<Option<std::path::PathBuf>> = std::sync::OnceLock::new();
        BASE.get_or_init(|| {
            let text = std::fs::read_to_string("/proc/self/cgroup").ok()?;
            let rel = text
                .lines()
                .find_map(|line| line.strip_prefix("0::"))?
                .trim()
                .trim_start_matches('/');
            let base = if rel.is_empty() {
                std::path::PathBuf::from("/sys/fs/cgroup")
            } else {
                std::path::Path::new("/sys/fs/cgroup").join(rel)
            };
            if !base.join("cgroup.procs").exists() {
                return None;
            }
            let probe = base.join(format!("oxideswarm-probe-{}", unsafe { libc::getpid() }));
            std::fs::create_dir(&probe).ok()?;
            let writable = std::fs::OpenOptions::new()
                .write(true)
                .open(probe.join("cgroup.procs"))
                .is_ok();
            let _ = std::fs::remove_dir(&probe);
            if writable {
                Some(base)
            } else {
                None
            }
        })
        .as_deref()
    }

    #[cfg(target_os = "linux")]
    fn prepare_cgroup_leaf(run_token: uuid::Uuid) -> Option<CgroupLeaf> {
        use std::os::unix::ffi::OsStrExt;
        use std::os::unix::fs::OpenOptionsExt;

        let base = delegated_cgroup_base()?;
        let leaf_dir = base.join(format!("oxideswarm-run-{run_token}"));
        let raw_bytes = leaf_dir.as_os_str().as_bytes();
        if raw_bytes.is_empty() || raw_bytes.len() + 1 > CGROUP_PATH_CAP || raw_bytes.contains(&0) {
            return None;
        }
        std::fs::create_dir(&leaf_dir).ok()?;
        let procs_file = match std::fs::OpenOptions::new()
            .read(true)
            .write(true)
            .custom_flags(libc::O_CLOEXEC)
            .open(leaf_dir.join("cgroup.procs"))
        {
            Ok(f) => f,
            Err(_) => {
                let _ = std::fs::remove_dir(&leaf_dir);
                return None;
            }
        };
        let kill_file = std::fs::OpenOptions::new()
            .write(true)
            .custom_flags(libc::O_CLOEXEC)
            .open(leaf_dir.join("cgroup.kill"))
            .ok();
        let mut path_cstr = [0u8; CGROUP_PATH_CAP];
        path_cstr[..raw_bytes.len()].copy_from_slice(raw_bytes);
        Some(CgroupLeaf {
            path_cstr,
            procs_fd: OwnedFd::from(procs_file),
            kill_fd: kill_file.map(OwnedFd::from),
        })
    }

    #[cfg(target_os = "linux")]
    unsafe fn rmdir_cgroup_leaf_with_retry(path_cstr: &[u8; CGROUP_PATH_CAP]) {
        if path_cstr[0] == 0 {
            return;
        }
        let ptr = path_cstr.as_ptr().cast::<libc::c_char>();
        let mut attempt = 0;
        while attempt < 6 {
            if libc::rmdir(ptr) == 0 {
                return;
            }
            let err = std::io::Error::last_os_error().raw_os_error();
            if err != Some(libc::EBUSY) {
                return;
            }
            let req = libc::timespec {
                tv_sec: 0,
                tv_nsec: 5_000_000,
            };
            let _ = libc::nanosleep(&req, std::ptr::null_mut());
            attempt += 1;
        }
    }

    #[cfg(target_os = "linux")]
    unsafe fn kill_cgroup_leaf(
        kill_fd: libc::c_int,
        procs_fd: libc::c_int,
        path_cstr: &[u8; CGROUP_PATH_CAP],
    ) {
        if path_cstr[0] == 0 {
            return;
        }
        if kill_fd >= 0 {
            let _ = libc::write(kill_fd, b"1".as_ptr().cast::<libc::c_void>(), 1);
        }
        if procs_fd >= 0 && libc::lseek(procs_fd, 0, libc::SEEK_SET) >= 0 {
            let mut buf = [0u8; 4096];
            let n = libc::read(procs_fd, buf.as_mut_ptr().cast::<libc::c_void>(), buf.len());
            if n > 0 {
                let mut pid: libc::pid_t = 0;
                let mut has_digit = false;
                for &b in &buf[..n as usize] {
                    if b.is_ascii_digit() {
                        has_digit = true;
                        pid = pid
                            .saturating_mul(10)
                            .saturating_add((b - b'0') as libc::pid_t);
                    } else {
                        if has_digit && pid > 1 {
                            let _ = libc::kill(pid, libc::SIGKILL);
                        }
                        pid = 0;
                        has_digit = false;
                    }
                }
                if has_digit && pid > 1 {
                    let _ = libc::kill(pid, libc::SIGKILL);
                }
            }
        }
        rmdir_cgroup_leaf_with_retry(path_cstr);
    }

    unsafe fn close_fds_except(keep: &[libc::c_int], max_nofile: libc::c_int) {
        let mut max_keep: libc::c_int = -1;
        for &fd in keep {
            if fd > max_keep {
                max_keep = fd;
            }
        }

        let mut fd: libc::c_int = 0;
        while fd <= max_keep {
            let mut should_keep = false;
            for &k in keep {
                if k == fd {
                    should_keep = true;
                    break;
                }
            }
            if !should_keep {
                let _ = libc::close(fd);
            }
            fd += 1;
        }

        // Bionic's app seccomp filter only added `close_range` in Android 13 (API 33), while our
        // `minSdk` is 24. Never call `SYS_close_range` on Android so API 26-32 does not trap with
        // `SIGSYS`; use the bounded `close` loop below instead.
        #[cfg(target_os = "linux")]
        {
            let first_after = (max_keep + 1) as libc::c_uint;
            let last_fd = libc::c_uint::MAX;
            let flags: libc::c_uint = 0;
            if libc::syscall(libc::SYS_close_range, first_after, last_fd, flags) == 0 {
                return;
            }
        }

        let mut fd = max_keep + 1;
        while fd < max_nofile {
            let _ = libc::close(fd);
            fd += 1;
        }
    }

    #[cfg(any(target_os = "linux", target_os = "android"))]
    unsafe fn kill_escaped_by_env(
        task_pgid: libc::pid_t,
        worker_pid: libc::pid_t,
        needle: &TaskEnvNeedle,
    ) -> usize {
        if needle[0] == 0 {
            return 0;
        }
        let self_pid = libc::getpid();
        let my_euid = libc::geteuid();
        let proc_fd = libc::open(
            c"/proc".as_ptr(),
            libc::O_RDONLY | libc::O_DIRECTORY | libc::O_CLOEXEC,
        );
        if proc_fd < 0 {
            return 0;
        }
        let mut killed = 0usize;
        let mut dir_buf = [0u8; 4096];
        let mut env_buf = [0u8; 4096];
        loop {
            let nread = libc::syscall(
                libc::SYS_getdents64,
                proc_fd,
                dir_buf.as_mut_ptr().cast::<libc::c_void>(),
                dir_buf.len(),
            );
            if nread <= 0 {
                break;
            }
            let nread = nread as usize;
            let mut bpos = 0usize;
            while bpos + 19 <= nread {
                let reclen = u16::from_ne_bytes([dir_buf[bpos + 16], dir_buf[bpos + 17]]) as usize;
                if reclen < 19 || bpos + reclen > nread {
                    break;
                }
                let name_slice = &dir_buf[bpos + 19..bpos + reclen];
                bpos += reclen;

                let mut pid: libc::pid_t = 0;
                let mut valid_pid = false;
                for &b in name_slice {
                    if b == 0 {
                        break;
                    }
                    if !b.is_ascii_digit() {
                        valid_pid = false;
                        break;
                    }
                    valid_pid = true;
                    pid = pid
                        .saturating_mul(10)
                        .saturating_add((b - b'0') as libc::pid_t);
                }
                if !valid_pid
                    || pid <= 1
                    || pid == self_pid
                    || pid == worker_pid
                    || pid == task_pgid
                {
                    continue;
                }

                let mut path_buf = [0u8; 48];
                path_buf[..6].copy_from_slice(b"/proc/");
                let mut dec = [0u8; 16];
                let dec_len = format_u32_decimal(pid as u32, &mut dec);
                path_buf[6..6 + dec_len].copy_from_slice(&dec[..dec_len]);
                path_buf[6 + dec_len..6 + dec_len + 9].copy_from_slice(b"/environ\0");

                let env_fd = libc::open(
                    path_buf.as_ptr().cast::<libc::c_char>(),
                    libc::O_RDONLY | libc::O_CLOEXEC,
                );
                if env_fd < 0 {
                    continue;
                }

                let mut st: libc::stat = std::mem::zeroed();
                if libc::fstat(env_fd, &mut st) == 0 && st.st_uid != my_euid {
                    let _ = libc::close(env_fd);
                    continue;
                }

                let mut matched = false;
                let mut carry = 0usize;
                let mut first_chunk = true;
                loop {
                    let r = libc::read(
                        env_fd,
                        env_buf[carry..].as_mut_ptr().cast::<libc::c_void>(),
                        env_buf.len() - carry,
                    );
                    if r <= 0 {
                        break;
                    }
                    let total = carry + (r as usize);
                    if env_block_contains_needle_chunk(&env_buf[..total], needle, first_chunk) {
                        matched = true;
                        break;
                    }
                    first_chunk = false;
                    if total >= TASK_ENV_NEEDLE_LEN {
                        env_buf.copy_within(total - TASK_ENV_NEEDLE_LEN..total, 0);
                        carry = TASK_ENV_NEEDLE_LEN;
                    } else {
                        carry = total;
                    }
                }
                let _ = libc::close(env_fd);

                if matched && libc::kill(pid, libc::SIGKILL) == 0 {
                    killed += 1;
                }
            }
        }
        let _ = libc::close(proc_fd);
        killed
    }

    #[cfg(target_os = "macos")]
    unsafe fn kill_escaped_by_env(
        task_pgid: libc::pid_t,
        worker_pid: libc::pid_t,
        needle: &TaskEnvNeedle,
        spawn_tvsec: u64,
    ) -> usize {
        if needle[0] == 0 {
            return 0;
        }
        let self_pid = libc::getpid();
        let my_euid = libc::geteuid();

        let mut pids = [0 as libc::pid_t; 4096];
        let n_pids = libc::proc_listallpids(
            pids.as_mut_ptr().cast::<libc::c_void>(),
            std::mem::size_of_val(&pids) as libc::c_int,
        );
        if n_pids <= 0 {
            return 0;
        }
        let count = (n_pids as usize).min(pids.len());

        let mut buf_ptr: *mut libc::c_void = std::ptr::null_mut();
        let mut buf_cap: libc::size_t = 0;

        let mut killed = 0usize;
        for &pid in &pids[..count] {
            if pid <= 1 || pid == self_pid || pid == worker_pid || pid == task_pgid {
                continue;
            }
            let mut bsd_info = std::mem::zeroed::<libc::proc_bsdinfo>();
            let info_rc = libc::proc_pidinfo(
                pid,
                libc::PROC_PIDTBSDINFO,
                0,
                (&mut bsd_info as *mut libc::proc_bsdinfo).cast::<libc::c_void>(),
                std::mem::size_of::<libc::proc_bsdinfo>() as libc::c_int,
            );
            if info_rc != std::mem::size_of::<libc::proc_bsdinfo>() as libc::c_int {
                continue;
            }
            if bsd_info.pbi_uid != my_euid && bsd_info.pbi_ruid != my_euid {
                continue;
            }
            if bsd_info.pbi_start_tvsec < spawn_tvsec {
                continue;
            }

            if buf_ptr.is_null() {
                let mut arg_max_int: libc::c_int = 0;
                let mut arg_max_size: libc::size_t = std::mem::size_of::<libc::c_int>();
                let mut max_mib: [libc::c_int; 2] = [libc::CTL_KERN, libc::KERN_ARGMAX];
                buf_cap = if libc::sysctl(
                    max_mib.as_mut_ptr(),
                    2,
                    (&mut arg_max_int as *mut libc::c_int).cast::<libc::c_void>(),
                    &mut arg_max_size,
                    std::ptr::null_mut(),
                    0,
                ) == 0
                    && arg_max_int > 4096
                {
                    (arg_max_int as usize).min(4 * 1024 * 1024)
                } else {
                    1024 * 1024
                };
                let mapped = libc::mmap(
                    std::ptr::null_mut(),
                    buf_cap,
                    libc::PROT_READ | libc::PROT_WRITE,
                    libc::MAP_PRIVATE | libc::MAP_ANON,
                    -1,
                    0,
                );
                if mapped == libc::MAP_FAILED || mapped.is_null() {
                    return 0;
                }
                buf_ptr = mapped;
            }

            let mut mib: [libc::c_int; 3] = [libc::CTL_KERN, libc::KERN_PROCARGS2, pid];
            let mut size: libc::size_t = buf_cap;
            if libc::sysctl(
                mib.as_mut_ptr(),
                3,
                buf_ptr,
                &mut size,
                std::ptr::null_mut(),
                0,
            ) == 0
                && size > std::mem::size_of::<libc::c_int>()
            {
                let valid_len = size.min(buf_cap);
                let slice = std::slice::from_raw_parts(buf_ptr.cast::<u8>(), valid_len);
                if env_block_contains_needle(&slice[std::mem::size_of::<libc::c_int>()..], needle)
                    && libc::kill(pid, libc::SIGKILL) == 0
                {
                    killed += 1;
                }
            }
        }

        if !buf_ptr.is_null() {
            let _ = libc::munmap(buf_ptr, buf_cap);
        }
        killed
    }

    #[cfg(not(any(target_os = "linux", target_os = "android", target_os = "macos")))]
    unsafe fn kill_escaped_by_env(
        _task_pgid: libc::pid_t,
        _worker_pid: libc::pid_t,
        _needle: &TaskEnvNeedle,
    ) -> usize {
        0
    }

    pub(super) fn configure_command(
        cmd: &mut tokio::process::Command,
        priority: ProcessPriority,
    ) -> PreparedUnix {
        cmd.process_group(0);
        let nice = priority.unix_nice();
        let worker_pid = unsafe { libc::getpid() };
        let run_token = uuid::Uuid::new_v4();
        cmd.env("RUSTY_GRID_RUN_TOKEN", run_token.to_string());
        let needle = build_task_env_needle(run_token);

        #[cfg(target_os = "macos")]
        let spawn_tvsec: u64 = unsafe {
            let mut tv = libc::timeval {
                tv_sec: 0,
                tv_usec: 0,
            };
            if libc::gettimeofday(&mut tv, std::ptr::null_mut()) == 0 && tv.tv_sec > 2 {
                (tv.tv_sec as u64).saturating_sub(2)
            } else {
                0
            }
        };

        let max_nofile: libc::c_int = unsafe {
            let mut rlim = libc::rlimit {
                rlim_cur: 0,
                rlim_max: 0,
            };
            #[allow(clippy::unnecessary_cast)]
            if libc::getrlimit(libc::RLIMIT_NOFILE, &mut rlim) == 0 && rlim.rlim_cur > 0 {
                let cur = rlim.rlim_cur as u64;
                if cur < (libc::c_int::MAX as u64) {
                    cur as libc::c_int
                } else {
                    65536
                }
            } else {
                65536
            }
        };

        let (lifeline_read, lifeline_write) = match create_lifeline_pipe() {
            Ok((r, w)) => (Some(r), Some(w)),
            Err(_) => (None, None),
        };
        let read_raw = lifeline_read.as_ref().map_or(-1, AsRawFd::as_raw_fd);
        let write_raw = lifeline_write.as_ref().map_or(-1, AsRawFd::as_raw_fd);

        #[cfg(target_os = "linux")]
        let cgroup_leaf = prepare_cgroup_leaf(run_token);
        #[cfg(target_os = "linux")]
        let cgroup_procs_raw = cgroup_leaf
            .as_ref()
            .map_or(-1, |leaf| leaf.procs_fd.as_raw_fd());
        #[cfg(target_os = "linux")]
        let cgroup_kill_raw = cgroup_leaf
            .as_ref()
            .and_then(|leaf| leaf.kill_fd.as_ref())
            .map_or(-1, AsRawFd::as_raw_fd);
        #[cfg(target_os = "linux")]
        let cgroup_path_cstr = cgroup_leaf
            .as_ref()
            .map_or([0u8; CGROUP_PATH_CAP], |leaf| leaf.path_cstr);

        unsafe {
            cmd.pre_exec(move || {
                let task_pgid = libc::getpid();
                let _ = libc::setpgid(0, 0);

                if nice > 0 {
                    libc::setpriority(libc::PRIO_PROCESS, 0, nice);
                }

                if read_raw >= 0 {
                    let child_b = libc::fork();
                    if child_b < 0 {
                        return Err(std::io::Error::last_os_error());
                    }
                    if child_b == 0 {
                        let child_c = libc::fork();
                        if child_c < 0 {
                            libc::_exit(1);
                        }
                        if child_c > 0 {
                            libc::_exit(0);
                        }

                        let _ = libc::setsid();
                        #[cfg(target_os = "linux")]
                        close_fds_except(
                            &[read_raw, cgroup_procs_raw, cgroup_kill_raw],
                            max_nofile,
                        );
                        #[cfg(not(target_os = "linux"))]
                        close_fds_except(&[read_raw], max_nofile);

                        let mut byte: u8 = 0;
                        let mut worker_dismissed = false;
                        loop {
                            let n = libc::read(
                                read_raw,
                                (&mut byte as *mut u8).cast::<libc::c_void>(),
                                1,
                            );
                            if n > 0 {
                                worker_dismissed = true;
                                break;
                            }
                            if n == 0 {
                                break;
                            }
                            if n < 0 {
                                if std::io::Error::last_os_error().raw_os_error()
                                    == Some(libc::EINTR)
                                {
                                    continue;
                                }
                                break;
                            }
                        }

                        if !worker_dismissed {
                            if task_pgid > 1 {
                                let _ = libc::killpg(task_pgid, libc::SIGTERM);
                                let mut step = 0;
                                while step < 10 {
                                    if libc::killpg(task_pgid, 0) != 0 {
                                        break;
                                    }
                                    let req = libc::timespec {
                                        tv_sec: 0,
                                        tv_nsec: 20_000_000,
                                    };
                                    let _ = libc::nanosleep(&req, std::ptr::null_mut());
                                    step += 1;
                                }
                                let _ = libc::killpg(task_pgid, libc::SIGKILL);
                            }

                            #[cfg(target_os = "linux")]
                            kill_cgroup_leaf(cgroup_kill_raw, cgroup_procs_raw, &cgroup_path_cstr);

                            #[cfg(target_os = "macos")]
                            let swept =
                                kill_escaped_by_env(task_pgid, worker_pid, &needle, spawn_tvsec);
                            #[cfg(not(target_os = "macos"))]
                            let swept = kill_escaped_by_env(task_pgid, worker_pid, &needle);

                            if swept > 0 {
                                if task_pgid > 1 {
                                    let _ = libc::killpg(task_pgid, libc::SIGKILL);
                                }
                                #[cfg(target_os = "macos")]
                                let _ = kill_escaped_by_env(
                                    task_pgid,
                                    worker_pid,
                                    &needle,
                                    spawn_tvsec,
                                );
                                #[cfg(not(target_os = "macos"))]
                                let _ = kill_escaped_by_env(task_pgid, worker_pid, &needle);
                            }
                        }

                        let _ = libc::close(read_raw);
                        #[cfg(target_os = "linux")]
                        {
                            if cgroup_procs_raw >= 0 {
                                let _ = libc::close(cgroup_procs_raw);
                            }
                            if cgroup_kill_raw >= 0 {
                                let _ = libc::close(cgroup_kill_raw);
                            }
                        }
                        libc::_exit(0);
                    }

                    let mut status: libc::c_int = 0;
                    loop {
                        let waited = libc::waitpid(child_b, &mut status, 0);
                        if waited >= 0 {
                            break;
                        }
                        if std::io::Error::last_os_error().raw_os_error() != Some(libc::EINTR) {
                            break;
                        }
                    }

                    let _ = libc::close(read_raw);
                    if write_raw >= 0 {
                        let _ = libc::close(write_raw);
                    }
                }

                #[cfg(target_os = "linux")]
                if cgroup_procs_raw >= 0 {
                    let mut dec = [0u8; 16];
                    let dec_len = format_u32_decimal(task_pgid as u32, &mut dec);
                    let _ = libc::write(
                        cgroup_procs_raw,
                        dec.as_ptr().cast::<libc::c_void>(),
                        dec_len,
                    );
                    let _ = libc::close(cgroup_procs_raw);
                    if cgroup_kill_raw >= 0 {
                        let _ = libc::close(cgroup_kill_raw);
                    }
                }

                Ok(())
            });
        }

        PreparedUnix {
            lifeline_read,
            lifeline_write,
            worker_pid,
            needle,
            #[cfg(target_os = "macos")]
            spawn_tvsec,
            #[cfg(target_os = "linux")]
            cgroup_leaf,
        }
    }

    pub(super) fn spawn_command(
        prepared: &mut PreparedUnix,
        cmd: &mut tokio::process::Command,
    ) -> std::io::Result<Child> {
        #[cfg(not(any(target_os = "linux", target_os = "android")))]
        let _guard = SPAWN_LOCK
            .lock()
            .unwrap_or_else(|poisoned| poisoned.into_inner());

        let res = cmd.spawn();
        // Drop the worker's copy of the lifeline read end immediately so only the detached
        // supervisor holds it open.
        drop(prepared.lifeline_read.take());
        res
    }

    pub(super) fn attach(child: &Child, prepared: &PreparedUnix) -> Option<RawId> {
        // The child leads its own group (see `configure_command`), so the group id is its PID.
        // Never 0 or 1: `killpg(0)` would signal the worker's own group.
        let pgid = child
            .id()
            .and_then(|pid| libc::pid_t::try_from(pid).ok())
            .filter(|pgid| *pgid > 1)?;
        Some(RawId {
            pgid,
            worker_pid: prepared.worker_pid,
            needle: prepared.needle,
            #[cfg(target_os = "macos")]
            spawn_tvsec: prepared.spawn_tvsec,
            #[cfg(target_os = "linux")]
            cgroup_path_cstr: prepared
                .cgroup_leaf
                .as_ref()
                .map_or([0u8; CGROUP_PATH_CAP], |leaf| leaf.path_cstr),
            #[cfg(target_os = "linux")]
            cgroup_procs_fd: prepared
                .cgroup_leaf
                .as_ref()
                .map_or(-1, |leaf| leaf.procs_fd.as_raw_fd()),
            #[cfg(target_os = "linux")]
            cgroup_kill_fd: prepared
                .cgroup_leaf
                .as_ref()
                .and_then(|leaf| leaf.kill_fd.as_ref())
                .map_or(-1, AsRawFd::as_raw_fd),
        })
    }

    pub(super) fn request_stop(id: RawId) -> bool {
        signal(id.pgid, libc::SIGTERM)
    }

    pub(super) fn kill(id: RawId) {
        if id.pgid <= 1 {
            return;
        }
        signal(id.pgid, libc::SIGKILL);
        unsafe {
            #[cfg(target_os = "linux")]
            kill_cgroup_leaf(id.cgroup_kill_fd, id.cgroup_procs_fd, &id.cgroup_path_cstr);

            #[cfg(target_os = "macos")]
            let swept = kill_escaped_by_env(id.pgid, id.worker_pid, &id.needle, id.spawn_tvsec);
            #[cfg(not(target_os = "macos"))]
            let swept = kill_escaped_by_env(id.pgid, id.worker_pid, &id.needle);

            if swept > 0 {
                signal(id.pgid, libc::SIGKILL);
                #[cfg(target_os = "macos")]
                let _ = kill_escaped_by_env(id.pgid, id.worker_pid, &id.needle, id.spawn_tvsec);
                #[cfg(not(target_os = "macos"))]
                let _ = kill_escaped_by_env(id.pgid, id.worker_pid, &id.needle);
            }
        }
    }

    pub(super) fn release(id: RawId) {
        #[cfg(target_os = "linux")]
        unsafe {
            rmdir_cgroup_leaf_with_retry(&id.cgroup_path_cstr);
        }
        #[cfg(not(target_os = "linux"))]
        let _ = id;
    }

    fn signal(pgid: libc::pid_t, sig: libc::c_int) -> bool {
        if pgid <= 1 {
            return false;
        }
        // SAFETY: killpg only sends a signal. `pgid` is the group this module created for a task;
        // the group id cannot be recycled while a member of the group is alive.
        unsafe { libc::killpg(pgid, sig) == 0 }
    }
}

#[cfg(windows)]
mod imp {
    use rusty_grid_core::capabilities::{
        attach_suspended_process_to_kill_on_close_job, close_job_object, terminate_job_object,
    };

    /// A Job Object handle, as an integer so it can sit in a `static`.
    pub(super) type RawId = isize;

    pub(super) fn attach(child: &super::SpawnedChild) -> Option<RawId> {
        let process = child.raw_handle()?;
        attach_suspended_process_to_kill_on_close_job(process)
    }

    /// Windows has no polite group signal; the caller goes straight to `kill`.
    pub(super) fn request_stop(_id: RawId) -> bool {
        false
    }

    pub(super) fn kill(id: RawId) {
        terminate_job_object(id);
    }

    pub(super) fn release(id: RawId) {
        close_job_object(id);
    }
}

#[cfg(not(any(unix, windows)))]
mod imp {
    use tokio::process::Child;

    pub(super) type RawId = u32;

    pub(super) fn attach(_child: &Child) -> Option<RawId> {
        None
    }

    pub(super) fn request_stop(_id: RawId) -> bool {
        false
    }

    pub(super) fn kill(_id: RawId) {}

    pub(super) fn release(_id: RawId) {}
}

/// Trees that are alive right now (group descriptors on Unix, job handles on Windows).
static LIVE: Mutex<Vec<imp::RawId>> = Mutex::new(Vec::new());

fn live() -> MutexGuard<'static, Vec<imp::RawId>> {
    LIVE.lock().unwrap_or_else(|poisoned| poisoned.into_inner())
}

/// Kills every task process tree that is still alive and returns how many there were.
///
/// For a shutdown path that does not unwind (a `SIGTERM` or console-close handler): dropping the
/// task futures kills their trees as well, but a process that is terminated by the default signal
/// action never gets to drop anything.
pub fn terminate_all_task_process_trees() -> usize {
    let snapshot: Vec<imp::RawId> = live().clone();
    for id in &snapshot {
        imp::kill(*id);
    }
    snapshot.len()
}

struct TreeInner {
    id: imp::RawId,
    done: AtomicBool,
    kill_lock: Mutex<()>,
    #[cfg(windows)]
    spawned_at: std::time::Instant,
    #[cfg(windows)]
    app_container: Mutex<Option<crate::isolation::windows::WindowsAppContainerGuard>>,
    #[cfg(unix)]
    lifeline_write: Mutex<Option<std::os::unix::io::OwnedFd>>,
    #[cfg(target_os = "linux")]
    cgroup_leaf: Mutex<Option<imp::CgroupLeaf>>,
}

impl TreeInner {
    fn kill_and_release_once(&self, child_already_exited: bool) {
        if self.done.load(Ordering::Acquire) {
            return;
        }
        let _guard = self
            .kill_lock
            .lock()
            .unwrap_or_else(|poisoned| poisoned.into_inner());
        if self.done.swap(true, Ordering::AcqRel) {
            return;
        }
        #[cfg(windows)]
        if !child_already_exited {
            // Avoid terminating a freshly resumed MSYS2/Cygwin child while it is still inside
            // `msys-2.0.dll` `dll_crt0_1` shared-memory initialization, which can stall
            // concurrent MSYS2 processes on Windows for 15s waiting on the global init lock.
            const MIN_LIVE_KILL_AGE: std::time::Duration = std::time::Duration::from_millis(80);
            if let Some(remaining) = MIN_LIVE_KILL_AGE.checked_sub(self.spawned_at.elapsed()) {
                std::thread::sleep(remaining);
            }
        }
        #[cfg(not(windows))]
        let _ = child_already_exited;

        live().retain(|registered| *registered != self.id);
        imp::kill(self.id);
        imp::release(self.id);
        #[cfg(windows)]
        if let Ok(mut ac) = self.app_container.lock() {
            drop(ac.take());
        }
        #[cfg(target_os = "linux")]
        if let Ok(mut leaf) = self.cgroup_leaf.lock() {
            drop(leaf.take());
        }
        #[cfg(unix)]
        if let Ok(mut lifeline) = self.lifeline_write.lock() {
            if let Some(w) = lifeline.take() {
                use std::os::unix::io::AsRawFd;
                unsafe {
                    let _ = libc::write(w.as_raw_fd(), b"0".as_ptr().cast::<libc::c_void>(), 1);
                }
                drop(w);
            }
        }
    }
}

impl Drop for TreeInner {
    fn drop(&mut self) {
        self.kill_and_release_once(true);
    }
}

/// The process tree of one task: a supervised process group (Unix) or a Job Object (Windows).
///
/// Create it with [`ConfiguredCommand::attach`] right after the child is spawned. Dropping it
/// kills whatever is still running in the tree and closes the lifeline pipe write end.
pub(crate) struct ProcessTree {
    inner: Option<Arc<TreeInner>>,
    #[cfg(windows)]
    fallback_app_container:
        Option<Arc<Mutex<Option<crate::isolation::windows::WindowsAppContainerGuard>>>>,
}

impl ProcessTree {
    /// Starts tracking the tree of the freshly spawned `child`. If the platform cannot provide a
    /// container (see [`Self::is_tracked`]) the tree is empty and the caller must fall back to
    /// signalling the leader.
    pub(crate) fn attach(child: &SpawnedChild, configured: ConfiguredCommand) -> Self {
        #[cfg(unix)]
        let mut unix = configured.unix;
        #[cfg(windows)]
        let mut app_container = configured.app_container;
        #[cfg(not(any(unix, windows)))]
        let _ = configured;

        #[cfg(unix)]
        let id = imp::attach(child, &unix);
        #[cfg(not(unix))]
        let id = imp::attach(child);

        let inner = id.map(|id| {
            live().push(id);
            Arc::new(TreeInner {
                id,
                done: AtomicBool::new(false),
                kill_lock: Mutex::new(()),
                #[cfg(windows)]
                spawned_at: std::time::Instant::now(),
                #[cfg(windows)]
                app_container: Mutex::new(app_container.take()),
                #[cfg(unix)]
                lifeline_write: Mutex::new(unix.lifeline_write.take()),
                #[cfg(target_os = "linux")]
                cgroup_leaf: Mutex::new(unix.cgroup_leaf.take()),
            })
        });
        Self {
            inner,
            #[cfg(windows)]
            fallback_app_container: app_container.map(|ac| Arc::new(Mutex::new(Some(ac)))),
        }
    }

    /// Whether the whole tree (not only the leader) can be reached.
    pub(crate) fn is_tracked(&self) -> bool {
        self.inner.is_some()
    }

    /// Asks the tree to stop (`SIGTERM` to the group). Returns whether a request could be made;
    /// on Windows there is no such request and this is `false`.
    pub(crate) fn request_stop(&self) -> bool {
        self.inner
            .as_ref()
            .is_some_and(|inner| !inner.done.load(Ordering::Acquire) && imp::request_stop(inner.id))
    }

    /// Kills every process of the tree on a blocking thread so neither the process-table sweep on
    /// Unix nor Job Object termination/close on Windows ever blocks a Tokio runtime worker thread,
    /// while serialising `imp::kill` and `imp::release` through `TreeInner` so a Windows Job
    /// Object handle is never closed before `TerminateJobObject` completes. Safe to repeat.
    ///
    /// `live_leader` is the leader's PID, to be passed only while it has not been reaped: when the
    /// platform gave us no container (a Windows process that could not be put into a job) it is
    /// all there is to go on, and `taskkill /F /T` takes the leader and its children.
    pub(crate) async fn kill_async(&self, live_leader: Option<u32>) {
        match self.inner.as_ref() {
            Some(inner) => {
                if !inner.done.load(Ordering::Acquire) {
                    let inner = Arc::clone(inner);
                    let child_already_exited = live_leader.is_none();
                    let _ = tokio::task::spawn_blocking(move || {
                        inner.kill_and_release_once(child_already_exited);
                    })
                    .await;
                }
            }
            None => {
                kill_by_leader(live_leader);
                #[cfg(windows)]
                if let Some(fb) = &self.fallback_app_container {
                    if let Ok(mut ac) = fb.lock() {
                        drop(ac.take());
                    }
                }
            }
        }
    }
}

impl Drop for ProcessTree {
    fn drop(&mut self) {
        if let Some(inner) = self.inner.take() {
            if !inner.done.load(Ordering::Acquire) {
                if let Ok(runtime) = tokio::runtime::Handle::try_current() {
                    runtime.spawn_blocking(move || {
                        inner.kill_and_release_once(false);
                    });
                    return;
                }
                inner.kill_and_release_once(false);
            }
        }
        #[cfg(windows)]
        if let Some(fb) = self.fallback_app_container.take() {
            if let Ok(mut ac) = fb.lock() {
                drop(ac.take());
            }
        }
    }
}

#[cfg(windows)]
fn kill_by_leader(live_leader: Option<u32>) {
    let Some(pid) = live_leader else {
        return;
    };
    let taskkill = move || {
        // Acquire `WINDOWS_SPAWN_LOCK` so `taskkill` cannot race with the brief pipe-inheritance
        // or `SetProcessWindowStation` window in `spawn_with_security_capabilities` (M5, N7).
        let mut child = {
            let _spawn_guard = crate::isolation::windows::WINDOWS_SPAWN_LOCK
                .lock()
                .unwrap_or_else(|poisoned| poisoned.into_inner());
            std::process::Command::new("taskkill")
                .args(["/F", "/T", "/PID", &pid.to_string()])
                .stdin(std::process::Stdio::null())
                .stdout(std::process::Stdio::null())
                .stderr(std::process::Stdio::null())
                .spawn()
        };
        if let Ok(ref mut c) = child {
            let _ = c.wait();
        }
    };
    // Starting and waiting for a process must not happen on a runtime thread.
    match tokio::runtime::Handle::try_current() {
        Ok(runtime) => {
            runtime.spawn_blocking(taskkill);
        }
        Err(_) => taskkill(),
    }
}

#[cfg(not(windows))]
fn kill_by_leader(live_leader: Option<u32>) {
    // Unix always has a group when there is a live leader; nothing else to try.
    let _ = live_leader;
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_priority_parsing_accepts_the_documented_spellings() {
        for (text, expected) in [
            ("normal", ProcessPriority::Normal),
            ("NORMAL", ProcessPriority::Normal),
            ("below-normal", ProcessPriority::BelowNormal),
            ("below_normal", ProcessPriority::BelowNormal),
            ("low", ProcessPriority::BelowNormal),
            (" idle ", ProcessPriority::Idle),
        ] {
            assert_eq!(text.parse::<ProcessPriority>(), Ok(expected), "{text:?}");
        }
        assert!("realtime".parse::<ProcessPriority>().is_err());
        assert!("".parse::<ProcessPriority>().is_err());
        assert_eq!(ProcessPriority::default(), ProcessPriority::BelowNormal);
        for p in [
            ProcessPriority::Normal,
            ProcessPriority::BelowNormal,
            ProcessPriority::Idle,
        ] {
            assert_eq!(p.as_str().parse::<ProcessPriority>(), Ok(p));
        }
    }

    #[test]
    fn test_task_env_needle_matches_exact_nul_delimited_entry() {
        let run_token = uuid::Uuid::new_v4();
        let needle = build_task_env_needle(run_token);
        let expected = format!("RUSTY_GRID_RUN_TOKEN={run_token}\0");
        assert_eq!(&needle[..], expected.as_bytes());

        // Exact match at start of buffer.
        assert!(env_block_contains_needle(&needle, &needle));

        // Match after preceding NUL-terminated entries.
        let mut block = Vec::new();
        block.extend_from_slice(b"PATH=/usr/bin\0HOME=/tmp\0");
        block.extend_from_slice(&needle);
        block.extend_from_slice(b"USER=worker\0");
        assert!(env_block_contains_needle(&block, &needle));
        assert!(!env_block_contains_needle_chunk(&needle, &needle, false));
        assert!(env_block_contains_needle_chunk(&block, &needle, false));

        // Substring suffix of another variable name must not match.
        let fake = format!("NOT_{expected}").into_bytes();
        assert!(!env_block_contains_needle(&fake, &needle));

        // Different run token UUID must not match.
        let other_token = uuid::Uuid::new_v4();
        let other_needle = build_task_env_needle(other_token);
        assert!(!env_block_contains_needle(&block, &other_needle));
    }

    #[test]
    fn test_format_u32_decimal() {
        let mut buf = [0u8; 16];
        let n = format_u32_decimal(0, &mut buf);
        assert_eq!(&buf[..n], b"0");
        let n = format_u32_decimal(1234567, &mut buf);
        assert_eq!(&buf[..n], b"1234567");
    }

    #[cfg(unix)]
    #[test]
    fn test_priority_maps_to_nice_increments() {
        assert_eq!(ProcessPriority::Normal.unix_nice(), 0);
        assert_eq!(ProcessPriority::BelowNormal.unix_nice(), 10);
        assert_eq!(ProcessPriority::Idle.unix_nice(), 19);
    }

    #[cfg(unix)]
    #[test]
    fn test_lifeline_pipe_sets_cloexec_on_both_ends() {
        use std::os::unix::io::AsRawFd;

        let (read_fd, write_fd) = imp::create_lifeline_pipe().expect("create lifeline pipe");
        for fd in [read_fd.as_raw_fd(), write_fd.as_raw_fd()] {
            let flags = unsafe { libc::fcntl(fd, libc::F_GETFD) };
            assert!(flags >= 0, "fcntl(F_GETFD) failed on fd {fd}");
            assert_ne!(
                flags & libc::FD_CLOEXEC,
                0,
                "FD_CLOEXEC must be set on lifeline pipe fd {fd} (flags={flags:#x})"
            );
        }
    }

    #[cfg(unix)]
    #[test]
    fn test_signalling_a_non_group_id_is_refused_instead_of_hitting_our_own_group() {
        // killpg(0) would signal the caller's own process group, killpg(1) init's.
        assert!(!imp::request_stop(imp::RawId::for_pgid_only(0)));
        assert!(!imp::request_stop(imp::RawId::for_pgid_only(1)));
        assert!(!imp::request_stop(imp::RawId::for_pgid_only(-5)));
        imp::kill(imp::RawId::for_pgid_only(0));
        imp::kill(imp::RawId::for_pgid_only(1));
    }
}
