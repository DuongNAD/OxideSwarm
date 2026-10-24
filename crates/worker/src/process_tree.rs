//! Supervision of a task's whole process tree, and the priority task processes run at.
//!
//! A task is rarely one process: `cargo build`, `make -j8` or a shell script fan out into
//! grandchildren, and a careless one leaves a daemon behind. Taking such a tree down by walking a
//! snapshot of the process table is racy (anything forked after the snapshot, or already
//! re-parented to init, is missed) and can hit an unrelated process that reuses a PID. So each
//! task process starts in a container the operating system itself tracks:
//!
//! * **Unix**: the child becomes the leader of a new process group (`process_group(0)`). The group
//!   id is the leader's PID, stays reserved while *any* member is alive, and `killpg` reaches every
//!   member in one call, grandchildren included. Stopping is `SIGTERM` to the group, a short
//!   grace, then `SIGKILL` to the group.
//! * **Windows**: the child is assigned to a Job Object created with
//!   `JOB_OBJECT_LIMIT_KILL_ON_JOB_CLOSE`. `TerminateJobObject` ends every process in the job, and
//!   closing the last handle to it (worker exit, or a worker crash: the OS closes the handle) does
//!   the same. The assignment happens right after `CreateProcess`, so a process spawned in the
//!   first microseconds of the child's life could escape; if the assignment fails altogether the
//!   caller falls back to `taskkill /F /T`.
//!
//! Every live tree is registered, so [`terminate_all_task_process_trees`] can take them all down
//! from a signal handler. Dropping a task's `ProcessTree` kills what is left of it, which is what makes
//! an aborted task future (or a runtime that is shut down) clean up after itself.
//!
//! What is *not* covered: after a worker crash (SIGKILL, OOM kill) on Unix nothing kills the
//! groups; they are left running until they finish. Memory and disk quotas are not enforced
//! either (see [`ProcessPriority`] for the one limit that is: CPU priority).

use std::str::FromStr;
use std::sync::atomic::{AtomicBool, Ordering};
use std::sync::{Mutex, MutexGuard};

use tracing::warn;

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

/// Prepares `cmd` so that the process it spawns leads its own process group (Unix) and runs at
/// `priority`. Must be called before `spawn`.
pub(crate) fn configure_command(cmd: &mut tokio::process::Command, priority: ProcessPriority) {
    #[cfg(unix)]
    {
        cmd.process_group(0);
        let nice = priority.unix_nice();
        unsafe {
            cmd.pre_exec(move || {
                #[cfg(target_os = "linux")]
                {
                    libc::prctl(libc::PR_SET_PDEATHSIG, libc::SIGKILL);
                }
                if nice > 0 {
                    libc::setpriority(libc::PRIO_PROCESS, 0, nice);
                }
                Ok(())
            });
        }
    }

    #[cfg(windows)]
    {
        const CREATE_SUSPENDED: u32 = 0x0000_0004;
        let flag = priority.windows_class_flag() | CREATE_SUSPENDED;
        cmd.creation_flags(flag);
    }

    #[cfg(not(any(unix, windows)))]
    {
        let _ = (cmd, priority);
    }
}

#[cfg(unix)]
mod imp {
    use tokio::process::Child;

    /// A process group id.
    pub(super) type RawId = libc::pid_t;

    pub(super) fn attach(child: &Child) -> Option<RawId> {
        // The child leads its own group (see `configure_command`), so the group id is its PID.
        // Never 0 or 1: `killpg(0)` would signal the worker's own group.
        child
            .id()
            .and_then(|pid| RawId::try_from(pid).ok())
            .filter(|pgid| *pgid > 1)
    }

    pub(super) fn request_stop(id: RawId) -> bool {
        signal(id, libc::SIGTERM)
    }

    pub(super) fn kill(id: RawId) {
        signal(id, libc::SIGKILL);
    }

    pub(super) fn release(_id: RawId) {}

    fn signal(id: RawId, sig: libc::c_int) -> bool {
        if id <= 1 {
            return false;
        }
        // SAFETY: killpg only sends a signal. `id` is the group this module created for a task;
        // the group id cannot be recycled while a member of the group is alive.
        unsafe { libc::killpg(id, sig) == 0 }
    }
}

#[cfg(windows)]
mod imp {
    use tokio::process::Child;
    use windows_sys::Win32::Foundation::{CloseHandle, HANDLE};
    use windows_sys::Win32::System::JobObjects::{
        AssignProcessToJobObject, CreateJobObjectW, JobObjectExtendedLimitInformation,
        SetInformationJobObject, TerminateJobObject, JOBOBJECT_EXTENDED_LIMIT_INFORMATION,
        JOB_OBJECT_LIMIT_KILL_ON_JOB_CLOSE,
    };
    use windows_sys::Win32::System::LibraryLoader::{GetModuleHandleA, GetProcAddress};

    /// A Job Object handle, as an integer so it can sit in a `static`.
    pub(super) type RawId = isize;

    unsafe fn resume_process(process: HANDLE) {
        type NtResumeProcessFn = unsafe extern "system" fn(HANDLE) -> i32;
        let ntdll = GetModuleHandleA(c"ntdll.dll".as_ptr() as *const u8);
        if !ntdll.is_null() {
            let func = GetProcAddress(ntdll, c"NtResumeProcess".as_ptr() as *const u8);
            if let Some(func) = func {
                let nt_resume: NtResumeProcessFn = std::mem::transmute(func);
                let _ = nt_resume(process);
            }
        }
    }

    pub(super) fn attach(child: &Child) -> Option<RawId> {
        let process: HANDLE = child.raw_handle()?;
        // SAFETY: plain Win32 calls on a handle created here and on the live process handle that
        // `child` owns; the job handle is closed on every failure path and otherwise owned by the
        // `ProcessTree` that stores the returned id.
        unsafe {
            let job = CreateJobObjectW(std::ptr::null(), std::ptr::null());
            if job.is_null() {
                resume_process(process);
                return None;
            }
            let mut info: JOBOBJECT_EXTENDED_LIMIT_INFORMATION = std::mem::zeroed();
            info.BasicLimitInformation.LimitFlags = JOB_OBJECT_LIMIT_KILL_ON_JOB_CLOSE;
            let configured = SetInformationJobObject(
                job,
                JobObjectExtendedLimitInformation,
                &info as *const JOBOBJECT_EXTENDED_LIMIT_INFORMATION as *const core::ffi::c_void,
                std::mem::size_of::<JOBOBJECT_EXTENDED_LIMIT_INFORMATION>() as u32,
            );
            if configured == 0 || AssignProcessToJobObject(job, process) == 0 {
                CloseHandle(job);
                resume_process(process);
                return None;
            }
            resume_process(process);
            Some(job as RawId)
        }
    }

    /// Windows has no polite group signal; the caller goes straight to `kill`.
    pub(super) fn request_stop(_id: RawId) -> bool {
        false
    }

    pub(super) fn kill(id: RawId) {
        // SAFETY: `id` is a job handle that has not been released (see `ProcessTree::drop`).
        unsafe {
            TerminateJobObject(id as HANDLE, 1);
        }
    }

    pub(super) fn release(id: RawId) {
        // SAFETY: the last use of the handle; `kill_all` cannot race it (both hold `LIVE`).
        unsafe {
            CloseHandle(id as HANDLE);
        }
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

/// Trees that are alive right now (group ids on Unix, job handles on Windows).
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
    let live = live();
    for id in live.iter() {
        imp::kill(*id);
    }
    live.len()
}

/// The process tree of one task: a process group (Unix) or a Job Object (Windows).
///
/// Create it with [`ProcessTree::attach`] right after the child is spawned. Dropping it kills
/// whatever is still running in the tree.
pub(crate) struct ProcessTree {
    id: Option<imp::RawId>,
    killed: AtomicBool,
}

impl ProcessTree {
    /// Starts tracking the tree of the freshly spawned `child`. If the platform cannot provide a
    /// container (see [`Self::is_tracked`]) the tree is empty and the caller must fall back to
    /// signalling the leader.
    pub(crate) fn attach(child: &tokio::process::Child) -> Self {
        let id = imp::attach(child);
        if let Some(id) = id {
            live().push(id);
        }
        Self {
            id,
            killed: AtomicBool::new(false),
        }
    }

    /// Whether the whole tree (not only the leader) can be reached.
    pub(crate) fn is_tracked(&self) -> bool {
        self.id.is_some()
    }

    /// Asks the tree to stop (`SIGTERM` to the group). Returns whether a request could be made;
    /// on Windows there is no such request and this is `false`.
    pub(crate) fn request_stop(&self) -> bool {
        self.id.is_some_and(imp::request_stop)
    }

    /// Kills every process of the tree. Cheap and non-blocking (one system call), safe to repeat.
    ///
    /// `live_leader` is the leader's PID, to be passed only while it has not been reaped: when the
    /// platform gave us no container (a Windows process that could not be put into a job) it is
    /// all there is to go on, and `taskkill /F /T` takes the leader and its children.
    pub(crate) fn kill(&self, live_leader: Option<u32>) {
        self.killed.store(true, Ordering::Release);
        match self.id {
            Some(id) => imp::kill(id),
            None => kill_by_leader(live_leader),
        }
    }
}

impl Drop for ProcessTree {
    fn drop(&mut self) {
        let Some(id) = self.id else {
            return;
        };
        // Hold the registry across kill and release: `terminate_all_task_process_trees` must not
        // find an id whose Windows handle has just been closed.
        let mut live = live();
        live.retain(|registered| *registered != id);
        if !self.killed.load(Ordering::Acquire) {
            imp::kill(id);
        }
        imp::release(id);
    }
}

#[cfg(windows)]
fn kill_by_leader(live_leader: Option<u32>) {
    let Some(pid) = live_leader else {
        return;
    };
    let taskkill = move || {
        let _ = std::process::Command::new("taskkill")
            .args(["/F", "/T", "/PID", &pid.to_string()])
            .stdin(std::process::Stdio::null())
            .stdout(std::process::Stdio::null())
            .stderr(std::process::Stdio::null())
            .status();
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

    #[cfg(unix)]
    #[test]
    fn test_priority_maps_to_nice_increments() {
        assert_eq!(ProcessPriority::Normal.unix_nice(), 0);
        assert_eq!(ProcessPriority::BelowNormal.unix_nice(), 10);
        assert_eq!(ProcessPriority::Idle.unix_nice(), 19);
    }

    #[cfg(unix)]
    #[test]
    fn test_signalling_a_non_group_id_is_refused_instead_of_hitting_our_own_group() {
        // killpg(0) would signal the caller's own process group, killpg(1) init's.
        assert!(!imp::request_stop(0));
        assert!(!imp::request_stop(1));
        assert!(!imp::request_stop(-5));
        imp::kill(0);
        imp::kill(1);
    }
}
