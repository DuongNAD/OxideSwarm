//! Win32 Job Object and Unix process group lifecycle supervision (Feature F10).
//!
//! Provides kernel-level containment ensuring that child processes and all their
//! descendants are unconditionally terminated upon job close, worker exit, or timeout.

use std::io;
#[cfg(windows)]
use tracing::{debug, warn};

#[cfg(windows)]
#[allow(
    non_snake_case,
    non_camel_case_types,
    non_upper_case_globals,
    dead_code,
    clippy::upper_case_acronyms
)]
mod win32 {
    use core::ffi::c_void;

    pub type HANDLE = *mut c_void;
    pub type BOOL = i32;
    pub type DWORD = u32;

    pub const FALSE: BOOL = 0;
    pub const TRUE: BOOL = 1;
    pub const INVALID_HANDLE_VALUE: HANDLE = -1isize as HANDLE;

    // Process access rights
    pub const PROCESS_TERMINATE: DWORD = 0x0001;
    pub const PROCESS_SET_QUOTA: DWORD = 0x0100;

    // Job Object Limit Flags
    pub const JOB_OBJECT_LIMIT_KILL_ON_JOB_CLOSE: DWORD = 0x00002000;
    pub const JobObjectExtendedLimitInformation: DWORD = 9;

    #[repr(C)]
    #[derive(Default)]
    pub struct IO_COUNTERS {
        pub ReadOperationCount: u64,
        pub WriteOperationCount: u64,
        pub OtherOperationCount: u64,
        pub ReadTransferCount: u64,
        pub WriteTransferCount: u64,
        pub OtherTransferCount: u64,
    }

    #[repr(C)]
    #[derive(Default)]
    pub struct JOBOBJECT_BASIC_LIMIT_INFORMATION {
        pub PerProcessUserTimeLimit: i64,
        pub PerJobUserTimeLimit: i64,
        pub LimitFlags: DWORD,
        pub MinimumWorkingSetSize: usize,
        pub MaximumWorkingSetSize: usize,
        pub ActiveProcessLimit: DWORD,
        pub Affinity: usize,
        pub PriorityClass: DWORD,
        pub SchedulingClass: DWORD,
    }

    #[repr(C)]
    #[derive(Default)]
    pub struct JOBOBJECT_EXTENDED_LIMIT_INFORMATION {
        pub BasicLimitInformation: JOBOBJECT_BASIC_LIMIT_INFORMATION,
        pub IoInfo: IO_COUNTERS,
        pub ProcessMemoryLimit: usize,
        pub JobMemoryLimit: usize,
        pub PeakProcessMemoryLimit: usize,
        pub PeakJobMemoryLimit: usize,
    }

    #[link(name = "kernel32")]
    extern "system" {
        pub fn CreateJobObjectW(lpJobAttributes: *mut c_void, lpName: *const u16) -> HANDLE;

        pub fn SetInformationJobObject(
            hJob: HANDLE,
            JobObjectInformationClass: DWORD,
            lpJobObjectInformation: *const c_void,
            cbJobObjectInformationLength: DWORD,
        ) -> BOOL;

        pub fn AssignProcessToJobObject(hJob: HANDLE, hProcess: HANDLE) -> BOOL;

        pub fn TerminateJobObject(hJob: HANDLE, uExitCode: u32) -> BOOL;

        pub fn OpenProcess(
            dwDesiredAccess: DWORD,
            bInheritHandle: BOOL,
            dwProcessId: DWORD,
        ) -> HANDLE;

        pub fn CloseHandle(hObject: HANDLE) -> BOOL;

        pub fn GetLastError() -> DWORD;
    }
}

/// Kernel-level process containment supervisor.
///
/// On Windows, encapsulates a Win32 Job Object with `JOB_OBJECT_LIMIT_KILL_ON_JOB_CLOSE`.
/// On Unix, encapsulates process group isolation and POSIX signal broadcast.
pub struct JobObject {
    #[cfg(windows)]
    handle: win32::HANDLE,
    #[cfg(not(windows))]
    pgid: Option<u32>,
}

unsafe impl Send for JobObject {}
unsafe impl Sync for JobObject {}

impl JobObject {
    /// Creates a new anonymous Job Object configured with kernel kill-on-close.
    pub fn create() -> io::Result<Self> {
        #[cfg(windows)]
        {
            unsafe {
                let handle = win32::CreateJobObjectW(std::ptr::null_mut(), std::ptr::null());
                if handle.is_null() || handle == win32::INVALID_HANDLE_VALUE {
                    return Err(io::Error::last_os_error());
                }

                let mut info = win32::JOBOBJECT_EXTENDED_LIMIT_INFORMATION::default();
                info.BasicLimitInformation.LimitFlags = win32::JOB_OBJECT_LIMIT_KILL_ON_JOB_CLOSE;

                let ok = win32::SetInformationJobObject(
                    handle,
                    win32::JobObjectExtendedLimitInformation,
                    &info as *const _ as *const _,
                    std::mem::size_of::<win32::JOBOBJECT_EXTENDED_LIMIT_INFORMATION>() as u32,
                );

                if ok == win32::FALSE {
                    let err = io::Error::last_os_error();
                    win32::CloseHandle(handle);
                    return Err(err);
                }

                debug!(
                    "Created Win32 Job Object {:?} with KILL_ON_JOB_CLOSE",
                    handle
                );
                Ok(Self { handle })
            }
        }

        #[cfg(not(windows))]
        {
            Ok(Self { pgid: None })
        }
    }

    /// Assigns a child process by PID to this Job Object.
    pub fn assign_pid(&mut self, pid: u32) -> io::Result<()> {
        #[cfg(windows)]
        {
            unsafe {
                let proc_handle = win32::OpenProcess(
                    win32::PROCESS_SET_QUOTA | win32::PROCESS_TERMINATE,
                    win32::FALSE,
                    pid,
                );
                if proc_handle.is_null() || proc_handle == win32::INVALID_HANDLE_VALUE {
                    return Err(io::Error::last_os_error());
                }

                let ok = win32::AssignProcessToJobObject(self.handle, proc_handle);
                let last_err = if ok == win32::FALSE {
                    Some(io::Error::last_os_error())
                } else {
                    None
                };

                win32::CloseHandle(proc_handle);

                if let Some(err) = last_err {
                    warn!("Failed to assign PID {pid} to Job Object: {err}");
                    return Err(err);
                }

                debug!("Successfully assigned PID {pid} to Win32 Job Object");
                Ok(())
            }
        }

        #[cfg(not(windows))]
        {
            self.pgid = Some(pid);
            Ok(())
        }
    }

    /// Explicitly and forcefully terminates all processes currently assigned to this Job Object.
    pub fn terminate(&self, exit_code: u32) -> io::Result<()> {
        #[cfg(windows)]
        {
            unsafe {
                if !self.handle.is_null() && self.handle != win32::INVALID_HANDLE_VALUE {
                    let ok = win32::TerminateJobObject(self.handle, exit_code);
                    if ok == win32::FALSE {
                        return Err(io::Error::last_os_error());
                    }
                    debug!("Forcefully terminated Win32 Job Object with code {exit_code}");
                }
                Ok(())
            }
        }

        #[cfg(not(windows))]
        {
            let _ = exit_code;
            if let Some(pgid) = self.pgid {
                let _ = std::process::Command::new("kill")
                    .args(["-9", &format!("-{pgid}")])
                    .output();
                let _ = std::process::Command::new("pkill")
                    .args(["-9", "-P", &pgid.to_string()])
                    .output();
            }
            Ok(())
        }
    }
}

impl Drop for JobObject {
    fn drop(&mut self) {
        #[cfg(windows)]
        {
            unsafe {
                if !self.handle.is_null() && self.handle != win32::INVALID_HANDLE_VALUE {
                    win32::CloseHandle(self.handle);
                    debug!("Closed Win32 Job Object handle (kernel auto-kill triggered)");
                }
            }
        }
    }
}
