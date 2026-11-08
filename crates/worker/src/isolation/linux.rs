//! The Linux implementation of `--isolation fs|fs+net` (design 0033 D2): Landlock for files,
//! `seccomp` for sockets and a few dangerous calls, a private user and network namespace under
//! `fs+net`.
//!
//! Unlike macOS there is no wrapper program: the policy is applied by a `pre_exec` hook in the
//! task leader, after FEAT-2's hook has forked the lifeline supervisor and moved the leader into
//! its cgroup (so the supervisor is never restricted) and right before `execve`. Everything the
//! hook needs is built before `spawn` (path descriptors opened with `O_PATH`, the BPF program,
//! the id-map texts), and the hook itself makes raw system calls only: no allocation, no locks,
//! no logging, because it runs between `fork` and `exec` of a multi-threaded process.
//!
//! What each mechanism does and does not do (all of it checked in CI on `ubuntu-latest`, see
//! `tests/test_feat3_os_isolation.rs`):
//!
//! * **Landlock** denies every file access that no rule allows: read, write, execute, create,
//!   remove, rename between directories (ABI 2), truncate (ABI 3) and `ioctl` on devices (ABI 5).
//!   It does not restrict `stat`, path lookup, or **connecting to a pathname Unix socket**, so
//!   sockets are closed by seccomp instead. From ABI 6 it also stops signals to processes outside
//!   the task's domain and connects to abstract Unix sockets of other domains. It always stops
//!   `ptrace` (and so `/proc/<pid>/environ`, `mem`, `fd`) of processes outside the task's domain.
//! * **seccomp** refuses `socket(2)` for `AF_UNIX` (`fs`) or for every domain (`fs+net`; netlink
//!   included, since without a private network namespace it would list the host's sockets),
//!   `keyctl`/`add_key`/`request_key`, `io_uring_*` (which could create sockets behind the filter's back) and the
//!   `TIOCSTI` ioctl; a foreign system-call ABI (32-bit, x32) kills the process, because the
//!   filter matches native numbers only.
//! * **`unshare(CLONE_NEWUSER | CLONE_NEWNET)`** (`fs+net`, best effort): an empty network
//!   namespace, so even a socket the filter missed reaches nothing. Hosts that forbid
//!   unprivileged user namespaces (sysctl, AppArmor) keep seccomp and Landlock only.

#![cfg_attr(not(target_os = "linux"), allow(dead_code))]

use std::path::{Path, PathBuf};

use super::{canonical, IsolationLevel};

// ---- Landlock constants (include/uapi/linux/landlock.h) ---------------------------------------

const LANDLOCK_CREATE_RULESET_VERSION: u32 = 1;

/// Filesystem access rights, by the ABI that introduced them.
mod fs {
    pub const EXECUTE: u64 = 1 << 0;
    pub const WRITE_FILE: u64 = 1 << 1;
    pub const READ_FILE: u64 = 1 << 2;
    pub const READ_DIR: u64 = 1 << 3;
    /// `REMOVE_DIR` .. `MAKE_SYM`: bits 4..=12, ABI 1.
    pub const ABI1_ALL: u64 = (1 << 13) - 1;
    pub const REFER: u64 = 1 << 13; // ABI 2
    pub const TRUNCATE: u64 = 1 << 14; // ABI 3
    pub const IOCTL_DEV: u64 = 1 << 15; // ABI 5
    /// The rights that make sense for a file (a rule on a file with any other bit is `EINVAL`).
    pub const FILE_RIGHTS: u64 = EXECUTE | WRITE_FILE | READ_FILE | TRUNCATE | IOCTL_DEV;
}

/// Network access rights (ABI 4).
mod net {
    pub const BIND_TCP: u64 = 1 << 0;
    pub const CONNECT_TCP: u64 = 1 << 1;
}

/// Scopes (ABI 6).
mod scope {
    pub const ABSTRACT_UNIX_SOCKET: u64 = 1 << 0;
    pub const SIGNAL: u64 = 1 << 1;
}

/// The filesystem rights a ruleset of `abi` handles (and therefore denies unless a rule allows).
fn handled_fs(abi: u32) -> u64 {
    let mut handled = fs::ABI1_ALL;
    if abi >= 2 {
        handled |= fs::REFER;
    }
    if abi >= 3 {
        handled |= fs::TRUNCATE;
    }
    if abi >= 5 {
        handled |= fs::IOCTL_DEV;
    }
    handled
}

/// TCP bind and connect are handled under `fs+net` from ABI 4; seccomp does not depend on it.
fn handled_net(abi: u32, level: IsolationLevel) -> u64 {
    if abi >= 4 && level == IsolationLevel::FsNet {
        net::BIND_TCP | net::CONNECT_TCP
    } else {
        0
    }
}

/// Signals and abstract Unix sockets are scoped from ABI 6.
fn scoped(abi: u32) -> u64 {
    if abi >= 6 {
        scope::ABSTRACT_UNIX_SOCKET | scope::SIGNAL
    } else {
        0
    }
}

/// The size of `struct landlock_ruleset_attr` for `abi` (the kernel zero-fills a shorter one).
fn attr_size(abi: u32) -> usize {
    if abi >= 6 {
        24
    } else if abi >= 4 {
        16
    } else {
        8
    }
}

/// What a rule is for.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
enum Want {
    /// System and toolchain files: read and execute.
    ReadExec,
    /// The task's own sandbox: everything.
    ReadWrite,
    /// A character device the task may use (`/dev/null`, `/dev/urandom`).
    Device,
}

/// The access bits of a rule of kind `want` on a directory or file, within what the ruleset
/// handles.
fn access_for(want: Want, is_dir: bool, handled: u64) -> u64 {
    let wanted = match (want, is_dir) {
        (Want::ReadExec, true) => fs::EXECUTE | fs::READ_FILE | fs::READ_DIR,
        (Want::ReadExec, false) => fs::EXECUTE | fs::READ_FILE,
        (Want::ReadWrite, true) => handled,
        (Want::ReadWrite, false) => handled & fs::FILE_RIGHTS,
        (Want::Device, _) => fs::READ_FILE | fs::WRITE_FILE | fs::TRUNCATE | fs::IOCTL_DEV,
    };
    wanted & handled
}

// ---- The policy's paths -------------------------------------------------------------------------

/// System locations a confined task may read and execute (those that exist on the host).
const SYSTEM_READ_ONLY: &[&str] = &[
    "/usr",
    "/bin",
    "/sbin",
    "/lib",
    "/lib32",
    "/lib64",
    "/libx32",
    "/opt",
    "/etc/ld.so.cache",
    "/etc/ld.so.conf",
    "/etc/ld.so.conf.d",
    "/etc/alternatives",
    "/etc/ssl",
    "/etc/ca-certificates",
    "/etc/pki",
    "/etc/resolv.conf",
    "/etc/hosts",
    "/etc/host.conf",
    "/etc/nsswitch.conf",
    "/etc/gai.conf",
    "/etc/services",
    "/etc/protocols",
    "/etc/passwd",
    "/etc/group",
    "/etc/localtime",
    "/etc/os-release",
    "/etc/terminfo",
    "/sys/devices/system/cpu",
    "/proc/cpuinfo",
    "/proc/meminfo",
    "/proc/stat",
    "/proc/version",
    "/proc/loadavg",
    "/proc/sys/kernel",
];

/// Devices a confined task may open for writing besides its sandbox.
const WRITABLE_DEVICES: &[&str] = &["/dev/null", "/dev/zero", "/dev/urandom", "/dev/random"];

/// The read-only roots of the policy: the system locations and the host's Rust toolchain. Only
/// `$CARGO_HOME/{bin,registry,git,config.toml,config}` are named, never `$CARGO_HOME` itself, so
/// `credentials` and `credentials.toml` stay out of reach.
fn read_only_roots(host: &super::HostDirs) -> Vec<PathBuf> {
    let mut roots: Vec<PathBuf> = SYSTEM_READ_ONLY.iter().map(|p| PathBuf::from(*p)).collect();
    roots.push(host.rustup_home.clone());
    for leaf in ["bin", "registry", "git", "config.toml", "config"] {
        roots.push(host.cargo_home.join(leaf));
    }
    roots
}

/// The paths to grant for `root` so that nothing in `protected` is reachable: `root` itself when
/// it contains no protected file, otherwise (recursively) its entries other than the protected
/// ones. Landlock can only allow, never carve a hole out of a granted directory, so this is how
/// a secret file inside a readable directory stays unreadable. Symlinks are skipped (a link is
/// only a pointer; its target is granted, or not, by its own path).
fn carve(root: &Path, protected: &[PathBuf], out: &mut Vec<PathBuf>) {
    if !protected.iter().any(|p| p.starts_with(root)) {
        out.push(root.to_path_buf());
        return;
    }
    if protected.iter().any(|p| p == root) {
        return;
    }
    let Ok(entries) = std::fs::read_dir(root) else {
        return;
    };
    for entry in entries.flatten() {
        match entry.file_type() {
            Ok(t) if !t.is_symlink() => carve(&entry.path(), protected, out),
            _ => {}
        }
    }
}

/// The read-only grants for `host`, with `protected` files cut out. Roots that do not exist are
/// left out.
fn read_only_grants(host: &super::HostDirs, protected: &[PathBuf]) -> Vec<PathBuf> {
    let protected: Vec<PathBuf> = protected
        .iter()
        .filter(|p| !p.as_os_str().is_empty())
        .map(|p| canonical(p))
        .collect();
    let mut grants = Vec::new();
    for root in read_only_roots(host) {
        let Ok(root) = root.canonicalize() else {
            continue;
        };
        carve(&root, &protected, &mut grants);
    }
    grants.sort();
    grants.dedup();
    grants
}

// ---- The seccomp filter ------------------------------------------------------------------------

/// One classic BPF instruction (`struct sock_filter`).
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
#[repr(C)]
struct SockFilter {
    code: u16,
    jt: u8,
    jf: u8,
    k: u32,
}

/// System-call numbers and the audit architecture of one native ABI.
struct ArchInfo {
    audit_arch: u32,
    /// x86_64 only: the x32 ABI shares the audit architecture and flags its numbers with a bit.
    x32_guard: bool,
    socket: u32,
    ioctl: u32,
    io_uring_setup: u32,
    io_uring_enter: u32,
    io_uring_register: u32,
    keyctl: u32,
    add_key: u32,
    request_key: u32,
}

#[allow(dead_code)]
const X86_64: ArchInfo = ArchInfo {
    audit_arch: 0xC000_003E,
    x32_guard: true,
    socket: 41,
    ioctl: 16,
    io_uring_setup: 425,
    io_uring_enter: 426,
    io_uring_register: 427,
    keyctl: 250,
    add_key: 248,
    request_key: 249,
};

#[allow(dead_code)]
const AARCH64: ArchInfo = ArchInfo {
    audit_arch: 0xC000_00B7,
    x32_guard: false,
    socket: 198,
    ioctl: 29,
    io_uring_setup: 425,
    io_uring_enter: 426,
    io_uring_register: 427,
    keyctl: 219,
    add_key: 217,
    request_key: 218,
};

#[cfg(target_arch = "x86_64")]
const NATIVE: Option<&ArchInfo> = Some(&X86_64);
#[cfg(target_arch = "aarch64")]
const NATIVE: Option<&ArchInfo> = Some(&AARCH64);
#[cfg(not(any(target_arch = "x86_64", target_arch = "aarch64")))]
const NATIVE: Option<&ArchInfo> = None;

const BPF_LD_W_ABS: u16 = 0x20;
const BPF_JEQ_K: u16 = 0x15;
const BPF_JGE_K: u16 = 0x35;
const BPF_RET_K: u16 = 0x06;

const SECCOMP_RET_ALLOW: u32 = 0x7fff_0000;
const SECCOMP_RET_KILL_PROCESS: u32 = 0x8000_0000;
const SECCOMP_RET_ERRNO: u32 = 0x0005_0000;

const OFFSET_NR: u32 = 0;
const OFFSET_ARCH: u32 = 4;
/// Offset of the low 32 bits of argument `i` (little-endian architectures only).
const fn offset_arg(i: u32) -> u32 {
    16 + 8 * i
}

const AF_UNIX: u32 = 1;
/// `ioctl` request that pushes a character into a terminal's input queue.
const TIOCSTI: u32 = 0x5412;
const X32_SYSCALL_BIT: u32 = 0x4000_0000;
const EPERM: u16 = 1;
const EACCES: u16 = 13;
const ENOSYS: u16 = 38;

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
enum Verdict {
    Allow,
    Kill,
    Errno(u16),
}

impl Verdict {
    fn action(self) -> u32 {
        match self {
            Verdict::Allow => SECCOMP_RET_ALLOW,
            Verdict::Kill => SECCOMP_RET_KILL_PROCESS,
            Verdict::Errno(e) => SECCOMP_RET_ERRNO | u32::from(e),
        }
    }
}

/// Where a conditional jump goes: on to the next instruction, over `n` instructions, or to a
/// verdict placed after the body.
#[derive(Debug, Clone, Copy)]
enum Jump {
    Fall,
    Skip(u8),
    Ret(Verdict),
}

struct Op {
    code: u16,
    k: u32,
    jt: Jump,
    jf: Jump,
}

fn load(offset: u32) -> Op {
    Op {
        code: BPF_LD_W_ABS,
        k: offset,
        jt: Jump::Fall,
        jf: Jump::Fall,
    }
}

fn jeq(k: u32, jt: Jump, jf: Jump) -> Op {
    Op {
        code: BPF_JEQ_K,
        k,
        jt,
        jf,
    }
}

/// The seccomp program for `level`:
///
/// 1. a foreign architecture (or the x32 ABI) kills the process;
/// 2. `io_uring_setup|enter|register` fail with `ENOSYS`;
/// 3. `ioctl(_, TIOCSTI, _)` fails with `EPERM`;
/// 4. `socket(domain, ...)` fails with `EACCES` for `AF_UNIX` (`fs`) or for every domain but
///    `AF_NETLINK` too (`fs+net`: no socket at all, because without a private network namespace
///    netlink would list the host's sockets, routes and addresses);
/// 5. `keyctl`, `add_key` and `request_key` fail with `EPERM` (the worker user's keyring);
/// 6. everything else is allowed. `socketpair` stays allowed: it cannot reach anything outside.
fn build_filter(arch: &ArchInfo, level: IsolationLevel) -> Vec<SockFilter> {
    let mut ops = vec![
        load(OFFSET_ARCH),
        jeq(arch.audit_arch, Jump::Fall, Jump::Ret(Verdict::Kill)),
        load(OFFSET_NR),
    ];
    if arch.x32_guard {
        ops.push(Op {
            code: BPF_JGE_K,
            k: X32_SYSCALL_BIT,
            jt: Jump::Ret(Verdict::Kill),
            jf: Jump::Fall,
        });
    }
    for nr in [
        arch.io_uring_setup,
        arch.io_uring_enter,
        arch.io_uring_register,
    ] {
        ops.push(jeq(nr, Jump::Ret(Verdict::Errno(ENOSYS)), Jump::Fall));
    }
    for nr in [arch.keyctl, arch.add_key, arch.request_key] {
        ops.push(jeq(nr, Jump::Ret(Verdict::Errno(EPERM)), Jump::Fall));
    }
    // ioctl: only TIOCSTI is refused. A match always returns; a miss skips the two checks.
    ops.push(jeq(arch.ioctl, Jump::Fall, Jump::Skip(2)));
    ops.push(load(offset_arg(1)));
    ops.push(jeq(
        TIOCSTI,
        Jump::Ret(Verdict::Errno(EPERM)),
        Jump::Ret(Verdict::Allow),
    ));
    // socket: every domain under fs+net, AF_UNIX under fs.
    if level == IsolationLevel::FsNet {
        ops.push(jeq(
            arch.socket,
            Jump::Ret(Verdict::Errno(EACCES)),
            Jump::Fall,
        ));
    } else {
        ops.push(jeq(arch.socket, Jump::Fall, Jump::Skip(2)));
        ops.push(load(offset_arg(0)));
        ops.push(jeq(
            AF_UNIX,
            Jump::Ret(Verdict::Errno(EACCES)),
            Jump::Ret(Verdict::Allow),
        ));
    }

    // The verdicts follow the body; falling off the end of the body allows.
    let mut verdicts = vec![Verdict::Allow, Verdict::Kill];
    for op in &ops {
        for jump in [op.jt, op.jf] {
            if let Jump::Ret(v) = jump {
                if !verdicts.contains(&v) {
                    verdicts.push(v);
                }
            }
        }
    }
    let body = ops.len();
    let offset = |index: usize, jump: Jump| -> u8 {
        let relative = match jump {
            Jump::Fall => 0,
            Jump::Skip(n) => usize::from(n),
            Jump::Ret(v) => {
                let at = body + verdicts.iter().position(|x| *x == v).expect("collected");
                at - (index + 1)
            }
        };
        u8::try_from(relative).expect("the program is small")
    };
    let mut program: Vec<SockFilter> = ops
        .iter()
        .enumerate()
        .map(|(i, op)| SockFilter {
            code: op.code,
            jt: offset(i, op.jt),
            jf: offset(i, op.jf),
            k: op.k,
        })
        .collect();
    program.extend(verdicts.iter().map(|v| SockFilter {
        code: BPF_RET_K,
        jt: 0,
        jf: 0,
        k: v.action(),
    }));
    program
}

// ---- Applying the policy (Linux only) ----------------------------------------------------------

#[cfg(target_os = "linux")]
pub(super) use sys::Confinement;
#[cfg(target_os = "linux")]
pub use sys::{landlock_abi, private_netns_available, probe};

#[cfg(target_os = "linux")]
mod sys {
    use super::*;
    use crate::isolation::{HostDirs, Jail};
    use std::ffi::CString;
    use std::io;
    use std::os::fd::{AsRawFd, FromRawFd, OwnedFd};
    use std::os::unix::ffi::OsStrExt;

    const SYS_LANDLOCK_CREATE_RULESET: libc::c_long = 444;
    const SYS_LANDLOCK_ADD_RULE: libc::c_long = 445;
    const SYS_LANDLOCK_RESTRICT_SELF: libc::c_long = 446;
    const LANDLOCK_RULE_PATH_BENEATH: libc::c_long = 1;

    const PR_SET_NO_NEW_PRIVS: libc::c_int = 38;
    const PR_SET_SECCOMP: libc::c_int = 22;
    const SECCOMP_MODE_FILTER: libc::c_ulong = 2;
    const CLONE_NEWUSER: libc::c_int = 0x1000_0000;
    const CLONE_NEWNET: libc::c_int = 0x4000_0000;

    /// `struct landlock_ruleset_attr` (ABI 6 layout).
    #[repr(C)]
    struct RulesetAttr {
        handled_access_fs: u64,
        handled_access_net: u64,
        scoped: u64,
    }

    /// `struct landlock_path_beneath_attr`.
    #[repr(C, packed)]
    struct PathBeneathAttr {
        allowed_access: u64,
        parent_fd: i32,
    }

    /// `struct sock_fprog`.
    #[repr(C)]
    struct SockFprog {
        len: u16,
        filter: *const SockFilter,
    }

    /// The Landlock ABI version of the running kernel (`1` = Linux 5.13), or why there is none.
    pub fn landlock_abi() -> io::Result<u32> {
        // SAFETY: a null attribute with size 0 and the VERSION flag only asks for the version.
        let rc = unsafe {
            libc::syscall(
                SYS_LANDLOCK_CREATE_RULESET,
                std::ptr::null::<RulesetAttr>(),
                0 as libc::c_long,
                libc::c_long::from(LANDLOCK_CREATE_RULESET_VERSION),
            )
        };
        if rc < 0 {
            Err(io::Error::last_os_error())
        } else {
            Ok(rc as u32)
        }
    }

    /// Words for a Landlock failure, for the start-up error.
    fn describe_landlock_error(e: &io::Error) -> String {
        match e.raw_os_error() {
            Some(libc::ENOSYS) => "this kernel has no Landlock (it needs Linux 5.13 or newer built with CONFIG_SECURITY_LANDLOCK)".to_string(),
            Some(libc::EOPNOTSUPP) => "Landlock is built into this kernel but disabled (add `landlock` to the `lsm=` boot parameter)".to_string(),
            _ => format!("the Landlock probe failed: {e}"),
        }
    }

    fn open_path(path: &Path) -> Option<OwnedFd> {
        let c = CString::new(path.as_os_str().as_bytes()).ok()?;
        // SAFETY: `c` is a valid NUL-terminated string; the result is checked.
        let fd = unsafe { libc::open(c.as_ptr(), libc::O_PATH | libc::O_CLOEXEC) };
        // SAFETY: `fd` is a fresh descriptor that nothing else owns.
        (fd >= 0).then(|| unsafe { OwnedFd::from_raw_fd(fd) })
    }

    fn is_dir(fd: &OwnedFd) -> bool {
        // SAFETY: `st` is valid for writes and `fd` is open.
        unsafe {
            let mut st: libc::stat = std::mem::zeroed();
            libc::fstat(fd.as_raw_fd(), &mut st) == 0
                && (st.st_mode & libc::S_IFMT) == libc::S_IFDIR
        }
    }

    /// What the leader may read of its own `/proc/<pid>`.
    const PROC_SELF_ENTRIES: [&std::ffi::CStr; 14] = [
        c"/proc/self/status",
        c"/proc/self/stat",
        c"/proc/self/statm",
        c"/proc/self/maps",
        c"/proc/self/smaps",
        c"/proc/self/cgroup",
        c"/proc/self/limits",
        c"/proc/self/fd",
        c"/proc/self/fdinfo",
        c"/proc/self/comm",
        c"/proc/self/cmdline",
        c"/proc/self/environ",
        c"/proc/self/auxv",
        c"/proc/self/mountinfo",
    ];

    struct Rule {
        fd: OwnedFd,
        access: u64,
    }

    /// Everything the `pre_exec` hook needs, prepared before `spawn`.
    pub(in crate::isolation) struct Confinement {
        level: IsolationLevel,
        handled_fs: u64,
        handled_net: u64,
        scoped: u64,
        attr_size: usize,
        rules: Vec<Rule>,
        /// Access of the rule on the task leader's own `/proc/<pid>`, opened in the child.
        try_namespaces: bool,
        filter: Vec<SockFilter>,
        /// `"<uid> <uid> 1\n"` and `"<gid> <gid> 1\n"`: the identity mapping written after
        /// entering the user namespace, so the task keeps its numeric ids.
        uid_map: Vec<u8>,
        gid_map: Vec<u8>,
    }

    impl Confinement {
        /// Checks the host can enforce `jail.level` and opens the paths of the policy.
        pub(in crate::isolation) fn build(jail: &Jail<'_>) -> Result<Self, String> {
            let level = jail.level;
            let abi = landlock_abi().map_err(|e| {
                format!(
                    "--isolation {level} needs Landlock: {}",
                    describe_landlock_error(&e)
                )
            })?;
            let arch = NATIVE.ok_or_else(|| {
                format!(
                    "--isolation {level} supports x86_64 and aarch64 Linux only (this is {})",
                    std::env::consts::ARCH
                )
            })?;
            let handled = handled_fs(abi);
            let mut rules: Vec<Rule> = Vec::new();

            let sandbox = canonical(jail.sandbox_dir);
            let fd = open_path(&sandbox).ok_or_else(|| {
                format!(
                    "cannot open the task sandbox {} for the policy",
                    sandbox.display()
                )
            })?;
            let access = access_for(Want::ReadWrite, is_dir(&fd), handled);
            rules.push(Rule { fd, access });

            for device in WRITABLE_DEVICES {
                if let Some(fd) = open_path(Path::new(device)) {
                    let access = access_for(Want::Device, is_dir(&fd), handled);
                    rules.push(Rule { fd, access });
                }
            }
            for path in read_only_grants(jail.host, jail.protected_files) {
                if let Some(fd) = open_path(&path) {
                    let access = access_for(Want::ReadExec, is_dir(&fd), handled);
                    rules.push(Rule { fd, access });
                }
            }

            // SAFETY: plain getters.
            let (uid, gid) = unsafe { (libc::geteuid(), libc::getegid()) };
            Ok(Self {
                level,
                handled_fs: handled,
                handled_net: handled_net(abi, level),
                scoped: scoped(abi),
                attr_size: attr_size(abi),
                rules,
                try_namespaces: level == IsolationLevel::FsNet
                    && std::env::var_os("RUSTY_GRID_ISOLATION_NO_USERNS").is_none(),
                filter: build_filter(arch, level),
                uid_map: format!("{uid} {uid} 1\n").into_bytes(),
                gid_map: format!("{gid} {gid} 1\n").into_bytes(),
            })
        }

        /// The hook for `Command::pre_exec`: owns the descriptors until the command is dropped.
        pub(in crate::isolation) fn into_hook(
            self,
        ) -> impl FnMut() -> io::Result<()> + Send + Sync + 'static {
            move || self.apply()
        }

        /// Confines the calling process. Runs between `fork` and `exec`: raw system calls only.
        fn apply(&self) -> io::Result<()> {
            // SAFETY: only raw system calls on data prepared before the fork; no allocation.
            unsafe {
                if libc::prctl(
                    PR_SET_NO_NEW_PRIVS,
                    1 as libc::c_ulong,
                    0 as libc::c_ulong,
                    0 as libc::c_ulong,
                    0 as libc::c_ulong,
                ) != 0
                {
                    return Err(io::Error::last_os_error());
                }
                if self.try_namespaces {
                    self.enter_namespaces();
                }
                self.landlock()?;
                self.seccomp()
            }
        }

        /// Best effort: an empty network namespace, with the task's ids mapped to themselves.
        unsafe fn enter_namespaces(&self) {
            if libc::unshare(CLONE_NEWUSER | CLONE_NEWNET) != 0 {
                return;
            }
            write_proc(c"/proc/self/setgroups", b"deny");
            write_proc(c"/proc/self/uid_map", &self.uid_map);
            write_proc(c"/proc/self/gid_map", &self.gid_map);
        }

        unsafe fn landlock(&self) -> io::Result<()> {
            let attr = RulesetAttr {
                handled_access_fs: self.handled_fs,
                handled_access_net: self.handled_net,
                scoped: self.scoped,
            };
            let ruleset = libc::syscall(
                SYS_LANDLOCK_CREATE_RULESET,
                &attr as *const RulesetAttr,
                self.attr_size as libc::c_long,
                0 as libc::c_long,
            );
            if ruleset < 0 {
                return Err(io::Error::last_os_error());
            }
            let ruleset = ruleset as libc::c_int;
            let outcome = self.fill_and_restrict(ruleset);
            libc::close(ruleset);
            outcome
        }

        unsafe fn fill_and_restrict(&self, ruleset: libc::c_int) -> io::Result<()> {
            for rule in &self.rules {
                add_rule(ruleset, rule.access, rule.fd.as_raw_fd())?;
            }
            // The task leader's own `/proc/<pid>` entries (the paths are resolved here, in the
            // child, so they name the task and not the worker). Not `net`, which shows the
            // sockets of the network namespace, nor `task/*/net`. Other processes' directories
            // stay out of reach.
            for path in PROC_SELF_ENTRIES {
                let fd = libc::open(path.as_ptr(), libc::O_PATH | libc::O_CLOEXEC);
                if fd < 0 {
                    continue;
                }
                let mut st: libc::stat = std::mem::zeroed();
                let access = if libc::fstat(fd, &mut st) == 0
                    && (st.st_mode & libc::S_IFMT) == libc::S_IFDIR
                {
                    (fs::READ_FILE | fs::READ_DIR) & self.handled_fs
                } else {
                    fs::READ_FILE & self.handled_fs
                };
                let added = add_rule(ruleset, access, fd);
                libc::close(fd);
                added?;
            }
            if libc::syscall(
                SYS_LANDLOCK_RESTRICT_SELF,
                ruleset as libc::c_long,
                0 as libc::c_long,
            ) != 0
            {
                return Err(io::Error::last_os_error());
            }
            Ok(())
        }

        unsafe fn seccomp(&self) -> io::Result<()> {
            let program = SockFprog {
                len: self.filter.len() as u16,
                filter: self.filter.as_ptr(),
            };
            if libc::prctl(
                PR_SET_SECCOMP,
                SECCOMP_MODE_FILTER,
                &program as *const SockFprog as libc::c_ulong,
                0 as libc::c_ulong,
                0 as libc::c_ulong,
            ) != 0
            {
                return Err(io::Error::last_os_error());
            }
            Ok(())
        }
    }

    unsafe fn add_rule(ruleset: libc::c_int, access: u64, fd: libc::c_int) -> io::Result<()> {
        let attr = PathBeneathAttr {
            allowed_access: access,
            parent_fd: fd,
        };
        if libc::syscall(
            SYS_LANDLOCK_ADD_RULE,
            ruleset as libc::c_long,
            LANDLOCK_RULE_PATH_BENEATH,
            &attr as *const PathBeneathAttr,
            0 as libc::c_long,
        ) != 0
        {
            return Err(io::Error::last_os_error());
        }
        Ok(())
    }

    /// Writes `data` to a `/proc/self` file, ignoring every failure.
    unsafe fn write_proc(path: &std::ffi::CStr, data: &[u8]) {
        let fd = libc::open(path.as_ptr(), libc::O_WRONLY | libc::O_CLOEXEC);
        if fd >= 0 {
            let _ = libc::write(fd, data.as_ptr().cast(), data.len());
            libc::close(fd);
        }
    }

    /// The shell script of the start-up probe: write inside, not beside, and check that the
    /// seccomp filter is installed (`Seccomp: 2`, read through the shell's own redirection from
    /// the one `/proc/<pid>` directory the policy allows).
    const PROBE_SCRIPT: &str = r#"echo ok > "$1/inside" || exit 2
if echo no > "$2/outside" 2>/dev/null; then rm -f "$2/outside"; exit 3; fi
sec=
while IFS=: read -r key value; do [ "$key" = Seccomp ] && sec=$value; done < /proc/self/status
case "$sec" in *2) ;; *) exit 4 ;; esac
exit 0"#;

    /// Start-up self-check: asks for the Landlock ABI, then runs `/bin/sh` under the real policy
    /// in a temporary directory below `sandbox_base`. It must write inside the directory, fail to
    /// write beside it and report an installed seccomp filter.
    pub async fn probe(level: IsolationLevel, sandbox_base: &Path) -> Result<(), String> {
        let abi = landlock_abi().map_err(|e| {
            format!(
                "--isolation {level} needs Landlock: {}",
                describe_landlock_error(&e)
            )
        })?;
        let probe_dir = sandbox_base.join(format!(".isolation-probe-{}", std::process::id()));
        std::fs::create_dir_all(&probe_dir).map_err(|e| {
            format!(
                "--isolation {level}: cannot create the probe directory {}: {e}",
                probe_dir.display()
            )
        })?;
        let base = sandbox_base.to_path_buf();
        let dir = probe_dir.clone();
        let outcome = tokio::task::spawn_blocking(move || run_probe(level, &base, &dir))
            .await
            .map_err(|e| format!("--isolation {level}: the probe task failed: {e}"))
            .and_then(|r| r);
        let _ = std::fs::remove_dir_all(&probe_dir);
        outcome?;
        if level == IsolationLevel::FsNet {
            if private_netns_available() {
                tracing::info!("fs+net: tasks run in a private network namespace (and seccomp refuses every socket)");
            } else {
                tracing::warn!("fs+net: unprivileged user namespaces are unavailable (or RUSTY_GRID_ISOLATION_NO_USERNS is set), so tasks share the worker's network namespace; seccomp refuses every socket() call, which is the only network barrier (design 0033)");
            }
        }
        if abi < 3 {
            tracing::warn!(abi, "Landlock ABI below 3 (Linux < 6.2): a task can truncate files it owns inside the read-only toolchain directories");
        }
        if abi < 6 {
            tracing::warn!(abi, "Landlock ABI below 6 (Linux < 6.12): a task can send signals to the worker and to other processes of the same user");
        }
        Ok(())
    }

    /// Whether a task can enter a private user and network namespace on this host.
    pub fn private_netns_available() -> bool {
        use std::os::unix::process::CommandExt;
        if std::env::var_os("RUSTY_GRID_ISOLATION_NO_USERNS").is_some() {
            return false;
        }
        let mut cmd = std::process::Command::new("/bin/true");
        // SAFETY: the hook only calls `unshare`.
        unsafe {
            cmd.pre_exec(|| {
                if libc::unshare(CLONE_NEWUSER | CLONE_NEWNET) != 0 {
                    return Err(io::Error::last_os_error());
                }
                Ok(())
            });
        }
        cmd.output().is_ok_and(|o| o.status.success())
    }

    fn run_probe(level: IsolationLevel, base: &Path, dir: &Path) -> Result<(), String> {
        use std::os::unix::process::CommandExt;
        let fail =
            |what: String| format!("--isolation {level} cannot be enforced on this host: {what}");
        let host = HostDirs::detect();
        let jail = Jail {
            level,
            host: &host,
            protected_files: &[],
            sandbox_base: base,
            sandbox_dir: dir,
        };
        let confinement = Confinement::build(&jail).map_err(fail)?;
        let inside = canonical(dir);
        let outside = canonical(base);
        let mut cmd = std::process::Command::new("/bin/sh");
        cmd.args(["-c", PROBE_SCRIPT, "probe"])
            .arg(&inside)
            .arg(&outside)
            .current_dir(dir)
            .env_clear()
            .stdin(std::process::Stdio::null());
        // SAFETY: the hook makes raw system calls only (see `Confinement::apply`).
        unsafe {
            cmd.pre_exec(confinement.into_hook());
        }
        let output = cmd
            .output()
            .map_err(|e| fail(format!("the policy could not be applied to the probe: {e}")))?;
        match output.status.code() {
            Some(0) => Ok(()),
            Some(3) => Err(fail(
                "the probe wrote outside its directory, so the Landlock policy was not applied"
                    .to_string(),
            )),
            Some(4) => Err(fail("the probe ran without a seccomp filter".to_string())),
            _ => Err(fail(format!(
                "the probe failed ({}): {}",
                output.status,
                String::from_utf8_lossy(&output.stderr).trim()
            ))),
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    /// A tiny classic-BPF interpreter for the instructions the filter uses.
    fn run(program: &[SockFilter], arch: u32, nr: u32, args: [u64; 6]) -> u32 {
        let (mut acc, mut pc) = (0u32, 0usize);
        loop {
            let i = program[pc];
            match i.code {
                BPF_LD_W_ABS => {
                    acc = match i.k {
                        OFFSET_NR => nr,
                        OFFSET_ARCH => arch,
                        k if k >= 16 => {
                            let (index, byte) = (((k - 16) / 8) as usize, (k - 16) % 8);
                            (args[index] >> (byte * 8)) as u32
                        }
                        k => panic!("unexpected load offset {k}"),
                    };
                    pc += 1;
                }
                BPF_JEQ_K => pc += 1 + usize::from(if acc == i.k { i.jt } else { i.jf }),
                BPF_JGE_K => pc += 1 + usize::from(if acc >= i.k { i.jt } else { i.jf }),
                BPF_RET_K => return i.k,
                code => panic!("unexpected opcode {code:#x}"),
            }
        }
    }

    const ALLOW: u32 = SECCOMP_RET_ALLOW;
    const DENY: u32 = SECCOMP_RET_ERRNO | EACCES as u32;

    fn check(arch: &ArchInfo, level: IsolationLevel) {
        let prog = build_filter(arch, level);
        let call =
            |nr: u32, a0: u64, a1: u64| run(&prog, arch.audit_arch, nr, [a0, a1, 0, 0, 0, 0]);
        let socket = |domain: u64| call(arch.socket, domain, 1);

        // Ordinary calls pass, whatever their arguments look like.
        assert_eq!(call(0, 1, 2), ALLOW);
        assert_eq!(call(arch.ioctl, 0, 0x5401), ALLOW, "TCGETS");
        assert_eq!(
            call(arch.ioctl, 0, 0x1_0000_5412),
            SECCOMP_RET_ERRNO | u32::from(EPERM),
            "the request is an unsigned int: the high word is ignored by the kernel, so by the filter"
        );
        assert_eq!(
            call(arch.ioctl, 0, u64::from(TIOCSTI)),
            SECCOMP_RET_ERRNO | u32::from(EPERM)
        );
        // The kernel keyring is refused at every level.
        for nr in [arch.keyctl, arch.add_key, arch.request_key] {
            assert_eq!(call(nr, 0, 0), SECCOMP_RET_ERRNO | u32::from(EPERM));
        }
        // io_uring is refused at every level.
        for nr in [
            arch.io_uring_setup,
            arch.io_uring_enter,
            arch.io_uring_register,
        ] {
            assert_eq!(call(nr, 0, 0), SECCOMP_RET_ERRNO | u32::from(ENOSYS));
        }
        // AF_UNIX is refused at every level, with the same error.
        assert_eq!(socket(1), DENY);
        assert_eq!(
            socket(0x1_0000_0001),
            DENY,
            "the domain is an int: the high word is ignored"
        );
        match level {
            IsolationLevel::FsNet => {
                for domain in [2, 10, 16, 17, 40, 3] {
                    assert_eq!(socket(domain), DENY, "domain {domain}");
                }
            }
            _ => {
                for domain in [2, 10, 16, 17] {
                    assert_eq!(socket(domain), ALLOW, "domain {domain} stays open under fs");
                }
            }
        }
        // A foreign ABI is killed, whatever it asks for.
        let other = if arch.audit_arch == X86_64.audit_arch {
            AARCH64.audit_arch
        } else {
            X86_64.audit_arch
        };
        for foreign in [0x4000_0003u32, other] {
            assert_eq!(run(&prog, foreign, 0, [0; 6]), SECCOMP_RET_KILL_PROCESS);
        }
        if arch.x32_guard {
            assert_eq!(
                run(
                    &prog,
                    arch.audit_arch,
                    X32_SYSCALL_BIT | arch.socket,
                    [2, 0, 0, 0, 0, 0]
                ),
                SECCOMP_RET_KILL_PROCESS,
                "x32 numbers are killed"
            );
        }
        // socketpair-style calls and plain reads are not touched.
        assert_eq!(call(arch.socket + 1, 1, 0), ALLOW);
    }

    #[test]
    fn test_seccomp_filter_behaves_on_both_architectures_and_levels() {
        for arch in [&X86_64, &AARCH64] {
            for level in [IsolationLevel::Fs, IsolationLevel::FsNet] {
                check(arch, level);
            }
        }
    }

    #[test]
    fn test_seccomp_program_is_small_and_ends_in_verdicts() {
        let prog = build_filter(&X86_64, IsolationLevel::FsNet);
        assert!(prog.len() < 40, "{}", prog.len());
        assert_eq!(prog.last().unwrap().code, BPF_RET_K);
        let aarch = build_filter(&AARCH64, IsolationLevel::FsNet);
        assert!(aarch.len() < prog.len(), "no x32 guard on aarch64");
    }

    #[cfg(target_os = "linux")]
    #[test]
    fn test_syscall_numbers_match_this_kernel_abi() {
        let native = NATIVE.expect("a supported architecture");
        assert_eq!(native.socket, libc::SYS_socket as u32);
        assert_eq!(native.ioctl, libc::SYS_ioctl as u32);
        assert_eq!(native.io_uring_setup, libc::SYS_io_uring_setup as u32);
        assert_eq!(native.io_uring_enter, libc::SYS_io_uring_enter as u32);
        assert_eq!(native.io_uring_register, libc::SYS_io_uring_register as u32);
        assert_eq!(native.keyctl, libc::SYS_keyctl as u32);
        assert_eq!(native.add_key, libc::SYS_add_key as u32);
        assert_eq!(native.request_key, libc::SYS_request_key as u32);
    }

    #[test]
    fn test_landlock_access_depends_on_the_abi() {
        assert_eq!(handled_fs(1), 0x1FFF);
        assert_eq!(handled_fs(2), 0x1FFF | fs::REFER);
        assert_eq!(handled_fs(3) & fs::TRUNCATE, fs::TRUNCATE);
        assert_eq!(handled_fs(4) & fs::IOCTL_DEV, 0);
        assert_eq!(handled_fs(5) & fs::IOCTL_DEV, fs::IOCTL_DEV);
        assert_eq!(
            handled_fs(9),
            handled_fs(5),
            "later ABIs add no known right"
        );
        assert_eq!(handled_net(3, IsolationLevel::FsNet), 0);
        assert_eq!(handled_net(4, IsolationLevel::Fs), 0);
        assert_eq!(handled_net(4, IsolationLevel::FsNet), 3);
        assert_eq!(scoped(5), 0);
        assert_eq!(scoped(6), 3);
        assert_eq!((attr_size(1), attr_size(4), attr_size(6)), (8, 16, 24));
    }

    #[test]
    fn test_rule_access_is_limited_to_what_the_object_and_the_ruleset_allow() {
        let handled = handled_fs(5);
        let dir = access_for(Want::ReadExec, true, handled);
        assert_eq!(dir, fs::EXECUTE | fs::READ_FILE | fs::READ_DIR);
        let file = access_for(Want::ReadExec, false, handled);
        assert_eq!(file & fs::READ_DIR, 0, "READ_DIR on a file would be EINVAL");
        assert_eq!(
            access_for(Want::ReadWrite, false, handled) & !fs::FILE_RIGHTS,
            0
        );
        assert_eq!(access_for(Want::ReadWrite, true, handled), handled);
        assert_ne!(access_for(Want::ReadWrite, true, handled) & fs::REFER, 0);
        // On ABI 1 there is no truncate or ioctl right to grant.
        let old = access_for(Want::Device, false, handled_fs(1));
        assert_eq!(old, fs::READ_FILE | fs::WRITE_FILE);
        for want in [Want::ReadExec, Want::ReadWrite, Want::Device] {
            for abi in 1..=6 {
                for is_dir in [true, false] {
                    let a = access_for(want, is_dir, handled_fs(abi));
                    assert_eq!(a & !handled_fs(abi), 0, "{want:?} abi {abi}");
                }
            }
        }
    }

    #[test]
    fn test_toolchain_roots_never_include_the_cargo_credentials() {
        let host = super::super::HostDirs::from_home("/h".into(), None, None);
        let roots = read_only_roots(&host);
        assert!(roots.contains(&PathBuf::from("/h/.rustup")));
        assert!(roots.contains(&PathBuf::from("/h/.cargo/registry")));
        assert!(!roots.contains(&PathBuf::from("/h/.cargo")));
        assert!(!roots.iter().any(|r| r.ends_with("credentials")
            || r.ends_with("credentials.toml")
            || r.as_path() == Path::new("/h")));
        for forbidden in ["/tmp", "/run", "/var", "/home", "/root", "/proc", "/dev"] {
            assert!(
                !roots.contains(&PathBuf::from(forbidden)),
                "{forbidden} is never granted wholesale"
            );
        }
    }

    #[test]
    fn test_carve_leaves_a_protected_file_out_and_keeps_its_siblings() {
        let dir = tempfile::tempdir().unwrap();
        let root = dir.path().canonicalize().unwrap();
        std::fs::create_dir_all(root.join("a/deep")).unwrap();
        std::fs::create_dir_all(root.join("b")).unwrap();
        std::fs::write(root.join("a/deep/cluster.secret"), "s").unwrap();
        std::fs::write(root.join("a/deep/other"), "o").unwrap();
        std::fs::write(root.join("a/keep"), "k").unwrap();
        std::fs::write(root.join("top"), "t").unwrap();
        #[cfg(unix)]
        std::os::unix::fs::symlink(root.join("a/deep/cluster.secret"), root.join("link")).unwrap();

        // Nothing protected under the root: the root itself.
        let mut out = Vec::new();
        carve(&root, &[PathBuf::from("/elsewhere/secret")], &mut out);
        assert_eq!(out, vec![root.clone()]);

        let protected = vec![root.join("a/deep/cluster.secret")];
        let mut out = Vec::new();
        carve(&root, &protected, &mut out);
        out.sort();
        let mut expected = vec![
            root.join("a/deep/other"),
            root.join("a/keep"),
            root.join("b"),
            root.join("top"),
        ];
        expected.sort();
        assert_eq!(
            out, expected,
            "the secret and the symlink to it are not granted"
        );
        for granted in &out {
            assert!(
                !protected.iter().any(|p| p.starts_with(granted)),
                "{} would expose the secret",
                granted.display()
            );
        }

        // The protected path itself as a root grants nothing.
        let mut out = Vec::new();
        carve(&protected[0], &protected, &mut out);
        assert!(out.is_empty());
    }

    #[test]
    fn test_read_only_grants_cut_protected_files_out_of_toolchain_directories() {
        let dir = tempfile::tempdir().unwrap();
        let rustup = dir.path().canonicalize().unwrap().join("rustup");
        std::fs::create_dir_all(rustup.join("toolchains")).unwrap();
        std::fs::write(rustup.join("cluster.secret"), "s").unwrap();
        let host = super::super::HostDirs::from_home(
            dir.path().to_path_buf(),
            Some(rustup.clone()),
            Some(dir.path().join("cargo")),
        );
        let open = read_only_grants(&host, &[]);
        assert!(open.contains(&rustup));
        let closed = read_only_grants(&host, &[rustup.join("cluster.secret")]);
        assert!(!closed.contains(&rustup));
        assert!(closed.contains(&rustup.join("toolchains")));
        assert!(!closed.contains(&rustup.join("cluster.secret")));
    }
}
