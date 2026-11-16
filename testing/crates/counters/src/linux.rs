//! The Linux side: fork a child that waits for a byte, open the counters on it while it waits,
//! let it exec, then read the counts when it exits.

use std::ffi::{CStr, CString, OsString};
use std::io::{self, PipeReader, PipeWriter, Read, Write};
use std::os::raw::{c_char, c_int, c_ulong};
use std::os::unix::ffi::OsStrExt;
use std::process::ExitCode;

const PERF_TYPE_HARDWARE: u32 = 0;
const PERF_TYPE_HW_CACHE: u32 = 3;
const PERF_COUNT_HW_CPU_CYCLES: u64 = 0;
const PERF_COUNT_HW_INSTRUCTIONS: u64 = 1;
const PERF_COUNT_HW_CACHE_MISSES: u64 = 3;
const PERF_COUNT_HW_CACHE_LL: u64 = 2;
const PERF_COUNT_HW_CACHE_OP_READ: u64 = 0;
const PERF_COUNT_HW_CACHE_RESULT_ACCESS: u64 = 0;
const PERF_COUNT_HW_CACHE_RESULT_MISS: u64 = 1;

/// The attribute flags the tool sets: disabled, inherit, exclude_kernel, exclude_hv and
/// enable_on_exec, at their bit positions in `perf_event_attr`.
const FLAG_DISABLED: u64 = 1 << 0;
const FLAG_INHERIT: u64 = 1 << 1;
const FLAG_EXCLUDE_KERNEL: u64 = 1 << 5;
const FLAG_EXCLUDE_HV: u64 = 1 << 6;
const FLAG_ENABLE_ON_EXEC: u64 = 1 << 12;

/// A cache event: cache id in the low byte, then operation, then result.
const fn cache_event(op: u64, result: u64) -> u64 {
    PERF_COUNT_HW_CACHE_LL | (op << 8) | (result << 16)
}

/// The five counters, in the order the line prints them: name, type, config.
const EVENTS: [(&str, u32, u64); 5] = [
    ("cycles", PERF_TYPE_HARDWARE, PERF_COUNT_HW_CPU_CYCLES),
    (
        "instructions",
        PERF_TYPE_HARDWARE,
        PERF_COUNT_HW_INSTRUCTIONS,
    ),
    (
        "cache_misses",
        PERF_TYPE_HARDWARE,
        PERF_COUNT_HW_CACHE_MISSES,
    ),
    (
        "llc_loads",
        PERF_TYPE_HW_CACHE,
        cache_event(
            PERF_COUNT_HW_CACHE_OP_READ,
            PERF_COUNT_HW_CACHE_RESULT_ACCESS,
        ),
    ),
    (
        "llc_misses",
        PERF_TYPE_HW_CACHE,
        cache_event(PERF_COUNT_HW_CACHE_OP_READ, PERF_COUNT_HW_CACHE_RESULT_MISS),
    ),
];

/// `struct perf_event_attr` at its newest kernel size, 136 bytes. The tool sets a few fields and
/// leaves the rest zero, which the kernel reads as "not asked for".
#[allow(dead_code)] // the fields the tool never sets are here so the layout and size are the kernel's
#[repr(C)]
#[derive(Default)]
struct PerfEventAttr {
    type_: u32,
    size: u32,
    config: u64,
    sample_period: u64,
    sample_type: u64,
    read_format: u64,
    flags: u64,
    wakeup_events: u32,
    bp_type: u32,
    bp_addr: u64,
    bp_len: u64,
    branch_sample_type: u64,
    sample_regs_user: u64,
    sample_stack_user: u32,
    clockid: i32,
    sample_regs_intr: u64,
    aux_watermark: u32,
    sample_max_stack: u16,
    reserved_2: u16,
    aux_sample_size: u32,
    reserved_3: u32,
    sig_data: u64,
    config3: u64,
}

const _: () = assert!(std::mem::size_of::<PerfEventAttr>() == 136);

pub fn run() -> ExitCode {
    let args: Vec<OsString> = std::env::args_os().skip(1).collect();
    if args.is_empty() {
        eprintln!("usage: counters CMD...");
        return ExitCode::from(2);
    }
    let Ok(command) = args
        .iter()
        .map(|arg| CString::new(arg.as_bytes()))
        .collect::<Result<Vec<CString>, _>>()
    else {
        return ExitCode::from(2);
    };
    let mut argv: Vec<*const c_char> = command.iter().map(|arg| arg.as_ptr()).collect();
    argv.push(std::ptr::null());

    // The child waits for one byte before it execs, so the counters can be opened on it first.
    let Ok((go_read, go_write)) = std::io::pipe() else {
        return ExitCode::from(3);
    };
    let child = fork_child();
    if child < 0 {
        return ExitCode::from(3);
    }
    if child == 0 {
        wait_then_exec(go_read, go_write, &argv);
    }
    drop(go_read);

    let fds: Vec<Option<c_int>> = EVENTS
        .iter()
        .map(
            |&(name, kind, config)| match open_event(kind, config, child) {
                Ok(fd) => Some(fd),
                Err(err) => {
                    eprintln!("{name}: {}", strerror(err.raw_os_error().unwrap_or(0)));
                    None
                }
            },
        )
        .collect();

    let mut go_write = go_write;
    if go_write.write_all(b"x").is_err() {
        return ExitCode::from(5);
    }
    let status = wait_for(child);

    let mut line = String::from("counters");
    for ((name, _, _), fd) in EVENTS.iter().zip(fds) {
        match fd.and_then(read_count) {
            Some(count) => line.push_str(&format!(" {name} {count}")),
            None => line.push_str(&format!(" {name} na")),
        }
    }
    eprintln!("{line}");

    let code = match status {
        Some(status) if libc::WIFEXITED(status) => libc::WEXITSTATUS(status),
        _ => 1,
    };
    ExitCode::from(u8::try_from(code).unwrap_or(1))
}

/// Forks. Returns the child's pid in the parent, 0 in the child, or -1 on failure.
fn fork_child() -> libc::pid_t {
    // SAFETY: the process has one thread when it gets here, and the child does nothing but
    // wait, exec or exit, so no lock or allocator state is taken from another thread.
    unsafe { libc::fork() }
}

/// The child's side, which never returns: it waits for the parent's byte, then execs the command.
fn wait_then_exec(go_read: PipeReader, go_write: PipeWriter, argv: &[*const c_char]) -> ! {
    drop(go_write);
    let mut byte = [0u8; 1];
    let mut go_read = go_read;
    if !matches!(go_read.read(&mut byte), Ok(1)) {
        exit_now(4);
    }
    // SAFETY: argv is a null-ended list of pointers into CStrings the child still holds, copied
    // by the fork, so each one is a valid C string.
    unsafe { libc::execvp(argv[0], argv.as_ptr()) };
    exit_now(127)
}

fn exit_now(code: c_int) -> ! {
    // SAFETY: _exit ends this process at once, without running handlers or destructors, which is
    // what a forked child must do when it has failed to exec.
    unsafe { libc::_exit(code) }
}

/// Opens one counter on `pid`, enabled from its exec. The error is the kernel's.
fn open_event(kind: u32, config: u64, pid: libc::pid_t) -> io::Result<c_int> {
    let attr = PerfEventAttr {
        type_: kind,
        size: std::mem::size_of::<PerfEventAttr>() as u32,
        config,
        flags: FLAG_DISABLED
            | FLAG_INHERIT
            | FLAG_EXCLUDE_KERNEL
            | FLAG_EXCLUDE_HV
            | FLAG_ENABLE_ON_EXEC,
        ..Default::default()
    };
    // SAFETY: attr is a perf_event_attr of the size it names, which the kernel reads and does not
    // keep; cpu is -1 (any) and group is -1 (none), as the tool has always asked.
    let fd = unsafe {
        libc::syscall(
            libc::SYS_perf_event_open,
            std::ptr::from_ref(&attr),
            pid,
            -1 as c_int,
            -1 as c_int,
            0 as c_ulong,
        )
    };
    if fd < 0 {
        Err(io::Error::last_os_error())
    } else {
        Ok(fd as c_int)
    }
}

/// Reads one counter's value, or None if the read did not give 8 bytes.
fn read_count(fd: c_int) -> Option<u64> {
    let mut buf = [0u8; 8];
    // SAFETY: buf has the 8 writable bytes given to read, which writes no more than that.
    let n = unsafe { libc::read(fd, buf.as_mut_ptr().cast(), buf.len()) };
    (n == 8).then(|| u64::from_ne_bytes(buf))
}

/// Waits for `child`, retrying an interrupted wait. None if the wait failed for another reason.
fn wait_for(child: libc::pid_t) -> Option<c_int> {
    loop {
        let mut status: c_int = 0;
        // SAFETY: status is a c_int the kernel writes the wait status into, and child is this
        // process's own child.
        let done = unsafe { libc::waitpid(child, &mut status, 0) };
        if done == child {
            return Some(status);
        }
        if io::Error::last_os_error().raw_os_error() != Some(libc::EINTR) {
            return None;
        }
    }
}

/// The text `perror` prints for an error number, without the code the std error type appends.
fn strerror(code: c_int) -> String {
    // SAFETY: strerror returns a NUL-ended message; the next line copies it before anything else
    // can overwrite it.
    let message: *mut c_char = unsafe { libc::strerror(code) };
    // SAFETY: message is the non-null pointer strerror gave, to a NUL-ended string that stays
    // valid until the next call to strerror.
    unsafe { CStr::from_ptr(message) }
        .to_string_lossy()
        .into_owned()
}
