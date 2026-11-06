//! Linux `perf_event_open`: a group of counters on the calling thread, user space only.
//!
//! The syscall is `libc::syscall(SYS_perf_event_open, ..)`. `libc` has the syscall number but no
//! `perf_event_attr`, so the struct is declared here with the kernel's layout (the 112-byte
//! `PERF_ATTR_SIZE_VER5`, which every kernel since 2.6.33 accepts).

use std::time::{Duration, Instant};

use crate::{Error, Event, Sample};

const PERF_TYPE_HARDWARE: u32 = 0;
const PERF_TYPE_HW_CACHE: u32 = 3;
const PERF_COUNT_HW_CPU_CYCLES: u64 = 0;
const PERF_COUNT_HW_INSTRUCTIONS: u64 = 1;
const PERF_COUNT_HW_BRANCH_MISSES: u64 = 5;
/// Cache id, then operation (read = 0), then result (miss = 1), packed as the kernel expects.
const PERF_COUNT_HW_CACHE_L1D: u64 = 0;
const PERF_COUNT_HW_CACHE_LL: u64 = 2;
const CACHE_READ_MISS: u64 = 1 << 16;

const PERF_FORMAT_TOTAL_TIME_ENABLED: u64 = 1 << 0;
const PERF_FORMAT_TOTAL_TIME_RUNNING: u64 = 1 << 1;
const PERF_FORMAT_GROUP: u64 = 1 << 3;

/// `flags` bits: `disabled` (bit 0), `exclude_kernel` (bit 5), `exclude_hv` (bit 6).
const FLAG_DISABLED: u64 = 1 << 0;
const FLAG_EXCLUDE_KERNEL: u64 = 1 << 5;
const FLAG_EXCLUDE_HV: u64 = 1 << 6;

const PERF_FLAG_FD_CLOEXEC: libc::c_ulong = 1 << 3;
/// `_IO('$', n)`: enable, disable and reset. With `IOC_FLAG_GROUP` they act on the whole group.
const PERF_EVENT_IOC_ENABLE: libc::c_ulong = 0x2400;
const PERF_EVENT_IOC_DISABLE: libc::c_ulong = 0x2401;
const PERF_EVENT_IOC_RESET: libc::c_ulong = 0x2403;
const PERF_IOC_FLAG_GROUP: libc::c_ulong = 1;

const PARANOID_PATH: &str = "/proc/sys/kernel/perf_event_paranoid";

/// `struct perf_event_attr`, through `sample_max_stack`: 112 bytes.
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
    config1: u64,
    config2: u64,
    branch_sample_type: u64,
    sample_regs_user: u64,
    sample_stack_user: u32,
    clockid: i32,
    sample_regs_intr: u64,
    aux_watermark: u32,
    sample_max_stack: u16,
    reserved: u16,
}

fn type_and_config(event: Event) -> (u32, u64) {
    match event {
        Event::Cycles => (PERF_TYPE_HARDWARE, PERF_COUNT_HW_CPU_CYCLES),
        Event::Instructions => (PERF_TYPE_HARDWARE, PERF_COUNT_HW_INSTRUCTIONS),
        Event::BranchMisses => (PERF_TYPE_HARDWARE, PERF_COUNT_HW_BRANCH_MISSES),
        Event::L1dReadMisses => (
            PERF_TYPE_HW_CACHE,
            PERF_COUNT_HW_CACHE_L1D | CACHE_READ_MISS,
        ),
        Event::LlcReadMisses => (PERF_TYPE_HW_CACHE, PERF_COUNT_HW_CACHE_LL | CACHE_READ_MISS),
    }
}

/// The paranoia level the kernel reports, for the permission hint (`?` when unreadable).
fn paranoid_level() -> String {
    std::fs::read_to_string(PARANOID_PATH)
        .map(|s| s.trim().to_string())
        .unwrap_or_else(|_| "?".to_string())
}

/// Opens `event` in the group led by `group_fd` (`-1` for a new leader). Returns the fd.
fn open_event(event: Event, group_fd: libc::c_int) -> Result<libc::c_int, Error> {
    let (type_, config) = type_and_config(event);
    let attr = PerfEventAttr {
        type_,
        size: std::mem::size_of::<PerfEventAttr>() as u32,
        config,
        read_format: PERF_FORMAT_GROUP
            | PERF_FORMAT_TOTAL_TIME_ENABLED
            | PERF_FORMAT_TOTAL_TIME_RUNNING,
        flags: FLAG_DISABLED | FLAG_EXCLUDE_KERNEL | FLAG_EXCLUDE_HV,
        ..PerfEventAttr::default()
    };
    // SAFETY: `attr` is a live, fully initialised `perf_event_attr` of the size passed in; pid 0
    // (the calling thread) and cpu -1 (any CPU) are plain integers; the kernel only reads `attr`.
    let fd = unsafe {
        libc::syscall(
            libc::SYS_perf_event_open,
            &attr as *const PerfEventAttr,
            0 as libc::pid_t,
            -1 as libc::c_int,
            group_fd,
            PERF_FLAG_FD_CLOEXEC,
        )
    };
    if fd >= 0 {
        return Ok(fd as libc::c_int);
    }
    let err = std::io::Error::last_os_error();
    Err(match err.raw_os_error() {
        Some(libc::EACCES) | Some(libc::EPERM) => Error::PermissionDenied {
            hint: format!(
                "the kernel refuses user-space counting ({PARANOID_PATH} is {}); \
                 a value of 2 or lower allows counting your own threads",
                paranoid_level()
            ),
        },
        Some(libc::ENOSYS) => Error::Unsupported("the kernel has no perf_event_open"),
        _ => Error::Io(err),
    })
}

/// Whether an open failure means "this PMU has no such event" rather than a real error.
fn is_missing_event(err: &Error) -> bool {
    match err {
        Error::Io(e) => matches!(
            e.raw_os_error(),
            Some(libc::ENOENT) | Some(libc::ENODEV) | Some(libc::EOPNOTSUPP)
        ),
        _ => false,
    }
}

/// A group of hardware counters on the calling thread. Closes its file descriptors on drop.
#[derive(Debug)]
pub struct Counters {
    /// File descriptors in group order: the leader first.
    fds: Vec<libc::c_int>,
    events: Vec<Event>,
    missing: Vec<Event>,
}

impl Counters {
    /// Opens the requested events as one group. Events the PMU lacks are left out and listed by
    /// [`Counters::missing`]; if none can be opened, the error names the first missing event.
    /// A permission or kernel error stops the whole open.
    pub fn new(events: &[Event]) -> Result<Counters, Error> {
        let mut counters = Counters {
            fds: Vec::new(),
            events: Vec::new(),
            missing: Vec::new(),
        };
        for &event in events {
            let group_fd = counters.fds.first().copied().unwrap_or(-1);
            match open_event(event, group_fd) {
                Ok(fd) => {
                    counters.fds.push(fd);
                    counters.events.push(event);
                }
                Err(err) if is_missing_event(&err) => counters.missing.push(event),
                Err(err) => return Err(err), // `counters` closes what it opened on drop
            }
        }
        if counters.fds.is_empty() && !counters.missing.is_empty() {
            return Err(Error::NoSuchEvent(counters.missing[0]));
        }
        Ok(counters)
    }

    /// The requested events this PMU does not implement. Empty when every one was opened.
    pub fn missing(&self) -> &[Event] {
        &self.missing
    }

    /// Runs `f` `iterations` times with the group counting, then reads the group.
    ///
    /// The counters start at zero and stop after the loop, so the sample covers only the calls.
    /// The closure's return value is discarded: wrap it in [`std::hint::black_box`] if the
    /// compiler could drop it.
    pub fn measure<F: FnMut()>(&mut self, iterations: u64, mut f: F) -> Result<Sample, Error> {
        let Some(&leader) = self.fds.first() else {
            // No events were opened (an empty request): time the loop, count nothing.
            let start = Instant::now();
            for _ in 0..iterations {
                f();
            }
            return Ok(Sample {
                iterations,
                totals: Vec::new(),
                missing: self.missing.clone(),
                time_enabled: 0,
                time_running: 0,
                elapsed: start.elapsed(),
            });
        };
        // SAFETY: `leader` is an open perf fd this struct owns; the requests take no argument
        // pointer, and `PERF_IOC_FLAG_GROUP` applies each one to the whole group.
        unsafe {
            ioctl(leader, PERF_EVENT_IOC_RESET)?;
            ioctl(leader, PERF_EVENT_IOC_ENABLE)?;
        }
        let start = Instant::now();
        for _ in 0..iterations {
            f();
        }
        let elapsed: Duration = start.elapsed();
        // SAFETY: as above.
        unsafe { ioctl(leader, PERF_EVENT_IOC_DISABLE)? };
        let (time_enabled, time_running, values) = self.read_group()?;
        Ok(Sample {
            iterations,
            totals: self.events.iter().copied().zip(values).collect(),
            missing: self.missing.clone(),
            time_enabled,
            time_running,
            elapsed,
        })
    }

    /// Reads the group: `nr`, time enabled, time running, then one value per member.
    fn read_group(&self) -> Result<(u64, u64, Vec<u64>), Error> {
        let words = 3 + self.fds.len();
        let mut buf = vec![0u64; words];
        let bytes = words * std::mem::size_of::<u64>();
        // SAFETY: `buf` is valid for `bytes` writable bytes; the leader fd is open.
        let n = unsafe { libc::read(self.fds[0], buf.as_mut_ptr().cast(), bytes) };
        if n < 0 {
            return Err(Error::Io(std::io::Error::last_os_error()));
        }
        if (n as usize) < 3 * std::mem::size_of::<u64>() || buf[0] as usize != self.fds.len() {
            return Err(Error::Io(std::io::Error::other(
                "unexpected perf group read size",
            )));
        }
        Ok((buf[1], buf[2], buf[3..3 + self.fds.len()].to_vec()))
    }
}

/// Issues a perf ioctl on the whole group.
///
/// # Safety
///
/// `fd` must be an open perf event file descriptor.
unsafe fn ioctl(fd: libc::c_int, request: libc::c_ulong) -> Result<(), Error> {
    // SAFETY: the caller guarantees `fd` is a perf fd; the group flag is an integer argument.
    if unsafe { libc::ioctl(fd, request as _, PERF_IOC_FLAG_GROUP) } < 0 {
        return Err(Error::Io(std::io::Error::last_os_error()));
    }
    Ok(())
}

impl Drop for Counters {
    fn drop(&mut self) {
        for &fd in &self.fds {
            // SAFETY: each fd was returned by perf_event_open and is closed exactly once here.
            unsafe { libc::close(fd) };
        }
    }
}
