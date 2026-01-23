#![allow(unsafe_code)]

use std::io;
use std::sync::OnceLock;
use std::sync::atomic::{AtomicBool, Ordering};

#[derive(Debug, Clone, Copy, Default)]
pub struct PerfSample {
    pub cache_misses: u64,
    pub branch_instructions: u64,
    pub branch_misses: u64,
}

static NODE_PERF_ENABLED: OnceLock<bool> = OnceLock::new();
static NODE_PERF_AVAILABLE: AtomicBool = AtomicBool::new(true);

pub fn node_perf_enabled() -> bool {
    if !cfg!(all(target_os = "linux")) {
        return false;
    }
    if !NODE_PERF_AVAILABLE.load(Ordering::Relaxed) {
        return false;
    }
    *NODE_PERF_ENABLED.get_or_init(|| {
        env_flag("HELIOS_NODE_PERF_COUNTERS") || env_flag("DAEDALUS_NODE_PERF_COUNTERS")
    })
}

pub fn disable_node_perf() -> bool {
    NODE_PERF_AVAILABLE.swap(false, Ordering::Relaxed)
}

fn env_flag(name: &str) -> bool {
    std::env::var(name)
        .ok()
        .map(|v| matches!(v.trim().to_ascii_lowercase().as_str(), "1" | "true" | "yes" | "on"))
        .unwrap_or(false)
}

#[cfg(all(target_os = "linux"))]
pub struct PerfCounterGuard {
    fds: [std::os::unix::io::RawFd; 3],
}

#[cfg(not(all(target_os = "linux")))]
pub struct PerfCounterGuard;

#[cfg(all(target_os = "linux"))]
impl PerfCounterGuard {
    pub fn start() -> io::Result<Self> {
        let cache_fd = sys::open_counter(sys::PERF_COUNT_HW_CACHE_MISSES, -1)?;
        let branch_fd = match sys::open_counter(sys::PERF_COUNT_HW_BRANCH_INSTRUCTIONS, cache_fd) {
            Ok(fd) => fd,
            Err(err) => {
                sys::close_fd(cache_fd);
                return Err(err);
            }
        };
        let miss_fd = match sys::open_counter(sys::PERF_COUNT_HW_BRANCH_MISSES, cache_fd) {
            Ok(fd) => fd,
            Err(err) => {
                sys::close_fd(branch_fd);
                sys::close_fd(cache_fd);
                return Err(err);
            }
        };

        sys::ioctl_group(cache_fd, sys::PERF_EVENT_IOC_RESET)?;
        sys::ioctl_group(cache_fd, sys::PERF_EVENT_IOC_ENABLE)?;

        Ok(Self {
            fds: [cache_fd, branch_fd, miss_fd],
        })
    }

    pub fn finish(self) -> io::Result<PerfSample> {
        let leader = self.fds[0];
        let _ = sys::ioctl_group(leader, sys::PERF_EVENT_IOC_DISABLE);
        let cache_misses = sys::read_counter(self.fds[0])?;
        let branch_instructions = sys::read_counter(self.fds[1])?;
        let branch_misses = sys::read_counter(self.fds[2])?;
        Ok(PerfSample {
            cache_misses,
            branch_instructions,
            branch_misses,
        })
    }
}

#[cfg(all(target_os = "linux"))]
impl Drop for PerfCounterGuard {
    fn drop(&mut self) {
        for fd in &self.fds {
            sys::close_fd(*fd);
        }
    }
}

#[cfg(not(all(target_os = "linux")))]
impl PerfCounterGuard {
    pub fn start() -> io::Result<Self> {
        Err(io::Error::new(
            io::ErrorKind::Unsupported,
            "perf counters unavailable",
        ))
    }

    pub fn finish(self) -> io::Result<PerfSample> {
        Err(io::Error::new(
            io::ErrorKind::Unsupported,
            "perf counters unavailable",
        ))
    }
}

#[cfg(all(target_os = "linux"))]
mod sys {
    use super::*;

    pub const PERF_TYPE_HARDWARE: u32 = 0;
    pub const PERF_COUNT_HW_CACHE_MISSES: u64 = 3;
    pub const PERF_COUNT_HW_BRANCH_INSTRUCTIONS: u64 = 4;
    pub const PERF_COUNT_HW_BRANCH_MISSES: u64 = 5;

    pub const PERF_EVENT_IOC_ENABLE: libc::c_ulong = 0x2400;
    pub const PERF_EVENT_IOC_DISABLE: libc::c_ulong = 0x2401;
    pub const PERF_EVENT_IOC_RESET: libc::c_ulong = 0x2403;
    pub const PERF_IOC_FLAG_GROUP: libc::c_ulong = 1;

    const PERF_ATTR_DISABLED: u64 = 1 << 0;
    const PERF_ATTR_INHERIT: u64 = 1 << 1;
    const PERF_ATTR_EXCLUDE_KERNEL: u64 = 1 << 5;
    const PERF_ATTR_EXCLUDE_HV: u64 = 1 << 6;

    #[repr(C)]
    #[derive(Clone, Copy, Default)]
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
        __reserved_2: u16,
    }

    pub fn open_counter(config: u64, group_fd: std::os::unix::io::RawFd) -> io::Result<std::os::unix::io::RawFd> {
        let mut attr = PerfEventAttr::default();
        attr.type_ = PERF_TYPE_HARDWARE;
        attr.size = std::mem::size_of::<PerfEventAttr>() as u32;
        attr.config = config;
        attr.flags = PERF_ATTR_DISABLED | PERF_ATTR_INHERIT | PERF_ATTR_EXCLUDE_KERNEL | PERF_ATTR_EXCLUDE_HV;

        let fd = unsafe {
            libc::syscall(
                libc::SYS_perf_event_open,
                &mut attr as *mut PerfEventAttr,
                0,
                -1,
                group_fd,
                0,
            )
        };
        if fd < 0 {
            return Err(io::Error::last_os_error());
        }
        Ok(fd as std::os::unix::io::RawFd)
    }

    pub fn ioctl_group(fd: std::os::unix::io::RawFd, cmd: libc::c_ulong) -> io::Result<()> {
        let res = unsafe { libc::ioctl(fd, cmd, PERF_IOC_FLAG_GROUP) };
        if res < 0 {
            return Err(io::Error::last_os_error());
        }
        Ok(())
    }

    pub fn read_counter(fd: std::os::unix::io::RawFd) -> io::Result<u64> {
        let mut value: u64 = 0;
        let res = unsafe {
            libc::read(
                fd,
                &mut value as *mut u64 as *mut libc::c_void,
                std::mem::size_of::<u64>(),
            )
        };
        if res < 0 {
            return Err(io::Error::last_os_error());
        }
        if res as usize != std::mem::size_of::<u64>() {
            return Err(io::Error::new(
                io::ErrorKind::UnexpectedEof,
                "perf counter read truncated",
            ));
        }
        Ok(value)
    }

    pub fn close_fd(fd: std::os::unix::io::RawFd) {
        if fd >= 0 {
            unsafe {
                libc::close(fd);
            }
        }
    }
}
