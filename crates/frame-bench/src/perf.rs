//! User-space instructions retired by the calling thread (`perf_event_open`), to compare runtime
//! work across machines in instructions rather than nanoseconds.

/// A started instruction counter of the calling thread; `None` from [`Self::start`] where the
/// kernel refuses (`perf_event_paranoid` > 2, no PMU in a VM) or off Linux.
pub struct InstructionCounter {
    #[cfg(target_os = "linux")]
    fd: libc::c_int,
}

#[cfg(target_os = "linux")]
impl InstructionCounter {
    pub fn start() -> Option<Self> {
        // `perf_event_attr` up to `config` plus the flag word; the rest stays zero.
        #[repr(C)]
        #[derive(Default)]
        struct Attr {
            type_: u32,
            size: u32,
            config: u64,
            sample_period: u64,
            sample_type: u64,
            read_format: u64,
            flags: u64,
            rest: [u64; 10],
        }
        const PERF_TYPE_HARDWARE: u32 = 0;
        const PERF_COUNT_HW_INSTRUCTIONS: u64 = 1;
        // disabled, exclude_kernel, exclude_hv
        const FLAGS: u64 = 1 | (1 << 5) | (1 << 6);
        const ENABLE: libc::c_ulong = 0x2400;
        const RESET: libc::c_ulong = 0x2403;
        let attr = Attr {
            type_: PERF_TYPE_HARDWARE,
            size: core::mem::size_of::<Attr>() as u32,
            config: PERF_COUNT_HW_INSTRUCTIONS,
            flags: FLAGS,
            ..Attr::default()
        };
        // Safety: `attr` is a valid, zero-padded `perf_event_attr` that outlives the call.
        let fd = unsafe {
            libc::syscall(
                libc::SYS_perf_event_open,
                &attr as *const Attr,
                0,
                -1,
                -1,
                libc::c_ulong::from(0u8),
            )
        };
        let fd = libc::c_int::try_from(fd).ok().filter(|fd| *fd >= 0)?;
        // Safety: `fd` is the perf event opened above.
        unsafe {
            libc::ioctl(fd, RESET as _, 0);
            libc::ioctl(fd, ENABLE as _, 0);
        }
        Some(Self { fd })
    }

    /// Instructions since [`Self::start`].
    pub fn read(&self) -> u64 {
        let mut value = 0u64;
        // Safety: reads one `u64` counter value into `value`.
        let read = unsafe { libc::read(self.fd, (&mut value as *mut u64).cast(), 8) };
        if read == 8 { value } else { 0 }
    }
}

#[cfg(target_os = "linux")]
impl Drop for InstructionCounter {
    fn drop(&mut self) {
        // Safety: closes the event this counter owns.
        unsafe { libc::close(self.fd) };
    }
}

#[cfg(not(target_os = "linux"))]
impl InstructionCounter {
    pub fn start() -> Option<Self> {
        None
    }

    pub fn read(&self) -> u64 {
        0
    }
}
