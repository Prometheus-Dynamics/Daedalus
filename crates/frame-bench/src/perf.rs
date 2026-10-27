//! User-space hardware counters of the calling thread (`perf_event_open`), to compare runtime
//! work across machines in instructions and cache or TLB misses rather than nanoseconds.
//!
//! Each event opens on its own, so a PMU or kernel that refuses one (`perf_event_paranoid` > 2,
//! no PMU in a VM, no last-level-cache event on the core) reports that event as unavailable and
//! keeps the others. [`pause_counters`] and [`resume_counters`] stop and restart every open
//! counter of the process, so a caller can exclude a span (a handler's cache thrash) from the
//! runtime's count.

use core::sync::atomic::{AtomicI32, Ordering};

/// Counter slots that [`pause_counters`] and [`resume_counters`] act on. One live set at a time.
const SLOTS: usize = 8;
static LIVE: [AtomicI32; SLOTS] = [const { AtomicI32::new(-1) }; SLOTS];

/// Stop every live counter of the process until [`resume_counters`].
pub fn pause_counters() {
    for_each_live(sys::disable);
}

/// Restart the counters stopped by [`pause_counters`].
pub fn resume_counters() {
    for_each_live(sys::enable);
}

fn for_each_live(op: fn(libc::c_int)) {
    for slot in &LIVE {
        let fd = slot.load(Ordering::Relaxed);
        if fd >= 0 {
            op(fd);
        }
    }
}

/// One counted event: its name in reports and its `perf_event_attr` type and config.
#[derive(Clone, Copy, Debug)]
pub struct Event {
    pub name: &'static str,
    kind: u32,
    config: u64,
}

const PERF_TYPE_HARDWARE: u32 = 0;
const PERF_TYPE_HW_CACHE: u32 = 3;
/// `cache_id | (op << 8) | (result << 16)` for read misses: result 1 is "miss".
const fn cache_read_miss(cache: u64) -> u64 {
    cache | (1 << 16)
}
const L1D: u64 = 0;
const L1I: u64 = 1;
const LL: u64 = 2;
const DTLB: u64 = 3;

pub const INSTRUCTIONS: Event = Event {
    name: "instructions",
    kind: PERF_TYPE_HARDWARE,
    config: 1,
};
pub const CYCLES: Event = Event {
    name: "cycles",
    kind: PERF_TYPE_HARDWARE,
    config: 0,
};
pub const L1D_READ_MISS: Event = Event {
    name: "l1d_read_miss",
    kind: PERF_TYPE_HW_CACHE,
    config: cache_read_miss(L1D),
};
pub const L1I_READ_MISS: Event = Event {
    name: "l1i_read_miss",
    kind: PERF_TYPE_HW_CACHE,
    config: cache_read_miss(L1I),
};
pub const LLC_READ_MISS: Event = Event {
    name: "llc_read_miss",
    kind: PERF_TYPE_HW_CACHE,
    config: cache_read_miss(LL),
};
pub const DTLB_READ_MISS: Event = Event {
    name: "dtlb_read_miss",
    kind: PERF_TYPE_HW_CACHE,
    config: cache_read_miss(DTLB),
};

/// Every event [`CacheCounters::start`] tries, instructions first.
pub const ALL_EVENTS: [Event; 6] = [
    INSTRUCTIONS,
    CYCLES,
    L1D_READ_MISS,
    L1I_READ_MISS,
    LLC_READ_MISS,
    DTLB_READ_MISS,
];

/// A started set of counters of the calling thread. Events the kernel refuses are absent, and
/// [`Self::read`] reports them as `None`.
pub struct CacheCounters {
    counters: Vec<(Event, Option<libc::c_int>)>,
}

impl CacheCounters {
    /// Open and start `events` on the calling thread, and register them for
    /// [`pause_counters`]. Events the kernel refuses read as `None`.
    pub fn start(events: &[Event]) -> Self {
        let mut counters = Vec::with_capacity(events.len());
        for event in events {
            counters.push((*event, sys::open(event.kind, event.config)));
        }
        let mut slot = 0;
        for (_, fd) in &counters {
            if let Some(fd) = *fd {
                if slot < SLOTS {
                    LIVE[slot].store(fd, Ordering::Relaxed);
                    slot += 1;
                }
                sys::reset(fd);
                sys::enable(fd);
            }
        }
        Self { counters }
    }

    /// Each event with its count since [`Self::start`] (`None` if it did not open).
    pub fn read(&self) -> Vec<(&'static str, Option<u64>)> {
        self.counters
            .iter()
            .map(|(event, fd)| (event.name, fd.map(sys::read)))
            .collect()
    }

    /// Whether `event` opened.
    pub fn is_available(&self, event: &Event) -> bool {
        self.counters
            .iter()
            .any(|(e, fd)| e.name == event.name && fd.is_some())
    }
}

impl Drop for CacheCounters {
    fn drop(&mut self) {
        for slot in &LIVE {
            slot.store(-1, Ordering::Relaxed);
        }
        for (_, fd) in &self.counters {
            if let Some(fd) = *fd {
                sys::close(fd);
            }
        }
    }
}

/// User-space instructions retired by the calling thread.
pub struct InstructionCounter {
    counters: CacheCounters,
}

impl InstructionCounter {
    /// `None` where the kernel refuses the instruction counter.
    pub fn start() -> Option<Self> {
        let counters = CacheCounters::start(&[INSTRUCTIONS]);
        counters
            .is_available(&INSTRUCTIONS)
            .then_some(Self { counters })
    }

    /// Instructions since [`Self::start`].
    pub fn read(&self) -> u64 {
        self.counters.read()[0].1.unwrap_or(0)
    }
}

#[cfg(target_os = "linux")]
mod sys {
    use libc::c_int;

    /// `perf_event_attr` up to `config` plus the flag word; the rest stays zero.
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
    // disabled, exclude_kernel, exclude_hv
    const FLAGS: u64 = 1 | (1 << 5) | (1 << 6);
    const ENABLE: libc::c_ulong = 0x2400;
    const DISABLE: libc::c_ulong = 0x2401;
    const RESET: libc::c_ulong = 0x2403;

    pub fn open(kind: u32, config: u64) -> Option<c_int> {
        let attr = Attr {
            type_: kind,
            size: core::mem::size_of::<Attr>() as u32,
            config,
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
        libc::c_int::try_from(fd).ok().filter(|fd| *fd >= 0)
    }

    pub fn enable(fd: c_int) {
        // Safety: `fd` is a perf event this module opened.
        unsafe { libc::ioctl(fd, ENABLE as _, 0) };
    }

    pub fn disable(fd: c_int) {
        // Safety: `fd` is a perf event this module opened.
        unsafe { libc::ioctl(fd, DISABLE as _, 0) };
    }

    pub fn reset(fd: c_int) {
        // Safety: `fd` is a perf event this module opened.
        unsafe { libc::ioctl(fd, RESET as _, 0) };
    }

    pub fn read(fd: c_int) -> u64 {
        let mut value = 0u64;
        // Safety: reads one `u64` counter value into `value`.
        let read = unsafe { libc::read(fd, (&mut value as *mut u64).cast(), 8) };
        if read == 8 { value } else { 0 }
    }

    pub fn close(fd: c_int) {
        // Safety: closes the event this module opened.
        unsafe { libc::close(fd) };
    }
}

#[cfg(not(target_os = "linux"))]
mod sys {
    use libc::c_int;

    pub fn open(_kind: u32, _config: u64) -> Option<c_int> {
        None
    }
    pub fn enable(_fd: c_int) {}
    pub fn disable(_fd: c_int) {}
    pub fn reset(_fd: c_int) {}
    pub fn read(_fd: c_int) -> u64 {
        0
    }
    pub fn close(_fd: c_int) {}
}
