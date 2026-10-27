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
        for (_, fd) in &self.counters {
            if let Some(fd) = *fd {
                for slot in &LIVE {
                    let _ = slot.compare_exchange(fd, -1, Ordering::Relaxed, Ordering::Relaxed);
                }
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
        open_with(kind, config, 0, 0)
    }

    /// An instruction event sampling the instruction pointer and call chain (`sample_type` bits:
    /// IP, CALLCHAIN) every `period` instructions.
    pub fn open_sampling(period: u64, sample_type: u64) -> Option<c_int> {
        // PERF_TYPE_HARDWARE, PERF_COUNT_HW_INSTRUCTIONS
        open_with(0, 1, period, sample_type)
    }

    fn open_with(kind: u32, config: u64, sample_period: u64, sample_type: u64) -> Option<c_int> {
        let attr = Attr {
            type_: kind,
            size: core::mem::size_of::<Attr>() as u32,
            config,
            sample_period,
            sample_type,
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
    pub fn open_sampling(_period: u64, _sample_type: u64) -> Option<c_int> {
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

/// Instruction-pointer samples of the calling thread: one sample per `period` retired user
/// instructions (`PERF_SAMPLE_IP` into a ring buffer), for a per-function profile of a run.
#[cfg(target_os = "linux")]
pub struct IpSampler {
    fd: libc::c_int,
    map: *mut u8,
    map_len: usize,
}

#[cfg(target_os = "linux")]
impl IpSampler {
    /// Ring buffer data pages (a power of two): 4 MiB, room for ~250k samples at 16 bytes each.
    const DATA_PAGES: usize = 1024;

    /// Sample the instruction pointer only.
    pub fn start(period: u64) -> Option<Self> {
        Self::start_with(period, 1)
    }

    /// Sample the instruction pointer and the call chain (frame pointers needed for deep chains).
    pub fn start_callchain(period: u64) -> Option<Self> {
        Self::start_with(period, 1 | (1 << 5))
    }

    fn start_with(period: u64, sample_type: u64) -> Option<Self> {
        let fd = sys::open_sampling(period, sample_type)?;
        let page = unsafe { libc::sysconf(libc::_SC_PAGESIZE) } as usize;
        let map_len = (1 + Self::DATA_PAGES) * page;
        // Safety: maps the ring buffer the kernel fills for `fd`; unmapped in `Drop`.
        let map = unsafe {
            libc::mmap(
                core::ptr::null_mut(),
                map_len,
                libc::PROT_READ | libc::PROT_WRITE,
                libc::MAP_SHARED,
                fd,
                0,
            )
        };
        if map == libc::MAP_FAILED {
            sys::close(fd);
            return None;
        }
        sys::reset(fd);
        sys::enable(fd);
        Some(Self {
            fd,
            map: map.cast(),
            map_len,
        })
    }

    /// Stop sampling and return each sample's chain, leaf first: the instruction pointer, then the
    /// callers the sampler recorded (kernel and context markers left out). Samples are oldest first.
    pub fn finish(self) -> Vec<Vec<u64>> {
        sys::disable(self.fd);
        const DATA_HEAD: usize = 1024;
        const DATA_TAIL: usize = 1032;
        const DATA_OFFSET: usize = 1040;
        const DATA_SIZE: usize = 1048;
        let read_u64 = |offset: usize| -> u64 {
            // Safety: `offset` is inside the metadata page of the mapping.
            unsafe { (self.map.add(offset) as *const u64).read_volatile() }
        };
        let head = read_u64(DATA_HEAD);
        core::sync::atomic::fence(Ordering::Acquire);
        let tail = read_u64(DATA_TAIL);
        let data_offset = read_u64(DATA_OFFSET) as usize;
        let data_size = read_u64(DATA_SIZE) as usize;
        let data = unsafe { self.map.add(data_offset) };
        let byte = |at: u64| -> u8 {
            // Safety: `at % data_size` is inside the data area.
            unsafe { data.add((at as usize) % data_size).read_volatile() }
        };
        let mut samples = Vec::new();
        let mut pos = tail;
        while pos + 8 <= head {
            let mut header = [0u8; 8];
            for (i, b) in header.iter_mut().enumerate() {
                *b = byte(pos + i as u64);
            }
            let kind = u32::from_ne_bytes([header[0], header[1], header[2], header[3]]);
            let size = u16::from_ne_bytes([header[6], header[7]]) as u64;
            if size < 8 {
                break;
            }
            // PERF_RECORD_SAMPLE (9): the ip, then (with the call chain) its length and entries.
            if kind == 9 && size >= 16 {
                let mut words = Vec::with_capacity(((size - 8) / 8) as usize);
                let mut at = pos + 8;
                while at + 8 <= pos + size {
                    let mut word = [0u8; 8];
                    for (i, b) in word.iter_mut().enumerate() {
                        *b = byte(at + i as u64);
                    }
                    words.push(u64::from_ne_bytes(word));
                    at += 8;
                }
                let chain: Vec<u64> = match words.as_slice() {
                    // ip, nr, then nr entries
                    [ip, nr, entries @ ..] if entries.len() as u64 >= *nr => {
                        // The kernel repeats the leaf ip at the start of a user chain.
                        let mut chain = vec![*ip];
                        for addr in entries[..*nr as usize]
                            .iter()
                            .copied()
                            .filter(|addr| *addr < 0xffff_ffff_ffff_f000)
                        {
                            if chain.last() != Some(&addr) {
                                chain.push(addr);
                            }
                        }
                        chain
                    }
                    [ip, ..] => vec![*ip],
                    [] => Vec::new(),
                };
                samples.push(chain);
            }
            pos += size;
        }
        // Safety: publishes the consumed position back to the kernel.
        unsafe { (self.map.add(DATA_TAIL) as *mut u64).write_volatile(head) };
        samples
    }
}

#[cfg(target_os = "linux")]
impl Drop for IpSampler {
    fn drop(&mut self) {
        // Safety: unmaps and closes what `start` created.
        unsafe {
            libc::munmap(self.map.cast(), self.map_len);
        }
        sys::close(self.fd);
    }
}
