//! What a running step can see and report, with no allocation anywhere.

use crate::Word;

/// `at` when the failing node isn't known.
pub const NO_NODE: u32 = u32::MAX;
/// Longest fault reason, in bytes.
pub const REASON_CAPACITY: usize = 120;
/// In [`StepCx::flags`]: the body should reset its own state before running.
pub const RESTART: u32 = 1;

/// A step's `(done, total)` pair, in cells the host sized when it loaded the graph.
#[repr(C)]
pub struct Progress {
    pub done: Word,
    pub total: Word,
}

impl Progress {
    pub const fn new() -> Progress {
        Progress {
            done: Word::new(0),
            total: Word::new(0),
        }
    }
}

impl Default for Progress {
    fn default() -> Self {
        Progress::new()
    }
}

#[repr(C)]
pub struct StepCx {
    pub at: u32,
    pub reason_len: u32,
    pub reason: [u8; REASON_CAPACITY],
    /// The host's cancel word. Non-zero asks the step to stop. Null means nobody can cancel.
    pub cancel: *const Word,
    /// This step's progress cells. Null means nobody is reading.
    pub progress: *const Progress,
    pub flags: u32,
    pub reserved: u32,
    /// The host's clock, in nanoseconds. The runner never reads a clock itself.
    pub now_ns: u64,
}

// SAFETY: `cancel` and `progress` point at `Word`s, which are `Sync`, and are only ever read
// through shared references. The rest of the struct is plain data.
unsafe impl Send for StepCx {}

impl StepCx {
    pub const fn new() -> StepCx {
        StepCx {
            at: NO_NODE,
            reason_len: 0,
            reason: [0; REASON_CAPACITY],
            cancel: core::ptr::null(),
            progress: core::ptr::null(),
            flags: 0,
            reserved: 0,
            now_ns: 0,
        }
    }

    /// Records a failure. `reason` is cut to [`REASON_CAPACITY`] bytes, on a char boundary.
    pub fn fault(&mut self, at: Option<u32>, reason: &str) {
        self.at = at.unwrap_or(NO_NODE);
        let mut end = reason.len().min(REASON_CAPACITY);
        while !reason.is_char_boundary(end) {
            end -= 1;
        }
        self.reason[..end].copy_from_slice(&reason.as_bytes()[..end]);
        self.reason_len = end as u32;
    }

    pub fn reason(&self) -> &str {
        let len = (self.reason_len as usize).min(REASON_CAPACITY);
        core::str::from_utf8(&self.reason[..len]).unwrap_or("")
    }

    /// Whether the host has asked this run to stop.
    #[inline]
    pub fn cancelled(&self) -> bool {
        // SAFETY: the runner sets `cancel` to null or to a `Word` that outlives this call.
        unsafe { self.cancel.as_ref() }.is_some_and(|word| word.get() != 0)
    }

    /// Reports how far the step has got.
    #[inline]
    pub fn progress(&self, done: u32, total: u32) {
        // SAFETY: the runner sets `progress` to null or to cells that outlive this call.
        if let Some(cells) = unsafe { self.progress.as_ref() } {
            cells.done.set(done);
            cells.total.set(total);
        }
    }

    /// Whether the body should reset its own state before running.
    #[inline]
    pub fn restarting(&self) -> bool {
        self.flags & RESTART != 0
    }
}

impl Default for StepCx {
    fn default() -> Self {
        StepCx::new()
    }
}
