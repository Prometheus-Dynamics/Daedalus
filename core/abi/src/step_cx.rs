//! What a running step can see and report, with no allocation anywhere.

use core::ffi::c_void;

use crate::Word;

/// `at` when the failing node isn't known.
pub const NO_NODE: u32 = u32::MAX;
/// Longest fault reason, in bytes.
pub const REASON_CAPACITY: usize = 120;
/// In [`StepCx::flags`]: the body should reset its own state before running.
pub const RESTART: u32 = 1;
/// In [`StepCx::flags`], set by a step that faults: the reason it gave didn't fit, and was cut.
pub const REASON_CUT: u32 = 2;

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

/// A body's error: a code and a short reason, with nothing formatted or allocated.
pub trait FixedError {
    fn code(&self) -> u32;
    fn reason(&self) -> &str;
}

impl FixedError for &'static str {
    fn code(&self) -> u32 {
        1
    }

    fn reason(&self) -> &str {
        self
    }
}

/// The plain [`FixedError`]: a code and a fixed message.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub struct Fault {
    pub code: u32,
    pub reason: &'static str,
}

impl FixedError for Fault {
    fn code(&self) -> u32 {
        self.code
    }

    fn reason(&self) -> &str {
        self.reason
    }
}

impl core::fmt::Display for Fault {
    fn fmt(&self, f: &mut core::fmt::Formatter<'_>) -> core::fmt::Result {
        write!(f, "{} ({})", self.reason, self.code)
    }
}

/// Where a step's longer account of a failure goes: the host's, when it set one. A fault's
/// reason is a headline in a fixed place. What doesn't fit there is written here, in as many
/// pieces as the step likes, and the host keeps it with the step's fault.
#[repr(C)]
#[derive(Clone, Copy, Debug)]
pub struct DetailSink {
    pub context: *mut c_void,
    /// Takes one more piece of text from the step `step`. Called on whatever thread runs the
    /// step, so `context` is something every thread may use at once.
    pub write: Option<extern "C" fn(context: *mut c_void, step: u32, text: *const u8, len: usize)>,
}

impl DetailSink {
    pub const NONE: DetailSink = DetailSink {
        context: core::ptr::null_mut(),
        write: None,
    };
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
    /// The code of the error that caused the fault, zero when there was none.
    pub code: u32,
    /// The host's clock, in nanoseconds. The runner never reads a clock itself.
    pub now_ns: u64,
    /// The bytes a step hands over, for output cells marked `HANDOVER_PENDING`: cell `k` of the
    /// outputs reads `handover[k]`. The loader turns each into a record right after the step returns.
    pub handover: [crate::BytesC; 2],
    /// Where a longer account of a failure goes. Empty means nobody is keeping one.
    #[cfg(not(daedalus_abi_baseline))]
    pub detail: DetailSink,
    /// Which step of the plan this call is, set by the runner. The detail is kept under it.
    #[cfg(not(daedalus_abi_baseline))]
    pub step: u32,
}

// SAFETY: `cancel` and `progress` point at `Word`s, which are `Sync`, and are only ever read
// through shared references. `detail` is a function and a context every thread may use at once,
// which is what setting one promises. The rest of the struct is plain data.
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
            code: 0,
            now_ns: 0,
            handover: [crate::NO_BYTES; 2],
            #[cfg(not(daedalus_abi_baseline))]
            detail: DetailSink::NONE,
            #[cfg(not(daedalus_abi_baseline))]
            step: 0,
        }
    }

    /// Whether anybody keeps what [`StepCx::detail`] is given. A body asks before it formats
    /// something long.
    #[cfg(not(daedalus_abi_baseline))]
    #[inline]
    pub fn wants_detail(&self) -> bool {
        self.detail.write.is_some()
    }

    /// Gives the host more to keep with this step's failure than the reason holds: the lines a
    /// check found, a tool's output, a whole error chain. It may be called any number of times
    /// before the step returns its fault, and the pieces are kept in order. It allocates nothing
    /// here, and does nothing when the host keeps no detail. What a step writes and then doesn't
    /// fault is dropped.
    #[cfg(not(daedalus_abi_baseline))]
    pub fn detail(&self, text: &str) {
        if let Some(write) = self.detail.write {
            write(self.detail.context, self.step, text.as_ptr(), text.len());
        }
    }

    /// [`StepCx::detail`] for formatted text: `cx.detail_fmt(format_args!("{error:#}"))`. The
    /// pieces go straight to the host, so nothing is allocated for the whole.
    #[cfg(not(daedalus_abi_baseline))]
    pub fn detail_fmt(&self, args: core::fmt::Arguments<'_>) {
        struct Pieces<'a>(&'a StepCx);
        impl core::fmt::Write for Pieces<'_> {
            fn write_str(&mut self, text: &str) -> core::fmt::Result {
                self.0.detail(text);
                Ok(())
            }
        }
        if self.wants_detail() {
            // The writer never fails, so neither does this.
            let _ = core::fmt::write(&mut Pieces(self), args);
        }
    }

    /// A build from before a step could give a detail: nobody keeps one.
    #[cfg(daedalus_abi_baseline)]
    pub fn wants_detail(&self) -> bool {
        false
    }

    /// A build from before a step could give a detail: there is nowhere to write it.
    #[cfg(daedalus_abi_baseline)]
    pub fn detail(&self, _: &str) {}

    /// A build from before a step could give a detail: there is nowhere to write it.
    #[cfg(daedalus_abi_baseline)]
    pub fn detail_fmt(&self, _: core::fmt::Arguments<'_>) {}

    /// Records a failure. `reason` is cut to [`REASON_CAPACITY`] bytes, on a char boundary, and
    /// [`StepCx::reason_cut`] says when it was: the reason is a headline, held in a fixed place
    /// so that reporting a failure allocates nothing. A body with more to say than fits gives
    /// the rest to [`StepCx::detail`].
    pub fn fault(&mut self, at: Option<u32>, reason: &str) {
        self.at = at.unwrap_or(NO_NODE);
        let mut end = reason.len().min(REASON_CAPACITY);
        while !reason.is_char_boundary(end) {
            end -= 1;
        }
        self.note_cut(end < reason.len());
        self.reason[..end].copy_from_slice(&reason.as_bytes()[..end]);
        self.reason_len = end as u32;
    }

    /// Records a panic: the reason is `panicked: ` and then what the panic said, cut to
    /// [`REASON_CAPACITY`] bytes on a char boundary. The node a body named before it panicked
    /// is kept.
    pub fn fault_panic(&mut self, message: &str) {
        const PREFIX: &str = "panicked: ";
        let room = REASON_CAPACITY - PREFIX.len();
        let mut end = message.len().min(room);
        while !message.is_char_boundary(end) {
            end -= 1;
        }
        self.note_cut(end < message.len());
        self.reason[..PREFIX.len()].copy_from_slice(PREFIX.as_bytes());
        self.reason[PREFIX.len()..PREFIX.len() + end].copy_from_slice(&message.as_bytes()[..end]);
        self.reason_len = (PREFIX.len() + end) as u32;
    }

    /// Records a body's error: its code, and its reason as [`StepCx::fault`] keeps it.
    pub fn fail(&mut self, at: u32, error: &impl FixedError) {
        self.fault(Some(at), error.reason());
        self.code = error.code();
    }

    fn note_cut(&mut self, cut: bool) {
        if cut {
            self.flags |= REASON_CUT;
        } else {
            self.flags &= !REASON_CUT;
        }
    }

    /// Whether the reason the step gave was longer than fits, so that [`StepCx::reason`] is only
    /// its start.
    pub fn reason_cut(&self) -> bool {
        self.flags & REASON_CUT != 0
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
