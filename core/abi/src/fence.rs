//! Work a step started that finishes later. The runner polls it.

/// What polling a fence found.
#[repr(u32)]
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum FencePoll {
    Ready = 0,
    Pending = 1,
    Failed = 2,
}

#[repr(C)]
#[derive(Clone, Copy, Debug)]
pub struct Fence {
    pub poll: unsafe extern "C" fn(*mut ()) -> FencePoll,
    pub data: *mut (),
}

// SAFETY: whoever fills in `poll` and `data` promises they can be used from any thread. The safe
// constructor only stores a word, and `data` is otherwise opaque here.
unsafe impl Send for Fence {}

// SAFETY: ignores its argument, so any pointer is fine.
unsafe extern "C" fn always_ready(_: *mut ()) -> FencePoll {
    FencePoll::Ready
}

/// A fence's poll function, written without `unsafe`. `word` is whatever [`Fence::of`] was given.
pub trait FenceWord: 'static {
    fn poll(word: usize) -> FencePoll;
}

// SAFETY: treats `data` as a plain number and never dereferences it.
unsafe extern "C" fn poll_word<F: FenceWord>(data: *mut ()) -> FencePoll {
    F::poll(data.addr())
}

impl Fence {
    /// A fence that is always ready.
    pub const NONE: Fence = Fence {
        poll: always_ready,
        data: core::ptr::null_mut(),
    };

    /// A fence polled by `F`, carrying `word`.
    pub const fn of<F: FenceWord>(word: usize) -> Fence {
        Fence {
            poll: poll_word::<F>,
            data: core::ptr::without_provenance_mut(word),
        }
    }

    /// # Safety
    ///
    /// `self.data` must still be what `self.poll` expects: the work the fence stands for must
    /// not have been released.
    // SAFETY: the contract is the section above.
    #[inline]
    pub unsafe fn poll(&self) -> FencePoll {
        // SAFETY: the caller promises `data` is still valid for `poll`.
        unsafe { (self.poll)(self.data) }
    }
}
