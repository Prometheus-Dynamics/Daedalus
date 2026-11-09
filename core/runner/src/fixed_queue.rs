//! A queue of typed values with a fixed capacity and no heap, for generated code.

use core::mem::MaybeUninit;

/// What a full queue does with a new value.
#[repr(u8)]
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum Overflow {
    DropOldest = 0,
    DropNewest = 1,
    Error = 2,
}

/// A full queue refused a value.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub struct QueueFull;

/// Up to `N` values of `T`, oldest first. Usable in a `static`.
pub struct FixedQueue<T: Copy, const N: usize> {
    values: [MaybeUninit<T>; N],
    head: u16,
    len: u16,
}

impl<T: Copy, const N: usize> Default for FixedQueue<T, N> {
    fn default() -> Self {
        Self::new()
    }
}

impl<T: Copy, const N: usize> FixedQueue<T, N> {
    const FITS: () = assert!(
        N > 0 && N <= u16::MAX as usize,
        "a queue holds 1 to 65535 values"
    );

    pub const fn new() -> Self {
        let () = Self::FITS;
        FixedQueue {
            values: [const { MaybeUninit::uninit() }; N],
            head: 0,
            len: 0,
        }
    }

    pub const fn len(&self) -> usize {
        self.len as usize
    }

    pub const fn is_empty(&self) -> bool {
        self.len == 0
    }

    pub fn clear(&mut self) {
        self.len = 0;
    }

    /// `index` wrapped once. There's no division, for chips that have none.
    #[inline]
    fn wrap(index: usize) -> usize {
        if index >= N { index - N } else { index }
    }

    /// Adds `value`, following `overflow` if the queue is full.
    #[inline]
    pub fn push(&mut self, value: T, overflow: Overflow) -> Result<(), QueueFull> {
        if usize::from(self.len) == N {
            match overflow {
                Overflow::DropNewest => return Ok(()),
                Overflow::Error => return Err(QueueFull),
                Overflow::DropOldest => {
                    self.head = Self::wrap(usize::from(self.head) + 1) as u16;
                    self.len -= 1;
                }
            }
        }
        let at = Self::wrap(usize::from(self.head) + usize::from(self.len));
        self.values[at] = MaybeUninit::new(value);
        self.len += 1;
        Ok(())
    }

    /// Takes the oldest value.
    #[inline]
    pub fn pop(&mut self) -> Option<T> {
        if self.len == 0 {
            return None;
        }
        let at = usize::from(self.head);
        self.head = Self::wrap(at + 1) as u16;
        self.len -= 1;
        // SAFETY: `len` was above zero, so `head` is a slot `push` wrote and nothing has taken.
        Some(unsafe { self.values[at].assume_init() })
    }
}
