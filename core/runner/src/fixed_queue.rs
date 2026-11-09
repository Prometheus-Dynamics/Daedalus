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

impl Overflow {
    /// From the byte a stored plan holds. An unknown byte drops the oldest.
    pub const fn from_byte(byte: u8) -> Overflow {
        match byte {
            1 => Overflow::DropNewest,
            2 => Overflow::Error,
            _ => Overflow::DropOldest,
        }
    }
}

/// A full queue refused a value.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub struct QueueFull;

/// Where a queue's values are: the index arithmetic, with no storage of its own.
#[derive(Clone, Copy, Debug, Default, PartialEq, Eq)]
pub struct Ring {
    pub head: u16,
    pub len: u16,
}

/// `index` wrapped once. There's no division, for chips that have none.
#[inline]
fn wrap(index: usize, capacity: usize) -> usize {
    if index >= capacity {
        index - capacity
    } else {
        index
    }
}

impl Ring {
    pub const fn new() -> Ring {
        Ring { head: 0, len: 0 }
    }

    /// Makes room for a value and says where it goes. `None` means the value is dropped.
    #[inline]
    pub fn push(
        &mut self,
        capacity: usize,
        overflow: Overflow,
    ) -> Result<Option<usize>, QueueFull> {
        if usize::from(self.len) == capacity {
            match overflow {
                Overflow::DropNewest => return Ok(None),
                Overflow::Error => return Err(QueueFull),
                Overflow::DropOldest => {
                    self.head = wrap(usize::from(self.head) + 1, capacity) as u16;
                    self.len -= 1;
                }
            }
        }
        let at = wrap(usize::from(self.head) + usize::from(self.len), capacity);
        self.len += 1;
        Ok(Some(at))
    }

    /// Takes the oldest value's position.
    #[inline]
    pub fn pop(&mut self, capacity: usize) -> Option<usize> {
        if self.len == 0 {
            return None;
        }
        let at = usize::from(self.head);
        self.head = wrap(at + 1, capacity) as u16;
        self.len -= 1;
        Some(at)
    }
}

/// Up to `N` values of `T`, oldest first. Usable in a `static`.
pub struct FixedQueue<T: Copy, const N: usize> {
    values: [MaybeUninit<T>; N],
    ring: Ring,
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
            ring: Ring::new(),
        }
    }

    pub const fn len(&self) -> usize {
        self.ring.len as usize
    }

    pub const fn is_empty(&self) -> bool {
        self.ring.len == 0
    }

    pub fn clear(&mut self) {
        self.ring.len = 0;
    }

    /// Adds `value`, following `overflow` if the queue is full.
    #[inline]
    pub fn push(&mut self, value: T, overflow: Overflow) -> Result<(), QueueFull> {
        if let Some(at) = self.ring.push(N, overflow)? {
            self.values[at] = MaybeUninit::new(value);
        }
        Ok(())
    }

    /// Takes the oldest value.
    #[inline]
    pub fn pop(&mut self) -> Option<T> {
        let at = self.ring.pop(N)?;
        // SAFETY: `ring.pop` only returns a position `push` wrote and nothing has taken since.
        Some(unsafe { self.values[at].assume_init() })
    }
}
