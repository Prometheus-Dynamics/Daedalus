//! Fixed-capacity edge queues.

/// What a full [`Queue`] does with a new value.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
#[cfg_attr(feature = "defmt", derive(defmt::Format))]
pub enum Overflow {
    /// Drop the oldest queued value (latest-only edges are capacity 1 with this policy).
    DropOldest,
    /// Drop the new value.
    DropNewest,
    /// Refuse the new value with [`QueueFull`].
    Error,
}

/// A push refused by a full queue whose policy is [`Overflow::Error`].
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
#[cfg_attr(feature = "defmt", derive(defmt::Format))]
pub struct QueueFull;

/// A ring buffer of at most `N` values (`1..=u16::MAX`), stored inline: an edge's storage is
/// exactly `N` slots, sized by the host plan compiler from the edge policy.
pub struct Queue<T, const N: usize> {
    slots: [Option<T>; N],
    head: u16,
    len: u16,
}

impl<T, const N: usize> Queue<T, N> {
    const CAPACITY_OK: () = assert!(N > 0 && N <= u16::MAX as usize, "queue capacity");

    pub const fn new() -> Self {
        let () = Self::CAPACITY_OK;
        Self {
            slots: [const { None }; N],
            head: 0,
            len: 0,
        }
    }

    pub const fn capacity(&self) -> usize {
        N
    }

    pub const fn len(&self) -> usize {
        self.len as usize
    }

    pub const fn is_empty(&self) -> bool {
        self.len == 0
    }

    /// The oldest value, without removing it.
    pub fn peek(&self) -> Option<&T> {
        self.slots[self.head as usize].as_ref()
    }

    /// Append `value`, applying `overflow` when the queue is full.
    #[inline]
    pub fn push(&mut self, value: T, overflow: Overflow) -> Result<(), QueueFull> {
        if self.len() == N {
            match overflow {
                Overflow::DropOldest => drop(self.pop()),
                Overflow::DropNewest => return Ok(()),
                Overflow::Error => return Err(QueueFull),
            }
        }
        let tail = wrap::<N>(self.head as usize + self.len());
        self.slots[tail] = Some(value);
        self.len += 1;
        Ok(())
    }

    /// Remove and return the oldest value.
    #[inline]
    pub fn pop(&mut self) -> Option<T> {
        if self.len == 0 {
            return None;
        }
        let value = self.slots[self.head as usize].take();
        self.head = wrap::<N>(self.head as usize + 1) as u16;
        self.len -= 1;
        value
    }

    /// Remove every value and return the oldest: what a node in fire mode `any` receives from
    /// an edge (the runtime drains the edge and the handler reads its first value).
    #[inline]
    pub fn take_oldest(&mut self) -> Option<T> {
        let oldest = self.pop();
        self.clear();
        oldest
    }

    pub fn clear(&mut self) {
        while self.pop().is_some() {}
    }
}

impl<T, const N: usize> Default for Queue<T, N> {
    fn default() -> Self {
        Self::new()
    }
}

/// `index mod N` for `index < 2 * N`, without a division (Cortex-M0 has none).
#[inline(always)]
const fn wrap<const N: usize>(index: usize) -> usize {
    if index >= N { index - N } else { index }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn fifo_wraps_and_reports_full() {
        let mut q = Queue::<u8, 3>::new();
        for v in 0..3 {
            q.push(v, Overflow::Error).unwrap();
        }
        assert_eq!(q.push(9, Overflow::Error), Err(QueueFull));
        assert_eq!(q.pop(), Some(0));
        q.push(3, Overflow::Error).unwrap();
        assert_eq!(
            [q.pop(), q.pop(), q.pop(), q.pop()],
            [Some(1), Some(2), Some(3), None]
        );
    }

    #[test]
    fn overflow_policies() {
        let mut latest = Queue::<u8, 1>::new();
        latest.push(1, Overflow::DropOldest).unwrap();
        latest.push(2, Overflow::DropOldest).unwrap();
        assert_eq!(latest.pop(), Some(2));

        let mut keep = Queue::<u8, 2>::new();
        for v in 1..=3 {
            keep.push(v, Overflow::DropNewest).unwrap();
        }
        assert_eq!(keep.peek(), Some(&1));
        assert_eq!(keep.take_oldest(), Some(1));
        assert!(keep.is_empty());
    }
}
