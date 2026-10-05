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

impl Overflow {
    pub const ALL: [Self; 3] = [Self::DropOldest, Self::DropNewest, Self::Error];
}

/// A push refused by a full queue whose policy is [`Overflow::Error`].
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
#[cfg_attr(feature = "defmt", derive(defmt::Format))]
pub struct QueueFull;

/// The head and length of a ring of slots: the index arithmetic shared by the typed [`Queue`]
/// of compiled graphs and the byte queues of the loaded-plan interpreter, which store the
/// values themselves.
#[derive(Clone, Copy, Debug, Default)]
pub struct Ring {
    head: u16,
    len: u16,
}

impl Ring {
    pub const fn new() -> Self {
        Self { head: 0, len: 0 }
    }

    pub const fn len(&self) -> usize {
        self.len as usize
    }

    pub const fn is_empty(&self) -> bool {
        self.len == 0
    }

    /// The oldest slot.
    pub const fn head(&self) -> usize {
        self.head as usize
    }

    /// The slot to write a new value to, applying `overflow` when all `capacity` slots are used
    /// (`Ok(None)`: the value is dropped). With [`Overflow::DropOldest`] the returned slot is the
    /// oldest one, which the caller overwrites.
    #[inline]
    pub fn push(
        &mut self,
        capacity: usize,
        overflow: Overflow,
    ) -> Result<Option<usize>, QueueFull> {
        if self.len() == capacity {
            match overflow {
                Overflow::DropOldest => self.advance(capacity),
                Overflow::DropNewest => return Ok(None),
                Overflow::Error => return Err(QueueFull),
            }
        }
        let tail = wrap(self.head as usize + self.len(), capacity);
        self.len += 1;
        Ok(Some(tail))
    }

    /// Remove the oldest slot and return it (its value stays in place until overwritten).
    #[inline]
    pub fn pop(&mut self, capacity: usize) -> Option<usize> {
        if self.len == 0 {
            return None;
        }
        let head = self.head as usize;
        self.advance(capacity);
        Some(head)
    }

    /// Forget every slot (the values stay in place until overwritten).
    pub fn clear(&mut self) {
        self.len = 0;
    }

    #[inline]
    fn advance(&mut self, capacity: usize) {
        self.head = wrap(self.head as usize + 1, capacity) as u16;
        self.len -= 1;
    }
}

/// A ring buffer of at most `N` values (`1..=u16::MAX`), stored inline: an edge's storage is
/// exactly `N` slots, sized by the host plan compiler from the edge policy.
pub struct Queue<T, const N: usize> {
    slots: [Option<T>; N],
    ring: Ring,
}

impl<T, const N: usize> Queue<T, N> {
    const CAPACITY_OK: () = assert!(N > 0 && N <= u16::MAX as usize, "queue capacity");

    pub const fn new() -> Self {
        let () = Self::CAPACITY_OK;
        Self {
            slots: [const { None }; N],
            ring: Ring::new(),
        }
    }

    pub const fn capacity(&self) -> usize {
        N
    }

    pub const fn len(&self) -> usize {
        self.ring.len()
    }

    pub const fn is_empty(&self) -> bool {
        self.ring.is_empty()
    }

    /// The oldest value, without removing it.
    pub fn peek(&self) -> Option<&T> {
        self.slots[self.ring.head()].as_ref()
    }

    /// Append `value`, applying `overflow` when the queue is full.
    #[inline]
    pub fn push(&mut self, value: T, overflow: Overflow) -> Result<(), QueueFull> {
        if let Some(slot) = self.ring.push(N, overflow)? {
            // Replaces (drops) the oldest value when the queue was full.
            self.slots[slot] = Some(value);
        }
        Ok(())
    }

    /// Remove and return the oldest value.
    #[inline]
    pub fn pop(&mut self) -> Option<T> {
        let slot = self.ring.pop(N)?;
        self.slots[slot].take()
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

/// `index mod capacity` for `index < 2 * capacity`, without a division (Cortex-M0 has none).
#[inline(always)]
const fn wrap(index: usize, capacity: usize) -> usize {
    if index >= capacity {
        index - capacity
    } else {
        index
    }
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

        let mut ring = Ring::new();
        assert_eq!(ring.push(2, Overflow::DropOldest), Ok(Some(0)));
        assert_eq!(ring.push(2, Overflow::DropOldest), Ok(Some(1)));
        // Full: the oldest slot is reused.
        assert_eq!(ring.push(2, Overflow::DropOldest), Ok(Some(0)));
        assert_eq!(
            (ring.pop(2), ring.pop(2), ring.pop(2)),
            (Some(1), Some(0), None)
        );
    }
}
