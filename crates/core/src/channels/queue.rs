//! Channel queues: `crossbeam-queue`'s lock-free queues where the target has compare-and-swap,
//! otherwise locked `VecDeque`s with the same `push`/`pop` subset (`crossbeam-queue` has no
//! queues there; see "Portability" in docs/development.md).

#[cfg(target_has_atomic = "ptr")]
pub(super) use crossbeam_queue::{ArrayQueue, SegQueue};

#[cfg(not(target_has_atomic = "ptr"))]
pub(super) use locked::{ArrayQueue, SegQueue};

// Also built in tests, so the fallback is exercised on hosts with compare-and-swap.
#[cfg(any(test, not(target_has_atomic = "ptr")))]
mod locked {
    use crate::portable::{Mutex, lock_recover};
    use alloc::collections::VecDeque;

    /// Bounded FIFO.
    pub(in crate::channels) struct ArrayQueue<T> {
        items: Mutex<VecDeque<T>>,
        capacity: usize,
    }

    impl<T> ArrayQueue<T> {
        pub(in crate::channels) fn new(capacity: usize) -> Self {
            assert!(capacity > 0, "capacity must be non-zero");
            Self {
                items: Mutex::new(VecDeque::with_capacity(capacity)),
                capacity,
            }
        }

        /// Enqueues `value`, or hands it back when the queue is full.
        pub(in crate::channels) fn push(&self, value: T) -> Result<(), T> {
            let mut items = lock_recover(&self.items);
            if items.len() == self.capacity {
                return Err(value);
            }
            items.push_back(value);
            Ok(())
        }

        pub(in crate::channels) fn pop(&self) -> Option<T> {
            lock_recover(&self.items).pop_front()
        }
    }

    /// Unbounded FIFO.
    pub(in crate::channels) struct SegQueue<T>(Mutex<VecDeque<T>>);

    impl<T> SegQueue<T> {
        pub(in crate::channels) fn new() -> Self {
            Self(Mutex::new(VecDeque::new()))
        }

        pub(in crate::channels) fn push(&self, value: T) {
            lock_recover(&self.0).push_back(value);
        }

        pub(in crate::channels) fn pop(&self) -> Option<T> {
            lock_recover(&self.0).pop_front()
        }
    }

    #[cfg(test)]
    mod tests {
        use super::{ArrayQueue, SegQueue};

        #[test]
        fn locked_queues_are_fifo_and_bounded() {
            let bounded = ArrayQueue::new(2);
            assert_eq!(bounded.push(1), Ok(()));
            assert_eq!(bounded.push(2), Ok(()));
            assert_eq!(bounded.push(3), Err(3));
            assert_eq!(bounded.pop(), Some(1));
            assert_eq!(bounded.push(3), Ok(()));
            assert_eq!(
                (bounded.pop(), bounded.pop(), bounded.pop()),
                (Some(2), Some(3), None)
            );

            let unbounded = SegQueue::new();
            (0..100).for_each(|value| unbounded.push(value));
            assert!((0..100).all(|value| unbounded.pop() == Some(value)));
            assert_eq!(unbounded.pop(), None);
        }
    }
}
