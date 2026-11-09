//! Reusing large values. A warm value costs no allocation, and needs no drop hook: the pool keeps
//! a reference to everything it made, so a value is idle exactly when the pool's reference is the
//! only one left.

#![cfg(feature = "alloc")]

use alloc::sync::Arc;
use alloc::vec::Vec;

/// A value that can be refilled: `reset` lets go of its contents and keeps its allocation.
pub trait Reusable: Send + Sync + 'static {
    fn reset(&mut self);
}

impl<T: Send + Sync + 'static> Reusable for Vec<T> {
    fn reset(&mut self) {
        self.clear();
    }
}

/// Values of one type, reused once their last holder lets go.
#[derive(Debug)]
pub struct RecyclePool<O> {
    made: Vec<Arc<O>>,
    next: usize,
}

impl<O> Default for RecyclePool<O> {
    fn default() -> Self {
        RecyclePool {
            made: Vec::new(),
            next: 0,
        }
    }
}

impl<O: Reusable> RecyclePool<O> {
    pub fn new() -> Self {
        Self::default()
    }

    /// How many values nobody holds right now.
    pub fn idle(&self) -> usize {
        self.made
            .iter()
            .filter(|value| Arc::strong_count(value) == 1)
            .count()
    }

    /// An idle value, reset. `None` when every value is held.
    pub fn take(&mut self) -> Option<Arc<O>> {
        for offset in 0..self.made.len() {
            let at = (self.next + offset) % self.made.len();
            // `get_mut` only succeeds when the pool's reference is the only one.
            if let Some(value) = Arc::get_mut(&mut self.made[at]) {
                value.reset();
                self.next = (at + 1) % self.made.len();
                return Some(self.made[at].clone());
            }
        }
        None
    }

    /// An idle value, or a new one from `make` when none is idle. Only the new one allocates.
    pub fn provide(&mut self, make: impl FnOnce() -> O) -> Arc<O> {
        self.take().unwrap_or_else(|| {
            let value = Arc::new(make());
            self.made.push(value.clone());
            value
        })
    }
}

/// Holds one value and refills it in place when nobody else still holds it.
#[derive(Debug)]
pub struct PayloadSlot<T> {
    value: Option<Arc<T>>,
}

impl<T> Default for PayloadSlot<T> {
    fn default() -> Self {
        PayloadSlot { value: None }
    }
}

impl<T: Reusable> PayloadSlot<T> {
    /// The value in this slot after `fill`. It is reused when this slot holds the only reference.
    /// A value someone still holds is left alone, and `make` builds its replacement.
    pub fn owned_reused(&mut self, make: impl FnOnce() -> T, fill: impl FnOnce(&mut T)) -> Arc<T> {
        if let Some(value) = self.value.as_mut().and_then(Arc::get_mut) {
            value.reset();
            fill(value);
            return self.value.clone().unwrap_or_else(|| Arc::new(make()));
        }
        let mut value = make();
        fill(&mut value);
        let value = Arc::new(value);
        self.value = Some(value.clone());
        value
    }
}
