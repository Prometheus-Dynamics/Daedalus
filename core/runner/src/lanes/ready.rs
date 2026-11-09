//! The ready queue: one shared queue of fixed capacity, the step count. A step sits at its rank,
//! the position priority order gave it, so taking the lowest set bit takes the best ready step.

#![cfg(feature = "std")]

use std::sync::{Condvar, Mutex, MutexGuard, PoisonError};
use std::vec;
use std::vec::Vec;

struct Inner {
    /// One bit per rank.
    bits: Vec<u32>,
    /// No word before this one has a bit set.
    first: usize,
    /// Steps not yet finished in this tick.
    remaining: u32,
    /// Workers asleep in `next`.
    waiting: u32,
    stop: bool,
}

/// The shared queue.
pub(super) struct Ready {
    inner: Mutex<Inner>,
    wake: Condvar,
}

impl Ready {
    pub(super) fn new(steps: usize) -> Ready {
        Ready {
            inner: Mutex::new(Inner {
                bits: vec![0; steps.div_ceil(32)],
                first: 0,
                remaining: 0,
                waiting: 0,
                stop: false,
            }),
            wake: Condvar::new(),
        }
    }

    fn lock(&self) -> MutexGuard<'_, Inner> {
        self.inner.lock().unwrap_or_else(PoisonError::into_inner)
    }

    /// Readies `remaining` steps for a tick, with nothing queued yet.
    pub(super) fn begin(&self, remaining: u32) {
        let mut inner = self.lock();
        inner.bits.fill(0);
        inner.first = 0;
        inner.remaining = remaining;
        inner.stop = false;
    }

    /// Queues a step before the workers start.
    pub(super) fn seed(&self, rank: u32) {
        self.lock().set(rank);
    }

    /// Records that a step finished, with the steps it readied, and takes the next step to run.
    /// `stop` ends the tick for every worker. `None` means this worker is done for the tick.
    pub(super) fn next(&self, finished: bool, readied: &[u32], stop: bool) -> Option<u32> {
        let mut inner = self.lock();
        for &rank in readied {
            inner.set(rank);
        }
        inner.stop |= stop;
        inner.remaining -= u32::from(finished);
        if inner.waiting > 0 && (inner.stop || inner.remaining == 0 || readied.len() > 1) {
            self.wake.notify_all();
        }
        loop {
            if inner.stop || inner.remaining == 0 {
                return None;
            }
            if let Some(rank) = inner.take() {
                // More may be queued than this worker takes.
                if inner.waiting > 0 && inner.has_any() {
                    self.wake.notify_one();
                }
                return Some(rank);
            }
            inner.waiting += 1;
            inner = self
                .wake
                .wait(inner)
                .unwrap_or_else(PoisonError::into_inner);
            inner.waiting -= 1;
        }
    }
}

impl Inner {
    fn set(&mut self, rank: u32) {
        let word = rank as usize / 32;
        self.bits[word] |= 1 << (rank % 32);
        self.first = self.first.min(word);
    }

    fn has_any(&self) -> bool {
        self.bits[self.first..].iter().any(|&word| word != 0)
    }

    fn take(&mut self) -> Option<u32> {
        for at in self.first..self.bits.len() {
            let word = self.bits[at];
            if word != 0 {
                let bit = word.trailing_zeros();
                self.bits[at] = word & !(1 << bit);
                self.first = at;
                return Some(at as u32 * 32 + bit);
            }
        }
        self.first = self.bits.len().saturating_sub(1);
        None
    }
}
