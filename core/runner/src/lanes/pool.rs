//! The worker pool: threads made once at host start, and a fan-out that spawns and allocates
//! nothing.

#![cfg(feature = "std")]

use std::panic::{AssertUnwindSafe, catch_unwind};
use std::sync::{Arc, Condvar, Mutex, MutexGuard, PoisonError};
use std::thread::{self, JoinHandle};
use std::vec::Vec;

/// A job as the workers see it: called with the worker's number, from 1. Erased so the pool can
/// hold a borrowed closure without boxing it.
type Job = *const (dyn Fn(usize) + Sync);

struct State {
    job: Option<Job>,
    generation: u64,
    active: usize,
    panicked: bool,
    shutdown: bool,
}

// SAFETY: `job` is only dereferenced while `Pool::run` keeps the closure alive, and the closure is
// `Sync`.
unsafe impl Send for State {}

struct Shared {
    state: Mutex<State>,
    start: Condvar,
    finished: Condvar,
}

impl Shared {
    fn lock(&self) -> MutexGuard<'_, State> {
        self.state.lock().unwrap_or_else(PoisonError::into_inner)
    }
}

/// A fixed set of worker threads. One pool can serve many runners, one fan-out at a time.
pub struct Pool {
    shared: Arc<Shared>,
    threads: Vec<JoinHandle<()>>,
    /// Keeps fan-outs from overlapping.
    run: Mutex<()>,
}

impl Pool {
    /// A pool that runs jobs on `workers` threads, counting the caller's, so at least one.
    pub fn new(workers: usize) -> Pool {
        let shared = Arc::new(Shared {
            state: Mutex::new(State {
                job: None,
                generation: 0,
                active: 0,
                panicked: false,
                shutdown: false,
            }),
            start: Condvar::new(),
            finished: Condvar::new(),
        });
        let threads = (1..workers.max(1))
            .map(|number| {
                let shared = Arc::clone(&shared);
                thread::spawn(move || worker(&shared, number))
            })
            .collect();
        Pool {
            shared,
            threads,
            run: Mutex::new(()),
        }
    }

    /// How many threads run a job, counting the caller's.
    pub fn workers(&self) -> usize {
        self.threads.len() + 1
    }

    /// Runs `job` once on every worker and on this thread, with the worker's number (this thread
    /// is 0), and returns when all of them have finished.
    pub(crate) fn run(&self, job: &(dyn Fn(usize) + Sync)) {
        let _one_at_a_time = self.run.lock().unwrap_or_else(PoisonError::into_inner);
        // SAFETY: only the lifetime is erased. The workers finish with `job` before this function
        // returns, because it waits for `active` to reach zero even if the job panics here.
        let erased: Job =
            unsafe { core::mem::transmute(job as *const (dyn Fn(usize) + Sync + '_)) };
        {
            let mut state = self.shared.lock();
            state.job = Some(erased);
            state.generation += 1;
            state.active = self.threads.len();
            state.panicked = false;
            if !self.threads.is_empty() {
                self.shared.start.notify_all();
            }
        }
        let mine = catch_unwind(AssertUnwindSafe(|| job(0)));
        let mut state = self.shared.lock();
        while state.active > 0 {
            state = self
                .shared
                .finished
                .wait(state)
                .unwrap_or_else(PoisonError::into_inner);
        }
        state.job = None;
        let panicked = state.panicked;
        drop(state);
        if let Err(payload) = mine {
            std::panic::resume_unwind(payload);
        }
        assert!(!panicked, "a lane worker panicked outside a step body");
    }
}

impl Drop for Pool {
    fn drop(&mut self) {
        self.shared.lock().shutdown = true;
        self.shared.start.notify_all();
        for handle in self.threads.drain(..) {
            let _ = handle.join();
        }
    }
}

fn worker(shared: &Shared, number: usize) {
    let mut seen = 0;
    loop {
        let job = {
            let mut state = shared.lock();
            while state.generation == seen && !state.shutdown {
                state = shared
                    .start
                    .wait(state)
                    .unwrap_or_else(PoisonError::into_inner);
            }
            if state.shutdown {
                return;
            }
            seen = state.generation;
            state.job
        };
        let result = catch_unwind(AssertUnwindSafe(|| {
            if let Some(job) = job {
                // SAFETY: `Pool::run` keeps the closure alive until `active` reaches zero, and
                // this worker counts itself out only after this call returns.
                unsafe { (*job)(number) };
            }
        }));
        let mut state = shared.lock();
        state.panicked |= result.is_err();
        state.active -= 1;
        if state.active == 0 {
            shared.finished.notify_all();
        }
    }
}
