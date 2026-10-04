//! Threads a parallel run fans out to: a Rayon pool with `executor-pool`, otherwise a small set
//! of persistent threads parked between runs. Either way a frame spawns no threads and allocates
//! nothing to dispatch work.

use crate::prelude::*;
use std::sync::{Arc, OnceLock};

use super::{ExecuteError, NodeError};

/// Calls `work` on up to `workers` threads at once ([`WorkerPool::fan_out`]); each call pulls
/// segments from a shared queue until the run drains.
pub(crate) struct WorkerPool {
    #[cfg(feature = "executor-pool")]
    pool: rayon::ThreadPool,
    #[cfg(not(feature = "executor-pool"))]
    helpers: parked::Helpers,
}

impl WorkerPool {
    /// The pool in `slot`, created with room for `workers` concurrent calls on first use.
    pub(crate) fn get_or_init(
        slot: &OnceLock<Arc<WorkerPool>>,
        workers: usize,
    ) -> Result<Arc<WorkerPool>, ExecuteError> {
        if let Some(pool) = slot.get() {
            return Ok(pool.clone());
        }
        let pool = Arc::new(Self::new(workers.max(1))?);
        Ok(slot.get_or_init(|| pool).clone())
    }

    #[cfg(feature = "executor-pool")]
    fn new(workers: usize) -> Result<Self, ExecuteError> {
        let pool = rayon::ThreadPoolBuilder::new()
            .num_threads(workers)
            .thread_name(|idx| format!("daedalus-exec-{idx}"))
            .build()
            .map_err(|err| ExecuteError::HandlerFailed {
                node: "pool_init".into(),
                error: NodeError::Handler(err.to_string()),
            })?;
        Ok(Self { pool })
    }

    #[cfg(not(feature = "executor-pool"))]
    fn new(workers: usize) -> Result<Self, ExecuteError> {
        // The calling thread is one of the workers.
        let helpers =
            parked::Helpers::spawn(workers - 1).map_err(|err| ExecuteError::HandlerFailed {
                node: "pool_init".into(),
                error: NodeError::Handler(err.to_string()),
            })?;
        Ok(Self { helpers })
    }

    /// Run `work` on `workers` threads concurrently (fewer when the pool is smaller) and return
    /// once every call has returned. `work` must not panic.
    pub(crate) fn fan_out(&self, workers: usize, work: &(dyn Fn() + Sync)) {
        #[cfg(feature = "executor-pool")]
        {
            // Binary `join` splitting keeps the jobs on the stack: no allocation per call.
            fn split(workers: usize, work: &(dyn Fn() + Sync)) {
                if workers <= 1 {
                    work();
                } else {
                    let half = workers / 2;
                    rayon::join(|| split(half, work), || split(workers - half, work));
                }
            }
            let workers = workers.min(self.pool.current_num_threads());
            self.pool.install(|| split(workers, work));
        }
        #[cfg(not(feature = "executor-pool"))]
        self.helpers.run(workers.saturating_sub(1), work);
    }
}

#[cfg(not(feature = "executor-pool"))]
mod parked {
    use crate::portable::Arc;
    use crate::sync::{Condvar, Mutex};
    use std::panic::{self, AssertUnwindSafe};
    use std::thread::JoinHandle;

    /// Helper threads that sleep on a condvar until [`Helpers::run`] hands them a job.
    pub(super) struct Helpers {
        shared: Arc<Shared>,
        threads: Vec<JoinHandle<()>>,
    }

    struct Shared {
        state: Mutex<State>,
        work: Condvar,
        done: Condvar,
    }

    #[derive(Default)]
    struct State {
        job: Option<Job>,
        /// Helpers still allowed to pick up `job`.
        unclaimed: usize,
        /// Helpers currently inside `job`.
        active: usize,
        shutdown: bool,
    }

    /// A borrowed `work` closure with its lifetime erased; [`Finish`] keeps it alive.
    #[derive(Clone, Copy)]
    struct Job(*const (dyn Fn() + Sync + 'static));

    // SAFETY: the pointee is `Sync`, and `Helpers::run` does not return (or unwind) while a
    // helper may still call it.
    unsafe impl Send for Job {}

    impl Helpers {
        pub(super) fn spawn(count: usize) -> std::io::Result<Self> {
            let shared = Arc::new(Shared {
                state: Mutex::new(State::default()),
                work: Condvar::new(),
                done: Condvar::new(),
            });
            let mut helpers = Self {
                shared,
                threads: Vec::with_capacity(count),
            };
            for idx in 0..count {
                let shared = helpers.shared.clone();
                let handle = std::thread::Builder::new()
                    .name(format!("daedalus-exec-{idx}"))
                    .spawn(move || helper_loop(&shared))?;
                helpers.threads.push(handle);
            }
            Ok(helpers)
        }

        /// Run `work` here and on up to `helpers` parked threads; return once all calls return.
        pub(super) fn run(&self, helpers: usize, work: &(dyn Fn() + Sync)) {
            let helpers = helpers.min(self.threads.len());
            if helpers == 0 {
                work();
                return;
            }
            // SAFETY: only the lifetime is erased; `Finish` waits until no helper uses the job.
            let job = Job(unsafe {
                core::mem::transmute::<*const (dyn Fn() + Sync + '_), *const (dyn Fn() + Sync)>(
                    work,
                )
            });
            {
                let mut state = self.shared.state.lock();
                state.job = Some(job);
                state.unclaimed = helpers;
            }
            for _ in 0..helpers {
                self.shared.work.notify_one();
            }
            let _finish = Finish(&self.shared);
            work();
        }
    }

    /// Withdraws the job and waits for helpers inside it, also when `work` unwinds.
    struct Finish<'a>(&'a Shared);

    impl Drop for Finish<'_> {
        fn drop(&mut self) {
            let mut state = self.0.state.lock();
            state.unclaimed = 0;
            while state.active > 0 {
                self.0.done.wait(&mut state);
            }
            state.job = None;
        }
    }

    fn helper_loop(shared: &Shared) {
        let mut state = shared.state.lock();
        loop {
            if state.shutdown {
                return;
            }
            match state.job {
                Some(job) if state.unclaimed > 0 => {
                    state.unclaimed -= 1;
                    state.active += 1;
                    crate::sync::MutexGuard::unlocked(&mut state, || {
                        // SAFETY: `Finish` keeps the closure alive until `active` drops to zero.
                        let work = unsafe { &*job.0 };
                        let _ = panic::catch_unwind(AssertUnwindSafe(work));
                    });
                    state.active -= 1;
                    if state.active == 0 {
                        shared.done.notify_all();
                    }
                }
                _ => shared.work.wait(&mut state),
            }
        }
    }

    impl Drop for Helpers {
        fn drop(&mut self) {
            self.shared.state.lock().shutdown = true;
            self.shared.work.notify_all();
            for thread in self.threads.drain(..) {
                let _ = thread.join();
            }
        }
    }
}
