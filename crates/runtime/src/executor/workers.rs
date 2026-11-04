//! Threads a parallel run fans out to: a small set of persistent threads parked between runs. A
//! frame spawns no threads and allocates nothing to dispatch work.

use crate::prelude::*;
use crate::sync::Mutex;
use std::sync::{Arc, OnceLock};

use super::{ExecuteError, ExecutionTelemetry, MetricsLevel, NodeError};
use daedalus_core::platform::{Clock, Instant};

/// Calls `work` on up to `workers` threads at once ([`WorkerPool::fan_out`]); each call pulls
/// segments from a shared queue until the run drains.
pub(crate) struct WorkerPool {
    helpers: parked::Helpers,
    /// Telemetry the workers record into, kept between runs: a worker takes one and gives it
    /// back, and the run absorbs them all, leaving each zeroed with its storage. At most one per
    /// concurrent call, so the list never grows past `workers` (its capacity).
    telemetry: Mutex<Vec<ExecutionTelemetry>>,
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

    fn new(workers: usize) -> Result<Self, ExecuteError> {
        // The calling thread is one of the workers.
        let helpers =
            parked::Helpers::spawn(workers - 1).map_err(|err| ExecuteError::HandlerFailed {
                node: "pool_init".into(),
                error: NodeError::Handler(err.to_string()),
            })?;
        // Returns once every helper has started and stocked its port-buffer pool, then parked.
        helpers.warm_all(&crate::io::prewarm_port_buffers);
        Ok(Self {
            helpers,
            telemetry: Mutex::new(Vec::with_capacity(workers)),
        })
    }

    /// A worker's telemetry for a run at `level` over a plan of `nodes` nodes and `edges` edges,
    /// timed by `clock` from the run's `origin`: a kept one (its storage reused), or a new one
    /// before the pool has as many as its calls need. A kept one may already hold this run's counts
    /// (a worker that finished gave its telemetry back for the next worker of the run), so it is
    /// not zeroed here: the run absorbs every kept telemetry once all its workers are done, which
    /// zeroes them.
    pub(crate) fn take_telemetry(
        &self,
        clock: &Clock,
        level: MetricsLevel,
        origin: Option<Instant>,
        nodes: usize,
        edges: usize,
    ) -> ExecutionTelemetry {
        let kept = self.telemetry.lock().pop();
        let mut telemetry = kept.unwrap_or_else(|| {
            let mut telemetry = ExecutionTelemetry::default();
            telemetry.reserve_plan(nodes, edges);
            telemetry
        });
        telemetry.begin_pooled_run(clock, level, origin);
        telemetry
    }

    /// Keep a worker's telemetry for the next run.
    pub(crate) fn give_telemetry(&self, telemetry: ExecutionTelemetry) {
        self.telemetry.lock().push(telemetry);
    }

    /// Make sure the pool keeps a telemetry for each of `workers` concurrent calls, sized to the
    /// plan. A parallel run takes one per call and gives it back, so with these in place a run
    /// never creates one. Creating them lazily let an adaptive frame that first ran `workers`
    /// calls in parallel after serial frames allocate the missing ones inside the measured window.
    pub(crate) fn reserve_telemetry(
        &self,
        workers: usize,
        nodes: usize,
        edges: usize,
        transports: &[Option<crate::plan::RuntimeEdgeTransport>],
    ) {
        let mut kept = self.telemetry.lock();
        while kept.len() < workers {
            let mut telemetry = ExecutionTelemetry::default();
            telemetry.reserve_plan(nodes, edges);
            telemetry.reserve_adapter_texts(transports);
            kept.push(telemetry);
        }
    }

    /// Fold every kept worker's telemetry into `target`, zeroing them but keeping their storage.
    ///
    /// Two things a worker builds the first time it meets them, and which worker meets them varies
    /// from frame to frame, so each kept telemetry is brought to the union of what the workers
    /// hold. Payload texts: a worker builds a type's text on its first lifecycle record of that
    /// type. Buffers: a worker holds at most the run's records, so each reserves that many, and a
    /// frame that gives a worker most of them does not grow its buffers. Both settle after one
    /// frame, whatever the schedule.
    pub(crate) fn absorb_telemetry(&self, target: &mut ExecutionTelemetry) {
        let mut kept = self.telemetry.lock();
        for telemetry in kept.iter_mut() {
            target.absorb(telemetry);
        }
        for telemetry in kept.iter() {
            target.share_payload_texts(telemetry);
        }
        let lifecycle = target.data_lifecycle.len();
        let trace = target.trace.as_ref().map_or(0, Vec::len);
        for telemetry in kept.iter_mut() {
            telemetry.share_payload_texts(target);
            telemetry.data_lifecycle.reserve(lifecycle);
            if trace > 0 {
                telemetry.trace.get_or_insert_with(Vec::new).reserve(trace);
            }
        }
    }

    /// Run `work` on `workers` threads concurrently (fewer when the pool is smaller) and return
    /// once every call has returned. `work` must not panic.
    pub(crate) fn fan_out(&self, workers: usize, work: &(dyn Fn() + Sync)) {
        self.helpers.run(workers.saturating_sub(1), work);
    }
}

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

        /// Run `warm` once on every helper thread and on this one, returning only when all of
        /// them have run it: each helper is started and parked again before this returns.
        pub(super) fn warm_all(&self, warm: &(dyn Fn() + Sync)) {
            let total = self.threads.len() + 1;
            let arrived = Mutex::new(0usize);
            let everyone = Condvar::new();
            self.run(self.threads.len(), &|| {
                warm();
                let mut count = arrived.lock();
                *count += 1;
                if *count == total {
                    everyone.notify_all();
                }
                while *count < total {
                    everyone.wait(&mut count);
                }
            });
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
