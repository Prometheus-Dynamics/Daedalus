use std::panic::{self, AssertUnwindSafe};
use std::sync::atomic::{AtomicUsize, Ordering};
use std::sync::{Arc, Mutex, OnceLock, mpsc};

type PollJob = Box<dyn FnOnce() + Send + 'static>;

pub const DEFAULT_ASYNC_POLL_WORKER_LIMIT: usize = 2;
pub const DEFAULT_ASYNC_POLL_OVERFLOW_THREAD_LIMIT: usize = 2;
const ASYNC_POLL_QUEUE_DEPTH_PER_WORKER: usize = 64;

static POLL_WORKER_LIMIT: AtomicUsize = AtomicUsize::new(DEFAULT_ASYNC_POLL_WORKER_LIMIT);
static POLL_OVERFLOW_THREAD_LIMIT: AtomicUsize =
    AtomicUsize::new(DEFAULT_ASYNC_POLL_OVERFLOW_THREAD_LIMIT);
static ACTIVE_POLL_OVERFLOW_THREADS: AtomicUsize = AtomicUsize::new(0);
static POLL_POOL: OnceLock<BlockingPollPool> = OnceLock::new();

struct BlockingPollPool {
    sender: mpsc::SyncSender<PollJob>,
}

#[derive(Clone, Debug, PartialEq, Eq)]
pub(crate) enum PollJobSubmitError {
    QueueSaturated {
        job_name: &'static str,
        overflow_limit: usize,
    },
    WorkerSpawnFailed {
        job_name: &'static str,
        error: String,
    },
    Disconnected {
        job_name: &'static str,
    },
}

impl BlockingPollPool {
    fn new(worker_limit: usize) -> Self {
        let worker_limit = worker_limit.max(1);
        let (sender, receiver) =
            mpsc::sync_channel::<PollJob>(worker_limit * ASYNC_POLL_QUEUE_DEPTH_PER_WORKER);
        let receiver = Arc::new(Mutex::new(receiver));
        for worker_idx in 0..worker_limit {
            let receiver = Arc::clone(&receiver);
            let builder =
                std::thread::Builder::new().name(format!("daedalus-gpu-async-poll-{worker_idx}"));
            if let Err(error) = builder.spawn(move || worker_loop(receiver)) {
                tracing::warn!(
                    target: "daedalus_gpu::poll_driver",
                    worker_idx,
                    error = %error,
                    "failed to start async gpu poll worker"
                );
            }
        }
        Self { sender }
    }

    fn submit(&self, job: PollJob) -> Result<(), mpsc::TrySendError<PollJob>> {
        self.sender.try_send(job)
    }
}

fn worker_loop(receiver: Arc<Mutex<mpsc::Receiver<PollJob>>>) {
    loop {
        let job = {
            let receiver = receiver
                .lock()
                .unwrap_or_else(|poisoned| poisoned.into_inner());
            receiver.recv()
        };
        let Ok(job) = job else {
            break;
        };
        run_poll_job("shared_worker", job);
    }
}

fn run_poll_job(job_name: &'static str, job: PollJob) -> bool {
    match panic::catch_unwind(AssertUnwindSafe(job)) {
        Ok(()) => true,
        Err(_) => {
            tracing::error!(
                target: "daedalus_gpu::poll_driver",
                job_name,
                "async gpu poll job panicked"
            );
            false
        }
    }
}

fn poll_pool() -> &'static BlockingPollPool {
    POLL_POOL.get_or_init(|| BlockingPollPool::new(POLL_WORKER_LIMIT.load(Ordering::Relaxed)))
}

pub(crate) fn submit_poll_job(
    job_name: &'static str,
    job: impl FnOnce() + Send + 'static,
) -> Result<(), PollJobSubmitError> {
    match poll_pool().submit(Box::new(job)) {
        Ok(()) => Ok(()),
        Err(mpsc::TrySendError::Full(job)) => {
            if !try_acquire_overflow_thread_slot() {
                tracing::warn!(
                    target: "daedalus_gpu::poll_driver",
                    job_name,
                    overflow_limit = async_poll_overflow_thread_limit(),
                    "async gpu poll queue and overflow workers are full; rejecting poll job"
                );
                drop(job);
                return Err(PollJobSubmitError::QueueSaturated {
                    job_name,
                    overflow_limit: async_poll_overflow_thread_limit(),
                });
            }
            tracing::warn!(
                target: "daedalus_gpu::poll_driver",
                job_name,
                overflow_active = active_async_poll_overflow_threads(),
                overflow_limit = async_poll_overflow_thread_limit(),
                "async gpu poll queue is full; running poll job on a bounded overflow thread"
            );
            let builder = std::thread::Builder::new()
                .name(format!("daedalus-gpu-async-poll-overflow-{job_name}"));
            match builder.spawn(move || {
                let _slot = OverflowThreadSlotGuard;
                run_poll_job(job_name, job);
            }) {
                Ok(_) => Ok(()),
                Err(error) => {
                    release_overflow_thread_slot();
                    let error = error.to_string();
                    tracing::warn!(
                        target: "daedalus_gpu::poll_driver",
                        job_name,
                        error = %error,
                        "failed to start overflow async gpu poll worker"
                    );
                    Err(PollJobSubmitError::WorkerSpawnFailed { job_name, error })
                }
            }
        }
        Err(mpsc::TrySendError::Disconnected(job)) => {
            drop(job);
            tracing::warn!(
                target: "daedalus_gpu::poll_driver",
                job_name,
                "async gpu poll queue disconnected; rejecting poll job"
            );
            Err(PollJobSubmitError::Disconnected { job_name })
        }
    }
}

impl std::fmt::Display for PollJobSubmitError {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            PollJobSubmitError::QueueSaturated {
                job_name,
                overflow_limit,
            } => write!(
                f,
                "async gpu poll job '{job_name}' rejected because the queue is saturated and overflow limit {overflow_limit} is reached"
            ),
            PollJobSubmitError::WorkerSpawnFailed { job_name, error } => {
                write!(
                    f,
                    "async gpu poll job '{job_name}' rejected because overflow worker spawn failed: {error}"
                )
            }
            PollJobSubmitError::Disconnected { job_name } => {
                write!(
                    f,
                    "async gpu poll job '{job_name}' rejected because the poll queue is disconnected"
                )
            }
        }
    }
}

impl std::error::Error for PollJobSubmitError {}

/// Set the maximum number of shared blocking workers used by async GPU polling.
///
/// The limit applies when the pool is first initialized. If async GPU polling has already started,
/// the current pool keeps its existing worker count.
pub fn set_async_poll_worker_limit(limit: usize) -> usize {
    POLL_WORKER_LIMIT.swap(limit.max(1), Ordering::Relaxed)
}

pub fn async_poll_worker_limit() -> usize {
    POLL_WORKER_LIMIT.load(Ordering::Relaxed)
}

/// Set the maximum number of temporary overflow threads used when the shared async GPU poll queue
/// is saturated.
///
/// A limit of `0` disables overflow threads. Saturated jobs are rejected so async callers do not
/// unexpectedly run blocking GPU polling work inline.
pub fn set_async_poll_overflow_thread_limit(limit: usize) -> usize {
    POLL_OVERFLOW_THREAD_LIMIT.swap(limit, Ordering::Relaxed)
}

pub fn async_poll_overflow_thread_limit() -> usize {
    POLL_OVERFLOW_THREAD_LIMIT.load(Ordering::Relaxed)
}

pub fn active_async_poll_overflow_threads() -> usize {
    ACTIVE_POLL_OVERFLOW_THREADS.load(Ordering::Relaxed)
}

fn try_acquire_overflow_thread_slot() -> bool {
    let limit = async_poll_overflow_thread_limit();
    let mut active = ACTIVE_POLL_OVERFLOW_THREADS.load(Ordering::Relaxed);
    loop {
        if active >= limit {
            return false;
        }
        match ACTIVE_POLL_OVERFLOW_THREADS.compare_exchange_weak(
            active,
            active + 1,
            Ordering::AcqRel,
            Ordering::Relaxed,
        ) {
            Ok(_) => return true,
            Err(observed) => active = observed,
        }
    }
}

fn release_overflow_thread_slot() {
    ACTIVE_POLL_OVERFLOW_THREADS.fetch_sub(1, Ordering::AcqRel);
}

struct OverflowThreadSlotGuard;

impl Drop for OverflowThreadSlotGuard {
    fn drop(&mut self) {
        release_overflow_thread_slot();
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::collections::HashSet;
    use std::sync::OnceLock;
    use std::time::Duration;

    fn test_lock() -> &'static Mutex<()> {
        static LOCK: OnceLock<Mutex<()>> = OnceLock::new();
        LOCK.get_or_init(|| Mutex::new(()))
    }

    #[test]
    fn shared_poll_pool_bounds_workers_under_fanout() {
        let _guard = test_lock().lock().expect("poll driver test lock");
        let limit = async_poll_worker_limit().max(1);
        let jobs = limit * 32;
        let (tx, rx) = mpsc::channel();

        for _ in 0..jobs {
            let tx = tx.clone();
            submit_poll_job("fanout_test", move || {
                std::thread::sleep(Duration::from_millis(2));
                let name = std::thread::current()
                    .name()
                    .unwrap_or("unnamed")
                    .to_string();
                tx.send(name).expect("fanout test receiver should be open");
            })
            .expect("fanout poll job should submit");
        }
        drop(tx);

        let worker_names: HashSet<_> = rx.iter().take(jobs).collect();
        assert!(
            worker_names.len() <= limit,
            "poll pool used {} workers with limit {limit}: {worker_names:?}",
            worker_names.len()
        );
    }

    #[test]
    fn overflow_slots_are_bounded_and_released() {
        let _guard = test_lock().lock().expect("poll driver test lock");
        let previous_limit = set_async_poll_overflow_thread_limit(2);
        while try_acquire_overflow_thread_slot() {}
        while active_async_poll_overflow_threads() > 0 {
            release_overflow_thread_slot();
        }

        assert!(try_acquire_overflow_thread_slot());
        assert!(try_acquire_overflow_thread_slot());
        assert!(!try_acquire_overflow_thread_slot());
        assert_eq!(active_async_poll_overflow_threads(), 2);

        release_overflow_thread_slot();
        assert!(try_acquire_overflow_thread_slot());
        assert_eq!(active_async_poll_overflow_threads(), 2);

        release_overflow_thread_slot();
        release_overflow_thread_slot();
        assert_eq!(active_async_poll_overflow_threads(), 0);
        set_async_poll_overflow_thread_limit(previous_limit);
    }

    #[test]
    fn shared_worker_survives_panicking_job() {
        let _guard = test_lock().lock().expect("poll driver test lock");
        let (sender, receiver) = mpsc::sync_channel::<PollJob>(2);
        let receiver = Arc::new(Mutex::new(receiver));
        let (done_tx, done_rx) = mpsc::channel();
        let handle = std::thread::spawn(move || worker_loop(receiver));

        sender
            .send(Box::new(|| panic!("poll job panic for regression test")))
            .expect("panic job send");
        sender
            .send(Box::new(move || {
                done_tx.send(()).expect("done receiver should be open");
            }))
            .expect("follow-up job send");
        drop(sender);

        done_rx
            .recv_timeout(Duration::from_secs(1))
            .expect("worker should continue after a panicking job");
        handle
            .join()
            .expect("worker should exit after sender closes");
    }

    #[test]
    fn overflow_slot_guard_releases_after_panic() {
        let _guard = test_lock().lock().expect("poll driver test lock");
        let previous_limit = set_async_poll_overflow_thread_limit(1);
        while active_async_poll_overflow_threads() > 0 {
            release_overflow_thread_slot();
        }
        assert!(try_acquire_overflow_thread_slot());
        assert_eq!(active_async_poll_overflow_threads(), 1);

        let result = panic::catch_unwind(|| {
            let _slot = OverflowThreadSlotGuard;
            panic!("overflow poll job panic for regression test");
        });

        assert!(result.is_err());
        assert_eq!(active_async_poll_overflow_threads(), 0);
        set_async_poll_overflow_thread_limit(previous_limit);
    }
}
