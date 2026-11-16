//! Worker programs: the process backend, started from the config. Each program is started and
//! greeted at start, and the nodes its hello declares join the registry, so a graph uses them as it
//! uses a plugin's node. A program that does not come up is tried again by the retry thread.

use std::any::Any;
use std::borrow::Cow;
use std::sync::{Arc, Mutex, MutexGuard, PoisonError, RwLock, RwLockReadGuard, RwLockWriteGuard};

use daedalus_abi::{Lend, Step};
use daedalus_backend::{
    Backend, CompileCx, Compiled, MemorySpace, NodeRef, Offer, Refusal, Region, Transfer,
};
use daedalus_backend_process::{Greeter, ProcessBackend, ProcessError, WorkerSpec, WorkerStatus};
use daedalus_host::listen::ServiceHost;
use daedalus_integrity::Integrity;

use crate::config::WorkerConfig;
use crate::retry::Attempt;

/// The process backend behind a lock, so that a worker can be added while planning goes on. The
/// host holds it as one backend, and every method is the backend's own.
pub(crate) struct WorkerBackend(RwLock<ProcessBackend>);

impl WorkerBackend {
    fn read(&self) -> RwLockReadGuard<'_, ProcessBackend> {
        self.0.read().unwrap_or_else(PoisonError::into_inner)
    }

    fn write(&self) -> RwLockWriteGuard<'_, ProcessBackend> {
        self.0.write().unwrap_or_else(PoisonError::into_inner)
    }
}

impl Backend for WorkerBackend {
    fn id(&self) -> &str {
        daedalus_backend_process::BACKEND_ID
    }

    fn version(&self) -> u64 {
        self.read().version()
    }

    fn memory(&self) -> Vec<MemorySpace> {
        self.read().memory()
    }

    fn placements(&self) -> Vec<Cow<'static, str>> {
        self.read().placements()
    }

    fn offers(&self) -> Vec<Cow<'static, str>> {
        self.read().offers()
    }

    fn takes_offers(&self) -> bool {
        self.read().takes_offers()
    }

    fn bind_offer(&self, space: &str, readers: &[&str]) -> Option<Arc<dyn Lend>> {
        self.read().bind_offer(space, readers)
    }

    fn transfers(&self) -> Vec<Transfer> {
        self.read().transfers()
    }

    fn can_take(&self, offer: &Offer<'_>) -> Vec<NodeRef> {
        self.read().can_take(offer)
    }

    fn compile(&self, region: &Region<'_>, cx: &CompileCx<'_>) -> Result<Compiled, Refusal> {
        self.read().compile(region, cx)
    }

    fn bind_transfer(
        &self,
        code: &(dyn Any + Send + Sync),
        cx: &CompileCx<'_>,
    ) -> Option<Result<Step, Refusal>> {
        self.read().bind_transfer(code, cx)
    }
}

/// One configured worker and what became of its start.
struct Entry {
    spec: WorkerSpec,
    label: String,
    /// Why it is not up, when its last try failed. None once it has started.
    failure: Option<Failure>,
}

struct Failure {
    reason: String,
    /// True when the integrity check refused it, rather than the program failing to start.
    refused: bool,
}

/// The workers the daemon runs, with the state each one is in.
pub(crate) struct Workers {
    backend: Arc<WorkerBackend>,
    /// Starts and greets the workers. It holds no lock of the backend's, so a start that waits for
    /// a worker's hello blocks nothing else.
    greeter: Greeter,
    entries: Mutex<Vec<Entry>>,
}

impl Workers {
    /// The workers the config names, checked under `integrity`. None is started yet.
    pub(crate) fn new(integrity: Integrity, configs: &[WorkerConfig]) -> Arc<Workers> {
        let backend = ProcessBackend::new().with_integrity(integrity);
        let greeter = backend.greeter();
        Arc::new(Workers {
            backend: Arc::new(WorkerBackend(RwLock::new(backend))),
            greeter,
            entries: Mutex::new(
                configs
                    .iter()
                    .map(|config| Entry {
                        spec: spec(config),
                        label: config.label(),
                        // Until its first try, a worker is named as not yet started.
                        failure: Some(Failure {
                            reason: "its first start has not finished yet".to_owned(),
                            refused: false,
                        }),
                    })
                    .collect(),
            ),
        })
    }

    /// The backend the host holds for the workers.
    pub(crate) fn backend(&self) -> Arc<dyn Backend> {
        self.backend.clone()
    }

    /// One attempt per worker, for the retry thread to make first: a worker is started there, so the
    /// daemon answers while one waits for its hello.
    pub(crate) fn attempts(self: &Arc<Self>) -> Vec<Box<dyn Attempt>> {
        let count = self.lock().len();
        (0..count)
            .map(|index| -> Box<dyn Attempt> {
                Box::new(WorkerAttempt {
                    workers: Arc::clone(self),
                    index,
                })
            })
            .collect()
    }

    /// Starts the worker at `index` and records how it went. The greeting waits on the worker with
    /// no lock held; only the install takes the host's engine lock, and it waits for nothing.
    fn try_start(&self, index: usize, host: &ServiceHost) -> Result<String, String> {
        let spec = self.lock()[index].spec.clone();
        let installed = match self.greeter.greet(&spec) {
            Ok(greeted) => {
                host.with_engine(|registry, _| self.backend.write().install(greeted, registry))
            }
            Err(error) => Err(error),
        };
        let mut entries = self.lock();
        match installed {
            Ok(_) => {
                entries[index].failure = None;
                let nodes = self
                    .backend
                    .read()
                    .worker_status(&spec)
                    .map(|status| status.nodes);
                Ok(format!(
                    "worker {} started, serving {}",
                    entries[index].label,
                    nodes.unwrap_or_default().join(" ")
                ))
            }
            Err(error) => {
                let reason = error.to_string();
                entries[index].failure = Some(Failure {
                    reason: reason.clone(),
                    refused: matches!(error, ProcessError::Integrity(_)),
                });
                Err(reason)
            }
        }
    }

    /// Each worker's state, one line each, for `inspect host`.
    pub(crate) fn listing(&self) -> Vec<String> {
        let entries = self.lock();
        let backend = self.backend.read();
        entries
            .iter()
            .map(|entry| format!("worker {}: {}", entry.label, state(entry, &backend)))
            .collect()
    }

    /// The workers that are not up, named with the reason, for a planning error.
    pub(crate) fn not_running(&self) -> Vec<String> {
        self.lock()
            .iter()
            .filter_map(|entry| {
                entry
                    .failure
                    .as_ref()
                    .map(|failure| format!("worker {} ({})", entry.label, failure.reason))
            })
            .collect()
    }

    /// Stops the workers' processes. Called once the host is gone.
    pub(crate) fn shutdown(&self) {
        self.backend.write().shutdown();
    }

    fn lock(&self) -> MutexGuard<'_, Vec<Entry>> {
        self.entries.lock().unwrap_or_else(PoisonError::into_inner)
    }
}

/// What `inspect host` says of one worker.
fn state(entry: &Entry, backend: &ProcessBackend) -> String {
    if let Some(failure) = &entry.failure {
        return if failure.refused {
            format!("refused: {}", failure.reason)
        } else {
            format!("waiting, not started: {}", failure.reason)
        };
    }
    match backend.worker_status(&entry.spec) {
        Some(WorkerStatus {
            running: true,
            nodes,
        }) => format!("running, serving {}", nodes.join(" ")),
        Some(WorkerStatus {
            running: false,
            nodes,
        }) => format!(
            "restarting, serving {} (it starts again at its next step)",
            nodes.join(" ")
        ),
        None => "starting".to_owned(),
    }
}

/// The spec a `[[worker]]` table starts.
fn spec(config: &WorkerConfig) -> WorkerSpec {
    let mut spec = WorkerSpec::new(&config.program);
    for arg in &config.args {
        spec = spec.arg(arg.clone());
    }
    for (key, value) in &config.env {
        spec = spec.env(key.clone(), value.clone());
    }
    if let Some(cwd) = &config.cwd {
        spec = spec.cwd(cwd.clone());
    }
    for file in &config.also_check {
        spec = spec.also_check(file.clone());
    }
    spec
}

/// A worker waiting to be started again.
pub(crate) struct WorkerAttempt {
    workers: Arc<Workers>,
    index: usize,
}

impl Attempt for WorkerAttempt {
    fn kind(&self) -> &'static str {
        "worker"
    }

    fn name(&self) -> String {
        self.workers.lock()[self.index].label.clone()
    }

    fn down_word(&self) -> &'static str {
        "is not up"
    }

    fn retry(&mut self, host: &ServiceHost) -> Result<String, String> {
        self.workers.try_start(self.index, host)
    }
}
