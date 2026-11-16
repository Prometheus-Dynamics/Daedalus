//! What a worker is started with. Two specs with the same fields share one worker process.

use std::path::PathBuf;
use std::process::Command;
use std::time::Duration;

use daedalus_wire::DEFAULT_CAP;

/// How to start a worker, and how long it may take.
#[derive(Clone, Debug)]
pub struct WorkerSpec {
    pub program: PathBuf,
    pub args: Vec<String>,
    /// Variables set for the worker, over what it inherits.
    pub env: Vec<(String, String)>,
    /// Start the worker with none of the host's environment: only `env` is set.
    pub env_clear: bool,
    /// Variables of the host's that the worker doesn't inherit.
    pub env_remove: Vec<String>,
    pub cwd: Option<PathBuf>,
    /// The longest a region may run. `None` means no deadline.
    pub deadline: Option<Duration>,
    /// How long a worker has to send its hello.
    pub startup: Duration,
    /// How long a worker has to answer a cancel, or to exit after shutdown, before it is killed.
    pub grace: Duration,
    /// The largest packet this host accepts from the worker.
    pub packet_cap: u32,
    /// Runs on the command before it starts, for a launcher's own flags.
    pub launch: Option<fn(&mut Command)>,
    /// The smallest packet or value, in bytes, that goes through the memory lent to the worker
    /// rather than on its socket (Linux). `None` picks by what the worker agreed:
    /// [`daedalus_process_shm::ANY_TYPE_THRESHOLD`] for a worker that takes values of any type by
    /// reference, [`daedalus_process_shm::THRESHOLD`] for any other.
    pub shared_from: Option<usize>,
    /// Files the worker reads by path, besides its program: an interpreter's script, for one. Under
    /// Locked and Record each is checked against the lock before each start. The worker
    /// reads them after the check, so this is check-then-use, not a binding.
    pub checked_files: Vec<PathBuf>,
}

impl WorkerSpec {
    pub fn new(program: impl Into<PathBuf>) -> WorkerSpec {
        WorkerSpec {
            program: program.into(),
            args: Vec::new(),
            env: Vec::new(),
            env_clear: false,
            env_remove: Vec::new(),
            cwd: None,
            deadline: None,
            startup: Duration::from_secs(5),
            grace: Duration::from_millis(500),
            packet_cap: DEFAULT_CAP,
            launch: None,
            shared_from: None,
            checked_files: Vec::new(),
        }
    }

    /// Checks `path` against the lock before each start of this worker, under Locked and Record.
    pub fn also_check(mut self, path: impl Into<PathBuf>) -> WorkerSpec {
        self.checked_files.push(path.into());
        self
    }

    pub fn arg(mut self, arg: impl Into<String>) -> WorkerSpec {
        self.args.push(arg.into());
        self
    }

    pub fn env(mut self, key: impl Into<String>, value: impl Into<String>) -> WorkerSpec {
        self.env.push((key.into(), value.into()));
        self
    }

    /// Starts the worker with none of the host's environment. What [`WorkerSpec::env`] sets is
    /// still set, whichever is called first.
    pub fn env_clear(mut self) -> WorkerSpec {
        self.env_clear = true;
        self
    }

    /// Keeps one of the host's variables from the worker. A variable [`WorkerSpec::env`] also
    /// sets is set.
    pub fn env_remove(mut self, key: impl Into<String>) -> WorkerSpec {
        self.env_remove.push(key.into());
        self
    }

    /// Where the worker starts. By default it is where the host is.
    pub fn cwd(mut self, cwd: impl Into<PathBuf>) -> WorkerSpec {
        self.cwd = Some(cwd.into());
        self
    }

    pub fn deadline(mut self, deadline: Duration) -> WorkerSpec {
        self.deadline = Some(deadline);
        self
    }

    pub fn grace(mut self, grace: Duration) -> WorkerSpec {
        self.grace = grace;
        self
    }

    /// The smallest packet or value, in bytes, that goes through the memory lent to the worker.
    /// What is smaller goes on the socket; what is at least this goes in the lent buffers, and a
    /// value is placed there for a worker that takes values by reference.
    pub fn shared_from(mut self, bytes: usize) -> WorkerSpec {
        self.shared_from = Some(bytes);
        self
    }

    /// Whether two specs start the same worker. The launch hook is not compared.
    pub fn same_as(&self, other: &WorkerSpec) -> bool {
        self.program == other.program
            && self.args == other.args
            && self.env == other.env
            && self.env_clear == other.env_clear
            && self.env_remove == other.env_remove
            && self.cwd == other.cwd
            && self.deadline == other.deadline
            && self.startup == other.startup
            && self.grace == other.grace
            && self.packet_cap == other.packet_cap
            && self.shared_from == other.shared_from
            && self.checked_files == other.checked_files
    }
}
