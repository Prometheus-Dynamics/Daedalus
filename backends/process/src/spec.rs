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
        }
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
    }
}
