//! Runs one parsed command against a daemon, through `core/client`, and gives its canonical line.

use std::collections::{BTreeMap, BTreeSet};

use daedalus_client::protocol::{DEFAULT_SESSION, ErrorCode, TypedValue};
use daedalus_client::{Client, ClientError, Watch, WireType, WireValue};

use super::Mutation;
use super::show;
use super::syntax::{self, Cmd};

/// Why a command gave no answer. Each one is one error line.
enum Fail {
    /// The line is not a command, or names a blob or watch the scenario never made.
    Usage,
    /// A file cannot be read.
    Local,
    /// The client failed: a service error with its code, or anything else.
    Client(ClientError),
}

impl From<ClientError> for Fail {
    fn from(error: ClientError) -> Fail {
        Fail::Client(error)
    }
}

/// The state a scenario builds up: the sessions, the watches it opened and the blobs it made.
pub struct Driver {
    root: Client,
    current: Client,
    watches: BTreeMap<u64, Watch>,
    blobs: BTreeMap<String, Vec<u8>>,
    /// Watches whose first event a mutated driver has dropped.
    dropped: BTreeSet<u64>,
    mutation: Mutation,
}

impl Driver {
    pub fn new(root: Client, mutation: Mutation) -> Driver {
        Driver {
            current: root.clone(),
            root,
            watches: BTreeMap::new(),
            blobs: BTreeMap::new(),
            dropped: BTreeSet::new(),
            mutation,
        }
    }

    /// The one output line for one input line.
    pub fn answer(&mut self, line: &str) -> String {
        let cmd = match syntax::parse(line) {
            Ok(cmd) => cmd,
            Err(_) => return self.error_line(None),
        };
        match self.run(cmd) {
            Ok(text) => text,
            Err(Fail::Usage) => self.error_line(None),
            Err(Fail::Local) => "error local".to_owned(),
            Err(Fail::Client(ClientError::Service(error))) => self.error_line(Some(error.code)),
            Err(Fail::Client(_)) => "error local".to_owned(),
        }
    }

    fn error_line(&self, code: Option<ErrorCode>) -> String {
        match (self.mutation, code) {
            // A mutated driver answers every error with one code.
            (Mutation::SwallowCode, _) => show::error_code(ErrorCode::Other),
            (_, Some(code)) => show::error_code(code),
            (_, None) => "error usage".to_owned(),
        }
    }

    fn run(&mut self, cmd: Cmd) -> Result<String, Fail> {
        Ok(match cmd {
            Cmd::Ping => show::pong(self.current.ping()?),
            Cmd::Session(name) => {
                let name = if name.is_empty() {
                    DEFAULT_SESSION.to_owned()
                } else {
                    name
                };
                self.current = self.root.in_session(&name)?;
                show::session(&name)
            }
            Cmd::LoadPlugin(path) => show::plugin(&self.current.load_plugin(&path)?),
            Cmd::PutGraph { name, file } => {
                let json = std::fs::read_to_string(&file).map_err(|_| Fail::Local)?;
                show::graph(&name, &self.current.put_graph(&name, &json)?)
            }
            Cmd::PatchGraph { name, file } => {
                let patch = std::fs::read_to_string(&file).map_err(|_| Fail::Local)?;
                show::graph(&name, &self.current.patch_graph(&name, &patch)?)
            }
            Cmd::Plan { graph, options } => show::plan(&self.current.plan(&graph, options)?),
            Cmd::Blob { name, hex } => {
                let bytes = syntax::unhex(&hex).ok_or(Fail::Usage)?;
                let len = bytes.len();
                self.blobs.insert(name.clone(), bytes);
                show::blob(&name, len)
            }
            Cmd::MakePlan {
                graph,
                blob,
                config,
            } => {
                let (hash, bytes) = self.current.make_plan(&graph, config)?;
                let len = bytes.len();
                self.blobs.insert(blob, bytes);
                show::made(hash, len)
            }
            Cmd::PutPlan { name, blob } => {
                let bytes = self.blobs.get(&blob).ok_or(Fail::Usage)?.clone();
                self.current.put_plan(&name, &bytes)?;
                show::ack()
            }
            Cmd::Start { graph, config } => show::started(self.current.start(&graph, config)?),
            Cmd::Feed { run, port, value } => {
                let fed = self.current.feed(run, &port, value)?;
                show::fed(fed.woke, fed.dropped)
            }
            Cmd::Step { run, count } => show::stepped(&self.current.step(run, count)?),
            Cmd::Take { run, port } => {
                let mut value = self.current.take(run, &port)?;
                if self.mutation == Mutation::WrongValue {
                    value = value.map(bump);
                }
                show::taken(&value)
            }
            Cmd::Watch { id, target, kinds } => {
                let watch = self.current.watch(target, kinds)?;
                self.watches.insert(id, watch);
                show::watching(id)
            }
            Cmd::Next { id } => self.next(id)?,
            Cmd::Unwatch { id } => {
                // The watch stays in the table, so events already queued can still be read.
                self.watches.get(&id).ok_or(Fail::Usage)?.close();
                show::unwatched(id)
            }
            Cmd::Reload { run, graph } => show::reloaded(&self.current.reload(run, &graph)?),
            Cmd::Cancel { run } => {
                self.current.cancel(run)?;
                show::ack()
            }
            Cmd::Stop { run, mode } => {
                let stopped = self.current.stop(run, mode)?;
                show::stopped(stopped.tick, stopped.dropped)
            }
            Cmd::Inspect(target) => show::inspection(&self.current.inspect(target)?),
            Cmd::CacheClear => {
                self.current.cache_clear()?;
                show::ack()
            }
            Cmd::Trace(limit) => show::trace(&self.current.trace(limit)?),
        })
    }

    fn next(&mut self, id: u64) -> Result<String, Fail> {
        let watch = self.watches.get(&id).ok_or(Fail::Usage)?;
        if self.mutation == Mutation::DropEvent && self.dropped.insert(id) {
            // The first event of the watch is lost, and the line says the stream ended.
            if watch.next_event().is_some() {
                return Ok(show::stream_ended(id));
            }
        }
        Ok(match watch.next_event() {
            Some(event) => show::event(id, &event),
            None => show::stream_ended(id),
        })
    }
}

/// A mutated driver's wrong value: one more than a `u32`, and anything else unchanged.
fn bump(mut value: TypedValue) -> TypedValue {
    if let (WireType::Scalar(_), WireValue::U32(n)) = (&value.ty, &value.value) {
        value.value = WireValue::U32(n.wrapping_add(1));
    }
    value
}
