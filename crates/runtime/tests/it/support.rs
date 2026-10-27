//! Node handlers shared by the executor-level integration tests.

use std::sync::Arc;

use daedalus_runtime::executor::NodeError;
use daedalus_runtime::io::NodeIo;
use daedalus_runtime::state::ExecutionContext;
use daedalus_runtime::sync::Mutex;
use daedalus_runtime::{NodeHandler, RuntimeNode};

/// Records the id of every node it runs.
pub(super) struct LogHandler {
    pub(super) log: Arc<Mutex<Vec<String>>>,
}

impl NodeHandler for LogHandler {
    fn run(
        &self,
        node: &RuntimeNode,
        _ctx: &ExecutionContext,
        _io: &mut NodeIo,
    ) -> Result<(), NodeError> {
        self.log.lock().push(node.id.clone());
        Ok(())
    }
}

/// Forwards the `in` payloads of the `echo` node to `out`.
pub(super) struct EchoHandler;

impl NodeHandler for EchoHandler {
    fn run(
        &self,
        node: &RuntimeNode,
        _ctx: &ExecutionContext,
        io: &mut NodeIo,
    ) -> Result<(), NodeError> {
        if node.id == "echo" {
            let inputs: Vec<_> = io
                .inputs_for("in")
                .map(|payload| payload.inner.clone())
                .collect();
            for payload in inputs {
                io.push_payload("out", payload);
            }
        }
        Ok(())
    }
}
