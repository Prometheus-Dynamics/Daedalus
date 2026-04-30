use super::errors::NodeError;
use crate::io::NodeIo;
use crate::plan::RuntimeNode;
use crate::state::ExecutionContext;
use daedalus_transport::Payload;
use std::sync::Arc;

pub type DirectPayloadFn = Arc<
    dyn Fn(&RuntimeNode, &ExecutionContext, Payload) -> Result<Option<Payload>, NodeError>
        + Send
        + Sync,
>;

/// Handler abstraction for executing a node.
///
/// ```no_run
/// use daedalus_runtime::executor::NodeHandler;
/// use daedalus_runtime::io::NodeIo;
/// use daedalus_runtime::state::ExecutionContext;
///
/// fn handler(
///     _node: &daedalus_runtime::RuntimeNode,
///     _ctx: &ExecutionContext,
///     _io: &mut NodeIo,
/// ) -> Result<(), daedalus_runtime::executor::NodeError> {
///     Ok(())
/// }
///
/// let _h: &dyn NodeHandler = &handler;
/// ```
pub trait NodeHandler: Send + Sync {
    fn run(
        &self,
        node: &RuntimeNode,
        ctx: &ExecutionContext,
        io: &mut NodeIo,
    ) -> Result<(), NodeError>;

    fn direct_payload_handler(&self, _stable_id: u128) -> Option<DirectPayloadFn> {
        None
    }
}

impl<F> NodeHandler for F
where
    F: Fn(&crate::plan::RuntimeNode, &ExecutionContext, &mut NodeIo) -> Result<(), NodeError>
        + Send
        + Sync,
{
    fn run(
        &self,
        node: &RuntimeNode,
        ctx: &ExecutionContext,
        io: &mut NodeIo,
    ) -> Result<(), NodeError> {
        (self)(node, ctx, io)
    }
}
