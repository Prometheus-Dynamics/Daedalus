//! Helpers shared by the host-graph integration tests.

use daedalus_data::model::Value;
use daedalus_runtime::RuntimeNode;
use daedalus_runtime::executor::{NodeError, NodeHandler};
use daedalus_runtime::io::NodeIo;
use daedalus_runtime::state::ExecutionContext;

/// The type key of the `Int` values the host-graph tests feed and read.
pub(crate) const INT_KEY: &str = "typeexpr:{\"Scalar\":\"Int\"}";

/// Adds one to the int on `in` and pushes it to `out`, for the `inc` node.
pub(crate) struct IncrementHandler;

impl NodeHandler for IncrementHandler {
    fn run(
        &self,
        node: &RuntimeNode,
        _ctx: &ExecutionContext,
        io: &mut NodeIo,
    ) -> Result<(), NodeError> {
        if node.id == "inc" {
            let Some(Value::Int(value)) = io.get_typed_ref::<Value>("in") else {
                return Err(NodeError::InvalidInput("expected int".to_string()));
            };
            let next = Value::Int(value + 1);
            io.push_as_to("out", INT_KEY.into(), next);
        }
        Ok(())
    }
}
