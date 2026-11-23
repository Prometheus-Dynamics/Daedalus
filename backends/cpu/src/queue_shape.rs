//! The queue an edge's policy asks for, in the forms that lay out their own queues.

use daedalus_backend::Refusal;
use daedalus_graph::EdgePolicyDoc;
use daedalus_runner::Overflow;
use daedalus_types::NodeIndex;

/// Capacity and what a full queue does. `fifo` is the capacity of a FIFO that declares none.
/// These forms' queues keep a value until it is read, so a `per_tick` edge into `reader` is
/// refused: the steps form runs it.
pub(crate) fn queue_shape(
    policy: &EdgePolicyDoc,
    fifo: u32,
    reader: NodeIndex,
) -> Result<(u32, Overflow), Refusal> {
    Ok(match policy {
        EdgePolicyDoc::Fifo => (fifo.max(1), Overflow::Error),
        EdgePolicyDoc::LatestOnly => (1, Overflow::DropOldest),
        EdgePolicyDoc::Bounded { capacity, overflow } => (
            (*capacity).max(1),
            match overflow.as_str() {
                "drop_newest" => Overflow::DropNewest,
                "error_on_full" | "block" => Overflow::Error,
                _ => Overflow::DropOldest,
            },
        ),
        EdgePolicyDoc::PerTick => {
            return Err(Refusal::new(
                vec![reader],
                "a per_tick edge needs the steps form: this form's queues keep a value until it is read",
            ));
        }
    })
}
