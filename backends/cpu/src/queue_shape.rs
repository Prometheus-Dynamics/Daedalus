//! The queue an edge's policy asks for, in the forms that lay out their own queues.

use daedalus_graph::EdgePolicyDoc;
use daedalus_runner::Overflow;

/// Capacity and what a full queue does. `fifo` is the capacity of a FIFO that declares none.
pub(crate) fn queue_shape(policy: &EdgePolicyDoc, fifo: u32) -> (u32, Overflow) {
    match policy {
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
    }
}
