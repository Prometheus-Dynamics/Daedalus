//! This crate's integration tests that need no process of their own, as one test binary:
//! one link instead of one per file. A file that needs a process to itself is a
//! `[[test]]` of its own in Cargo.toml, with the reason beside it. A new file goes in one
//! or the other; the boundary check refuses a test file in neither.

#[cfg(feature = "blob")]
#[path = "blob_library.rs"]
mod blob_library;
#[cfg(all(feature = "blob", feature = "receive"))]
#[path = "blob_receive.rs"]
mod blob_receive;
#[cfg(feature = "chip")]
#[path = "chip_queue.rs"]
mod chip_queue;
#[cfg(feature = "chip-params")]
#[path = "chip_scalar.rs"]
mod chip_scalar;
#[cfg(feature = "chip-params")]
#[path = "chip_wire.rs"]
mod chip_wire;
#[path = "closure_node.rs"]
mod closure_node;
mod common;
#[path = "compound_ports.rs"]
mod compound_ports;
#[path = "config_faults.rs"]
mod config_faults;
#[path = "const_shapes.rs"]
mod const_shapes;
#[path = "convert_once.rs"]
mod convert_once;
#[path = "default_fallback.rs"]
mod default_fallback;
#[cfg(any(feature = "blob", all(feature = "blob", feature = "receive")))]
#[path = "shared/blob_fixture.rs"]
mod fixture;
#[cfg(feature = "merge")]
#[path = "fusion_equivalence.rs"]
mod fusion_equivalence;
#[path = "handed_bytes.rs"]
mod handed_bytes;
#[path = "inside.rs"]
mod inside;
#[cfg(feature = "merge")]
#[path = "merge.rs"]
mod merge;
#[path = "node_fault.rs"]
mod node_fault;
#[path = "node_metrics.rs"]
mod node_metrics;
#[path = "owned_code.rs"]
mod owned_code;
#[path = "patch_const.rs"]
mod patch_const;
#[cfg(feature = "merge")]
#[path = "reload_state.rs"]
mod reload_state;
#[path = "steps.rs"]
mod steps;
#[path = "take_wait.rs"]
mod take_wait;
#[path = "transfer.rs"]
mod transfer;
#[path = "wait.rs"]
mod wait;
