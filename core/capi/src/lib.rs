//! The C surface of Daedalus: a host that loads plugins, compiles graphs over their nodes, and
//! runs them, through functions any language can call. `include/daedalus.h` is the contract,
//! written by hand, and a test holds every function here to it.
//!
//! Every call returns a [`Status`]. Results come back through pointers the caller gives. A host
//! and a graph are handles: 64-bit numbers that name a place and a generation, so a handle that
//! was released, or was never one, is answered with a status and never followed. No call lets a
//! panic cross into C: each is run under a guard that turns one into `DAEDALUS_PANIC`.
//!
//! A host made here has no nodes of its own. Its nodes and the backends that run them come
//! from plugins, installed by path.

mod graph;
mod guard;
mod handles;
mod host;
mod io;
mod status;

pub use graph::{
    PortInfo, Telemetry, TickInfo, daedalus_graph_compile, daedalus_graph_explain,
    daedalus_graph_port, daedalus_graph_port_count, daedalus_graph_port_index,
    daedalus_graph_release, daedalus_graph_telemetry, daedalus_tick,
};
pub use guard::{daedalus_last_error, daedalus_selftest_panic};
pub use host::{
    Version, daedalus_host_create, daedalus_host_install, daedalus_host_release,
    daedalus_host_schemas, daedalus_version,
};
pub use io::{
    ByteSpan, KIND_BOOL, KIND_BYTES, KIND_F64, KIND_I64, Push, PushValue, daedalus_push_batch,
    daedalus_push_bool, daedalus_push_bool_at, daedalus_push_bytes, daedalus_push_bytes_at,
    daedalus_push_f64, daedalus_push_f64_at, daedalus_push_i64, daedalus_push_i64_at,
    daedalus_take_bool, daedalus_take_bool_at, daedalus_take_bytes, daedalus_take_bytes_at,
    daedalus_take_f64, daedalus_take_f64_at, daedalus_take_i64, daedalus_take_i64_at,
};
pub use status::{Status, daedalus_status_name};

/// A host or a graph. Zero is never one.
pub type Handle = u64;

/// How many hosts and graphs are live in this process. For tests that nothing is left behind.
#[doc(hidden)]
pub fn live_handles() -> (usize, usize) {
    (host::HOSTS.live(), graph::GRAPHS.live())
}
