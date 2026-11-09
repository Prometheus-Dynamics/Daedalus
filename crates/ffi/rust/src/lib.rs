//! Rust SDK for out-of-process Daedalus plugins.
//!
//! A plugin is written exactly like an in-process Rust plugin: `#[node]` functions with typed
//! ports, `#[derive(NodeConfig)]` config, `state(T)` nodes and capability nodes, installed through
//! `declare_plugin!`. [`Worker`] takes that plugin, reads its schema from the same registry the
//! host would, and serves it as a persistent worker over stdin and stdout. The host starts the
//! worker binary, negotiates the worker protocol and invokes nodes by id.
//!
//! The worker depends on the registry and handler machinery of the `daedalus` facade, not on the
//! stable in-process plugin ABI, so a worker built against one Daedalus version runs under any host
//! that speaks the same worker protocol version.
//!
//! ```ignore
//! use daedalus::{PluginRegistry, declare_plugin, macros::node, runtime::NodeError};
//!
//! #[node(id = "add", inputs("a", "b"), outputs("out"))]
//! fn add(a: i64, b: i64) -> Result<i64, NodeError> {
//!     Ok(a + b)
//! }
//!
//! declare_plugin!(MathPlugin, "demo.math", [add]);
//!
//! fn main() -> std::io::Result<()> {
//!     daedalus_ffi_rust::Worker::from_plugin(&MathPlugin::new())
//!         .map_err(std::io::Error::other)?
//!         .serve()
//! }
//! ```
//!
//! What the worker does not share with the in-process path is documented in `README.md`. In short:
//! state lives in the worker process (it is not exported as a `WireValue`), and images are not
//! converted yet.

mod events;
mod invoke;
mod schema;
mod worker;

pub use daedalus_ffi_core as core;
pub use daedalus_ffi_core::WORKER_PROTOCOL_VERSION;
pub use events::{emit_event, event_debug, event_info, event_trace, event_warn};
pub use schema::rust_worker_package;
pub use worker::{Worker, WorkerBuildError};
