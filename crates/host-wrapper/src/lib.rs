//! `daedalus-host`: a safe Rust wrapper over the `daedalus-surface` C ABI, for shared mode.
//!
//! A process links either the Daedalus core (static mode) or this crate plus the surface library
//! (shared mode), never both. This crate names no core type: it loads the surface by path with
//! `libloading`, checks its handshake against the `lemnos-abi` layout hash, and returns RAII
//! handles ([`Host`], [`Graph`]) whose `Drop` releases them. Scalars, bytes and (on Linux) dma-buf
//! frames are the values that cross. Ports go by name or, resolved once, by index ([`InputPort`],
//! [`OutputPort`]), and [`Graph::push_batch`] pushes several values in one call.
//!
//! ```no_run
//! # fn main() -> Result<(), daedalus_host::HostError> {
//! let surface = daedalus_host::Surface::load("libdaedalus_surface.so")?;
//! let host = surface.create_host()?;
//! host.install("libmy_plugin.so")?;
//! let graph = host.compile(r#"{"format":"daedalus.graph", ...}"#)?;
//! graph.push_i64("step", 1)?;
//! graph.tick()?;
//! let total = graph.take_i64("total")?;
//! # let _ = total; Ok(()) }
//! ```

mod error;
#[cfg(target_os = "linux")]
mod frame;
mod host;
mod ports;
mod surface;

pub use error::HostError;
#[cfg(target_os = "linux")]
pub use frame::{Frame, FrameLease};
pub use host::{Graph, Host};
pub use ports::{InputPort, OutputPort, Push};
pub use surface::{SURFACE_ABI, Surface, SurfaceFeatures};
