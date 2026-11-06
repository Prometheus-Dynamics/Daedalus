//! `daedalus-surface`: the shared-library surface of Daedalus.
//!
//! A small C ABI over the in-process graph runtime, for a process that does not link the core
//! (shared mode, see `docs/shared-library.md`). Opaque generation-counted handles, status codes,
//! byte buffers and scalars cross; no Rust type does. Every entry point runs under the
//! `lemnos-abi` panic guard, and calls are per frame or per job, never per node.
//!
//! What is built: the version handshake, host lifecycle, stable-path plugin install, graph
//! compile from a `daedalus.graph` document, scalar and byte push, tick, scalar and byte take,
//! the node schema, explain and telemetry JSON, and dma-buf frames on Linux: push a frame into a
//! `daedalus:frame` port, take one out as a lease (`daedalus_surface_push_frame`,
//! `daedalus_surface_take_frame`, `daedalus_surface_frame_release`). Frames cross as file
//! descriptors that are shared, never copied; off Linux those calls return `NOT_SUPPORTED`.
//!
//! Linux is the tested platform. The code is portable Rust; only the shared-mode test is
//! Unix-only.

mod abi;
#[cfg(feature = "alloc-count")]
mod alloc;
mod ffi;
mod frames;
mod graph;
mod host;
mod io;
mod state;

#[cfg(test)]
mod tests;

pub use abi::{
    FEATURE_ALLOC_COUNT, FEATURE_GRAPHS, FEATURE_PLUGINS, FEATURE_TELEMETRY, SURFACE_MAJOR,
    SURFACE_MINOR, feature_set,
};
