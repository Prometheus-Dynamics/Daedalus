//! The C worker SDK's API, in Rust. Every `dd_*` function of `languages/cpp/sdk/include/daedalus_worker.h`
//! is exported from this crate, built as a static library that a C or C++ worker links. The wire
//! codec and the worker messages are `daedalus-wire`'s; this crate converts between the C layouts
//! and the wire's typed values, and runs the loop over them.
//!
//! The unsafe code is the C boundary: reading the type tables and values a C worker passes, calling
//! its node functions, and the stdio descriptors. Each unsafe block says why it is sound.

mod arena;
mod convert;
mod ctx;
pub mod ffi;
mod internal;
mod run;
mod send;
mod serve;
mod stdio;

pub use ctx::DdCtx;
pub use serve::serve_io;
