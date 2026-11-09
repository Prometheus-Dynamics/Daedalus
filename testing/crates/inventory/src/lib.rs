//! The CPU rows of the acceleration inventory for `next`: one row per (op, backend) for the
//! kernels in `backends/kernels-sys`. The rows are checked against the files and tests they name
//! (`tests/registry.rs`), and the conformance tests record the evidence for them.
#![forbid(unsafe_code)]

mod registry;

pub use registry::{CPU, EXEMPT, KERNEL_ROWS, OPS};
