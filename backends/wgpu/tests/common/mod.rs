//! Shared by the tests that need a real adapter.

#![allow(dead_code)]

use daedalus_wgpu::{CapsRecord, Options, WgpuBackend};

/// The caps of this machine's adapter, or `None` to skip. With `DAEDALUS_GPU_REQUIRED=1`, as in
/// the GPU lane of CI, having no adapter is a failure instead.
pub fn adapter() -> Option<CapsRecord> {
    match WgpuBackend::open(Options::default()).start() {
        Ok(record) => Some(record),
        Err(error) => {
            assert!(
                std::env::var_os("DAEDALUS_GPU_REQUIRED").is_none(),
                "this lane needs an adapter: {error}"
            );
            None
        }
    }
}
