//! The Node worker, as a worker spec. The SDK is in `sdk/`, the canonical fixture nodes in
//! `fixtures/`, and the fixture worker is `fixtures/worker.mjs`.

#![forbid(unsafe_code)]

use std::path::{Path, PathBuf};

use daedalus_backend_process::WorkerSpec;

/// The fixture worker script, in this crate.
pub fn fixture_worker() -> PathBuf {
    Path::new(env!("CARGO_MANIFEST_DIR"))
        .join("fixtures")
        .join("worker.mjs")
}

/// A spec that starts `script` with `node`.
pub fn spec(script: &Path) -> WorkerSpec {
    WorkerSpec::new("node").arg(script.display().to_string())
}
