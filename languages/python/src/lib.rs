//! The Python worker, as a worker spec. The SDK is in `sdk/` and the canonical fixture nodes in
//! `fixtures/`; the fixture worker is `fixtures/worker.py`.

#![forbid(unsafe_code)]

use std::path::{Path, PathBuf};

use daedalus_backend_process::WorkerSpec;

/// The fixture worker script, in this crate.
pub fn fixture_worker() -> PathBuf {
    Path::new(env!("CARGO_MANIFEST_DIR"))
        .join("fixtures")
        .join("worker.py")
}

/// A spec that starts `script` with `python3`. The script finds its own SDK.
pub fn spec(script: &Path) -> WorkerSpec {
    WorkerSpec::new("python3").arg(script.display().to_string())
}
