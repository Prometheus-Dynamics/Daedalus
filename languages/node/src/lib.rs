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

/// A spec that starts `script` with `node`. Node's cold start on a Windows runner can pass the
/// default five-second hello window, so this spec allows thirty seconds.
pub fn spec(script: &Path) -> WorkerSpec {
    let mut spec = WorkerSpec::new("node").arg(script.display().to_string());
    spec.startup = std::time::Duration::from_secs(30);
    spec
}
