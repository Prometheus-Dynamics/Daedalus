//! The Java worker, as a worker spec. The SDK is in `sdk/` and the canonical fixture nodes in
//! `fixtures/`. Classes are compiled with `javac` into a directory the spec names.

#![forbid(unsafe_code)]

use std::io;
use std::path::{Path, PathBuf};
use std::process::Command;

use daedalus_backend_process::WorkerSpec;

/// The Java sources of the SDK and the fixtures, in this crate.
pub fn sources() -> Vec<PathBuf> {
    let root = Path::new(env!("CARGO_MANIFEST_DIR"));
    let mut found = Vec::new();
    for dir in [
        root.join("sdk/src/main/java"),
        root.join("fixtures/src/main/java"),
    ] {
        collect(&dir, &mut found);
    }
    found
}

fn collect(dir: &Path, found: &mut Vec<PathBuf>) {
    let Ok(entries) = std::fs::read_dir(dir) else {
        return;
    };
    for entry in entries.flatten() {
        let path = entry.path();
        if path.is_dir() {
            collect(&path, found);
        } else if path.extension().is_some_and(|ext| ext == "java") {
            found.push(path);
        }
    }
}

/// Compiles the SDK and the fixtures into `out`, with `javac`.
pub fn compile(out: &Path, extra: &[PathBuf]) -> io::Result<()> {
    std::fs::create_dir_all(out)?;
    let status = Command::new("javac")
        .arg("-d")
        .arg(out)
        .args(sources())
        .args(extra)
        .status()?;
    if status.success() {
        Ok(())
    } else {
        Err(io::Error::other("javac failed"))
    }
}

/// A spec that starts the fixture worker from `classes`, with the JVM's log lines sent to stderr,
/// so a JVM warning never reaches the packet stream.
pub fn spec(classes: &Path) -> WorkerSpec {
    WorkerSpec::new("java")
        .arg("-Xlog:disable")
        .arg("-Xlog:all=warning:stderr")
        .arg("-cp")
        .arg(classes.display().to_string())
        .arg("fixtures.FixtureWorker")
}
