//! Builds the dylib fixture once per test binary, and hands out copies of it.

use std::path::{Path, PathBuf};
use std::process::Command;
use std::sync::OnceLock;

fn fixture_source() -> PathBuf {
    Path::new(env!("CARGO_MANIFEST_DIR")).join("../../testing/fixtures/dylib/Cargo.toml")
}

/// The fixture library, built by cargo into the test target directory.
pub fn fixture_library() -> PathBuf {
    static BUILT: OnceLock<PathBuf> = OnceLock::new();
    BUILT
        .get_or_init(|| {
            let target = Path::new(env!("CARGO_TARGET_TMPDIR")).join("dylib-fixture");
            let cargo = std::env::var("CARGO").unwrap_or_else(|_| "cargo".to_owned());
            let status = Command::new(cargo)
                .args(["build", "-q", "--manifest-path"])
                .arg(fixture_source())
                .arg("--target-dir")
                .arg(&target)
                .status()
                .expect("cargo runs");
            assert!(status.success(), "the dylib fixture builds");
            target.join("debug").join(format!(
                "{}daedalus_dylib_fixture{}",
                std::env::consts::DLL_PREFIX,
                std::env::consts::DLL_SUFFIX
            ))
        })
        .clone()
}

/// A copy of the fixture at `name` in the test tmp directory. Each copy is a separate library to
/// the loader, which is what a reload needs.
pub fn copy_fixture(name: &str) -> PathBuf {
    let dir = Path::new(env!("CARGO_TARGET_TMPDIR")).join("loader-copies");
    std::fs::create_dir_all(&dir).expect("tmp dir");
    let dest = dir.join(format!("{name}{}", std::env::consts::DLL_SUFFIX));
    let _ = std::fs::remove_file(&dest);
    std::fs::copy(fixture_library(), &dest).expect("copy the fixture");
    dest
}
