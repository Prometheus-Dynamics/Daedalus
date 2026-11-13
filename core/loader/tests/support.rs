//! Builds the dylib fixture once per test binary, and hands out copies of it.

use std::path::{Path, PathBuf};
use std::process::Command;
use std::sync::OnceLock;

fn fixture_source() -> PathBuf {
    Path::new(env!("CARGO_MANIFEST_DIR")).join("../../testing/fixtures/dylib/Cargo.toml")
}

/// Builds the library of `manifest` into its own directory under the test target directory,
/// with `flags` as extra compiler flags, and gives the path of the library named `lib`.
fn build(manifest: PathBuf, dir: &str, lib: &str, flags: Option<&str>) -> PathBuf {
    let target = Path::new(env!("CARGO_TARGET_TMPDIR")).join(dir);
    let cargo = std::env::var("CARGO").unwrap_or_else(|_| "cargo".to_owned());
    let mut command = Command::new(cargo);
    command
        .args(["build", "-q", "--manifest-path"])
        .arg(manifest)
        .arg("--target-dir")
        .arg(&target);
    if let Some(flags) = flags {
        let mut all = std::env::var("RUSTFLAGS").unwrap_or_default();
        all.push(' ');
        all.push_str(flags);
        command.env("RUSTFLAGS", all.trim());
    }
    let status = command.status().expect("cargo runs");
    assert!(status.success(), "{lib} builds");
    target.join("debug").join(format!(
        "{}{lib}{}",
        std::env::consts::DLL_PREFIX,
        std::env::consts::DLL_SUFFIX
    ))
}

/// The fixture library, built by cargo into the test target directory.
pub fn fixture_library() -> PathBuf {
    static BUILT: OnceLock<PathBuf> = OnceLock::new();
    BUILT
        .get_or_init(|| {
            build(
                fixture_source(),
                "dylib-fixture",
                "daedalus_dylib_fixture",
                None,
            )
        })
        .clone()
}

/// The same fixture built as a plugin made before the newest fields were added: its `StepCx`
/// ends where every build's does.
#[allow(dead_code)] // each test binary uses the fixtures it needs
pub fn baseline_library() -> PathBuf {
    static BUILT: OnceLock<PathBuf> = OnceLock::new();
    BUILT
        .get_or_init(|| {
            build(
                fixture_source(),
                "dylib-fixture-baseline",
                "daedalus_dylib_fixture",
                Some("--cfg daedalus_abi_baseline"),
            )
        })
        .clone()
}

/// A library with only the entry symbol plugins had before they described their shapes.
#[allow(dead_code)] // each test binary uses the fixtures it needs
pub fn old_library() -> PathBuf {
    static BUILT: OnceLock<PathBuf> = OnceLock::new();
    BUILT
        .get_or_init(|| {
            let manifest = Path::new(env!("CARGO_MANIFEST_DIR"))
                .join("../../testing/fixtures/dylib-old/Cargo.toml");
            build(
                manifest,
                "dylib-old-fixture",
                "daedalus_dylib_old_fixture",
                None,
            )
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
