//! What the Python tests share: the interpreter, the built shared libraries, and the fixture plugin.
//! Cargo builds the libraries beside the test binary (as dev-dependencies), so they are found in
//! the test's deps folder, as the `languages/rust-capi` tests find theirs.

use std::path::{Path, PathBuf};
use std::process::Command;
use std::sync::OnceLock;

use daedalus_lang_python::DEFAULT_INTERPRETER;

/// The interpreter the SDK's spec starts, [`DEFAULT_INTERPRETER`], when it runs here. `None` when
/// it doesn't: these tests then say so and do nothing.
pub fn python() -> Option<String> {
    Command::new(DEFAULT_INTERPRETER)
        .arg("--version")
        .output()
        .is_ok_and(|out| out.status.success())
        .then(|| DEFAULT_INTERPRETER.to_owned())
}

/// The built shared library `name` (without prefix or suffix), beside this test binary.
pub fn library(name: &str) -> PathBuf {
    let exe = std::env::current_exe().expect("the test's own path");
    let file = format!(
        "{}{name}{}",
        std::env::consts::DLL_PREFIX,
        std::env::consts::DLL_SUFFIX
    );
    let found = exe.parent().expect("the deps directory").join(file);
    assert!(found.exists(), "{} isn't there", found.display());
    found
}

/// The dylib plugin fixture, built once into this test's target area.
pub fn plugin() -> &'static Path {
    static BUILT: OnceLock<PathBuf> = OnceLock::new();
    BUILT.get_or_init(|| {
        let target = Path::new(env!("CARGO_TARGET_TMPDIR")).join("dylib-fixture");
        daedalus_drive_conformance::build_fixture(&target).expect("the dylib fixture builds")
    })
}
