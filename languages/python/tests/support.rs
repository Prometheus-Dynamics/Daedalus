//! What the Python tests share: the interpreter, the built shared libraries, and the fixture plugin.
//! Cargo builds the libraries beside the test binary (as dev-dependencies), so they are found in
//! the test's deps folder, as the `languages/rust-capi` tests find theirs.

use std::path::{Path, PathBuf};
use std::process::Command;
use std::sync::OnceLock;

/// The interpreter: `python3`, or `python` where that is the name. `None` when neither runs.
pub fn python() -> Option<String> {
    ["python3", "python"].into_iter().find_map(|name| {
        Command::new(name)
            .arg("--version")
            .output()
            .is_ok_and(|out| out.status.success())
            .then(|| name.to_owned())
    })
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
        let target = Path::new(env!("CARGO_TARGET_TMPDIR")).join("python-plugin");
        daedalus_drive_conformance::build_fixture(&target).expect("the dylib fixture builds")
    })
}
