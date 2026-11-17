//! The Node SDK's unit tests, under cargo: `node --test` over every file in `sdk/test/`. The
//! codec and client tests need no daemon; the worker tests start a worker from the SDK. A missing
//! `node` (or one older than the SDK's engines floor, 22) is a skip, said on stderr.

use std::path::{Path, PathBuf};
use std::process::Command;

fn sdk_tests() -> Vec<PathBuf> {
    let dir = Path::new(env!("CARGO_MANIFEST_DIR"))
        .join("sdk")
        .join("test");
    let mut files: Vec<PathBuf> = std::fs::read_dir(&dir)
        .expect("the SDK's test directory")
        .filter_map(|entry| entry.ok().map(|entry| entry.path()))
        .filter(|path| path.to_string_lossy().ends_with(".test.js"))
        .collect();
    files.sort();
    files
}

fn node_major() -> Option<u32> {
    let output = Command::new("node").arg("--version").output().ok()?;
    let text = String::from_utf8(output.stdout).ok()?;
    text.trim()
        .trim_start_matches('v')
        .split('.')
        .next()?
        .parse()
        .ok()
}

#[test]
// The skip line goes to stderr, where a run without a Node 22 shows why nothing ran.
#[allow(clippy::print_stderr)]
fn the_node_sdk_unit_tests_pass() {
    match node_major() {
        Some(major) if major >= 22 => {}
        _ => {
            eprintln!("skipped: no Node 22 on the path, so the Node SDK's unit tests did not run");
            return;
        }
    }
    let files = sdk_tests();
    assert!(!files.is_empty(), "the SDK has no unit tests");
    let output = Command::new("node")
        .current_dir(Path::new(env!("CARGO_MANIFEST_DIR")).join("sdk"))
        .arg("--test")
        .args(&files)
        .output()
        .expect("node runs");
    assert!(
        output.status.success(),
        "node --test failed:\n{}{}",
        String::from_utf8_lossy(&output.stdout),
        String::from_utf8_lossy(&output.stderr)
    );
}
