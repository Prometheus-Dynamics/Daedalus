//! The Node SDK's unit tests, under cargo: `node --test` over every file in `sdk/test/`. The
//! codec and client tests need no daemon; the worker tests start a worker from the SDK. A missing
//! `node` (or one older than the SDK's engines floor, 22) is a skip, said on stderr.
//!
//! A test that hangs fails, named: `node --test` stops any one test after `TEST_LIMIT`, and the run
//! as a whole is stopped after `RUN_LIMIT`. The output goes to files, not pipes: a child the tests
//! start that outlives them would hold a pipe open, and reading it to its end would never finish.

use std::fs::File;
use std::path::{Path, PathBuf};
use std::process::{Command, Stdio};
use std::time::{Duration, Instant};

/// The whole suite takes about a second; a test still going after this is hung.
const TEST_LIMIT: Duration = Duration::from_secs(120);
/// `node --test` itself, past every test's limit: a process that won't exit.
const RUN_LIMIT: Duration = Duration::from_secs(300);

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
    let dir = Path::new(env!("CARGO_TARGET_TMPDIR")).join("node-units");
    std::fs::create_dir_all(&dir).expect("the output directory");
    let (out, err) = (dir.join("stdout"), dir.join("stderr"));
    let mut child = Command::new("node")
        .current_dir(Path::new(env!("CARGO_MANIFEST_DIR")).join("sdk"))
        .arg("--test")
        .arg(format!("--test-timeout={}", TEST_LIMIT.as_millis()))
        .args(&files)
        .stdin(Stdio::null())
        .stdout(File::create(&out).expect("stdout file"))
        .stderr(File::create(&err).expect("stderr file"))
        .spawn()
        .expect("node runs");
    let started = Instant::now();
    let status = loop {
        if let Some(status) = child.try_wait().expect("node can be waited on") {
            break Some(status);
        }
        if started.elapsed() > RUN_LIMIT {
            let _ = child.kill();
            let _ = child.wait();
            break None;
        }
        std::thread::sleep(Duration::from_millis(50));
    };
    let read = |path: &Path| std::fs::read_to_string(path).unwrap_or_default();
    assert!(
        status.is_some_and(|status| status.success()),
        "node --test {}:\n{}{}",
        match status {
            Some(status) => format!("failed ({status})"),
            None => format!("was still running after {RUN_LIMIT:?} and was stopped"),
        },
        read(&out),
        read(&err)
    );
}
