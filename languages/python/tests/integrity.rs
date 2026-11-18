//! The interpreted-worker path, end to end. The program is the SDK's default interpreter, found on
//! the path and checked as a file; the worker's script is named with `also_check`. Record writes
//! both entries, Locked starts the worker from them, and a changed script is refused before the
//! start.
//!
//! Linux only, as the checks read `/proc/self/fd`. It does nothing where that interpreter is not on
//! the path: the test returns at once, and says nothing.

#![cfg(target_os = "linux")]

use std::path::{Path, PathBuf};

use daedalus_backend_process::{ProcessBackend, ProcessError};
use daedalus_integrity::{Integrity, find_program, read_lock};
use daedalus_lang_python::{DEFAULT_INTERPRETER, spec};
use daedalus_worker_conformance::runner::registry;

/// A scratch directory for this test, emptied first.
fn scratch() -> PathBuf {
    let dir = Path::new(env!("CARGO_TARGET_TMPDIR")).join("python-integrity");
    let _ = std::fs::remove_dir_all(&dir);
    std::fs::create_dir_all(&dir).expect("scratch");
    dir
}

/// A copy of the fixture worker and its node modules, beside a link to the SDK. The worker finds
/// its SDK and its modules from its own directory, so the copy runs as the original does, and the
/// repository's fixture is never changed.
fn worker_copy(dir: &Path) -> PathBuf {
    let fixtures = Path::new(env!("CARGO_MANIFEST_DIR")).join("fixtures");
    let copy = dir.join("fixtures");
    std::fs::create_dir_all(&copy).expect("fixtures dir");
    for name in ["worker.py", "nodes.py", "showcase.py"] {
        std::fs::copy(fixtures.join(name), copy.join(name)).expect("copy");
    }
    let sdk = Path::new(env!("CARGO_MANIFEST_DIR")).join("sdk");
    std::os::unix::fs::symlink(&sdk, dir.join("sdk")).expect("link the sdk");
    copy.join("worker.py")
}

#[test]
fn an_interpreted_worker_is_checked_recorded_run_and_a_changed_script_refused() {
    let path_var = std::env::var_os("PATH");
    if find_program(DEFAULT_INTERPRETER.as_ref(), path_var.as_deref()).is_none() {
        return;
    }
    let dir = scratch();
    let script = worker_copy(&dir);
    let lock = dir.join("daedalus.lock");

    // Record: both the interpreter and the script are written.
    let mut recording =
        ProcessBackend::new().with_integrity(Integrity::record(&lock).expect("record mode"));
    recording
        .add_worker(spec(&script).also_check(&script), &registry())
        .expect("record starts the worker");
    let recorded = read_lock(&lock).expect("the lock was written");
    let python =
        find_program(DEFAULT_INTERPRETER.as_ref(), path_var.as_deref()).expect("the interpreter");
    let digest_of =
        |path: &Path| daedalus_integrity::Digest::of(&std::fs::read(path).expect("read"));
    assert_eq!(
        recorded.get(DEFAULT_INTERPRETER),
        Some(&digest_of(&python)),
        "the interpreter"
    );
    assert_eq!(
        recorded.get("worker.py"),
        Some(&digest_of(&script)),
        "the script"
    );

    // Locked: the worker starts from the interpreter and the script as the lock approves them.
    let mut checking =
        ProcessBackend::new().with_integrity(Integrity::locked(&lock).expect("locked mode"));
    checking
        .add_worker(spec(&script).also_check(&script), &registry())
        .expect("the approved worker starts");
    assert!(
        checking.worker_pid("fixture.scalar_add").is_some(),
        "the worker is running"
    );
    assert_eq!(checking.telemetry().spawns, 1);

    // The script changes. The start is refused before the worker runs, and the message names it.
    let mut changed = std::fs::read(&script).expect("read");
    changed.extend_from_slice(b"\n# a change the lock does not approve\n");
    std::fs::write(&script, changed).expect("write");
    let mut refused =
        ProcessBackend::new().with_integrity(Integrity::locked(&lock).expect("locked mode"));
    let error = refused
        .add_worker(spec(&script).also_check(&script), &registry())
        .expect_err("a changed script is refused");
    match error {
        ProcessError::Integrity(inner) => {
            let text = inner.to_string();
            assert!(text.contains("worker.py"), "{text}");
            assert!(text.contains("is not the file the lock approves"), "{text}");
        }
        other => panic!("expected the check to refuse before the start, got {other}"),
    }
    assert!(refused.worker_pid("fixture.scalar_add").is_none());
    assert_eq!(refused.telemetry().spawns, 0, "nothing was started");
}
