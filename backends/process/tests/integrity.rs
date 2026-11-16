//! Plugin integrity in the process backend: a worker's program, and the files it names with
//! `also_check`, are checked against the lock before each start. A respawn is checked again, off
//! the tick, and a worker whose file no longer passes stays down until the lock approves it.

// Linux only: the checks read `/proc/self/fd`, and the workers here are Linux processes.
#![cfg(target_os = "linux")]

mod support;

use std::ffi::OsStr;
use std::path::{Path, PathBuf};
use std::process::Command;
use std::time::{Duration, Instant};

use daedalus_abi::testing::{release, run_once};
use daedalus_abi::{Slot, SlotState, Slots, StepCx, StepEnd};
use daedalus_backend::{
    Backend, BodyIndex, CompileCx, Compiled, NO_METADATA, NodeBody, PortRef, RateId, Region,
};
use daedalus_backend_process::{
    BASE_DELAY, MAX_DELAY, ProcessBackend, ProcessError, WorkerSpec, delay_after,
};
use daedalus_integrity::{Digest, Integrity, Lock, find_program, read_lock};
use daedalus_types::{BackendIndex, NodeIndex, PortIndex};
use daedalus_worker_conformance::runner::registry;
use support::worker_binary;

/// A new directory for one test's files.
fn scratch(name: &str) -> PathBuf {
    let dir = Path::new(env!("CARGO_TARGET_TMPDIR")).join(format!("process-integrity-{name}"));
    let _ = std::fs::remove_dir_all(&dir);
    std::fs::create_dir_all(&dir).expect("scratch");
    dir
}

/// A copy of the worker fixture at `dir/name`, which is what a host approves.
fn copy_worker(dir: &Path, name: &str) -> PathBuf {
    let dest = dir.join(name);
    std::fs::copy(worker_binary(), &dest).expect("copy the worker");
    dest
}

fn digest_of(path: &Path) -> Digest {
    Digest::of(&std::fs::read(path).expect("read"))
}

fn write_lock(path: &Path, entries: &[(&str, Digest)]) {
    let mut lock = Lock::new();
    for (key, digest) in entries {
        lock.set(key.to_string(), *digest);
    }
    std::fs::write(path, lock.render()).expect("write lock");
}

/// Puts `from`'s bytes at `to` by a new file renamed over it, so a process running `to` keeps its
/// own bytes.
fn replace_file(from: &Path, to: &Path) {
    let tmp = to.with_extension("tmp");
    std::fs::copy(from, &tmp).expect("copy");
    std::fs::rename(&tmp, to).expect("rename over");
}

fn locked(lock: &Path) -> ProcessBackend {
    ProcessBackend::new().with_integrity(Integrity::locked(lock).expect("locked mode"))
}

/// The worker spec for `program`, with a short startup so a program that is not a worker fails fast.
fn quick(program: &Path) -> WorkerSpec {
    let mut spec = WorkerSpec::new(program);
    spec.startup = Duration::from_millis(300);
    spec
}

fn integrity_message(error: &ProcessError) -> Option<String> {
    match error {
        ProcessError::Integrity(inner) => Some(inner.to_string()),
        _ => None,
    }
}

#[test]
fn an_approved_worker_starts_and_serves_its_node() {
    let dir = scratch("approved");
    let program = copy_worker(&dir, "worker");
    let lock = dir.join("daedalus.lock");
    write_lock(&lock, &[("worker", digest_of(&program))]);

    let mut backend = locked(&lock);
    backend
        .add_worker(WorkerSpec::new(&program), &registry())
        .expect("the approved worker starts");
    assert!(backend.worker_pid("fixture.scalar_add").is_some());
    let _ = std::fs::remove_dir_all(dir);
}

#[test]
fn a_changed_worker_is_refused_and_both_digests_are_named() {
    let dir = scratch("flipped");
    let program = copy_worker(&dir, "worker");
    let lock = dir.join("daedalus.lock");
    let approved = digest_of(&program);
    write_lock(&lock, &[("worker", approved)]);
    let mut bytes = std::fs::read(&program).unwrap();
    let middle = bytes.len() / 2;
    bytes[middle] ^= 0x01;
    std::fs::write(&program, &bytes).unwrap();
    let found = digest_of(&program);

    let error = locked(&lock)
        .add_worker(WorkerSpec::new(&program), &registry())
        .expect_err("a changed worker is refused");
    let text =
        integrity_message(&error).unwrap_or_else(|| panic!("not an integrity refusal: {error}"));
    assert!(text.contains(&approved.to_string()), "{text}");
    assert!(text.contains(&found.to_string()), "{text}");
    let _ = std::fs::remove_dir_all(dir);
}

#[test]
fn a_worker_not_in_the_lock_is_refused() {
    let dir = scratch("unlisted");
    let program = copy_worker(&dir, "worker");
    let lock = dir.join("daedalus.lock");
    write_lock(&lock, &[("other", Digest::of(b"other"))]);

    let error = locked(&lock)
        .add_worker(WorkerSpec::new(&program), &registry())
        .expect_err("an unlisted worker is refused");
    assert!(
        integrity_message(&error).is_some_and(|text| text.contains("daedalus-lock record")),
        "{error}"
    );
    let _ = std::fs::remove_dir_all(dir);
}

#[test]
fn record_writes_the_worker_and_locked_then_accepts_it() {
    let dir = scratch("record");
    let program = copy_worker(&dir, "worker");
    let lock = dir.join("daedalus.lock");

    let mut recording =
        ProcessBackend::new().with_integrity(Integrity::record(&lock).expect("record mode"));
    recording
        .add_worker(WorkerSpec::new(&program), &registry())
        .expect("record starts the worker");
    let recorded = read_lock(&lock).expect("the lock was written");
    assert_eq!(recorded.get("worker"), Some(&digest_of(&program)));

    let mut checking = locked(&lock);
    checking
        .add_worker(WorkerSpec::new(&program), &registry())
        .expect("the recorded worker starts under Locked");
    let _ = std::fs::remove_dir_all(dir);
}

#[test]
fn a_bare_name_is_found_on_the_path_and_checked_under_its_file_name() {
    let dir = scratch("bare");
    let lock = dir.join("daedalus.lock");
    let path_var = std::env::var_os("PATH");
    let found = find_program(OsStr::new("sh"), path_var.as_deref()).expect("sh is on the path");
    write_lock(&lock, &[("sh", digest_of(&found))]);

    // A shell is not a worker: the start fails at its hello, but only after the check passed.
    let mut spec = quick(Path::new("sh"));
    spec = spec.arg("-c").arg("exit 0");
    let error = locked(&lock)
        .add_worker(spec, &registry())
        .expect_err("a shell is no worker");
    assert!(
        integrity_message(&error).is_none(),
        "the check passed: {error}"
    );

    write_lock(&lock, &[("sh", Digest::of(b"not sh"))]);
    let error = locked(&lock)
        .add_worker(quick(Path::new("sh")).arg("-c").arg("exit 0"), &registry())
        .expect_err("a changed sh is refused");
    assert!(integrity_message(&error).is_some(), "{error}");
    let _ = std::fs::remove_dir_all(dir);
}

#[test]
fn a_hashbang_program_is_refused_under_locked_with_the_script_sentence() {
    let dir = scratch("script");
    let script = dir.join("run");
    std::fs::write(&script, "#!/bin/sh\nexit 0\n").unwrap();
    make_executable(&script);
    let lock = dir.join("daedalus.lock");
    write_lock(&lock, &[("run", digest_of(&script))]);

    let error = locked(&lock)
        .add_worker(quick(&script), &registry())
        .expect_err("a script is refused under Locked");
    let text = integrity_message(&error).expect("an integrity refusal");
    assert!(text.contains("is a script"), "{text}");
    assert!(text.contains("also_check"), "{text}");
    let _ = std::fs::remove_dir_all(dir);
}

#[test]
fn a_hashbang_program_runs_by_name_under_record_and_records_nothing() {
    let dir = scratch("script-record");
    let script = dir.join("run");
    std::fs::write(&script, "#!/bin/sh\nexit 0\n").unwrap();
    make_executable(&script);
    let lock = dir.join("daedalus.lock");

    let mut recording =
        ProcessBackend::new().with_integrity(Integrity::record(&lock).expect("record mode"));
    let error = recording
        .add_worker(quick(&script), &registry())
        .expect_err("the script is no worker");
    assert!(integrity_message(&error).is_none(), "{error}");
    assert!(
        read_lock(&lock).is_err() || read_lock(&lock).unwrap().get("run").is_none(),
        "a script writes no entry"
    );
    let _ = std::fs::remove_dir_all(dir);
}

#[test]
fn the_worker_has_no_descriptor_for_its_program_after_exec() {
    let dir = scratch("cloexec");
    let out = dir.join("fds.txt");
    let canonical = std::fs::canonicalize("/bin/sh").expect("/bin/sh");
    let lock = dir.join("daedalus.lock");
    write_lock(&lock, &[("sh", digest_of(Path::new("/bin/sh")))]);

    let spec = quick(Path::new("/bin/sh"))
        .arg("-c")
        .arg(format!("ls -l /proc/self/fd > '{}'", out.display()));
    let _ = locked(&lock).add_worker(spec, &registry());
    let mut listing = String::new();
    for _ in 0..200 {
        if let Ok(text) = std::fs::read_to_string(&out)
            && !text.is_empty()
        {
            listing = text;
            break;
        }
        std::thread::sleep(Duration::from_millis(10));
    }
    assert!(!listing.is_empty(), "the shell wrote its descriptors");
    assert!(
        !listing.contains(&format!("-> {}", canonical.display())),
        "the program's file is not open in the worker: {listing}"
    );
    let _ = std::fs::remove_dir_all(dir);
}

/// Renames the file named by `SWAP_FROM` over the one named by `SWAP_TO`, after the host has
/// checked the program and before it executes it. The spec's environment carries the two names.
fn swap_before_exec(command: &mut Command) {
    let mut from = None;
    let mut to = None;
    for (key, value) in command.get_envs() {
        match key.to_str() {
            Some("SWAP_FROM") => from = value.map(PathBuf::from),
            Some("SWAP_TO") => to = value.map(PathBuf::from),
            _ => {}
        }
    }
    if let (Some(from), Some(to)) = (from, to) {
        let _ = std::fs::rename(from, to);
    }
}

#[test]
fn a_program_swapped_between_check_and_exec_is_the_one_that_runs() {
    let dir = scratch("swap");
    let program = copy_worker(&dir, "worker");
    let lock = dir.join("daedalus.lock");
    write_lock(&lock, &[("worker", digest_of(&program))]);
    // What the path names after the check: a script that would exit at once if it were run by
    // its path.
    let impostor = dir.join("impostor");
    std::fs::write(&impostor, "#!/bin/sh\nexit 3\n").unwrap();
    make_executable(&impostor);

    let mut spec = WorkerSpec::new(&program);
    spec.launch = Some(swap_before_exec);
    let spec = spec
        .env("SWAP_FROM", impostor.display().to_string())
        .env("SWAP_TO", program.display().to_string());
    locked(&lock)
        .add_worker(spec, &registry())
        .expect("the checked file runs, and it says hello");
    assert!(!impostor.exists(), "the swap happened before the exec");
    assert!(
        std::fs::read(&program).unwrap().starts_with(b"#!"),
        "the path now names the impostor"
    );
    let _ = std::fs::remove_dir_all(dir);
}

#[test]
fn a_worker_whose_file_changed_is_refused_at_respawn_until_the_lock_approves_it() {
    let dir = scratch("respawn");
    let program = copy_worker(&dir, "worker");
    let lock = dir.join("daedalus.lock");
    write_lock(&lock, &[("worker", digest_of(&program))]);

    let mut backend = locked(&lock);
    backend
        .add_worker(WorkerSpec::new(&program), &registry())
        .expect("starts");
    let mut step = support_step(&backend);
    let (end, _, value) = run_add(&mut step);
    assert!(
        matches!(end, StepEnd::Done) && value == Some(42),
        "first run works"
    );

    // A kill of the worker this backend started, by the pid it reports.
    let pid = backend.worker_pid("fixture.scalar_add").expect("a worker");
    let killed = Command::new("kill")
        .args(["-9", &pid.to_string()])
        .status()
        .expect("kill");
    assert!(killed.success());
    let (end, _, _) = run_add(&mut step);
    assert!(
        matches!(end, StepEnd::Fault),
        "the run that found the worker gone faults"
    );

    // The file changes: a worker that is no longer the approved file. The lock still has the old
    // digest, so every start is refused.
    let changed = dir.join("changed");
    std::fs::copy(worker_binary(), &changed).unwrap();
    append_bytes(&changed, &[0u8; 16]);
    replace_file(&changed, &program);

    // The retry never stops: it is tried again after each backoff, and each refusal is counted.
    // Four refusals cross three backoff steps, which the delays after each failure keep under the
    // cap. The test does not wait out the cap: the delays here are 0.1 s, 0.2 s and 0.4 s.
    let deadline = Instant::now() + Duration::from_secs(20);
    let mut refusal_seen = false;
    let mut skipped = 0;
    let mut scheduled = Vec::new();
    while backend.telemetry().refused < 4 {
        assert!(
            Instant::now() < deadline,
            "no fourth refusal: {:?}",
            backend.telemetry()
        );
        let (end, reason, _) = run_add(&mut step);
        match end {
            StepEnd::Skipped => skipped += 1,
            StepEnd::Fault if reason.contains("is not the file the lock approves") => {
                refusal_seen = true;
            }
            other => panic!("unexpected {other:?}: {reason}"),
        }
        let refused = backend.telemetry().refused as u32;
        while scheduled.len() < refused as usize {
            scheduled.push(delay_after(scheduled.len() as u32 + 1));
        }
        std::thread::sleep(Duration::from_millis(50));
    }
    assert!(
        scheduled.iter().all(|delay| *delay <= MAX_DELAY),
        "every scheduled delay is at or under the cap: {scheduled:?}"
    );
    assert!(
        scheduled.windows(2).all(|pair| pair[0] <= pair[1]),
        "the delay grows with each failure: {scheduled:?}"
    );
    assert!(scheduled.len() >= 4, "more than one backoff was crossed");
    assert!(refusal_seen, "the refusal names the file");
    assert!(
        skipped > 0,
        "runs between attempts are skipped, not blocked"
    );

    // The owner approves the new file. The next start that is due loads it.
    write_lock(&lock, &[("worker", digest_of(&program))]);
    let deadline = Instant::now() + Duration::from_secs(20);
    loop {
        assert!(Instant::now() < deadline, "the worker never recovered");
        let (end, _, value) = run_add(&mut step);
        if matches!(end, StepEnd::Done) {
            assert_eq!(value, Some(42));
            break;
        }
        std::thread::sleep(Duration::from_millis(50));
    }
    release(step);
    let _ = std::fs::remove_dir_all(dir);
}

#[test]
fn the_backoff_between_respawns_never_passes_the_cap() {
    assert_eq!(delay_after(1), BASE_DELAY);
    assert_eq!(delay_after(1000), MAX_DELAY);
    assert!(MAX_DELAY <= Duration::from_secs(8));
}

#[test]
fn an_also_checked_file_that_changed_is_refused_before_the_start() {
    let dir = scratch("also");
    let program = copy_worker(&dir, "worker");
    let helper = dir.join("helper.py");
    std::fs::write(&helper, "# helper\n").unwrap();
    let lock = dir.join("daedalus.lock");
    write_lock(
        &lock,
        &[
            ("worker", digest_of(&program)),
            ("helper.py", digest_of(&helper)),
        ],
    );

    locked(&lock)
        .add_worker(WorkerSpec::new(&program).also_check(&helper), &registry())
        .expect("the approved helper passes and the worker starts");

    std::fs::write(&helper, "# changed\n").unwrap();
    let error = locked(&lock)
        .add_worker(WorkerSpec::new(&program).also_check(&helper), &registry())
        .expect_err("a changed helper is refused");
    let text = integrity_message(&error).expect("an integrity refusal");
    assert!(text.contains("helper.py"), "{text}");
    let _ = std::fs::remove_dir_all(dir);
}

fn make_executable(path: &Path) {
    use std::os::unix::fs::PermissionsExt;
    let mut permissions = std::fs::metadata(path).unwrap().permissions();
    permissions.set_mode(0o755);
    std::fs::set_permissions(path, permissions).unwrap();
}

fn append_bytes(path: &Path, extra: &[u8]) {
    use std::io::Write;
    let mut file = std::fs::OpenOptions::new().append(true).open(path).unwrap();
    file.write_all(extra).unwrap();
}

/// The one-node step of the fixture's scalar add, compiled in `backend`.
fn support_step(backend: &ProcessBackend) -> daedalus_abi::Step {
    let inputs = [
        PortRef {
            node: NodeIndex(0),
            port: PortIndex(0),
        },
        PortRef {
            node: NodeIndex(0),
            port: PortIndex(1),
        },
    ];
    let outputs = [PortRef {
        node: NodeIndex(0),
        port: PortIndex(0),
    }];
    let nodes = [NodeIndex(0)];
    let region = Region {
        backend: BackendIndex(0),
        nodes: &nodes,
        inputs: &inputs,
        outputs: &outputs,
        incoming: &[],
        placed: &[],
        read_in: &[],
        rate: RateId(0),
    };
    let bodies = [NodeBody {
        node: NodeIndex(0),
        id: "fixture.scalar_add",
        body: BodyIndex(0),
        inputs: 2,
        outputs: 1,
        merges: false,
        fire_all: false,
        label: None,
        metadata: &NO_METADATA,
        code: &(),
        text: "fixture.scalar_add",
    }];
    let cx = CompileCx {
        types: &[],
        consts: &[],
        spaces: &[],
        bodies: &bodies,
        edges: &[],
        host_inputs: &[],
        host_outputs: &[],
        fifo_capacity: 64,
        merge_allowed: false,
        freeze_params: false,
        sharing: &[],
        lent: &[],
        offered: &[],
    };
    match backend.compile(&region, &cx) {
        Ok(Compiled::Steps(mut units)) => units.pop().expect("a unit").step,
        other => panic!("compile: {other:?}"),
    }
}

/// Runs the scalar add once with 2 + 40: how it ended, its fault text, and the sum.
fn run_add(step: &mut daedalus_abi::Step) -> (StepEnd, String, Option<i64>) {
    let mut cx = StepCx::new();
    let mut a = 2i64.to_le_bytes();
    let mut b = 40i64.to_le_bytes();
    let mut cells = [
        Slot {
            state: SlotState::Full as u32,
            owner: 0,
            data: a.as_mut_ptr(),
            len: 8,
            cap: 8,
            ..Slot::EMPTY
        },
        Slot {
            state: SlotState::Full as u32,
            owner: 0,
            data: b.as_mut_ptr(),
            len: 8,
            cap: 8,
            ..Slot::EMPTY
        },
        Slot::EMPTY,
    ];
    let end = run_once(step, &mut Slots::of(&mut cells), &mut cx);
    let value = cells[2]
        .bytes()
        .and_then(|bytes| <[u8; 8]>::try_from(bytes).ok())
        .map(i64::from_le_bytes);
    (end, cx.reason().to_owned(), value)
}
