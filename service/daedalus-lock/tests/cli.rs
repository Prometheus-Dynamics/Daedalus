//! The `daedalus-lock` commands, and each exit code they give. Writing a lock needs Unix.

#![cfg(unix)]

use std::path::{Path, PathBuf};
use std::process::{Command, Output};

fn lock_tool() -> Command {
    Command::new(env!("CARGO_BIN_EXE_daedalus-lock"))
}

/// A new directory for one test.
fn scratch(name: &str) -> PathBuf {
    let dir = Path::new(env!("CARGO_TARGET_TMPDIR")).join(format!("daedalus-lock-{name}"));
    let _ = std::fs::remove_dir_all(&dir);
    std::fs::create_dir_all(&dir).expect("scratch");
    dir
}

fn text(output: &Output) -> (String, String) {
    (
        String::from_utf8_lossy(&output.stdout).into_owned(),
        String::from_utf8_lossy(&output.stderr).into_owned(),
    )
}

fn code(output: &Output) -> i32 {
    output.status.code().expect("exited")
}

#[test]
fn record_creates_the_lock_with_file_name_keys_and_check_passes() {
    let dir = scratch("record");
    let file = dir.join("libfoo.so");
    std::fs::write(&file, b"approved bytes").unwrap();
    let lock = dir.join("daedalus.lock");

    let out = lock_tool()
        .arg("--lock")
        .arg(&lock)
        .args(["record"])
        .arg(&file)
        .output()
        .unwrap();
    assert_eq!(code(&out), 0, "{:?}", text(&out));
    let written = std::fs::read_to_string(&lock).unwrap();
    let digest = daedalus_integrity::Digest::of(b"approved bytes").to_string();
    assert_eq!(written, format!("# daedalus.lock 1\n{digest}  libfoo.so\n"));

    let check = lock_tool()
        .arg("--lock")
        .arg(&lock)
        .args(["check"])
        .arg(&file)
        .output()
        .unwrap();
    assert_eq!(code(&check), 0, "{:?}", text(&check));
    assert!(text(&check).0.contains(": OK"));
    let _ = std::fs::remove_dir_all(dir);
}

#[test]
fn record_absolute_writes_the_absolute_path_as_the_key() {
    let dir = scratch("absolute");
    let file = dir.join("tool");
    std::fs::write(&file, b"tool").unwrap();
    let lock = dir.join("daedalus.lock");
    let out = lock_tool()
        .arg("--lock")
        .arg(&lock)
        .args(["record", "--absolute"])
        .arg(&file)
        .output()
        .unwrap();
    assert_eq!(code(&out), 0, "{:?}", text(&out));
    let written = std::fs::read_to_string(&lock).unwrap();
    let key = written.lines().nth(1).unwrap().split("  ").nth(1).unwrap();
    assert!(Path::new(key).is_absolute(), "{key}");
    let _ = std::fs::remove_dir_all(dir);
}

#[test]
fn record_warns_about_a_hashbang_file_and_still_records_it() {
    let dir = scratch("hashbang");
    let script = dir.join("run");
    std::fs::write(&script, "#!/bin/sh\nexit 0\n").unwrap();
    let lock = dir.join("daedalus.lock");
    let out = lock_tool()
        .arg("--lock")
        .arg(&lock)
        .args(["record"])
        .arg(&script)
        .output()
        .unwrap();
    assert_eq!(code(&out), 0);
    let (_, stderr) = text(&out);
    assert!(stderr.contains("starts with `#!`"), "{stderr}");
    let _ = std::fs::remove_dir_all(dir);
}

#[test]
fn record_refuses_to_change_an_approved_entry_and_leaves_the_lock_alone() {
    let dir = scratch("conflict");
    let file = dir.join("lib.so");
    std::fs::write(&file, b"first").unwrap();
    let lock = dir.join("daedalus.lock");
    assert_eq!(
        code(
            &lock_tool()
                .arg("--lock")
                .arg(&lock)
                .args(["record"])
                .arg(&file)
                .output()
                .unwrap()
        ),
        0
    );
    let before = std::fs::read_to_string(&lock).unwrap();
    std::fs::write(&file, b"second").unwrap();
    let out = lock_tool()
        .arg("--lock")
        .arg(&lock)
        .args(["record"])
        .arg(&file)
        .output()
        .unwrap();
    assert_eq!(code(&out), 1, "{:?}", text(&out));
    assert!(text(&out).1.contains("daedalus-lock update"));
    assert_eq!(std::fs::read_to_string(&lock).unwrap(), before);
    let _ = std::fs::remove_dir_all(dir);
}

#[test]
fn exit_2_for_usage_and_exit_3_for_an_unreadable_file_or_lock() {
    let dir = scratch("codes");
    let lock = dir.join("daedalus.lock");
    let no_paths = lock_tool()
        .arg("--lock")
        .arg(&lock)
        .arg("record")
        .output()
        .unwrap();
    assert_eq!(code(&no_paths), 2, "record with no PATH is a usage error");
    let unknown = lock_tool()
        .arg("--lock")
        .arg(&lock)
        .arg("frobnicate")
        .output()
        .unwrap();
    assert_eq!(code(&unknown), 2);
    assert!(text(&unknown).1.contains("is not a command"));
    let no_command = lock_tool().output().unwrap();
    assert_eq!(code(&no_command), 2);
    let bad_option = lock_tool().arg("--nope").arg("check").output().unwrap();
    assert_eq!(code(&bad_option), 2);
    let root_on_record = lock_tool()
        .arg("--lock")
        .arg(&lock)
        .args(["record", "--root", "."])
        .output()
        .unwrap();
    assert_eq!(code(&root_on_record), 2);

    let missing_file = lock_tool()
        .arg("--lock")
        .arg(&lock)
        .args(["record"])
        .arg(dir.join("absent.so"))
        .output()
        .unwrap();
    assert_eq!(code(&missing_file), 3, "{:?}", text(&missing_file));

    let absent_lock = lock_tool()
        .arg("--lock")
        .arg(&lock)
        .arg("check")
        .output()
        .unwrap();
    assert_eq!(
        code(&absent_lock),
        3,
        "a check needs a lock: {:?}",
        text(&absent_lock)
    );

    std::fs::write(&lock, "# daedalus.lock 1\nnot an entry\n").unwrap();
    let malformed = lock_tool()
        .arg("--lock")
        .arg(&lock)
        .arg("check")
        .output()
        .unwrap();
    assert_eq!(code(&malformed), 3);
    assert!(
        text(&malformed).1.contains("line 2"),
        "{:?}",
        text(&malformed)
    );

    let help = lock_tool().arg("--help").output().unwrap();
    assert_eq!(code(&help), 0);
    assert!(text(&help).0.starts_with("usage: daedalus-lock"));
    let _ = std::fs::remove_dir_all(dir);
}

#[test]
fn check_fails_with_exit_1_for_a_changed_file_and_for_one_with_no_entry() {
    let dir = scratch("check");
    let file = dir.join("lib.so");
    std::fs::write(&file, b"approved").unwrap();
    let other = dir.join("other.so");
    std::fs::write(&other, b"unlisted").unwrap();
    let lock = dir.join("daedalus.lock");
    lock_tool()
        .arg("--lock")
        .arg(&lock)
        .args(["record"])
        .arg(&file)
        .output()
        .unwrap();

    std::fs::write(&file, b"changed").unwrap();
    let changed = lock_tool()
        .arg("--lock")
        .arg(&lock)
        .args(["check"])
        .arg(&file)
        .output()
        .unwrap();
    assert_eq!(code(&changed), 1);
    let shown = text(&changed).0;
    assert!(shown.contains("FAILED"), "{shown}");
    assert!(
        shown.contains(&daedalus_integrity::Digest::of(b"approved").to_string()),
        "{shown}"
    );

    let unlisted = lock_tool()
        .arg("--lock")
        .arg(&lock)
        .args(["check"])
        .arg(&other)
        .output()
        .unwrap();
    assert_eq!(code(&unlisted), 1);
    assert!(text(&unlisted).0.contains("no entry in the lock"));
    let _ = std::fs::remove_dir_all(dir);
}

#[test]
fn check_with_no_paths_checks_each_entry_under_the_locks_directory() {
    let dir = scratch("check-all");
    let file = dir.join("lib.so");
    std::fs::write(&file, b"approved").unwrap();
    let lock = dir.join("daedalus.lock");
    lock_tool()
        .arg("--lock")
        .arg(&lock)
        .args(["record"])
        .arg(&file)
        .output()
        .unwrap();

    let ok = lock_tool()
        .arg("--lock")
        .arg(&lock)
        .arg("check")
        .output()
        .unwrap();
    assert_eq!(code(&ok), 0, "{:?}", text(&ok));
    assert!(text(&ok).0.contains("lib.so: OK"));

    std::fs::remove_file(&file).unwrap();
    let missing = lock_tool()
        .arg("--lock")
        .arg(&lock)
        .arg("check")
        .output()
        .unwrap();
    assert_eq!(code(&missing), 1);
    assert!(text(&missing).0.contains("missing"));

    let _ = std::fs::remove_dir_all(dir);
}

#[test]
fn update_approves_the_changed_file_and_shows_old_and_new() {
    let dir = scratch("update");
    let file = dir.join("lib.so");
    std::fs::write(&file, b"old build").unwrap();
    let lock = dir.join("daedalus.lock");
    lock_tool()
        .arg("--lock")
        .arg(&lock)
        .args(["record"])
        .arg(&file)
        .output()
        .unwrap();
    std::fs::write(&file, b"new build").unwrap();

    let out = lock_tool()
        .arg("--lock")
        .arg(&lock)
        .arg("update")
        .output()
        .unwrap();
    assert_eq!(code(&out), 0, "{:?}", text(&out));
    let shown = text(&out).0;
    assert!(shown.contains("->"), "{shown}");
    assert!(shown.contains(&daedalus_integrity::Digest::of(b"new build").to_string()));

    let check = lock_tool()
        .arg("--lock")
        .arg(&lock)
        .arg("check")
        .output()
        .unwrap();
    assert_eq!(code(&check), 0, "{:?}", text(&check));
    let _ = std::fs::remove_dir_all(dir);
}

#[test]
fn update_of_a_file_with_no_entry_is_exit_1() {
    let dir = scratch("update-none");
    let file = dir.join("lib.so");
    std::fs::write(&file, b"x").unwrap();
    let lock = dir.join("daedalus.lock");
    std::fs::write(&lock, "# daedalus.lock 1\n").unwrap();
    let out = lock_tool()
        .arg("--lock")
        .arg(&lock)
        .args(["update"])
        .arg(&file)
        .output()
        .unwrap();
    assert_eq!(code(&out), 1, "{:?}", text(&out));
    assert!(text(&out).1.contains("record it first"));
    let _ = std::fs::remove_dir_all(dir);
}

#[test]
fn sha256sum_reads_the_lock_it_writes() {
    let dir = scratch("sha256sum");
    let file = dir.join("lib.so");
    std::fs::write(&file, b"for sha256sum").unwrap();
    let lock = dir.join("daedalus.lock");
    lock_tool()
        .arg("--lock")
        .arg(&lock)
        .args(["record"])
        .arg(&file)
        .output()
        .unwrap();
    let Ok(status) = Command::new("sha256sum")
        .current_dir(&dir)
        .args(["-c", "daedalus.lock"])
        .status()
    else {
        return; // no sha256sum here
    };
    assert!(status.success(), "sha256sum -c reads the lock");
    let _ = std::fs::remove_dir_all(dir);
}
