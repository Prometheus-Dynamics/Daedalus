//! Writing a lock: no entry is lost when several writers share it, and a conflict writes nothing.
//! Unix only: the write takes a lock on the directory.

#![cfg(unix)]

use crate::support;

use std::path::Path;
use std::process::Command;
use std::sync::Arc;
use std::thread;

use daedalus_integrity::{Digest, merge, read_lock, update};

fn entries(prefix: &str, count: usize) -> Vec<(String, Digest)> {
    (0..count)
        .map(|n| {
            (
                format!("{prefix}{n:03}.so"),
                Digest::of(format!("{prefix}{n}").as_bytes()),
            )
        })
        .collect()
}

fn keys_in(path: &Path) -> Vec<String> {
    read_lock(path)
        .unwrap()
        .entries()
        .map(|(key, _)| key.to_string())
        .collect()
}

#[test]
fn merge_from_eight_threads_loses_no_entry() {
    let dir = support::scratch("merge-threads");
    let lock_path = Arc::new(dir.join("daedalus.lock"));
    let handles: Vec<_> = (0..8)
        .map(|thread_number| {
            let lock_path = lock_path.clone();
            thread::spawn(move || {
                for entry in entries(&format!("t{thread_number}-"), 10) {
                    merge(&lock_path, &[entry]).expect("merge");
                }
            })
        })
        .collect();
    for handle in handles {
        handle.join().unwrap();
    }
    let lock = read_lock(&lock_path).unwrap();
    assert_eq!(lock.entries().count(), 80, "every entry from every thread");
    let _ = std::fs::remove_dir_all(dir);
}

#[test]
fn a_conflict_writes_nothing() {
    let dir = support::scratch("merge-conflict");
    let lock_path = dir.join("daedalus.lock");
    merge(&lock_path, &[("a.so".to_string(), Digest::of(b"one"))]).unwrap();
    let before = std::fs::read_to_string(&lock_path).unwrap();
    let result = merge(
        &lock_path,
        &[
            ("b.so".to_string(), Digest::of(b"new")),
            ("a.so".to_string(), Digest::of(b"two")),
        ],
    );
    assert!(result.is_err(), "a different digest for a.so is a conflict");
    assert_eq!(std::fs::read_to_string(&lock_path).unwrap(), before);
    assert!(!dir.join(".daedalus.lock.tmp").exists());
    let _ = std::fs::remove_dir_all(dir);
}

#[test]
fn merge_keeps_an_entry_that_is_already_recorded() {
    let dir = support::scratch("merge-same");
    let lock_path = dir.join("daedalus.lock");
    let digest = Digest::of(b"same");
    merge(&lock_path, &[("a.so".to_string(), digest)]).unwrap();
    merge(&lock_path, &[("a.so".to_string(), digest)]).unwrap();
    assert_eq!(keys_in(&lock_path), vec!["a.so".to_string()]);
    let _ = std::fs::remove_dir_all(dir);
}

#[test]
fn a_malformed_lock_is_not_overwritten() {
    let dir = support::scratch("merge-malformed");
    let lock_path = dir.join("daedalus.lock");
    std::fs::write(&lock_path, "not a lock\n").unwrap();
    assert!(merge(&lock_path, &[("a.so".to_string(), Digest::of(b"x"))]).is_err());
    assert_eq!(std::fs::read_to_string(&lock_path).unwrap(), "not a lock\n");
    let _ = std::fs::remove_dir_all(dir);
}

/// The worker half of the two-process test. It does nothing unless the test asks for it.
#[test]
fn merge_child() {
    let Ok(lock) = std::env::var("DAEDALUS_INTEGRITY_MERGE_LOCK") else {
        return;
    };
    let prefix = std::env::var("DAEDALUS_INTEGRITY_MERGE_PREFIX").unwrap();
    for entry in entries(&prefix, 25) {
        merge(Path::new(&lock), &[entry]).expect("merge in the child");
    }
}

#[test]
fn merge_from_two_processes_loses_no_entry() {
    let dir = support::scratch("merge-procs");
    let lock_path = dir.join("daedalus.lock");
    let exe = std::env::current_exe().unwrap();
    let children: Vec<_> = ["p", "q"]
        .iter()
        .map(|prefix| {
            Command::new(&exe)
                .args([
                    &child_test_name(),
                    "--exact",
                    "--test-threads=1",
                    "--nocapture",
                ])
                .env("DAEDALUS_INTEGRITY_MERGE_LOCK", &lock_path)
                .env("DAEDALUS_INTEGRITY_MERGE_PREFIX", prefix)
                .spawn()
                .expect("start the child")
        })
        .collect();
    let mut children = children;
    for child in &mut children {
        assert!(child.wait().unwrap().success(), "the child merged");
    }
    let lock = read_lock(&lock_path).unwrap();
    assert_eq!(lock.entries().count(), 50, "both processes' entries");
    let _ = std::fs::remove_dir_all(dir);
}

#[test]
fn update_replaces_an_approved_entry_and_keeps_the_rest() {
    let dir = support::scratch("merge-update");
    let lock_path = dir.join("daedalus.lock");
    merge(
        &lock_path,
        &[
            ("a.so".to_string(), Digest::of(b"one")),
            ("b.so".to_string(), Digest::of(b"kept")),
        ],
    )
    .unwrap();
    update(&lock_path, &[("a.so".to_string(), Digest::of(b"two"))]).unwrap();
    let lock = read_lock(&lock_path).unwrap();
    assert_eq!(lock.get("a.so"), Some(&Digest::of(b"two")));
    assert_eq!(lock.get("b.so"), Some(&Digest::of(b"kept")));
    let _ = std::fs::remove_dir_all(dir);
}

/// The child test's full name in this binary: `merge_child` alone, or under its module when this file
/// is one module of a larger test binary.
fn child_test_name() -> String {
    match module_path!().split_once("::") {
        Some((_, module)) => format!("{module}::merge_child"),
        None => "merge_child".to_string(),
    }
}
