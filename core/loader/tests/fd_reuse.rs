//! A reused descriptor number is not a reused name: the loader keeps a file's descriptor for its
//! generation's life, and a name carries a serial. Alone in its own test binary, so no other test
//! opens or closes a descriptor while this one checks which number a file got. Linux only.

#![cfg(target_os = "linux")]

mod support;

use std::path::{Path, PathBuf};

use daedalus_integrity::{Digest, Integrity};
use daedalus_loader::{LoadError, Loader};
use support::{fixture_library, old_library};

fn scratch(name: &str) -> PathBuf {
    let dir = Path::new(env!("CARGO_TARGET_TMPDIR")).join(format!("integrity-{name}"));
    let _ = std::fs::remove_dir_all(&dir);
    std::fs::create_dir_all(&dir).expect("scratch");
    dir
}

fn replace_file(source: &Path, dest: &Path) {
    let tmp = dest.with_extension("tmp");
    std::fs::copy(source, &tmp).expect("copy");
    std::fs::rename(&tmp, dest).expect("rename over");
}

fn digest_of(path: &Path) -> Digest {
    let file = std::fs::File::open(path).expect("open");
    daedalus_integrity::hash_file(&file).expect("hash").0
}

fn write_lock(path: &Path, entries: &[(&str, Digest)]) {
    let mut lock = daedalus_integrity::Lock::new();
    for (key, digest) in entries {
        lock.set(key.to_string(), *digest);
    }
    std::fs::write(path, lock.render()).expect("write lock");
}

/// The descriptor numbers of this process that point at `path`.
fn fds_of(path: &Path) -> Vec<i32> {
    let mut found: Vec<i32> = std::fs::read_dir("/proc/self/fd")
        .unwrap()
        .flatten()
        .filter(|entry| std::fs::read_link(entry.path()).is_ok_and(|target| target == path))
        .filter_map(|entry| entry.file_name().to_str()?.parse().ok())
        .collect();
    found.sort_unstable();
    found
}

#[test]
fn a_reused_fd_number_loads_the_new_file_not_the_old_library() {
    let dir = scratch("fd-reuse");
    let first = dir.join("first.so");
    replace_file(&fixture_library(), &first);
    let lock = dir.join("daedalus.lock");
    write_lock(&lock, &[("first.so", digest_of(&first))]);

    // A second generation of the same file keeps the first library loaded after the first
    // generation's descriptor is closed: the loader matches the file it opens against it.
    let mut loader = Loader::with_integrity(Integrity::locked(&lock).unwrap());
    let kept = loader.open(&first).expect("first loads");
    let again = loader.open(&first).expect("the same file loads again");
    let held = fds_of(&first);
    assert_eq!(held.len(), 2, "two generations hold the file: {held:?}");
    drop(kept);
    let freed: Vec<i32> = held
        .iter()
        .copied()
        .filter(|fd| !fds_of(&first).contains(fd))
        .collect();
    assert_eq!(
        freed.len(),
        1,
        "one descriptor was closed with the first generation"
    );

    // A different library, which has no shapes, is checked and then loaded from the descriptor
    // number that was just freed.
    let old = dir.join("old.so");
    replace_file(&old_library(), &old);
    write_lock(
        &lock,
        &[("first.so", digest_of(&first)), ("old.so", digest_of(&old))],
    );
    let checked = integrity_open(&lock, &old);
    assert_eq!(
        fds_of(&old),
        vec![freed[0]],
        "the new file has the freed number"
    );
    let error = Loader::new()
        .open_checked(checked)
        .expect_err("the old library is refused, not the one loaded under that number");
    assert!(matches!(error, LoadError::Older { .. }), "{error}");
    drop(again);
}

fn integrity_open(lock: &Path, path: &Path) -> daedalus_integrity::Checked {
    Integrity::locked(lock)
        .unwrap()
        .open(path)
        .expect("checked")
}
