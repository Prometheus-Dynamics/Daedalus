//! Plugin integrity in the loader: a lock approves a library, and what loads is what was checked.
//! Linux only: the checks read `/proc/self/fd`. The Off mode is in `off.rs`, on every platform.

#![cfg(target_os = "linux")]

mod support;

use std::path::{Path, PathBuf};
use std::sync::atomic::AtomicBool;

use daedalus_integrity::{Digest, Integrity, Lock, read_lock};
use daedalus_loader::{LoadError, Loader, Retry, load_with_retry};
use support::{baseline_library, fixture_library, old_library};

fn scratch(name: &str) -> PathBuf {
    let dir = Path::new(env!("CARGO_TARGET_TMPDIR")).join(format!("integrity-{name}"));
    let _ = std::fs::remove_dir_all(&dir);
    std::fs::create_dir_all(&dir).expect("scratch");
    dir
}

/// Writes a lock that approves exactly `entries`, replacing any lock there was.
fn write_lock(path: &Path, entries: &[(&str, Digest)]) {
    let mut lock = Lock::new();
    for (key, digest) in entries {
        lock.set(key.to_string(), *digest);
    }
    std::fs::write(path, lock.render()).expect("write lock");
}

fn digest_of(path: &Path) -> Digest {
    let file = std::fs::File::open(path).expect("open");
    daedalus_integrity::hash_file(&file).expect("hash").0
}

/// Puts a copy of `source` at `dest` by writing a new file and renaming it over `dest`, so a
/// library already mapped from `dest` keeps its own bytes.
fn replace_file(source: &Path, dest: &Path) {
    let tmp = dest.with_extension("tmp");
    std::fs::copy(source, &tmp).expect("copy");
    std::fs::rename(&tmp, dest).expect("rename over");
}

fn locked(lock: &Path) -> Loader {
    Loader::with_integrity(Integrity::locked(lock).expect("locked mode"))
}

#[test]
fn a_library_in_the_lock_loads() {
    let dir = scratch("in-lock");
    let lib = dir.join("fixture.so");
    replace_file(&fixture_library(), &lib);
    let lock = dir.join("daedalus.lock");
    write_lock(&lock, &[("fixture.so", digest_of(&lib))]);

    let plugin = locked(&lock).open(&lib).expect("approved library loads");
    assert_eq!(plugin.name(), "dylib-fixture");
    assert_eq!(plugin.generation().digest(), Some(&digest_of(&lib)));
}

#[test]
fn one_flipped_byte_is_refused_with_both_digests() {
    let dir = scratch("flipped");
    let lib = dir.join("fixture.so");
    replace_file(&fixture_library(), &lib);
    let lock = dir.join("daedalus.lock");
    let approved = digest_of(&lib);
    write_lock(&lock, &[("fixture.so", approved)]);

    let mut bytes = std::fs::read(&lib).unwrap();
    let middle = bytes.len() / 2;
    bytes[middle] ^= 0x01;
    std::fs::write(&lib, &bytes).unwrap();
    let found = digest_of(&lib);
    assert_ne!(approved, found);

    let error = locked(&lock)
        .open(&lib)
        .expect_err("a changed library is refused");
    let text = error.to_string();
    assert!(matches!(error, LoadError::Integrity(_)), "{text}");
    assert!(
        text.contains(&approved.to_string()),
        "names the approved digest: {text}"
    );
    assert!(
        text.contains(&found.to_string()),
        "names the found digest: {text}"
    );
}

#[test]
fn a_library_not_in_the_lock_is_refused() {
    let dir = scratch("unlisted");
    let lib = dir.join("fixture.so");
    replace_file(&fixture_library(), &lib);
    let lock = dir.join("daedalus.lock");
    write_lock(&lock, &[("other.so", Digest::of(b"other"))]);

    let error = locked(&lock)
        .open(&lib)
        .expect_err("an unlisted library is refused");
    let text = error.to_string();
    assert!(text.contains("daedalus-lock record"), "{text}");
    assert!(text.contains(&digest_of(&lib).to_string()), "{text}");
}

#[test]
fn record_writes_the_entry_and_locked_then_accepts_it() {
    let dir = scratch("record");
    let lib = dir.join("fixture.so");
    replace_file(&fixture_library(), &lib);
    let lock = dir.join("daedalus.lock");

    let mut recording = Loader::with_integrity(Integrity::record(&lock).expect("record mode"));
    let plugin = recording.open(&lib).expect("record loads the library");
    assert_eq!(plugin.name(), "dylib-fixture");
    let recorded = read_lock(&lock).expect("the lock was written");
    assert_eq!(recorded.get("fixture.so"), Some(&digest_of(&lib)));

    let plugin = locked(&lock)
        .open(&lib)
        .expect("the recorded library now loads under Locked");
    assert_eq!(plugin.name(), "dylib-fixture");
}

#[test]
fn record_refuses_to_change_an_approved_entry_and_loads_nothing_new() {
    let dir = scratch("record-conflict");
    let lib = dir.join("fixture.so");
    replace_file(&fixture_library(), &lib);
    let lock = dir.join("daedalus.lock");
    write_lock(&lock, &[("fixture.so", Digest::of(b"an older build"))]);
    let before = std::fs::read_to_string(&lock).unwrap();

    let mut recording = Loader::with_integrity(Integrity::record(&lock).unwrap());
    let error = recording
        .open(&lib)
        .expect_err("a changed approved entry is a conflict");
    assert!(
        error.to_string().contains("daedalus-lock update"),
        "{error}"
    );
    assert_eq!(std::fs::read_to_string(&lock).unwrap(), before);
}

#[test]
fn the_fd_is_what_loads_not_the_path() {
    let dir = scratch("swap");
    let lib = dir.join("swap.so");
    replace_file(&fixture_library(), &lib);
    let lock = dir.join("daedalus.lock");
    write_lock(&lock, &[("swap.so", digest_of(&lib))]);

    let integrity = Integrity::locked(&lock).unwrap();
    let checked = integrity.open(&lib).expect("checked");
    // Between the check and the load, the path now names another library: the old build.
    replace_file(&old_library(), &lib);

    let plugin = Loader::new()
        .open_checked(checked)
        .expect("the checked file loads, not the path");
    assert_eq!(plugin.name(), "dylib-fixture", "the fixture's nodes load");

    let by_path = Loader::new()
        .open(&lib)
        .expect_err("the path now names the old build");
    assert!(matches!(by_path, LoadError::Older { .. }), "{by_path}");
}

#[test]
fn a_loaded_generation_keeps_its_file_open_until_it_is_dropped() {
    let dir = scratch("fd-held");
    let lib = dir.join("held.so");
    replace_file(&fixture_library(), &lib);
    let lock = dir.join("daedalus.lock");
    write_lock(&lock, &[("held.so", digest_of(&lib))]);
    let ino = std::os::unix::fs::MetadataExt::ino(&std::fs::metadata(&lib).unwrap());

    let holds = |ino: u64| {
        std::fs::read_dir("/proc/self/fd")
            .unwrap()
            .flatten()
            .any(|entry| {
                std::fs::metadata(entry.path()).is_ok_and(|meta| {
                    std::os::unix::fs::MetadataExt::ino(&meta) == ino
                        && std::os::unix::fs::MetadataExt::dev(&meta)
                            == std::os::unix::fs::MetadataExt::dev(
                                &std::fs::metadata(&lib).unwrap(),
                            )
                })
            })
    };
    let plugin = locked(&lock).open(&lib).expect("loads");
    assert!(holds(ino), "the file stays open while the generation lives");
    drop(plugin);
    assert!(!holds(ino), "the file is closed with the generation");
}

#[test]
fn a_second_load_while_the_first_lives_gets_its_own_file_and_name() {
    let dir = scratch("second");
    let first = dir.join("first.so");
    let second = dir.join("second.so");
    replace_file(&fixture_library(), &first);
    replace_file(&baseline_library(), &second);
    let lock = dir.join("daedalus.lock");
    write_lock(
        &lock,
        &[
            ("first.so", digest_of(&first)),
            ("second.so", digest_of(&second)),
        ],
    );

    let mut loader = locked(&lock);
    let one = loader.open(&first).expect("first loads");
    let two = loader
        .open(&second)
        .expect("second loads while the first lives");
    assert_eq!(one.generation().number(), 1);
    assert_eq!(two.generation().number(), 2);
    assert!(!one.nodes().is_empty() && !two.nodes().is_empty());
}

#[test]
fn a_refused_reload_is_retried_and_loads_once_the_lock_approves_it() {
    let dir = scratch("retry");
    let lib = dir.join("serving.so");
    replace_file(&fixture_library(), &lib);
    let lock = dir.join("daedalus.lock");
    write_lock(&lock, &[("serving.so", digest_of(&lib))]);

    let mut loader = locked(&lock);
    let current = loader.open(&lib).expect("the first load is approved");

    // The reload is a different build, which the lock does not approve yet.
    replace_file(&baseline_library(), &lib);
    let stop = AtomicBool::new(false);
    let mut retry = Retry::new();
    let mut attempts = 0;
    let reloaded = load_with_retry(&mut loader, &lib, &stop, &mut retry, |delay| {
        assert!(delay <= std::time::Duration::from_secs(8));
        attempts += 1;
        if attempts == 2 {
            // The owner approves the new build; the next try loads it.
            write_lock(&lock, &[("serving.so", digest_of(&lib))]);
        }
    })
    .expect("the reload loads once the lock approves it");
    assert!(attempts >= 2, "the refusal was retried: {attempts}");
    assert_eq!(reloaded.generation().number(), 2);
    assert_eq!(current.generation().number(), 1);
    assert!(
        current.backend("fixture.cpu").is_some(),
        "the old generation still serves"
    );
}
