//! The names a library is loaded by: unique per serial, and each one is the file it names.

#![cfg(target_os = "linux")]

mod support;

use std::collections::HashSet;
use std::os::unix::fs::MetadataExt;
use std::os::unix::io::AsRawFd;

use daedalus_integrity::dlopen_name;

#[test]
fn names_are_unique_per_serial_and_all_resolve_to_the_fd() {
    let dir = support::scratch("dlopen-name");
    let path = dir.join("lib.so");
    std::fs::write(&path, b"not really a library").unwrap();
    let file = std::fs::File::open(&path).unwrap();
    let fd = file.as_raw_fd();
    let ino = file.metadata().unwrap().ino();

    // The loader compares names as strings, so the test does too: `PathBuf` equality compares
    // components, which would call `a//3` and `a/3` the same.
    let mut seen = HashSet::new();
    for serial in 0..2000u64 {
        let name = dlopen_name(serial, fd);
        let text = name.to_str().expect("ASCII").to_string();
        assert!(
            seen.insert(text.clone()),
            "serial {serial} repeats a name: {text:?}"
        );
        let resolved = std::fs::metadata(&name).expect("the name resolves");
        assert_eq!(resolved.ino(), ino, "serial {serial} names the file");
    }
    let _ = std::fs::remove_dir_all(dir);
}

#[test]
fn a_different_fd_gives_a_different_name() {
    assert_ne!(dlopen_name(7, 4), dlopen_name(7, 5));
}
