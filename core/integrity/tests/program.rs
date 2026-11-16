//! Finding a worker program by name.

use std::ffi::OsStr;
use std::path::PathBuf;

use daedalus_integrity::find_program;

mod support;

#[test]
fn a_bare_name_is_searched_for_on_the_path_in_order() {
    let first = support::scratch("program-first");
    let second = support::scratch("program-second");
    std::fs::write(second.join("tool"), b"second").unwrap();
    std::fs::write(first.join("tool"), b"first").unwrap();
    let path = std::env::join_paths([&first, &second]).unwrap();
    assert_eq!(
        find_program(OsStr::new("tool"), Some(&path)),
        Some(first.join("tool"))
    );
    let missing = std::env::join_paths([&second]).unwrap();
    assert_eq!(
        find_program(OsStr::new("absent"), Some(&missing)),
        None,
        "a bare name not on the path is not found"
    );
    assert_eq!(find_program(OsStr::new("tool"), None), None);
    let _ = std::fs::remove_dir_all(first);
    let _ = std::fs::remove_dir_all(second);
}

#[test]
fn a_name_with_a_separator_is_used_as_given() {
    let given = PathBuf::from("./relative/python3");
    assert_eq!(
        find_program(OsStr::new("./relative/python3"), None),
        Some(given)
    );
    assert_eq!(
        find_program(OsStr::new("/usr/bin/python3"), None),
        Some(PathBuf::from("/usr/bin/python3"))
    );
}
