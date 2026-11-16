//! The lock's format: a round trip, each strictness rule with its line, and key lookup.

use std::path::Path;

use daedalus_integrity::{Digest, Lock};

fn hex(text: &str) -> String {
    Digest::of(text.as_bytes()).to_string()
}

fn parse_err(text: &str) -> (usize, String) {
    let error = Lock::parse(text).expect_err("the lock should be refused");
    (error.line, error.why)
}

#[test]
fn a_lock_round_trips_through_its_text() {
    let text = format!(
        "# daedalus.lock 1\n{}  /opt/daedalus/bin/python3\n{}  libfoo.so\n",
        hex("b"),
        hex("a")
    );
    let lock = Lock::parse(&text).expect("valid lock");
    assert_eq!(lock.render(), text);
    assert_eq!(Lock::parse(&lock.render()).unwrap(), lock);
}

#[test]
fn a_header_alone_is_an_empty_lock() {
    let lock = Lock::parse("# daedalus.lock 1\n").unwrap();
    assert_eq!(lock.entries().count(), 0);
    assert_eq!(lock.render(), "# daedalus.lock 1\n");
}

#[test]
fn each_strictness_rule_is_refused_with_its_line() {
    let good_hex = hex("a");
    let good = format!("# daedalus.lock 1\n{good_hex}  a\n");
    assert!(Lock::parse(&good).is_ok());

    assert_eq!(parse_err("").0, 1, "an empty file has no header");
    assert_eq!(parse_err("").1, "the header is missing");
    assert_eq!(
        parse_err("# daedalus.lock 2\n").0,
        1,
        "another header version"
    );
    assert_eq!(parse_err("# something\n").0, 1);
    let (line, _) = parse_err(&format!("# daedalus.lock 1\n{good_hex}  a"));
    assert_eq!(line, 2, "a missing final LF");
    let (line, _) = parse_err(&format!("# daedalus.lock 1\r\n{good_hex}  a\n"));
    assert_eq!(line, 1, "CRLF");
    let (line, why) = parse_err(&format!("# daedalus.lock 1\n{good_hex}\ta\n"));
    assert_eq!(line, 2, "a tab in place of two spaces: {why}");
    let (line, _) = parse_err(&format!("# daedalus.lock 1\n{good_hex} a\n"));
    assert_eq!(line, 2, "one space");
    let (line, _) = parse_err(&format!(
        "# daedalus.lock 1\n{}  a\n",
        good_hex.to_uppercase()
    ));
    assert_eq!(line, 2, "uppercase hex");
    let (line, _) = parse_err(&format!("# daedalus.lock 1\n{}  a\n", &good_hex[..63]));
    assert_eq!(line, 2, "a short digest");
    let (line, _) = parse_err(&format!("# daedalus.lock 1\n# note\n{good_hex}  a\n"));
    assert_eq!(line, 2, "a comment after the header");
    let (line, _) = parse_err(&format!("# daedalus.lock 1\n{good_hex}  a\n\n"));
    assert_eq!(line, 3, "an empty line");
    let (line, _) = parse_err(&format!(
        "# daedalus.lock 1\n{good_hex}  a\n{good_hex}  a\n"
    ));
    assert_eq!(line, 3, "a repeated key");
    let (line, _) = parse_err(&format!(
        "# daedalus.lock 1\n{good_hex}  b\n{good_hex}  a\n"
    ));
    assert_eq!(line, 3, "keys out of order");
    let (line, _) = parse_err(&format!("# daedalus.lock 1\n{good_hex}  a\\b\n"));
    assert_eq!(line, 2, "a backslash in a key");
    let (line, _) = parse_err(&format!("# daedalus.lock 1\n{good_hex}  \n"));
    assert_eq!(line, 2, "an empty key");
}

#[test]
fn a_file_name_key_matches_any_directory_and_an_absolute_key_only_its_own() {
    let digest = Digest::of(b"x");
    let mut lock = Lock::new();
    lock.set("libfoo.so".to_string(), digest);
    let (key, found) = lock.expected_for(Path::new("/anywhere/libfoo.so")).unwrap();
    assert_eq!((key, found), ("libfoo.so", digest));
    assert!(
        lock.expected_for(Path::new("/anywhere/libbar.so"))
            .is_none()
    );

    let mut lock = Lock::new();
    lock.set("/opt/a/libfoo.so".to_string(), digest);
    assert!(
        lock.expected_for(Path::new("/opt/a/libfoo.so")).is_some(),
        "an absolute key matches its own path"
    );
    assert!(
        lock.expected_for(Path::new("/opt/b/libfoo.so")).is_none(),
        "an absolute key does not match another path with the same name"
    );
}

#[test]
fn an_absolute_key_wins_over_a_file_name_key() {
    let mut lock = Lock::new();
    let file_name = Digest::of(b"by name");
    let absolute = Digest::of(b"absolute");
    lock.set("libfoo.so".to_string(), file_name);
    lock.set("/opt/a/libfoo.so".to_string(), absolute);
    let (key, found) = lock.expected_for(Path::new("/opt/a/libfoo.so")).unwrap();
    assert_eq!((key, found), ("/opt/a/libfoo.so", absolute));
    let (key, found) = lock.expected_for(Path::new("/opt/b/libfoo.so")).unwrap();
    assert_eq!((key, found), ("libfoo.so", file_name));
}
