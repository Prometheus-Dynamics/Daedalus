//! SHA-256 vectors, the text form of a digest, and hashing a file.

use crate::support;

use std::io::Write;

use daedalus_integrity::{Digest, hash_file};

#[test]
fn empty_input_has_the_standard_digest() {
    assert_eq!(
        Digest::of(b"").to_string(),
        "e3b0c44298fc1c149afbf4c8996fb92427ae41e4649b934ca495991b7852b855"
    );
}

#[test]
fn abc_has_the_standard_digest() {
    assert_eq!(
        Digest::of(b"abc").to_string(),
        "ba7816bf8f01cfea414140de5dae2223b00361a396177a9cb410ff61f20015ad"
    );
}

#[test]
fn text_form_round_trips_and_rejects_bad_text() {
    let digest = Digest::of(b"abc");
    assert_eq!(digest.to_string().parse::<Digest>(), Ok(digest));
    let upper = digest.to_string().to_uppercase();
    assert!(upper.parse::<Digest>().is_err(), "uppercase is refused");
    assert!(
        "abc".parse::<Digest>().is_err(),
        "a short digest is refused"
    );
    let not_hex = "g".repeat(64);
    assert!(
        not_hex.parse::<Digest>().is_err(),
        "a non-hex digit is refused"
    );
}

#[test]
fn a_file_hashes_to_the_digest_of_its_bytes_and_reports_its_head() {
    let dir = support::scratch("digest-file");
    let path = dir.join("script");
    let body: Vec<u8> = (0..200_000u32).map(|n| (n % 251) as u8).collect();
    std::fs::File::create(&path)
        .unwrap()
        .write_all(&body)
        .unwrap();
    let file = std::fs::File::open(&path).unwrap();
    let (digest, head) = hash_file(&file).unwrap();
    assert_eq!(digest, Digest::of(&body));
    assert_eq!(head, [body[0], body[1]]);
    let _ = std::fs::remove_dir_all(dir);
}

#[test]
fn a_hashbang_file_is_seen_in_its_head() {
    let dir = support::scratch("digest-head");
    let path = dir.join("run");
    std::fs::write(&path, b"#!/bin/sh\necho hi\n").unwrap();
    let (_, head) = hash_file(&std::fs::File::open(&path).unwrap()).unwrap();
    assert_eq!(&head, b"#!");
    let _ = std::fs::remove_dir_all(dir);
}
