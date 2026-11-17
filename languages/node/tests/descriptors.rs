//! The worker's descriptors on the host's socket pair. On Linux the host gives the worker one socket
//! for stdin and stdout, so the two share an open file, and so a flag set on one is set on the
//! other. Until the first run the worker reads stdin itself, blocking, and nothing changes the
//! flags. Once the reader thread wraps stdin, both become non-blocking, and the worker's writes
//! must cope with that. This runs the worker as the host runs it, and reads the flags from `/proc`.

#![cfg(target_os = "linux")]

use std::io::{Read, Write};
use std::os::fd::OwnedFd;
use std::os::unix::net::UnixStream;
use std::process::{Command, Stdio};

use daedalus_lang_node::fixture_worker;

/// Whether descriptor `fd` of process `pid` is non-blocking, from its open-file flags.
fn non_blocking(pid: u32, fd: u32) -> bool {
    let text = std::fs::read_to_string(format!("/proc/{pid}/fdinfo/{fd}")).expect("fdinfo");
    let flags = text
        .lines()
        .find_map(|line| line.strip_prefix("flags:"))
        .expect("a flags line");
    let flags = u32::from_str_radix(flags.trim(), 8).expect("octal flags");
    flags & 0o4000 != 0
}

fn send(stream: &mut UnixStream, body: &[u8]) {
    stream
        .write_all(&(body.len() as u32).to_le_bytes())
        .and_then(|()| stream.write_all(body))
        .expect("the worker takes a packet");
}

fn receive(stream: &mut UnixStream) -> Vec<u8> {
    let mut len = [0u8; 4];
    stream.read_exact(&mut len).expect("a packet header");
    let mut body = vec![0u8; u32::from_le_bytes(len) as usize];
    stream.read_exact(&mut body).expect("a packet body");
    body
}

/// A run of `fixture.scalar_add` with a = 2 and b = 40, as the host encodes it.
fn scalar_add_run() -> Vec<u8> {
    let node = b"fixture.scalar_add";
    let text = |out: &mut Vec<u8>, raw: &[u8]| {
        out.extend_from_slice(&(raw.len() as u32).to_le_bytes());
        out.extend_from_slice(raw);
    };
    let mut body = vec![1u8];
    body.extend_from_slice(&1u64.to_le_bytes()); // run
    body.extend_from_slice(&0u32.to_le_bytes()); // region
    body.extend_from_slice(&1u32.to_le_bytes()); // one node
    text(&mut body, node);
    body.extend_from_slice(&2u32.to_le_bytes()); // two inputs
    for (port, value) in [(&b"a"[..], 2i64), (&b"b"[..], 40)] {
        text(&mut body, node);
        text(&mut body, port);
        body.extend_from_slice(&value.to_le_bytes());
    }
    body
}

#[test]
// The skip line goes to stderr, where a run without node shows why nothing was checked.
#[allow(clippy::print_stderr)]
fn the_worker_keeps_its_descriptors_blocking_until_its_first_run_and_then_shares_one_mode() {
    if Command::new("node").arg("--version").output().is_err() {
        eprintln!("skipped: no node on the path, so the worker's descriptors were not checked");
        return;
    }
    let (mut host, child_end) = UnixStream::pair().expect("a socket pair");
    let child_out = child_end.try_clone().expect("a second end of the pair");
    let mut child = Command::new("node")
        .arg(fixture_worker())
        .stdin(Stdio::from(OwnedFd::from(child_end)))
        .stdout(Stdio::from(OwnedFd::from(child_out)))
        .stderr(Stdio::null())
        .spawn()
        .expect("node starts");
    let pid = child.id();

    receive(&mut host); // the hello
    assert!(
        !non_blocking(pid, 0) && !non_blocking(pid, 1),
        "after the hello"
    );

    send(&mut host, &[0x05]); // a ping
    assert_eq!(receive(&mut host)[0], 0x46, "a pong");
    assert!(
        !non_blocking(pid, 0) && !non_blocking(pid, 1),
        "after a ping"
    );

    send(&mut host, &scalar_add_run());
    assert_eq!(receive(&mut host)[0], 0x41, "an outputs packet");
    // The reader wrapped stdin, and fd 1 shares its open file: the two modes are the same.
    assert_eq!(
        non_blocking(pid, 0),
        non_blocking(pid, 1),
        "fd 0 and fd 1 share one open file"
    );

    // Writes after the reader started still go through whole: a second run is answered.
    send(&mut host, &scalar_add_run());
    assert_eq!(receive(&mut host)[0], 0x41, "a second outputs packet");

    drop(host);
    child
        .wait()
        .expect("the worker ends when its stream closes");
}
