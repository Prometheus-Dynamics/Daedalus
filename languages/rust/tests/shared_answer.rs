//! A worker asks its host for shared memory and never ignores the answer, or its absence: a host
//! that says plain is logged with its reason, and an older host that says nothing before its first
//! run is logged as such. Either way the worker serves on the plain path.

#![cfg(target_os = "linux")]

use std::os::unix::net::UnixStream;
use std::process::{Command, Stdio};

use daedalus_process_shm::PLAIN_LINE;
use daedalus_wire::codec::Writer;
use daedalus_wire::shared::{ANY_TYPE, BY_REFERENCE, Shared, VERSION};
use daedalus_wire::{DEFAULT_CAP, Schema, WorkerMsg, kind, read_packet, write_packet};

const WORKER: &str = env!("CARGO_BIN_EXE_daedalus-rust-serve-worker");

/// A run of `serve.adds` with `x`, as `tests/serve.rs` builds it.
fn run(number: u64, x: i64) -> Vec<u8> {
    let node = "serve.adds";
    let mut out = Writer::new();
    out.u8(kind::RUN);
    out.u64(number);
    out.u32(0);
    out.u32(1);
    out.str(node).expect("a short string");
    out.u32(1);
    out.str(node).expect("a short string");
    out.str("x").expect("a short string");
    out.i64(x);
    out.buf
}

/// The worker started on one socket, as a host on Linux starts it, past its hello and its ask.
fn start() -> (std::process::Child, UnixStream) {
    let (host, worker) = UnixStream::pair().expect("a socket pair");
    let input = worker.try_clone().expect("the socket clones");
    let child = Command::new(WORKER)
        .stdin(Stdio::from(std::os::fd::OwnedFd::from(input)))
        .stdout(Stdio::from(std::os::fd::OwnedFd::from(worker)))
        .stderr(Stdio::null())
        .spawn()
        .expect("the worker starts");
    let mut reader = host.try_clone().expect("the socket clones");
    let mut buf = Vec::new();
    read_packet(&mut reader, DEFAULT_CAP, &mut buf).expect("the hello arrives");
    assert_eq!(buf[0], kind::HELLO);
    read_packet(&mut reader, DEFAULT_CAP, &mut buf).expect("the ask arrives");
    assert_eq!(
        Shared::decode(&buf).expect("the ask decodes"),
        Shared::Ask { version: VERSION }
    );
    read_packet(&mut reader, DEFAULT_CAP, &mut buf).expect("the want arrives");
    assert_eq!(
        Shared::decode(&buf).expect("the want decodes"),
        Shared::Want {
            features: BY_REFERENCE | ANY_TYPE
        }
    );
    (child, host)
}

/// The packets up to and including the run's outputs, as the log lines among them.
fn until_outputs(socket: &mut UnixStream) -> Vec<String> {
    let mut logs = Vec::new();
    let mut buf = Vec::new();
    loop {
        read_packet(socket, DEFAULT_CAP, &mut buf).expect("a packet arrives");
        if buf[0] == kind::OUTPUTS {
            return logs;
        }
        if let Ok(WorkerMsg::Log { text, .. }) = WorkerMsg::decode(&buf, &Schema::new()) {
            logs.push(text);
        }
    }
}

#[test]
fn a_host_that_says_nothing_is_logged_once_as_plain_and_served() {
    let (mut child, mut host) = start();
    // An older host knows no ask: it goes straight to its first run.
    write_packet(&mut host, &run(1, 1), DEFAULT_CAP).expect("the run is written");
    assert_eq!(until_outputs(&mut host), [PLAIN_LINE.to_owned()]);
    write_packet(&mut host, &run(2, 1), DEFAULT_CAP).expect("the run is written");
    assert!(until_outputs(&mut host).is_empty(), "the line is said once");
    drop(host);
    assert!(child.wait().expect("the worker exits").success());
}

#[test]
fn a_host_that_says_plain_is_logged_with_its_reason_and_served() {
    let (mut child, mut host) = start();
    let mut answer = Writer::new();
    Shared::Plain {
        reason: "the host lends no memory here".to_owned(),
    }
    .encode(true, &mut answer)
    .expect("the answer encodes");
    write_packet(&mut host, &answer.buf, DEFAULT_CAP).expect("the answer is written");
    write_packet(&mut host, &run(1, 1), DEFAULT_CAP).expect("the run is written");
    assert_eq!(
        until_outputs(&mut host),
        [format!("{PLAIN_LINE}: the host lends no memory here")]
    );
    drop(host);
    assert!(child.wait().expect("the worker exits").success());
}
