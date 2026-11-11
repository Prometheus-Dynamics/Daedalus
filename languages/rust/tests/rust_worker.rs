//! The fixture worker's state, through the worker protocol: a run keeps its state, the state is
//! exported as a state value, and an imported state is where the next run starts.

use std::path::Path;
use std::process::{Command, Stdio};

use daedalus_wire::codec::Writer;
use daedalus_wire::{DEFAULT_CAP, kind, read_packet, write_packet};

const WORKER: &str = env!("CARGO_BIN_EXE_daedalus-rust-fixture-worker");
const NODE: &str = "fixture.stateful_counter";

fn run(number: u64, delta: i64) -> Vec<u8> {
    let mut out = Writer::new();
    out.u8(kind::RUN);
    out.u64(number);
    out.u32(0);
    out.u32(1);
    out.str(NODE).expect("a short string");
    out.u32(1);
    out.str(NODE).expect("a short string");
    out.str("delta").expect("a short string");
    out.i64(delta);
    out.buf
}

fn export() -> Vec<u8> {
    let mut out = Writer::new();
    out.u8(kind::STATE_EXPORT);
    out.str(NODE).expect("a short string");
    out.buf
}

fn import(value: i64) -> Vec<u8> {
    let mut out = Writer::new();
    out.u8(kind::STATE_IMPORT);
    out.str(NODE).expect("a short string");
    out.i64(value);
    out.buf
}

/// The last eight bytes of a packet, read as the little-endian i64 the counter holds.
fn last_i64(packet: &[u8]) -> i64 {
    let raw: [u8; 8] = packet[packet.len() - 8..].try_into().expect("eight bytes");
    i64::from_le_bytes(raw)
}

#[test]
fn a_stateful_node_exports_and_imports_its_state_through_the_protocol() {
    let mut child = Command::new(Path::new(WORKER))
        .stdin(Stdio::piped())
        .stdout(Stdio::piped())
        .stderr(Stdio::piped())
        .spawn()
        .expect("the worker starts");
    let mut stdout = child.stdout.take().expect("stdout");
    let mut stdin = child.stdin.take().expect("stdin");
    let mut buf = Vec::new();
    read_packet(&mut stdout, DEFAULT_CAP, &mut buf).expect("the hello arrives");

    let mut exchange = |stdin: &mut std::process::ChildStdin, request: &[u8]| {
        write_packet(stdin, request, DEFAULT_CAP).expect("the request is written");
        read_packet(&mut stdout, DEFAULT_CAP, &mut buf).expect("the reply arrives");
        buf.clone()
    };

    let outputs = exchange(&mut stdin, &run(1, 5));
    assert_eq!(outputs[0], kind::OUTPUTS);
    assert_eq!(last_i64(&outputs), 5);
    let state = exchange(&mut stdin, &export());
    assert_eq!(state[0], kind::STATE_VALUE, "the export is a state value");
    assert_eq!(last_i64(&state), 5);

    // An import has no reply; the next run starts from the imported value.
    write_packet(&mut stdin, &import(100), DEFAULT_CAP).expect("the import is written");
    let outputs = exchange(&mut stdin, &run(2, 5));
    assert_eq!(last_i64(&outputs), 105);
    let state = exchange(&mut stdin, &export());
    assert_eq!(last_i64(&state), 105);

    write_packet(&mut stdin, &[kind::SHUTDOWN], DEFAULT_CAP).expect("shutdown is written");
    drop(stdin);
    assert!(child.wait().expect("the worker exits").success());
}
