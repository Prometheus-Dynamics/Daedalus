//! A stateful node's state through the worker protocol: a run keeps its state, the state is exported
//! as a state value, and an imported state is what the next run starts from.

use std::io::{Read, Write};
use std::path::{Path, PathBuf};
use std::process::{Command, Stdio};
use std::sync::OnceLock;

use daedalus_lang_cpp::build;
use daedalus_wire::codec::Writer;
use daedalus_wire::{DEFAULT_CAP, kind, read_packet, write_packet};

fn built() -> PathBuf {
    static BUILT: OnceLock<PathBuf> = OnceLock::new();
    BUILT
        .get_or_init(|| {
            let dir =
                daedalus_lang_cpp::build_dir(Path::new(env!("CARGO_TARGET_TMPDIR")), "cpp-state");
            build(&dir).expect("cmake builds the fixture worker")
        })
        .clone()
}

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

/// The last eight bytes of a packet, read as the little-endian i64 a counter's value is.
fn last_i64(packet: &[u8]) -> i64 {
    let raw: [u8; 8] = packet[packet.len() - 8..].try_into().expect("eight bytes");
    i64::from_le_bytes(raw)
}

/// Sends one request and reads the reply the worker sends back.
fn exchange(stdin: &mut impl Write, stdout: &mut impl Read, request: &[u8]) -> Vec<u8> {
    let mut buf = Vec::new();
    write_packet(stdin, request, DEFAULT_CAP).expect("the request is written");
    read_packet(stdout, DEFAULT_CAP, &mut buf).expect("the reply arrives");
    buf
}

#[test]
fn state_is_exported_and_imported_through_the_worker_protocol() {
    let worker = built();
    let mut child = Command::new(&worker)
        .stdin(Stdio::piped())
        .stdout(Stdio::piped())
        .stderr(Stdio::piped())
        .spawn()
        .expect("the worker starts");
    let mut stdout = child.stdout.take().expect("stdout");
    let mut stdin = child.stdin.take().expect("stdin");
    let mut hello = Vec::new();
    read_packet(&mut stdout, DEFAULT_CAP, &mut hello).expect("the hello arrives");

    // A run adds its input to the state, and the state is exported as it stands.
    let outputs = exchange(&mut stdin, &mut stdout, &run(1, 5));
    assert_eq!(outputs[0], kind::OUTPUTS);
    assert_eq!(last_i64(&outputs), 5);
    let state = exchange(&mut stdin, &mut stdout, &export());
    assert_eq!(state[0], kind::STATE_VALUE, "the export is a state value");
    assert_eq!(last_i64(&state), 5);

    // An import has no reply. The next run starts from the imported state.
    write_packet(&mut stdin, &import(100), DEFAULT_CAP).expect("the import is written");
    let outputs = exchange(&mut stdin, &mut stdout, &run(2, 5));
    assert_eq!(
        last_i64(&outputs),
        105,
        "the run starts from the imported state"
    );
    let state = exchange(&mut stdin, &mut stdout, &export());
    assert_eq!(last_i64(&state), 105);

    write_packet(&mut stdin, &[kind::SHUTDOWN], DEFAULT_CAP).expect("shutdown is written");
    drop(stdin);
    let status = child.wait().expect("the worker exits");
    assert!(status.success());
}
