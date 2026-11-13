//! The serve loop's contract, run over the test worker: a panicking node is a fault and the worker
//! keeps serving, a malformed line is an error and the next run still runs, a clean end of input is a
//! clean exit, events arrive in emission order before the outputs, and a node that polls the context
//! sees a cancel.

use std::path::Path;
use std::process::{Command, Stdio};

use daedalus_wire::codec::Writer;
use daedalus_wire::{
    DEFAULT_CAP, FaultCode, LogLevel, Schema, WorkerMsg, kind, read_packet, write_packet,
};

const WORKER: &str = env!("CARGO_BIN_EXE_daedalus-rust-serve-worker");

/// The status byte of an outputs packet, as the wire encodes a run's status.
const RUN_OK: u8 = 0;
const RUN_CANCELLED: u8 = 1;

/// A run of one node with one input port `x`.
fn run(number: u64, node: &str, x: i64) -> Vec<u8> {
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

fn cancel(number: u64) -> Vec<u8> {
    let mut out = Writer::new();
    out.u8(kind::CANCEL);
    out.u64(number);
    out.buf
}

/// A worker started with its pipes, and the hello read off it.
struct Worker {
    child: std::process::Child,
    stdin: Option<std::process::ChildStdin>,
    stdout: std::process::ChildStdout,
}

impl Worker {
    fn start() -> Worker {
        let mut child = Command::new(Path::new(WORKER))
            .stdin(Stdio::piped())
            .stdout(Stdio::piped())
            .stderr(Stdio::piped())
            .spawn()
            .expect("the worker starts");
        let stdin = child.stdin.take();
        let mut stdout = child.stdout.take().expect("stdout");
        let mut hello = Vec::new();
        read_packet(&mut stdout, DEFAULT_CAP, &mut hello).expect("the hello arrives");
        assert_eq!(hello[0], kind::HELLO);
        Worker {
            child,
            stdin,
            stdout,
        }
    }

    fn send(&mut self, body: &[u8]) {
        let stdin = self.stdin.as_mut().expect("stdin is open");
        write_packet(stdin, body, DEFAULT_CAP).expect("the packet is written");
    }

    /// The next packet from the worker, decoded as a message.
    fn next(&mut self) -> WorkerMsg {
        let mut buf = Vec::new();
        read_packet(&mut self.stdout, DEFAULT_CAP, &mut buf).expect("a packet arrives");
        WorkerMsg::decode(&buf, &Schema::new()).expect("the packet decodes")
    }

    /// The next packet's bytes, for the outputs, whose values need the host's schema to decode.
    fn next_raw(&mut self) -> Vec<u8> {
        let mut buf = Vec::new();
        read_packet(&mut self.stdout, DEFAULT_CAP, &mut buf).expect("a packet arrives");
        buf
    }

    /// The next packet's first byte, which is its kind.
    fn next_kind(&mut self) -> u8 {
        self.next_raw()[0]
    }

    fn finish(mut self) -> std::process::ExitStatus {
        self.stdin.take();
        self.child.wait().expect("the worker exits")
    }
}

#[test]
fn a_panicking_node_is_a_fault_and_the_worker_keeps_serving() {
    let mut worker = Worker::start();
    worker.send(&run(1, "serve.panics", 1));
    match worker.next() {
        WorkerMsg::Fault(fault) => {
            assert_eq!(fault.code, FaultCode::NodePanic);
            assert_eq!(fault.run, 1);
            assert_eq!(
                fault.reason, "node panicked: the node panicked on purpose",
                "the reason is what the panic said"
            );
        }
        other => panic!("expected a fault, got {other:?}"),
    }
    assert_eq!(
        worker.next_kind(),
        kind::OUTPUTS,
        "the run ends with its outputs"
    );

    worker.send(&run(2, "serve.adds", 1));
    assert_eq!(worker.next_kind(), kind::OUTPUTS, "the next run still runs");
    assert!(worker.finish().success());
}

#[test]
fn a_malformed_line_is_an_error_and_the_next_message_still_runs() {
    let mut worker = Worker::start();
    worker.send(&[0x7f]);
    match worker.next() {
        WorkerMsg::Fault(fault) => assert_eq!(fault.code, FaultCode::MalformedPacket),
        other => panic!("expected a malformed-packet fault, got {other:?}"),
    }
    worker.send(&run(1, "serve.adds", 41));
    assert_eq!(worker.next_kind(), kind::OUTPUTS, "the worker still serves");
    assert!(worker.finish().success());
}

#[test]
fn end_of_input_is_a_clean_exit() {
    let mut worker = Worker::start();
    worker.send(&run(1, "serve.adds", 1));
    assert_eq!(worker.next_kind(), kind::OUTPUTS);
    // Closing stdin without a shutdown message is the host going away: the worker exits cleanly.
    assert!(worker.finish().success());
}

#[test]
fn events_travel_in_the_response_in_emission_order() {
    let mut worker = Worker::start();
    worker.send(&run(1, "serve.events", 9));
    match worker.next() {
        WorkerMsg::Progress(progress) => assert_eq!(progress.fraction, Some(0.5)),
        other => panic!("expected the first progress, got {other:?}"),
    }
    match worker.next() {
        WorkerMsg::Log { level, text } => {
            assert_eq!(level, LogLevel::Info);
            assert_eq!(text, "after the progress");
        }
        other => panic!("expected the log line, got {other:?}"),
    }
    match worker.next() {
        WorkerMsg::Progress(progress) => assert_eq!(progress.fraction, Some(1.0)),
        other => panic!("expected the second progress, got {other:?}"),
    }
    let outputs = worker.next_raw();
    assert_eq!(outputs[0], kind::OUTPUTS, "the outputs come last");
    assert_eq!(outputs[9], RUN_OK, "the run finished");
    assert!(worker.finish().success());
}

#[test]
fn a_node_that_polls_the_context_sees_a_cancel() {
    let mut worker = Worker::start();
    // The cancel follows the run at once: the worker must not lose it, whichever thread is first.
    worker.send(&run(7, "serve.waits", 0));
    worker.send(&cancel(7));
    let outputs = worker.next_raw();
    assert_eq!(outputs[0], kind::OUTPUTS);
    assert_eq!(
        outputs[1..9],
        7u64.to_le_bytes(),
        "the outputs name the run"
    );
    assert_eq!(outputs[9], RUN_CANCELLED, "the run ends cancelled");
    assert!(worker.finish().success());
}
