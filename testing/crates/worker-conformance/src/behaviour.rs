//! The worker behaviour suite: what a worker must do beyond the thirteen value fixtures.
//!
//! Each behaviour is one named case, and each is independent, so a language can opt in by name
//! while its SDK catches up. `behaviours(spec, &["events"])` runs one case; an empty list runs all.
//! A case that the worker fails is a [`Failure`] named after the behaviour, with round 0. A worker
//! that cannot be started, or does not register with the process backend, is an error instead.
//!
//! Five cases speak the wire protocol directly, as the host does: `events`, `state`, `chatty` and
//! `malformed` read and write packets, and `large` runs through the process backend with the
//! value fixture `fixture.raw_io`. `cancel` runs through the process backend, as a host would
//! cancel a run.
//!
//! Every read of a worker's output waits at most [`REPLY_WAIT`], and a worker that will not answer
//! is killed when the case ends, so a broken worker fails its case and does not hang the suite.

use std::process::{Child, ChildStdin, Command, Stdio};
use std::sync::mpsc::{Receiver, RecvTimeoutError, channel};
use std::thread;
use std::time::{Duration, Instant};

use daedalus_abi::testing::{release, run_once};
use daedalus_abi::{Slot, SlotState, Slots, StepCx, StepEnd, Word, own};
use daedalus_backend_process::{ProcessBackend, ProcessError, WorkerSpec};
use daedalus_wire::codec::Writer;
use daedalus_wire::{
    BoundaryValue, DEFAULT_CAP, FaultCode, Hello, HostMsg, Outputs, Run, RunStatus, Schema,
    WireValue, WorkerMsg, read_packet, write_packet,
};

use crate::fixtures::{CHATTY, EVENTS, STATEFUL_LABEL, WAIT_CANCEL};
use crate::large::{Transport, large_values};
use crate::runner::{Failure, registry};

/// The names of the behaviours, in the order they run.
pub const BEHAVIOURS: [&str; 6] = ["cancel", "events", "state", "chatty", "malformed", "large"];

/// The longest a worker may take to send one packet, once asked.
pub const REPLY_WAIT: Duration = Duration::from_secs(10);

/// How long `cancel` lets a run go before it sends the cancel.
const CANCEL_AFTER: Duration = Duration::from_millis(100);

/// The longest a run may take to end once cancelled. A worker that answers at once is well under.
/// The worker is given this as the host's grace too, so on a loaded machine a worker that does
/// answer is not killed first; one that ignores the cancel is killed at the bound, which fails it.
const CANCEL_BOUND: Duration = Duration::from_secs(5);

/// The size of the large value: a mebibyte, in and out.
const LARGE: usize = 1 << 20;

const COUNTER: &str = "fixture.stateful_counter";
const SCALAR_ADD: &str = "fixture.scalar_add";

/// A behaviour's refusal to run: a worker the process backend would not register, or a failure.
enum Stop {
    Refused(ProcessError),
    Failed(String),
}

impl From<String> for Stop {
    fn from(reason: String) -> Stop {
        Stop::Failed(reason)
    }
}

/// Runs the named behaviours (all of them when `names` is empty) against a worker started from
/// `spec`. A behaviour the worker fails is returned as a failure, named after it.
pub fn behaviours(spec: WorkerSpec, names: &[&str]) -> Result<Vec<Failure>, ProcessError> {
    let mut failures = Vec::new();
    for name in names {
        if !BEHAVIOURS.contains(name) {
            failures.push(Failure {
                fixture: "behaviour",
                round: 0,
                reason: format!("there is no behaviour named {name}"),
            });
        }
    }
    for &name in BEHAVIOURS
        .iter()
        .filter(|name| names.is_empty() || names.contains(name))
    {
        let outcome = match name {
            "cancel" => cancel(&spec),
            "events" => events(&spec),
            "state" => state(&spec),
            "chatty" => chatty(&spec),
            "malformed" => malformed(&spec),
            _ => large(&spec),
        };
        match outcome {
            Ok(()) => {}
            Err(Stop::Refused(error)) => return Err(error),
            Err(Stop::Failed(reason)) => failures.push(Failure {
                fixture: name,
                round: 0,
                reason,
            }),
        }
    }
    Ok(failures)
}

/// The transport a worker's large values went by, and its bytes checked. The behaviour `large`
/// runs this; a language's doc line should name the transport it reports here.
pub fn large_transport(spec: &WorkerSpec) -> Result<Transport, String> {
    let large = large_values(spec.clone(), LARGE, 2)?;
    large
        .transport()
        .ok_or_else(|| "the host counted neither a plain worker nor an agreed one".to_owned())
}

/// `cancel`: a node that waits until it sees the cancel is cancelled mid-run, and its run ends as
/// cancelled within [`CANCEL_BOUND`]. Not as a timeout, and not as a kill.
///
/// The worker must: poll `cancelled()` (or read its cancel flag) while it runs, and once it is
/// cancelled, end the run with status `cancelled` (the host then reports the run as cancelled).
fn cancel(spec: &WorkerSpec) -> Result<(), Stop> {
    let mut backend = ProcessBackend::new();
    backend
        .add_worker(spec.clone().grace(CANCEL_BOUND), &registry())
        .map_err(Stop::Refused)?;
    let mut step = crate::runner::compile_node(&backend, WAIT_CANCEL, 1, 1)?;
    let word = Word::new(0);
    let mut input = 0i64.to_le_bytes();
    let mut cells = [
        Slot {
            state: SlotState::Full as u32,
            owner: 0,
            data: input.as_mut_ptr(),
            len: input.len(),
            cap: input.len(),
            ..Slot::EMPTY
        },
        Slot::EMPTY,
    ];
    let mut cx = StepCx::new();
    cx.cancel = &word as *const Word;
    let started = Instant::now();
    let end = thread::scope(|scope| {
        scope.spawn(|| {
            thread::sleep(CANCEL_AFTER);
            word.set(1);
        });
        run_once(&mut step, &mut Slots::of(&mut cells), &mut cx)
    });
    let elapsed = started.elapsed();
    release(step);
    if !matches!(end, StepEnd::Fault) {
        return Err(format!(
            "a cancelled run ended {}, not as cancelled",
            match end {
                StepEnd::Done => "done",
                StepEnd::Pending(_) => "pending",
                StepEnd::Skipped => "skipped",
                StepEnd::Fault => "faulted",
            }
        )
        .into());
    }
    if !(cx.code_is_own() && cx.code == own::CANCELLED) {
        return Err(format!(
            "the run did not end as cancelled: {} (code {})",
            cx.reason(),
            cx.code
        )
        .into());
    }
    if elapsed > CANCEL_BOUND {
        return Err(format!("the run took {elapsed:?} to end once cancelled").into());
    }
    Ok(())
}

/// `events`: progress, log and detail arrive in the order the node emits them, all before the
/// run's outputs.
///
/// The worker's `fixture.events` node must emit, in this order: progress 0.5 "half", log Info
/// "after the progress", detail "a detail line", progress 1.0 "done", and then give its input.
fn events(spec: &WorkerSpec) -> Result<(), Stop> {
    let mut pipe = Pipe::start(spec)?;
    let (before, outputs) = pipe.run(1, EVENTS, &[("x", WireValue::I64(9))])?;
    let got: Vec<String> = before.iter().map(describe).collect();
    let want = [
        "progress 0.5 half",
        "log Info after the progress",
        "detail a detail line",
        "progress 1.0 done",
    ];
    if got != want {
        return Err(format!("the events came as {got:?}, not {want:?}").into());
    }
    expect_value(&outputs, "y", &WireValue::I64(9))?;
    Ok(())
}

/// `state`: an unset state exports as a fault, never as a value. A state round-trips through an
/// export and an import: what is exported is what the next run starts from. Both an integer state
/// and a string state are checked.
///
/// The worker must: answer an export of a node that has run no state, or has no state set, with a
/// fault that names the node; answer an export with a state value that carries the state the node
/// holds; and, after an import, start the next run from the imported value.
fn state(spec: &WorkerSpec) -> Result<(), Stop> {
    let mut pipe = Pipe::start(spec)?;
    pipe.send(&HostMsg::StateExport {
        node: STATEFUL_LABEL.to_owned(),
    })?;
    match pipe.next()? {
        WorkerMsg::Fault(fault) if fault.node.as_deref() == Some(STATEFUL_LABEL) => {}
        other => {
            return Err(format!(
                "an unset string state should export a fault, got {}",
                describe(&other)
            )
            .into());
        }
    }

    let (_, out) = pipe.run(1, COUNTER, &[("delta", WireValue::I64(5))])?;
    expect_value(&out, "total", &WireValue::I64(5))?;
    pipe.expect_state(COUNTER, &WireValue::I64(5))?;
    pipe.send(&HostMsg::StateImport {
        node: COUNTER.to_owned(),
        value: WireValue::I64(100),
    })?;
    let (_, out) = pipe.run(2, COUNTER, &[("delta", WireValue::I64(5))])?;
    expect_value(&out, "total", &WireValue::I64(105))?;
    pipe.expect_state(COUNTER, &WireValue::I64(105))?;

    let (_, out) = pipe.run(3, STATEFUL_LABEL, &[("in", WireValue::String("a".into()))])?;
    expect_value(&out, "out", &WireValue::String("a".into()))?;
    pipe.expect_state(STATEFUL_LABEL, &WireValue::String("a".into()))?;
    pipe.send(&HostMsg::StateImport {
        node: STATEFUL_LABEL.to_owned(),
        value: WireValue::String("carried".into()),
    })?;
    let (_, out) = pipe.run(4, STATEFUL_LABEL, &[("in", WireValue::String("!".into()))])?;
    expect_value(&out, "out", &WireValue::String("carried!".into()))?;
    pipe.expect_state(STATEFUL_LABEL, &WireValue::String("carried!".into()))?;
    Ok(())
}

/// `chatty`: a node body that prints to stdout does not corrupt the packet stream. The run's
/// outputs arrive intact.
///
/// The worker must keep its packets on stdout clean: a node's text goes through the SDK's log or
/// to stderr, never to the stream the protocol uses. `fixture.chatty` prints a line, then gives
/// its input plus one.
fn chatty(spec: &WorkerSpec) -> Result<(), Stop> {
    let mut pipe = Pipe::start(spec)?;
    let (_, outputs) = pipe.run(1, CHATTY, &[("x", WireValue::I64(41))])?;
    expect_value(&outputs, "y", &WireValue::I64(42))?;
    Ok(())
}

/// `malformed`: a malformed packet is answered with a fault, and the next good packet still runs.
///
/// The worker must: answer a packet it cannot decode with a `malformed packet` fault (it may keep
/// running, but must not exit), and then run the next valid run as usual.
fn malformed(spec: &WorkerSpec) -> Result<(), Stop> {
    let mut pipe = Pipe::start(spec)?;
    pipe.write_raw(&[0x7f])?;
    match pipe.next()? {
        WorkerMsg::Fault(fault) if fault.code == FaultCode::MalformedPacket => {}
        other => {
            return Err(format!(
                "a malformed packet should be answered with a malformed-packet fault, got {}",
                describe(&other)
            )
            .into());
        }
    }
    let (_, outputs) = pipe.run(
        2,
        SCALAR_ADD,
        &[("a", WireValue::I64(2)), ("b", WireValue::I64(40))],
    )?;
    expect_value(&outputs, "out", &WireValue::I64(42))?;
    Ok(())
}

/// `large`: a 1 MiB value goes in and comes back, the same bytes reversed, run twice.
///
/// The worker must: take a `bytes` input of any size and give a `bytes` output of the same size
/// (a plain packet, or a value by reference, as the worker and host agree). See `large_transport`
/// for which transport the host reports.
fn large(spec: &WorkerSpec) -> Result<(), Stop> {
    large_transport(spec)?;
    Ok(())
}

/// One running worker, with its pipes and the schema its hello declares. The worker is killed
/// when the pipe is dropped, so a case never leaves one behind.
struct Pipe {
    child: Child,
    stdin: ChildStdin,
    packets: Receiver<Result<Vec<u8>, String>>,
    schema: Schema,
}

impl Drop for Pipe {
    fn drop(&mut self) {
        // This is the child this pipe started, by the handle it holds.
        let _ = self.child.kill();
        let _ = self.child.wait();
    }
}

impl Pipe {
    /// Starts the worker of `spec` and reads its hello.
    fn start(spec: &WorkerSpec) -> Result<Pipe, String> {
        let mut command = Command::new(&spec.program);
        command.args(&spec.args);
        if spec.env_clear {
            command.env_clear();
        }
        for key in &spec.env_remove {
            command.env_remove(key);
        }
        for (key, value) in &spec.env {
            command.env(key, value);
        }
        if let Some(cwd) = &spec.cwd {
            command.current_dir(cwd);
        }
        if let Some(launch) = spec.launch {
            launch(&mut command);
        }
        command
            .stdin(Stdio::piped())
            .stdout(Stdio::piped())
            .stderr(Stdio::null());
        let mut child = command
            .spawn()
            .map_err(|error| format!("the worker would not start: {error}"))?;
        let (Some(stdin), Some(mut stdout)) = (child.stdin.take(), child.stdout.take()) else {
            return Err("the worker's pipes are not there".into());
        };
        let (sender, packets) = channel();
        let cap = spec.packet_cap;
        thread::spawn(move || {
            loop {
                let mut buf = Vec::new();
                match read_packet(&mut stdout, cap, &mut buf) {
                    Ok(()) => {
                        if sender.send(Ok(buf)).is_err() {
                            return;
                        }
                    }
                    Err(error) => {
                        let _ = sender.send(Err(error.to_string()));
                        return;
                    }
                }
            }
        });
        let mut pipe = Pipe {
            child,
            stdin,
            packets,
            schema: Schema::new(),
        };
        let buf = pipe.raw()?;
        let hello = match WorkerMsg::decode(&buf, &Schema::new()) {
            Ok(WorkerMsg::Hello(hello)) => hello,
            Ok(other) => {
                return Err(format!(
                    "the first packet was {}, not a hello",
                    describe(&other)
                ));
            }
            Err(error) => return Err(format!("the hello does not decode: {error}")),
        };
        hello
            .check()
            .map_err(|error| format!("the hello is refused: {error}"))?;
        pipe.schema = schema_of(&hello);
        Ok(pipe)
    }

    /// The next packet's bytes, or why there is none.
    fn raw(&mut self) -> Result<Vec<u8>, String> {
        match self.packets.recv_timeout(REPLY_WAIT) {
            Ok(Ok(buf)) => Ok(buf),
            Ok(Err(error)) => Err(format!("the packet stream is broken: {error}")),
            Err(RecvTimeoutError::Timeout) => {
                Err(format!("the worker sent no packet within {REPLY_WAIT:?}"))
            }
            Err(RecvTimeoutError::Disconnected) => Err("the worker closed its output".into()),
        }
    }

    /// The next message from the worker.
    fn next(&mut self) -> Result<WorkerMsg, String> {
        let buf = self.raw()?;
        WorkerMsg::decode(&buf, &self.schema)
            .map_err(|error| format!("a packet does not decode: {error}"))
    }

    fn send(&mut self, msg: &HostMsg) -> Result<(), String> {
        let mut out = Writer::new();
        msg.encode(&self.schema, &mut out)
            .map_err(|error| format!("the host's message does not encode: {error}"))?;
        write_packet(&mut self.stdin, &out.buf, DEFAULT_CAP)
            .map_err(|error| format!("the worker did not take a packet: {error}"))
    }

    fn write_raw(&mut self, body: &[u8]) -> Result<(), String> {
        write_packet(&mut self.stdin, body, DEFAULT_CAP)
            .map_err(|error| format!("the worker did not take a packet: {error}"))
    }

    /// Sends a run of one node with these inputs, and reads until its outputs. Gives the messages
    /// that came before the outputs, in order, and the outputs. A run that faults is an error.
    fn run(
        &mut self,
        run: u64,
        node: &str,
        inputs: &[(&str, WireValue)],
    ) -> Result<(Vec<WorkerMsg>, Outputs), String> {
        let inputs = inputs
            .iter()
            .map(|(port, value)| BoundaryValue {
                node: node.to_owned(),
                port: (*port).to_owned(),
                value: value.clone(),
            })
            .collect();
        self.send(&HostMsg::Run(Run {
            run,
            region: 0,
            nodes: vec![node.to_owned()],
            inputs,
        }))?;
        let mut before = Vec::new();
        loop {
            match self.next()? {
                WorkerMsg::Outputs(outputs) => {
                    if outputs.status != RunStatus::Ok {
                        return Err(format!("the run ended {:?}", outputs.status));
                    }
                    return Ok((before, outputs));
                }
                WorkerMsg::Fault(fault) => {
                    return Err(format!(
                        "the run faulted: {}: {}",
                        fault.code.name(),
                        fault.reason
                    ));
                }
                other => before.push(other),
            }
        }
    }

    /// Exports a node's state, and checks it is `value`.
    fn expect_state(&mut self, node: &str, value: &WireValue) -> Result<(), String> {
        self.send(&HostMsg::StateExport {
            node: node.to_owned(),
        })?;
        match self.next()? {
            WorkerMsg::StateValue {
                node: of,
                value: got,
            } if of == node && got == *value => Ok(()),
            other => Err(format!(
                "exporting {node} should give {value:?}, got {}",
                describe(&other)
            )),
        }
    }
}

/// The schema a worker's hello declares, for its packets.
fn schema_of(hello: &Hello) -> Schema {
    Schema::from_hello(hello, &[])
}

/// Checks a run's output port holds `value`.
fn expect_value(outputs: &Outputs, port: &str, value: &WireValue) -> Result<(), String> {
    match outputs.outputs.iter().find(|entry| entry.port == port) {
        Some(entry) if entry.value == *value => Ok(()),
        Some(entry) => Err(format!(
            "output {port} should be {value:?}, got {:?}",
            entry.value
        )),
        None => Err(format!("the run gave no output {port}")),
    }
}

/// A message as a short line, for comparing and for reporting.
fn describe(msg: &WorkerMsg) -> String {
    match msg {
        WorkerMsg::Progress(progress) => format!(
            "progress {} {}",
            progress
                .fraction
                .map_or_else(|| "none".to_owned(), |fraction| format!("{fraction:?}")),
            progress.message
        ),
        WorkerMsg::Log { level, text } => format!("log {level:?} {text}"),
        WorkerMsg::Detail { text, .. } => format!("detail {text}"),
        WorkerMsg::NodeCode { code, .. } => format!("code {code}"),
        WorkerMsg::Fault(fault) => format!("fault {}", fault.code.name()),
        WorkerMsg::StateValue { node, value } => format!("state {node} {value:?}"),
        WorkerMsg::Outputs(outputs) => format!("outputs {:?}", outputs.status),
        WorkerMsg::Hello(_) => "hello".to_owned(),
        WorkerMsg::Pong => "pong".to_owned(),
    }
}
