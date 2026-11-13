//! The worker's link and its supervision, and the step that runs a region in the worker.
//!
//! A region is one run message. The step writes the run, reads packets until its outputs come
//! back, and copies them into the output cells. A dead, hung or malformed worker is killed, and
//! the link goes down until a backoff has passed. A node's own fault leaves the worker up.

use std::sync::{Arc, Mutex};
use std::time::{Duration, Instant};

use daedalus_abi::{OwnedStep, Slot, Slots, StepCx, StepEnd};
use daedalus_value::OwnerKind;
use daedalus_wire::codec::{Reader, Writer};
use daedalus_wire::{Dir, Hello, Schema, WireError, WireType, WorkerMsg, decode_value};

use crate::backoff::delay_after;
use crate::child::{Got, Worker};
use crate::spec::WorkerSpec;
use crate::telemetry::{Count, Telemetry};

/// The stderr bytes a fault report carries.
const STDERR_REPORT: usize = 512;

/// The bytes an inline output may hold: the runner copies anything this long.
const INLINE: usize = 16;

/// One node as the worker declared it.
#[derive(Clone, Debug)]
pub(crate) struct NodePorts {
    pub id: String,
    pub inputs: Vec<(String, WireType)>,
    pub outputs: Vec<(String, WireType)>,
}

/// What the link is doing right now.
pub(crate) enum Link {
    Up(Worker),
    /// Down until the backoff has passed. Nothing runs before then.
    Down {
        until: Instant,
    },
}

pub(crate) struct Supervisor {
    pub link: Link,
    pub failures: u32,
    pub run: u64,
    pub writer: Writer,
    pub killed: Vec<u32>,
    pub fault: Option<String>,
}

/// A worker, its declarations and its state. Shared by every step that runs in it.
pub(crate) struct Shared {
    pub spec: WorkerSpec,
    pub schema: Schema,
    pub nodes: Vec<NodePorts>,
    pub telemetry: Arc<Telemetry>,
    pub state: Mutex<Supervisor>,
}

impl Shared {
    pub(crate) fn lock(&self) -> std::sync::MutexGuard<'_, Supervisor> {
        match self.state.lock() {
            Ok(guard) => guard,
            Err(poisoned) => poisoned.into_inner(),
        }
    }
}

/// Where a port is: its node's position in the region, its node in `Shared::nodes`, and its
/// index in that node's inputs or outputs.
#[derive(Clone, Copy, Debug)]
pub(crate) struct Binding {
    pub node: u16,
    pub port: u16,
}

/// The way a run ended, before the step reports it.
enum Outcome {
    /// The worker sent outputs. The status is the worker's.
    Replied(RunStatus),
    /// The worker's channel broke: it exited, or sent a packet it could not write.
    Gone(String),
    /// The deadline passed and the cancel was not answered, so the worker was killed.
    Killed(u32),
}

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub(crate) enum RunStatus {
    Ok,
    Cancelled,
    Fault,
}

/// Why a link is not up.
pub(crate) enum NotUp {
    /// The worker failed a moment ago and its next start isn't due. Not a new failure.
    BackingOff(String),
    /// The worker was started and didn't come up.
    Start(String),
}

/// What a fault does to the worker's link.
#[derive(Clone, Copy, PartialEq, Eq)]
enum Blame {
    /// The worker is fine: the fault was in the run's own data, or the worker reported it.
    Nobody,
    /// The worker failed. It is killed, and its next start waits out the backoff.
    Worker,
    /// The host stopped the worker itself, at a deadline or a cancel it didn't answer. It is
    /// started again on the next run, with no wait: nothing here says the next start would fail.
    Host,
}

/// Brings the link up. A down link waits for its backoff, then starts the worker again.
pub(crate) fn ensure_up(shared: &Shared, sup: &mut Supervisor) -> Result<(), NotUp> {
    let until = match &sup.link {
        Link::Up(_) => return Ok(()),
        Link::Down { until } => *until,
    };
    let now = Instant::now();
    if now < until {
        return Err(NotUp::BackingOff(format!(
            "worker {} is backing off for {:?}",
            shared.spec.program.display(),
            until - now
        )));
    }
    match start(shared) {
        Ok(worker) => {
            shared.telemetry.bump(Count::Restart);
            sup.link = Link::Up(worker);
            Ok(())
        }
        Err(reason) => {
            sup.failures = sup.failures.saturating_add(1);
            sup.link = Link::Down {
                until: Instant::now() + delay_after(sup.failures),
            };
            Err(NotUp::Start(reason))
        }
    }
}

/// Starts a worker and reads its hello. A hello with another layout is refused.
fn start(shared: &Shared) -> Result<Worker, String> {
    let mut worker =
        Worker::spawn(&shared.spec, &shared.telemetry).map_err(|error| error.to_string())?;
    let hello = read_hello(&worker, shared.spec.startup)?;
    if let Err(error) = hello.check() {
        worker.kill();
        return Err(format!("{}: {error}", shared.spec.program.display()));
    }
    Ok(worker)
}

pub(crate) fn read_hello(worker: &Worker, startup: Duration) -> Result<Hello, String> {
    match worker.receive(Some(startup)) {
        Got::Packet(buf) => {
            let decoded = WorkerMsg::decode(&buf, &Schema::new());
            worker.give_back(buf);
            match decoded {
                Ok(WorkerMsg::Hello(hello)) => Ok(hello),
                Ok(_) => Err("the worker's first message was not a hello".into()),
                Err(error) => Err(format!("the worker's hello did not decode: {error}")),
            }
        }
        Got::Broken(error) => Err(format!("the worker closed before its hello: {error}")),
        Got::Timeout => Err(format!("no hello within {startup:?}")),
    }
}

/// The step that runs a region in a worker.
pub(crate) struct WorkerStep {
    pub shared: Arc<Shared>,
    /// Indexes into `shared.nodes`, in the region's order.
    pub region: Vec<u16>,
    pub reads: Vec<Binding>,
    pub writes: Vec<Binding>,
    /// One buffer per output, kept between runs so a run reuses them.
    pub outbufs: Vec<Vec<u8>>,
    pub produced: Vec<bool>,
}

impl WorkerStep {
    pub(crate) fn new(
        shared: Arc<Shared>,
        region: Vec<u16>,
        reads: Vec<Binding>,
        writes: Vec<Binding>,
    ) -> WorkerStep {
        let outbufs = writes.iter().map(|_| Vec::new()).collect();
        let produced = vec![false; writes.len()];
        WorkerStep {
            shared,
            region,
            reads,
            writes,
            outbufs,
            produced,
        }
    }

    fn port_type(&self, binding: Binding, dir: Dir) -> &WireType {
        let node = &self.shared.nodes[binding.node as usize];
        match dir {
            Dir::Output => &node.outputs[binding.port as usize].1,
            _ => &node.inputs[binding.port as usize].1,
        }
    }

    /// Writes the run message: the region's nodes, then each input that has a value.
    fn encode_run(&self, run: u64, cells: &[Slot], out: &mut Writer) -> Result<usize, String> {
        out.u8(daedalus_wire::kind::RUN);
        out.u64(run);
        out.u32(0);
        out.len(self.region.len())
            .map_err(|error| error.to_string())?;
        for node in &self.region {
            out.str(&self.shared.nodes[*node as usize].id)
                .map_err(|error| error.to_string())?;
        }
        let mut count = 0usize;
        for (index, _) in self.reads.iter().enumerate() {
            if cells[index].bytes().is_some() {
                count += 1;
            }
        }
        out.len(count).map_err(|error| error.to_string())?;
        let mut sent = 0usize;
        for (index, binding) in self.reads.iter().enumerate() {
            let Some(bytes) = cells[index].bytes() else {
                continue;
            };
            let node = &self.shared.nodes[binding.node as usize];
            out.str(&node.id).map_err(|error| error.to_string())?;
            out.str(&node.inputs[binding.port as usize].0)
                .map_err(|error| error.to_string())?;
            put_raw(self.port_type(*binding, Dir::Input), bytes, out)?;
            sent += bytes.len();
        }
        Ok(sent)
    }

    /// Reads an `Outputs` packet into the output buffers. Returns the worker's status.
    fn read_outputs(&mut self, buf: &[u8], schema: &Schema) -> Result<RunStatus, WireError> {
        let mut input = Reader::new(buf);
        let _kind = input.u8()?;
        let _run = input.u64()?;
        let status = match input.u8()? {
            0 => RunStatus::Ok,
            1 => RunStatus::Cancelled,
            2 => RunStatus::Fault,
            tag => {
                return Err(WireError::BadTag {
                    what: "run status",
                    tag: u32::from(tag),
                });
            }
        };
        let count = input.count()?;
        for produced in self.produced.iter_mut() {
            *produced = false;
        }
        for _ in 0..count {
            let node = input.text()?;
            let port = input.text()?;
            let position = self.writes.iter().position(|binding| {
                let ports = &self.shared.nodes[binding.node as usize];
                ports.id == node && ports.outputs[binding.port as usize].0 == port
            });
            let Some(index) = position else {
                return Err(WireError::UnknownPort {
                    node: node.to_owned(),
                    port: port.to_owned(),
                });
            };
            let binding = self.writes[index];
            let ty = self.port_type(binding, Dir::Output).clone();
            let out = &mut self.outbufs[index];
            out.clear();
            read_raw(&ty, &mut input, schema, out)?;
            self.produced[index] = true;
        }
        input.finish()?;
        Ok(status)
    }

    /// Points an output cell at its buffer: inline for a short value, shared for a long one.
    fn publish(cell: &mut Slot, bytes: &mut Vec<u8>) {
        let len = bytes.len();
        if len <= INLINE {
            cell.owner = OwnerKind::Inline as u32;
            cell.data = bytes.as_mut_ptr();
        } else {
            let shared: Arc<[u8]> = Arc::from(bytes.as_slice());
            let raw = Arc::into_raw(shared);
            cell.owner = OwnerKind::Shared as u32;
            cell.data = raw.cast::<u8>().cast_mut();
        }
        cell.len = len;
        cell.cap = len;
        cell.state = daedalus_abi::SlotState::Full as u32;
    }
}

impl OwnedStep for WorkerStep {
    fn run(&mut self, slots: &mut Slots, cx: &mut StepCx) -> StepEnd {
        let shared = self.shared.clone();
        let mut guard = shared.lock();
        let sup: &mut Supervisor = &mut guard;
        match ensure_up(&shared, sup) {
            Ok(()) => {}
            // The failure that started the backoff was reported when it happened. Until the next
            // start is due the run is skipped, with the reason: not another fault, so a host that
            // stops on faults isn't stopped by the wait, and nothing sleeps on the tick.
            Err(NotUp::BackingOff(reason)) => {
                cx.fault(None, &reason);
                return StepEnd::Skipped;
            }
            Err(NotUp::Start(reason)) => {
                return self.fail(&shared, sup, cx, &reason, Blame::Nobody);
            }
        }
        let cells = slots.as_mut_slice();
        sup.run = sup.run.wrapping_add(1);
        let run = sup.run;
        sup.writer.buf.clear();
        let sent = match self.encode_run(run, cells, &mut sup.writer) {
            Ok(sent) => sent,
            Err(reason) => return self.fail(&shared, sup, cx, &reason, Blame::Nobody),
        };
        shared.telemetry.bump(Count::BytesIn(sent as u64));
        shared.telemetry.bump(Count::Run);
        sup.fault = None;

        let Link::Up(worker) = &mut sup.link else {
            return self.fail(&shared, sup, cx, "worker is not running", Blame::Worker);
        };
        if let Err(error) = worker.send(&sup.writer.buf) {
            return self.fail(
                &shared,
                sup,
                cx,
                &format!("could not send the run: {error}"),
                Blame::Worker,
            );
        }
        let outcome = self.wait(&shared, sup, run, cx);
        match outcome {
            Outcome::Replied(RunStatus::Ok) => {
                sup.failures = 0;
                // An output the worker did not send stays an empty cell, which the runner reads as
                // missing.
                for index in 0..self.writes.len() {
                    if self.produced[index] {
                        let cell = &mut cells[self.reads.len() + index];
                        Self::publish(cell, &mut self.outbufs[index]);
                    }
                }
                StepEnd::Done
            }
            Outcome::Replied(RunStatus::Cancelled) => {
                let reason = sup
                    .fault
                    .clone()
                    .unwrap_or_else(|| "the run was cancelled".into());
                shared.telemetry.bump(Count::Fault);
                cx.fault(None, &reason);
                StepEnd::Fault
            }
            Outcome::Replied(RunStatus::Fault) => {
                let reason = sup.fault.clone().unwrap_or_else(|| "a node faulted".into());
                shared.telemetry.bump(Count::Fault);
                cx.fault(None, &reason);
                StepEnd::Fault
            }
            Outcome::Gone(reason) => {
                let text = format!("worker exited mid-run: {reason}");
                self.fail(&shared, sup, cx, &text, Blame::Worker)
            }
            Outcome::Killed(pid) => {
                sup.killed.push(pid);
                let text = format!(
                    "deadline passed; the worker did not answer the cancel, and was killed (pid {pid})"
                );
                self.fail(&shared, sup, cx, &text, Blame::Host)
            }
        }
    }
}

impl WorkerStep {
    /// Reads packets until this run's outputs arrive, or the worker is gone, or killed.
    fn wait(
        &mut self,
        shared: &Shared,
        sup: &mut Supervisor,
        run: u64,
        cx: &mut StepCx,
    ) -> Outcome {
        let mut deadline = shared.spec.deadline.map(|limit| Instant::now() + limit);
        let mut cancelled = false;
        loop {
            // The wait wakes at least this often, so a cancel the host asks for is sent promptly.
            let poll = Duration::from_millis(5);
            let timeout = Some(match deadline {
                Some(at) => at.saturating_duration_since(Instant::now()).min(poll),
                None => poll,
            });
            let Link::Up(worker) = &mut sup.link else {
                return Outcome::Gone("the link is down".into());
            };
            match worker.receive(timeout) {
                Got::Packet(buf) => {
                    let kind = buf.first().copied();
                    let run_of = buf
                        .get(1..9)
                        .map(|raw| u64::from_le_bytes(raw.try_into().unwrap_or([0; 8])));
                    let outcome = match kind {
                        // Outputs from an earlier run that was cancelled or killed are stale.
                        Some(daedalus_wire::kind::OUTPUTS) if run_of != Some(run) => None,
                        Some(daedalus_wire::kind::OUTPUTS) => {
                            let schema = &shared.schema;
                            match self.read_outputs(&buf, schema) {
                                Ok(status) => {
                                    shared.telemetry.bump(Count::BytesOut(buf.len() as u64));
                                    Some(Outcome::Replied(status))
                                }
                                Err(error) => Some(Outcome::Gone(format!(
                                    "a malformed outputs packet: {error}"
                                ))),
                            }
                        }
                        _ => {
                            self.note(shared, sup, &buf, run, cx);
                            None
                        }
                    };
                    let Link::Up(worker) = &sup.link else {
                        return Outcome::Gone("the link is down".into());
                    };
                    worker.give_back(buf);
                    if let Some(outcome) = outcome {
                        return outcome;
                    }
                }
                Got::Broken(error) => return Outcome::Gone(error.to_string()),
                Got::Timeout => {
                    let due = deadline.is_some_and(|at| Instant::now() >= at);
                    if !due {
                        // Nothing is due. A cancel the host asked for is sent to the worker now.
                        if !cancelled && cx.cancelled() {
                            cancelled = true;
                            let Link::Up(worker) = &mut sup.link else {
                                return Outcome::Gone("the link is down".into());
                            };
                            if let Err(error) = worker.send(&Worker::cancel_body(run)) {
                                return Outcome::Gone(format!(
                                    "could not send the cancel: {error}"
                                ));
                            }
                            deadline = Some(Instant::now() + shared.spec.grace);
                        }
                        continue;
                    }
                    if cancelled {
                        let Link::Up(worker) = &mut sup.link else {
                            return Outcome::Gone("the link is down".into());
                        };
                        let pid = worker.pid();
                        worker.kill();
                        return Outcome::Killed(pid);
                    }
                    cancelled = true;
                    let Link::Up(worker) = &mut sup.link else {
                        return Outcome::Gone("the link is down".into());
                    };
                    if let Err(error) = worker.send(&Worker::cancel_body(run)) {
                        return Outcome::Gone(format!("could not send the cancel: {error}"));
                    }
                    deadline = Some(Instant::now() + shared.spec.grace);
                }
            }
        }
    }

    /// Handles a packet other than outputs: a node's fault is recorded for the report, progress
    /// is passed on. Anything else is ignored.
    fn note(&self, shared: &Shared, sup: &mut Supervisor, buf: &[u8], run: u64, cx: &mut StepCx) {
        match WorkerMsg::decode(buf, &shared.schema) {
            Ok(WorkerMsg::Fault(fault)) if fault.run == run => {
                let at = fault.node.as_ref().and_then(|node| {
                    self.region
                        .iter()
                        .position(|region| shared.nodes[*region as usize].id == *node)
                });
                let text = format!("{}: {}", fault.code.name(), fault.reason);
                if let Some(position) = at {
                    cx.fault(Some(position as u32), &text);
                } else {
                    cx.fault(None, &text);
                }
                sup.fault = Some(text);
            }
            Ok(WorkerMsg::Progress(progress)) if progress.run == run => {
                if let Some(fraction) = progress.fraction {
                    let done = (fraction.clamp(0.0, 1.0) * 1000.0) as u32;
                    cx.progress(done, 1000);
                }
            }
            _ => {}
        }
    }

    /// Ends the step as a fault. A worker that failed, or that the host stopped, is killed and its
    /// link goes down: after a failure until the backoff has passed, after a stop until the next
    /// run.
    fn fail(
        &mut self,
        shared: &Shared,
        sup: &mut Supervisor,
        cx: &mut StepCx,
        reason: &str,
        blame: Blame,
    ) -> StepEnd {
        shared.telemetry.bump(Count::Fault);
        let mut text = reason.to_owned();
        if blame != Blame::Nobody {
            let until = if blame == Blame::Worker {
                sup.failures = sup.failures.saturating_add(1);
                Instant::now() + delay_after(sup.failures)
            } else {
                Instant::now()
            };
            if let Link::Up(mut worker) = std::mem::replace(&mut sup.link, Link::Down { until }) {
                let tail = worker.stderr_tail(STDERR_REPORT);
                if !tail.is_empty() {
                    text.push_str("; stderr: ");
                    text.push_str(tail.trim_end());
                }
                worker.kill();
            }
        }
        cx.fault(None, &text);
        StepEnd::Fault
    }
}

/// Writes one input's bytes as its port type's wire form. Scalars are their own bytes, strings and
/// byte fields are length-prefixed, and a compound value is already its wire form.
fn put_raw(ty: &WireType, bytes: &[u8], out: &mut Writer) -> Result<(), String> {
    match ty {
        WireType::Scalar(scalar) => match fixed_size(*scalar) {
            Some(size) if bytes.len() == size => {
                out.buf.extend_from_slice(bytes);
                Ok(())
            }
            Some(size) => Err(format!(
                "a {scalar:?} input is {} bytes, not {size}",
                bytes.len()
            )),
            None => out.bytes(bytes).map_err(|error| error.to_string()),
        },
        WireType::Opaque(_) => out.bytes(bytes).map_err(|error| error.to_string()),
        _ => {
            out.buf.extend_from_slice(bytes);
            Ok(())
        }
    }
}

/// Reads one output's bytes from the wire into `out`, in the slot form `put_raw` writes back.
fn read_raw(
    ty: &WireType,
    input: &mut Reader<'_>,
    schema: &Schema,
    out: &mut Vec<u8>,
) -> Result<(), WireError> {
    match ty {
        WireType::Scalar(scalar) => match fixed_size(*scalar) {
            Some(size) => out.extend_from_slice(input.take(size)?),
            None => {
                let len = input.count()?;
                out.extend_from_slice(input.take(len)?);
            }
        },
        WireType::Opaque(_) => {
            let len = input.count()?;
            out.extend_from_slice(input.take(len)?);
        }
        _ => {
            let from = input.pos();
            decode_value(ty, input, schema)?;
            out.extend_from_slice(input.span(from));
        }
    }
    Ok(())
}

/// The byte width of a scalar with a fixed width. Strings and byte fields have none.
fn fixed_size(scalar: daedalus_types::Scalar) -> Option<usize> {
    use daedalus_types::Scalar;
    Some(match scalar {
        Scalar::Unit => 0,
        Scalar::Bool | Scalar::I8 | Scalar::U8 => 1,
        Scalar::I16 | Scalar::U16 => 2,
        Scalar::I32 | Scalar::U32 | Scalar::F32 => 4,
        Scalar::Int | Scalar::ISize | Scalar::U64 | Scalar::USize | Scalar::Float => 8,
        Scalar::String | Scalar::Bytes => return None,
    })
}
