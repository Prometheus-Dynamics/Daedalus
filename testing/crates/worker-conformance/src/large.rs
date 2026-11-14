//! Large values through a worker: `fixture.raw_io` with a value well past the shared-memory
//! threshold, run several times, each answer checked. What the host's counters say about how the
//! values travelled is given back, so each language's worker can show it takes values by
//! reference: inputs as places, outputs held where the worker wrote them.

use daedalus_abi::testing::{release, run_once};
use daedalus_abi::{OWNER_HANDED, Slot, SlotState, Slots, StepCx, StepEnd};
use daedalus_backend::{
    Backend, BodyIndex, CompileCx, Compiled, NO_METADATA, NodeBody, PortRef, RateId, Region,
};
use daedalus_backend_process::{ProcessBackend, Snapshot, WorkerSpec};
use daedalus_types::{BackendIndex, NodeIndex, PortIndex};

use crate::runner::registry;

/// What a run of large values showed.
#[derive(Clone, Debug)]
pub struct Large {
    /// The host's counters after the runs.
    pub telemetry: Snapshot,
    /// How many outputs the host's cell held in the worker's value buffer, not as its own copy.
    pub held: usize,
}

/// Runs `fixture.raw_io` `runs` times with `len` bytes through a worker started from `spec`, and
/// checks each answer is the value reversed. Each output cell is let go before the next run.
pub fn large_values(spec: WorkerSpec, len: usize, runs: usize) -> Result<Large, String> {
    let mut backend = ProcessBackend::new();
    backend
        .add_worker(spec, &registry())
        .map_err(|error| format!("the worker registers: {error}"))?;
    let ports = [PortRef {
        node: NodeIndex(0),
        port: PortIndex(0),
    }];
    let nodes = [NodeIndex(0)];
    let region = Region {
        backend: BackendIndex(0),
        nodes: &nodes,
        inputs: &ports,
        outputs: &ports,
        incoming: &[],
        placed: &[],
        rate: RateId(0),
    };
    let bodies = [NodeBody {
        node: NodeIndex(0),
        id: "fixture.raw_io",
        body: BodyIndex(0),
        inputs: 1,
        outputs: 1,
        merges: false,
        fire_all: false,
        label: None,
        metadata: &NO_METADATA,
        code: &(),
        text: "fixture.raw_io",
    }];
    let cx = CompileCx {
        types: &[],
        consts: &[],
        spaces: &[],
        bodies: &bodies,
        edges: &[],
        host_inputs: &[],
        host_outputs: &[],
        fifo_capacity: 64,
        merge_allowed: false,
        freeze_params: false,
        sharing: &[],
    };
    let mut step = match backend.compile(&region, &cx) {
        Ok(Compiled::Steps(mut units)) => units.pop().ok_or("no step")?.step,
        Ok(_) => return Err("the backend gave a form other than steps".into()),
        Err(refusal) => return Err(format!("refused: {}", refusal.reason)),
    };
    let mut payload: Vec<u8> = (0..len).map(|at| (at * 31 + 7) as u8).collect();
    let mut held = 0;
    let mut result = Ok(());
    for run in 0..runs {
        let mut cells = [
            Slot {
                state: SlotState::Full as u32,
                owner: 0,
                data: payload.as_mut_ptr(),
                len,
                cap: len,
                ..Slot::EMPTY
            },
            Slot::EMPTY,
        ];
        let mut cx = StepCx::new();
        let end = run_once(&mut step, &mut Slots::of(&mut cells), &mut cx);
        let out = std::mem::replace(&mut cells[1], Slot::EMPTY);
        if !matches!(end, StepEnd::Done) {
            result = Err(format!("run {run} did not finish: {}", cx.reason()));
        } else if !out
            .bytes()
            .is_some_and(|back| back.len() == len && back.iter().rev().eq(payload.iter()))
        {
            result = Err(format!("run {run} did not give the value reversed"));
        }
        held += usize::from(out.owner == OWNER_HANDED);
        daedalus_runner::drop_owned(out);
        if result.is_err() {
            break;
        }
    }
    release(step);
    result.map(|()| Large {
        telemetry: backend.telemetry(),
        held,
    })
}
