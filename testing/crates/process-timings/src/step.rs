//! A one-node region compiled to a step, and a cell full of a value's bytes.

use daedalus_abi::{Slot, SlotState, Step};
use daedalus_backend::{
    Backend, BodyIndex, CompileCx, Compiled, NO_METADATA, NodeBody, PortRef, RateId, Region,
};
use daedalus_backend_process::ProcessBackend;
use daedalus_types::{BackendIndex, NodeIndex, PortIndex};

/// Node `id`, alone in a region whose inputs and outputs are its own `inputs` and `outputs`.
pub fn one_node(backend: &ProcessBackend, id: &'static str, inputs: u16, outputs: u16) -> Step {
    let port = |port| PortRef {
        node: NodeIndex(0),
        port: PortIndex(port),
    };
    let ins: Vec<PortRef> = (0..inputs).map(port).collect();
    let outs: Vec<PortRef> = (0..outputs).map(port).collect();
    let nodes = [NodeIndex(0)];
    let region = Region {
        backend: BackendIndex(0),
        nodes: &nodes,
        inputs: &ins,
        outputs: &outs,
        incoming: &[],
        placed: &[],
        rate: RateId(0),
    };
    let bodies = [NodeBody {
        node: NodeIndex(0),
        id,
        body: BodyIndex(0),
        inputs,
        outputs,
        merges: false,
        fire_all: false,
        label: None,
        metadata: &NO_METADATA,
        code: &(),
        text: id,
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
    match backend.compile(&region, &cx) {
        Ok(Compiled::Steps(mut units)) => units.pop().expect("a unit").step,
        other => panic!("compile {id}: {other:?}"),
    }
}

/// A cell holding `bytes`, which the caller keeps alive while the cell is used.
pub fn full(bytes: &mut [u8]) -> Slot {
    Slot {
        state: SlotState::Full as u32,
        owner: 0,
        data: bytes.as_mut_ptr(),
        len: bytes.len(),
        cap: bytes.len(),
        ..Slot::EMPTY
    }
}
