//! A `#[node]`'s step as a plugin builds it. A plugin carries this backend and compiles its own
//! bodies, so the step is the one a host would build, with one difference: every input is read
//! and every output written in its type's crossing form, since that is what the cells of the
//! plugin's space hold.

use alloc::collections::BTreeMap;
use alloc::format;
use alloc::string::{String, ToString};

use daedalus_abi::Step;
use daedalus_backend::{BodyIndex, CompileCx, NodeBody};
use daedalus_registry::NodeRecord;
use daedalus_types::NodeIndex;

use crate::backend::{Wiring, has_port, ports, step_of};
use crate::steps::CpuBody;
use crate::steps::live::Readers;

/// Builds the step of `record` for the node at `node`, with every port in its crossing form.
/// Refused, in words, for a body that is not a CPU function, a node with a fan-in, and a port
/// whose type has no crossing form.
pub fn boundary_step(record: &NodeRecord, node: u32) -> Result<Step, String> {
    let id = record.decl.id;
    let Some(cpu) = record.body.code.downcast_ref::<CpuBody>() else {
        return Err(format!("{id} has no CPU function for a plugin to build"));
    };
    if !record.decl.fanin.is_empty() {
        return Err(format!(
            "{id} takes a fan-in, which a plugin's node cannot yet"
        ));
    }
    let inputs = cpu.shape.params.iter().filter(|param| has_port(param.role));
    let (inputs, outputs) = (inputs.count() as u16, record.decl.outputs.len() as u16);
    let index = NodeIndex(node);
    let wiring = Wiring {
        outputs: ports(index, outputs),
        inputs: ports(index, inputs),
    };
    let metadata = BTreeMap::new();
    let body = NodeBody {
        node: index,
        id,
        body: BodyIndex(0),
        inputs,
        outputs,
        merges: false,
        fire_all: false,
        metrics: record.decl.metrics.len() as u16,
        label: None,
        metadata: &metadata,
        code: record.body.code,
        text: record.body.text,
    };
    let cx = CompileCx {
        types: &[],
        consts: &[],
        spaces: &[],
        bodies: &[],
        edges: &[],
        host_inputs: &[],
        host_outputs: &[],
        fifo_capacity: 0,
        merge_allowed: false,
        freeze_params: false,
        sharing: &[],
        lent: &[],
        offered: &[],
    };
    let step = step_of(index, &body, &cx, &wiring, &Readers::default());
    step.map(|(step, _)| step)
        .map_err(|refusal| refusal.reason.to_string())
}
