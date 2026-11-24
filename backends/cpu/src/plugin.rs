//! A `#[node]`'s step as a plugin builds it. A plugin carries this backend and compiles its own
//! bodies, so the step is the one a host would build, with one difference: an input or output on
//! an edge that crosses the plugin's boundary is read and written in its type's crossing form.
//! An edge between two of the plugin's own nodes, whose value nothing else reads, carries the
//! value as it is: both ends are this build's code.

use alloc::collections::BTreeMap;
use alloc::format;
use alloc::string::{String, ToString};
use alloc::vec::Vec;

use daedalus_abi::Step;
use daedalus_backend::{BodyIndex, CompileCx, ConstOf, NodeBody, PortRef};
use daedalus_graph::Lit;
use daedalus_registry::NodeRecord;
use daedalus_types::NodeIndex;

use crate::backend::{Wiring, has_port, ports, step_of};
use crate::steps::CpuBody;
use crate::steps::live::Readers;

/// Where a plugin's node is in its region: the constants the graph gives its inputs, by port (a
/// config is the one named `config`), and its ports on edges to and from other nodes of the
/// plugin that nothing else reads, by index.
#[derive(Clone, Copy, Debug, Default)]
pub struct Place<'a> {
    pub consts: &'a [(&'a str, Lit)],
    pub inner_inputs: &'a [u16],
    pub inner_outputs: &'a [u16],
}

/// Builds the step of `record` for the node at `node`. A port on an edge that crosses the
/// plugin's boundary is in its crossing form; one in `place`'s inner lists carries the value as
/// it is. Refused, in words, for a body that is not a CPU function, a node with a fan-in, a port
/// on the boundary whose type has no crossing form, and a constant that does not fit its port.
pub fn boundary_step(record: &NodeRecord, node: u32, place: &Place<'_>) -> Result<Step, String> {
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
    let inputs_across: Vec<PortRef> = ports(index, inputs)
        .into_iter()
        .filter(|at| !place.inner_inputs.contains(&at.port.0))
        .collect();
    let (outputs_across, carried): (Vec<PortRef>, Vec<PortRef>) = ports(index, outputs)
        .into_iter()
        .partition(|at| !place.inner_outputs.contains(&at.port.0));
    let wiring = Wiring {
        outputs: outputs_across,
        inputs: inputs_across,
        carried,
        plugin: true,
    };
    let consts: Vec<ConstOf<'_>> = place
        .consts
        .iter()
        .map(|(port, value)| ConstOf {
            node: index,
            port,
            value,
        })
        .collect();
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
        consts: &consts,
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
