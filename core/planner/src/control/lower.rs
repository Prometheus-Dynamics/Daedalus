//! Planning the bodies, and the step that runs them.

use std::collections::BTreeMap;
use std::sync::{Arc, Mutex};

use daedalus_abi::{OwnedStep, Slot, Slots, Step, StepCx, StepEnd};
use daedalus_backend::{
    Backend, CompileCx, Compiled, MemorySpace, NodeRef, Offer, PortRef, Refusal, Region, SpaceKind,
    StepUnit, Transfer, UnitInfo,
};
use daedalus_graph::{ControlOp, NodeKindDoc};
use daedalus_registry::Registry;
use daedalus_runner::{
    INLINE_BYTES, Nested, NestedStep, SlotList, drop_owned, place, put, put_typed, read_bool,
    typed_view,
};
use daedalus_types::{PortIndex, SpaceIndex, TypeIndex};

use super::ControlError;
use super::kinds::{BODY, ELSE, THEN, closed};
use crate::api::{PlanError, Planned, plan};
use crate::config::Config;
use crate::form::Form;
use crate::solve::Graph;
use crate::stitch::Item;

/// The id of the built-in backend that holds control nodes.
pub const CONTROL_BACKEND: &str = "daedalus.control";
const HOST_SPACE: &str = "daedalus.control.host";

/// One body of a control node, as planned.
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct NestedBody {
    pub node: u32,
    pub body: String,
    /// Per step, the nodes of the body it covers.
    pub covers: Vec<Vec<u32>>,
}

struct Body {
    nested: Nested,
    /// Port name to cell, in the order `Nested` takes its views.
    inputs: Vec<(String, u32)>,
    outputs: BTreeMap<String, u32>,
}

struct Shared {
    id: String,
    op: ControlOp,
    max: u32,
    /// The node's port names, in frame order.
    inputs: Vec<String>,
    outputs: Vec<String>,
    bodies: Mutex<BTreeMap<String, Body>>,
    covers: Vec<NestedBody>,
}

fn take_steps(
    planned: &mut Planned,
    id: &str,
    name: &str,
) -> Result<(Vec<NestedStep>, Vec<Vec<u32>>), PlanError> {
    let mut units: Vec<Vec<Option<StepUnit>>> = Vec::new();
    for region in &mut planned.regions.regions {
        let compiled = std::mem::replace(&mut region.compiled, Compiled::Steps(Vec::new()));
        let Compiled::Steps(list) = compiled else {
            return Err(PlanError::FormUnavailable {
                form: Form::Steps,
                backend: id.to_string(),
            });
        };
        units.push(list.into_iter().map(Some).collect());
    }
    let waits = || {
        PlanError::Solve(vec![crate::pipeline::SolveError::Control(
            ControlError::BodyWaits {
                node: id.to_string(),
                body: name.to_string(),
            },
        )])
    };
    let (mut steps, mut covers) = (Vec::new(), Vec::new());
    let layout = std::mem::take(&mut planned.stitched.layout);
    for item in layout {
        match item {
            Item::Run {
                unit,
                covers: nodes,
                reads,
                writes,
            } => {
                let Some(found) = units[unit.region as usize][unit.unit as usize].take() else {
                    continue;
                };
                if found.info.may_pend {
                    found.step.discard();
                    return Err(waits());
                }
                steps.push(NestedStep {
                    step: found.step,
                    reads,
                    writes,
                });
                covers.push(nodes);
            }
            Item::Move {
                from_slot,
                to_slot,
                transfer,
            } => {
                let Some(step) = planned.transfer_steps[transfer as usize].take() else {
                    return Err(PlanError::TransferUnavailable {
                        id: id.to_string(),
                        reason: "a body's transfer has no step of its own".to_string(),
                    });
                };
                steps.push(NestedStep {
                    step,
                    reads: vec![from_slot],
                    writes: vec![to_slot],
                });
            }
            Item::Wait { .. } => return Err(waits()),
        }
    }
    Ok((steps, covers))
}

fn host_slots(ports: &[crate::stitch::HostPort]) -> BTreeMap<String, u32> {
    ports
        .iter()
        .map(|port| (port.port.clone(), port.slot))
        .collect()
}

/// The built-in backend control nodes run on. It holds their planned bodies.
pub struct ControlBackend {
    nodes: BTreeMap<u32, Arc<Shared>>,
    host: std::borrow::Cow<'static, str>,
}

fn nested_config(config: &Config) -> Config {
    Config {
        form: Form::Steps,
        ..config.clone()
    }
}

fn prepare_body(
    id: &str,
    name: &str,
    node: u32,
    body: &daedalus_graph::GraphBody,
    registry: &Registry,
    backends: &[&dyn Backend],
    config: &Config,
) -> Result<(Body, NestedBody), PlanError> {
    let wrap = |error| PlanError::Control {
        node: id.to_string(),
        body: name.to_string(),
        error: Box::new(error),
    };
    let mut planned = plan(&closed(body), registry, backends, config).map_err(wrap)?;
    let (steps, covers) = take_steps(&mut planned, id, name).map_err(wrap)?;
    let inputs: Vec<(String, u32)> = host_slots(&planned.stitched.host_inputs)
        .into_iter()
        .collect();
    let views = inputs.iter().map(|(_, cell)| *cell).collect();
    let cells = planned.stitched.slots.len();
    let outputs = host_slots(&planned.stitched.host_outputs);
    let nested = Nested::new(steps, cells, views);
    let record = NestedBody {
        node,
        body: name.to_string(),
        covers,
    };
    Ok((
        Body {
            nested,
            inputs,
            outputs,
        },
        record,
    ))
}

impl ControlBackend {
    /// Plans every body. `None` if the graph has no control nodes.
    pub(crate) fn prepare(
        graph: &Graph,
        registry: &Registry,
        backends: &[&dyn Backend],
        config: &Config,
    ) -> Result<Option<ControlBackend>, PlanError> {
        let config = nested_config(config);
        let mut nodes = BTreeMap::new();
        for (index, node) in graph.nodes.iter().enumerate() {
            let Some(control) = node
                .doc
                .control
                .as_ref()
                .filter(|_| node.doc.kind == NodeKindDoc::Control)
            else {
                continue;
            };
            let id = node.doc.id.clone();
            let (mut bodies, mut covers) = (BTreeMap::new(), Vec::new());
            for (name, body) in &control.bodies {
                let (body, record) =
                    prepare_body(&id, name, index as u32, body, registry, backends, &config)?;
                bodies.insert(name.clone(), body);
                covers.push(record);
            }
            nodes.insert(
                index as u32,
                Arc::new(Shared {
                    id,
                    op: control.op,
                    max: control.max.unwrap_or(0),
                    inputs: node.inputs.iter().map(|port| port.name.clone()).collect(),
                    outputs: node.outputs.iter().map(|port| port.name.clone()).collect(),
                    bodies: Mutex::new(bodies),
                    covers,
                }),
            );
        }
        // Sharing another backend's host memory means values reach a control node with no transfer.
        let host = backends
            .iter()
            .flat_map(|backend| backend.memory())
            .find(|space| space.kind == SpaceKind::Host)
            .map_or(HOST_SPACE.into(), |space| space.id);
        Ok((!nodes.is_empty()).then_some(ControlBackend { nodes, host }))
    }

    /// What each control node runs.
    pub(crate) fn records(&self) -> Vec<NestedBody> {
        self.nodes
            .values()
            .flat_map(|shared| shared.covers.iter().cloned())
            .collect()
    }
}

impl Backend for ControlBackend {
    fn id(&self) -> &str {
        CONTROL_BACKEND
    }

    fn memory(&self) -> Vec<MemorySpace> {
        vec![MemorySpace {
            id: self.host.clone(),
            kind: SpaceKind::Host,
            readable: true,
        }]
    }

    fn transfers(&self) -> Vec<Transfer> {
        Vec::new()
    }

    /// Control nodes aren't offered: the planner places them here directly.
    fn can_take(&self, _: &Offer<'_>) -> Vec<NodeRef> {
        Vec::new()
    }

    fn compile(&self, region: &Region<'_>, _: &CompileCx<'_>) -> Result<Compiled, Refusal> {
        let [node] = region.nodes else {
            return Err(Refusal::whole_region(
                "a control node is a region of its own",
            ));
        };
        let Some(shared) = self.nodes.get(&node.0) else {
            return Err(Refusal::whole_region("not a control node"));
        };
        let step = Step::boxed(ControlStep {
            shared: Arc::clone(shared),
        });
        let ports = |count: usize| -> Vec<PortRef> {
            (0..count)
                .map(|port| PortRef {
                    node: *node,
                    port: PortIndex(port as u16),
                })
                .collect()
        };
        let info = UnitInfo {
            covers: vec![*node],
            reads: ports(shared.inputs.len()),
            writes: ports(shared.outputs.len()),
            absorbed: Vec::new(),
            may_pend: false,
        };
        Ok(Compiled::Steps(vec![StepUnit { step, info }]))
    }
}

struct ControlStep {
    shared: Arc<Shared>,
}

fn drop_held(held: Option<Slot>) {
    if let Some(slot) = held {
        drop_owned(slot);
    }
}

fn lend(body: &Body, shared: &Shared, slots: &mut Slots) -> Vec<Slot> {
    body.inputs
        .iter()
        .map(|(name, _)| shared.input(slots, name))
        .collect()
}

fn finished(end: StepEnd, cx: &mut StepCx, id: &str) -> Option<StepEnd> {
    match end {
        StepEnd::Done | StepEnd::Skipped => None,
        StepEnd::Fault => Some(StepEnd::Fault),
        StepEnd::Pending(_) => {
            cx.fault(
                None,
                &format!("a step in control node `{id}` tried to wait"),
            );
            Some(StepEnd::Fault)
        }
    }
}

impl Shared {
    fn input(&self, slots: &mut Slots, name: &str) -> Slot {
        let at = self.inputs.iter().position(|port| port == name);
        at.and_then(|at| slots.cell(at).map(|cell| *cell))
            .unwrap_or(Slot::EMPTY)
    }

    /// The output cell for `name`. It sits after the inputs in the frame.
    fn output<'s>(&self, slots: &'s mut Slots, name: &str) -> Option<&'s mut Slot> {
        let at = self.outputs.iter().position(|port| port == name)?;
        slots.cell(self.inputs.len() + at)
    }
}

// The runner stamps a published value with its slot's real type and space.
const UNSTAMPED: (TypeIndex, SpaceIndex) = (TypeIndex(0), SpaceIndex(0));

impl ControlStep {
    fn run_if(
        &self,
        bodies: &mut BTreeMap<String, Body>,
        slots: &mut Slots,
        cx: &mut StepCx,
    ) -> StepEnd {
        let shared = &*self.shared;
        let Some(cond) = read_bool(&shared.input(slots, "cond")) else {
            return StepEnd::Skipped;
        };
        let Some(body) = bodies.get_mut(if cond { THEN } else { ELSE }) else {
            return StepEnd::Skipped;
        };
        let inputs = lend(body, shared, slots);
        let end = body.nested.run(&inputs, cx);
        if let Some(end) = finished(end, cx, &shared.id) {
            body.nested.reset();
            return end;
        }
        for name in &shared.outputs {
            let taken = body
                .outputs
                .get(name)
                .and_then(|&inner| body.nested.take(inner));
            if let (Some(taken), Some(out)) = (taken, shared.output(slots, name)) {
                put(out, taken);
            }
        }
        body.nested.reset();
        StepEnd::Done
    }

    fn run_for_each(
        &self,
        bodies: &mut BTreeMap<String, Body>,
        slots: &mut Slots,
        cx: &mut StepCx,
    ) -> StepEnd {
        let shared = &*self.shared;
        let source = shared.input(slots, "list");
        let (Some(list), Some(body)) = (typed_view::<SlotList>(&source), bodies.get_mut(BODY))
        else {
            return StepEnd::Skipped;
        };
        let inner = body.outputs.get("out").copied();
        let mut collected = SlotList::with_capacity(list.len());
        for at in 0..list.len() {
            if cx.cancelled() {
                return StepEnd::Skipped;
            }
            let element = list.get(at).copied().unwrap_or(Slot::EMPTY);
            let end = body.nested.run(&[element], cx);
            if let Some(end) = finished(end, cx, &shared.id) {
                body.nested.reset();
                return end;
            }
            if let Some(value) = inner.and_then(|inner| body.nested.take(inner)) {
                collected.push(value);
            }
            body.nested.reset();
            cx.progress(at as u32 + 1, list.len() as u32);
        }
        if let Some(cell) = shared.output(slots, "out") {
            put_typed(cell, UNSTAMPED.0, UNSTAMPED.1, Arc::new(collected));
        }
        StepEnd::Done
    }

    fn run_until(
        &self,
        bodies: &mut BTreeMap<String, Body>,
        slots: &mut Slots,
        cx: &mut StepCx,
    ) -> StepEnd {
        let shared = &*self.shared;
        let init = shared.input(slots, "init");
        let Some(body) = bodies.get_mut(BODY) else {
            return StepEnd::Skipped;
        };
        if init.state != daedalus_abi::SlotState::Full as u32 {
            return StepEnd::Skipped;
        }
        let (state, done) = (
            body.outputs.get("state").copied(),
            body.outputs.get("done").copied(),
        );
        let mut held: Option<Slot> = None;
        let mut buffers = [[0_u8; INLINE_BYTES]; 2];
        for round in 0..shared.max {
            if cx.cancelled() {
                drop_held(held);
                return StepEnd::Skipped;
            }
            let end = body.nested.run(&[held.unwrap_or(init)], cx);
            if let Some(end) = finished(end, cx, &shared.id) {
                body.nested.reset();
                drop_held(held);
                return end;
            }
            let flag = done.and_then(|cell| body.nested.take(cell));
            let finish = flag.as_ref().and_then(read_bool);
            drop_held(flag);
            let Some(next) = state.and_then(|cell| body.nested.take(cell)) else {
                body.nested.reset();
                drop_held(held);
                cx.fault(None, &format!("the body of `{}` wrote no state", shared.id));
                return StepEnd::Fault;
            };
            let next = place(next, &mut buffers[round as usize % 2]);
            drop_held(held.replace(next));
            body.nested.reset();
            if finish == Some(true) {
                if let (Some(value), Some(cell)) = (held.take(), shared.output(slots, "state")) {
                    put(cell, value);
                }
                return StepEnd::Done;
            }
        }
        drop_held(held);
        cx.fault(
            None,
            &format!("`{}` reached its limit of {} rounds", shared.id, shared.max),
        );
        StepEnd::Fault
    }
}

impl OwnedStep for ControlStep {
    fn run(&mut self, slots: &mut Slots, cx: &mut StepCx) -> StepEnd {
        let mut bodies = self.shared.bodies.lock().unwrap_or_else(|e| e.into_inner());
        match self.shared.op {
            ControlOp::If => self.run_if(&mut bodies, slots, cx),
            ControlOp::ForEach => self.run_for_each(&mut bodies, slots, cx),
            ControlOp::Until => self.run_until(&mut bodies, slots, cx),
        }
    }
}
