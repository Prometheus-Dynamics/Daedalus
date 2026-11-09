//! Turns a finished plan into a running host: takes the steps out, wires the wake lists, sizes every
//! buffer once.

#![cfg(feature = "std")]

use std::collections::{BTreeMap, HashMap};
use std::fmt;
use std::mem;
use std::string::String;
use std::vec;
use std::vec::Vec;

use daedalus_abi::Step;
use daedalus_backend::{Compiled, StepUnit, UnitInfo};
use daedalus_graph::{EdgePolicyDoc, NodeDoc, NodeKindDoc};
use daedalus_plan::Span;
use daedalus_planner::Planned;
use daedalus_planner::stitch::Item;
use daedalus_planner::stitch::{HostPort, Stitched};
use daedalus_runner::owned_run;
use daedalus_runner::{Fire, Recovery, SlotKind, SlotSpec, StepMeta};
use daedalus_types::{SpaceIndex, TypeExpr, TypeIndex, TypeKey};
use daedalus_value::{Overflow, Pressure};

use crate::intern::{intern, intern_key, intern_keys, intern_ports, intern_wake};
use crate::{Dir, Host, PortSpec, Ports, StateStore};

/// What can be set when a plan loads.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub struct LoadOptions {
    /// The capacity of a FIFO edge that declares none.
    pub fifo_capacity: u32,
}

impl Default for LoadOptions {
    fn default() -> Self {
        LoadOptions { fifo_capacity: 64 }
    }
}

/// Why a plan didn't load. The running graph is not touched.
#[derive(Debug)]
#[non_exhaustive]
pub enum LoadError {
    /// The plan isn't in the steps form, so it has nothing to run in this process.
    NotSteps,
    /// The plan has a transfer with no step, which only a binding loader can run.
    TransferUnbound { id: String },
    /// Two host ports share a name, so the host couldn't tell them apart.
    DuplicatePort { name: String },
    /// The port table is malformed.
    Ports(crate::PortsError),
    /// The runner refused the plan.
    Run(daedalus_runner::LoadError),
}

impl fmt::Display for LoadError {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            LoadError::NotSteps => write!(f, "the plan is not in the steps form"),
            LoadError::TransferUnbound { id } => write!(f, "transfer `{id}` has no step"),
            LoadError::DuplicatePort { name } => write!(f, "two host ports are named `{name}`"),
            LoadError::Ports(error) => write!(f, "the port table is malformed: {error:?}"),
            LoadError::Run(error) => write!(f, "the runner refused the plan: {error:?}"),
        }
    }
}

impl std::error::Error for LoadError {}

/// One node, as a reload compares it.
#[derive(Clone, Debug, PartialEq, Eq)]
pub(crate) struct NodeEntry {
    pub(crate) key: String,
    pub(crate) body: String,
}

fn node_key(doc: &NodeDoc, seen: &mut HashMap<String, u32>) -> String {
    if let Some(label) = &doc.label {
        return label.clone();
    }
    let count = seen.entry(doc.id.clone()).or_insert(0);
    let key = std::format!("{}#{}", doc.id, *count);
    *count += 1;
    key
}

/// The nodes of a plan with their keys and the body each runs.
pub(crate) fn node_entries(planned: &Planned) -> Vec<NodeEntry> {
    let mut backend_of = vec![None; planned.graph.nodes.len()];
    for region in &planned.regions.regions {
        for &node in &region.formed.nodes {
            backend_of[node as usize] = Some(region.formed.backend.0);
        }
    }
    let mut seen = HashMap::new();
    planned
        .graph
        .nodes
        .iter()
        .enumerate()
        .map(|(at, node)| NodeEntry {
            key: node_key(&node.doc, &mut seen),
            body: std::format!("{:?}:{}", backend_of[at], node.doc.id),
        })
        .collect()
}

/// The bridge nodes a plan's host ports belong to, for comparing two plans.
pub(crate) fn bridge_keys(planned: &Planned) -> Vec<String> {
    let mut keys: Vec<String> = planned
        .graph
        .nodes
        .iter()
        .filter(|node| node.doc.kind == NodeKindDoc::HostBridge)
        .map(|node| {
            node.doc
                .label
                .clone()
                .unwrap_or_else(|| node.doc.id.clone())
        })
        .collect();
    keys.sort_unstable();
    keys.dedup();
    keys
}

fn take_steps(planned: &mut Planned) -> Vec<Vec<Option<StepUnit>>> {
    planned
        .regions
        .regions
        .iter_mut()
        .map(|region| match &mut region.compiled {
            Compiled::Steps(units) => mem::take(units).into_iter().map(Some).collect(),
            _ => Vec::new(),
        })
        .collect()
}

struct Built {
    steps: Vec<Step>,
    meta: Vec<StepMeta>,
    table: Vec<u32>,
    slots: Vec<SlotSpec>,
    edges: EdgeRules,
}

/// The pressure each edge declared, by the slot it lands on.
type EdgeRules = BTreeMap<u32, Pressure>;

/// One step's wiring while it's being laid out.
struct Draft {
    reads: Vec<u32>,
    writes: Vec<u32>,
    may_pend: bool,
    fire: Fire,
    required: Vec<u32>,
    reuse: bool,
    always: bool,
}

fn span(table: &mut Vec<u32>, values: &[u32]) -> Span {
    let start = table.len() as u32;
    table.extend_from_slice(values);
    Span {
        start,
        len: values.len() as u32,
    }
}

/// Builds the plan's steps in layout order, with each step's slots and join rules.
fn lay_out(planned: &mut Planned, options: LoadOptions) -> Result<Built, LoadError> {
    let mut units = take_steps(planned);
    let mut transfers = mem::take(&mut planned.transfer_steps);
    let stitched = &planned.stitched;
    let mut steps = Vec::new();
    let mut drafts = Vec::new();
    let mut edges = EdgeRules::new();
    let host_spaces = host_spaces(stitched);
    for item in &stitched.layout {
        match item {
            Item::Run {
                unit,
                covers,
                reads,
                writes,
            } => {
                let Some(StepUnit { step, info }) = units[unit.region as usize]
                    .get_mut(unit.unit as usize)
                    .and_then(Option::take)
                else {
                    return Err(LoadError::NotSteps);
                };
                steps.push(step);
                let connected = connected_reads(planned, &info, reads);
                let (fire, required) = join_for(planned, covers, &connected);
                for (slot, edge) in &connected {
                    apply_edge(&mut edges, *slot, &planned.graph.edges[*edge].policy);
                }
                drafts.push(Draft {
                    reads: reads.clone(),
                    writes: writes.clone(),
                    may_pend: info.may_pend,
                    always: reads.is_empty(),
                    fire,
                    required,
                    reuse: covers.len() == 1
                        && planned.graph.nodes[covers[0] as usize].reusable
                        && writes.iter().all(|slot| {
                            host_spaces.contains(&stitched.slots[*slot as usize].space)
                        }),
                });
            }
            Item::Move {
                from_slot,
                to_slot,
                transfer,
            } => {
                let Some(step) = transfers.get_mut(*transfer as usize).and_then(Option::take)
                else {
                    let id = stitched.transfers[*transfer as usize].id.clone();
                    return Err(LoadError::TransferUnbound { id });
                };
                steps.push(step);
                drafts.push(Draft {
                    reads: vec![*from_slot],
                    writes: vec![*to_slot],
                    may_pend: false,
                    fire: Fire::Any,
                    required: Vec::new(),
                    reuse: false,
                    always: false,
                });
            }
            // A step that returned `Pending` already holds its consumers back until its fence
            // is ready, so a wait needs no step of its own.
            Item::Wait { .. } => {}
        }
    }
    planned.transfer_steps = transfers;
    let slots = slot_specs(planned, options, &edges);
    let (meta, table) = wire(&drafts);
    Ok(Built {
        steps,
        meta,
        table,
        slots,
        edges,
    })
}

/// The spaces a host port lives in. Reuse keeps an output between runs, which only a value the
/// process owns can do, so a reusable node stays reusable only when it writes into one of these.
fn host_spaces(stitched: &Stitched) -> Vec<u32> {
    stitched
        .host_inputs
        .iter()
        .chain(&stitched.host_outputs)
        .map(|port| stitched.slots[port.slot as usize].space)
        .collect()
}

/// The unit's reads that have an edge, as (slot, edge). A read with no edge has no slot, so these
/// line up with the layout's `reads`. A unit where they don't gets no join and no edge rule.
fn connected_reads(planned: &Planned, info: &UnitInfo, reads: &[u32]) -> Vec<(u32, usize)> {
    let edges = &planned.graph.edges;
    let found: Vec<usize> = info
        .reads
        .iter()
        .filter_map(|input| {
            let node = input.node.0;
            let port = planned.graph.nodes[node as usize]
                .inputs
                .get(usize::from(input.port.0))?;
            edges
                .iter()
                .position(|edge| edge.to.node == node && edge.to.port == port.name)
        })
        .collect();
    if found.len() != reads.len() {
        return Vec::new();
    }
    reads.iter().copied().zip(found).collect()
}

/// The join rule of a unit: whether a partial set waits, and the slots its nodes require.
fn join_for(planned: &Planned, covers: &[u32], connected: &[(u32, usize)]) -> (Fire, Vec<u32>) {
    let nodes = &planned.graph.nodes;
    let fire = if covers
        .iter()
        .any(|&node| nodes[node as usize].fire == daedalus_registry::Fire::All)
    {
        Fire::All
    } else {
        Fire::Any
    };
    let required = connected
        .iter()
        .filter(|(_, edge)| {
            let to = &planned.graph.edges[*edge].to;
            covers.contains(&to.node) && nodes[to.node as usize].required.contains(&to.port)
        })
        .map(|(slot, _)| *slot)
        .collect();
    (fire, required)
}

/// Records what an edge declared, for the slot it lands on. FIFO is the plain default.
fn apply_edge(rules: &mut EdgeRules, slot: u32, policy: &EdgePolicyDoc) {
    let pressure = match policy {
        EdgePolicyDoc::Fifo => return,
        EdgePolicyDoc::LatestOnly => Pressure::LatestOnly,
        EdgePolicyDoc::Bounded { capacity, overflow } => Pressure::Bounded {
            cap: (*capacity).max(1),
            overflow: match overflow.as_str() {
                "drop_newest" => Overflow::DropNewest,
                "error_on_full" => Overflow::ErrorOnFull,
                _ => Overflow::DropOldest,
            },
        },
    };
    rules.insert(slot, pressure);
}

/// Each slot's capacity and read rule. A slot with several readers keeps its value for all of
/// them, and an edge that declared a bound gets exactly that many cells.
fn slot_specs(planned: &Planned, options: LoadOptions, edges: &EdgeRules) -> Vec<SlotSpec> {
    planned
        .stitched
        .slots
        .iter()
        .enumerate()
        .map(|(at, info)| {
            if info.consumers > 1 {
                return SlotSpec {
                    cap: 1,
                    kind: SlotKind::Held,
                };
            }
            let cap = match edges.get(&(at as u32)) {
                Some(Pressure::Bounded { cap, .. }) => *cap,
                Some(Pressure::LatestOnly) => 1,
                _ => options.fifo_capacity.max(1),
            };
            SlotSpec {
                cap,
                kind: SlotKind::Fifo,
            }
        })
        .collect()
}

/// The step records, with a wake edge from each producer to the readers of what it wrote.
fn wire(drafts: &[Draft]) -> (Vec<StepMeta>, Vec<u32>) {
    let mut last_writer: HashMap<u32, usize> = HashMap::new();
    let mut wakes: Vec<Vec<u32>> = vec![Vec::new(); drafts.len()];
    for (at, draft) in drafts.iter().enumerate() {
        for slot in &draft.reads {
            if let Some(&writer) = last_writer.get(slot)
                && writer != at
            {
                wakes[writer].push(at as u32);
            }
        }
        for slot in &draft.writes {
            last_writer.insert(*slot, at);
        }
    }
    let mut table = Vec::new();
    let meta = drafts
        .iter()
        .zip(wakes)
        .map(|(draft, mut wake)| {
            wake.sort_unstable();
            wake.dedup();
            StepMeta {
                wake: span(&mut table, &wake),
                inputs: span(&mut table, &draft.reads),
                outputs: span(&mut table, &draft.writes),
                always: draft.always,
                may_pend: draft.may_pend,
                recovery: Recovery::Fail,
                fire: draft.fire,
                required: span(&mut table, &draft.required),
                reuse: draft.reuse,
            }
        })
        .collect();
    (meta, table)
}

/// The sorted port table. Interned, so loading the same graph again adds nothing.
fn port_table(
    planned: &Planned,
    meta: &[StepMeta],
    table: &[u32],
) -> Result<Ports<'static>, LoadError> {
    let stitched = &planned.stitched;
    let exprs = planned.types.exprs();
    let spec = |port: &HostPort, dir: Dir, wake: &'static [u32]| {
        let info = &stitched.slots[port.slot as usize];
        let ty = TypeIndex(info.ty.unwrap_or(0));
        let text = exprs.get(ty.0 as usize).map_or_else(String::new, type_text);
        PortSpec {
            name: intern(&port.port),
            dir,
            slot: port.slot,
            ty,
            space: SpaceIndex(info.space as u16),
            key: intern_key(if text.is_empty() { "unknown" } else { &text }),
            wake,
        }
    };
    let readers_of = |slot: u32| -> Vec<u32> {
        let inputs = |meta: &StepMeta| {
            let start = meta.inputs.start as usize;
            table[start..start + meta.inputs.len as usize].contains(&slot)
        };
        meta.iter()
            .enumerate()
            .filter(|(_, record)| inputs(record))
            .map(|(at, _)| at as u32)
            .collect()
    };
    let mut specs: Vec<PortSpec<'static>> = Vec::new();
    for port in &stitched.host_inputs {
        specs.push(spec(port, Dir::Input, intern_wake(&readers_of(port.slot))));
    }
    for port in &stitched.host_outputs {
        specs.push(spec(port, Dir::Output, &[]));
    }
    specs.sort_by(|a, b| a.name.cmp(b.name));
    if let Some(pair) = specs.windows(2).find(|pair| pair[0].name == pair[1].name) {
        return Err(LoadError::DuplicatePort {
            name: String::from(pair[0].name),
        });
    }
    Ports::new(intern_ports(specs)).map_err(LoadError::Ports)
}

/// A key for a type. Named types use their own, and a composite one gets a readable name, since
/// these keys only name a type in messages and inspection.
fn type_text(expr: &TypeExpr) -> String {
    match expr {
        TypeExpr::Scalar(scalar) => String::from(scalar.key().as_str()),
        TypeExpr::Opaque(key) => String::from(key.as_str()),
        other => std::format!("{other:?}")
            .to_lowercase()
            .chars()
            .map(|c| if c.is_ascii_alphanumeric() { c } else { '_' })
            .collect(),
    }
}

fn type_keys(planned: &Planned) -> &'static [TypeKey] {
    let texts: Vec<String> = planned.types.exprs().iter().map(type_text).collect();
    intern_keys(&texts)
}

/// A graph loaded from a plan, and everything that outlives one version of it.
pub struct LoadedHost {
    pub(crate) host: Host<'static>,
    pub(crate) state: StateStore,
    pub(crate) nodes: Vec<NodeEntry>,
    pub(crate) bridges: Vec<String>,
    pub(crate) options: LoadOptions,
}

/// Builds the host for a plan. Nothing else is touched, so a failure leaves any running graph as
/// it was.
pub(crate) fn build_host(
    planned: &mut Planned,
    options: LoadOptions,
) -> Result<(Host<'static>, Vec<NodeEntry>, Vec<String>), LoadError> {
    let nodes = node_entries(planned);
    let bridges = bridge_keys(planned);
    let built = lay_out(planned, options)?;
    let Built {
        steps,
        meta,
        table,
        slots,
        edges,
    } = built;
    let ports = port_table(planned, &meta, &table)?;
    let types = type_keys(planned);
    let host_inputs: Vec<u32> = planned
        .stitched
        .host_inputs
        .iter()
        .map(|p| p.slot)
        .collect();
    let (mut runner, buffers) =
        owned_run::build(steps, meta, table, slots, &host_inputs).map_err(LoadError::Run)?;
    for (slot, pressure) in edges {
        // A slot a kept rule can't apply to stays on the default.
        let _ = runner.set_pressure(slot, pressure);
    }
    let mut host = Host::new(runner, ports, types);
    host.owned = Some(buffers);
    Ok((host, nodes, bridges))
}

impl LoadedHost {
    /// Loads a finished plan. Slots, pools, fences and telemetry arrays are sized once, here.
    pub fn load(mut planned: Planned, options: LoadOptions) -> Result<LoadedHost, LoadError> {
        let (host, nodes, bridges) = build_host(&mut planned, options)?;
        Ok(LoadedHost {
            host,
            state: StateStore::new(),
            nodes,
            bridges,
            options,
        })
    }

    pub fn host(&mut self) -> &mut Host<'static> {
        &mut self.host
    }

    /// Asks the run to stop at the next step boundary. The word belongs to the running graph.
    pub fn cancel_word(&self) -> &daedalus_runner::CancelWord {
        self.host
            .owned
            .as_ref()
            .expect("a loaded host owns its buffers")
            .cancel()
    }

    /// Begins ticking on a worker, with every host port streamed. Stop the stream before a reload.
    pub fn start(self) -> crate::stream::StreamGraph<LoadedHost> {
        let (mut inputs, mut outputs) = (Vec::new(), Vec::new());
        for at in 0..self.host.ports.len() {
            let Some(spec) = self.host.ports.at(at) else {
                continue;
            };
            match spec.dir {
                Dir::Input => inputs.push(spec.name),
                Dir::Output => outputs.push(spec.name),
            }
        }
        crate::stream::StreamGraph::new(self, &inputs, &outputs)
    }

    pub fn state(&mut self) -> &mut StateStore {
        &mut self.state
    }

    /// The keys of the nodes this graph runs, for reports.
    pub fn node_keys(&self) -> impl Iterator<Item = &str> {
        self.nodes.iter().map(|node| node.key.as_str())
    }
}

impl crate::stream::StreamHost for LoadedHost {
    fn feed(
        &mut self,
        port: &str,
        value: daedalus_value::Value,
    ) -> Result<crate::FeedOutcome, crate::HostError> {
        self.host.feed(port, value)
    }
    fn step(&mut self) -> Result<daedalus_runner::TickReport, crate::HostError> {
        self.host.step()
    }
    fn take_value(
        &mut self,
        port: &str,
    ) -> Result<Option<daedalus_value::Value>, crate::HostError> {
        self.host.take_value(port)
    }
}

impl fmt::Debug for LoadedHost {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.debug_struct("LoadedHost")
            .field("nodes", &self.nodes)
            .finish()
    }
}
