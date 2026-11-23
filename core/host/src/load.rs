//! Turns a finished plan into a running host: takes the steps out, wires the wake lists, sizes every
//! buffer once.

#![cfg(feature = "std")]

use std::collections::{BTreeSet, HashMap};
use std::fmt;
use std::mem;
use std::string::String;
use std::sync::OnceLock;
use std::time::Instant;
use std::vec;
use std::vec::Vec;

use daedalus_backend::StepUnit;
use daedalus_made::{BindError, Bound, HostPort, MadeError};
use daedalus_runner::owned_run;
use daedalus_runner::{ProbeClock, StepMeta, best_clock};
use daedalus_types::{SpaceIndex, TypeExpr, TypeIndex, TypeKey};
use daedalus_value::Pressure;

use crate::fault::FaultRing;
use crate::intern::{intern, intern_key, intern_keys, intern_names, intern_ports, intern_wake};
use crate::lay_out::{Built, lay_out, slot_stamps};
use crate::patch::ConstTable;
use crate::tick_overhead::{TickOverheadReport, TickOverheadWindow};
use crate::unmerged::Planner;
use crate::{Dir, Host, PortSpec, Ports, StateStore};

/// What can be set when a plan loads.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub struct LoadOptions {
    /// The capacity of a FIFO edge that declares none.
    pub fifo_capacity: u32,
    /// How many failure reports the host keeps.
    pub fault_reports: usize,
    /// How many ticks of overhead to keep. Zero leaves the probe off.
    pub overhead_window: usize,
    /// The bytes of detail kept for a step's failure. Zero keeps none.
    pub fault_detail_bytes: usize,
    /// Keep what the nodes' bodies report as their metrics, for [`LoadedHost::metric_rows`].
    /// Off by default: a body's report then goes nowhere, and costs it one test.
    pub node_metrics: bool,
}

impl Default for LoadOptions {
    fn default() -> Self {
        LoadOptions {
            fifo_capacity: 64,
            fault_reports: 32,
            overhead_window: 0,
            fault_detail_bytes: 16 << 10,
            node_metrics: false,
        }
    }
}

/// Why a plan didn't load. The running graph is not touched.
#[derive(Debug)]
#[non_exhaustive]
pub enum LoadError {
    /// The plan isn't in the steps form, so it has nothing to run in this process.
    NotSteps,
    /// A move in the plan has no step. The planner makes one for every move of a steps-form plan,
    /// so this means the plan was changed by hand.
    TransferUnbound { id: String },
    /// Two host ports share a name, so the host couldn't tell them apart.
    DuplicatePort { name: String },
    /// The port table is malformed.
    Ports(crate::PortsError),
    /// The runner refused the plan.
    Run(daedalus_runner::LoadError),
    /// A node's recovery metadata is malformed, or merged nodes ask for different recoveries.
    Recovery { node: String, reason: String },
    /// A bounded edge's overflow word names no rule.
    Overflow { overflow: String },
    /// The made plan's bytes don't read.
    Made(MadeError),
    /// The made plan doesn't bind to this host's backends and registry.
    Bind(BindError),
}

impl fmt::Display for LoadError {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            LoadError::NotSteps => write!(f, "the plan is not in the steps form"),
            LoadError::TransferUnbound { id } => write!(f, "transfer `{id}` has no step"),
            LoadError::DuplicatePort { name } => write!(f, "two host ports are named `{name}`"),
            LoadError::Ports(error) => write!(f, "the port table is malformed: {error:?}"),
            LoadError::Run(error) => write!(f, "the runner refused the plan: {error:?}"),
            LoadError::Recovery { node, reason } => write!(f, "node `{node}` {reason}"),
            LoadError::Overflow { overflow } => write!(
                f,
                "an edge is bounded with overflow `{overflow}`, which is not one of {}",
                daedalus_graph::OverflowDoc::WORDS
            ),
            LoadError::Made(error) => error.fmt(f),
            LoadError::Bind(error) => error.fmt(f),
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

fn node_key(label: &Option<String>, id: &str, seen: &mut HashMap<String, u32>) -> String {
    if let Some(label) = label {
        return label.clone();
    }
    let count = seen.entry(id.to_string()).or_insert(0);
    let key = std::format!("{}#{}", id, *count);
    *count += 1;
    key
}

/// The nodes of a plan with their keys and the body each runs.
pub(crate) fn node_entries(bound: &Bound) -> Vec<NodeEntry> {
    let mut seen = HashMap::new();
    bound
        .facts
        .nodes
        .iter()
        .map(|node| NodeEntry {
            key: node_key(&node.label, &node.id, &mut seen),
            // Another version of a backend, as a plugin loaded again is, is another body.
            body: std::format!(
                "{:?}:{}",
                node.backend.and_then(|index| bound.backend(index)),
                node.id
            ),
        })
        .collect()
}

/// The bridge nodes a plan's host ports belong to, for comparing two plans.
pub(crate) fn bridge_keys(bound: &Bound) -> Vec<String> {
    let mut keys: Vec<String> = bound
        .facts
        .nodes
        .iter()
        .filter(|node| node.bridge)
        .map(|node| node.label.clone().unwrap_or_else(|| node.id.clone()))
        .collect();
    keys.sort_unstable();
    keys.dedup();
    keys
}

pub(crate) fn take_steps(bound: &mut Bound) -> Vec<Vec<Option<StepUnit>>> {
    mem::take(&mut bound.units)
}

fn port_table(
    bound: &Bound,
    meta: &[StepMeta],
    table: &[u32],
) -> Result<Ports<'static>, LoadError> {
    let stitched = &bound.facts.stitched;
    let exprs = &bound.facts.types;
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
pub(crate) fn type_text(expr: &TypeExpr) -> String {
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

fn monotonic_ns() -> u64 {
    static START: OnceLock<Instant> = OnceLock::new();
    START.get_or_init(Instant::now).elapsed().as_nanos() as u64
}

/// The clock a probe times with: the platform's cheapest one that the kernel allows, and the
/// monotonic clock otherwise. On x86_64 Linux the time stamp counter is used only when the kernel's
/// clocksource is `tsc`.
pub fn probe_clock() -> ProbeClock {
    best_clock(monotonic_ns, kernel_clock_ok())
}

fn kernel_clock_ok() -> bool {
    #[cfg(all(target_arch = "x86_64", target_os = "linux"))]
    {
        std::fs::read_to_string("/sys/devices/system/clocksource/clocksource0/current_clocksource")
            .is_ok_and(|name| name.trim() == "tsc")
    }
    #[cfg(not(all(target_arch = "x86_64", target_os = "linux")))]
    {
        true
    }
}

fn type_keys(bound: &Bound) -> &'static [TypeKey] {
    let texts: Vec<String> = bound.facts.types.iter().map(type_text).collect();
    intern_keys(&texts)
}

/// A graph loaded from a plan, and everything that outlives one version of it.
pub struct LoadedHost {
    pub(crate) host: Host<'static>,
    pub(crate) state: StateStore,
    pub(crate) nodes: Vec<NodeEntry>,
    pub(crate) bridges: Vec<String>,
    pub(crate) options: LoadOptions,
    pub(crate) covers: Vec<&'static [&'static str]>,
    pub(crate) node_names: Vec<&'static str>,
    pub(crate) fault_seen: Vec<u32>,
    pub(crate) faults: FaultRing,
    pub(crate) detail: crate::detail::Details,
    pub(crate) planner: Option<Planner>,
    pub(crate) unmerged: bool,
    pub(crate) consts: ConstTable,
    pub(crate) off_nodes: BTreeSet<String>,
    pub(crate) overhead: Option<TickOverheadWindow>,
    pub(crate) same_as: Vec<Option<String>>,
    /// For each step that is a move, the transfer it makes.
    pub(crate) moves: Vec<Option<&'static str>>,
    /// The metrics the graph's nodes declare.
    pub(crate) metric_list: Vec<MetricInfo>,
    /// For each of them, the step whose cells hold it and its place among them. `None` when
    /// the host keeps no metrics, or no step runs the node.
    pub(crate) metric_cells: Vec<Option<(u32, u32)>>,
}

/// One metric a node declares.
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct MetricInfo {
    /// The node, as the host names it.
    pub node: String,
    pub name: String,
    pub unit: daedalus_registry::MetricUnit,
}

/// What a node's body has reported for one of its metrics.
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct MetricRow {
    /// The node, as the host names it.
    pub node: String,
    pub name: String,
    pub unit: daedalus_registry::MetricUnit,
    /// The values reported, added up.
    pub sum: u64,
    /// How many values were reported.
    pub count: u64,
    /// The largest value reported.
    pub max: u64,
}

/// The metrics of every node of a plan, in node order.
pub(crate) fn metric_list(bound: &Bound, nodes: &[NodeEntry]) -> Vec<MetricInfo> {
    let facts = bound.facts.nodes.iter().zip(nodes);
    facts
        .flat_map(|(node, entry)| {
            node.metrics.iter().map(|metric| MetricInfo {
                node: entry.key.clone(),
                name: metric.name.clone(),
                unit: metric.unit,
            })
        })
        .collect()
}

/// Everything one version of the graph brings.
pub(crate) struct Parts {
    pub(crate) host: Host<'static>,
    /// For each step, what a step of another plan must equal to stand in for it.
    pub(crate) same_as: Vec<Option<String>>,
    /// For each step that is a move, the transfer it makes.
    pub(crate) moves: Vec<Option<&'static str>>,
    pub(crate) nodes: Vec<NodeEntry>,
    pub(crate) bridges: Vec<String>,
    pub(crate) covers: Vec<&'static [&'static str]>,
    pub(crate) node_names: Vec<&'static str>,
    pub(crate) metric_list: Vec<MetricInfo>,
    pub(crate) metric_cells: Vec<Option<(u32, u32)>>,
}

/// Builds the host for a plan. Nothing else is touched, so a failure leaves any running graph as
/// it was.
pub(crate) fn build_host(bound: &mut Bound, options: LoadOptions) -> Result<Parts, LoadError> {
    let nodes = node_entries(bound);
    let bridges = bridge_keys(bound);
    let built = lay_out(bound, options)?;
    let Built {
        steps,
        meta,
        table,
        slots,
        edges,
        covers,
        same_as,
        transfers,
        kept,
        extra,
        fans,
        edge_names,
    } = built;
    let node_names: Vec<&'static str> = nodes.iter().map(|node| intern(&node.key)).collect();
    let metric_list = metric_list(bound, &nodes);
    let step_nodes = covers.clone();
    let covers = covers
        .iter()
        .map(|covers| {
            let names: Vec<&'static str> = covers
                .iter()
                .map(|&node| node_names[node as usize])
                .collect();
            intern_names(&names)
        })
        .collect();
    let ports = port_table(bound, &meta, &table)?;
    let types = type_keys(bound);
    let clock: Option<ProbeClock> = (options.overhead_window > 0).then(probe_clock);
    let host_inputs: Vec<u32> = bound
        .facts
        .stitched
        .host_inputs
        .iter()
        .map(|p| p.slot)
        .collect();
    let mut stamps = slot_stamps(&bound.facts.stitched);
    // A fan-out's added slot carries the values of the plan slot it was made for.
    for carried in extra {
        stamps.push(stamps[carried as usize]);
    }
    // Where each declared metric's cell is: its step, and its place among that step's cells,
    // which follow the nodes the step runs, in that order.
    let mut metric_cells = vec![None; metric_list.len()];
    let mut per_step = Vec::new();
    if options.node_metrics {
        let mut first_of = vec![0usize; bound.facts.nodes.len() + 1];
        for (node, facts) in bound.facts.nodes.iter().enumerate() {
            first_of[node + 1] = first_of[node] + facts.metrics.len();
        }
        for (step, nodes) in step_nodes.iter().enumerate() {
            let mut at = 0u32;
            for &node in nodes {
                let own = first_of[node as usize]..first_of[node as usize + 1];
                for cell in &mut metric_cells[own] {
                    *cell = Some((step as u32, at));
                    at += 1;
                }
            }
            per_step.push(at);
        }
    }
    let wiring = owned_run::Wiring {
        meta,
        table,
        fans,
        metrics: per_step,
    };
    let (mut runner, buffers) = owned_run::build(steps, wiring, slots, stamps, &host_inputs, clock)
        .map_err(LoadError::Run)?;
    for (slot, rule) in edges {
        // A slot a kept rule can't apply to stays on the default.
        let _ = runner.set_pressure(slot, rule.pressure);
        let _ = runner.set_blocking(slot, rule.blocking);
        let _ = runner.set_per_tick(slot, rule.per_tick);
    }
    // A slot that keeps its value, for several readers or as a held input's context, holds the
    // newest one, so a new one replaces it.
    for slot in kept {
        let _ = runner.set_pressure(slot, Pressure::LatestOnly);
    }
    let mut host = Host::new(runner, ports, types);
    host.owned = Some(buffers);
    host.edge_names = edge_names;
    host.set_context(
        bound
            .facts
            .stitched
            .host_inputs
            .iter()
            .filter(|port| port.policy.is_context())
            .map(|port| port.slot)
            .collect(),
    );
    Ok(Parts {
        metric_list,
        metric_cells,
        host,
        same_as,
        moves: transfers,
        nodes,
        bridges,
        covers,
        node_names,
    })
}

impl LoadedHost {
    /// Loads a finished plan. Slots, pools, fences and telemetry arrays are sized once, here.
    pub fn load(planned: impl Into<Bound>, options: LoadOptions) -> Result<LoadedHost, LoadError> {
        LoadedHost::load_with_consts(planned, options, ConstTable::new())
    }

    /// Loads a made plan: decodes its bytes, binds each region to the backend of its own id and
    /// loads the result. Nothing is planned here, so a host built without the planner loads it.
    pub fn load_made(
        bytes: &[u8],
        registry: &daedalus_registry::Registry,
        backends: &[&dyn daedalus_backend::Backend],
        options: LoadOptions,
    ) -> Result<LoadedHost, LoadError> {
        let made = daedalus_made::decode(bytes).map_err(LoadError::Made)?;
        let bound = daedalus_made::bind(&made, registry, backends).map_err(LoadError::Bind)?;
        LoadedHost::load(bound, options)
    }

    /// As [`load`](LoadedHost::load), with a const table the plan's step bodies already hold.
    pub fn load_with_consts(
        planned: impl Into<Bound>,
        options: LoadOptions,
        consts: ConstTable,
    ) -> Result<LoadedHost, LoadError> {
        let mut bound: Bound = planned.into();
        let parts = build_host(&mut bound, options)?;
        consts.seed(&bound);
        let mut loaded = LoadedHost {
            detail: Default::default(),
            state: StateStore::new(),
            fault_seen: vec![0; parts.covers.len()],
            faults: FaultRing::new(options.fault_reports),
            options,
            planner: None,
            unmerged: false,
            consts,
            off_nodes: BTreeSet::new(),
            overhead: (options.overhead_window > 0)
                .then(|| TickOverheadWindow::new(options.overhead_window)),
            host: parts.host,
            same_as: parts.same_as,
            moves: parts.moves,
            metric_list: parts.metric_list,
            metric_cells: parts.metric_cells,
            nodes: parts.nodes,
            bridges: parts.bridges,
            covers: parts.covers,
            node_names: parts.node_names,
        };
        loaded.attach_detail();
        Ok(loaded)
    }

    /// Runs one tick and records what failed in it.
    pub fn step(&mut self) -> Result<daedalus_runner::TickReport, crate::HostError> {
        self.consts.set_ticking(true);
        let report = self.host.step();
        self.consts.set_ticking(false);
        if let (Some(window), Some(probe)) = (
            self.overhead.as_mut(),
            self.host.owned.as_ref().and_then(|owned| owned.probe()),
        ) {
            window.record(probe);
        }
        self.collect_faults();
        report
    }

    /// The metrics the graph's nodes declare, in node order: what each node's body reports about
    /// its own work. Known from the plan, so a graph need not run to be asked.
    pub fn metrics(&self) -> Vec<MetricInfo> {
        self.metric_list.clone()
    }

    /// What each node's body has reported for each of its metrics since the plan was loaded: the
    /// sum of the values, how many there were, and the largest. Empty unless the host was loaded
    /// with [`LoadOptions::node_metrics`]. A reload starts them again.
    pub fn metric_rows(&self) -> Vec<MetricRow> {
        let cells = self.metric_list.iter().zip(&self.metric_cells);
        cells
            .filter_map(|(info, cell)| {
                let (step, at) = (*cell)?;
                let (sum, count, max) =
                    self.host.runner.metric_cells(step).get(at as usize)?.read();
                Some(MetricRow {
                    node: info.node.clone(),
                    name: info.name.clone(),
                    unit: info.unit,
                    sum,
                    count,
                    max,
                })
            })
            .collect()
    }

    /// The tick overhead over the recent window, when the probe is on.
    pub fn tick_overhead(&self) -> Option<TickOverheadReport> {
        self.overhead.as_ref().map(TickOverheadWindow::report)
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

    /// The cancel word as a handle another thread can keep, to cancel a tick this one is running.
    pub fn cancel_handle(&self) -> daedalus_runner::owned_run::CancelHandle {
        self.host
            .owned
            .as_ref()
            .expect("a loaded host owns its buffers")
            .cancel_handle()
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
        LoadedHost::step(self)
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
