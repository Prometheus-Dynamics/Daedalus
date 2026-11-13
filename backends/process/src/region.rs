//! The backend: which workers there are, which nodes each serves, and how a region compiles to a
//! step that runs in its worker.

use std::borrow::Cow;
use std::collections::BTreeMap;
use std::path::Path;
use std::sync::{Arc, Mutex};

use daedalus_abi::Step;
use daedalus_backend::{
    Backend, CompileCx, Compiled, MemorySpace, NodeRef, Offer, Refusal, Region, SpaceKind,
    StepUnit, Transfer, UnitInfo,
};
use daedalus_registry::{Registry, TypeRef};
use daedalus_types::NodeIndex;
use daedalus_types::TypeKey;
use daedalus_wire::codec::Writer;
use daedalus_wire::{Hello, Schema, WireType};

use crate::ProcessError;
use crate::child::Worker;
use crate::spec::WorkerSpec;
use crate::supervise::{Binding, Link, NodePorts, Shared, Supervisor, WorkerStep, read_hello};
use crate::telemetry::{Snapshot, Telemetry};

pub const BACKEND_ID: &str = "process";
/// The memory this backend's nodes read from. Values reach it through transfers the host owns:
/// this backend offers none.
pub const HOST: &str = "process.host";

/// The workers a backend runs, and which node each one serves.
pub struct ProcessBackend {
    workers: Vec<Arc<Shared>>,
    /// Node id to (worker index, node index inside that worker).
    served: BTreeMap<String, (usize, usize)>,
    telemetry: Arc<Telemetry>,
    log: Arc<crate::log::LogSink>,
}

impl Default for ProcessBackend {
    fn default() -> Self {
        ProcessBackend::new()
    }
}

impl ProcessBackend {
    pub fn new() -> ProcessBackend {
        ProcessBackend {
            workers: Vec::new(),
            served: BTreeMap::new(),
            telemetry: Arc::new(Telemetry::default()),
            log: Arc::default(),
        }
    }

    /// Starts a worker, reads its hello, and checks every node it declares against the registry.
    /// A spec equal to one already started shares that worker.
    pub fn add_worker(
        &mut self,
        spec: WorkerSpec,
        registry: &Registry,
    ) -> Result<(), ProcessError> {
        if self.workers.iter().any(|shared| shared.spec.same_as(&spec)) {
            return Ok(());
        }
        let (worker, hello) = self.greet(&spec)?;
        self.attach(spec, worker, &hello, registry)
    }

    /// Starts a worker and reads its hello, as [`ProcessBackend::add_worker`] does, and registers
    /// the nodes it declares that `registry` doesn't have yet: a host needn't know a worker's
    /// nodes before it has met the worker. A node the registry already has is checked against it,
    /// and a worker that disagrees is refused. Returns the ids of the nodes this registered.
    /// A spec equal to one already started shares that worker, and registers nothing.
    pub fn add_worker_declaring(
        &mut self,
        spec: WorkerSpec,
        registry: &mut Registry,
    ) -> Result<Vec<String>, ProcessError> {
        if self.workers.iter().any(|shared| shared.spec.same_as(&spec)) {
            return Ok(Vec::new());
        }
        let (worker, hello) = self.greet(&spec)?;
        let added = crate::declare::register_new(&spec.program, &hello, registry)?;
        self.attach(spec, worker, &hello, registry)?;
        Ok(added)
    }

    /// Starts the worker of `spec` and reads its hello.
    fn greet(&self, spec: &WorkerSpec) -> Result<(Worker, Hello), ProcessError> {
        let worker = Worker::spawn(spec, &self.telemetry)?;
        let hello = read_hello(&worker, spec.startup).map_err(|reason| ProcessError::Startup {
            program: spec.program.clone(),
            reason,
        })?;
        hello.check().map_err(|error| ProcessError::Hello {
            program: spec.program.clone(),
            error,
        })?;
        Ok((worker, hello))
    }

    /// Checks a started worker's nodes against the registry, and takes it on.
    fn attach(
        &mut self,
        spec: WorkerSpec,
        worker: Worker,
        hello: &Hello,
        registry: &Registry,
    ) -> Result<(), ProcessError> {
        let nodes = check_declarations(&spec.program, hello, registry)?;
        for node in &nodes {
            if self.served.contains_key(&node.id) {
                return Err(ProcessError::Declaration {
                    program: spec.program.clone(),
                    node: node.id.clone(),
                    reason: "another worker already serves this node".into(),
                });
            }
        }
        let index = self.workers.len();
        for (position, node) in nodes.iter().enumerate() {
            self.served.insert(node.id.clone(), (index, position));
        }
        let shared = Shared {
            spec,
            schema: Schema::from_hello(hello, &[]),
            nodes,
            telemetry: self.telemetry.clone(),
            log: self.log.clone(),
            state: Mutex::new(Supervisor {
                link: Link::Up(worker),
                failures: 0,
                run: 0,
                writer: Writer::new(),
                killed: Vec::new(),
                fault: None,
            }),
        };
        self.workers.push(Arc::new(shared));
        Ok(())
    }

    /// Hands every log message a worker of this backend sends to `handler`, for workers added
    /// before or after. A worker's messages are read while one of its regions runs, so a line
    /// sent between runs arrives with the next run. The handler is called on the thread that
    /// runs the region, with the worker held: it should take the line and return, and not call
    /// this backend. With or without a handler a line is counted in [`Snapshot::logs`] and kept
    /// with the worker's stderr, so a fault report ends with what the worker last said.
    pub fn on_log(&self, handler: impl Fn(&crate::WorkerLog<'_>) + Send + Sync + 'static) {
        self.log.set(Some(Arc::new(handler)));
    }

    /// Stops handing log messages to the handler [`ProcessBackend::on_log`] set.
    pub fn clear_on_log(&self) {
        self.log.set(None);
    }

    pub fn telemetry(&self) -> Snapshot {
        self.telemetry.snapshot()
    }

    /// The pid of the worker that serves `node`, while it is running.
    pub fn worker_pid(&self, node: &str) -> Option<u32> {
        let (worker, _) = *self.served.get(node)?;
        match &self.workers[worker].lock().link {
            Link::Up(worker) => Some(worker.pid()),
            Link::Down { .. } => None,
        }
    }

    /// How many stderr bytes the worker for `node` holds in its tail, at most 64 KiB.
    pub fn stderr_held(&self, node: &str) -> usize {
        match self.served.get(node) {
            Some((worker, _)) => {
                let sup = self.workers[*worker].lock();
                match &sup.link {
                    Link::Up(worker) => worker.stderr_tail(usize::MAX).len(),
                    Link::Down { .. } => 0,
                }
            }
            None => 0,
        }
    }

    /// The pids this backend killed for the worker that serves `node`, in order.
    pub fn killed_pids(&self, node: &str) -> Vec<u32> {
        match self.served.get(node) {
            Some((worker, _)) => self.workers[*worker].lock().killed.clone(),
            None => Vec::new(),
        }
    }

    /// Stops every worker: each is asked to shut down, then killed after its grace period.
    pub fn shutdown(&mut self) {
        for shared in self.workers.drain(..) {
            let mut sup = shared.lock();
            if let Link::Up(worker) = std::mem::replace(
                &mut sup.link,
                Link::Down {
                    until: std::time::Instant::now(),
                },
            ) {
                drop(worker);
            }
        }
        self.served.clear();
    }
}

/// Compares each declared node with the registry. A port's name, count and type must match.
fn check_declarations(
    program: &Path,
    hello: &Hello,
    registry: &Registry,
) -> Result<Vec<NodePorts>, ProcessError> {
    let mut out = Vec::new();
    for decl in &hello.nodes {
        let fail = |reason: String| ProcessError::Declaration {
            program: program.to_path_buf(),
            node: decl.id.clone(),
            reason,
        };
        let entry = registry
            .entry(&decl.id)
            .ok_or_else(|| fail("the registry has no such node".into()))?;
        let registered_inputs = entry.decl.inputs;
        let registered_outputs = entry.decl.outputs;
        if registered_inputs.len() != decl.inputs.len()
            || registered_outputs.len() != decl.outputs.len()
        {
            return Err(fail(format!(
                "the registry has {} inputs and {} outputs, the worker declares {} and {}",
                registered_inputs.len(),
                registered_outputs.len(),
                decl.inputs.len(),
                decl.outputs.len()
            )));
        }
        for (registered, declared) in registered_inputs.iter().zip(&decl.inputs) {
            check_port(
                registered.name,
                &registered.ty,
                &declared.name,
                &declared.ty,
            )
            .map_err(fail)?;
        }
        for (registered, declared) in registered_outputs.iter().zip(&decl.outputs) {
            check_port(
                registered.name,
                &registered.ty,
                &declared.name,
                &declared.ty,
            )
            .map_err(fail)?;
        }
        out.push(NodePorts {
            id: decl.id.clone(),
            inputs: decl
                .inputs
                .iter()
                .map(|port| (port.name.clone(), port.ty.clone()))
                .collect(),
            outputs: decl
                .outputs
                .iter()
                .map(|port| (port.name.clone(), port.ty.clone()))
                .collect(),
        });
    }
    Ok(out)
}

fn check_port(reg_name: &str, reg_ty: &TypeRef, name: &str, ty: &WireType) -> Result<(), String> {
    if reg_name != name {
        return Err(format!("port {reg_name} is declared as {name}"));
    }
    let matches = match reg_ty {
        TypeRef::Expr(expr) => WireType::from_expr(expr) == *ty,
        TypeRef::Key(key) => key_matches(key, ty),
        TypeRef::Body => true,
    };
    if matches {
        Ok(())
    } else {
        Err(format!(
            "port {name} has another type than the registry's {reg_ty:?}"
        ))
    }
}

fn key_matches(key: &TypeKey, ty: &WireType) -> bool {
    match ty {
        WireType::Scalar(scalar) => scalar.key().as_str() == key.as_str(),
        WireType::Opaque(text) => text == key.as_str(),
        _ => false,
    }
}

impl Backend for ProcessBackend {
    fn id(&self) -> &str {
        BACKEND_ID
    }

    fn memory(&self) -> Vec<MemorySpace> {
        vec![MemorySpace {
            id: Cow::Borrowed(HOST),
            kind: SpaceKind::Host,
            readable: true,
        }]
    }

    fn transfers(&self) -> Vec<Transfer> {
        Vec::new()
    }

    /// Offers every node with a body for this backend. A node no worker serves is refused in
    /// `compile`, and the planner assigns it elsewhere.
    fn can_take(&self, offer: &Offer<'_>) -> Vec<NodeRef> {
        offer
            .nodes
            .iter()
            .map(|node| NodeRef {
                node: node.node,
                body: node.body,
            })
            .collect()
    }

    fn compile(&self, region: &Region<'_>, cx: &CompileCx<'_>) -> Result<Compiled, Refusal> {
        if !cx.edges.is_empty() {
            return Err(Refusal::new(
                region.nodes.to_vec(),
                "the process backend does not carry values along edges inside a region",
            ));
        }
        // Each node's worker, by its registered id.
        let mut placed: Vec<(usize, usize)> = Vec::new();
        let mut unserved = Vec::new();
        for node in region.nodes {
            let id = cx
                .bodies
                .iter()
                .find(|body| body.node == *node)
                .map(|body| body.id);
            match id.and_then(|id| self.served.get(id)) {
                Some(place) => placed.push(*place),
                None => unserved.push(*node),
            }
        }
        if !unserved.is_empty() {
            return Err(Refusal::new(unserved, "no worker serves this node"));
        }
        let Some(&(worker, _)) = placed.first() else {
            return Err(Refusal::whole_region("the region has no nodes"));
        };
        let mut other_worker: Vec<NodeIndex> = Vec::new();
        for (node, (w, _)) in region.nodes.iter().zip(&placed) {
            if *w != worker {
                other_worker.push(*node);
            }
        }
        if !other_worker.is_empty() {
            return Err(Refusal::new(other_worker, "a region runs in one worker"));
        }
        let shared = self.workers[worker].clone();
        let node_of = |node: NodeIndex| -> Option<u16> {
            let id = cx.bodies.iter().find(|body| body.node == node)?.id;
            let (_, index) = *self.served.get(id)?;
            Some(index as u16)
        };
        let mut reads = Vec::new();
        for port in region.inputs {
            let node = node_of(port.node).ok_or_else(|| {
                Refusal::whole_region("a boundary port names a node outside the region")
            })?;
            reads.push(Binding {
                node,
                port: port.port.0,
            });
        }
        let mut writes = Vec::new();
        for port in region.outputs {
            let node = node_of(port.node).ok_or_else(|| {
                Refusal::whole_region("a boundary port names a node outside the region")
            })?;
            writes.push(Binding {
                node,
                port: port.port.0,
            });
        }
        let region_nodes: Vec<u16> = region
            .nodes
            .iter()
            .filter_map(|node| node_of(*node))
            .collect();
        let info = UnitInfo {
            covers: region.nodes.to_vec(),
            reads: region.inputs.to_vec(),
            writes: region.outputs.to_vec(),
            absorbed: Vec::new(),
            may_pend: false,
        };
        let step: Step = Step::boxed(WorkerStep::new(shared, region_nodes, reads, writes));
        Ok(Compiled::Steps(vec![StepUnit { step, info }]))
    }
}
