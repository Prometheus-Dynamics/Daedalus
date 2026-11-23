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
use daedalus_integrity::Integrity;
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
use crate::vet::vet;

pub const BACKEND_ID: &str = "process";
/// The memory this backend's nodes read from. Values reach it through transfers the host owns:
/// this backend offers none.
pub const HOST: &str = "process.host";
/// The workers' lent memory, which this backend offers other backends' nodes to write a worker's
/// input into: a cell there holds its type's wire form, written in room a worker's value buffers
/// lend, and the worker reads it where it is (Linux).
pub const LENT: &str = "process.lent";

/// One worker as its backend holds it.
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct WorkerStatus {
    /// The nodes the worker declared in its hello.
    pub nodes: Vec<String>,
    /// Whether the worker is up now.
    pub running: bool,
}

/// Starts workers and reads their hellos, with what a backend needs to do so and nothing else.
#[derive(Clone)]
pub struct Greeter {
    integrity: Integrity,
    telemetry: Arc<Telemetry>,
}

impl Greeter {
    /// Starts the worker of `spec` and reads its hello. This waits on the worker, for up to its
    /// startup time, so it should run without any lock another request needs.
    pub fn greet(&self, spec: &WorkerSpec) -> Result<Greeted, ProcessError> {
        let (worker, hello) = greet_with(&self.integrity, &self.telemetry, spec)?;
        Ok(Greeted {
            spec: spec.clone(),
            worker,
            hello,
        })
    }
}

/// A worker that has said hello, and is not yet in a backend. Dropping it stops the worker.
pub struct Greeted {
    spec: WorkerSpec,
    worker: Worker,
    hello: Hello,
}

/// Starts the worker of `spec` under `integrity`, reads its hello, and checks what it offers.
fn greet_with(
    integrity: &Integrity,
    telemetry: &Arc<Telemetry>,
    spec: &WorkerSpec,
) -> Result<(Worker, Hello), ProcessError> {
    let vetted = vet(integrity, spec)?;
    let mut worker = Worker::spawn(spec, vetted.program.as_ref(), telemetry)?;
    let hello = read_hello(&worker, spec.startup).map_err(|reason| ProcessError::Startup {
        program: spec.program.clone(),
        reason,
    })?;
    hello.check().map_err(|error| ProcessError::Hello {
        program: spec.program.clone(),
        error,
    })?;
    worker
        .offer(spec.startup, telemetry)
        .map_err(|reason| ProcessError::Startup {
            program: spec.program.clone(),
            reason,
        })?;
    vetted.record()?;
    Ok((worker, hello))
}

/// The workers a backend runs, and which node each one serves.
pub struct ProcessBackend {
    /// What each worker's files are checked under, from the time the worker is added.
    integrity: Integrity,
    workers: Vec<Arc<Shared>>,
    /// Node id to (worker index, node index inside that worker).
    served: BTreeMap<String, (usize, usize)>,
    telemetry: Arc<Telemetry>,
    log: Arc<crate::log::LogSink>,
    /// The types whose values go to a worker by descriptor.
    #[cfg(target_os = "linux")]
    dmabufs: Vec<crate::supervise::values::DmaBufType>,
}

impl Default for ProcessBackend {
    fn default() -> Self {
        ProcessBackend::new()
    }
}

impl ProcessBackend {
    pub fn new() -> ProcessBackend {
        ProcessBackend {
            integrity: Integrity::off(),
            workers: Vec::new(),
            served: BTreeMap::new(),
            telemetry: Arc::new(Telemetry::default()),
            log: Arc::default(),
            #[cfg(target_os = "linux")]
            dmabufs: Vec::new(),
        }
    }

    /// Sets how the workers this backend starts check their files. Set it before adding workers:
    /// a worker keeps the mode it was added under. The default is Off.
    #[must_use]
    pub fn with_integrity(mut self, integrity: Integrity) -> ProcessBackend {
        self.integrity = integrity;
        self
    }

    /// Lists `T` as a type whose values are a dma-buf (`daedalus_dmabuf::DmaBufBacked`). A `T`
    /// that reaches a worker's `bytes` or opaque input goes as its buffer's descriptor to a worker
    /// that agreed values by reference: sent once per buffer, mapped read-only by the worker, and
    /// never copied. A worker on the plain path is sent its bytes, read through a read-only
    /// mapping. Linux only.
    #[cfg(target_os = "linux")]
    #[must_use]
    pub fn with_dmabuf<T: daedalus_dmabuf::DmaBufBacked>(mut self) -> ProcessBackend {
        self.dmabufs.push(crate::supervise::values::DmaBufType {
            fd_of: crate::supervise::values::fd_of::<T>,
        });
        self
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

    /// The worker that serves every one of `nodes`, when one worker serves them all.
    fn one_worker(&self, nodes: &[&str]) -> Option<&Arc<Shared>> {
        let mut workers = nodes
            .iter()
            .map(|node| self.served.get(*node).map(|at| at.0));
        let first = workers.next()??;
        workers
            .all(|other| other == Some(first))
            .then(|| &self.workers[first])
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
        greet_with(&self.integrity, &self.telemetry, spec)
    }

    /// A handle that starts workers and reads their hellos without the backend: a host can greet a
    /// worker on a thread of its own, and then [`ProcessBackend::install`] it under its lock.
    pub fn greeter(&self) -> Greeter {
        Greeter {
            integrity: self.integrity.clone(),
            telemetry: Arc::clone(&self.telemetry),
        }
    }

    /// Takes a worker [`Greeter::greet`] started: registers the nodes it declares that `registry`
    /// doesn't have, and attaches it. Returns the ids of the nodes this registered. This does no
    /// waiting, so it is the part to do under a lock.
    pub fn install(
        &mut self,
        greeted: Greeted,
        registry: &mut Registry,
    ) -> Result<Vec<String>, ProcessError> {
        let Greeted {
            spec,
            worker,
            hello,
        } = greeted;
        let added = crate::declare::register_new(&spec.program, &hello, registry)?;
        self.attach(spec, worker, &hello, registry)?;
        Ok(added)
    }

    /// Checks a started worker's nodes against the registry, and takes it on.
    fn attach(
        &mut self,
        spec: WorkerSpec,
        #[cfg_attr(not(target_os = "linux"), allow(unused_mut))] mut worker: Worker,
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
        #[cfg(target_os = "linux")]
        let rooms = daedalus_process_shm::Rooms::new();
        #[cfg(target_os = "linux")]
        worker.point_rooms(&rooms);
        let shared = Shared {
            #[cfg(target_os = "linux")]
            rooms,
            integrity: self.integrity.clone(),
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
                code: None,
                panicked: false,
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

    /// What this backend holds for the worker started from `spec`: the nodes it serves, and whether
    /// it is up now. `None` when no worker was started from that spec. A worker that crashed is
    /// down until a step needs it, when it starts again.
    pub fn worker_status(&self, spec: &WorkerSpec) -> Option<WorkerStatus> {
        let shared = self
            .workers
            .iter()
            .find(|shared| shared.spec.same_as(spec))?;
        let running = matches!(shared.lock().link, Link::Up(_));
        Some(WorkerStatus {
            nodes: shared.nodes.iter().map(|node| node.id.clone()).collect(),
            running,
        })
    }

    /// The pid of the worker that serves `node`, while it is running.
    pub fn worker_pid(&self, node: &str) -> Option<u32> {
        let (worker, _) = *self.served.get(node)?;
        match &self.workers[worker].lock().link {
            Link::Up(worker) => Some(worker.pid()),
            Link::Checking(_) | Link::Down { .. } => None,
        }
    }

    /// How many stderr bytes the worker for `node` holds in its tail, at most 64 KiB.
    pub fn stderr_held(&self, node: &str) -> usize {
        match self.served.get(node) {
            Some((worker, _)) => {
                let sup = self.workers[*worker].lock();
                match &sup.link {
                    Link::Up(worker) => worker.stderr_tail(usize::MAX).len(),
                    Link::Checking(_) | Link::Down { .. } => 0,
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
        let mut spaces = vec![MemorySpace {
            id: Cow::Borrowed(HOST),
            kind: SpaceKind::Host,
            readable: true,
        }];
        if cfg!(target_os = "linux") {
            spaces.push(MemorySpace {
                id: Cow::Borrowed(LENT),
                kind: SpaceKind::Shared,
                readable: true,
            });
        }
        spaces
    }

    fn offers(&self) -> Vec<Cow<'static, str>> {
        if cfg!(target_os = "linux") {
            vec![Cow::Borrowed(LENT)]
        } else {
            Vec::new()
        }
    }

    /// The rooms of the worker that serves every reader. Readers in more than one worker are lent
    /// none: one value can't be written in two workers' memory, so it is copied to each.
    fn bind_offer(&self, space: &str, readers: &[&str]) -> Option<Arc<dyn daedalus_abi::Lend>> {
        if space != LENT {
            return None;
        }
        let shared = self.one_worker(readers)?;
        #[cfg(target_os = "linux")]
        return Some(shared.rooms.clone());
        #[cfg(not(target_os = "linux"))]
        {
            let _ = shared;
            None
        }
    }

    fn transfers(&self) -> Vec<Transfer> {
        crate::wire_space::transfers()
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
            decodes: Vec::new(),
        };
        let step: Step = Step::boxed(WorkerStep::new(
            shared,
            region_nodes,
            reads,
            writes,
            #[cfg(target_os = "linux")]
            self.dmabufs.clone(),
        ));
        Ok(Compiled::Steps(vec![StepUnit {
            step,
            info,
            patch: None,
        }]))
    }
}
