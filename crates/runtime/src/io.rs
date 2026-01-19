use std::any::{Any, TypeId};
use std::collections::BTreeMap;
use std::collections::HashMap;
#[cfg(feature = "gpu")]
use std::collections::HashSet;
use std::collections::VecDeque;
use std::sync::Arc;
use std::time::Instant;
use std::sync::{OnceLock, RwLock};

use daedalus_data::model::{TypeExpr, Value};
use daedalus_data::typing;
#[cfg(feature = "gpu")]
use image::{DynamicImage, GrayAlphaImage, GrayImage, RgbImage, RgbaImage};

use crate::executor::queue::{apply_policy, apply_policy_owned};
use crate::executor::{
    CorrelatedPayload, EdgePayload, EdgeStorage, ExecutionTelemetry, next_correlation_id,
};
use crate::fanin::parse_indexed_port;
#[allow(unused_imports)]
use crate::plan::{BackpressureStrategy, EdgePolicyKind, RuntimeNode};
use daedalus_core::sync::{SyncGroup, SyncPolicy};
use daedalus_planner::NodeRef;

type EdgeInfo = (NodeRef, String, NodeRef, String, EdgePolicyKind);

#[derive(Clone)]
struct DrainedInput {
    port: String,
    edge_idx: usize,
    payload: CorrelatedPayload,
}

pub type ConstCoercer = Box<
    dyn Fn(&daedalus_data::model::Value) -> Option<Box<dyn Any + Send + Sync>>
        + Send
        + Sync
        + 'static,
>;

pub type ConstCoercerMap = Arc<RwLock<HashMap<&'static str, ConstCoercer>>>;

static GLOBAL_CONST_COERCERS: OnceLock<ConstCoercerMap> = OnceLock::new();
type OutputMover = Box<
    dyn Fn(Box<dyn Any + Send + Sync>) -> EdgePayload + Send + Sync + 'static
>;
pub type OutputMoverMap = Arc<RwLock<HashMap<TypeId, OutputMover>>>;
static OUTPUT_MOVERS: OnceLock<OutputMoverMap> = OnceLock::new();

fn output_movers() -> &'static OutputMoverMap {
    OUTPUT_MOVERS.get_or_init(|| Arc::new(RwLock::new(HashMap::new())))
}

/// Create a new output mover registry.
///
/// ```no_run
/// use daedalus_runtime::io::new_output_mover_map;
/// let map = new_output_mover_map();
/// assert!(map.read().unwrap().is_empty());
/// ```
pub fn new_output_mover_map() -> OutputMoverMap {
    Arc::new(RwLock::new(HashMap::new()))
}

/// Register an output mover in the provided map.
///
/// Movers take ownership of the output value, allowing zero-copy payload wrapping.
pub fn register_output_mover_in<T, F>(map: &OutputMoverMap, mover: F)
where
    T: Any + Send + Sync + 'static,
    F: Fn(T) -> EdgePayload + Send + Sync + 'static,
{
    let mut guard = map.write().expect("OUTPUT_MOVERS lock poisoned");
    guard.insert(
        TypeId::of::<T>(),
        Box::new(move |any| {
            let boxed = any.downcast::<T>().expect("output mover type mismatch");
            mover(*boxed)
        }),
    );
}

/// Register a global output mover.
pub fn register_output_mover<T, F>(mover: F)
where
    T: Any + Send + Sync + 'static,
    F: Fn(T) -> EdgePayload + Send + Sync + 'static,
{
    register_output_mover_in(output_movers(), mover);
}

fn try_move_output<T>(movers: Option<&OutputMoverMap>, value: T) -> Result<EdgePayload, T>
where
    T: Any + Send + Sync + 'static,
{
    let map = match movers {
        Some(map) => map,
        None => return Err(value),
    };
    let guard = match map.read() {
        Ok(guard) => guard,
        Err(_) => return Err(value),
    };
    let mover = match guard.get(&TypeId::of::<T>()) {
        Some(mover) => mover,
        None => return Err(value),
    };
    let boxed: Box<dyn Any + Send + Sync> = Box::new(value);
    Ok(mover(boxed))
}

#[cfg(feature = "gpu")]
fn promote_payload_for_host(payload: EdgePayload) -> EdgePayload {
    use daedalus_gpu::{ErasedPayload, Payload};

    match payload {
        EdgePayload::Any(a) => {
            if let Some(ep) = a.downcast_ref::<ErasedPayload>() {
                return EdgePayload::Payload(ep.clone());
            }
            if let Some(p) = a.downcast_ref::<Payload<DynamicImage>>() {
                return match p.clone() {
                    Payload::Cpu(img) => EdgePayload::Payload(ErasedPayload::from_cpu::<DynamicImage>(img)),
                    Payload::Gpu(g) => EdgePayload::Payload(ErasedPayload::from_gpu::<DynamicImage>(g)),
                };
            }
            if let Some(p) = a.downcast_ref::<Payload<GrayImage>>() {
                return match p.clone() {
                    Payload::Cpu(img) => EdgePayload::Payload(ErasedPayload::from_cpu::<GrayImage>(img)),
                    Payload::Gpu(g) => EdgePayload::Payload(ErasedPayload::from_gpu::<GrayImage>(g)),
                };
            }
            if let Some(img) = a.downcast_ref::<DynamicImage>() {
                return EdgePayload::Payload(ErasedPayload::from_cpu::<DynamicImage>(img.clone()));
            }
            if let Some(img) = a.downcast_ref::<Arc<DynamicImage>>() {
                return EdgePayload::Payload(ErasedPayload::from_cpu::<DynamicImage>((**img).clone()));
            }
            if let Some(img) = a.downcast_ref::<GrayImage>() {
                return EdgePayload::Payload(ErasedPayload::from_cpu::<GrayImage>(img.clone()));
            }
            if let Some(img) = a.downcast_ref::<Arc<GrayImage>>() {
                return EdgePayload::Payload(ErasedPayload::from_cpu::<GrayImage>((**img).clone()));
            }
            if let Some(img) = a.downcast_ref::<RgbImage>() {
                return EdgePayload::Payload(ErasedPayload::from_cpu::<RgbImage>(img.clone()));
            }
            if let Some(img) = a.downcast_ref::<Arc<RgbImage>>() {
                return EdgePayload::Payload(ErasedPayload::from_cpu::<RgbImage>((**img).clone()));
            }
            if let Some(img) = a.downcast_ref::<RgbaImage>() {
                return EdgePayload::Payload(ErasedPayload::from_cpu::<RgbaImage>(img.clone()));
            }
            if let Some(img) = a.downcast_ref::<Arc<RgbaImage>>() {
                return EdgePayload::Payload(ErasedPayload::from_cpu::<RgbaImage>((**img).clone()));
            }
            if let Some(img) = a.downcast_ref::<GrayAlphaImage>() {
                let dyn_img = DynamicImage::ImageLumaA8(img.clone());
                return EdgePayload::Payload(ErasedPayload::from_cpu::<DynamicImage>(dyn_img));
            }
            if let Some(img) = a.downcast_ref::<Arc<GrayAlphaImage>>() {
                let dyn_img = DynamicImage::ImageLumaA8((**img).clone());
                return EdgePayload::Payload(ErasedPayload::from_cpu::<DynamicImage>(dyn_img));
            }
            EdgePayload::Any(a)
        }
        other => other,
    }
}

/// Create a new constant coercer registry.
///
/// ```
/// use daedalus_runtime::io::new_const_coercer_map;
/// let map = new_const_coercer_map();
/// assert!(map.read().unwrap().is_empty());
/// ```
pub fn new_const_coercer_map() -> ConstCoercerMap {
    Arc::new(RwLock::new(HashMap::new()))
}

/// Register a conversion for constant default values.
///
/// Constant defaults are injected into node inputs as `Any` payloads (typically `i64`, `f64`,
/// `bool`, `String`, or `daedalus_data::model::Value`). Most payload types should flow through the
/// graph without conversion, but scalar configuration inputs often want a native Rust type (e.g.
/// `u32`, `f32`, or plugin-defined enums).
///
/// Static (non-plugin) builds can call this during initialization to support enum constants.
///
/// Dynamic plugins should prefer `PluginRegistry::register_const_coercer` so the host and plugin
/// share a single coercer map.
/// Register a global const value coercer.
///
/// ```
/// use daedalus_runtime::io::register_const_coercer;
/// use daedalus_data::model::Value;
/// register_const_coercer::<i64, _>(|v| match v { Value::Int(i) => Some(*i), _ => None });
/// ```
pub fn register_const_coercer<T, F>(coercer: F)
where
    T: Any + Send + Sync + 'static,
    F: Fn(&daedalus_data::model::Value) -> Option<T> + Send + Sync + 'static,
{
    let key = std::any::type_name::<T>();
    let map = GLOBAL_CONST_COERCERS.get_or_init(new_const_coercer_map);
    let mut guard = map.write().expect("GLOBAL_CONST_COERCERS lock poisoned");
    guard.insert(
        key,
        Box::new(move |v| coercer(v).map(|t| Box::new(t) as Box<dyn Any + Send + Sync>)),
    );
}

/// Minimal node I/O surface backed by executor queues.
///
/// ```no_run
/// use daedalus_runtime::io::NodeIo;
/// use daedalus_runtime::executor::EdgePayload;
///
/// fn handler(io: &mut NodeIo) {
///     io.push_output(Some("out"), EdgePayload::Unit);
/// }
/// ```
pub struct NodeIo<'a> {
    inputs: Vec<(String, CorrelatedPayload)>,
    borrowed_cache: std::cell::UnsafeCell<Vec<Box<dyn Any + Send + Sync>>>,
    sync_groups: Vec<SyncGroup>,
    port_overrides: HashMap<String, (Option<BackpressureStrategy>, Option<usize>)>,
    current_corr_id: u64,
    outgoing: Vec<usize>,
    has_incoming_edges: bool,
    queues: &'a Arc<Vec<EdgeStorage>>,
    telemetry: &'a mut ExecutionTelemetry,
    edges: &'a [EdgeInfo],
    #[allow(dead_code)]
    seg_idx: usize,
    node_id: String,
    warnings_seen: &'a std::sync::Arc<std::sync::Mutex<std::collections::HashSet<String>>>,
    backpressure: BackpressureStrategy,
    #[cfg(feature = "gpu")]
    gpu: Option<daedalus_gpu::GpuContextHandle>,
    #[cfg(feature = "gpu")]
    target_compute: daedalus_planner::ComputeAffinity,
    #[cfg(feature = "gpu")]
    payload_edges: &'a HashSet<usize>,
    const_coercers: Option<ConstCoercerMap>,
    output_movers: Option<OutputMoverMap>,
}

impl<'a> NodeIo<'a> {
    #[cfg(feature = "gpu")]
    fn dynamic_image_to_t<T: Any + Clone>(img: DynamicImage) -> Option<T> {
        let want = TypeId::of::<T>();
        if want == TypeId::of::<DynamicImage>() {
            let any_ref: &dyn Any = &img;
            return any_ref.downcast_ref::<T>().cloned();
        }
        if want == TypeId::of::<GrayImage>() {
            let gray = img.to_luma8();
            let any_ref: &dyn Any = &gray;
            return any_ref.downcast_ref::<T>().cloned();
        }
        if want == TypeId::of::<GrayAlphaImage>() {
            let gray = img.to_luma_alpha8();
            let any_ref: &dyn Any = &gray;
            return any_ref.downcast_ref::<T>().cloned();
        }
        if want == TypeId::of::<RgbImage>() {
            let rgb = img.to_rgb8();
            let any_ref: &dyn Any = &rgb;
            return any_ref.downcast_ref::<T>().cloned();
        }
        if want == TypeId::of::<RgbaImage>() {
            let rgba = img.to_rgba8();
            let any_ref: &dyn Any = &rgba;
            return any_ref.downcast_ref::<T>().cloned();
        }
        None
    }

    #[allow(clippy::too_many_arguments)]
    /// Construct the I/O facade for a node (internal runtime API).
    pub fn new(
        incoming_edges: Vec<usize>,
        outgoing_edges: Vec<usize>,
        queues: &'a Arc<Vec<EdgeStorage>>,
        warnings_seen: &'a Arc<std::sync::Mutex<std::collections::HashSet<String>>>,
        edges: &'a [EdgeInfo],
        mut sync_groups: Vec<SyncGroup>,
        #[cfg(feature = "gpu")] gpu_entry_edges: &'a HashSet<usize>,
        #[cfg(feature = "gpu")] gpu_exit_edges: &'a HashSet<usize>,
        #[cfg(feature = "gpu")] payload_edges: &'a HashSet<usize>,
        seg_idx: usize,
        node_id: String,
        telemetry: &'a mut ExecutionTelemetry,
        backpressure: BackpressureStrategy,
        const_inputs: &[(String, daedalus_data::model::Value)],
        const_coercers: Option<ConstCoercerMap>,
        output_movers: Option<OutputMoverMap>,
        #[cfg(feature = "gpu")] gpu: Option<daedalus_gpu::GpuContextHandle>,
        #[cfg(feature = "gpu")] target_compute: daedalus_planner::ComputeAffinity,
    ) -> Self {
        let is_host_bridge =
            node_id.ends_with("io.host_bridge") || node_id.ends_with("io.host_output");
        let has_incoming_edges = !incoming_edges.is_empty();
        if sync_groups.is_empty() && !is_host_bridge {
            // Default behavior: multi-input nodes should only fire when all ports are ready.
            // If the node doesn't specify sync metadata, create an implicit AllReady group
            // across all incoming ports so downstream consumers never see partial inputs.
            let mut ports: Vec<String> = incoming_edges
                .iter()
                .filter_map(|edge_idx| edges.get(*edge_idx).map(|(_, _, _, to_port, _)| to_port))
                .cloned()
                .collect();
            ports.sort();
            ports.dedup();
            if ports.len() > 1 {
                sync_groups.push(SyncGroup {
                    name: "__implicit_all_ready".into(),
                    policy: SyncPolicy::AllReady,
                    backpressure: None,
                    capacity: None,
                    ports,
                });
            }
        }

        let mut drained: Vec<DrainedInput> = Vec::new();
        for edge_idx in &incoming_edges {
            if let Some(storage) = queues.get(*edge_idx) {
                match storage {
                    EdgeStorage::Locked(q_arc) => {
                        if let Ok(mut q) = q_arc.lock() {
                            while let Some(payload) = q.pop_front() {
                                #[allow(unused_mut)]
                                let mut payload = payload;
                                let now = Instant::now();
                                telemetry
                                    .record_edge_wait(*edge_idx, now.saturating_duration_since(payload.enqueued_at));
                                let port = edges
                                    .get(*edge_idx)
                                    .map(|(_, _, _, to_port, _)| to_port.clone())
                                    .unwrap_or_default();
                                #[cfg(feature = "gpu")]
                                {
                                    payload = Self::convert_incoming(
                                        payload,
                                        *edge_idx,
                                        gpu_entry_edges,
                                        gpu_exit_edges,
                                        gpu.as_ref(),
                                    );
                                }
                                drained.push(DrainedInput {
                                    port,
                                    edge_idx: *edge_idx,
                                    payload,
                                });
                            }
                        }
                    }
                    #[cfg(feature = "lockfree-queues")]
                    EdgeStorage::BoundedLf(q) => {
                        while let Some(payload) = q.pop() {
                            #[allow(unused_mut)]
                            let mut payload = payload;
                            let now = Instant::now();
                            telemetry
                                .record_edge_wait(*edge_idx, now.saturating_duration_since(payload.enqueued_at));
                            let port = edges
                                .get(*edge_idx)
                                .map(|(_, _, _, to_port, _)| to_port.clone())
                                .unwrap_or_default();
                            #[cfg(feature = "gpu")]
                            {
                                payload = Self::convert_incoming(
                                    payload,
                                    *edge_idx,
                                    gpu_entry_edges,
                                    gpu_exit_edges,
                                    gpu.as_ref(),
                                );
                            }
                            drained.push(DrainedInput {
                                port,
                                edge_idx: *edge_idx,
                                payload,
                            });
                        }
                    }
                }
            }
        }
        if std::env::var_os("DAEDALUS_TRACE_EDGE_IO").is_some() {
            for item in &drained {
                log::debug!(
                    "node input drained node={} port={} edge_idx={} payload={}",
                    node_id,
                    item.port,
                    item.edge_idx,
                    edge_payload_desc(&item.payload.inner)
                );
            }
        }
        if log::log_enabled!(log::Level::Debug) && drained.is_empty() {
            let ports: Vec<String> = incoming_edges
                .iter()
                .filter_map(|edge_idx| edges.get(*edge_idx).map(|(_, _, _, to_port, _)| to_port.clone()))
                .collect();
            if !ports.is_empty() {
                log::debug!("node inputs empty node={} ports={:?}", node_id, ports);
            }
        }

        let has_drained = !drained.is_empty();
        let mut const_payloads: Vec<(String, CorrelatedPayload)> = Vec::new();
        // Apply constant defaults only when no incoming payload exists for the port.
        for (port, value) in const_inputs {
            if drained.iter().any(|p| p.port == *port) {
                continue;
            }
            let payload = match value {
                // Prefer scalar carriers for cross-dylib stability; coercion still has access to
                // the `Value` shape via `coerce_const_any`.
                daedalus_data::model::Value::Int(v) => EdgePayload::Any(Arc::new(*v)),
                daedalus_data::model::Value::Float(f) => EdgePayload::Any(Arc::new(*f)),
                daedalus_data::model::Value::Bool(b) => EdgePayload::Any(Arc::new(*b)),
                // Keep JSON-authored constants available to coercers (e.g., enum coercion).
                daedalus_data::model::Value::String(s) => {
                    EdgePayload::Any(Arc::new(s.to_string()))
                }
                other => EdgePayload::Value(other.clone()),
            };
            const_payloads.push((port.clone(), CorrelatedPayload::from_edge(payload)));
        }

        let (mut aligned_inputs, leftovers, ready) = align_drained_inputs(drained, &sync_groups);
        if log::log_enabled!(log::Level::Debug) && !sync_groups.is_empty() && !ready {
            log::debug!(
                "node sync groups not ready node={} groups={:?}",
                node_id,
                sync_groups
            );
        }
        if !sync_groups.is_empty() && !ready {
            aligned_inputs.clear();
        } else if has_drained || !has_incoming_edges {
            aligned_inputs.extend(const_payloads);
        }
        requeue_drained(leftovers, queues, edges);

        let current_corr_id = aligned_inputs
            .first()
            .map(|(_, cp)| cp.correlation_id)
            .unwrap_or_else(next_correlation_id);

        let mut port_overrides = HashMap::new();
        for group in &sync_groups {
            for port in &group.ports {
                port_overrides.insert(port.clone(), (group.backpressure.clone(), group.capacity));
            }
        }

        Self {
            inputs: aligned_inputs,
            borrowed_cache: std::cell::UnsafeCell::new(Vec::new()),
            sync_groups,
            port_overrides,
            current_corr_id,
            outgoing: outgoing_edges,
            has_incoming_edges,
            queues,
            telemetry,
            edges,
            seg_idx,
            node_id,
            warnings_seen,
            backpressure,
            #[cfg(feature = "gpu")]
            gpu,
            #[cfg(feature = "gpu")]
            target_compute,
            #[cfg(feature = "gpu")]
            payload_edges,
            const_coercers,
            output_movers,
        }
    }

    /// Returns the consumed inputs for this node.
    /// Borrow the drained inputs.
    ///
    /// ```no_run
    /// use daedalus_runtime::io::NodeIo;
    /// fn handler(io: &NodeIo) {
    ///     let _ = io.inputs();
    /// }
    /// ```
    pub fn inputs(&self) -> &[(String, CorrelatedPayload)] {
        &self.inputs
    }

    /// Whether this node has any incoming edges.
    pub fn has_incoming_edges(&self) -> bool {
        self.has_incoming_edges
    }

    fn take_input(&mut self, port: &str) -> Option<(usize, CorrelatedPayload)> {
        let idx = match self.inputs.iter().position(|(p, _)| p == port) {
            Some(idx) => idx,
            None => {
                if std::env::var_os("DAEDALUS_TRACE_MISSING_INPUTS").is_some() {
                    let ports: Vec<&str> = self.inputs.iter().map(|(p, _)| p.as_str()).collect();
                    eprintln!(
                        "daedalus-runtime: missing input node={} port={} available_ports={:?}",
                        self.node_id,
                        port,
                        ports
                    );
                }
                return None;
            }
        };
        let payload = self.inputs.remove(idx).1;
        Some((idx, payload))
    }

    fn restore_input(&mut self, idx: usize, port: &str, payload: CorrelatedPayload) {
        if idx <= self.inputs.len() {
            self.inputs.insert(idx, (port.to_string(), payload));
        } else {
            self.inputs.push((port.to_string(), payload));
        }
    }

    fn cache_borrowed<T: Any + Send + Sync>(&self, value: T) -> &T {
        let cache = unsafe { &mut *self.borrowed_cache.get() };
        cache.push(Box::new(value));
        cache
            .last()
            .and_then(|boxed| boxed.downcast_ref::<T>())
            .expect("borrowed cache type mismatch")
    }

    /// Returns sync groups metadata for this node.
    /// Return sync group metadata.
    ///
    /// ```no_run
    /// use daedalus_runtime::io::NodeIo;
    /// fn handler(io: &NodeIo) {
    ///     let _ = io.sync_groups();
    /// }
    /// ```
    pub fn sync_groups(&self) -> &[SyncGroup] {
        &self.sync_groups
    }

    /// Push a payload to all outgoing edges (fan-out). Optionally filter by port.
    /// Push a prepared payload to an output port.
    ///
    /// ```no_run
    /// use daedalus_runtime::io::NodeIo;
    /// use daedalus_runtime::executor::EdgePayload;
    /// fn handler(io: &mut NodeIo) {
    ///     io.push_output(Some("out"), EdgePayload::Unit);
    /// }
    /// ```
    pub fn push_output(&mut self, port: Option<&str>, payload: EdgePayload) {
        let correlated = CorrelatedPayload {
            correlation_id: self.current_corr_id,
            inner: payload,
            enqueued_at: Instant::now(),
        };
        self.push_correlated(port, correlated);
    }

    /// Push a pre-correlated payload (used by host-bridge style nodes).
    pub fn push_correlated_payload(&mut self, port: Option<&str>, correlated: CorrelatedPayload) {
        self.push_correlated(port, correlated);
    }

    fn push_correlated(&mut self, port: Option<&str>, correlated: CorrelatedPayload) {
        #[cfg(feature = "gpu")]
        let mut matches: Vec<(usize, String, EdgePolicyKind, BackpressureStrategy, Option<usize>, bool)> = Vec::new();
        #[cfg(not(feature = "gpu"))]
        let mut matches: Vec<(usize, String, EdgePolicyKind, BackpressureStrategy, Option<usize>)> = Vec::new();
        for edge_idx in &self.outgoing {
            if let Some((_, from_port, _, _, policy)) = self.edges.get(*edge_idx) {
                if let Some(p) = port
                    && !p.eq_ignore_ascii_case(from_port)
                {
                    continue;
                }
                let (bp_override, cap_override) = self
                    .port_overrides
                    .get(from_port)
                    .cloned()
                    .unwrap_or((None, None));
                let bp = bp_override.unwrap_or(self.backpressure.clone());
                #[cfg(feature = "gpu")]
                {
                    let needs_payload = self.payload_edges.contains(edge_idx);
                    matches.push((*edge_idx, from_port.clone(), policy.clone(), bp, cap_override, needs_payload));
                }
                #[cfg(not(feature = "gpu"))]
                {
                    matches.push((*edge_idx, from_port.clone(), policy.clone(), bp, cap_override));
                }
            }
        }

        if matches.len() == 1 {
            #[cfg(feature = "gpu")]
            let (edge_idx, from_port, policy, bp, cap_override, needs_payload) = matches.remove(0);
            #[cfg(not(feature = "gpu"))]
            let (edge_idx, from_port, policy, bp, cap_override) = matches.remove(0);
            if std::env::var_os("DAEDALUS_TRACE_EDGE_IO").is_some() {
                log::warn!(
                    "node output enqueue node={} port={} edge_idx={} payload={}",
                    self.node_id,
                    from_port,
                    edge_idx,
                    edge_payload_desc(&correlated.inner)
                );
            }
            if std::env::var_os("DAEDALUS_TRACE_EDGE_IO_STDERR").is_some() {
                eprintln!(
                    "node output enqueue node={} port={} edge_idx={} payload={}",
                    self.node_id,
                    from_port,
                    edge_idx,
                    edge_payload_desc(&correlated.inner)
                );
            }
            let mut effective_policy = policy;
            if let Some(cap) = cap_override {
                effective_policy = EdgePolicyKind::Bounded { cap };
            }
            #[cfg(feature = "gpu")]
            let mut correlated = if needs_payload {
                let mut updated = correlated;
                updated.inner = promote_payload_for_host(updated.inner);
                updated
            } else {
                correlated
            };
            apply_policy_owned(
                edge_idx,
                &effective_policy,
                correlated,
                self.queues,
                self.warnings_seen,
                self.telemetry,
                Some(format!("edge_{}_{}", self.node_id, from_port)),
                bp,
            );
            return;
        }

        #[cfg(feature = "gpu")]
        for (edge_idx, from_port, mut policy, bp, cap_override, needs_payload) in matches {
            if std::env::var_os("DAEDALUS_TRACE_EDGE_IO").is_some() {
                log::warn!(
                    "node output enqueue node={} port={} edge_idx={} payload={}",
                    self.node_id,
                    from_port,
                    edge_idx,
                    edge_payload_desc(&correlated.inner)
                );
            }
            if std::env::var_os("DAEDALUS_TRACE_EDGE_IO_STDERR").is_some() {
                eprintln!(
                    "node output enqueue node={} port={} edge_idx={} payload={}",
                    self.node_id,
                    from_port,
                    edge_idx,
                    edge_payload_desc(&correlated.inner)
                );
            }
            if let Some(cap) = cap_override {
                policy = EdgePolicyKind::Bounded { cap };
            }
            let mut payload = correlated.clone();
            if needs_payload {
                payload.inner = promote_payload_for_host(payload.inner);
            }
            apply_policy(
                edge_idx,
                &policy,
                &payload,
                self.queues,
                self.warnings_seen,
                self.telemetry,
                Some(format!("edge_{}_{}", self.node_id, from_port)),
                bp,
            );
        }
        #[cfg(not(feature = "gpu"))]
        for (edge_idx, from_port, mut policy, bp, cap_override) in matches {
            if std::env::var_os("DAEDALUS_TRACE_EDGE_IO").is_some() {
                log::warn!(
                    "node output enqueue node={} port={} edge_idx={} payload={}",
                    self.node_id,
                    from_port,
                    edge_idx,
                    edge_payload_desc(&correlated.inner)
                );
            }
            if std::env::var_os("DAEDALUS_TRACE_EDGE_IO_STDERR").is_some() {
                eprintln!(
                    "node output enqueue node={} port={} edge_idx={} payload={}",
                    self.node_id,
                    from_port,
                    edge_idx,
                    edge_payload_desc(&correlated.inner)
                );
            }
            if let Some(cap) = cap_override {
                policy = EdgePolicyKind::Bounded { cap };
            }
            apply_policy(
                edge_idx,
                &policy,
                &correlated,
                self.queues,
                self.warnings_seen,
                self.telemetry,
                Some(format!("edge_{}_{}", self.node_id, from_port)),
                bp,
            );
        }
    }

    #[cfg(feature = "gpu")]
    pub fn push_payload<T>(&mut self, port: Option<&str>, value: daedalus_gpu::Payload<T>)
    where
        T: daedalus_gpu::GpuSendable + Clone + Send + Sync + 'static,
        T::GpuRepr: Clone + Send + Sync + 'static,
    {
        match value {
            daedalus_gpu::Payload::Cpu(v) => {
                let payload = EdgePayload::Payload(daedalus_gpu::ErasedPayload::from_cpu::<T>(v));
                self.push_output(port, payload);
            }
            daedalus_gpu::Payload::Gpu(g) => {
                let payload = EdgePayload::Payload(daedalus_gpu::ErasedPayload::from_gpu::<T>(g));
                self.push_output(port, payload);
            }
        }
    }

    pub fn push_any<T: Any + Send + Sync + 'static>(&mut self, port: Option<&str>, value: T) {
        self.push_output(port, EdgePayload::Any(Arc::new(value)));
    }

    pub fn push_typed<T>(&mut self, port: Option<&str>, value: T)
    where
        T: Any + Clone + Send + Sync + 'static,
    {
        if std::env::var_os("DAEDALUS_TRACE_EDGE_IO").is_some() {
            let port_name = port.unwrap_or("<all>");
            log::warn!(
                "node output prepare node={} port={} type={}",
                self.node_id,
                port_name,
                std::any::type_name::<T>()
            );
        }
        if std::env::var_os("DAEDALUS_TRACE_EDGE_IO_STDERR").is_some() {
            let port_name = port.unwrap_or("<all>");
            eprintln!(
                "node output prepare node={} port={} type={}",
                self.node_id,
                port_name,
                std::any::type_name::<T>()
            );
        }
        match try_move_output(self.output_movers.as_ref(), value) {
            Ok(payload) => self.push_output(port, payload),
            Err(value) => {
                self.push_any(port, value);
            }
        }
    }

    /// Push a `Value` payload to an output port.
    ///
    /// ```no_run
    /// use daedalus_runtime::io::NodeIo;
    /// use daedalus_data::model::Value;
    /// fn handler(io: &mut NodeIo) {
    ///     io.push_value(Some("out"), Value::Int(1));
    /// }
    /// ```
    pub fn push_value(&mut self, port: Option<&str>, value: daedalus_data::model::Value) {
        self.push_output(port, EdgePayload::Value(value));
    }

    /// Iterate all inputs for a given port name.
    /// Iterate inputs for a named port.
    ///
    /// ```no_run
    /// use daedalus_runtime::io::NodeIo;
    /// fn handler(io: &NodeIo) {
    ///     for _payload in io.inputs_for("in") {}
    /// }
    /// ```
    pub fn inputs_for<'b>(&'b self, port: &str) -> impl Iterator<Item = &'b CorrelatedPayload> {
        self.inputs
            .iter()
            .filter(move |(p, _)| p == port)
            .map(|(_, payload)| payload)
    }

    /// Typed accessor for Any payloads.
    pub fn get_any<T: Any + Clone + Send + Sync>(&self, port: &str) -> Option<T> {
        #[cfg(feature = "gpu")]
        let want = TypeId::of::<T>();
        self.inputs_for(port).find_map(|p| match &p.inner {
            EdgePayload::Any(a) => {
                a.downcast_ref::<T>()
                    .cloned()
                    .or_else(|| self.coerce_const_any::<T>(a.as_ref()))
            }
            #[cfg(feature = "gpu")]
            EdgePayload::Payload(ep) => {
                if let Some(v) = ep.try_downcast_cpu_any::<T>() {
                    return Some(v);
                }
                if want == TypeId::of::<DynamicImage>() {
                    return ep.clone_cpu::<DynamicImage>().and_then(Self::dynamic_image_to_t::<T>);
                }
                if want == TypeId::of::<GrayImage>()
                    && let Some(gray) = ep.clone_cpu::<GrayImage>()
                {
                    let any_ref: &dyn Any = &gray;
                    return any_ref.downcast_ref::<T>().cloned();
                }
                if want == TypeId::of::<RgbImage>()
                    && let Some(rgb) = ep.clone_cpu::<RgbImage>()
                {
                    let any_ref: &dyn Any = &rgb;
                    return any_ref.downcast_ref::<T>().cloned();
                }
                if want == TypeId::of::<RgbaImage>()
                    && let Some(rgba) = ep.clone_cpu::<RgbaImage>()
                {
                    let any_ref: &dyn Any = &rgba;
                    return any_ref.downcast_ref::<T>().cloned();
                }
                ep.clone_cpu::<DynamicImage>().and_then(|img| {
                    if want == TypeId::of::<DynamicImage>()
                        || want == TypeId::of::<GrayImage>()
                        || want == TypeId::of::<GrayAlphaImage>()
                        || want == TypeId::of::<RgbImage>()
                        || want == TypeId::of::<RgbaImage>()
                    {
                        return Self::dynamic_image_to_t::<T>(img);
                    }
                    None
                }).or_else(|| {
                    if log::log_enabled!(log::Level::Debug)
                        && (want == TypeId::of::<DynamicImage>()
                            || want == TypeId::of::<GrayImage>()
                            || want == TypeId::of::<GrayAlphaImage>()
                            || want == TypeId::of::<RgbImage>()
                            || want == TypeId::of::<RgbaImage>())
                    {
                        log::debug!("payload type mismatch port={} payload={:?}", port, ep);
                    }
                    None
                })
            },
            #[cfg(feature = "gpu")]
            EdgePayload::GpuImage(h)
                if TypeId::of::<T>() == TypeId::of::<daedalus_gpu::GpuImageHandle>() =>
            {
                let any_ref: &dyn Any = h;
                any_ref.downcast_ref::<T>().cloned()
            }
            _ => None,
        })
    }

    /// Borrow a typed Any payload without cloning.
    pub fn get_any_ref<T: Any + Send + Sync>(&self, port: &str) -> Option<&T> {
        self.inputs_for(port).find_map(|p| match &p.inner {
            EdgePayload::Any(a) => a.downcast_ref::<T>(),
            #[cfg(feature = "gpu")]
            EdgePayload::GpuImage(h)
                if TypeId::of::<T>() == TypeId::of::<daedalus_gpu::GpuImageHandle>() =>
            {
                let any_ref: &dyn Any = h;
                any_ref.downcast_ref::<T>()
            }
            _ => None,
        })
    }

    /// Borrow a typed input with constant coercion support.
    pub fn get_typed_ref<T>(&self, port: &str) -> Option<&T>
    where
        T: Any + Clone + Send + Sync,
    {
        if let Some(v) = self.get_any_ref::<T>(port) {
            return Some(v);
        }
        if let Some(v) = self.get_any::<T>(port) {
            return Some(self.cache_borrowed(v));
        }

        if let Some(v) = self.get_any_ref::<i64>(port)
            && let Some(t) = Self::coerce_from_i64::<T>(*v)
        {
            return Some(self.cache_borrowed(t));
        }
        if let Some(v) = self.get_any_ref::<f64>(port)
            && let Some(t) = Self::coerce_from_f64::<T>(*v)
        {
            return Some(self.cache_borrowed(t));
        }
        if let Some(v) = self.get_any_ref::<bool>(port)
            && let Some(t) = {
                let any_ref: &dyn Any = v;
                any_ref.downcast_ref::<T>().cloned()
            }
        {
            return Some(self.cache_borrowed(t));
        }
        if let Some(v) = self.get_any_ref::<String>(port)
            && let Some(t) = {
                let any_ref: &dyn Any = v;
                any_ref.downcast_ref::<T>().cloned()
            }
        {
            return Some(self.cache_borrowed(t));
        }
        if let Some(v) = self.get_value(port)
            && let Some(t) = self.coerce_from_value::<T>(v)
        {
            return Some(self.cache_borrowed(t));
        }

        None
    }

    /// Move a typed Any payload, cloning only when shared.
    pub fn get_any_mut<T>(&mut self, port: &str) -> Option<T>
    where
        T: Any + Clone + Send + Sync,
    {
        let (idx, payload) = self.take_input(port)?;
        let mut handled = None;
        let mut payload = payload;
        match std::mem::replace(&mut payload.inner, EdgePayload::Unit) {
            EdgePayload::Any(a) => match Arc::downcast::<T>(a) {
                Ok(arc) => {
                    handled = Some(match Arc::try_unwrap(arc) {
                        Ok(v) => v,
                        Err(arc) => (*arc).clone(),
                    });
                }
                Err(a) => {
                    payload.inner = EdgePayload::Any(a);
                }
            },
            other => {
                payload.inner = other;
            }
        }
        if handled.is_none() {
            self.restore_input(idx, port, payload);
        }
        handled
    }

    /// Move a typed input with constant coercion support, cloning only when shared.
    pub fn get_typed_mut<T>(&mut self, port: &str) -> Option<T>
    where
        T: Any + Clone + Send + Sync,
    {
        #[cfg(feature = "gpu")]
        let want = TypeId::of::<T>();
        let (idx, mut payload) = self.take_input(port)?;
        let mut out: Option<T> = None;
        match std::mem::replace(&mut payload.inner, EdgePayload::Unit) {
            #[cfg(feature = "gpu")]
            EdgePayload::Payload(ep) => {
                let mut ep_opt = Some(ep);
                let downcast_owned = |value: Box<dyn Any + Send + Sync>| {
                    value.downcast::<T>().ok().map(|boxed| *boxed)
                };
                if std::env::var_os("DAEDALUS_TRACE_PAYLOAD_TAKE").is_some() {
                    if let Some(ep) = ep_opt.as_ref() {
                        let clone_ok = if want == TypeId::of::<DynamicImage>() {
                            ep.clone_cpu::<DynamicImage>().is_some()
                        } else if want == TypeId::of::<GrayImage>() {
                            ep.clone_cpu::<GrayImage>().is_some()
                        } else if want == TypeId::of::<RgbImage>() {
                            ep.clone_cpu::<RgbImage>().is_some()
                        } else if want == TypeId::of::<RgbaImage>() {
                            ep.clone_cpu::<RgbaImage>().is_some()
                        } else {
                            false
                        };
                        eprintln!(
                            "daedalus-runtime: payload probe node={} port={} type={} clone_cpu={}",
                            self.node_id,
                            port,
                            std::any::type_name::<T>(),
                            clone_ok
                        );
                    }
                }
                if want == TypeId::of::<DynamicImage>() {
                    if let Some(ep) = ep_opt.take() {
                        match ep.take_cpu::<DynamicImage>() {
                            Ok(cpu) => out = downcast_owned(Box::new(cpu)),
                            Err(rest) => {
                                if let Some(cpu) = rest.clone_cpu::<DynamicImage>() {
                                    out = downcast_owned(Box::new(cpu));
                                } else {
                                    ep_opt = Some(rest);
                                }
                            }
                        }
                    }
                } else if want == TypeId::of::<GrayImage>() {
                    if let Some(ep) = ep_opt.take() {
                        match ep.take_cpu::<GrayImage>() {
                            Ok(cpu) => out = downcast_owned(Box::new(cpu)),
                            Err(rest) => {
                                if let Some(cpu) = rest.clone_cpu::<GrayImage>() {
                                    out = downcast_owned(Box::new(cpu));
                                } else {
                                    ep_opt = Some(rest);
                                }
                            }
                        }
                    }
                } else if want == TypeId::of::<RgbImage>() {
                    if let Some(ep) = ep_opt.take() {
                        match ep.take_cpu::<RgbImage>() {
                            Ok(cpu) => out = downcast_owned(Box::new(cpu)),
                            Err(rest) => {
                                if let Some(cpu) = rest.clone_cpu::<RgbImage>() {
                                    out = downcast_owned(Box::new(cpu));
                                } else {
                                    ep_opt = Some(rest);
                                }
                            }
                        }
                    }
                } else if want == TypeId::of::<RgbaImage>() {
                    if let Some(ep) = ep_opt.take() {
                        match ep.take_cpu::<RgbaImage>() {
                            Ok(cpu) => out = downcast_owned(Box::new(cpu)),
                            Err(rest) => {
                                if let Some(cpu) = rest.clone_cpu::<RgbaImage>() {
                                    out = downcast_owned(Box::new(cpu));
                                } else {
                                    ep_opt = Some(rest);
                                }
                            }
                        }
                    }
                }

                if out.is_none() {
                    if let Some(ep) = ep_opt.as_ref()
                        && (want == TypeId::of::<DynamicImage>()
                            || want == TypeId::of::<GrayImage>()
                            || want == TypeId::of::<GrayAlphaImage>()
                            || want == TypeId::of::<RgbImage>()
                            || want == TypeId::of::<RgbaImage>())
                        && let Some(img) = ep.clone_cpu::<DynamicImage>()
                    {
                        out = Self::dynamic_image_to_t::<T>(img);
                    }
                }
                if out.is_none()
                    && let Some(ep) = ep_opt
                {
                    payload.inner = EdgePayload::Payload(ep);
                }
            }
            EdgePayload::Any(a) => {
                let any = a;
                match Arc::downcast::<T>(any) {
                    Ok(arc) => {
                        out = Some(match Arc::try_unwrap(arc) {
                            Ok(v) => v,
                            Err(arc) => (*arc).clone(),
                        });
                    }
                    Err(any) => match Arc::downcast::<i64>(any) {
                        Ok(arc) => {
                            let v = match Arc::try_unwrap(arc) {
                                Ok(v) => v,
                                Err(arc) => *arc,
                            };
                            out = Self::coerce_from_i64::<T>(v);
                        }
                        Err(any) => match Arc::downcast::<f64>(any) {
                            Ok(arc) => {
                                let v = match Arc::try_unwrap(arc) {
                                    Ok(v) => v,
                                    Err(arc) => *arc,
                                };
                                out = Self::coerce_from_f64::<T>(v);
                            }
                            Err(any) => match Arc::downcast::<bool>(any) {
                                Ok(arc) => {
                                    let v = match Arc::try_unwrap(arc) {
                                        Ok(v) => v,
                                        Err(arc) => *arc,
                                    };
                                    let any_ref: &dyn Any = &v;
                                    out = any_ref.downcast_ref::<T>().cloned();
                                }
                                Err(any) => match Arc::downcast::<String>(any) {
                                    Ok(arc) => {
                                        let v = match Arc::try_unwrap(arc) {
                                            Ok(v) => v,
                                            Err(arc) => (*arc).clone(),
                                        };
                                        let any_ref: &dyn Any = &v;
                                        out = any_ref.downcast_ref::<T>().cloned();
                                    }
                                    Err(any) => match Arc::downcast::<daedalus_data::model::Value>(any) {
                                        Ok(arc) => {
                                            let v = match Arc::try_unwrap(arc) {
                                                Ok(v) => v,
                                                Err(arc) => (*arc).clone(),
                                            };
                                            out = self.coerce_from_value::<T>(&v);
                                        }
                                        Err(any) => {
                                            payload.inner = EdgePayload::Any(any);
                                        }
                                    },
                                },
                            },
                        },
                    },
                }
            }
            EdgePayload::Value(v) => {
                out = self.coerce_from_value::<T>(&v);
                payload.inner = EdgePayload::Value(v);
            }
            other => {
                payload.inner = other;
            }
        }

        if out.is_none() {
            if std::env::var_os("DAEDALUS_TRACE_MISSING_INPUTS").is_some() {
                let desc = match &payload.inner {
                    EdgePayload::Any(a) => format!("Any({})", std::any::type_name_of_val(a.as_ref())),
                    #[cfg(feature = "gpu")]
                    EdgePayload::Payload(ep) => format!("Payload({ep:?})"),
                    #[cfg(feature = "gpu")]
                    EdgePayload::GpuImage(_) => "GpuImage".to_string(),
                    EdgePayload::Value(v) => format!("Value({v:?})"),
                    EdgePayload::Bytes(_) => "Bytes".to_string(),
                    EdgePayload::Unit => "Unit".to_string(),
                };
                eprintln!(
                    "daedalus-runtime: input mismatch node={} port={} expected={} payload={}",
                    self.node_id,
                    port,
                    std::any::type_name::<T>(),
                    desc
                );
            }
            self.restore_input(idx, port, payload);
        }
        out
    }

    /// Collect all typed Any payloads for a port (in arrival order).
    pub fn get_any_all<T: Any + Clone + Send + Sync>(&self, port: &str) -> Vec<T> {
        #[cfg(feature = "gpu")]
        let want = TypeId::of::<T>();
        let mut out: Vec<T> = Vec::new();
        for p in self.inputs_for(port) {
            match &p.inner {
                EdgePayload::Any(a) => {
                    if let Some(v) = a
                        .downcast_ref::<T>()
                        .cloned()
                        .or_else(|| self.coerce_const_any::<T>(a.as_ref()))
                    {
                        out.push(v);
                    }
                }
                #[cfg(feature = "gpu")]
                EdgePayload::Payload(ep) => {
                    if let Some(img) = ep.clone_cpu::<DynamicImage>()
                        && (want == TypeId::of::<DynamicImage>()
                            || want == TypeId::of::<GrayImage>()
                            || want == TypeId::of::<GrayAlphaImage>()
                            || want == TypeId::of::<RgbImage>()
                            || want == TypeId::of::<RgbaImage>())
                        && let Some(v) = Self::dynamic_image_to_t::<T>(img)
                    {
                        out.push(v);
                    }
                }
                #[cfg(feature = "gpu")]
                EdgePayload::GpuImage(h)
                    if TypeId::of::<T>() == TypeId::of::<daedalus_gpu::GpuImageHandle>() =>
                {
                    let any_ref: &dyn Any = h;
                    if let Some(v) = any_ref.downcast_ref::<T>().cloned() {
                        out.push(v);
                    }
                }
                _ => {}
            }
        }
        out
    }

    /// Collect typed Any payloads for indexed fan-in ports `{prefix}{N}` ordered by `N`.
    ///
    /// Example: `prefix="in"` collects from `in0`, `in1`, ... in numeric order.
    pub fn get_any_all_fanin<T: Any + Clone + Send + Sync>(&self, prefix: &str) -> Vec<T> {
        self.get_any_all_fanin_indexed::<T>(prefix)
            .into_iter()
            .map(|(_, v)| v)
            .collect()
    }

    /// Collect typed Any payloads for indexed fan-in ports `{prefix}{N}` ordered by `N`,
    /// preserving the parsed index.
    pub fn get_any_all_fanin_indexed<T: Any + Clone + Send + Sync>(&self, prefix: &str) -> Vec<(u32, T)> {
        let mut ports: BTreeMap<u32, String> = BTreeMap::new();
        for (port, _) in &self.inputs {
            if let Some(idx) = parse_indexed_port(prefix, port) {
                ports.entry(idx).or_insert_with(|| port.clone());
            }
        }
        let mut out = Vec::with_capacity(ports.len());
        for (idx, port) in ports {
            if let Some(v) = self.get_any::<T>(&port) {
                out.push((idx, v));
            }
        }
        out
    }

    fn coerce_const_any<T: Any + Clone>(&self, v: &dyn Any) -> Option<T> {
        use daedalus_data::model::Value as V;

        if let Some(i) = v
            .downcast_ref::<i64>()
            .copied()
        {
            return Self::coerce_from_i64::<T>(i)
                .or_else(|| self.coerce_via_registry::<T>(&V::Int(i)));
        }

        if let Some(f) = v
            .downcast_ref::<f64>()
            .copied()
        {
            return Self::coerce_from_f64::<T>(f)
                .or_else(|| self.coerce_via_registry::<T>(&V::Float(f)));
        }

        if let Some(b) = v
            .downcast_ref::<bool>()
            .copied()
        {
            let any_ref: &dyn Any = &b;
            return any_ref
                .downcast_ref::<T>()
                .cloned()
                .or_else(|| self.coerce_via_registry::<T>(&V::Bool(b)));
        }

        if let Some(s) = v
            .downcast_ref::<String>()
            .cloned()
        {
            let any_ref: &dyn Any = &s;
            return any_ref
                .downcast_ref::<T>()
                .cloned()
                .or_else(|| self.coerce_via_registry::<T>(&V::String(s.into())));
        }

        if let Some(val) = v
            .downcast_ref::<daedalus_data::model::Value>()
            .cloned()
        {
            return self.coerce_from_value::<T>(&val);
        }

        None
    }

    fn coerce_via_registry<T: Any + Clone>(&self, v: &daedalus_data::model::Value) -> Option<T> {
        let key = std::any::type_name::<T>();
        let global = GLOBAL_CONST_COERCERS.get_or_init(new_const_coercer_map);
        let map = self.const_coercers.as_ref().unwrap_or(global);
        let guard = map.read().ok()?;
        let coercer = guard.get(key)?;
        let out = coercer(v)?;
        out.downcast::<T>().ok().map(|b| (*b).clone())
    }

    fn coerce_from_i64<T: Any + Clone>(v: i64) -> Option<T> {
        use std::any::TypeId;
        let want = TypeId::of::<T>();

        macro_rules! cast_int {
            ($t:ty) => {{
                if want == TypeId::of::<$t>() {
                    let out: $t = <$t>::try_from(v).ok()?;
                    let any_ref: &dyn Any = &out;
                    return any_ref.downcast_ref::<T>().cloned();
                }
            }};
        }

        cast_int!(i8);
        cast_int!(i16);
        cast_int!(i32);
        cast_int!(i64);
        cast_int!(isize);
        cast_int!(u8);
        cast_int!(u16);
        cast_int!(u32);
        cast_int!(u64);
        cast_int!(usize);

        if want == TypeId::of::<f32>() {
            let out = v as f32;
            let any_ref: &dyn Any = &out;
            return any_ref.downcast_ref::<T>().cloned();
        }
        if want == TypeId::of::<f64>() {
            let out = v as f64;
            let any_ref: &dyn Any = &out;
            return any_ref.downcast_ref::<T>().cloned();
        }

        None
    }

    fn coerce_from_f64<T: Any + Clone>(v: f64) -> Option<T> {
        use std::any::TypeId;
        let want = TypeId::of::<T>();

        if want == TypeId::of::<f32>() {
            let out = v as f32;
            let any_ref: &dyn Any = &out;
            return any_ref.downcast_ref::<T>().cloned();
        }
        if want == TypeId::of::<f64>() {
            let out = v;
            let any_ref: &dyn Any = &out;
            return any_ref.downcast_ref::<T>().cloned();
        }

        // Allow float -> int when it's integral.
        if v.fract() == 0.0 {
            let as_i = v as i64;
            return Self::coerce_from_i64::<T>(as_i);
        }

        None
    }

    /// Best-effort typed accessor that supports constant defaults.
    ///
    /// Daedalus graph JSON encodes constant inputs as `daedalus_data::model::Value`. At runtime we
    /// inject these into the node as `Any` payloads (e.g. `i64`, `f64`, `bool`, `String`, or
    /// `Value`). This helper bridges the gap so node handlers can request their native types
    /// (e.g. `u32`, `f32`, enums) without failing a `TypeId` downcast.
    ///
    /// This is intended for scalar/enum config inputs, not large payload types (e.g. image buffers).
    pub fn get_typed<T>(&self, port: &str) -> Option<T>
    where
        T: Any + Clone + Send + Sync,
    {
        let want = std::any::TypeId::of::<T>();
        let initial = self.get_any::<T>(port);
        if log::log_enabled!(log::Level::Debug) && want == std::any::TypeId::of::<image::DynamicImage>() {
            log::debug!(
                "get_typed dynamic_image port={} has_any={}",
                port,
                initial.is_some()
            );
        }
        if let Some(v) = initial {
            return Some(v);
        }

        // Common scalar carriers produced by const injection.
        if let Some(v) = self.get_any::<i64>(port)
            && let Some(t) = Self::coerce_from_i64::<T>(v)
        {
            return Some(t);
        }
        if let Some(v) = self.get_any::<f64>(port)
            && let Some(t) = Self::coerce_from_f64::<T>(v)
        {
            return Some(t);
        }
        if let Some(v) = self.get_any::<bool>(port)
            && let Some(t) = {
                let any_ref: &dyn Any = &v;
                any_ref.downcast_ref::<T>().cloned()
            }
        {
            return Some(t);
        }
        if let Some(v) = self.get_any::<String>(port)
            && let Some(t) = {
                let any_ref: &dyn Any = &v;
                any_ref.downcast_ref::<T>().cloned()
            }
        {
            return Some(t);
        }
        if let Some(v) = self.get_any::<daedalus_data::model::Value>(port)
            && let Some(t) = self.coerce_from_value::<T>(&v) {
                return Some(t);
            }

        if let Some(v) = self.get_value(port)
            && let Some(t) = self.coerce_from_value::<T>(v) {
                return Some(t);
            }

        None
    }

    fn coerce_from_value<T: Any + Clone>(&self, v: &daedalus_data::model::Value) -> Option<T> {
        use daedalus_data::model::Value as V;

        let any_ref: &dyn Any = v;
        if let Some(t) = any_ref.downcast_ref::<T>().cloned() {
            return Some(t);
        }

        if let Some(name) = Self::enum_name_from_index::<T>(v)
            && let Some(t) = {
                let any_ref: &dyn Any = &name;
                any_ref.downcast_ref::<T>().cloned()
            }
        {
            return Some(t);
        }

        let j: Option<daedalus_data::model::Value> = match v {
            V::Int(i) => {
                if let Some(t) = Self::coerce_from_i64::<T>(*i) {
                    return Some(t);
                }
                Some(V::Int(*i))
            }
            V::Float(f) => {
                if let Some(t) = Self::coerce_from_f64::<T>(*f) {
                    return Some(t);
                }
                Some(V::Float(*f))
            }
            V::Bool(b) => {
                let any_ref: &dyn Any = b;
                if let Some(t) = any_ref.downcast_ref::<T>().cloned() {
                    return Some(t);
                }
                Some(V::Bool(*b))
            }
            V::String(s) => {
                let owned = s.clone().into_owned();
                let any_ref: &dyn Any = &owned;
                if let Some(t) = any_ref.downcast_ref::<T>().cloned() {
                    return Some(t);
                }
                // Enum coercion is handled via the registered const coercers.
                Some(V::String(owned.into()))
            }
            // Allow enum coercers to handle enum payloads.
            V::Enum(_) => Some(v.clone()),
            other => Some(other.clone()),
        };

        j.as_ref().and_then(|v| self.coerce_via_registry::<T>(v))
    }

    fn enum_name_from_index<T: Any>(v: &daedalus_data::model::Value) -> Option<String> {
        let Value::Int(raw) = v else { return None };
        if *raw < 0 {
            return None;
        }
        let idx = *raw as usize;
        let expr = typing::override_type_expr::<T>()
            .or_else(|| typing::lookup_type_by_rust_name(std::any::type_name::<T>()));
        match expr {
            Some(TypeExpr::Enum(variants)) => variants.get(idx).map(|ev| ev.name.clone()),
            _ => None,
        }
    }

    /// Get a raw `Any` reference for capability-based dispatch.
    pub fn get_any_raw(&self, port: &str) -> Option<&dyn Any> {
        self.inputs_for(port).find_map(|p| match &p.inner {
            EdgePayload::Any(a) => Some(a.as_ref() as &dyn Any),
            _ => None,
        })
    }

    #[cfg(feature = "gpu")]
    pub fn get_erased_payload(&self, port: &str) -> Option<&daedalus_gpu::ErasedPayload> {
        self.inputs_for(port).find_map(|p| match &p.inner {
            EdgePayload::Payload(ep) => Some(ep),
            _ => None,
        })
    }

    #[cfg(feature = "gpu")]
    pub fn get_payload<T>(&self, port: &str) -> Option<daedalus_gpu::Payload<T>>
    where
        T: daedalus_gpu::GpuSendable + Clone + Send + Sync + 'static,
        T::GpuRepr: Clone + Send + Sync + 'static,
    {
        // Prefer GPU only when:
        // - the node declares a GPU affinity, AND
        // - a GPU context is actually available in this execution.
        //
        // This matters for `GpuPreferred` nodes: the executor may fall back to CPU when no GPU
        // is available, in which case payload decoding must also accept CPU values.
        let wants_gpu = self.gpu.is_some()
            && matches!(
                self.target_compute,
                daedalus_planner::ComputeAffinity::GpuPreferred
                    | daedalus_planner::ComputeAffinity::GpuRequired
            );
        for p in self.inputs_for(port) {
            match &p.inner {
                EdgePayload::Payload(ep) => {
                    if wants_gpu {
                        if let Some(g) = ep.clone_gpu::<T>() {
                            return Some(daedalus_gpu::Payload::Gpu(g));
                        }
                        if let Some(cpu) = ep.clone_cpu::<T>()
                            && let Some(ctx) = &self.gpu
                            && let Ok(handle) = cpu.upload(ctx)
                        {
                            return Some(daedalus_gpu::Payload::Gpu(handle));
                        }
                    } else {
                        if let Some(cpu) = ep.clone_cpu::<T>() {
                            return Some(daedalus_gpu::Payload::Cpu(cpu));
                        }
                        if let Some(g) = ep.clone_gpu::<T>()
                            && let Some(ctx) = &self.gpu
                            && let Ok(cpu) = T::download(&g, ctx)
                        {
                            return Some(daedalus_gpu::Payload::Cpu(cpu));
                        }
                    }
                }
            EdgePayload::Any(a) => {
                    // Plugins sometimes pass a `Payload<T>` through `Any` (e.g. when a node
                    // signature uses a type alias that the `#[node]` macro can't see through).
                    // Accept that representation here so downstream `Payload<T>` inputs can still
                    // be satisfied.
                    let payload_any: Option<daedalus_gpu::Payload<T>> = a
                        .downcast_ref::<daedalus_gpu::Payload<T>>()
                        .cloned();
                    if let Some(payload_any) = payload_any {
                        if wants_gpu {
                            match payload_any {
                                daedalus_gpu::Payload::Gpu(g) => {
                                    return Some(daedalus_gpu::Payload::Gpu(g));
                                }
                                daedalus_gpu::Payload::Cpu(cpu) => {
                                    if let Some(ctx) = &self.gpu
                                        && let Ok(handle) = cpu.upload(ctx)
                                    {
                                        return Some(daedalus_gpu::Payload::Gpu(handle));
                                    }
                                }
                            }
                        } else {
                            match payload_any {
                                daedalus_gpu::Payload::Cpu(cpu) => {
                                    return Some(daedalus_gpu::Payload::Cpu(cpu));
                                }
                                daedalus_gpu::Payload::Gpu(g) => {
                                    if let Some(ctx) = &self.gpu
                                        && let Ok(cpu) = T::download(&g, ctx)
                                    {
                                        return Some(daedalus_gpu::Payload::Cpu(cpu));
                                    }
                                }
                            }
                        }
                    }

                    if wants_gpu {
                        let cpu = a
                            .downcast_ref::<T>()
                            .cloned();
                        if let Some(cpu) = cpu
                            && let Some(ctx) = &self.gpu
                            && let Ok(handle) = cpu.upload(ctx)
                        {
                            return Some(daedalus_gpu::Payload::Gpu(handle));
                        }
                        if let Some(converted) = crate::convert::convert_arc::<T>(a)
                            && let Some(ctx) = &self.gpu
                            && let Ok(handle) = converted.upload(ctx)
                        {
                            return Some(daedalus_gpu::Payload::Gpu(handle));
                        }
                        if let Some(ep) = a.downcast_ref::<daedalus_gpu::ErasedPayload>()
                            && let Ok(uploaded) = ep.upload(self.gpu.as_ref()?)
                            && let Some(g) = uploaded.as_gpu::<T>()
                        {
                            return Some(daedalus_gpu::Payload::Gpu(g.clone()));
                        }
                    } else {
                        let cpu = a
                            .downcast_ref::<T>()
                            .cloned();
                        if let Some(cpu) = cpu {
                            return Some(daedalus_gpu::Payload::Cpu(cpu));
                        }
                        if let Some(converted) = crate::convert::convert_arc::<T>(a) {
                            return Some(daedalus_gpu::Payload::Cpu(converted));
                        }
                        let g = a
                            .downcast_ref::<T::GpuRepr>()
                            .cloned();
                        if let Some(g) = g
                            && let Some(ctx) = &self.gpu
                            && let Ok(cpu) = T::download(&g, ctx)
                        {
                            return Some(daedalus_gpu::Payload::Cpu(cpu));
                        }
                        if let Some(ep) = a.downcast_ref::<daedalus_gpu::ErasedPayload>()
                            && let Ok(downloaded) = ep.download(self.gpu.as_ref()?)
                            && let Some(cpu) = downloaded.as_cpu::<T>()
                        {
                            return Some(daedalus_gpu::Payload::Cpu(cpu.clone()));
                        }
                    }
                }
                EdgePayload::GpuImage(h) => {
                    if TypeId::of::<T::GpuRepr>() == TypeId::of::<daedalus_gpu::GpuImageHandle>() {
                        if wants_gpu {
                            let any_ref: &dyn Any = h;
                            if let Some(repr) = any_ref.downcast_ref::<T::GpuRepr>() {
                                return Some(daedalus_gpu::Payload::Gpu(repr.clone()));
                            }
                        } else if let Some(ctx) = &self.gpu {
                            let any_ref: &dyn Any = h;
                            if let Some(repr) = any_ref.downcast_ref::<T::GpuRepr>()
                                && let Ok(cpu) = T::download(repr, ctx)
                            {
                                return Some(daedalus_gpu::Payload::Cpu(cpu));
                            }
                        }
                    }
                }
                _ => {}
            }
        }
        None
    }

    #[cfg(feature = "gpu")]
    pub fn get_payload_mut<T>(&mut self, port: &str) -> Option<daedalus_gpu::Payload<T>>
    where
        T: daedalus_gpu::GpuSendable + Clone + Send + Sync + 'static,
        T::GpuRepr: Clone + Send + Sync + 'static,
    {
        let wants_gpu = self.gpu.is_some()
            && matches!(
                self.target_compute,
                daedalus_planner::ComputeAffinity::GpuPreferred
                    | daedalus_planner::ComputeAffinity::GpuRequired
            );
        let (idx, mut payload) = self.take_input(port)?;

        let mut out: Option<daedalus_gpu::Payload<T>> = None;
        match std::mem::replace(&mut payload.inner, EdgePayload::Unit) {
            EdgePayload::Payload(ep) => {
                let mut ep_opt = Some(ep);
                if wants_gpu {
                    if let Some(ep) = ep_opt.as_ref() {
                        if let Some(g) = ep.clone_gpu::<T>() {
                            out = Some(daedalus_gpu::Payload::Gpu(g));
                        } else if let Some(cpu) = ep.clone_cpu::<T>()
                            && let Some(ctx) = &self.gpu
                            && let Ok(handle) = cpu.upload(ctx)
                        {
                            out = Some(daedalus_gpu::Payload::Gpu(handle));
                        }
                    }
                } else if let Some(ep) = ep_opt.take() {
                    match ep.take_cpu::<T>() {
                        Ok(cpu) => out = Some(daedalus_gpu::Payload::Cpu(cpu)),
                        Err(rest) => {
                            ep_opt = Some(rest);
                            if let Some(ep) = ep_opt.as_ref() {
                                if let Some(cpu) = ep.clone_cpu::<T>() {
                                    out = Some(daedalus_gpu::Payload::Cpu(cpu));
                                } else if let Some(g) = ep.clone_gpu::<T>()
                                    && let Some(ctx) = &self.gpu
                                    && let Ok(cpu) = T::download(&g, ctx)
                                {
                                    out = Some(daedalus_gpu::Payload::Cpu(cpu));
                                }
                            }
                        }
                    }
                }
                if out.is_none()
                    && let Some(ep) = ep_opt
                {
                    payload.inner = EdgePayload::Payload(ep);
                }
            }
            EdgePayload::Any(a) => {
                let any = a;
                match Arc::downcast::<daedalus_gpu::Payload<T>>(any) {
                    Ok(arc) => {
                        let payload_any = match Arc::try_unwrap(arc) {
                            Ok(v) => v,
                            Err(arc) => (*arc).clone(),
                        };
                        if wants_gpu {
                            match payload_any {
                                daedalus_gpu::Payload::Gpu(g) => out = Some(daedalus_gpu::Payload::Gpu(g)),
                                daedalus_gpu::Payload::Cpu(cpu) => {
                                    if let Some(ctx) = &self.gpu
                                        && let Ok(handle) = cpu.upload(ctx)
                                    {
                                        out = Some(daedalus_gpu::Payload::Gpu(handle));
                                    }
                                }
                            }
                        } else {
                            match payload_any {
                                daedalus_gpu::Payload::Cpu(cpu) => out = Some(daedalus_gpu::Payload::Cpu(cpu)),
                                daedalus_gpu::Payload::Gpu(g) => {
                                    if let Some(ctx) = &self.gpu
                                        && let Ok(cpu) = T::download(&g, ctx)
                                    {
                                        out = Some(daedalus_gpu::Payload::Cpu(cpu));
                                    }
                                }
                            }
                        }
                    }
                    Err(any) => {
                        if wants_gpu {
                            match Arc::downcast::<T>(any) {
                                Ok(arc) => {
                                    let cpu = match Arc::try_unwrap(arc) {
                                        Ok(v) => v,
                                        Err(arc) => (*arc).clone(),
                                    };
                                    if let Some(ctx) = &self.gpu
                                        && let Ok(handle) = cpu.upload(ctx)
                                    {
                                        out = Some(daedalus_gpu::Payload::Gpu(handle));
                                    }
                                }
                                Err(any) => {
                                    payload.inner = EdgePayload::Any(any);
                                }
                            }
                        } else {
                            match Arc::downcast::<T>(any) {
                                Ok(arc) => {
                                    let cpu = match Arc::try_unwrap(arc) {
                                        Ok(v) => v,
                                        Err(arc) => (*arc).clone(),
                                    };
                                    out = Some(daedalus_gpu::Payload::Cpu(cpu));
                                }
                                Err(any) => {
                                    payload.inner = EdgePayload::Any(any);
                                }
                            }
                        }
                    }
                }
            }
            EdgePayload::GpuImage(h) => {
                payload.inner = EdgePayload::GpuImage(h);
            }
            other => {
                payload.inner = other;
            }
        }

        if out.is_none() {
            self.restore_input(idx, port, payload);
        }
        out
    }

    #[cfg(feature = "gpu")]
    fn convert_incoming(
        mut payload: CorrelatedPayload,
        edge_idx: usize,
        entries: &HashSet<usize>,
        exits: &HashSet<usize>,
        gpu: Option<&daedalus_gpu::GpuContextHandle>,
    ) -> CorrelatedPayload {
        let Some(ctx) = gpu else {
            return payload;
        };
        if entries.contains(&edge_idx) {
            payload.inner = match payload.inner {
                EdgePayload::Any(ref a) => {
                    if let Some(ep) = a.downcast_ref::<daedalus_gpu::ErasedPayload>() {
                        ep.upload(ctx)
                            .map(EdgePayload::Payload)
                            .unwrap_or_else(|_| EdgePayload::Any(a.clone()))
                    } else if let Some(img) = a.downcast_ref::<daedalus_gpu::GpuImageHandle>() {
                        EdgePayload::GpuImage(img.clone())
                    } else {
                        EdgePayload::Any(a.clone())
                    }
                }
                EdgePayload::Payload(ref ep) => ep
                    .upload(ctx)
                    .map(EdgePayload::Payload)
                    .unwrap_or_else(|_| EdgePayload::Payload(ep.clone())),
                other => other,
            };
        } else if exits.contains(&edge_idx) {
            payload.inner = match payload.inner {
                EdgePayload::Any(ref a) => {
                    if let Some(ep) = a.downcast_ref::<daedalus_gpu::ErasedPayload>() {
                        ep.download(ctx)
                            .map(EdgePayload::Payload)
                            .unwrap_or_else(|_| EdgePayload::Any(a.clone()))
                    } else {
                        EdgePayload::Any(a.clone())
                    }
                }
                EdgePayload::Payload(ref ep) => ep
                    .download(ctx)
                    .map(EdgePayload::Payload)
                    .unwrap_or_else(|_| EdgePayload::Payload(ep.clone())),
                EdgePayload::GpuImage(h) => EdgePayload::GpuImage(h),
                other => other,
            };
        }
        payload
    }

    /// Convenience accessor for value payloads.
    /// Get a structured `Value` payload for a port.
    ///
    /// ```no_run
    /// use daedalus_runtime::io::NodeIo;
    /// fn handler(io: &NodeIo) {
    ///     let _ = io.get_value("in");
    /// }
    /// ```
    pub fn get_value(&self, port: &str) -> Option<&Value> {
        self.inputs_for(port).find_map(|p| match &p.inner {
            EdgePayload::Value(v) => Some(v),
            _ => None,
        })
    }

    pub fn get_int(&self, port: &str) -> Option<i64> {
        self.get_value(port).and_then(|v| match v {
            daedalus_data::model::Value::Int(i) => Some(*i),
            _ => None,
        })
    }

    pub fn get_float(&self, port: &str) -> Option<f64> {
        self.get_value(port).and_then(|v| match v {
            daedalus_data::model::Value::Float(f) => Some(*f),
            _ => None,
        })
    }

    /// Group inputs by port name (preserves encounter order per port).
    pub fn inputs_grouped(&self) -> Vec<(String, Vec<&CorrelatedPayload>)> {
        let mut groups: Vec<(String, Vec<&CorrelatedPayload>)> = Vec::new();
        for (port, payload) in &self.inputs {
            if let Some((_, vec)) = groups.iter_mut().find(|(p, _)| p == port) {
                vec.push(payload);
            } else {
                groups.push((port.clone(), vec![payload]));
            }
        }
        groups
    }

    /// Flush is a no-op now since we apply immediately; kept for symmetry.
    pub fn flush(&mut self) -> Result<(), crate::executor::ExecuteError> {
        Ok(())
    }
}

fn edge_payload_desc(payload: &EdgePayload) -> String {
    match payload {
        EdgePayload::Any(a) => format!("Any({})", std::any::type_name_of_val(a.as_ref())),
        #[cfg(feature = "gpu")]
        EdgePayload::Payload(ep) => format!("Payload({ep:?})"),
        #[cfg(feature = "gpu")]
        EdgePayload::GpuImage(_) => "GpuImage".to_string(),
        EdgePayload::Value(v) => format!("Value({v:?})"),
        EdgePayload::Bytes(_) => "Bytes".to_string(),
        EdgePayload::Unit => "Unit".to_string(),
    }
}

fn align_drained_inputs(
    drained: Vec<DrainedInput>,
    sync_groups: &[SyncGroup],
) -> (Vec<(String, CorrelatedPayload)>, Vec<DrainedInput>, bool) {
    if sync_groups.is_empty() {
        let inputs = drained
            .into_iter()
            .map(|d| (d.port, d.payload))
            .collect::<Vec<_>>();
        return (inputs, Vec::new(), true);
    }

    let mut grouped_ports: std::collections::HashSet<String> = std::collections::HashSet::new();
    for group in sync_groups {
        if group.ports.is_empty() {
            continue;
        }
        for port in &group.ports {
            grouped_ports.insert(port.clone());
        }
    }

    let mut per_port: std::collections::HashMap<String, VecDeque<DrainedInput>> =
        std::collections::HashMap::new();
    for item in drained {
        per_port
            .entry(item.port.clone())
            .or_default()
            .push_back(item);
    }

    let mut selected: Vec<DrainedInput> = Vec::new();
    let mut all_groups_ready = true;

    for group in sync_groups {
        if group.ports.is_empty() {
            // An empty sync group is treated as a no-op. This is useful for callers that
            // intentionally disable implicit sync behavior without requiring correlation alignment.
            continue;
        }
        match group.policy {
            SyncPolicy::Latest => {
                // Require every port to have at least one payload. Take the newest and drop older.
                for port in &group.ports {
                    let Some(q) = per_port.get_mut(port) else {
                        all_groups_ready = false;
                        break;
                    };
                    if q.is_empty() {
                        all_groups_ready = false;
                        break;
                    }
                }
                if !all_groups_ready {
                    break;
                }
                for port in &group.ports {
                    if let Some(q) = per_port.get_mut(port)
                        && let Some(payload) = q.pop_back()
                    {
                        q.clear();
                        selected.push(payload);
                    }
                }
            }
            SyncPolicy::AllReady | SyncPolicy::ZipByTag => {
                // Find the oldest correlation id present across all ports, then take one per port.
                let mut common: Option<std::collections::HashSet<u64>> = None;
                for port in &group.ports {
                    let Some(q) = per_port.get(port) else {
                        all_groups_ready = false;
                        break;
                    };
                    let ids: std::collections::HashSet<u64> =
                        q.iter().map(|cp| cp.payload.correlation_id).collect();
                    common = match common {
                        None => Some(ids),
                        Some(mut acc) => {
                            acc.retain(|id| ids.contains(id));
                            Some(acc)
                        }
                    };
                    if common.as_ref().is_some_and(|acc| acc.is_empty()) {
                        all_groups_ready = false;
                        break;
                    }
                }
                if !all_groups_ready {
                    break;
                }
                let Some(common) = common else {
                    all_groups_ready = false;
                    break;
                };
                let Some(target_id) = common.iter().copied().min() else {
                    all_groups_ready = false;
                    break;
                };

                for port in &group.ports {
                    if let Some(q) = per_port.get_mut(port)
                        && let Some(idx) = q
                            .iter()
                            .position(|cp| cp.payload.correlation_id == target_id)
                    {
                        let payload = q.remove(idx).unwrap();
                        selected.push(payload);
                    } else {
                        all_groups_ready = false;
                        break;
                    }
                }
                if !all_groups_ready {
                    break;
                }
            }
        }
    }

    if !all_groups_ready {
        // Node should not fire: requeue everything we drained and return no inputs.
        let mut leftovers: Vec<DrainedInput> = Vec::new();
        leftovers.extend(selected);
        for (_, mut q) in per_port {
            while let Some(item) = q.pop_front() {
                leftovers.push(item);
            }
        }
        return (Vec::new(), leftovers, false);
    }

    // Fire: emit selected group payloads + any ungrouped payloads.
    let mut inputs: Vec<(String, CorrelatedPayload)> =
        selected.into_iter().map(|d| (d.port, d.payload)).collect();

    // Preserve original per-port order for ungrouped payloads.
    let mut leftovers: Vec<DrainedInput> = Vec::new();
    for (port, mut q) in per_port {
        if grouped_ports.contains(&port) {
            while let Some(item) = q.pop_front() {
                leftovers.push(item);
            }
            continue;
        }
        while let Some(item) = q.pop_front() {
            inputs.push((item.port, item.payload));
        }
    }

    (inputs, leftovers, true)
}

fn requeue_drained(
    leftovers: Vec<DrainedInput>,
    queues: &Arc<Vec<EdgeStorage>>,
    edges: &[EdgeInfo],
) {
    if leftovers.is_empty() {
        return;
    }

    let mut per_edge: HashMap<usize, Vec<CorrelatedPayload>> = HashMap::new();
    for item in leftovers {
        per_edge
            .entry(item.edge_idx)
            .or_default()
            .push(item.payload);
    }

    for (edge_idx, payloads) in per_edge {
        let Some(storage) = queues.get(edge_idx) else {
            continue;
        };
        let policy = edges
            .get(edge_idx)
            .map(|(_, _, _, _, policy)| policy)
            .cloned()
            .unwrap_or(EdgePolicyKind::Fifo);
        match storage {
            EdgeStorage::Locked(q_arc) => {
                if let Ok(mut q) = q_arc.lock() {
                    q.ensure_policy(&policy);
                    for mut payload in payloads {
                        payload.enqueued_at = Instant::now();
                        let _ = q.push(&policy, payload);
                    }
                }
            }
            #[cfg(feature = "lockfree-queues")]
            EdgeStorage::BoundedLf(q) => {
                for mut payload in payloads {
                    payload.enqueued_at = Instant::now();
                    if q.push(payload.clone()).is_err() {
                        let _ = q.pop();
                        let _ = q.push(payload);
                    }
                }
            }
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::executor::ExecutionTelemetry;
    use crate::executor::queue::{EdgeQueue, EdgeStorage, RingBuf};
    use daedalus_planner::NodeRef;
    use serde::Deserialize;
    use std::collections::HashSet;

    fn payload(v: i32, corr: u64) -> CorrelatedPayload {
        CorrelatedPayload {
            correlation_id: corr,
            inner: EdgePayload::Any(Arc::new(v)),
            enqueued_at: std::time::Instant::now(),
        }
    }

    #[test]
    fn aligns_all_ready() {
        let drained = vec![
            DrainedInput {
                port: "a".into(),
                edge_idx: 0,
                payload: payload(1, 1),
            },
            DrainedInput {
                port: "a".into(),
                edge_idx: 0,
                payload: payload(2, 2),
            },
            DrainedInput {
                port: "b".into(),
                edge_idx: 1,
                payload: payload(10, 1),
            },
            DrainedInput {
                port: "b".into(),
                edge_idx: 1,
                payload: payload(20, 2),
            },
        ];
        let group = SyncGroup {
            name: "g".into(),
            policy: SyncPolicy::AllReady,
            backpressure: None,
            capacity: None,
            ports: vec!["a".into(), "b".into()],
        };
        let (out, leftovers, ready) = align_drained_inputs(drained, &[group]);
        assert!(ready);
        let vals: Vec<(String, i32)> = out
            .into_iter()
            .map(|(p, pl)| {
                (
                    p,
                    match pl.inner {
                        EdgePayload::Any(ref a) => *a.downcast_ref::<i32>().unwrap(),
                        _ => panic!("expected Any"),
                    },
                )
            })
            .collect();
        assert_eq!(vals, vec![("a".into(), 1), ("b".into(), 10)]);

        let mut left_vals: Vec<(String, i32)> = leftovers
            .into_iter()
            .map(|d| {
                let v = match d.payload.inner {
                    EdgePayload::Any(ref a) => *a.downcast_ref::<i32>().unwrap(),
                    _ => panic!("expected Any"),
                };
                (d.port, v)
            })
            .collect();
        left_vals.sort_by(|a, b| a.0.cmp(&b.0).then(a.1.cmp(&b.1)));
        assert_eq!(left_vals, vec![("a".into(), 2), ("b".into(), 20)]);
    }

    #[test]
    fn aligns_latest() {
        let drained = vec![
            DrainedInput {
                port: "a".into(),
                edge_idx: 0,
                payload: payload(1, 1),
            },
            DrainedInput {
                port: "a".into(),
                edge_idx: 0,
                payload: payload(2, 2),
            },
            DrainedInput {
                port: "b".into(),
                edge_idx: 1,
                payload: payload(10, 3),
            },
            DrainedInput {
                port: "b".into(),
                edge_idx: 1,
                payload: payload(20, 4),
            },
        ];
        let group = SyncGroup {
            name: "g".into(),
            policy: SyncPolicy::Latest,
            backpressure: None,
            capacity: None,
            ports: vec!["a".into(), "b".into()],
        };
        let (out, leftovers, ready) = align_drained_inputs(drained, &[group]);
        assert!(ready);
        assert!(leftovers.is_empty());
        let vals: Vec<(String, i32)> = out
            .into_iter()
            .map(|(p, pl)| {
                (
                    p,
                    match pl.inner {
                        EdgePayload::Any(ref a) => *a.downcast_ref::<i32>().unwrap(),
                        _ => panic!("expected Any"),
                    },
                )
            })
            .collect();
        assert_eq!(vals, vec![("a".into(), 2), ("b".into(), 20)]);
    }

    #[test]
    fn aligns_zip_by_tag() {
        use daedalus_data::model::{StructFieldValue, Value};
        let tagged = |tag: &str, v: i32, corr: u64| {
            let fields = vec![
                StructFieldValue {
                    name: "tag".to_string(),
                    value: Value::String(tag.to_string().into()),
                },
                StructFieldValue {
                    name: "v".to_string(),
                    value: Value::Int(i64::from(v)),
                },
            ];
            CorrelatedPayload {
                correlation_id: corr,
                inner: EdgePayload::Value(Value::Struct(fields)),
                enqueued_at: std::time::Instant::now(),
            }
        };
        let inputs = vec![
            ("a".into(), tagged("x", 1, 1)),
            ("b".into(), tagged("y", 200, 2)),
            ("b".into(), tagged("x", 100, 1)),
            ("a".into(), tagged("y", 2, 2)),
        ];
        let group = SyncGroup {
            name: "g".into(),
            policy: SyncPolicy::ZipByTag,
            backpressure: None,
            capacity: None,
            ports: vec!["a".into(), "b".into()],
        };
        let drained = inputs
            .into_iter()
            .enumerate()
            .map(|(idx, (port, payload))| DrainedInput {
                port,
                edge_idx: idx,
                payload,
            })
            .collect::<Vec<_>>();
        let (out, leftovers, ready) = align_drained_inputs(drained, &[group]);
        assert!(ready);
        assert!(!out.is_empty());
        assert!(!leftovers.is_empty());
        let tags: Vec<String> = out
            .chunks(2)
            .map(|chunk| {
                chunk
                    .iter()
                    .map(|(_, p)| match &p.inner {
                        EdgePayload::Value(Value::Struct(fields)) => fields
                            .iter()
                            .find(|f| f.name == "tag")
                            .and_then(|f| match &f.value {
                                Value::String(s) => Some(s.to_string()),
                                _ => None,
                            })
                            .unwrap(),
                        _ => "missing".to_string(),
                    })
                    .next()
                    .unwrap()
            })
            .collect();
        assert_eq!(tags, vec!["x".to_string()]);
    }

    #[test]
    fn not_ready_returns_no_inputs_and_requeues() {
        let drained = vec![DrainedInput {
            port: "a".into(),
            edge_idx: 0,
            payload: payload(1, 1),
        }];
        let group = SyncGroup {
            name: "g".into(),
            policy: SyncPolicy::AllReady,
            backpressure: None,
            capacity: None,
            ports: vec!["a".into(), "b".into()],
        };
        let (out, leftovers, ready) = align_drained_inputs(drained, &[group]);
        assert!(!ready);
        assert!(out.is_empty());
        assert_eq!(leftovers.len(), 1);
    }

    #[test]
    fn port_override_applies_backpressure_and_capacity() {
        let queues = Arc::new(vec![EdgeStorage::Locked(Arc::new(std::sync::Mutex::new(
            EdgeQueue::Bounded {
                ring: RingBuf::new(5),
            },
        )))]);
        let edges = vec![(
            NodeRef(0),
            "out".to_string(),
            NodeRef(1),
            "in".to_string(),
            EdgePolicyKind::Bounded { cap: 5 },
        )];
        let sg = SyncGroup {
            name: "g".into(),
            policy: SyncPolicy::AllReady,
            backpressure: Some(BackpressureStrategy::ErrorOnOverflow),
            capacity: Some(1),
            ports: vec!["out".into()],
        };
        let warnings = Arc::new(std::sync::Mutex::new(HashSet::new()));
        let mut telem = ExecutionTelemetry::default();

        #[cfg(feature = "gpu")]
        let gpu_entry_edges = HashSet::new();
        #[cfg(feature = "gpu")]
        let gpu_exit_edges = HashSet::new();
        #[cfg(feature = "gpu")]
        let payload_edges = HashSet::new();

        let mut io = NodeIo::new(
            vec![],
            vec![0],
            &queues,
            &warnings,
            &edges,
            vec![sg],
            #[cfg(feature = "gpu")]
            &gpu_entry_edges,
            #[cfg(feature = "gpu")]
            &gpu_exit_edges,
            #[cfg(feature = "gpu")]
            &payload_edges,
            0,
            "node".into(),
            &mut telem,
            BackpressureStrategy::None,
            &[],
            None,
            None,
            #[cfg(feature = "gpu")]
            None,
            #[cfg(feature = "gpu")]
            daedalus_planner::ComputeAffinity::CpuOnly,
        );

        io.push_any(Some("out"), 1i32);
        io.push_any(Some("out"), 2i32); // should trigger overflow with cap=1 + ErrorOnOverflow override
        assert!(telem.backpressure_events > 0);
    }

    #[derive(Debug, Clone, Copy, PartialEq, Eq, Deserialize)]
    #[serde(rename_all = "snake_case")]
    enum TestEnum {
        Auto,
        Cpu,
        Gpu,
    }

    impl TestEnum {
        fn from_label(raw: &str) -> Option<Self> {
            match raw.trim().to_ascii_lowercase().as_str() {
                "auto" => Some(Self::Auto),
                "cpu" => Some(Self::Cpu),
                "gpu" => Some(Self::Gpu),
                _ => None,
            }
        }
    }

    fn register_test_enum() {
        use daedalus_data::model::Value;
        daedalus_data::typing::register_enum::<TestEnum>(["auto", "cpu", "gpu"]);
        register_const_coercer::<TestEnum, _>(|v| match v {
            Value::Int(_) => NodeIo::enum_name_from_index::<TestEnum>(v)
                .and_then(|name| TestEnum::from_label(&name)),
            Value::String(s) => TestEnum::from_label(s),
            Value::Enum(ev) => TestEnum::from_label(&ev.name),
            _ => None,
        });
    }

    #[cfg(feature = "gpu")]
    #[derive(Clone, Debug, PartialEq)]
    struct DummyPayload {
        value: i32,
    }

    #[cfg(feature = "gpu")]
    impl daedalus_gpu::GpuSendable for DummyPayload {
        type GpuRepr = ();
    }

    #[cfg(feature = "gpu")]
    fn make_io_with_payload(payload: CorrelatedPayload) -> NodeIo<'static> {
        let queues: &'static Arc<Vec<EdgeStorage>> = Box::leak(Box::new(Arc::new(vec![])));
        let edges: &'static [EdgeInfo] = Box::leak(Box::new(Vec::new()));
        let warnings: &'static Arc<std::sync::Mutex<std::collections::HashSet<String>>> =
            Box::leak(Box::new(Arc::new(std::sync::Mutex::new(HashSet::new()))));
        let telem: &'static mut ExecutionTelemetry = Box::leak(Box::new(ExecutionTelemetry::default()));
        let gpu_entry_edges: &'static HashSet<usize> = Box::leak(Box::new(HashSet::new()));
        let gpu_exit_edges: &'static HashSet<usize> = Box::leak(Box::new(HashSet::new()));
        let payload_edges: &'static HashSet<usize> = Box::leak(Box::new(HashSet::new()));

        let mut io = NodeIo::new(
            vec![],
            vec![],
            queues,
            warnings,
            edges,
            vec![],
            gpu_entry_edges,
            gpu_exit_edges,
            payload_edges,
            0,
            "node".into(),
            telem,
            BackpressureStrategy::None,
            &[],
            None,
            None,
            None,
            daedalus_planner::ComputeAffinity::CpuOnly,
        );
        io.inputs = vec![("in".to_string(), payload)];
        io
    }

    #[cfg(feature = "gpu")]
    #[test]
    fn get_any_reads_payload_any_type() {
        let payload = CorrelatedPayload {
            correlation_id: 1,
            inner: EdgePayload::Payload(daedalus_gpu::ErasedPayload::from_cpu::<DummyPayload>(DummyPayload { value: 42 })),
            enqueued_at: std::time::Instant::now(),
        };
        let io = make_io_with_payload(payload);
        let got = io.get_any::<DummyPayload>("in");
        assert_eq!(got, Some(DummyPayload { value: 42 }));
    }

    #[cfg(feature = "gpu")]
    #[test]
    fn get_typed_mut_moves_payload_any_type() {
        let payload = CorrelatedPayload {
            correlation_id: 1,
            inner: EdgePayload::Payload(daedalus_gpu::ErasedPayload::from_cpu::<DummyPayload>(DummyPayload { value: 7 })),
            enqueued_at: std::time::Instant::now(),
        };
        let mut io = make_io_with_payload(payload);
        let got = io.get_typed_mut::<DummyPayload>("in");
        assert_eq!(got, Some(DummyPayload { value: 7 }));
        assert!(io.inputs.is_empty());
    }

    #[test]
    fn get_typed_parses_enum_from_value_enum() {
        use daedalus_data::model::{EnumValue, Value};
        register_test_enum();
        let queues = Arc::new(vec![]);
        let edges = vec![];
        let warnings = Arc::new(std::sync::Mutex::new(HashSet::new()));
        let mut telem = ExecutionTelemetry::default();

        #[cfg(feature = "gpu")]
        let gpu_entry_edges = HashSet::new();
        #[cfg(feature = "gpu")]
        let gpu_exit_edges = HashSet::new();

        assert_eq!(
            NodeIo::enum_name_from_index::<TestEnum>(&Value::Int(2)),
            Some("gpu".to_string())
        );

        let io = NodeIo::new(
            vec![],
            vec![],
            &queues,
            &warnings,
            &edges,
            vec![],
            #[cfg(feature = "gpu")]
            &gpu_entry_edges,
            #[cfg(feature = "gpu")]
            &gpu_exit_edges,
            #[cfg(feature = "gpu")]
            &payload_edges,
            0,
            "node".into(),
            &mut telem,
            BackpressureStrategy::None,
            &[(
                "mode".to_string(),
                Value::Enum(EnumValue {
                    name: "gpu".to_string(),
                    value: None,
                }),
            )],
            None,
            None,
            #[cfg(feature = "gpu")]
            None,
            #[cfg(feature = "gpu")]
            daedalus_planner::ComputeAffinity::CpuOnly,
        );

        assert_eq!(io.get_typed::<TestEnum>("mode"), Some(TestEnum::Gpu));
    }

    #[test]
    fn get_typed_parses_enum_from_value_string() {
        use daedalus_data::model::Value;
        register_test_enum();
        let queues = Arc::new(vec![]);
        let edges = vec![];
        let warnings = Arc::new(std::sync::Mutex::new(HashSet::new()));
        let mut telem = ExecutionTelemetry::default();

        #[cfg(feature = "gpu")]
        let gpu_entry_edges = HashSet::new();
        #[cfg(feature = "gpu")]
        let gpu_exit_edges = HashSet::new();
        #[cfg(feature = "gpu")]
        let payload_edges = HashSet::new();

        let io = NodeIo::new(
            vec![],
            vec![],
            &queues,
            &warnings,
            &edges,
            vec![],
            #[cfg(feature = "gpu")]
            &gpu_entry_edges,
            #[cfg(feature = "gpu")]
            &gpu_exit_edges,
            #[cfg(feature = "gpu")]
            &payload_edges,
            0,
            "node".into(),
            &mut telem,
            BackpressureStrategy::None,
            &[("mode".to_string(), Value::String("cpu".into()))],
            None,
            None,
            #[cfg(feature = "gpu")]
            None,
            #[cfg(feature = "gpu")]
            daedalus_planner::ComputeAffinity::CpuOnly,
        );

        assert_eq!(io.get_typed::<TestEnum>("mode"), Some(TestEnum::Cpu));
    }

    #[test]
    fn get_typed_parses_enum_from_value_int_index() {
        use daedalus_data::model::Value;
        register_test_enum();
        assert!(daedalus_data::typing::lookup_type::<TestEnum>().is_some());
        let queues = Arc::new(vec![]);
        let edges = vec![];
        let warnings = Arc::new(std::sync::Mutex::new(HashSet::new()));
        let mut telem = ExecutionTelemetry::default();

        #[cfg(feature = "gpu")]
        let gpu_entry_edges = HashSet::new();
        #[cfg(feature = "gpu")]
        let gpu_exit_edges = HashSet::new();
        #[cfg(feature = "gpu")]
        let payload_edges = HashSet::new();

        // ExecMode variants are registered in order [Auto, Cpu, Gpu]; index 2 => Gpu.
        let io = NodeIo::new(
            vec![],
            vec![],
            &queues,
            &warnings,
            &edges,
            vec![],
            #[cfg(feature = "gpu")]
            &gpu_entry_edges,
            #[cfg(feature = "gpu")]
            &gpu_exit_edges,
            #[cfg(feature = "gpu")]
            &payload_edges,
            0,
            "node".into(),
            &mut telem,
            BackpressureStrategy::None,
            &[("mode".to_string(), Value::Int(2))],
            None,
            None,
            #[cfg(feature = "gpu")]
            None,
            #[cfg(feature = "gpu")]
            daedalus_planner::ComputeAffinity::CpuOnly,
        );

        assert_eq!(io.get_typed::<TestEnum>("mode"), Some(TestEnum::Gpu));
    }
}
