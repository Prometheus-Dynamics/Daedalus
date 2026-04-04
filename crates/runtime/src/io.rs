use std::any::{Any, TypeId};
use std::collections::BTreeMap;
use std::collections::HashMap;
#[cfg(feature = "gpu")]
use std::collections::HashSet;
use std::collections::VecDeque;
use std::ops::{Deref, DerefMut};
use std::sync::Arc;
use std::sync::Mutex;
use std::sync::{OnceLock, RwLock};
use std::time::Instant;

use daedalus_data::model::{TypeExpr, Value};
use daedalus_data::typing;
#[cfg(feature = "gpu")]
use image::{DynamicImage, GrayAlphaImage, GrayImage, RgbImage, RgbaImage};

use crate::executor::queue::{ApplyPolicyOwnedArgs, apply_policy, apply_policy_owned};
use crate::executor::{
    CorrelatedValue, EdgeStorage, ExecutionTelemetry, RuntimeValue, next_correlation_id,
};
use crate::fanin::parse_indexed_port;
#[allow(unused_imports)]
use crate::plan::{BackpressureStrategy, EdgePolicyKind, RuntimeNode};
use daedalus_core::sync::{SyncGroup, SyncPolicy};
use daedalus_planner::NodeRef;

type EdgeInfo = (NodeRef, String, NodeRef, String, EdgePolicyKind);

#[derive(Clone, Debug, PartialEq, Eq, serde::Serialize, serde::Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum TypedInputResolutionKind {
    Exact,
    ConstCoercion,
    ValueCoercion,
    RuntimeConversion,
    ComputeExact,
    ComputeConversion,
}

#[derive(Clone, Debug, PartialEq, Eq, serde::Serialize, serde::Deserialize)]
pub struct TypedInputResolution {
    pub port: String,
    pub kind: TypedInputResolutionKind,
    pub source_value: String,
    pub source_rust: Option<String>,
    pub target_rust: String,
    pub source_typeexpr: Option<TypeExpr>,
    pub target_typeexpr: Option<TypeExpr>,
    pub compatibility_path: Option<typing::TypeCompatibilityPath>,
    pub runtime_conversion: Option<crate::RuntimeConversionResolution>,
}

#[derive(Clone)]
struct DrainedInput {
    port: String,
    edge_idx: usize,
    payload: CorrelatedValue,
}

#[cfg(feature = "gpu")]
struct ConvertIncomingContext<'a> {
    node_idx: usize,
    edge_idx: usize,
    entries: &'a HashSet<usize>,
    exits: &'a HashSet<usize>,
    materialization_cache: Option<&'a MaterializationCacheHandle>,
    gpu: Option<&'a daedalus_gpu::GpuContextHandle>,
    telemetry: &'a mut ExecutionTelemetry,
}

pub type ConstCoercer = Box<
    dyn Fn(&daedalus_data::model::Value) -> Option<Box<dyn Any + Send + Sync>>
        + Send
        + Sync
        + 'static,
>;

pub type ConstCoercerMap = Arc<RwLock<HashMap<&'static str, ConstCoercer>>>;

static GLOBAL_CONST_COERCERS: OnceLock<ConstCoercerMap> = OnceLock::new();
type OutputMover = Box<dyn Fn(Box<dyn Any + Send + Sync>) -> RuntimeValue + Send + Sync + 'static>;
pub type OutputMoverMap = Arc<RwLock<HashMap<TypeId, OutputMover>>>;
static OUTPUT_MOVERS: OnceLock<OutputMoverMap> = OnceLock::new();

fn output_movers() -> &'static OutputMoverMap {
    OUTPUT_MOVERS.get_or_init(|| Arc::new(RwLock::new(HashMap::new())))
}

/// Per-execution cache for expensive payload materializations (e.g. wrapping large CPU images into
/// `DataCell` so uploads/downloads are deduped across fanout).
#[cfg(feature = "gpu")]
pub(crate) type MaterializationCache = HashMap<usize, RuntimeValue>;

#[cfg(feature = "gpu")]
pub(crate) type MaterializationCacheHandle = Arc<Mutex<MaterializationCache>>;

#[cfg(feature = "gpu")]
pub(crate) fn new_materialization_cache() -> MaterializationCacheHandle {
    Arc::new(Mutex::new(HashMap::new()))
}

/// Per-execution cache for CPU-side `Any` conversions across fanout.
///
/// Keyed by (source_any_ptr, target_type_id) so a single producer feeding multiple consumers
/// doesn't redo expensive conversions/materializations N times.
pub(crate) type AnyConversionCache = HashMap<(usize, TypeId), Arc<dyn Any + Send + Sync>>;

pub(crate) type AnyConversionCacheHandle = Arc<Mutex<AnyConversionCache>>;

pub(crate) fn new_any_conversion_cache() -> AnyConversionCacheHandle {
    Arc::new(Mutex::new(HashMap::new()))
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
    F: Fn(T) -> RuntimeValue + Send + Sync + 'static,
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
    F: Fn(T) -> RuntimeValue + Send + Sync + 'static,
{
    register_output_mover_in(output_movers(), mover);
}

fn try_move_output<T>(movers: Option<&OutputMoverMap>, value: T) -> Result<RuntimeValue, T>
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
fn promote_value_for_host(payload: RuntimeValue) -> (RuntimeValue, bool) {
    use daedalus_gpu::{Compute, DataCell};

    match payload {
        RuntimeValue::Any(a) => {
            if let Some(ep) = a.downcast_ref::<DataCell>() {
                return (RuntimeValue::Data(ep.clone()), true);
            }
            if let Some(p) = a.downcast_ref::<Compute<DynamicImage>>() {
                return (
                    match p.clone() {
                        Compute::Cpu(img) => {
                            RuntimeValue::Data(DataCell::from_cpu::<DynamicImage>(img))
                        }
                        Compute::Gpu(g) => {
                            RuntimeValue::Data(DataCell::from_gpu::<DynamicImage>(g))
                        }
                    },
                    true,
                );
            }
            if let Some(p) = a.downcast_ref::<Compute<GrayImage>>() {
                return (
                    match p.clone() {
                        Compute::Cpu(img) => {
                            RuntimeValue::Data(DataCell::from_cpu::<GrayImage>(img))
                        }
                        Compute::Gpu(g) => RuntimeValue::Data(DataCell::from_gpu::<GrayImage>(g)),
                    },
                    true,
                );
            }
            if let Some(img) = a.downcast_ref::<DynamicImage>() {
                return (
                    RuntimeValue::Data(DataCell::from_cpu::<DynamicImage>(img.clone())),
                    true,
                );
            }
            if let Some(img) = a.downcast_ref::<Arc<DynamicImage>>() {
                return (
                    RuntimeValue::Data(DataCell::from_cpu::<DynamicImage>((**img).clone())),
                    true,
                );
            }
            if let Some(img) = a.downcast_ref::<GrayImage>() {
                return (
                    RuntimeValue::Data(DataCell::from_cpu::<GrayImage>(img.clone())),
                    true,
                );
            }
            if let Some(img) = a.downcast_ref::<Arc<GrayImage>>() {
                return (
                    RuntimeValue::Data(DataCell::from_cpu::<GrayImage>((**img).clone())),
                    true,
                );
            }
            if let Some(img) = a.downcast_ref::<RgbImage>() {
                return (
                    RuntimeValue::Data(DataCell::from_cpu::<RgbImage>(img.clone())),
                    true,
                );
            }
            if let Some(img) = a.downcast_ref::<Arc<RgbImage>>() {
                return (
                    RuntimeValue::Data(DataCell::from_cpu::<RgbImage>((**img).clone())),
                    true,
                );
            }
            if let Some(img) = a.downcast_ref::<RgbaImage>() {
                return (
                    RuntimeValue::Data(DataCell::from_cpu::<RgbaImage>(img.clone())),
                    true,
                );
            }
            if let Some(img) = a.downcast_ref::<Arc<RgbaImage>>() {
                return (
                    RuntimeValue::Data(DataCell::from_cpu::<RgbaImage>((**img).clone())),
                    true,
                );
            }
            if let Some(img) = a.downcast_ref::<GrayAlphaImage>() {
                let dyn_img = DynamicImage::ImageLumaA8(img.clone());
                return (
                    RuntimeValue::Data(DataCell::from_cpu::<DynamicImage>(dyn_img)),
                    true,
                );
            }
            if let Some(img) = a.downcast_ref::<Arc<GrayAlphaImage>>() {
                let dyn_img = DynamicImage::ImageLumaA8((**img).clone());
                return (
                    RuntimeValue::Data(DataCell::from_cpu::<DynamicImage>(dyn_img)),
                    true,
                );
            }
            (RuntimeValue::Any(a), false)
        }
        other => (other, false),
    }
}

#[cfg(feature = "gpu")]
fn promote_any_with_cache(
    a: &Arc<dyn Any + Send + Sync>,
    cache: Option<&MaterializationCacheHandle>,
) -> (RuntimeValue, bool) {
    // This is potentially expensive (cloning large CPU images). Cache it per execution so fanout
    // doesn't duplicate the work.
    let Some(cache) = cache else {
        return promote_value_for_host(RuntimeValue::Any(a.clone()));
    };

    // `Arc::as_ptr` returns a fat pointer for trait objects; cast through `*const ()` to
    // obtain the data pointer for hashing.
    let key = (Arc::as_ptr(a) as *const ()) as usize;

    if let Ok(guard) = cache.lock()
        && let Some(hit) = guard.get(&key)
    {
        return (hit.clone(), false);
    }

    let (promoted, materialized) = promote_value_for_host(RuntimeValue::Any(a.clone()));
    if let Ok(mut guard) = cache.lock() {
        guard.insert(key, promoted.clone());
    }
    (promoted, materialized)
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
/// use daedalus_runtime::executor::RuntimeValue;
///
/// fn handler(io: &mut NodeIo) {
///     io.push_output(Some("out"), RuntimeValue::Unit);
/// }
/// ```
pub struct NodeIo<'a> {
    inputs: Vec<(String, CorrelatedValue)>,
    borrowed_cache: std::cell::UnsafeCell<Vec<Box<dyn Any + Send + Sync>>>,
    sync_groups: Vec<SyncGroup>,
    port_overrides: HashMap<String, (Option<BackpressureStrategy>, Option<usize>)>,
    current_corr_id: u64,
    outgoing: Vec<usize>,
    has_incoming_edges: bool,
    queues: &'a Arc<Vec<EdgeStorage>>,
    telemetry_ptr: *mut ExecutionTelemetry,
    telemetry_lifetime: std::marker::PhantomData<&'a mut ExecutionTelemetry>,
    edges: &'a [EdgeInfo],
    #[allow(dead_code)]
    seg_idx: usize,
    node_idx: usize,
    node_id: String,
    active_nodes: Option<&'a [bool]>,
    warnings_seen: &'a std::sync::Arc<std::sync::Mutex<std::collections::HashSet<String>>>,
    backpressure: BackpressureStrategy,
    #[cfg(feature = "gpu")]
    gpu: Option<daedalus_gpu::GpuContextHandle>,
    #[cfg(feature = "gpu")]
    target_compute: daedalus_planner::ComputeAffinity,
    #[cfg(feature = "gpu")]
    data_edges: &'a HashSet<usize>,
    #[cfg(feature = "gpu")]
    materialization_cache: Option<MaterializationCacheHandle>,
    const_coercers: Option<ConstCoercerMap>,
    output_movers: Option<OutputMoverMap>,
    any_conversion_cache: AnyConversionCacheHandle,
}

/// An `Arc<T>` wrapper that supports copy-on-write mutation via `Arc::make_mut`.
///
/// This is the ergonomic building block for `PortAccessMode::MutBorrowed`:
/// if the graph proves exclusivity, mutation is in-place; otherwise it falls back to COW.
pub struct CowArcMut<T> {
    arc: Arc<T>,
}

impl<T> CowArcMut<T> {
    pub fn new(arc: Arc<T>) -> Self {
        Self { arc }
    }

    pub fn as_arc(&self) -> &Arc<T> {
        &self.arc
    }

    pub fn into_arc(self) -> Arc<T> {
        self.arc
    }

    pub fn make_mut(&mut self) -> &mut T
    where
        T: Clone,
    {
        Arc::make_mut(&mut self.arc)
    }
}

impl<T> Deref for CowArcMut<T> {
    type Target = T;
    fn deref(&self) -> &Self::Target {
        &self.arc
    }
}

impl<T: Clone> DerefMut for CowArcMut<T> {
    fn deref_mut(&mut self) -> &mut Self::Target {
        Arc::make_mut(&mut self.arc)
    }
}

impl<'a> NodeIo<'a> {
    #[cfg(feature = "gpu")]
    fn convert_cpu_value_to_t<S, T>(value: S) -> Option<T>
    where
        S: Any + Send + Sync + 'static,
        T: Any + Clone + Send + Sync + 'static,
    {
        let any: Arc<dyn Any + Send + Sync> = Arc::new(value);
        crate::convert::convert_arc::<T>(&any)
    }

    #[cfg(feature = "gpu")]
    fn convert_cpu_value_to_arc<S, T>(value: S) -> Option<Arc<T>>
    where
        S: Any + Send + Sync + 'static,
        T: Any + Send + Sync + 'static,
    {
        let any: Arc<dyn Any + Send + Sync> = Arc::new(value);
        crate::convert::convert_to_arc::<T>(&any)
    }

    #[cfg(feature = "gpu")]
    fn convert_backing_value_to_t<S, T>(backing: daedalus_gpu::Backing<S>) -> Option<T>
    where
        S: Any + Clone + Send + Sync + 'static,
        T: Any + Clone + Send + Sync + 'static,
    {
        Self::convert_cpu_value_to_t::<S, T>(backing.into_owned())
    }

    #[cfg(feature = "gpu")]
    fn convert_backing_value_to_arc<S, T>(backing: daedalus_gpu::Backing<S>) -> Option<Arc<T>>
    where
        S: Any + Clone + Send + Sync + 'static,
        T: Any + Send + Sync + 'static,
    {
        Self::convert_cpu_value_to_arc::<S, T>(backing.into_owned())
    }

    #[cfg(feature = "gpu")]
    fn any_backing_ref<T>(any: &Arc<dyn Any + Send + Sync>) -> Option<&T>
    where
        T: Any + Send + Sync + 'static,
    {
        any.downcast_ref::<daedalus_gpu::Backing<T>>()
            .map(AsRef::as_ref)
    }

    #[cfg(feature = "gpu")]
    fn any_backing_owned<T>(any: &Arc<dyn Any + Send + Sync>) -> Option<T>
    where
        T: Any + Clone + Send + Sync + 'static,
    {
        any.downcast_ref::<daedalus_gpu::Backing<T>>()
            .map(|backing| backing.clone().into_owned())
    }

    #[cfg(feature = "gpu")]
    fn any_backing_arc<T>(any: &Arc<dyn Any + Send + Sync>) -> Option<Arc<T>>
    where
        T: Any + Send + Sync + 'static,
    {
        any.downcast_ref::<daedalus_gpu::Backing<T>>()
            .and_then(daedalus_gpu::Backing::shared_arc)
    }

    fn resolved_type_expr<T: Any>() -> Option<TypeExpr> {
        typing::override_type_expr::<T>()
            .or_else(|| typing::lookup_type_by_rust_name(std::any::type_name::<T>()))
    }

    fn source_type_expr_from_rust_name(rust: &str) -> Option<TypeExpr> {
        typing::lookup_type_by_rust_name(rust)
    }

    fn estimate_any_bytes(value: &(dyn Any + Send + Sync)) -> Option<u64> {
        crate::executor::any_ref_size_bytes(value)
    }

    fn with_telemetry<R>(&self, f: impl FnOnce(&mut ExecutionTelemetry) -> R) -> R {
        // SAFETY: `NodeIo` is created with exclusive access to the executor telemetry for a single
        // node execution. This raw pointer aliases that same exclusive borrow so immutable helper
        // methods can still record conversion/materialization counters.
        unsafe { f(&mut *self.telemetry_ptr) }
    }

    fn record_conversion_bytes(&self, bytes: Option<u64>) {
        if let Some(bytes) = bytes {
            self.with_telemetry(|telemetry| telemetry.record_node_conversion(self.node_idx, bytes));
        }
    }

    #[cfg(feature = "gpu")]
    fn record_materialization_bytes(&self, bytes: Option<u64>) {
        if let Some(bytes) = bytes {
            self.with_telemetry(|telemetry| {
                telemetry.record_node_materialization(self.node_idx, bytes)
            });
        }
    }

    #[cfg(feature = "gpu")]
    fn record_gpu_transfer_bytes(&self, upload: bool, bytes: Option<u64>) {
        if let Some(bytes) = bytes {
            self.with_telemetry(|telemetry| {
                telemetry.record_node_gpu_transfer(self.node_idx, upload, bytes)
            });
        }
    }

    fn typed_input_resolution<T>(
        &self,
        port: &str,
        payload: &RuntimeValue,
        kind: TypedInputResolutionKind,
        source_rust: Option<String>,
        runtime_conversion: Option<crate::RuntimeConversionResolution>,
    ) -> TypedInputResolution
    where
        T: Any,
    {
        let source_typeexpr = source_rust
            .as_deref()
            .and_then(Self::source_type_expr_from_rust_name)
            .or_else(|| {
                runtime_conversion.as_ref().and_then(|resolution| {
                    resolution
                        .steps
                        .first()
                        .and_then(|step| Self::source_type_expr_from_rust_name(&step.from_rust))
                })
            });
        let target_typeexpr = Self::resolved_type_expr::<T>();
        let compatibility_path = source_typeexpr.as_ref().and_then(|from| {
            target_typeexpr
                .as_ref()
                .and_then(|to| typing::explain_typeexpr_conversion(from, to))
        });

        TypedInputResolution {
            port: port.to_string(),
            kind,
            source_value: runtime_value_desc(payload),
            source_rust,
            target_rust: std::any::type_name::<T>().to_string(),
            source_typeexpr,
            target_typeexpr,
            compatibility_path,
            runtime_conversion,
        }
    }

    fn resolve_typed_from_any<T>(
        &self,
        port: &str,
        any: &Arc<dyn Any + Send + Sync>,
        payload: &RuntimeValue,
    ) -> Option<(T, TypedInputResolution)>
    where
        T: Any + Clone + Send + Sync + 'static,
    {
        if let Some(value) = any.downcast_ref::<T>().cloned() {
            let resolution = self.typed_input_resolution::<T>(
                port,
                payload,
                TypedInputResolutionKind::Exact,
                Some(std::any::type_name::<T>().to_string()),
                None,
            );
            return Some((value, resolution));
        }

        if let Some(value) = any.downcast_ref::<Arc<T>>().map(|arc| (**arc).clone()) {
            let resolution = self.typed_input_resolution::<T>(
                port,
                payload,
                TypedInputResolutionKind::Exact,
                Some(std::any::type_name::<T>().to_string()),
                None,
            );
            return Some((value, resolution));
        }

        #[cfg(feature = "gpu")]
        if let Some(value) = Self::any_backing_owned::<T>(any) {
            let resolution = self.typed_input_resolution::<T>(
                port,
                payload,
                TypedInputResolutionKind::Exact,
                Some(std::any::type_name::<daedalus_gpu::Backing<T>>().to_string()),
                None,
            );
            return Some((value, resolution));
        }

        let source_any = any.as_ref();
        let source_rust = if source_any.downcast_ref::<i64>().is_some() {
            Some(std::any::type_name::<i64>().to_string())
        } else if source_any.downcast_ref::<f64>().is_some() {
            Some(std::any::type_name::<f64>().to_string())
        } else if source_any.downcast_ref::<bool>().is_some() {
            Some(std::any::type_name::<bool>().to_string())
        } else if source_any.downcast_ref::<String>().is_some() {
            Some(std::any::type_name::<String>().to_string())
        } else if source_any.downcast_ref::<Value>().is_some() {
            Some(std::any::type_name::<Value>().to_string())
        } else {
            None
        };

        if let Some(value) = self.coerce_const_any::<T>(any.as_ref()) {
            let resolution = self.typed_input_resolution::<T>(
                port,
                payload,
                TypedInputResolutionKind::ConstCoercion,
                source_rust.clone(),
                None,
            );
            return Some((value, resolution));
        }

        let runtime_conversion = crate::convert::explain_conversion_to::<T>(any);
        let source_rust = runtime_conversion
            .as_ref()
            .and_then(|path| path.steps.first().map(|step| step.from_rust.clone()))
            .or(source_rust)
            .or_else(|| Some(std::any::type_name_of_val(any.as_ref()).to_string()));
        if let Some(value) = self
            .convert_any_to_arc_cached::<T>(any)
            .map(|arc| (*arc).clone())
        {
            let resolution = self.typed_input_resolution::<T>(
                port,
                payload,
                TypedInputResolutionKind::RuntimeConversion,
                source_rust,
                runtime_conversion,
            );
            return Some((value, resolution));
        }

        None
    }

    #[cfg(feature = "gpu")]
    fn resolve_typed_from_data_cell<T>(
        &self,
        port: &str,
        ep: &daedalus_gpu::DataCell,
        payload: &RuntimeValue,
    ) -> Option<(T, TypedInputResolution)>
    where
        T: Any + Clone + Send + Sync + 'static,
    {
        if let Some(value) = ep.try_downcast_cpu_any::<T>() {
            let resolution = self.typed_input_resolution::<T>(
                port,
                payload,
                TypedInputResolutionKind::ComputeExact,
                Some(std::any::type_name::<T>().to_string()),
                None,
            );
            return Some((value, resolution));
        }

        if let Some(backing) = ep.try_downcast_cpu_any::<daedalus_gpu::Backing<T>>() {
            let resolution = self.typed_input_resolution::<T>(
                port,
                payload,
                TypedInputResolutionKind::ComputeExact,
                Some(std::any::type_name::<daedalus_gpu::Backing<T>>().to_string()),
                None,
            );
            return Some((backing.into_owned(), resolution));
        }

        if let Some(img) = ep.try_downcast_cpu_any::<daedalus_gpu::Backing<DynamicImage>>()
            && let Some(value) = Self::convert_backing_value_to_t::<DynamicImage, T>(img)
        {
            let resolution = self.typed_input_resolution::<T>(
                port,
                payload,
                TypedInputResolutionKind::ComputeConversion,
                Some(std::any::type_name::<daedalus_gpu::Backing<DynamicImage>>().to_string()),
                crate::convert::explain_conversion_from_types::<DynamicImage, T>(),
            );
            return Some((value, resolution));
        }

        if let Some(gray) = ep.try_downcast_cpu_any::<daedalus_gpu::Backing<GrayImage>>()
            && let Some(value) = Self::convert_backing_value_to_t::<GrayImage, T>(gray)
        {
            let resolution = self.typed_input_resolution::<T>(
                port,
                payload,
                TypedInputResolutionKind::ComputeConversion,
                Some(std::any::type_name::<daedalus_gpu::Backing<GrayImage>>().to_string()),
                crate::convert::explain_conversion_from_types::<GrayImage, T>(),
            );
            return Some((value, resolution));
        }

        if let Some(rgb) = ep.try_downcast_cpu_any::<daedalus_gpu::Backing<RgbImage>>()
            && let Some(value) = Self::convert_backing_value_to_t::<RgbImage, T>(rgb)
        {
            let resolution = self.typed_input_resolution::<T>(
                port,
                payload,
                TypedInputResolutionKind::ComputeConversion,
                Some(std::any::type_name::<daedalus_gpu::Backing<RgbImage>>().to_string()),
                crate::convert::explain_conversion_from_types::<RgbImage, T>(),
            );
            return Some((value, resolution));
        }

        if let Some(rgba) = ep.try_downcast_cpu_any::<daedalus_gpu::Backing<RgbaImage>>()
            && let Some(value) = Self::convert_backing_value_to_t::<RgbaImage, T>(rgba)
        {
            let resolution = self.typed_input_resolution::<T>(
                port,
                payload,
                TypedInputResolutionKind::ComputeConversion,
                Some(std::any::type_name::<daedalus_gpu::Backing<RgbaImage>>().to_string()),
                crate::convert::explain_conversion_from_types::<RgbaImage, T>(),
            );
            return Some((value, resolution));
        }

        if let Some(img) = ep.clone_cpu::<DynamicImage>()
            && let Some(value) = Self::convert_cpu_value_to_t::<DynamicImage, T>(img)
        {
            let resolution = self.typed_input_resolution::<T>(
                port,
                payload,
                TypedInputResolutionKind::ComputeConversion,
                Some(std::any::type_name::<DynamicImage>().to_string()),
                crate::convert::explain_conversion_from_types::<DynamicImage, T>(),
            );
            return Some((value, resolution));
        }

        if let Some(gray) = ep.clone_cpu::<GrayImage>()
            && let Some(value) = Self::convert_cpu_value_to_t::<GrayImage, T>(gray)
        {
            let resolution = self.typed_input_resolution::<T>(
                port,
                payload,
                TypedInputResolutionKind::ComputeConversion,
                Some(std::any::type_name::<GrayImage>().to_string()),
                crate::convert::explain_conversion_from_types::<GrayImage, T>(),
            );
            return Some((value, resolution));
        }

        if let Some(rgb) = ep.clone_cpu::<RgbImage>()
            && let Some(value) = Self::convert_cpu_value_to_t::<RgbImage, T>(rgb)
        {
            let resolution = self.typed_input_resolution::<T>(
                port,
                payload,
                TypedInputResolutionKind::ComputeConversion,
                Some(std::any::type_name::<RgbImage>().to_string()),
                crate::convert::explain_conversion_from_types::<RgbImage, T>(),
            );
            return Some((value, resolution));
        }

        if let Some(rgba) = ep.clone_cpu::<RgbaImage>()
            && let Some(value) = Self::convert_cpu_value_to_t::<RgbaImage, T>(rgba)
        {
            let resolution = self.typed_input_resolution::<T>(
                port,
                payload,
                TypedInputResolutionKind::ComputeConversion,
                Some(std::any::type_name::<RgbaImage>().to_string()),
                crate::convert::explain_conversion_from_types::<RgbaImage, T>(),
            );
            return Some((value, resolution));
        }

        None
    }

    fn resolve_typed_from_value<T>(
        &self,
        port: &str,
        correlated: &CorrelatedValue,
    ) -> Option<(T, TypedInputResolution)>
    where
        T: Any + Clone + Send + Sync + 'static,
    {
        match &correlated.inner {
            RuntimeValue::Any(any) => self.resolve_typed_from_any(port, any, &correlated.inner),
            RuntimeValue::Value(value) => self.coerce_from_value::<T>(value).map(|resolved| {
                let resolution = self.typed_input_resolution::<T>(
                    port,
                    &correlated.inner,
                    TypedInputResolutionKind::ValueCoercion,
                    Some(std::any::type_name::<Value>().to_string()),
                    None,
                );
                (resolved, resolution)
            }),
            #[cfg(feature = "gpu")]
            RuntimeValue::Data(ep) => {
                self.resolve_typed_from_data_cell::<T>(port, ep, &correlated.inner)
            }
            _ => None,
        }
    }

    #[cfg(feature = "gpu")]
    fn dynamic_image_to_t<T: Any + Clone + Send + Sync>(img: DynamicImage) -> Option<T> {
        if let Some(converted) = Self::convert_cpu_value_to_t::<DynamicImage, T>(img.clone()) {
            return Some(converted);
        }
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

    #[cfg(feature = "gpu")]
    fn data_cell_to_t<T>(&self, ep: &daedalus_gpu::DataCell) -> Option<T>
    where
        T: Any + Clone + Send + Sync + 'static,
    {
        if let Some(v) = ep.try_downcast_cpu_any::<T>() {
            return Some(v);
        }
        if let Some(backing) = ep.try_downcast_cpu_any::<daedalus_gpu::Backing<T>>() {
            return Some(backing.into_owned());
        }
        if let Some(img) = ep.try_downcast_cpu_any::<daedalus_gpu::Backing<DynamicImage>>()
            && let Some(v) = Self::convert_backing_value_to_t::<DynamicImage, T>(img)
        {
            self.record_materialization_bytes(crate::executor::runtime_value_size_bytes(
                &RuntimeValue::Data(ep.clone()),
            ));
            self.record_conversion_bytes(Self::estimate_any_bytes(&v));
            return Some(v);
        }
        if let Some(gray) = ep.try_downcast_cpu_any::<daedalus_gpu::Backing<GrayImage>>()
            && let Some(v) = Self::convert_backing_value_to_t::<GrayImage, T>(gray)
        {
            self.record_materialization_bytes(crate::executor::runtime_value_size_bytes(
                &RuntimeValue::Data(ep.clone()),
            ));
            self.record_conversion_bytes(Self::estimate_any_bytes(&v));
            return Some(v);
        }
        if let Some(rgb) = ep.try_downcast_cpu_any::<daedalus_gpu::Backing<RgbImage>>()
            && let Some(v) = Self::convert_backing_value_to_t::<RgbImage, T>(rgb)
        {
            self.record_materialization_bytes(crate::executor::runtime_value_size_bytes(
                &RuntimeValue::Data(ep.clone()),
            ));
            self.record_conversion_bytes(Self::estimate_any_bytes(&v));
            return Some(v);
        }
        if let Some(rgba) = ep.try_downcast_cpu_any::<daedalus_gpu::Backing<RgbaImage>>()
            && let Some(v) = Self::convert_backing_value_to_t::<RgbaImage, T>(rgba)
        {
            self.record_materialization_bytes(crate::executor::runtime_value_size_bytes(
                &RuntimeValue::Data(ep.clone()),
            ));
            self.record_conversion_bytes(Self::estimate_any_bytes(&v));
            return Some(v);
        }
        if let Some(img) = ep.clone_cpu::<DynamicImage>()
            && let Some(v) = Self::convert_cpu_value_to_t::<DynamicImage, T>(img)
        {
            self.record_materialization_bytes(crate::executor::runtime_value_size_bytes(
                &RuntimeValue::Data(ep.clone()),
            ));
            self.record_conversion_bytes(Self::estimate_any_bytes(&v));
            return Some(v);
        }
        if let Some(gray) = ep.clone_cpu::<GrayImage>()
            && let Some(v) = Self::convert_cpu_value_to_t::<GrayImage, T>(gray)
        {
            self.record_materialization_bytes(crate::executor::runtime_value_size_bytes(
                &RuntimeValue::Data(ep.clone()),
            ));
            self.record_conversion_bytes(Self::estimate_any_bytes(&v));
            return Some(v);
        }
        if let Some(rgb) = ep.clone_cpu::<RgbImage>()
            && let Some(v) = Self::convert_cpu_value_to_t::<RgbImage, T>(rgb)
        {
            self.record_materialization_bytes(crate::executor::runtime_value_size_bytes(
                &RuntimeValue::Data(ep.clone()),
            ));
            self.record_conversion_bytes(Self::estimate_any_bytes(&v));
            return Some(v);
        }
        if let Some(rgba) = ep.clone_cpu::<RgbaImage>()
            && let Some(v) = Self::convert_cpu_value_to_t::<RgbaImage, T>(rgba)
        {
            self.record_materialization_bytes(crate::executor::runtime_value_size_bytes(
                &RuntimeValue::Data(ep.clone()),
            ));
            self.record_conversion_bytes(Self::estimate_any_bytes(&v));
            return Some(v);
        }
        None
    }

    #[cfg(feature = "gpu")]
    fn data_cell_to_arc<T>(&self, ep: &daedalus_gpu::DataCell) -> Option<Arc<T>>
    where
        T: Any + Send + Sync + 'static,
    {
        if let Some(v) = ep.arc_cpu_any::<T>() {
            return Some(v);
        }
        if let Some(backing) = ep.arc_cpu_any::<daedalus_gpu::Backing<T>>()
            && let Some(shared) = backing.shared_arc()
        {
            return Some(shared);
        }
        if let Some(img) = ep.try_downcast_cpu_any::<daedalus_gpu::Backing<DynamicImage>>()
            && let Some(v) = Self::convert_backing_value_to_arc::<DynamicImage, T>(img)
        {
            self.record_materialization_bytes(crate::executor::runtime_value_size_bytes(
                &RuntimeValue::Data(ep.clone()),
            ));
            self.record_conversion_bytes(Self::estimate_any_bytes(v.as_ref()));
            return Some(v);
        }
        if let Some(gray) = ep.try_downcast_cpu_any::<daedalus_gpu::Backing<GrayImage>>()
            && let Some(v) = Self::convert_backing_value_to_arc::<GrayImage, T>(gray)
        {
            self.record_materialization_bytes(crate::executor::runtime_value_size_bytes(
                &RuntimeValue::Data(ep.clone()),
            ));
            self.record_conversion_bytes(Self::estimate_any_bytes(v.as_ref()));
            return Some(v);
        }
        if let Some(rgb) = ep.try_downcast_cpu_any::<daedalus_gpu::Backing<RgbImage>>()
            && let Some(v) = Self::convert_backing_value_to_arc::<RgbImage, T>(rgb)
        {
            self.record_materialization_bytes(crate::executor::runtime_value_size_bytes(
                &RuntimeValue::Data(ep.clone()),
            ));
            self.record_conversion_bytes(Self::estimate_any_bytes(v.as_ref()));
            return Some(v);
        }
        if let Some(rgba) = ep.try_downcast_cpu_any::<daedalus_gpu::Backing<RgbaImage>>()
            && let Some(v) = Self::convert_backing_value_to_arc::<RgbaImage, T>(rgba)
        {
            self.record_materialization_bytes(crate::executor::runtime_value_size_bytes(
                &RuntimeValue::Data(ep.clone()),
            ));
            self.record_conversion_bytes(Self::estimate_any_bytes(v.as_ref()));
            return Some(v);
        }
        if let Some(img) = ep.clone_cpu::<DynamicImage>()
            && let Some(v) = Self::convert_cpu_value_to_arc::<DynamicImage, T>(img)
        {
            self.record_materialization_bytes(crate::executor::runtime_value_size_bytes(
                &RuntimeValue::Data(ep.clone()),
            ));
            self.record_conversion_bytes(Self::estimate_any_bytes(v.as_ref()));
            return Some(v);
        }
        if let Some(gray) = ep.clone_cpu::<GrayImage>()
            && let Some(v) = Self::convert_cpu_value_to_arc::<GrayImage, T>(gray)
        {
            self.record_materialization_bytes(crate::executor::runtime_value_size_bytes(
                &RuntimeValue::Data(ep.clone()),
            ));
            self.record_conversion_bytes(Self::estimate_any_bytes(v.as_ref()));
            return Some(v);
        }
        if let Some(rgb) = ep.clone_cpu::<RgbImage>()
            && let Some(v) = Self::convert_cpu_value_to_arc::<RgbImage, T>(rgb)
        {
            self.record_materialization_bytes(crate::executor::runtime_value_size_bytes(
                &RuntimeValue::Data(ep.clone()),
            ));
            self.record_conversion_bytes(Self::estimate_any_bytes(v.as_ref()));
            return Some(v);
        }
        if let Some(rgba) = ep.clone_cpu::<RgbaImage>()
            && let Some(v) = Self::convert_cpu_value_to_arc::<RgbaImage, T>(rgba)
        {
            self.record_materialization_bytes(crate::executor::runtime_value_size_bytes(
                &RuntimeValue::Data(ep.clone()),
            ));
            self.record_conversion_bytes(Self::estimate_any_bytes(v.as_ref()));
            return Some(v);
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
        #[cfg(feature = "gpu")] data_edges: &'a HashSet<usize>,
        seg_idx: usize,
        node_idx: usize,
        node_id: String,
        active_nodes: Option<&'a [bool]>,
        telemetry: &'a mut ExecutionTelemetry,
        backpressure: BackpressureStrategy,
        const_inputs: &[(String, daedalus_data::model::Value)],
        const_coercers: Option<ConstCoercerMap>,
        output_movers: Option<OutputMoverMap>,
        any_conversion_cache: AnyConversionCacheHandle,
        #[cfg(feature = "gpu")] materialization_cache: Option<MaterializationCacheHandle>,
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
                    EdgeStorage::Locked { queue, metrics } => {
                        if let Ok(mut q) = queue.lock() {
                            while let Some(payload) = q.pop_front() {
                                #[allow(unused_mut)]
                                let mut payload = payload;
                                let now = Instant::now();
                                telemetry.record_edge_wait(
                                    *edge_idx,
                                    now.saturating_duration_since(payload.enqueued_at),
                                );
                                let transport_bytes = if cfg!(feature = "metrics")
                                    && telemetry.metrics_level.is_detailed()
                                {
                                    crate::executor::runtime_value_size_bytes(&payload.inner)
                                } else {
                                    None
                                };
                                telemetry.record_node_transport_in(node_idx, transport_bytes);
                                let port = edges
                                    .get(*edge_idx)
                                    .map(|(_, _, _, to_port, _)| to_port.clone())
                                    .unwrap_or_default();
                                #[cfg(feature = "gpu")]
                                {
                                    payload = Self::convert_incoming(
                                        payload,
                                        ConvertIncomingContext {
                                            node_idx,
                                            edge_idx: *edge_idx,
                                            entries: gpu_entry_edges,
                                            exits: gpu_exit_edges,
                                            materialization_cache: materialization_cache.as_ref(),
                                            gpu: gpu.as_ref(),
                                            telemetry,
                                        },
                                    );
                                }
                                drained.push(DrainedInput {
                                    port,
                                    edge_idx: *edge_idx,
                                    payload,
                                });
                            }
                            let current_queue_bytes = q.transport_bytes();
                            metrics.set_current_bytes(current_queue_bytes);
                            telemetry.record_edge_depth(*edge_idx, q.len());
                            telemetry.record_edge_queue_bytes(*edge_idx, current_queue_bytes);
                        }
                    }
                    #[cfg(feature = "lockfree-queues")]
                    EdgeStorage::BoundedLf { queue, metrics } => {
                        while let Some(payload) = queue.pop() {
                            #[allow(unused_mut)]
                            let mut payload = payload;
                            let now = Instant::now();
                            telemetry.record_edge_wait(
                                *edge_idx,
                                now.saturating_duration_since(payload.enqueued_at),
                            );
                            let transport_bytes = if cfg!(feature = "metrics")
                                && telemetry.metrics_level.is_detailed()
                            {
                                crate::executor::runtime_value_size_bytes(&payload.inner)
                            } else {
                                None
                            };
                            metrics.adjust_bytes(0, transport_bytes.unwrap_or(0));
                            telemetry.record_node_transport_in(node_idx, transport_bytes);
                            let port = edges
                                .get(*edge_idx)
                                .map(|(_, _, _, to_port, _)| to_port.clone())
                                .unwrap_or_default();
                            #[cfg(feature = "gpu")]
                            {
                                payload = Self::convert_incoming(
                                    payload,
                                    ConvertIncomingContext {
                                        node_idx,
                                        edge_idx: *edge_idx,
                                        entries: gpu_entry_edges,
                                        exits: gpu_exit_edges,
                                        materialization_cache: materialization_cache.as_ref(),
                                        gpu: gpu.as_ref(),
                                        telemetry,
                                    },
                                );
                            }
                            drained.push(DrainedInput {
                                port,
                                edge_idx: *edge_idx,
                                payload,
                            });
                        }
                        telemetry.record_edge_depth(*edge_idx, queue.len());
                        let (current_queue_bytes, _) = metrics.snapshot();
                        telemetry.record_edge_queue_bytes(*edge_idx, current_queue_bytes);
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
                    runtime_value_desc(&item.payload.inner)
                );
            }
        }
        if std::env::var_os("DAEDALUS_TRACE_EDGE_IO_STDERR").is_some() {
            for item in &drained {
                eprintln!(
                    "node input drained node={} port={} edge_idx={} payload={}",
                    node_id,
                    item.port,
                    item.edge_idx,
                    runtime_value_desc(&item.payload.inner)
                );
            }
        }
        if log::log_enabled!(log::Level::Debug) && drained.is_empty() {
            let ports: Vec<String> = incoming_edges
                .iter()
                .filter_map(|edge_idx| {
                    edges
                        .get(*edge_idx)
                        .map(|(_, _, _, to_port, _)| to_port.clone())
                })
                .collect();
            if !ports.is_empty() {
                log::debug!("node inputs empty node={} ports={:?}", node_id, ports);
            }
        }

        let has_drained = !drained.is_empty();
        let mut const_payloads: Vec<(String, CorrelatedValue)> = Vec::new();
        // Apply constant defaults only when no incoming payload exists for the port.
        for (port, value) in const_inputs {
            if drained.iter().any(|p| p.port == *port) {
                continue;
            }
            // Keep authored constants as `Value` so plugin/FFI boundaries retain full type
            // context (notably enum-like config ports such as `mode`).
            let payload = RuntimeValue::Value(value.clone());
            const_payloads.push((port.clone(), CorrelatedValue::from_edge(payload)));
        }

        let const_ports: std::collections::HashSet<String> =
            const_inputs.iter().map(|(port, _)| port.clone()).collect();
        let (mut aligned_inputs, leftovers, ready) =
            align_drained_inputs(drained, &sync_groups, &const_ports);
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
        if std::env::var_os("DAEDALUS_TRACE_EDGE_IO_STDERR").is_some() {
            for (port, payload) in &aligned_inputs {
                eprintln!(
                    "node input aligned node={} port={} payload={}",
                    node_id,
                    port,
                    runtime_value_desc(&payload.inner)
                );
            }
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
        let telemetry_ptr = telemetry as *mut ExecutionTelemetry;

        Self {
            inputs: aligned_inputs,
            borrowed_cache: std::cell::UnsafeCell::new(Vec::new()),
            sync_groups,
            port_overrides,
            current_corr_id,
            outgoing: outgoing_edges,
            has_incoming_edges,
            queues,
            telemetry_ptr,
            telemetry_lifetime: std::marker::PhantomData,
            edges,
            seg_idx,
            node_idx,
            node_id,
            active_nodes,
            warnings_seen,
            backpressure,
            #[cfg(feature = "gpu")]
            gpu,
            #[cfg(feature = "gpu")]
            target_compute,
            #[cfg(feature = "gpu")]
            data_edges,
            #[cfg(feature = "gpu")]
            materialization_cache,
            const_coercers,
            output_movers,
            any_conversion_cache,
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
    pub fn inputs(&self) -> &[(String, CorrelatedValue)] {
        &self.inputs
    }

    /// Whether this node has any incoming edges.
    pub fn has_incoming_edges(&self) -> bool {
        self.has_incoming_edges
    }

    fn take_input(&mut self, port: &str) -> Option<(usize, CorrelatedValue)> {
        let idx = match self.inputs.iter().position(|(p, _)| p == port) {
            Some(idx) => idx,
            None => {
                if std::env::var_os("DAEDALUS_TRACE_MISSING_INPUTS").is_some() {
                    let ports: Vec<&str> = self.inputs.iter().map(|(p, _)| p.as_str()).collect();
                    eprintln!(
                        "daedalus-runtime: missing input node={} port={} available_ports={:?}",
                        self.node_id, port, ports
                    );
                }
                return None;
            }
        };
        let payload = self.inputs.remove(idx).1;
        Some((idx, payload))
    }

    fn restore_input(&mut self, idx: usize, port: &str, payload: CorrelatedValue) {
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
    /// use daedalus_runtime::executor::RuntimeValue;
    /// fn handler(io: &mut NodeIo) {
    ///     io.push_output(Some("out"), RuntimeValue::Unit);
    /// }
    /// ```
    pub fn push_output(&mut self, port: Option<&str>, payload: RuntimeValue) {
        let correlated = CorrelatedValue {
            correlation_id: self.current_corr_id,
            inner: payload,
            enqueued_at: Instant::now(),
        };
        self.push_correlated(port, correlated);
    }

    /// Push a pre-correlated payload (used by host-bridge style nodes).
    pub fn push_correlated_value(&mut self, port: Option<&str>, correlated: CorrelatedValue) {
        self.push_correlated(port, correlated);
    }

    fn push_correlated(&mut self, port: Option<&str>, correlated: CorrelatedValue) {
        #[cfg(feature = "gpu")]
        let mut matches: Vec<(
            usize,
            String,
            EdgePolicyKind,
            BackpressureStrategy,
            Option<usize>,
            bool,
        )> = Vec::new();
        #[cfg(not(feature = "gpu"))]
        let mut matches: Vec<(
            usize,
            String,
            EdgePolicyKind,
            BackpressureStrategy,
            Option<usize>,
        )> = Vec::new();
        for edge_idx in &self.outgoing {
            if let Some((_, from_port, to, _to_port, policy)) = self.edges.get(*edge_idx) {
                if let Some(active_nodes) = self.active_nodes
                    && !active_nodes.get(to.0).copied().unwrap_or(true)
                {
                    continue;
                }
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
                    let needs_data = self.data_edges.contains(edge_idx);
                    matches.push((
                        *edge_idx,
                        from_port.clone(),
                        policy.clone(),
                        bp,
                        cap_override,
                        needs_data,
                    ));
                }
                #[cfg(not(feature = "gpu"))]
                {
                    matches.push((
                        *edge_idx,
                        from_port.clone(),
                        policy.clone(),
                        bp,
                        cap_override,
                    ));
                }
            }
        }

        if matches.len() == 1 {
            #[cfg(feature = "gpu")]
            let (edge_idx, from_port, policy, bp, cap_override, needs_data) = matches.remove(0);
            #[cfg(not(feature = "gpu"))]
            let (edge_idx, from_port, policy, bp, cap_override) = matches.remove(0);
            if std::env::var_os("DAEDALUS_TRACE_EDGE_IO").is_some() {
                log::warn!(
                    "node output enqueue node={} port={} edge_idx={} payload={}",
                    self.node_id,
                    from_port,
                    edge_idx,
                    runtime_value_desc(&correlated.inner)
                );
            }
            if std::env::var_os("DAEDALUS_TRACE_EDGE_IO_STDERR").is_some() {
                eprintln!(
                    "node output enqueue node={} port={} edge_idx={} payload={}",
                    self.node_id,
                    from_port,
                    edge_idx,
                    runtime_value_desc(&correlated.inner)
                );
            }
            let mut effective_policy = policy;
            if let Some(cap) = cap_override {
                effective_policy = EdgePolicyKind::Bounded { cap };
            }
            #[cfg(feature = "gpu")]
            let mut materialized_bytes = None;
            #[cfg(feature = "gpu")]
            let correlated = if needs_data {
                let mut updated = correlated;
                let (promoted, materialized) = match updated.inner {
                    RuntimeValue::Any(a) => {
                        promote_any_with_cache(&a, self.materialization_cache.as_ref())
                    }
                    other => promote_value_for_host(other),
                };
                if materialized {
                    materialized_bytes = crate::executor::runtime_value_size_bytes(&promoted);
                }
                updated.inner = promoted;
                updated
            } else {
                correlated
            };
            #[cfg(not(feature = "gpu"))]
            let correlated = correlated;
            #[cfg(feature = "gpu")]
            self.record_materialization_bytes(materialized_bytes);
            let transport_bytes = if cfg!(feature = "metrics")
                && self.with_telemetry(|telemetry| telemetry.metrics_level.is_detailed())
            {
                crate::executor::runtime_value_size_bytes(&correlated.inner)
            } else {
                None
            };
            self.with_telemetry(|telemetry| {
                telemetry.record_node_transport_out(self.node_idx, transport_bytes);
                apply_policy_owned(ApplyPolicyOwnedArgs {
                    edge_idx,
                    policy: &effective_policy,
                    payload: correlated,
                    queues: self.queues,
                    warnings_seen: self.warnings_seen,
                    telem: telemetry,
                    warning_label: Some(format!("edge_{}_{}", self.node_id, from_port)),
                    backpressure: bp,
                });
            });
            return;
        }

        #[cfg(feature = "gpu")]
        for (edge_idx, from_port, mut policy, bp, cap_override, needs_data) in matches {
            if std::env::var_os("DAEDALUS_TRACE_EDGE_IO").is_some() {
                log::warn!(
                    "node output enqueue node={} port={} edge_idx={} payload={}",
                    self.node_id,
                    from_port,
                    edge_idx,
                    runtime_value_desc(&correlated.inner)
                );
            }
            if std::env::var_os("DAEDALUS_TRACE_EDGE_IO_STDERR").is_some() {
                eprintln!(
                    "node output enqueue node={} port={} edge_idx={} payload={}",
                    self.node_id,
                    from_port,
                    edge_idx,
                    runtime_value_desc(&correlated.inner)
                );
            }
            let mut materialized_bytes = None;
            if let Some(cap) = cap_override {
                policy = EdgePolicyKind::Bounded { cap };
            }
            let mut payload = correlated.clone();
            if needs_data {
                let (promoted, materialized) = match payload.inner {
                    RuntimeValue::Any(a) => {
                        promote_any_with_cache(&a, self.materialization_cache.as_ref())
                    }
                    other => promote_value_for_host(other),
                };
                if materialized {
                    materialized_bytes = crate::executor::runtime_value_size_bytes(&promoted);
                }
                payload.inner = promoted;
            }
            self.record_materialization_bytes(materialized_bytes);
            let transport_bytes = if cfg!(feature = "metrics")
                && self.with_telemetry(|telemetry| telemetry.metrics_level.is_detailed())
            {
                crate::executor::runtime_value_size_bytes(&payload.inner)
            } else {
                None
            };
            self.with_telemetry(|telemetry| {
                telemetry.record_node_transport_out(self.node_idx, transport_bytes);
                apply_policy(
                    edge_idx,
                    &policy,
                    &payload,
                    self.queues,
                    self.warnings_seen,
                    telemetry,
                    Some(format!("edge_{}_{}", self.node_id, from_port)),
                    bp,
                );
            });
        }
        #[cfg(not(feature = "gpu"))]
        for (edge_idx, from_port, mut policy, bp, cap_override) in matches {
            if std::env::var_os("DAEDALUS_TRACE_EDGE_IO").is_some() {
                log::warn!(
                    "node output enqueue node={} port={} edge_idx={} payload={}",
                    self.node_id,
                    from_port,
                    edge_idx,
                    runtime_value_desc(&correlated.inner)
                );
            }
            if std::env::var_os("DAEDALUS_TRACE_EDGE_IO_STDERR").is_some() {
                eprintln!(
                    "node output enqueue node={} port={} edge_idx={} payload={}",
                    self.node_id,
                    from_port,
                    edge_idx,
                    runtime_value_desc(&correlated.inner)
                );
            }
            if let Some(cap) = cap_override {
                policy = EdgePolicyKind::Bounded { cap };
            }
            let transport_bytes = if cfg!(feature = "metrics")
                && self.with_telemetry(|telemetry| telemetry.metrics_level.is_detailed())
            {
                crate::executor::runtime_value_size_bytes(&correlated.inner)
            } else {
                None
            };
            self.with_telemetry(|telemetry| {
                telemetry.record_node_transport_out(self.node_idx, transport_bytes);
                apply_policy(
                    edge_idx,
                    &policy,
                    &correlated,
                    self.queues,
                    self.warnings_seen,
                    telemetry,
                    Some(format!("edge_{}_{}", self.node_id, from_port)),
                    bp,
                );
            });
        }
    }

    #[cfg(feature = "gpu")]
    pub fn push_compute<T>(&mut self, port: Option<&str>, value: daedalus_gpu::Compute<T>)
    where
        T: daedalus_gpu::DeviceBridge + Clone + Send + Sync + 'static,
        T::Device: Clone + Send + Sync + 'static,
    {
        match value {
            daedalus_gpu::Compute::Cpu(v) => {
                let payload = RuntimeValue::Data(daedalus_gpu::DataCell::from_cpu::<T>(v));
                self.push_output(port, payload);
            }
            daedalus_gpu::Compute::Gpu(g) => {
                let payload = RuntimeValue::Data(daedalus_gpu::DataCell::from_gpu::<T>(g));
                self.push_output(port, payload);
            }
        }
    }

    pub fn push_any<T: Any + Send + Sync + 'static>(&mut self, port: Option<&str>, value: T) {
        self.push_output(port, RuntimeValue::Any(Arc::new(value)));
    }

    /// Push an `Arc<T>` as an `Any` payload without re-wrapping/allocating.
    ///
    /// This is the preferred path for in-place / copy-on-write transforms:
    /// nodes can `take_any_arc`, mutate via `Arc::make_mut`, then `push_any_arc`.
    pub fn push_any_arc<T: Any + Send + Sync + 'static>(
        &mut self,
        port: Option<&str>,
        value: Arc<T>,
    ) {
        let any: Arc<dyn Any + Send + Sync> = value;
        self.push_output(port, RuntimeValue::Any(any));
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
        self.push_output(port, RuntimeValue::Value(value));
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
    pub fn inputs_for<'b>(&'b self, port: &str) -> impl Iterator<Item = &'b CorrelatedValue> {
        self.inputs
            .iter()
            .filter(move |(p, _)| p == port)
            .map(|(_, payload)| payload)
    }

    /// Typed accessor for Any payloads.
    pub fn get_any<T: Any + Clone + Send + Sync>(&self, port: &str) -> Option<T> {
        self.inputs_for(port).find_map(|p| match &p.inner {
            RuntimeValue::Any(a) => {
                a.downcast_ref::<T>()
                    .cloned()
                    .or_else(|| a.downcast_ref::<Arc<T>>().map(|arc| (**arc).clone()))
                    .or_else(|| {
                        #[cfg(feature = "gpu")]
                        {
                            Self::any_backing_owned::<T>(a)
                        }
                        #[cfg(not(feature = "gpu"))]
                        {
                            None
                        }
                    })
                    .or_else(|| self.coerce_const_any::<T>(a.as_ref()))
                    // Allow CPU-side conversions between Any payloads (e.g., GrayImage -> DynamicImage)
                    // using the global conversion registry.
                    .or_else(|| {
                        self.convert_any_to_arc_cached::<T>(a)
                            .map(|arc| (*arc).clone())
                    })
            }
            RuntimeValue::Value(v) => self.coerce_from_value::<T>(v),
            #[cfg(feature = "gpu")]
            RuntimeValue::Data(ep) => self.data_cell_to_t::<T>(ep),
            _ => None,
        })
    }

    /// Borrow a typed Any payload without cloning.
    pub fn get_any_ref<T: Any + Send + Sync>(&self, port: &str) -> Option<&T> {
        self.inputs_for(port).find_map(|p| match &p.inner {
            RuntimeValue::Any(a) => a
                .downcast_ref::<T>()
                .or_else(|| a.downcast_ref::<Arc<T>>().map(Arc::as_ref))
                .or({
                    #[cfg(feature = "gpu")]
                    {
                        Self::any_backing_ref::<T>(a)
                    }
                    #[cfg(not(feature = "gpu"))]
                    {
                        None
                    }
                }),
            #[cfg(feature = "gpu")]
            RuntimeValue::Data(ep) => ep.as_cpu_any::<T>().or_else(|| {
                ep.as_cpu_any::<daedalus_gpu::Backing<T>>()
                    .map(AsRef::as_ref)
            }),
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
        if let Some((value, _)) = self.get_typed_with_resolution::<T>(port) {
            return Some(self.cache_borrowed(value));
        }

        if std::env::var_os("DAEDALUS_TRACE_MISSING_INPUTS").is_some() {
            let desc = self
                .inputs_for(port)
                .next()
                .map(|payload| match &payload.inner {
                    RuntimeValue::Any(a) => {
                        format!("Any({})", std::any::type_name_of_val(a.as_ref()))
                    }
                    #[cfg(feature = "gpu")]
                    RuntimeValue::Data(ep) => format!("Data({ep:?})"),
                    RuntimeValue::Value(v) => format!("Value({v:?})"),
                    RuntimeValue::Bytes(_) => "Bytes".to_string(),
                    RuntimeValue::Unit => "Unit".to_string(),
                })
                .unwrap_or_else(|| "None".to_string());
            eprintln!(
                "daedalus-runtime: input mismatch node={} port={} expected={} payload={}",
                self.node_id,
                port,
                std::any::type_name::<T>(),
                desc
            );
        }

        None
    }

    fn convert_any_to_arc_cached<T>(&self, a: &Arc<dyn Any + Send + Sync>) -> Option<Arc<T>>
    where
        T: Any + Send + Sync + 'static,
    {
        // Fast path: already the desired type.
        if let Ok(arc) = Arc::downcast::<T>(a.clone()) {
            return Some(arc);
        }

        // `Arc::as_ptr` returns a fat pointer for trait objects; cast through `*const ()` to
        // obtain the data pointer for hashing.
        let src_ptr = (Arc::as_ptr(a) as *const ()) as usize;
        let key = (src_ptr, TypeId::of::<T>());

        if let Ok(guard) = self.any_conversion_cache.lock()
            && let Some(hit) = guard.get(&key)
        {
            return Arc::downcast::<T>(hit.clone()).ok();
        }

        let converted = crate::convert::convert_to_arc::<T>(a)?;
        self.record_conversion_bytes(
            Self::estimate_any_bytes(converted.as_ref())
                .or_else(|| Self::estimate_any_bytes(a.as_ref())),
        );
        if let Ok(mut guard) = self.any_conversion_cache.lock() {
            let as_any: Arc<dyn Any + Send + Sync> = converted.clone();
            guard.insert(key, as_any);
        }
        Some(converted)
    }

    /// Borrow an `Arc<T>` from an `Any` payload (increments refcount, does not clone `T`).
    pub fn get_any_arc<T>(&self, port: &str) -> Option<Arc<T>>
    where
        T: Any + Send + Sync + 'static,
    {
        self.inputs_for(port).find_map(|p| match &p.inner {
            RuntimeValue::Any(a) => a
                .downcast_ref::<Arc<T>>()
                .cloned()
                .or_else(|| {
                    #[cfg(feature = "gpu")]
                    {
                        Self::any_backing_arc::<T>(a)
                    }
                    #[cfg(not(feature = "gpu"))]
                    {
                        None
                    }
                })
                .or_else(|| self.convert_any_to_arc_cached::<T>(a)),
            #[cfg(feature = "gpu")]
            RuntimeValue::Data(ep) => self.data_cell_to_arc::<T>(ep),
            _ => None,
        })
    }

    /// Borrow a typed CPU payload by reference (no clone).
    #[cfg(feature = "gpu")]
    pub fn get_compute_ref<T>(&self, port: &str) -> Option<&T>
    where
        T: daedalus_gpu::DeviceBridge + Clone + Send + Sync + 'static,
        T::Device: Clone + Send + Sync + 'static,
    {
        self.inputs_for(port).find_map(|p| match &p.inner {
            RuntimeValue::Data(ep) => ep
                .as_cpu::<T>()
                .or_else(|| ep.as_cpu::<daedalus_gpu::Backing<T>>().map(AsRef::as_ref)),
            _ => None,
        })
    }

    /// Borrow a typed CPU payload as an `Arc<T>` (no clone) when possible.
    #[cfg(feature = "gpu")]
    pub fn get_compute_arc<T>(&self, port: &str) -> Option<Arc<T>>
    where
        T: Send + Sync + 'static,
    {
        self.inputs_for(port).find_map(|p| match &p.inner {
            RuntimeValue::Data(ep) => self.data_cell_to_arc::<T>(ep),
            _ => None,
        })
    }

    /// Take a typed CPU payload as an `Arc<T>`, dropping the input payload.
    ///
    /// This is a true move at the graph level: if the value is shared elsewhere it remains
    /// valid (because the underlying allocation is reference-counted).
    #[cfg(feature = "gpu")]
    pub fn take_compute_arc<T>(&mut self, port: &str) -> Option<Arc<T>>
    where
        T: Send + Sync + 'static,
    {
        let (idx, mut payload) = self.take_input(port)?;
        let mut out: Option<Arc<T>> = None;
        match std::mem::replace(&mut payload.inner, RuntimeValue::Unit) {
            RuntimeValue::Data(ep) => {
                out = ep.arc_cpu_any::<T>();
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

    /// Take a typed CPU payload and return a COW-mutable wrapper.
    #[cfg(feature = "gpu")]
    pub fn take_compute_cow_mut<T>(&mut self, port: &str) -> Option<CowArcMut<T>>
    where
        T: Clone + Send + Sync + 'static,
    {
        self.take_compute_arc::<T>(port).map(CowArcMut::new)
    }

    /// Move a typed Any payload, cloning only when shared.
    pub fn get_any_mut<T>(&mut self, port: &str) -> Option<T>
    where
        T: Any + Clone + Send + Sync,
    {
        let (idx, payload) = self.take_input(port)?;
        let mut handled = None;
        let mut payload = payload;
        match std::mem::replace(&mut payload.inner, RuntimeValue::Unit) {
            RuntimeValue::Any(a) => match Arc::downcast::<T>(a) {
                Ok(arc) => {
                    handled = Some(match Arc::try_unwrap(arc) {
                        Ok(v) => v,
                        Err(arc) => (*arc).clone(),
                    });
                }
                Err(a) => match Arc::downcast::<Arc<T>>(a) {
                    Ok(arc) => {
                        handled = Some((**arc).clone());
                    }
                    Err(a) => {
                        payload.inner = RuntimeValue::Any(a);
                    }
                },
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

    /// Take an `Arc<T>` from an `Any` payload without cloning `T`.
    ///
    /// This enables true in-place / COW transforms for `Arc<T>` payloads.
    /// If the `Arc` is uniquely owned at this point in the graph, `Arc::make_mut`
    /// will mutate without cloning.
    pub fn take_any_arc<T>(&mut self, port: &str) -> Option<Arc<T>>
    where
        T: Any + Send + Sync + 'static,
    {
        let (idx, mut payload) = self.take_input(port)?;
        let mut out: Option<Arc<T>> = None;
        match std::mem::replace(&mut payload.inner, RuntimeValue::Unit) {
            RuntimeValue::Any(a) => match Arc::downcast::<T>(a) {
                Ok(arc) => {
                    out = Some(arc);
                }
                Err(a) => match Arc::downcast::<Arc<T>>(a) {
                    Ok(arc) => {
                        out = Some((*arc).clone());
                    }
                    Err(a) => {
                        payload.inner = RuntimeValue::Any(a);
                    }
                },
            },
            other => {
                payload.inner = other;
            }
        }
        if out.is_none() {
            self.restore_input(idx, port, payload);
        }
        out
    }

    /// Take an input `Arc<T>` and return a COW-mutable wrapper.
    ///
    /// This is the recommended pattern for `PortAccessMode::MutBorrowed` inputs: downstream code
    /// can mutate in-place via `Arc::make_mut` when the graph proves exclusivity, otherwise it
    /// transparently clones.
    pub fn take_any_cow_mut<T>(&mut self, port: &str) -> Option<CowArcMut<T>>
    where
        T: Any + Clone + Send + Sync + 'static,
    {
        self.take_any_arc::<T>(port).map(CowArcMut::new)
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
        match std::mem::replace(&mut payload.inner, RuntimeValue::Unit) {
            #[cfg(feature = "gpu")]
            RuntimeValue::Data(ep) => {
                let mut ep_opt = Some(ep);
                let downcast_owned = |value: Box<dyn Any + Send + Sync>| {
                    value.downcast::<T>().ok().map(|boxed| *boxed)
                };
                if std::env::var_os("DAEDALUS_TRACE_PAYLOAD_TAKE").is_some()
                    && let Some(ep) = ep_opt.as_ref()
                {
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
                } else if want == TypeId::of::<RgbaImage>()
                    && let Some(ep) = ep_opt.take()
                {
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

                if out.is_none()
                    && let Some(ep) = ep_opt.as_ref()
                    && (want == TypeId::of::<DynamicImage>()
                        || want == TypeId::of::<GrayImage>()
                        || want == TypeId::of::<GrayAlphaImage>()
                        || want == TypeId::of::<RgbImage>()
                        || want == TypeId::of::<RgbaImage>())
                    && let Some(img) = ep.clone_cpu::<DynamicImage>()
                {
                    out = Self::dynamic_image_to_t::<T>(img);
                }
                if out.is_none()
                    && let Some(ep) = ep_opt.take()
                {
                    match ep.take_cpu_any::<T>() {
                        Ok(value) => out = Some(value),
                        Err(rest) => ep_opt = Some(rest),
                    }
                }
                if out.is_none()
                    && let Some(ep) = ep_opt
                {
                    payload.inner = RuntimeValue::Data(ep);
                }
            }
            RuntimeValue::Any(a) => {
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
                            out = Self::coerce_from_i64::<T>(v)
                                .or_else(|| self.coerce_via_registry::<T>(&Value::Int(v)));
                        }
                        Err(any) => match Arc::downcast::<f64>(any) {
                            Ok(arc) => {
                                let v = match Arc::try_unwrap(arc) {
                                    Ok(v) => v,
                                    Err(arc) => *arc,
                                };
                                out = Self::coerce_from_f64::<T>(v)
                                    .or_else(|| self.coerce_via_registry::<T>(&Value::Float(v)));
                            }
                            Err(any) => match Arc::downcast::<bool>(any) {
                                Ok(arc) => {
                                    let v = match Arc::try_unwrap(arc) {
                                        Ok(v) => v,
                                        Err(arc) => *arc,
                                    };
                                    let any_ref: &dyn Any = &v;
                                    out = any_ref
                                        .downcast_ref::<T>()
                                        .cloned()
                                        .or_else(|| self.coerce_via_registry::<T>(&Value::Bool(v)));
                                }
                                Err(any) => match Arc::downcast::<String>(any) {
                                    Ok(arc) => {
                                        let v = match Arc::try_unwrap(arc) {
                                            Ok(v) => v,
                                            Err(arc) => (*arc).clone(),
                                        };
                                        let any_ref: &dyn Any = &v;
                                        out = any_ref.downcast_ref::<T>().cloned().or_else(|| {
                                            self.coerce_via_registry::<T>(&Value::String(v.into()))
                                        });
                                    }
                                    Err(any) => {
                                        match Arc::downcast::<daedalus_data::model::Value>(any) {
                                            Ok(arc) => {
                                                let v = match Arc::try_unwrap(arc) {
                                                    Ok(v) => v,
                                                    Err(arc) => (*arc).clone(),
                                                };
                                                out = self.coerce_from_value::<T>(&v);
                                            }
                                            Err(any) => {
                                                payload.inner = RuntimeValue::Any(any);
                                            }
                                        }
                                    }
                                },
                            },
                        },
                    },
                }
            }
            RuntimeValue::Value(v) => {
                out = self.coerce_from_value::<T>(&v);
                payload.inner = RuntimeValue::Value(v);
            }
            other => {
                payload.inner = other;
            }
        }

        if out.is_none() {
            if std::env::var_os("DAEDALUS_TRACE_MISSING_INPUTS").is_some() {
                let desc = match &payload.inner {
                    RuntimeValue::Any(a) => {
                        format!("Any({})", std::any::type_name_of_val(a.as_ref()))
                    }
                    #[cfg(feature = "gpu")]
                    RuntimeValue::Data(ep) => format!("Data({ep:?})"),
                    RuntimeValue::Value(v) => format!("Value({v:?})"),
                    RuntimeValue::Bytes(_) => "Bytes".to_string(),
                    RuntimeValue::Unit => "Unit".to_string(),
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
                RuntimeValue::Any(a) => {
                    if let Some(v) = a
                        .downcast_ref::<T>()
                        .cloned()
                        .or_else(|| self.coerce_const_any::<T>(a.as_ref()))
                    {
                        out.push(v);
                    }
                }
                #[cfg(feature = "gpu")]
                RuntimeValue::Data(ep) => {
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
    pub fn get_any_all_fanin_indexed<T: Any + Clone + Send + Sync>(
        &self,
        prefix: &str,
    ) -> Vec<(u32, T)> {
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

        if let Some(i) = v.downcast_ref::<i64>().copied() {
            return Self::coerce_from_i64::<T>(i)
                .or_else(|| self.coerce_via_registry::<T>(&V::Int(i)));
        }

        if let Some(f) = v.downcast_ref::<f64>().copied() {
            return Self::coerce_from_f64::<T>(f)
                .or_else(|| self.coerce_via_registry::<T>(&V::Float(f)));
        }

        if let Some(b) = v.downcast_ref::<bool>().copied() {
            let any_ref: &dyn Any = &b;
            return any_ref
                .downcast_ref::<T>()
                .cloned()
                .or_else(|| self.coerce_via_registry::<T>(&V::Bool(b)));
        }

        if let Some(s) = v.downcast_ref::<String>().cloned() {
            let any_ref: &dyn Any = &s;
            return any_ref
                .downcast_ref::<T>()
                .cloned()
                .or_else(|| self.coerce_via_registry::<T>(&V::String(s.into())));
        }

        if let Some(val) = v.downcast_ref::<daedalus_data::model::Value>().cloned() {
            return self.coerce_from_value::<T>(&val);
        }

        None
    }

    fn coerce_via_registry<T: Any + Clone>(&self, v: &daedalus_data::model::Value) -> Option<T> {
        let key = std::any::type_name::<T>();
        let global = GLOBAL_CONST_COERCERS.get_or_init(new_const_coercer_map);
        let map = self.const_coercers.as_ref().unwrap_or(global);
        let guard = map.read().ok()?;
        let Some(coercer) = guard.get(key) else {
            if std::env::var_os("DAEDALUS_TRACE_CONST_COERCERS_STDERR").is_some() {
                let kind = match v {
                    daedalus_data::model::Value::Unit => "Unit",
                    daedalus_data::model::Value::Bool(_) => "Bool",
                    daedalus_data::model::Value::Int(_) => "Int",
                    daedalus_data::model::Value::Float(_) => "Float",
                    daedalus_data::model::Value::String(_) => "String",
                    daedalus_data::model::Value::Bytes(_) => "Bytes",
                    daedalus_data::model::Value::List(_) => "List",
                    daedalus_data::model::Value::Map(_) => "Map",
                    daedalus_data::model::Value::Tuple(_) => "Tuple",
                    daedalus_data::model::Value::Struct(_) => "Struct",
                    daedalus_data::model::Value::Enum(_) => "Enum",
                };
                let mut preview = guard.keys().take(24).cloned().collect::<Vec<_>>();
                preview.sort_unstable();
                eprintln!(
                    "daedalus-runtime: missing const coercer key='{}' value_kind='{}' known_keys_sample={:?}",
                    key, kind, preview
                );
            }
            return None;
        };
        let Some(out) = coercer(v) else {
            if std::env::var_os("DAEDALUS_TRACE_CONST_COERCERS_STDERR").is_some() {
                let kind = match v {
                    daedalus_data::model::Value::Unit => "Unit",
                    daedalus_data::model::Value::Bool(_) => "Bool",
                    daedalus_data::model::Value::Int(_) => "Int",
                    daedalus_data::model::Value::Float(_) => "Float",
                    daedalus_data::model::Value::String(_) => "String",
                    daedalus_data::model::Value::Bytes(_) => "Bytes",
                    daedalus_data::model::Value::List(_) => "List",
                    daedalus_data::model::Value::Map(_) => "Map",
                    daedalus_data::model::Value::Tuple(_) => "Tuple",
                    daedalus_data::model::Value::Struct(_) => "Struct",
                    daedalus_data::model::Value::Enum(_) => "Enum",
                };
                eprintln!(
                    "daedalus-runtime: const coercer returned None key='{}' value_kind='{}'",
                    key, kind
                );
            }
            return None;
        };
        match out.downcast::<T>() {
            Ok(b) => Some((*b).clone()),
            Err(_) => {
                if std::env::var_os("DAEDALUS_TRACE_CONST_COERCERS_STDERR").is_some() {
                    eprintln!(
                        "daedalus-runtime: const coercer downcast failed key='{}' target_type='{}'",
                        key,
                        std::any::type_name::<T>()
                    );
                }
                None
            }
        }
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
        self.get_typed_with_resolution(port).map(|(value, _)| value)
    }

    pub fn get_typed_with_resolution<T>(&self, port: &str) -> Option<(T, TypedInputResolution)>
    where
        T: Any + Clone + Send + Sync + 'static,
    {
        #[cfg(feature = "gpu")]
        let want = std::any::TypeId::of::<T>();

        let resolved = self
            .inputs_for(port)
            .find_map(|payload| self.resolve_typed_from_value::<T>(port, payload));

        #[cfg(feature = "gpu")]
        if log::log_enabled!(log::Level::Debug)
            && want == std::any::TypeId::of::<image::DynamicImage>()
        {
            log::debug!(
                "get_typed dynamic_image port={} resolved={}",
                port,
                resolved.is_some()
            );
        }

        if resolved.is_none() && std::env::var_os("DAEDALUS_TRACE_PAYLOAD_GET_STDERR").is_some() {
            let payload = self
                .inputs_for(port)
                .next()
                .map(|cp| runtime_value_desc(&cp.inner))
                .unwrap_or_else(|| "None".to_string());
            eprintln!(
                "daedalus-runtime: get_typed miss node={} port={} type={} payload={}",
                self.node_id,
                port,
                std::any::type_name::<T>(),
                payload
            );
        }

        resolved
    }

    pub fn explain_typed_input<T>(&self, port: &str) -> Option<TypedInputResolution>
    where
        T: Any + Clone + Send + Sync + 'static,
    {
        self.get_typed_with_resolution::<T>(port)
            .map(|(_, resolution)| resolution)
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
            RuntimeValue::Any(a) => Some(a.as_ref() as &dyn Any),
            _ => None,
        })
    }

    #[cfg(feature = "gpu")]
    pub fn get_data_cell(&self, port: &str) -> Option<&daedalus_gpu::DataCell> {
        self.inputs_for(port).find_map(|p| match &p.inner {
            RuntimeValue::Data(ep) => Some(ep),
            _ => None,
        })
    }

    #[cfg(feature = "gpu")]
    pub fn get_compute<T>(&self, port: &str) -> Option<daedalus_gpu::Compute<T>>
    where
        T: daedalus_gpu::DeviceBridge + Clone + Send + Sync + 'static,
        T::Device: Clone + Send + Sync + 'static,
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
        let strict_gpu = self.gpu.is_some()
            && matches!(
                self.target_compute,
                daedalus_planner::ComputeAffinity::GpuRequired
            );
        for p in self.inputs_for(port) {
            match &p.inner {
                RuntimeValue::Data(ep) => {
                    if wants_gpu {
                        if let Some(g) = ep.clone_gpu::<T>() {
                            return Some(daedalus_gpu::Compute::Gpu(g));
                        }
                        if let Some(ctx) = &self.gpu {
                            match ep.upload_if_needed(ctx) {
                                Ok((uploaded, _did_transfer)) => {
                                    if _did_transfer {
                                        self.record_gpu_transfer_bytes(
                                            true,
                                            crate::executor::runtime_value_size_bytes(
                                                &RuntimeValue::Data(uploaded.clone()),
                                            ),
                                        );
                                    }
                                    if let Some(g) = uploaded.clone_gpu::<T>() {
                                        return Some(daedalus_gpu::Compute::Gpu(g));
                                    }
                                }
                                Err(err) => {
                                    if std::env::var_os("DAEDALUS_TRACE_PAYLOAD_GET_STDERR")
                                        .is_some()
                                    {
                                        eprintln!(
                                            "daedalus-runtime: get_compute upload_if_needed failed node={} port={} type={} err={}",
                                            self.node_id,
                                            port,
                                            std::any::type_name::<T>(),
                                            err
                                        );
                                    }
                                }
                            }
                            // Fallback path for payloads that don't support erased upload.
                            if let Some(cpu) = self.data_cell_to_t::<T>(ep) {
                                let cpu_bytes = Self::estimate_any_bytes(&cpu);
                                match cpu.upload(ctx) {
                                    Ok(handle) => {
                                        self.record_gpu_transfer_bytes(true, cpu_bytes);
                                        return Some(daedalus_gpu::Compute::Gpu(handle));
                                    }
                                    Err(err) => {
                                        if std::env::var_os("DAEDALUS_TRACE_PAYLOAD_GET_STDERR")
                                            .is_some()
                                        {
                                            eprintln!(
                                                "daedalus-runtime: get_compute cpu.upload failed node={} port={} type={} err={}",
                                                self.node_id,
                                                port,
                                                std::any::type_name::<T>(),
                                                err
                                            );
                                        }
                                    }
                                }
                            }
                        }
                    }
                    if !wants_gpu || !strict_gpu {
                        if let Some(cpu) = self.data_cell_to_t::<T>(ep) {
                            return Some(daedalus_gpu::Compute::Cpu(cpu));
                        }
                        if let Some(ctx) = &self.gpu {
                            if let Ok((downloaded, did_transfer)) = ep.download_if_needed(ctx)
                                && let Some(cpu) = self.data_cell_to_t::<T>(&downloaded)
                            {
                                if did_transfer {
                                    self.record_gpu_transfer_bytes(
                                        false,
                                        Self::estimate_any_bytes(&cpu),
                                    );
                                }
                                return Some(daedalus_gpu::Compute::Cpu(cpu));
                            }
                            // Fallback path for payloads that don't support erased download.
                            if let Some(g) = ep.clone_gpu::<T>()
                                && let Ok(cpu) = T::download(&g, ctx)
                            {
                                self.record_gpu_transfer_bytes(
                                    false,
                                    Self::estimate_any_bytes(&cpu),
                                );
                                return Some(daedalus_gpu::Compute::Cpu(cpu));
                            }
                        }
                    }
                }
                RuntimeValue::Any(a) => {
                    // Plugins sometimes pass a `Compute<T>` through `Any` (e.g. when a node
                    // signature uses a type alias that the `#[node]` macro can't see through).
                    // Accept that representation here so downstream `Compute<T>` inputs can still
                    // be satisfied.
                    let payload_any: Option<daedalus_gpu::Compute<T>> =
                        a.downcast_ref::<daedalus_gpu::Compute<T>>().cloned();
                    if let Some(payload_any) = payload_any {
                        if wants_gpu {
                            match payload_any {
                                daedalus_gpu::Compute::Gpu(g) => {
                                    return Some(daedalus_gpu::Compute::Gpu(g));
                                }
                                daedalus_gpu::Compute::Cpu(cpu) => {
                                    if let Some(ctx) = &self.gpu {
                                        let cpu_bytes = Self::estimate_any_bytes(&cpu);
                                        if let Ok(handle) = cpu.upload(ctx) {
                                            self.record_gpu_transfer_bytes(true, cpu_bytes);
                                            return Some(daedalus_gpu::Compute::Gpu(handle));
                                        }
                                    }
                                }
                            }
                        } else {
                            match payload_any {
                                daedalus_gpu::Compute::Cpu(cpu) => {
                                    return Some(daedalus_gpu::Compute::Cpu(cpu));
                                }
                                daedalus_gpu::Compute::Gpu(g) => {
                                    if let Some(ctx) = &self.gpu
                                        && let Ok(cpu) = T::download(&g, ctx)
                                    {
                                        self.record_gpu_transfer_bytes(
                                            false,
                                            Self::estimate_any_bytes(&cpu),
                                        );
                                        return Some(daedalus_gpu::Compute::Cpu(cpu));
                                    }
                                }
                            }
                        }
                    }

                    if wants_gpu {
                        if let Some(cpu) = Self::any_backing_owned::<T>(a)
                            && let Some(ctx) = &self.gpu
                        {
                            let cpu_bytes = Self::estimate_any_bytes(&cpu);
                            if let Ok(handle) = cpu.upload(ctx) {
                                self.record_gpu_transfer_bytes(true, cpu_bytes);
                                return Some(daedalus_gpu::Compute::Gpu(handle));
                            }
                        }
                        if let Some(backing) =
                            a.downcast_ref::<daedalus_gpu::Backing<DynamicImage>>()
                            && let Some(cpu) =
                                Self::convert_backing_value_to_t::<DynamicImage, T>(backing.clone())
                            && let Some(ctx) = &self.gpu
                        {
                            let cpu_bytes = Self::estimate_any_bytes(&cpu);
                            if let Ok(handle) = cpu.upload(ctx) {
                                self.record_conversion_bytes(
                                    cpu_bytes.or_else(|| Self::estimate_any_bytes(a.as_ref())),
                                );
                                self.record_gpu_transfer_bytes(true, cpu_bytes);
                                return Some(daedalus_gpu::Compute::Gpu(handle));
                            }
                        }
                        if let Some(backing) = a.downcast_ref::<daedalus_gpu::Backing<GrayImage>>()
                            && let Some(cpu) =
                                Self::convert_backing_value_to_t::<GrayImage, T>(backing.clone())
                            && let Some(ctx) = &self.gpu
                        {
                            let cpu_bytes = Self::estimate_any_bytes(&cpu);
                            if let Ok(handle) = cpu.upload(ctx) {
                                self.record_conversion_bytes(
                                    cpu_bytes.or_else(|| Self::estimate_any_bytes(a.as_ref())),
                                );
                                self.record_gpu_transfer_bytes(true, cpu_bytes);
                                return Some(daedalus_gpu::Compute::Gpu(handle));
                            }
                        }
                        if let Some(backing) = a.downcast_ref::<daedalus_gpu::Backing<RgbImage>>()
                            && let Some(cpu) =
                                Self::convert_backing_value_to_t::<RgbImage, T>(backing.clone())
                            && let Some(ctx) = &self.gpu
                        {
                            let cpu_bytes = Self::estimate_any_bytes(&cpu);
                            if let Ok(handle) = cpu.upload(ctx) {
                                self.record_conversion_bytes(
                                    cpu_bytes.or_else(|| Self::estimate_any_bytes(a.as_ref())),
                                );
                                self.record_gpu_transfer_bytes(true, cpu_bytes);
                                return Some(daedalus_gpu::Compute::Gpu(handle));
                            }
                        }
                        if let Some(backing) = a.downcast_ref::<daedalus_gpu::Backing<RgbaImage>>()
                            && let Some(cpu) =
                                Self::convert_backing_value_to_t::<RgbaImage, T>(backing.clone())
                            && let Some(ctx) = &self.gpu
                        {
                            let cpu_bytes = Self::estimate_any_bytes(&cpu);
                            if let Ok(handle) = cpu.upload(ctx) {
                                self.record_conversion_bytes(
                                    cpu_bytes.or_else(|| Self::estimate_any_bytes(a.as_ref())),
                                );
                                self.record_gpu_transfer_bytes(true, cpu_bytes);
                                return Some(daedalus_gpu::Compute::Gpu(handle));
                            }
                        }
                        let cpu = a.downcast_ref::<T>().cloned();
                        if let Some(cpu) = cpu
                            && let Some(ctx) = &self.gpu
                        {
                            let cpu_bytes = Self::estimate_any_bytes(&cpu);
                            if let Ok(handle) = cpu.upload(ctx) {
                                self.record_gpu_transfer_bytes(true, cpu_bytes);
                                return Some(daedalus_gpu::Compute::Gpu(handle));
                            }
                        }
                        if let Some(converted) = crate::convert::convert_arc::<T>(a)
                            && let Some(ctx) = &self.gpu
                        {
                            let converted_bytes = Self::estimate_any_bytes(&converted);
                            if let Ok(handle) = converted.upload(ctx) {
                                self.record_conversion_bytes(
                                    converted_bytes
                                        .or_else(|| Self::estimate_any_bytes(a.as_ref())),
                                );
                                self.record_gpu_transfer_bytes(true, converted_bytes);
                                return Some(daedalus_gpu::Compute::Gpu(handle));
                            }
                        }
                        if let Some(ep) = a.downcast_ref::<daedalus_gpu::DataCell>()
                            && let Ok(uploaded) = ep.upload(self.gpu.as_ref()?)
                            && let Some(g) = uploaded.as_gpu::<T>()
                        {
                            self.record_gpu_transfer_bytes(
                                true,
                                crate::executor::runtime_value_size_bytes(&RuntimeValue::Data(
                                    uploaded.clone(),
                                )),
                            );
                            return Some(daedalus_gpu::Compute::Gpu(g.clone()));
                        }
                    }
                    if !wants_gpu || !strict_gpu {
                        if let Some(cpu) = Self::any_backing_owned::<T>(a) {
                            return Some(daedalus_gpu::Compute::Cpu(cpu));
                        }
                        if let Some(backing) =
                            a.downcast_ref::<daedalus_gpu::Backing<DynamicImage>>()
                            && let Some(cpu) =
                                Self::convert_backing_value_to_t::<DynamicImage, T>(backing.clone())
                        {
                            self.record_conversion_bytes(
                                Self::estimate_any_bytes(&cpu)
                                    .or_else(|| Self::estimate_any_bytes(a.as_ref())),
                            );
                            return Some(daedalus_gpu::Compute::Cpu(cpu));
                        }
                        if let Some(backing) = a.downcast_ref::<daedalus_gpu::Backing<GrayImage>>()
                            && let Some(cpu) =
                                Self::convert_backing_value_to_t::<GrayImage, T>(backing.clone())
                        {
                            self.record_conversion_bytes(
                                Self::estimate_any_bytes(&cpu)
                                    .or_else(|| Self::estimate_any_bytes(a.as_ref())),
                            );
                            return Some(daedalus_gpu::Compute::Cpu(cpu));
                        }
                        if let Some(backing) = a.downcast_ref::<daedalus_gpu::Backing<RgbImage>>()
                            && let Some(cpu) =
                                Self::convert_backing_value_to_t::<RgbImage, T>(backing.clone())
                        {
                            self.record_conversion_bytes(
                                Self::estimate_any_bytes(&cpu)
                                    .or_else(|| Self::estimate_any_bytes(a.as_ref())),
                            );
                            return Some(daedalus_gpu::Compute::Cpu(cpu));
                        }
                        if let Some(backing) = a.downcast_ref::<daedalus_gpu::Backing<RgbaImage>>()
                            && let Some(cpu) =
                                Self::convert_backing_value_to_t::<RgbaImage, T>(backing.clone())
                        {
                            self.record_conversion_bytes(
                                Self::estimate_any_bytes(&cpu)
                                    .or_else(|| Self::estimate_any_bytes(a.as_ref())),
                            );
                            return Some(daedalus_gpu::Compute::Cpu(cpu));
                        }
                        let cpu = a.downcast_ref::<T>().cloned();
                        if let Some(cpu) = cpu {
                            return Some(daedalus_gpu::Compute::Cpu(cpu));
                        }
                        if let Some(converted) = crate::convert::convert_arc::<T>(a) {
                            self.record_conversion_bytes(
                                Self::estimate_any_bytes(&converted)
                                    .or_else(|| Self::estimate_any_bytes(a.as_ref())),
                            );
                            return Some(daedalus_gpu::Compute::Cpu(converted));
                        }
                        let g = a.downcast_ref::<T::Device>().cloned();
                        if let Some(g) = g
                            && let Some(ctx) = &self.gpu
                            && let Ok(cpu) = T::download(&g, ctx)
                        {
                            self.record_gpu_transfer_bytes(false, Self::estimate_any_bytes(&cpu));
                            return Some(daedalus_gpu::Compute::Cpu(cpu));
                        }
                        if let Some(ep) = a.downcast_ref::<daedalus_gpu::DataCell>()
                            && let Ok(downloaded) = ep.download(self.gpu.as_ref()?)
                            && let Some(cpu) = downloaded.as_cpu::<T>()
                        {
                            self.record_gpu_transfer_bytes(false, Self::estimate_any_bytes(cpu));
                            return Some(daedalus_gpu::Compute::Cpu(cpu.clone()));
                        }
                    }
                }
                _ => {}
            }
        }
        if std::env::var_os("DAEDALUS_TRACE_PAYLOAD_GET_STDERR").is_some() {
            let payload = self
                .inputs_for(port)
                .next()
                .map(|cp| runtime_value_desc(&cp.inner))
                .unwrap_or_else(|| "None".to_string());
            eprintln!(
                "daedalus-runtime: get_compute miss node={} port={} type={} wants_gpu={} strict_gpu={} payload={}",
                self.node_id,
                port,
                std::any::type_name::<T>(),
                wants_gpu,
                strict_gpu,
                payload
            );
        }
        None
    }

    #[cfg(feature = "gpu")]
    pub fn get_compute_mut<T>(&mut self, port: &str) -> Option<daedalus_gpu::Compute<T>>
    where
        T: daedalus_gpu::DeviceBridge + Clone + Send + Sync + 'static,
        T::Device: Clone + Send + Sync + 'static,
    {
        let wants_gpu = self.gpu.is_some()
            && matches!(
                self.target_compute,
                daedalus_planner::ComputeAffinity::GpuPreferred
                    | daedalus_planner::ComputeAffinity::GpuRequired
            );
        let (idx, mut payload) = self.take_input(port)?;

        let mut out: Option<daedalus_gpu::Compute<T>> = None;
        match std::mem::replace(&mut payload.inner, RuntimeValue::Unit) {
            RuntimeValue::Data(ep) => {
                let mut ep_opt = Some(ep);
                if wants_gpu {
                    if let Some(ep) = ep_opt.as_ref() {
                        if let Some(g) = ep.clone_gpu::<T>() {
                            out = Some(daedalus_gpu::Compute::Gpu(g));
                        } else if let Some(ctx) = &self.gpu {
                            if let Ok((uploaded, did_transfer)) = ep.upload_if_needed(ctx)
                                && let Some(g) = uploaded.clone_gpu::<T>()
                            {
                                if did_transfer {
                                    self.record_gpu_transfer_bytes(
                                        true,
                                        crate::executor::runtime_value_size_bytes(
                                            &RuntimeValue::Data(uploaded.clone()),
                                        ),
                                    );
                                }
                                out = Some(daedalus_gpu::Compute::Gpu(g));
                            } else if let Some(backing) =
                                ep.try_downcast_cpu_any::<daedalus_gpu::Backing<T>>()
                            {
                                let cpu = backing.into_owned();
                                let cpu_bytes = Self::estimate_any_bytes(&cpu);
                                if let Ok(handle) = cpu.upload(ctx) {
                                    self.record_gpu_transfer_bytes(true, cpu_bytes);
                                    out = Some(daedalus_gpu::Compute::Gpu(handle));
                                }
                            } else if let Some(cpu) = ep.clone_cpu::<T>() {
                                let cpu_bytes = Self::estimate_any_bytes(&cpu);
                                if let Ok(handle) = cpu.upload(ctx) {
                                    self.record_gpu_transfer_bytes(true, cpu_bytes);
                                    out = Some(daedalus_gpu::Compute::Gpu(handle));
                                }
                            }
                        }
                    }
                } else if let Some(ep) = ep_opt.take() {
                    match ep.take_cpu::<T>() {
                        Ok(cpu) => out = Some(daedalus_gpu::Compute::Cpu(cpu)),
                        Err(rest) => {
                            ep_opt = Some(rest);
                            if let Some(ep) = ep_opt.as_ref() {
                                if let Some(backing) =
                                    ep.try_downcast_cpu_any::<daedalus_gpu::Backing<T>>()
                                {
                                    out = Some(daedalus_gpu::Compute::Cpu(backing.into_owned()));
                                } else if let Some(cpu) = ep.clone_cpu::<T>() {
                                    out = Some(daedalus_gpu::Compute::Cpu(cpu));
                                } else if let Some(ctx) = &self.gpu {
                                    if let Ok((downloaded, did_transfer)) =
                                        ep.download_if_needed(ctx)
                                        && let Some(cpu) = downloaded.clone_cpu::<T>()
                                    {
                                        if did_transfer {
                                            self.record_gpu_transfer_bytes(
                                                false,
                                                Self::estimate_any_bytes(&cpu),
                                            );
                                        }
                                        out = Some(daedalus_gpu::Compute::Cpu(cpu));
                                    } else if let Some(g) = ep.clone_gpu::<T>()
                                        && let Ok(cpu) = T::download(&g, ctx)
                                    {
                                        self.record_gpu_transfer_bytes(
                                            false,
                                            Self::estimate_any_bytes(&cpu),
                                        );
                                        out = Some(daedalus_gpu::Compute::Cpu(cpu));
                                    }
                                }
                            }
                        }
                    }
                }
                if out.is_none()
                    && let Some(ep) = ep_opt
                {
                    payload.inner = RuntimeValue::Data(ep);
                }
            }
            RuntimeValue::Any(a) => {
                let any = a;
                match Arc::downcast::<daedalus_gpu::Compute<T>>(any) {
                    Ok(arc) => {
                        let payload_any = match Arc::try_unwrap(arc) {
                            Ok(v) => v,
                            Err(arc) => (*arc).clone(),
                        };
                        if wants_gpu {
                            match payload_any {
                                daedalus_gpu::Compute::Gpu(g) => {
                                    out = Some(daedalus_gpu::Compute::Gpu(g))
                                }
                                daedalus_gpu::Compute::Cpu(cpu) => {
                                    if let Some(ctx) = &self.gpu {
                                        let cpu_bytes = Self::estimate_any_bytes(&cpu);
                                        if let Ok(handle) = cpu.upload(ctx) {
                                            self.record_gpu_transfer_bytes(true, cpu_bytes);
                                            out = Some(daedalus_gpu::Compute::Gpu(handle));
                                        }
                                    }
                                }
                            }
                        } else {
                            match payload_any {
                                daedalus_gpu::Compute::Cpu(cpu) => {
                                    out = Some(daedalus_gpu::Compute::Cpu(cpu))
                                }
                                daedalus_gpu::Compute::Gpu(g) => {
                                    if let Some(ctx) = &self.gpu
                                        && let Ok(cpu) = T::download(&g, ctx)
                                    {
                                        self.record_gpu_transfer_bytes(
                                            false,
                                            Self::estimate_any_bytes(&cpu),
                                        );
                                        out = Some(daedalus_gpu::Compute::Cpu(cpu));
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
                                    if let Some(ctx) = &self.gpu {
                                        let cpu_bytes = Self::estimate_any_bytes(&cpu);
                                        if let Ok(handle) = cpu.upload(ctx) {
                                            self.record_gpu_transfer_bytes(true, cpu_bytes);
                                            out = Some(daedalus_gpu::Compute::Gpu(handle));
                                        }
                                    }
                                }
                                Err(any) => match Arc::downcast::<daedalus_gpu::Backing<T>>(any) {
                                    Ok(arc) => {
                                        let cpu = match Arc::try_unwrap(arc) {
                                            Ok(v) => v.into_owned(),
                                            Err(arc) => (*arc).clone().into_owned(),
                                        };
                                        if let Some(ctx) = &self.gpu {
                                            let cpu_bytes = Self::estimate_any_bytes(&cpu);
                                            if let Ok(handle) = cpu.upload(ctx) {
                                                self.record_gpu_transfer_bytes(true, cpu_bytes);
                                                out = Some(daedalus_gpu::Compute::Gpu(handle));
                                            }
                                        }
                                    }
                                    Err(any) => match Arc::downcast::<T::Device>(any) {
                                        Ok(arc) => {
                                            let gpu = match Arc::try_unwrap(arc) {
                                                Ok(v) => v,
                                                Err(arc) => (*arc).clone(),
                                            };
                                            out = Some(daedalus_gpu::Compute::Gpu(gpu));
                                        }
                                        Err(any) => {
                                            payload.inner = RuntimeValue::Any(any);
                                        }
                                    },
                                },
                            }
                        } else {
                            match Arc::downcast::<T>(any) {
                                Ok(arc) => {
                                    let cpu = match Arc::try_unwrap(arc) {
                                        Ok(v) => v,
                                        Err(arc) => (*arc).clone(),
                                    };
                                    out = Some(daedalus_gpu::Compute::Cpu(cpu));
                                }
                                Err(any) => match Arc::downcast::<daedalus_gpu::Backing<T>>(any) {
                                    Ok(arc) => {
                                        let cpu = match Arc::try_unwrap(arc) {
                                            Ok(v) => v.into_owned(),
                                            Err(arc) => (*arc).clone().into_owned(),
                                        };
                                        out = Some(daedalus_gpu::Compute::Cpu(cpu));
                                    }
                                    Err(any) => match Arc::downcast::<T::Device>(any) {
                                        Ok(arc) => {
                                            let gpu = match Arc::try_unwrap(arc) {
                                                Ok(v) => v,
                                                Err(arc) => (*arc).clone(),
                                            };
                                            if let Some(ctx) = &self.gpu
                                                && let Ok(cpu) = T::download(&gpu, ctx)
                                            {
                                                self.record_gpu_transfer_bytes(
                                                    false,
                                                    Self::estimate_any_bytes(&cpu),
                                                );
                                                out = Some(daedalus_gpu::Compute::Cpu(cpu));
                                            } else {
                                                payload.inner = RuntimeValue::Any(Arc::new(gpu));
                                            }
                                        }
                                        Err(any) => {
                                            payload.inner = RuntimeValue::Any(any);
                                        }
                                    },
                                },
                            }
                        }
                    }
                }
            }
            other => {
                payload.inner = other;
            }
        }

        if out.is_none() {
            if std::env::var_os("DAEDALUS_TRACE_PAYLOAD_GET_STDERR").is_some() {
                let desc = runtime_value_desc(&payload.inner);
                eprintln!(
                    "daedalus-runtime: get_compute_mut miss node={} port={} type={} wants_gpu={} payload={}",
                    self.node_id,
                    port,
                    std::any::type_name::<T>(),
                    wants_gpu,
                    desc
                );
            }
            self.restore_input(idx, port, payload);
        }
        out
    }

    #[cfg(feature = "gpu")]
    fn convert_incoming(
        mut payload: CorrelatedValue,
        ctx: ConvertIncomingContext<'_>,
    ) -> CorrelatedValue {
        let ConvertIncomingContext {
            node_idx,
            edge_idx,
            entries,
            exits,
            materialization_cache,
            gpu,
            telemetry,
        } = ctx;
        let Some(ctx) = gpu else {
            return payload;
        };
        if entries.contains(&edge_idx) {
            payload.inner = match payload.inner {
                RuntimeValue::Any(ref a) => {
                    let (promoted, materialized) = promote_any_with_cache(a, materialization_cache);
                    if materialized {
                        telemetry.record_node_materialization(
                            node_idx,
                            crate::executor::runtime_value_size_bytes(&promoted).unwrap_or(0),
                        );
                    }
                    match promoted {
                        RuntimeValue::Data(ep) => match ep.upload_if_needed(ctx) {
                            Ok((uploaded, did_transfer)) => {
                                if did_transfer {
                                    telemetry.record_edge_gpu_transfer(edge_idx, true);
                                    telemetry.record_node_gpu_transfer(
                                        node_idx,
                                        true,
                                        crate::executor::runtime_value_size_bytes(
                                            &RuntimeValue::Data(uploaded.clone()),
                                        )
                                        .unwrap_or(0),
                                    );
                                }
                                RuntimeValue::Data(uploaded)
                            }
                            Err(_) => RuntimeValue::Data(ep),
                        },
                        RuntimeValue::Any(a) => RuntimeValue::Any(a),
                        other => other,
                    }
                }
                RuntimeValue::Data(ref ep) => match ep.upload_if_needed(ctx) {
                    Ok((uploaded, did_transfer)) => {
                        if did_transfer {
                            telemetry.record_edge_gpu_transfer(edge_idx, true);
                            telemetry.record_node_gpu_transfer(
                                node_idx,
                                true,
                                crate::executor::runtime_value_size_bytes(&RuntimeValue::Data(
                                    uploaded.clone(),
                                ))
                                .unwrap_or(0),
                            );
                        }
                        RuntimeValue::Data(uploaded)
                    }
                    Err(_) => RuntimeValue::Data(ep.clone()),
                },
                other => other,
            };
        } else if exits.contains(&edge_idx) {
            payload.inner = match payload.inner {
                RuntimeValue::Any(ref a) => {
                    if let Some(ep) = a.downcast_ref::<daedalus_gpu::DataCell>() {
                        match ep.download_if_needed(ctx) {
                            Ok((downloaded, did_transfer)) => {
                                if did_transfer {
                                    telemetry.record_edge_gpu_transfer(edge_idx, false);
                                    telemetry.record_node_gpu_transfer(
                                        node_idx,
                                        false,
                                        crate::executor::runtime_value_size_bytes(
                                            &RuntimeValue::Data(downloaded.clone()),
                                        )
                                        .unwrap_or(0),
                                    );
                                }
                                RuntimeValue::Data(downloaded)
                            }
                            Err(_) => RuntimeValue::Any(a.clone()),
                        }
                    } else {
                        RuntimeValue::Any(a.clone())
                    }
                }
                RuntimeValue::Data(ref ep) => match ep.download_if_needed(ctx) {
                    Ok((downloaded, did_transfer)) => {
                        if did_transfer {
                            telemetry.record_edge_gpu_transfer(edge_idx, false);
                            telemetry.record_node_gpu_transfer(
                                node_idx,
                                false,
                                crate::executor::runtime_value_size_bytes(&RuntimeValue::Data(
                                    downloaded.clone(),
                                ))
                                .unwrap_or(0),
                            );
                        }
                        RuntimeValue::Data(downloaded)
                    }
                    Err(_) => RuntimeValue::Data(ep.clone()),
                },
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
            RuntimeValue::Value(v) => Some(v),
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
    pub fn inputs_grouped(&self) -> Vec<(String, Vec<&CorrelatedValue>)> {
        let mut groups: Vec<(String, Vec<&CorrelatedValue>)> = Vec::new();
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

fn runtime_value_desc(payload: &RuntimeValue) -> String {
    match payload {
        RuntimeValue::Any(a) => {
            #[cfg(feature = "gpu")]
            if a.is::<daedalus_gpu::GpuImageHandle>() {
                return "Any(GpuImageHandle)".to_string();
            }
            format!("Any({})", std::any::type_name_of_val(a.as_ref()))
        }
        #[cfg(feature = "gpu")]
        RuntimeValue::Data(ep) => format!("Data({ep:?})"),
        RuntimeValue::Value(v) => format!("Value({v:?})"),
        RuntimeValue::Bytes(_) => "Bytes".to_string(),
        RuntimeValue::Unit => "Unit".to_string(),
    }
}

fn align_drained_inputs(
    drained: Vec<DrainedInput>,
    sync_groups: &[SyncGroup],
    const_ports: &std::collections::HashSet<String>,
) -> (Vec<(String, CorrelatedValue)>, Vec<DrainedInput>, bool) {
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
            if !const_ports.contains(port) {
                grouped_ports.insert(port.clone());
            }
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
                    if const_ports.contains(port) {
                        continue;
                    }
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
                    if const_ports.contains(port) {
                        continue;
                    }
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
                    if const_ports.contains(port) {
                        continue;
                    }
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
                if common.is_none() {
                    // Group satisfied entirely by constant ports.
                    continue;
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
                    if const_ports.contains(port) {
                        continue;
                    }
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
    let mut inputs: Vec<(String, CorrelatedValue)> =
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

    let mut per_edge: HashMap<usize, Vec<CorrelatedValue>> = HashMap::new();
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
            EdgeStorage::Locked { queue, metrics } => {
                if let Ok(mut q) = queue.lock() {
                    q.ensure_policy(&policy);
                    for mut payload in payloads {
                        payload.enqueued_at = Instant::now();
                        let _ = q.push(&policy, payload);
                    }
                    metrics.set_current_bytes(q.transport_bytes());
                }
            }
            #[cfg(feature = "lockfree-queues")]
            EdgeStorage::BoundedLf { queue, metrics } => {
                for mut payload in payloads {
                    let transport_bytes =
                        crate::executor::runtime_value_size_bytes(&payload.inner).unwrap_or(0);
                    payload.enqueued_at = Instant::now();
                    if queue.push(payload.clone()).is_err() {
                        let removed_bytes = queue
                            .pop()
                            .and_then(|removed| {
                                crate::executor::runtime_value_size_bytes(&removed.inner)
                            })
                            .unwrap_or(0);
                        let _ = queue.push(payload);
                        metrics.adjust_bytes(transport_bytes, removed_bytes);
                    } else {
                        metrics.adjust_bytes(transport_bytes, 0);
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
    use std::collections::{BTreeSet, HashSet};

    fn payload(v: i32, corr: u64) -> CorrelatedValue {
        CorrelatedValue {
            correlation_id: corr,
            inner: RuntimeValue::Any(Arc::new(v)),
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
        let empty_consts = std::collections::HashSet::new();
        let (out, leftovers, ready) = align_drained_inputs(drained, &[group], &empty_consts);
        assert!(ready);
        let vals: Vec<(String, i32)> = out
            .into_iter()
            .map(|(p, pl)| {
                (
                    p,
                    match pl.inner {
                        RuntimeValue::Any(ref a) => *a.downcast_ref::<i32>().unwrap(),
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
                    RuntimeValue::Any(ref a) => *a.downcast_ref::<i32>().unwrap(),
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
        let empty_consts = std::collections::HashSet::new();
        let (out, leftovers, ready) = align_drained_inputs(drained, &[group], &empty_consts);
        assert!(ready);
        assert!(leftovers.is_empty());
        let vals: Vec<(String, i32)> = out
            .into_iter()
            .map(|(p, pl)| {
                (
                    p,
                    match pl.inner {
                        RuntimeValue::Any(ref a) => *a.downcast_ref::<i32>().unwrap(),
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
            CorrelatedValue {
                correlation_id: corr,
                inner: RuntimeValue::Value(Value::Struct(fields)),
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
        let empty_consts = std::collections::HashSet::new();
        let (out, leftovers, ready) = align_drained_inputs(drained, &[group], &empty_consts);
        assert!(ready);
        assert!(!out.is_empty());
        assert!(!leftovers.is_empty());
        let tags: Vec<String> = out
            .chunks(2)
            .map(|chunk| {
                chunk
                    .iter()
                    .map(|(_, p)| match &p.inner {
                        RuntimeValue::Value(Value::Struct(fields)) => fields
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
        let empty_consts = std::collections::HashSet::new();
        let (out, leftovers, ready) = align_drained_inputs(drained, &[group], &empty_consts);
        assert!(!ready);
        assert!(out.is_empty());
        assert_eq!(leftovers.len(), 1);
    }

    #[test]
    fn port_override_applies_backpressure_and_capacity() {
        let queues = Arc::new(vec![EdgeStorage::Locked {
            queue: Arc::new(std::sync::Mutex::new(EdgeQueue::Bounded {
                ring: RingBuf::new(5),
            })),
            metrics: Arc::new(crate::executor::queue::EdgeStorageMetrics::default()),
        }]);
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
        let data_edges: HashSet<usize> = HashSet::new();

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
            &data_edges,
            0,
            0,
            "node".into(),
            None,
            &mut telem,
            BackpressureStrategy::None,
            &[],
            None,
            None,
            new_any_conversion_cache(),
            #[cfg(feature = "gpu")]
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

    #[derive(Clone, Debug, PartialEq, Eq)]
    struct RuntimeSource(u8);

    #[derive(Clone, Debug, PartialEq, Eq)]
    struct RuntimeMid(u16);

    #[derive(Clone, Debug, PartialEq, Eq)]
    struct RuntimeTarget(u32);

    fn register_runtime_resolution_types() {
        daedalus_data::typing::register_type::<RuntimeSource>(TypeExpr::Opaque(
            "test:runtime:source".to_string(),
        ));
        daedalus_data::typing::register_type::<RuntimeMid>(TypeExpr::Opaque(
            "test:runtime:mid".to_string(),
        ));
        daedalus_data::typing::register_type::<RuntimeTarget>(TypeExpr::Opaque(
            "test:runtime:target".to_string(),
        ));
        daedalus_data::typing::register_compatibility_with_rule(
            TypeExpr::Opaque("test:runtime:source".to_string()),
            TypeExpr::Opaque("test:runtime:mid".to_string()),
            typing::CompatibilityRule {
                kind: typing::CompatibilityKind::View,
                cost: 1,
                capabilities: ["view-compatible".to_string()].into_iter().collect(),
            },
        );
        daedalus_data::typing::register_compatibility_with_rule(
            TypeExpr::Opaque("test:runtime:mid".to_string()),
            TypeExpr::Opaque("test:runtime:target".to_string()),
            typing::CompatibilityRule {
                kind: typing::CompatibilityKind::Convert,
                cost: 2,
                capabilities: BTreeSet::new(),
            },
        );
        crate::convert::register_conversion::<RuntimeSource, RuntimeMid>(|v| {
            Some(RuntimeMid(v.0 as u16))
        });
        crate::convert::register_conversion::<RuntimeMid, RuntimeTarget>(|v| {
            Some(RuntimeTarget(v.0 as u32))
        });
    }

    #[cfg(feature = "gpu")]
    #[derive(Clone, Debug, PartialEq)]
    struct DummyCompute {
        value: i32,
    }

    #[cfg(feature = "gpu")]
    static DUMMY_PAYLOAD_CLONES: std::sync::atomic::AtomicUsize =
        std::sync::atomic::AtomicUsize::new(0);

    #[cfg(feature = "gpu")]
    #[derive(Debug, PartialEq)]
    struct CountedCompute {
        value: i32,
    }

    #[cfg(feature = "gpu")]
    impl Clone for CountedCompute {
        fn clone(&self) -> Self {
            DUMMY_PAYLOAD_CLONES.fetch_add(1, std::sync::atomic::Ordering::SeqCst);
            Self { value: self.value }
        }
    }

    #[cfg(feature = "gpu")]
    impl daedalus_gpu::DeviceBridge for DummyCompute {
        type Device = ();
    }

    #[cfg(feature = "gpu")]
    impl daedalus_gpu::DeviceBridge for CountedCompute {
        type Device = ();
    }

    #[cfg(feature = "gpu")]
    fn make_io_with_value(payload: CorrelatedValue) -> NodeIo<'static> {
        let queues: &'static Arc<Vec<EdgeStorage>> = Box::leak(Box::new(Arc::new(vec![])));
        let edges: &'static [EdgeInfo] = Box::leak(Box::new(Vec::new()));
        let warnings: &'static Arc<std::sync::Mutex<std::collections::HashSet<String>>> =
            Box::leak(Box::new(Arc::new(std::sync::Mutex::new(HashSet::new()))));
        let telem: &'static mut ExecutionTelemetry =
            Box::leak(Box::new(ExecutionTelemetry::default()));
        let gpu_entry_edges: &'static HashSet<usize> = Box::leak(Box::new(HashSet::new()));
        let gpu_exit_edges: &'static HashSet<usize> = Box::leak(Box::new(HashSet::new()));
        let data_edges: &'static HashSet<usize> = Box::leak(Box::new(HashSet::new()));

        let mut io = NodeIo::new(
            vec![],
            vec![],
            queues,
            warnings,
            edges,
            vec![],
            gpu_entry_edges,
            gpu_exit_edges,
            data_edges,
            0,
            0,
            "node".into(),
            None,
            telem,
            BackpressureStrategy::None,
            &[],
            None,
            None,
            new_any_conversion_cache(),
            Some(new_materialization_cache()),
            None,
            daedalus_planner::ComputeAffinity::CpuOnly,
        );
        io.inputs = vec![("in".to_string(), payload)];
        io
    }

    fn make_io_with_any_value(payload: CorrelatedValue) -> NodeIo<'static> {
        let queues: &'static Arc<Vec<EdgeStorage>> = Box::leak(Box::new(Arc::new(vec![])));
        let edges: &'static [EdgeInfo] = Box::leak(Box::new(Vec::new()));
        let warnings: &'static Arc<std::sync::Mutex<std::collections::HashSet<String>>> =
            Box::leak(Box::new(Arc::new(std::sync::Mutex::new(HashSet::new()))));
        let telem: &'static mut ExecutionTelemetry =
            Box::leak(Box::new(ExecutionTelemetry::default()));
        #[cfg(feature = "gpu")]
        let gpu_entry_edges: &'static HashSet<usize> = Box::leak(Box::new(HashSet::new()));
        #[cfg(feature = "gpu")]
        let gpu_exit_edges: &'static HashSet<usize> = Box::leak(Box::new(HashSet::new()));
        #[cfg(feature = "gpu")]
        let data_edges: &'static HashSet<usize> = Box::leak(Box::new(HashSet::new()));

        let mut io = NodeIo::new(
            vec![],
            vec![],
            queues,
            warnings,
            edges,
            vec![],
            #[cfg(feature = "gpu")]
            gpu_entry_edges,
            #[cfg(feature = "gpu")]
            gpu_exit_edges,
            #[cfg(feature = "gpu")]
            data_edges,
            0,
            0,
            "node".into(),
            None,
            telem,
            BackpressureStrategy::None,
            &[],
            None,
            None,
            new_any_conversion_cache(),
            #[cfg(feature = "gpu")]
            Some(new_materialization_cache()),
            #[cfg(feature = "gpu")]
            None,
            #[cfg(feature = "gpu")]
            daedalus_planner::ComputeAffinity::CpuOnly,
        );
        io.inputs = vec![("in".to_string(), payload)];
        io
    }

    #[cfg(feature = "gpu")]
    #[test]
    fn get_any_reads_payload_any_type() {
        let payload = CorrelatedValue {
            correlation_id: 1,
            inner: RuntimeValue::Data(daedalus_gpu::DataCell::from_cpu::<DummyCompute>(
                DummyCompute { value: 42 },
            )),
            enqueued_at: std::time::Instant::now(),
        };
        let io = make_io_with_value(payload);
        let got = io.get_any::<DummyCompute>("in");
        assert_eq!(got, Some(DummyCompute { value: 42 }));
    }

    #[cfg(feature = "gpu")]
    #[test]
    fn get_typed_ref_borrows_payload_any_type_without_clone() {
        DUMMY_PAYLOAD_CLONES.store(0, std::sync::atomic::Ordering::SeqCst);
        let payload = CorrelatedValue {
            correlation_id: 1,
            inner: RuntimeValue::Data(daedalus_gpu::DataCell::from_cpu::<CountedCompute>(
                CountedCompute { value: 42 },
            )),
            enqueued_at: std::time::Instant::now(),
        };
        let io = make_io_with_value(payload);
        let got = io.get_typed_ref::<CountedCompute>("in");
        assert_eq!(got, Some(&CountedCompute { value: 42 }));
        assert_eq!(
            DUMMY_PAYLOAD_CLONES.load(std::sync::atomic::Ordering::SeqCst),
            0
        );
    }

    #[test]
    fn get_typed_reads_any_arc_carrier() {
        let payload = CorrelatedValue {
            correlation_id: 1,
            inner: RuntimeValue::Any(Arc::new(Arc::new(String::from("frame")))),
            enqueued_at: std::time::Instant::now(),
        };
        let io = make_io_with_any_value(payload);
        assert_eq!(io.get_typed::<String>("in"), Some(String::from("frame")));
        assert_eq!(
            io.get_typed_ref::<String>("in").map(String::as_str),
            Some("frame")
        );
        assert_eq!(
            io.get_any_arc::<String>("in")
                .as_deref()
                .map(String::as_str),
            Some("frame")
        );
    }

    #[test]
    fn get_typed_with_resolution_reports_runtime_conversion_path() {
        register_runtime_resolution_types();

        let payload = CorrelatedValue {
            correlation_id: 1,
            inner: RuntimeValue::Any(Arc::new(RuntimeSource(7))),
            enqueued_at: std::time::Instant::now(),
        };
        let io = make_io_with_any_value(payload);
        let (got, resolution) = io
            .get_typed_with_resolution::<RuntimeTarget>("in")
            .expect("typed runtime conversion");

        assert_eq!(got, RuntimeTarget(7));
        assert_eq!(resolution.kind, TypedInputResolutionKind::RuntimeConversion);
        assert_eq!(
            resolution
                .runtime_conversion
                .as_ref()
                .map(|path| path.steps.len()),
            Some(2)
        );
        assert_eq!(
            resolution
                .compatibility_path
                .as_ref()
                .map(|path| path.total_cost),
            Some(3)
        );
    }

    #[test]
    fn explain_typed_input_reports_const_coercion() {
        let payload = CorrelatedValue {
            correlation_id: 1,
            inner: RuntimeValue::Any(Arc::new(5_i64)),
            enqueued_at: std::time::Instant::now(),
        };
        let io = make_io_with_any_value(payload);
        let resolution = io
            .explain_typed_input::<u32>("in")
            .expect("typed const coercion");

        assert_eq!(resolution.kind, TypedInputResolutionKind::ConstCoercion);
        assert_eq!(
            resolution.source_rust.as_deref(),
            Some(std::any::type_name::<i64>())
        );
        assert_eq!(io.get_typed::<u32>("in"), Some(5_u32));
    }

    #[test]
    fn get_typed_reads_any_arc_dynamic_image_carrier() {
        let image = image::DynamicImage::new_rgba8(4, 4);
        let payload = CorrelatedValue {
            correlation_id: 1,
            inner: RuntimeValue::Any(Arc::new(Arc::new(image.clone()))),
            enqueued_at: std::time::Instant::now(),
        };
        let io = make_io_with_any_value(payload);
        let got = io.get_typed::<image::DynamicImage>("in");
        assert_eq!(
            got.as_ref().map(|img| (img.width(), img.height())),
            Some((4, 4))
        );
        assert_eq!(
            io.get_typed_ref::<image::DynamicImage>("in")
                .map(|img| (img.width(), img.height())),
            Some((4, 4))
        );
        assert_eq!(
            io.get_any_arc::<image::DynamicImage>("in")
                .as_deref()
                .map(|img| (img.width(), img.height())),
            Some((4, 4))
        );
    }

    #[cfg(feature = "plugins")]
    #[test]
    fn get_typed_converts_dynamic_image_to_gray_image_with_standard_image_support() {
        let mut registry = crate::plugins::PluginRegistry::new();
        registry
            .register_standard_image_support()
            .expect("register standard image support");

        let image = image::DynamicImage::ImageRgba8(image::RgbaImage::from_pixel(
            3,
            2,
            image::Rgba([255, 255, 255, 255]),
        ));
        let payload = CorrelatedValue {
            correlation_id: 1,
            inner: RuntimeValue::Any(Arc::new(image)),
            enqueued_at: std::time::Instant::now(),
        };
        let io = make_io_with_any_value(payload);
        let (gray, resolution) = io
            .get_typed_with_resolution::<image::GrayImage>("in")
            .expect("dynamic image should convert to gray");

        assert_eq!(gray.dimensions(), (3, 2));
        assert_eq!(gray.get_pixel(0, 0)[0], 255);
        assert_eq!(resolution.kind, TypedInputResolutionKind::RuntimeConversion);
    }

    #[cfg(feature = "gpu")]
    #[test]
    fn get_typed_mut_moves_payload_any_type() {
        let payload = CorrelatedValue {
            correlation_id: 1,
            inner: RuntimeValue::Data(daedalus_gpu::DataCell::from_cpu::<DummyCompute>(
                DummyCompute { value: 7 },
            )),
            enqueued_at: std::time::Instant::now(),
        };
        let mut io = make_io_with_value(payload);
        let got = io.get_typed_mut::<DummyCompute>("in");
        assert_eq!(got, Some(DummyCompute { value: 7 }));
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
        #[cfg(feature = "gpu")]
        let data_edges: HashSet<usize> = HashSet::new();

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
            &data_edges,
            0,
            0,
            "node".into(),
            None,
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
            new_any_conversion_cache(),
            #[cfg(feature = "gpu")]
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
        let data_edges: HashSet<usize> = HashSet::new();

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
            &data_edges,
            0,
            0,
            "node".into(),
            None,
            &mut telem,
            BackpressureStrategy::None,
            &[("mode".to_string(), Value::String("cpu".into()))],
            None,
            None,
            new_any_conversion_cache(),
            #[cfg(feature = "gpu")]
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
        let data_edges: HashSet<usize> = HashSet::new();

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
            &data_edges,
            0,
            0,
            "node".into(),
            None,
            &mut telem,
            BackpressureStrategy::None,
            &[("mode".to_string(), Value::Int(2))],
            None,
            None,
            new_any_conversion_cache(),
            #[cfg(feature = "gpu")]
            None,
            #[cfg(feature = "gpu")]
            None,
            #[cfg(feature = "gpu")]
            daedalus_planner::ComputeAffinity::CpuOnly,
        );

        assert_eq!(io.get_typed::<TestEnum>("mode"), Some(TestEnum::Gpu));
    }
}
