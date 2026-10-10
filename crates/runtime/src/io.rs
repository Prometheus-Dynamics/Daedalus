use crate::portable::Arc;
use crate::prelude::*;
use crate::sync::RwLock;
use core::any::Any;
use core::ops::{Deref, DerefMut};

use daedalus_core::platform::Clock;
use daedalus_data::model::{TypeExpr, Value};
use daedalus_data::typing;
use daedalus_transport::Payload;

use crate::executor::{CorrelatedPayload, NodeError};
use crate::handles::PortId;
use crate::type_index::TypeIndex;

pub const DEFAULT_OUTPUT_PORT: &str = "out";

#[derive(Clone, Debug, PartialEq, Eq, serde::Serialize, serde::Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum TypedInputResolutionKind {
    Exact,
    ConstCoercion,
    ValueCoercion,
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
}

pub type ConstCoercer = Box<dyn Fn(&Value) -> Option<Box<dyn Any + Send + Sync>> + Send + Sync>;
pub type ConstCoercerMap = Arc<RwLock<HashMap<&'static str, ConstCoercer>>>;

/// One port-tagged payload on a node's inputs or outputs.
pub type NodePort = (PortId, CorrelatedPayload);

#[cfg(feature = "std")]
std::thread_local! {
    /// Cleared port buffers reused by `NodeIo`s and the executor on this thread.
    static PORT_BUFFERS: core::cell::RefCell<Vec<Vec<NodePort>>> =
        const { core::cell::RefCell::new(Vec::new()) };
}

/// Cleared port buffers reused by `NodeIo`s and the executor (one pool without threads).
#[cfg(not(feature = "std"))]
static PORT_BUFFERS: crate::sync::Mutex<Vec<Vec<NodePort>>> = crate::sync::Mutex::new(Vec::new());

/// Runs `f` on the port buffer pool; `None` if it is unavailable (thread teardown, or held by
/// an interrupted caller without `std`).
fn with_port_buffers<R>(f: impl FnOnce(&mut Vec<Vec<NodePort>>) -> R) -> Option<R> {
    #[cfg(feature = "std")]
    return PORT_BUFFERS.try_with(|pool| f(&mut pool.borrow_mut())).ok();
    #[cfg(not(feature = "std"))]
    PORT_BUFFERS.try_lock().map(|mut pool| f(&mut pool))
}

/// An empty port buffer, reusing a recycled one's capacity when the pool has one.
pub(crate) fn port_buffer() -> Vec<NodePort> {
    with_port_buffers(Vec::pop).flatten().unwrap_or_default()
}

/// Port buffers one thread keeps for reuse.
const POOLED_PORT_BUFFERS: usize = 16;

/// Clear `ports` and keep its capacity for [`port_buffer`] (a few modest buffers per thread).
pub(crate) fn recycle_ports(mut ports: Vec<NodePort>) {
    const MAX_CAPACITY: usize = 256;
    ports.clear();
    if ports.capacity() == 0 || ports.capacity() > MAX_CAPACITY {
        return;
    }
    with_port_buffers(|pool| {
        if pool.len() < POOLED_PORT_BUFFERS {
            pool.push(ports);
        }
    });
}

/// Stock this thread's port-buffer pool with empty buffers, so the first nodes it runs take
/// their buffers from the pool instead of allocating (worker threads call this at pool start).
pub(crate) fn prewarm_port_buffers() {
    const PREWARM_CAPACITY: usize = 8;
    with_port_buffers(|pool| {
        pool.reserve(POOLED_PORT_BUFFERS.saturating_sub(pool.len()));
        while pool.len() < POOLED_PORT_BUFFERS {
            pool.push(Vec::with_capacity(PREWARM_CAPACITY));
        }
    });
}

pub fn new_const_coercer_map() -> ConstCoercerMap {
    Arc::new(RwLock::new(HashMap::new()))
}

/// A node's const inputs as ready-made payloads, shared by its calls (see [`NodeIo`]).
pub type NodeConstInputs = Vec<NodePort>;

/// What every call of one node shares: the executor's const coercers, type index and clock, and
/// the node's connected output ports. The executor builds one per node, so a call clones one
/// `Arc` instead of each part.
#[derive(Clone, Default)]
pub struct NodeIoEnv {
    const_coercers: Option<ConstCoercerMap>,
    types: Option<TypeIndex>,
    /// The node's connected output ports, so pushes by name reuse their ids.
    output_ports: Option<Arc<[PortId]>>,
    /// Stamps the payloads pushes build (the executor's clock).
    clock: Clock,
}

impl NodeIoEnv {
    pub(crate) fn new(
        const_coercers: Option<ConstCoercerMap>,
        types: Option<TypeIndex>,
        output_ports: Option<Arc<[PortId]>>,
        clock: Clock,
    ) -> Self {
        Self {
            const_coercers,
            types,
            output_ports,
            clock,
        }
    }
}

/// The clock of a `NodeIo` without an environment.
static PLATFORM_CLOCK: Clock = Clock::platform();

/// Most const inputs a call shares instead of copying (one bit each in `NodeIo::consts_taken`).
const SHARED_CONSTS: usize = 64;

/// A node's inputs and outputs for one call. Its port lists reuse per-thread buffers, so a
/// steady-state tick does not allocate them however many ports a node has, and its const inputs
/// are the node's shared list rather than per-call copies.
pub struct NodeIo {
    /// Edge inputs.
    inputs: Vec<NodePort>,
    outputs: Vec<NodePort>,
    env: Option<Arc<NodeIoEnv>>,
    /// Const inputs, after the edge inputs; taken ones are marked in `consts_taken`.
    consts: Option<Arc<NodeConstInputs>>,
    consts_taken: u64,
}

impl NodeIo {
    pub fn empty() -> Self {
        Self::from_port_buffer(port_buffer())
    }

    pub fn from_inputs(inputs: impl IntoIterator<Item = NodePort>) -> Self {
        let mut buffer = port_buffer();
        buffer.extend(inputs);
        Self::from_port_buffer(buffer)
    }

    pub fn from_single_input(port: PortId, payload: CorrelatedPayload) -> Self {
        Self::from_inputs([(port, payload)])
    }

    /// Take `inputs` as the input list (a [`port_buffer`]).
    pub(crate) fn from_port_buffer(inputs: Vec<NodePort>) -> Self {
        Self {
            inputs,
            outputs: port_buffer(),
            env: None,
            consts: None,
            consts_taken: 0,
        }
    }

    /// A call's io: edge `inputs` (a [`port_buffer`]), the node's environment and its const
    /// inputs (copied into the inputs when there are too many to track as shared).
    pub(crate) fn for_call(
        mut inputs: Vec<NodePort>,
        env: Arc<NodeIoEnv>,
        consts: Option<&Arc<NodeConstInputs>>,
    ) -> Self {
        let consts = consts.filter(|consts| !consts.is_empty());
        let consts = match consts {
            Some(consts) if consts.len() > SHARED_CONSTS => {
                inputs.extend(consts.iter().cloned());
                None
            }
            consts => consts.cloned(),
        };
        Self {
            inputs,
            outputs: port_buffer(),
            env: Some(env),
            consts,
            consts_taken: 0,
        }
    }

    fn env(&self) -> Option<&NodeIoEnv> {
        self.env.as_deref()
    }

    fn env_mut(&mut self) -> &mut NodeIoEnv {
        Arc::make_mut(self.env.get_or_insert_with(Default::default))
    }

    pub fn with_const_coercers(mut self, const_coercers: Option<ConstCoercerMap>) -> Self {
        self.env_mut().const_coercers = const_coercers;
        self
    }

    /// Resolve pushes to these output port names to the given ids instead of allocating new
    /// ones (the executor passes each node's connected output ports).
    pub fn with_output_ports(mut self, ports: Option<Arc<[PortId]>>) -> Self {
        self.env_mut().output_ports = ports;
        self
    }

    /// The id for an optional output port name ([`DEFAULT_OUTPUT_PORT`] for `None`): a known
    /// output port's id, or a new one.
    fn port_or_default(&self, port: Option<&str>) -> PortId {
        let Some(name) = port else {
            return PortId::from_static(DEFAULT_OUTPUT_PORT);
        };
        self.env()
            .and_then(|env| env.output_ports.as_deref())
            .into_iter()
            .flatten()
            .find(|known| known.as_str() == name)
            .cloned()
            .unwrap_or_else(|| PortId::new(name))
    }

    /// Stamp the payloads pushes build with `clock` (see `Payload::stamp`); the executor passes
    /// its own.
    pub fn with_clock(mut self, clock: Clock) -> Self {
        self.env_mut().clock = clock;
        self
    }

    /// The clock pushes stamp payloads with; stamp payloads a handler builds itself with it
    /// before [`Self::push_payload`].
    pub fn clock(&self) -> &Clock {
        self.env().map_or(&PLATFORM_CLOCK, |env| &env.clock)
    }

    /// Resolve generic pushes ([`Self::push_to`]) through `types`.
    pub fn with_type_index(mut self, types: Option<TypeIndex>) -> Self {
        self.env_mut().types = types;
        self
    }

    /// The const inputs not taken yet, with their index in the node's list.
    fn untaken_consts(&self) -> impl Iterator<Item = (usize, &NodePort)> {
        let taken = self.consts_taken;
        self.consts
            .as_deref()
            .into_iter()
            .flatten()
            .enumerate()
            .filter(move |(idx, _)| taken & (1 << idx) == 0)
    }

    /// Every input: the edge inputs, then the const inputs not taken yet.
    pub fn inputs(&self) -> impl Iterator<Item = &NodePort> {
        self.inputs
            .iter()
            .chain(self.untaken_consts().map(|(_, port)| port))
    }

    pub fn inputs_for<'a>(&'a self, port: &'a str) -> impl Iterator<Item = &'a CorrelatedPayload> {
        self.inputs()
            .filter(move |(name, _)| name == port)
            .map(|(_, payload)| payload)
    }

    /// The node's shared const inputs when no call has taken one, and whether an edge input
    /// feeds `port` (for caches keyed by the const list: see `const_cache`).
    pub(crate) fn shared_consts(&self) -> Option<&Arc<NodeConstInputs>> {
        self.consts.as_ref().filter(|_| self.consts_taken == 0)
    }

    pub(crate) fn has_edge_input(&self, port: &str) -> bool {
        self.inputs.iter().any(|(name, _)| name == port)
    }

    pub fn outputs(&self) -> &[NodePort] {
        &self.outputs
    }

    pub fn take_outputs(mut self) -> Vec<NodePort> {
        core::mem::take(&mut self.outputs)
    }

    /// Move out the payload pushed to `port`, if any, recycling the output list.
    pub(crate) fn take_output(mut self, port: &PortId) -> Option<Payload> {
        let idx = self.outputs.iter().position(|(name, _)| name == port)?;
        Some(self.outputs.swap_remove(idx).1.inner)
    }

    pub fn push_payload(&mut self, port: impl Into<PortId>, payload: Payload) {
        self.outputs
            .push((port.into(), CorrelatedPayload::from_edge(payload)));
    }

    pub fn push_payload_default(&mut self, payload: Payload) {
        self.push_payload(DEFAULT_OUTPUT_PORT, payload);
    }

    pub fn push_as<T>(
        &mut self,
        port: Option<&str>,
        type_key: daedalus_transport::TypeKey,
        value: T,
    ) where
        T: Send + Sync + 'static,
    {
        self.push_as_to(self.port_or_default(port), type_key, value);
    }

    pub fn push_as_to<T>(
        &mut self,
        port: impl Into<PortId>,
        type_key: daedalus_transport::TypeKey,
        value: T,
    ) where
        T: Send + Sync + 'static,
    {
        let payload = Payload::owned(type_key, value).stamp(self.clock());
        self.push_payload(port, payload);
    }

    pub fn push_as_default<T>(&mut self, type_key: daedalus_transport::TypeKey, value: T)
    where
        T: Send + Sync + 'static,
    {
        self.push_as_to(DEFAULT_OUTPUT_PORT, type_key, value);
    }

    pub fn push_arc_as<T>(
        &mut self,
        port: Option<&str>,
        type_key: daedalus_transport::TypeKey,
        value: Arc<T>,
    ) where
        T: Send + Sync + 'static,
    {
        self.push_arc_as_to(self.port_or_default(port), type_key, value);
    }

    pub fn push_arc_as_to<T>(
        &mut self,
        port: impl Into<PortId>,
        type_key: daedalus_transport::TypeKey,
        value: Arc<T>,
    ) where
        T: Send + Sync + 'static,
    {
        let payload = Payload::shared(type_key, value).stamp(self.clock());
        self.push_payload(port, payload);
    }

    pub fn push_arc_as_default<T>(&mut self, type_key: daedalus_transport::TypeKey, value: Arc<T>)
    where
        T: Send + Sync + 'static,
    {
        self.push_arc_as_to(DEFAULT_OUTPUT_PORT, type_key, value);
    }

    /// Push `value` under the key the graph's registry gives `T` (its `TypeIndex`, see
    /// [`Self::with_type_index`]; builtins only without one). Fails when the registry has no
    /// single key for `T`; use [`Self::push_as_to`] to name the key.
    pub fn push_to<T>(&mut self, port: impl Into<PortId>, value: T) -> Result<(), NodeError>
    where
        T: Send + Sync + 'static,
    {
        let type_key = self.type_index().key_of::<T>()?;
        self.push_as_to(port, type_key, value);
        Ok(())
    }

    /// [`Self::push_to`] on [`DEFAULT_OUTPUT_PORT`].
    pub fn push_default<T>(&mut self, value: T) -> Result<(), NodeError>
    where
        T: Send + Sync + 'static,
    {
        self.push_to(DEFAULT_OUTPUT_PORT, value)
    }

    /// [`Self::push_to`] on `port`, or [`DEFAULT_OUTPUT_PORT`] for `None`.
    pub fn push<T>(&mut self, port: Option<&str>, value: T) -> Result<(), NodeError>
    where
        T: Send + Sync + 'static,
    {
        self.push_to(self.port_or_default(port), value)
    }

    /// The type index generic pushes resolve through.
    pub fn type_index(&self) -> &TypeIndex {
        self.env()
            .and_then(|env| env.types.as_ref())
            .unwrap_or_else(|| TypeIndex::builtin())
    }

    pub fn push_value(&mut self, port: Option<&str>, value: Value) {
        self.push_value_to(self.port_or_default(port), value);
    }

    pub fn push_value_to(&mut self, port: impl Into<PortId>, value: Value) {
        let payload = Payload::owned("value", value).stamp(self.clock());
        self.push_payload(port, payload);
    }

    pub fn push_value_default(&mut self, value: Value) {
        self.push_value_to(DEFAULT_OUTPUT_PORT, value);
    }

    pub fn push_correlated_payload(&mut self, port: impl Into<PortId>, payload: CorrelatedPayload) {
        self.outputs.push((port.into(), payload));
    }

    /// Take the input on `port`: an edge input moves out; a const input is a shared clone, marked
    /// taken for this call.
    pub fn take_input_payload(&mut self, port: &str) -> Option<CorrelatedPayload> {
        if let Some(idx) = self.inputs.iter().position(|(name, _)| name == port) {
            return Some(self.inputs.remove(idx).1);
        }
        let (idx, (_, payload)) = self.untaken_consts().find(|(_, (name, _))| name == port)?;
        let payload = payload.clone();
        self.consts_taken |= 1 << idx;
        Some(payload)
    }

    pub fn get_payload(&self, port: &str) -> Option<&Payload> {
        self.inputs()
            .find(|(name, _)| name == port)
            .map(|(_, payload)| &payload.inner)
    }

    pub fn get_typed_ref<T>(&self, port: &str) -> Option<&T>
    where
        T: Send + Sync + 'static,
    {
        self.get_payload(port)?.get_ref::<T>()
    }

    pub fn get_ref<T>(&self, port: &str) -> Option<&T>
    where
        T: Send + Sync + 'static,
    {
        self.get_typed_ref(port)
    }

    pub fn get_arc<T>(&self, port: &str) -> Option<Arc<T>>
    where
        T: Send + Sync + 'static,
    {
        self.get_payload(port)?.get_arc::<T>()
    }

    pub fn payload_raw(&self, port: &str) -> Option<&dyn Any> {
        self.get_payload(port)?.value_any()
    }

    pub fn get_typed<T>(&self, port: &str) -> Option<T>
    where
        T: Clone + Send + Sync + 'static,
    {
        if let Some(value) = self.get_payload(port)?.get_ref::<T>() {
            return Some(value.clone());
        }
        self.get_payload(port)?
            .get_ref::<Value>()
            .and_then(|value| self.coerce_value::<T>(value))
    }

    pub fn get_all_fanin_indexed<T>(&self, prefix: &str) -> Vec<(u32, T)>
    where
        T: Clone + Send + Sync + 'static,
    {
        self.inputs()
            .filter_map(|(port, payload)| {
                let index = crate::fanin::parse_indexed_port(prefix, port.as_str())?;
                let value = payload.inner.get_ref::<T>()?.clone();
                Some((index, value))
            })
            .collect()
    }

    pub fn get_typed_mut<T>(&mut self, port: &str) -> Option<T>
    where
        T: Clone + Send + Sync + 'static,
    {
        let payload = self.take_input_payload(port)?;
        if let Some(value) = payload.inner.get_ref::<T>() {
            return Some(value.clone());
        }
        payload
            .inner
            .get_ref::<Value>()
            .and_then(|value| self.coerce_value::<T>(value))
    }

    /// Take the input as an owned `T`; a `Value` input (a graph constant) is coerced to `T`
    /// without first failing a move (which would box the payload).
    pub fn take_owned<T>(&mut self, port: &str) -> Option<T>
    where
        T: Send + Sync + 'static,
    {
        let payload = self.take_input_payload(port)?.inner;
        if core::any::TypeId::of::<T>() != core::any::TypeId::of::<Value>()
            && let Some(value) = payload.get_ref::<Value>()
        {
            return self.coerce_value::<T>(value);
        }
        payload.try_into_owned::<T>().ok()
    }

    /// Coerce a `Value` input (a graph constant) to `T` through the builtin conversions and the
    /// registered const coercers ([`crate::const_coerce`]).
    pub fn coerce_input<T>(&self, port: &str) -> Option<T>
    where
        T: Send + Sync + 'static,
    {
        self.coerce_value(self.get_payload(port)?.get_ref::<Value>()?)
    }

    pub fn take_modify<T>(&mut self, port: &str) -> Option<T>
    where
        T: Send + Sync + 'static,
    {
        self.take_owned(port)
    }

    pub(crate) fn coerce_value<T>(&self, value: &Value) -> Option<T>
    where
        T: Send + Sync + 'static,
    {
        if let Some(map) = self.env().and_then(|env| env.const_coercers.as_ref())
            && let Some(coercer) = map.read().get(core::any::type_name::<T>())
            && let Some(any) = coercer(value)
            && let Ok(typed) = any.downcast::<T>()
        {
            return Some(*typed);
        }

        typing::coerce_builtin_const_value::<T>(value)
    }

    pub fn flush(&mut self) -> Result<(), crate::executor::NodeError> {
        Ok(())
    }
}

impl Drop for NodeIo {
    fn drop(&mut self) {
        recycle_ports(core::mem::take(&mut self.inputs));
        recycle_ports(core::mem::take(&mut self.outputs));
    }
}

/// An `Arc<T>` wrapper that supports copy-on-write mutation via `Arc::make_mut`.
///
/// The transport layer can hand this to `mut` node parameters when the graph cannot prove
/// single ownership. Exclusive producers still mutate in place; shared fanout falls back to COW.
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

impl<T> DerefMut for CowArcMut<T>
where
    T: Clone,
{
    fn deref_mut(&mut self) -> &mut Self::Target {
        self.make_mut()
    }
}
