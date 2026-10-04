use parking_lot::RwLock;
use std::any::Any;
use std::collections::HashMap;
use std::ops::{Deref, DerefMut};
use std::sync::Arc;

use daedalus_data::model::{TypeExpr, Value};
use daedalus_data::typing;
use daedalus_transport::Payload;
use smallvec::SmallVec;

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

pub fn new_const_coercer_map() -> ConstCoercerMap {
    Arc::new(RwLock::new(HashMap::new()))
}

pub struct NodeIo {
    inputs: SmallVec<[NodePort; 4]>,
    outputs: SmallVec<[NodePort; 4]>,
    const_coercers: Option<ConstCoercerMap>,
    types: Option<TypeIndex>,
    /// The node's connected output ports, so pushes by name reuse their ids.
    output_ports: Option<Arc<[PortId]>>,
}

impl NodeIo {
    pub fn empty() -> Self {
        Self {
            inputs: SmallVec::new(),
            outputs: SmallVec::new(),
            const_coercers: None,
            types: None,
            output_ports: None,
        }
    }

    pub fn from_inputs(inputs: impl IntoIterator<Item = NodePort>) -> Self {
        Self {
            inputs: inputs.into_iter().collect(),
            outputs: SmallVec::new(),
            const_coercers: None,
            types: None,
            output_ports: None,
        }
    }

    pub fn from_single_input(port: PortId, payload: CorrelatedPayload) -> Self {
        let mut inputs = SmallVec::new();
        inputs.push((port, payload));
        Self {
            inputs,
            outputs: SmallVec::new(),
            const_coercers: None,
            types: None,
            output_ports: None,
        }
    }

    pub fn with_const_coercers(mut self, const_coercers: Option<ConstCoercerMap>) -> Self {
        self.const_coercers = const_coercers;
        self
    }

    /// Resolve pushes to these output port names to the given ids instead of allocating new
    /// ones (the executor passes each node's connected output ports).
    pub fn with_output_ports(mut self, ports: Option<Arc<[PortId]>>) -> Self {
        self.output_ports = ports;
        self
    }

    /// The id for an optional output port name ([`DEFAULT_OUTPUT_PORT`] for `None`): a known
    /// output port's id, or a new one.
    fn port_or_default(&self, port: Option<&str>) -> PortId {
        let Some(name) = port else {
            return PortId::from_static(DEFAULT_OUTPUT_PORT);
        };
        self.output_ports
            .iter()
            .flat_map(|ports| ports.iter())
            .find(|known| known.as_str() == name)
            .cloned()
            .unwrap_or_else(|| PortId::new(name))
    }

    /// Resolve generic pushes ([`Self::push_to`]) through `types`.
    pub fn with_type_index(mut self, types: Option<TypeIndex>) -> Self {
        self.types = types;
        self
    }

    pub fn inputs(&self) -> &[NodePort] {
        &self.inputs
    }

    pub fn inputs_for<'a>(&'a self, port: &'a str) -> impl Iterator<Item = &'a CorrelatedPayload> {
        self.inputs
            .iter()
            .filter(move |(name, _)| name == port)
            .map(|(_, payload)| payload)
    }

    pub fn outputs(&self) -> &[NodePort] {
        &self.outputs
    }

    pub fn take_outputs(self) -> Vec<NodePort> {
        self.outputs.into_vec()
    }

    pub fn take_outputs_small(self) -> SmallVec<[NodePort; 4]> {
        self.outputs
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
        self.push_payload(port, Payload::owned(type_key, value));
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
        self.push_payload(port, Payload::shared(type_key, value));
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
        self.types.as_ref().unwrap_or_else(|| TypeIndex::builtin())
    }

    pub fn push_value(&mut self, port: Option<&str>, value: Value) {
        self.push_value_to(self.port_or_default(port), value);
    }

    pub fn push_value_to(&mut self, port: impl Into<PortId>, value: Value) {
        self.push_payload(port, Payload::owned("value", value));
    }

    pub fn push_value_default(&mut self, value: Value) {
        self.push_value_to(DEFAULT_OUTPUT_PORT, value);
    }

    pub fn push_correlated_payload(&mut self, port: impl Into<PortId>, payload: CorrelatedPayload) {
        self.outputs.push((port.into(), payload));
    }

    pub fn take_input_payload(&mut self, port: &str) -> Option<CorrelatedPayload> {
        let idx = self.inputs.iter().position(|(name, _)| name == port)?;
        Some(self.inputs.remove(idx).1)
    }

    pub fn get_payload(&self, port: &str) -> Option<&Payload> {
        self.inputs
            .iter()
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
        self.inputs
            .iter()
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

    /// Move the input out of `port`; const and `Value` inputs coerce to `T` like
    /// [`Self::get_typed`].
    pub fn take_owned<T>(&mut self, port: &str) -> Option<T>
    where
        T: Send + Sync + 'static,
    {
        let payload = self.take_input_payload(port)?.inner;
        if std::any::TypeId::of::<T>() != std::any::TypeId::of::<Value>()
            && let Some(value) = payload.get_ref::<Value>()
        {
            return self.coerce_value::<T>(value);
        }
        payload.try_into_owned::<T>().ok()
    }

    pub fn take_modify<T>(&mut self, port: &str) -> Option<T>
    where
        T: Send + Sync + 'static,
    {
        self.take_owned(port)
    }

    fn coerce_value<T>(&self, value: &Value) -> Option<T>
    where
        T: Send + Sync + 'static,
    {
        if let Some(map) = self.const_coercers.as_ref()
            && let Some(coercer) = map.read().get(std::any::type_name::<T>())
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
