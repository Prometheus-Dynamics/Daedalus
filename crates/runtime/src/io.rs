use std::any::Any;
use std::collections::HashMap;
use std::ops::{Deref, DerefMut};
use std::sync::{Arc, RwLock};

use daedalus_data::model::{TypeExpr, Value};
use daedalus_data::typing;
use daedalus_transport::Payload;
use smallvec::SmallVec;

use crate::executor::CorrelatedPayload;

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

pub fn new_const_coercer_map() -> ConstCoercerMap {
    Arc::new(RwLock::new(HashMap::new()))
}

pub struct NodeIo {
    inputs: SmallVec<[(String, CorrelatedPayload); 4]>,
    outputs: SmallVec<[(String, CorrelatedPayload); 4]>,
    const_coercers: Option<ConstCoercerMap>,
}

impl NodeIo {
    pub fn empty() -> Self {
        Self {
            inputs: SmallVec::new(),
            outputs: SmallVec::new(),
            const_coercers: None,
        }
    }

    pub fn from_inputs(inputs: Vec<(String, CorrelatedPayload)>) -> Self {
        Self {
            inputs: SmallVec::from_vec(inputs),
            outputs: SmallVec::new(),
            const_coercers: None,
        }
    }

    pub fn from_single_input(port: String, payload: CorrelatedPayload) -> Self {
        let mut inputs = SmallVec::new();
        inputs.push((port, payload));
        Self {
            inputs,
            outputs: SmallVec::new(),
            const_coercers: None,
        }
    }

    pub fn with_const_coercers(mut self, const_coercers: Option<ConstCoercerMap>) -> Self {
        self.const_coercers = const_coercers;
        self
    }

    pub fn inputs(&self) -> &[(String, CorrelatedPayload)] {
        &self.inputs
    }

    pub fn inputs_for<'a>(&'a self, port: &'a str) -> impl Iterator<Item = &'a CorrelatedPayload> {
        self.inputs
            .iter()
            .filter(move |(name, _)| name == port)
            .map(|(_, payload)| payload)
    }

    pub fn outputs(&self) -> &[(String, CorrelatedPayload)] {
        &self.outputs
    }

    pub fn take_outputs(self) -> Vec<(String, CorrelatedPayload)> {
        self.outputs.into_vec()
    }

    pub fn take_outputs_small(self) -> SmallVec<[(String, CorrelatedPayload); 4]> {
        self.outputs
    }

    pub fn push_payload(&mut self, port: impl Into<String>, payload: Payload) {
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
        self.push_as_to(port.unwrap_or(DEFAULT_OUTPUT_PORT), type_key, value);
    }

    pub fn push_as_to<T>(
        &mut self,
        port: impl Into<String>,
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
        self.push_arc_as_to(port.unwrap_or(DEFAULT_OUTPUT_PORT), type_key, value);
    }

    pub fn push_arc_as_to<T>(
        &mut self,
        port: impl Into<String>,
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

    pub fn push_any<T>(&mut self, port: Option<&str>, value: T)
    where
        T: Send + Sync + 'static,
    {
        let type_key = crate::transport::typeexpr_transport_key(&typing::type_expr::<T>())
            .unwrap_or_else(|_| daedalus_transport::TypeKey::new(std::any::type_name::<T>()));
        self.push_as_to(port.unwrap_or(DEFAULT_OUTPUT_PORT), type_key, value);
    }

    pub fn push_to<T>(&mut self, port: impl Into<String>, value: T)
    where
        T: Send + Sync + 'static,
    {
        let type_key = crate::transport::typeexpr_transport_key(&typing::type_expr::<T>())
            .unwrap_or_else(|_| daedalus_transport::TypeKey::new(std::any::type_name::<T>()));
        self.push_as_to(port, type_key, value);
    }

    pub fn push_default<T>(&mut self, value: T)
    where
        T: Send + Sync + 'static,
    {
        self.push_to(DEFAULT_OUTPUT_PORT, value);
    }

    pub fn push<T>(&mut self, port: Option<&str>, value: T)
    where
        T: Send + Sync + 'static,
    {
        self.push_to(port.unwrap_or(DEFAULT_OUTPUT_PORT), value);
    }

    pub fn push_output<T>(&mut self, port: Option<&str>, value: T)
    where
        T: Send + Sync + 'static,
    {
        self.push_to(port.unwrap_or(DEFAULT_OUTPUT_PORT), value);
    }

    pub fn push_output_default<T>(&mut self, value: T)
    where
        T: Send + Sync + 'static,
    {
        self.push_default(value);
    }

    pub fn push_value(&mut self, port: Option<&str>, value: Value) {
        self.push_value_to(port.unwrap_or(DEFAULT_OUTPUT_PORT), value);
    }

    pub fn push_value_to(&mut self, port: impl Into<String>, value: Value) {
        self.push_payload(port, Payload::owned("value", value));
    }

    pub fn push_value_default(&mut self, value: Value) {
        self.push_value_to(DEFAULT_OUTPUT_PORT, value);
    }

    pub fn push_correlated_payload(&mut self, port: impl Into<String>, payload: CorrelatedPayload) {
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
                let index = crate::fanin::parse_indexed_port(prefix, port)?;
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

    pub fn take_owned<T>(&mut self, port: &str) -> Option<T>
    where
        T: Send + Sync + 'static,
    {
        self.take_input_payload(port)?
            .inner
            .try_into_owned::<T>()
            .ok()
    }

    pub fn take_modify<T>(&mut self, port: &str) -> Option<T>
    where
        T: Send + Sync + 'static,
    {
        self.take_owned(port)
    }

    fn coerce_value<T>(&self, value: &Value) -> Option<T>
    where
        T: Clone + Send + Sync + 'static,
    {
        if let Some(map) = self.const_coercers.as_ref()
            && let Ok(guard) = map.read()
            && let Some(coercer) = guard.get(std::any::type_name::<T>())
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
