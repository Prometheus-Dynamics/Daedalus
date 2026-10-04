//! Plugin side of the stable handler path: the registry `invoke` runs nodes from, input
//! decoding and output encoding.

use std::collections::HashMap;
use std::panic::{AssertUnwindSafe, catch_unwind};
use std::sync::{Arc, Mutex, OnceLock};

use super::value::{Arena, StableValue, to_value};
use super::{StableInput, StableOutputSink, builtin, status};
use crate::data::model::{TypeExpr, Value, ValueType};
use crate::dylib::__support::LinkedDep;
use crate::dylib::{StrSink, extract};
use crate::runtime::executor::{CorrelatedPayload, NodeHandler, panic_message};
use crate::runtime::handler_registry::HandlerRegistry;
use crate::runtime::handles::PortId;
use crate::runtime::host_bridge::ValueSerializerMap;
use crate::runtime::io::{ConstCoercerMap, NodeIo};
use crate::runtime::plugins::{Plugin, StableCodec};
use crate::runtime::{ExecutionContext, NodeError, RuntimeNode, StateStore, TypeIndex};
use crate::transport::{Payload, TypeKey};

/// The plugin's stable runtime, built on the first `invoke` (an error if its registry fails).
pub type StableRuntimeCell = OnceLock<Result<StableRuntime, String>>;

type Failure = (u32, String);

/// What `invoke` runs nodes with: the plugin's handlers and conversions, its nodes in schema
/// order, and one state store per host node instance.
pub struct StableRuntime {
    handlers: HandlerRegistry,
    const_coercers: ConstCoercerMap,
    value_serializers: ValueSerializerMap,
    types: TypeIndex,
    nodes: Vec<StableNode>,
    /// The context (and with it the state store) of each host node instance.
    instances: Mutex<HashMap<u64, ExecutionContext>>,
}

struct StableNode {
    node: RuntimeNode,
    /// Graph nodes (expanded by the host's planner) and declaration-only nodes have none.
    has_handler: bool,
    state_id: Arc<str>,
    inputs: Vec<StablePort>,
    outputs: Vec<StablePort>,
}

struct StablePort {
    name: String,
    id: PortId,
    key: TypeKey,
    codec: Option<StableCodec>,
    /// The builtin scalar the handler's Rust type is, if any.
    scalar: Option<ValueType>,
}

impl StableRuntime {
    fn build<P: Plugin + Default>(deps: &[LinkedDep]) -> Result<Self, String> {
        let (registry, plugin) = extract::registry::<P>(deps, true)?;
        let schema = extract::schema(&registry, &plugin, deps)?;
        let builtins = crate::data::typing::builtin_type_exprs();
        let port = |node: &str, port: &daedalus_ffi_host::core::WirePort| {
            let codec = registry.stable_codec(node, &port.name).cloned();
            let scalar = codec
                .as_ref()
                .and_then(|codec| match builtins.get(&codec.type_id) {
                    Some(TypeExpr::Scalar(scalar)) => Some(*scalar),
                    _ => None,
                });
            StablePort {
                id: PortId::new(port.name.clone()),
                key: port
                    .type_key
                    .clone()
                    .unwrap_or_else(|| crate::registry::typeexpr_transport_key(&port.ty)),
                name: port.name.clone(),
                codec,
                scalar,
            }
        };
        let nodes = schema
            .nodes
            .iter()
            .map(|node| StableNode {
                node: RuntimeNode::new(node.id.clone()),
                has_handler: registry.handlers.has_handler(&node.id),
                state_id: Arc::from(node.id.as_str()),
                inputs: node.inputs.iter().map(|p| port(&node.id, p)).collect(),
                outputs: node.outputs.iter().map(|p| port(&node.id, p)).collect(),
            })
            .collect();
        Ok(Self {
            types: registry.type_index(),
            const_coercers: registry.const_coercers.clone(),
            value_serializers: registry.value_serializers.clone(),
            handlers: registry.handlers.clone_arc(),
            nodes,
            instances: Mutex::default(),
        })
    }

    fn context(&self, instance: u64, node: &StableNode) -> ExecutionContext {
        let mut instances = self.instances.lock().unwrap_or_else(|err| err.into_inner());
        instances
            .entry(instance)
            .or_insert_with(|| {
                ExecutionContext::detached(StateStore::default(), node.state_id.clone())
            })
            .clone()
    }

    /// # Safety
    /// Every pointer in `inputs` must be valid for the call, and `outputs` must be the host's
    /// sink for it.
    unsafe fn invoke(
        &self,
        index: u32,
        instance: u64,
        inputs: &[StableInput],
        outputs: StableOutputSink,
    ) -> Result<(), Failure> {
        let failed = |message: String| (status::FAILED, message);
        let node = self
            .nodes
            .get(index as usize)
            .ok_or_else(|| failed(format!("no node with index {index}")))?;
        if !node.has_handler {
            return Err(failed(format!("node `{}` has no handler", node.node.id)));
        }
        // Decoded straight into the `NodeIo`'s pooled port buffer; the first failure stops it.
        let mut failure = None;
        let ports = inputs.iter().map_while(|input| {
            // Safety: forwarded from the caller.
            match unsafe { decode_port(node, input) } {
                Ok(port) => Some(port),
                Err(message) => {
                    failure = Some((status::INVALID_INPUT, message));
                    None
                }
            }
        });
        let mut io = NodeIo::from_inputs(ports)
            .with_const_coercers(Some(self.const_coercers.clone()))
            .with_type_index(Some(self.types.clone()));
        if let Some(failure) = failure {
            return Err(failure);
        }
        let ctx = self.context(instance, node);
        self.handlers
            .run(&node.node, &ctx, &mut io)
            .map_err(|error| (error_status(&error), error.to_string()))?;

        let push = outputs
            .push
            .ok_or_else(|| failed("no output sink".into()))?;
        let mut arena = Arena::default();
        for (port, payload) in io.take_outputs() {
            let name = port.as_str();
            let value = self
                .encode_output(node.output(name), &payload.inner, &mut arena)
                .map_err(|err| failed(format!("output `{name}` of `{}`: {err}", node.node.id)))?;
            // Safety: the host's sink for this call; the value borrows `payload` and `arena`.
            if !unsafe { push(outputs.ctx, name.as_ptr(), name.len(), &value) } {
                return Err(failed(format!("the host rejected output `{name}`")));
            }
        }
        Ok(())
    }

    fn encode_output(
        &self,
        port: Option<&StablePort>,
        payload: &Payload,
        arena: &mut Arena,
    ) -> Result<StableValue, String> {
        if let Some(handle) = payload.foreign_handle() {
            return Ok(StableValue::handle(handle, payload.residency()));
        }
        let unsupported = || {
            format!(
                "`{}` has no stable encoding (derive `DaedalusToValue` for it, or register a \
                 value serializer)",
                payload.storage_rust_type_name().unwrap_or("bytes")
            )
        };
        let any = payload.value_any_sync().ok_or_else(unsupported)?;
        if let Some(value) = any.downcast_ref::<Value>() {
            return Ok(arena.encode(value));
        }
        if let Some(value) = builtin::encode(any) {
            return Ok(value);
        }
        let encoded = port
            .and_then(|port| port.codec.as_ref()?.encode.as_ref())
            .and_then(|encode| encode(any))
            .or_else(|| {
                let serializers = self.value_serializers.read();
                serializers.get(&(*any).type_id())?(any)
            });
        encoded
            .map(|value| arena.encode_owned(value))
            .ok_or_else(unsupported)
    }
}

impl StableNode {
    fn input(&self, name: &str) -> Option<&StablePort> {
        find(&self.inputs, name).or_else(|| {
            // Fan-in ports arrive indexed (`in0`, `in1`, ...) under the declared name.
            find(
                &self.inputs,
                name.trim_end_matches(|c: char| c.is_ascii_digit()),
            )
        })
    }

    fn output(&self, name: &str) -> Option<&StablePort> {
        find(&self.outputs, name)
    }
}

fn find<'a>(ports: &'a [StablePort], name: &str) -> Option<&'a StablePort> {
    ports.iter().find(|port| port.name == name)
}

/// # Safety
/// Every pointer in `input` must be valid for the call.
unsafe fn decode_port(
    node: &StableNode,
    input: &StableInput,
) -> Result<(PortId, CorrelatedPayload), String> {
    // Safety (both): forwarded from the caller.
    let name = unsafe { super::value::str_at(input.port_ptr, input.port_len) }?;
    let port = node.input(name);
    let payload = unsafe { decode_input(port, name, &input.value) }
        .map_err(|err| format!("input `{name}` of `{}`: {err}", node.node.id))?;
    let id = port.map_or_else(|| PortId::new(name), |port| port.id.clone());
    Ok((id, CorrelatedPayload::from_edge(payload)))
}

/// # Safety
/// Every pointer in `value` must be valid for the call.
unsafe fn decode_input(
    port: Option<&StablePort>,
    name: &str,
    value: &StableValue,
) -> Result<Payload, String> {
    let key = port.map_or_else(|| TypeKey::new(name), |port| port.key.clone());
    // Safety (throughout): forwarded from the caller.
    if let Some(handle) = unsafe { value.as_handle() } {
        return Ok(Payload::foreign(key, handle.clone(), value.residency()));
    }
    if let Some(scalar) = port.and_then(|port| port.scalar) {
        return unsafe { builtin::decode(scalar, key, value) };
    }
    let value = unsafe { to_value(value) }?;
    match port.and_then(|port| port.codec.as_ref()) {
        Some(codec) if codec.type_id != std::any::TypeId::of::<Value>() => {
            (codec.decode)(key, &value)
                .ok_or_else(|| format!("cannot build `{}` from {value:?}", codec.rust_type))
        }
        // No Rust type known (or `Value` itself): handlers coerce `Value` payloads.
        _ => Ok(Payload::owned(key, value)),
    }
}

fn error_status(error: &NodeError) -> u32 {
    match error {
        NodeError::Handler(_) => status::HANDLER_ERROR,
        NodeError::InvalidInput(_) => status::INVALID_INPUT,
        NodeError::BackpressureDrop(_) => status::BACKPRESSURE,
        _ => status::FAILED,
    }
}

/// The `invoke` entry point of plugin `P` (see [`super::InvokeFn`]).
///
/// # Safety
/// The arguments must follow [`super::InvokeFn`]'s contract.
#[allow(clippy::too_many_arguments)]
pub unsafe fn invoke<P: Plugin + Default>(
    cell: &StableRuntimeCell,
    deps: &[LinkedDep],
    node: u32,
    instance: u64,
    inputs: *const StableInput,
    len: usize,
    outputs: StableOutputSink,
    error: StrSink,
) -> u32 {
    let result = catch_unwind(AssertUnwindSafe(|| {
        let runtime = cell
            .get_or_init(|| StableRuntime::build::<P>(deps))
            .as_ref()
            .map_err(|err| (status::FAILED, format!("plugin registry: {err}")))?;
        let inputs = if len == 0 || inputs.is_null() {
            &[][..]
        } else {
            // Safety: the host passes `len` inputs.
            unsafe { std::slice::from_raw_parts(inputs, len) }
        };
        // Safety: forwarded from the caller.
        unsafe { runtime.invoke(node, instance, inputs, outputs) }
    }));
    let (code, message) = match result {
        Ok(Ok(())) => return status::OK,
        Ok(Err(failure)) => failure,
        Err(panic) => (status::PANIC, panic_message(&*panic)),
    };
    // Safety: the host's sink for this call.
    unsafe { error.write(&message) };
    code
}

/// The `release` entry point (see [`super::ReleaseFn`]).
pub fn release(cell: &StableRuntimeCell, instance: u64) {
    let _ = catch_unwind(AssertUnwindSafe(|| {
        if let Some(Ok(runtime)) = cell.get() {
            let mut instances = runtime
                .instances
                .lock()
                .unwrap_or_else(|err| err.into_inner());
            instances.remove(&instance);
        }
    }));
}
