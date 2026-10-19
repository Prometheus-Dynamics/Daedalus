//! Plugin side of the stable handler path: the registry instances run nodes from, input
//! decoding and output encoding.

use std::panic::{AssertUnwindSafe, catch_unwind};
use std::sync::{Arc, Mutex};

use super::value::{Arena, StableValue, to_value};
use super::{NO_FANIN, StableInput, StableOutputSink, builtin, status};
use crate::bytes::{SHARED_BYTES_KEY, SharedBytes};
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
use crate::transport::{Payload, Residency, TypeKey};

/// The plugin's stable runtime: built by the first `create` (again after `unload`), shared by
/// every instance created from it.
pub struct StableRuntimeCell(Mutex<Option<Arc<StableRuntime>>>);

impl StableRuntimeCell {
    pub const fn new() -> Self {
        Self(Mutex::new(None))
    }

    fn get_or_build<P: Plugin + Default>(
        &self,
        deps: &[LinkedDep],
    ) -> Result<Arc<StableRuntime>, String> {
        let mut cell = self.0.lock().unwrap_or_else(|err| err.into_inner());
        if let Some(runtime) = &*cell {
            return Ok(runtime.clone());
        }
        let runtime = Arc::new(StableRuntime::build::<P>(deps)?);
        *cell = Some(runtime.clone());
        Ok(runtime)
    }
}

impl Default for StableRuntimeCell {
    fn default() -> Self {
        Self::new()
    }
}

type Failure = (u32, String);

/// What instances run nodes with: the plugin's handlers and conversions and its nodes in schema
/// order.
pub struct StableRuntime {
    handlers: HandlerRegistry,
    const_coercers: ConstCoercerMap,
    value_serializers: ValueSerializerMap,
    types: TypeIndex,
    nodes: Vec<StableNode>,
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
    /// A [`SharedBytes`] port: crosses as a `daedalus:bytes` handle.
    shared_bytes: bool,
}

/// One host node instance: its node, execution context (and with it node state) and the port
/// ids its calls resolved. The host never runs one instance concurrently.
struct StableInstance {
    runtime: Arc<StableRuntime>,
    node: usize,
    ctx: ExecutionContext,
    /// Indexed fan-in port ids (`in3`), built on first use.
    fanin: Vec<(u32, u32, PortId)>,
    /// The last call's outputs and their port indices, position by position.
    outputs: Vec<(PortId, u32)>,
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
            let key = port
                .type_key
                .clone()
                .unwrap_or_else(|| crate::registry::typeexpr_transport_key(&port.ty));
            StablePort {
                id: PortId::new(port.name.clone()),
                shared_bytes: key.as_str() == SHARED_BYTES_KEY,
                key,
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
        })
    }

    fn instance(self: &Arc<Self>, index: u32) -> Result<StableInstance, Failure> {
        let failed = |message: String| (status::FAILED, message);
        let node = self
            .nodes
            .get(index as usize)
            .ok_or_else(|| failed(format!("no node with index {index}")))?;
        if !node.has_handler {
            return Err(failed(format!("node `{}` has no handler", node.node.id)));
        }
        Ok(StableInstance {
            runtime: self.clone(),
            node: index as usize,
            ctx: ExecutionContext::detached(StateStore::default(), node.state_id.clone()),
            fanin: Vec::new(),
            outputs: Vec::new(),
        })
    }

    fn encode_output(
        &self,
        port: &StablePort,
        payload: &Payload,
        arena: &mut Arena,
    ) -> Result<StableValue, String> {
        if let Some(value) = arena.encode_foreign(payload) {
            return Ok(value);
        }
        let unsupported = || {
            format!(
                "`{}` has no stable encoding (derive `DaedalusToValue` for it, or register a \
                 value serializer)",
                payload.storage_rust_type_name().unwrap_or("bytes")
            )
        };
        let any = payload.value_any_sync().ok_or_else(unsupported)?;
        if let Some(bytes) = any.downcast_ref::<SharedBytes>() {
            // A local buffer lends this build's handle; a received one passes the host's on.
            return Ok(arena.lend(bytes.to_handle(), Residency::Cpu, !bytes.is_foreign()));
        }
        if let Some(value) = any.downcast_ref::<Value>() {
            return Ok(arena.encode(value));
        }
        if let Some(value) = builtin::encode(any) {
            return Ok(value);
        }
        let encoded = port
            .codec
            .as_ref()
            .and_then(|codec| codec.encode.as_ref())
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

impl StableInstance {
    /// # Safety
    /// Every pointer in `inputs` must be valid for the call, and `outputs` must be the host's
    /// sink for it.
    unsafe fn invoke(
        &mut self,
        inputs: &[StableInput],
        outputs: StableOutputSink,
    ) -> Result<(), Failure> {
        let failed = |message: String| (status::FAILED, message);
        let runtime = &*self.runtime;
        let node = &runtime.nodes[self.node];
        // Decoded straight into the `NodeIo`'s pooled port buffer; the first failure stops it.
        let mut failure = None;
        let fanin = &mut self.fanin;
        let ports = inputs.iter().map_while(|input| {
            // Safety: forwarded from the caller.
            match unsafe { decode_port(node, fanin, input) } {
                Ok(port) => Some(port),
                Err(message) => {
                    failure = Some((status::INVALID_INPUT, message));
                    None
                }
            }
        });
        let mut io = NodeIo::from_inputs(ports)
            .with_const_coercers(Some(runtime.const_coercers.clone()))
            .with_type_index(Some(runtime.types.clone()));
        if let Some(failure) = failure {
            return Err(failure);
        }
        runtime
            .handlers
            .run(&node.node, &self.ctx, &mut io)
            .map_err(|error| (error_status(&error), error.to_string()))?;

        let push = outputs
            .push
            .ok_or_else(|| failed("no output sink".into()))?;
        let mut arena = Arena::default();
        for (position, (port, payload)) in io.take_outputs().into_iter().enumerate() {
            let index =
                output_index(&mut self.outputs, node, position, &port).ok_or_else(|| {
                    failed(format!(
                        "node `{}` pushed undeclared output `{port}`",
                        node.node.id
                    ))
                })?;
            let value = runtime
                .encode_output(&node.outputs[index as usize], &payload.inner, &mut arena)
                .map_err(|err| failed(format!("output `{port}` of `{}`: {err}", node.node.id)))?;
            // Safety: the host's sink for this call; the value borrows `payload` and `arena`.
            if !unsafe { push(outputs.ctx, index, &value) } {
                return Err(failed(format!("the host rejected output `{port}`")));
            }
        }
        Ok(())
    }
}

/// The index of output `port`, pushed at `position`: the cached index when the same port was
/// pushed there last call, else a lookup (cached for the next call).
fn output_index(
    cache: &mut Vec<(PortId, u32)>,
    node: &StableNode,
    position: usize,
    port: &PortId,
) -> Option<u32> {
    if let Some((cached, index)) = cache.get(position)
        && cached == port
    {
        return Some(*index);
    }
    let index = node
        .outputs
        .iter()
        .position(|out| out.name == port.as_str())? as u32;
    cache.truncate(position);
    cache.push((port.clone(), index));
    Some(index)
}

/// # Safety
/// Every pointer in `input` must be valid for the call.
unsafe fn decode_port(
    node: &StableNode,
    fanin: &mut Vec<(u32, u32, PortId)>,
    input: &StableInput,
) -> Result<(PortId, CorrelatedPayload), String> {
    let port = node
        .inputs
        .get(input.port as usize)
        .ok_or_else(|| format!("node `{}` has no input {}", node.node.id, input.port))?;
    // Safety: forwarded from the caller.
    let payload = unsafe { decode_input(port, &input.value) }
        .map_err(|err| format!("input `{}` of `{}`: {err}", port.name, node.node.id))?;
    let id = if input.fanin == NO_FANIN {
        port.id.clone()
    } else if let Some((.., id)) = fanin
        .iter()
        .find(|(port, index, _)| (*port, *index) == (input.port, input.fanin))
    {
        id.clone()
    } else {
        // Fan-in ports arrive indexed (`in0`, `in1`, ...) under the declared name.
        let id = PortId::new(format!("{}{}", port.name, input.fanin));
        fanin.push((input.port, input.fanin, id.clone()));
        id
    };
    Ok((id, CorrelatedPayload::from_edge(payload)))
}

/// # Safety
/// Every pointer in `value` must be valid for the call.
unsafe fn decode_input(port: &StablePort, value: &StableValue) -> Result<Payload, String> {
    let key = port.key.clone();
    // Safety (throughout): forwarded from the caller.
    if let Some(handle) = unsafe { value.as_handle() } {
        if port.shared_bytes {
            // The host's buffer, read in place; the host keeps its own code loaded.
            let bytes = SharedBytes::from_handle(handle.clone(), None)
                .map_err(|err| format!("expected `daedalus:bytes`: {err:?}"))?;
            return Ok(Payload::owned(key, bytes));
        }
        return Ok(Payload::foreign(key, handle.clone(), value.residency()));
    }
    if port.shared_bytes && value.tag == super::tag::BYTES {
        return Ok(Payload::owned(
            key,
            SharedBytes::from(unsafe { value.as_bytes() }),
        ));
    }
    if let Some(scalar) = port.scalar {
        return unsafe { builtin::decode(scalar, key, value) };
    }
    let value = unsafe { to_value(value) }?;
    match &port.codec {
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
        NodeError::InputDropped(_) => status::INPUT_DROPPED,
        _ => status::FAILED,
    }
}

/// Run `body`, reporting a failure or panic as a status code and a message to `error`.
///
/// # Safety
/// `error` must be the host's sink for the call.
unsafe fn guarded(error: StrSink, body: impl FnOnce() -> Result<(), Failure>) -> u32 {
    let (code, message) = match catch_unwind(AssertUnwindSafe(body)) {
        Ok(Ok(())) => return status::OK,
        Ok(Err(failure)) => failure,
        Err(panic) => (status::PANIC, panic_message(&*panic)),
    };
    // Safety: forwarded from the caller.
    unsafe { error.write(&message) };
    code
}

/// The `create` entry point of plugin `P` (see [`super::CreateFn`]).
///
/// # Safety
/// The arguments must follow [`super::CreateFn`]'s contract.
pub unsafe fn create<P: Plugin + Default>(
    cell: &StableRuntimeCell,
    deps: &[LinkedDep],
    node: u32,
    instance: *mut u64,
    error: StrSink,
) -> u32 {
    // Safety: forwarded from the caller.
    unsafe {
        guarded(error, || {
            let out = instance
                .as_mut()
                .ok_or_else(|| (status::FAILED, "null instance pointer".to_string()))?;
            let runtime = cell
                .get_or_build::<P>(deps)
                .map_err(|err| (status::FAILED, format!("plugin registry: {err}")))?;
            let state = Box::new(runtime.instance(node)?);
            *out = Box::into_raw(state) as u64;
            Ok(())
        })
    }
}

/// The `invoke` entry point (see [`super::InvokeFn`]).
///
/// # Safety
/// The arguments must follow [`super::InvokeFn`]'s contract.
pub unsafe fn invoke(
    instance: u64,
    inputs: *const StableInput,
    len: usize,
    outputs: StableOutputSink,
    error: StrSink,
) -> u32 {
    // Safety: forwarded from the caller.
    unsafe {
        guarded(error, || {
            // Safety: a live handle from `create`, never used concurrently.
            let instance = (instance as *mut StableInstance)
                .as_mut()
                .ok_or_else(|| (status::FAILED, "null instance".to_string()))?;
            let inputs = if len == 0 || inputs.is_null() {
                &[][..]
            } else {
                // Safety: the host passes `len` inputs.
                std::slice::from_raw_parts(inputs, len)
            };
            instance.invoke(inputs, outputs)
        })
    }
}

/// The `release` entry point (see [`super::ReleaseFn`]).
///
/// # Safety
/// `instance` must be a live handle from [`create`], not used afterwards.
pub unsafe fn release(instance: u64) {
    let instance = instance as *mut StableInstance;
    if instance.is_null() {
        return;
    }
    let _ = catch_unwind(AssertUnwindSafe(|| {
        // Safety: guaranteed by the caller.
        drop(unsafe { Box::from_raw(instance) });
    }));
}

/// The `unload` entry point (see [`super::UnloadFn`]).
pub fn unload(cell: &StableRuntimeCell) {
    let _ = catch_unwind(AssertUnwindSafe(|| {
        let runtime = cell.0.lock().unwrap_or_else(|err| err.into_inner()).take();
        drop(runtime);
    }));
}
