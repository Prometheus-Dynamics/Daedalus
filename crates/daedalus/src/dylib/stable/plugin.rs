//! Plugin side of the stable handler path: the registry instances run nodes from, input
//! decoding and output encoding.

use std::panic::{AssertUnwindSafe, catch_unwind};
use std::sync::{Arc, Mutex};

use super::services::{self, HostServices, PluginSink};
use super::value::{Arena, StableValue, to_value};
use super::{NO_FANIN, StableInput, StableOutputSink, builtin, pod, status};
use crate::bytes::{SHARED_BYTES_KEY, SharedBytes};
use crate::data::model::{TypeExpr, Value, ValueType};
use crate::dylib::__support::LinkedDep;
use crate::dylib::{StrSink, extract};
use crate::runtime::executor::{CorrelatedPayload, NodeHandler, panic_message};
use crate::runtime::handler_registry::HandlerRegistry;
use crate::runtime::handles::PortId;
use crate::runtime::host_bridge::ValueSerializerMap;
use crate::runtime::io::{ConstCoercerMap, NodeConstInputs, NodeIo, NodeIoEnv};
use crate::runtime::plugins::{Plugin, StableCodec};
use crate::runtime::{ExecutionContext, NodeError, RuntimeNode, StateStore, TypeIndex};
use crate::transport::{LocalCodec, Payload, PayloadSlot, PodCodec, PodValue, Residency, TypeKey};

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

pub(super) type Failure = (u32, String);

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
    /// A `StablePod` struct port (`T` or `Vec<T>`): crosses as a typed byte view.
    pod: Option<PodCodec>,
    /// A `Local<T>` port: crosses as an opaque handle of this build.
    local: Option<LocalCodec>,
}

/// One host node instance: its node, execution context (and with it node state), the port ids
/// its calls resolved and what its calls reuse. The host never runs one instance concurrently.
struct StableInstance {
    runtime: Arc<StableRuntime>,
    node: usize,
    ctx: ExecutionContext,
    /// Indexed fan-in port ids (`in3`), built on first use.
    fanin: Vec<(u32, u32, PortId)>,
    /// The last call's outputs and their port indices, position by position.
    outputs: Vec<(PortId, u32)>,
    /// The handler io's environment, shared by every call.
    env: Arc<NodeIoEnv>,
    /// The decoded inputs' payload allocations, position by position (released after a call).
    slots: Vec<PayloadSlot>,
    /// What the encoded outputs borrow; cleared after a call.
    arena: Arena,
    /// The const inputs the host set ([`set_consts_entry`]), decoded once and shared by every
    /// call's io; `None` when there are none.
    consts: Option<Arc<NodeConstInputs>>,
    /// The host's metric sink ([`set_services_entry`]); `None` without one.
    sink: Option<PluginSink>,
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
                pod: codec.as_ref().and_then(|codec| codec.pod),
                local: codec.as_ref().and_then(|codec| codec.local),
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
        let env = NodeIo::empty()
            .with_const_coercers(Some(self.const_coercers.clone()))
            .with_type_index(Some(self.types.clone()))
            .shared_env()
            .expect("set above");
        Ok(StableInstance {
            runtime: self.clone(),
            node: index as usize,
            ctx: ExecutionContext::detached(StateStore::default(), node.state_id.clone()),
            fanin: Vec::new(),
            outputs: Vec::new(),
            env,
            slots: Vec::new(),
            arena: Arena::default(),
            consts: None,
            sink: None,
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
        if let Some(handle) = port.local.as_ref().and_then(|codec| (codec.to_handle)(any)) {
            // This build's object, lent by handle: the host carries it without reading it.
            return Ok(arena.lend(handle, Residency::Cpu, true));
        }
        if let Some(value) = any.downcast_ref::<Value>() {
            return Ok(arena.encode(value));
        }
        if let Some(value) = port
            .pod
            .as_ref()
            .and_then(|codec| pod::encode(arena, codec, any))
        {
            return Ok(value);
        }
        if let Some(value) = any.downcast_ref::<PodValue>() {
            return Ok(pod::encode_value(arena, value));
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
    /// Forward the custom metrics the call recorded to the host's telemetry. Nothing is built
    /// when nothing was recorded.
    fn forward_metrics(&self) {
        let Some(sink) = self.sink else {
            return;
        };
        let metrics = self.ctx.state.drain_node_custom_metrics(&self.ctx.node_id);
        for (name, value) in &metrics {
            // Safety: the host keeps its sink for this instance's life, and this call runs under it.
            unsafe { services::forward(sink, name, value) };
        }
    }

    /// Deliver the host services: the node's and graph's metadata become this instance's context
    /// (its state starts empty, so this runs once, right after create), and metrics go to the
    /// host's sink from now on.
    ///
    /// # Safety
    /// The services must be the host's, valid for the call; the sink must outlive the instance.
    unsafe fn set_services(&mut self, services: &HostServices) -> Result<(), Failure> {
        // Safety: forwarded from the caller.
        let metadata =
            unsafe { to_value(&services.metadata) }.map_err(|message| (status::FAILED, message))?;
        let graph = unsafe { to_value(&services.graph_metadata) }
            .map_err(|message| (status::FAILED, message))?;
        self.ctx = ExecutionContext::new(
            StateStore::default(),
            self.ctx.node_id.clone(),
            Arc::new(services::metadata_map(metadata)),
            Arc::new(services::metadata_map(graph)),
            Arc::new(crate::runtime::capabilities::CapabilityRegistry::new()),
        );
        self.sink = Some(PluginSink {
            sink: services.sink,
            record: services.record_metric,
        });
        Ok(())
    }

    /// Replace the instance's const inputs with `inputs` (decoded here, so their strings and
    /// bytes are copied once); an empty list clears them.
    ///
    /// # Safety
    /// Every pointer in `inputs` must be valid for the call.
    unsafe fn set_consts(&mut self, inputs: &[StableInput]) -> Result<(), Failure> {
        if inputs.len() > MAX_SHARED_CONSTS {
            return Err((
                status::FAILED,
                format!(
                    "{} const inputs; at most {MAX_SHARED_CONSTS} are shared",
                    inputs.len()
                ),
            ));
        }
        let runtime = self.runtime.clone();
        let node = &runtime.nodes[self.node];
        let mut decoded = Vec::with_capacity(inputs.len());
        for input in inputs {
            // A fresh slot per const: the decoded payload keeps its own allocation.
            let mut slot = PayloadSlot::default();
            // Safety: forwarded from the caller.
            let port = unsafe { decode_port(node, &mut self.fanin, &mut slot, input) }
                .map_err(|message| (status::INVALID_INPUT, message))?;
            decoded.push(port);
        }
        self.consts = (!decoded.is_empty()).then(|| Arc::new(decoded));
        Ok(())
    }

    /// # Safety
    /// Every pointer in `inputs` must be valid for the call, and `outputs` must be the host's
    /// sink for it.
    unsafe fn invoke(
        &mut self,
        inputs: &[StableInput],
        outputs: StableOutputSink,
    ) -> Result<(), Failure> {
        // Safety: forwarded from the caller.
        let result = catch_unwind(AssertUnwindSafe(|| unsafe { self.run(inputs, outputs) }));
        // Nothing the call decoded or encoded outlives it, a panic's included; the buffers stay
        // for the next.
        self.arena.clear();
        self.slots.iter_mut().for_each(PayloadSlot::release);
        result.unwrap_or_else(|panic| std::panic::resume_unwind(panic))
    }

    /// # Safety
    /// As [`Self::invoke`].
    unsafe fn run(
        &mut self,
        inputs: &[StableInput],
        outputs: StableOutputSink,
    ) -> Result<(), Failure> {
        let failed = |message: String| (status::FAILED, message);
        let runtime = &*self.runtime;
        let node = &runtime.nodes[self.node];
        // Decoded straight into the `NodeIo`'s pooled port buffer, each payload in its
        // position's slot; the first failure stops it.
        let mut failure = None;
        let fanin = &mut self.fanin;
        if self.slots.len() < inputs.len() {
            self.slots.resize_with(inputs.len(), PayloadSlot::default);
        }
        let ports = inputs
            .iter()
            .zip(self.slots.iter_mut())
            .map_while(|(input, slot)| {
                // Safety: forwarded from the caller.
                match unsafe { decode_port(node, fanin, slot, input) } {
                    Ok(port) => Some(port),
                    Err(message) => {
                        failure = Some((status::INVALID_INPUT, message));
                        None
                    }
                }
            });
        let mut io = NodeIo::from_inputs(ports)
            .with_env(self.env.clone())
            .with_shared_consts(self.consts.clone());
        if let Some(failure) = failure {
            return Err(failure);
        }
        let ran = runtime.handlers.run(&node.node, &self.ctx, &mut io);
        self.forward_metrics();
        ran.map_err(|error| (error_status(&error), error.to_string()))?;

        let push = outputs
            .push
            .ok_or_else(|| failed("no output sink".into()))?;
        for (position, (port, payload)) in io.drain_outputs().enumerate() {
            let index =
                output_index(&mut self.outputs, node, position, &port).ok_or_else(|| {
                    failed(format!(
                        "node `{}` pushed undeclared output `{port}`",
                        node.node.id
                    ))
                })?;
            let value = runtime
                .encode_output(
                    &node.outputs[index as usize],
                    &payload.inner,
                    &mut self.arena,
                )
                .map_err(|err| failed(format!("output `{port}` of `{}`: {err}", node.node.id)))?;
            // Safety: the host's sink for this call; the value borrows `payload` and the arena.
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
    slot: &mut PayloadSlot,
    input: &StableInput,
) -> Result<(PortId, CorrelatedPayload), String> {
    let port = node
        .inputs
        .get(input.port as usize)
        .ok_or_else(|| format!("node `{}` has no input {}", node.node.id, input.port))?;
    // Safety: forwarded from the caller.
    let payload = unsafe { decode_input(port, slot, &input.value) }
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
unsafe fn decode_input(
    port: &StablePort,
    slot: &mut PayloadSlot,
    value: &StableValue,
) -> Result<Payload, String> {
    let key = port.key.clone();
    // Safety (throughout): forwarded from the caller.
    if let Some(handle) = unsafe { value.as_handle() } {
        if port.shared_bytes {
            // The host's buffer, read in place; the host keeps its own code loaded.
            let bytes = SharedBytes::from_handle(handle.clone(), None)
                .map_err(|err| format!("expected `daedalus:bytes`: {err:?}"))?;
            return Ok(slot.owned(key, bytes));
        }
        if let Some(codec) = &port.local {
            // The same object this build lent, checked to be this build's `Local<T>`.
            return (codec.decode)(slot, key, handle).map_err(|err| err.to_string());
        }
        return Ok(slot.foreign(key, handle.clone(), value.residency(), false));
    }
    if port.shared_bytes && value.tag == super::tag::BYTES {
        // Borrowed bytes (a host without a handle for them): copied, as nothing owns them.
        return Ok(slot.owned(key, SharedBytes::from(unsafe { value.as_bytes() })));
    }
    if value.tag == super::tag::POD {
        return match &port.pod {
            Some(codec) => unsafe { pod::decode(codec, key, slot, value) },
            None => unsafe { pod::decode_value(key, slot, value) },
        };
    }
    if let Some(scalar) = port.scalar {
        return unsafe { builtin::decode(scalar, key, slot, value) };
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
pub(super) unsafe fn guarded(error: StrSink, body: impl FnOnce() -> Result<(), Failure>) -> u32 {
    let (code, message) = match catch_unwind(AssertUnwindSafe(body)) {
        Ok(Ok(())) => return status::OK,
        Ok(Err(failure)) => failure,
        Err(panic) => (status::PANIC, panic_message(&*panic)),
    };
    error.write(&message);
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
    // No message goes back from a release, so the `lemnos-abi` guard (a panic is dropped) does.
    lemnos_abi::guard_or((), || {
        // Safety: guaranteed by the caller.
        drop(unsafe { Box::from_raw(instance) });
    });
}

/// Most const inputs one instance shares (see `NodeIo`'s shared consts).
const MAX_SHARED_CONSTS: usize = 64;

/// The `set_consts` entry point (see [`super::SetConstsFn`]).
///
/// # Safety
/// The arguments must follow [`super::SetConstsFn`]'s contract.
pub unsafe extern "C" fn set_consts_entry(
    instance: u64,
    consts: *const StableInput,
    len: usize,
    error: StrSink,
) -> u32 {
    // Safety: forwarded from the caller.
    unsafe {
        guarded(error, || {
            // Safety: a live handle from `create`, never used concurrently.
            let instance = (instance as *mut StableInstance)
                .as_mut()
                .ok_or_else(|| (status::FAILED, "null instance".to_string()))?;
            let inputs = if len == 0 || consts.is_null() {
                &[][..]
            } else {
                // Safety: the host passes `len` inputs.
                std::slice::from_raw_parts(consts, len)
            };
            // Safety: the host's consts are valid for the call.
            instance.set_consts(inputs)
        })
    }
}

/// The `set_services` entry point (see [`super::services::SetServicesFn`]).
///
/// # Safety
/// The arguments must follow [`super::services::SetServicesFn`]'s contract.
pub unsafe extern "C" fn set_services_entry(
    instance: u64,
    services: *const HostServices,
    error: StrSink,
) -> u32 {
    // Safety: forwarded from the caller.
    unsafe {
        guarded(error, || {
            // Safety: a live handle from `create`, never used concurrently.
            let instance = (instance as *mut StableInstance)
                .as_mut()
                .ok_or_else(|| (status::FAILED, "null instance".to_string()))?;
            let services = services
                .as_ref()
                .ok_or_else(|| (status::FAILED, "null services".to_string()))?;
            // Safety: the host's services, valid for the call.
            instance.set_services(services)
        })
    }
}

/// The `unload` entry point (see [`super::UnloadFn`]).
pub fn unload(cell: &StableRuntimeCell) {
    lemnos_abi::guard_or((), || {
        let runtime = cell.0.lock().unwrap_or_else(|err| err.into_inner()).take();
        drop(runtime);
    });
}
