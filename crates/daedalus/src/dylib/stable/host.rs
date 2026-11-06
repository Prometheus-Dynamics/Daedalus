//! Host side of the stable handler path: installing a plugin from its schema, with handlers that
//! call the plugin's current generation through a [`PluginSlot`].

use std::any::Any;
use std::ffi::c_void;
use std::panic::{AssertUnwindSafe, catch_unwind};
use std::sync::Arc;

use super::generation::{Generation, PluginSlot};
use super::services::{self, HostServices, HostSink};
use super::value::{Arena, StableValue, to_value};
use super::{NO_FANIN, StableInput, StableOutputSink, builtin, pod, status, tag};
use crate::bytes::{SHARED_BYTES_KEY, SharedBytes};
use crate::data::model::{Value, ValueType};
use crate::dylib::{PluginSchema, StrSink, loader};
use crate::registry::capability::PluginManifest;
use crate::runtime::handles::PortId;
use crate::runtime::host_bridge::ValueSerializerMap;
use crate::runtime::io::{NodeConstInputs, NodeIo};
use crate::runtime::plugins::{Plugin, PluginError, PluginInstallContext, PluginResult};
use crate::runtime::{ExecutionContext, NodeError};
use crate::transport::{
    ForeignInterface, ForeignInterfaceInfo, FrameInterface, LocalInterface, Payload, PayloadSlot,
    PodCodec, PodValue, Residency, TypeKey, frame_handle_from_v2, is_frame_v2,
};

/// The first [`super::STABLE_ABI_VERSION`] that reads [`tag::POD`] values.
const POD_VERSION: u32 = 5;

/// A plugin installed through its schema and stable entry points.
pub struct StablePlugin<'a> {
    pub id: &'static str,
    pub schema: &'a PluginSchema,
    pub slot: Arc<PluginSlot>,
    pub foreign_interfaces: &'a [ForeignInterfaceInfo],
}

impl Plugin for StablePlugin<'_> {
    fn id(&self) -> &'static str {
        self.id
    }

    fn manifest(&self) -> PluginManifest {
        let mut manifest = daedalus_ffi_host::plugin_manifest_from_schema(self.schema);
        // Nodes are discovered as they register. Boundary contracts describe Rust payload
        // layouts, and no Rust payload crosses this path.
        manifest.provided_nodes.clear();
        manifest.boundary_contracts.clear();
        manifest
    }

    fn install(&self, ctx: &mut PluginInstallContext<'_>) -> PluginResult<()> {
        let failed = |message: String| PluginError::Install { message };
        // A plugin built against `daedalus:frame` v2 is fed this host's v3 frames lent as v2
        // and its frames are read through v3, so the host's graph only ever sees v3.
        let frame_v2 = self.foreign_interfaces.iter().any(is_frame_v2);
        for interface in self.foreign_interfaces {
            let interface = match is_frame_v2(interface) {
                true => FrameInterface::info(),
                false => interface,
            };
            ctx.register_foreign_interface_info(*interface)?;
        }
        let pods: std::collections::BTreeMap<TypeKey, PodCodec> = self
            .schema
            .nodes
            .iter()
            .flat_map(|node| node.inputs.iter().chain(&node.outputs))
            .filter_map(|port| {
                let key = port.type_key.clone()?;
                Some((key.clone(), *ctx.stable_pod(&key)?))
            })
            .collect();
        let port = |port: &daedalus_ffi_host::core::WirePort| {
            let key = port
                .type_key
                .clone()
                .unwrap_or_else(|| crate::registry::typeexpr_transport_key(&port.ty));
            HostPort {
                id: PortId::new(port.name.clone()),
                name: port.name.clone(),
                scalar: builtin::scalar_of(&port.ty, &key),
                shared_bytes: key.as_str() == SHARED_BYTES_KEY,
                pod: pods.get(&key).copied(),
                key,
            }
        };
        let mut shared_bytes = false;
        for (index, node) in self.schema.nodes.iter().enumerate() {
            let decl = daedalus_ffi_host::node_decl_from_schema(node)
                .map_err(|err| failed(format!("node `{}`: {err}", node.id)))?;
            let host_node = Arc::new(HostNode {
                plugin: self.id,
                node: node.id.clone(),
                ordinal: index,
                slot: self.slot.clone(),
                inputs: node.inputs.iter().map(port).collect(),
                outputs: node.outputs.iter().map(port).collect(),
                serializers: ctx.value_serializers.clone(),
                frame_v2,
            });
            shared_bytes |= host_node
                .inputs
                .iter()
                .chain(&host_node.outputs)
                .any(|port| port.shared_bytes);
            ctx.register_node_decl(decl)?;
            ctx.handlers
                .try_on(&node.id, move |_node, ctx, io| host_node.run(ctx, io))
                .map_err(|err| failed(err.to_string()))?;
        }
        if shared_bytes {
            // So the host feeds and takes `SharedBytes` by type.
            ctx.register_boundary_type::<SharedBytes>(SHARED_BYTES_KEY)?;
        }
        Ok(())
    }
}

struct HostPort {
    name: String,
    id: PortId,
    key: TypeKey,
    scalar: Option<ValueType>,
    shared_bytes: bool,
    /// The host's `StablePod` type for the port's key (`PluginRegistry::register_stable_pod`).
    pod: Option<PodCodec>,
}

struct HostNode {
    plugin: &'static str,
    node: String,
    /// The node's index in the slot's interface.
    ordinal: usize,
    slot: Arc<PluginSlot>,
    inputs: Vec<HostPort>,
    outputs: Vec<HostPort>,
    serializers: ValueSerializerMap,
    /// The plugin reads frames through `daedalus:frame` v2.
    frame_v2: bool,
}

/// A host node instance's plugin-side state (node state included) in one generation, plus the
/// buffers its calls reuse. Released when the host drops the node's state or the instance
/// moves to a newer generation.
struct Instance {
    generation: Arc<Generation>,
    handle: u64,
    /// The last call's input ports and their (port, fan-in) indices, position by position.
    resolved: Vec<(PortId, u32, u32)>,
    /// Empty between calls.
    inputs: Vec<StableInput>,
    /// Empty between calls.
    pushed: Vec<(u32, Payload)>,
    /// What the inputs borrow; empty between calls.
    arena: Arena,
    /// Scalar outputs' payload allocations, by output port (a scalar holds no resource, so
    /// keeping the last one until the next call holds nothing back).
    scalars: Vec<PayloadSlot>,
    /// The const list the instance holds, as the host's `Arc` (so a patch, which copies on
    /// write, changes its identity); `None` when it holds none or the plugin sends consts per
    /// call.
    consts: Option<Arc<NodeConstInputs>>,
    /// What the plugin's record callback writes to; the instance's services point at it, so it
    /// outlives the instance's handle (released first, in `Drop`).
    services: Option<Box<HostSink>>,
}

// Safety: `inputs` and `arena` hold pointers only during a call (they are empty between calls),
// and the plugin handle is only used by the call holding the instance.
unsafe impl Send for Instance {}
// Safety: see above; shared references never touch the plugin state.
unsafe impl Sync for Instance {}

impl Drop for Instance {
    fn drop(&mut self) {
        // Safety: a live handle of this generation, whose library the instance keeps open.
        unsafe { (self.generation.handlers.release)(self.handle) };
    }
}

impl HostNode {
    fn run(&self, ctx: &ExecutionContext, io: &mut NodeIo) -> Result<(), NodeError> {
        let instance = match ctx.take_node_state::<Box<Instance>>() {
            Some(instance) if instance.generation.number == self.slot.number() => instance,
            // A new node, or one whose plugin was reloaded: its state resets (the old instance
            // is released on its own generation).
            stale => {
                drop(stale);
                let mut created = Box::new(self.create()?);
                self.deliver_services(&mut created, ctx)?;
                created
            }
        };
        let mut instance = instance;
        let result = self
            .sync_consts(&mut instance, io)
            .and_then(|()| self.call(&mut instance, io));
        // The metrics the plugin recorded during the call, into this node's telemetry.
        if let Some(sink) = instance.services.as_mut() {
            for (name, value) in sink.pending.drain(..) {
                ctx.state
                    .record_node_custom_metric(&ctx.node_id, name, value);
            }
        }
        ctx.set_node_state(instance);
        // A sweep point: close a build a reload retired once nothing can run it (one atomic load
        // when there is none).
        self.slot.sweep();
        result
    }

    fn create(&self) -> Result<Instance, NodeError> {
        let generation = self.slot.current();
        let mut handle = 0;
        let mut message: Option<String> = None;
        let index = generation.nodes[self.ordinal];
        // Safety: the generation's entry point, with a sink pointing to a local.
        let code =
            unsafe { (generation.handlers.create)(index, &mut handle, string_sink(&mut message)) };
        if code != status::OK {
            return Err(self.error(code, message));
        }
        Ok(Instance {
            generation,
            handle,
            resolved: Vec::new(),
            inputs: Vec::new(),
            pushed: Vec::new(),
            arena: Arena::default(),
            scalars: Vec::new(),
            consts: None,
            services: None,
        })
    }

    /// Deliver the host services to a new instance, once: its metric sink is the node's state
    /// store, and its context carries the node's and the graph's metadata.
    fn deliver_services(
        &self,
        instance: &mut Instance,
        ctx: &ExecutionContext,
    ) -> Result<(), NodeError> {
        let Some(set_services) = instance
            .generation
            .extension
            .map(|extension| extension.set_services)
        else {
            return Ok(());
        };
        let mut sink = Box::<HostSink>::default();
        // The encoded maps borrow these values' strings, so they live through the call.
        let (metadata, graph_metadata) = (
            services::metadata_value(&ctx.metadata),
            services::metadata_value(&ctx.graph_metadata),
        );
        let mut arena = Arena::default();
        let services = HostServices {
            // Mutable provenance: the record callback writes through this pointer.
            sink: (&mut *sink as *mut HostSink).cast::<c_void>(),
            record_metric: services::record_metric,
            metadata: arena.encode(&metadata),
            graph_metadata: arena.encode(&graph_metadata),
        };
        let mut message: Option<String> = None;
        // Safety: the generation's entry point; the services borrow `ctx`'s maps and `arena`, both
        // alive for the call; the sink is boxed, so it stays where the services point.
        let code = unsafe { set_services(instance.handle, &services, string_sink(&mut message)) };
        if code != status::OK {
            return Err(self.error(code, message));
        }
        instance.services = Some(sink);
        Ok(())
    }

    /// Make the instance hold the const inputs `io` has now (when its plugin takes them once):
    /// on create, and after a patch changed them. A call with the same list sends nothing.
    fn sync_consts(&self, instance: &mut Instance, io: &NodeIo) -> Result<(), NodeError> {
        let Some(set_consts) = instance
            .generation
            .extension
            .map(|extension| extension.set_consts)
        else {
            return Ok(());
        };
        let wanted = io.shared_consts();
        let same = match (&instance.consts, wanted) {
            (None, None) => true,
            (Some(held), Some(wanted)) => Arc::ptr_eq(held, wanted),
            _ => false,
        };
        if same {
            return Ok(());
        }
        let mut arena = Arena::default();
        let mut encoded = Vec::new();
        for (port, payload) in wanted.into_iter().flat_map(|consts| consts.iter()) {
            let (index, fanin) = self.lookup_input(port.as_str()).ok_or_else(|| {
                NodeError::InvalidInput(format!(
                    "plugin `{}` node `{}` has no input `{port}`",
                    self.plugin, self.node
                ))
            })?;
            encoded.push(StableInput {
                port: index,
                fanin,
                value: self.encode_input(
                    port.as_str(),
                    index,
                    &payload.inner,
                    &mut arena,
                    &instance.generation,
                )?,
            });
        }
        let mut message: Option<String> = None;
        // Safety: the generation's entry point; the encoded inputs borrow `io` and the arena,
        // both alive for the call; the sink points to a local.
        let code = unsafe {
            set_consts(
                instance.handle,
                encoded.as_ptr(),
                encoded.len(),
                string_sink(&mut message),
            )
        };
        if code != status::OK {
            return Err(self.error(code, message));
        }
        instance.consts = wanted.cloned();
        Ok(())
    }

    fn call(&self, instance: &mut Instance, io: &mut NodeIo) -> Result<(), NodeError> {
        let result = self.invoke(instance, io);
        // Whatever the outcome, nothing the call lent outlives it; the buffers stay.
        instance.inputs.clear();
        instance.arena.clear();
        instance.pushed.clear();
        result
    }

    fn invoke(&self, instance: &mut Instance, io: &mut NodeIo) -> Result<(), NodeError> {
        let Instance {
            generation,
            handle,
            resolved,
            inputs,
            pushed,
            arena,
            scalars,
            consts,
            services: _,
        } = instance;
        // The consts the instance holds are not sent again; otherwise every input is.
        if consts.is_some() {
            for (position, (port, payload)) in io.edge_inputs().iter().enumerate() {
                self.push_input(resolved, inputs, arena, position, port, payload, generation)?;
            }
        } else {
            for (position, (port, payload)) in io.inputs().enumerate() {
                self.push_input(resolved, inputs, arena, position, port, payload, generation)?;
            }
        }
        if scalars.len() < self.outputs.len() {
            scalars.resize_with(self.outputs.len(), PayloadSlot::default);
        }
        let mut outputs = Outputs {
            node: self,
            generation,
            pushed,
            scalars,
            error: None,
        };
        let sink = StableOutputSink {
            ctx: (&mut outputs as *mut Outputs<'_>).cast::<c_void>(),
            push: Some(push_output),
        };
        let mut message: Option<String> = None;
        // Safety: the inputs borrow `io` and the arena, both alive for the call; the sinks
        // point to locals that outlive it; the handle is this instance's.
        let code = unsafe {
            (generation.handlers.invoke)(
                *handle,
                inputs.as_ptr(),
                inputs.len(),
                sink,
                string_sink(&mut message),
            )
        };
        let Outputs { error, .. } = outputs;
        if code == status::OK {
            for (port, payload) in pushed.drain(..) {
                io.push_payload(self.outputs[port as usize].id.clone(), payload);
            }
            return Ok(());
        }
        Err(self.error(code, error.or(message)))
    }

    /// Encode one input of a call at `position` onto `inputs`, for a call on `generation`.
    #[allow(clippy::too_many_arguments)]
    fn push_input(
        &self,
        resolved: &mut Vec<(PortId, u32, u32)>,
        inputs: &mut Vec<StableInput>,
        arena: &mut Arena,
        position: usize,
        port: &PortId,
        payload: &crate::runtime::executor::CorrelatedPayload,
        generation: &Arc<Generation>,
    ) -> Result<(), NodeError> {
        let (index, fanin) = self.resolve_input(resolved, position, port)?;
        inputs.push(StableInput {
            port: index,
            fanin,
            value: self.encode_input(port.as_str(), index, &payload.inner, arena, generation)?,
        });
        Ok(())
    }

    fn error(&self, code: u32, message: Option<String>) -> NodeError {
        let message = message.unwrap_or_else(|| "no message".to_string());
        let message = format!("plugin `{}` node `{}`: {message}", self.plugin, self.node);
        match code {
            status::INVALID_INPUT => NodeError::InvalidInput(message),
            status::BACKPRESSURE => NodeError::BackpressureDrop(message),
            status::INPUT_DROPPED => NodeError::InputDropped(message),
            status::PANIC => NodeError::Handler(format!("{message} (the handler panicked)")),
            _ => NodeError::Handler(message),
        }
    }

    /// The (port, fan-in) indices of input `port` at `position`: the cached ones when the same
    /// port came there last call, else a lookup by name (cached for the next call).
    fn resolve_input(
        &self,
        resolved: &mut Vec<(PortId, u32, u32)>,
        position: usize,
        port: &PortId,
    ) -> Result<(u32, u32), NodeError> {
        if let Some((cached, index, fanin)) = resolved.get(position)
            && cached == port
        {
            return Ok((*index, *fanin));
        }
        let (index, fanin) = self.lookup_input(port.as_str()).ok_or_else(|| {
            NodeError::InvalidInput(format!(
                "plugin `{}` node `{}` has no input `{port}`",
                self.plugin, self.node
            ))
        })?;
        resolved.truncate(position);
        resolved.push((port.clone(), index, fanin));
        Ok((index, fanin))
    }

    fn lookup_input(&self, name: &str) -> Option<(u32, u32)> {
        let find = |name: &str| self.inputs.iter().position(|port| port.name == name);
        if let Some(index) = find(name) {
            return Some((index as u32, NO_FANIN));
        }
        // Fan-in ports arrive indexed (`in0`, `in1`, ...) under the declared name.
        let declared = name.trim_end_matches(|c: char| c.is_ascii_digit());
        let fanin: u32 = name[declared.len()..].parse().ok()?;
        (fanin != NO_FANIN).then_some(())?;
        Some((find(declared)? as u32, fanin))
    }

    fn encode_input(
        &self,
        port: &str,
        index: u32,
        payload: &Payload,
        arena: &mut Arena,
        generation: &Arc<Generation>,
    ) -> Result<StableValue, NodeError> {
        let pods = generation.handlers.version >= POD_VERSION;
        if self.frame_v2
            && let Some(value) = arena.lend_frame_as_v2(payload)
        {
            return Ok(value);
        }
        if let Some(value) = arena.encode_foreign(payload) {
            return Ok(value);
        }
        if let Some(any) = payload.value_any_sync() {
            if let Some(bytes) = any.downcast_ref::<SharedBytes>() {
                // Lent, not copied: the plugin reads the host's buffer in place.
                return Ok(arena.lend(bytes.to_handle(), Residency::Cpu, !bytes.is_foreign()));
            }
            if let Some(value) = any.downcast_ref::<Value>() {
                return Ok(arena.encode(value));
            }
            if pods {
                let codec = self.inputs.get(index as usize).and_then(|port| port.pod);
                if let Some(value) = codec.and_then(|codec| pod::encode(arena, &codec, any)) {
                    return Ok(value);
                }
                if let Some(value) = any.downcast_ref::<PodValue>() {
                    return Ok(pod::encode_value(arena, value));
                }
            }
            if let Some(value) = builtin::encode(any) {
                return Ok(value);
            }
            let serialized = {
                let serializers = self.serializers.read();
                serializers
                    .get(&(*any).type_id())
                    .and_then(|serialize| serialize(any))
            };
            if let Some(value) = serialized {
                return Ok(arena.encode_owned(value));
            }
        }
        Err(NodeError::InvalidInput(format!(
            "input `{port}` of plugin `{}` node `{}` holds `{}` (`{}`), which cannot cross the \
             stable plugin boundary; pass a builtin, a `Value`, `SharedBytes`, a type with a \
             registered value serializer, or a foreign interface",
            self.plugin,
            self.node,
            payload.type_key(),
            payload.storage_rust_type_name().unwrap_or("bytes"),
        )))
    }
}

/// A sink writing into `slot`.
fn string_sink(slot: &mut Option<String>) -> StrSink {
    StrSink {
        ctx: (slot as *mut Option<String>).cast(),
        write: Some(loader::write_string),
    }
}

/// The output sink's state for one call.
struct Outputs<'a> {
    node: &'a HostNode,
    generation: &'a Arc<Generation>,
    pushed: &'a mut Vec<(u32, Payload)>,
    scalars: &'a mut [PayloadSlot],
    error: Option<String>,
}

impl Outputs<'_> {
    /// # Safety
    /// Every pointer in `value` must be valid for the call.
    unsafe fn push(&mut self, index: u32, value: &StableValue) -> Result<(), String> {
        let port = self
            .node
            .outputs
            .get(index as usize)
            .ok_or_else(|| format!("no output {index}"))?;
        let key = port.key.clone();
        // Safety (throughout): forwarded from the caller.
        let payload = if let Some(handle) = unsafe { value.as_handle() } {
            if port.shared_bytes {
                // Read in place; the buffer keeps this build loaded until it is dropped.
                let keepalive: Arc<dyn Any + Send + Sync> = self.generation.clone();
                let bytes = SharedBytes::from_handle(handle.clone(), Some(keepalive))
                    .map_err(|err| format!("expected `daedalus:bytes`: {err:?}"))?;
                Payload::owned(key, bytes)
            } else {
                // A plugin-local object of a build that counts them keeps the build loaded by
                // its count (the slot unloads the build once it is 0), so its payload is the plain
                // handle: nothing per object, and it goes wherever any handle goes. Any other
                // handle the plugin made runs its code with nothing tracking the payload.
                if value.from_sender() {
                    if self.generation.counts_objects() && handle.view::<LocalInterface>().is_ok() {
                        // Its last release may run on any host thread: the build's code stays
                        // mapped once it is retired (unloaded, never closed).
                        self.generation.lend_local();
                    } else {
                        self.generation.pin();
                    }
                }
                // A frame from a plugin built against frame v2 is read through v3.
                Payload::foreign(key, frame_handle_from_v2(handle.clone()), value.residency())
            }
        } else if value.tag == tag::POD {
            let slot = &mut self.scalars[index as usize];
            match &port.pod {
                Some(codec) => unsafe { pod::decode(codec, key, slot, value) }?,
                None => unsafe { pod::decode_value(key, slot, value) }?,
            }
        } else if port.shared_bytes && value.tag == tag::BYTES {
            Payload::owned(key, SharedBytes::from(unsafe { value.as_bytes() }))
        } else if let Some(scalar) = port.scalar {
            unsafe { builtin::decode(scalar, key, &mut self.scalars[index as usize], value) }?
        } else {
            Payload::owned(key, unsafe { to_value(value) }?)
        };
        self.pushed.push((index, payload));
        Ok(())
    }
}

unsafe extern "C" fn push_output(ctx: *mut c_void, port: u32, value: *const StableValue) -> bool {
    // Safety: `ctx` is the `Outputs` of the running call; the plugin passes a value valid for
    // this call.
    let (outputs, value) = unsafe { (&mut *ctx.cast::<Outputs<'_>>(), value.as_ref()) };
    let result = catch_unwind(AssertUnwindSafe(|| {
        let value = value.ok_or("null output value")?;
        // Safety: as above.
        unsafe { outputs.push(port, value) }.map_err(|err| {
            let name = outputs.node.outputs.get(port as usize);
            format!("output `{}`: {err}", name.map_or("?", |port| &port.name))
        })
    }));
    let error = match result {
        Ok(Ok(())) => return true,
        Ok(Err(error)) => error,
        Err(_) => "decoding an output panicked".to_string(),
    };
    outputs.error.get_or_insert(error);
    false
}
