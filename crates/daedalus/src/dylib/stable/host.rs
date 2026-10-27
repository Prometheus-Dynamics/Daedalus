//! Host side of the stable handler path: installing a plugin from its schema, with handlers that
//! call the plugin's current generation through a [`PluginSlot`].

use std::any::Any;
use std::ffi::c_void;
use std::panic::{AssertUnwindSafe, catch_unwind};
use std::sync::atomic::{AtomicU64, Ordering};
use std::sync::{Arc, Mutex, RwLock, Weak};

use super::value::{Arena, StableValue, to_value};
use super::{NO_FANIN, StableHandlers, StableInput, StableOutputSink, builtin, status, tag};
use crate::bytes::{SHARED_BYTES_KEY, SharedBytes};
use crate::data::model::{Value, ValueType};
use crate::dylib::library::LibraryHandle;
use crate::dylib::{PluginSchema, StrSink, loader};
use crate::registry::capability::PluginManifest;
use crate::runtime::handles::PortId;
use crate::runtime::host_bridge::ValueSerializerMap;
use crate::runtime::io::NodeIo;
use crate::runtime::plugins::{Plugin, PluginError, PluginInstallContext, PluginResult};
use crate::runtime::{ExecutionContext, NodeError};
use crate::transport::{
    ForeignInterface, ForeignInterfaceInfo, FrameInterface, Payload, Residency, TypeKey,
    frame_handle_from_v2, is_frame_v2,
};

/// One loaded build of a plugin: its stable entry points and the library they live in. Node
/// instances and buffers the build produced hold it; when the last one drops, the plugin's
/// runtime is unloaded and the library closed (unless pinned).
pub struct Generation {
    number: u64,
    handlers: StableHandlers,
    /// The schema index, in this build, of each node the slot's installs registered.
    nodes: Vec<u32>,
    /// `None` for a descriptor of this process (tests).
    library: Option<Arc<LibraryHandle>>,
}

impl Generation {
    pub fn new(
        number: u64,
        handlers: StableHandlers,
        nodes: Vec<u32>,
        library: Option<Arc<LibraryHandle>>,
    ) -> Self {
        Self {
            number,
            handlers,
            nodes,
            library,
        }
    }

    /// Never close this build's library: the host keeps something that points into it.
    fn pin(&self) {
        if let Some(library) = &self.library {
            library.pin();
        }
    }
}

impl Drop for Generation {
    fn drop(&mut self) {
        // Safety: the plugin's entry point; every instance of this generation was released
        // (each holds the generation), and the library is still open (dropped after this).
        unsafe { (self.handlers.unload)() };
    }
}

/// What a stable install's handlers call: a plugin's current [`Generation`], swapped in place by
/// a reload. Node instances bound to an older generation are reset on their next call.
pub struct PluginSlot {
    current: RwLock<Arc<Generation>>,
    number: AtomicU64,
    /// Every generation published, to report the live ones.
    published: Mutex<Vec<Weak<Generation>>>,
    /// Each installed node's id and ports (JSON), which a reload must keep.
    interface: Vec<(String, String)>,
}

impl PluginSlot {
    /// A slot for `schema`'s nodes, running `generation` (whose `nodes` index `schema.nodes`).
    pub fn new(schema: &PluginSchema, generation: Generation) -> Self {
        let number = generation.number;
        let generation = Arc::new(generation);
        Self {
            published: Mutex::new(vec![Arc::downgrade(&generation)]),
            current: RwLock::new(generation),
            number: AtomicU64::new(number),
            interface: schema.nodes.iter().map(node_interface).collect(),
        }
    }

    pub fn current(&self) -> Arc<Generation> {
        self.current
            .read()
            .unwrap_or_else(|err| err.into_inner())
            .clone()
    }

    /// The current generation's number (one atomic load).
    fn number(&self) -> u64 {
        self.number.load(Ordering::Acquire)
    }

    /// The schema index in `schema` of each installed node, or what changed in its interface.
    pub fn node_indices(&self, schema: &PluginSchema) -> Result<Vec<u32>, Vec<String>> {
        let mut changes = Vec::new();
        let indices = self
            .interface
            .iter()
            .filter_map(|(id, ports)| {
                let Some(index) = schema.nodes.iter().position(|node| &node.id == id) else {
                    changes.push(format!("node `{id}` was removed"));
                    return None;
                };
                if node_interface(&schema.nodes[index]).1 != *ports {
                    changes.push(format!("node `{id}` changed its ports"));
                }
                Some(index as u32)
            })
            .collect();
        if changes.is_empty() {
            Ok(indices)
        } else {
            Err(changes)
        }
    }

    /// Make `generation` current: calls already running finish on the old one, and every node
    /// instance's next call resets it onto the new one.
    pub fn publish(&self, generation: Generation) {
        let number = generation.number;
        let generation = Arc::new(generation);
        {
            let mut published = self.published.lock().unwrap_or_else(|err| err.into_inner());
            published.retain(|generation| generation.strong_count() > 0);
            published.push(Arc::downgrade(&generation));
        }
        let old = std::mem::replace(
            &mut *self.current.write().unwrap_or_else(|err| err.into_inner()),
            generation,
        );
        self.number.store(number, Ordering::Release);
        drop(old);
    }

    /// The numbers of the generations still loaded (current first).
    pub fn live_generations(&self) -> Vec<u64> {
        let current = self.number();
        let mut live: Vec<u64> = self
            .published
            .lock()
            .unwrap_or_else(|err| err.into_inner())
            .iter()
            .filter_map(Weak::upgrade)
            .map(|generation| generation.number)
            .filter(|number| *number != current)
            .collect();
        live.insert(0, current);
        live
    }
}

fn node_interface(node: &daedalus_ffi_host::core::NodeSchema) -> (String, String) {
    let ports = serde_json::to_string(&(&node.inputs, &node.outputs)).unwrap_or_default();
    (node.id.clone(), ports)
}

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
}

// Safety: `inputs` holds pointers only during a call (it is empty between calls), and the
// plugin handle is only used by the call holding the instance.
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
                Box::new(self.create()?)
            }
        };
        let mut instance = instance;
        let result = self.call(&mut instance, io);
        ctx.set_node_state(instance);
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
        })
    }

    fn call(&self, instance: &mut Instance, io: &mut NodeIo) -> Result<(), NodeError> {
        let Instance {
            generation,
            handle,
            resolved,
            inputs,
            pushed,
        } = instance;
        let mut arena = Arena::default();
        for (position, (port, payload)) in io.inputs().enumerate() {
            let (index, fanin) = self.resolve_input(resolved, position, port)?;
            inputs.push(StableInput {
                port: index,
                fanin,
                value: self.encode_input(port.as_str(), &payload.inner, &mut arena)?,
            });
        }
        let mut outputs = Outputs {
            node: self,
            generation,
            pushed,
            error: None,
        };
        let sink = StableOutputSink {
            ctx: (&mut outputs as *mut Outputs<'_>).cast::<c_void>(),
            push: Some(push_output),
        };
        let mut message: Option<String> = None;
        // Safety: the inputs borrow `io` and `arena`, both alive for the call; the sinks point
        // to locals that outlive it; the handle is this instance's.
        let code = unsafe {
            (generation.handlers.invoke)(
                *handle,
                inputs.as_ptr(),
                inputs.len(),
                sink,
                string_sink(&mut message),
            )
        };
        inputs.clear();
        drop(arena);
        let Outputs { error, .. } = outputs;
        if code == status::OK {
            for (port, payload) in pushed.drain(..) {
                io.push_payload(self.outputs[port as usize].id.clone(), payload);
            }
            return Ok(());
        }
        pushed.clear();
        Err(self.error(code, error.or(message)))
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
        payload: &Payload,
        arena: &mut Arena,
    ) -> Result<StableValue, NodeError> {
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
                if value.from_sender() {
                    // The handle's code is the plugin's and nothing tracks the payload.
                    self.generation.pin();
                }
                // A frame from a plugin built against frame v2 is read through v3.
                Payload::foreign(key, frame_handle_from_v2(handle.clone()), value.residency())
            }
        } else if port.shared_bytes && value.tag == tag::BYTES {
            Payload::owned(key, SharedBytes::from(unsafe { value.as_bytes() }))
        } else if let Some(scalar) = port.scalar {
            unsafe { builtin::decode(scalar, key, value) }?
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
