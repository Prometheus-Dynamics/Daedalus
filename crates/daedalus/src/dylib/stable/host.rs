//! Host side of the stable handler path: installing a plugin from its schema, with handlers that
//! call the plugin's `invoke`.

use std::ffi::c_void;
use std::panic::{AssertUnwindSafe, catch_unwind};
use std::sync::Arc;
use std::sync::atomic::{AtomicU64, Ordering};

use super::value::{Arena, StableValue, str_at, to_value};
use super::{ReleaseFn, StableHandlers, StableInput, StableOutputSink, builtin, status};
use crate::data::model::{Value, ValueType};
use crate::dylib::{PluginSchema, StrSink, loader};
use crate::registry::capability::PluginManifest;
use crate::runtime::handles::PortId;
use crate::runtime::host_bridge::ValueSerializerMap;
use crate::runtime::io::NodeIo;
use crate::runtime::plugins::{Plugin, PluginError, PluginInstallContext, PluginResult};
use crate::runtime::{ExecutionContext, NodeError};
use crate::transport::{ForeignInterfaceInfo, Payload, TypeKey};

/// A plugin installed through its schema and stable entry points.
pub struct StablePlugin<'a> {
    pub id: &'static str,
    pub schema: &'a PluginSchema,
    pub handlers: StableHandlers,
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
        for interface in self.foreign_interfaces {
            ctx.register_foreign_interface_info(*interface)?;
        }
        for (index, node) in self.schema.nodes.iter().enumerate() {
            let decl = daedalus_ffi_host::node_decl_from_schema(node)
                .map_err(|err| failed(format!("node `{}`: {err}", node.id)))?;
            let outputs = node
                .outputs
                .iter()
                .map(|port| {
                    let key = port
                        .type_key
                        .clone()
                        .unwrap_or_else(|| crate::registry::typeexpr_transport_key(&port.ty));
                    OutputPort {
                        id: PortId::new(port.name.clone()),
                        name: port.name.clone(),
                        scalar: builtin::scalar_of(&port.ty, &key),
                        key,
                    }
                })
                .collect();
            let host_node = Arc::new(HostNode {
                plugin: self.id,
                node: node.id.clone(),
                index: u32::try_from(index).map_err(|err| failed(err.to_string()))?,
                handlers: self.handlers,
                outputs,
                serializers: ctx.value_serializers.clone(),
            });
            ctx.register_node_decl(decl)?;
            ctx.handlers
                .try_on(&node.id, move |_node, ctx, io| host_node.run(ctx, io))
                .map_err(|err| failed(err.to_string()))?;
        }
        Ok(())
    }
}

struct OutputPort {
    name: String,
    id: PortId,
    key: TypeKey,
    scalar: Option<ValueType>,
}

struct HostNode {
    plugin: &'static str,
    node: String,
    index: u32,
    handlers: StableHandlers,
    outputs: Vec<OutputPort>,
    serializers: ValueSerializerMap,
}

/// A host node instance's plugin-side state, released when the host drops the node's state.
struct Instance {
    id: u64,
    release: ReleaseFn,
}

impl Instance {
    fn new(release: ReleaseFn) -> Self {
        static NEXT: AtomicU64 = AtomicU64::new(1);
        Self {
            id: NEXT.fetch_add(1, Ordering::Relaxed),
            release,
        }
    }
}

impl Drop for Instance {
    fn drop(&mut self) {
        // Safety: `release` is the plugin's entry point; plugin libraries are never unloaded.
        unsafe { (self.release)(self.id) };
    }
}

impl HostNode {
    fn run(&self, ctx: &ExecutionContext, io: &mut NodeIo) -> Result<(), NodeError> {
        let instance = ctx
            .state
            .take_node_state::<Instance>(&ctx.node_id)
            .unwrap_or_else(|| Instance::new(self.handlers.release));
        let result = self.call(instance.id, io);
        ctx.state.set_node_state(&ctx.node_id, instance);
        result
    }

    fn call(&self, instance: u64, io: &mut NodeIo) -> Result<(), NodeError> {
        thread_local! {
            /// Input lists reused across calls (cleared after each).
            static INPUTS: std::cell::Cell<Vec<StableInput>> = const { std::cell::Cell::new(Vec::new()) };
        }
        let mut inputs = INPUTS.take();
        let result = self.call_with(instance, io, &mut inputs);
        inputs.clear();
        INPUTS.set(inputs);
        result
    }

    fn call_with(
        &self,
        instance: u64,
        io: &mut NodeIo,
        inputs: &mut Vec<StableInput>,
    ) -> Result<(), NodeError> {
        let mut arena = Arena::default();
        for (port, payload) in io.inputs() {
            let port = port.as_str();
            inputs.push(StableInput {
                port_ptr: port.as_ptr(),
                port_len: port.len(),
                value: self.encode_input(port, &payload.inner, &mut arena)?,
            });
        }
        let mut outputs = Outputs {
            node: self,
            pushed: Vec::new(),
            error: None,
        };
        let sink = StableOutputSink {
            ctx: (&mut outputs as *mut Outputs<'_>).cast::<c_void>(),
            push: Some(push_output),
        };
        let mut message: Option<String> = None;
        let error = StrSink {
            ctx: (&mut message as *mut Option<String>).cast(),
            write: Some(loader::write_string),
        };
        // Safety: the inputs borrow `io` and `arena`, both alive for the call; the sinks point
        // to locals that outlive it.
        let code = unsafe {
            (self.handlers.invoke)(
                self.index,
                instance,
                inputs.as_ptr(),
                inputs.len(),
                sink,
                error,
            )
        };
        inputs.clear();
        drop(arena);
        let Outputs { pushed, error, .. } = outputs;
        if code == status::OK {
            for (port, payload) in pushed {
                io.push_payload(port, payload);
            }
            return Ok(());
        }
        let message = error
            .or(message)
            .unwrap_or_else(|| "no message".to_string());
        let message = format!("plugin `{}` node `{}`: {message}", self.plugin, self.node);
        Err(match code {
            status::INVALID_INPUT => NodeError::InvalidInput(message),
            status::BACKPRESSURE => NodeError::BackpressureDrop(message),
            status::PANIC => NodeError::Handler(format!("{message} (the handler panicked)")),
            _ => NodeError::Handler(message),
        })
    }

    fn encode_input(
        &self,
        port: &str,
        payload: &Payload,
        arena: &mut Arena,
    ) -> Result<StableValue, NodeError> {
        if let Some(handle) = payload.foreign_handle() {
            return Ok(StableValue::handle(handle, payload.residency()));
        }
        if let Some(any) = payload.value_any_sync() {
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
             stable plugin boundary; pass a builtin, a `Value`, a type with a registered value \
             serializer, or a foreign interface",
            self.plugin,
            self.node,
            payload.type_key(),
            payload.storage_rust_type_name().unwrap_or("bytes"),
        )))
    }
}

/// The output sink's state for one call.
struct Outputs<'a> {
    node: &'a HostNode,
    pushed: Vec<(PortId, Payload)>,
    error: Option<String>,
}

impl Outputs<'_> {
    /// # Safety
    /// Every pointer in `value` must be valid for the call.
    unsafe fn push(&mut self, name: &str, value: &StableValue) -> Result<(), String> {
        let port = self
            .node
            .outputs
            .iter()
            .find(|port| port.name == name)
            .ok_or_else(|| format!("undeclared output `{name}`"))?;
        let key = port.key.clone();
        // Safety (throughout): forwarded from the caller.
        let payload = if let Some(handle) = unsafe { value.as_handle() } {
            Payload::foreign(key, handle.clone(), value.residency())
        } else if let Some(scalar) = port.scalar {
            unsafe { builtin::decode(scalar, key, value) }?
        } else {
            Payload::owned(key, unsafe { to_value(value) }?)
        };
        self.pushed.push((port.id.clone(), payload));
        Ok(())
    }
}

unsafe extern "C" fn push_output(
    ctx: *mut c_void,
    port_ptr: *const u8,
    port_len: usize,
    value: *const StableValue,
) -> bool {
    // Safety: `ctx` is the `Outputs` of the running call; the plugin passes a port name and a
    // value valid for this call.
    let (outputs, value) = unsafe { (&mut *ctx.cast::<Outputs<'_>>(), value.as_ref()) };
    let result = catch_unwind(AssertUnwindSafe(|| {
        let value = value.ok_or("null output value")?;
        // Safety: as above.
        let name = unsafe { str_at(port_ptr, port_len) }?;
        // Safety: as above.
        unsafe { outputs.push(name, value) }.map_err(|err| format!("output `{name}`: {err}"))
    }));
    let error = match result {
        Ok(Ok(())) => return true,
        Ok(Err(error)) => error,
        Err(_) => "decoding an output panicked".to_string(),
    };
    outputs.error.get_or_insert(error);
    false
}
