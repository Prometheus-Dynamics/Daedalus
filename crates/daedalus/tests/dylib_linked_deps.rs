//! `export_plugin!(.., deps [..])` in-process (no dlopen): the descriptor introspects the plugin
//! with its linked dependency installed first, and once the host installed that dependency the
//! plugin registers and runs against the dependency's types.

use std::ffi::c_void;
use std::sync::Arc;

use daedalus::engine::{Engine, EngineConfig};
use daedalus::macros::{node, plugin};
use daedalus::runtime::NodeError;
use daedalus::runtime::plugins::RegistryPluginExt;
use daedalus::transport::{Payload, TypeKey};
use daedalus::{PluginRegistry, PluginSchema, StrSink};
use daedalus_plugins_example_project::{Counter, ExampleProjectPlugin, LEASE_KEY, Lease};

#[node(id = "lease", inputs("counter"), outputs("lease"))]
fn lease(counter: &Counter) -> Result<Lease, NodeError> {
    Ok(Lease(counter.0 as u32))
}

#[node(id = "slot", inputs("lease"), outputs("slot"))]
fn slot(lease: &Lease) -> Result<i32, NodeError> {
    Ok(lease.0 as i32)
}

#[plugin(id = "linked_dependent", nodes(lease, slot))]
struct LinkedDependentPlugin;

// No `deps("example_rust")` on the plugin: linking adds it to the schema anyway.
daedalus::export_plugin!(LinkedDependentPlugin, deps[ExampleProjectPlugin]);

unsafe extern "C" fn capture(ctx: *mut c_void, ptr: *const u8, len: usize) {
    let slot = unsafe { &mut *ctx.cast::<Option<String>>() };
    let bytes = unsafe { std::slice::from_raw_parts(ptr, len) };
    *slot = Some(String::from_utf8_lossy(bytes).into_owned());
}

fn sink(slot: &mut Option<String>) -> StrSink {
    StrSink {
        ctx: (slot as *mut Option<String>).cast(),
        write: Some(capture),
    }
}

#[test]
fn linked_dependency_resolves_keys_and_the_plugin_runs_once_the_host_installed_it() {
    let descriptor = daedalus_plugin_descriptor();
    let mut json = None;
    assert!(unsafe { (descriptor.schema)(sink(&mut json)) }, "{json:?}");
    let schema: PluginSchema = serde_json::from_str(&json.unwrap()).unwrap();
    assert_eq!(schema.plugin.name, "linked_dependent");
    assert_eq!(schema.dependencies, ["example_rust"]);
    let slot_decl = schema
        .nodes
        .iter()
        .find(|n| n.id.ends_with("slot"))
        .unwrap();
    assert_eq!(
        slot_decl.inputs[0].type_key,
        Some(TypeKey::new(LEASE_KEY)),
        "the linked dependency's mapping"
    );
    assert!(!schema.plugin.metadata.contains_key("external_types"));

    let mut registry = PluginRegistry::new();
    registry
        .install_plugin(&ExampleProjectPlugin::default())
        .unwrap();
    let registry_ptr = (&mut registry as *mut PluginRegistry).cast::<c_void>();
    let mut message = None;
    assert!(
        unsafe { (descriptor.register)(registry_ptr, sink(&mut message)) },
        "{message:?}"
    );

    let graph = registry
        .graph_builder()
        .unwrap()
        .input_typed::<Counter>("counter")
        .unwrap()
        .node_id("linked_dependent:lease", "lease")
        .node_id("linked_dependent:slot", "slot")
        .connect("counter", "lease.counter")
        .connect("lease.lease", "slot.lease")
        .connect("slot.slot", "slot")
        .build();
    let mut host = Engine::new(EngineConfig::default())
        .unwrap()
        .compile_registry(&registry, graph)
        .unwrap();
    host.push_payload(
        "counter",
        Payload::shared("example:counter", Arc::new(Counter(5))),
    );
    host.tick().unwrap();
    assert_eq!(host.take::<i32>("slot"), Some(5));
}
