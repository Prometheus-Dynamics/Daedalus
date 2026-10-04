//! A dynamic plugin whose nodes use a type keyed by a dependency plugin it does not link: its
//! schema still exports (listing the dependency and the port as an external type), while
//! installing it fails with `UnkeyedForeignType`. Nothing in this test binary installs the
//! example plugin, so the type's mapping is unknown here, as in a separately loaded library.

use std::ffi::c_void;

use daedalus::macros::{node, plugin};
use daedalus::runtime::NodeError;
use daedalus::{PluginRegistry, PluginSchema, StrSink};
use daedalus_plugins_example_project::Lease;

#[node(id = "slot", inputs("lease"), outputs("slot"))]
fn slot(lease: &Lease) -> Result<i32, NodeError> {
    Ok(lease.0 as i32)
}

#[plugin(id = "unlinked_dependent", deps("example_rust"), nodes(slot))]
struct UnlinkedDependentPlugin;

daedalus::export_plugin!(UnlinkedDependentPlugin);

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
fn unlinked_dependency_types_are_external_in_the_schema_and_fail_install() {
    let descriptor = daedalus_plugin_descriptor();
    let mut json = None;
    assert!(unsafe { (descriptor.schema)(sink(&mut json)) }, "{json:?}");
    let schema: PluginSchema = serde_json::from_str(&json.unwrap()).unwrap();
    assert_eq!(schema.dependencies, ["example_rust"]);
    let external = &schema.plugin.metadata["external_types"];
    assert_eq!(
        external,
        &serde_json::json!([{
            "owner": "unlinked_dependent:slot",
            "port": "lease",
            "rust_type": std::any::type_name::<Lease>(),
        }])
    );

    let mut registry = PluginRegistry::new();
    let registry_ptr = (&mut registry as *mut PluginRegistry).cast::<c_void>();
    let mut message = None;
    assert!(!unsafe { (descriptor.register)(registry_ptr, sink(&mut message)) });
    let message = message.unwrap();
    assert!(message.contains("declares no type key"), "{message}");
    assert!(message.contains("deps ["), "{message}");
}
