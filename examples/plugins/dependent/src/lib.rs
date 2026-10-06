//! A plugin that depends on another plugin: its nodes use `Lease`, whose key
//! (`example:lease`) the example plugin maps, and `Counter`, which the example crate owns.
//!
//! `deps("example_rust")` declares the dependency (exported in the schema; hosts must install
//! the example plugin first), and `export_plugin!(.., deps [ExampleProjectPlugin])` links it,
//! so the library resolves `Lease`'s key when it introspects itself in its own registry.
//! Without the link the schema still loads, listing the ports as `external_types`, but
//! installing fails with `UnkeyedForeignType`.
//!
//! `crate_build` registers this crate's build (features exported by `build.rs`); the library
//! exports it with the linked example plugin's, so hosts can compare both crates' builds.

use daedalus::macros::{node, plugin};
use daedalus::runtime::NodeError;
use daedalus_plugins_example_project::{Counter, Lease};

/// Lease a slot for a counter.
#[node(id = "lease", inputs("counter"), outputs("lease"))]
fn lease(counter: &Counter) -> Result<Lease, NodeError> {
    u32::try_from(counter.0)
        .map(Lease)
        .map_err(|_| NodeError::InvalidInput("negative counter".into()))
}

/// The slot a lease holds.
#[node(id = "slot", inputs("lease"), outputs("slot"))]
fn slot(lease: &Lease) -> Result<i32, NodeError> {
    Ok(lease.0 as i32)
}

#[plugin(
    id = "example_dependent",
    crate_build,
    deps("example_rust"),
    nodes(lease, slot)
)]
pub struct DependentPlugin;

#[cfg(feature = "dylib")]
daedalus::export_plugin!(
    DependentPlugin,
    deps[daedalus_plugins_example_project::ExampleProjectPlugin]
);
