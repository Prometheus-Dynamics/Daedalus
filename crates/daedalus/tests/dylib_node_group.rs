//! A node group exported by a dylib plugin (Rust ABI): the library's install registers
//! `groups_example:pipe`, a host places it on a graph, and it expands into its stages, which
//! run and report telemetry under the canonical group id.
// musl executables are static and cannot dlopen (see `dylib::library`): no loader there.
#![cfg(not(target_env = "musl"))]

#[path = "support/cdylib.rs"]
mod cdylib;

use std::path::Path;
use std::sync::OnceLock;

use cdylib::{build_cdylib, host_matching_features};

use daedalus::PluginLibrary;
use daedalus::engine::{Engine, EngineConfig};
use daedalus::runtime::plugins::PluginRegistry;

const PACKAGE: &str = "daedalus-plugins-node-groups-dylib";
const LIB_NAME: &str = "daedalus_plugins_node_groups_dylib";
const GROUP: &str = "groups_example:pipe";
const STABLE_PACKAGE: &str = "daedalus-plugins-node-groups-stable";
const STABLE_LIB_NAME: &str = "daedalus_plugins_node_groups_stable";

/// Build the node-groups plugin as a cdylib (once per test binary) and return its path.
fn plugin_cdylib() -> &'static Path {
    static PATH: OnceLock<std::path::PathBuf> = OnceLock::new();
    PATH.get_or_init(|| build_cdylib(PACKAGE, LIB_NAME, &host_matching_features()))
}

/// Build the stable-path variant (its descriptor claims another rustc) as a cdylib.
fn stable_cdylib() -> &'static Path {
    static PATH: OnceLock<std::path::PathBuf> = OnceLock::new();
    PATH.get_or_init(|| build_cdylib(STABLE_PACKAGE, STABLE_LIB_NAME, &host_matching_features()))
}

#[test]
fn dylib_group_expands_to_its_stages_and_rolls_up_under_its_id() {
    expands_and_rolls_up(plugin_cdylib());
}

#[test]
fn stable_path_group_expands_to_its_stages_and_rolls_up_under_its_id() {
    expands_and_rolls_up(stable_cdylib());
}

/// Load `path` through the plugin loader (Rust ABI or stable, as its descriptor selects), install
/// it, place the group on a graph and check it runs as its stages with a rollup under its id.
fn expands_and_rolls_up(path: &Path) {
    let library = unsafe { PluginLibrary::load(path) }.expect("load plugin");
    let mut registry = PluginRegistry::new();
    library.install_into(&mut registry).expect("install plugin");

    let caps = registry.combined_transport_capabilities().expect("caps");
    assert!(
        caps.nodes()
            .get(&daedalus::registry::ids::NodeId::new(GROUP))
            .is_some(),
        "the exported group is listed as a node"
    );

    let graph = registry
        .graph_builder()
        .expect("builder")
        .try_node_from_id(GROUP, "g")
        .and_then(|b| b.try_connect("x", ("g", "x")))
        .and_then(|b| b.try_connect(("g", "out"), "out"))
        .expect("wire group graph")
        .build();
    let mut host = Engine::new(EngineConfig::default())
        .expect("engine")
        .compile_registry(&registry, graph)
        .expect("compile");
    host.push("x", 3_i32);
    let telemetry = host.tick().expect("tick");
    assert_eq!(host.take::<i32>("out"), Some(8));

    let labels = host.node_labels();
    for stage in ["::add", "::double"] {
        let idx = labels
            .iter()
            .position(|label| label.ends_with(stage))
            .unwrap_or_else(|| panic!("no stage `{stage}` in {labels:?}"));
        assert!(
            telemetry.node_metrics.get(idx).is_some(),
            "stage `{stage}` telemetry"
        );
    }
    assert!(
        telemetry.group_metrics.contains_key(GROUP),
        "rollup under the canonical id"
    );
}
