//! A host-fed input's version stamp decides whether a reusable node replays: the host bumps the
//! version when its data changes, and without a version a fresh push is a new identity.

use std::sync::atomic::{AtomicUsize, Ordering};

use daedalus::{
    engine::{Engine, EngineConfig, MetricsLevel},
    macros::{node, plugin},
    runtime::{NodeError, handler_registry::HandlerRegistry, plugins::PluginRegistry},
};

static CALLS: AtomicUsize = AtomicUsize::new(0);

#[node(id = "reuse_host.double", inputs("x"), outputs("y"), reusable)]
fn double(x: i64) -> Result<i64, NodeError> {
    CALLS.fetch_add(1, Ordering::SeqCst);
    Ok(x * 2)
}

#[plugin(id = "reuse_host", nodes(double))]
struct ReuseHostPlugin;

#[test]
fn a_host_version_decides_replay_and_no_version_compares_identity() {
    let mut registry = PluginRegistry::new();
    let plugin = ReuseHostPlugin::new();
    registry.install(&plugin).expect("install");
    let node = plugin.double.alias("node");
    let graph = registry
        .graph_builder()
        .expect("builder")
        .input_typed::<i64>("x")
        .and_then(|b| b.try_node(&node))
        .and_then(|b| b.try_connect("x", &node.inputs.x))
        .and_then(|b| b.try_connect(&node.outputs.y, "y"))
        .expect("wire")
        .build();
    let mut host: daedalus::engine::HostGraph<HandlerRegistry> =
        Engine::new(EngineConfig::default().with_metrics_level(MetricsLevel::Off))
            .expect("engine")
            .compile_registry(&registry, graph)
            .expect("compile");

    host.host().set_input_version("x", Some(1));
    host.push("x", 3_i64);
    host.tick().expect("tick");
    assert_eq!(host.take::<i64>("y"), Some(6));
    assert_eq!(CALLS.load(Ordering::SeqCst), 1);

    // A fresh push under the same version: the host says the data did not change, so it replays.
    host.push("x", 3_i64);
    let replayed = host.tick().expect("tick");
    assert_eq!(replayed.nodes_reused, 1);
    assert_eq!(host.take::<i64>("y"), Some(6));
    assert_eq!(CALLS.load(Ordering::SeqCst), 1);

    // The host bumps the version when its data changes: the node runs.
    host.host().set_input_version("x", Some(2));
    host.push("x", 5_i64);
    host.tick().expect("tick");
    assert_eq!(host.take::<i64>("y"), Some(10));
    assert_eq!(CALLS.load(Ordering::SeqCst), 2);

    // No version: a fresh push is a new identity, so the node runs.
    host.host().set_input_version("x", None);
    host.push("x", 5_i64);
    let fresh = host.tick().expect("tick");
    assert_eq!(fresh.nodes_reused, 0);
    assert_eq!(CALLS.load(Ordering::SeqCst), 3);
}
