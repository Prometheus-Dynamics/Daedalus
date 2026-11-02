//! The shared graph under the full runtime: the same `graph.json` the device plans, run through a
//! `PluginRegistry` by the engine, matches the shared node functions called by hand.
#![cfg(feature = "runtime")]

mod common;

use common::{Reference, signal};
use daedalus::{
    GraphDocument,
    engine::{Engine, EngineConfig},
    runtime::plugins::PluginRegistry,
};
use daedalus_mcu_shared::SharedPlugin;

const GRAPH: &str = include_str!("../graph.json");

#[test]
fn shared_graph_runs_under_the_full_runtime() {
    let mut registry = PluginRegistry::new();
    registry.install(&SharedPlugin::new()).expect("install");
    let doc = GraphDocument::from_json(GRAPH).expect("graph.json");
    let mut host = Engine::new(EngineConfig::default())
        .expect("engine")
        .compile_registry(&registry, doc.graph)
        .expect("compile");
    let mut reference = Reference::new();
    let mut rises = 0;
    for raw in signal() {
        host.push("sample", raw);
        host.tick().expect("tick");
        let expected = reference.step(raw);
        assert_eq!(host.take::<f32>("level"), Some(expected.level));
        assert_eq!(host.take::<bool>("active"), Some(expected.active));
        assert_eq!(host.take::<u32>("rises"), expected.rises);
        rises += usize::from(expected.rises.is_some());
    }
    assert_eq!(rises, 2);
}
