//! The same graph under a full host matches the node functions.

#![cfg(feature = "host")]

mod common;

use common::{Reference, signal};
use daedalus::prelude::{Config, LoadOptions, LoadedHost, plan};
use daedalus_chip_shared::NODES;
use daedalus_cpu::CpuBackend;
use daedalus_graph::GraphDoc;
use daedalus_registry::Registry;

#[test]
fn shared_graph_runs_under_a_full_host() {
    let graph = GraphDoc::parse(include_str!("../graph.json"))
        .unwrap()
        .graph;
    let mut registry = Registry::new();
    registry.register_all(&NODES).unwrap();
    let planned = plan(&graph, &registry, &[&CpuBackend], &Config::default()).unwrap();
    let mut loaded = LoadedHost::load(planned, LoadOptions::default()).unwrap();
    let host = loaded.host();
    let mut reference = Reference::default();
    let mut rises = 0;
    for raw in signal() {
        host.feed_typed("sample", std::sync::Arc::new(raw)).unwrap();
        host.step().unwrap();
        let expected = reference.step(raw);
        assert_eq!(
            (
                host.take::<f32>("level").unwrap(),
                host.take::<bool>("active").unwrap(),
                host.take::<u32>("rises").unwrap(),
            ),
            expected
        );
        rises += usize::from(expected.2.is_some());
    }
    assert_eq!(rises, 2);
}
