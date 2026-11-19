//! A plugin node hands the bytes it was given on by reference (`daedalus_abi::forward`): the
//! runner moves the value when the node is its only reader, and takes another reference when it
//! isn't. Either way nothing is copied and nothing is released twice.

use crate::support;

use std::sync::Arc;

use daedalus_graph::{EdgeDoc, EdgePolicyDoc, GraphBody, NodeDoc, NodeKindDoc, PortRefDoc};
use daedalus_host::{LoadOptions, LoadedHost};
use daedalus_loader::{Loader, PluginBackend};
use daedalus_planner::{Config, plan};
use daedalus_registry::Registry;
use support::copy_fixture;

fn edge(from: u32, from_port: &str, to: u32, to_port: &str) -> EdgeDoc {
    EdgeDoc {
        from: PortRefDoc {
            node: from,
            port: from_port.to_string(),
        },
        to: PortRefDoc {
            node: to,
            port: to_port.to_string(),
        },
        policy: EdgePolicyDoc::default(),
    }
}

/// `host.x` read by `readers` `fixture.pass_bytes` nodes, each into an output `out<k>`.
fn host(name: &str, readers: u32) -> LoadedHost {
    host_of(name, readers, "fixture.pass_bytes")
}

/// `host.x` read by `readers` nodes `node`, each into an output `out<k>`.
fn host_of(name: &str, readers: u32, node: &str) -> LoadedHost {
    let plugin = Arc::new(
        Loader::new()
            .open(copy_fixture(name))
            .expect("the fixture loads"),
    );
    let mut registry = Registry::new();
    plugin.join(&mut registry).expect("the plugin's nodes join");
    let mut bridge = NodeDoc::new("host");
    bridge.kind = NodeKindDoc::HostBridge;
    bridge.label = Some("io".to_string());
    bridge
        .port_types
        .insert("x".to_string(), "bytes".to_string());
    let mut nodes = vec![bridge];
    let mut edges = Vec::new();
    for k in 0..readers {
        let out = format!("out{k}");
        nodes[0].port_types.insert(out.clone(), "bytes".to_string());
        nodes.push(NodeDoc::new(node));
        edges.push(edge(0, "x", k + 1, "in"));
        edges.push(edge(k + 1, "out", 0, &out));
    }
    let graph = GraphBody {
        nodes,
        edges,
        groups: Vec::new(),
    };
    let backend = PluginBackend::new(Arc::clone(&plugin), 0);
    let planned =
        plan(&graph, &registry, &[&backend], &Config::default()).expect("the graph plans");
    LoadedHost::load(planned, LoadOptions::default()).expect("the plan loads")
}

/// Feeds `bytes` shared, steps, and checks every output is the same bytes, in the same place.
fn round(host: &mut LoadedHost, bytes: &Arc<[u8]>, readers: u32) {
    host.host()
        .feed_shared("x", Arc::clone(bytes))
        .expect("a feed");
    host.step().expect("a tick");
    for k in 0..readers {
        let value = host
            .host()
            .take_value(&format!("out{k}"))
            .expect("a take")
            .expect("a value");
        let out = value.bytes().expect("bytes");
        assert_eq!(
            out.as_ptr(),
            bytes.as_ptr(),
            "output {k} is the input's bytes, not a copy"
        );
        assert_eq!(out.len(), bytes.len());
    }
}

#[test]
fn a_node_that_is_the_only_reader_moves_the_value_on() {
    let mut host = host("forward-one", 1);
    let bytes: Arc<[u8]> = vec![3u8; 4096].into();
    for _ in 0..3 {
        round(&mut host, &bytes, 1);
    }
    drop(host);
    assert_eq!(
        Arc::strong_count(&bytes),
        1,
        "every reference the run took was given back"
    );
}

#[test]
fn two_readers_each_hand_on_their_own_reference() {
    let mut host = host("forward-two", 2);
    let bytes: Arc<[u8]> = vec![5u8; 4096].into();
    for _ in 0..3 {
        round(&mut host, &bytes, 2);
    }
    drop(host);
    assert_eq!(
        Arc::strong_count(&bytes),
        1,
        "every reference the run took was given back"
    );
}

/// One tick through `node`, with the test holding two references besides the run's, so a
/// reference released twice shows as a count of one where two belong, with nothing freed.
fn count_after_one_tick(name: &str, node: &str) -> usize {
    let mut host = host_of(name, 1, node);
    let bytes: Arc<[u8]> = vec![9u8; 4096].into();
    let spare = Arc::clone(&bytes);
    host.host()
        .feed_shared("x", Arc::clone(&bytes))
        .expect("a feed");
    host.step().expect("a tick");
    let taken = host
        .host()
        .take_value("out0")
        .expect("a take")
        .expect("a value");
    drop(taken);
    drop(host);
    let count = Arc::strong_count(&bytes);
    if count < 2 {
        // The run released a reference it didn't own, and that was `spare`'s: forgetting it keeps
        // the last drop below the only one.
        std::mem::forget(spare);
    }
    count
}

#[test]
fn copying_the_cell_releases_the_reference_twice_and_the_mark_does_not() {
    // Without the mark: the input and the output both own the one reference, and both release it.
    assert_eq!(count_after_one_tick("forward-copy", "fixture.copy_cell"), 1);
    // With it: the runner moves the reference, and it is released once.
    assert_eq!(
        count_after_one_tick("forward-mark", "fixture.pass_bytes"),
        2
    );
}
