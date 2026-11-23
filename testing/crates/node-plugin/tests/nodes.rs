//! A crate of `#[node]` functions loaded as a plugin gives what the same crate gives linked in:
//! the same outputs, state kept across a reload, and a node's own metrics.

use std::path::PathBuf;
use std::sync::Arc;

use daedalus_cpu::CpuBackend;
use daedalus_graph::{EdgeDoc, EdgePolicyDoc, GraphBody, Lit, NodeDoc, NodeKindDoc, PortRefDoc};
use daedalus_host::{LoadOptions, LoadedHost};
use daedalus_loader::{Loader, Plugin, PluginBackend};
use daedalus_node_plugin::NODES;
use daedalus_planner::{Config, Planned, plan};
use daedalus_registry::Registry;

/// This crate's own library, which cargo builds beside the test.
fn library() -> PathBuf {
    let exe = std::env::current_exe().expect("the test's own path");
    let name = format!(
        "{}daedalus_node_plugin{}",
        std::env::consts::DLL_PREFIX,
        std::env::consts::DLL_SUFFIX
    );
    let path = exe.parent().expect("a directory").join(name);
    assert!(path.exists(), "{} was not built", path.display());
    path
}

fn edge(from: usize, from_port: &str, to: usize, to_port: &str) -> EdgeDoc {
    EdgeDoc {
        from: PortRefDoc {
            node: from as u32,
            port: from_port.to_string(),
        },
        to: PortRefDoc {
            node: to as u32,
            port: to_port.to_string(),
        },
        policy: EdgePolicyDoc::default(),
    }
}

/// `host.x -> each node in turn -> host.out`, each node given as its id, input and output.
fn chain(nodes: &[(&str, &str, &str)]) -> GraphBody {
    let mut bridge = NodeDoc::new("host");
    bridge.kind = NodeKindDoc::HostBridge;
    for port in ["x", "out"] {
        bridge
            .port_types
            .insert(port.to_string(), "u32".to_string());
    }
    let mut docs = vec![bridge];
    let mut edges = Vec::new();
    let mut last = (0, "x");
    for (at, (id, input, output)) in nodes.iter().enumerate() {
        let mut doc = NodeDoc::new(*id);
        doc.label = Some((*id).to_string());
        docs.push(doc);
        edges.push(edge(last.0, last.1, at + 1, input));
        last = (at + 1, output);
    }
    edges.push(edge(last.0, last.1, 0, "out"));
    GraphBody {
        nodes: docs,
        edges,
        groups: Vec::new(),
    }
}

/// The graph planned with the nodes linked into this test.
fn linked(graph: &GraphBody) -> Planned {
    let mut registry = Registry::new();
    registry.register_all(&NODES).expect("the nodes register");
    plan(graph, &registry, &[&CpuBackend], &Config::default()).expect("the linked graph plans")
}

/// The graph planned with the nodes from the loaded library, and nothing linked.
fn loaded(plugin: &Arc<Plugin>, graph: &GraphBody) -> Planned {
    let mut registry = Registry::new();
    plugin.join(&mut registry).expect("the plugin's nodes join");
    let backend = PluginBackend::new(Arc::clone(plugin), 0);
    plan(graph, &registry, &[&backend], &Config::default()).expect("the loaded graph plans")
}

fn through(host: &mut LoadedHost, x: u32) -> Option<u32> {
    let io = host.host();
    io.feed_inline("x", &x.to_le_bytes()).expect("a feed");
    io.step().expect("a tick");
    io.take_copy::<u32>("out").expect("a take")
}

fn host(planned: Planned) -> LoadedHost {
    LoadedHost::load(planned, LoadOptions::default()).expect("the plan loads")
}

const WHOLE: [(&str, &str, &str); 7] = [
    ("np.triple", "x", "y"),
    ("np.total", "x", "sum"),
    ("np.counted", "x", "y"),
    ("np.split", "x", "pair"),
    ("np.join", "pair", "y"),
    ("np.fill", "x", "bytes"),
    ("np.first", "bytes", "y"),
];

#[test]
fn the_plugin_declares_the_crates_nodes_with_their_ports() {
    let plugin = Loader::new().open(library()).expect("the plugin loads");
    assert_eq!(plugin.name(), "node-plugin");
    let ids: Vec<&str> = plugin.nodes().iter().map(|node| node.id.as_str()).collect();
    let linked: Vec<&str> = NODES.iter().map(|record| record.decl.id).collect();
    assert_eq!(ids, linked);
    let split = &plugin.nodes()[3];
    let names = |ports: &[daedalus_registry::OwnedPort]| -> Vec<String> {
        ports.iter().map(|port| port.name.clone()).collect()
    };
    assert_eq!(
        (names(&split.inputs), names(&split.outputs)),
        (vec!["x".to_string()], vec!["pair".to_string()])
    );
}

#[test]
fn every_node_gives_what_it_gives_linked_and_state_goes_on() {
    let plugin = Arc::new(Loader::new().open(library()).expect("the plugin loads"));
    let graph = chain(&WHOLE);
    let mut from_file = host(loaded(&plugin, &graph));
    let mut built_in = host(linked(&graph));
    // The total in the middle is a running one, so each answer depends on the ones before.
    for x in [1u32, 2, 70_000, 3] {
        let got = through(&mut from_file, x);
        assert!(got.is_some(), "x = {x}");
        assert_eq!(got, through(&mut built_in, x), "x = {x}");
    }
}

#[test]
fn a_reload_keeps_the_state_of_a_plugins_node() {
    let plugin = Arc::new(Loader::new().open(library()).expect("the plugin loads"));
    let graph = chain(&[("np.total", "x", "sum")]);
    let mut host = host(loaded(&plugin, &graph));
    assert_eq!([5, 6].map(|x| through(&mut host, x)), [Some(5), Some(11)]);
    let report = host.reload(loaded(&plugin, &graph)).expect("reloads");
    assert_eq!(report.fresh.total(), 0);
    assert_eq!(through(&mut host, 1), Some(12));
}

#[test]
fn a_plugins_node_runs_though_its_metrics_do_not_cross_yet() {
    let plugin = Arc::new(Loader::new().open(library()).expect("the plugin loads"));
    let graph = chain(&[("np.counted", "x", "y")]);
    let options = LoadOptions {
        node_metrics: true,
        ..LoadOptions::default()
    };
    let mut host = LoadedHost::load(loaded(&plugin, &graph), options).expect("the plan loads");
    for x in [4u32, 6] {
        assert_eq!(through(&mut host, x), Some(x));
    }
    // The plugin's entry does not carry a node's metrics yet, so the host has none to list
    // and gives the body no cells: the body's reports go nowhere, and the node still runs.
    assert!(host.metrics().is_empty());
    assert!(host.metric_rows().is_empty());
}

#[test]
fn a_fault_and_a_restart_go_as_they_go_linked() {
    let plugin = Arc::new(Loader::new().open(library()).expect("the plugin loads"));
    let mut graph = chain(&[("np.fragile", "x", "sum")]);
    graph.nodes[1].metadata.insert(
        "daedalus.node.recovery".to_string(),
        Lit::Str("restart".to_string()),
    );
    let mut from_file = host(loaded(&plugin, &graph));
    let mut built_in = host(linked(&graph));
    let run = |host: &mut LoadedHost, x: u32| {
        let io = host.host();
        io.feed_inline("x", &x.to_le_bytes()).expect("a feed");
        let ticked = io.step().is_ok();
        (ticked, io.take_copy::<u32>("out").expect("a take"))
    };
    let mut seen = Vec::new();
    for x in [5u32, 6, 13, 1, 1] {
        let got = run(&mut from_file, x);
        assert_eq!(got, run(&mut built_in, x), "x = {x}");
        seen.push(got.1);
    }
    // The total ran, the thirteenth failed and gave nothing.
    assert_eq!(seen[..3], [Some(5), Some(11), None]);
    let faults: Vec<(Vec<&str>, String)> = from_file
        .fault_reports()
        .map(|fault| (fault.covers.to_vec(), fault.reason().to_string()))
        .collect();
    assert_eq!(faults, [(vec!["np.fragile"], "thirteen".to_string())]);
}
