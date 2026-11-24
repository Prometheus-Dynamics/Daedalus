//! A crate of `#[node]` functions loaded as a plugin gives what the same crate gives linked in:
//! the same outputs, state kept across a reload, a node's own metrics, the constants and config
//! a graph gives a node, a value with no crossing form between two of its nodes, and a refusal in
//! the plugin's own words.

use std::path::PathBuf;
use std::sync::Arc;

use daedalus_cpu::CpuBackend;
use daedalus_graph::{
    EdgeDoc, EdgePolicyDoc, GraphBody, Lit, LitField, NodeDoc, NodeKindDoc, PortRefDoc,
};
use daedalus_host::{LoadOptions, LoadedHost};
use daedalus_loader::{Loader, Plugin, PluginBackend};
use daedalus_macros::node;
use daedalus_node_plugin::{Kept, NODES};
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

#[test]
fn a_constant_reaches_a_plugins_node_as_it_reaches_it_linked() {
    // `host.x -> np.scale (by 3) -> np.triple -> host.out`.
    let mut graph = chain(&[("np.scale", "x", "y"), ("np.triple", "x", "y")]);
    graph.nodes[1].consts.insert("by".to_string(), Lit::Int(3));
    let mut from_file = host(loaded(&plugin_of(), &graph));
    let mut built_in = host(linked(&graph));
    for x in [0u32, 1, 7] {
        let got = through(&mut from_file, x);
        assert_eq!(got, Some(x * 9), "x = {x}");
        assert_eq!(got, through(&mut built_in, x), "x = {x}");
    }
}

#[test]
fn a_patched_constant_is_not_offered_to_a_plugins_node_yet_and_is_refused_by_name() {
    let mut graph = chain(&[("np.scale", "x", "y")]);
    graph.nodes[1].consts.insert("by".to_string(), Lit::Int(3));
    let mut host = host(loaded(&plugin_of(), &graph));
    assert_eq!(through(&mut host, 2), Some(6));
    let error = host
        .patch_const("np.scale", "by", Lit::Int(5))
        .expect_err("no step of the plugin takes a later value yet");
    assert!(
        matches!(error, daedalus_host::PatchError::NotRead { .. }),
        "{error}"
    );
    assert_eq!(through(&mut host, 2), Some(6));
}

fn plugin_of() -> Arc<Plugin> {
    Arc::new(Loader::new().open(library()).expect("the plugin loads"))
}

#[test]
fn a_plugins_config_is_refused_by_the_planner_until_the_entry_carries_its_fields() {
    let field = |name: &str, value| LitField {
        name: name.to_string(),
        value: Lit::Int(value),
    };
    let mut graph = chain(&[("np.shifted", "x", "y")]);
    let config = Lit::Struct(vec![field("add", 4), field("then_double", 1)]);
    graph.nodes[1].consts.insert("config".to_string(), config);
    // Linked, the config is read.
    assert_eq!(through(&mut host(linked(&graph)), 1), Some(10));
    // From the file, the entry names the node's ports and not its config's fields, so the
    // planner has nothing to fit the config to.
    let plugin = plugin_of();
    let mut registry = Registry::new();
    plugin.join(&mut registry).expect("the plugin's nodes join");
    let backend = PluginBackend::new(Arc::clone(&plugin), 0);
    let error = plan(&graph, &registry, &[&backend], &Config::default())
        .expect_err("a config the entry does not describe is refused");
    assert!(format!("{error:?}").contains("NoPortOrField"), "{error:?}");
}

#[test]
fn a_plugins_refusal_reaches_the_host_in_its_own_words() {
    let plugin = Arc::new(Loader::new().open(library()).expect("the plugin loads"));
    let mut graph = chain(&[("np.triple", "x", "y")]);
    let mut sum = NodeDoc::new("np.sum");
    sum.label = Some("sum".to_string());
    graph.nodes.push(sum);
    graph.edges[1] = edge(2, "y", 0, "out");
    let mut registry = Registry::new();
    plugin.join(&mut registry).expect("the plugin's nodes join");
    let backend = PluginBackend::new(Arc::clone(&plugin), 0);
    let error = plan(&graph, &registry, &[&backend], &Config::default())
        .expect_err("a node the plugin cannot build is refused");
    let words = format!("{error:?}");
    assert!(
        words.contains("np.sum takes a fan-in, which a plugin's node cannot yet"),
        "the plugin's words are lost: {words}"
    );
}

#[test]
fn a_value_with_no_crossing_form_goes_between_two_plugin_nodes_as_it_is() {
    let graph = chain(&[("np.keep", "x", "kept"), ("np.weigh", "kept", "y")]);
    let mut from_file = host(loaded(&plugin_of(), &graph));
    let mut built_in = host(linked(&graph));
    for x in [0u32, 1, 5, 63, 64, 100] {
        let got = through(&mut from_file, x);
        assert!(got.is_some(), "x = {x}");
        assert_eq!(got, through(&mut built_in, x), "x = {x}");
    }
}

/// How many values the plugin's build holds in boxes, asked of the plugin by a graph of one node.
fn held_in(host: &mut LoadedHost, plugin: &Arc<Plugin>) -> Option<u32> {
    let graph = chain(&[("np.held", "x", "y")]);
    host.reload(loaded(plugin, &graph)).expect("reloads");
    through(host, 0)
}

#[test]
fn every_value_inside_a_plugin_is_released_before_its_library_closes() {
    let plugin = plugin_of();
    let probe = plugin.generation_probe();

    // A reload: the old graph's cells give their values back.
    let graph = chain(&[("np.keep", "x", "kept"), ("np.weigh", "kept", "y")]);
    let mut host = host(loaded(&plugin, &graph));
    for x in [3u32, 4, 5] {
        assert!(through(&mut host, x).is_some());
    }
    assert_eq!(held_in(&mut host, &plugin), Some(0), "after a reload");

    // A fault with the value still in its reader's cell, and the restart after it.
    let mut graph = chain(&[("np.keep", "x", "kept"), ("np.weigh_or_fail", "kept", "y")]);
    graph.nodes[2].metadata.insert(
        "daedalus.node.recovery".to_string(),
        Lit::Str("restart".to_string()),
    );
    host.reload(loaded(&plugin, &graph)).expect("reloads");
    for x in [3u32, 6, 4] {
        let io = host.host();
        io.feed_inline("x", &x.to_le_bytes()).expect("a feed");
        io.step().expect("a tick: the node restarts");
        let out = io.take_copy::<u32>("out").expect("a take");
        assert_eq!(out.is_some(), x != 6, "x = {x}");
    }
    let faults: Vec<String> = host
        .fault_reports()
        .map(|fault| fault.reason().to_string())
        .collect();
    assert_eq!(faults, ["fifteen"]);
    assert_eq!(held_in(&mut host, &plugin), Some(0), "after a fault");

    // Taking the plugin out: the graph goes, then the plugin, and the library closes, which it
    // cannot while any value it made is held.
    host.reload(loaded(&plugin, &graph)).expect("reloads");
    assert!(through(&mut host, 3).is_some());
    drop(host);
    drop(plugin);
    assert!(
        probe.upgrade().is_none(),
        "the library closes once the graph and the plugin are gone"
    );
}

/// A node of this program that reads the plugin's value.
#[node(id = "t.weigh", inputs(kept), outputs(y))]
fn weigh_here(kept: &Kept) -> u32 {
    kept.items.len() as u32
}

#[test]
fn a_value_with_no_crossing_form_cannot_leave_the_plugin_and_is_refused_in_words() {
    let plugin = plugin_of();
    let graph = chain(&[("np.keep", "x", "kept"), ("t.weigh", "kept", "y")]);
    let mut registry = Registry::new();
    plugin.join(&mut registry).expect("the plugin's nodes join");
    registry.register(&WEIGH_HERE_NODE).expect("registers");
    let backend = PluginBackend::new(Arc::clone(&plugin), 0);
    let error = plan(
        &graph,
        &registry,
        &[&backend, &CpuBackend],
        &Config::default(),
    )
    .expect_err("a value with no crossing form cannot cross");
    let words = format!("{error:?}");
    assert!(
        words.contains(
            "output `kept` of np.keep is read by a node outside this plugin: the edge crosses the plugin's boundary"
        ),
        "{words}"
    );
}
