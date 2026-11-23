//! Two plugins loaded from files, in one graph built from what they declare; one of them replaced
//! while the host runs; and what is refused by name today: a linked node beside a plugin's, and a
//! plugin behind a linked wrapper beside one that has none.

use std::path::{Path, PathBuf};
use std::process::Command;
use std::sync::Arc;

use daedalus_cpu::CpuBackend;
use daedalus_fake_npu::PluginNpu;
use daedalus_graph::{EdgeDoc, EdgePolicyDoc, GraphBody, NodeDoc, NodeKindDoc, PortRefDoc};
use daedalus_host::{LoadOptions, LoadedHost};
use daedalus_loader::{Loader, Plugin, PluginBackend};
use daedalus_macros::node;
use daedalus_planner::explain::explain_plan;
use daedalus_planner::{Config, plan};
use daedalus_registry::Registry;

/// A node linked into this program.
#[node(id = "t.inc", inputs(x), outputs(y))]
fn inc(x: u32) -> u32 {
    x + 1
}

/// The first plugin: the fake device's, which cargo builds beside this test.
fn npu_library() -> PathBuf {
    let exe = std::env::current_exe().expect("the test's own path");
    let name = format!(
        "{}daedalus_fake_npu_plugin{}",
        std::env::consts::DLL_PREFIX,
        std::env::consts::DLL_SUFFIX
    );
    exe.parent().expect("a directory").join(name)
}

/// The second plugin: the dylib fixture, built on its own into the directory the loader's and
/// the daemon's tests share, so it is built once for all of them.
fn fixture_library() -> PathBuf {
    let manifest = Path::new(env!("CARGO_MANIFEST_DIR")).join("../../fixtures/dylib/Cargo.toml");
    let target = Path::new(env!("CARGO_TARGET_TMPDIR")).join("dylib-fixture");
    let cargo = std::env::var("CARGO").unwrap_or_else(|_| "cargo".to_owned());
    let status = Command::new(cargo)
        .args(["build", "-q", "--manifest-path"])
        .arg(manifest)
        .arg("--target-dir")
        .arg(&target)
        .status()
        .expect("cargo runs");
    assert!(status.success(), "the fixture builds");
    target.join("debug").join(format!(
        "{}daedalus_dylib_fixture{}",
        std::env::consts::DLL_PREFIX,
        std::env::consts::DLL_SUFFIX
    ))
}

/// A node of a plugin, as a graph node, with the names of its first input and output: all read
/// from what the plugin declares.
fn from_inventory(plugin: &Plugin, id: &str) -> (NodeDoc, String, String) {
    let decl = plugin
        .nodes()
        .iter()
        .find(|node| node.id == id)
        .expect("the plugin declares the node");
    let mut doc = NodeDoc::new(id);
    doc.label = Some(id.to_string());
    (
        doc,
        decl.inputs[0].name.clone(),
        decl.outputs[0].name.clone(),
    )
}

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

fn bridge() -> NodeDoc {
    let mut bridge = NodeDoc::new("host");
    bridge.kind = NodeKindDoc::HostBridge;
    for port in ["x", "out"] {
        bridge
            .port_types
            .insert(port.to_string(), "u32".to_string());
    }
    bridge
}

/// Both plugins, loaded from their files, and a registry with their nodes and the linked one.
fn loaded(loader: &mut Loader) -> (Arc<Plugin>, Arc<Plugin>, Registry) {
    let npu = Arc::new(loader.open(npu_library()).expect("the first loads"));
    let fixture = Arc::new(loader.open(fixture_library()).expect("the second loads"));
    let mut registry = Registry::new();
    registry
        .register_all(&[&INC_NODE])
        .expect("the linked node registers");
    npu.join(&mut registry)
        .expect("the first plugin's nodes join");
    fixture
        .join(&mut registry)
        .expect("the second plugin's nodes join");
    (npu, fixture, registry)
}

/// `host.x -> fixture.pass -> npu.scale -> host.out`, with every plugin port's name read from
/// the plugin that declares it.
fn two_plugin_graph(npu: &Plugin, fixture: &Plugin) -> GraphBody {
    let (pass, pass_in, pass_out) = from_inventory(fixture, "fixture.pass");
    let (scale, scale_in, scale_out) = from_inventory(npu, "npu.scale");
    GraphBody {
        nodes: vec![bridge(), pass, scale],
        edges: vec![
            edge(0, "x", 1, &pass_in),
            edge(1, &pass_out, 2, &scale_in),
            edge(2, &scale_out, 0, "out"),
        ],
        groups: Vec::new(),
    }
}

fn through(host: &mut LoadedHost, x: u32) -> Option<u32> {
    let io = host.host();
    io.feed_inline("x", &x.to_le_bytes()).expect("a feed");
    io.step().expect("a tick");
    io.take_copy::<u32>("out").expect("a take")
}

#[test]
fn two_plugins_run_one_graph_built_from_what_they_declare() {
    let (npu, fixture, registry) = loaded(&mut Loader::new());
    // What a host learns from the files alone: each node's id, and each port's name and type.
    let ids = |plugin: &Plugin| -> Vec<String> {
        plugin.nodes().iter().map(|node| node.id.clone()).collect()
    };
    assert_eq!(ids(&npu), ["npu.scale", "npu.add", "npu.negate"]);
    assert!(ids(&fixture).contains(&"fixture.pass".to_string()));

    let graph = two_plugin_graph(&npu, &fixture);
    let device = PluginBackend::new(Arc::clone(&npu), 0);
    let cells = PluginBackend::new(Arc::clone(&fixture), 0);
    let planned =
        plan(&graph, &registry, &[&device, &cells], &Config::default()).expect("the graph plans");
    let placed: Vec<Option<String>> = explain_plan(&planned)
        .nodes
        .into_iter()
        .map(|node| node.backend)
        .collect();
    let on = |id: &str| Some(id.to_string());
    assert_eq!(placed, [None, on("fixture.cpu"), on("fake-npu")]);

    let mut host = LoadedHost::load(planned, LoadOptions::default()).expect("the plan loads");
    for x in [0u32, 1, 41] {
        assert_eq!(through(&mut host, x), Some(x * 2));
    }
}

#[test]
fn a_linked_node_beside_a_plugin_node_is_refused_by_name_today() {
    let (_npu, fixture, registry) = loaded(&mut Loader::new());
    let (pass, pass_in, pass_out) = from_inventory(&fixture, "fixture.pass");
    let graph = GraphBody {
        nodes: vec![bridge(), NodeDoc::new("t.inc"), pass],
        edges: vec![
            edge(0, "x", 1, "x"),
            edge(1, "y", 2, &pass_in),
            edge(2, &pass_out, 0, "out"),
        ],
        groups: Vec::new(),
    };
    let cells = PluginBackend::new(Arc::clone(&fixture), 0);
    let refused = plan(
        &graph,
        &registry,
        &[&cells, &CpuBackend],
        &Config::default(),
    )
    .expect_err("nothing moves a value from the CPU backend's space to the plugin's");
    // The edge and both spaces are named. Nothing of the linked node's reaches the plugin.
    let told = format!("{refused:?}");
    for part in [
        "NoTransfer",
        "fixture.pass.in",
        "t.inc.y",
        "cpu.host",
        "plugin.host",
    ] {
        assert!(told.contains(part), "{told}");
    }
}

#[test]
fn a_plugin_behind_a_linked_wrapper_is_refused_beside_one_that_has_none_today() {
    let (npu, fixture, registry) = loaded(&mut Loader::new());
    let graph = two_plugin_graph(&npu, &fixture);
    // The wrapper gives the device its own memory; the other plugin's values are in the
    // plugin space, and nothing moves a value between the two.
    let device = PluginNpu::from_plugin(Arc::clone(&npu)).expect("the plugin has the backend");
    let cells = PluginBackend::new(Arc::clone(&fixture), 0);
    let refused = plan(
        &graph,
        &registry,
        &[&device, &cells, &CpuBackend],
        &Config::default(),
    )
    .expect_err("nothing moves a value from the plugin space to the device's");
    let told = format!("{refused:?}");
    for part in ["NoTransfer", "npu.scale.x", "plugin.host", "fake-npu:sram"] {
        assert!(told.contains(part), "{told}");
    }
}

#[test]
fn a_plugin_is_replaced_while_the_host_runs_its_graph() {
    let mut loader = Loader::new();
    let (npu, fixture, registry) = loaded(&mut loader);
    let graph = two_plugin_graph(&npu, &fixture);
    let planned = |npu: &Arc<Plugin>, registry: &Registry| {
        let device = PluginBackend::new(Arc::clone(npu), 0);
        let cells = PluginBackend::new(Arc::clone(&fixture), 0);
        plan(&graph, registry, &[&device, &cells], &Config::default()).expect("the graph plans")
    };
    let mut host =
        LoadedHost::load(planned(&npu, &registry), LoadOptions::default()).expect("loads");
    assert_eq!(through(&mut host, 4), Some(8));

    // The same file again, under another name: a second generation of the same plugin.
    let again = Path::new(env!("CARGO_TARGET_TMPDIR"))
        .join(format!("mixed-npu-again{}", std::env::consts::DLL_SUFFIX));
    let _ = std::fs::remove_file(&again);
    std::fs::copy(npu_library(), &again).expect("copy the plugin");
    let old = npu.generation_probe();
    let newer = Arc::new(loader.open(&again).expect("the plugin loads again"));
    assert_eq!(
        (npu.generation().number(), newer.generation().number()),
        (1, 2)
    );

    // The running graph moves onto it, and goes on giving answers. The other plugin's node
    // is carried as it is.
    let report = host
        .reload(planned(&newer, &registry))
        .expect("the graph reloads");
    assert!(report.fresh.names().iter().any(|name| name == "npu.scale"));
    assert!(
        report
            .carried
            .names()
            .iter()
            .any(|name| name == "fixture.pass")
    );
    assert_eq!(through(&mut host, 5), Some(10));

    // The first library is let go once nothing of it is held.
    drop(npu);
    assert!(old.upgrade().is_none(), "the old generation is retired");
}
