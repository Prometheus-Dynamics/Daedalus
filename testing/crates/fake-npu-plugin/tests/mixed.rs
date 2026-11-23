//! Two plugins loaded from files, in one graph built from what they declare; one of them replaced
//! while the host runs; and what is refused by name today: a linked node beside a plugin's, and a
//! plugin behind a linked wrapper beside one that has none.

use std::path::{Path, PathBuf};
use std::process::Command;
use std::sync::Arc;

use daedalus_cpu::CpuBackend;
use daedalus_derive::Type;
use daedalus_fake_npu::PluginNpu;
use daedalus_graph::{EdgeDoc, EdgePolicyDoc, GraphBody, NodeDoc, NodeKindDoc, PortRefDoc};
use daedalus_host::{LoadOptions, LoadedHost};
use daedalus_loader::{Loader, Plugin, PluginBackend};
use daedalus_macros::node;
use daedalus_planner::explain::explain_plan;
use daedalus_planner::{Config, plan};
use daedalus_registry::{NodeRecord, Registry};

/// A node linked into this program.
#[node(id = "t.inc", inputs(x), outputs(y))]
fn inc(x: u32) -> u32 {
    x + 1
}

/// A type of the host's own, with no wire form: a Rust value that lives in this program's memory.
#[derive(Clone, Type)]
#[type_key("host.kept")]
struct Kept {
    #[allow(dead_code)] // made and never read: the graph that would pass it on does not plan
    inner: u32,
}

#[node(id = "t.seal", inputs(x), outputs(y))]
fn seal(x: u32) -> Kept {
    Kept { inner: x }
}

/// Where the bytes it was given are, and how many: to tell a read in place from a copy.
#[node(id = "t.where", inputs(bytes), outputs(at, len))]
fn where_at(bytes: &[u8]) -> (u64, u32) {
    (bytes.as_ptr() as u64, bytes.len() as u32)
}

#[node(id = "t.fill", inputs(x), outputs(bytes))]
fn fill(x: u32) -> Vec<u8> {
    vec![x as u8; 3000]
}

#[node(id = "t.sum", inputs(bytes), outputs(sum))]
fn sum(bytes: &[u8]) -> u32 {
    bytes.iter().map(|byte| u32::from(*byte)).sum()
}

const LINKED: [&NodeRecord; 5] = [&INC_NODE, &SEAL_NODE, &WHERE_AT_NODE, &FILL_NODE, &SUM_NODE];

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
        .register_all(&LINKED)
        .expect("the linked nodes register");
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
    assert_eq!(
        ids(&npu),
        [
            "npu.scale",
            "npu.add",
            "npu.negate",
            "npu.keep",
            "npu.number"
        ]
    );
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
fn a_linked_node_and_a_plugin_node_pass_a_number_both_ways() {
    let (_npu, fixture, registry) = loaded(&mut Loader::new());
    let (pass, pass_in, pass_out) = from_inventory(&fixture, "fixture.pass");
    let mut before = NodeDoc::new("t.inc");
    before.label = Some("before".to_string());
    let mut after = NodeDoc::new("t.inc");
    after.label = Some("after".to_string());
    let graph = GraphBody {
        nodes: vec![bridge(), before, pass, after],
        edges: vec![
            edge(0, "x", 1, "x"),
            edge(1, "y", 2, &pass_in),
            edge(2, &pass_out, 3, "x"),
            edge(3, "y", 0, "out"),
        ],
        groups: Vec::new(),
    };
    let cells = PluginBackend::new(Arc::clone(&fixture), 0);
    let planned = plan(
        &graph,
        &registry,
        &[&cells, &CpuBackend],
        &Config::default(),
    )
    .expect("the graph plans");
    // The plan says how each value crosses: written in the crossing space, handed on.
    let crossing: Vec<(Option<String>, Vec<String>, Option<String>)> = explain_plan(&planned)
        .edges
        .into_iter()
        .filter(|edge| edge.to_node == 2 || edge.from_node == 2)
        .map(|edge| (edge.written_in, edge.transfer, edge.read_in))
        .collect();
    let space = |id: &str| Some(id.to_string());
    assert_eq!(
        crossing,
        [
            (
                space("cpu.wire"),
                vec!["plugin.from_cpu_wire".to_string()],
                space("plugin.host")
            ),
            (
                space("plugin.host"),
                vec!["plugin.to_cpu_wire".to_string()],
                space("cpu.wire")
            ),
        ]
    );
    let mut host = LoadedHost::load(planned, LoadOptions::default()).expect("the plan loads");
    for x in [0u32, 1, 41, u32::MAX - 2] {
        assert_eq!(through(&mut host, x), Some(x + 2));
    }
}

/// A host bridge whose ports have the given type keys.
fn bridge_of(ports: &[(&str, &str)]) -> NodeDoc {
    let mut bridge = NodeDoc::new("host");
    bridge.kind = NodeKindDoc::HostBridge;
    for (port, key) in ports {
        bridge
            .port_types
            .insert((*port).to_string(), (*key).to_string());
    }
    bridge
}

fn plan_with(
    fixture: &Arc<Plugin>,
    registry: &Registry,
    graph: &GraphBody,
) -> Result<daedalus_planner::Planned, daedalus_planner::PlanError> {
    let cells = PluginBackend::new(Arc::clone(fixture), 0);
    plan(graph, registry, &[&cells, &CpuBackend], &Config::default())
}

#[test]
fn a_rust_value_with_no_crossing_form_is_refused_by_name() {
    let (npu, _fixture, registry) = loaded(&mut Loader::new());
    let (keep, keep_in, keep_out) = from_inventory(&npu, "npu.keep");
    let graph = GraphBody {
        nodes: vec![
            bridge_of(&[("x", "u32"), ("out", "host.kept")]),
            NodeDoc::new("t.seal"),
            keep,
        ],
        edges: vec![
            edge(0, "x", 1, "x"),
            edge(1, "y", 2, &keep_in),
            edge(2, &keep_out, 0, "out"),
        ],
        groups: Vec::new(),
    };
    let refused = plan_with(&npu, &registry, &graph).expect_err("a Rust value can't cross");
    // The node, the output and the type are named, with what to do about it.
    assert_eq!(
        refused.to_string(),
        "no backend can run node `t.seal` (cpu: refused (output `y` of t.seal is read outside \
         this program (by a worker or a plugin), but mixed::Kept has no crossing form: add \
         #[derive(WireForm)] to mixed::Kept))"
    );
}

#[test]
fn a_megabyte_a_plugin_hands_over_is_read_in_place_by_a_linked_node() {
    let (_npu, fixture, registry) = loaded(&mut Loader::new());
    let (handover, handover_in, handover_out) = from_inventory(&fixture, "fixture.handover");
    let graph = GraphBody {
        nodes: vec![
            bridge_of(&[
                ("x", "u32"),
                ("bytes", "bytes"),
                ("at", "u64"),
                ("len", "u32"),
            ]),
            handover,
            NodeDoc::new("t.where"),
        ],
        edges: vec![
            edge(0, "x", 1, &handover_in),
            edge(1, &handover_out, 2, "bytes"),
            edge(1, &handover_out, 0, "bytes"),
            edge(2, "at", 0, "at"),
            edge(2, "len", 0, "len"),
        ],
        groups: Vec::new(),
    };
    let planned = plan_with(&fixture, &registry, &graph).expect("the graph plans");
    let mut host = LoadedHost::load(planned, LoadOptions::default()).expect("the plan loads");
    let io = host.host();
    io.feed_inline("x", &1u32.to_le_bytes()).expect("a feed");
    io.step().expect("a tick");
    let held = io.take_value("bytes").expect("a take").expect("the bytes");
    let bytes = held.bytes().expect("bytes");
    assert_eq!((bytes.len(), bytes[0]), (1 << 20, 7));
    // The linked node read the plugin's megabyte where the plugin has it.
    assert_eq!(io.take_copy::<u32>("len").expect("a take"), Some(1 << 20));
    assert_eq!(
        io.take_copy::<u64>("at").expect("a take"),
        Some(bytes.as_ptr() as u64)
    );
}

#[test]
fn bytes_from_a_linked_node_pass_through_a_plugin_to_a_linked_node() {
    let (_npu, fixture, registry) = loaded(&mut Loader::new());
    let (pass, pass_in, pass_out) = from_inventory(&fixture, "fixture.pass_bytes");
    let graph = GraphBody {
        nodes: vec![
            bridge_of(&[("x", "u32"), ("out", "u32")]),
            NodeDoc::new("t.fill"),
            pass,
            NodeDoc::new("t.sum"),
        ],
        edges: vec![
            edge(0, "x", 1, "x"),
            edge(1, "bytes", 2, &pass_in),
            edge(2, &pass_out, 3, "bytes"),
            edge(3, "sum", 0, "out"),
        ],
        groups: Vec::new(),
    };
    let planned = plan_with(&fixture, &registry, &graph).expect("the graph plans");
    let mut host = LoadedHost::load(planned, LoadOptions::default()).expect("the plan loads");
    for x in [1u32, 9] {
        assert_eq!(through(&mut host, x), Some(x * 3000));
    }
}

#[test]
fn a_graph_that_needs_a_plugins_codec_does_not_plan_today() {
    let (npu, fixture, registry) = loaded(&mut Loader::new());
    let (number, number_in, number_out) = from_inventory(&npu, "npu.number");
    let graph = GraphBody {
        nodes: vec![
            bridge(),
            NodeDoc::new("t.inc"),
            number,
            NodeDoc::new("t.inc"),
        ],
        edges: vec![
            edge(0, "x", 1, "x"),
            edge(1, "y", 2, &number_in),
            edge(2, &number_out, 3, "x"),
            edge(3, "y", 0, "out"),
        ],
        groups: Vec::new(),
    };
    // The planner finds the other plugin's codec between `u32` and its type and puts the
    // conversions in. They are declared for that plugin's backend, which has no step for them.
    let device = PluginBackend::new(Arc::clone(&npu), 0);
    let cells = PluginBackend::new(Arc::clone(&fixture), 0);
    let refused = plan(
        &graph,
        &registry,
        &[&device, &cells, &CpuBackend],
        &Config::default(),
    )
    .expect_err("no step for a conversion");
    let told = refused.to_string();
    for part in [
        "dylib.number.from_value",
        "dylib.number.to_value",
        "fixture.cpu",
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
