//! A backend the core has never heard of, shipped as a plugin: it loads, runs nodes beside CPU
//! nodes, refuses what it cannot run, moves values through its own transfer table, and survives
//! a reload. Nothing under `core/` or in the CPU backend names it.

use std::path::{Path, PathBuf};
use std::sync::Arc;

use daedalus_backend::Backend;
use daedalus_cpu::CpuBackend;
use daedalus_fake_npu::{BACKEND_ID, DOWNLOAD, FakeNpu, HOST, PluginNpu, SRAM, UPLOAD, counters};
use daedalus_graph::{EdgeDoc, EdgePolicyDoc, GraphBody, NodeDoc, NodeKindDoc, PortRefDoc};
use daedalus_host::{LoadOptions, LoadedHost};
use daedalus_loader::Loader;
use daedalus_macros::node;
use daedalus_planner::explain::explain_plan;
use daedalus_planner::{Config, Planned, plan};
use daedalus_registry::{NodeRecord, Registry};

#[node(id = "t.inc", inputs(x), outputs(y))]
fn inc(x: u32) -> u32 {
    x + 1
}

/// The CPU body of the node the plugin serves, for a run with no NPU at all.
#[node(id = "npu.scale", inputs(x), outputs(y))]
fn scale_cpu(x: u32) -> u32 {
    x * 2
}

/// The same node written against the authoring crate, for the backend linked in.
#[node(
    id = "npu.scale",
    inputs(x),
    outputs(y),
    backend = daedalus_fake_npu_author(model = "scale2", x: u32, y: u32)
)]
const SCALE_NPU: &str = "scale2";

/// A node whose model the device does not have, with a CPU body to fall back on.
#[node(
    id = "npu.root",
    inputs(x),
    outputs(y),
    backend = daedalus_fake_npu_author(model = "sqrt", x: u32, y: u32)
)]
const ROOT_NPU: &str = "sqrt";

#[node(id = "npu.root", inputs(x), outputs(y))]
fn root_cpu(x: u32) -> u32 {
    x.isqrt()
}

/// The plugin's library. Cargo builds it with this test, since the test links the same crate,
/// and puts it beside the test's own binary.
fn library() -> PathBuf {
    let exe = std::env::current_exe().expect("the test's own path");
    let dir = exe.parent().expect("the directory of the test binary");
    let name = format!(
        "{}daedalus_fake_npu_plugin{}",
        std::env::consts::DLL_PREFIX,
        std::env::consts::DLL_SUFFIX
    );
    let path = dir.join(&name);
    assert!(path.exists(), "{} was not built", path.display());
    path
}

/// A copy of the library of this test's own, so each test loads a library nobody else has.
fn copy(test: &str, name: &str) -> PathBuf {
    let dir = Path::new(env!("CARGO_TARGET_TMPDIR")).join(format!("fake-npu-{test}"));
    std::fs::create_dir_all(&dir).expect("a directory for the copies");
    let path = dir.join(format!("{name}{}", std::env::consts::DLL_SUFFIX));
    let _ = std::fs::remove_file(&path);
    std::fs::copy(library(), &path).expect("copy the plugin");
    path
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

/// `host.x -> t.inc -> <middle> -> t.inc -> host.out`.
fn graph(middle: &str) -> GraphBody {
    let mut bridge = NodeDoc::new("host");
    bridge.kind = NodeKindDoc::HostBridge;
    bridge.label = Some("io".to_string());
    for port in ["x", "out"] {
        bridge
            .port_types
            .insert(port.to_string(), "u32".to_string());
    }
    let labelled = |id: &str, label: &str| {
        let mut doc = NodeDoc::new(id);
        doc.label = Some(label.to_string());
        doc
    };
    GraphBody {
        nodes: vec![
            bridge,
            labelled("t.inc", "before"),
            labelled(middle, "middle"),
            labelled("t.inc", "after"),
        ],
        edges: vec![
            edge(0, "x", 1, "x"),
            edge(1, "y", 2, "x"),
            edge(2, "y", 3, "x"),
            edge(3, "y", 0, "out"),
        ],
        groups: Vec::new(),
    }
}

fn registry(records: &[&'static NodeRecord]) -> Registry {
    let mut registry = Registry::new();
    registry.register_all(records).expect("the nodes register");
    registry
}

fn through(host: &mut LoadedHost, x: u32) -> u32 {
    let io = host.host();
    io.feed_inline("x", &x.to_le_bytes()).expect("a feed");
    io.step().expect("a tick");
    io.take_copy::<u32>("out")
        .expect("a take")
        .expect("a value")
}

fn load(planned: Planned) -> LoadedHost {
    LoadedHost::load(planned, LoadOptions::default()).expect("the plan loads")
}

/// Which backend each labelled node of a plan runs on.
fn placed(planned: &Planned, node: u32) -> Option<String> {
    explain_plan(planned)
        .nodes
        .into_iter()
        .find(|explained| explained.node == node)
        .and_then(|explained| explained.backend)
}

#[test]
fn loads_as_plugin() {
    let plugin = Loader::new()
        .open(copy("loads", "npu"))
        .expect("the plugin loads");
    assert_eq!(plugin.name(), "fake-npu-plugin");
    // The backend is in the library, with its nodes: nothing was linked into this test.
    let backends: Vec<&str> = plugin
        .backends()
        .iter()
        .map(|backend| backend.id())
        .collect();
    assert_eq!(backends, [BACKEND_ID]);
    let nodes: Vec<&str> = plugin.nodes().iter().map(|node| node.id.as_str()).collect();
    assert_eq!(nodes, ["npu.scale", "npu.add", "npu.negate"]);

    // A registry that had never heard of them now plans a graph with one.
    let mut registry = registry(&[&INC_NODE]);
    plugin.join(&mut registry).expect("the plugin's nodes join");
    let npu = PluginNpu::from_plugin(Arc::new(plugin)).expect("the plugin has the backend");
    assert_eq!((npu.id(), npu.generation()), (BACKEND_ID, 1));
    assert_eq!(npu.memory()[0].id, SRAM);
    let planned = plan(
        &graph("npu.negate"),
        &registry,
        &[&npu, &CpuBackend],
        &Config::default(),
    )
    .expect("the graph plans");
    assert_eq!(placed(&planned, 2).as_deref(), Some(BACKEND_ID));
}

#[test]
fn node_runs_on_fake_npu() {
    let plugin = Loader::new()
        .open(copy("runs", "npu"))
        .expect("the plugin loads");
    let mut with_npu = registry(&[&INC_NODE]);
    plugin.join(&mut with_npu).expect("the plugin's nodes join");
    let npu = PluginNpu::from_plugin(Arc::new(plugin)).expect("the plugin has the backend");
    let planned = plan(
        &graph("npu.scale"),
        &with_npu,
        &[&npu, &CpuBackend],
        &Config::default(),
    )
    .expect("the graph plans");
    assert_eq!(placed(&planned, 1).as_deref(), Some("cpu"));
    assert_eq!(placed(&planned, 2).as_deref(), Some(BACKEND_ID));
    assert_eq!(placed(&planned, 3).as_deref(), Some("cpu"));
    let mut mixed = load(planned);

    // The same graph with no NPU anywhere: the node's CPU body runs.
    let cpu_only = registry(&[&INC_NODE, &SCALE_CPU_NODE]);
    let planned = plan(
        &graph("npu.scale"),
        &cpu_only,
        &[&CpuBackend],
        &Config::default(),
    )
    .expect("the graph plans");
    assert_eq!(placed(&planned, 2).as_deref(), Some("cpu"));
    let mut plain = load(planned);

    let before = counters();
    for x in [0, 1, 5, 1000, u32::MAX / 4] {
        let on_npu = through(&mut mixed, x);
        assert_eq!(on_npu, through(&mut plain, x), "x = {x}");
        assert_eq!(on_npu, (x + 1) * 2 + 1);
    }
    // Each value went up to the device and came back down, through this side's transfers.
    // The ops themselves are counted inside the library, which has its own copy of the
    // counters: the test for the linked backend checks those.
    let after = counters();
    assert!(after.uploads >= before.uploads + 5);
    assert!(after.downloads >= before.downloads + 5);
}

/// `host.x -> npu.scale -> <middle> -> npu.scale -> host.out`: a middle node between two the
/// device runs, so that running it on the device too crosses between backends least.
fn between_npu_nodes(middle: &str) -> GraphBody {
    let mut graph = graph(middle);
    graph.nodes[1].id = "npu.scale".to_string();
    graph.nodes[3].id = "npu.scale".to_string();
    graph
}

#[test]
fn unsupported_op_refused_and_reassigned() {
    let npu = FakeNpu::new();
    // The middle node has a body for the device and one for the CPU, and the device is where
    // its neighbours are, so the planner gives it to the device first.
    let both = registry(&[&SCALE_NPU_NODE, &ROOT_NPU_NODE, &ROOT_CPU_NODE]);
    let planned = plan(
        &between_npu_nodes("npu.root"),
        &both,
        &[&npu, &CpuBackend],
        &Config::default(),
    )
    .expect("the graph plans");
    // One round in which the device refused the node, and one that assigned it without the
    // device. Its neighbours stayed where they were.
    assert_eq!(planned.regions.rounds, 2);
    assert_eq!(placed(&planned, 1).as_deref(), Some(BACKEND_ID));
    assert_eq!(placed(&planned, 2).as_deref(), Some("cpu"));
    assert_eq!(placed(&planned, 3).as_deref(), Some(BACKEND_ID));
    // 20 doubled is 40, whose root is 6, doubled is 12.
    assert_eq!(through(&mut load(planned), 20), 12);

    // A middle node the device can run takes one round, and all three are one region's.
    let fine = registry(&[&SCALE_NPU_NODE]);
    let planned = plan(
        &between_npu_nodes("npu.scale"),
        &fine,
        &[&npu, &CpuBackend],
        &Config::default(),
    )
    .expect("the graph plans");
    assert_eq!(planned.regions.rounds, 1);
    assert_eq!(placed(&planned, 2).as_deref(), Some(BACKEND_ID));
    assert_eq!(through(&mut load(planned), 3), 24);

    // With no other body, planning fails, and the error names the backend and its reason.
    let only = registry(&[&SCALE_NPU_NODE, &ROOT_NPU_NODE]);
    let error = plan(
        &between_npu_nodes("npu.root"),
        &only,
        &[&npu, &CpuBackend],
        &Config::default(),
    )
    .expect_err("nothing can run the node")
    .to_string();
    assert!(error.contains("npu.root"), "{error}");
    assert!(error.contains("fake-npu: refused"), "{error}");
    assert!(error.contains("the fake NPU has no op `sqrt`"), "{error}");
}

#[test]
fn transfer_path_planned() {
    let nodes = registry(&[&INC_NODE, &SCALE_NPU_NODE]);
    let planned = plan(
        &graph("npu.scale"),
        &nodes,
        &[&FakeNpu::new(), &CpuBackend],
        &Config::default(),
    )
    .expect("the graph plans");
    // CPU to the device and back: one entry of the backend's table each way.
    let used: Vec<&str> = planned
        .stitched
        .transfers
        .iter()
        .map(|transfer| transfer.id.as_str())
        .collect();
    assert_eq!(used, [UPLOAD, DOWNLOAD]);
    let explained = explain_plan(&planned);
    let into = explained
        .edges
        .iter()
        .find(|edge| edge.to_node == 2)
        .unwrap();
    assert_eq!(into.transfer, [UPLOAD]);
    let out_of = explained
        .edges
        .iter()
        .find(|edge| edge.from_node == 2)
        .unwrap();
    assert_eq!(out_of.transfer, [DOWNLOAD]);
    let before = counters();
    assert_eq!(through(&mut load(planned), 1), 5);
    // The device ran one op, at its fixed cost, and the value went up and came down once.
    let after = counters();
    assert!(after.ops > before.ops && after.uploads > before.uploads);
    assert!(after.downloads > before.downloads);
    assert_eq!(after.busy_ns, after.ops * daedalus_fake_npu::OP_COST_NS);

    // The same device with an empty table: planning fails, naming the port and both spaces.
    let error = plan(
        &graph("npu.scale"),
        &nodes,
        &[&FakeNpu::without_transfers(), &CpuBackend],
        &Config::default(),
    )
    .expect_err("nothing reaches the device's memory")
    .to_string();
    assert!(error.contains(HOST) && error.contains(SRAM), "{error}");
    assert!(error.contains("nothing moves a value from"), "{error}");
    assert!(error.contains('x') || error.contains('y'), "{error}");
}

#[test]
fn reload_keeps_backend() {
    let mut loader = Loader::new();
    let first = loader
        .open(copy("reload", "first"))
        .expect("the plugin loads");
    let mut nodes = registry(&[&INC_NODE]);
    first.join(&mut nodes).expect("the plugin's nodes join");
    let npu = PluginNpu::from_plugin(Arc::new(first)).expect("the plugin has the backend");
    let planned = |npu: &PluginNpu| {
        plan(
            &graph("npu.scale"),
            &nodes,
            &[npu, &CpuBackend],
            &Config::default(),
        )
        .expect("the graph plans")
    };
    let mut host = load(planned(&npu));
    assert_eq!(through(&mut host, 2), 7);

    // A reload of the same graph with the same plugin: the node stays on its body.
    let report = host.reload(planned(&npu)).expect("the reload fits");
    assert!(report.carried.contains("middle"), "{report:?}");
    assert_eq!(report.fresh.total(), 0);
    assert_eq!(through(&mut host, 3), 9);

    // The plugin is loaded again: a second generation of the same backend. The node is still
    // on the fake NPU, and the reload reports that its body changed.
    let second = loader
        .open(copy("reload", "second"))
        .expect("the plugin loads again");
    let reloaded = PluginNpu::from_plugin(Arc::new(second)).expect("the plugin has the backend");
    assert_eq!(reloaded.generation(), 2);
    let next = planned(&reloaded);
    assert_eq!(placed(&next, 2).as_deref(), Some(BACKEND_ID));
    let report = host.reload(next).expect("the reload fits");
    assert_eq!(report.fresh.names(), ["middle"]);
    assert!(report.carried.contains("before") && report.carried.contains("after"));
    assert_eq!(through(&mut host, 4), 11);
}

fn rust_and_toml(root: &Path, files: &mut Vec<PathBuf>) {
    let Ok(entries) = std::fs::read_dir(root) else {
        return;
    };
    for entry in entries {
        let path = entry.expect("dir entry").path();
        if path.is_dir() {
            rust_and_toml(&path, files);
        } else if path
            .extension()
            .is_some_and(|ext| ext == "rs" || ext == "toml")
        {
            files.push(path);
        }
    }
}

/// Adding this backend changed nothing in Daedalus: the core and the CPU backend do not name it.
#[test]
fn the_core_and_the_cpu_backend_name_no_npu() {
    let root = Path::new(env!("CARGO_MANIFEST_DIR")).join("../../..");
    let mut files = Vec::new();
    rust_and_toml(&root.join("core"), &mut files);
    rust_and_toml(&root.join("backends/cpu"), &mut files);
    assert!(files.len() > 100, "the sources were found");
    let mut hits = Vec::new();
    for path in files {
        let text = std::fs::read_to_string(&path).expect("read a source file");
        for (at, line) in text.lines().enumerate() {
            let lower = line.to_lowercase();
            // `npu` as a word of its own, so that `input` is not one.
            let npu = lower
                .split(|c: char| !c.is_ascii_alphanumeric())
                .any(|word| word == "npu");
            if npu || lower.contains("fake_npu") || lower.contains("fake-npu") {
                hits.push(format!("{}:{}: {line}", path.display(), at + 1));
            }
        }
    }
    assert!(hits.is_empty(), "{hits:#?}");
}
