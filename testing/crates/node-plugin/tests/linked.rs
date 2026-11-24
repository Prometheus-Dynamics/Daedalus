//! Bytes a node linked into the host makes reach a plugin's node as they are: the planner
//! writes them in the host's memory and hands them to the plugin with no copy, says so in the
//! plan's explanation, and writes their wire form once only when a worker reads them too.

use std::path::PathBuf;
use std::sync::atomic::{AtomicU32, Ordering};
use std::sync::{Arc, Mutex, MutexGuard};

use std::borrow::Cow;

use daedalus_abi::{OwnedStep, Slots, Step, StepCx, StepEnd};
use daedalus_backend::{
    Backend, CompileCx, Compiled, MemorySpace, NodeRef, Offer, Refusal, Region, SpaceKind,
    StepUnit, Transfer, TransferCost, UnitInfo,
};
use daedalus_cpu::CpuBackend;
use daedalus_graph::{EdgeDoc, EdgePolicyDoc, GraphBody, NodeDoc, NodeKindDoc, PortRefDoc};
use daedalus_host::{LoadOptions, LoadedHost};
use daedalus_loader::{
    BYTES_TO_PLUGIN, CPU_HOST, CPU_WIRE, Loader, PLUGIN_SPACE, PluginBackend, TO_PLUGIN,
    bytes_copied,
};
use daedalus_macros::node;
use daedalus_node_plugin::Pair;
use daedalus_planner::explain::{EdgeExplain, explain_plan};
use daedalus_planner::{Config, Planned, plan};
use daedalus_planner_tests::{decl, key, u32_ty};
use daedalus_registry::Registry;

/// Where the bytes `t.bytes` made last are, the low 32 bits of their address.
static MADE_AT: AtomicU32 = AtomicU32::new(0);

/// A list of bytes made by a node linked into this test.
#[node(id = "t.bytes", inputs(x), outputs(bytes))]
fn make_bytes(x: u32) -> Vec<u8> {
    let bytes = vec![x as u8; 4096];
    MADE_AT.store(bytes.as_ptr() as usize as u32, Ordering::SeqCst);
    bytes
}

/// A linked node that reads the bytes too.
#[node(id = "t.len", inputs(bytes), outputs(len))]
fn len_here(bytes: &[u8]) -> u32 {
    bytes.len() as u32
}

/// A value of the plugin's crate with a wire form, made by a linked node.
#[node(id = "t.pair", inputs(x), outputs(pair))]
fn make_pair(x: u32) -> Pair {
    Pair {
        low: x,
        high: x * 2,
    }
}

/// A stand-in for a worker: it reads from its own space, which a value reaches from `cpu.wire`,
/// and its steps do nothing. Only planned, never run.
struct Worker;

const WORKER_SPACE: &str = "w.host";
const TO_WORKER: &str = "w.from_cpu_wire";

struct Nothing;

impl OwnedStep for Nothing {
    fn run(&mut self, _: &mut Slots, _: &mut StepCx) -> StepEnd {
        StepEnd::Done
    }
}

fn nothing(_: &CompileCx<'_>) -> Result<Step, Refusal> {
    Ok(Step::boxed(Nothing))
}

impl Backend for Worker {
    fn id(&self) -> &str {
        "w"
    }

    fn memory(&self) -> Vec<MemorySpace> {
        vec![MemorySpace {
            id: Cow::Borrowed(WORKER_SPACE),
            kind: SpaceKind::Host,
            readable: true,
        }]
    }

    fn transfers(&self) -> Vec<Transfer> {
        vec![Transfer {
            id: TO_WORKER,
            key: None,
            from: Cow::Borrowed(CPU_WIRE),
            to: Cow::Borrowed(WORKER_SPACE),
            cost: TransferCost::Handoff,
            make: nothing,
        }]
    }

    fn can_take(&self, offer: &Offer<'_>) -> Vec<NodeRef> {
        offer
            .nodes
            .iter()
            .map(|node| NodeRef {
                node: node.node,
                body: node.body,
            })
            .collect()
    }

    fn compile(&self, region: &Region<'_>, _: &CompileCx<'_>) -> Result<Compiled, Refusal> {
        let unit = |node: &daedalus_types::NodeIndex| StepUnit {
            step: Step::boxed(Nothing),
            info: UnitInfo {
                covers: vec![*node],
                reads: region
                    .inputs
                    .iter()
                    .filter(|at| at.node == *node)
                    .copied()
                    .collect(),
                writes: region
                    .outputs
                    .iter()
                    .filter(|at| at.node == *node)
                    .copied()
                    .collect(),
                ..UnitInfo::default()
            },
            patch: None,
        };
        Ok(Compiled::Steps(region.nodes.iter().map(unit).collect()))
    }
}

/// The tests that run `t.bytes` read where it put its bytes, so they run one at a time.
fn serial() -> MutexGuard<'static, ()> {
    static SERIAL: Mutex<()> = Mutex::new(());
    SERIAL
        .lock()
        .unwrap_or_else(|poisoned| poisoned.into_inner())
}

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

/// A graph of `nodes` after a host bridge with the u32 ports `ports`, joined by `edges`.
fn graph(ports: &[&str], nodes: &[&str], edges: Vec<EdgeDoc>) -> GraphBody {
    let mut bridge = NodeDoc::new("host");
    bridge.kind = NodeKindDoc::HostBridge;
    for port in ports {
        bridge
            .port_types
            .insert((*port).to_string(), "u32".to_string());
    }
    let mut docs = vec![bridge];
    docs.extend(nodes.iter().map(|id| NodeDoc::new(*id)));
    GraphBody {
        nodes: docs,
        edges,
        groups: Vec::new(),
    }
}

/// `graph` planned with the plugin's nodes from its library, this test's nodes linked, and
/// `more` backends after the CPU backend and the plugin's.
fn mixed(graph: &GraphBody, more: &[&dyn Backend], registry: Registry) -> Planned {
    let plugin = Arc::new(Loader::new().open(library()).expect("the plugin loads"));
    let mut registry = registry;
    plugin.join(&mut registry).expect("the plugin's nodes join");
    registry
        .register_all(&[&MAKE_BYTES_NODE, &LEN_HERE_NODE, &MAKE_PAIR_NODE])
        .expect("the linked nodes register");
    let backend = PluginBackend::new(plugin, 0);
    let mut backends: Vec<&dyn Backend> = vec![&CpuBackend, &backend];
    backends.extend_from_slice(more);
    plan(graph, &registry, &backends, &Config::default()).expect("the graph plans")
}

/// The explanation of the edge from node `from` to node `to`.
fn edge_of(planned: &Planned, from: u32, to: u32) -> EdgeExplain {
    explain_plan(planned)
        .edges
        .into_iter()
        .find(|edge| edge.from_node == from && edge.to_node == to)
        .expect("the edge")
}

fn spaces(edge: &EdgeExplain) -> (Vec<&str>, Option<&str>, Option<&str>) {
    (
        edge.transfer.iter().map(String::as_str).collect(),
        edge.written_in.as_deref(),
        edge.read_in.as_deref(),
    )
}

fn load(planned: Planned) -> LoadedHost {
    LoadedHost::load(planned, LoadOptions::default()).expect("the plan loads")
}

fn tick(host: &mut LoadedHost, x: u32, outputs: &[&str]) -> Vec<Option<u32>> {
    let io = host.host();
    io.feed_inline("x", &x.to_le_bytes()).expect("a feed");
    io.step().expect("a tick");
    outputs
        .iter()
        .map(|port| io.take_copy::<u32>(port).expect("a take"))
        .collect()
}

#[test]
fn a_linked_nodes_bytes_reach_a_plugins_node_where_they_are() {
    let _serial = serial();
    // `host.x -> t.bytes -> np.at -> host.out`
    let edges = vec![
        edge(0, "x", 1, "x"),
        edge(1, "bytes", 2, "bytes"),
        edge(2, "y", 0, "out"),
    ];
    let planned = mixed(
        &graph(&["x", "out"], &["t.bytes", "np.at"], edges),
        &[],
        Registry::new(),
    );
    assert_eq!(
        spaces(&edge_of(&planned, 1, 2)),
        (vec![BYTES_TO_PLUGIN], Some(CPU_HOST), Some(PLUGIN_SPACE))
    );
    let mut host = load(planned);
    let copied = bytes_copied();
    for x in [1u32, 2, 3] {
        let at = tick(&mut host, x, &["out"]);
        assert_eq!(at, [Some(MADE_AT.load(Ordering::SeqCst))], "x = {x}");
    }
    assert_eq!(bytes_copied(), copied, "nothing is copied");
}

#[test]
fn bytes_a_plugin_and_a_linked_node_read_are_written_once_in_the_hosts_memory() {
    let _serial = serial();
    // `t.bytes` fans out to the plugin's `np.at` and the linked `t.len`.
    let edges = vec![
        edge(0, "x", 1, "x"),
        edge(1, "bytes", 2, "bytes"),
        edge(1, "bytes", 3, "bytes"),
        edge(2, "y", 0, "out"),
        edge(3, "len", 0, "len"),
    ];
    let planned = mixed(
        &graph(&["x", "out", "len"], &["t.bytes", "np.at", "t.len"], edges),
        &[],
        Registry::new(),
    );
    assert_eq!(
        spaces(&edge_of(&planned, 1, 2)),
        (vec![BYTES_TO_PLUGIN], Some(CPU_HOST), Some(PLUGIN_SPACE))
    );
    assert_eq!(
        spaces(&edge_of(&planned, 1, 3)),
        (vec![], Some(CPU_HOST), Some(CPU_HOST))
    );
    let mut host = load(planned);
    for x in [4u32, 5] {
        let got = tick(&mut host, x, &["out", "len"]);
        assert_eq!(got, [Some(MADE_AT.load(Ordering::SeqCst)), Some(4096)]);
    }
}

#[test]
fn bytes_a_plugin_and_a_worker_read_are_written_once_in_their_wire_form() {
    // A worker reads from `cpu.wire`, so the bytes are written there, once, and the plugin
    // reads that same form: a copy for the plugin alone would add a second.
    let mut registry = Registry::new();
    decl("w.read")
        .input("bytes", key("bytes"))
        .output("n", u32_ty())
        .body("w")
        .add(&mut registry);
    let edges = vec![
        edge(0, "x", 1, "x"),
        edge(1, "bytes", 2, "bytes"),
        edge(1, "bytes", 3, "bytes"),
        edge(2, "y", 0, "out"),
        edge(3, "n", 0, "n"),
    ];
    let planned = mixed(
        &graph(&["x", "out", "n"], &["t.bytes", "np.at", "w.read"], edges),
        &[&Worker],
        registry,
    );
    assert_eq!(
        spaces(&edge_of(&planned, 1, 2)),
        (vec![TO_PLUGIN], Some(CPU_WIRE), Some(PLUGIN_SPACE))
    );
    assert_eq!(
        spaces(&edge_of(&planned, 1, 3)),
        (vec![TO_WORKER], Some(CPU_WIRE), Some(WORKER_SPACE))
    );
}

#[test]
fn any_other_value_reaches_a_plugin_in_its_wire_form() {
    // Only bytes are the same in the host's memory and in a cell: any other value is written in
    // its wire form for the plugin.
    let edges = vec![
        edge(0, "x", 1, "x"),
        edge(1, "pair", 2, "pair"),
        edge(2, "y", 0, "out"),
    ];
    let planned = mixed(
        &graph(&["x", "out"], &["t.pair", "np.join"], edges),
        &[],
        Registry::new(),
    );
    assert_eq!(
        spaces(&edge_of(&planned, 1, 2)),
        (vec![TO_PLUGIN], Some(CPU_WIRE), Some(PLUGIN_SPACE))
    );
    let mut host = load(planned);
    assert_eq!(tick(&mut host, 5, &["out"]), [Some(15)]);
}
