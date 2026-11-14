//! The measurements next's `cpu-timings` makes, against the Daedalus the dev image ships
//! (dev at 02e2b3b), with the same sections and the same words in its output, so one judge and
//! one table read both: `host_round_trip`, `probe_off` and `probe_on` (a sixteen-stage chain
//! with its metrics off and at their default), `startup`, and `allocations`.

use std::alloc::{GlobalAlloc, Layout, System};
use std::cell::Cell;
use std::time::Instant;

use daedalus_data::model::{TypeExpr, Value, ValueType};
use daedalus_engine::{Engine, EngineConfig, HostGraph};
use daedalus_planner::{Edge, Graph, NodeInstance};
use daedalus_registry::capability::{NodeDecl, PortDecl};
use daedalus_runtime::RuntimeNode;
use daedalus_runtime::executor::{MetricsLevel, NodeError, NodeHandler};
use daedalus_runtime::host_bridge::{HOST_BRIDGE_ID, HOST_BRIDGE_META_KEY, HostBridgeManager};
use daedalus_runtime::plugins::PluginRegistry;
use daedalus_transport::Payload;

struct Counting;

thread_local! {
    static ALLOCATIONS: Cell<usize> = const { Cell::new(0) };
}

// SAFETY: every call goes to the system allocator, and the count is this thread's own.
unsafe impl GlobalAlloc for Counting {
    unsafe fn alloc(&self, layout: Layout) -> *mut u8 {
        let _ = ALLOCATIONS.try_with(|count| count.set(count.get() + 1));
        // SAFETY: the caller's contract is the system allocator's.
        unsafe { System.alloc(layout) }
    }
    unsafe fn dealloc(&self, ptr: *mut u8, layout: Layout) {
        // SAFETY: the caller's contract is the system allocator's.
        unsafe { System.dealloc(ptr, layout) }
    }
    unsafe fn realloc(&self, ptr: *mut u8, layout: Layout, new_size: usize) -> *mut u8 {
        let _ = ALLOCATIONS.try_with(|count| count.set(count.get() + 1));
        // SAFETY: the caller's contract is the system allocator's.
        unsafe { System.realloc(ptr, layout, new_size) }
    }
}

#[global_allocator]
static ALLOCATOR: Counting = Counting;

const INT_KEY: &str = "typeexpr:{\"Scalar\":\"Int\"}";
const PER_BATCH: u32 = 1_000;
const WARM_UP: u32 = 20;

struct Increment;

impl NodeHandler for Increment {
    fn run(
        &self,
        node: &RuntimeNode,
        _ctx: &daedalus_runtime::state::ExecutionContext,
        io: &mut daedalus_runtime::io::NodeIo,
    ) -> Result<(), NodeError> {
        if node.id == "inc" {
            let Some(Value::Int(value)) = io.get_typed_ref::<Value>("in") else {
                return Err(NodeError::InvalidInput("expected int".to_string()));
            };
            let next = Value::Int(value + 1);
            io.push_payload("out", Payload::owned(INT_KEY, next));
        }
        Ok(())
    }
}

fn node(id: &str, label: &str, inputs: &[&str], outputs: &[&str]) -> NodeInstance {
    NodeInstance::new(id)
        .with_label(label)
        .with_inputs(inputs.iter().copied())
        .with_outputs(outputs.iter().copied())
}

/// `host.in -> inc x stages -> host.out`.
fn compile(stages: usize, level: MetricsLevel) -> HostGraph<Increment> {
    let int_ty = TypeExpr::Scalar(ValueType::Int);
    let mut plugins = PluginRegistry::new();
    plugins
        .register_node_decl(
            NodeDecl::new(HOST_BRIDGE_ID)
                .metadata(HOST_BRIDGE_META_KEY, Value::Bool(true))
                .input(PortDecl::new("out", INT_KEY).schema(int_ty.clone()))
                .output(PortDecl::new("in", INT_KEY).schema(int_ty.clone())),
        )
        .expect("register host decl");
    plugins
        .register_node_decl(
            NodeDecl::new("inc")
                .input(PortDecl::new("in", INT_KEY).schema(int_ty.clone()))
                .output(PortDecl::new("out", INT_KEY).schema(int_ty)),
        )
        .expect("register inc decl");
    let mut host = node(HOST_BRIDGE_ID, "host", &["out"], &["in"]);
    host.metadata
        .insert(HOST_BRIDGE_META_KEY.to_string(), Value::Bool(true));
    let mut nodes = vec![host];
    let mut edges = Vec::new();
    for stage in 0..stages {
        nodes.push(node("inc", &format!("s{stage}"), &["in"], &["out"]));
        let (from, port) = if stage == 0 { (0, "in") } else { (stage, "out") };
        edges.push(Edge::new(from, port, stage + 1, "in"));
    }
    edges.push(Edge::new(stages, "out", 0, "out"));
    let graph = Graph {
        nodes,
        edges,
        metadata: Default::default(),
    };
    let mut host_graph = Engine::new(EngineConfig::default().with_metrics_level(level))
        .expect("engine")
        .compile_host_graph_plugin_registry(
            &plugins,
            graph,
            Increment,
            HostBridgeManager::new(),
            "host",
        )
        .expect("compile host graph");
    host_graph.prepare().expect("prepare");
    host_graph.host().set_event_recording(false);
    host_graph
}

fn round_trip(graph: &mut HostGraph<Increment>, value: i64) -> i64 {
    graph.push_payload("in", Payload::owned(INT_KEY, Value::Int(value)));
    graph.tick().expect("tick");
    match graph.take_payload("out").as_ref().and_then(|p| p.get_ref::<Value>()) {
        Some(Value::Int(out)) => *out,
        _ => panic!("no output"),
    }
}

fn per_call(batches: usize, mut op: impl FnMut()) -> Vec<f64> {
    for _ in 0..WARM_UP {
        for _ in 0..PER_BATCH {
            op();
        }
    }
    let mut times = Vec::with_capacity(batches);
    for _ in 0..batches {
        let start = Instant::now();
        for _ in 0..PER_BATCH {
            op();
        }
        times.push(start.elapsed().as_nanos() as f64 / f64::from(PER_BATCH));
    }
    times.sort_by(f64::total_cmp);
    times
}

fn report(section: &str, times: &[f64]) {
    println!(
        "{section} median_ns {:.1} min_ns {:.1} max_ns {:.1} batches {} calls_per_batch {PER_BATCH}",
        times[times.len() / 2],
        times[0],
        times[times.len() - 1],
        times.len()
    );
}

fn timed(section: &str, stages: usize, level: MetricsLevel, batches: usize) {
    let mut graph = compile(stages, level);
    let mut value = 0_i64;
    let times = per_call(batches, || {
        value = value.wrapping_add(1);
        assert_eq!(round_trip(&mut graph, value), value.wrapping_add(stages as i64));
    });
    report(section, &times);
}

fn status_kb(name: &str) -> String {
    std::fs::read_to_string("/proc/self/status")
        .ok()
        .and_then(|status| {
            let line = status.lines().find_map(|line| line.strip_prefix(name))?.to_string();
            line.trim_start_matches(':').split_whitespace().next().map(str::to_string)
        })
        .unwrap_or_else(|| "?".to_string())
}

fn startup(started: Instant) {
    let at_start = status_kb("VmRSS");
    let mut graph = compile(1, MetricsLevel::Off);
    let loaded_ns = started.elapsed().as_nanos();
    assert_eq!(round_trip(&mut graph, 1), 2);
    let first_step_ns = started.elapsed().as_nanos();
    let loaded = status_kb("VmRSS");
    for value in 0..10_000 {
        round_trip(&mut graph, value);
    }
    println!(
        "startup planned_ns {loaded_ns} loaded_ns {loaded_ns} first_step_ns {first_step_ns} \
         rss_at_start_kb {at_start} rss_loaded_kb {loaded} rss_after_10000_ticks_kb {} rss_peak_kb {}",
        status_kb("VmRSS"),
        status_kb("VmHWM")
    );
}

fn allocations() {
    for (name, stages) in [("one_step", 1usize), ("sixteen_steps", 16)] {
        let mut graph = compile(stages, MetricsLevel::Off);
        for value in 0..1_000 {
            round_trip(&mut graph, value);
        }
        let before = ALLOCATIONS.with(Cell::get);
        for value in 0..1_000 {
            round_trip(&mut graph, value);
        }
        let made = ALLOCATIONS.with(Cell::get) - before;
        println!("allocations {name} per_1000_round_trips {made}");
    }
}

fn main() {
    let started = Instant::now();
    let args: Vec<String> = std::env::args().skip(1).collect();
    let section = args.first().map(String::as_str).unwrap_or("");
    let batches = args.get(1).map_or(101, |text| text.parse().expect("BATCHES is a number"));
    match section {
        "host_round_trip" => timed("host_round_trip", 1, MetricsLevel::Off, batches),
        "probe_off" => timed("probe_off", 16, MetricsLevel::Off, batches),
        "probe_on" => timed("probe_every_1", 16, MetricsLevel::default(), batches),
        "startup" => startup(started),
        "allocations" => allocations(),
        _ => {
            eprintln!("usage: dev-measure host_round_trip|probe_off|probe_on|startup|allocations [BATCHES]");
            std::process::exit(2);
        }
    }
}
