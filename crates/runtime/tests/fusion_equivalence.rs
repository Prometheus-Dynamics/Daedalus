//! Node fusion changes how payloads travel, never what nodes see: random DAGs (chains, fan-out,
//! fan-in, conditional and repeated outputs, optional and required inputs, `fire = "all"`,
//! state, failing nodes, latest-only edges, opted-out nodes) run tick by tick with fusion on and
//! off, and every node call's inputs, the run results and the per-node metrics must match.

use std::collections::{BTreeMap, HashMap};
use std::sync::Arc;
use std::sync::atomic::{AtomicU64, Ordering};

use daedalus_data::model::Value;
use daedalus_planner::{Edge, ExecutionPlan, Graph, NodeInstance};
use daedalus_runtime::executor::{
    EdgeMetrics, EdgeTickSample, ExecutionTelemetry, FrameProbe, FrameTickSample, MetricsLevel,
};
use daedalus_runtime::sync::Mutex;
use daedalus_runtime::{
    FusionBlock, NODE_FIRE_META_KEY, NODE_FUSION_META_KEY, NODE_REQUIRED_INPUTS_META_KEY,
    NodeError, NodeHandler, OwnedExecutor, RuntimeNode, RuntimePlan, SchedulerConfig,
    build_runtime,
};
use daedalus_transport::Payload;
use rand::rngs::StdRng;
use rand::{RngExt, SeedableRng};

/// What a scripted node does on each call.
#[derive(Clone, Copy, Debug, Default)]
struct Behaviour {
    /// Skip `out0` when `(tick + value) % skip == 0` (a conditional output).
    skip: u64,
    /// Push `out0` twice.
    double: bool,
    /// Fail on this tick.
    fail_on: Option<u64>,
}

/// One node call: node, tick, the inputs it saw (in order).
type Call = (String, u64, Vec<(String, i64)>);

#[derive(Default)]
struct Script {
    tick: AtomicU64,
    behaviours: HashMap<String, Behaviour>,
    calls: Mutex<Vec<Call>>,
    state: Mutex<HashMap<String, i64>>,
}

struct ScriptHandler(Arc<Script>);

impl NodeHandler for ScriptHandler {
    fn run(
        &self,
        node: &RuntimeNode,
        _ctx: &daedalus_runtime::state::ExecutionContext,
        io: &mut daedalus_runtime::io::NodeIo,
    ) -> Result<(), NodeError> {
        let script = &self.0;
        let tick = script.tick.load(Ordering::SeqCst);
        let inputs: Vec<(String, i64)> = io
            .inputs()
            .map(|(port, payload)| {
                let value = *payload.inner.get_ref::<i64>().expect("i64 input");
                (port.to_string(), value)
            })
            .collect();
        script
            .calls
            .lock()
            .push((node.id.clone(), tick, inputs.clone()));
        let behaviour = script.behaviours.get(&node.id).copied().unwrap_or_default();
        if behaviour.fail_on == Some(tick) {
            return Err(NodeError::Handler(format!(
                "{} failed on tick {tick}",
                node.id
            )));
        }
        let calls = {
            let mut state = script.state.lock();
            let calls = state.entry(node.id.clone()).or_default();
            *calls += 1;
            *calls
        };
        let sum: i64 = inputs.iter().map(|(_, value)| value).sum();
        let value = sum.wrapping_mul(31).wrapping_add(tick as i64 * 7 + calls);
        let push = |io: &mut daedalus_runtime::io::NodeIo, port: &'static str, value: i64| {
            io.push_payload(port, Payload::owned("i64", value));
        };
        if behaviour.skip == 0 || !(tick + value.unsigned_abs()).is_multiple_of(behaviour.skip) {
            push(io, "out0", value);
            if behaviour.double {
                push(io, "out0", value + 1);
            }
        }
        push(io, "out1", value * 3);
        Ok(())
    }
}

fn strs(items: &[&str]) -> Value {
    Value::List(
        items
            .iter()
            .map(|item| Value::String((*item).to_string().into()))
            .collect(),
    )
}

/// A random DAG of `nodes` scripted nodes: a backbone `out0 -> in0` chain (fusable) plus random
/// edges that add fan-out, fan-in and skipped links.
fn random_graph(seed: u64, nodes: usize) -> (Graph, HashMap<String, Behaviour>) {
    let mut rng = StdRng::seed_from_u64(seed);
    let mut graph = Graph::default();
    let mut behaviours = HashMap::new();
    for idx in 0..nodes {
        let id = format!("n{idx}");
        let mut node = NodeInstance::new(id.clone())
            .with_inputs(["in0", "in1", "in2"])
            .with_outputs(["out0", "out1"]);
        let required: Vec<&str> = ["in0", "in1"]
            .into_iter()
            .filter(|_| rng.random_range(0..3) == 0)
            .collect();
        if !required.is_empty() {
            node = node.with_metadata(NODE_REQUIRED_INPUTS_META_KEY, strs(&required));
            if rng.random_range(0..4) == 0 {
                node = node.with_metadata(NODE_FIRE_META_KEY, Value::String("all".into()));
            }
        }
        if rng.random_range(0..10) == 0 {
            node = node.with_metadata(NODE_FUSION_META_KEY, Value::Bool(false));
        }
        graph.nodes.push(node);
        behaviours.insert(
            id,
            Behaviour {
                skip: [0, 0, 2, 3][rng.random_range(0..4)],
                double: rng.random_range(0..5) == 0,
                fail_on: (rng.random_range(0..8) == 0).then(|| rng.random_range(0..6)),
            },
        );
    }
    let mut edge = |from: usize, out: &str, to: usize, input: &str, rng: &mut StdRng| {
        let mut edge = Edge::new(from, out, to, input);
        if rng.random_range(0..5) == 0 {
            edge = edge.with_metadata(
                "daedalus.edge.pressure",
                Value::String("latest_only".into()),
            );
        }
        graph.edges.push(edge);
    };
    for idx in 1..nodes {
        if rng.random_range(0..6) != 0 {
            edge(idx - 1, "out0", idx, "in0", &mut rng);
        }
    }
    for _ in 0..nodes {
        let from = rng.random_range(0..nodes - 1);
        let to = rng.random_range(from + 1..nodes);
        let out = ["out0", "out1", "out1"][rng.random_range(0..3)];
        let input = ["in0", "in1", "in2", "in2"][rng.random_range(0..4)];
        edge(from, out, to, input, &mut rng);
    }
    (graph, behaviours)
}

fn plan(graph: Graph) -> Arc<RuntimePlan> {
    Arc::new(build_runtime(
        &ExecutionPlan::new(graph, vec![]),
        &SchedulerConfig::default(),
    ))
}

#[derive(Clone, Copy, Debug)]
enum Mode {
    Serial,
    Parallel,
    Adaptive,
}

/// Everything observable about one tick, made comparable across fused and unfused runs.
#[derive(Debug, PartialEq)]
struct TickOutcome {
    result: Result<(), String>,
    nodes_executed: usize,
    errors: Vec<(String, String)>,
    /// Per node: calls, and inputs and outputs moved (detailed metrics).
    calls: BTreeMap<usize, (usize, u64, u64)>,
    pressure_events: BTreeMap<usize, u64>,
}

fn outcome(result: Result<ExecutionTelemetry, daedalus_runtime::ExecuteError>) -> TickOutcome {
    // Parallel failures name their segment, which fusion renumbers: keep the node part.
    let node = |id: &str| id.rsplit(':').next().unwrap_or(id).to_string();
    match result {
        Ok(telemetry) => {
            let mut errors: Vec<_> = telemetry
                .errors
                .iter()
                .map(|failure| (node(&failure.node_id), failure.code.clone()))
                .collect();
            errors.sort();
            TickOutcome {
                result: Ok(()),
                nodes_executed: telemetry.nodes_executed,
                errors,
                calls: telemetry
                    .node_metrics
                    .iter()
                    .map(|(idx, metrics)| {
                        let transport = metrics.transport.as_ref();
                        let moved = transport.map_or((0, 0), |t| (t.in_count, t.out_count));
                        (idx, (metrics.calls, moved.0, moved.1))
                    })
                    .collect(),
                pressure_events: telemetry
                    .edge_metrics
                    .iter()
                    .map(|(&edge, metrics): (&usize, &EdgeMetrics)| {
                        (edge, metrics.pressure_events.total)
                    })
                    .filter(|(_, events)| *events > 0)
                    .collect(),
            }
        }
        Err(error) => TickOutcome {
            result: Err(error.to_string()),
            nodes_executed: 0,
            errors: Vec::new(),
            calls: BTreeMap::new(),
            pressure_events: BTreeMap::new(),
        },
    }
}

struct Run {
    ticks: Vec<TickOutcome>,
    calls: Vec<Call>,
}

fn run(
    plan: &Arc<RuntimePlan>,
    behaviours: &HashMap<String, Behaviour>,
    fusion: bool,
    mode: Mode,
    fail_fast: bool,
    metrics: MetricsLevel,
) -> Run {
    let script = Arc::new(Script {
        behaviours: behaviours.clone(),
        ..Script::default()
    });
    let mut exec = OwnedExecutor::new(plan.clone(), ScriptHandler(script.clone()))
        .with_node_fusion(fusion)
        .with_fail_fast(fail_fast)
        .with_metrics_level(metrics)
        .with_pool_size(Some(3));
    let ticks = (0..6)
        .map(|tick| {
            script.tick.store(tick, Ordering::SeqCst);
            outcome(match mode {
                Mode::Serial => exec.run_in_place(),
                Mode::Parallel => exec.run_parallel_in_place(),
                Mode::Adaptive => exec.run_adaptive_in_place(),
            })
        })
        .collect();
    let mut calls = std::mem::take(&mut *script.calls.lock());
    if !matches!(mode, Mode::Serial) {
        // Independent segments interleave on workers: compare each node's own calls.
        calls.sort_by(|a, b| (&a.0, a.1).cmp(&(&b.0, b.1)));
    }
    Run { ticks, calls }
}

#[test]
fn fused_and_unfused_runs_match_on_random_graphs() {
    let mut fused_edges = 0;
    let mut blocked = BTreeMap::<String, usize>::new();
    for seed in 0..60u64 {
        let (graph, behaviours) = random_graph(seed, 4 + (seed as usize % 9));
        let plan = plan(graph);
        let explanation = plan.explain();
        fused_edges += explanation.edges.iter().filter(|edge| edge.fused).count();
        for edge in &explanation.edges {
            if let Some(block) = &edge.fusion_block {
                let name = format!("{block:?}");
                *blocked
                    .entry(name.split('(').next().unwrap_or_default().to_string())
                    .or_default() += 1;
            }
        }
        let modes: &[(Mode, bool)] = &[
            (Mode::Serial, true),
            (Mode::Serial, false),
            (Mode::Parallel, false),
            (Mode::Adaptive, false),
        ];
        for &(mode, fail_fast) in modes {
            for metrics in [MetricsLevel::Basic, MetricsLevel::Detailed] {
                let context = format!("seed {seed}, {mode:?}, fail_fast {fail_fast}, {metrics:?}");
                let fused = run(&plan, &behaviours, true, mode, fail_fast, metrics);
                let unfused = run(&plan, &behaviours, false, mode, fail_fast, metrics);
                assert_eq!(fused.calls, unfused.calls, "node calls differ: {context}");
                assert_eq!(
                    fused.ticks, unfused.ticks,
                    "tick outcomes differ: {context}"
                );
            }
        }
    }
    assert!(
        fused_edges > 60,
        "random graphs fused only {fused_edges} edges"
    );
    for reason in [
        "SourceFanOut",
        "TargetFanIn",
        "FireAll",
        "OptedOut",
        "NotAdjacent",
    ] {
        assert!(
            blocked.contains_key(reason),
            "no edge blocked by {reason}: {blocked:?}"
        );
    }
}

/// `a -> b -> c -> d`, plus `b.out1` fanned out to `c` and `d`.
fn chain() -> Graph {
    let mut graph = Graph::default();
    for id in ["a", "b", "c", "d"] {
        graph.nodes.push(
            NodeInstance::new(id)
                .with_inputs(["in0", "in1", "in2"])
                .with_outputs(["out0", "out1"]),
        );
    }
    graph.edges.extend([
        Edge::new(0, "out0", 1, "in0"),
        Edge::new(1, "out0", 2, "in0"),
        Edge::new(2, "out0", 3, "in0"),
        Edge::new(1, "out1", 2, "in1"),
        Edge::new(1, "out1", 3, "in1"),
    ]);
    graph
}

#[test]
fn chain_runs_as_one_fused_unit() {
    let plan = plan(chain());
    let explanation = plan.explain();
    let fused: Vec<usize> = explanation
        .edges
        .iter()
        .filter(|edge| edge.fused)
        .map(|edge| edge.index)
        .collect();
    assert_eq!(fused, vec![0, 1, 2]);
    assert_eq!(explanation.fused_units.len(), 1);
    assert_eq!(explanation.fused_units[0].node_ids, ["a", "b", "c", "d"]);
    assert_eq!(explanation.fused_units[0].edges, vec![0, 1, 2]);
    assert_eq!(
        explanation.edges[3].fusion_block,
        Some(FusionBlock::SourceFanOut(2))
    );

    let script = Arc::new(Script::default());
    let mut exec = OwnedExecutor::new(plan.clone(), ScriptHandler(script.clone()))
        .with_metrics_level(MetricsLevel::Detailed);
    let probe = Arc::new(FrameProbe::for_plan(&plan));
    exec.set_frame_probe(Some(probe.clone()));
    for tick in 0..3 {
        script.tick.store(tick, Ordering::SeqCst);
        let telemetry = exec.run_in_place().expect("tick");
        assert_eq!(telemetry.nodes_executed, 4);
        let mut sample = FrameTickSample::default();
        let mut edges = vec![EdgeTickSample::default(); plan.edges.len()];
        probe.take_tick(&mut sample, &mut edges);
        assert_eq!(sample.fused_handoffs, 3);
        for (edge, row) in edges.iter().enumerate() {
            let fused = u64::from(edge < 3);
            assert_eq!(row.fused, fused, "edge {edge}");
            assert_eq!(
                row.wait_ns == 0,
                edge < 3,
                "only unfused edges queue: edge {edge}"
            );
            if cfg!(feature = "metrics") {
                let metrics = telemetry.edge_metrics.get(&edge);
                assert_eq!(
                    metrics.map_or(0, |m| m.fused_handoffs),
                    fused,
                    "edge {edge}"
                );
            }
        }
    }

    let mut graph = chain();
    graph.nodes[2] = graph.nodes[2]
        .clone()
        .with_metadata(NODE_FUSION_META_KEY, Value::Bool(false));
    let explanation = self::plan(graph).explain();
    assert_eq!(
        explanation.edges[1].fusion_block,
        Some(FusionBlock::OptedOut)
    );
    assert_eq!(explanation.fused_units[0].node_ids, ["a", "b"]);
}

/// A payload a fused edge's slot kept while its consumer was inactive is delivered before the
/// next one, as without fusion.
#[test]
fn held_slot_payloads_survive_fusion() {
    let plan = plan(chain());
    let outcome = |fusion: bool| {
        let script = Arc::new(Script::default());
        let mut exec = OwnedExecutor::new(plan.clone(), ScriptHandler(script.clone()))
            .with_node_fusion(fusion);
        for tick in 0..4 {
            script.tick.store(tick, Ordering::SeqCst);
            let mask = (tick == 1).then(|| Arc::new(vec![true, true, false, true]));
            exec.set_active_nodes_mask(mask);
            exec.run_in_place().expect("tick");
        }
        std::mem::take(&mut *script.calls.lock())
    };
    let fused = outcome(true);
    assert_eq!(fused, outcome(false));
    let held: Vec<_> = fused
        .iter()
        .filter(|(node, tick, _)| node == "c" && *tick == 2)
        .flat_map(|(_, _, inputs)| inputs.iter().filter(|(port, _)| port == "in0"))
        .collect();
    assert_eq!(
        held.len(),
        2,
        "c sees tick 1's held payload and tick 2's: {fused:?}"
    );
}
