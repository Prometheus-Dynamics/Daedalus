//! Output reuse for reusable nodes (`#[node(reusable)]`): a node whose inputs and constants are
//! unchanged replays its last outputs instead of running its handler.

use std::collections::HashMap;
use std::sync::Arc;
use std::sync::atomic::{AtomicU32, Ordering};
use std::time::Duration;

use daedalus_data::model::Value;
use daedalus_planner::{Edge, ExecutionPlan, Graph, NodeInstance};
use daedalus_runtime::executor::OwnedExecutor;
use daedalus_runtime::io::NodeIo;
use daedalus_runtime::state::ExecutionContext;
use daedalus_runtime::{
    NODE_REQUIRED_INPUTS_META_KEY, NODE_REUSABLE_META_KEY, NodeError, NodeHandler, RuntimeNode,
    SchedulerConfig, build_runtime,
};
use daedalus_transport::Payload;

use daedalus_runtime::sync::Mutex;

/// `src` (not reusable) pushes the current source value, re-sending the same payload while the
/// value is unchanged, as a host or a held input would. `pure` (reusable) and `pure2` count their
/// calls and fail the calls they are told to; `sink` records what arrives.
struct Fixture {
    source: Mutex<(i32, Option<Payload>)>,
    calls: Mutex<HashMap<String, u32>>,
    failures: Mutex<HashMap<String, u32>>,
    sink: Mutex<Vec<i32>>,
    /// Optional second source for `opt`: pushed only while set.
    opt: Mutex<Option<i32>>,
    /// Counts `src` runs, so a test can tell whether the source ran.
    src_runs: AtomicU32,
}

impl Fixture {
    fn new() -> Arc<Self> {
        Arc::new(Self {
            source: Mutex::new((0, None)),
            calls: Mutex::new(HashMap::new()),
            failures: Mutex::new(HashMap::new()),
            sink: Mutex::new(Vec::new()),
            opt: Mutex::new(None),
            src_runs: AtomicU32::new(0),
        })
    }

    fn set_value(&self, value: i32) {
        *self.source.lock() = (value, None);
    }

    fn calls(&self, node: &str) -> u32 {
        self.calls.lock().get(node).copied().unwrap_or(0)
    }

    fn fail_next(&self, node: &str, times: u32) {
        self.failures.lock().insert(node.to_string(), times);
    }
}

struct Handler(Arc<Fixture>);

impl NodeHandler for Handler {
    fn run(
        &self,
        node: &RuntimeNode,
        _ctx: &ExecutionContext,
        io: &mut NodeIo,
    ) -> Result<(), NodeError> {
        let fx = &self.0;
        match node.id.as_str() {
            "src" => {
                fx.src_runs.fetch_add(1, Ordering::Relaxed);
                let mut source = fx.source.lock();
                let (value, held) = &mut *source;
                let payload = match held {
                    Some(payload) => payload.clone(),
                    None => {
                        let fresh = Payload::owned("test:i32", *value);
                        *held = Some(fresh.clone());
                        fresh
                    }
                };
                io.push_payload("out", payload);
            }
            "opt_src" => {
                if let Some(value) = *fx.opt.lock() {
                    io.push_payload("out", Payload::owned("test:i32", value));
                }
            }
            "pure" | "pure2" => {
                *fx.calls.lock().entry(node.id.clone()).or_insert(0) += 1;
                {
                    let mut failures = fx.failures.lock();
                    if let Some(left) = failures.get_mut(node.id.as_str())
                        && *left > 0
                    {
                        *left -= 1;
                        return Err(NodeError::Handler("told to fail".into()));
                    }
                }
                let value = io.get_typed::<i32>("in").unwrap_or(0);
                let factor = if node.id == "pure" { 10 } else { 2 };
                let extra = io.get_typed::<i32>("opt").unwrap_or(0);
                io.push_payload("out", Payload::owned("test:i32", value * factor + extra));
            }
            "sink" => {
                if let Some(value) = io.get_typed::<i32>("in") {
                    fx.sink.lock().push(value);
                }
            }
            _ => {}
        }
        Ok(())
    }
}

fn reusable() -> Value {
    Value::Bool(true)
}

/// `src -> pure -> sink`, with `pure` reusable.
fn one_pure() -> ExecutionPlan {
    let mut graph = Graph::default();
    graph.nodes.push(NodeInstance::new("src"));
    graph
        .nodes
        .push(NodeInstance::new("pure").with_metadata(NODE_REUSABLE_META_KEY, reusable()));
    graph.nodes.push(NodeInstance::new("sink"));
    graph.edges.push(Edge::new(0, "out", 1, "in"));
    graph.edges.push(Edge::new(1, "out", 2, "in"));
    ExecutionPlan::new(graph, vec![])
}

/// `src -> pure -> pure2 -> sink`, both reusable.
fn chain() -> ExecutionPlan {
    let mut graph = Graph::default();
    graph.nodes.push(NodeInstance::new("src"));
    graph
        .nodes
        .push(NodeInstance::new("pure").with_metadata(NODE_REUSABLE_META_KEY, reusable()));
    graph
        .nodes
        .push(NodeInstance::new("pure2").with_metadata(NODE_REUSABLE_META_KEY, reusable()));
    graph.nodes.push(NodeInstance::new("sink"));
    graph.edges.push(Edge::new(0, "out", 1, "in"));
    graph.edges.push(Edge::new(1, "out", 2, "in"));
    graph.edges.push(Edge::new(2, "out", 3, "in"));
    ExecutionPlan::new(graph, vec![])
}

fn plan(execution: ExecutionPlan) -> Arc<daedalus_runtime::RuntimePlan> {
    Arc::new(build_runtime(&execution, &SchedulerConfig::default()))
}

#[test]
fn unchanged_inputs_replay_and_a_changed_input_reruns() {
    let fx = Fixture::new();
    let mut exec = OwnedExecutor::new(plan(one_pure()), Handler(fx.clone()));

    fx.set_value(1);
    let first = exec.run_in_place().expect("tick");
    assert_eq!(fx.calls("pure"), 1);
    assert_eq!(first.nodes_reused, 0);

    let second = exec.run_in_place().expect("tick");
    assert_eq!(
        fx.calls("pure"),
        1,
        "unchanged input: the handler does not run"
    );
    assert_eq!(second.nodes_reused, 1);
    assert_eq!(
        *fx.sink.lock(),
        vec![10, 10],
        "the replayed output still reaches the sink"
    );

    fx.set_value(2);
    let third = exec.run_in_place().expect("tick");
    assert_eq!(fx.calls("pure"), 2, "changed input: the handler runs");
    assert_eq!(third.nodes_reused, 0);
    assert_eq!(fx.sink.lock().last().copied(), Some(20));
}

#[test]
fn an_upstream_change_invalidates_the_reuse_chain() {
    let fx = Fixture::new();
    let mut exec = OwnedExecutor::new(plan(chain()), Handler(fx.clone()));

    fx.set_value(1);
    exec.run_in_place().expect("tick");
    assert_eq!((fx.calls("pure"), fx.calls("pure2")), (1, 1));

    let steady = exec.run_in_place().expect("tick");
    assert_eq!((fx.calls("pure"), fx.calls("pure2")), (1, 1), "both replay");
    assert_eq!(steady.nodes_reused, 2);

    fx.set_value(3);
    exec.run_in_place().expect("tick");
    assert_eq!(
        (fx.calls("pure"), fx.calls("pure2")),
        (2, 2),
        "the change reaches both"
    );
    assert_eq!(fx.sink.lock().last().copied(), Some(60), "3 * 10 * 2");
}

#[test]
fn a_failed_run_stores_nothing_and_takes_the_normal_failure_path() {
    let fx = Fixture::new();
    let mut exec = OwnedExecutor::new(plan(one_pure()), Handler(fx.clone()));

    fx.set_value(1);
    exec.run_in_place().expect("tick"); // stored: value 1
    assert_eq!(fx.calls("pure"), 1);

    fx.set_value(2);
    fx.fail_next("pure", 1);
    let failed = exec.run_in_place().expect("contained");
    assert_eq!(failed.errors.len(), 1, "the failure is recorded as usual");
    assert_eq!(fx.calls("pure"), 2);

    // Back to value 1: the stored result was dropped by the failure, so the handler runs again
    // rather than replaying an old run.
    fx.set_value(1);
    let recovered = exec.run_in_place().expect("tick");
    assert_eq!(fx.calls("pure"), 3, "nothing was stored by the failed run");
    assert_eq!(recovered.nodes_reused, 0);
}

#[test]
fn a_failing_reusable_node_backs_off_like_any_other() {
    let fx = Fixture::new();
    let mut exec = OwnedExecutor::new(plan(one_pure()), Handler(fx.clone()))
        .with_backoff_after(1)
        .with_backoff(Duration::from_secs(60), Duration::from_secs(60));
    fx.set_value(1);
    fx.fail_next("pure", 1);
    let failed = exec.run_in_place().expect("contained");
    let pure = failed
        .node_health
        .iter()
        .find(|report| report.node_id == "pure")
        .expect("reported");
    assert_eq!(
        pure.state, "backed_off",
        "a reusable node is backed off on failure"
    );
    let skipped = exec.run_in_place().expect("tick");
    assert_eq!(
        skipped.recovery().backoff_skips,
        1,
        "the backed-off node runs nothing"
    );
    assert_eq!(fx.calls("pure"), 1);
}

#[test]
fn manual_off_and_on_keep_the_stored_result_and_clear_on_request() {
    let fx = Fixture::new();
    let mut exec = OwnedExecutor::new(plan(one_pure()), Handler(fx.clone()));
    fx.set_value(1);
    exec.run_in_place().expect("tick");
    assert_eq!(fx.calls("pure"), 1);

    exec.try_set_node_enabled("pure", false)
        .expect("known node");
    exec.run_in_place().expect("tick");
    exec.try_set_node_enabled("pure", true).expect("known node");
    let back = exec.run_in_place().expect("tick");
    assert_eq!(fx.calls("pure"), 1, "switching off and on keeps the result");
    assert_eq!(back.nodes_reused, 1);

    exec.clear_reuse();
    exec.run_in_place().expect("tick");
    assert_eq!(fx.calls("pure"), 2, "clear_reuse drops the stored result");
}

#[test]
fn a_reloaded_graph_starts_with_nothing_stored() {
    let fx = Fixture::new();
    let mut exec = OwnedExecutor::new(plan(one_pure()), Handler(fx.clone()));
    fx.set_value(1);
    exec.run_in_place().expect("tick");
    // A reload compiles a new executor; its table is empty.
    let mut reloaded = OwnedExecutor::new(plan(one_pure()), Handler(fx.clone()));
    reloaded.run_in_place().expect("tick");
    assert_eq!(fx.calls("pure"), 2, "the reloaded graph runs the handler");
}

#[test]
fn a_missing_optional_input_is_part_of_the_stamp() {
    let fx = Fixture::new();
    let mut graph = Graph::default();
    graph.nodes.push(NodeInstance::new("src"));
    graph.nodes.push(NodeInstance::new("opt_src"));
    graph.nodes.push(
        NodeInstance::new("pure")
            .with_metadata(NODE_REUSABLE_META_KEY, reusable())
            .with_metadata(
                NODE_REQUIRED_INPUTS_META_KEY,
                Value::List(vec![Value::String("in".into())]),
            ),
    );
    graph.nodes.push(NodeInstance::new("sink"));
    graph.edges.push(Edge::new(0, "out", 2, "in"));
    graph.edges.push(Edge::new(1, "out", 2, "opt"));
    graph.edges.push(Edge::new(2, "out", 3, "in"));
    let mut exec = OwnedExecutor::new(plan(ExecutionPlan::new(graph, vec![])), Handler(fx.clone()));

    fx.set_value(1);
    exec.run_in_place().expect("tick"); // opt absent
    assert_eq!(fx.calls("pure"), 1);
    exec.run_in_place().expect("tick"); // opt absent again
    assert_eq!(fx.calls("pure"), 1, "same inputs, same absence: replayed");

    *fx.opt.lock() = Some(5);
    exec.run_in_place().expect("tick"); // opt present
    assert_eq!(
        fx.calls("pure"),
        2,
        "an arriving optional input invalidates"
    );
}

#[test]
fn a_reusable_node_reuses_under_parallel_runs() {
    let fx = Fixture::new();
    let mut exec = OwnedExecutor::new(plan(one_pure()), Handler(fx.clone()));
    fx.set_value(1);
    exec.run_parallel_in_place().expect("tick");
    let steady = exec.run_parallel_in_place().expect("tick");
    assert_eq!(
        fx.calls("pure"),
        1,
        "a parallel tick replays the stored result"
    );
    assert_eq!(steady.nodes_reused, 1, "the worker's count is merged");
}
