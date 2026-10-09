//! Execution domains: a shared upstream graph fanned out to several downstream graphs runs once
//! per frame, downstream graphs come and go between ticks, failures stay in their graph, and
//! structural sharing finds the common prefix of loaded graphs.

use std::sync::Mutex;
use std::sync::atomic::{AtomicUsize, Ordering};

use daedalus::{
    NodeHandle,
    engine::{Engine, EngineConfig, ExecutionDomain, HostGraph, LinkMode, MetricsLevel},
    macros::{node, plugin},
    planner::Graph,
    runtime::{NodeError, handler_registry::HandlerRegistry, plugins::PluginRegistry},
};

/// Runs of `scale`; tests using it hold [`SERIAL`].
static SCALE_RUNS: AtomicUsize = AtomicUsize::new(0);
static SERIAL: Mutex<()> = Mutex::new(());

/// The shared preprocessing stand-in: deterministic, side-effect free (but counted), fails on a
/// negative input.
#[node(id = "scale", inputs("x"), outputs("y"), shareable)]
fn scale(x: i64) -> Result<i64, NodeError> {
    SCALE_RUNS.fetch_add(1, Ordering::SeqCst);
    if x < 0 {
        return Err(NodeError::InvalidInput("negative frame".into()));
    }
    Ok(x * 2)
}

#[node(id = "add", inputs("x"), outputs("y"))]
fn add(x: i64) -> Result<i64, NodeError> {
    Ok(x + 1)
}

#[node(id = "neg", inputs("x"), outputs("y"))]
fn neg(x: i64) -> Result<i64, NodeError> {
    Ok(-x)
}

/// A detector that fails on one value.
#[node(id = "reject", inputs("x"), outputs("y"))]
fn reject(x: i64) -> Result<i64, NodeError> {
    if x == 14 {
        return Err(NodeError::InvalidInput("cannot decode 14".into()));
    }
    Ok(x)
}

#[plugin(id = "dom", nodes(scale, add, neg, reject))]
struct DomainPlugin;

fn registry() -> PluginRegistry {
    let mut registry = PluginRegistry::new();
    registry.install(&DomainPlugin::new()).expect("install");
    registry
}

/// `host.<input> -> stages... -> host.<output>`.
fn chain(registry: &PluginRegistry, stages: &[&str], input: &str, output: &str) -> Graph {
    build_chain(registry, stages, input, output, false)
}

/// [`chain`] with its input declared shared: in a hand-laid domain one payload reaches several
/// graphs, and the stages take their input by value, so they need a planned copy.
fn shared_chain(registry: &PluginRegistry, stages: &[&str], input: &str, output: &str) -> Graph {
    build_chain(registry, stages, input, output, true)
}

fn build_chain(
    registry: &PluginRegistry,
    stages: &[&str],
    input: &str,
    output: &str,
    shared: bool,
) -> Graph {
    let handles: Vec<NodeHandle> = stages
        .iter()
        .enumerate()
        .map(|(at, stage)| NodeHandle::new(format!("dom:{stage}")).alias(format!("s{at}")))
        .collect();
    let mut builder = registry
        .graph_builder()
        .expect("builder")
        .input_typed::<i64>(input)
        .expect("input");
    if shared {
        builder = builder.shared_input(input);
    }
    for handle in &handles {
        builder = builder.try_node(handle).expect("node");
    }
    builder = builder
        .try_connect(input, &handles[0].input("x"))
        .expect("wire input");
    for pair in handles.windows(2) {
        builder = builder
            .try_connect(&pair[0].output("y"), &pair[1].input("x"))
            .expect("wire");
    }
    builder
        .try_connect(&handles[handles.len() - 1].output("y"), output)
        .expect("wire output")
        .build()
}

fn engine() -> Engine {
    Engine::new(EngineConfig::default().with_metrics_level(MetricsLevel::Off)).expect("engine")
}

fn compile(registry: &PluginRegistry, graph: Graph) -> HostGraph<HandlerRegistry> {
    engine().compile_registry(registry, graph).expect("compile")
}

/// `up` (scale, `x -> y`) fanned out to downstream graphs `name -> stages`, each reading `y`
/// and writing `out`; domain input `x` feeds `up`.
fn domain(
    registry: &PluginRegistry,
    downstream: &[(&str, &[&str])],
) -> ExecutionDomain<HandlerRegistry> {
    let mut domain = ExecutionDomain::new();
    domain
        .add_graph(
            "up",
            compile(registry, shared_chain(registry, &["scale"], "x", "y")),
        )
        .expect("add up");
    domain.route_input("x", "up", "x").expect("route");
    for (name, stages) in downstream {
        add_downstream(&mut domain, registry, name, stages);
    }
    domain
}

fn add_downstream(
    domain: &mut ExecutionDomain<HandlerRegistry>,
    registry: &PluginRegistry,
    name: &str,
    stages: &[&str],
) {
    domain
        .add_graph(
            name,
            compile(registry, shared_chain(registry, stages, "y", "out")),
        )
        .expect("add downstream");
    domain
        .link("up", "y", name, "y", LinkMode::Latest)
        .expect("link");
}

fn out(domain: &mut ExecutionDomain<HandlerRegistry>, graph: &str) -> Option<i64> {
    domain
        .take_payload(graph, "out")
        .map(|payload| *payload.get_ref::<i64>().expect("i64 output"))
}

fn serial() -> std::sync::MutexGuard<'static, ()> {
    SERIAL.lock().unwrap_or_else(|e| e.into_inner())
}

#[test]
fn shared_upstream_runs_once_per_frame_and_matches_separate_graphs() {
    let _serial = serial();
    let registry = registry();
    let mut domain = domain(&registry, &[("a", &["add"]), ("b", &["neg"])]);
    let mut separate_a = compile(&registry, chain(&registry, &["scale", "add"], "x", "out"));
    let mut separate_b = compile(&registry, chain(&registry, &["scale", "neg"], "x", "out"));
    assert_eq!(domain.tick_order().collect::<Vec<_>>(), ["up", "a", "b"]);

    for frame in 1..=10i64 {
        let before = SCALE_RUNS.load(Ordering::SeqCst);
        assert_eq!(domain.push("x", frame).expect("push"), 1);
        let tick = domain.tick();
        assert!(
            tick.is_ok() && tick.ran == 3,
            "{tick:?} {:?} {:?} {:?}",
            domain.last_error("up"),
            domain.last_error("a"),
            domain.last_error("b")
        );
        assert_eq!(
            SCALE_RUNS.load(Ordering::SeqCst) - before,
            1,
            "upstream once"
        );
        let expected_a = separate_a.run_once_latest::<_, i64>(("x", frame), "out");
        let expected_b = separate_b.run_once_latest::<_, i64>(("x", frame), "out");
        assert_eq!(out(&mut domain, "a"), expected_a.expect("separate a"));
        assert_eq!(out(&mut domain, "b"), expected_b.expect("separate b"));
    }

    let stats = domain.stats();
    let up = stats.graph("up").expect("up stats");
    assert_eq!((up.runs, up.consumers, up.avoided_runs), (10, 2, 10));
    assert_eq!((up.nodes, up.avoided_node_runs), (1, 10));
    assert_eq!(stats.avoided_runs, 10);
    let explanation = domain.explain();
    assert_eq!(explanation.graph("up").expect("up").consumers, ["a", "b"]);
    assert_eq!(explanation.links.len(), 2);
    assert!(explanation.links.iter().all(|link| link.zero_copy()));
    let text = explanation.to_string();
    assert!(
        text.contains("shared: runs once for 2 graphs (a, b)"),
        "{text}"
    );
    assert!(text.contains("link up.y -> a.y [latest]"), "{text}");
    assert!(text.contains("input x -> up.x"), "{text}");
}

#[test]
fn downstream_graphs_come_and_go_between_ticks() {
    let _serial = serial();
    let registry = registry();
    let mut domain = domain(&registry, &[("a", &["add"])]);
    domain.push("x", 1i64).expect("push");
    assert_eq!(domain.tick().ran, 2);
    assert_eq!(out(&mut domain, "a"), Some(3));

    add_downstream(&mut domain, &registry, "c", &["add", "add"]);
    domain.push("x", 2i64).expect("push");
    assert_eq!(domain.tick().ran, 3);
    assert_eq!(
        (out(&mut domain, "a"), out(&mut domain, "c")),
        (Some(5), Some(6))
    );

    let removed = domain.remove_graph("a").expect("remove a");
    assert_eq!(removed.host_alias(), "host");
    assert!(domain.graph("a").is_none());
    assert_eq!(domain.tick_order().collect::<Vec<_>>(), ["up", "c"]);
    let before = SCALE_RUNS.load(Ordering::SeqCst);
    domain.push("x", 3i64).expect("push");
    assert_eq!(domain.tick().ran, 2);
    assert_eq!(out(&mut domain, "c"), Some(8));
    assert_eq!(SCALE_RUNS.load(Ordering::SeqCst) - before, 1);

    assert!(domain.unlink("up", "y", "c", "y"));
    domain.push("x", 4i64).expect("push");
    let tick = domain.tick();
    assert_eq!((tick.ran, tick.idle), (1, 1));
    assert_eq!(out(&mut domain, "c"), None);
    assert_eq!(domain.stats().graph("up").expect("up").runs, 4);

    // Links may point either way as long as they form no cycle.
    domain
        .link("c", "out", "up", "x", LinkMode::All)
        .expect("c feeds up");
    assert_eq!(domain.tick_order().collect::<Vec<_>>(), ["c", "up"]);
    assert!(
        domain.link("up", "y", "c", "y", LinkMode::Latest).is_err(),
        "cycle"
    );
    assert_eq!(domain.tick_order().collect::<Vec<_>>(), ["c", "up"]);
}

#[test]
fn failures_stay_in_their_graph() {
    let _serial = serial();
    let registry = registry();
    let mut domain = domain(&registry, &[("a", &["add"]), ("b", &["reject"])]);
    domain
        .add_graph(
            "solo",
            compile(&registry, shared_chain(&registry, &["add"], "x", "out")),
        )
        .expect("add solo");
    domain.route_input("x", "solo", "x").expect("route solo");

    // b fails on 14 (frame 7); a and solo are untouched.
    domain.push("x", 7i64).expect("push");
    let tick = domain.tick();
    assert_eq!((tick.ran, tick.failed, tick.skipped), (3, 1, 0), "{tick:?}");
    assert_eq!(out(&mut domain, "a"), Some(15));
    assert_eq!(out(&mut domain, "solo"), Some(8));
    assert!(domain.last_error("b").is_some());
    assert!(domain.last_error("a").is_none());
    domain.push("x", 8i64).expect("push");
    assert!(domain.tick().is_ok());
    assert_eq!(
        (out(&mut domain, "a"), out(&mut domain, "b")),
        (Some(17), Some(16))
    );
    assert_eq!(out(&mut domain, "solo"), Some(9));
    assert!(domain.clear_error("b").is_some());

    // The upstream fails: its downstream graphs are skipped, the independent graph runs.
    domain.push("x", -1i64).expect("push");
    let tick = domain.tick();
    assert_eq!((tick.ran, tick.failed, tick.skipped), (1, 1, 2), "{tick:?}");
    assert_eq!(out(&mut domain, "solo"), Some(0));
    assert_eq!((out(&mut domain, "a"), out(&mut domain, "b")), (None, None));
    assert!(domain.last_error("up").is_some());
    domain.push("x", 9i64).expect("push");
    assert!(domain.tick().is_ok());
    assert_eq!(out(&mut domain, "a"), Some(19));
    assert_eq!(
        out(&mut domain, "a"),
        None,
        "one value per frame, nothing stale"
    );
    let stats = domain.stats();
    assert_eq!(stats.graph("a").expect("a").skipped, 1);
    assert_eq!(stats.graph("b").expect("b").failures, 1);
}

#[test]
fn structural_sharing_runs_the_common_prefix_once() {
    let _serial = serial();
    let registry = registry();
    let graphs = [
        ("plus", chain(&registry, &["scale", "add"], "x", "out")),
        ("minus", chain(&registry, &["scale", "neg"], "x", "out")),
        ("plain", chain(&registry, &["scale"], "x", "out")),
        // `add` is not shareable: `plus2` shares `scale` but keeps its own `add`.
        ("plus2", chain(&registry, &["scale", "add"], "x", "out")),
    ];
    let mut domain =
        ExecutionDomain::load_shared(&engine(), &registry, graphs).expect("load shared");
    let explanation = domain.explain();
    assert_eq!(explanation.shared_nodes.len(), 1, "{explanation}");
    let shared = &explanation.shared_nodes[0];
    assert_eq!(shared.node_id, "dom:scale");
    assert_eq!(shared.upstream, "shared");
    assert_eq!(shared.graphs, ["plus", "minus", "plain", "plus2"]);
    assert!(
        domain.graph("plain").is_none(),
        "fully shared: served by the upstream"
    );
    assert_eq!(
        domain.tick_order().collect::<Vec<_>>(),
        ["shared", "plus", "minus", "plus2"]
    );
    let text = explanation.to_string();
    assert!(text.contains("shared: runs once for 3 graphs"), "{text}");
    assert!(
        text.contains("tap shared.s0.y -> host as plain.out"),
        "{text}"
    );

    for frame in 1..=5i64 {
        let before = SCALE_RUNS.load(Ordering::SeqCst);
        domain.push("x", frame).expect("push");
        assert!(domain.tick().is_ok());
        assert_eq!(SCALE_RUNS.load(Ordering::SeqCst) - before, 1);
        assert_eq!(out(&mut domain, "plus"), Some(frame * 2 + 1));
        assert_eq!(out(&mut domain, "plus2"), Some(frame * 2 + 1));
        assert_eq!(out(&mut domain, "minus"), Some(-frame * 2));
        assert_eq!(out(&mut domain, "plain"), Some(frame * 2));
        assert_eq!(out(&mut domain, "plain"), None);
    }
    assert_eq!(domain.stats().avoided_runs, 10);
}

#[test]
fn nothing_shareable_compiles_every_graph_as_is() {
    let _serial = serial();
    let registry = registry();
    let graphs = [
        ("one", chain(&registry, &["add"], "x", "out")),
        ("two", chain(&registry, &["add", "neg"], "x", "out")),
    ];
    let mut domain = ExecutionDomain::load_shared(&engine(), &registry, graphs).expect("load");
    assert!(domain.explain().shared_nodes.is_empty());
    assert_eq!(domain.tick_order().collect::<Vec<_>>(), ["one", "two"]);
    assert_eq!(domain.push("x", 4i64).expect("push"), 2);
    assert!(domain.tick().is_ok());
    assert_eq!(
        (out(&mut domain, "one"), out(&mut domain, "two")),
        (Some(5), Some(-5))
    );
}
