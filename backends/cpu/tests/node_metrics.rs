//! A node's metrics: declared on the node with `#[node(metrics(..))]`, and listed by a host from
//! the plan, without running the graph.

use daedalus_backend::Backend;
use daedalus_cpu::{CpuBackend, Ctx, Timed};
use daedalus_graph::{EdgeDoc, EdgePolicyDoc, GraphBody, NodeDoc, NodeKindDoc, PortRefDoc};
use daedalus_host::{LoadOptions, LoadedHost, MetricInfo, MetricRow};
use daedalus_macros::node;
use daedalus_planner::{Config, Planned, plan};
use daedalus_registry::{MetricDecl, MetricUnit, NodeRecord, Registry};

/// A body with stages of its own to report on.
#[node(
    id = "m.detect",
    inputs(x),
    outputs(out),
    metrics(threshold = "ns", decode = "ns", candidates = "count")
)]
fn detect(x: i64, ctx: &Ctx) -> i64 {
    // The threshold stage is timed; the candidates are counted, `x` of them; decode never runs.
    let out = ctx.timed(DetectMetric::Threshold, || x + 1);
    ctx.metric(DetectMetric::Candidates, x as u64);
    out
}

#[node(id = "m.plain", inputs(x), outputs(out))]
fn plain(x: i64) -> i64 {
    x
}

/// A second node with a metric, which the backend merges into one step with `m.detect`.
#[node(id = "m.refine", inputs(x), outputs(out), metrics(passes = "count"))]
fn refine(x: i64, ctx: &Ctx) -> i64 {
    ctx.metric(RefineMetric::Passes, 2);
    x
}

const NODES: [&NodeRecord; 3] = [&DETECT_NODE, &PLAIN_NODE, &REFINE_NODE];

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

fn registry() -> Registry {
    let mut registry = Registry::new();
    registry.register_all(&NODES).unwrap();
    registry
}

/// The host's `x` through `m.detect` and `m.plain` to the host's `out`.
fn planned() -> Planned {
    let mut host = NodeDoc::new("host");
    host.kind = NodeKindDoc::HostBridge;
    host.port_types.insert("x".into(), "i64".into());
    host.port_types.insert("out".into(), "i64".into());
    let graph = GraphBody {
        nodes: vec![host, NodeDoc::new("m.detect"), NodeDoc::new("m.plain")],
        edges: vec![
            edge(0, "x", 1, "x"),
            edge(1, "out", 2, "x"),
            edge(2, "out", 0, "out"),
        ],
        groups: Vec::new(),
    };
    plan(&graph, &registry(), &[&CpuBackend], &Config::default()).unwrap()
}

fn listed(host: &LoadedHost) -> Vec<(String, String, MetricUnit)> {
    host.metrics()
        .into_iter()
        .map(|MetricInfo { node, name, unit }| (node, name, unit))
        .collect()
}

fn expected() -> Vec<(String, String, MetricUnit)> {
    [
        ("threshold", MetricUnit::Ns),
        ("decode", MetricUnit::Ns),
        ("candidates", MetricUnit::Count),
    ]
    .into_iter()
    .map(|(name, unit)| ("m.detect#0".to_string(), name.to_string(), unit))
    .collect()
}

#[test]
fn a_nodes_declaration_lists_its_metrics_in_order_with_their_units() {
    let metric = |name, unit| MetricDecl { name, unit };
    assert_eq!(
        DETECT_DECL.metrics,
        [
            metric("threshold", MetricUnit::Ns),
            metric("decode", MetricUnit::Ns),
            metric("candidates", MetricUnit::Count),
        ]
    );
    assert!(PLAIN_DECL.metrics.is_empty());
}

#[test]
fn a_host_lists_a_graphs_metrics_from_the_plan_without_running_it() {
    let host = LoadedHost::load(planned(), LoadOptions::default()).unwrap();
    assert_eq!(listed(&host), expected());
}

#[test]
fn a_stored_plan_keeps_the_metrics_and_one_stored_before_them_lists_none() {
    let mut made = planned().made().unwrap();
    let backends: [&dyn Backend; 1] = [&CpuBackend];
    let load = |made: &daedalus_made::MadePlan| {
        let bytes = daedalus_made::encode(made).unwrap();
        LoadedHost::load_made(&bytes, &registry(), &backends, LoadOptions::default()).unwrap()
    };
    assert_eq!(listed(&load(&made)), expected());
    // What reading a plan stored before the metrics gives: see core/made/src/stored_outputs.rs.
    for node in &mut made.facts.nodes {
        node.metrics.clear();
    }
    assert!(listed(&load(&made)).is_empty());
}

/// The host's `x` through `m.detect` and `m.refine` to the host's `out`, merged or not.
fn chain(merge: bool) -> Planned {
    let mut host = NodeDoc::new("host");
    host.kind = NodeKindDoc::HostBridge;
    host.port_types.insert("x".into(), "i64".into());
    host.port_types.insert("out".into(), "i64".into());
    let graph = GraphBody {
        nodes: vec![host, NodeDoc::new("m.detect"), NodeDoc::new("m.refine")],
        edges: vec![
            edge(0, "x", 1, "x"),
            edge(1, "out", 2, "x"),
            edge(2, "out", 0, "out"),
        ],
        groups: Vec::new(),
    };
    let config = Config {
        merge,
        ..Config::default()
    };
    plan(&graph, &registry(), &[&CpuBackend], &config).unwrap()
}

/// Feeds 4, 9 and 2, a tick each, and gives what the graph gave and the host's metric rows.
fn run(planned: Planned, node_metrics: bool) -> (Vec<i64>, Vec<MetricRow>) {
    let options = LoadOptions {
        node_metrics,
        ..LoadOptions::default()
    };
    let mut host = LoadedHost::load(planned, options).unwrap();
    let mut out = Vec::new();
    for x in [4_i64, 9, 2] {
        host.host().feed_typed("x", std::sync::Arc::new(x)).unwrap();
        host.step().unwrap();
        out.push(host.host().take_copy::<i64>("out").unwrap().unwrap());
    }
    (out, host.metric_rows())
}

fn row<'r>(rows: &'r [MetricRow], node: &str, name: &str) -> &'r MetricRow {
    rows.iter()
        .find(|row| row.node == node && row.name == name)
        .unwrap_or_else(|| panic!("no row for {node} {name}: {rows:?}"))
}

#[test]
fn what_a_body_reports_is_read_back_as_a_sum_a_count_and_a_maximum() {
    for merge in [false, true] {
        let (out, rows) = run(chain(merge), true);
        assert_eq!(out, [5, 10, 3], "merge {merge}");
        assert_eq!(rows.len(), 4, "merge {merge}: {rows:?}");
        let candidates = row(&rows, "m.detect#0", "candidates");
        assert_eq!(
            (candidates.sum, candidates.count, candidates.max),
            (15, 3, 9),
            "merge {merge}"
        );
        let threshold = row(&rows, "m.detect#0", "threshold");
        assert_eq!(threshold.count, 3, "merge {merge}");
        assert!(threshold.max <= threshold.sum, "merge {merge}");
        // Declared, and never reported.
        let decode = row(&rows, "m.detect#0", "decode");
        assert_eq!((decode.sum, decode.count, decode.max), (0, 0, 0));
        // A node merged into the same step keeps its own cells.
        let passes = row(&rows, "m.refine#0", "passes");
        assert_eq!(
            (passes.sum, passes.count, passes.max),
            (6, 3, 2),
            "merge {merge}"
        );
    }
}

#[test]
fn the_backend_merges_the_two_nodes_into_one_step_when_it_may() {
    let steps = |merge| {
        let mut host = LoadedHost::load(chain(merge), LoadOptions::default()).unwrap();
        host.host().runner().step_count()
    };
    assert_eq!((steps(true), steps(false)), (1, 2));
}

#[test]
fn a_host_that_keeps_no_metrics_has_no_rows_and_the_graph_runs_the_same() {
    let (out, rows) = run(chain(true), false);
    assert_eq!(out, [5, 10, 3]);
    assert!(rows.is_empty());
}

#[test]
fn a_node_records_the_crate_it_is_declared_in_as_its_provider() {
    let here = daedalus_registry::provider!();
    assert_eq!(
        (here.name, here.version),
        (env!("CARGO_PKG_NAME"), env!("CARGO_PKG_VERSION"))
    );
    assert_eq!(here.name, "daedalus-cpu");
    assert_eq!(DETECT_DECL.provider, here);
    assert_eq!(PLAIN_DECL.provider, here);
}
