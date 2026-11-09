use daedalus_backend::{SpaceKind, TransferCost};
use daedalus_graph::{GraphBody, NodeDoc};
use daedalus_planner::debug::{self, from_json};
use daedalus_planner::diagnostic::{Code, Severity, bundle};
use daedalus_planner::explain::{PlaceReason, explain_plan, json};
use daedalus_planner::lint::{
    EXCLUSIVE_ON_FANOUT, FIRE_ALL_MAY_NOT_PRODUCE, UNCONNECTED_REQUIRED_INPUT, UNUSED_OUTPUT, lint,
};
use daedalus_planner::{Config, Form, PlanError, Planned, plan, solve};
use daedalus_planner_tests::{Mem, Named, decl, edge, node, u8_ty};
use daedalus_registry::Registry;

fn body(nodes: Vec<NodeDoc>, edges: Vec<daedalus_graph::EdgeDoc>) -> GraphBody {
    GraphBody {
        nodes,
        edges,
        groups: Vec::new(),
    }
}

fn registry() -> Registry {
    let mut r = Registry::new();
    decl("t.src").output("out", u8_ty()).body("a").add(&mut r);
    decl("t.two")
        .output("out", u8_ty())
        .output("spare", u8_ty())
        .body("a")
        .add(&mut r);
    decl("t.sink").input("in", u8_ty()).body("a").add(&mut r);
    decl("t.take")
        .input_move("in", u8_ty())
        .body("a")
        .add(&mut r);
    decl("t.copy")
        .input("in", u8_ty())
        .output("out", u8_ty())
        .conversion(1, false)
        .body("a")
        .add(&mut r);
    decl("t.maybe")
        .conditional_output("out", u8_ty())
        .body("a")
        .add(&mut r);
    r
}

fn warnings(graph: &GraphBody) -> Vec<(String, String)> {
    let registry = registry();
    let solved = solve(graph, &registry, &Config::default()).unwrap();
    lint(&solved, &registry)
        .into_iter()
        .map(|d| {
            assert_eq!((d.code, d.severity), (Code::LintWarning, Severity::Warning));
            (d.rule.unwrap(), d.node.unwrap())
        })
        .collect()
}

fn one(rule: &str, node: &str) -> Vec<(String, String)> {
    vec![(rule.to_string(), node.to_string())]
}

#[test]
fn unconnected_required_input_is_one_warning() {
    let graph = body(vec![node("t.sink")], Vec::new());
    assert_eq!(warnings(&graph), one(UNCONNECTED_REQUIRED_INPUT, "t.sink"));
}

#[test]
fn unused_output_is_one_warning() {
    let graph = body(
        vec![node("t.two"), node("t.sink")],
        vec![edge(0, "out", 1, "in")],
    );
    assert_eq!(warnings(&graph), one(UNUSED_OUTPUT, "t.two"));
}

#[test]
fn exclusive_access_on_a_fanned_out_source_is_one_warning() {
    let graph = body(
        vec![node("t.src"), node("t.take"), node("t.sink")],
        vec![edge(0, "out", 1, "in"), edge(0, "out", 2, "in")],
    );
    let found = warnings(&graph);
    assert_eq!(found, one(EXCLUSIVE_ON_FANOUT, "t.take"));
}

#[test]
fn fire_all_on_a_conditional_source_is_one_warning() {
    let graph = body(
        vec![node("t.maybe"), node("t.sink")],
        vec![edge(0, "out", 1, "in")],
    );
    assert_eq!(warnings(&graph), one(FIRE_ALL_MAY_NOT_PRODUCE, "t.sink"));
}

fn lint_config(lints: bool) -> Config {
    Config {
        lints,
        form: Form::Blob,
        ..Config::default()
    }
}

#[test]
fn lints_are_off_unless_asked_for() {
    let graph = body(vec![node("t.sink")], Vec::new());
    let planned = plan(&graph, &registry(), &[&Named("a")], &lint_config(false)).unwrap();
    assert!(planned.warnings.is_empty());
}

#[test]
fn a_plan_with_only_warnings_is_ok() {
    let graph = body(vec![node("t.sink")], Vec::new());
    let planned = plan(&graph, &registry(), &[&Named("a")], &lint_config(true)).unwrap();
    assert_eq!(planned.warnings.len(), 1);
    assert_eq!(planned.warnings[0].severity, Severity::Warning);
}

fn three_backends() -> Registry {
    let mut r = Registry::new();
    let mut add = |id: &str, backends: &[&str]| {
        let mut d = decl(id).input("in", u8_ty()).output("out", u8_ty());
        for backend in backends {
            d = d.body(backend);
        }
        d.add(&mut r);
    };
    add("s.cpu", &["cpu"]);
    add("x.cgn", &["cpu", "gpu", "npu"]);
    add("x.gn", &["gpu", "npu"]);
    add("s.npu", &["npu"]);
    r
}

fn forced(id: &str, backend: &str) -> NodeDoc {
    let mut doc = node(id);
    doc.backend = Some(backend.to_string());
    doc
}

fn planned_three() -> Planned {
    let graph = body(
        vec![
            forced("s.cpu", "cpu"),
            node("x.cgn"),
            node("x.gn"),
            forced("s.npu", "npu"),
        ],
        vec![
            edge(0, "out", 1, "in"),
            edge(1, "out", 2, "in"),
            edge(2, "out", 3, "in"),
        ],
    );
    let (cpu, gpu, npu) = (Named("cpu"), Named("gpu"), Named("npu"));
    plan(
        &graph,
        &three_backends(),
        &[&cpu, &gpu, &npu],
        &lint_config(false),
    )
    .unwrap()
}

#[test]
fn explain_for_three_backends_matches_the_fixture() {
    let path = concat!(
        env!("CARGO_MANIFEST_DIR"),
        "/fixtures/explain_three_backends.json"
    );
    let text = json::to_json(&explain_plan(&planned_three())).unwrap();
    if std::env::var_os("BLESS").is_some() {
        std::fs::write(path, &text).unwrap();
    }
    assert_eq!(text, std::fs::read_to_string(path).unwrap());
}

#[test]
fn explain_gives_each_node_a_reason() {
    let explain = explain_plan(&planned_three());
    assert_eq!(explain.nodes[0].reason, PlaceReason::Forced);
    assert_eq!(explain.nodes[0].backend.as_deref(), Some("cpu"));
    assert!(matches!(
        explain.nodes[1].reason,
        PlaceReason::Chosen { .. }
    ));
    assert_eq!(explain.units.len(), explain.regions.len());
}

#[test]
fn explain_shows_conversions_and_transfers_on_the_edge() {
    let mut r = Registry::new();
    let key = daedalus_planner_tests::u32_ty();
    decl("t.src").output("out", u8_ty()).body("a").add(&mut r);
    decl("t.widen")
        .input("in", u8_ty())
        .output("out", key.clone())
        .conversion(1, false)
        .body("a")
        .add(&mut r);
    decl("t.sink").input("in", key).body("b").add(&mut r);
    let graph = body(
        vec![node("t.src"), node("t.sink")],
        vec![edge(0, "out", 1, "in")],
    );
    let a = Mem {
        id: "a",
        spaces: vec![("m.a", SpaceKind::Device, true)],
        moves: vec![("t.move", "m.a", "m.b", TransferCost::Copy)],
        ..Mem::default()
    };
    let b = Mem {
        id: "b",
        spaces: vec![("m.b", SpaceKind::Device, true)],
        ..Mem::default()
    };
    let planned = plan(&graph, &r, &[&a, &b], &lint_config(false)).unwrap();
    let explain = explain_plan(&planned);
    assert_eq!(explain.edges.len(), 1);
    assert_eq!(explain.edges[0].to_node, 1);
    assert_eq!(explain.edges[0].conversion, ["t.widen"]);
    assert_eq!(explain.edges[0].transfer, ["t.move"]);
}

#[test]
fn a_plan_reads_back_from_json() {
    let planned = planned_three();
    let dump = from_json(&debug::to_json(&planned).unwrap()).unwrap();
    assert_eq!(dump, debug::dump(&planned).unwrap());
}

#[test]
fn errors_convert_and_bundle_for_editors() {
    let graph = body(vec![node("t.gone")], Vec::new());
    let error = plan(&graph, &registry(), &[&Named("a")], &lint_config(false))
        .err()
        .unwrap();
    assert!(matches!(error, PlanError::Solve(_)));
    let found = error.diagnostics();
    assert_eq!(found[0].code, Code::NodeMissing);
    assert_eq!(bundle(&found).missing_nodes, ["t.gone"]);
}
