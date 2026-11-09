use daedalus_backend::Backend;
use daedalus_graph::{EdgeDoc, GraphBody, NodeDoc, NodeKindDoc};
use daedalus_planner::assign::error::{AssignError, BodyState};
use daedalus_planner::assign::{ASSIGN_BUDGET, Assignment, Input, Placed, Reason, Removed, assign};
use daedalus_planner::solve::type_table::TypeTable;
use daedalus_planner::{Config, solve};
use daedalus_planner_tests::{DECLINED, Named, decl, edge, node, u8_ty};
use daedalus_registry::Registry;

const CPU: Named = Named("cpu");
const GPU: Named = Named("gpu");
const NPU: Named = Named("npu");

/// A node `id` with bodies on `backends`, one input and one output.
fn add(registry: &mut Registry, id: &str, backends: &[&str]) {
    let mut d = decl(id).input("in", u8_ty()).output("out", u8_ty());
    for backend in backends {
        d = d.body(backend);
    }
    d.add(registry);
}

fn forced(id: &str, backend: &str) -> NodeDoc {
    let mut doc = node(id);
    doc.backend = Some(backend.to_string());
    doc
}

fn run(
    registry: &Registry,
    nodes: Vec<NodeDoc>,
    edges: Vec<EdgeDoc>,
    backends: &[&dyn Backend],
) -> Result<Assignment, Vec<AssignError>> {
    let body = GraphBody {
        nodes,
        edges,
        groups: Vec::new(),
    };
    let graph = solve(&body, registry, &Config::default()).unwrap();
    let mut types = TypeTable::default();
    for node in &graph.nodes {
        for port in node.inputs.iter().chain(&node.outputs) {
            types.add(&port.ty);
        }
    }
    let removed = Removed::new();
    assign(&Input {
        graph: &graph,
        registry,
        backends,
        types: &types,
        budget: ASSIGN_BUDGET,
        removed: &removed,
    })
}

fn at(assignment: &Assignment, node: usize) -> u8 {
    match assignment.placed[node] {
        Placed::Backend(index) => index.0,
        Placed::Host => panic!("node {node} is on the host"),
    }
}

fn places(assignment: &Assignment) -> Vec<u8> {
    (0..assignment.placed.len())
        .map(|n| at(assignment, n))
        .collect()
}

fn registry() -> Registry {
    let mut registry = Registry::new();
    add(&mut registry, "s.cpu", &["cpu"]);
    add(&mut registry, "s.gpu", &["gpu"]);
    add(&mut registry, "s.npu", &["npu"]);
    add(&mut registry, "x.cg", &["cpu", "gpu"]);
    add(&mut registry, "x.cgn", &["cpu", "gpu", "npu"]);
    add(&mut registry, "x.gn", &["gpu", "npu"]);
    registry
}

fn line(count: u32) -> Vec<EdgeDoc> {
    (0..count).map(|i| edge(i, "out", i + 1, "in")).collect()
}

#[test]
fn two_backends_fewest_crossings_beats_priority() {
    let result = run(
        &registry(),
        vec![forced("s.gpu", "gpu"), node("x.cg"), forced("s.gpu", "gpu")],
        line(2),
        &[&CPU, &GPU],
    )
    .unwrap();
    assert_eq!(places(&result), [1, 1, 1]);
    assert_eq!(result.crossings, 0);
}

#[test]
fn tie_goes_to_priority() {
    let result = run(
        &registry(),
        vec![forced("s.cpu", "cpu"), node("x.cg"), forced("s.gpu", "gpu")],
        line(2),
        &[&CPU, &GPU],
    )
    .unwrap();
    assert_eq!(places(&result), [0, 0, 1]);
    assert_eq!(result.crossings, 1);
}

#[test]
fn three_backends_exact() {
    let result = run(
        &registry(),
        vec![
            forced("s.cpu", "cpu"),
            node("x.cgn"),
            node("x.gn"),
            forced("s.npu", "npu"),
        ],
        line(3),
        &[&CPU, &GPU, &NPU],
    )
    .unwrap();
    assert_eq!(places(&result), [0, 0, 2, 2]);
    assert_eq!(result.crossings, 1);
}

#[test]
fn fanout_counts_pairs_not_edges() {
    let result = run(
        &registry(),
        vec![
            forced("s.cpu", "cpu"),
            node("x.cg"),
            forced("s.gpu", "gpu"),
            forced("s.gpu", "gpu"),
        ],
        vec![
            edge(0, "out", 1, "in"),
            edge(1, "out", 2, "in"),
            edge(1, "out", 3, "in"),
        ],
        &[&CPU, &GPU],
    )
    .unwrap();
    assert_eq!(
        at(&result, 1),
        0,
        "a per-edge count would pick the second backend"
    );
    assert_eq!(result.crossings, 1);
}

#[test]
fn fanout_to_two_other_backends_costs_two() {
    let result = run(
        &registry(),
        vec![
            forced("s.cpu", "cpu"),
            forced("s.gpu", "gpu"),
            forced("s.npu", "npu"),
        ],
        vec![edge(0, "out", 1, "in"), edge(0, "out", 2, "in")],
        &[&CPU, &GPU, &NPU],
    )
    .unwrap();
    assert_eq!(result.crossings, 2);
    assert_eq!(result.reasons[0], Reason::Forced);
}

#[test]
fn no_candidates_lists_bodies() {
    let mut registry = registry();
    decl("t.no")
        .body_needing("cpu", &[DECLINED])
        .body("gone")
        .add(&mut registry);
    let errors = run(&registry, vec![node("t.no")], Vec::new(), &[&CPU]).unwrap_err();
    let [AssignError::NodeUnplaceable { node, bodies }] = &errors[..] else {
        panic!("{errors:?}");
    };
    assert_eq!(node, "t.no");
    let states: Vec<_> = bodies
        .iter()
        .map(|b| (b.backend.as_str(), b.state.clone()))
        .collect();
    assert_eq!(
        states,
        [
            ("cpu", BodyState::Declined),
            ("gone", BodyState::NotSupplied)
        ]
    );
}

#[test]
fn forced_unavailable_names_forced_backend() {
    let mut registry = registry();
    decl("t.half")
        .body("cpu")
        .body_needing("gpu", &[DECLINED])
        .add(&mut registry);
    let errors = run(
        &registry,
        vec![forced("t.half", "gpu")],
        Vec::new(),
        &[&CPU, &GPU],
    )
    .unwrap_err();
    let [
        AssignError::ForcedUnavailable {
            backend, bodies, ..
        },
    ] = &errors[..]
    else {
        panic!("{errors:?}");
    };
    assert_eq!(backend, "gpu");
    assert_eq!(bodies.len(), 2);
}

#[test]
fn forced_unknown_when_the_backend_has_no_body() {
    let errors = run(
        &registry(),
        vec![forced("s.cpu", "gpu")],
        Vec::new(),
        &[&CPU, &GPU],
    )
    .unwrap_err();
    assert!(
        matches!(&errors[..], [AssignError::ForcedUnknown { .. }]),
        "{errors:?}"
    );
}

#[test]
fn host_bridge_edges_count_as_crossings() {
    let mut host = node("host");
    host.kind = NodeKindDoc::HostBridge;
    host.port_types
        .insert("value".to_string(), "u8".to_string());
    let result = run(
        &registry(),
        vec![host, node("x.cg")],
        vec![edge(0, "value", 1, "in")],
        &[&CPU, &GPU],
    )
    .unwrap();
    assert_eq!(result.placed[0], Placed::Host);
    assert_eq!(result.reasons[0], Reason::HostBridge);
    assert_eq!(result.crossings, 1);
}

#[test]
fn a_free_node_says_why_it_was_chosen() {
    let result = run(&registry(), vec![node("x.cg")], Vec::new(), &[&CPU, &GPU]).unwrap();
    assert_eq!(
        result.reasons[0],
        Reason::Chosen {
            crossings: 0,
            rank: 0
        }
    );
}
