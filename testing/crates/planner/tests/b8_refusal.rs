use daedalus_backend::Backend;
use daedalus_graph::{EdgeDoc, NodeDoc};
use daedalus_planner::assign::error::{AssignError, BodyState};
use daedalus_planner::assign::{ASSIGN_BUDGET, Placed};
use daedalus_planner::regions::{Input, RegionError, Regions, build};
use daedalus_planner_tests::{Named, Refuser, decl, edge, node, solved, u8_ty};
use daedalus_registry::Registry;

fn registry() -> Registry {
    let mut registry = Registry::new();
    for id in ["t.x", "t.y", "t.z"] {
        decl(id)
            .optional("in", u8_ty())
            .output("out", u8_ty())
            .body("cpu")
            .body("gpu")
            .add(&mut registry);
    }
    registry
}

fn run(
    nodes: Vec<NodeDoc>,
    edges: Vec<EdgeDoc>,
    backends: &[&dyn Backend],
) -> Result<Regions, RegionError> {
    let registry = registry();
    let (graph, types) = solved(&registry, nodes, edges);
    build(&Input {
        graph: &graph,
        registry: &registry,
        backends,
        types: &types,
        budget: ASSIGN_BUDGET,
        merge_allowed: true,
        fifo_capacity: 4,
    })
}

fn rank(result: &Regions, node: usize) -> u8 {
    match result.assignment.placed[node] {
        Placed::Backend(index) => index.0,
        Placed::Host => panic!("on the host"),
    }
}

const GPU: Named = Named("gpu");

#[test]
fn refused_once_is_reassigned_and_succeeds() {
    let cpu = Refuser {
        id: "cpu",
        refuse: &["t.y"],
        whole: false,
    };
    let result = run(vec![node("t.y")], Vec::new(), &[&cpu, &GPU]).unwrap();
    assert_eq!((rank(&result, 0), result.rounds), (1, 2));
}

#[test]
fn refusals_repeat_until_none_left() {
    let cpu = Refuser {
        id: "cpu",
        refuse: &["t.x"],
        whole: false,
    };
    let gpu = Refuser {
        id: "gpu",
        refuse: &["t.y"],
        whole: false,
    };
    let result = run(
        vec![node("t.x"), node("t.y")],
        vec![edge(0, "out", 1, "in")],
        &[&cpu, &gpu],
    )
    .unwrap();
    assert_eq!((rank(&result, 0), rank(&result, 1)), (1, 0));
    assert!(result.rounds >= 2);
}

#[test]
fn no_candidates_left_lists_every_reason() {
    let cpu = Refuser {
        id: "cpu",
        refuse: &["t.z"],
        whole: false,
    };
    let gpu = Refuser {
        id: "gpu",
        refuse: &["t.z"],
        whole: false,
    };
    let error = run(vec![node("t.z")], Vec::new(), &[&cpu, &gpu]).unwrap_err();
    let RegionError::Assign(errors) = error else {
        panic!("{error:?}");
    };
    let [AssignError::NodeUnplaceable { bodies, .. }] = &errors[..] else {
        panic!("{errors:?}");
    };
    assert_eq!(bodies.len(), 2);
    assert!(
        bodies
            .iter()
            .all(|body| matches!(body.state, BodyState::Refused(_)))
    );
}

#[test]
fn forced_refusal_fails_at_once() {
    let cpu = Refuser {
        id: "cpu",
        refuse: &["t.x"],
        whole: false,
    };
    let mut doc = node("t.x");
    doc.backend = Some("cpu".to_string());
    let error = run(vec![doc], Vec::new(), &[&cpu, &GPU]).unwrap_err();
    assert!(
        matches!(error, RegionError::ForcedRefused { .. }),
        "{error:?}"
    );
}

#[test]
fn whole_region_refusal_refuses_every_node_in_it() {
    let cpu = Refuser {
        id: "cpu",
        refuse: &["t.y"],
        whole: true,
    };
    let result = run(
        vec![node("t.x"), node("t.y")],
        vec![edge(0, "out", 1, "in")],
        &[&cpu, &GPU],
    )
    .unwrap();
    assert_eq!((rank(&result, 0), rank(&result, 1)), (1, 1));
}
