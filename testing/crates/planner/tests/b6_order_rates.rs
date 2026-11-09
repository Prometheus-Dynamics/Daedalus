use daedalus_graph::{EdgeDoc, GraphBody, NodeDoc, RateDoc, SyncGroupDoc, SyncPolicy};
use daedalus_planner::solve::Graph;
use daedalus_planner::solve::order::OrderError;
use daedalus_planner::solve::rates::{Rate, RateError};
use daedalus_planner::{Config, SolveError, solve};
use daedalus_planner_tests::{decl, edge, node, u8_ty};
use daedalus_registry::Registry;

fn registry() -> Registry {
    let mut registry = Registry::new();
    decl("t.src")
        .output("out", u8_ty())
        .body("sim")
        .add(&mut registry);
    decl("t.pass")
        .input("in", u8_ty())
        .output("out", u8_ty())
        .body("sim")
        .add(&mut registry);
    decl("t.join")
        .input("a", u8_ty())
        .input("b", u8_ty())
        .output("out", u8_ty())
        .body("sim")
        .add(&mut registry);
    registry
}

fn run(nodes: Vec<NodeDoc>, edges: Vec<EdgeDoc>) -> Result<Graph, Vec<SolveError>> {
    solve(
        &GraphBody {
            nodes,
            edges,
            groups: Vec::new(),
        },
        &registry(),
        &Config::default(),
    )
}

fn ratio(num: u32, den: u32) -> NodeDoc {
    let mut doc = node("t.pass");
    doc.rate = Some(RateDoc { num, den });
    doc
}

fn rate(num: u64, den: u64) -> Rate {
    Rate::new(num, den).unwrap()
}

#[test]
fn order_is_index_stable_for_two_roots() {
    let graph = run(
        vec![node("t.pass"), node("t.src"), node("t.src"), node("t.pass")],
        vec![edge(2, "out", 0, "in"), edge(1, "out", 3, "in")],
    )
    .unwrap();
    assert_eq!(graph.order, [1, 2, 0, 3]);
}

#[test]
fn cycle_names_every_node_on_it() {
    let mut last = node("t.pass");
    last.label = Some("hangs off the cycle".to_string());
    let errors = run(
        vec![node("t.join"), node("t.pass"), node("t.src"), node("t.src")],
        vec![
            edge(0, "out", 1, "in"),
            edge(1, "out", 0, "a"),
            edge(2, "out", 0, "b"),
        ],
    )
    .unwrap_err();
    assert_eq!(
        errors,
        [SolveError::Order(OrderError::ScheduleConflict {
            cycle: vec!["t.join".to_string(), "t.pass".to_string()],
        })]
    );
}

#[test]
fn a_node_after_a_cycle_is_not_named() {
    let errors = run(
        vec![node("t.join"), node("t.pass"), node("t.src"), ratio(1, 1)],
        vec![
            edge(0, "out", 1, "in"),
            edge(1, "out", 0, "a"),
            edge(2, "out", 0, "b"),
            edge(1, "out", 3, "in"),
        ],
    )
    .unwrap_err();
    let [SolveError::Order(OrderError::ScheduleConflict { cycle })] = &errors[..] else {
        panic!("{errors:?}");
    };
    assert_eq!(cycle.len(), 2);
}

#[test]
fn ratio_node_changes_out_rate() {
    let graph = run(
        vec![node("t.src"), ratio(1, 2), ratio(2, 3), node("t.pass")],
        vec![
            edge(0, "out", 1, "in"),
            edge(1, "out", 2, "in"),
            edge(2, "out", 3, "in"),
        ],
    )
    .unwrap();
    assert_eq!(
        (graph.nodes[1].rate_in, graph.nodes[1].rate_out),
        (Rate::ONE, rate(1, 2))
    );
    assert_eq!(graph.nodes[2].rate_out, rate(1, 3));
    assert_eq!(graph.nodes[3].rate_in, rate(1, 3));
    assert_eq!(graph.edge_rates, [Rate::ONE, rate(1, 2), rate(1, 3)]);
}

#[test]
fn rate_mismatch_at_join_is_error() {
    let errors = run(
        vec![node("t.src"), ratio(1, 2), node("t.join")],
        vec![
            edge(0, "out", 1, "in"),
            edge(0, "out", 2, "a"),
            edge(1, "out", 2, "b"),
        ],
    )
    .unwrap_err();
    assert_eq!(
        errors,
        [SolveError::Rates(RateError::RateMismatch {
            node: "t.join".into(),
            first: Rate::ONE,
            second: rate(1, 2),
        })]
    );
}

fn synced(policy: SyncPolicy) -> NodeDoc {
    let mut join = node("t.join");
    join.sync_groups.push(SyncGroupDoc {
        name: "pair".to_string(),
        policy,
        ports: vec!["a".to_string(), "b".to_string()],
    });
    join
}

#[test]
fn sync_group_ports_must_share_rate() {
    let errors = run(
        vec![node("t.src"), ratio(1, 2), synced(SyncPolicy::AllReady)],
        vec![
            edge(0, "out", 1, "in"),
            edge(0, "out", 2, "a"),
            edge(1, "out", 2, "b"),
        ],
    )
    .unwrap_err();
    assert!(
        matches!(&errors[..], [SolveError::Rates(RateError::SyncGroupRate { group, .. })] if group == "pair"),
        "{errors:?}"
    );
}

#[test]
fn alignment_policy_recorded_not_applied() {
    let graph = run(
        vec![node("t.src"), synced(SyncPolicy::ZipByTag)],
        vec![edge(0, "out", 1, "a"), edge(0, "out", 1, "b")],
    )
    .unwrap();
    assert_eq!(
        graph.nodes[1].doc.sync_groups[0].policy,
        SyncPolicy::ZipByTag
    );
    assert_eq!(graph.edges.len(), 2, "nothing aligns data here");
}

#[test]
fn a_zero_rate_is_rejected() {
    let errors = run(vec![ratio(0, 2)], Vec::new()).unwrap_err();
    assert!(matches!(
        &errors[..],
        [SolveError::Rates(RateError::RateInvalid { .. })]
    ));
}
