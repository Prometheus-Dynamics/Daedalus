//! A group's edge policies: each sets the policy of the connects into one stage input, in the
//! declared form and the document form, and one that matches no connect, or a second for the same
//! input, is refused by name.

use std::collections::BTreeMap;

use daedalus_graph::{
    EdgePolicy, EdgePolicyDoc, GraphBody, GroupDef, GroupDoc, StagePolicyDoc, StagePort,
    StagePortDoc,
};
use daedalus_planner::solve::Graph;
use daedalus_planner::solve::expand::ExpandError;
use daedalus_planner::{Config, SolveError, solve};
use daedalus_planner_tests::{decl, node, u8_ty};
use daedalus_registry::Registry;

const fn stage(stage: &'static str, port: &'static str) -> StagePort {
    StagePort { stage, port }
}

/// `a` feeds `b` and `c`; `b`'s edge keeps only the newest value.
static DECLARED: GroupDef = GroupDef {
    stages: &[("a", "t.pass"), ("b", "t.pass"), ("c", "t.pass")],
    connects: &[
        (stage("a", "out"), stage("b", "in")),
        (stage("a", "out"), stage("c", "in")),
    ],
    inputs: &[("in", &[stage("a", "in")])],
    outputs: &[("b", stage("b", "out")), ("c", stage("c", "out"))],
    edge_policies: &[(stage("b", "in"), EdgePolicy::LatestOnly)],
    ..GroupDef::new("t.declared")
};

static UNMATCHED: GroupDef = GroupDef {
    stages: &[("a", "t.pass"), ("b", "t.pass")],
    connects: &[(stage("a", "out"), stage("b", "in"))],
    inputs: &[("in", &[stage("a", "in")])],
    outputs: &[("out", stage("b", "out"))],
    edge_policies: &[(stage("b", "inn"), EdgePolicy::LatestOnly)],
    ..GroupDef::new("t.unmatched")
};

static TWICE: GroupDef = GroupDef {
    stages: &[("a", "t.pass"), ("b", "t.pass")],
    connects: &[(stage("a", "out"), stage("b", "in"))],
    inputs: &[("in", &[stage("a", "in")])],
    outputs: &[("out", stage("b", "out"))],
    edge_policies: &[
        (stage("b", "in"), EdgePolicy::LatestOnly),
        (
            stage("b", "in"),
            EdgePolicy::Bounded {
                capacity: 2,
                overflow: "drop_oldest",
            },
        ),
    ],
    ..GroupDef::new("t.twice")
};

/// The group's input `in` feeds `a` and `b`; `a` fires only sometimes, so its edge keeps only the
/// newest value whatever edge the caller feeds `in` with.
static FANNED: GroupDef = GroupDef {
    stages: &[("a", "t.pass"), ("b", "t.pass")],
    inputs: &[("in", &[stage("a", "in"), stage("b", "in")])],
    outputs: &[("a", stage("a", "out")), ("b", stage("b", "out"))],
    edge_policies: &[(stage("a", "in"), EdgePolicy::LatestOnly)],
    ..GroupDef::new("t.fanned")
};

/// `FANNED` inside another group, fed by a connect with no policy.
static NESTING: GroupDef = GroupDef {
    stages: &[("src", "t.pass"), ("inner", "t.fanned")],
    connects: &[(stage("src", "out"), stage("inner", "in"))],
    inputs: &[("in", &[stage("src", "in")])],
    outputs: &[("a", stage("inner", "a")), ("b", stage("inner", "b"))],
    ..GroupDef::new("t.nesting")
};

/// `FANNED` inside another group whose own policy for that connect differs from `FANNED`'s.
static NESTING_BOUNDED: GroupDef = GroupDef {
    edge_policies: &[(
        stage("inner", "in"),
        EdgePolicy::Bounded {
            capacity: 4,
            overflow: "drop_oldest",
        },
    )],
    ..NESTING
};

fn registry() -> Registry {
    let mut registry = Registry::new();
    decl("t.pass")
        .input("in", u8_ty())
        .output("out", u8_ty())
        .body("sim")
        .add(&mut registry);
    for group in [&DECLARED, &UNMATCHED, &TWICE, &FANNED, &NESTING] {
        let mut declared = decl(group.id).input("in", u8_ty());
        for (name, _) in group.outputs {
            declared = declared.output(name, u8_ty());
        }
        declared.group(group).body("sim").add(&mut registry);
    }
    let mut declared = decl("t.nesting_bounded").input("in", u8_ty());
    for (name, _) in NESTING_BOUNDED.outputs {
        declared = declared.output(name, u8_ty());
    }
    declared
        .group(&NESTING_BOUNDED)
        .body("sim")
        .add(&mut registry);
    registry
}

fn run(body: GraphBody) -> Result<Graph, Vec<SolveError>> {
    solve(&body, &registry(), &Config::default())
}

fn one(id: &str) -> GraphBody {
    GraphBody {
        nodes: vec![node(id)],
        ..GraphBody::default()
    }
}

/// Each internal edge's policy, by the label of the node it leads into.
fn policies(graph: &Graph) -> BTreeMap<String, EdgePolicyDoc> {
    graph
        .edges
        .iter()
        .map(|edge| {
            let to = &graph.nodes[edge.to.node as usize].doc;
            (to.label.clone().unwrap_or_default(), edge.policy.clone())
        })
        .collect()
}

#[test]
fn a_declared_group_gives_one_connect_its_policy_and_the_rest_fifo() {
    let graph = run(one("t.declared")).unwrap();
    let policies = policies(&graph);
    assert_eq!(policies.len(), 2, "{policies:?}");
    let by_stage = |name: &str| {
        policies
            .iter()
            .find(|(label, _)| label.ends_with(&format!("::{name}")))
            .map(|(_, policy)| policy.clone())
    };
    assert_eq!(by_stage("b"), Some(EdgePolicyDoc::LatestOnly));
    assert_eq!(by_stage("c"), Some(EdgePolicyDoc::Fifo));
}

#[test]
fn the_document_form_reads_and_expands_the_same() {
    let text = r#"{
        "nodes": [{ "id": "g.doc", "kind": "group" }],
        "groups": [{
            "id": "g.doc",
            "stages": { "a": "t.pass", "b": "t.pass" },
            "connects": [[{ "stage": "a", "port": "out" }, { "stage": "b", "port": "in" }]],
            "inputs": { "in": [{ "stage": "a", "port": "in" }] },
            "outputs": { "out": { "stage": "b", "port": "out" } },
            "edge_policies": [{
                "at": { "stage": "b", "port": "in" },
                "policy": { "kind": "bounded", "capacity": 2, "overflow": "drop_oldest" }
            }]
        }]
    }"#;
    let body: GraphBody = serde_json::from_str(text).unwrap();
    let group: &GroupDoc = &body.groups[0];
    assert_eq!(
        group.edge_policies,
        [StagePolicyDoc {
            at: StagePortDoc {
                stage: "b".into(),
                port: "in".into(),
            },
            policy: EdgePolicyDoc::Bounded {
                capacity: 2,
                overflow: "drop_oldest".into(),
            },
        }]
    );
    let graph = run(body.clone()).unwrap();
    assert_eq!(
        graph.edges[0].policy,
        EdgePolicyDoc::Bounded {
            capacity: 2,
            overflow: "drop_oldest".into(),
        }
    );
    // A group without them writes none, so its text is what it was before them.
    let mut plain = body;
    plain.groups[0].edge_policies.clear();
    let written = serde_json::to_string(&plain.groups[0]).unwrap();
    assert!(!written.contains("edge_policies"), "{written}");
}

#[test]
fn a_policy_no_connect_leads_into_is_refused_by_name() {
    assert_eq!(
        run(one("t.unmatched")).unwrap_err(),
        [SolveError::Expand(ExpandError::EdgePolicyUnmatched {
            group: "t.unmatched".into(),
            stage: "b".into(),
            port: "inn".into(),
        })]
    );
}

#[test]
fn two_policies_for_one_input_are_refused_by_name() {
    assert_eq!(
        run(one("t.twice")).unwrap_err(),
        [SolveError::Expand(ExpandError::EdgePolicyTwice {
            group: "t.twice".into(),
            stage: "b".into(),
            port: "in".into(),
        })]
    );
}

/// `t.pass` feeding the group `id` on its input `in`, over an edge with `policy`.
fn fed(id: &str, policy: EdgePolicyDoc) -> GraphBody {
    let mut feed = daedalus_planner_tests::edge(0, "out", 1, "in");
    feed.policy = policy;
    GraphBody {
        nodes: vec![node("t.pass"), node(id)],
        edges: vec![feed],
        groups: Vec::new(),
    }
}

fn into_stage(graph: &Graph, name: &str) -> EdgePolicyDoc {
    let suffix = format!("::{name}");
    let found: Vec<_> = graph
        .edges
        .iter()
        .filter(|edge| {
            let to = &graph.nodes[edge.to.node as usize].doc;
            to.label
                .as_deref()
                .is_some_and(|label| label.ends_with(&suffix))
        })
        .collect();
    assert_eq!(found.len(), 1, "{:?}", graph.edges);
    found[0].policy.clone()
}

#[test]
fn a_policy_on_a_group_inputs_target_applies_to_the_callers_edge() {
    for caller in [EdgePolicyDoc::Fifo, EdgePolicyDoc::LatestOnly] {
        let graph = run(fed("t.fanned", caller.clone())).unwrap();
        assert_eq!(
            into_stage(&graph, "a"),
            EdgePolicyDoc::LatestOnly,
            "{caller:?}"
        );
        // The input's other target keeps the caller's edge as it is.
        assert_eq!(into_stage(&graph, "b"), caller);
    }
}

#[test]
fn a_callers_policy_that_differs_from_the_groups_is_refused_by_name() {
    let bounded = EdgePolicyDoc::Bounded {
        capacity: 4,
        overflow: "drop_oldest".into(),
    };
    let errors = run(fed("t.fanned", bounded.clone())).unwrap_err();
    let text = errors[0].to_string();
    assert!(
        text.contains("drop that edge's own policy, or make it LatestOnly"),
        "{text}"
    );
    assert_eq!(
        errors,
        [SolveError::Expand(ExpandError::EdgePolicyConflict {
            group: "t.fanned".into(),
            stage: "a".into(),
            port: "in".into(),
            group_policy: EdgePolicyDoc::LatestOnly,
            edge_policy: bounded,
        })]
    );
}

#[test]
fn a_nested_groups_input_policy_applies_to_the_enclosing_connect() {
    let graph = run(fed("t.nesting", EdgePolicyDoc::Fifo)).unwrap();
    assert_eq!(into_stage(&graph, "a"), EdgePolicyDoc::LatestOnly);
    assert_eq!(into_stage(&graph, "b"), EdgePolicyDoc::Fifo);
    let errors = run(fed("t.nesting_bounded", EdgePolicyDoc::Fifo)).unwrap_err();
    assert!(
        matches!(
            &errors[..],
            [SolveError::Expand(ExpandError::EdgePolicyConflict { group, stage, .. })]
                if group == "t.fanned" && stage == "a"
        ),
        "{errors:?}"
    );
}
