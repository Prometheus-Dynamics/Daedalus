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

fn registry() -> Registry {
    let mut registry = Registry::new();
    decl("t.pass")
        .input("in", u8_ty())
        .output("out", u8_ty())
        .body("sim")
        .add(&mut registry);
    for group in [&DECLARED, &UNMATCHED, &TWICE] {
        let mut declared = decl(group.id).input("in", u8_ty());
        for (name, _) in group.outputs {
            declared = declared.output(name, u8_ty());
        }
        declared.group(group).body("sim").add(&mut registry);
    }
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
