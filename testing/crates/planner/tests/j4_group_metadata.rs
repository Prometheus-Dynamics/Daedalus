//! A group's stages keep metadata: what the group's own node was given, and what the group's
//! definition gives each stage, which wins.

use std::collections::BTreeMap;

use daedalus_backend::Compiled;
use daedalus_graph::{GraphBody, GroupDef, GroupDoc, Lit, NodeDoc, NodeKindDoc, StagePortDoc};
use daedalus_planner::{Config, plan, solve};
use daedalus_planner_tests::{decl, edge, node, u8_ty};
use daedalus_registry::Registry;
use daedalus_sim::{BACKEND_ID, SimBackend, SimConfig};
use daedalus_types::Literal;

const FUSION: &str = "daedalus.node.fusion";
const RECOVERY: &str = "daedalus.node.recovery";

fn registry() -> Registry {
    let mut registry = Registry::new();
    decl("t.pass")
        .input("in", u8_ty())
        .output("out", u8_ty())
        .body(BACKEND_ID)
        .add(&mut registry);
    decl("t.source")
        .output("out", u8_ty())
        .body(BACKEND_ID)
        .add(&mut registry);
    registry
}

fn at(stage: &str, port: &str) -> StagePortDoc {
    StagePortDoc {
        stage: stage.to_string(),
        port: port.to_string(),
    }
}

/// A group of two stages in a row, `first` then `second`, exposed as `in` and `out`.
fn pair(id: &str, first: &str, second: &str) -> GroupDoc {
    GroupDoc {
        id: id.to_string(),
        stages: BTreeMap::from([
            ("first".to_string(), first.to_string()),
            ("second".to_string(), second.to_string()),
        ]),
        connects: vec![(at("first", "out"), at("second", "in"))],
        inputs: BTreeMap::from([("in".to_string(), vec![at("first", "in")])]),
        outputs: BTreeMap::from([("out".to_string(), at("second", "out"))]),
        consts: Vec::new(),
        metadata: BTreeMap::new(),
    }
}

fn group_node(id: &str, metadata: &[(&str, Lit)]) -> NodeDoc {
    let mut group = node(id);
    group.kind = NodeKindDoc::Group;
    for (key, value) in metadata {
        group.metadata.insert((*key).to_string(), value.clone());
    }
    group
}

fn text(value: &str) -> Lit {
    Lit::Str(value.to_string())
}

/// A source feeding one group node, with the group definitions given.
fn body(group: NodeDoc, groups: Vec<GroupDoc>) -> GraphBody {
    GraphBody {
        nodes: vec![node("t.source"), group],
        edges: vec![edge(0, "out", 1, "in")],
        groups,
    }
}

fn stage_metadata(body: &GraphBody) -> Vec<(String, BTreeMap<String, Lit>)> {
    let graph = solve(body, &registry(), &Config::default()).unwrap();
    graph
        .nodes
        .iter()
        .filter(|node| node.group.is_some())
        .map(|node| {
            (
                node.doc.label.clone().unwrap_or_default(),
                node.doc.metadata.clone(),
            )
        })
        .collect()
}

#[test]
fn a_group_nodes_metadata_reaches_its_stages() {
    let instance = group_node(
        "g.pair",
        &[(FUSION, Lit::Bool(false)), (RECOVERY, text("skip"))],
    );
    let stages = stage_metadata(&body(instance, vec![pair("g.pair", "t.pass", "t.pass")]));
    assert_eq!(stages.len(), 2);
    for (label, metadata) in &stages {
        assert_eq!(metadata.get(FUSION), Some(&Lit::Bool(false)), "{label}");
        assert_eq!(metadata.get(RECOVERY), Some(&text("skip")), "{label}");
    }
}

#[test]
fn a_nested_groups_stages_carry_it_too() {
    let instance = group_node("g.outer", &[(RECOVERY, text("skip"))]);
    let groups = vec![
        pair("g.outer", "g.inner", "t.pass"),
        pair("g.inner", "t.pass", "t.pass"),
    ];
    let stages = stage_metadata(&body(instance, groups));
    assert_eq!(stages.len(), 3, "two in the inner group and one beside it");
    for (label, metadata) in &stages {
        assert_eq!(metadata.get(RECOVERY), Some(&text("skip")), "{label}");
    }
}

#[test]
fn a_stages_own_metadata_is_kept_and_wins() {
    let instance = group_node(
        "g.pair",
        &[(FUSION, Lit::Bool(true)), (RECOVERY, text("skip"))],
    );
    let mut group = pair("g.pair", "t.pass", "t.pass");
    group.metadata.insert(
        "second".to_string(),
        BTreeMap::from([
            (FUSION.to_string(), Lit::Bool(false)),
            ("t.own".to_string(), Lit::Int(4)),
        ]),
    );
    let stages = stage_metadata(&body(instance, vec![group]));
    let of = |name: &str| {
        &stages
            .iter()
            .find(|(label, _)| label.ends_with(name))
            .unwrap()
            .1
    };
    assert_eq!(of("first").get(FUSION), Some(&Lit::Bool(true)));
    assert_eq!(of("first").get("t.own"), None);
    assert_eq!(
        of("second").get(FUSION),
        Some(&Lit::Bool(false)),
        "its own wins"
    );
    assert_eq!(of("second").get("t.own"), Some(&Lit::Int(4)));
    assert_eq!(
        of("second").get(RECOVERY),
        Some(&text("skip")),
        "the rest is inherited"
    );
    // With no metadata on the group's node, a stage still has its own.
    let mut group = pair("g.pair", "t.pass", "t.pass");
    group.metadata.insert(
        "first".to_string(),
        BTreeMap::from([(RECOVERY.to_string(), text("skip"))]),
    );
    let stages = stage_metadata(&body(group_node("g.pair", &[]), vec![group]));
    assert_eq!(stages[0].1.get(RECOVERY), Some(&text("skip")));
    assert!(stages[1].1.is_empty());
}

#[test]
fn a_declared_groups_stage_metadata_is_kept() {
    static GROUP: GroupDef = GroupDef {
        id: "t.declared",
        stages: &[("only", "t.pass")],
        connects: &[],
        inputs: &[(
            "in",
            &[daedalus_graph::StagePort {
                stage: "only",
                port: "in",
            }],
        )],
        outputs: &[(
            "out",
            daedalus_graph::StagePort {
                stage: "only",
                port: "out",
            },
        )],
        consts: &[],
        metadata: &[("only", FUSION, Literal::Bool(false))],
    };
    let mut registry = registry();
    decl("t.declared")
        .input("in", u8_ty())
        .output("out", u8_ty())
        .group(&GROUP)
        .body(BACKEND_ID)
        .add(&mut registry);
    let body = GraphBody {
        nodes: vec![node("t.source"), node("t.declared")],
        edges: vec![edge(0, "out", 1, "in")],
        groups: Vec::new(),
    };
    let graph = solve(&body, &registry, &Config::default()).unwrap();
    assert_eq!(
        graph.nodes[1].doc.metadata.get(FUSION),
        Some(&Lit::Bool(false))
    );
}

/// The nodes each step covers, for a group whose node has `metadata`.
fn steps(metadata: &[(&str, Lit)]) -> Vec<Vec<u32>> {
    let backend = SimBackend::new(SimConfig {
        merge: true,
        ..SimConfig::default()
    });
    let body = body(
        group_node("g.pair", metadata),
        vec![pair("g.pair", "t.pass", "t.pass")],
    );
    let planned = plan(&body, &registry(), &[&backend], &Config::default()).unwrap();
    let mut steps = Vec::new();
    for region in &planned.regions.regions {
        let Compiled::Steps(units) = &region.compiled else {
            panic!("the sim compiles to steps");
        };
        for unit in units {
            steps.push(unit.info.covers.iter().map(|node| node.0).collect());
        }
    }
    steps
}

#[test]
fn a_merge_the_groups_fusion_key_forbids_is_not_made() {
    assert_eq!(
        steps(&[]),
        [vec![0, 1, 2]],
        "one merged step without the key"
    );
    assert_eq!(
        steps(&[(FUSION, Lit::Bool(false))]),
        [vec![0], vec![1], vec![2]],
        "the stages are steps of their own"
    );
}
