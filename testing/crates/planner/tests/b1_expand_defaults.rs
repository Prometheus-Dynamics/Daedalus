use std::collections::BTreeMap;

use daedalus_graph::{GraphBody, GroupDoc, Lit, NodeKindDoc, StagePortDoc};
use daedalus_planner::solve::expand::ExpandError;
use daedalus_planner::solve::hydrate::HydrateError;
use daedalus_planner::{Config, SolveError, solve};
use daedalus_planner_tests::{decl, edge, node, u8_ty};
use daedalus_registry::{Fire, Registry};

fn registry() -> Registry {
    let mut registry = Registry::new();
    for id in ["t.pass", "t.other"] {
        decl(id)
            .input("in", u8_ty())
            .output("out", u8_ty())
            .body("sim")
            .add(&mut registry);
    }
    decl("t.any")
        .input("in", u8_ty())
        .optional("extra", u8_ty())
        .output("out", u8_ty())
        .fire(Fire::Any)
        .body("sim")
        .add(&mut registry);
    registry
}

fn at(stage: &str, port: &str) -> StagePortDoc {
    StagePortDoc {
        stage: stage.to_string(),
        port: port.to_string(),
    }
}

/// A group holding one `inner` stage that runs `stage_id`, exposed as `in` and `out`.
fn wrapper(id: &str, stage_id: &str) -> GroupDoc {
    GroupDoc {
        id: id.to_string(),
        stages: BTreeMap::from([("inner".to_string(), stage_id.to_string())]),
        connects: Vec::new(),
        inputs: BTreeMap::from([("in".to_string(), vec![at("inner", "in")])]),
        outputs: BTreeMap::from([("out".to_string(), at("inner", "out"))]),
        consts: Vec::new(),
        metadata: BTreeMap::new(),
    }
}

fn group_node(id: &str) -> daedalus_graph::NodeDoc {
    let mut group = node(id);
    group.kind = NodeKindDoc::Group;
    group
}

fn run(body: GraphBody) -> Result<daedalus_planner::solve::Graph, Vec<SolveError>> {
    solve(&body, &registry(), &Config::default())
}

#[test]
fn nested_groups_expand_to_a_flat_graph() {
    let body = GraphBody {
        nodes: vec![node("t.pass"), group_node("g.outer"), node("t.other")],
        edges: vec![edge(0, "out", 1, "in"), edge(1, "out", 2, "in")],
        groups: vec![wrapper("g.outer", "g.inner"), wrapper("g.inner", "t.pass")],
    };
    let graph = run(body).unwrap();
    assert_eq!(graph.nodes.len(), 3);
    let middle = &graph.nodes[1];
    assert_eq!(middle.doc.id, "t.pass");
    assert_eq!(middle.doc.label.as_deref(), Some("g.outer::inner::inner"));
    assert_eq!(
        middle.group.as_deref(),
        Some("g.outer"),
        "rolls up to the outermost group"
    );
    assert_eq!(
        graph.edges,
        [edge(0, "out", 1, "in"), edge(1, "out", 2, "in")]
    );
}

#[test]
fn a_group_const_reaches_the_stage_and_the_outer_value_wins() {
    let mut group = wrapper("g.c", "t.pass");
    group.consts.push(daedalus_graph::StageConstDoc {
        at: at("inner", "in"),
        value: Lit::Int(1),
    });
    let graph = run(GraphBody {
        nodes: vec![group_node("g.c")],
        edges: Vec::new(),
        groups: vec![group.clone()],
    })
    .unwrap();
    assert_eq!(graph.nodes[0].doc.consts["in"], Lit::Int(1));
    let mut outer = group_node("g.c");
    outer.consts.insert("in".to_string(), Lit::Int(9));
    let graph = run(GraphBody {
        nodes: vec![outer],
        edges: Vec::new(),
        groups: vec![group],
    })
    .unwrap();
    assert_eq!(graph.nodes[0].doc.consts["in"], Lit::Int(9));
}

#[test]
fn a_group_on_a_declaration_expands_too() {
    static STAGES: &[(&str, &str)] = &[("a", "t.pass")];
    static GROUP: daedalus_graph::GroupDef = daedalus_graph::GroupDef {
        id: "t.declared",
        stages: STAGES,
        connects: &[],
        inputs: &[],
        outputs: &[(
            "out",
            daedalus_graph::StagePort {
                stage: "a",
                port: "out",
            },
        )],
        consts: &[],
        metadata: &[],
    };
    let mut registry = registry();
    decl("t.declared")
        .output("out", u8_ty())
        .group(&GROUP)
        .body("sim")
        .add(&mut registry);
    let body = GraphBody {
        nodes: vec![node("t.declared"), node("t.other")],
        edges: vec![edge(0, "out", 1, "in")],
        groups: Vec::new(),
    };
    let graph = solve(&body, &registry, &Config::default()).unwrap();
    assert_eq!(graph.nodes[0].doc.id, "t.pass");
    assert_eq!(graph.edges, [edge(0, "out", 1, "in")]);
}

#[test]
fn group_depth_17_fails_with_the_chain() {
    let mut groups: Vec<GroupDoc> = (0..16)
        .map(|i| wrapper(&format!("g.{i}"), &format!("g.{}", i + 1)))
        .collect();
    groups.push(wrapper("g.16", "t.pass"));
    let body = GraphBody {
        nodes: vec![group_node("g.0")],
        edges: Vec::new(),
        groups,
    };
    let errors = run(body).unwrap_err();
    let [SolveError::Expand(ExpandError::GroupDepth { chain })] = &errors[..] else {
        panic!("{errors:?}");
    };
    assert_eq!(chain.len(), 17);
    assert_eq!((chain[0].as_str(), chain[16].as_str()), ("g.0", "g.16"));
}

#[test]
fn sixteen_levels_are_allowed() {
    let mut groups: Vec<GroupDoc> = (0..15)
        .map(|i| wrapper(&format!("g.{i}"), &format!("g.{}", i + 1)))
        .collect();
    groups.push(wrapper("g.15", "t.pass"));
    let body = GraphBody {
        nodes: vec![group_node("g.0")],
        edges: Vec::new(),
        groups,
    };
    assert_eq!(run(body).unwrap().nodes.len(), 1);
}

#[test]
fn outer_edge_to_unexposed_group_port_names_the_port() {
    let body = GraphBody {
        nodes: vec![node("t.pass"), group_node("g.w")],
        edges: vec![edge(0, "out", 1, "secret")],
        groups: vec![wrapper("g.w", "t.pass")],
    };
    assert_eq!(
        run(body).unwrap_err(),
        [SolveError::Expand(ExpandError::PortMissing {
            group: "g.w".into(),
            node: "g.w".into(),
            port: "secret".into(),
        })]
    );
}

#[test]
fn missing_ids_reported_once_each_with_suggestions() {
    let body = GraphBody {
        nodes: vec![node("t.pasz"), node("t.pasz"), node("zzz")],
        edges: Vec::new(),
        groups: Vec::new(),
    };
    let errors = run(body).unwrap_err();
    assert_eq!(errors.len(), 2);
    let SolveError::Hydrate(HydrateError::NodeMissing { id, suggestions }) = &errors[0] else {
        panic!("{errors:?}");
    };
    assert_eq!(id, "t.pasz");
    assert_eq!(suggestions[0], "t.pass");
}

#[test]
fn graph_value_overrides_declared_fire_mode() {
    let mut any = node("t.any");
    assert_eq!(
        run(GraphBody {
            nodes: vec![any.clone()],
            ..GraphBody::default()
        })
        .unwrap()
        .nodes[0]
            .fire,
        Fire::Any
    );
    any.fire = Some("all".to_string());
    assert_eq!(
        run(GraphBody {
            nodes: vec![any],
            ..GraphBody::default()
        })
        .unwrap()
        .nodes[0]
            .fire,
        Fire::All
    );
}

#[test]
fn declared_required_inputs_copied_to_solved_node() {
    let graph = run(GraphBody {
        nodes: vec![node("t.any")],
        ..GraphBody::default()
    })
    .unwrap();
    assert_eq!(
        graph.nodes[0].required,
        ["in"],
        "an optional input isn't required"
    );
}

#[test]
fn host_bridge_declared_types_are_kept() {
    let mut bridge = node("host.in");
    bridge.kind = NodeKindDoc::HostBridge;
    bridge
        .port_types
        .insert("frame".to_string(), "u8".to_string());
    let graph = run(GraphBody {
        nodes: vec![bridge],
        ..GraphBody::default()
    })
    .unwrap();
    assert_eq!(graph.nodes[0].doc.port_types["frame"], "u8");
}
