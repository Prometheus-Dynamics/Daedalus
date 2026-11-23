//! What a graph needs of a host: the providers of its nodes, listed by the registry and checked
//! against a stored document's `requires`.

use std::collections::BTreeMap;

use daedalus_graph::{
    GraphBody, GraphDoc, GroupDef, GroupDoc, NodeKindDoc, Requirement, StagePort,
};
use daedalus_planner_tests::{decl, node, u8_ty};
use daedalus_registry::{Registry, RequireError};

const fn stage(stage: &'static str, port: &'static str) -> StagePort {
    StagePort { stage, port }
}

/// A group from `vision` whose one stage is a node from `filters`.
static WRAPPED: GroupDef = GroupDef {
    stages: &[("inner", "f.blur")],
    inputs: &[("in", &[stage("inner", "in")])],
    outputs: &[("out", stage("inner", "out"))],
    ..GroupDef::new("v.wrapped")
};

fn registry() -> Registry {
    let mut registry = Registry::new();
    let pass = |id: &str| {
        decl(id)
            .input("in", u8_ty())
            .output("out", u8_ty())
            .body("sim")
    };
    pass("v.detect")
        .provider("vision", "1.4.2")
        .add(&mut registry);
    pass("v.refine")
        .provider("vision", "1.4.2")
        .add(&mut registry);
    pass("f.blur")
        .provider("filters", "0.3.1")
        .add(&mut registry);
    // A plugin's node: provided by name, with no version.
    pass("p.infer")
        .provider("npu-plugin", "")
        .add(&mut registry);
    // A node nobody provides.
    pass("x.loose").add(&mut registry);
    pass("v.wrapped")
        .provider("vision", "1.4.2")
        .group(&WRAPPED)
        .add(&mut registry);
    registry
}

fn requirement(id: &str, version: Option<&str>) -> Requirement {
    Requirement {
        id: id.to_string(),
        version: version.map(str::to_string),
    }
}

fn graph(ids: &[&str]) -> GraphBody {
    let mut host = node("host");
    host.kind = NodeKindDoc::HostBridge;
    let mut nodes = vec![host];
    nodes.extend(ids.iter().map(|id| node(id)));
    GraphBody {
        nodes,
        ..GraphBody::default()
    }
}

#[test]
fn a_graphs_requirements_are_the_providers_of_its_nodes_each_once() {
    let needs = registry().requirements(&graph(&[
        "v.detect",
        "v.refine",
        "p.infer",
        "x.loose",
        "nobody.registered",
    ]));
    assert_eq!(
        needs,
        [
            requirement("npu-plugin", None),
            requirement("vision", Some("1.4.2")),
        ]
    );
}

#[test]
fn a_groups_stages_count_whether_the_group_is_registered_or_in_the_document() {
    // A registered group: its own provider and its stage's.
    assert_eq!(
        registry().requirements(&graph(&["v.wrapped"])),
        [
            requirement("filters", Some("0.3.1")),
            requirement("vision", Some("1.4.2")),
        ]
    );
    // A group the document defines.
    let mut body = graph(&[]);
    body.groups.push(GroupDoc {
        id: "local".to_string(),
        stages: BTreeMap::from([("only".to_string(), "f.blur".to_string())]),
        connects: Vec::new(),
        inputs: BTreeMap::new(),
        outputs: BTreeMap::new(),
        consts: Vec::new(),
        metadata: BTreeMap::new(),
        edge_policies: Vec::new(),
    });
    assert_eq!(
        registry().requirements(&body),
        [requirement("filters", Some("0.3.1"))]
    );
}

fn check(requires: Vec<Requirement>) -> Vec<String> {
    let mut doc = GraphDoc::new(graph(&["v.detect"]));
    doc.requires = requires;
    match registry().check_requires(&doc) {
        Ok(()) => Vec::new(),
        Err(errors) => errors.iter().map(RequireError::to_string).collect(),
    }
}

#[test]
fn a_document_that_requires_what_the_host_has_is_accepted() {
    // What the registry itself says the graph needs.
    let needs = registry().requirements(&graph(&["v.detect", "v.wrapped", "p.infer"]));
    assert_eq!(check(needs), Vec::<String>::new());
    // An older requirement on the same major line, and one with no version at all.
    let older = vec![
        requirement("vision", Some("1.2.0")),
        requirement("vision", Some("1")),
        requirement("filters", None),
        requirement("npu-plugin", None),
    ];
    assert_eq!(check(older), Vec::<String>::new());
    assert_eq!(check(Vec::new()), Vec::<String>::new());
}

#[test]
fn a_provider_the_host_lacks_is_refused_by_name() {
    assert_eq!(
        check(vec![
            requirement("vision-nodes", Some("0.4.0")),
            requirement("gone", None),
        ]),
        [
            "requires `vision-nodes` 0.4.0, and no node from it is registered",
            "requires `gone`, and no node from it is registered",
        ]
    );
}

#[test]
fn a_version_the_host_does_not_meet_is_refused_with_both_versions() {
    // Newer than the host has; another major line; another minor line while the major is 0.
    assert_eq!(
        check(vec![
            requirement("vision", Some("1.5.0")),
            requirement("vision", Some("2.0.0")),
            requirement("vision", Some("0.9.0")),
            requirement("filters", Some("0.2.0")),
            requirement("filters", Some("0.3.2")),
        ]),
        [
            "requires `vision` 1.5.0, and the host has 1.4.2",
            "requires `vision` 2.0.0, and the host has 1.4.2",
            "requires `vision` 0.9.0, and the host has 1.4.2",
            "requires `filters` 0.2.0, and the host has 0.3.1",
            "requires `filters` 0.3.2, and the host has 0.3.1",
        ]
    );
    // The same minor line while the major is 0, and not newer: met.
    assert_eq!(
        check(vec![requirement("filters", Some("0.3.0"))]),
        Vec::<String>::new()
    );
}

#[test]
fn a_version_that_is_not_a_plain_version_and_a_provider_with_none_are_refused_by_name() {
    assert_eq!(
        check(vec![
            requirement("vision", Some(">=1.2.0")),
            requirement("npu-plugin", Some("1.0.0")),
        ]),
        [
            "requires `vision` at `>=1.2.0`, which is not a version such as 1.4.2",
            "requires `npu-plugin` 1.0.0, and the host's `npu-plugin` has no version to compare",
        ]
    );
}
