use daedalus_graph::{GraphBody, GraphPatch, Lit, NodeDoc, NodeSelector, PatchOp};
use daedalus_planner::edit::{
    RefreshPolicy, analyze, apply_patch, refresh_ports, refresh_ports_with,
};
use daedalus_planner_tests::{decl, edge, node, u8_ty};
use daedalus_registry::Registry;

fn index(at: u32) -> NodeSelector {
    NodeSelector {
        index: Some(at),
        id: None,
    }
}

fn patch(ops: Vec<PatchOp>) -> GraphPatch {
    GraphPatch { version: 1, ops }
}

fn three() -> GraphBody {
    GraphBody {
        nodes: vec![node("t.a"), node("t.b"), node("t.c")],
        edges: vec![edge(0, "out", 1, "in"), edge(1, "out", 2, "in")],
        groups: Vec::new(),
    }
}

fn set_const(node: NodeSelector) -> PatchOp {
    PatchOp::SetConst {
        node,
        port: "gain".to_string(),
        value: Some(Lit::Int(4)),
    }
}

#[test]
fn apply_patch_selects_nodes_by_ui_node_id() {
    let mut graph = three();
    graph.nodes[1].ui_id = Some("ui-1".to_string());
    let selector = NodeSelector {
        index: None,
        id: Some("ui-1".to_string()),
    };
    let report = apply_patch(&mut graph, &patch(vec![set_const(selector)])).unwrap();
    assert_eq!(report.applied[0].node, 1);
    assert_eq!(graph.nodes[1].consts["gain"], Lit::Int(4));
}

#[test]
fn apply_patch_sets_const() {
    let mut graph = three();
    apply_patch(&mut graph, &patch(vec![set_const(index(2))])).unwrap();
    assert_eq!(graph.nodes[2].consts["gain"], Lit::Int(4));
    let clear = PatchOp::SetConst {
        node: index(2),
        port: "gain".to_string(),
        value: None,
    };
    apply_patch(&mut graph, &patch(vec![clear])).unwrap();
    assert!(graph.nodes[2].consts.is_empty());
}

#[test]
fn apply_patch_replaces_node_id() {
    let mut graph = three();
    let op = PatchOp::ReplaceNodeId {
        node: index(0),
        new_id: "t.z".to_string(),
    };
    apply_patch(&mut graph, &patch(vec![op])).unwrap();
    assert_eq!(graph.nodes[0].id, "t.z");
}

#[test]
fn apply_patch_deletes_nodes_and_remaps_edges() {
    let mut graph = GraphBody {
        nodes: vec![node("t.a"), node("t.b"), node("t.c"), node("t.d")],
        edges: vec![edge(0, "out", 1, "in"), edge(2, "out", 3, "in")],
        groups: Vec::new(),
    };
    let report = apply_patch(
        &mut graph,
        &patch(vec![PatchOp::DeleteNode { node: index(1) }]),
    )
    .unwrap();
    assert_eq!((report.removed_nodes, report.removed_edges), (1, 1));
    let ids: Vec<&str> = graph.nodes.iter().map(|n| n.id.as_str()).collect();
    assert_eq!(ids, ["t.a", "t.c", "t.d"]);
    assert_eq!(graph.edges, [edge(1, "out", 2, "in")]);
}

#[test]
fn a_later_index_means_the_graph_before_the_patch() {
    let mut graph = three();
    let ops = vec![PatchOp::DeleteNode { node: index(0) }, set_const(index(2))];
    apply_patch(&mut graph, &patch(ops)).unwrap();
    assert_eq!(graph.nodes[1].id, "t.c");
    assert_eq!(graph.nodes[1].consts["gain"], Lit::Int(4));
}

#[test]
fn unknown_selector_is_reported() {
    let mut graph = three();
    let report = apply_patch(&mut graph, &patch(vec![set_const(index(9))])).unwrap();
    assert_eq!(report.not_found[0].selector, index(9));
    assert!(report.applied.is_empty());
    let text = report_json(&report);
    assert!(text.contains("not_found"), "{text}");
}

fn report_json(report: &daedalus_planner::edit::PatchReport) -> String {
    serde_json::to_string(report).unwrap()
}

#[test]
fn analyze_tracks_downstream_and_sink_nodes() {
    let impact = analyze(&three(), &patch(vec![set_const(index(1))]));
    assert_eq!(impact.downstream_nodes, [1, 2]);
    assert_eq!(impact.sink_nodes, [2]);
}

fn registry() -> Registry {
    let mut registry = Registry::new();
    decl("t.src")
        .output("frame", u8_ty())
        .body("sim")
        .add(&mut registry);
    decl("t.map")
        .input("frame", u8_ty())
        .input("map", u8_ty())
        .output("pose", u8_ty())
        .output("saved", u8_ty())
        .body("sim")
        .add(&mut registry);
    decl("t.sink")
        .input("in0", u8_ty())
        .body("sim")
        .add(&mut registry);
    registry
}

fn listed(id: &str, inputs: &[&str], outputs: &[&str]) -> NodeDoc {
    let mut doc = node(id);
    doc.inputs = inputs.iter().map(ToString::to_string).collect();
    doc.outputs = outputs.iter().map(ToString::to_string).collect();
    doc
}

fn stored() -> GraphBody {
    GraphBody {
        nodes: vec![
            listed("t.src", &[], &["frame"]),
            listed("t.map", &["frame", "legacy"], &["pose", "debug"]),
            listed("t.sink", &["in0"], &[]),
            listed("t.unknown", &["x"], &[]),
        ],
        edges: vec![edge(0, "frame", 1, "frame"), edge(1, "debug", 2, "in0")],
        groups: Vec::new(),
    }
}

#[test]
fn refresh_adds_new_ports_and_drops_unused_old_ones() {
    let mut graph = stored();
    let report = refresh_ports(&mut graph, &registry());
    assert!(report.changed());
    let map = &report.nodes[0];
    assert_eq!(map.node, 1);
    assert_eq!(map.added_inputs, ["map"]);
    assert_eq!(map.added_outputs, ["saved"]);
    assert_eq!(map.removed_inputs, ["legacy"]);
    assert_eq!(map.kept_outputs, ["debug"]);
    assert_eq!(graph.nodes[1].inputs, ["frame", "map"]);
    assert_eq!(graph.nodes[1].outputs, ["pose", "debug", "saved"]);
    assert_eq!(
        graph.nodes[3].inputs,
        ["x"],
        "an unknown node is left alone"
    );
}

#[test]
fn refresh_is_idempotent() {
    let mut graph = stored();
    let registry = registry();
    refresh_ports(&mut graph, &registry);
    let once = graph.clone();
    let again = refresh_ports(&mut graph, &registry);
    assert!(!again.changed(), "{again:?}");
    assert_eq!(again.nodes[0].kept_outputs, ["debug"]);
    assert_eq!(graph, once);
}

fn with_stale_const() -> GraphBody {
    let mut graph = stored();
    graph.nodes[1]
        .consts
        .insert("legacy".to_string(), Lit::Int(5));
    graph
}

#[test]
fn stale_consts_are_kept_by_default() {
    let mut graph = with_stale_const();
    let report = refresh_ports(&mut graph, &registry());
    assert_eq!(graph.nodes[1].consts["legacy"], Lit::Int(5));
    assert_eq!(report.nodes[0].kept_inputs, ["legacy"]);
    assert!(report.nodes[0].dropped_consts.is_empty());
}

#[test]
fn drop_stale_consts_drops_const_only_ports_and_reports_values() {
    let mut graph = with_stale_const();
    let policy = RefreshPolicy {
        drop_stale_consts: true,
    };
    let report = refresh_ports_with(&mut graph, &registry(), policy);
    assert!(graph.nodes[1].consts.is_empty());
    assert_eq!(
        report.nodes[0].dropped_consts,
        [("legacy".to_string(), Lit::Int(5))]
    );
    assert_eq!(report.nodes[0].removed_inputs, ["legacy"]);
}
