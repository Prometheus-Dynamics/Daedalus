use daedalus_graph::{EdgeDoc, GraphBody, Lit, NodeDoc, NodeKindDoc};
use daedalus_planner::share::{Split, UPSTREAM_HOST, split_shared};
use daedalus_planner::{Config, solve};
use daedalus_planner_tests::{decl, edge, node, u8_ty};
use daedalus_registry::Registry;

fn registry() -> Registry {
    let mut registry = Registry::new();
    decl("t.mask")
        .input("in", u8_ty())
        .config("gain", u8_ty())
        .output("out", u8_ty())
        .shareable()
        .body("sim")
        .add(&mut registry);
    decl("t.pyramid")
        .input("in", u8_ty())
        .output("out", u8_ty())
        .shareable()
        .body("sim")
        .add(&mut registry);
    decl("t.local")
        .input("in", u8_ty())
        .output("out", u8_ty())
        .body("sim")
        .add(&mut registry);
    decl("t.detect")
        .input("in", u8_ty())
        .output("out", u8_ty())
        .body("sim")
        .add(&mut registry);
    registry
}

fn host() -> NodeDoc {
    let mut doc = node("cam");
    doc.kind = NodeKindDoc::HostBridge;
    doc.port_types.insert("frame".to_string(), "u8".to_string());
    doc
}

fn graph(body: Vec<NodeDoc>, edges: Vec<EdgeDoc>) -> daedalus_planner::solve::Graph {
    let doc = GraphBody {
        nodes: body,
        edges,
        groups: Vec::new(),
    };
    solve(&doc, &registry(), &Config::default()).unwrap()
}

/// `cam -> first -> t.pyramid -> t.detect`.
fn chain(first: NodeDoc) -> daedalus_planner::solve::Graph {
    graph(
        vec![host(), first, node("t.pyramid"), node("t.detect")],
        vec![
            edge(0, "frame", 1, "in"),
            edge(1, "out", 2, "in"),
            edge(2, "out", 3, "in"),
        ],
    )
}

fn split(a: &daedalus_planner::solve::Graph, b: &daedalus_planner::solve::Graph) -> Split {
    split_shared(&[a, b])
}

fn ids(graph: &GraphBody) -> Vec<&str> {
    graph.nodes.iter().map(|node| node.id.as_str()).collect()
}

#[test]
fn identical_mask_and_pyramid_share_one_upstream_cut_once() {
    let (a, b) = (chain(node("t.mask")), chain(node("t.mask")));
    let result = split(&a, &b);
    let upstream = result.upstream.as_ref().unwrap();
    assert_eq!(ids(upstream), ["t.mask", "t.pyramid", "cam", UPSTREAM_HOST]);
    assert_eq!(result.shared.len(), 2);
    assert_eq!(result.shared[0].replaces, [(0, 1), (1, 1)]);
    for remainder in &result.remainders {
        assert_eq!(ids(&remainder.graph), ["t.detect", UPSTREAM_HOST]);
        assert_eq!(remainder.links.len(), 1);
        assert!(remainder.has_nodes);
    }
    assert_eq!(result.remainders[0].links, result.remainders[1].links);
}

#[test]
fn a_node_not_flagged_shareable_blocks_sharing_below_it() {
    let (a, b) = (chain(node("t.local")), chain(node("t.local")));
    let result = split(&a, &b);
    assert!(result.upstream.is_none());
    assert_eq!(ids(&result.remainders[0].graph).len(), 4);
}

#[test]
fn a_node_fed_by_a_non_shareable_node_is_not_shared() {
    let build = || {
        graph(
            vec![host(), node("t.mask"), node("t.local"), node("t.pyramid")],
            vec![
                edge(0, "frame", 1, "in"),
                edge(1, "out", 2, "in"),
                edge(2, "out", 3, "in"),
            ],
        )
    };
    let (a, b) = (build(), build());
    let result = split(&a, &b);
    assert_eq!(ids(result.upstream.as_ref().unwrap())[0], "t.mask");
    assert_eq!(result.shared.len(), 1);
    assert_eq!(
        ids(&result.remainders[0].graph),
        ["t.local", "t.pyramid", UPSTREAM_HOST]
    );
}

#[test]
fn labels_and_ui_ids_are_ignored() {
    let mut named = node("t.mask");
    named.label = Some("left".to_string());
    named.ui_id = Some("ui-7".to_string());
    let (a, b) = (chain(named), chain(node("t.mask")));
    assert_eq!(split(&a, &b).shared.len(), 2);
}

#[test]
fn metadata_counts() {
    let mut tagged = node("t.mask");
    tagged.metadata.insert("note".to_string(), Lit::Int(1));
    let (a, b) = (chain(tagged), chain(node("t.mask")));
    assert!(split(&a, &b).upstream.is_none());
}

fn with_gain(value: i64) -> NodeDoc {
    let mut doc = node("t.mask");
    doc.consts.insert("gain".to_string(), Lit::Int(value));
    doc
}

#[test]
fn a_different_const_is_not_shared() {
    let (a, b) = (chain(with_gain(1)), chain(with_gain(2)));
    assert!(split(&a, &b).upstream.is_none());
    let (c, d) = (chain(with_gain(3)), chain(with_gain(3)));
    assert_eq!(split(&c, &d).shared.len(), 2);
}

#[test]
fn a_different_host_input_is_not_shared() {
    let mut other = host();
    other.port_types.clear();
    other
        .port_types
        .insert("frame".to_string(), "u8".to_string());
    other.label = Some("other".to_string());
    let a = chain(node("t.mask"));
    let b = graph(vec![other, node("t.mask")], vec![edge(0, "frame", 1, "in")]);
    assert!(split(&a, &b).upstream.is_none());
}

#[test]
fn the_upstream_and_every_remainder_validate() {
    let (a, b) = (chain(node("t.mask")), chain(node("t.mask")));
    let result = split(&a, &b);
    let registry = registry();
    let config = Config::default();
    solve(result.upstream.as_ref().unwrap(), &registry, &config).unwrap();
    for remainder in &result.remainders {
        solve(&remainder.graph, &registry, &config).unwrap();
    }
}

#[test]
fn a_host_output_on_a_shared_node_becomes_an_alias() {
    let build = || {
        let mut cam = host();
        cam.port_types.insert("shown".to_string(), "u8".to_string());
        graph(
            vec![cam, node("t.mask")],
            vec![edge(0, "frame", 1, "in"), edge(1, "out", 0, "shown")],
        )
    };
    let (a, b) = (build(), build());
    let result = split(&a, &b);
    let remainder = &result.remainders[0];
    assert!(!remainder.has_nodes);
    assert_eq!(remainder.aliases[0].host, "cam");
    assert_eq!(remainder.aliases[0].port, "shown");
    assert!(remainder.graph.nodes.is_empty());
}

#[test]
fn nothing_in_common_leaves_each_graph_whole() {
    let a = chain(node("t.mask"));
    let b = graph(
        vec![host(), node("t.detect")],
        vec![edge(0, "frame", 1, "in")],
    );
    let result = split(&a, &b);
    assert!(result.upstream.is_none());
    assert_eq!(ids(&result.remainders[1].graph), ["cam", "t.detect"]);
}
