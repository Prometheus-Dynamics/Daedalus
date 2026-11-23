use daedalus_graph::{EdgePolicyDoc, GraphBody, GraphDoc, HostInputPolicy, NodeDoc, NodeKindDoc};
use daedalus_planner::solve::Graph;
use daedalus_planner::solve::convert::ConvertError;
use daedalus_planner::solve::host_inputs::{
    HostInputError, HostNode, host_input_policy, set_host_input_policy,
};
use daedalus_planner::{Config, SolveError, solve};
use daedalus_planner_tests::{decl, edge, f64_ty, key, node, u8_ty, u32_ty};
use daedalus_registry::Registry;

fn registry() -> Registry {
    let mut registry = Registry::new();
    decl("t.u8")
        .output("out", u8_ty())
        .body("sim")
        .add(&mut registry);
    decl("t.a")
        .output("out", key("t.a"))
        .body("sim")
        .add(&mut registry);
    decl("t.sinkb")
        .input("in", key("t.b"))
        .body("sim")
        .add(&mut registry);
    decl("t.sinkf")
        .input("in", f64_ty())
        .body("sim")
        .add(&mut registry);
    decl("t.sinku32")
        .input("in", u32_ty())
        .body("sim")
        .add(&mut registry);
    decl("t.read")
        .input("in", u8_ty())
        .body("sim")
        .add(&mut registry);
    decl("t.take")
        .input_move("in", u8_ty())
        .body("sim")
        .add(&mut registry);
    decl("c.copy")
        .input("in", u8_ty())
        .output("out", u8_ty())
        .conversion(1, false)
        .body("sim")
        .add(&mut registry);
    registry
}

fn conversion(registry: &mut Registry, id: &str, from: &str, to: &str, cost: u32) {
    decl(id)
        .input("in", key(from))
        .output("out", key(to))
        .conversion(cost, false)
        .body("sim")
        .add(registry);
}

fn run(
    registry: &Registry,
    nodes: Vec<NodeDoc>,
    edges: Vec<daedalus_graph::EdgeDoc>,
) -> Result<Graph, Vec<SolveError>> {
    solve(
        &GraphBody {
            nodes,
            edges,
            groups: Vec::new(),
        },
        registry,
        &Config::default(),
    )
}

fn ids(graph: &Graph) -> Vec<&str> {
    graph
        .nodes
        .iter()
        .map(|node| node.doc.id.as_str())
        .collect()
}

#[test]
fn readers_of_one_output_that_need_the_same_conversion_share_one() {
    let mut registry = registry();
    conversion(&mut registry, "c.ab", "t.a", "t.b", 1);
    let mut kept = edge(0, "out", 2, "in");
    kept.policy = EdgePolicyDoc::LatestOnly;
    let graph = run(
        &registry,
        vec![
            node("t.a"),
            node("t.sinkb"),
            node("t.sinkb"),
            node("t.sinkb"),
        ],
        vec![
            edge(0, "out", 1, "in"),
            kept.clone(),
            edge(0, "out", 3, "in"),
        ],
    )
    .unwrap();
    // One conversion, read by all three.
    assert_eq!(
        ids(&graph),
        ["t.a", "t.sinkb", "t.sinkb", "t.sinkb", "c.ab"]
    );
    // Each reader's own policy stays on the edge into it.
    let mut into_second = edge(4, "out", 2, "in");
    into_second.policy = EdgePolicyDoc::LatestOnly;
    assert_eq!(
        graph.edges,
        [
            edge(0, "out", 4, "in"),
            edge(4, "out", 1, "in"),
            into_second,
            edge(4, "out", 3, "in"),
        ]
    );
}

#[test]
fn a_reader_of_the_plain_type_reads_the_output_beside_the_shared_conversion() {
    let mut registry = registry();
    conversion(&mut registry, "c.ab", "t.a", "t.b", 1);
    decl("t.sinka")
        .input("in", key("t.a"))
        .body("sim")
        .add(&mut registry);
    let graph = run(
        &registry,
        vec![
            node("t.a"),
            node("t.sinkb"),
            node("t.sinka"),
            node("t.sinkb"),
            node("t.sinkb"),
        ],
        vec![
            edge(0, "out", 1, "in"),
            edge(0, "out", 2, "in"),
            edge(0, "out", 3, "in"),
            edge(0, "out", 4, "in"),
        ],
    )
    .unwrap();
    assert_eq!(ids(&graph)[5..], ["c.ab"]);
    assert_eq!(
        graph.edges,
        [
            edge(0, "out", 5, "in"),
            edge(5, "out", 1, "in"),
            edge(0, "out", 2, "in"),
            edge(5, "out", 3, "in"),
            edge(5, "out", 4, "in"),
        ]
    );
}

#[test]
fn two_target_types_from_one_output_get_a_conversion_each() {
    let mut registry = registry();
    conversion(&mut registry, "c.ab", "t.a", "t.b", 1);
    conversion(&mut registry, "c.ac", "t.a", "t.c", 1);
    decl("t.sinkc")
        .input("in", key("t.c"))
        .body("sim")
        .add(&mut registry);
    let graph = run(
        &registry,
        vec![
            node("t.a"),
            node("t.sinkb"),
            node("t.sinkc"),
            node("t.sinkb"),
        ],
        vec![
            edge(0, "out", 1, "in"),
            edge(0, "out", 2, "in"),
            edge(0, "out", 3, "in"),
        ],
    )
    .unwrap();
    assert_eq!(ids(&graph)[4..], ["c.ab", "c.ac"]);
}

#[test]
fn two_paths_that_start_alike_share_their_start() {
    let mut registry = registry();
    conversion(&mut registry, "c.ab", "t.a", "t.b", 1);
    conversion(&mut registry, "c.bc", "t.b", "t.c", 1);
    decl("t.sinkc")
        .input("in", key("t.c"))
        .body("sim")
        .add(&mut registry);
    // One reader wants `t.b`, two want `t.c`, which is reached through `t.b`.
    let graph = run(
        &registry,
        vec![
            node("t.a"),
            node("t.sinkc"),
            node("t.sinkb"),
            node("t.sinkc"),
        ],
        vec![
            edge(0, "out", 1, "in"),
            edge(0, "out", 2, "in"),
            edge(0, "out", 3, "in"),
        ],
    )
    .unwrap();
    assert_eq!(ids(&graph)[4..], ["c.ab", "c.bc"]);
    assert_eq!(
        graph.edges,
        [
            edge(0, "out", 4, "in"),
            edge(4, "out", 5, "in"),
            edge(5, "out", 1, "in"),
            edge(4, "out", 2, "in"),
            edge(5, "out", 3, "in"),
        ]
    );
}

#[test]
fn a_reader_that_takes_its_value_keeps_a_conversion_of_its_own() {
    let mut registry = registry();
    conversion(&mut registry, "c.ab", "t.a", "t.b", 1);
    decl("t.takeb")
        .input_move("in", key("t.b"))
        .body("sim")
        .add(&mut registry);
    // Two readers share one conversion; the one that takes its value has its own, and no copy
    // is needed (the registry has none for `t.b`).
    let graph = run(
        &registry,
        vec![
            node("t.a"),
            node("t.sinkb"),
            node("t.takeb"),
            node("t.sinkb"),
        ],
        vec![
            edge(0, "out", 1, "in"),
            edge(0, "out", 2, "in"),
            edge(0, "out", 3, "in"),
        ],
    )
    .unwrap();
    assert_eq!(ids(&graph)[4..], ["c.ab", "c.ab"]);
    assert_eq!(
        graph.edges,
        [
            edge(0, "out", 4, "in"),
            edge(4, "out", 1, "in"),
            edge(0, "out", 5, "in"),
            edge(5, "out", 2, "in"),
            edge(4, "out", 3, "in"),
        ]
    );
}

#[test]
fn two_outputs_of_the_same_type_are_converted_apart() {
    let mut registry = registry();
    conversion(&mut registry, "c.ab", "t.a", "t.b", 1);
    let graph = run(
        &registry,
        vec![node("t.a"), node("t.a"), node("t.sinkb"), node("t.sinkb")],
        vec![edge(0, "out", 2, "in"), edge(1, "out", 3, "in")],
    )
    .unwrap();
    assert_eq!(ids(&graph)[4..], ["c.ab", "c.ab"]);
}

fn inserted(registry: &Registry) -> Vec<String> {
    let graph = run(
        registry,
        vec![node("t.a"), node("t.sinkb")],
        vec![edge(0, "out", 1, "in")],
    )
    .unwrap();
    graph.nodes[2..]
        .iter()
        .map(|node| node.doc.id.clone())
        .collect()
}

#[test]
fn cheapest_path_wins_then_fewer_steps_then_id() {
    let mut cheapest = registry();
    conversion(&mut cheapest, "c.direct", "t.a", "t.b", 5);
    conversion(&mut cheapest, "c.first", "t.a", "t.m", 1);
    conversion(&mut cheapest, "c.second", "t.m", "t.b", 1);
    assert_eq!(inserted(&cheapest), ["c.first", "c.second"]);

    let mut fewer = registry();
    conversion(&mut fewer, "c.direct", "t.a", "t.b", 2);
    conversion(&mut fewer, "c.first", "t.a", "t.m", 1);
    conversion(&mut fewer, "c.second", "t.m", "t.b", 1);
    assert_eq!(inserted(&fewer), ["c.direct"]);

    let mut by_id = registry();
    conversion(&mut by_id, "c.z", "t.a", "t.b", 2);
    conversion(&mut by_id, "c.y", "t.a", "t.b", 2);
    assert_eq!(inserted(&by_id), ["c.y"]);
}

#[test]
fn missing_converter_names_edge_and_types() {
    let registry = registry();
    let errors = run(
        &registry,
        vec![node("t.a"), node("t.sinkb")],
        vec![edge(0, "out", 1, "in")],
    )
    .unwrap_err();
    assert_eq!(
        errors,
        [SolveError::Convert(ConvertError::ConverterMissing {
            from_node: "t.a".into(),
            from_port: "out".into(),
            to_node: "t.sinkb".into(),
            to_port: "in".into(),
            from_type: "t.a".into(),
            to_type: "t.b".into(),
            note: None,
        })]
    );
}

#[test]
fn lossy_numeric_pair_is_never_converted() {
    let mut registry = registry();
    decl("c.lossy")
        .input("in", u8_ty())
        .output("out", f64_ty())
        .conversion(1, true)
        .body("sim")
        .add(&mut registry);
    let errors = run(
        &registry,
        vec![node("t.u8"), node("t.sinkf")],
        vec![edge(0, "out", 1, "in")],
    )
    .unwrap_err();
    let [SolveError::Convert(ConvertError::ConverterMissing { note, .. })] = &errors[..] else {
        panic!("{errors:?}");
    };
    assert!(note.is_some_and(|note| note.contains("lose data")));
}

#[test]
fn fanout_to_move_consumer_inserts_one_copy_per_consumer() {
    let registry = registry();
    let graph = run(
        &registry,
        vec![node("t.u8"), node("t.take"), node("t.take"), node("t.read")],
        vec![
            edge(0, "out", 1, "in"),
            edge(0, "out", 2, "in"),
            edge(0, "out", 3, "in"),
        ],
    )
    .unwrap();
    assert_eq!(
        ids(&graph),
        ["t.u8", "t.take", "t.take", "t.read", "c.copy", "c.copy"]
    );
    assert!(
        graph.edges.contains(&edge(0, "out", 3, "in")),
        "a reader shares the original"
    );
    assert!(graph.edges.contains(&edge(4, "out", 1, "in")));
    assert!(graph.edges.contains(&edge(5, "out", 2, "in")));
}

#[test]
fn a_single_move_consumer_needs_no_copy() {
    let registry = registry();
    let graph = run(
        &registry,
        vec![node("t.u8"), node("t.take")],
        vec![edge(0, "out", 1, "in")],
    )
    .unwrap();
    assert_eq!(graph.nodes.len(), 2);
}

fn bridge(policy: Option<HostInputPolicy>) -> NodeDoc {
    let mut host = node("host");
    host.kind = NodeKindDoc::HostBridge;
    host.port_types
        .insert("value".to_string(), "u8".to_string());
    if let Some(policy) = policy {
        host.host_inputs.insert("value".to_string(), policy);
    }
    host
}

#[test]
fn held_host_input_to_move_consumer_is_fanout() {
    let registry = registry();
    let held = run(
        &registry,
        vec![bridge(Some(HostInputPolicy::Held)), node("t.take")],
        vec![edge(0, "value", 1, "in")],
    )
    .unwrap();
    assert_eq!(ids(&held), ["host", "t.take", "c.copy"]);
    let consumed = run(
        &registry,
        vec![bridge(None), node("t.take")],
        vec![edge(0, "value", 1, "in")],
    )
    .unwrap();
    assert_eq!(consumed.nodes.len(), 2);
}

fn doc_with_bridge() -> GraphDoc {
    GraphDoc::new(GraphBody {
        nodes: vec![bridge(None)],
        ..GraphBody::default()
    })
}

#[test]
fn host_input_policy_round_trips() {
    let mut doc = doc_with_bridge();
    let old =
        set_host_input_policy(&mut doc.graph, "host", "value", HostInputPolicy::Held).unwrap();
    assert_eq!(old, HostInputPolicy::Consume);
    let reloaded = GraphDoc::parse(&doc.to_json().unwrap()).unwrap();
    assert_eq!(
        host_input_policy(&reloaded.graph, "host", "value"),
        Ok(HostInputPolicy::Held)
    );
    assert_eq!(
        reloaded.graph.nodes[0].host_inputs["value"],
        HostInputPolicy::Held
    );
}

#[test]
fn unknown_host_and_port_are_typed_errors() {
    let doc = doc_with_bridge();
    assert_eq!(
        host_input_policy(&doc.graph, "nope", "value"),
        Err(HostInputError::UnknownHost {
            host: "nope".into(),
            available: vec!["host".into()]
        })
    );
    assert_eq!(
        host_input_policy(&doc.graph, "host", "nope"),
        Err(HostInputError::UnknownPort {
            host: "host".into(),
            port: "nope".into(),
            available: vec!["value".into()]
        })
    );
    let mut plain = GraphBody::default();
    plain.nodes.push(node("t.u8"));
    assert_eq!(
        host_input_policy(&plain, HostNode::Node(0), "value"),
        Err(HostInputError::NotAHostBridge { node: 0 })
    );
}
