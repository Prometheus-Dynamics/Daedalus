use daedalus_graph::{GraphBody, Lit, NodeDoc, NodeKindDoc};
use daedalus_planner::solve::Graph;
use daedalus_planner::solve::ports::{Direction, PortError};
use daedalus_planner::solve::typecheck::TypeCheckError;
use daedalus_planner::{Config, SolveError, solve};
use daedalus_planner_tests::{decl, edge, f64_ty, key, node, u8_ty};
use daedalus_registry::{Registry, TypeRef};
use daedalus_types::{Scalar, TypeKey};

fn registry() -> Registry {
    let mut registry = Registry::new();
    decl("t.u8")
        .output("out", u8_ty())
        .body("sim")
        .add(&mut registry);
    decl("t.f64")
        .output("out", f64_ty())
        .body("sim")
        .add(&mut registry);
    decl("t.sink8")
        .input("in", u8_ty())
        .body("sim")
        .add(&mut registry);
    decl("t.sinkf")
        .input("in", f64_ty())
        .body("sim")
        .add(&mut registry);
    decl("t.pair")
        .input("a", key("generic:t"))
        .input("b", key("generic:t"))
        .output("out", key("generic:t"))
        .body("sim")
        .add(&mut registry);
    decl("t.any")
        .input("in", key("generic"))
        .output("out", key("generic"))
        .body("sim")
        .add(&mut registry);
    registry
}

fn run_with(
    nodes: Vec<NodeDoc>,
    edges: Vec<daedalus_graph::EdgeDoc>,
    strict: bool,
) -> Result<Graph, Vec<SolveError>> {
    let body = GraphBody {
        nodes,
        edges,
        groups: Vec::new(),
    };
    solve(
        &body,
        &registry(),
        &Config {
            strict_ports: strict,
        },
    )
}

fn run(nodes: Vec<NodeDoc>, edges: Vec<daedalus_graph::EdgeDoc>) -> Result<Graph, Vec<SolveError>> {
    run_with(nodes, edges, false)
}

#[test]
fn generic_var_unifies_across_three_ports() {
    let graph = run(
        vec![node("t.u8"), node("t.u8"), node("t.pair"), node("t.sink8")],
        vec![
            edge(0, "out", 2, "a"),
            edge(1, "out", 2, "b"),
            edge(2, "out", 3, "in"),
        ],
    )
    .unwrap();
    for port in graph.nodes[2].inputs.iter().chain(&graph.nodes[2].outputs) {
        assert_eq!(port.ty, u8_ty(), "port {}", port.name);
    }
}

#[test]
fn mismatch_names_both_types() {
    let errors = run(
        vec![node("t.u8"), node("t.f64"), node("t.pair")],
        vec![edge(0, "out", 2, "a"), edge(1, "out", 2, "b")],
    )
    .unwrap_err();
    let [
        SolveError::Types(TypeCheckError::TypeMismatch {
            from_type, to_type, ..
        }),
    ] = &errors[..]
    else {
        panic!("{errors:?}");
    };
    let mut names = [from_type.as_str(), to_type.as_str()];
    names.sort();
    assert_eq!(
        names,
        [Scalar::Float.key().as_str(), Scalar::U8.key().as_str()]
    );
}

#[test]
fn unknown_port_is_port_missing() {
    let errors = run(
        vec![node("t.u8"), node("t.sink8")],
        vec![edge(0, "nope", 1, "in")],
    )
    .unwrap_err();
    assert_eq!(
        errors,
        [SolveError::Ports(PortError::PortMissing {
            node: "t.u8".into(),
            direction: Direction::Output,
            port: "nope".into(),
        })]
    );
}

#[test]
fn every_error_in_the_graph_is_reported_in_one_run() {
    let errors = run(
        vec![
            node("t.u8"),
            node("t.sink8"),
            node("t.f64"),
            node("t.pair"),
            node("t.u8"),
        ],
        vec![
            edge(0, "nope", 1, "in"),
            edge(0, "out", 1, "other"),
            edge(2, "out", 3, "a"),
            edge(4, "out", 3, "b"),
        ],
    )
    .unwrap_err();
    assert_eq!(errors.len(), 3, "{errors:?}");
}

#[test]
fn duplicate_port_is_port_duplicate() {
    let mut sink = node("t.sink8");
    sink.inputs = vec!["in".to_string(), "in".to_string()];
    assert_eq!(
        run(vec![sink], Vec::new()).unwrap_err(),
        [SolveError::Ports(PortError::PortDuplicate {
            node: "t.sink8".into(),
            direction: Direction::Input,
            port: "in".into(),
        })]
    );
}

#[test]
fn a_listed_port_the_declaration_lacks_is_port_extra() {
    let mut sink = node("t.sink8");
    sink.inputs = vec!["in".to_string(), "old".to_string()];
    assert_eq!(
        run(vec![sink], Vec::new()).unwrap_err(),
        [SolveError::Ports(PortError::PortExtra {
            node: "t.sink8".into(),
            direction: Direction::Input,
            port: "old".into(),
        })]
    );
}

#[test]
fn strict_mode_checks_listed_ports() {
    let mut pair = node("t.pair");
    pair.inputs = vec!["a".to_string()];
    pair.outputs = vec!["out".to_string()];
    assert!(
        run(vec![pair.clone()], Vec::new()).is_ok(),
        "off by default"
    );
    assert_eq!(
        run_with(vec![pair], Vec::new(), true).unwrap_err(),
        [SolveError::Ports(PortError::PortMissing {
            node: "t.pair".into(),
            direction: Direction::Input,
            port: "b".into(),
        })]
    );
}

#[test]
fn generic_marker_is_case_insensitive() {
    let mut any = node("t.any");
    any.port_types
        .insert("in".to_string(), "Generic".to_string());
    let graph = run(vec![any.clone()], Vec::new()).unwrap();
    assert_eq!(
        graph.nodes[0].inputs[0].ty,
        TypeRef::Key(TypeKey::new("generic"))
    );
    any.port_types
        .insert("in".to_string(), "GENERIC:x".to_string());
    let graph = run(vec![any], Vec::new()).unwrap();
    assert_eq!(
        graph.nodes[0].inputs[0].ty,
        TypeRef::Key(TypeKey::new("generic:x"))
    );
}

#[test]
fn dynamic_port_type_from_typed_field_only() {
    let mut typed = node("t.any");
    typed.port_types.insert("out".to_string(), "u8".to_string());
    let mut client = node("t.any");
    client.metadata.insert(
        "dynamic_output_types".to_string(),
        Lit::Str("u8".to_string()),
    );
    let graph = run(vec![typed, client], Vec::new()).unwrap();
    assert_eq!(graph.nodes[0].outputs[0].ty, u8_ty());
    assert_eq!(
        graph.nodes[1].outputs[0].ty,
        TypeRef::Key(TypeKey::new("generic"))
    );
}

#[test]
fn a_host_bridge_uses_the_types_it_names() {
    let mut bridge = node("host");
    bridge.kind = NodeKindDoc::HostBridge;
    bridge
        .port_types
        .insert("value".to_string(), "u8".to_string());
    let graph = run(
        vec![bridge, node("t.sink8")],
        vec![edge(0, "value", 1, "in")],
    )
    .unwrap();
    assert_eq!(graph.nodes[0].outputs[0].ty, u8_ty());
}
