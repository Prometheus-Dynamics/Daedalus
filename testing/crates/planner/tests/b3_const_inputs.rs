use daedalus_graph::{GraphBody, Lit, NodeDoc};
use daedalus_planner::solve::Graph;
use daedalus_planner::solve::consts::ConstError;
use daedalus_planner::{Config, SolveError, solve};
use daedalus_planner_tests::{decl, node, u8_ty};
use daedalus_registry::Registry;
use daedalus_types::Literal;

fn registry() -> Registry {
    let mut registry = Registry::new();
    decl("t.c")
        .defaulted("gain", u8_ty(), Literal::Int(3))
        .optional("maybe", u8_ty())
        .input("needed", u8_ty())
        .body("sim")
        .add(&mut registry);
    decl("t.cfg")
        .config("settings", u8_ty())
        .body("sim")
        .add(&mut registry);
    registry
}

fn with(id: &str, port: &str, value: Lit) -> NodeDoc {
    let mut doc = node(id);
    doc.consts.insert(port.to_string(), value);
    doc
}

fn run(doc: NodeDoc) -> Result<Graph, Vec<SolveError>> {
    let body = GraphBody {
        nodes: vec![doc],
        ..GraphBody::default()
    };
    solve(&body, &registry(), &Config::default())
}

#[test]
fn graph_const_overrides_declared_default() {
    assert_eq!(
        run(node("t.c")).unwrap().nodes[0].consts["gain"],
        Lit::Int(3)
    );
    assert_eq!(
        run(with("t.c", "gain", Lit::Int(9))).unwrap().nodes[0].consts["gain"],
        Lit::Int(9)
    );
}

#[test]
fn const_of_wrong_scalar_type_is_type_mismatch() {
    for bad in [Lit::Str("no".to_string()), Lit::Int(256), Lit::Int(-1)] {
        let errors = run(with("t.c", "gain", bad)).unwrap_err();
        let [SolveError::Consts(ConstError::TypeMismatch { node, port, .. })] = &errors[..] else {
            panic!("{errors:?}");
        };
        assert_eq!((node.as_str(), port.as_str()), ("t.c", "gain"));
    }
}

#[test]
fn optional_port_accepts_unit_only_when_optional() {
    assert!(run(with("t.c", "maybe", Lit::Unit)).is_ok());
    assert!(run(with("t.c", "needed", Lit::Unit)).is_err());
}

#[test]
fn config_port_const_lands_on_solved_node() {
    let graph = run(with("t.cfg", "settings", Lit::Int(7))).unwrap();
    assert_eq!(graph.nodes[0].consts["settings"], Lit::Int(7));
}

#[test]
fn a_const_on_an_unknown_port_is_port_missing() {
    let errors = run(with("t.c", "nope", Lit::Int(1))).unwrap_err();
    assert!(matches!(errors[..], [SolveError::Ports(_)]), "{errors:?}");
}

#[test]
fn a_port_with_a_const_is_not_required() {
    let graph = run(with("t.c", "needed", Lit::Int(1))).unwrap();
    assert!(graph.nodes[0].required.is_empty());
}
