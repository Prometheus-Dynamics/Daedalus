use daedalus_graph::LitField;
use daedalus_graph::{GraphBody, Lit, NodeDoc};
use daedalus_planner::solve::Graph;
use daedalus_planner::solve::consts::ConstError;
use daedalus_planner::{Config, SolveError, solve};
use daedalus_planner_tests::{decl, key, node, str_ty, u8_ty};
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
    decl("t.fields")
        .input("gain", u8_ty())
        .config_fields(
            "config",
            key("t.settings"),
            &[
                ("dictionary", u8_ty()),
                ("gain", u8_ty()),
                ("name", str_ty()),
            ],
        )
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
    assert_eq!(
        errors,
        [SolveError::Consts(ConstError::NoPortOrField {
            node: "t.c".into(),
            name: "nope".into(),
            fields: Vec::new(),
        })]
    );
}

fn field(name: &str, value: Lit) -> LitField {
    LitField {
        name: name.into(),
        value,
    }
}

#[test]
fn a_const_naming_a_config_field_folds_into_the_packed_config() {
    let mut doc = with("t.fields", "dictionary", Lit::Int(4));
    doc.consts.insert("name".into(), Lit::Str("tags".into()));
    let graph = run(doc).unwrap();
    let consts = &graph.nodes[0].consts;
    assert_eq!(
        consts["config"],
        Lit::Struct(vec![
            field("dictionary", Lit::Int(4)),
            field("name", Lit::Str("tags".into())),
        ])
    );
    assert!(!consts.contains_key("dictionary") && !consts.contains_key("name"));
}

#[test]
fn a_field_const_joins_the_fields_a_packed_config_sets() {
    let mut doc = with("t.fields", "dictionary", Lit::Int(4));
    doc.consts.insert(
        "config".into(),
        Lit::Struct(vec![field("name", Lit::Str("tags".into()))]),
    );
    let graph = run(doc).unwrap();
    assert_eq!(
        graph.nodes[0].consts["config"],
        Lit::Struct(vec![
            field("name", Lit::Str("tags".into())),
            field("dictionary", Lit::Int(4)),
        ])
    );
}

#[test]
fn a_field_const_is_checked_against_the_field_type() {
    let errors = run(with("t.fields", "dictionary", Lit::Str("no".into()))).unwrap_err();
    let [SolveError::Consts(ConstError::TypeMismatch { node, port, .. })] = &errors[..] else {
        panic!("{errors:?}");
    };
    assert_eq!((node.as_str(), port.as_str()), ("t.fields", "dictionary"));
}

#[test]
fn a_field_set_both_packed_and_alone_is_refused() {
    let mut doc = with("t.fields", "dictionary", Lit::Int(4));
    doc.consts.insert(
        "config".into(),
        Lit::Struct(vec![field("dictionary", Lit::Int(5))]),
    );
    assert_eq!(
        run(doc).unwrap_err(),
        [SolveError::Consts(ConstError::ConfigFieldTwice {
            node: "t.fields".into(),
            field: "dictionary".into(),
        })]
    );
}

#[test]
fn a_name_that_is_a_port_and_a_field_stays_the_port() {
    let graph = run(with("t.fields", "gain", Lit::Int(2))).unwrap();
    let consts = &graph.nodes[0].consts;
    assert_eq!(consts["gain"], Lit::Int(2));
    assert!(!consts.contains_key("config"));
}

#[test]
fn an_unknown_name_on_a_node_with_a_config_lists_its_fields() {
    let errors = run(with("t.fields", "dictonary", Lit::Int(4))).unwrap_err();
    assert_eq!(
        errors,
        [SolveError::Consts(ConstError::NoPortOrField {
            node: "t.fields".into(),
            name: "dictonary".into(),
            fields: vec!["dictionary".into(), "gain".into(), "name".into()],
        })]
    );
    let text = errors[0].to_string();
    assert!(text.contains("dictionary, gain, name"), "{text}");
}

#[test]
fn a_port_with_a_const_is_not_required() {
    let graph = run(with("t.c", "needed", Lit::Int(1))).unwrap();
    assert!(graph.nodes[0].required.is_empty());
}

#[test]
fn a_group_sets_its_stage_config_fields_by_name() {
    use daedalus_graph::{GroupDef, StagePort};
    static GROUP: GroupDef = GroupDef {
        stages: &[("s", "t.fields")],
        inputs: &[(
            "dict",
            &[StagePort {
                stage: "s",
                port: "dictionary",
            }],
        )],
        consts: &[(
            StagePort {
                stage: "s",
                port: "name",
            },
            Literal::Str("tags"),
        )],
        ..GroupDef::new("t.grouped")
    };
    let mut registry = registry();
    decl("t.grouped")
        .group(&GROUP)
        .body("sim")
        .add(&mut registry);
    let body = GraphBody {
        nodes: vec![with("t.grouped", "dict", Lit::Int(4))],
        ..GraphBody::default()
    };
    let graph = solve(&body, &registry, &Config::default()).unwrap();
    assert_eq!(graph.nodes[0].doc.id, "t.fields");
    let Lit::Struct(set) = &graph.nodes[0].consts["config"] else {
        panic!("{:?}", graph.nodes[0].consts);
    };
    let mut set = set.clone();
    set.sort_by(|a, b| a.name.cmp(&b.name));
    assert_eq!(
        set,
        [
            field("dictionary", Lit::Int(4)),
            field("name", Lit::Str("tags".into())),
        ]
    );
}

#[test]
fn a_field_cannot_join_a_packed_config_that_is_not_a_struct() {
    let mut doc = with("t.fields", "dictionary", Lit::Int(4));
    doc.consts.insert("config".into(), Lit::Int(1));
    let errors = run(doc).unwrap_err();
    let [SolveError::Consts(ConstError::TypeMismatch { port, .. })] = &errors[..] else {
        panic!("{errors:?}");
    };
    assert_eq!(port, "config");
}
