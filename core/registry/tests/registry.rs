use daedalus_backend::Offer;
use daedalus_registry::*;
use daedalus_types::{NodeIndex, Scalar, TypeExpr, TypeIndex, TypeKey};

const U8: TypeRef = TypeRef::Key(Scalar::U8.key());
const U16: TypeRef = TypeRef::Key(Scalar::U16.key());
const A_INPUTS: &[PortDecl] = &[PortDecl::new("in", U8)];
const A_OUTPUTS: &[PortDecl] = &[PortDecl::new("out", U8)];

const fn decl(
    at: &'static str,
    inputs: &'static [PortDecl],
    outputs: &'static [PortDecl],
) -> NodeDecl {
    NodeDecl {
        id: "demo.blur",
        at: SourceAt(at),
        summary: "",
        description: "",
        inputs,
        outputs,
        config: &[],
        fanin: &[],
        fire: Fire::All,
        reusable: false,
        shareable: false,
        conversion: None,
        group: None,
    }
}

const fn body(backend: &'static str, at: &'static str) -> BodyRecord {
    BodyRecord {
        backend,
        at: SourceAt(at),
        needs: &[],
        state: None,
        ports: &[],
        memory: &[],
        code: &NoCode,
        text: "",
    }
}

const CPU_DECL: NodeDecl = decl("a.rs:1", A_INPUTS, A_OUTPUTS);
const CPU: NodeRecord = NodeRecord {
    decl: &CPU_DECL,
    body: body("cpu", "a.rs:1"),
};
const ALT_DECL: NodeDecl = decl("b.rs:2", A_INPUTS, A_OUTPUTS);
const ALT: NodeRecord = NodeRecord {
    decl: &ALT_DECL,
    body: body("alt", "b.rs:2"),
};
const CPU_AGAIN: NodeRecord = NodeRecord {
    decl: &ALT_DECL,
    body: body("cpu", "c.rs:3"),
};

const OTHER_OUTPUTS: &[PortDecl] = &[PortDecl::new("result", U8)];
const OTHER_DECL: NodeDecl = decl("d.rs:4", A_INPUTS, OTHER_OUTPUTS);
const OTHER: NodeRecord = NodeRecord {
    decl: &OTHER_DECL,
    body: body("third", "d.rs:4"),
};

fn registered(records: &[&'static NodeRecord]) -> Registry {
    let mut registry = Registry::new();
    registry.register_all(records).unwrap();
    registry
}

#[test]
fn a_new_id_creates_the_entry() {
    let registry = registered(&[&CPU]);
    let entry = registry.entry("demo.blur").unwrap();
    assert_eq!(entry.decl.inputs[0].name, "in");
    assert_eq!(entry.bodies.len(), 1);
    assert!(registry.entry("nope").is_none());
}

#[test]
fn the_same_declaration_with_a_new_backend_adds_a_body() {
    let registry = registered(&[&ALT, &CPU]);
    let backends: Vec<_> = registry
        .entry("demo.blur")
        .unwrap()
        .bodies
        .iter()
        .map(|b| b.backend)
        .collect();
    assert_eq!(backends, ["alt", "cpu"]);
}

#[test]
fn a_different_declaration_names_both_places_and_the_port() {
    let mut registry = registered(&[&CPU]);
    assert_eq!(
        registry.register(&OTHER).unwrap_err(),
        RegError::DeclConflict {
            id: "demo.blur".into(),
            first: SourceAt("a.rs:1"),
            second: SourceAt("d.rs:4"),
            port: Some("out"),
        }
    );
    assert_eq!(
        registry.entry("demo.blur").unwrap().bodies.len(),
        1,
        "nothing changed"
    );
}

#[test]
fn a_second_body_for_a_backend_names_both_places() {
    let mut registry = registered(&[&CPU]);
    assert_eq!(
        registry.register(&CPU_AGAIN).unwrap_err(),
        RegError::DuplicateBody {
            id: "demo.blur".into(),
            backend: "cpu",
            first: SourceAt("a.rs:1"),
            second: SourceAt("c.rs:3"),
        }
    );
}

#[test]
fn register_all_reports_every_error() {
    let mut registry = Registry::new();
    let errors = registry
        .register_all(&[&CPU, &CPU_AGAIN, &OTHER])
        .unwrap_err();
    assert_eq!(errors.len(), 2);
}

fn snapshot(registry: &Registry) -> Vec<(String, Vec<&'static str>)> {
    registry
        .entries()
        .map(|e| {
            (
                e.decl.id.to_string(),
                e.bodies.iter().map(|b| b.backend).collect(),
            )
        })
        .collect()
}

#[test]
fn registration_order_does_not_change_the_result() {
    let forward = registered(&[&CPU, &ALT, &TYPED_A]);
    let backward = registered(&[&TYPED_A, &ALT, &CPU]);
    assert_eq!(snapshot(&forward), snapshot(&backward));
}

const BODY_INPUTS: &[PortDecl] = &[PortDecl::new("in", TypeRef::Body)];
const BODY_OUTPUTS: &[PortDecl] = &[PortDecl::new("out", TypeRef::Body)];

const fn typed_decl(at: &'static str) -> NodeDecl {
    let mut node = decl(at, BODY_INPUTS, BODY_OUTPUTS);
    node.id = "demo.typed";
    node
}

const fn typed_body(
    backend: &'static str,
    ports: &'static [(&'static str, TypeRef)],
) -> BodyRecord {
    let mut record = body(backend, "t.src:1");
    record.ports = ports;
    record
}

const SAME_TYPES: &[(&str, TypeRef)] = &[("in", U8), ("out", U8)];
const WRONG_TYPES: &[(&str, TypeRef)] = &[("in", U16), ("out", U8)];
const TYPED_DECL_A: NodeDecl = typed_decl("s.rs:1");
const TYPED_A: NodeRecord = NodeRecord {
    decl: &TYPED_DECL_A,
    body: typed_body("alt", SAME_TYPES),
};
const TYPED_DECL_B: NodeDecl = typed_decl("s.rs:2");
const TYPED_SAME: NodeRecord = NodeRecord {
    decl: &TYPED_DECL_B,
    body: typed_body("fourth", SAME_TYPES),
};
const TYPED_WRONG: NodeRecord = NodeRecord {
    decl: &TYPED_DECL_B,
    body: typed_body("fifth", WRONG_TYPES),
};

#[test]
fn a_body_supplies_a_port_type_and_a_later_body_must_agree() {
    let mut registry = registered(&[&TYPED_A, &TYPED_SAME]);
    let entry = registry.entry("demo.typed").unwrap();
    assert_eq!(entry.port_type(&entry.decl.inputs[0]), U8);
    assert!(matches!(
        registry.register(&TYPED_WRONG).unwrap_err(),
        RegError::DeclConflict {
            port: Some("in"),
            ..
        }
    ));
}

const fn conversion_decl() -> NodeDecl {
    let mut node = decl("k.rs:1", A_INPUTS, WIDE_OUTPUTS);
    node.id = "demo.widen";
    node.conversion = Some(Conversion {
        cost: 3,
        lossy: false,
    });
    node
}

const WIDE_OUTPUTS: &[PortDecl] = &[PortDecl::new("out", U16)];
const WIDEN_DECL: NodeDecl = conversion_decl();
const WIDEN: NodeRecord = NodeRecord {
    decl: &WIDEN_DECL,
    body: body("cpu", "k.rs:1"),
};

#[test]
fn conversions_are_read_from_marked_nodes() {
    let registry = registered(&[&WIDEN, &CPU]);
    assert_eq!(
        registry.conversions(),
        [ConversionEntry {
            id: "demo.widen",
            from: Scalar::U8.key(),
            to: Scalar::U16.key(),
            cost: 3,
            lossy: false,
        }]
    );
}

#[test]
fn one_key_has_one_structure() {
    let mut table = TypeTable::new();
    let key = TypeKey::new("acme.thing");
    let record = |at, scalar| TypeRecord {
        key: key.clone(),
        expr: TypeExpr::Scalar(scalar),
        at: SourceAt(at),
    };
    table.register(record("a:1", Scalar::U8)).unwrap();
    table.register(record("b:2", Scalar::U8)).unwrap();
    assert_eq!(
        table.register(record("c:3", Scalar::U16)).unwrap_err(),
        RegError::TypeConflict {
            key: key.clone(),
            first: SourceAt("a:1"),
            second: SourceAt("c:3")
        }
    );
    assert_eq!(table.records().count(), 1);
}

#[test]
fn an_offer_lists_only_nodes_with_a_body_for_the_backend() {
    let registry = registered(&[&CPU, &ALT, &WIDEN]);
    let nodes = [
        (NodeIndex(0), "demo.blur"),
        (NodeIndex(1), "demo.widen"),
        (NodeIndex(2), "demo.missing"),
    ];
    let data = registry.offer_data("alt", &nodes, |_| Some(TypeIndex(5)));
    let listed = data.nodes();
    let offer = Offer { nodes: &listed };
    assert_eq!(offer.nodes.len(), 1);
    assert_eq!(offer.nodes[0].node, NodeIndex(0));
    assert_eq!(offer.nodes[0].inputs[0].ty, TypeIndex(5));
    assert_eq!(offer.nodes[0].outputs[0].name, "out");
    let none = registry.offer_data("alt", &nodes, |_| None);
    assert!(
        none.nodes().is_empty(),
        "a port type with no index leaves the node out"
    );
}

#[test]
fn an_owned_declaration_joins_like_a_record() {
    let mut registry = registered(&[&CPU]);
    let worker = |backend: &str, port: &str| OwnedDecl {
        id: "demo.blur".into(),
        source: "worker.py:1".into(),
        inputs: vec![OwnedPort {
            name: "in".into(),
            ty: U8,
            optional: false,
        }],
        outputs: vec![OwnedPort {
            name: port.into(),
            ty: U8,
            optional: false,
        }],
        body: OwnedBody {
            backend: backend.into(),
            text: String::new(),
        },
    };
    registry.register_owned(worker("process", "out")).unwrap();
    assert_eq!(registry.entry("demo.blur").unwrap().bodies.len(), 2);
    assert!(matches!(
        registry
            .register_owned(worker("python", "other"))
            .unwrap_err(),
        RegError::DeclConflict { .. }
    ));
}

#[test]
fn bad_ids_and_empty_backends_are_refused() {
    let bad: &'static NodeDecl = Box::leak(Box::new(NodeDecl {
        id: "Bad",
        ..CPU_DECL.clone()
    }));
    let record = Box::leak(Box::new(NodeRecord {
        decl: bad,
        body: body("cpu", "x"),
    }));
    assert!(matches!(
        Registry::new().register(record),
        Err(RegError::InvalidId { .. })
    ));
    let record = Box::leak(Box::new(NodeRecord {
        decl: &CPU_DECL,
        body: body("", "x"),
    }));
    assert!(matches!(
        Registry::new().register(record),
        Err(RegError::UnknownBackend { .. })
    ));
}
