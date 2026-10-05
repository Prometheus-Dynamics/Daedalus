use super::*;
use daedalus_data::model::{TypeExpr, ValueType};
use daedalus_mcu::McuType;
use daedalus_planner::{Edge, HostPortTypes, NodeInstance};
use daedalus_registry::typeexpr_transport_key;
use daedalus_runtime::host_bridge::{HOST_BRIDGE_ID, HOST_BRIDGE_META_KEY};

// The functions are only called through their generated `run` glue.
#[allow(dead_code)]
mod nodes {
    use daedalus_mcu::{NodeError, node};

    #[node(id = "t.gain", inputs("x", "k"), outputs("y"))]
    pub fn gain(x: f32, k: f32) -> f32 {
        x * k
    }

    #[node(id = "t.check", inputs("x", "limit"), outputs("ok", "over"))]
    pub fn check(x: &f32, limit: Option<f32>) -> Result<(bool, Option<f32>), NodeError> {
        let limit = limit.unwrap_or(1.0);
        if x.is_nan() {
            return Err(NodeError(1));
        }
        Ok((*x <= limit, (*x > limit).then_some(*x)))
    }

    #[node(id = "t.join", inputs("a", "b"), outputs("sum"), fire = "all")]
    pub fn join(a: i32, b: i32) -> i32 {
        a + b
    }
}

const NODES: [NodeDesc; 3] = [nodes::gain::NODE, nodes::check::NODE, nodes::join::NODE];

fn host() -> NodeInstance {
    NodeInstance::new(HOST_BRIDGE_ID)
        .with_label("host")
        .with_metadata(HOST_BRIDGE_META_KEY, Value::Bool(true))
}

/// `host.x -> gain(k = 2) -> check -> host.ok` (+ `check.over -> host.over`).
fn pipeline() -> Graph {
    Graph {
        nodes: vec![
            host(),
            NodeInstance::new("t.gain").with_const_input("k", Value::Int(2)),
            NodeInstance::new("t.check"),
        ],
        edges: vec![
            Edge::new(0, "x", 1, "x").with_metadata(
                "daedalus.edge.pressure",
                Value::String("latest_only".into()),
            ),
            Edge::new(1, "y", 2, "x"),
            Edge::new(2, "ok", 0, "ok"),
            Edge::new(2, "over", 0, "over"),
        ],
        metadata: Default::default(),
    }
}

fn unsupported(result: Result<McuPlan, CompileError>) -> String {
    match result {
        Err(err @ (CompileError::Unsupported(_) | CompileError::Planner(_))) => err.to_string(),
        other => panic!("expected a compile error, got {other:?}"),
    }
}

#[test]
fn builtin_keys_match_the_runtime() {
    fn key<T: McuType>(scalar: ValueType) {
        assert_eq!(
            T::KEY,
            typeexpr_transport_key(&TypeExpr::Scalar(scalar)).as_str()
        );
    }
    key::<()>(ValueType::Unit);
    key::<bool>(ValueType::Bool);
    key::<i8>(ValueType::I8);
    key::<i16>(ValueType::I16);
    key::<i32>(ValueType::I32);
    key::<i64>(ValueType::Int);
    key::<isize>(ValueType::ISize);
    key::<u8>(ValueType::U8);
    key::<u16>(ValueType::U16);
    key::<u32>(ValueType::U32);
    key::<u64>(ValueType::U64);
    key::<usize>(ValueType::USize);
    key::<f32>(ValueType::F32);
    key::<f64>(ValueType::Float);
}

#[test]
fn descriptors_record_ports_and_readiness() {
    let check = nodes::check::NODE;
    assert_eq!(check.id, "t.check");
    assert!(check.path.ends_with("tests::nodes::check"));
    let inputs: Vec<_> = check.inputs.iter().map(|p| (p.name, p.optional)).collect();
    assert_eq!(inputs, [("x", false), ("limit", true)]);
    let outputs: Vec<_> = check.outputs.iter().map(|p| (p.name, p.optional)).collect();
    assert_eq!(outputs, [("ok", false), ("over", true)]);
    assert!(nodes::join::NODE.fire_all && !check.fire_all);
    let mut state = ();
    let ctx = daedalus_mcu::Ctx::default();
    assert_eq!(
        nodes::check::run(&mut state, &ctx, 3.0, None),
        Ok((Some(false), Some(3.0)))
    );
    assert_eq!(
        nodes::check::run(&mut state, &ctx, f32::NAN, None),
        Err(daedalus_mcu::NodeError(1))
    );
}

#[test]
fn plans_schedule_queues_constants_and_host_ports() {
    let plan = plan(pipeline(), &NODES, &CompileOptions::default()).unwrap();
    let names: Vec<_> = plan.nodes.iter().map(|n| n.name.as_str()).collect();
    assert_eq!(names, ["t.gain", "t.check"]);
    let shapes: Vec<_> = plan
        .edges
        .iter()
        .map(|e| (e.ty.as_str(), e.capacity, e.overflow))
        .collect();
    assert_eq!(
        shapes,
        [
            ("f32", 1, Overflow::DropOldest),
            // Drained by its consumer every tick.
            ("f32", 1, Overflow::DropOldest),
            ("bool", 4, Overflow::Error),
            ("f32", 4, Overflow::Error),
        ]
    );
    assert_eq!(
        plan.nodes[0].inputs,
        [
            InputSource::Edge {
                edge: 0,
                required: true
            },
            InputSource::Const {
                value: Some(Scalar::F32(2.0)),
                expr: "2.0_f32".into(),
                required: true
            },
        ]
    );
    assert_eq!(plan.nodes[1].inputs[1], InputSource::Absent);
    assert_eq!(
        plan.host_inputs,
        [HostPort {
            name: "x".into(),
            ty: "f32".into(),
            key: f32::KEY.into(),
            edges: vec![0]
        }]
    );
    assert_eq!(plan.host_outputs.len(), 2);

    let source = plan.to_rust(&CompileOptions::default());
    for expected in [
        "pub struct Graph {",
        "e0: ::daedalus_mcu::Queue<f32, 1>,",
        "pub fn push_x(&mut self, value: f32)",
        "pub fn pop_over(&mut self) -> Option<f32>",
        "::run(&mut self.s0, &ctx, i0, 2.0_f32)",
        "::run(&mut self.s1, &ctx, i0, None)",
    ] {
        assert!(
            source.contains(expected),
            "missing `{expected}` in:\n{source}"
        );
    }
}

#[test]
fn declared_host_types_widen_at_the_push() {
    let mut graph = pipeline();
    let mut types = HostPortTypes::default();
    types.declare(true, "x", TypeExpr::Scalar(ValueType::U16));
    types.write_to_node_metadata(&mut graph.nodes[0].metadata);
    let plan = plan(graph, &NODES, &CompileOptions::default()).unwrap();
    assert!(plan.edges[0].widen && !plan.edges[1].widen);
    assert_eq!(plan.host_inputs[0].ty, "u16");
    let source = plan.to_rust(&CompileOptions::default());
    assert!(
        source.contains("<f32 as ::core::convert::From<_>>::from(value)"),
        "{source}"
    );
}

#[test]
fn fire_all_waits_on_required_edges() {
    let graph = Graph {
        nodes: vec![host(), NodeInstance::new("t.join")],
        edges: vec![
            Edge::new(0, "a", 1, "a"),
            Edge::new(0, "b", 1, "b"),
            Edge::new(1, "sum", 0, "sum"),
        ],
        metadata: Default::default(),
    };
    let plan = plan(
        graph,
        &NODES,
        &CompileOptions {
            fifo_capacity: 8,
            ..Default::default()
        },
    )
    .unwrap();
    assert!(plan.nodes[0].wait_all);
    let source = plan.to_rust(&CompileOptions::default());
    assert!(
        source.contains("if !self.e0.is_empty() && !self.e1.is_empty() {"),
        "{source}"
    );
    assert!(
        source.contains("self.e0.pop()") && source.contains("Queue<i32, 8>"),
        "{source}"
    );
}

#[test]
fn rejects_what_the_device_cannot_run() {
    let opts = CompileOptions::default();

    let mut graph = pipeline();
    graph.nodes[1].const_inputs.clear();
    assert!(unsupported(plan(graph, &NODES, &opts)).contains("`t.gain.k` is not connected"));

    let mut graph = pipeline();
    graph.edges.push(Edge::new(0, "x2", 2, "x"));
    assert!(unsupported(plan(graph, &NODES, &opts)).contains("fan-in"));

    let mut graph = pipeline();
    graph.nodes[1].const_inputs[0].1 = Value::String("two".into());
    assert!(unsupported(plan(graph, &NODES, &opts)).contains("t.gain"));

    let mut graph = pipeline();
    graph.nodes.push(NodeInstance::new("t.unknown"));
    assert!(unsupported(plan(graph, &NODES, &opts)).contains("t.unknown"));

    // i32 -> f32 is not lossless: the planner finds no converter.
    let mut graph = pipeline();
    graph.nodes.push(NodeInstance::new("t.join"));
    graph.edges.push(Edge::new(3, "sum", 1, "k"));
    graph.nodes[1].const_inputs.clear();
    assert!(unsupported(plan(graph, &NODES, &opts)).contains("ConverterMissing"));
}

/// `pipeline()` with `gain.k` tunable in `0..=10`.
fn tunable_pipeline() -> Graph {
    let mut graph = pipeline();
    let range = Value::List(vec![Value::Int(0), Value::Int(10)]);
    graph.nodes[1].metadata.insert(
        PARAMS_META_KEY.into(),
        Value::Map(vec![(Value::String("k".into()), range)]),
    );
    graph
}

#[test]
fn constants_rules_match_the_planner() {
    let values = [
        Value::Int(0),
        Value::Int(-1),
        Value::Int(255),
        Value::Int(256),
        Value::Int(1 << 24),
        Value::Int((1 << 24) + 1),
        Value::Int(i64::MAX),
        Value::Float(0.5),
        Value::Float(-3.0),
        Value::Float(1e300),
        Value::Float(f64::INFINITY),
    ];
    for kind in ScalarKind::ALL
        .into_iter()
        .filter(|k| *k != ScalarKind::Bool)
    {
        let key = typeexpr_transport_key(&TypeExpr::Scalar(match kind {
            ScalarKind::I8 => ValueType::I8,
            ScalarKind::I16 => ValueType::I16,
            ScalarKind::I32 => ValueType::I32,
            ScalarKind::I64 => ValueType::Int,
            ScalarKind::U8 => ValueType::U8,
            ScalarKind::U16 => ValueType::U16,
            ScalarKind::U32 => ValueType::U32,
            ScalarKind::U64 => ValueType::U64,
            ScalarKind::F32 => ValueType::F32,
            _ => ValueType::Float,
        }));
        let TypeExpr::Scalar(value_type) = daedalus_registry::transport_key_typeexpr(&key) else {
            unreachable!()
        };
        for value in &values {
            assert_eq!(
                scalar_value(kind, value).is_ok(),
                value_type.check_value(value).is_ok(),
                "{kind:?} {value:?}"
            );
        }
    }
}

#[test]
fn widening_table_matches_the_runtime() {
    let registry = registry(&[]).unwrap();
    let adapters = registry.transport_capabilities.adapters();
    for from in ScalarKind::ALL {
        for to in ScalarKind::ALL {
            let id = format!(
                "daedalus.builtin.widen.{}_to_{}",
                from.rust_name(),
                to.rust_name()
            );
            let id = daedalus_transport::AdapterId::new(id);
            assert_eq!(from.widens_to(to), adapters.contains_key(&id), "{id:?}");
        }
    }
}

#[test]
fn tunable_constants_become_parameters() {
    let plan = plan(tunable_pipeline(), &NODES, &CompileOptions::default()).unwrap();
    assert_eq!(
        plan.params,
        [PlanParam {
            name: "t.gain.k".into(),
            node: 0,
            input: 1,
            value: Scalar::F32(2.0),
            min: Scalar::F32(0.0),
            max: Scalar::F32(10.0),
        }]
    );
    assert_eq!(
        plan.nodes[0].inputs[1],
        InputSource::Param {
            param: 0,
            required: true
        }
    );
    let source = plan.to_rust(&CompileOptions::default());
    for expected in [
        "pub const PARAM_NAMES: [&str; 1] = [\"t.gain.k\"];",
        "p0: 2.0_f32,",
        "::run(&mut self.s0, &ctx, i0, self.p0)",
        "pub fn set_t_gain_k(&mut self, value: f32)",
        "impl ::daedalus_mcu::Tunable for Graph {",
    ] {
        assert!(
            source.contains(expected),
            "missing `{expected}` in:\n{source}"
        );
    }
    let manifest = plan.manifest(None);
    assert_eq!(manifest.params[0].max, Some(serde_json::json!(10.0)));
    // `(id 0, F32 tag 9, 2.5 LE)`, checked against the range on the host.
    let update = manifest
        .param_update("t.gain.k", &Value::Float(2.5))
        .unwrap();
    assert_eq!(update, [0, 9, 0x00, 0x00, 0x20, 0x40]);
    assert!(manifest.param_update("t.gain.k", &Value::Int(11)).is_err());
    assert!(manifest.param_update("t.gain.x", &Value::Int(1)).is_err());

    // Frozen: the same code as without markers (the plan hash covers the metadata).
    let frozen = CompileOptions {
        freeze_params: true,
        ..Default::default()
    };
    let body = |graph| {
        let source = compile(graph, &NODES, &frozen).unwrap();
        source.split_once("NODE_IDS").unwrap().1.to_string()
    };
    assert_eq!(body(tunable_pipeline()), body(pipeline()));
}

#[test]
fn rejects_invalid_parameters() {
    let opts = CompileOptions::default();
    let with = |port: &str, range: Value| {
        let mut graph = pipeline();
        graph.nodes[1].metadata.insert(
            PARAMS_META_KEY.into(),
            Value::Map(vec![(Value::String(port.to_string().into()), range)]),
        );
        unsupported(plan(graph, &NODES, &opts))
    };
    let range = |min, max| Value::List(vec![Value::Float(min), Value::Float(max)]);
    assert!(with("x", Value::Unit).contains("connected to an edge"));
    assert!(with("nope", Value::Unit).contains("no such input"));
    assert!(with("k", range(3.0, 4.0)).contains("outside its range"));
    assert!(with("k", Value::Int(3)).contains("not [min, max]"));
}

#[test]
fn library_manifest_hashes_interfaces() {
    let library = library(&NODES).unwrap();
    assert_eq!(library.types, Vec::<String>::new());
    assert_eq!(library.type_id(f32::KEY), Some(ScalarKind::F32 as u16));
    assert_eq!(
        LibraryManifest::from_json(&library.to_json()).unwrap(),
        library
    );

    // The hash covers interfaces, not code locations.
    let mut moved = specs(&NODES);
    moved[0].path = "elsewhere::gain".into();
    assert_eq!(
        LibraryManifest::new(moved.clone()).unwrap().hash,
        library.hash
    );
    moved[0].inputs[1].optional = true;
    assert_ne!(LibraryManifest::new(moved).unwrap().hash, library.hash);
    let mut tampered = library.clone();
    tampered.hash ^= 1;
    assert!(LibraryManifest::from_json(&tampered.to_json()).is_err());

    let source = library.to_rust().unwrap();
    for expected in [
        "pub static LIBRARY: ::daedalus_mcu::loaded::Library",
        "state: ::daedalus_mcu::loaded::StateEntry::of::<",
        "unsafe fn run_1(",
        "let out = ::",
        "io.input(0), io.input_opt(1))?;",
        "io.output(1, out.1);",
    ] {
        assert!(
            source.contains(expected),
            "missing `{expected}` in:\n{source}"
        );
    }
}

#[test]
fn blobs_are_deterministic_and_name_their_library() {
    let library = library(&NODES).unwrap();
    let plan = compile_loaded(tunable_pipeline(), &library, &CompileOptions::default()).unwrap();
    let blob = plan.to_blob(&library).unwrap();
    let again = compile_loaded(tunable_pipeline(), &library, &CompileOptions::default())
        .unwrap()
        .to_blob(&library)
        .unwrap();
    assert_eq!(blob, again);
    assert_eq!(
        blob[..5],
        [b'D', b'M', b'C', b'U', daedalus_mcu::loaded::FORMAT_VERSION]
    );
    let mut header = daedalus_mcu::wire::Reader::new(&blob[5..]);
    assert_eq!(header.varint(), Ok(library.hash));
    assert_eq!(header.varint(), Ok(plan.hash));

    let manifest = plan.manifest(Some(&library));
    assert_eq!(manifest.library_hash, Some(library.hash));
    // Blob edge order: node inputs in schedule order, then host outputs.
    assert_eq!(
        manifest.edges,
        [
            "host.x -> t.gain.x",
            "t.gain.y -> t.check.x",
            "t.check.ok -> host.ok",
            "t.check.over -> host.over"
        ]
    );
    assert_eq!(
        PlanManifest::from_json(&manifest.to_json()).unwrap(),
        manifest
    );

    // Constants of non-scalar types only exist in compiled mode.
    let mut plan = plan;
    plan.nodes[0].inputs[1] = InputSource::Const {
        value: None,
        expr: "()".into(),
        required: true,
    };
    assert!(plan.to_blob(&library).is_err());
}
