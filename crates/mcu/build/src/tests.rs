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
                expr: "2_f32".into(),
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
        "::run(&mut self.s0, &ctx, i0, 2_f32)",
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
