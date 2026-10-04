use daedalus_data::model::Value;
use daedalus_planner::{Edge, ExecutionPlan, Graph, NodeInstance};
use daedalus_runtime::{
    BackpressureStrategy, DEFAULT_OUTPUT_PORT, Executor, NodeHandler, RuntimeEdgePolicy,
    RuntimeNode, SchedulerConfig, build_runtime, executor::NodeError, io::NodeIo,
};

struct Handler {
    seen_ports: std::sync::Arc<parking_lot::Mutex<Vec<String>>>,
}

impl NodeHandler for Handler {
    fn run(
        &self,
        node: &RuntimeNode,
        _ctx: &daedalus_runtime::state::ExecutionContext,
        io: &mut NodeIo,
    ) -> Result<(), NodeError> {
        match node.id.as_str() {
            "prod" => {
                io.push_payload(
                    "a",
                    daedalus_transport::Payload::bytes((&b"hello"[..]).into()),
                );
            }
            "cons" => {
                if let Some((port, _)) = io.inputs().first() {
                    self.seen_ports.lock().push(port.to_string());
                }
            }
            _ => {}
        }
        Ok(())
    }
}

#[test]
fn node_io_respects_ports_and_policies() {
    // Graph: producer has two outputs (a -> consumer, b -> unused)
    let mut graph = Graph::default();
    graph
        .nodes
        .push(NodeInstance::new("prod").with_outputs(["a", "b"]));
    graph
        .nodes
        .push(NodeInstance::new("cons").with_inputs(["in"]));
    graph.edges.push(Edge::new(0, "a", 1, "in"));

    let exec = ExecutionPlan::new(graph, vec![]);
    let rt = build_runtime(
        &exec,
        &SchedulerConfig {
            default_policy: RuntimeEdgePolicy::default(),
            backpressure: BackpressureStrategy::None,
        },
    );

    let seen = std::sync::Arc::new(parking_lot::Mutex::new(Vec::new()));
    let handler = Handler {
        seen_ports: seen.clone(),
    };
    let telemetry = Executor::new(&rt, handler).run().expect("runtime run");
    assert_eq!(telemetry.nodes_executed, 2);
    let ports = seen.lock().clone();
    assert_eq!(ports, vec!["in".to_string()]);
}

#[test]
fn node_io_default_output_helpers_use_default_port() {
    let mut io = NodeIo::empty();
    io.push_default(7_i64).unwrap();
    io.push_value_default(Value::Bool(true));

    let outputs = io.take_outputs();
    assert_eq!(outputs.len(), 2);
    assert_eq!(outputs[0].0, DEFAULT_OUTPUT_PORT);
    assert_eq!(outputs[1].0, DEFAULT_OUTPUT_PORT);
    assert_eq!(outputs[0].1.inner.get_ref::<i64>(), Some(&7_i64));
    assert_eq!(
        outputs[1].1.inner.get_ref::<Value>(),
        Some(&Value::Bool(true))
    );
}

#[test]
fn node_io_explicit_output_helpers_preserve_ports() {
    let mut io = NodeIo::empty();
    io.push_to("custom", 9_i64).unwrap();
    io.push_value_to("value", Value::Int(11));

    let outputs = io.take_outputs();
    assert_eq!(outputs.len(), 2);
    assert_eq!(outputs[0].0, "custom");
    assert_eq!(outputs[1].0, "value");
    assert_eq!(outputs[0].1.inner.get_ref::<i64>(), Some(&9_i64));
    assert_eq!(outputs[1].1.inner.get_ref::<Value>(), Some(&Value::Int(11)));
}

#[test]
fn take_owned_coerces_const_value_inputs() {
    use daedalus_runtime::executor::CorrelatedPayload;
    let value = |v| CorrelatedPayload::from_edge(daedalus_transport::Payload::owned("value", v));
    let mut io = NodeIo::from_inputs([
        ("scale".into(), value(Value::Float(2.5))),
        ("raw".into(), value(Value::Int(3))),
    ]);
    assert_eq!(io.take_owned::<f64>("scale"), Some(2.5));
    assert_eq!(io.take_owned::<Value>("raw"), Some(Value::Int(3)));
    assert_eq!(io.take_owned::<f64>("missing"), None);
}
