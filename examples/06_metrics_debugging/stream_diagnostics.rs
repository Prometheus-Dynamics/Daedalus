use parking_lot::Mutex;
use std::sync::Arc;
use std::time::Duration;

use daedalus::data::model::Value;
use daedalus::planner::{Edge, ExecutionPlan, Graph, NodeInstance};
use daedalus::runtime::host_bridge::{HOST_BRIDGE_ID, HOST_BRIDGE_META_KEY};
use daedalus::runtime::io::NodeIo;
use daedalus::runtime::state::ExecutionContext;
use daedalus::runtime::{
    NodeError, NodeHandler, RuntimeNode, SchedulerConfig, SharedStreamGraph, StreamGraph,
    build_runtime,
};
use daedalus::transport::Payload;

struct EchoHandler;

impl NodeHandler for EchoHandler {
    fn run(
        &self,
        node: &RuntimeNode,
        _ctx: &ExecutionContext,
        io: &mut NodeIo,
    ) -> Result<(), NodeError> {
        if node.id == "stream.echo" {
            let inputs = io
                .inputs_for("in")
                .map(|payload| payload.inner.clone())
                .collect::<Vec<_>>();
            for payload in inputs {
                io.push_payload("out", payload);
            }
        }
        Ok(())
    }
}

fn stream_plan() -> ExecutionPlan {
    let mut graph = Graph::default();
    graph.nodes.push(
        NodeInstance::new(HOST_BRIDGE_ID)
            .with_label("host")
            .with_inputs(["out"])
            .with_outputs(["in"])
            .with_metadata(HOST_BRIDGE_META_KEY, Value::Bool(true))
            .with_metadata(
                "dynamic_inputs",
                Value::String(std::borrow::Cow::Borrowed("generic")),
            )
            .with_metadata(
                "dynamic_outputs",
                Value::String(std::borrow::Cow::Borrowed("generic")),
            ),
    );
    graph.nodes.push(
        NodeInstance::new("stream.echo")
            .with_inputs(["in"])
            .with_outputs(["out"]),
    );
    graph.edges.push(Edge::new(0, "in", 1, "in"));
    graph.edges.push(Edge::new(1, "out", 0, "out"));
    ExecutionPlan::new(graph, vec![])
}

fn main() -> Result<(), Box<dyn std::error::Error>> {
    let runtime = Arc::new(build_runtime(&stream_plan(), &SchedulerConfig::default()));
    let mut graph = StreamGraph::new(runtime, EchoHandler);
    let input = graph.input("in")?;
    let output = graph.output("out")?;
    graph.start()?;

    let graph: SharedStreamGraph<EchoHandler> = Arc::new(Mutex::new(graph));
    let mut worker = StreamGraph::spawn_continuous(Arc::clone(&graph), Duration::from_millis(5));

    input.feed(Payload::owned("example:u32", 7_u32))?;
    let payload = output
        .recv_timeout(Duration::from_secs(1))?
        .ok_or("stream output timed out")?;
    println!("out={:?}", payload.get_ref::<u32>());

    let diagnostics = graph.lock().diagnostics();
    println!("graph_diagnostics={diagnostics:?}");
    println!("worker_diagnostics={:?}", worker.diagnostics());
    worker.stop_timeout(Duration::from_secs(1))?;
    Ok(())
}
