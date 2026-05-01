use std::collections::BTreeMap;
use std::sync::{Arc, Mutex};
use std::time::Duration;

use daedalus::data::model::Value;
use daedalus::planner::{Edge, ExecutionPlan, Graph, NodeInstance, NodeRef, PortRef};
use daedalus::registry::ids::NodeId;
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
    graph.nodes.push(NodeInstance {
        id: NodeId::new(HOST_BRIDGE_ID),
        bundle: None,
        label: Some("host".into()),
        inputs: vec!["out".into()],
        outputs: vec!["in".into()],
        compute: daedalus::ComputeAffinity::CpuOnly,
        const_inputs: vec![],
        sync_groups: vec![],
        metadata: BTreeMap::from([
            (HOST_BRIDGE_META_KEY.to_string(), Value::Bool(true)),
            (
                "dynamic_inputs".to_string(),
                Value::String(std::borrow::Cow::Borrowed("generic")),
            ),
            (
                "dynamic_outputs".to_string(),
                Value::String(std::borrow::Cow::Borrowed("generic")),
            ),
        ]),
    });
    graph.nodes.push(NodeInstance {
        id: NodeId::new("stream.echo"),
        bundle: None,
        label: None,
        inputs: vec!["in".into()],
        outputs: vec!["out".into()],
        compute: daedalus::ComputeAffinity::CpuOnly,
        const_inputs: vec![],
        sync_groups: vec![],
        metadata: BTreeMap::new(),
    });
    graph.edges.push(Edge {
        from: PortRef {
            node: NodeRef(0),
            port: "in".into(),
        },
        to: PortRef {
            node: NodeRef(1),
            port: "in".into(),
        },
        metadata: BTreeMap::new(),
    });
    graph.edges.push(Edge {
        from: PortRef {
            node: NodeRef(1),
            port: "out".into(),
        },
        to: PortRef {
            node: NodeRef(0),
            port: "out".into(),
        },
        metadata: BTreeMap::new(),
    });
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

    let diagnostics = graph
        .lock()
        .map_err(|_| "stream graph lock poisoned")?
        .diagnostics();
    println!("graph_diagnostics={diagnostics:?}");
    println!("worker_diagnostics={:?}", worker.diagnostics());
    worker.stop_timeout(Duration::from_secs(1))?;
    Ok(())
}
