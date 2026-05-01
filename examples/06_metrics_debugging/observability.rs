use std::sync::{Arc, Mutex};

use daedalus::{
    ComputeAffinity,
    engine::{Engine, EngineConfig, GpuBackend, MetricsLevel, RuntimeMode},
    macros::{node, plugin},
    planner::{ExecutionPlan, Graph, NodeInstance},
    registry::ids::NodeId,
    runtime::{
        NodeError, NodeHandler, RuntimeEdgePolicy, RuntimeNode, SchedulerConfig, build_runtime,
        plugins::PluginRegistry,
    },
};
use tracing_subscriber::EnvFilter;

#[node(id = "observability.double", inputs("value"), outputs("value"))]
fn double(value: &i64) -> Result<i64, NodeError> {
    Ok(*value * 2)
}

#[plugin(id = "example.metrics.observability", nodes(double))]
struct ObservabilityPlugin;

#[derive(Clone)]
struct FailingHarness {
    calls: Arc<Mutex<Vec<String>>>,
}

impl NodeHandler for FailingHarness {
    fn run(
        &self,
        node: &RuntimeNode,
        _ctx: &daedalus::runtime::ExecutionContext,
        _io: &mut daedalus::runtime::NodeIo,
    ) -> Result<(), NodeError> {
        self.calls
            .lock()
            .unwrap_or_else(|poisoned| poisoned.into_inner())
            .push(node.id.clone());
        if node.id.starts_with("fail") {
            return Err(NodeError::InvalidInput(
                "intentional observability failure".into(),
            ));
        }
        Ok(())
    }
}

fn init_tracing() {
    let filter = EnvFilter::try_from_default_env()
        .unwrap_or_else(|_| EnvFilter::new("daedalus_runtime=trace,daedalus_engine=debug"));
    let _ = tracing_subscriber::fmt().with_env_filter(filter).try_init();
}

fn host_graph_observability() -> Result<(), Box<dyn std::error::Error>> {
    let mut registry = PluginRegistry::new();
    let plugin = ObservabilityPlugin::new();
    registry.install(&plugin)?;

    let double = plugin.double.alias("double");
    let graph = registry
        .graph_builder()?
        .inputs(|g| {
            g.input("in");
        })
        .outputs(|g| {
            g.output("out");
        })
        .nodes(|g| {
            g.add_handle(&double);
        })
        .try_edges(|g| {
            let double = g.node("double");
            g.try_connect("in", &double.input("value"))?;
            g.try_connect(&double.output("value"), "out")?;
            Ok(())
        })?
        .build();

    let engine = Engine::new(
        EngineConfig::from(GpuBackend::Cpu)
            .with_metrics_level(MetricsLevel::Trace)
            .with_host_event_limit(Some(16)),
    )?;
    let mut runtime = engine.compile_registry(&registry, graph)?;

    runtime.push("in", 21_i64);
    runtime.push("in", 22_i64);
    let telemetry = runtime.tick()?;

    println!("host output: {:?}", runtime.drain_owned::<i64>("out")?);
    println!("host bridge events:");
    for event in runtime.host().events() {
        println!(
            "  {:?} {}.{} correlation={} type={} reason={:?}",
            event.kind, event.alias, event.port, event.correlation_id, event.type_key, event.reason
        );
    }
    println!("{}", telemetry.report().to_table());
    println!("{}", telemetry.compact_snapshot());
    Ok(())
}

fn failing_plan() -> daedalus::runtime::RuntimePlan {
    let mut graph = Graph::default();
    for id in ["fail.left", "fail.right", "ok.after"] {
        graph.nodes.push(NodeInstance {
            id: NodeId::new(id),
            bundle: None,
            label: None,
            inputs: vec![],
            outputs: vec![],
            compute: ComputeAffinity::CpuOnly,
            const_inputs: vec![],
            sync_groups: vec![],
            metadata: Default::default(),
        });
    }
    build_runtime(
        &ExecutionPlan::new(graph, vec![]),
        &SchedulerConfig {
            default_policy: RuntimeEdgePolicy::fifo(),
            backpressure: Default::default(),
        },
    )
}

fn segment_failure_observability() -> Result<(), Box<dyn std::error::Error>> {
    let calls = Arc::new(Mutex::new(Vec::new()));
    let engine = Engine::new(
        EngineConfig::from(GpuBackend::Cpu)
            .with_runtime_mode(RuntimeMode::Parallel)
            .with_metrics_level(MetricsLevel::Detailed)
            .with_fail_fast(false),
    )?;
    let telemetry = engine.execute(
        failing_plan(),
        FailingHarness {
            calls: Arc::clone(&calls),
        },
    )?;

    println!(
        "non-fail-fast calls: {:?}",
        calls
            .lock()
            .unwrap_or_else(|poisoned| poisoned.into_inner())
    );
    println!("segment failures:");
    for error in &telemetry.errors {
        println!(
            "  node={} code={} error={}",
            error.node_id, error.code, error.message
        );
    }
    println!("{}", telemetry.compact_snapshot());
    Ok(())
}

fn main() -> Result<(), Box<dyn std::error::Error>> {
    init_tracing();
    host_graph_observability()?;
    segment_failure_observability()?;
    Ok(())
}
