//! Demonstrates typed payloads using nodes defined directly in the example.
//! Run with: cargo run -p daedalus-rs --features "engine,plugins" --example typed_any

#![cfg(all(feature = "engine", feature = "plugins"))]

use std::{thread::sleep, time::Duration};

use daedalus::{
    data::model::Value,
    declare_plugin,
    engine::{Engine, EngineConfig, GpuBackend, RuntimeMode},
    graph_builder::GraphBuilder,
    host_bridge::host_port,
    install_host_bridge,
    macros::node,
    runtime::{
        BackpressureStrategy, EdgePolicyKind, HostBridgeHandle, HostBridgeManager, NodeError,
        executor::Executor,
        plugins::{PluginRegistry, RegistryPluginExt},
    },
};

#[derive(Clone, Debug)]
struct Frame {
    bytes: Vec<u8>,
}

#[derive(Clone, Debug)]
struct Detection {
    #[allow(dead_code)]
    id: i64,
}

#[node(id = "count", inputs("frame"), outputs("count"))]
fn count(frame: Frame) -> Result<i64, NodeError> {
    Ok(frame.bytes.len() as i64)
}

#[node(id = "decode", inputs("frame"), outputs("detections"))]
fn decode(frame: Frame) -> Result<Vec<Detection>, NodeError> {
    Ok(vec![Detection {
        id: frame.bytes.len() as i64,
    }])
}

declare_plugin!(TypedAnyPlugin, "example.typed_any", [decode, count]);

fn main() -> Result<(), Box<dyn std::error::Error>> {
    let host_mgr = HostBridgeManager::new();
    let mut reg = PluginRegistry::new();
    let plugin = TypedAnyPlugin::new();
    reg.install_plugin(&plugin)?;
    install_host_bridge(&mut reg, host_mgr.clone())?;
    let handlers = reg.take_handlers();

    let dec = plugin.decode.alias("decode");
    let cnt = plugin.count.alias("count");

    let graph = GraphBuilder::new(&reg.registry)
        .host_bridge("host")
        .node(&dec)
        .node(&cnt)
        .connect(&host_port("host", "frame"), &dec.inputs.frame)
        .connect(&host_port("host", "frame"), &cnt.inputs.frame)
        .connect(&dec.outputs.detections, &host_port("host", "detections"))
        .connect(&cnt.outputs.count, &host_port("host", "count"))
        .build();

    let mut cfg = EngineConfig::default();
    cfg.runtime.mode = RuntimeMode::Parallel;
    cfg.runtime.pool_size = None;
    cfg.runtime.default_policy = EdgePolicyKind::Fifo;
    cfg.runtime.backpressure = BackpressureStrategy::None;
    cfg.gpu = GpuBackend::Cpu;
    let engine = Engine::new(cfg)?;

    // Plan + build runtime to discover host bridge ports.
    let planner_output = engine.plan(&reg.registry, graph)?;
    let runtime_plan = engine.build_runtime_plan(&planner_output.plan)?;
    host_mgr.populate_from_plan(&runtime_plan);
    println!(
        "runtime plan nodes: {}, edges: {}, segments: {}",
        runtime_plan.nodes.len(),
        runtime_plan.edges.len(),
        runtime_plan.segments.len()
    );

    // Run two host-driven cycles with a 1s gap, draining two graph outputs (detections + count).
    let host = host_mgr.handle("host").expect("host bridge handle missing");
    let handlers_arc = handlers.clone_arc();

    host.push_any(
        "frame",
        Frame {
            bytes: b"frame-one".to_vec(),
        },
    );
    let telemetry1 = run_once(
        runtime_plan.clone(),
        handlers_arc.clone_arc(),
        host_mgr.clone(),
    )?;
    let detections1 = drain_detections(&host);
    let counts1 = drain_counts(&host)?;
    println!(
        "cycle1 detections: {:?}, counts: {:?}",
        detections1, counts1
    );
    println!("telemetry1: {:?}", telemetry1);

    sleep(Duration::from_secs(1));

    host.push_any(
        "frame",
        Frame {
            bytes: b"second-frame".to_vec(),
        },
    );
    let telemetry2 = run_once(runtime_plan, handlers_arc, host_mgr)?;
    let detections2 = drain_detections(&host);
    let counts2 = drain_counts(&host)?;
    println!(
        "cycle2 detections: {:?}, counts: {:?}",
        detections2, counts2
    );
    println!("telemetry2: {:?}", telemetry2);
    Ok(())
}

fn run_once(
    plan: daedalus::runtime::RuntimePlan,
    handlers: daedalus::runtime::handler_registry::HandlerRegistry,
    host_mgr: HostBridgeManager,
) -> Result<daedalus::runtime::ExecutionTelemetry, Box<dyn std::error::Error>> {
    let exec = Executor::new(&plan, handlers)
        .with_host_bridges(host_mgr)
        .with_pool_size(None);
    Ok(exec.run_parallel()?)
}

fn drain_detections(host: &HostBridgeHandle) -> Vec<Vec<Detection>> {
    let mut out = Vec::new();
    loop {
        let mut pushed = false;
        for port in host
            .incoming_ports()
            .filter(|p| p.name().eq_ignore_ascii_case("detections"))
        {
            if let Some((_corr, dets)) = port.try_pop_any::<Vec<Detection>>() {
                out.push(dets);
                pushed = true;
                break;
            }
        }
        if !pushed {
            break;
        }
    }
    out
}

fn drain_counts(host: &HostBridgeHandle) -> Result<Vec<i64>, NodeError> {
    let mut out = Vec::new();
    loop {
        let mut got = None;
        for port in host
            .incoming_ports()
            .filter(|p| p.name().eq_ignore_ascii_case("count"))
            .filter(|p| p.can_type_to::<Value>())
        {
            if let Some((_corr, value)) = port.try_pop::<Value>()? {
                got = Some(value);
                break;
            }
        }
        let Some(v) = got else {
            break;
        };
        match v {
            Value::Int(i) => out.push(i),
            other => {
                return Err(NodeError::InvalidInput(format!(
                    "expected Value::Int for count, got {other:?}"
                )));
            }
        }
    }
    Ok(out)
}

#[cfg(not(all(feature = "engine", feature = "plugins")))]
fn main() {
    eprintln!("enable `engine` and `plugins` features to run this example");
}
