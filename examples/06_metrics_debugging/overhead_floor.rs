use std::time::{Duration, Instant};

use daedalus::{
    engine::{Engine, EngineConfig, GpuBackend, MetricsLevel},
    macros::{node, plugin},
    runtime::{
        ExecutionTelemetry, NodeError, handler_registry::HandlerRegistry, plugins::PluginRegistry,
    },
    transport::{Payload, TypeKey},
};

const WARMUP_ITERS: i64 = 1_000;
const MEASURE_ITERS: i64 = 20_000;

#[node(
    id = "overhead.identity",
    inputs("value"),
    outputs("value"),
    same_payload
)]
fn identity(value: &i64) -> Result<i64, NodeError> {
    Ok(*value)
}

#[plugin(id = "example.overhead_floor", nodes(identity))]
struct OverheadFloorPlugin;

#[derive(Default)]
struct TimingTotals {
    graph: Duration,
    node_envelope: Duration,
    handler: Duration,
    transport: Duration,
    adapter: Duration,
}

impl TimingTotals {
    fn record(&mut self, telemetry: &ExecutionTelemetry) {
        self.graph += telemetry.graph_duration;
        self.node_envelope += telemetry
            .node_metrics
            .values()
            .map(|metrics| metrics.total_duration)
            .sum::<Duration>();
        self.handler += telemetry
            .node_metrics
            .values()
            .map(|metrics| metrics.handler_duration)
            .sum::<Duration>();
        self.transport += telemetry
            .edge_metrics
            .values()
            .map(|metrics| metrics.transport_apply_duration)
            .sum::<Duration>();
        self.adapter += telemetry
            .edge_metrics
            .values()
            .map(|metrics| metrics.adapter_duration)
            .sum::<Duration>();
    }

    fn runtime_overhead(&self) -> Duration {
        self.graph
            .saturating_sub(self.handler)
            .saturating_sub(self.transport)
            .saturating_sub(self.adapter)
    }

    fn node_overhead(&self) -> Duration {
        self.node_envelope
            .saturating_sub(self.handler)
            .saturating_sub(self.transport)
            .saturating_sub(self.adapter)
    }
}

fn build_runtime(
    metrics_level: MetricsLevel,
) -> Result<daedalus::engine::HostGraph<HandlerRegistry>, Box<dyn std::error::Error>> {
    let mut registry = PluginRegistry::new();
    let plugin = OverheadFloorPlugin::new();
    registry.install(&plugin)?;
    let identity = plugin.identity.alias("identity");
    let graph = registry
        .graph_builder()?
        .inputs(|g| {
            g.input("in");
        })
        .outputs(|g| {
            g.output("out");
        })
        .nodes(|g| {
            g.add_handle(&identity);
        })
        .try_edges(|g| {
            let identity = g.node("identity");
            g.try_connect("in", &identity.input("value"))?;
            g.try_connect(&identity.output("value"), "out")?;
            Ok(())
        })?
        .build();

    let engine =
        Engine::new(EngineConfig::from(GpuBackend::Cpu).with_metrics_level(metrics_level))?;
    Ok(engine.compile_registry(&registry, graph)?)
}

fn run_detailed() -> Result<TimingTotals, Box<dyn std::error::Error>> {
    let mut runtime = build_runtime(MetricsLevel::Detailed)?;
    for value in 0..WARMUP_ITERS {
        runtime.push("in", value);
        runtime.tick()?;
        let _ = runtime.drain_owned::<i64>("out")?;
    }

    let mut totals = TimingTotals::default();
    for value in 0..MEASURE_ITERS {
        runtime.push("in", value);
        let telemetry = runtime.tick()?;
        let _ = runtime.drain_owned::<i64>("out")?;
        totals.record(&telemetry);
    }
    Ok(totals)
}

fn run_metrics_off_wall_typed_push() -> Result<Duration, Box<dyn std::error::Error>> {
    let mut runtime = build_runtime(MetricsLevel::Off)?;
    for value in 0..WARMUP_ITERS {
        runtime.push("in", value);
        runtime.tick()?;
        let _ = runtime.drain_owned::<i64>("out")?;
    }

    let start = Instant::now();
    for value in 0..MEASURE_ITERS {
        runtime.push("in", value);
        runtime.tick()?;
        let _ = runtime.drain_owned::<i64>("out")?;
    }
    Ok(start.elapsed())
}

fn run_metrics_off_wall_payload_push() -> Result<Duration, Box<dyn std::error::Error>> {
    let mut runtime = build_runtime(MetricsLevel::Off)?;
    let type_key = TypeKey::new("i64");
    for value in 0..WARMUP_ITERS {
        runtime.push_payload("in", Payload::owned(type_key.clone(), value));
        runtime.tick()?;
        let _ = runtime.drain_owned::<i64>("out")?;
    }

    let start = Instant::now();
    for value in 0..MEASURE_ITERS {
        runtime.push_payload("in", Payload::owned(type_key.clone(), value));
        runtime.tick()?;
        let _ = runtime.drain_owned::<i64>("out")?;
    }
    Ok(start.elapsed())
}

fn run_metrics_off_wall_payload_take() -> Result<Duration, Box<dyn std::error::Error>> {
    let mut runtime = build_runtime(MetricsLevel::Off)?;
    let type_key = TypeKey::new("i64");
    for value in 0..WARMUP_ITERS {
        runtime.push_payload("in", Payload::owned(type_key.clone(), value));
        runtime.tick()?;
        let _ = runtime.take_owned::<i64>("out")?;
    }

    let start = Instant::now();
    for value in 0..MEASURE_ITERS {
        runtime.push_payload("in", Payload::owned(type_key.clone(), value));
        runtime.tick()?;
        let _ = runtime.take_owned::<i64>("out")?;
    }
    Ok(start.elapsed())
}

fn run_metrics_off_bound_push_take() -> Result<Duration, Box<dyn std::error::Error>> {
    let mut runtime = build_runtime(MetricsLevel::Off)?;
    let input = runtime.bind_input::<i64>("in");
    let output = runtime.bind_output::<i64>("out");
    for value in 0..WARMUP_ITERS {
        input.push(value);
        runtime.tick()?;
        let _ = output
            .try_take()
            .map_err(|_| "bound output type mismatch")?
            .ok_or("bound output missing during warmup")?;
    }

    let start = Instant::now();
    for value in 0..MEASURE_ITERS {
        input.push(value);
        runtime.tick()?;
        let _ = output
            .try_take()
            .map_err(|_| "bound output type mismatch")?
            .ok_or("bound output missing")?;
    }
    Ok(start.elapsed())
}

fn run_metrics_off_direct_host() -> Result<Duration, Box<dyn std::error::Error>> {
    let mut runtime = build_runtime(MetricsLevel::Off)?;
    let type_key = TypeKey::new("i64");
    for value in 0..WARMUP_ITERS {
        let Some((_, output)) =
            runtime.tick_direct_payload("in", Payload::owned(type_key.clone(), value), "out")?
        else {
            return Err("direct host path unsupported".into());
        };
        let _ = output
            .ok_or("direct host output missing")?
            .try_into_owned::<i64>()
            .map_err(|_| "direct host output type mismatch")?;
    }

    let start = Instant::now();
    for value in 0..MEASURE_ITERS {
        let Some((_, output)) =
            runtime.tick_direct_payload("in", Payload::owned(type_key.clone(), value), "out")?
        else {
            return Err("direct host path unsupported".into());
        };
        let _ = output
            .ok_or("direct host output missing")?
            .try_into_owned::<i64>()
            .map_err(|_| "direct host output type mismatch")?;
    }
    Ok(start.elapsed())
}

fn run_metrics_off_cached_direct_host() -> Result<Duration, Box<dyn std::error::Error>> {
    let mut runtime = build_runtime(MetricsLevel::Off)?;
    let route = runtime
        .direct_host_route("in", "out")
        .ok_or("direct host route unsupported")?;
    let type_key = TypeKey::new("i64");
    for value in 0..WARMUP_ITERS {
        let (_, output) =
            runtime.tick_direct_route(&route, Payload::owned(type_key.clone(), value))?;
        let _ = output
            .ok_or("direct host output missing")?
            .try_into_owned::<i64>()
            .map_err(|_| "direct host output type mismatch")?;
    }

    let start = Instant::now();
    for value in 0..MEASURE_ITERS {
        let (_, output) =
            runtime.tick_direct_route(&route, Payload::owned(type_key.clone(), value))?;
        let _ = output
            .ok_or("direct host output missing")?
            .try_into_owned::<i64>()
            .map_err(|_| "direct host output type mismatch")?;
    }
    Ok(start.elapsed())
}

fn run_metrics_off_cached_direct_payload() -> Result<Duration, Box<dyn std::error::Error>> {
    let mut runtime = build_runtime(MetricsLevel::Off)?;
    let route = runtime
        .direct_host_route("in", "out")
        .ok_or("direct host route unsupported")?;
    let type_key = TypeKey::new("i64");
    for value in 0..WARMUP_ITERS {
        let output =
            runtime.tick_direct_route_payload(&route, Payload::owned(type_key.clone(), value))?;
        let _ = output
            .ok_or("direct host output missing")?
            .try_into_owned::<i64>()
            .map_err(|_| "direct host output type mismatch")?;
    }

    let start = Instant::now();
    for value in 0..MEASURE_ITERS {
        let output =
            runtime.tick_direct_route_payload(&route, Payload::owned(type_key.clone(), value))?;
        let _ = output
            .ok_or("direct host output missing")?
            .try_into_owned::<i64>()
            .map_err(|_| "direct host output type mismatch")?;
    }
    Ok(start.elapsed())
}

fn run_metrics_off_bound_lane() -> Result<Duration, Box<dyn std::error::Error>> {
    let mut runtime = build_runtime(MetricsLevel::Off)?;
    let lane = runtime
        .bind_lane::<i64>("in", "out")
        .ok_or("bound lane unsupported")?;
    for value in 0..WARMUP_ITERS {
        let output = runtime.run_lane(&lane, value)?;
        let _ = output
            .ok_or("bound lane output missing")?
            .try_into_owned::<i64>()
            .map_err(|_| "bound lane output type mismatch")?;
    }

    let start = Instant::now();
    for value in 0..MEASURE_ITERS {
        let output = runtime.run_lane(&lane, value)?;
        let _ = output
            .ok_or("bound lane output missing")?
            .try_into_owned::<i64>()
            .map_err(|_| "bound lane output type mismatch")?;
    }
    Ok(start.elapsed())
}

fn avg_ns(duration: Duration) -> f64 {
    duration.as_nanos() as f64 / MEASURE_ITERS as f64
}

fn main() -> Result<(), Box<dyn std::error::Error>> {
    let detailed = run_detailed()?;
    let typed_wall = run_metrics_off_wall_typed_push()?;
    let payload_wall = run_metrics_off_wall_payload_push()?;
    let payload_take_wall = run_metrics_off_wall_payload_take()?;
    let bound_push_take_wall = run_metrics_off_bound_push_take()?;
    let direct_host_wall = run_metrics_off_direct_host()?;
    let cached_direct_host_wall = run_metrics_off_cached_direct_host()?;
    let cached_direct_payload_wall = run_metrics_off_cached_direct_payload()?;
    let bound_lane_wall = run_metrics_off_bound_lane()?;

    println!("overhead floor: one identity node, direct input/output handoff");
    println!("iterations: {MEASURE_ITERS} measured, {WARMUP_ITERS} warmup");
    println!("detailed telemetry averages:");
    println!("  graph_total_ns: {:.1}", avg_ns(detailed.graph));
    println!("  handler_ns: {:.1}", avg_ns(detailed.handler));
    println!(
        "  runtime_overhead_ns: {:.1}",
        avg_ns(detailed.runtime_overhead())
    );
    println!("  node_envelope_ns: {:.1}", avg_ns(detailed.node_envelope));
    println!(
        "  node_overhead_ns: {:.1}",
        avg_ns(detailed.node_overhead())
    );
    println!("  transport_ns: {:.1}", avg_ns(detailed.transport));
    println!("  adapter_ns: {:.1}", avg_ns(detailed.adapter));
    println!("metrics-off wall average:");
    println!("  typed_push_tick_drain_ns: {:.1}", avg_ns(typed_wall));
    println!("  payload_push_tick_drain_ns: {:.1}", avg_ns(payload_wall));
    println!(
        "  payload_push_tick_take_ns: {:.1}",
        avg_ns(payload_take_wall)
    );
    println!(
        "  bound_push_tick_take_ns: {:.1}",
        avg_ns(bound_push_take_wall)
    );
    println!("  direct_host_tick_ns: {:.1}", avg_ns(direct_host_wall));
    println!(
        "  cached_direct_host_tick_ns: {:.1}",
        avg_ns(cached_direct_host_wall)
    );
    println!(
        "  cached_direct_payload_ns: {:.1}",
        avg_ns(cached_direct_payload_wall)
    );
    println!("  bound_lane_ns: {:.1}", avg_ns(bound_lane_wall));
    Ok(())
}
