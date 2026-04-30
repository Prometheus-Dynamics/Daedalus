use std::sync::atomic::{AtomicUsize, Ordering};

use daedalus::{
    engine::{Engine, EngineConfig, HostGraph, MetricsLevel},
    macros::{node, plugin},
    runtime::{NodeError, RuntimeSink, handler_registry::HandlerRegistry, plugins::PluginRegistry},
};

static PREVIEW_CALLS: AtomicUsize = AtomicUsize::new(0);
static ARCHIVE_CALLS: AtomicUsize = AtomicUsize::new(0);
static AUDIT_CALLS: AtomicUsize = AtomicUsize::new(0);

#[node(id = "demand.preview", inputs("value"), outputs("value"))]
fn preview(value: &i64) -> Result<i64, NodeError> {
    PREVIEW_CALLS.fetch_add(1, Ordering::Relaxed);
    Ok(*value + 1)
}

#[node(id = "demand.archive", inputs("value"), outputs("value"))]
fn archive(value: &i64) -> Result<i64, NodeError> {
    ARCHIVE_CALLS.fetch_add(1, Ordering::Relaxed);
    Ok(*value * 100)
}

#[node(id = "demand.audit", inputs("value"), outputs("value"))]
fn audit(value: &i64) -> Result<i64, NodeError> {
    AUDIT_CALLS.fetch_add(1, Ordering::Relaxed);
    Ok(*value * -1)
}

#[plugin(id = "example.demand", nodes(preview, archive, audit))]
struct DemandPlugin;

fn reset_calls() {
    PREVIEW_CALLS.store(0, Ordering::Relaxed);
    ARCHIVE_CALLS.store(0, Ordering::Relaxed);
    AUDIT_CALLS.store(0, Ordering::Relaxed);
}

fn calls() -> (usize, usize, usize) {
    (
        PREVIEW_CALLS.load(Ordering::Relaxed),
        ARCHIVE_CALLS.load(Ordering::Relaxed),
        AUDIT_CALLS.load(Ordering::Relaxed),
    )
}

fn build_runtime() -> Result<HostGraph<HandlerRegistry>, Box<dyn std::error::Error>> {
    let mut registry = PluginRegistry::new();
    let plugin = DemandPlugin::new();
    registry.install(&plugin)?;
    let preview = plugin.preview.alias("preview");
    let archive = plugin.archive.alias("archive");
    let audit = plugin.audit.alias("audit");
    let graph = registry
        .graph_builder()?
        .inputs(|g| {
            g.input("in");
        })
        .outputs(|g| {
            g.output("preview_out");
            g.output("archive_out");
            g.output("audit_out");
        })
        .nodes(|g| {
            g.add_handle(&preview);
            g.add_handle(&archive);
            g.add_handle(&audit);
        })
        .try_edges(|g| {
            let preview = g.node("preview");
            let archive = g.node("archive");
            let audit = g.node("audit");
            g.try_connect("in", &preview.input("value"))?;
            g.try_connect("in", &archive.input("value"))?;
            g.try_connect("in", &audit.input("value"))?;
            g.try_connect(&preview.output("value"), "preview_out")?;
            g.try_connect(&archive.output("value"), "archive_out")?;
            g.try_connect(&audit.output("value"), "audit_out")?;
            Ok(())
        })?
        .build();

    let engine = Engine::new(EngineConfig::default().with_metrics_level(MetricsLevel::Detailed))?;
    Ok(engine.compile_registry(&registry, graph)?)
}

fn print_case(
    name: &str,
    runtime: &mut HostGraph<HandlerRegistry>,
    telemetry_nodes: usize,
) -> Result<(), Box<dyn std::error::Error>> {
    let preview = runtime.drain_owned::<i64>("preview_out")?;
    let archive = runtime.drain_owned::<i64>("archive_out")?;
    let audit = runtime.drain_owned::<i64>("audit_out")?;
    let (preview_calls, archive_calls, audit_calls) = calls();
    println!(
        "{name}: outputs preview={preview:?} archive={archive:?} audit={audit:?}; calls preview={preview_calls} archive={archive_calls} audit={audit_calls}; telemetry_nodes={telemetry_nodes}"
    );
    Ok(())
}

fn main() -> Result<(), Box<dyn std::error::Error>> {
    let mut runtime = build_runtime()?;

    reset_calls();
    runtime.push("in", 7_i64);
    let telemetry = runtime.tick()?;
    print_case("full tick", &mut runtime, telemetry.nodes_executed)?;
    println!("{}", telemetry.compact_snapshot());

    reset_calls();
    runtime.push("in", 7_i64);
    let preview_sink = RuntimeSink::node_id("io.host_bridge").port("preview_out");
    let telemetry = runtime.tick_selected([preview_sink.clone()])?;
    print_case("selected preview", &mut runtime, telemetry.nodes_executed)?;
    let selected_plan = runtime.explain_selected([preview_sink])?;
    println!(
        "selected preview plan: nodes={} edges={} handoffs={:?}",
        selected_plan.nodes.len(),
        selected_plan.edges.len(),
        selected_plan
            .edges
            .iter()
            .map(|edge| (edge.index, &edge.handoff, edge.handoff_reason.as_str()))
            .collect::<Vec<_>>()
    );
    println!("{}", telemetry.compact_snapshot());

    reset_calls();
    runtime.push("in", 7_i64);
    let telemetry = runtime.tick_selected([
        RuntimeSink::node_id("io.host_bridge").port("preview_out"),
        RuntimeSink::node_id("io.host_bridge").port("archive_out"),
    ])?;
    print_case(
        "selected preview+archive",
        &mut runtime,
        telemetry.nodes_executed,
    )?;
    println!("{}", telemetry.compact_snapshot());

    Ok(())
}
