//! A `Gpu<T>` node with `fallback` runs its CPU fallback on the default (CPU) backend, and a
//! plain `Cpu<T>` graph runs unchanged.
use daedalus::{
    engine::{Engine, EngineConfig},
    macros::{node, plugin},
    runtime::{NodeError, plugins::PluginRegistry},
    transport::{Cpu, Gpu},
};

#[node(id = "repro.fb.cpu", inputs("frame"), outputs("frame"))]
fn cpu_filter(frame: Cpu<i64>) -> Result<Cpu<i64>, NodeError> {
    Ok(frame)
}

#[node(
    id = "repro.fb.gpu",
    fallback = "repro.fb.cpu",
    inputs("frame"),
    outputs("frame")
)]
fn gpu_filter(frame: Gpu<i64>) -> Result<Gpu<i64>, NodeError> {
    Ok(frame)
}

#[node(id = "repro.fb.pre", inputs("in"), outputs("out"))]
fn pre(value: &i64) -> Result<Cpu<i64>, NodeError> {
    Ok(Cpu::new(*value))
}

#[plugin(id = "repro.fb", nodes(pre, cpu_filter, gpu_filter))]
struct ReproPlugin;

#[test]
fn control_cpu_node_directly() {
    let mut registry = PluginRegistry::new();
    let plugin = ReproPlugin::new();
    registry.install(&plugin).unwrap();
    let pre = plugin.pre.clone().alias("pre");
    let cpu = plugin.cpu_filter.clone().alias("gpu");
    let graph = registry
        .graph_builder()
        .unwrap()
        .inputs(|g| {
            g.input("in");
        })
        .outputs(|g| {
            g.output("out");
        })
        .nodes(|g| {
            g.add_handle(&pre);
            g.add_handle(&cpu);
        })
        .try_edges(|g| {
            let p = g.node("pre");
            let n = g.node("gpu");
            g.try_connect("in", &p.input("in"))?;
            g.try_connect(&p.output("out"), &n.input("frame"))?;
            g.try_connect(&n.output("frame"), "out")?;
            Ok(())
        })
        .unwrap()
        .build();
    let engine = Engine::new(EngineConfig::default()).unwrap();
    let mut runtime = engine.compile_registry(&registry, graph).unwrap();
    runtime.push("in", 7_i64);
    runtime.tick().unwrap();
    assert_eq!(
        runtime.drain_owned::<Cpu<i64>>("out").unwrap(),
        vec![Cpu::new(7_i64)]
    );
}

#[test]
fn gpu_node_with_fallback_runs_on_cpu_backend() {
    let mut registry = PluginRegistry::new();
    let plugin = ReproPlugin::new();
    registry.install(&plugin).unwrap();
    let pre = plugin.pre.clone().alias("pre");
    let gpu = plugin.gpu_filter.clone().alias("gpu");
    let graph = registry
        .graph_builder()
        .unwrap()
        .inputs(|g| {
            g.input("in");
        })
        .outputs(|g| {
            g.output("out");
        })
        .nodes(|g| {
            g.add_handle(&pre);
            g.add_handle(&gpu);
        })
        .try_edges(|g| {
            let p = g.node("pre");
            let n = g.node("gpu");
            g.try_connect("in", &p.input("in"))?;
            g.try_connect(&p.output("out"), &n.input("frame"))?;
            g.try_connect(&n.output("frame"), "out")?;
            Ok(())
        })
        .unwrap()
        .build();
    let engine = Engine::new(EngineConfig::default()).unwrap();
    let mut runtime = engine.compile_registry(&registry, graph).unwrap();
    runtime.push("in", 7_i64);
    runtime.tick().unwrap();
    assert_eq!(
        runtime.drain_owned::<Cpu<i64>>("out").unwrap(),
        vec![Cpu::new(7_i64)]
    );
}
