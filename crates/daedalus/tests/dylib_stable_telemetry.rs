//! Custom metrics a stable plugin node records reach the host node's telemetry, and the plugin
//! context carries the graph's metadata (`examples/plugins/stable_abi`'s `metrics_probe`): a
//! counter per call and the `title` metadata as a text metric, read back from each tick's
//! telemetry.
// musl executables are static and cannot dlopen (see `dylib::library`): no loader there.
#![cfg(all(unix, not(target_env = "musl")))]

#[path = "support/cdylib.rs"]
mod cdylib;
#[path = "support/frames.rs"]
mod frames;

use std::sync::OnceLock;

use daedalus::PluginLibrary;
use daedalus::data::model::{TypeExpr, ValueType};
use daedalus::dylib::InstallPath;
use daedalus::engine::{Engine, EngineConfig, MetricsLevel};
use daedalus::runtime::executor::CustomMetricValue;
use daedalus::runtime::plugins::{PluginRegistry, RegistryPluginExt};
use daedalus_plugins_stable_abi::StableAbiPlugin;
use frames::FramesHost;

fn library() -> &'static PluginLibrary {
    static LIBRARY: OnceLock<PluginLibrary> = OnceLock::new();
    // Safety: a trusted test plugin.
    LIBRARY.get_or_init(|| unsafe { PluginLibrary::load(cdylib::stable_abi_cdylib()) }.unwrap())
}

/// Two ticks: each tick's custom metrics of every node, and the node's result.
/// One tick: the input it ran, the node's custom metrics in that tick's telemetry, and the result.
type Tick = (i64, Vec<(String, CustomMetricValue)>, Option<i64>);

fn run(registry: &PluginRegistry) -> Vec<Tick> {
    let builder = registry
        .graph_builder()
        .unwrap()
        .graph_metadata("title", "telemetry demo")
        .input_as("x", TypeExpr::Scalar(ValueType::Int))
        .node_id("stable_abi:metrics_probe", "probe")
        .connect("x", "probe.x")
        .connect("probe.out", "out");
    // Custom metrics are collected at the basic level.
    let config = EngineConfig::default().with_metrics_level(MetricsLevel::Basic);
    let mut host = Engine::new(config)
        .unwrap()
        .compile_registry(registry, builder.build())
        .unwrap();
    (1..=2_i64)
        .map(|tick| {
            host.push("x", tick);
            let telemetry = host.tick().unwrap();
            let custom = telemetry
                .node_metrics
                .values()
                .flat_map(|metrics| metrics.custom.iter())
                .map(|(name, value)| (name.clone(), value.clone()))
                .collect();
            (tick, custom, host.take::<i64>("out"))
        })
        .collect()
}

#[test]
fn a_stable_nodes_custom_metrics_reach_the_host_telemetry_with_the_graph_metadata() {
    let mut statics = PluginRegistry::new();
    statics.install_plugin(&FramesHost::new()).unwrap();
    statics.install_plugin(&StableAbiPlugin::new()).unwrap();
    let mut stables = PluginRegistry::new();
    stables.install_plugin(&FramesHost::new()).unwrap();
    assert_eq!(
        library().install_into(&mut stables).unwrap(),
        InstallPath::Stable
    );
    let (want, got) = (run(&statics), run(&stables));
    let calls = |runs: &[Tick]| {
        runs.iter()
            .map(|(_, custom, _)| {
                custom
                    .iter()
                    .find(|(name, _)| name == "probe_calls")
                    .map(|(_, v)| v.clone())
            })
            .collect::<Vec<_>>()
    };
    assert_eq!(
        calls(&want),
        vec![Some(CustomMetricValue::Counter(1)); 2],
        "static"
    );
    assert_eq!(calls(&got), calls(&want), "stable matches static");
    assert_eq!(
        want.iter()
            .map(|(_, custom, out)| (custom.clone(), *out))
            .collect::<Vec<_>>(),
        got.iter()
            .map(|(_, custom, out)| (custom.clone(), *out))
            .collect::<Vec<_>>(),
        "every metric and result matches static"
    );
}
