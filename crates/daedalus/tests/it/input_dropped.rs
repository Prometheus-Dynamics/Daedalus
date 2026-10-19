//! A `#[node]` returning `NodeError::InputDropped` skips the tick without failing it, on the
//! scheduled tick and on the direct host fast path, and hosts can count it separately.

use daedalus::{
    engine::{Engine, EngineConfig, HostGraph},
    macros::{node, plugin},
    runtime::{NodeError, handler_registry::HandlerRegistry, plugins::PluginRegistry},
};

/// Drops negative frames (a stand-in for a camera frame that failed mid-write).
#[node(id = "test.input_dropped.check", inputs("frame"), outputs("out"))]
fn check(frame: i64) -> Result<i64, NodeError> {
    if frame < 0 {
        return Err(NodeError::InputDropped(format!(
            "frame {frame} failed mid-write"
        )));
    }
    Ok(frame * 2)
}

#[plugin(id = "test.input_dropped", nodes(check))]
struct DropPlugin;

fn compile() -> HostGraph<HandlerRegistry> {
    let mut registry = PluginRegistry::new();
    let plugin = DropPlugin::new();
    registry.install(&plugin).expect("install");
    let check = plugin.check.alias("check");
    let graph = registry
        .graph_builder()
        .expect("builder")
        .input_typed::<i64>("frame")
        .and_then(|b| b.try_node(&check))
        .and_then(|b| b.try_connect("frame", &check.inputs.frame))
        .and_then(|b| b.try_connect(&check.outputs.out, "out"))
        .expect("wire")
        .build();
    Engine::new(EngineConfig::default())
        .expect("engine")
        .compile_registry(&registry, graph)
        .expect("compile")
}

#[test]
fn a_dropped_input_skips_the_tick_and_is_counted_apart_from_failures() {
    let mut host = compile();
    host.push("frame", -1_i64);
    let telemetry = host.tick().expect("a dropped input does not fail the tick");
    assert!(telemetry.errors.is_empty());
    assert_eq!(telemetry.recovery().dropped_inputs, 1);
    assert_eq!(host.take::<i64>("out"), None);

    host.push("frame", 3_i64);
    host.tick().expect("tick");
    assert_eq!(host.take::<i64>("out"), Some(6));
}

#[test]
fn a_dropped_input_on_the_direct_host_path_returns_no_output() {
    let mut host = compile();
    assert_eq!(
        host.run_direct_once::<_, i64>("frame", "out", -1_i64)
            .expect("a dropped input does not fail the direct run"),
        None
    );
    assert_eq!(
        host.run_direct_once::<_, i64>("frame", "out", 4_i64)
            .expect("run"),
        Some(8)
    );
}
