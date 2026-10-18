//! Held host inputs and batched pushes.
//!
//! `imu` is a context input declared `held_input`: its last value stays in the graph and reaches
//! every tick until a push replaces it (or `clear_input` drops it). Held pushes never trigger a
//! tick by themselves, so context can arrive at its own rate. A batch pushes a frame and its
//! context together, so one tick sees both or neither and a frame never pairs with stale context.

use daedalus::{
    engine::{Engine, EngineConfig, MetricsLevel},
    macros::{node, plugin},
    runtime::{NodeError, plugins::PluginRegistry},
};

/// Combines a frame with the held context: `frame * 1000 + imu`.
#[node(id = "held.fuse", inputs("frame", "imu"), outputs("out"))]
fn fuse(frame: i64, imu: i64) -> Result<i64, NodeError> {
    Ok(frame * 1000 + imu)
}

#[plugin(id = "example.held_inputs", nodes(fuse))]
struct HeldPlugin;

fn main() -> Result<(), Box<dyn std::error::Error>> {
    let mut registry = PluginRegistry::new();
    let plugin = HeldPlugin::new();
    registry.install(&plugin)?;
    let fuse = plugin.fuse.alias("fuse");
    let graph = registry
        .graph_builder()?
        .input_typed::<i64>("frame")?
        .input_typed::<i64>("imu")?
        .held_input("imu")
        .try_node(&fuse)?
        .try_connect("frame", &fuse.inputs.frame)?
        .try_connect("imu", &fuse.inputs.imu)?
        .try_connect(&fuse.outputs.out, "out")?
        .build();
    let engine = Engine::new(EngineConfig::default().with_metrics_level(MetricsLevel::Basic))?;
    let mut host = engine.compile_registry(&registry, graph)?;

    // One context push serves every following frame tick without being pushed again.
    host.push("imu", 7_i64);
    for frame in 1..=3_i64 {
        host.push("frame", frame);
        host.tick()?;
        println!(
            "frame {frame} (held imu=7): out={:?}",
            host.take::<i64>("out")
        );
    }

    // A replacement context is seen from the next tick on.
    host.push("imu", 8_i64);
    host.push("frame", 4_i64);
    host.tick()?;
    println!("frame 4 (imu replaced): out={:?}", host.take::<i64>("out"));

    // A batch lands whole: the frame and the context it belongs to are in the same tick.
    let outcomes = host
        .batch()
        .push("imu", 9_i64)
        .push("frame", 5_i64)
        .commit()?;
    println!("batch accepted {} payloads", outcomes.len());
    host.tick()?;
    println!("frame 5 (batched imu=9): out={:?}", host.take::<i64>("out"));

    // Clearing the context leaves the required input missing, so the node does not run.
    host.clear_input("imu");
    host.push("frame", 6_i64);
    let telemetry = host.tick()?;
    println!(
        "frame 6 (imu cleared): nodes run={} out={:?}",
        telemetry.nodes_executed,
        host.take::<i64>("out")
    );
    Ok(())
}
