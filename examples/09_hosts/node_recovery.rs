//! Node recovery: what a failing node does to the run.
//!
//! The default `NodeRecovery::Fail` fails the run on a node error. `SkipFrame` drops that node's
//! outputs for the tick and keeps running. `Retry` runs the handler again on the same inputs,
//! and fails the run only once its attempts are spent. The node below fails a set number of
//! times, so each scenario shows one policy's effect. Recovery counts appear in the telemetry.

use std::sync::atomic::{AtomicU32, Ordering};

use daedalus::{
    engine::{Engine, EngineConfig, MetricsLevel},
    macros::{node, plugin},
    runtime::{NodeError, NodeRecovery, plugins::PluginRegistry},
};

/// Failures the next calls of `flaky` must still raise.
static FAILURES_LEFT: AtomicU32 = AtomicU32::new(0);

/// The input is borrowed: a retry re-reads the same value (a by-value input is taken on the first
/// attempt).
#[node(id = "recovery.flaky", inputs("value"), outputs("value"))]
fn flaky(value: &i64) -> Result<i64, NodeError> {
    let left = FAILURES_LEFT.load(Ordering::SeqCst);
    if left > 0 {
        FAILURES_LEFT.store(left - 1, Ordering::SeqCst);
        return Err(NodeError::Handler("flaky node failed".into()));
    }
    Ok(value * 10)
}

#[plugin(id = "example.node_recovery", nodes(flaky))]
struct RecoveryPlugin;

/// Runs one tick of `flaky` under `policy`, after `failures` failing calls.
fn scenario(
    label: &str,
    policy: NodeRecovery,
    failures: u32,
) -> Result<(), Box<dyn std::error::Error>> {
    let mut registry = PluginRegistry::new();
    let plugin = RecoveryPlugin::new();
    registry.install(&plugin)?;
    let flaky = plugin.flaky.alias("flaky");
    let graph = registry
        .graph_builder()?
        .try_single_node_io(&flaky, "in", "value", "value", "out")?
        .build();
    let engine = Engine::new(
        EngineConfig::default()
            .with_metrics_level(MetricsLevel::Basic)
            .with_recovery(policy),
    )?;
    let mut host = engine.compile_registry(&registry, graph)?;

    FAILURES_LEFT.store(failures, Ordering::SeqCst);
    host.push("in", 4_i64);
    match host.tick_until_idle() {
        Ok(telemetry) => {
            let recovery = telemetry.as_ref().map(|t| t.recovery()).unwrap_or_default();
            println!(
                "{label}: ran, out={:?} retries={} skipped_frames={}",
                host.take::<i64>("out"),
                recovery.retries,
                recovery.skipped_frames
            );
        }
        Err(err) => println!("{label}: run failed: {err}"),
    }
    Ok(())
}

fn main() -> Result<(), Box<dyn std::error::Error>> {
    scenario("fail (default), one failure", NodeRecovery::Fail, 1)?;
    scenario("skip_frame, one failure", NodeRecovery::SkipFrame, 1)?;
    scenario("retry(2), two failures", NodeRecovery::retry(2), 2)?;
    scenario("retry(1), two failures", NodeRecovery::retry(1), 2)?;
    Ok(())
}
