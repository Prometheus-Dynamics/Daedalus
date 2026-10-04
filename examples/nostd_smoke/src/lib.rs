//! `no_std` + `alloc` smoke test of the serial runtime and engine (`scripts/ci.sh nostd`):
//! checked for `thumbv7em-none-eabihf`, and its tests run the same code natively with `std` off.
//!
//! Both entry points run `host.in -> inc -> host.out` once, timed by an injected counter
//! [`Clock`] (there is no OS clock): [`runtime_increment`] plans the graph, feeds the host
//! bridge, runs the serial executor in place and pops the output; [`engine_increment`] does the
//! same through a plugin registry, `Engine` and `HostGraph`. Each returns the output and its
//! lineage age on the clock, which only advances if the runtime stamped the payload with it.
#![no_std]

extern crate alloc;
#[cfg(test)]
extern crate std;

use core::sync::atomic::{AtomicU32, Ordering};
use core::time::Duration;

use daedalus_core::platform::Clock;
use daedalus_data::model::{TypeExpr, Value, ValueType};
use daedalus_engine::{Engine, EngineConfig};
use daedalus_planner::{Edge, ExecutionPlan, Graph, NodeInstance};
use daedalus_registry::capability::{NodeDecl, PortDecl};
use daedalus_runtime::host_bridge::{HOST_BRIDGE_ID, HOST_BRIDGE_META_KEY, HostBridgeManager};
use daedalus_runtime::io::NodeIo;
use daedalus_runtime::plugins::PluginRegistry;
use daedalus_runtime::state::ExecutionContext;
use daedalus_runtime::{Executor, NodeError, RuntimeNode, SchedulerConfig, build_runtime};

/// The builtin key of `i64` (`TypeExpr::Scalar(Int)`).
const INT: &str = "typeexpr:{\"Scalar\":\"Int\"}";

/// Stands in for a target timer: advances 1 ms per reading.
fn counter_clock() -> Clock {
    static MILLIS: AtomicU32 = AtomicU32::new(0);
    Clock::new(|| Duration::from_millis(MILLIS.fetch_add(1, Ordering::Relaxed).into()))
}

fn graph() -> Graph {
    let host = NodeInstance::new(HOST_BRIDGE_ID)
        .with_label("host")
        .with_inputs(["out"])
        .with_outputs(["in"])
        .with_metadata(HOST_BRIDGE_META_KEY, Value::Bool(true));
    let inc = NodeInstance::new("inc")
        .with_inputs(["in"])
        .with_outputs(["out"]);
    Graph {
        nodes: alloc::vec![host, inc],
        edges: alloc::vec![Edge::new(0, "in", 1, "in"), Edge::new(1, "out", 0, "out")],
        metadata: Default::default(),
    }
}

fn increment(node: &RuntimeNode, _: &ExecutionContext, io: &mut NodeIo) -> Result<(), NodeError> {
    if node.id == "inc" {
        let x = *io
            .get_typed_ref::<i64>("in")
            .ok_or_else(|| NodeError::InvalidInput("expected i64".into()))?;
        io.push_as_to("out", INT.into(), x + 1);
    }
    Ok(())
}

/// Runtime only: plan, host bridge push, serial `run_in_place`, pop.
pub fn runtime_increment(x: i64) -> Option<(i64, Duration)> {
    let clock = counter_clock();
    let plan = build_runtime(
        &ExecutionPlan::new(graph(), alloc::vec![]),
        &SchedulerConfig::default(),
    );
    let bridges = HostBridgeManager::new();
    bridges.populate_from_plan(&plan);
    bridges.set_clock(clock.clone());
    let host = bridges.ensure_handle("host");
    host.push_as("in", INT, x);
    Executor::try_new(&plan, increment)
        .ok()?
        .with_clock(clock.clone())
        .with_host_bridges(bridges)
        .run_in_place()
        .ok()?;
    let output = host.try_pop_payload("out")?;
    Some((*output.get_ref::<i64>()?, output.lineage().age(&clock)))
}

/// Through the engine: declare the nodes, compile a `HostGraph`, push, tick, take.
pub fn engine_increment(x: i64) -> Option<(i64, Duration)> {
    let clock = counter_clock();
    let port = |name: &str| PortDecl::new(name, INT).schema(TypeExpr::Scalar(ValueType::Int));
    let mut plugins = PluginRegistry::new();
    plugins
        .register_node_decl(
            NodeDecl::new(HOST_BRIDGE_ID)
                .metadata(HOST_BRIDGE_META_KEY, Value::Bool(true))
                .input(port("out"))
                .output(port("in")),
        )
        .ok()?;
    plugins
        .register_node_decl(NodeDecl::new("inc").input(port("in")).output(port("out")))
        .ok()?;
    let mut host = Engine::new(EngineConfig::default().with_clock(clock.clone()))
        .ok()?
        .compile_host_graph_plugin_registry(
            &plugins,
            graph(),
            increment,
            HostBridgeManager::new(),
            "host",
        )
        .ok()?;
    host.push("in", x);
    host.tick().ok()?;
    let output = host.take_payload("out")?;
    Some((*output.get_ref::<i64>()?, output.lineage().age(&clock)))
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn serial_runtime_runs_without_std() {
        let (output, age) = runtime_increment(41).expect("runtime run");
        assert_eq!(output, 42);
        assert!(
            age > Duration::ZERO,
            "lineage is stamped by the injected clock"
        );
    }

    #[test]
    fn engine_runs_without_std() {
        let (output, age) = engine_increment(1).expect("engine run");
        assert_eq!(output, 2);
        assert!(
            age > Duration::ZERO,
            "lineage is stamped by the injected clock"
        );
    }
}
