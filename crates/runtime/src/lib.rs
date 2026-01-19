//! Runtime orchestration scaffolding. See `PLAN.md` for the detailed roadmap.
//! Transforms planner `ExecutionPlan` into a runnable `RuntimePlan` with edge policies and schedulable segments.

pub mod capabilities;
pub mod config;
mod convert;
pub mod debug;
pub mod executor;
pub mod fanin;
pub mod graph_builder;
pub mod handler_registry;
pub mod handles;
pub mod host_bridge;
pub mod io;
mod plan;
#[cfg(feature = "plugins")]
pub mod plugins;
mod scheduler;
pub mod snapshot;
pub mod state;

/// Apply a plugin prefix to a node id without duplicating overlapping segments.
///
/// Examples:
/// - prefix `ai`, id `ai:run` => `ai:run`
/// - prefix `cv:aruco`, id `cv:decode_grid` => `cv:aruco:decode_grid`
/// - prefix `cv`, id `aruco:decode_grid` => `cv:aruco:decode_grid`
///
/// ```
/// use daedalus_runtime::apply_node_prefix;
/// assert_eq!(apply_node_prefix("ai", "ai:run"), "ai:run");
/// assert_eq!(apply_node_prefix("cv", "aruco:decode_grid"), "cv:aruco:decode_grid");
/// ```
pub fn apply_node_prefix(prefix: &str, id: &str) -> String {
    let prefix = prefix.trim_matches(':').trim();
    let id = id.trim_matches(':').trim();

    if prefix.is_empty() {
        return id.to_string();
    }
    if id.is_empty() {
        return prefix.to_string();
    }

    let prefix_parts: Vec<&str> = prefix
        .split(':')
        .map(str::trim)
        .filter(|p| !p.is_empty())
        .collect();
    let id_parts: Vec<&str> = id
        .split(':')
        .map(str::trim)
        .filter(|p| !p.is_empty())
        .collect();

    if prefix_parts.is_empty() {
        return id.to_string();
    }
    if id_parts.is_empty() {
        return prefix.to_string();
    }

    let max_overlap = std::cmp::min(prefix_parts.len(), id_parts.len());
    let mut overlap = 0usize;
    while overlap < max_overlap && prefix_parts[overlap] == id_parts[overlap] {
        overlap += 1;
    }

    if overlap == prefix_parts.len() {
        // Already fully prefixed.
        return id.to_string();
    }

    let mut out: Vec<&str> =
        Vec::with_capacity(prefix_parts.len() + id_parts.len().saturating_sub(overlap));
    out.extend_from_slice(&prefix_parts);
    out.extend_from_slice(&id_parts[overlap..]);
    out.join(":")
}

pub use config::*;
pub use convert::{ConversionRegistry, convert_arc};
pub use executor::{
    EdgePayload, ExecuteError, ExecutionTelemetry, Executor, NodeError, NodeHandler, NodeMetrics,
};
pub use fanin::FanIn;
pub use handles::{NodeHandle, PortHandle};
pub use host_bridge::{
    HOST_BRIDGE_META_KEY, HostBridgeHandle, HostBridgeManager, HostBridgeSerialized,
    HostBridgeSerializedPayload, bridge_handler,
};
pub use io::{NodeIo, register_output_mover};
pub use plan::{BackpressureStrategy, EdgePolicyKind, RuntimeNode, RuntimePlan, RuntimeSegment};
pub use scheduler::{SchedulerConfig, build_runtime};
