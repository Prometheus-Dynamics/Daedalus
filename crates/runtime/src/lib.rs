//! Runtime execution layer for planner-produced graphs.
//!
//! This crate owns runtime plans, executor paths, handler dispatch, host bridge
//! queues, streaming workers, state/resources, transport execution, and
//! telemetry.
//!
//! `no_std` + `alloc` without the `std` feature (implied by the default `threads`): the serial
//! executor, host bridge (push, poll, await) and stream polling (see "Portability" in
//! docs/development.md).
#![cfg_attr(not(any(feature = "std", test)), no_std)]

#[cfg_attr(not(feature = "std"), macro_use)]
extern crate alloc;
#[cfg(all(test, not(feature = "std")))]
extern crate std;

#[cfg(feature = "alloc-probe")]
pub mod alloc_probe;

#[cfg(all(
    feature = "threads",
    target_family = "wasm",
    any(target_os = "unknown", not(target_feature = "atomics"))
))]
compile_error!(
    "daedalus-runtime: this target cannot spawn threads; disable the `threads` feature \
     (`default-features = false`)"
);

mod portable;
mod prelude;
mod trace;

/// The hash maps in the runtime's API: `std`'s with `std`, `hashbrown`'s without.
pub mod collections {
    #[cfg(not(feature = "std"))]
    pub use hashbrown::{HashMap, HashSet, hash_map};
    #[cfg(feature = "std")]
    pub use std::collections::{HashMap, HashSet, hash_map};

    /// A map with a fast, unseeded hasher ([`FxHasher`]), for hot-path maps keyed by ids the
    /// runtime itself assigns (stable node ids, port names). Not DoS-resistant; deterministic,
    /// so a map built by a separately compiled plugin hashes like the host's.
    pub type FastHashMap<K, V> = hashbrown::HashMap<K, V, core::hash::BuildHasherDefault<FxHasher>>;

    /// The Fx hash (rotate, xor, multiply per word), as rustc uses for its own maps.
    #[derive(Clone, Copy, Default)]
    pub struct FxHasher {
        hash: u64,
    }

    impl FxHasher {
        const SEED: u64 = 0x51_7c_c1_b7_27_22_0a_95;

        #[inline]
        fn add(&mut self, word: u64) {
            self.hash = (self.hash.rotate_left(5) ^ word).wrapping_mul(Self::SEED);
        }
    }

    impl core::hash::Hasher for FxHasher {
        #[inline]
        fn write(&mut self, bytes: &[u8]) {
            let (words, rest) = bytes.as_chunks::<8>();
            for word in words {
                self.add(u64::from_le_bytes(*word));
            }
            if !rest.is_empty() {
                let mut word = [0u8; 8];
                word[..rest.len()].copy_from_slice(rest);
                self.add(u64::from_le_bytes(word));
            }
        }

        #[inline]
        fn write_u8(&mut self, value: u8) {
            self.add(u64::from(value));
        }

        #[inline]
        fn write_u64(&mut self, value: u64) {
            self.add(value);
        }

        #[inline]
        fn write_u128(&mut self, value: u128) {
            self.add(value as u64);
            self.add((value >> 64) as u64);
        }

        #[inline]
        fn write_usize(&mut self, value: usize) {
            self.add(value as u64);
        }

        #[inline]
        fn finish(&self) -> u64 {
            self.hash
        }
    }
}

pub mod capabilities;
pub mod config;
pub mod const_cache;
pub mod const_coerce;
pub mod debug;
pub mod executor;
pub mod fanin;
pub mod foreign;
pub mod graph_builder;
pub mod handler_registry;
pub mod handles;
pub mod host_bridge;
pub mod io;
mod perf;
mod plan;
#[cfg(feature = "plugins")]
pub mod plugins;
mod scheduler;
pub mod snapshot;
pub mod state;
mod state_error;
pub mod stream;
pub mod sync;
pub mod transport;
pub mod type_index;
pub use daedalus_transport as transport_types;

use alloc::string::{String, ToString};
use alloc::vec::Vec;

daedalus_core::build_facts!();

/// Apply a plugin prefix to a node id without duplicating overlapping segments.
///
/// Prefixes already present at the start of the id are not duplicated.
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

    let max_overlap = core::cmp::min(prefix_parts.len(), id_parts.len());
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
pub use daedalus_core::metadata::{EMBEDDED_GRAPH_KEY, EMBEDDED_HOST_KEY, NODE_OVERLOADS_KEY};
pub use executor::{
    AdapterPathReport, CustomMetricValue, DataLifecycleEvent, DataLifecycleStage, DirectPayloadFn,
    EdgeAdapterClass, EdgeOverheadStats, EdgePressureMetrics, EdgePressureReason, EdgeTickSample,
    ExecuteError, ExecutionTelemetry, Executor, ExecutorMaskError, FfiAdapterTelemetry,
    FfiBackendTelemetry, FfiPackageTelemetry, FfiPayloadTelemetry, FfiTelemetryReport,
    FfiWorkerTelemetry, FrameOverheadReport, FrameOverheadWindow, FrameProbe, FrameStat,
    FrameTickSample, InternalTransferMetrics, MetricsLevel, NodeAllocationSpikeExplanation,
    NodeError, NodeHandler, NodeMetrics, NodeMetricsMap, NodeResourceMetrics, OwnedExecutor,
    OwnershipReport, ProfileLevel, Profiler, ResourceMetrics, TelemetryReport,
    TelemetryReportFilter, estimate_payload_bytes, register_runtime_data_size_inspector,
};
pub use fanin::FanIn;
pub use handles::{
    CapabilityId, FeatureFlag, HostAlias, NodeAlias, NodeHandle, NodeHandleId, PortHandle, PortId,
};
#[cfg(all(feature = "std", target_os = "linux"))]
pub use host_bridge::InboundFd;
pub use host_bridge::{
    DEFAULT_HOST_BRIDGE_EVENT_LIMIT, DEFAULT_HOST_BRIDGE_EVENT_RECORDING, HOST_BRIDGE_META_KEY,
    HOST_HELD_INPUTS_KEY, HostBatchOutcomes, HostBatchRejected, HostBridgeConfig, HostBridgeHandle,
    HostBridgeManager, HostInputBatch, HostIoTime, HostPortStats, InboundWait, InboundWaiter,
    PayloadInspection, PayloadSummary, bridge_handler, inspect_payload,
};
pub use io::{
    DEFAULT_OUTPUT_PORT, NodeIo, NodePort, TypedInputResolution, TypedInputResolutionKind,
};
pub use plan::{
    BackpressureStrategy, DemandError, DemandSlice, DemandSliceEntry, DemandTelemetry, FusionBlock,
    HostPortConnection, HostPortDescriptor, HostPortDirection, NODE_COST_META_KEY,
    NODE_EXECUTION_KIND_META_KEY, NODE_FIRE_META_KEY, NODE_FUSION_META_KEY,
    NODE_REQUIRED_INPUTS_META_KEY, NODE_SHAREABLE_META_KEY, NodeExecutionKind, NodeFire,
    RuntimeBranchExplanation, RuntimeEdge, RuntimeEdgeExplanation, RuntimeEdgeHandoff,
    RuntimeEdgePolicy, RuntimeEdgeTransport, RuntimeFusedUnitExplanation, RuntimeNode,
    RuntimeNodeExplanation, RuntimePlan, RuntimePlanError, RuntimePlanExplanation, RuntimeSegment,
    RuntimeSink, node_instance_keys,
};
pub use scheduler::{SchedulerConfig, build_runtime};
pub use state::{
    ExecutionContext, ManagedByteBuffer, ManagedResource, NodeResourceSnapshot, ResourceClass,
    ResourceLifecycleEvent, ResourceUsage, RuntimeResources, StateStore,
};
pub use state_error::StateError;
pub use stream::{
    DEFAULT_STREAM_IDLE_SLEEP, GraphInput, GraphOutput, InputStats, OutputStats,
    OutputSubscription, SharedStreamGraph, StreamExecutionMode, StreamGraph,
    StreamGraphDiagnostics, StreamGraphState, StreamTelemetrySummary, StreamWorkerConfig,
    StreamWorkerState,
};
#[cfg(feature = "threads")]
pub use stream::{StreamGraphWorker, StreamWorkerDiagnostics, StreamWorkerStopError};
pub use transport::RuntimeTransport;
pub use type_index::{TypeIndex, TypeKeyUses};
