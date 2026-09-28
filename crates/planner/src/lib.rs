//! Deterministic graph planning for Daedalus.
//!
//! This crate validates graph structure, hydrates registry declarations, resolves
//! type and transport requirements, annotates GPU segments, schedules execution,
//! and emits planner diagnostics plus runtime-plan input data.

pub mod debug;
mod diagnostics;
mod document;
mod graph;
pub mod helpers;
mod metadata;
mod passes;
mod patch;

daedalus_core::build_facts!();

pub use diagnostics::{
    Diagnostic, DiagnosticCode, DiagnosticSpan, DiagnosticsBundle, MissingGroup, MissingNode,
    MissingPort, TypeMismatch, bundle,
};
pub use document::{
    GRAPH_DOCUMENT_FORMAT, GRAPH_DOCUMENT_SCHEMA_VERSION, GraphDocument, GraphDocumentError,
    MissingPlugins, PluginRequirement, UnmetReason, UnmetRequirement, check_plugin_requirements,
};
pub use graph::{
    ComputeAffinity, DEFAULT_PLAN_VERSION, Edge, EdgeBufferInfo, ExecutionPlan, GpuSegment, Graph,
    NodeInstance, NodeRef, PortRef, StableHash,
};
pub use metadata::{
    DYNAMIC_INPUT_LABELS_KEY, DYNAMIC_INPUT_TYPES_KEY, DYNAMIC_OUTPUT_LABELS_KEY,
    DYNAMIC_OUTPUT_TYPES_KEY, DynamicPortMetadata, EMBEDDED_GROUP_KEY, GROUP_ID_KEY,
    GROUP_LABEL_KEY, GroupMetadata, HOST_BRIDGE_META_KEY, HOST_INPUT_TYPES_KEY,
    HOST_OUTPUT_TYPES_KEY, HostPortTypes, descriptor_dynamic_port_type, descriptor_metadata_string,
    descriptor_metadata_value, host_bridge_metadata, is_generic_marker, is_host_bridge_metadata,
    metadata_string,
};
pub use passes::{
    AdapterResolutionMode, AppliedPlannerLowering, EdgeResolutionExplanation, EdgeResolutionKind,
    NodeOverloadResolution, OverloadPortResolution, PlanExplanation, PlannerConfig, PlannerInput,
    PlannerLoweringContext, PlannerLoweringInfo, PlannerLoweringPhase, PlannerLoweringRegistry,
    PlannerOutput, build_plan, edge_explanations, explain_plan, register_planner_lowering,
    registered_planner_lowerings,
};
pub use patch::{GraphMetadataSelector, GraphNodeSelector, GraphPatch, GraphPatchOp, PatchReport};
