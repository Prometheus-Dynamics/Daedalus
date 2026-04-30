//! Planner passes and execution plan model scaffolding. See `PLAN.md` for staged tasks.
//! Exposes a deterministic pass pipeline from registry-sourced graphs to an `ExecutionPlan`.
//!
//! Pass order (stubs today, contract documented):
//! hydrate_registry -> typecheck -> convert -> align -> gpu -> schedule -> lint.

pub mod debug;
mod diagnostics;
mod graph;
pub mod helpers;
mod metadata;
mod passes;
mod patch;

pub use diagnostics::{
    Diagnostic, DiagnosticCode, DiagnosticSpan, DiagnosticsBundle, MissingGroup, MissingNode,
    MissingPort, TypeMismatch, bundle,
};
pub use graph::{
    ComputeAffinity, DEFAULT_PLAN_VERSION, Edge, EdgeBufferInfo, ExecutionPlan, GpuSegment, Graph,
    NodeInstance, NodeRef, PortRef, StableHash,
};
pub use metadata::{
    DYNAMIC_INPUT_LABELS_KEY, DYNAMIC_INPUT_TYPES_KEY, DYNAMIC_OUTPUT_LABELS_KEY,
    DYNAMIC_OUTPUT_TYPES_KEY, DynamicPortMetadata, EMBEDDED_GROUP_KEY, GROUP_ID_KEY,
    GROUP_LABEL_KEY, GroupMetadata, HOST_BRIDGE_META_KEY, descriptor_dynamic_port_type,
    descriptor_metadata_string, descriptor_metadata_value, is_host_bridge_metadata,
    metadata_string,
};
pub use passes::{
    AdapterResolutionMode, AppliedPlannerLowering, EdgeResolutionExplanation, EdgeResolutionKind,
    NodeOverloadResolution, OverloadPortResolution, PlanExplanation, PlannerConfig, PlannerInput,
    PlannerLoweringContext, PlannerLoweringInfo, PlannerLoweringPhase, PlannerLoweringRegistry,
    PlannerOutput, build_plan, explain_plan, register_planner_lowering,
    registered_planner_lowerings,
};
pub use patch::{GraphMetadataSelector, GraphNodeSelector, GraphPatch, GraphPatchOp, PatchReport};
