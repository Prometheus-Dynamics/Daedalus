use daedalus_data::model::{TypeExpr, Value};
use daedalus_registry::capability::{AdapterPathStep, CapabilityRegistry};
use std::collections::BTreeMap;
use std::fmt;
use std::str::FromStr;

use crate::diagnostics::Diagnostic;
use crate::graph::{ExecutionPlan, Graph, StableHash, stable_hash_serialized};

use super::lowerings::PlannerLoweringRegistry;

/// Static planner config controlling optional passes.
///
/// ```ignore
/// use daedalus_planner::PlannerConfig;
/// let cfg = PlannerConfig::default();
/// assert!(!cfg.enable_gpu);
/// ```
#[derive(Clone, Debug, Default)]
pub struct PlannerConfig {
    pub enable_gpu: bool,
    pub enable_lints: bool,
    pub active_features: Vec<String>,
    pub transport_capabilities: Option<CapabilityRegistry>,
    /// Scoped lowering registry used by this planning run.
    ///
    /// Defaults to the process-global registry for compatibility with plugin-style registration,
    /// but embedders can pass `PlannerLoweringRegistry::new()` or a prebuilt registry to isolate
    /// tenants/tests from global lowering state.
    pub lowerings: PlannerLoweringRegistry,
    /// When true, validate `GraphNode.inputs/outputs` strictly against the registry.
    ///
    /// This is intended for UI-persisted graphs where the node port lists are part of the
    /// persisted contract. It is deliberately off by default so "minimal" graphs (that omit
    /// declared ports and rely only on edges) remain valid.
    pub strict_port_declarations: bool,
    #[cfg(feature = "gpu")]
    pub gpu_caps: Option<daedalus_gpu::GpuCapabilities>,
}

impl PlannerConfig {
    pub fn stable_hash(&self) -> StableHash {
        #[derive(serde::Serialize)]
        struct PlannerConfigFingerprint<'a> {
            enable_gpu: bool,
            enable_lints: bool,
            active_features: &'a [String],
            transport_capabilities:
                Option<daedalus_registry::capability::CapabilityRegistrySnapshot>,
            lowerings: Vec<PlannerLoweringInfo>,
            strict_port_declarations: bool,
            #[cfg(feature = "gpu")]
            gpu_caps: &'a Option<daedalus_gpu::GpuCapabilities>,
        }

        let fingerprint = PlannerConfigFingerprint {
            enable_gpu: self.enable_gpu,
            enable_lints: self.enable_lints,
            active_features: &self.active_features,
            transport_capabilities: self
                .transport_capabilities
                .as_ref()
                .map(CapabilityRegistry::snapshot),
            lowerings: self.lowerings.registered(),
            strict_port_declarations: self.strict_port_declarations,
            #[cfg(feature = "gpu")]
            gpu_caps: &self.gpu_caps,
        };
        stable_hash_serialized("daedalus_planner::PlannerConfig", &fingerprint)
    }
}

/// Input to the planner.
///
/// ```ignore
/// use daedalus_planner::{PlannerInput, Graph};
/// let input = PlannerInput { graph: Graph::default() };
/// assert_eq!(input.graph.nodes.len(), 0);
/// ```
#[derive(Clone, Debug)]
pub struct PlannerInput {
    pub graph: Graph,
}

/// Planner output: final plan and any diagnostics.
///
/// ```ignore
/// use daedalus_planner::{PlannerOutput, ExecutionPlan, Graph};
/// let out = PlannerOutput { plan: ExecutionPlan::new(Graph::default(), vec![]), diagnostics: vec![] };
/// assert!(out.diagnostics.is_empty());
/// ```
#[derive(Clone, Debug)]
pub struct PlannerOutput {
    pub plan: ExecutionPlan,
    pub diagnostics: Vec<Diagnostic>,
}

#[derive(
    Clone, Copy, Debug, PartialEq, Eq, PartialOrd, Ord, serde::Serialize, serde::Deserialize,
)]
pub enum PlannerLoweringPhase {
    BeforeTypecheck,
    AfterConvert,
}

#[derive(Clone, Debug, PartialEq, Eq, serde::Serialize, serde::Deserialize)]
pub struct PlannerLoweringInfo {
    pub id: String,
    pub phase: PlannerLoweringPhase,
}

#[derive(Clone, Debug, PartialEq, serde::Serialize, serde::Deserialize)]
pub struct AppliedPlannerLowering {
    pub id: String,
    pub phase: PlannerLoweringPhase,
    pub summary: String,
    pub changed: bool,
    pub metadata: BTreeMap<String, Value>,
}

#[derive(Clone, Copy, Debug, PartialEq, Eq, serde::Serialize, serde::Deserialize)]
pub enum EdgeResolutionKind {
    Exact,
    Conversion,
    Missing,
}

impl EdgeResolutionKind {
    pub(super) fn as_str(self) -> &'static str {
        match self {
            EdgeResolutionKind::Exact => "exact",
            EdgeResolutionKind::Conversion => "conversion",
            EdgeResolutionKind::Missing => "missing",
        }
    }
}

impl fmt::Display for EdgeResolutionKind {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.write_str(self.as_str())
    }
}

impl FromStr for EdgeResolutionKind {
    type Err = ();

    fn from_str(value: &str) -> Result<Self, Self::Err> {
        match value {
            "exact" => Ok(EdgeResolutionKind::Exact),
            "conversion" => Ok(EdgeResolutionKind::Conversion),
            "missing" => Ok(EdgeResolutionKind::Missing),
            _ => Err(()),
        }
    }
}

#[derive(Clone, Copy, Debug, PartialEq, Eq, serde::Serialize, serde::Deserialize)]
pub enum AdapterResolutionMode {
    None,
    Exact,
    View,
    Materialize,
    Convert,
    Mixed,
}

impl AdapterResolutionMode {
    pub(super) fn as_str(self) -> &'static str {
        match self {
            AdapterResolutionMode::None => "none",
            AdapterResolutionMode::Exact => "exact",
            AdapterResolutionMode::View => "view",
            AdapterResolutionMode::Materialize => "materialize",
            AdapterResolutionMode::Convert => "convert",
            AdapterResolutionMode::Mixed => "mixed",
        }
    }
}

impl fmt::Display for AdapterResolutionMode {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.write_str(self.as_str())
    }
}

impl FromStr for AdapterResolutionMode {
    type Err = ();

    fn from_str(value: &str) -> Result<Self, Self::Err> {
        match value {
            "none" => Ok(AdapterResolutionMode::None),
            "exact" => Ok(AdapterResolutionMode::Exact),
            "view" => Ok(AdapterResolutionMode::View),
            "materialize" => Ok(AdapterResolutionMode::Materialize),
            "convert" => Ok(AdapterResolutionMode::Convert),
            "mixed" => Ok(AdapterResolutionMode::Mixed),
            _ => Err(()),
        }
    }
}

#[derive(Clone, Debug, PartialEq, Eq, serde::Serialize, serde::Deserialize)]
pub struct EdgeResolutionExplanation {
    pub from_node: String,
    pub from_port: String,
    pub to_node: String,
    pub to_port: String,
    pub from_type: TypeExpr,
    pub to_type: TypeExpr,
    pub target_access: daedalus_transport::AccessMode,
    pub target_exclusive: bool,
    pub target_residency: Option<daedalus_transport::Residency>,
    pub transport_target: Option<daedalus_transport::TypeKey>,
    pub resolution_kind: EdgeResolutionKind,
    pub adapter_mode: AdapterResolutionMode,
    pub total_cost: u64,
    pub converter_steps: Vec<String>,
    #[serde(default)]
    pub adapter_path: Vec<AdapterPathStep>,
}

#[derive(Clone, Debug, PartialEq, Eq, serde::Serialize, serde::Deserialize)]
pub struct OverloadPortResolution {
    pub port: String,
    pub from_node: String,
    pub from_port: String,
    pub from_type: TypeExpr,
    pub to_type: TypeExpr,
    pub resolution_kind: EdgeResolutionKind,
    pub adapter_mode: AdapterResolutionMode,
    pub total_cost: u64,
    pub converter_steps: Vec<String>,
}

#[derive(Clone, Debug, PartialEq, Eq, serde::Serialize, serde::Deserialize)]
pub struct NodeOverloadResolution {
    pub node: String,
    pub overload_id: String,
    pub overload_label: Option<String>,
    pub total_cost: u64,
    pub ports: Vec<OverloadPortResolution>,
}

#[derive(Clone, Debug, Default, PartialEq, serde::Serialize, serde::Deserialize)]
pub struct PlanExplanation {
    pub lowerings: Vec<AppliedPlannerLowering>,
    pub overloads: Vec<NodeOverloadResolution>,
    pub edges: Vec<EdgeResolutionExplanation>,
}
