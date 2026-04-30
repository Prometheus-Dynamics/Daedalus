use daedalus_data::model::TypeExpr;
use daedalus_registry::capability::AdapterPathStep;

use super::{AdapterResolutionMode, EdgeResolutionKind, typeexpr_transport_key};

#[derive(Clone, Debug)]
pub(super) struct ResolvedEdgeCompatibility {
    pub(super) resolution_kind: EdgeResolutionKind,
    pub(super) adapter_mode: AdapterResolutionMode,
    pub(super) total_cost: u64,
    pub(super) transport_target: Option<daedalus_transport::TypeKey>,
    pub(super) converter_steps: Vec<String>,
    pub(super) adapter_path: Vec<AdapterPathStep>,
}

impl ResolvedEdgeCompatibility {
    pub(super) fn uses_adapter(&self) -> bool {
        !self.converter_steps.is_empty()
    }
}

fn adapter_mode_from_adapter_path(path: &[AdapterPathStep]) -> AdapterResolutionMode {
    if path.is_empty() {
        return AdapterResolutionMode::Exact;
    }
    let mut saw_view = false;
    let mut saw_materialize = false;
    let mut saw_convert = false;
    for step in path {
        match step.kind {
            daedalus_transport::AdaptKind::Identity => {}
            daedalus_transport::AdaptKind::View
            | daedalus_transport::AdaptKind::SharedView
            | daedalus_transport::AdaptKind::CowView
            | daedalus_transport::AdaptKind::MetadataOnly => saw_view = true,
            daedalus_transport::AdaptKind::Materialize
            | daedalus_transport::AdaptKind::Cow
            | daedalus_transport::AdaptKind::Branch => saw_materialize = true,
            daedalus_transport::AdaptKind::Reinterpret
            | daedalus_transport::AdaptKind::MutateInPlace
            | daedalus_transport::AdaptKind::DeviceTransfer
            | daedalus_transport::AdaptKind::DeviceUpload
            | daedalus_transport::AdaptKind::DeviceDownload
            | daedalus_transport::AdaptKind::Serialize
            | daedalus_transport::AdaptKind::Deserialize
            | daedalus_transport::AdaptKind::Custom => saw_convert = true,
        }
    }
    match (saw_view, saw_materialize, saw_convert) {
        (true, false, false) => AdapterResolutionMode::View,
        (false, true, false) => AdapterResolutionMode::Materialize,
        (false, false, true) => AdapterResolutionMode::Convert,
        (false, false, false) => AdapterResolutionMode::Exact,
        _ => AdapterResolutionMode::Mixed,
    }
}

pub(super) fn resolve_edge_adapter_request(
    transport_capabilities: Option<&daedalus_registry::capability::CapabilityRegistry>,
    from: &TypeExpr,
    to: &TypeExpr,
    request: daedalus_transport::AdaptRequest,
    active_features: &[String],
    allow_gpu: bool,
) -> Option<ResolvedEdgeCompatibility> {
    if from == to && !request.exclusive && request.residency.is_none() && request.layout.is_none() {
        return Some(ResolvedEdgeCompatibility {
            resolution_kind: EdgeResolutionKind::Exact,
            adapter_mode: AdapterResolutionMode::Exact,
            total_cost: 0,
            transport_target: None,
            converter_steps: Vec::new(),
            adapter_path: Vec::new(),
        });
    }

    let requested_target = request.target.clone();
    let adapter_resolution = transport_capabilities.and_then(|capabilities| {
        capabilities
            .resolve_adapter_path_for_with_context(
                &typeexpr_transport_key(from),
                &request,
                active_features,
                allow_gpu,
            )
            .ok()
    });

    if let Some(resolution) = adapter_resolution {
        return Some(ResolvedEdgeCompatibility {
            resolution_kind: EdgeResolutionKind::Conversion,
            adapter_mode: adapter_mode_from_adapter_path(&resolution.step_details),
            total_cost: resolution.total_cost,
            transport_target: resolution
                .resolved_target
                .filter(|target| target != &requested_target),
            converter_steps: resolution
                .steps
                .iter()
                .map(|step| step.to_string())
                .collect(),
            adapter_path: resolution.step_details,
        });
    }

    None
}
