//! Lowering of a typechecked graph into an [`ExecutionPlan`].

use daedalus_data::model::{TypeExpr, Value};
use daedalus_registry::typeexpr_transport_key;
use std::collections::HashMap;

use crate::diagnostics::{Diagnostic, DiagnosticCode};
use crate::graph::Graph;
use crate::metadata::PLAN_EDGE_EXPLANATIONS_KEY;

use super::{
    AdapterResolutionMode, EdgeResolutionExplanation, EdgeResolutionKind, PlannerCatalog,
    PlannerConfig, adapt_request_for_input, diagnostic_node_id, edge_resolution_to_value,
    input_access_for, latest_node, port_type, resolve_edge_adapter_request,
    target_residency_for_node,
};

pub(super) fn convert(
    graph: &mut Graph,
    catalog: &PlannerCatalog,
    diags: &mut Vec<Diagnostic>,
    config: &PlannerConfig,
) {
    let mut edge_explanations = Vec::new();
    let mut source_fanout: HashMap<(usize, String), usize> = HashMap::new();
    for edge in &graph.edges {
        *source_fanout
            .entry((edge.from.node.0, edge.from.port.clone()))
            .or_default() += 1;
    }
    for edge in &graph.edges {
        let from_node = match graph.nodes.get(edge.from.node.0) {
            Some(n) => n,
            None => continue,
        };
        let to_node = match graph.nodes.get(edge.to.node.0) {
            Some(n) => n,
            None => continue,
        };
        let from_desc = latest_node(catalog, &from_node.id);
        let to_desc = latest_node(catalog, &to_node.id);
        let from_ty = from_desc.and_then(|d| port_type(from_node, d, &edge.from.port, false));
        let to_ty = to_desc.and_then(|d| port_type(to_node, d, &edge.to.port, true));
        let (Some(out_ty), Some(in_ty)) = (from_ty, to_ty) else {
            continue;
        };
        let mut request = to_desc
            .map(|desc| adapt_request_for_input(input_access_for(desc, &edge.to.port), &in_ty))
            .unwrap_or_else(|| {
                daedalus_transport::AdaptRequest::new(typeexpr_transport_key(&in_ty))
            });
        let target_exclusive = matches!(
            request.access,
            daedalus_transport::AccessMode::Move | daedalus_transport::AccessMode::Modify
        ) && source_fanout
            .get(&(edge.from.node.0, edge.from.port.clone()))
            .copied()
            .unwrap_or(0)
            > 1;
        request.exclusive = target_exclusive;
        request.residency = target_residency_for_node(to_node, config);
        let target_access = request.access;
        let target_residency = request.residency;
        let allow_gpu = config.enable_gpu;
        let features: Vec<String> = config.active_features.clone();
        let resolved = resolve_edge_adapter_request(
            config.transport_capabilities.as_ref(),
            &out_ty,
            &in_ty,
            request,
            &features,
            allow_gpu,
        );
        if let Some(resolved) = resolved {
            edge_explanations.push(EdgeResolutionExplanation {
                from_node: from_node.id.0.clone(),
                from_port: edge.from.port.clone(),
                to_node: to_node.id.0.clone(),
                to_port: edge.to.port.clone(),
                from_type: out_ty,
                to_type: in_ty,
                target_access,
                target_exclusive,
                target_residency,
                transport_target: resolved.transport_target,
                resolution_kind: resolved.resolution_kind,
                adapter_mode: resolved.adapter_mode,
                total_cost: resolved.total_cost,
                converter_steps: resolved.converter_steps,
                adapter_path: resolved.adapter_path,
            });
            continue;
        }

        let mut feats = features.clone();
        feats.sort();
        let feats_str = if feats.is_empty() {
            "none".to_string()
        } else {
            feats.join(",")
        };
        let numeric_hint = match (&out_ty, &in_ty) {
            (TypeExpr::Scalar(from), TypeExpr::Scalar(to))
                if from.is_numeric() && to.is_numeric() =>
            {
                format!(
                    "; {} -> {} is not lossless on every target, so it is never converted \
                     implicitly: convert in the producer, change a port type, or register an adapter",
                    from.rust_name(),
                    to.rust_name()
                )
            }
            _ => String::new(),
        };
        diags.push(
            Diagnostic::new(
                DiagnosticCode::ConverterMissing,
                format!(
                    "no converter from {:?} to {:?} for edge {}.{} -> {}.{} [features: {}; gpu: {}]{}",
                    out_ty,
                    in_ty,
                    from_node.id.0,
                    edge.from.port,
                    to_node.id.0,
                    edge.to.port,
                    feats_str,
                    allow_gpu,
                    numeric_hint
                ),
            )
            .in_pass("convert")
            .at_node(diagnostic_node_id(to_node))
            .at_port(edge.to.port.clone()),
        );
        edge_explanations.push(EdgeResolutionExplanation {
            from_node: from_node.id.0.clone(),
            from_port: edge.from.port.clone(),
            to_node: to_node.id.0.clone(),
            to_port: edge.to.port.clone(),
            from_type: out_ty,
            to_type: in_ty,
            target_access,
            target_exclusive,
            target_residency,
            transport_target: None,
            resolution_kind: EdgeResolutionKind::Missing,
            adapter_mode: AdapterResolutionMode::None,
            total_cost: 0,
            converter_steps: Vec::new(),
            adapter_path: Vec::new(),
        });
    }
    edge_explanations.sort_by(|a, b| {
        a.from_node
            .cmp(&b.from_node)
            .then_with(|| a.from_port.cmp(&b.from_port))
            .then_with(|| a.to_node.cmp(&b.to_node))
            .then_with(|| a.to_port.cmp(&b.to_port))
    });
    if !edge_explanations.is_empty() {
        graph.metadata.insert(
            PLAN_EDGE_EXPLANATIONS_KEY.to_string(),
            Value::List(
                edge_explanations
                    .into_iter()
                    .map(edge_resolution_to_value)
                    .collect(),
            ),
        );
    }
}
