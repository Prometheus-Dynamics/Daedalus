use daedalus_core::metadata::UI_NODE_ID_KEY;
use daedalus_data::model::Value;
use daedalus_registry::capability::NodeDecl;
use daedalus_registry::ids::NodeId;

use crate::graph::ExecutionPlan;
use crate::graph::NodeInstance;
use crate::metadata::{
    DYNAMIC_INPUT_LABELS_KEY, DYNAMIC_INPUT_TYPES_KEY, DYNAMIC_INPUTS_KEY,
    DYNAMIC_OUTPUT_LABELS_KEY, DYNAMIC_OUTPUT_TYPES_KEY, DYNAMIC_OUTPUTS_KEY, HostPortTypes,
    PLAN_APPLIED_LOWERINGS_KEY, PLAN_CONVERTER_METADATA_PREFIX, PLAN_EDGE_EXPLANATIONS_KEY,
    PLAN_OVERLOAD_RESOLUTIONS_KEY, descriptor_metadata_value, is_host_bridge_metadata,
};

mod adapter;
mod align;
mod catalog;
mod convert;
mod embedded;
mod explain;
mod hydrate;
mod lint;
mod lowerings;
mod overloads;
mod schedule;
mod setup;
mod suggest;
mod type_utils;
mod typecheck;
mod types;
mod validate;

use adapter::resolve_edge_adapter_request;
use align::align;
pub use catalog::PlannerCatalog;
use catalog::simplify_rust_name;
use convert::convert;
use embedded::expand_embedded_graphs;
pub use explain::explain_plan;
use explain::{applied_lowering_to_value, edge_resolution_to_value, overload_resolution_to_value};
use hydrate::hydrate_registry;
use lint::lint;
use lowerings::apply_planner_lowerings;
pub use lowerings::{
    PlannerLoweringContext, PlannerLoweringRegistry, register_planner_lowering,
    registered_planner_lowerings,
};
use overloads::resolve_node_overloads;
use schedule::{gpu, schedule};
use setup::{apply_descriptor_defaults, clear_planner_owned_graph_metadata};
use suggest::suggest_nodes;
use type_utils::{
    adapt_request_for_input, input_access_for, input_ty_for, is_generic_marker, port_type,
    target_residency_for_node, typeexpr_transport_key,
};
use typecheck::typecheck;
pub use types::{
    AdapterResolutionMode, AppliedPlannerLowering, EdgeResolutionExplanation, EdgeResolutionKind,
    NodeOverloadResolution, OverloadPortResolution, PlanExplanation, PlannerConfig, PlannerInput,
    PlannerLoweringInfo, PlannerLoweringPhase, PlannerOutput,
};
use validate::validate_port_declarations;

pub(super) fn node_metadata_value(node: &NodeDecl, key: &str) -> Option<Value> {
    descriptor_metadata_value(node, key)
}

pub(super) fn is_host_bridge(node: &NodeInstance) -> bool {
    is_host_bridge_metadata(&node.metadata)
}

pub(super) fn diagnostic_node_id(node: &NodeInstance) -> String {
    if let Some(daedalus_data::model::Value::String(value)) = node.metadata.get(UI_NODE_ID_KEY) {
        let trimmed = value.trim();
        if !trimmed.is_empty() {
            return trimmed.to_string();
        }
    }
    if let Some(label) = node.label.as_deref() {
        let trimmed = label.trim();
        if !trimmed.is_empty() {
            return trimmed.to_string();
        }
    }
    node.id.0.clone()
}

/// Build an execution plan by running the ordered pass pipeline.
/// Currently stubs; contracts are enforced via deterministic diagnostics ordering.
/// Build an execution plan from a graph and registry.
///
pub fn build_plan(mut input: PlannerInput, config: PlannerConfig) -> PlannerOutput {
    let mut diags = Vec::new();
    let catalog = PlannerCatalog::from_config(&config);
    clear_planner_owned_graph_metadata(&mut input.graph);

    // Security/integrity: clients can attach arbitrary node metadata in Graph JSON. These keys are
    // planner-owned and must not be accepted as inputs, otherwise a client can "force" types.
    for node in &mut input.graph.nodes {
        node.metadata.remove(DYNAMIC_INPUT_TYPES_KEY);
        node.metadata.remove(DYNAMIC_OUTPUT_TYPES_KEY);
        node.metadata.remove(DYNAMIC_INPUT_LABELS_KEY);
        node.metadata.remove(DYNAMIC_OUTPUT_LABELS_KEY);
        node.metadata.remove(DYNAMIC_INPUTS_KEY);
        node.metadata.remove(DYNAMIC_OUTPUTS_KEY);
        // Declared host port types are graph inputs: they fix the bridge's port types, and every
        // edge is still type checked (and adapted) against the node port it connects to.
        if is_host_bridge(node) {
            HostPortTypes::from_node_metadata(&node.metadata)
                .to_dynamic()
                .write_to_node_metadata(&mut node.metadata);
        }
    }

    let mut applied_lowerings = Vec::new();
    expand_embedded_graphs(&mut input, &catalog, &mut diags);
    apply_descriptor_defaults(&mut input.graph, &catalog);
    applied_lowerings.extend(apply_planner_lowerings(
        &mut input.graph,
        &catalog,
        &config,
        &mut diags,
        PlannerLoweringPhase::BeforeTypecheck,
    ));
    hydrate_registry(&input, &catalog, &mut diags);
    validate_port_declarations(
        &input.graph,
        &catalog,
        &mut diags,
        config.strict_port_declarations,
    );
    let overload_resolutions =
        resolve_node_overloads(&mut input.graph, &catalog, &config, &mut diags);
    typecheck(&mut input.graph, &catalog, &mut diags);
    convert(&mut input.graph, &catalog, &mut diags, &config);
    applied_lowerings.extend(apply_planner_lowerings(
        &mut input.graph,
        &catalog,
        &config,
        &mut diags,
        PlannerLoweringPhase::AfterConvert,
    ));
    align(&mut input.graph, &mut diags);
    gpu(&mut input.graph, &config, &mut diags);
    schedule(&mut input.graph, &mut diags);
    if config.enable_lints {
        lint(&input, &catalog, &config, &mut diags);
    }

    if !applied_lowerings.is_empty() {
        input.graph.metadata.insert(
            PLAN_APPLIED_LOWERINGS_KEY.to_string(),
            Value::List(
                applied_lowerings
                    .iter()
                    .map(applied_lowering_to_value)
                    .collect(),
            ),
        );
    }
    if !overload_resolutions.is_empty() {
        input.graph.metadata.insert(
            PLAN_OVERLOAD_RESOLUTIONS_KEY.to_string(),
            Value::List(
                overload_resolutions
                    .into_iter()
                    .map(overload_resolution_to_value)
                    .collect(),
            ),
        );
    }

    let plan = ExecutionPlan::new(input.graph.clone(), diags.clone());
    PlannerOutput {
        plan,
        diagnostics: diags,
    }
}

pub(super) fn latest_node<'a>(catalog: &'a PlannerCatalog, id: &NodeId) -> Option<&'a NodeDecl> {
    catalog.node(id)
}

#[cfg(test)]
mod tests;
