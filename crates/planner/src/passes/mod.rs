use daedalus_core::metadata::UI_NODE_ID_KEY;
use daedalus_data::model::{TypeExpr, Value};
use daedalus_registry::capability::NodeDecl;
use daedalus_registry::ids::NodeId;
use std::collections::{BTreeMap, BTreeSet, HashMap};

use crate::diagnostics::{Diagnostic, DiagnosticCode};
use crate::graph::NodeInstance;
use crate::graph::{ExecutionPlan, Graph};
use crate::metadata::{
    DYNAMIC_INPUT_LABELS_KEY, DYNAMIC_INPUT_TYPES_KEY, DYNAMIC_INPUTS_KEY,
    DYNAMIC_OUTPUT_LABELS_KEY, DYNAMIC_OUTPUT_TYPES_KEY, DYNAMIC_OUTPUTS_KEY, DynamicPortMetadata,
    PLAN_APPLIED_LOWERINGS_KEY, PLAN_CONVERTER_METADATA_PREFIX, PLAN_EDGE_EXPLANATIONS_KEY,
    PLAN_OVERLOAD_RESOLUTIONS_KEY, descriptor_metadata_value, is_host_bridge_metadata,
};

mod adapter;
mod align;
mod catalog;
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
mod types;
mod validate;

use adapter::resolve_edge_adapter_request;
use align::align;
pub use catalog::PlannerCatalog;
use catalog::simplify_rust_name;
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
    adapt_request_for_input, input_access_for, input_ty_for, port_type, target_residency_for_node,
    typeexpr_transport_key,
};
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
/// ```ignore
/// use daedalus_planner::{build_plan, PlannerConfig, PlannerInput, Graph};
/// let out = build_plan(PlannerInput { graph: Graph::default() }, PlannerConfig::default());
/// assert_eq!(out.plan.graph.nodes.len(), 0);
/// ```
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

fn typecheck(graph: &mut Graph, catalog: &PlannerCatalog, diags: &mut Vec<Diagnostic>) {
    #[derive(Clone, Debug, PartialEq, Eq, PartialOrd, Ord)]
    struct TypeVarKey {
        node: usize,
        is_input: bool,
        port: String,
    }

    #[derive(Clone, Debug)]
    struct Dsu {
        parent: Vec<usize>,
        rank: Vec<u8>,
        binding: Vec<Option<TypeExpr>>,
    }

    impl Dsu {
        fn new() -> Self {
            Self {
                parent: Vec::new(),
                rank: Vec::new(),
                binding: Vec::new(),
            }
        }

        fn make_set(&mut self) -> usize {
            let id = self.parent.len();
            self.parent.push(id);
            self.rank.push(0);
            self.binding.push(None);
            id
        }

        fn find(&mut self, x: usize) -> usize {
            if self.parent[x] != x {
                let p = self.parent[x];
                self.parent[x] = self.find(p);
            }
            self.parent[x]
        }

        fn union(&mut self, a: usize, b: usize) -> Result<usize, (TypeExpr, TypeExpr)> {
            let mut ra = self.find(a);
            let mut rb = self.find(b);
            if ra == rb {
                return Ok(ra);
            }
            if self.rank[ra] < self.rank[rb] {
                std::mem::swap(&mut ra, &mut rb);
            }
            self.parent[rb] = ra;
            if self.rank[ra] == self.rank[rb] {
                self.rank[ra] = self.rank[ra].saturating_add(1);
            }

            match (&self.binding[ra], &self.binding[rb]) {
                (Some(a), Some(b)) if a != b => return Err((a.clone(), b.clone())),
                (None, Some(b)) => self.binding[ra] = Some(b.clone()),
                _ => {}
            }
            Ok(ra)
        }

        fn bind(&mut self, var: usize, ty: TypeExpr) -> Result<(), (TypeExpr, TypeExpr)> {
            let r = self.find(var);
            if let Some(existing) = &self.binding[r] {
                if existing != &ty {
                    return Err((existing.clone(), ty));
                }
                return Ok(());
            }
            self.binding[r] = Some(ty);
            Ok(())
        }

        fn bound_type(&mut self, var: usize) -> Option<TypeExpr> {
            let r = self.find(var);
            self.binding[r].clone()
        }
    }

    fn is_generic_marker(ty: &TypeExpr) -> bool {
        matches!(ty, TypeExpr::Opaque(value) if value.eq_ignore_ascii_case("generic"))
    }

    fn display_label_for_type(ty: &TypeExpr, lookup: &BTreeMap<TypeExpr, String>) -> String {
        if let Some(found) = lookup.get(ty) {
            return found.clone();
        }

        match ty {
            TypeExpr::Opaque(name) => {
                if name == "image" {
                    return "Image".to_string();
                }
                if let Some(flavor) = name.strip_prefix("image:") {
                    return if flavor.is_empty() {
                        "Image".to_string()
                    } else {
                        format!("Image ({flavor})")
                    };
                }
                if name == "cv:binary_image" {
                    return "Image (binary)".to_string();
                }
                if let Some(raw) = name.strip_prefix("rust:") {
                    return simplify_rust_name(raw);
                }
                name.clone()
            }
            TypeExpr::Scalar(value) => format!("{value:?}"),
            TypeExpr::Optional(inner) => {
                let inner_label = display_label_for_type(inner, lookup);
                format!("{inner_label}?")
            }
            TypeExpr::List(inner) => {
                let inner_label = display_label_for_type(inner, lookup);
                format!("{inner_label}[]")
            }
            TypeExpr::Map(key, value) => {
                let key_label = display_label_for_type(key, lookup);
                let value_label = display_label_for_type(value, lookup);
                format!("map<{key_label}, {value_label}>")
            }
            TypeExpr::Tuple(items) => {
                let parts = items
                    .iter()
                    .map(|item| display_label_for_type(item, lookup))
                    .collect::<Vec<_>>();
                format!("({})", parts.join(", "))
            }
            TypeExpr::Struct(fields) => {
                let mut names = BTreeSet::new();
                for field in fields {
                    names.insert(field.name.trim().to_ascii_lowercase());
                }
                if names.len() == 2 && names.contains("x") && names.contains("y") {
                    return "Point".to_string();
                }
                if names.len() == 4
                    && names.contains("r")
                    && names.contains("g")
                    && names.contains("b")
                    && names.contains("a")
                {
                    return "Pixel".to_string();
                }
                if names.contains("data_b64") && names.contains("width") && names.contains("height")
                {
                    return "Image".to_string();
                }
                "Struct".to_string()
            }
            TypeExpr::Enum(_) => "Enum".to_string(),
        }
    }

    fn label_for_type(ty: &TypeExpr, lookup: &BTreeMap<TypeExpr, String>) -> String {
        display_label_for_type(ty, lookup)
    }

    let mut vars: BTreeMap<TypeVarKey, usize> = BTreeMap::new();
    let mut dsu = Dsu::new();
    let type_label_lookup = catalog.type_label_lookup();

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

        if from_desc.is_none() {
            let suggestions = suggest_nodes(catalog, &from_node.id.0);
            diags.push(
                Diagnostic::new(
                    DiagnosticCode::NodeMissing,
                    format!("node {} not found in registry", from_node.id.0),
                )
                .in_pass("typecheck")
                .at_node(diagnostic_node_id(from_node))
                .with_meta(
                    "missing_node_id",
                    Value::String(std::borrow::Cow::Owned(from_node.id.0.clone())),
                )
                .with_meta(
                    "suggestions",
                    Value::List(
                        suggestions
                            .into_iter()
                            .map(|s| Value::String(std::borrow::Cow::Owned(s)))
                            .collect(),
                    ),
                ),
            );
            continue;
        }
        if to_desc.is_none() {
            let suggestions = suggest_nodes(catalog, &to_node.id.0);
            diags.push(
                Diagnostic::new(
                    DiagnosticCode::NodeMissing,
                    format!("node {} not found in registry", to_node.id.0),
                )
                .in_pass("typecheck")
                .at_node(diagnostic_node_id(to_node))
                .with_meta(
                    "missing_node_id",
                    Value::String(std::borrow::Cow::Owned(to_node.id.0.clone())),
                )
                .with_meta(
                    "suggestions",
                    Value::List(
                        suggestions
                            .into_iter()
                            .map(|s| Value::String(std::borrow::Cow::Owned(s)))
                            .collect(),
                    ),
                ),
            );
            continue;
        }

        if from_ty.is_none() {
            let available: Vec<Value> = from_desc
                .map(|d| {
                    d.outputs
                        .iter()
                        .map(|p| Value::String(std::borrow::Cow::Owned(p.name.clone())))
                        .collect()
                })
                .unwrap_or_default();
            diags.push(
                Diagnostic::new(
                    DiagnosticCode::PortMissing,
                    format!(
                        "output port `{}` not found on node {}",
                        edge.from.port, from_node.id.0
                    ),
                )
                .in_pass("typecheck")
                .at_node(diagnostic_node_id(from_node))
                .at_port(edge.from.port.clone())
                .with_meta(
                    "missing_port",
                    Value::String(std::borrow::Cow::Owned(edge.from.port.clone())),
                )
                .with_meta(
                    "missing_port_direction",
                    Value::String(std::borrow::Cow::Borrowed("output")),
                )
                .with_meta("available_ports", Value::List(available)),
            );
        }
        if to_ty.is_none() {
            let available: Vec<Value> = to_desc
                .map(|d| {
                    d.inputs
                        .iter()
                        .map(|p| Value::String(std::borrow::Cow::Owned(p.name.clone())))
                        .collect()
                })
                .unwrap_or_default();
            diags.push(
                Diagnostic::new(
                    DiagnosticCode::PortMissing,
                    format!(
                        "input port `{}` not found on node {}",
                        edge.to.port, to_node.id.0
                    ),
                )
                .in_pass("typecheck")
                .at_node(diagnostic_node_id(to_node))
                .at_port(edge.to.port.clone())
                .with_meta(
                    "missing_port",
                    Value::String(std::borrow::Cow::Owned(edge.to.port.clone())),
                )
                .with_meta(
                    "missing_port_direction",
                    Value::String(std::borrow::Cow::Borrowed("input")),
                )
                .with_meta("available_ports", Value::List(available)),
            );
        }

        let (Some(from_ty), Some(to_ty)) = (from_ty, to_ty) else {
            continue;
        };

        // Resolve `Opaque("generic")` as a proper type variable: graph edges constrain it.
        let from_term = if is_generic_marker(&from_ty) {
            let key = TypeVarKey {
                node: edge.from.node.0,
                is_input: false,
                port: edge.from.port.clone(),
            };
            let id = *vars.entry(key).or_insert_with(|| dsu.make_set());
            Some(id)
        } else {
            None
        };
        let to_term = if is_generic_marker(&to_ty) {
            let key = TypeVarKey {
                node: edge.to.node.0,
                is_input: true,
                port: edge.to.port.clone(),
            };
            let id = *vars.entry(key).or_insert_with(|| dsu.make_set());
            Some(id)
        } else {
            None
        };

        let conflict = match (from_term, to_term) {
            (Some(var), None) => dsu.bind(var, to_ty.clone()).err(),
            (None, Some(var)) => dsu.bind(var, from_ty.clone()).err(),
            (Some(a), Some(b)) => dsu.union(a, b).err(),
            (None, None) => None,
        };

        if let Some((a, b)) = conflict {
            let host = if is_generic_marker(&from_ty) {
                from_node
            } else {
                to_node
            };
            let port = if is_generic_marker(&from_ty) {
                edge.from.port.clone()
            } else {
                edge.to.port.clone()
            };
            diags.push(
                Diagnostic::new(
                    DiagnosticCode::TypeMismatch,
                    format!(
                        "generic port `{}` inferred conflicting types: {:?} vs {:?} (edge {}.{} -> {}.{})",
                        port,
                        a,
                        b,
                        from_node.id.0,
                        edge.from.port,
                        to_node.id.0,
                        edge.to.port
                    ),
                )
                .in_pass("typecheck")
                .at_node(diagnostic_node_id(host))
                .at_port(port)
                .with_meta(
                    "type_a",
                    Value::String(std::borrow::Cow::Owned(
                        serde_json::to_string(&a).unwrap_or_default(),
                    )),
                )
                .with_meta(
                    "type_b",
                    Value::String(std::borrow::Cow::Owned(
                        serde_json::to_string(&b).unwrap_or_default(),
                    )),
                ),
            );
        }
    }

    let mut dynamic_metadata_by_node: BTreeMap<usize, DynamicPortMetadata> = BTreeMap::new();

    // Apply solved generic types back onto node metadata so later passes/runtime can consume them.
    for (key, var) in vars {
        let Some(ty) = dsu.bound_type(var) else {
            continue;
        };
        let Some(node) = graph.nodes.get(key.node) else {
            continue;
        };
        let dynamic_metadata = dynamic_metadata_by_node
            .entry(key.node)
            .or_insert_with(|| DynamicPortMetadata::from_node_metadata(&node.metadata));
        dynamic_metadata.set_resolved_type(key.is_input, &key.port, ty.clone());
        let label = label_for_type(&ty, &type_label_lookup);
        dynamic_metadata.set_label(key.is_input, &key.port, label);
    }

    for (node_idx, dynamic_metadata) in dynamic_metadata_by_node {
        if let Some(node) = graph.nodes.get_mut(node_idx) {
            dynamic_metadata.write_to_node_metadata(&mut node.metadata);
        }
    }
}

fn convert(
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
        diags.push(
            Diagnostic::new(
                DiagnosticCode::ConverterMissing,
                format!(
                    "no converter from {:?} to {:?} for edge {}.{} -> {}.{} [features: {}; gpu: {}]",
                    out_ty,
                    in_ty,
                    from_node.id.0,
                    edge.from.port,
                    to_node.id.0,
                    edge.to.port,
                    feats_str,
                    allow_gpu
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

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn build_plan_strips_planner_owned_dynamic_metadata() {
        let mut metadata = BTreeMap::new();
        for key in [
            DYNAMIC_INPUT_TYPES_KEY,
            DYNAMIC_OUTPUT_TYPES_KEY,
            DYNAMIC_INPUT_LABELS_KEY,
            DYNAMIC_OUTPUT_LABELS_KEY,
            DYNAMIC_INPUTS_KEY,
            DYNAMIC_OUTPUTS_KEY,
        ] {
            metadata.insert(key.to_string(), Value::String("client".into()));
        }

        let graph = Graph {
            nodes: vec![NodeInstance {
                id: NodeId::new("demo.node"),
                bundle: None,
                label: None,
                inputs: Vec::new(),
                outputs: Vec::new(),
                compute: crate::graph::ComputeAffinity::CpuOnly,
                const_inputs: Vec::new(),
                sync_groups: Vec::new(),
                metadata,
            }],
            ..Graph::default()
        };

        let out = build_plan(PlannerInput { graph }, PlannerConfig::default());
        let node_metadata = &out.plan.graph.nodes[0].metadata;
        for key in [
            DYNAMIC_INPUT_TYPES_KEY,
            DYNAMIC_OUTPUT_TYPES_KEY,
            DYNAMIC_INPUT_LABELS_KEY,
            DYNAMIC_OUTPUT_LABELS_KEY,
            DYNAMIC_INPUTS_KEY,
            DYNAMIC_OUTPUTS_KEY,
        ] {
            assert!(!node_metadata.contains_key(key), "{key} was not stripped");
        }
    }
}
