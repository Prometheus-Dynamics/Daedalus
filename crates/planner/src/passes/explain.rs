use daedalus_data::model::{StructFieldValue, TypeExpr, Value};
use daedalus_registry::capability::AdapterPathStep;
use std::collections::BTreeMap;
use std::str::FromStr;

use crate::graph::Graph;
use crate::metadata::{
    PLAN_APPLIED_LOWERINGS_KEY, PLAN_EDGE_EXPLANATIONS_KEY, PLAN_OVERLOAD_RESOLUTIONS_KEY,
};

use super::{
    AppliedPlannerLowering, EdgeResolutionExplanation, NodeOverloadResolution,
    OverloadPortResolution, PlanExplanation, PlannerLoweringPhase,
};

fn owned_string_value(value: impl Into<String>) -> Value {
    Value::String(std::borrow::Cow::Owned(value.into()))
}

fn int_value(value: u64) -> Value {
    Value::Int(i64::try_from(value).unwrap_or(i64::MAX))
}

fn bool_value(value: bool) -> Value {
    Value::Bool(value)
}

fn struct_value(fields: Vec<(&str, Value)>) -> Value {
    Value::Struct(
        fields
            .into_iter()
            .map(|(name, value)| StructFieldValue {
                name: name.to_string(),
                value,
            })
            .collect(),
    )
}

fn string_keyed_map(entries: BTreeMap<String, Value>) -> Value {
    Value::Map(
        entries
            .into_iter()
            .map(|(key, value)| (owned_string_value(key), value))
            .collect(),
    )
}

fn typeexpr_to_value(ty: &TypeExpr) -> Value {
    owned_string_value(serde_json::to_string(ty).unwrap_or_default())
}

pub(super) fn applied_lowering_to_value(lowering: &AppliedPlannerLowering) -> Value {
    struct_value(vec![
        ("id", owned_string_value(lowering.id.clone())),
        (
            "phase",
            owned_string_value(match lowering.phase {
                PlannerLoweringPhase::BeforeTypecheck => "before_typecheck",
                PlannerLoweringPhase::AfterConvert => "after_convert",
            }),
        ),
        ("summary", owned_string_value(lowering.summary.clone())),
        ("changed", bool_value(lowering.changed)),
        ("metadata", string_keyed_map(lowering.metadata.clone())),
    ])
}

pub(super) fn edge_resolution_to_value(edge: EdgeResolutionExplanation) -> Value {
    let target_exclusive = edge.target_exclusive;
    let target_residency = edge.target_residency;
    let transport_target = edge.transport_target;
    let mut fields = vec![
        ("from_node", owned_string_value(edge.from_node)),
        ("from_port", owned_string_value(edge.from_port)),
        ("to_node", owned_string_value(edge.to_node)),
        ("to_port", owned_string_value(edge.to_port)),
        ("from_type", typeexpr_to_value(&edge.from_type)),
        ("to_type", typeexpr_to_value(&edge.to_type)),
        (
            "target_access",
            owned_string_value(edge.target_access.as_str()),
        ),
        (
            "resolution_kind",
            owned_string_value(edge.resolution_kind.as_str()),
        ),
        (
            "adapter_mode",
            owned_string_value(edge.adapter_mode.as_str()),
        ),
        ("total_cost", int_value(edge.total_cost)),
        (
            "converter_steps",
            Value::List(
                edge.converter_steps
                    .into_iter()
                    .map(owned_string_value)
                    .collect(),
            ),
        ),
        (
            "adapter_path",
            Value::List(
                edge.adapter_path
                    .into_iter()
                    .map(adapter_step_to_value)
                    .collect(),
            ),
        ),
    ];
    if target_exclusive {
        fields.push(("target_exclusive", bool_value(true)));
    }
    if let Some(residency) = target_residency {
        fields.push(("target_residency", owned_string_value(residency.as_str())));
    }
    if let Some(target) = transport_target {
        fields.push(("transport_target", owned_string_value(target.to_string())));
    }
    struct_value(fields)
}

fn adapter_step_to_value(step: AdapterPathStep) -> Value {
    struct_value(vec![
        ("adapter", owned_string_value(step.adapter.to_string())),
        ("from", owned_string_value(step.from.to_string())),
        ("to", owned_string_value(step.to.to_string())),
        ("kind", owned_string_value(step.kind.as_str())),
        ("access", owned_string_value(step.access.as_str())),
        ("cost", int_value(step.cost.weight())),
        ("requires_gpu", bool_value(step.requires_gpu)),
        (
            "residency",
            step.residency
                .map(|residency| owned_string_value(residency.as_str()))
                .unwrap_or(Value::Unit),
        ),
        (
            "layout",
            step.layout
                .map(|layout| owned_string_value(layout.as_str()))
                .unwrap_or(Value::Unit),
        ),
    ])
}

pub(super) fn overload_resolution_to_value(resolution: NodeOverloadResolution) -> Value {
    struct_value(vec![
        ("node", owned_string_value(resolution.node)),
        ("overload_id", owned_string_value(resolution.overload_id)),
        (
            "overload_label",
            resolution
                .overload_label
                .map(owned_string_value)
                .unwrap_or(Value::Unit),
        ),
        ("total_cost", int_value(resolution.total_cost)),
        (
            "ports",
            Value::List(
                resolution
                    .ports
                    .into_iter()
                    .map(|port| {
                        struct_value(vec![
                            ("port", owned_string_value(port.port)),
                            ("from_node", owned_string_value(port.from_node)),
                            ("from_port", owned_string_value(port.from_port)),
                            ("from_type", typeexpr_to_value(&port.from_type)),
                            ("to_type", typeexpr_to_value(&port.to_type)),
                            (
                                "resolution_kind",
                                owned_string_value(port.resolution_kind.as_str()),
                            ),
                            (
                                "adapter_mode",
                                owned_string_value(port.adapter_mode.as_str()),
                            ),
                            ("total_cost", int_value(port.total_cost)),
                            (
                                "converter_steps",
                                Value::List(
                                    port.converter_steps
                                        .into_iter()
                                        .map(owned_string_value)
                                        .collect(),
                                ),
                            ),
                        ])
                    })
                    .collect(),
            ),
        ),
    ])
}

fn string(value: &Value, name: &str) -> Option<String> {
    value.field(name)?.as_str().map(str::to_string)
}

fn u64_field(value: &Value, name: &str) -> Option<u64> {
    value.field(name)?.as_u64()
}

fn type_field(value: &Value, name: &str) -> Option<TypeExpr> {
    TypeExpr::from_json_value(value.field(name)?)
}

fn parsed<T: FromStr>(value: &Value, name: &str) -> Option<T> {
    value.field(name)?.as_str()?.parse().ok()
}

fn string_list(value: &Value, name: &str) -> Option<Vec<String>> {
    value.field(name)?.as_string_list()
}

fn parse_planner_lowering_phase(value: &Value) -> Option<PlannerLoweringPhase> {
    match value.field("phase")?.as_str()? {
        "before_typecheck" => Some(PlannerLoweringPhase::BeforeTypecheck),
        "after_convert" => Some(PlannerLoweringPhase::AfterConvert),
        _ => None,
    }
}

fn parse_applied_lowering(value: &Value) -> Option<AppliedPlannerLowering> {
    Some(AppliedPlannerLowering {
        id: string(value, "id")?,
        phase: parse_planner_lowering_phase(value)?,
        summary: string(value, "summary")?,
        changed: value.field("changed")?.as_bool()?,
        metadata: value.field("metadata")?.as_string_map()?,
    })
}

fn parse_edge_explanation(value: &Value) -> Option<EdgeResolutionExplanation> {
    Some(EdgeResolutionExplanation {
        from_node: string(value, "from_node")?,
        from_port: string(value, "from_port")?,
        to_node: string(value, "to_node")?,
        to_port: string(value, "to_port")?,
        from_type: type_field(value, "from_type")?,
        to_type: type_field(value, "to_type")?,
        target_access: parsed(value, "target_access")
            .unwrap_or(daedalus_transport::AccessMode::Read),
        target_exclusive: value
            .field("target_exclusive")
            .and_then(Value::as_bool)
            .unwrap_or(false),
        target_residency: parsed(value, "target_residency"),
        transport_target: string(value, "transport_target").map(daedalus_transport::TypeKey::new),
        resolution_kind: parsed(value, "resolution_kind")?,
        adapter_mode: parsed(value, "adapter_mode")?,
        total_cost: u64_field(value, "total_cost")?,
        converter_steps: string_list(value, "converter_steps")?,
        adapter_path: value
            .field("adapter_path")
            .and_then(Value::as_list)
            .map(|steps| steps.iter().filter_map(parse_adapter_step).collect())
            .unwrap_or_default(),
    })
}

fn parse_adapter_step(value: &Value) -> Option<AdapterPathStep> {
    let kind: daedalus_transport::AdaptKind = parsed(value, "kind")?;
    let mut cost = daedalus_transport::AdaptCost::new(kind);
    if let Some(weight) = u64_field(value, "cost") {
        cost.cpu_ns = weight.min(u64::from(u32::MAX)) as u32;
    }
    Some(AdapterPathStep {
        adapter: daedalus_transport::AdapterId::new(string(value, "adapter")?),
        from: daedalus_transport::TypeKey::new(string(value, "from")?),
        to: daedalus_transport::TypeKey::new(string(value, "to")?),
        kind,
        access: parsed(value, "access").unwrap_or(daedalus_transport::AccessMode::Read),
        cost,
        requires_gpu: value
            .field("requires_gpu")
            .and_then(Value::as_bool)
            .unwrap_or(false),
        residency: parsed(value, "residency"),
        layout: string(value, "layout").map(daedalus_transport::Layout::new),
    })
}

fn parse_overload_port_resolution(value: &Value) -> Option<OverloadPortResolution> {
    Some(OverloadPortResolution {
        port: string(value, "port")?,
        from_node: string(value, "from_node")?,
        from_port: string(value, "from_port")?,
        from_type: type_field(value, "from_type")?,
        to_type: type_field(value, "to_type")?,
        resolution_kind: parsed(value, "resolution_kind")?,
        adapter_mode: parsed(value, "adapter_mode")?,
        total_cost: u64_field(value, "total_cost")?,
        converter_steps: string_list(value, "converter_steps")?,
    })
}

fn parse_overload_resolution(value: &Value) -> Option<NodeOverloadResolution> {
    Some(NodeOverloadResolution {
        node: string(value, "node")?,
        overload_id: string(value, "overload_id")?,
        overload_label: match value.field("overload_label")? {
            Value::Unit => None,
            label => label.as_str().map(str::to_string),
        },
        total_cost: u64_field(value, "total_cost")?,
        ports: value
            .field("ports")?
            .as_list()?
            .iter()
            .filter_map(parse_overload_port_resolution)
            .collect(),
    })
}

fn parse_list<T>(
    metadata: &BTreeMap<String, Value>,
    key: &str,
    parse: fn(&Value) -> Option<T>,
) -> Vec<T> {
    metadata
        .get(key)
        .and_then(Value::as_list)
        .map(|items| items.iter().filter_map(parse).collect())
        .unwrap_or_default()
}

/// Typed edge resolution explanations the planner recorded in a plan graph's metadata
/// (`Graph::metadata`).
pub fn edge_explanations(
    graph_metadata: &BTreeMap<String, Value>,
) -> Vec<EdgeResolutionExplanation> {
    parse_list(
        graph_metadata,
        PLAN_EDGE_EXPLANATIONS_KEY,
        parse_edge_explanation,
    )
}

pub fn explain_plan(graph: &Graph) -> PlanExplanation {
    PlanExplanation {
        lowerings: parse_list(
            &graph.metadata,
            PLAN_APPLIED_LOWERINGS_KEY,
            parse_applied_lowering,
        ),
        overloads: parse_list(
            &graph.metadata,
            PLAN_OVERLOAD_RESOLUTIONS_KEY,
            parse_overload_resolution,
        ),
        edges: edge_explanations(&graph.metadata),
    }
}
