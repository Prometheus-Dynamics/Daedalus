use daedalus_data::model::{StructFieldValue, TypeExpr, Value};
use daedalus_registry::capability::AdapterPathStep;
use std::collections::BTreeMap;
use std::str::FromStr;

use crate::graph::Graph;
use crate::metadata::{
    PLAN_APPLIED_LOWERINGS_KEY, PLAN_EDGE_EXPLANATIONS_KEY, PLAN_OVERLOAD_RESOLUTIONS_KEY,
};

use super::{
    AdapterResolutionMode, AppliedPlannerLowering, EdgeResolutionExplanation, EdgeResolutionKind,
    NodeOverloadResolution, OverloadPortResolution, PlanExplanation, PlannerLoweringPhase,
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

fn struct_field<'a>(fields: &'a [StructFieldValue], name: &str) -> Option<&'a Value> {
    fields
        .iter()
        .find(|field| field.name == name)
        .map(|field| &field.value)
}

fn value_to_string_map(value: &Value) -> Option<BTreeMap<String, Value>> {
    let Value::Map(entries) = value else {
        return None;
    };
    let mut map = BTreeMap::new();
    for (key, value) in entries {
        let Value::String(key) = key else {
            return None;
        };
        map.insert(key.to_string(), value.clone());
    }
    Some(map)
}

fn typeexpr_to_value(ty: &TypeExpr) -> Value {
    owned_string_value(serde_json::to_string(ty).unwrap_or_default())
}

fn value_to_typeexpr(value: &Value) -> Option<TypeExpr> {
    match value {
        Value::String(json) => serde_json::from_str::<TypeExpr>(json).ok(),
        _ => None,
    }
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

fn value_as_string(value: &Value) -> Option<String> {
    match value {
        Value::String(value) => Some(value.to_string()),
        _ => None,
    }
}

fn value_as_u64(value: &Value) -> Option<u64> {
    match value {
        Value::Int(value) => (*value).try_into().ok(),
        _ => None,
    }
}

fn value_as_bool(value: &Value) -> Option<bool> {
    match value {
        Value::Bool(value) => Some(*value),
        _ => None,
    }
}

fn parse_edge_resolution_kind(value: &Value) -> Option<EdgeResolutionKind> {
    EdgeResolutionKind::from_str(&value_as_string(value)?).ok()
}

fn parse_adapter_mode(value: &Value) -> Option<AdapterResolutionMode> {
    AdapterResolutionMode::from_str(&value_as_string(value)?).ok()
}

fn parse_planner_lowering_phase(value: &Value) -> Option<PlannerLoweringPhase> {
    match value_as_string(value)?.as_str() {
        "before_typecheck" => Some(PlannerLoweringPhase::BeforeTypecheck),
        "after_convert" => Some(PlannerLoweringPhase::AfterConvert),
        _ => None,
    }
}

fn parse_access_mode(value: &Value) -> Option<daedalus_transport::AccessMode> {
    daedalus_transport::AccessMode::from_str(&value_as_string(value)?).ok()
}

fn parse_residency(value: &Value) -> Option<daedalus_transport::Residency> {
    daedalus_transport::Residency::from_str(&value_as_string(value)?).ok()
}

fn parse_applied_lowering(value: &Value) -> Option<AppliedPlannerLowering> {
    let Value::Struct(fields) = value else {
        return None;
    };
    Some(AppliedPlannerLowering {
        id: value_as_string(struct_field(fields, "id")?)?,
        phase: parse_planner_lowering_phase(struct_field(fields, "phase")?)?,
        summary: value_as_string(struct_field(fields, "summary")?)?,
        changed: value_as_bool(struct_field(fields, "changed")?)?,
        metadata: value_to_string_map(struct_field(fields, "metadata")?)?,
    })
}

fn parse_edge_explanation(value: &Value) -> Option<EdgeResolutionExplanation> {
    let Value::Struct(fields) = value else {
        return None;
    };
    Some(EdgeResolutionExplanation {
        from_node: value_as_string(struct_field(fields, "from_node")?)?,
        from_port: value_as_string(struct_field(fields, "from_port")?)?,
        to_node: value_as_string(struct_field(fields, "to_node")?)?,
        to_port: value_as_string(struct_field(fields, "to_port")?)?,
        from_type: value_to_typeexpr(struct_field(fields, "from_type")?)?,
        to_type: value_to_typeexpr(struct_field(fields, "to_type")?)?,
        target_access: struct_field(fields, "target_access")
            .and_then(parse_access_mode)
            .unwrap_or(daedalus_transport::AccessMode::Read),
        target_exclusive: struct_field(fields, "target_exclusive")
            .and_then(value_as_bool)
            .unwrap_or(false),
        target_residency: struct_field(fields, "target_residency").and_then(parse_residency),
        transport_target: struct_field(fields, "transport_target")
            .and_then(value_as_string)
            .map(daedalus_transport::TypeKey::new),
        resolution_kind: parse_edge_resolution_kind(struct_field(fields, "resolution_kind")?)?,
        adapter_mode: parse_adapter_mode(struct_field(fields, "adapter_mode")?)?,
        total_cost: value_as_u64(struct_field(fields, "total_cost")?)?,
        converter_steps: match struct_field(fields, "converter_steps")? {
            Value::List(values) => values.iter().filter_map(value_as_string).collect(),
            _ => return None,
        },
        adapter_path: match struct_field(fields, "adapter_path") {
            Some(Value::List(values)) => values.iter().filter_map(parse_adapter_step).collect(),
            _ => Vec::new(),
        },
    })
}

fn parse_adapter_step(value: &Value) -> Option<AdapterPathStep> {
    let Value::Struct(fields) = value else {
        return None;
    };
    let kind = struct_field(fields, "kind")
        .and_then(value_as_string)
        .and_then(|name| daedalus_transport::AdaptKind::from_str(&name).ok())?;
    let mut cost = daedalus_transport::AdaptCost::new(kind);
    if let Some(weight) = struct_field(fields, "cost").and_then(value_as_u64) {
        cost.cpu_ns = weight.min(u64::from(u32::MAX)) as u32;
    }
    Some(AdapterPathStep {
        adapter: daedalus_transport::AdapterId::new(value_as_string(struct_field(
            fields, "adapter",
        )?)?),
        from: daedalus_transport::TypeKey::new(value_as_string(struct_field(fields, "from")?)?),
        to: daedalus_transport::TypeKey::new(value_as_string(struct_field(fields, "to")?)?),
        kind,
        access: struct_field(fields, "access")
            .and_then(parse_access_mode)
            .unwrap_or(daedalus_transport::AccessMode::Read),
        cost,
        requires_gpu: struct_field(fields, "requires_gpu")
            .and_then(value_as_bool)
            .unwrap_or(false),
        residency: struct_field(fields, "residency").and_then(parse_residency),
        layout: struct_field(fields, "layout")
            .and_then(value_as_string)
            .map(daedalus_transport::Layout::new),
    })
}

fn parse_overload_port_resolution(value: &Value) -> Option<OverloadPortResolution> {
    let Value::Struct(fields) = value else {
        return None;
    };
    Some(OverloadPortResolution {
        port: value_as_string(struct_field(fields, "port")?)?,
        from_node: value_as_string(struct_field(fields, "from_node")?)?,
        from_port: value_as_string(struct_field(fields, "from_port")?)?,
        from_type: value_to_typeexpr(struct_field(fields, "from_type")?)?,
        to_type: value_to_typeexpr(struct_field(fields, "to_type")?)?,
        resolution_kind: parse_edge_resolution_kind(struct_field(fields, "resolution_kind")?)?,
        adapter_mode: parse_adapter_mode(struct_field(fields, "adapter_mode")?)?,
        total_cost: value_as_u64(struct_field(fields, "total_cost")?)?,
        converter_steps: match struct_field(fields, "converter_steps")? {
            Value::List(values) => values.iter().filter_map(value_as_string).collect(),
            _ => return None,
        },
    })
}

fn parse_overload_resolution(value: &Value) -> Option<NodeOverloadResolution> {
    let Value::Struct(fields) = value else {
        return None;
    };
    Some(NodeOverloadResolution {
        node: value_as_string(struct_field(fields, "node")?)?,
        overload_id: value_as_string(struct_field(fields, "overload_id")?)?,
        overload_label: match struct_field(fields, "overload_label")? {
            Value::Unit => None,
            value => value_as_string(value),
        },
        total_cost: value_as_u64(struct_field(fields, "total_cost")?)?,
        ports: match struct_field(fields, "ports")? {
            Value::List(values) => values
                .iter()
                .filter_map(parse_overload_port_resolution)
                .collect(),
            _ => return None,
        },
    })
}

pub fn explain_plan(graph: &Graph) -> PlanExplanation {
    let lowerings = match graph.metadata.get(PLAN_APPLIED_LOWERINGS_KEY) {
        Some(Value::List(values)) => values.iter().filter_map(parse_applied_lowering).collect(),
        _ => Vec::new(),
    };
    let overloads = match graph.metadata.get(PLAN_OVERLOAD_RESOLUTIONS_KEY) {
        Some(Value::List(values)) => values
            .iter()
            .filter_map(parse_overload_resolution)
            .collect(),
        _ => Vec::new(),
    };
    let edges = match graph.metadata.get(PLAN_EDGE_EXPLANATIONS_KEY) {
        Some(Value::List(values)) => values.iter().filter_map(parse_edge_explanation).collect(),
        _ => Vec::new(),
    };
    PlanExplanation {
        lowerings,
        overloads,
        edges,
    }
}
