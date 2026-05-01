use std::str::FromStr;

pub(super) fn struct_fields(
    value: &daedalus_data::model::Value,
) -> Option<&[daedalus_data::model::StructFieldValue]> {
    match value {
        daedalus_data::model::Value::Struct(fields) => Some(fields),
        _ => None,
    }
}

fn struct_field<'a>(
    fields: &'a [daedalus_data::model::StructFieldValue],
    name: &str,
) -> Option<&'a daedalus_data::model::Value> {
    fields
        .iter()
        .find(|field| field.name == name)
        .map(|field| &field.value)
}

pub(super) fn string_field<'a>(
    fields: &'a [daedalus_data::model::StructFieldValue],
    name: &str,
) -> Option<&'a str> {
    match struct_field(fields, name)? {
        daedalus_data::model::Value::String(value) => Some(value.as_ref()),
        _ => None,
    }
}

pub(super) fn typeexpr_field(
    fields: &[daedalus_data::model::StructFieldValue],
    name: &str,
) -> Option<daedalus_data::model::TypeExpr> {
    match struct_field(fields, name)? {
        daedalus_data::model::Value::String(json) => serde_json::from_str(json).ok(),
        _ => None,
    }
}

pub(super) fn access_field(
    fields: &[daedalus_data::model::StructFieldValue],
    name: &str,
) -> Option<daedalus_transport::AccessMode> {
    daedalus_transport::AccessMode::from_str(string_field(fields, name)?).ok()
}

pub(super) fn residency_field(
    fields: &[daedalus_data::model::StructFieldValue],
    name: &str,
) -> Option<daedalus_transport::Residency> {
    daedalus_transport::Residency::from_str(string_field(fields, name)?).ok()
}

pub(super) fn bool_field(
    fields: &[daedalus_data::model::StructFieldValue],
    name: &str,
) -> Option<bool> {
    match struct_field(fields, name)? {
        daedalus_data::model::Value::Bool(value) => Some(*value),
        _ => None,
    }
}

pub(super) fn adapter_steps_field(
    fields: &[daedalus_data::model::StructFieldValue],
    name: &str,
) -> Vec<daedalus_transport::AdapterId> {
    match struct_field(fields, name) {
        Some(daedalus_data::model::Value::List(values)) => values
            .iter()
            .filter_map(|value| match value {
                daedalus_data::model::Value::String(step) => {
                    Some(daedalus_transport::AdapterId::new(step.to_string()))
                }
                _ => None,
            })
            .collect(),
        _ => Vec::new(),
    }
}

pub(super) fn adapter_path_field(
    fields: &[daedalus_data::model::StructFieldValue],
    name: &str,
) -> Vec<daedalus_registry::capability::AdapterPathStep> {
    match struct_field(fields, name) {
        Some(daedalus_data::model::Value::List(values)) => {
            values.iter().filter_map(parse_adapter_path_step).collect()
        }
        _ => Vec::new(),
    }
}

fn parse_adapter_path_step(
    value: &daedalus_data::model::Value,
) -> Option<daedalus_registry::capability::AdapterPathStep> {
    let fields = struct_fields(value)?;
    let kind = parse_adapt_kind(string_field(fields, "kind")?)?;
    let mut cost = daedalus_transport::AdaptCost::new(kind);
    if let Some(weight) = u64_field(fields, "cost") {
        cost.cpu_ns = weight.min(u64::from(u32::MAX)) as u32;
    }
    Some(daedalus_registry::capability::AdapterPathStep {
        adapter: daedalus_transport::AdapterId::new(string_field(fields, "adapter")?.to_string()),
        from: daedalus_transport::TypeKey::new(string_field(fields, "from")?.to_string()),
        to: daedalus_transport::TypeKey::new(string_field(fields, "to")?.to_string()),
        kind,
        access: access_field(fields, "access").unwrap_or(daedalus_transport::AccessMode::Read),
        cost,
        requires_gpu: bool_field(fields, "requires_gpu").unwrap_or(false),
        residency: residency_field(fields, "residency"),
        layout: string_field(fields, "layout").map(daedalus_transport::Layout::new),
    })
}

fn parse_adapt_kind(name: &str) -> Option<daedalus_transport::AdaptKind> {
    daedalus_transport::AdaptKind::from_str(name).ok()
}

pub(super) fn u64_field(
    fields: &[daedalus_data::model::StructFieldValue],
    name: &str,
) -> Option<u64> {
    match struct_field(fields, name)? {
        daedalus_data::model::Value::Int(value) => u64::try_from(*value).ok(),
        _ => None,
    }
}

pub(super) fn typeexpr_transport_key(
    ty: &daedalus_data::model::TypeExpr,
) -> daedalus_transport::TypeKey {
    daedalus_registry::typeexpr_transport_key(ty)
}
