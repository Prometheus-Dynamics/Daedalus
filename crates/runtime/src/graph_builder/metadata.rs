use std::collections::BTreeMap;

use daedalus_data::model::Value;
use daedalus_registry::capability::{NodeDecl, PortDecl};
use daedalus_transport::{FreshnessPolicy, OverflowPolicy, PressurePolicy};

use crate::plan::{
    EDGE_CAPACITY_KEY, EDGE_FRESHNESS_LATEST_BY_SEQUENCE, EDGE_FRESHNESS_LATEST_BY_TIMESTAMP,
    EDGE_FRESHNESS_MAX_AGE, EDGE_FRESHNESS_MAX_LAG, EDGE_FRESHNESS_POLICY_KEY,
    EDGE_OVERFLOW_POLICY_KEY, EDGE_PRESSURE_BOUNDED, EDGE_PRESSURE_COALESCE,
    EDGE_PRESSURE_DROP_NEWEST, EDGE_PRESSURE_DROP_OLDEST, EDGE_PRESSURE_ERROR_ON_FULL,
    EDGE_PRESSURE_FIFO, EDGE_PRESSURE_LATEST_ONLY, EDGE_PRESSURE_POLICY_KEY, RuntimeEdgePolicy,
};

pub(super) fn metadata_from_node_decl(decl: &NodeDecl) -> BTreeMap<String, Value> {
    let mut metadata: BTreeMap<String, Value> = decl
        .metadata_json
        .iter()
        .filter_map(|(key, json)| {
            serde_json::from_str(json)
                .ok()
                .map(|value| (key.clone(), value))
        })
        .collect();
    decl.execution_kind.write_default_to_metadata(&mut metadata);
    metadata
}

pub(super) fn const_value_from_port_decl(port: &PortDecl) -> Option<Value> {
    port.const_value_json
        .as_deref()
        .and_then(|json| serde_json::from_str(json).ok())
}

pub(super) fn write_edge_policy_metadata(
    metadata: &mut BTreeMap<String, Value>,
    policy: &RuntimeEdgePolicy,
) {
    match &policy.pressure {
        PressurePolicy::LatestOnly => {
            metadata.insert(
                EDGE_PRESSURE_POLICY_KEY.to_string(),
                Value::String(EDGE_PRESSURE_LATEST_ONLY.into()),
            );
            metadata.remove(EDGE_CAPACITY_KEY);
        }
        PressurePolicy::Bounded { capacity, overflow } => {
            metadata.insert(
                EDGE_PRESSURE_POLICY_KEY.to_string(),
                Value::String(EDGE_PRESSURE_BOUNDED.into()),
            );
            metadata.insert(
                EDGE_CAPACITY_KEY.to_string(),
                Value::Int(i64::try_from(*capacity).unwrap_or(i64::MAX)),
            );
            if !matches!(overflow, OverflowPolicy::DropOldest) {
                metadata.insert(
                    EDGE_OVERFLOW_POLICY_KEY.to_string(),
                    Value::String(format!("{overflow:?}").into()),
                );
            }
        }
        PressurePolicy::DropNewest => {
            metadata.insert(
                EDGE_PRESSURE_POLICY_KEY.to_string(),
                Value::String(EDGE_PRESSURE_DROP_NEWEST.into()),
            );
            metadata.remove(EDGE_CAPACITY_KEY);
        }
        PressurePolicy::DropOldest => {
            metadata.insert(
                EDGE_PRESSURE_POLICY_KEY.to_string(),
                Value::String(EDGE_PRESSURE_DROP_OLDEST.into()),
            );
            metadata.remove(EDGE_CAPACITY_KEY);
        }
        PressurePolicy::ErrorOnFull => {
            metadata.insert(
                EDGE_PRESSURE_POLICY_KEY.to_string(),
                Value::String(EDGE_PRESSURE_ERROR_ON_FULL.into()),
            );
            metadata.remove(EDGE_CAPACITY_KEY);
        }
        PressurePolicy::Coalesce { .. } => {
            metadata.insert(
                EDGE_PRESSURE_POLICY_KEY.to_string(),
                Value::String(EDGE_PRESSURE_COALESCE.into()),
            );
            metadata.remove(EDGE_CAPACITY_KEY);
        }
        PressurePolicy::BufferAll => {
            metadata.insert(
                EDGE_PRESSURE_POLICY_KEY.to_string(),
                Value::String(EDGE_PRESSURE_FIFO.into()),
            );
            metadata.remove(EDGE_CAPACITY_KEY);
        }
    }

    match &policy.freshness {
        FreshnessPolicy::LatestBySequence => {
            metadata.insert(
                EDGE_FRESHNESS_POLICY_KEY.to_string(),
                Value::String(EDGE_FRESHNESS_LATEST_BY_SEQUENCE.into()),
            );
        }
        FreshnessPolicy::LatestByTimestamp => {
            metadata.insert(
                EDGE_FRESHNESS_POLICY_KEY.to_string(),
                Value::String(EDGE_FRESHNESS_LATEST_BY_TIMESTAMP.into()),
            );
        }
        FreshnessPolicy::PreserveAll => {
            metadata.remove(EDGE_FRESHNESS_POLICY_KEY);
        }
        FreshnessPolicy::MaxAge(_) => {
            metadata.insert(
                EDGE_FRESHNESS_POLICY_KEY.to_string(),
                Value::String(EDGE_FRESHNESS_MAX_AGE.into()),
            );
        }
        FreshnessPolicy::MaxLag { .. } => {
            metadata.insert(
                EDGE_FRESHNESS_POLICY_KEY.to_string(),
                Value::String(EDGE_FRESHNESS_MAX_LAG.into()),
            );
        }
    }
}
