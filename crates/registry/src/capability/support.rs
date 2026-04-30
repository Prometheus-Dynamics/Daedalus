use std::collections::BTreeSet;

use crate::diagnostics::{ConflictKind, RegistryError, RegistryErrorCode, RegistryErrorCompute};

pub(super) fn duplicate_error(kind: ConflictKind, key: impl Into<String>) -> RegistryError {
    let key = key.into();
    RegistryError::new(
        RegistryErrorCode::Conflict,
        format!("duplicate {kind:?} {key}"),
    )
    .with_conflict_key(key.clone())
    .with_conflict_kind(kind)
    .with_payload(RegistryErrorCompute::Conflict { key, kind })
}

pub(super) fn missing_dependency(key: impl Into<String>) -> RegistryError {
    let key = key.into();
    let message = if let Some(hint) = missing_dependency_provider_hint(&key) {
        format!("missing capability dependency {key}; install provider `{hint}`")
    } else {
        format!("missing capability dependency {key}")
    };
    RegistryError::new(RegistryErrorCode::MissingDependency, message)
        .with_payload(RegistryErrorCompute::MissingDependency { key })
}

fn missing_dependency_provider_hint(key: &str) -> Option<&'static str> {
    if key.contains("typeexpr:") {
        return Some("daedalus.builtin.primitive_types");
    }
    if key.contains("serializer ") || key.contains("provided serializer") {
        return Some("daedalus.builtin.primitive_serializers");
    }
    if key.contains("branch") {
        return Some("daedalus.builtin.std_branch");
    }
    if key.contains("host_bridge")
        || key.contains("io.host_bridge")
        || key.contains("host boundary")
    {
        return Some("daedalus.builtin.host_boundary");
    }
    None
}

pub(super) type ActiveFeatureSet<'a> = BTreeSet<&'a str>;

pub(super) fn active_feature_set(active_features: &[String]) -> ActiveFeatureSet<'_> {
    active_features.iter().map(String::as_str).collect()
}

pub(super) fn features_enabled(
    feature_flags: &[String],
    active_features: &ActiveFeatureSet<'_>,
) -> bool {
    feature_flags
        .iter()
        .all(|flag| active_features.contains(flag.as_str()))
}
