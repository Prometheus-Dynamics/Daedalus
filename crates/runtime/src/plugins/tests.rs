use super::*;
use daedalus_data::model::Value;

#[test]
fn plugin_registry_type_registries_are_isolated() {
    struct LocalType;

    let mut left = PluginRegistry::bare();
    let right = PluginRegistry::bare();
    left.type_registry
        .register_type::<LocalType>(TypeExpr::Scalar(ValueType::Bool))
        .unwrap();

    assert_eq!(
        left.type_registry.lookup_type::<LocalType>(),
        Some(TypeExpr::Scalar(ValueType::Bool))
    );
    assert_eq!(right.type_registry.lookup_type::<LocalType>(), None);
}

#[test]
fn plugin_registry_named_type_registries_are_isolated() {
    let mut left = PluginRegistry::bare();
    let right = PluginRegistry::bare();

    left.register_named_type(
        "test:registry:named",
        TypeExpr::Scalar(ValueType::Bool),
        HostExportPolicy::Value,
    )
    .expect("register named type");

    assert!(
        left.named_type_registry
            .lookup("test:registry:named")
            .is_some()
    );
    assert!(
        right
            .named_type_registry
            .lookup("test:registry:named")
            .is_none()
    );
}

#[test]
fn plugin_registry_value_serializers_are_isolated() {
    #[derive(Clone)]
    struct LocalType(bool);

    let mut left = PluginRegistry::bare();
    let right = PluginRegistry::bare();
    left.register_value_serializer::<LocalType, _>(|value| Value::Bool(value.0));

    assert!(
        left.value_serializers
            .read()
            .contains_key(&core::any::TypeId::of::<LocalType>())
    );
    assert!(
        !right
            .value_serializers
            .read()
            .contains_key(&core::any::TypeId::of::<LocalType>())
    );
}

#[test]
fn plugin_registry_transport_capabilities_are_isolated() {
    let mut left = PluginRegistry::bare();
    let right = PluginRegistry::bare();
    let key = TypeKey::new("test:isolated:type");

    left.register_transport_type_decl(key.clone(), TypeExpr::Scalar(ValueType::Bool))
        .expect("register transport type");

    assert!(left.transport_capabilities.type_decl(&key).is_some());
    assert!(right.transport_capabilities.type_decl(&key).is_none());
}

#[test]
fn builtin_numbers_have_one_key_per_rust_type() {
    let registry = PluginRegistry::new();
    let key = |value_type| typeexpr_transport_key(&TypeExpr::Scalar(value_type));
    let (i32_key, i64_key) = (key(ValueType::I32), key(ValueType::Int));
    assert_ne!(i32_key, i64_key);
    let adapters = registry.runtime_transport.adapters();
    let adapt = |id: &str, payload, target: &TypeKey| {
        let request = daedalus_transport::AdaptRequest::new(target.clone());
        adapters.adapt(&AdapterId::new(id), payload, &request)
    };

    let branched = adapt(
        "daedalus.builtin.branch.i64",
        Payload::owned(i64_key.clone(), 7_i64),
        &i64_key,
    );
    assert_eq!(branched.expect("branch").get_ref::<i64>(), Some(&7));
    let wrong_type = adapt(
        "daedalus.builtin.branch.i64",
        Payload::owned(i64_key.clone(), 7_i32),
        &i64_key,
    );
    assert!(
        wrong_type.is_err(),
        "a branch adapter only takes its own Rust type"
    );

    let widened = adapt(
        "daedalus.builtin.widen.i32_to_i64",
        Payload::owned(i32_key, -7_i32),
        &i64_key,
    );
    let widened = widened.expect("widen");
    assert_eq!(widened.type_key(), &i64_key);
    assert_eq!(widened.get_ref::<i64>(), Some(&-7));
}
