use super::*;
use daedalus_data::model::Value;

#[test]
fn plugin_registry_type_registries_are_isolated() {
    struct LocalType;

    let mut left = PluginRegistry::bare();
    let right = PluginRegistry::bare();
    left.type_registry
        .register_type::<LocalType>(TypeExpr::Scalar(ValueType::Bool));

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
            .contains_key(&std::any::TypeId::of::<LocalType>())
    );
    assert!(
        !right
            .value_serializers
            .read()
            .contains_key(&std::any::TypeId::of::<LocalType>())
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
