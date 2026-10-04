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

#[test]
fn builtin_branch_adapters_accept_every_primitive_under_their_key() {
    let registry = PluginRegistry::new();
    let int_key = typeexpr_transport_key(&TypeExpr::Scalar(ValueType::Int));
    let request = daedalus_transport::AdaptRequest::new(int_key.clone());
    let adapters = registry.runtime_transport.adapters();
    for id in ["i64", "i32", "u32"] {
        let id = AdapterId::new(format!("daedalus.builtin.branch.{id}"));
        let branch = |payload| adapters.adapt(&id, payload, &request).expect("branch");
        let wide = branch(Payload::owned(int_key.clone(), 7_i64));
        assert_eq!(wide.get_ref::<i64>(), Some(&7));
        let narrow = branch(Payload::owned(int_key.clone(), 7_u32));
        assert_eq!(narrow.get_ref::<u32>(), Some(&7));
    }
}
