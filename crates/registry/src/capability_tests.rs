use super::*;
use crate::diagnostics::{ConflictKind, RegistryErrorCode};
use daedalus_data::model::{TypeExpr, ValueType};
use daedalus_transport::{
    AccessMode, AdaptCost, AdaptKind, AdaptRequest, AdapterId, Residency, TypeKey,
};

#[test]
fn registers_capabilities_and_freezes_snapshot() {
    let mut reg = CapabilityRegistry::new();

    reg.register_type(
        TypeDecl::new("image:dynamic")
            .rust::<String>()
            .schema(TypeExpr::Scalar(ValueType::String))
            .export(ExportPolicy::Native),
    )
    .unwrap();
    reg.register_type(TypeDecl::new("image:gray8")).unwrap();
    reg.register_adapter(
        AdapterDecl::new("image.dynamic_to_gray", "image:dynamic", "image:gray8")
            .cost(AdaptCost::materialize())
            .access(AccessMode::Read),
    )
    .unwrap();
    reg.register_node(
        NodeDecl::new("images:gray")
            .input(PortDecl::new("img", "image:dynamic").access(AccessMode::Read))
            .output(PortDecl::new("out", "image:gray8")),
    )
    .unwrap();
    reg.register_serializer(SerializerDecl::new(
        "image.dynamic.value",
        "image:dynamic",
        ExportPolicy::Value,
    ))
    .unwrap();
    reg.register_device(DeviceDecl::new(
        "image.dynamic.gpu",
        "image:dynamic",
        "image:dynamic@gpu",
        "image.dynamic.upload",
        "image.dynamic.download",
    ))
    .unwrap();

    let snapshot = reg.snapshot();
    assert_eq!(snapshot.types.len(), 2);
    assert_eq!(snapshot.adapters.len(), 1);
    assert_eq!(snapshot.nodes.len(), 1);
    assert_eq!(snapshot.serializers.len(), 1);
    assert_eq!(snapshot.devices.len(), 1);
}

#[test]
fn rejects_duplicate_type_keys() {
    let mut reg = CapabilityRegistry::new();
    reg.register_type(TypeDecl::new("demo:type")).unwrap();

    let err = reg.register_type(TypeDecl::new("demo:type")).unwrap_err();
    assert_eq!(err.code(), RegistryErrorCode::Conflict);
    assert_eq!(err.conflict_kind(), Some(ConflictKind::Type));
    assert_eq!(err.conflict_key(), Some("demo:type"));
}

#[test]
fn node_ports_sort_deterministically() {
    let mut reg = CapabilityRegistry::new();
    reg.register_node(
        NodeDecl::new("demo:node")
            .input(PortDecl::new("z", "demo:z"))
            .input(PortDecl::new("a", "demo:a"))
            .output(PortDecl::new("out", "demo:out")),
    )
    .unwrap();

    let node = &reg.snapshot().nodes[0];
    assert_eq!(node.inputs[0].name, "a");
    assert_eq!(node.inputs[1].name, "z");
}

#[test]
fn resolves_cheapest_adapter_path() {
    let mut reg = CapabilityRegistry::new();
    reg.register_adapter(
        AdapterDecl::new("direct_materialize", "a", "c").cost(AdaptCost::materialize()),
    )
    .unwrap();
    reg.register_adapter(AdapterDecl::new("a_to_b_view", "a", "b").cost(AdaptCost::view()))
        .unwrap();
    reg.register_adapter(AdapterDecl::new("b_to_c_view", "b", "c").cost(AdaptCost::view()))
        .unwrap();

    let path = reg
        .resolve_adapter_path(&TypeKey::new("a"), &TypeKey::new("c"))
        .unwrap();
    assert_eq!(
        path.steps,
        vec![AdapterId::new("a_to_b_view"), AdapterId::new("b_to_c_view")]
    );
}

#[test]
fn reports_missing_adapter_path() {
    let reg = CapabilityRegistry::new();
    let err = reg
        .resolve_adapter_path(&TypeKey::new("a"), &TypeKey::new("b"))
        .unwrap_err();
    assert_eq!(err.code(), RegistryErrorCode::AdapterError);
}

#[test]
fn adapter_path_respects_request_constraints() {
    let mut reg = CapabilityRegistry::new();
    reg.register_adapter(
        AdapterDecl::new("cpu_path", "image:dynamic", "image:rgba")
            .cost(AdaptCost::view())
            .residency(Residency::Cpu),
    )
    .unwrap();
    reg.register_adapter(
        AdapterDecl::new("gpu_path", "image:dynamic", "image:rgba")
            .cost(AdaptCost::device_transfer())
            .residency(Residency::Gpu)
            .layout("rgba8-storage-texture"),
    )
    .unwrap();

    let request = AdaptRequest::new("image:rgba")
        .residency(Residency::Gpu)
        .layout("rgba8-storage-texture");
    let path = reg
        .resolve_adapter_path_for(&TypeKey::new("image:dynamic"), &request)
        .unwrap();

    assert_eq!(path.steps, vec![AdapterId::new("gpu_path")]);
}

#[test]
fn gpu_residency_request_can_target_registered_device_type() {
    let mut reg = CapabilityRegistry::new();
    reg.register_adapter(
        AdapterDecl::new("image.dynamic_to_rgba", "image:dynamic", "image:rgba")
            .cost(AdaptCost::materialize()),
    )
    .unwrap();
    reg.register_adapter(
        AdapterDecl::new("image.rgba_upload", "image:rgba", "image:rgba@gpu")
            .cost(AdaptCost::device_transfer())
            .requires_gpu(true)
            .residency(Residency::Gpu),
    )
    .unwrap();
    reg.register_adapter(
        AdapterDecl::new("image.rgba_download", "image:rgba@gpu", "image:rgba")
            .cost(AdaptCost::device_transfer())
            .requires_gpu(true)
            .residency(Residency::Cpu),
    )
    .unwrap();
    reg.register_device(DeviceDecl::new(
        "image.rgba.device",
        "image:rgba",
        "image:rgba@gpu",
        "image.rgba_upload",
        "image.rgba_download",
    ))
    .unwrap();

    let path = reg
        .resolve_adapter_path_for_with_context(
            &TypeKey::new("image:dynamic"),
            &AdaptRequest::new("image:rgba").residency(Residency::Gpu),
            &[],
            true,
        )
        .expect("gpu upload path");

    assert_eq!(
        path.steps,
        vec![
            AdapterId::new("image.dynamic_to_rgba"),
            AdapterId::new("image.rgba_upload")
        ]
    );
    assert_eq!(path.resolved_target, Some(TypeKey::new("image:rgba@gpu")));
}

#[test]
fn exclusive_same_type_request_requires_adapter_step() {
    let mut reg = CapabilityRegistry::new();
    reg.register_type(TypeDecl::new("frame")).unwrap();
    reg.register_adapter(
        AdapterDecl::new("frame.view", "frame", "frame")
            .cost(AdaptCost::view())
            .access(AccessMode::Modify),
    )
    .unwrap();
    reg.register_adapter(
        AdapterDecl::new("frame.branch", "frame", "frame")
            .cost(AdaptCost::new(AdaptKind::Branch))
            .access(AccessMode::Modify),
    )
    .unwrap();

    let exact = reg
        .resolve_adapter_path_for(
            &TypeKey::new("frame"),
            &AdaptRequest::new("frame").access(AccessMode::Modify),
        )
        .expect("non-exclusive same-type path");
    assert!(exact.steps.is_empty());

    let branched = reg
        .resolve_adapter_path_for(
            &TypeKey::new("frame"),
            &AdaptRequest::new("frame")
                .access(AccessMode::Modify)
                .exclusive(true),
        )
        .expect("exclusive same-type path");
    assert_eq!(branched.steps, vec![AdapterId::new("frame.branch")]);
}

#[test]
fn adapter_path_respects_feature_and_gpu_context() {
    let mut reg = CapabilityRegistry::new();
    reg.register_adapter(
        AdapterDecl::new("cpu_path", "image:dynamic", "image:rgba").cost(AdaptCost::materialize()),
    )
    .unwrap();
    reg.register_adapter(
        AdapterDecl::new("gpu_path", "image:dynamic", "image:rgba")
            .cost(AdaptCost::view())
            .feature_flag("gpu")
            .requires_gpu(true),
    )
    .unwrap();

    let request = AdaptRequest::new("image:rgba");
    let cpu = reg
        .resolve_adapter_path_for_with_context(&TypeKey::new("image:dynamic"), &request, &[], false)
        .unwrap();
    assert_eq!(cpu.steps, vec![AdapterId::new("cpu_path")]);

    let gpu_features = vec!["gpu".to_string()];
    let gpu = reg
        .resolve_adapter_path_for_with_context(
            &TypeKey::new("image:dynamic"),
            &request,
            &gpu_features,
            true,
        )
        .unwrap();
    assert_eq!(gpu.steps, vec![AdapterId::new("gpu_path")]);
}

#[test]
fn freeze_validates_cross_references() {
    let mut reg = CapabilityRegistry::new();
    reg.register_adapter(AdapterDecl::new("missing_target", "a", "b"))
        .unwrap();

    let err = reg.freeze().unwrap_err();
    assert_eq!(err.code(), RegistryErrorCode::MissingDependency);
}

#[test]
fn freeze_accepts_complete_capability_graph() {
    let mut reg = CapabilityRegistry::new();
    for key in ["image:dynamic", "image:dynamic@gpu"] {
        reg.register_type(TypeDecl::new(key)).unwrap();
    }
    reg.register_adapter(
        AdapterDecl::new("image.upload", "image:dynamic", "image:dynamic@gpu")
            .cost(AdaptCost::device_transfer()),
    )
    .unwrap();
    reg.register_adapter(
        AdapterDecl::new("image.download", "image:dynamic@gpu", "image:dynamic")
            .cost(AdaptCost::device_transfer()),
    )
    .unwrap();
    reg.register_device(DeviceDecl::new(
        "image.gpu",
        "image:dynamic",
        "image:dynamic@gpu",
        "image.upload",
        "image.download",
    ))
    .unwrap();
    reg.register_node(
        NodeDecl::new("images:shader")
            .input(
                PortDecl::new("img", "image:dynamic@gpu")
                    .access(AccessMode::Read)
                    .residency(Residency::Gpu),
            )
            .output(PortDecl::new("out", "image:dynamic@gpu").residency(Residency::Gpu)),
    )
    .unwrap();

    let snapshot = reg.freeze().unwrap();
    assert_eq!(snapshot.types.len(), 2);
    assert_eq!(snapshot.adapters.len(), 2);
    assert_eq!(snapshot.devices.len(), 1);
    assert_eq!(snapshot.nodes.len(), 1);
}

#[test]
fn registers_plugin_manifest_and_validates_provided_capabilities() {
    let mut reg = CapabilityRegistry::new();
    reg.register_type(TypeDecl::new("image:dynamic")).unwrap();
    reg.register_type(TypeDecl::new("image:gray8")).unwrap();
    reg.register_adapter(
        AdapterDecl::new("image.dynamic_to_gray", "image:dynamic", "image:gray8")
            .cost(AdaptCost::materialize()),
    )
    .unwrap();
    reg.register_adapter(
        AdapterDecl::new("image.gray_upload", "image:gray8", "image:gray8@gpu")
            .cost(AdaptCost::device_transfer()),
    )
    .unwrap();
    reg.register_adapter(
        AdapterDecl::new("image.gray_download", "image:gray8@gpu", "image:gray8")
            .cost(AdaptCost::device_transfer()),
    )
    .unwrap();
    reg.register_type(TypeDecl::new("image:gray8@gpu")).unwrap();
    reg.register_node(
        NodeDecl::new("images:gray")
            .input(PortDecl::new("image", "image:dynamic"))
            .output(PortDecl::new("gray", "image:gray8")),
    )
    .unwrap();
    reg.register_serializer(SerializerDecl::new(
        "image.gray8.value",
        "image:gray8",
        ExportPolicy::Value,
    ))
    .unwrap();
    reg.register_device(DeviceDecl::new(
        "image.gray8.gpu",
        "image:gray8",
        "image:gray8@gpu",
        "image.gray_upload",
        "image.gray_download",
    ))
    .unwrap();
    reg.register_plugin(
        PluginManifest::new("image-core")
            .version("1.0.0")
            .provided_type("image:dynamic")
            .provided_type("image:gray8")
            .provided_adapter("image.dynamic_to_gray")
            .provided_node("images:gray")
            .provided_serializer("image.gray8.value")
            .provided_device("image.gray8.gpu"),
    )
    .unwrap();

    let snapshot = reg.freeze().unwrap();
    assert_eq!(snapshot.plugins.len(), 1);
    assert_eq!(snapshot.plugins[0].id, "image-core");
    assert_eq!(snapshot.plugins[0].provided_types.len(), 2);
    assert_eq!(snapshot.plugins[0].provided_serializers.len(), 1);
    assert_eq!(snapshot.plugins[0].provided_devices.len(), 1);
}

#[test]
fn freeze_rejects_missing_plugin_dependency() {
    let mut reg = CapabilityRegistry::new();
    reg.register_plugin(PluginManifest::new("image-extra").dependency("image-core"))
        .unwrap();

    let err = reg.freeze().unwrap_err();
    assert_eq!(err.code(), RegistryErrorCode::MissingDependency);
}

#[test]
fn freeze_rejects_missing_plugin_provided_capability() {
    let mut reg = CapabilityRegistry::new();
    reg.register_plugin(PluginManifest::new("image-core").provided_type("image:dynamic"))
        .unwrap();

    let err = reg.freeze().unwrap_err();
    assert_eq!(err.code(), RegistryErrorCode::MissingDependency);
}

#[test]
fn filtered_snapshot_respects_feature_flags() {
    let mut reg = CapabilityRegistry::new();
    reg.register_type(TypeDecl::new("image:dynamic")).unwrap();
    reg.register_type(TypeDecl::new("image:dynamic@gpu").feature_flag("gpu"))
        .unwrap();
    reg.register_node(NodeDecl::new("images:cpu")).unwrap();
    reg.register_node(NodeDecl::new("images:gpu").feature_flag("gpu"))
        .unwrap();
    reg.register_plugin(PluginManifest::new("image-core"))
        .unwrap();
    reg.register_plugin(PluginManifest::new("image-gpu").feature_flag("gpu"))
        .unwrap();

    let cpu_snapshot = reg.freeze_filtered(&[]).unwrap();
    assert_eq!(
        cpu_snapshot
            .types
            .iter()
            .map(|decl| decl.key.as_str())
            .collect::<Vec<_>>(),
        vec!["image:dynamic"]
    );
    assert_eq!(
        cpu_snapshot
            .nodes
            .iter()
            .map(|decl| decl.id.0.as_str())
            .collect::<Vec<_>>(),
        vec!["images:cpu"]
    );
    assert_eq!(
        cpu_snapshot
            .plugins
            .iter()
            .map(|decl| decl.id.as_str())
            .collect::<Vec<_>>(),
        vec!["image-core"]
    );

    let gpu_features = vec!["gpu".to_string()];
    let gpu_snapshot = reg.freeze_filtered(&gpu_features).unwrap();
    assert_eq!(gpu_snapshot.types.len(), 2);
    assert_eq!(gpu_snapshot.nodes.len(), 2);
    assert_eq!(gpu_snapshot.plugins.len(), 2);
}
