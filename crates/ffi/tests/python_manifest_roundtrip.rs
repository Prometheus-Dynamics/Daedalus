use daedalus_data::model::{TypeExpr, ValueType};
use daedalus_ffi::PythonManifest;

#[test]
fn manifest_roundtrip_struct_enum() {
    // Small inline manifest to avoid external files.
    let manifest_json = r#"
    {
        "manifest_version": "1",
        "language": "python",
        "plugin": {"name":"demo_py","version":"0.1.0"},
        "nodes": [{
            "id":"demo_py:enum_struct",
            "py_module":"dummy",
            "py_function":"f",
            "stateful":false,
            "inputs":[{"name":"inp","ty":{"Scalar":"Int"}}],
            "outputs":[{"name":"out","ty":{"Enum":[{"name":"A","ty":null},{"name":"B","ty":{"Struct":[{"name":"x","ty":{"Scalar":"Int"}}]}}]}}]
        }]
    }"#;
    let manifest: PythonManifest = serde_json::from_str(manifest_json).expect("manifest parse");
    assert_eq!(manifest.manifest_version.as_deref(), Some("1"));
    let node = &manifest.nodes[0];
    assert_eq!(
        node.outputs[0].ty,
        TypeExpr::Enum(vec![
            daedalus_data::model::EnumVariant {
                name: "A".into(),
                ty: None
            },
            daedalus_data::model::EnumVariant {
                name: "B".into(),
                ty: Some(TypeExpr::Struct(vec![daedalus_data::model::StructField {
                    name: "x".into(),
                    ty: TypeExpr::Scalar(ValueType::Int)
                }]))
            }
        ])
    );
}
