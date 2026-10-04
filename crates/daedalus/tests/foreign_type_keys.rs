//! Port keys for types owned by other crates are deterministic: they come from the owning type
//! (`#[type_key]`), an explicit port `type_key`, or the plugin's `foreign_types(...)`, never from
//! whichever code happened to register the type first. Unkeyed foreign types fail loudly.

use daedalus::runtime::NodeError;
use daedalus::runtime::plugins::{PluginError, RegistryPluginExt};
use daedalus::transport::{TransportError, TypeKey};
use daedalus::{PluginRegistry, adapt, macros::node, plugin};

/// Stands in for a library (e.g. `styx-core`) whose optional `daedalus` feature owns its key.
mod styx {
    #[daedalus::type_key("styx:framelease")]
    pub struct FrameLease {
        pub width: u32,
    }

    #[daedalus::plugin(id = "styx", types(FrameLease))]
    pub struct StyxPlugin;
}

#[node(id = "frame_width", inputs("frame"), outputs("width"))]
fn frame_width(frame: &styx::FrameLease) -> Result<u32, NodeError> {
    Ok(frame.width)
}

#[adapt(id = "consumer.frame_to_gray", to = "consumer:gray8")]
fn frame_to_gray(frame: &styx::FrameLease) -> Result<Vec<u8>, TransportError> {
    Ok(vec![0; frame.width as usize])
}

#[plugin(
    id = "consumer",
    deps("styx"),
    nodes(frame_width),
    adapters(frame_to_gray)
)]
struct ConsumerPlugin;

fn port_key(registry: &PluginRegistry, node: &str, port: &str) -> TypeKey {
    let decl = registry
        .transport_capabilities
        .nodes()
        .values()
        .find(|decl| decl.id.0 == node)
        .unwrap_or_else(|| panic!("node {node} is not registered"));
    decl.inputs
        .iter()
        .chain(&decl.outputs)
        .find(|decl| decl.name == port)
        .map(|decl| decl.type_key.clone())
        .unwrap_or_else(|| panic!("node {node} has no port {port}"))
}

#[test]
fn owner_declared_keys_do_not_depend_on_install_order() {
    let mut registry = PluginRegistry::new();
    // The consumer installs before the library that registers the type.
    registry.install_plugin(&ConsumerPlugin::new()).unwrap();
    registry.install_plugin(&styx::StyxPlugin::new()).unwrap();
    registry.freeze().unwrap();

    let key = TypeKey::new("styx:framelease");
    assert_eq!(port_key(&registry, "consumer:frame_width", "frame"), key);
    let adapter = registry
        .transport_capabilities
        .adapters()
        .values()
        .find(|decl| decl.id.as_str() == "consumer.frame_to_gray")
        .unwrap();
    assert_eq!(adapter.from, key);
    let recorded = registry.boundary_types()[&key];
    assert!(
        recorded.type_name.ends_with("styx::FrameLease"),
        "{recorded}"
    );
}

#[node(
    id = "gray_len",
    inputs(port(name = "image", type_key = "image:gray8")),
    outputs("len")
)]
fn gray_len(image: &image::GrayImage) -> Result<u64, NodeError> {
    Ok(image.len() as u64)
}

#[node(id = "rgb_len", inputs("image"), outputs("len"))]
fn rgb_len(image: &image::RgbImage) -> Result<u64, NodeError> {
    Ok(image.len() as u64)
}

#[plugin(
    id = "imaging",
    foreign_types(image::RgbImage = "image:rgb8"),
    nodes(gray_len, rgb_len)
)]
struct ImagingPlugin;

#[test]
fn foreign_types_get_keys_from_the_port_or_the_plugin() {
    let mut registry = PluginRegistry::new();
    registry.install_plugin(&ImagingPlugin::new()).unwrap();
    registry.freeze().unwrap();

    assert_eq!(
        port_key(&registry, "imaging:gray_len", "image"),
        TypeKey::new("image:gray8")
    );
    assert_eq!(
        port_key(&registry, "imaging:rgb_len", "image"),
        TypeKey::new("image:rgb8")
    );
    let types = registry.boundary_types();
    assert!(
        types[&TypeKey::new("image:gray8")]
            .type_name
            .contains("ImageBuffer")
    );
    assert!(
        types[&TypeKey::new("image:rgb8")]
            .type_name
            .contains("Rgb<u8>")
    );
}

#[node(id = "gray_alpha_len", inputs("image"), outputs("len"))]
fn gray_alpha_len(image: &image::GrayAlphaImage) -> Result<u64, NodeError> {
    Ok(image.len() as u64)
}

#[plugin(id = "unkeyed", nodes(gray_alpha_len))]
struct UnkeyedPlugin;

#[test]
fn unkeyed_foreign_types_fail_loudly() {
    let mut registry = PluginRegistry::new();
    let err = registry.install_plugin(&UnkeyedPlugin::new()).unwrap_err();
    let PluginError::UnkeyedForeignType {
        ref owner,
        ref port,
        rust_type,
        ref key,
    } = err
    else {
        panic!("unexpected error: {err}");
    };
    assert_eq!(owner, "unkeyed:gray_alpha_len");
    assert_eq!(port, "image");
    assert!(rust_type.starts_with("image::"), "{rust_type}");
    assert!(key.as_str().starts_with("rust:image::"), "{key}");
    let message = err.to_string();
    for hint in ["type_key = ", "foreign_types(", "`daedalus` integration"] {
        assert!(message.contains(hint), "{message}");
    }
}

#[node(id = "rgb_width", inputs("image"), outputs("width"))]
fn rgb_width(image: &image::RgbImage) -> Result<u32, NodeError> {
    Ok(image.width())
}

#[plugin(id = "unmapped", nodes(rgb_width))]
struct UnmappedRgbPlugin;

#[plugin(
    id = "remapped",
    foreign_types(image::RgbImage = "other:rgb8"),
    nodes(rgb_width)
)]
struct RemappedRgbPlugin;

#[test]
fn foreign_type_mappings_stay_in_their_registry() {
    let mut mapped = PluginRegistry::new();
    mapped.install_plugin(&ImagingPlugin::new()).unwrap();
    // Another registry neither sees the mapping nor conflicts with it.
    let err = PluginRegistry::new()
        .install_plugin(&UnmappedRgbPlugin::new())
        .unwrap_err();
    assert!(
        matches!(err, PluginError::UnkeyedForeignType { .. }),
        "{err}"
    );
    let mut remapped = PluginRegistry::new();
    remapped.install_plugin(&RemappedRgbPlugin::new()).unwrap();
    assert_eq!(
        port_key(&remapped, "remapped:rgb_width", "image"),
        TypeKey::new("other:rgb8")
    );
    assert_eq!(
        port_key(&mapped, "imaging:rgb_len", "image"),
        TypeKey::new("image:rgb8")
    );
}

#[test]
fn one_key_names_one_rust_type() {
    struct Left;
    struct Right;
    let mut registry = PluginRegistry::new();
    registry
        .register_boundary_type::<Left>("test:shared")
        .unwrap();
    registry
        .register_boundary_type::<Left>("test:shared")
        .unwrap();
    let err = registry
        .register_boundary_type::<Right>("test:shared")
        .unwrap_err();
    assert!(
        matches!(err, PluginError::BoundaryTypeConflict(ref conflict) if conflict.key.as_str() == "test:shared"),
        "{err}"
    );
}
