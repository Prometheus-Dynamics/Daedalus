use daedalus::runtime::NodeError;
use daedalus::runtime::plugins::{PluginError, RegistryPluginExt};
use daedalus::{PluginRegistry, macros::node, plugin};

// `image` types declare no Daedalus key: the port, or the plugin, has to.
#[node(
    id = "gray_len",
    inputs(port(name = "image", type_key = "ui:gray8")),
    outputs(port(name = "len", type_key = "ui:len"))
)]
fn gray_len(image: &image::GrayImage) -> Result<u64, NodeError> {
    Ok(image.len() as u64)
}

#[node(id = "rgb_len", inputs("image"), outputs("len"))]
fn rgb_len(image: &image::RgbImage) -> Result<u64, NodeError> {
    Ok(image.len() as u64)
}

#[plugin(
    id = "ui.keyed",
    foreign_types(image::RgbImage = "ui:rgb8"),
    nodes(gray_len, rgb_len)
)]
struct UiKeyedPlugin;

// Without either, install fails loudly instead of using an order-dependent `rust:` key.
#[node(id = "luma_alpha_len", inputs("image"), outputs("len"))]
fn luma_alpha_len(image: &image::GrayAlphaImage) -> Result<u64, NodeError> {
    Ok(image.len() as u64)
}

#[plugin(id = "ui.unkeyed", nodes(luma_alpha_len))]
struct UiUnkeyedPlugin;

fn main() {
    let mut registry = PluginRegistry::new();
    registry
        .install_plugin(&UiKeyedPlugin::new())
        .expect("keyed foreign types install");
    let err = registry
        .install_plugin(&UiUnkeyedPlugin::new())
        .expect_err("unkeyed foreign type must fail");
    assert!(matches!(err, PluginError::UnkeyedForeignType { .. }), "{err}");
}
