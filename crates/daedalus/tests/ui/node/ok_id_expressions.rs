use daedalus::runtime::NodeError;
use daedalus::runtime::plugins::RegistryPluginExt;
use daedalus::transport::TransportError;
use daedalus::{PluginRegistry, adapt, macros::node, plugin};

macro_rules! my_ids {
    ($name:ident) => {
        concat!("eidos.", stringify!($name))
    };
}

const SCALE_ID: &str = "eidos.scale";

#[node(id = concat!("eidos.", "blur"), inputs("value"), outputs("out"))]
fn blur(value: i32) -> Result<i32, NodeError> {
    Ok(value)
}

#[node(id = my_ids!(sharpen), inputs("value"), outputs("out"))]
fn sharpen(value: i32) -> Result<i32, NodeError> {
    Ok(value)
}

#[node(id = SCALE_ID, inputs("value"), outputs("out"))]
fn scale(value: i32) -> Result<i32, NodeError> {
    Ok(value)
}

#[adapt(id = my_ids!(widen), from = "ui:narrow", to = "ui:wide", kind = "materialize")]
fn widen(value: &i32) -> Result<i64, TransportError> {
    Ok(i64::from(*value))
}

#[plugin(id = my_ids!(plugin), nodes(blur, sharpen, scale), adapters(widen))]
struct EidosPlugin;

fn main() {
    assert_eq!(BlurNode::ID, "eidos.blur");
    assert_eq!(SharpenNode::node_decl().expect("decl").id.0, "eidos.sharpen");
    assert_eq!(ScaleNode::ID, "eidos.scale");
    let mut registry = PluginRegistry::new();
    registry
        .install_plugin(&EidosPlugin::new())
        .expect("install plugin");
}
