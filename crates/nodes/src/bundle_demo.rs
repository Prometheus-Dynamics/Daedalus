//! Demo bundle with built-in handlers and descriptors for producer/decoder/sink using typed payloads.

use crate::declare_plugin;
use daedalus_macros::node;
use daedalus_runtime::NodeError;
use daedalus_runtime::handler_registry::HandlerRegistry;
use daedalus_runtime::plugins::PluginRegistry;

#[derive(Clone, Debug, serde::Deserialize)]
pub struct Frame {
    pub bytes: Vec<u8>,
}

#[derive(Clone, Debug, serde::Deserialize)]
pub struct Detection {
    pub id: i32,
}

declare_plugin!(DemoPlugin, "demo", [frame_src, decode, sink]);

/// Convenience for non-plugin consumers: install and get handlers in one shot.
pub fn install_bundle(registry: &mut PluginRegistry) -> Result<HandlerRegistry, &'static str> {
    registry.merge::<frame_src>()?;
    registry.merge::<decode>()?;
    registry.merge::<sink>()?;
    Ok(registry.take_handlers())
}

#[node(id = "demo:frame_src", bundle = "demo", outputs("frame"))]
fn frame_src() -> Result<Frame, NodeError> {
    Ok(Frame {
        bytes: b"frame".to_vec(),
    })
}

#[node(
    id = "demo:decode",
    bundle = "demo",
    inputs("frame"),
    outputs("detections")
)]
fn decode(frame: Frame) -> Result<Vec<Detection>, NodeError> {
    Ok(vec![Detection {
        id: frame.bytes.len() as i32,
    }])
}

#[node(id = "demo:sink", bundle = "demo", inputs("detections"))]
fn sink(detections: Vec<Detection>) -> Result<(), NodeError> {
    println!("demo detections: {:?}", detections);
    Ok(())
}
