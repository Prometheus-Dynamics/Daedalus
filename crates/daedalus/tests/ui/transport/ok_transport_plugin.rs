use daedalus::runtime::NodeError;
use daedalus::runtime::plugins::RegistryPluginExt;
use daedalus::transport::TransportError;
use daedalus::{PluginRegistry, adapt, device, macros::node, plugin, type_key};

#[type_key("ui:frame")]
#[derive(Clone)]
struct Frame(i32);

#[adapt(id = "ui.frame_branch", from = "ui:frame", to = "ui:frame", kind = "branch")]
fn branch_frame(frame: &Frame) -> Result<Frame, TransportError> {
    Ok(frame.clone())
}

#[derive(Clone)]
struct GpuFrame(Frame);

fn download_frame(frame: &GpuFrame) -> Result<Frame, TransportError> {
    Ok(frame.0.clone())
}

#[device(
    id = "ui.device.frame",
    cpu = "ui:frame",
    device = "ui:frame@gpu",
    download = download_frame
)]
fn upload_frame(frame: &Frame) -> Result<GpuFrame, TransportError> {
    Ok(GpuFrame(frame.clone()))
}

#[node(id = "ui.source", outputs("frame"))]
fn source() -> Result<Frame, NodeError> {
    Ok(Frame(1))
}

#[node(id = "ui.sink", inputs("frame"))]
fn sink(frame: &mut Frame) -> Result<(), NodeError> {
    frame.0 += 1;
    Ok(())
}

#[plugin(
    id = "ui.transport",
    types(Frame),
    nodes(source, sink),
    adapters(branch_frame),
    devices(upload_frame)
)]
struct UiTransportPlugin;

fn main() {
    let mut registry = PluginRegistry::new();
    registry
        .install_plugin(&UiTransportPlugin::new())
        .expect("install plugin");
}
