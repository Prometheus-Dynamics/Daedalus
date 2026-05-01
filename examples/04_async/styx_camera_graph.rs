#[cfg(feature = "styx-camera-example")]
use std::time::Duration;

#[cfg(feature = "styx-camera-example")]
use daedalus::{
    engine::{Engine, EngineConfig, GpuBackend, MetricsLevel},
    macros::{node, plugin},
    runtime::{NodeError, plugins::PluginRegistry},
};
#[cfg(feature = "styx-camera-example")]
use styx::extras::preview_window::PreviewWindow;
#[cfg(feature = "styx-camera-example")]
use styx::prelude::*;

#[cfg(feature = "styx-camera-example")]
#[node(id = "styx.frame.rotate180", inputs("frame"), outputs("frame"))]
fn rotate180(frame: FrameLease) -> Result<FrameLease, NodeError> {
    transform_packed_frame(
        &frame,
        FrameTransform {
            rotation: Rotation90::Deg180,
            mirror: false,
        },
    )
    .map_err(|err| NodeError::Handler(err.to_string()))
}

#[cfg(feature = "styx-camera-example")]
#[node(id = "styx.frame.mirror", inputs("frame"), outputs("frame"))]
fn mirror(frame: FrameLease) -> Result<FrameLease, NodeError> {
    transform_packed_frame(
        &frame,
        FrameTransform {
            rotation: Rotation90::Deg0,
            mirror: true,
        },
    )
    .map_err(|err| NodeError::Handler(err.to_string()))
}

#[cfg(feature = "styx-camera-example")]
#[node(id = "styx.frame.mark", inputs("frame"), outputs("frame"))]
fn mark(mut frame: FrameLease) -> Result<FrameLease, NodeError> {
    frame.meta_mut().timestamp = frame.meta().timestamp.saturating_add(1);
    Ok(frame)
}

#[cfg(feature = "styx-camera-example")]
#[plugin(id = "example.async.styx_camera", nodes(rotate180, mirror, mark))]
struct StyxCameraPlugin;

#[cfg(feature = "styx-camera-example")]
fn build_runtime() -> Result<
    daedalus::engine::HostGraph<daedalus::runtime::handler_registry::HandlerRegistry>,
    Box<dyn std::error::Error>,
> {
    let mut registry = PluginRegistry::new();
    let plugin = StyxCameraPlugin::new();
    registry.install(&plugin)?;

    let rotate = plugin.rotate180.alias("rotate");
    let mirror = plugin.mirror.alias("mirror");
    let mark = plugin.mark.alias("mark");
    let graph = registry
        .graph_builder()?
        .inputs(|g| {
            g.input("frame");
        })
        .outputs(|g| {
            g.output("preview");
        })
        .nodes(|g| {
            g.add_handle(&rotate);
            g.add_handle(&mirror);
            g.add_handle(&mark);
        })
        .try_edges(|g| {
            let rotate = g.node("rotate");
            let mirror = g.node("mirror");
            let mark = g.node("mark");
            g.try_connect("frame", &rotate.input("frame"))?;
            g.try_connect(&rotate.output("frame"), &mirror.input("frame"))?;
            g.try_connect(&mirror.output("frame"), &mark.input("frame"))?;
            g.try_connect(&mark.output("frame"), "preview")?;
            Ok(())
        })?
        .build();

    let engine = Engine::new(
        EngineConfig::from(GpuBackend::Cpu).with_metrics_level(MetricsLevel::Detailed),
    )?;
    Ok(engine.compile_registry(&registry, graph)?)
}

#[cfg(feature = "styx-camera-example")]
fn selected_mode<'a>(device: &'a ProbedDevice, selected: &SelectedCamera) -> Option<&'a Mode> {
    device
        .backends
        .iter()
        .find(|backend| backend.kind == selected.backend)
        .and_then(|backend| {
            backend
                .descriptor
                .modes
                .iter()
                .find(|mode| mode.id == selected.mode)
        })
}

#[cfg(feature = "styx-camera-example")]
fn main() -> Result<(), Box<dyn std::error::Error>> {
    let selected = CameraRequest::new()
        .backend_priority([BackendKind::V4l2, BackendKind::Libcamera])
        .format_priority([FourCc::new(*b"YUYV"), FourCc::new(*b"RGB4")])
        .max_resolution(1280, 720)
        .fastest_interval()
        .select()?;
    println!(
        "camera: {} backend={:?} mode={:?}",
        selected.device.identity.display, selected.backend, selected.mode
    );

    let mode = selected_mode(&selected.device, &selected)
        .cloned()
        .ok_or("selected camera mode was not found in descriptor")?;
    let handle = selected.start()?;
    let mut runtime = build_runtime()?;
    let mut preview = PreviewWindow::for_mode("daedalus styx graph", &mode).ok();

    let mut frames = 0usize;
    while frames < 240 {
        match handle.recv_blocking(Duration::from_millis(16)) {
            RecvOutcome::Data(frame) => {
                runtime.push("frame", frame);
                let telemetry = runtime.tick()?;
                for processed in runtime.drain_owned::<FrameLease>("preview")? {
                    frames += 1;
                    if let Some(window) = preview.as_mut()
                        && !window.show_if_open(&processed)
                    {
                        handle.stop();
                        return Ok(());
                    }
                    if frames.is_multiple_of(30) {
                        println!("#{frames}");
                        println!("{}", telemetry.compact_snapshot());
                    }
                }
            }
            RecvOutcome::Empty => continue,
            RecvOutcome::Closed => break,
        }
    }

    handle.stop();
    Ok(())
}

#[cfg(not(feature = "styx-camera-example"))]
fn main() {
    println!(
        "styx camera graph requires hardware support: cargo run -p daedalus-examples --features styx-camera-example --bin styx_camera_graph"
    );
}
