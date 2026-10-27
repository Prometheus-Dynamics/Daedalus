//! A host-owned frame type with a `daedalus:frame` provider feeds nodes that take `FrameView`
//! and `ForeignRef`: the planner inserts the provider's `View` adapter and the nodes read the
//! host's buffer in place. A `SharedFrame` node keeps frames in its state and hands them on.

use std::sync::Arc;

use daedalus::{
    engine::{Engine, EngineConfig},
    macros::{node, plugin},
    runtime::{NodeError, plugins::PluginRegistry},
    transport::{
        ForeignInterface, ForeignRef, FrameFormatKind, FrameInterface, FramePlane, FrameResidency,
        FrameSource, FrameView, Payload, Residency, SharedFrame, TypeKey, fourcc,
    },
    type_key,
};

const FRAME_KEY: &str = "test:foreign:gray_frame";

/// Stand-in for a camera library's frame: host-owned, never copied.
#[type_key(FRAME_KEY)]
struct GrayFrame {
    pixels: Vec<u8>,
    width: u32,
    sequence: u64,
}

impl FrameSource for GrayFrame {
    fn width(&self) -> u32 {
        self.width
    }
    fn height(&self) -> u32 {
        self.pixels.len() as u32 / self.width
    }
    fn format(&self) -> u32 {
        fourcc(b"R8  ")
    }
    fn sequence(&self) -> u64 {
        self.sequence
    }
    fn format_kind(&self) -> FrameFormatKind {
        FrameFormatKind::Pixel
    }
    fn residency(&self) -> FrameResidency {
        FrameResidency::Cpu
    }
    fn plane_count(&self) -> u32 {
        1
    }
    fn plane(&self, index: u32) -> Option<FramePlane> {
        (index == 0).then(|| FramePlane::cpu(&self.pixels, self.width.into()))
    }
    fn plane_data(&self, index: u32) -> Option<&[u8]> {
        (index == 0).then_some(&self.pixels[..])
    }
}

/// The owner's integration: registers the type and its `daedalus:frame` provider.
#[plugin(
    id = "test.foreign.owner",
    types(GrayFrame),
    foreign_providers(GrayFrame => FrameInterface)
)]
struct OwnerPlugin;

#[node(id = "test.foreign.sum", inputs("frame"), outputs("sum", "ptr"))]
fn sum(frame: FrameView<'_>) -> Result<(i64, i64), NodeError> {
    let pixels = frame
        .plane_bytes(0)
        .ok_or(NodeError::InvalidInput("plane 0 not CPU-readable".into()))?;
    Ok((
        pixels.iter().map(|&p| i64::from(p)).sum(),
        pixels.as_ptr() as i64,
    ))
}

#[node(id = "test.foreign.sequence", inputs("frame"), outputs("sequence"))]
fn sequence(frame: ForeignRef<'_, FrameInterface>) -> Result<i64, NodeError> {
    Ok(frame.sequence() as i64)
}

#[plugin(
    id = "test.foreign.consumer",
    deps("test.foreign.owner"),
    nodes(sum, sequence)
)]
struct ConsumerPlugin;

#[test]
fn frame_views_read_host_frames_in_place() {
    let mut registry = PluginRegistry::new();
    let consumer = ConsumerPlugin::new();
    registry.install(&consumer).expect("install consumer");
    registry
        .install(&OwnerPlugin::new())
        .expect("install owner");
    let interface = &registry.foreign_interfaces()[&TypeKey::new("daedalus:frame")];
    assert_eq!(interface, <FrameInterface as ForeignInterface>::info());

    let (sum, sequence) = (
        consumer.sum.alias("sum"),
        consumer.sequence.alias("sequence"),
    );
    let graph = registry
        .graph_builder()
        .expect("graph builder")
        .input_typed::<GrayFrame>("frame")
        .and_then(|b| b.try_node(&sum))
        .and_then(|b| b.try_node(&sequence))
        .and_then(|b| b.try_connect("frame", &sum.inputs.frame))
        .and_then(|b| b.try_connect("frame", &sequence.inputs.frame))
        .and_then(|b| b.try_connect(&sum.outputs.sum, "sum"))
        .and_then(|b| b.try_connect(&sum.outputs.ptr, "ptr"))
        .and_then(|b| b.try_connect(&sequence.outputs.sequence, "sequence"))
        .expect("wire graph")
        .build();
    let mut host = Engine::new(EngineConfig::default())
        .expect("engine")
        .compile_registry(&registry, graph)
        .expect("compile");

    let adapters: Vec<_> = host
        .explain_plan()
        .edges
        .into_iter()
        .flat_map(|edge| edge.adapter_steps)
        .map(|step| step.as_str().to_string())
        .collect();
    let provider = format!("daedalus.foreign:{FRAME_KEY}->daedalus:frame");
    assert_eq!(
        adapters,
        [provider.clone(), provider],
        "one view per consumer"
    );

    let frame = Arc::new(GrayFrame {
        pixels: vec![1, 2, 3, 4, 5, 6],
        width: 3,
        sequence: 42,
    });
    let pixels = frame.pixels.as_ptr() as i64;
    host.push_payload(
        "frame",
        Payload::shared_with(FRAME_KEY, frame.clone(), Residency::Cpu, None, None),
    );
    host.tick().expect("tick");
    assert_eq!(host.take::<i64>("sum"), Some(21));
    assert_eq!(host.take::<i64>("ptr"), Some(pixels), "zero copy");
    assert_eq!(host.take::<i64>("sequence"), Some(42));
    drop(host);
    assert_eq!(
        Arc::strong_count(&frame),
        1,
        "every handle released its reference"
    );
}

#[derive(Default)]
struct Last(Option<SharedFrame>);

/// Delays frames by one tick: owned frames in node state, returned as an output.
#[node(
    id = "test.foreign.delay",
    inputs("frame"),
    outputs("frame"),
    state(Last)
)]
fn delay(frame: SharedFrame, state: &mut Last) -> Result<Option<SharedFrame>, NodeError> {
    Ok(state.0.replace(frame))
}

#[plugin(id = "test.foreign.delayer", deps("test.foreign.owner"), nodes(delay))]
struct DelayPlugin;

#[test]
fn shared_frames_are_kept_and_returned_without_copying() {
    let mut registry = PluginRegistry::new();
    let (consumer, delayer) = (ConsumerPlugin::new(), DelayPlugin::new());
    registry
        .install(&OwnerPlugin::new())
        .expect("install owner");
    registry.install(&consumer).expect("install consumer");
    registry.install(&delayer).expect("install delayer");
    let (delay, sum) = (delayer.delay.alias("delay"), consumer.sum.alias("sum"));
    let graph = registry
        .graph_builder()
        .expect("graph builder")
        .input_typed::<GrayFrame>("frame")
        .and_then(|b| b.try_node(&delay))
        .and_then(|b| b.try_node(&sum))
        .and_then(|b| b.try_connect("frame", &delay.inputs.frame))
        .and_then(|b| b.try_connect(&delay.outputs.frame, &sum.inputs.frame))
        .and_then(|b| b.try_connect(&delay.outputs.frame, "delayed"))
        .and_then(|b| b.try_connect(&sum.outputs.sum, "sum"))
        .and_then(|b| b.try_connect(&sum.outputs.ptr, "ptr"))
        .expect("wire graph")
        .build();
    let mut host = Engine::new(EngineConfig::default())
        .expect("engine")
        .compile_registry(&registry, graph)
        .expect("compile");
    let frame = |sequence| {
        Arc::new(GrayFrame {
            pixels: vec![sequence as u8; 6],
            width: 3,
            sequence,
        })
    };
    let (first, second) = (frame(1), frame(2));
    let feed = |frame: &Arc<GrayFrame>| {
        Payload::shared_with(FRAME_KEY, frame.clone(), Residency::Cpu, None, None)
    };
    host.push_payload("frame", feed(&first));
    host.tick().expect("tick 1");
    assert!(host.take_payload("delayed").is_none());
    assert_eq!(Arc::strong_count(&first), 2, "held in node state");

    host.push_payload("frame", feed(&second));
    host.tick().expect("tick 2");
    let payload = host.take_payload("delayed").expect("the first frame");
    let delayed = SharedFrame::from_payload(&payload).unwrap().unwrap();
    assert_eq!(delayed.handle().data(), Arc::as_ptr(&first).cast());
    // A `FrameView` downstream read the returned frame in place.
    assert_eq!(host.take::<i64>("sum"), Some(6));
    assert_eq!(host.take::<i64>("ptr"), Some(first.pixels.as_ptr() as i64));
    drop((payload, delayed));
    assert_eq!(Arc::strong_count(&first), 1, "released once handed on");
    assert_eq!(Arc::strong_count(&second), 2, "the next one is held");
    drop(host);
    assert_eq!(Arc::strong_count(&second), 1, "released with the node");
}
