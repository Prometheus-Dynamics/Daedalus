//! A recycled frame (`Payload::providing_recycled`) reaches a node that keeps it across a tick as
//! a `SharedFrame` in its state, downcasts the kept frame to its lease type, and returns it. A
//! second node reads the returned frame through `FrameView` in place, and the pool reuses its
//! few nodes for every frame.

use std::sync::atomic::{AtomicUsize, Ordering};

use daedalus::{
    engine::{Engine, EngineConfig},
    macros::{node, plugin},
    runtime::{NodeError, plugins::PluginRegistry},
    transport::{
        FrameFormatKind, FrameInterface, FramePlane, FrameResidency, FrameSource, FrameView,
        Payload, RecyclePool, Residency, SharedFrame, fourcc,
    },
    type_key,
};

const LEASE_KEY: &str = "test:recycled:identity";

static DROPS: AtomicUsize = AtomicUsize::new(0);

/// A camera lease: host-owned, counted on drop.
#[type_key(LEASE_KEY)]
struct Lease {
    pixels: Vec<u8>,
    sequence: u64,
}

impl Drop for Lease {
    fn drop(&mut self) {
        DROPS.fetch_add(1, Ordering::SeqCst);
    }
}

impl FrameSource for Lease {
    fn width(&self) -> u32 {
        self.pixels.len() as u32
    }
    fn height(&self) -> u32 {
        1
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
        (index == 0).then(|| FramePlane::cpu(&self.pixels, self.width().into()))
    }
    fn plane_data(&self, index: u32) -> Option<&[u8]> {
        (index == 0).then_some(&self.pixels[..])
    }
}

#[plugin(
    id = "test.identity.owner",
    types(Lease),
    foreign_providers(Lease => FrameInterface)
)]
struct OwnerPlugin;

#[derive(Default)]
struct Held(Option<SharedFrame>);

/// Keeps each frame for one tick: the frame it held is downcast to its lease, returned as an
/// output, and reported by sequence (`-1` on the first tick, when nothing is held).
#[node(
    id = "test.identity.hold",
    inputs("frame"),
    outputs("frame", "held"),
    state(Held)
)]
fn hold(frame: SharedFrame, state: &mut Held) -> Result<(Option<SharedFrame>, i64), NodeError> {
    let previous = state.0.replace(frame);
    let held = previous
        .as_ref()
        .and_then(|frame| frame.downcast_ref::<Lease>())
        .map_or(-1, |lease| lease.sequence as i64);
    Ok((previous, held))
}

/// Reads the returned frame in place through `FrameView`.
#[node(id = "test.identity.read", inputs("frame"), outputs("sequence"))]
fn read(frame: FrameView<'_>) -> Result<i64, NodeError> {
    Ok(frame.sequence() as i64)
}

#[plugin(
    id = "test.identity.nodes",
    deps("test.identity.owner"),
    nodes(hold, read)
)]
struct NodesPlugin;

#[test]
fn a_recycled_frame_kept_in_node_state_is_downcast_and_read_in_place() {
    let mut registry = PluginRegistry::new();
    registry
        .install(&OwnerPlugin::new())
        .expect("install owner");
    let nodes = NodesPlugin::new();
    registry.install(&nodes).expect("install nodes");
    let (hold, read) = (nodes.hold.alias("hold"), nodes.read.alias("read"));
    let graph = registry
        .graph_builder()
        .expect("graph builder")
        .input_typed::<Lease>("frame")
        .and_then(|b| b.try_node(&hold))
        .and_then(|b| b.try_node(&read))
        .and_then(|b| b.try_connect("frame", &hold.inputs.frame))
        .and_then(|b| b.try_connect(&hold.outputs.frame, &read.inputs.frame))
        .and_then(|b| b.try_connect(&hold.outputs.held, "held"))
        .and_then(|b| b.try_connect(&read.outputs.sequence, "sequence"))
        .expect("wire graph")
        .build();
    let mut host = Engine::new(EngineConfig::default())
        .expect("engine")
        .compile_registry(&registry, graph)
        .expect("compile");

    let pool = RecyclePool::<Lease>::new();
    for sequence in 0..8u64 {
        let lease = Lease {
            pixels: vec![sequence as u8; 3],
            sequence,
        };
        host.push_payload(
            "frame",
            Payload::providing_recycled::<Lease, FrameInterface>(
                LEASE_KEY,
                &pool,
                lease,
                Residency::Cpu,
            ),
        );
        host.tick().expect("tick");
        let previous = sequence as i64 - 1;
        assert_eq!(host.take::<i64>("held"), Some(previous));
        if sequence == 0 {
            assert_eq!(host.take::<i64>("sequence"), None, "nothing returned yet");
        } else {
            assert_eq!(host.take::<i64>("sequence"), Some(previous));
        }
    }
    drop(host);
    assert_eq!(
        DROPS.load(Ordering::SeqCst),
        8,
        "each lease dropped once, when its last holder let go"
    );
    assert!(
        (1..=4).contains(&pool.idle()),
        "the frames in flight share a few reused nodes: {}",
        pool.idle()
    );
}
