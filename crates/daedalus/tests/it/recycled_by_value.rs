//! A recycled frame (`Payload::providing_recycled`) reaches a node that takes it by value: the
//! node's input is the payload's only holder, so `try_into_owned` moves the frame out and its
//! node goes back to the pool. A frame type that is not `Clone` has no fallback, so a payload
//! handed back would leave the node without its input.

use std::sync::atomic::{AtomicUsize, Ordering};

use daedalus::{
    engine::{Engine, EngineConfig},
    macros::{node, plugin},
    runtime::{NodeError, plugins::PluginRegistry},
    transport::{
        FrameFormatKind, FrameInterface, FramePlane, FrameResidency, FrameSource, Payload,
        RecyclePool, Residency, fourcc,
    },
    type_key,
};

const LEASE_KEY: &str = "test:recycled:lease";

static DROPS: AtomicUsize = AtomicUsize::new(0);

/// A camera lease: host-owned, not `Clone`, counted on drop.
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
    id = "test.recycled.owner",
    types(Lease),
    foreign_providers(Lease => FrameInterface)
)]
struct OwnerPlugin;

/// Takes the lease by value and reports its sequence; the lease drops here.
#[node(id = "test.recycled.consume", inputs("frame"), outputs("sequence"))]
fn consume(frame: Lease) -> Result<i64, NodeError> {
    Ok(frame.sequence as i64)
}

#[plugin(
    id = "test.recycled.consumer",
    deps("test.recycled.owner"),
    nodes(consume)
)]
struct ConsumerPlugin;

#[test]
fn a_by_value_node_takes_a_recycled_frame_and_its_node_is_reused() {
    let mut registry = PluginRegistry::new();
    registry
        .install(&OwnerPlugin::new())
        .expect("install owner");
    let consumer = ConsumerPlugin::new();
    registry.install(&consumer).expect("install consumer");
    let consume = consumer.consume.alias("consume");
    let graph = registry
        .graph_builder()
        .expect("graph builder")
        .input_typed::<Lease>("frame")
        .and_then(|b| b.try_node(&consume))
        .and_then(|b| b.try_connect("frame", &consume.inputs.frame))
        .and_then(|b| b.try_connect(&consume.outputs.sequence, "sequence"))
        .expect("wire graph")
        .build();
    let mut host = Engine::new(EngineConfig::default())
        .expect("engine")
        .compile_registry(&registry, graph)
        .expect("compile");

    let pool = RecyclePool::<Lease>::new();
    for sequence in 0..8u64 {
        let lease = Lease {
            pixels: vec![1, 2, 3],
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
        assert_eq!(host.take::<i64>("sequence"), Some(sequence as i64));
        assert_eq!(
            pool.idle(),
            1,
            "the node is back on the pool after the move"
        );
    }
    assert_eq!(
        DROPS.load(Ordering::SeqCst),
        8,
        "each lease dropped once, in the node"
    );
}
