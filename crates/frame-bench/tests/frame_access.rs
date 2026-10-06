//! `daedalus:frame` v2 access costs on the synthetic camera: a consumer that only reads
//! metadata and fds (a GPU importer) never maps or syncs a frame, and a CPU consumer maps each
//! buffer once and begins one CPU access per frame and plane.

use daedalus::{
    data::model::TypeExpr,
    engine::{Engine, EngineConfig},
    macros::{node, plugin},
    runtime::NodeError,
    transport::{FrameFormatKind, FrameResidency, FrameSource, FrameView, PlaneMapping},
};
use daedalus_frame_bench::{
    FRAME_INTERFACE_KEY, FrameBacking, FrameBuffer, FrameFeed, FrameSourceConfig,
    SyntheticFrameSource, frame_bench_registry,
};

/// What a dma-buf importer reads: geometry, format and each plane's fd, offset and stride.
#[node(id = "import", inputs("frame"), outputs("fds"))]
fn import(frame: FrameView<'_>) -> Result<i64, NodeError> {
    std::hint::black_box((
        frame.width(),
        frame.height(),
        frame.format(),
        frame.modifier(),
    ));
    assert_eq!(frame.format_kind(), FrameFormatKind::Pixel);
    let mut fds = 0;
    for plane in frame.planes() {
        assert_eq!(plane.mapping, PlaneMapping::Cached);
        std::hint::black_box((plane.offset, plane.stride, plane.len));
        fds += i64::from(plane.dmabuf_fd.is_some());
    }
    Ok(fds)
}

/// A CPU consumer: sums the luma plane.
#[node(id = "luma_sum", inputs("frame"), outputs("sum"))]
fn luma_sum(frame: FrameView<'_>) -> Result<i64, NodeError> {
    let luma = frame
        .plane_bytes(0)
        .ok_or_else(|| NodeError::InvalidInput("plane 0 not CPU-readable".into()))?;
    Ok(luma.iter().map(|&p| i64::from(p)).sum())
}

#[plugin(id = "frame_access_test", nodes(import, luma_sum))]
struct AccessPlugin;

/// Backings available here (dma-heap needs `/dev/dma_heap` access).
fn backings() -> Vec<FrameBacking> {
    [
        FrameBacking::DmaHeap,
        FrameBacking::Memfd,
        FrameBacking::Heap,
    ]
    .into_iter()
    .filter(|&backing| FrameBuffer::new(backing, 16, |_| {}).is_ok())
    .collect()
}

/// Run `ticks` frames from a 4-buffer ring on `backing` through `node`; returns the source.
fn drive(node: &str, backing: FrameBacking, ticks: usize) -> SyntheticFrameSource {
    let mut registry = frame_bench_registry().expect("registry");
    registry.install(&AccessPlugin::new()).expect("install");
    let consumer = daedalus::NodeHandle::new(format!("frame_access_test:{node}")).alias("c");
    let output = if node == "import" { "fds" } else { "sum" };
    let graph = registry
        .graph_builder()
        .expect("builder")
        .input_as("frame", TypeExpr::opaque(FRAME_INTERFACE_KEY))
        .try_node(&consumer)
        .and_then(|b| b.try_connect("frame", &consumer.input("frame")))
        .and_then(|b| b.try_connect(&consumer.output(output), "out"))
        .expect("wire")
        .build();
    let mut host = Engine::new(EngineConfig::default())
        .expect("engine")
        .compile_registry(&registry, graph)
        .expect("compile");
    let mut source = SyntheticFrameSource::new(FrameSourceConfig {
        width: 64,
        height: 48,
        buffers: 4,
        backing: Some(backing),
        feed: FrameFeed::Interface,
    })
    .expect("frame source");
    for _ in 0..ticks {
        host.push_payload("frame", source.next_payload());
        host.tick().expect("tick");
        let out = host.take::<i64>("out").expect("output");
        if node == "import" {
            assert_eq!(out, i64::from(backing == FrameBacking::DmaHeap));
        }
    }
    source
}

#[test]
fn fd_only_consumers_never_map_frames() {
    for backing in backings() {
        let source = drive("import", backing, 32);
        for frame in source.frames() {
            assert_eq!(frame.cpu_access_count(), 0, "{backing:?}: plane_data calls");
            assert_eq!(frame.map_count(), 0, "{backing:?}: mmap calls");
        }
    }
}

#[test]
fn cpu_consumers_map_once_and_access_once_per_frame_and_plane() {
    for backing in backings() {
        let source = drive("luma_sum", backing, 32);
        for frame in source.frames() {
            // 32 frames over a ring of 4: each buffer captured 8 times, one plane each.
            assert_eq!(frame.cpu_access_count(), 8, "{backing:?}");
            let maps = u64::from(backing != FrameBacking::Heap);
            assert_eq!(
                frame.map_count(),
                maps,
                "{backing:?}: mapped once, then cached"
            );
            let cpu = frame.residency() == FrameResidency::Cpu;
            assert_eq!(cpu, backing == FrameBacking::Heap, "{backing:?}");
        }
    }
}
