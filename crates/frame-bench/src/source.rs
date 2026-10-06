//! A synthetic camera: a ring of frames in external memory, exposed through `daedalus:frame`.

use std::sync::Arc;
use std::sync::atomic::{AtomicU64, Ordering::Relaxed};
use std::time::Instant;

use daedalus::transport::{
    FRAME_INTERFACE_KEY, ForeignHandle, FrameFormatKind, FrameInterface, FramePlane,
    FrameResidency, FrameSource, Payload, PlaneMapping, Residency, fourcc,
};
use daedalus::type_key;

use crate::buffer::{FrameBacking, FrameBuffer};

/// Key of [`SyntheticFrame`] payloads fed as the owner type ([`FrameFeed::Owner`]).
pub const SYNTHETIC_FRAME_KEY: &str = "daedalus.frame_bench:synthetic_frame";

/// One GRAY8 frame in a [`FrameBuffer`]. Sequence and timestamp change per capture; the pixels
/// are written once.
#[type_key(SYNTHETIC_FRAME_KEY)]
pub struct SyntheticFrame {
    buffer: FrameBuffer,
    width: u32,
    height: u32,
    sequence: AtomicU64,
    timestamp_ns: AtomicU64,
}

impl SyntheticFrame {
    pub fn backing(&self) -> FrameBacking {
        self.buffer.backing()
    }

    /// The pixels, mapping an fd-backed frame on first use (see [`FrameBuffer::bytes`]).
    pub fn bytes(&self) -> Option<&[u8]> {
        self.buffer.bytes()
    }

    /// `mmap` calls made for CPU reads: 0 while every consumer reads only metadata or fds.
    pub fn map_count(&self) -> u64 {
        self.buffer.map_count()
    }

    /// `plane_data` calls (CPU accesses begun) so far.
    pub fn cpu_access_count(&self) -> u64 {
        self.buffer.cpu_access_count()
    }
}

/// One GRAY8 plane. Metadata never maps the buffer; `plane_data` maps it lazily (once per
/// buffer) and brackets dma-buf reads with `DMA_BUF_IOCTL_SYNC`.
impl FrameSource for SyntheticFrame {
    fn width(&self) -> u32 {
        self.width
    }
    fn height(&self) -> u32 {
        self.height
    }
    fn format(&self) -> u32 {
        fourcc(b"R8  ")
    }
    fn format_kind(&self) -> FrameFormatKind {
        FrameFormatKind::Pixel
    }
    fn timestamp_ns(&self) -> u64 {
        self.timestamp_ns.load(Relaxed)
    }
    fn sequence(&self) -> u64 {
        self.sequence.load(Relaxed)
    }
    fn residency(&self) -> FrameResidency {
        match self.buffer.backing() {
            FrameBacking::Heap => FrameResidency::Cpu,
            FrameBacking::DmaHeap | FrameBacking::Memfd => FrameResidency::External,
        }
    }
    fn plane_count(&self) -> u32 {
        1
    }
    fn plane(&self, index: u32) -> Option<FramePlane> {
        let (stride, len) = (u64::from(self.width), self.buffer.len() as u64);
        (index == 0).then(|| match self.buffer.dmabuf_fd() {
            Some(fd) => FramePlane::dmabuf(fd, 0, stride, len).with_mapping(PlaneMapping::Cached),
            None => FramePlane {
                stride,
                len,
                ..FramePlane::default()
            },
        })
    }
    fn plane_data(&self, index: u32) -> Option<&[u8]> {
        (index == 0).then(|| self.buffer.begin_cpu_access())?
    }
    fn end_cpu_access(&self, _index: u32) {
        self.buffer.end_cpu_access();
    }
}

/// How frames enter the graph.
#[derive(Clone, Copy, Debug, Default, PartialEq, Eq, Hash)]
pub enum FrameFeed {
    /// Payloads already carry the `daedalus:frame` handle (key [`FRAME_INTERFACE_KEY`]): stage
    /// inputs need no adapter.
    #[default]
    Interface,
    /// Payloads carry the owner type ([`SYNTHETIC_FRAME_KEY`]); the planner inserts the
    /// provider's `View` adapter on each consumer edge, as for a camera's own frame type.
    Owner,
}

impl FrameFeed {
    pub fn as_str(self) -> &'static str {
        match self {
            FrameFeed::Interface => "interface",
            FrameFeed::Owner => "owner",
        }
    }
}

/// Capture settings of a [`SyntheticFrameSource`].
#[derive(Clone, Copy, Debug)]
pub struct FrameSourceConfig {
    pub width: u32,
    pub height: u32,
    /// Frames in the ring (a camera's buffer count).
    pub buffers: usize,
    /// `None` picks the first that works of dma-heap, memfd, heap.
    pub backing: Option<FrameBacking>,
    pub feed: FrameFeed,
}

impl Default for FrameSourceConfig {
    fn default() -> Self {
        Self {
            width: 640,
            height: 480,
            buffers: 4,
            backing: None,
            feed: FrameFeed::Interface,
        }
    }
}

/// A ring of [`SyntheticFrame`]s with their payloads built once: a capture
/// ([`Self::next_payload`]) stamps the next frame and clones its payload, so steady-state
/// capture never allocates or copies, like a camera handing out leased buffers.
pub struct SyntheticFrameSource {
    frames: Vec<Arc<SyntheticFrame>>,
    payloads: Vec<Payload>,
    next: usize,
    sequence: u64,
    started: Instant,
    feed: FrameFeed,
}

impl SyntheticFrameSource {
    pub fn new(config: FrameSourceConfig) -> std::io::Result<Self> {
        let len = config.width as usize * config.height as usize;
        let fill = |seed: usize| {
            move |bytes: &mut [u8]| {
                for (idx, byte) in bytes.iter_mut().enumerate() {
                    *byte = (idx + seed) as u8;
                }
            }
        };
        let frames = (0..config.buffers.max(1))
            .map(|seed| {
                let buffer = match config.backing {
                    Some(backing) => FrameBuffer::new(backing, len, fill(seed))?,
                    None => FrameBuffer::best(len, fill(seed)),
                };
                Ok(Arc::new(SyntheticFrame {
                    buffer,
                    width: config.width,
                    height: config.height,
                    sequence: AtomicU64::new(0),
                    timestamp_ns: AtomicU64::new(0),
                }))
            })
            .collect::<std::io::Result<Vec<_>>>()?;
        let payloads = frames
            .iter()
            .map(|frame| match config.feed {
                FrameFeed::Interface => Payload::foreign(
                    FRAME_INTERFACE_KEY,
                    ForeignHandle::from_arc::<_, FrameInterface>(frame.clone()),
                    Residency::External,
                ),
                FrameFeed::Owner => Payload::shared_with(
                    SYNTHETIC_FRAME_KEY,
                    frame.clone(),
                    Residency::External,
                    None,
                    Some(len as u64),
                ),
            })
            .collect();
        Ok(Self {
            frames,
            payloads,
            next: 0,
            sequence: 0,
            started: Instant::now(),
            feed: config.feed,
        })
    }

    pub fn feed(&self) -> FrameFeed {
        self.feed
    }

    pub fn backing(&self) -> FrameBacking {
        self.frames[0].backing()
    }

    pub fn frames(&self) -> &[Arc<SyntheticFrame>] {
        &self.frames
    }

    /// Capture the next frame: stamp its sequence and timestamp and return its payload (an
    /// `Arc` clone of the prebuilt one).
    pub fn next_payload(&mut self) -> Payload {
        let idx = self.next;
        self.next = (idx + 1) % self.frames.len();
        self.sequence += 1;
        let frame = &self.frames[idx];
        frame.sequence.store(self.sequence, Relaxed);
        frame
            .timestamp_ns
            .store(self.started.elapsed().as_nanos() as u64, Relaxed);
        self.payloads[idx].clone()
    }
}
