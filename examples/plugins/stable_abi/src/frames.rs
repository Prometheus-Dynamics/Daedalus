//! Frame nodes: owned [`SharedFrame`] inputs and outputs. The plugin keeps a host frame in node
//! state, returns received frames, returns frames it makes, wraps a frame to change its
//! metadata without copying pixels, and panics while holding one. The facade's `dylib_frames`
//! test counts the host frame's releases through each.

use std::sync::Arc;

use daedalus::macros::node;
use daedalus::runtime::NodeError;
use daedalus::transport::{
    FrameColorimetry, FrameFormatKind, FramePlane, FrameResidency, FrameSource, FrameView,
    SharedFrame, fourcc, frame_meta,
};

/// An R8 image the plugin owns (tightly packed rows).
pub struct GrayImage {
    pub width: u32,
    pub height: u32,
    pub pixels: Vec<u8>,
    pub sequence: u64,
    pub timestamp_ns: u64,
    pub colorimetry: FrameColorimetry,
}

impl FrameSource for GrayImage {
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
        self.timestamp_ns
    }
    fn sequence(&self) -> u64 {
        self.sequence
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
    fn colorimetry(&self) -> FrameColorimetry {
        self.colorimetry
    }
}

fn invalid(message: &str) -> NodeError {
    NodeError::InvalidInput(message.into())
}

/// Inverts an R8 frame into a new frame the plugin owns: a shared frame is never written in
/// place, so a node that changes pixels makes a frame (reading the input through its stride).
#[node(id = "frame_invert", inputs("frame"), outputs("inverted"))]
pub(crate) fn frame_invert(frame: SharedFrame) -> Result<SharedFrame, NodeError> {
    let view = frame.as_view();
    if view.format() != fourcc(b"R8  ") {
        return Err(invalid("frame_invert takes R8 frames"));
    }
    let (width, height) = (view.width(), view.height());
    let stride = view.plane(0).ok_or_else(|| invalid("no plane 0"))?.stride as usize;
    let src = view
        .plane_bytes(0)
        .ok_or_else(|| invalid("plane 0 is not CPU-readable"))?;
    let pixels = src
        .chunks(stride.max(1))
        .take(height as usize)
        .flat_map(|row| row[..width as usize].iter().map(|&p| 255 - p))
        .collect();
    Ok(SharedFrame::new(Arc::new(GrayImage {
        width,
        height,
        pixels,
        sequence: view.sequence(),
        timestamp_ns: view.timestamp_ns(),
        colorimetry: view.colorimetry(),
    })))
}

/// Hands the frame straight back (in and out, no pixel access): the handoff cost alone.
#[node(id = "frame_pass", inputs("frame"), outputs("frame"))]
pub(crate) fn frame_pass(frame: SharedFrame) -> Result<SharedFrame, NodeError> {
    Ok(frame)
}

/// A received frame with another sensor id and full range: every other accessor, the pixels
/// included, forwards to the original, which it keeps alive.
struct Restamped {
    inner: SharedFrame,
    sensor: String,
}

impl FrameSource for Restamped {
    fn width(&self) -> u32 {
        self.inner.width()
    }
    fn height(&self) -> u32 {
        self.inner.height()
    }
    fn format(&self) -> u32 {
        self.inner.format()
    }
    fn format_kind(&self) -> FrameFormatKind {
        self.inner.format_kind()
    }
    fn modifier(&self) -> u64 {
        self.inner.modifier()
    }
    fn timestamp_ns(&self) -> u64 {
        self.inner.timestamp_ns()
    }
    fn sequence(&self) -> u64 {
        self.inner.sequence()
    }
    fn residency(&self) -> FrameResidency {
        self.inner.residency()
    }
    fn plane_count(&self) -> u32 {
        self.inner.plane_count()
    }
    fn plane(&self, index: u32) -> Option<FramePlane> {
        self.inner.plane(index)
    }
    fn plane_data(&self, index: u32) -> Option<&[u8]> {
        self.inner.plane_data(index)
    }
    fn end_cpu_access(&self, index: u32) {
        self.inner.end_cpu_access(index)
    }
    fn colorimetry(&self) -> FrameColorimetry {
        self.inner
            .colorimetry()
            .with_range(daedalus::transport::ColorRange::Full)
    }
    fn metadata_u64(&self, key: u32) -> Option<u64> {
        self.inner.metadata_u64(key)
    }
    fn metadata_bytes(&self, key: u32) -> Option<&[u8]> {
        match key {
            frame_meta::SENSOR_ID => Some(self.sensor.as_bytes()),
            _ => self.inner.metadata_bytes(key),
        }
    }
}

/// NV12 (or any frame) passed through with a metadata tweak: a new sensor id and full range,
/// the pixels untouched and uncopied.
#[node(id = "frame_tag", inputs("frame", "sensor"), outputs("frame"))]
pub(crate) fn frame_tag(frame: SharedFrame, sensor: String) -> Result<SharedFrame, NodeError> {
    Ok(SharedFrame::new(Arc::new(Restamped {
        inner: frame,
        sensor,
    })))
}

#[derive(Default)]
pub(crate) struct Held(Option<SharedFrame>);

/// Keeps the latest frame in node state and returns the one it held before (none on the first
/// tick): frames retained past the call, released on a later tick or with the node.
#[node(id = "frame_hold", inputs("frame"), outputs("previous"), state(Held))]
pub(crate) fn frame_hold(
    frame: SharedFrame,
    state: &mut Held,
) -> Result<Option<SharedFrame>, NodeError> {
    Ok(state.0.replace(frame))
}

/// Panics on sequence 13 while holding two references to the frame.
#[node(id = "frame_explode", inputs("frame"), outputs("sequence"))]
pub(crate) fn frame_explode(frame: SharedFrame) -> Result<i64, NodeError> {
    let kept = frame.clone();
    let sequence = kept.as_view().sequence();
    assert_ne!(sequence, 13, "unlucky frame");
    Ok(sequence as i64)
}

/// What the plugin sees of a frame: v3 metadata, plane 0's fd (borrowed, so the host's own
/// number) and the address its CPU bytes are read from (the host's, nothing is copied).
#[node(
    id = "frame_info",
    inputs("frame"),
    outputs("exposure_ns", "sensor", "range", "fd", "ptr")
)]
pub(crate) fn frame_info(frame: FrameView<'_>) -> Result<(i64, String, i64, i64, i64), NodeError> {
    let fd = frame
        .plane(0)
        .and_then(|plane| plane.dmabuf_fd)
        .unwrap_or(-1);
    let ptr = frame
        .plane_bytes(0)
        .map_or(0, |bytes| bytes.as_ptr() as i64);
    Ok((
        frame.exposure_ns().map_or(-1, |ns| ns as i64),
        frame.sensor_id().unwrap_or_default().to_string(),
        i64::from(frame.colorimetry().range),
        i64::from(fd),
        ptr,
    ))
}
