//! Rectangles of a target for a fragment pass to draw, instead of the whole of it.

use std::sync::Arc;

use super::{FragmentError, invalid};
use crate::WgpuBackend;
use crate::context::Context;

/// Bytes of one rectangle as the vertex stage reads it: x, y, width, height, and the target's
/// width and height, each a `u32`.
pub(crate) const INSTANCE_BYTES: u64 = 24;

pub(crate) fn instance(
    rect: [u32; 4],
    (width, height): (u32, u32),
) -> [u8; INSTANCE_BYTES as usize] {
    let words = [rect[0], rect[1], rect[2], rect[3], width, height];
    let mut out = [0; INSTANCE_BYTES as usize];
    for (bytes, word) in out.as_chunks_mut::<4>().0.iter_mut().zip(words) {
        *bytes = word.to_le_bytes();
    }
    out
}

/// Up to a fixed number of rectangles, kept on the device and rewritten as they change.
pub struct FragmentRects {
    buffer: wgpu::Buffer,
    capacity: usize,
    count: usize,
    size: (u32, u32),
    /// Reused for every write, so writing rectangles allocates nothing.
    scratch: Vec<u8>,
    context: Arc<Context>,
}

impl std::fmt::Debug for FragmentRects {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("FragmentRects")
            .field("count", &self.count)
            .field("capacity", &self.capacity)
            .field("size", &self.size)
            .finish_non_exhaustive()
    }
}

impl FragmentRects {
    /// Room for `capacity` rectangles, at least one.
    pub fn new(backend: &WgpuBackend, capacity: usize) -> Result<FragmentRects, FragmentError> {
        let context = backend.context().map_err(FragmentError::Open)?;
        let capacity = capacity.max(1);
        let buffer = context.device.create_buffer(&wgpu::BufferDescriptor {
            label: None,
            size: capacity as u64 * INSTANCE_BYTES,
            usage: wgpu::BufferUsages::VERTEX | wgpu::BufferUsages::COPY_DST,
            mapped_at_creation: false,
        });
        Ok(FragmentRects {
            buffer,
            capacity,
            count: 0,
            size: (0, 0),
            scratch: Vec::with_capacity(capacity * INSTANCE_BYTES as usize),
            context,
        })
    }

    pub fn capacity(&self) -> usize {
        self.capacity
    }

    pub fn len(&self) -> usize {
        self.count
    }

    pub fn is_empty(&self) -> bool {
        self.count == 0
    }

    pub(crate) fn target_size(&self) -> (u32, u32) {
        self.size
    }

    pub(crate) fn buffer(&self) -> &wgpu::Buffer {
        &self.buffer
    }

    /// Sets the rectangles to draw, each `[x, y, width, height]` in pixels of a target of
    /// `target_size`. They travel with the next submit.
    pub fn write(
        &mut self,
        target_size: (u32, u32),
        rects: &[[u32; 4]],
    ) -> Result<(), FragmentError> {
        if rects.len() > self.capacity {
            return Err(invalid(format!(
                "{} rectangles, where there is room for {}",
                rects.len(),
                self.capacity
            )));
        }
        let (width, height) = target_size;
        let leaves = |&[x, y, w, h]: &[u32; 4]| {
            x.checked_add(w).is_none_or(|end| end > width)
                || y.checked_add(h).is_none_or(|end| end > height)
        };
        if let Some(rect) = rects.iter().find(|rect| leaves(rect)) {
            return Err(invalid(format!(
                "the rectangle {rect:?} leaves the {width}x{height} target"
            )));
        }
        self.scratch.clear();
        for &rect in rects {
            self.scratch.extend_from_slice(&instance(rect, target_size));
        }
        if !self.scratch.is_empty() {
            self.context
                .queue
                .write_buffer(&self.buffer, 0, &self.scratch);
        }
        self.count = rects.len();
        self.size = target_size;
        Ok(())
    }
}
