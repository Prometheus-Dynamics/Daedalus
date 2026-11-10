//! How long a pass took on the device.

use std::sync::atomic::{AtomicBool, Ordering};
use std::sync::{Arc, Mutex, PoisonError};
use std::time::Duration;

use crate::context::Context;

/// How a pass's time was measured.
#[derive(Clone, Copy, Debug, PartialEq, Eq, Hash)]
pub enum TimeSource {
    /// Timestamps the device wrote at the start and end of the pass.
    Timestamps,
    /// From submit until the device said it was done. Includes waiting behind earlier work.
    SubmitToDone,
}

/// Bytes of two timestamps.
const RESOLVED: u64 = 16;

/// A pair of timestamp queries and the buffers that bring them back. One per submission that is
/// timed: it's busy from the submit until its time has been read.
pub(crate) struct PassTimer {
    pub(crate) queries: wgpu::QuerySet,
    resolve: wgpu::Buffer,
    read: wgpu::Buffer,
    /// Set by the map callback.
    mapped: Arc<AtomicBool>,
    busy: AtomicBool,
    measured: Mutex<Option<Duration>>,
}

impl PassTimer {
    /// A timer, when the device can write timestamps in a pass.
    pub(crate) fn new(context: &Context) -> Option<PassTimer> {
        if !context
            .device
            .features()
            .contains(wgpu::Features::TIMESTAMP_QUERY)
        {
            return None;
        }
        let buffer = |usage| {
            context.device.create_buffer(&wgpu::BufferDescriptor {
                label: None,
                size: RESOLVED,
                usage,
                mapped_at_creation: false,
            })
        };
        Some(PassTimer {
            queries: context.device.create_query_set(&wgpu::QuerySetDescriptor {
                label: None,
                ty: wgpu::QueryType::Timestamp,
                count: 2,
            }),
            resolve: buffer(wgpu::BufferUsages::QUERY_RESOLVE | wgpu::BufferUsages::COPY_SRC),
            read: buffer(wgpu::BufferUsages::MAP_READ | wgpu::BufferUsages::COPY_DST),
            mapped: Arc::default(),
            busy: AtomicBool::new(false),
            measured: Mutex::new(None),
        })
    }

    /// Claims the timer for one submission. `false` while an earlier one is still in flight.
    pub(crate) fn claim(&self) -> bool {
        if self.busy.swap(true, Ordering::AcqRel) {
            // Held. If its time came back and nobody read it, it's free again.
            if !self.mapped.swap(false, Ordering::AcqRel) {
                return false;
            }
            self.read.unmap();
        }
        *self.measured.lock().unwrap_or_else(PoisonError::into_inner) = None;
        true
    }

    /// The pass's timestamp writes, for its descriptor.
    pub(crate) fn writes(&self) -> wgpu::ComputePassTimestampWrites<'_> {
        wgpu::ComputePassTimestampWrites {
            query_set: &self.queries,
            beginning_of_pass_write_index: Some(0),
            end_of_pass_write_index: Some(1),
        }
    }

    /// Records the commands that bring the two timestamps to where the host can map them.
    pub(crate) fn resolve(&self, encoder: &mut wgpu::CommandEncoder) {
        encoder.resolve_query_set(&self.queries, 0..2, &self.resolve, 0);
        encoder.copy_buffer_to_buffer(&self.resolve, 0, &self.read, 0, RESOLVED);
    }

    /// Asks for the map, after the submit. The caller holds the submit lock: a map can flush
    /// pending writes.
    pub(crate) fn map(&self) {
        let mapped = Arc::clone(&self.mapped);
        self.read
            .slice(..)
            .map_async(wgpu::MapMode::Read, move |result| {
                mapped.store(result.is_ok(), Ordering::Release);
            });
    }

    /// The pass's time, once the map has come back. Reading it frees the timer.
    pub(crate) fn read(&self, context: &Context) -> Option<Duration> {
        let mut measured = self.measured.lock().unwrap_or_else(PoisonError::into_inner);
        if measured.is_some() {
            return *measured;
        }
        if !self.mapped.load(Ordering::Acquire) {
            return None;
        }
        let ticks = {
            let view = self.read.slice(..).get_mapped_range().ok()?;
            let word = |at: usize| {
                let bytes = view.get(at..at + 8)?;
                Some(u64::from_le_bytes(bytes.try_into().ok()?))
            };
            word(8)?.saturating_sub(word(0)?)
        };
        self.read.unmap();
        self.mapped.store(false, Ordering::Release);
        self.busy.store(false, Ordering::Release);
        let nanos = ticks as f64 * f64::from(context.queue.get_timestamp_period());
        *measured = Some(Duration::from_nanos(nanos as u64));
        *measured
    }
}
