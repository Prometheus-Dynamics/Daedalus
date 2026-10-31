//! GPU time per dispatch: timestamp queries where the device has them, else submit→completion.

use std::sync::Arc;
use std::time::{Duration, Instant};

use parking_lot::Mutex;

use crate::{GpuContextHandle, GpuError};

use super::GpuTicket;
use super::gpu_storage::{GpuReadback, StagingRing};

/// Where a [`GpuTimed`] duration comes from.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum GpuTimeSource {
    /// Timestamp queries written at the start and end of the pass: GPU execution time.
    Timestamps,
    /// From the submit to the completion callback (seen on a device poll): includes queue wait
    /// and the poll's latency. Used when the device has no timestamp queries.
    SubmitToCompletion,
}

/// Times one pass per submission. Keep it in the node's state; pass
/// [`GpuTimer::compute_writes`] / [`GpuTimer::render_writes`] to the pass, then submit the
/// encoder with [`GpuTimer::submit`] and poll the returned [`GpuTimed`]. Record the result with
/// `ctx.duration_metric("gpu", time)` to report it next to the node's other metrics.
pub struct GpuTimer {
    queries: Option<(wgpu::QuerySet, wgpu::Buffer)>,
    period_ns: f64,
    staging: Arc<StagingRing>,
    device: wgpu::Device,
    queue: wgpu::Queue,
}

impl std::fmt::Debug for GpuTimer {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("GpuTimer")
            .field("source", &self.source())
            .finish_non_exhaustive()
    }
}

impl GpuTimer {
    /// A timer on `gpu`'s device: timestamp queries when the device enabled them
    /// (`GpuCapabilities::compute.features.timestamps`), else submit→completion timing.
    /// `Unsupported` on a non-wgpu backend.
    pub fn new(gpu: &GpuContextHandle) -> Result<Self, GpuError> {
        let (device, queue) = gpu.wgpu().ok_or(GpuError::Unsupported)?;
        let queries = device
            .features()
            .contains(wgpu::Features::TIMESTAMP_QUERY)
            .then(|| {
                let set = device.create_query_set(&wgpu::QuerySetDescriptor {
                    label: Some("daedalus-gpu-timer"),
                    ty: wgpu::QueryType::Timestamp,
                    count: 2,
                });
                let resolve = device.create_buffer(&wgpu::BufferDescriptor {
                    label: Some("daedalus-gpu-timer-resolve"),
                    size: 16,
                    usage: wgpu::BufferUsages::QUERY_RESOLVE | wgpu::BufferUsages::COPY_SRC,
                    mapped_at_creation: false,
                });
                (set, resolve)
            });
        Ok(Self {
            queries,
            period_ns: f64::from(queue.get_timestamp_period()),
            staging: Arc::default(),
            device: device.clone(),
            queue: queue.clone(),
        })
    }

    pub fn source(&self) -> GpuTimeSource {
        if self.queries.is_some() {
            GpuTimeSource::Timestamps
        } else {
            GpuTimeSource::SubmitToCompletion
        }
    }

    /// Timestamp writes for a compute pass (`None` without timestamp queries).
    pub fn compute_writes(&self) -> Option<wgpu::ComputePassTimestampWrites<'_>> {
        self.queries
            .as_ref()
            .map(|(set, _)| wgpu::ComputePassTimestampWrites {
                query_set: set,
                beginning_of_pass_write_index: Some(0),
                end_of_pass_write_index: Some(1),
            })
    }

    /// Timestamp writes for a render pass (`None` without timestamp queries).
    pub fn render_writes(&self) -> Option<wgpu::RenderPassTimestampWrites<'_>> {
        self.queries
            .as_ref()
            .map(|(set, _)| wgpu::RenderPassTimestampWrites {
                query_set: set,
                beginning_of_pass_write_index: Some(0),
                end_of_pass_write_index: Some(1),
            })
    }

    /// Resolve the pass's timestamps into a staging buffer (when timed by queries) and submit
    /// `encoder`. Never waits; poll the result.
    pub fn submit(&self, mut encoder: wgpu::CommandEncoder) -> GpuTimed {
        let staging = self.queries.as_ref().map(|(set, resolve)| {
            encoder.resolve_query_set(set, 0..2, resolve, 0);
            let staging = self.staging.take(&self.device, 16);
            encoder.copy_buffer_to_buffer(resolve, 0, &staging, 0, 16);
            staging
        });
        let submitted = Instant::now();
        let ticket = GpuTicket::submit_on(&self.device, &self.queue, [encoder.finish()]);
        let done_at = Arc::new(Mutex::new(None));
        let mark = done_at.clone();
        self.queue
            .on_submitted_work_done(move || *mark.lock() = Some(Instant::now()));
        let readback = staging
            .map(|staging| GpuReadback::begin(&self.device, &self.staging, staging, 16, 16, None));
        GpuTimed {
            ticket,
            readback,
            period_ns: self.period_ns,
            submitted,
            done_at,
        }
    }

    /// Staging buffers created for timestamp readbacks (one per measurement in flight).
    pub fn staging_buffers_created(&self) -> usize {
        self.staging.created()
    }
}

/// A timed submission ([`GpuTimer::submit`]).
#[derive(Debug)]
pub struct GpuTimed {
    ticket: GpuTicket,
    readback: Option<GpuReadback>,
    period_ns: f64,
    submitted: Instant,
    done_at: Arc<Mutex<Option<Instant>>>,
}

impl GpuTimed {
    /// The submission's ticket (completion without the timing).
    pub fn ticket(&self) -> &GpuTicket {
        &self.ticket
    }

    pub fn source(&self) -> GpuTimeSource {
        if self.readback.is_some() {
            GpuTimeSource::Timestamps
        } else {
            GpuTimeSource::SubmitToCompletion
        }
    }

    /// The GPU time once the work and its timing have arrived; never waits (one non-blocking
    /// poll). `Some(Err(..))` if the timestamp readback failed.
    pub fn try_elapsed(&mut self) -> Option<Result<Duration, GpuError>> {
        self.ticket.try_complete()?;
        match &mut self.readback {
            Some(readback) => {
                let mut raw = [0u8; 16];
                Some(readback.try_read_into(&mut raw)?.map(|()| {
                    let begin = u64::from_le_bytes(raw[..8].try_into().expect("8 bytes"));
                    let end = u64::from_le_bytes(raw[8..].try_into().expect("8 bytes"));
                    let ns = end.saturating_sub(begin) as f64 * self.period_ns;
                    Duration::from_nanos(ns as u64)
                }))
            }
            None => {
                let done = (*self.done_at.lock())?;
                Some(Ok(done.saturating_duration_since(self.submitted)))
            }
        }
    }

    /// Wait for the timing. For tests and shutdown.
    pub fn wait(&mut self) -> Result<Duration, GpuError> {
        self.ticket.wait();
        if let Some(readback) = &self.readback {
            readback.wait();
        }
        self.try_elapsed()
            .unwrap_or_else(|| Err(GpuError::Internal("timing did not arrive".into())))
    }
}
