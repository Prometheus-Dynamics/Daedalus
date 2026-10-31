//! Non-blocking GPU completion for nodes: submit on one tick, collect on a later one.

use std::collections::VecDeque;
use std::sync::Arc;
use std::sync::atomic::{AtomicBool, Ordering};

use crate::{GpuContextHandle, GpuError};

/// A submitted batch of GPU work. [`GpuTicket::try_complete`] polls the device without waiting,
/// so a node can submit on tick N, keep the ticket in its state, and emit the result on the first
/// later tick where the work has finished: the tick never waits on a GPU fence.
#[derive(Debug)]
pub struct GpuTicket {
    done: Arc<AtomicBool>,
    device: wgpu::Device,
    index: wgpu::SubmissionIndex,
}

impl GpuTicket {
    /// Submit `commands` on `gpu`'s queue. `Unsupported` on a non-wgpu backend.
    pub fn submit(
        gpu: &GpuContextHandle,
        commands: impl IntoIterator<Item = wgpu::CommandBuffer>,
    ) -> Result<Self, GpuError> {
        let (device, queue) = gpu.wgpu().ok_or(GpuError::Unsupported)?;
        Ok(Self::submit_on(device, queue, commands))
    }

    /// Submit `commands` on `queue` (of `device`).
    pub fn submit_on(
        device: &wgpu::Device,
        queue: &wgpu::Queue,
        commands: impl IntoIterator<Item = wgpu::CommandBuffer>,
    ) -> Self {
        let index = queue.submit(commands);
        let done = Arc::new(AtomicBool::new(false));
        let flag = done.clone();
        queue.on_submitted_work_done(move || flag.store(true, Ordering::Release));
        Self {
            done,
            device: device.clone(),
            index,
        }
    }

    /// `Some(())` once the work has finished. Never waits: it runs one non-blocking device poll
    /// (well under a microsecond when idle) and checks the completion flag.
    pub fn try_complete(&self) -> Option<()> {
        if !self.done.load(Ordering::Acquire) {
            let _ = self.device.poll(wgpu::PollType::Poll);
        }
        self.done.load(Ordering::Acquire).then_some(())
    }

    /// Whether the work has finished, without polling the device (as of the last poll anyone
    /// made on it).
    pub fn is_complete(&self) -> bool {
        self.done.load(Ordering::Acquire)
    }

    /// Wait for the work to finish. For shutdown and tests: a tick should use
    /// [`GpuTicket::try_complete`].
    pub fn wait(&self) {
        let _ = self.device.poll(wgpu::PollType::Wait {
            submission_index: Some(self.index.clone()),
            timeout: None,
        });
    }
}

/// At most `max` tickets in flight, each with the value it will produce (a staging buffer, a frame
/// id). A node pushes when [`GpuInFlight::is_full`] is false (otherwise it skips or drops the
/// frame) and pops completed values in submission order every tick.
#[derive(Debug)]
pub struct GpuInFlight<T> {
    max: usize,
    pending: VecDeque<(GpuTicket, T)>,
}

impl<T> GpuInFlight<T> {
    /// `max` is at least 1.
    pub fn new(max: usize) -> Self {
        Self {
            max: max.max(1),
            pending: VecDeque::new(),
        }
    }

    pub fn len(&self) -> usize {
        self.pending.len()
    }

    pub fn is_empty(&self) -> bool {
        self.pending.is_empty()
    }

    pub fn is_full(&self) -> bool {
        self.pending.len() >= self.max
    }

    /// Track `ticket` with its `value`; hands both back when `max` are already in flight.
    pub fn push(&mut self, ticket: GpuTicket, value: T) -> Result<(), (GpuTicket, T)> {
        if self.is_full() {
            return Err((ticket, value));
        }
        self.pending.push_back((ticket, value));
        Ok(())
    }

    /// The oldest value whose work has finished, if any (never waits). Call it in a loop to drain
    /// everything that completed.
    pub fn pop_completed(&mut self) -> Option<T> {
        self.pending.front()?.0.try_complete()?;
        self.pending.pop_front().map(|(_, value)| value)
    }

    /// Drop the oldest pending entry without waiting for it (latest-wins: make room for a newer
    /// frame). The GPU still finishes the work; its result is discarded.
    pub fn drop_oldest(&mut self) -> Option<T> {
        self.pending.pop_front().map(|(_, value)| value)
    }
}
