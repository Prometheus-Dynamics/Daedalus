use std::collections::HashMap;
use std::future::poll_fn;
use std::sync::atomic::{AtomicU64, Ordering};
use std::sync::{Arc, Mutex};
use std::task::Poll;

use crate::GpuError;

use super::{readback::ReadbackRequest, temp_pool};

static NEXT_ASYNC_MAP_ID: AtomicU64 = AtomicU64::new(1);

struct MapState {
    result: Option<Result<(), GpuError>>,
    completed: bool,
    waker: Option<std::task::Waker>,
}

fn begin_map_read_async(
    device: &wgpu::Device,
    slice: wgpu::BufferSlice<'_>,
) -> impl std::future::Future<Output = Result<(), GpuError>> + Send + 'static {
    begin_map_read_async_with_state(device, slice, true).1
}

fn begin_map_read_async_with_state(
    device: &wgpu::Device,
    slice: wgpu::BufferSlice<'_>,
    spawn_poller: bool,
) -> (
    Arc<Mutex<MapState>>,
    impl std::future::Future<Output = Result<(), GpuError>> + Send + 'static,
) {
    let map_id = NEXT_ASYNC_MAP_ID.fetch_add(1, Ordering::Relaxed);
    tracing::debug!(
        target: "daedalus_gpu::readback",
        map_id,
        "async gpu readback map requested"
    );
    let state = Arc::new(Mutex::new(MapState {
        result: None,
        completed: false,
        waker: None,
    }));

    slice.map_async(wgpu::MapMode::Read, {
        let state = Arc::clone(&state);
        move |res| {
            let ok = res.is_ok();
            complete_map_state(
                &state,
                res.map_err(|error| GpuError::Internal(format!("map failed: {error:?}"))),
            );
            tracing::debug!(
                target: "daedalus_gpu::readback",
                map_id,
                ok,
                "async gpu readback map callback completed"
            );
        }
    });

    if spawn_poller {
        submit_map_poll_job(device.clone(), vec![(map_id, Arc::clone(&state))]);
    }

    let future_state = Arc::clone(&state);
    let future = poll_fn(move |cx| {
        let mut state = future_state
            .lock()
            .unwrap_or_else(|poisoned| poisoned.into_inner());
        if let Some(done) = state.result.take() {
            return Poll::Ready(done);
        }
        state.waker = Some(cx.waker().clone());
        Poll::Pending
    });
    (state, future)
}

fn submit_map_poll_job(device: wgpu::Device, states: Vec<(u64, Arc<Mutex<MapState>>)>) {
    let state_handles = states
        .iter()
        .map(|(_, state)| Arc::clone(state))
        .collect::<Vec<_>>();
    super::poll_driver::submit_poll_job("readback_map", move || {
        let mut polls = 0_u64;
        loop {
            let all_completed = states.iter().all(|(_, state)| {
                state
                    .lock()
                    .unwrap_or_else(|poisoned| poisoned.into_inner())
                    .completed
            });
            if all_completed {
                tracing::trace!(
                    target: "daedalus_gpu::readback",
                    maps = states.len(),
                    polls,
                    "async gpu readback poll driver finished"
                );
                break;
            }
            let _ = device.poll(wgpu::PollType::Wait {
                submission_index: None,
                timeout: Some(std::time::Duration::from_millis(10)),
            });
            polls += 1;
            if polls.is_multiple_of(100) {
                tracing::trace!(
                    target: "daedalus_gpu::readback",
                    maps = states.len(),
                    polls,
                    "async gpu readback poll driver still waiting"
                );
            }
        }
    })
    .unwrap_or_else(|error| {
        let error = GpuError::Internal(error.to_string());
        for state in state_handles {
            complete_map_state(&state, Err(error.clone()));
        }
    });
}

pub(crate) async fn map_read_async(
    device: &wgpu::Device,
    slice: wgpu::BufferSlice<'_>,
) -> Result<(), GpuError> {
    begin_map_read_async(device, slice).await
}

pub(crate) async fn resolve_readbacks_async(
    device: &wgpu::Device,
    readbacks: Vec<ReadbackRequest>,
) -> Result<HashMap<u32, Vec<u8>>, GpuError> {
    let device_key = super::device_key(device);
    let mut result = HashMap::new();

    tracing::debug!(
        target: "daedalus_gpu::readback",
        readbacks = readbacks.len(),
        "resolving async gpu readbacks"
    );
    let pending_maps: Vec<_> = readbacks
        .iter()
        .map(|readback| begin_map_read_async_with_state(device, readback.buffer.slice(..), false))
        .collect();
    submit_map_poll_job(
        device.clone(),
        pending_maps
            .iter()
            .map(|(state, _)| (0, Arc::clone(state)))
            .collect(),
    );

    for (_, pending) in pending_maps {
        pending
            .await
            .map_err(|e| GpuError::Internal(format!("map failed: {e}")))?;
    }

    for ReadbackRequest {
        binding,
        buffer,
        size,
        is_texture,
        height,
        row_bytes,
        padded_bpr,
    } in readbacks
    {
        let slice = buffer.slice(..);
        {
            let data = slice.get_mapped_range();
            if is_texture {
                let mut trimmed = Vec::with_capacity(row_bytes * height as usize);
                for row in 0..height as usize {
                    let start = row * padded_bpr;
                    trimmed.extend_from_slice(&data[start..start + row_bytes]);
                }
                result.insert(binding, trimmed);
            } else {
                let mut buf = Vec::with_capacity(size as usize);
                let len = size.min(data.len() as u64) as usize;
                buf.extend_from_slice(&data[..len]);
                result.insert(binding, buf);
            }
        }
        buffer.unmap();
        if let Ok(mut p) = temp_pool().lock() {
            p.put_buffer(device_key, size, buffer);
            tracing::trace!(
                target: "daedalus_gpu::readback",
                binding,
                size,
                device_key,
                "returned async readback buffer to temp pool"
            );
        } else {
            tracing::warn!(
                target: "daedalus_gpu::readback",
                binding,
                size,
                "failed to return async readback buffer to temp pool because lock was poisoned"
            );
        }
    }

    Ok(result)
}

fn complete_map_state(state: &Arc<Mutex<MapState>>, result: Result<(), GpuError>) {
    let waker = {
        let mut state = state
            .lock()
            .unwrap_or_else(|poisoned| poisoned.into_inner());
        state.result = Some(result);
        state.completed = true;
        state.waker.take()
    };
    if let Some(waker) = waker {
        waker.wake();
    }
}
