use std::collections::HashMap;
use std::future::poll_fn;
use std::sync::atomic::{AtomicU64, Ordering};
use std::sync::{Arc, Mutex};
use std::task::Poll;
use std::time::{Duration, Instant};

use crate::GpuError;

use super::{readback::ReadbackRequest, temp_pool};

static NEXT_ASYNC_MAP_ID: AtomicU64 = AtomicU64::new(1);
const DEFAULT_ASYNC_READBACK_POLL_INTERVAL_MS: u64 = 10;
const DEFAULT_ASYNC_READBACK_TIMEOUT_MS: u64 = 30_000;
static ASYNC_READBACK_POLL_INTERVAL_MS: AtomicU64 =
    AtomicU64::new(DEFAULT_ASYNC_READBACK_POLL_INTERVAL_MS);
static ASYNC_READBACK_TIMEOUT_MS: AtomicU64 = AtomicU64::new(DEFAULT_ASYNC_READBACK_TIMEOUT_MS);

/// Set how long the async readback poll worker waits inside each `wgpu::Device::poll` call.
///
/// Very small values make timeout checks more responsive but increase wakeups. A zero duration is
/// normalized to 1ms to avoid a tight polling loop. Returns the previous interval.
pub fn set_async_readback_poll_interval(interval: Duration) -> Duration {
    let previous =
        ASYNC_READBACK_POLL_INTERVAL_MS.swap(duration_millis(interval), Ordering::Relaxed);
    Duration::from_millis(previous)
}

/// Current async readback poll interval.
pub fn async_readback_poll_interval() -> Duration {
    Duration::from_millis(ASYNC_READBACK_POLL_INTERVAL_MS.load(Ordering::Relaxed))
}

/// Set the maximum time an async readback map operation may wait before completing with an error.
///
/// A zero duration is normalized to 1ms so callers do not accidentally force every readback to time
/// out before the GPU can make progress. Returns the previous timeout.
pub fn set_async_readback_timeout(timeout: Duration) -> Duration {
    let previous = ASYNC_READBACK_TIMEOUT_MS.swap(duration_millis(timeout), Ordering::Relaxed);
    Duration::from_millis(previous)
}

/// Current async readback timeout.
pub fn async_readback_timeout() -> Duration {
    Duration::from_millis(ASYNC_READBACK_TIMEOUT_MS.load(Ordering::Relaxed))
}

fn duration_millis(duration: Duration) -> u64 {
    let millis = duration.as_millis().clamp(1, u128::from(u64::MAX));
    millis as u64
}

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
            let result = res.map_err(|error| GpuError::Internal(format!("map failed: {error:?}")));
            let ok = result.is_ok();
            let error = result.as_ref().err().map(ToString::to_string);
            let completed = complete_map_state(&state, result);
            tracing::debug!(
                target: "daedalus_gpu::readback",
                map_id,
                ok,
                error = error.as_deref(),
                completed,
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
    submit_map_poll_job_with_timeout(device, states, async_readback_timeout());
}

fn submit_map_poll_job_with_timeout(
    device: wgpu::Device,
    states: Vec<(u64, Arc<Mutex<MapState>>)>,
    timeout: Duration,
) {
    let state_handles = states
        .iter()
        .map(|(_, state)| Arc::clone(state))
        .collect::<Vec<_>>();
    let worker_state_handles = state_handles.clone();
    super::poll_driver::submit_poll_job("readback_map", move || {
        let started_at = Instant::now();
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
            if started_at.elapsed() >= timeout {
                let map_ids = states.iter().map(|(map_id, _)| *map_id).collect::<Vec<_>>();
                tracing::warn!(
                    target: "daedalus_gpu::readback",
                    maps = states.len(),
                    map_ids = ?map_ids,
                    polls,
                    timeout = ?timeout,
                    "async gpu readback poll driver timed out"
                );
                timeout_map_states(&worker_state_handles, timeout);
                break;
            }
            let _ = device.poll(wgpu::PollType::Wait {
                submission_index: None,
                timeout: Some(async_readback_poll_interval()),
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
        tracing::warn!(
            target: "daedalus_gpu::readback",
            maps = state_handles.len(),
            error = %error,
            "async gpu readback poll job submission failed"
        );
        let error = GpuError::Internal(error.to_string());
        complete_map_states(&state_handles, error);
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

fn complete_map_state(state: &Arc<Mutex<MapState>>, result: Result<(), GpuError>) -> bool {
    let waker = {
        let mut state = state
            .lock()
            .unwrap_or_else(|poisoned| poisoned.into_inner());
        if state.completed {
            return false;
        }
        state.result = Some(result);
        state.completed = true;
        state.waker.take()
    };
    if let Some(waker) = waker {
        waker.wake();
    }
    true
}

fn complete_map_states(states: &[Arc<Mutex<MapState>>], error: GpuError) {
    for state in states {
        complete_map_state(state, Err(error.clone()));
    }
}

fn timeout_map_states(states: &[Arc<Mutex<MapState>>], timeout: Duration) {
    complete_map_states(
        states,
        GpuError::Internal(format!(
            "async gpu readback map timed out after {timeout:?}"
        )),
    );
}

#[cfg(test)]
mod tests {
    use super::*;

    fn pending_state() -> Arc<Mutex<MapState>> {
        Arc::new(Mutex::new(MapState {
            result: None,
            completed: false,
            waker: None,
        }))
    }

    #[test]
    fn completing_map_state_is_idempotent() {
        let state = pending_state();

        assert!(complete_map_state(
            &state,
            Err(GpuError::Internal("timeout".into()))
        ));
        assert!(!complete_map_state(&state, Ok(())));

        let mut guard = state.lock().expect("map state lock");
        assert!(guard.completed);
        let result = guard.result.take().expect("stored result");
        assert!(matches!(result, Err(GpuError::Internal(message)) if message == "timeout"));
    }

    #[test]
    fn timing_out_multiple_map_states_marks_waiters_done() {
        let states = vec![pending_state(), pending_state()];

        timeout_map_states(&states, Duration::from_millis(1));

        for state in states {
            let mut guard = state.lock().expect("map state lock");
            assert!(guard.completed);
            let result = guard.result.take().expect("stored result");
            assert!(matches!(
                result,
                Err(GpuError::Internal(message))
                    if message == "async gpu readback map timed out after 1ms"
            ));
        }
    }

    #[test]
    fn async_readback_runtime_knobs_round_trip_and_normalize_zero() {
        let previous_interval = set_async_readback_poll_interval(Duration::from_millis(7));
        let previous_timeout = set_async_readback_timeout(Duration::from_millis(123));

        assert_eq!(async_readback_poll_interval(), Duration::from_millis(7));
        assert_eq!(async_readback_timeout(), Duration::from_millis(123));
        assert_eq!(
            set_async_readback_poll_interval(Duration::ZERO),
            Duration::from_millis(7)
        );
        assert_eq!(async_readback_poll_interval(), Duration::from_millis(1));
        assert_eq!(
            set_async_readback_timeout(Duration::ZERO),
            Duration::from_millis(123)
        );
        assert_eq!(async_readback_timeout(), Duration::from_millis(1));

        set_async_readback_poll_interval(previous_interval);
        set_async_readback_timeout(previous_timeout);
    }
}
