use std::collections::HashMap;
use std::future::poll_fn;
use std::sync::{Arc, Mutex};
use std::task::Poll;

use crate::GpuError;

use super::{readback::ReadbackRequest, temp_pool};

struct MapState {
    done: Option<Result<(), wgpu::BufferAsyncError>>,
    waker: Option<std::task::Waker>,
}

async fn map_read_async(
    device: &wgpu::Device,
    slice: wgpu::BufferSlice<'_>,
) -> Result<(), wgpu::BufferAsyncError> {
    let state = Arc::new(Mutex::new(MapState {
        done: None,
        waker: None,
    }));

    slice.map_async(wgpu::MapMode::Read, {
        let state = Arc::clone(&state);
        move |res| {
            let waker = {
                let mut state = state.lock().expect("map state lock");
                state.done = Some(res);
                state.waker.take()
            };
            if let Some(waker) = waker {
                waker.wake();
            }
        }
    });

    poll_fn(|cx| {
        let _ = device.poll(wgpu::PollType::Poll);
        let mut state = state.lock().expect("map state lock");
        if let Some(done) = state.done.take() {
            return Poll::Ready(done);
        }
        state.waker = Some(cx.waker().clone());
        Poll::Pending
    })
    .await
}

pub(crate) async fn resolve_readbacks_async(
    device: &wgpu::Device,
    readbacks: Vec<ReadbackRequest>,
) -> Result<HashMap<u32, Vec<u8>>, GpuError> {
    let device_key = device as *const _ as usize;
    let mut result = HashMap::new();

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
        map_read_async(device, slice)
            .await
            .map_err(|e| GpuError::Internal(format!("map failed: {e:?}")))?;

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
        }
    }

    Ok(result)
}
