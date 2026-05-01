use std::collections::{HashMap, VecDeque};
#[cfg(feature = "gpu-async")]
use std::future::poll_fn;
#[cfg(feature = "gpu-async")]
use std::sync::Arc;
use std::sync::{
    Mutex, OnceLock,
    atomic::{AtomicUsize, Ordering},
};
#[cfg(feature = "gpu-async")]
use std::task::Poll;

use super::fallback::{cached_spec, ctx};
use super::pipeline::{bind_group, pipeline_entry};
use super::prepare::prepare_resources;
use super::readback::{enqueue_readbacks, resolve_readbacks, return_pooled_textures};
use super::workgroups::derive_workgroups;
use super::{DispatchOptions, GpuBindings, ShaderBinding, ShaderRunOutput, ShaderSpec};
use crate::{GpuContextHandle, GpuError};

/// Lightweight context passed into shader-enabled nodes; wraps a spec + source and
/// exposes a simple dispatch helper.
pub struct ShaderInstance {
    pub name: &'static str,
    pub spec: &'static ShaderSpec,
}

pub struct ShaderContext {
    pub shaders: &'static [ShaderInstance],
    pub gpu: Option<GpuContextHandle>,
}

pub struct SingleDispatch<'ctx, 'a, B: GpuBindings<'a>> {
    pub(super) ctx: &'ctx ShaderContext,
    pub(super) bindings: &'a B,
}

static MAX_INFLIGHT_SUBMISSIONS_PER_DEVICE: AtomicUsize = AtomicUsize::new(2);

/// Tracks in-flight queue submissions for a single wgpu device.
///
/// The tracker is owned by the backend or fallback GPU context so tracked submissions have the
/// same lifetime as the device instead of living in a process-global map keyed by pointer address.
#[derive(Debug, Default)]
pub struct SubmissionTracker {
    in_flight: Mutex<VecDeque<wgpu::SubmissionIndex>>,
}

#[cfg(feature = "gpu-async")]
struct SubmissionWaitState {
    result: Option<Result<(), GpuError>>,
    waker: Option<std::task::Waker>,
}

#[cfg(feature = "gpu-async")]
async fn wait_for_submission_async(
    device: &wgpu::Device,
    submission: wgpu::SubmissionIndex,
) -> Result<(), GpuError> {
    let state = Arc::new(Mutex::new(SubmissionWaitState {
        result: None,
        waker: None,
    }));
    let wait_state = Arc::clone(&state);
    let device = device.clone();
    if let Err(error) = super::poll_driver::submit_poll_job("submission_wait", move || {
        let result = device
            .poll(wgpu::PollType::Wait {
                submission_index: Some(submission),
                timeout: None,
            })
            .map(|_| ())
            .map_err(|error| GpuError::Internal(format!("submission poll failed: {error:?}")));
        let waker = {
            let mut state = wait_state
                .lock()
                .unwrap_or_else(|poisoned| poisoned.into_inner());
            state.result = Some(result);
            state.waker.take()
        };
        if let Some(waker) = waker {
            waker.wake();
        }
    }) {
        let mut state = state
            .lock()
            .unwrap_or_else(|poisoned| poisoned.into_inner());
        state.result = Some(Err(GpuError::Internal(error.to_string())));
        if let Some(waker) = state.waker.take() {
            waker.wake();
        }
    }

    poll_fn(|cx| {
        let mut state = state
            .lock()
            .unwrap_or_else(|poisoned| poisoned.into_inner());
        if let Some(result) = state.result.take() {
            return Poll::Ready(result);
        }
        state.waker = Some(cx.waker().clone());
        Poll::Pending
    })
    .await
}

impl SubmissionTracker {
    fn lock_in_flight(&self) -> std::sync::MutexGuard<'_, VecDeque<wgpu::SubmissionIndex>> {
        self.in_flight.lock().unwrap_or_else(|poisoned| {
            tracing::warn!(
                target: "daedalus_gpu::dispatch",
                "gpu submission tracker lock poisoned; recovering tracked submissions"
            );
            poisoned.into_inner()
        })
    }

    pub(crate) fn track_and_throttle(
        &self,
        device: &wgpu::Device,
        submission: wgpu::SubmissionIndex,
    ) {
        {
            let mut in_flight = self.lock_in_flight();
            in_flight.push_back(submission.clone());
            tracing::debug!(
                target: "daedalus_gpu::dispatch",
                submission = ?submission,
                in_flight = in_flight.len(),
                limit = max_inflight_submissions_per_device(),
                "gpu submission tracked"
            );
        }

        loop {
            let wait_for = {
                let in_flight = self.lock_in_flight();
                if in_flight.len() > max_inflight_submissions_per_device() {
                    in_flight.front().cloned()
                } else {
                    None
                }
            };

            let Some(wait_for) = wait_for else {
                // Keep retirements moving even when we don't need to block.
                let _ = device.poll(wgpu::PollType::Poll);
                break;
            };

            let poll_result = device.poll(wgpu::PollType::Wait {
                submission_index: Some(wait_for.clone()),
                // Use a short timeout first to avoid blocking indefinitely in normal operation.
                timeout: Some(std::time::Duration::from_millis(5)),
            });

            match poll_result {
                Ok(status) if status.wait_finished() => {
                    tracing::debug!(
                        target: "daedalus_gpu::dispatch",
                        submission = ?wait_for,
                        "gpu submission retired after throttle poll"
                    );
                    self.lock_in_flight().pop_front();
                }
                Err(wgpu::PollError::Timeout) => {
                    // Hard backpressure path: block until this submission completes so we do not allow
                    // unbounded in-flight GPU work (which can trigger OOM on embedded GPUs).
                    tracing::warn!(
                        target: "daedalus_gpu::dispatch",
                        submission = ?wait_for,
                        limit = max_inflight_submissions_per_device(),
                        "gpu submission throttle timed out; waiting for completion"
                    );
                    let _ = device.poll(wgpu::PollType::Wait {
                        submission_index: Some(wait_for),
                        timeout: None,
                    });
                    self.lock_in_flight().pop_front();
                }
                Err(wgpu::PollError::WrongSubmissionIndex(_, _)) => {
                    tracing::warn!(
                        target: "daedalus_gpu::dispatch",
                        "gpu submission index was no longer valid; clearing tracked submissions"
                    );
                    self.lock_in_flight().clear();
                }
                _ => {}
            }
        }
    }

    #[cfg(feature = "gpu-async")]
    pub(crate) async fn track_and_throttle_async(
        &self,
        device: &wgpu::Device,
        submission: wgpu::SubmissionIndex,
    ) {
        {
            let mut in_flight = self.lock_in_flight();
            in_flight.push_back(submission.clone());
            tracing::debug!(
                target: "daedalus_gpu::dispatch",
                submission = ?submission,
                in_flight = in_flight.len(),
                limit = max_inflight_submissions_per_device(),
                "gpu submission tracked for async throttling"
            );
        }

        loop {
            let wait_for = {
                let in_flight = self.lock_in_flight();
                if in_flight.len() > max_inflight_submissions_per_device() {
                    in_flight.front().cloned()
                } else {
                    None
                }
            };

            let Some(wait_for) = wait_for else {
                let _ = device.poll(wgpu::PollType::Poll);
                break;
            };

            tracing::debug!(
                target: "daedalus_gpu::dispatch",
                submission = ?wait_for,
                limit = max_inflight_submissions_per_device(),
                "async gpu submission throttle waiting"
            );
            match wait_for_submission_async(device, wait_for.clone()).await {
                Ok(()) => {
                    tracing::debug!(
                        target: "daedalus_gpu::dispatch",
                        submission = ?wait_for,
                        "async gpu submission retired"
                    );
                    self.lock_in_flight().pop_front();
                }
                Err(error) => {
                    tracing::warn!(
                        target: "daedalus_gpu::dispatch",
                        error = %error,
                        submission = ?wait_for,
                        "async gpu submission throttle poll failed"
                    );
                    self.lock_in_flight().pop_front();
                }
            }
        }
    }

    #[cfg(test)]
    fn tracked_len_for_test(&self) -> usize {
        self.lock_in_flight().len()
    }
}

/// Set the maximum tracked in-flight submissions per device before dispatch throttles.
/// Returns the previous limit.
pub fn set_max_inflight_submissions_per_device(limit: usize) -> usize {
    MAX_INFLIGHT_SUBMISSIONS_PER_DEVICE.swap(limit.max(1), Ordering::Relaxed)
}

/// Current maximum tracked in-flight submissions per device.
pub fn max_inflight_submissions_per_device() -> usize {
    MAX_INFLIGHT_SUBMISSIONS_PER_DEVICE
        .load(Ordering::Relaxed)
        .max(1)
}

impl<'ctx, 'a, B: GpuBindings<'a>> SingleDispatch<'ctx, 'a, B> {
    /// Dispatch synchronously.
    ///
    /// This is a blocking compatibility helper: it may wait for GPU submission throttling or
    /// readback on the current thread. Async runtimes should use the `*_async` shader APIs.
    pub fn dispatch(self, gpu: Option<&GpuContextHandle>) -> Result<ShaderRunOutput, GpuError> {
        self.ctx.dispatch_bindings(self.bindings, gpu, None, None)
    }

    /// Dispatch synchronously using inferred workgroup counts and the context GPU, if available.
    ///
    /// This may block the current thread. Async runtimes should use `dispatch_auto_async`.
    pub fn dispatch_auto(self) -> Result<ShaderRunOutput, GpuError> {
        self.ctx.dispatch_bindings(self.bindings, None, None, None)
    }
}

impl ShaderContext {
    pub fn new(shaders: &'static [ShaderInstance]) -> Self {
        Self { shaders, gpu: None }
    }

    pub fn with_gpu(shaders: &'static [ShaderInstance], gpu: GpuContextHandle) -> Self {
        Self {
            shaders,
            gpu: Some(gpu),
        }
    }

    pub(super) fn resolve_gpu<'a>(
        &'a self,
        gpu_override: Option<&'a GpuContextHandle>,
    ) -> Option<&'a GpuContextHandle> {
        gpu_override.or(self.gpu.as_ref())
    }

    pub(super) fn shader_by_name(&self, name: &str) -> Option<&'static ShaderInstance> {
        static NAME_CACHE: OnceLock<
            Mutex<HashMap<usize, HashMap<&'static str, &'static ShaderInstance>>>,
        > = OnceLock::new();
        let key = self.shaders.as_ptr() as usize;
        if let Some(inst) = NAME_CACHE
            .get_or_init(|| Mutex::new(HashMap::new()))
            .lock()
            .ok()
            .and_then(|m| m.get(&key).and_then(|inner| inner.get(name).copied()))
        {
            return Some(inst);
        }
        if let Ok(mut m) = NAME_CACHE.get_or_init(|| Mutex::new(HashMap::new())).lock() {
            let entry = m
                .entry(key)
                .or_insert_with(|| self.shaders.iter().map(|s| (s.name, s)).collect());
            return entry.get(name).copied();
        }
        self.shaders.iter().find(|s| s.name == name)
    }

    /// Dispatch the first shader in this context with explicitly described bindings.
    pub fn dispatch_first(
        &self,
        bindings: &[ShaderBinding],
        gpu: Option<&GpuContextHandle>,
        workgroups: Option<[u32; 3]>,
        invocations: Option<[u32; 3]>,
    ) -> Result<ShaderRunOutput, GpuError> {
        let inst = self
            .shaders
            .first()
            .ok_or_else(|| GpuError::Internal("no shaders registered".into()))?;
        dispatch_shader_with_bindings(
            inst.spec,
            inst.spec.src,
            bindings,
            gpu,
            workgroups,
            invocations,
        )
    }

    /// Dispatch a shader by name with explicitly described bindings.
    pub fn dispatch_by_name(
        &self,
        name: &str,
        bindings: &[ShaderBinding],
        gpu: Option<&GpuContextHandle>,
        workgroups: Option<[u32; 3]>,
        invocations: Option<[u32; 3]>,
    ) -> Result<ShaderRunOutput, GpuError> {
        let inst = self
            .shader_by_name(name)
            .ok_or_else(|| GpuError::Internal(format!("shader `{}` not found", name)))?;
        dispatch_shader_with_bindings(
            inst.spec,
            inst.spec.src,
            bindings,
            gpu,
            workgroups,
            invocations,
        )
    }

    pub fn dispatch_bindings<'a, B: GpuBindings<'a>>(
        &self,
        bindings: &'a B,
        gpu: Option<&GpuContextHandle>,
        workgroups: Option<[u32; 3]>,
        invocations: Option<[u32; 3]>,
    ) -> Result<ShaderRunOutput, GpuError> {
        let inferred_invocations = invocations.or_else(|| bindings.invocation_hint());
        let gpu = self.resolve_gpu(gpu);
        dispatch_shader_with_bindings(
            B::spec(),
            B::spec().src,
            &bindings.bindings(gpu)?,
            gpu,
            workgroups,
            inferred_invocations,
        )
    }

    /// Convenience builder for single-dispatch calls.
    pub fn single<'a, B: GpuBindings<'a>>(&self, bindings: &'a B) -> SingleDispatch<'_, 'a, B> {
        SingleDispatch {
            ctx: self,
            bindings,
        }
    }
}

pub fn dispatch_shader_with_bindings(
    spec: &ShaderSpec,
    shader_src: &str,
    bindings: &[ShaderBinding],
    gpu_ctx: Option<&GpuContextHandle>,
    workgroups: Option<[u32; 3]>,
    invocations: Option<[u32; 3]>,
) -> Result<ShaderRunOutput, GpuError> {
    dispatch_shader_with_options(
        spec,
        shader_src,
        bindings,
        gpu_ctx,
        &DispatchOptions {
            workgroups,
            invocations,
        },
    )
}

pub fn dispatch_shader_with_options(
    spec: &ShaderSpec,
    shader_src: &str,
    bindings: &[ShaderBinding],
    gpu_ctx: Option<&GpuContextHandle>,
    opts: &DispatchOptions,
) -> Result<ShaderRunOutput, GpuError> {
    let (device, queue, backend_handle, submission_tracker) = if let Some(gpu_ctx) = gpu_ctx {
        let backend = gpu_ctx.backend_ref();
        let (device, queue) = backend.wgpu_device_queue().ok_or(GpuError::Unsupported)?;
        (
            device,
            queue,
            Some(backend),
            backend.wgpu_submission_tracker(),
        )
    } else {
        let ctx = ctx()?;
        (
            ctx.device.as_ref(),
            ctx.queue.as_ref(),
            None,
            Some(ctx.submission_tracker.as_ref()),
        )
    };

    let cached = cached_spec(spec)?;
    let layout_bindings = cached.bindings.clone();

    if layout_bindings.is_empty() {
        return Err(GpuError::Internal("shader defines no bindings".into()));
    }
    if let Some(wg) = cached.workgroup_size
        && wg.iter().copied().any(|v| v == 0)
    {
        return Err(GpuError::Internal("workgroup_size must be > 0".into()));
    }

    let prepared = prepare_resources(
        device,
        queue,
        backend_handle,
        bindings,
        layout_bindings.as_ref(),
        gpu_ctx,
    )?;

    let entry = pipeline_entry(device, shader_src, spec, layout_bindings.as_ref());
    let (bind_group_layout, pipeline) = (&entry.bind_group_layout, &entry.pipeline);
    let bind_group = bind_group(device, bind_group_layout, &prepared, entry.key);

    let mut encoder = device.create_command_encoder(&wgpu::CommandEncoderDescriptor {
        label: Some("encoder"),
    });
    {
        let mut cpass = encoder.begin_compute_pass(&wgpu::ComputePassDescriptor {
            label: Some("compute"),
            timestamp_writes: None,
        });
        cpass.set_pipeline(pipeline);
        cpass.set_bind_group(0, &bind_group, &[]);
        let wg_size = cached
            .workgroup_size
            .ok_or_else(|| GpuError::Internal("failed to infer workgroup size".into()))?;
        let wg_x = wg_size[0].max(1);
        let wg_y = wg_size[1].max(1);
        let wg_z = wg_size[2].max(1);

        let workgroups = if let Some(wg) = opts.workgroups {
            wg
        } else if let Some(inv) = opts.invocations {
            let inv_x = inv[0];
            let inv_y = inv[1].max(1);
            let inv_z = inv[2].max(1);
            let x = inv_x.div_ceil(wg_x);
            let y = inv_y.div_ceil(wg_y);
            let z = inv_z.div_ceil(wg_z);
            [x.max(1), y.max(1), z.max(1)]
        } else {
            derive_workgroups(&prepared, wg_x, wg_y, wg_z)?
        };
        cpass.dispatch_workgroups(workgroups[0], workgroups[1], workgroups[2]);
    }

    let (readbacks, pool_textures_to_return, texture_handles) =
        enqueue_readbacks(device, &prepared, &mut encoder);

    let submission_idx = queue.submit(Some(encoder.finish()));
    if let Some(tracker) = submission_tracker {
        tracker.track_and_throttle(device, submission_idx);
    } else {
        tracing::warn!(
            target: "daedalus_gpu::dispatch",
            "wgpu backend did not expose a submission tracker; falling back to untracked polling"
        );
        let _ = device.poll(wgpu::PollType::Poll);
    }

    let result = resolve_readbacks(device, readbacks)?;
    return_pooled_textures(pool_textures_to_return);

    Ok(ShaderRunOutput {
        buffers: result,
        textures: texture_handles,
    })
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn inflight_submission_limit_is_configurable() {
        let previous = set_max_inflight_submissions_per_device(3);
        assert_eq!(max_inflight_submissions_per_device(), 3);
        assert_eq!(set_max_inflight_submissions_per_device(0), 3);
        assert_eq!(max_inflight_submissions_per_device(), 1);
        set_max_inflight_submissions_per_device(previous);
    }

    #[test]
    fn submission_trackers_are_per_context() {
        let first = SubmissionTracker::default();
        let second = SubmissionTracker::default();

        assert_eq!(first.tracked_len_for_test(), 0);
        assert_eq!(second.tracked_len_for_test(), 0);
    }
}
