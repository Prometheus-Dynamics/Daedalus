use std::collections::HashMap;
use std::sync::{Arc, Mutex, OnceLock};
#[cfg(feature = "gpu-async")]
use std::{
    future::Future,
    pin::Pin,
    task::{Context, Poll, Waker},
};

use crate::GpuError;

use super::{BindingSpec, ShaderSpec, SubmissionTracker, infer_bindings, infer_workgroup_size};

#[derive(Clone)]
pub(crate) struct CachedSpec {
    pub bindings: Arc<[BindingSpec]>,
    pub workgroup_size: Option<[u32; 3]>,
}

#[derive(Clone, Debug, PartialEq, Eq, Hash)]
struct ShaderSpecCacheKey {
    name: &'static str,
    src: &'static str,
    entry: &'static str,
    workgroup_size: Option<[u32; 3]>,
    bindings: Vec<BindingSpec>,
}

impl From<&ShaderSpec> for ShaderSpecCacheKey {
    fn from(spec: &ShaderSpec) -> Self {
        Self {
            name: spec.name,
            src: spec.src,
            entry: spec.entry,
            workgroup_size: spec.workgroup_size,
            bindings: spec.bindings.to_vec(),
        }
    }
}

pub(crate) fn cached_spec(spec: &ShaderSpec) -> Result<CachedSpec, GpuError> {
    static CACHE: OnceLock<Mutex<HashMap<ShaderSpecCacheKey, CachedSpec>>> = OnceLock::new();
    let key = ShaderSpecCacheKey::from(spec);
    if let Ok(m) = CACHE.get_or_init(|| Mutex::new(HashMap::new())).lock()
        && let Some(cached) = m.get(&key).cloned()
    {
        return Ok(cached);
    }

    let bindings: Arc<[BindingSpec]> = if spec.bindings.is_empty() {
        Arc::from(
            infer_bindings(spec.src)
                .ok_or_else(|| GpuError::Internal("failed to infer bindings from shader".into()))?
                .into_boxed_slice(),
        )
    } else {
        Arc::from(spec.bindings)
    };
    let workgroup_size = spec
        .workgroup_size
        .or_else(|| infer_workgroup_size(spec.src));
    let cached = CachedSpec {
        bindings,
        workgroup_size,
    };
    if let Ok(mut m) = CACHE.get_or_init(|| Mutex::new(HashMap::new())).lock() {
        m.insert(key, cached.clone());
    }
    Ok(cached)
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::shader::BindingKind;
    #[cfg(feature = "gpu-async")]
    use std::task::{Context, Poll, Wake, Waker};
    #[cfg(feature = "gpu-async")]
    use std::{pin::Pin, sync::Arc};

    const STORAGE_SHADER: &str = r#"
        @group(0) @binding(0)
        var<storage, read> input: array<f32>;

        @compute @workgroup_size(8)
        fn main(@builtin(global_invocation_id) id: vec3<u32>) {
            _ = input[id.x];
        }
    "#;

    const UNIFORM_SHADER: &str = r#"
        struct Params { value: f32 };

        @group(0) @binding(0)
        var<uniform> params: Params;

        @compute @workgroup_size(4)
        fn main(@builtin(global_invocation_id) id: vec3<u32>) {
            _ = params.value + f32(id.x);
        }
    "#;

    #[test]
    fn cached_spec_uses_shader_content_not_stack_address() {
        fn cached_binding_kind(src: &'static str) -> BindingKind {
            let spec = ShaderSpec {
                name: "stack-built",
                src,
                entry: "main",
                workgroup_size: None,
                bindings: &[],
            };
            cached_spec(&spec).expect("cached spec").bindings[0].kind
        }

        assert_eq!(cached_binding_kind(STORAGE_SHADER), BindingKind::Storage);
        assert_eq!(cached_binding_kind(UNIFORM_SHADER), BindingKind::Uniform);
        assert_eq!(cached_binding_kind(STORAGE_SHADER), BindingKind::Storage);
    }

    #[cfg(feature = "gpu-async")]
    struct NoopWake;

    #[cfg(feature = "gpu-async")]
    impl Wake for NoopWake {
        fn wake(self: Arc<Self>) {}
    }

    #[cfg(feature = "gpu-async")]
    #[test]
    fn ctx_async_init_waiter_unregisters_on_cancel() {
        if GPU_CTX.get().is_some() {
            return;
        }
        let Some(leader) = try_start_ctx_async_init() else {
            return;
        };
        assert!(try_start_ctx_async_init().is_none());

        let mut wait = Box::pin(GpuCtxInitWaitFuture::new());
        let waker = Waker::from(Arc::new(NoopWake));
        let mut cx = Context::from_waker(&waker);

        assert!(matches!(Pin::new(&mut wait).poll(&mut cx), Poll::Pending));
        assert_eq!(ctx_async_waiter_count(), 1);

        drop(wait);
        assert_eq!(ctx_async_waiter_count(), 0);
        leader.finish();
    }
}

#[derive(Clone)]
pub(crate) struct GpuCtx {
    pub device: Arc<wgpu::Device>,
    pub queue: Arc<wgpu::Queue>,
    pub submission_tracker: Arc<SubmissionTracker>,
}

static GPU_CTX: OnceLock<Result<GpuCtx, GpuError>> = OnceLock::new();

#[cfg(feature = "gpu-async")]
static GPU_CTX_ASYNC_INIT: OnceLock<Mutex<GpuCtxAsyncInit>> = OnceLock::new();

#[cfg(feature = "gpu-async")]
#[derive(Default)]
struct GpuCtxAsyncInit {
    in_progress: bool,
    next_waiter_id: u64,
    waiters: Vec<GpuCtxInitWaiter>,
}

#[cfg(feature = "gpu-async")]
struct GpuCtxInitWaiter {
    id: u64,
    waker: Waker,
}

async fn create_ctx_async() -> Result<GpuCtx, GpuError> {
    let instance = wgpu::Instance::default();
    let adapter = instance
        .request_adapter(&wgpu::RequestAdapterOptions {
            power_preference: wgpu::PowerPreference::HighPerformance,
            compatible_surface: None,
            force_fallback_adapter: false,
        })
        .await
        .map_err(|_| GpuError::AdapterUnavailable)?;

    let (device, queue) = adapter
        .request_device(&wgpu::DeviceDescriptor {
            label: Some("device"),
            required_features: wgpu::Features::empty(),
            required_limits: adapter.limits(),
            experimental_features: wgpu::ExperimentalFeatures::disabled(),
            memory_hints: wgpu::MemoryHints::default(),
            trace: wgpu::Trace::default(),
        })
        .await
        .map_err(|e| GpuError::Internal(e.to_string()))?;

    Ok(GpuCtx {
        device: Arc::new(device),
        queue: Arc::new(queue),
        submission_tracker: Arc::new(SubmissionTracker::default()),
    })
}

pub(crate) fn ctx() -> Result<&'static GpuCtx, GpuError> {
    // Synchronous shader helpers are compatibility APIs: first use creates the fallback wgpu
    // device with `pollster::block_on`, so callers on async executors should prefer `ctx_async`.
    GPU_CTX
        .get_or_init(|| pollster::block_on(create_ctx_async()))
        .as_ref()
        .map_err(|e| e.clone())
}

#[cfg(feature = "gpu-async")]
pub(crate) async fn ctx_async() -> Result<&'static GpuCtx, GpuError> {
    // Async shader helpers avoid blocking the current thread while creating the fallback context.
    loop {
        if let Some(res) = GPU_CTX.get() {
            return res.as_ref().map_err(|e| e.clone());
        }
        if let Some(leader) = try_start_ctx_async_init() {
            let init = create_ctx_async().await;
            let _ = GPU_CTX.set(init);
            leader.finish();
            return GPU_CTX
                .get()
                .expect("ctx_async set result")
                .as_ref()
                .map_err(|e| e.clone());
        }
        GpuCtxInitWaitFuture::new().await;
    }
}

#[cfg(feature = "gpu-async")]
fn ctx_async_init_state() -> &'static Mutex<GpuCtxAsyncInit> {
    GPU_CTX_ASYNC_INIT.get_or_init(|| Mutex::new(GpuCtxAsyncInit::default()))
}

#[cfg(all(test, feature = "gpu-async"))]
fn ctx_async_waiter_count() -> usize {
    ctx_async_init_state()
        .lock()
        .unwrap_or_else(|poisoned| poisoned.into_inner())
        .waiters
        .len()
}

#[cfg(feature = "gpu-async")]
fn try_start_ctx_async_init() -> Option<GpuCtxInitLeader> {
    let mut state = ctx_async_init_state()
        .lock()
        .unwrap_or_else(|poisoned| poisoned.into_inner());
    if state.in_progress {
        return None;
    }
    state.in_progress = true;
    Some(GpuCtxInitLeader { active: true })
}

#[cfg(feature = "gpu-async")]
struct GpuCtxInitLeader {
    active: bool,
}

#[cfg(feature = "gpu-async")]
impl GpuCtxInitLeader {
    fn finish(mut self) {
        self.wake_waiters();
        self.active = false;
    }

    fn wake_waiters(&self) {
        let waiters = {
            let mut state = ctx_async_init_state()
                .lock()
                .unwrap_or_else(|poisoned| poisoned.into_inner());
            state.in_progress = false;
            state.waiters.drain(..).collect::<Vec<_>>()
        };
        for waiter in waiters {
            waiter.waker.wake();
        }
    }
}

#[cfg(feature = "gpu-async")]
impl Drop for GpuCtxInitLeader {
    fn drop(&mut self) {
        if self.active {
            self.wake_waiters();
        }
    }
}

#[cfg(feature = "gpu-async")]
struct GpuCtxInitWaitFuture {
    waiter_id: Option<u64>,
}

#[cfg(feature = "gpu-async")]
impl GpuCtxInitWaitFuture {
    fn new() -> Self {
        Self { waiter_id: None }
    }
}

#[cfg(feature = "gpu-async")]
impl Future for GpuCtxInitWaitFuture {
    type Output = ();

    fn poll(mut self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<Self::Output> {
        if GPU_CTX.get().is_some() {
            return Poll::Ready(());
        }
        let mut state = ctx_async_init_state()
            .lock()
            .unwrap_or_else(|poisoned| poisoned.into_inner());
        if !state.in_progress {
            return Poll::Ready(());
        }
        match self.waiter_id {
            Some(id) => {
                if let Some(waiter) = state.waiters.iter_mut().find(|waiter| waiter.id == id)
                    && !waiter.waker.will_wake(cx.waker())
                {
                    waiter.waker = cx.waker().clone();
                }
            }
            None => {
                let id = state.next_waiter_id;
                state.next_waiter_id = state.next_waiter_id.wrapping_add(1);
                state.waiters.push(GpuCtxInitWaiter {
                    id,
                    waker: cx.waker().clone(),
                });
                drop(state);
                self.waiter_id = Some(id);
            }
        }
        Poll::Pending
    }
}

#[cfg(feature = "gpu-async")]
impl Drop for GpuCtxInitWaitFuture {
    fn drop(&mut self) {
        let Some(id) = self.waiter_id.take() else {
            return;
        };
        let mut state = ctx_async_init_state()
            .lock()
            .unwrap_or_else(|poisoned| poisoned.into_inner());
        state.waiters.retain(|waiter| waiter.id != id);
    }
}
