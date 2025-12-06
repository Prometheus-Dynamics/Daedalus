use std::collections::HashMap;
use std::sync::{Arc, Mutex, OnceLock};

use crate::GpuError;

use super::{BindingSpec, ShaderSpec, infer_bindings, infer_workgroup_size};

#[derive(Clone)]
pub(crate) struct CachedSpec {
    pub bindings: Arc<[BindingSpec]>,
    pub workgroup_size: Option<[u32; 3]>,
}

pub(crate) fn cached_spec(spec: &ShaderSpec) -> Result<CachedSpec, GpuError> {
    static CACHE: OnceLock<Mutex<HashMap<usize, CachedSpec>>> = OnceLock::new();
    let key = spec as *const _ as usize;
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

#[derive(Clone)]
pub(crate) struct GpuCtx {
    pub device: Arc<wgpu::Device>,
    pub queue: Arc<wgpu::Queue>,
}

static GPU_CTX: OnceLock<Result<GpuCtx, GpuError>> = OnceLock::new();

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
    })
}

pub(crate) fn ctx() -> Result<&'static GpuCtx, GpuError> {
    GPU_CTX
        .get_or_init(|| pollster::block_on(create_ctx_async()))
        .as_ref()
        .map_err(|e| e.clone())
}

#[cfg(feature = "gpu-async")]
pub(crate) async fn ctx_async() -> Result<&'static GpuCtx, GpuError> {
    if let Some(res) = GPU_CTX.get() {
        return res.as_ref().map_err(|e| e.clone());
    }
    let init = create_ctx_async().await;
    let _ = GPU_CTX.set(init);
    GPU_CTX
        .get()
        .expect("ctx_async set result")
        .as_ref()
        .map_err(|e| e.clone())
}
