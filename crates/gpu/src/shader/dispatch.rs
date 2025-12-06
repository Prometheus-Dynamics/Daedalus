use std::collections::HashMap;
use std::sync::{Mutex, OnceLock};

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

impl<'ctx, 'a, B: GpuBindings<'a>> SingleDispatch<'ctx, 'a, B> {
    pub fn dispatch(self, gpu: Option<&GpuContextHandle>) -> Result<ShaderRunOutput, GpuError> {
        self.ctx.dispatch_bindings(self.bindings, gpu, None, None)
    }

    /// Dispatch using inferred workgroup counts and the context GPU (if available).
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
    let (device, queue, backend_handle) = if let Some(gpu_ctx) = gpu_ctx {
        let backend = gpu_ctx.backend_ref();
        let (device, queue) = backend.wgpu_device_queue().ok_or(GpuError::Unsupported)?;
        (device, queue, Some(backend))
    } else {
        let ctx = ctx()?;
        (ctx.device.as_ref(), ctx.queue.as_ref(), None)
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

    queue.submit(Some(encoder.finish()));

    let result = resolve_readbacks(device, readbacks)?;
    return_pooled_textures(pool_textures_to_return);

    Ok(ShaderRunOutput {
        buffers: result,
        textures: texture_handles,
    })
}
