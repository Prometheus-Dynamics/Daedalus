use wgpu::{Adapter, Backends};

use crate::GpuBackends;
use crate::backends::resolve_backends;

/// The `wgpu` backends to enumerate: `WGPU_BACKEND`, then `requested`, then every backend.
pub(super) fn preferred_backends(requested: Option<GpuBackends>) -> Backends {
    let env = std::env::var("WGPU_BACKEND")
        .ok()
        .map(|raw| GpuBackends::from_names(&raw));
    to_wgpu(resolve_backends(env, requested))
}

fn to_wgpu(backends: GpuBackends) -> Backends {
    let mut out = Backends::empty();
    out.set(Backends::VULKAN, backends.contains(GpuBackends::VULKAN));
    out.set(Backends::METAL, backends.contains(GpuBackends::METAL));
    out.set(Backends::DX12, backends.contains(GpuBackends::DX12));
    out.set(Backends::GL, backends.contains(GpuBackends::GL));
    out.set(
        Backends::BROWSER_WEBGPU,
        backends.contains(GpuBackends::BROWSER_WEBGPU),
    );
    out
}

pub(super) fn select_best_adapter(adapters: Vec<Adapter>) -> Option<Adapter> {
    adapters.into_iter().max_by_key(adapter_score)
}

fn adapter_score(adapter: &Adapter) -> i64 {
    let info = adapter.get_info();
    let mut score: i64 = match info.backend {
        wgpu::Backend::Vulkan => 500,
        wgpu::Backend::Metal => 450,
        wgpu::Backend::Dx12 => 400,
        wgpu::Backend::Gl => 250,
        wgpu::Backend::BrowserWebGpu => 150,
        wgpu::Backend::Noop => 0,
    };

    score += match info.device_type {
        wgpu::DeviceType::DiscreteGpu => 120,
        wgpu::DeviceType::IntegratedGpu => 90,
        wgpu::DeviceType::VirtualGpu => 40,
        wgpu::DeviceType::Cpu => -300,
        wgpu::DeviceType::Other => 0,
    };

    // Strongly avoid software adapters when possible.
    let lower_name = info.name.to_ascii_lowercase();
    if lower_name.contains("llvmpipe")
        || lower_name.contains("lavapipe")
        || lower_name.contains("softpipe")
    {
        score -= 800;
    }

    // Prefer adapters that can run our storage-heavy image pipeline.
    let rgba = adapter.get_texture_format_features(wgpu::TextureFormat::Rgba8Unorm);
    if rgba
        .allowed_usages
        .contains(wgpu::TextureUsages::STORAGE_BINDING)
    {
        score += 90;
    } else {
        score -= 200;
    }
    if rgba
        .allowed_usages
        .contains(wgpu::TextureUsages::TEXTURE_BINDING)
    {
        score += 20;
    } else {
        score -= 50;
    }
    if rgba.allowed_usages.contains(wgpu::TextureUsages::COPY_SRC)
        && rgba.allowed_usages.contains(wgpu::TextureUsages::COPY_DST)
    {
        score += 20;
    } else {
        score -= 120;
    }

    score
}
