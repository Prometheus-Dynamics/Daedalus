use wgpu::{Adapter, Backends};

pub(super) fn preferred_backends() -> Backends {
    Backends::from_env().unwrap_or_else(|| {
        #[cfg(all(target_os = "linux", target_arch = "aarch64"))]
        {
            // Headless Linux ARM targets (CM5) are significantly more stable with Vulkan.
            Backends::VULKAN
        }
        #[cfg(not(all(target_os = "linux", target_arch = "aarch64")))]
        {
            Backends::all()
        }
    })
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
