//! Pipeline build time with an empty pipeline cache and with one reloaded from disk. Run with
//! `MESA_SHADER_CACHE_DISABLE=true` to measure Daedalus's cache alone (Mesa keeps its own).

use std::time::Instant;

use daedalus_gpu::{
    FragmentPass, GpuBackendKind, GpuOptions, GpuPipelineCache, select_backend, wgpu,
};

use super::kv;

/// Distinct fragment shaders, each a small dense kernel.
fn shaders() -> Vec<String> {
    (0..12)
        .map(|k| {
            format!(
                "@group(0) @binding(0) var src: texture_2d<f32>;
                 @fragment fn fs(@builtin(position) pos: vec4<f32>) -> @location(0) vec4<f32> {{
                     let c = vec2<i32>(pos.xy);
                     var acc = vec4<f32>(0.0);
                     for (var i = 0; i < {taps}; i++) {{
                         acc += textureLoad(src, c + vec2<i32>(i % 3 - 1, i / 3 - 1), 0) * f32(i + {k});
                     }}
                     return acc * {scale};
                 }}",
                taps = 3 + k % 7,
                scale = 1.0 / f64::from(k + 2)
            )
        })
        .collect()
}

pub(super) fn startup() {
    let ctx = match select_backend(&GpuOptions {
        preferred_backend: Some(GpuBackendKind::Wgpu),
        ..Default::default()
    }) {
        Ok(ctx) if ctx.wgpu().is_some() => ctx,
        _ => return kv("pipeline_cache", "no wgpu context"),
    };
    let dir = std::env::temp_dir().join(format!("gpu-bench-pipeline-cache-{}", std::process::id()));
    let _ = std::fs::remove_dir_all(&dir);
    let shaders = shaders();
    let build = |label: &str| {
        let start = Instant::now();
        for shader in &shaders {
            if let Err(err) = FragmentPass::new(&ctx, shader, &[wgpu::TextureFormat::R32Float]) {
                return kv(label, format!("build failed: {err}"));
            }
        }
        kv(
            label,
            format!(
                "{:.2} ms for {} pipelines",
                start.elapsed().as_secs_f64() * 1e3,
                shaders.len()
            ),
        );
    };
    let cold = match GpuPipelineCache::open(&ctx, &dir) {
        Ok(cache) => cache,
        Err(err) => return kv("pipeline_cache", format!("unsupported: {err}")),
    };
    cold.install();
    build("pipelines_empty_cache");
    let saved = cold.save().unwrap_or(0);
    kv("pipeline_cache_saved_bytes", saved);
    match GpuPipelineCache::open(&ctx, &dir) {
        Ok(warm) => {
            kv("pipeline_cache_loaded_bytes", warm.loaded_bytes());
            warm.install();
            build("pipelines_loaded_cache");
        }
        Err(err) => kv("pipeline_cache_reload", format!("failed: {err}")),
    }
    let _ = std::fs::remove_dir_all(&dir);
}
