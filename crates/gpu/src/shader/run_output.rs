use std::collections::HashMap;

use crate::{GpuContextHandle, GpuError, GpuImageHandle, Payload};
use image::DynamicImage;

use super::ShaderContext;

/// Result buffers keyed by binding slot.
pub struct ShaderRunOutput {
    pub buffers: HashMap<u32, Vec<u8>>,
    pub textures: HashMap<u32, GpuImageHandle>,
}

impl ShaderRunOutput {
    /// Interpret a texture readback (RGBA8) into an ImageBuffer if possible.
    pub fn texture_rgba8(
        &self,
        binding: u32,
        width: u32,
        height: u32,
    ) -> Option<image::ImageBuffer<image::Rgba<u8>, Vec<u8>>> {
        self.buffers.get(&binding).and_then(|bytes| {
            image::ImageBuffer::<image::Rgba<u8>, _>::from_raw(width, height, bytes.clone())
        })
    }

    /// Interpret a texture readback (RGBA8) into a DynamicImage.
    pub fn texture_rgba8_image(
        &self,
        binding: u32,
        width: u32,
        height: u32,
    ) -> Option<DynamicImage> {
        self.texture_rgba8(binding, width, height)
            .map(DynamicImage::ImageRgba8)
    }

    /// Interpret an r32float texture readback into raw f32 values (row-major).
    pub fn texture_r32f(&self, binding: u32, width: u32, height: u32) -> Option<Vec<f32>> {
        self.buffers.get(&binding).and_then(|bytes| {
            let expected = (width as usize) * (height as usize) * 4;
            if bytes.len() < expected || bytes.len() % 4 != 0 {
                return None;
            }
            let mut out = Vec::with_capacity(bytes.len() / 4);
            for chunk in bytes.chunks_exact(4) {
                out.push(f32::from_le_bytes([chunk[0], chunk[1], chunk[2], chunk[3]]));
            }
            Some(out)
        })
    }

    /// Interpret an r32float texture readback into a grayscale ImageBuffer<u8> (clamped 0..1 -> 0..255).
    pub fn texture_r32f_gray_u8(
        &self,
        binding: u32,
        width: u32,
        height: u32,
    ) -> Option<image::ImageBuffer<image::Luma<u8>, Vec<u8>>> {
        self.texture_r32f(binding, width, height).and_then(|vals| {
            if vals.len() < (width as usize) * (height as usize) {
                return None;
            }
            let mut out = Vec::with_capacity(vals.len());
            for v in vals {
                let clamped = v.clamp(0.0, 1.0);
                out.push((clamped * 255.0).round().clamp(0.0, 255.0) as u8);
            }
            image::ImageBuffer::<image::Luma<u8>, _>::from_raw(width, height, out)
        })
    }

    /// Interpret an r32float texture into grayscale ImageBuffer<f32>.
    pub fn texture_r32f_image(
        &self,
        binding: u32,
        width: u32,
        height: u32,
    ) -> Option<image::ImageBuffer<image::Luma<f32>, Vec<f32>>> {
        self.texture_r32f(binding, width, height).and_then(|vals| {
            image::ImageBuffer::<image::Luma<f32>, _>::from_raw(width, height, vals)
        })
    }

    /// Interpret an rgba16float texture readback into raw f32 RGBA values (row-major).
    pub fn texture_rgba16f(&self, binding: u32, width: u32, height: u32) -> Option<Vec<f32>> {
        self.buffers.get(&binding).and_then(|bytes| {
            if bytes.len() < (width as usize) * (height as usize) * 8 {
                return None;
            }
            let mut out = Vec::with_capacity((width as usize) * (height as usize) * 4);
            for chunk in bytes.chunks_exact(2) {
                let half = u16::from_le_bytes([chunk[0], chunk[1]]);
                let f = half::f16::from_bits(half).to_f32();
                out.push(f);
            }
            Some(out)
        })
    }

    /// Interpret an rgba16float texture into ImageBuffer<Rgba<f32>>.
    pub fn texture_rgba16f_image(
        &self,
        binding: u32,
        width: u32,
        height: u32,
    ) -> Option<image::ImageBuffer<image::Rgba<f32>, Vec<f32>>> {
        self.texture_rgba16f(binding, width, height)
            .and_then(|vals| {
                image::ImageBuffer::<image::Rgba<f32>, _>::from_raw(width, height, vals)
            })
    }

    /// Storage texture helpers (aliases to the above, kept for clarity when using texture_storage_2d write/readback).
    pub fn storage_rgba8_image(
        &self,
        binding: u32,
        width: u32,
        height: u32,
    ) -> Option<DynamicImage> {
        self.texture_rgba8_image(binding, width, height)
    }

    pub fn storage_r32f(&self, binding: u32, width: u32, height: u32) -> Option<Vec<f32>> {
        self.texture_r32f(binding, width, height)
    }

    pub fn storage_rgba16f_image(
        &self,
        binding: u32,
        width: u32,
        height: u32,
    ) -> Option<image::ImageBuffer<image::Rgba<f32>, Vec<f32>>> {
        self.texture_rgba16f_image(binding, width, height)
    }

    /// Get a GPU image handle for a bound texture (when available).
    pub fn texture_handle(&self, binding: u32) -> Option<GpuImageHandle> {
        self.textures.get(&binding).cloned()
    }

    /// Convert a texture output into a Payload<DynamicImage>, preferring GPU handles and falling back to readback.
    pub fn into_payload(
        &self,
        binding: u32,
        gpu: Option<&GpuContextHandle>,
        width: u32,
        height: u32,
    ) -> Result<Payload<DynamicImage>, GpuError> {
        if let Some(handle) = self.texture_handle(binding) {
            return Ok(Payload::Gpu(handle));
        }
        let img = self
            .texture_rgba8(binding, width, height)
            .ok_or_else(|| GpuError::Internal("missing texture output".into()))?;
        Payload::<DynamicImage>::from_rgba_bytes(gpu, img.into_raw(), width, height)
            .map_err(|e| GpuError::Internal(e.to_string()))
    }

    /// Convert a texture output into a Payload using the shader context for GPU lookup.
    pub fn into_payload_with_ctx(
        &self,
        binding: u32,
        ctx: &ShaderContext,
        width: u32,
        height: u32,
    ) -> Result<Payload<DynamicImage>, GpuError> {
        self.into_payload(binding, ctx.gpu.as_ref(), width, height)
    }
}
