//! Image plugin for Daedalus: optional conversions built out-of-tree so the core/runtime
//! doesn't depend on `image`.

use daedalus::{Plugin, PluginRegistry};
use image::{DynamicImage, GrayImage, RgbImage, RgbaImage};

/// Register common image conversions into the global conversion registry.
pub fn register_image_conversions(reg: &mut PluginRegistry) {
    reg.register_conversion::<DynamicImage, RgbaImage>(|v| Some(v.to_rgba8()));
    reg.register_conversion::<DynamicImage, RgbImage>(|v| Some(v.to_rgb8()));
    reg.register_conversion::<DynamicImage, GrayImage>(|v| Some(v.to_luma8()));
    reg.register_conversion::<RgbaImage, DynamicImage>(|v| {
        Some(DynamicImage::ImageRgba8(v.clone()))
    });
    reg.register_conversion::<RgbImage, DynamicImage>(|v| Some(DynamicImage::ImageRgb8(v.clone())));
    reg.register_conversion::<GrayImage, DynamicImage>(|v| {
        Some(DynamicImage::ImageLuma8(v.clone()))
    });
    // Strip alpha.
    reg.register_conversion::<RgbaImage, RgbImage>(|v| {
        Some(image::ImageBuffer::from_fn(
            v.width(),
            v.height(),
            |x, y| {
                let p = v.get_pixel(x, y);
                image::Rgb([p[0], p[1], p[2]])
            },
        ))
    });
    // Add opaque alpha.
    reg.register_conversion::<RgbImage, RgbaImage>(|v| {
        Some(image::ImageBuffer::from_fn(
            v.width(),
            v.height(),
            |x, y| {
                let p = v.get_pixel(x, y);
                image::Rgba([p[0], p[1], p[2], 255])
            },
        ))
    });
}

#[cfg(feature = "gpu")]
pub fn register_image_packers(reg: &mut PluginRegistry) {
    use daedalus::gpu::ErasedPayload;
    use daedalus::runtime::EdgePayload;

    reg.register_output_packer(|img: &DynamicImage| {
        EdgePayload::Payload(ErasedPayload::from_cpu::<DynamicImage>(img.clone()))
    });
    reg.register_output_packer(|img: &GrayImage| {
        let dyn_img = DynamicImage::ImageLuma8(img.clone());
        EdgePayload::Payload(ErasedPayload::from_cpu::<DynamicImage>(dyn_img))
    });
    reg.register_output_packer(|img: &GrayAlphaImage| {
        let dyn_img = DynamicImage::ImageLumaA8(img.clone());
        EdgePayload::Payload(ErasedPayload::from_cpu::<DynamicImage>(dyn_img))
    });
    reg.register_output_packer(|img: &RgbImage| {
        let dyn_img = DynamicImage::ImageRgb8(img.clone());
        EdgePayload::Payload(ErasedPayload::from_cpu::<DynamicImage>(dyn_img))
    });
    reg.register_output_packer(|img: &RgbaImage| {
        let dyn_img = DynamicImage::ImageRgba8(img.clone());
        EdgePayload::Payload(ErasedPayload::from_cpu::<DynamicImage>(dyn_img))
    });
}

/// Simple plugin implementation so callers can install via `PluginRegistry::install_plugin`.
pub struct ImagePlugin;

impl Plugin for ImagePlugin {
    fn id(&self) -> &'static str {
        "images"
    }

    fn install(&self, registry: &mut PluginRegistry) -> Result<(), &'static str> {
        register_image_conversions(registry);
        #[cfg(feature = "gpu")]
        register_image_packers(registry);
        Ok(())
    }
}
