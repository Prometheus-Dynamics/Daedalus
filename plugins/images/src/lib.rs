//! Image plugin for Daedalus: optional conversions built out-of-tree so the core/runtime
//! doesn't depend on `image`.

use daedalus::{Plugin, PluginRegistry};
#[cfg(feature = "gpu")]
use image::GrayAlphaImage;
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
    use daedalus::runtime::RuntimeValue;
    use std::sync::Arc;

    reg.register_output_mover(|img: DynamicImage| RuntimeValue::Any(Arc::new(img)));
    reg.register_output_mover(|img: GrayImage| RuntimeValue::Any(Arc::new(img)));
    reg.register_output_mover(|img: GrayAlphaImage| {
        let dyn_img = DynamicImage::ImageLumaA8(img);
        RuntimeValue::Any(Arc::new(dyn_img))
    });
    reg.register_output_mover(|img: RgbImage| RuntimeValue::Any(Arc::new(img)));
    reg.register_output_mover(|img: RgbaImage| RuntimeValue::Any(Arc::new(img)));
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
