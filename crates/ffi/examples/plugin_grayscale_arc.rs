#![crate_type = "cdylib"]
//! Minimal Rust FFI plugin that converts a frame to grayscale (CPU path).

use daedalus::runtime::NodeError;
use daedalus::{ComputeAffinity, Payload, declare_plugin, ffi::export_plugin, macros::node};
use image::DynamicImage;

declare_plugin!(
    GrayscaleArcPlugin,
    "ffi.cv_grayscale_arc",
    [cv_grayscale_arc]
);

#[cfg_attr(
    feature = "gpu",
    node(
        id = "grayscale_arc",
        compute(ComputeAffinity::CpuOnly),
        summary = "Convert a frame to a grayscale mask (CPU-only conversion).",
        inputs(port(name = "frame"), port(name = "mode", default = 0)),
        outputs(port(name = "mask"))
    )
)]
#[cfg_attr(
    not(feature = "gpu"),
    node(
        id = "grayscale_arc",
        compute(ComputeAffinity::CpuOnly),
        summary = "Convert a frame to a grayscale mask (CPU-only conversion).",
        inputs(port(name = "frame"), port(name = "mode", default = 0)),
        outputs(port(name = "mask"))
    )
)]
fn cv_grayscale_arc(
    frame: Payload<DynamicImage>,
    _mode: i64,
) -> Result<Payload<DynamicImage>, NodeError> {
    match frame {
        Payload::Cpu(img) => {
            let out = match img {
                DynamicImage::ImageLuma8(_) => img,
                other => DynamicImage::ImageLuma8(other.to_luma8()),
            };
            Ok(Payload::Cpu(out))
        }
        Payload::Gpu(_) => Err(NodeError::InvalidInput(
            "grayscale_arc: GPU payload unsupported (insert cpu convert)".into(),
        )),
    }
}

export_plugin!(GrayscaleArcPlugin);
