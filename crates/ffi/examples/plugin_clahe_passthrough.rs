#![crate_type = "cdylib"]
//! Minimal Rust FFI plugin that accepts a `Payload<DynamicImage>` and returns it unchanged.

use daedalus::runtime::NodeError;
use daedalus::{ComputeAffinity, Payload, declare_plugin, ffi::export_plugin, macros::node};
use image::DynamicImage;

declare_plugin!(
    CvClahePassthroughPlugin,
    "ffi.cv_clahe_passthrough",
    [cv_clahe]
);

#[cfg_attr(
    feature = "gpu",
    node(
        id = "clahe",
        compute(ComputeAffinity::GpuPreferred),
        inputs("mask", "tile_size", "clip_limit"),
        outputs("mask")
    )
)]
#[cfg_attr(
    not(feature = "gpu"),
    node(
        id = "clahe",
        compute(ComputeAffinity::GpuPreferred),
        inputs("mask", "tile_size", "clip_limit"),
        outputs("mask")
    )
)]
fn cv_clahe(
    mask: Payload<DynamicImage>,
    tile_size: i64,
    clip_limit: f64,
) -> Result<Payload<DynamicImage>, NodeError> {
    // Intentionally a no-op pass-through. We only validate payload transport + decoding.
    let _ = (tile_size, clip_limit);
    Ok(mask)
}

export_plugin!(CvClahePassthroughPlugin);
