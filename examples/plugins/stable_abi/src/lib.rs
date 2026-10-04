//! A plugin run through the stable handler path (see `docs/dynamic-plugins.md`).
//!
//! Its `cdylib` (`--features dylib`) reports another rustc in its descriptor, as a plugin built
//! by another toolchain would, so a host refuses its Rust ABI and installs it through the stable
//! entry points. Its nodes cover what crosses that path: scalars, a derived struct, bytes, an
//! optional input, node state, a frame read through `daedalus:frame` without copying, and a
//! panic. The facade's `dylib_stable` test runs them statically and dynamically and compares.

use daedalus::macros::{node, plugin};
use daedalus::runtime::NodeError;
use daedalus::transport::FrameView;
use daedalus::{DaedalusToValue, DaedalusTypeExpr};

/// A derived struct: built from a `Value` through serde, turned into one through `ToValue`.
#[derive(Clone, Debug, PartialEq, DaedalusTypeExpr, DaedalusToValue, serde::Deserialize)]
#[daedalus(type_key = "stable_abi:point")]
pub struct Point {
    pub x: i64,
    pub y: f64,
    pub label: String,
}

#[node(id = "scale", inputs("value", "factor"), outputs("out"))]
fn scale(value: i32, factor: f64) -> Result<f64, NodeError> {
    Ok(f64::from(value) * factor)
}

#[node(id = "point", inputs("x", "y", "label"), outputs("point"))]
fn point(x: i64, y: f64, label: String) -> Result<Point, NodeError> {
    Ok(Point { x, y, label })
}

#[node(id = "describe", inputs("point"), outputs("text", "norm"))]
fn describe(point: &Point) -> Result<(String, f64), NodeError> {
    let norm = ((point.x * point.x) as f64 + point.y * point.y).sqrt();
    Ok((format!("{}@({}, {})", point.label, point.x, point.y), norm))
}

#[node(id = "checksum", inputs("bytes"), outputs("sum", "reversed"))]
fn checksum(bytes: Vec<u8>) -> Result<(u64, Vec<u8>), NodeError> {
    let sum = bytes.iter().map(|&b| u64::from(b)).sum();
    Ok((sum, bytes.into_iter().rev().collect()))
}

#[node(id = "or_default", inputs("value", "trigger"), outputs("out"))]
fn or_default(value: Option<i64>, trigger: bool) -> Result<i64, NodeError> {
    Ok(if trigger { value.unwrap_or(-1) } else { 0 })
}

#[derive(Default)]
struct Total(i64);

#[node(id = "accumulate", inputs("step"), outputs("total"), state(Total))]
fn accumulate(step: i64, state: &mut Total) -> Result<i64, NodeError> {
    state.0 += step;
    Ok(state.0)
}

/// The frame's pixel sum and the address it read them from (to check nothing was copied).
#[node(id = "frame_sum", inputs("frame"), outputs("sum", "ptr"))]
fn frame_sum(frame: FrameView<'_>) -> Result<(i64, i64), NodeError> {
    let plane = frame
        .plane(0)
        .ok_or_else(|| NodeError::InvalidInput("no plane".into()))?;
    let pixels = plane
        .data
        .ok_or_else(|| NodeError::InvalidInput("plane not mapped".into()))?;
    Ok((
        pixels.iter().map(|&p| i64::from(p)).sum(),
        pixels.as_ptr() as i64,
    ))
}

#[node(id = "explode", inputs("value"), outputs("out"))]
fn explode(value: i64) -> Result<i64, NodeError> {
    assert_ne!(value, 13, "unlucky input");
    Ok(value)
}

#[plugin(
    id = "stable_abi",
    nodes(
        scale, point, describe, checksum, or_default, accumulate, frame_sum, explode
    )
)]
pub struct StableAbiPlugin;

/// The rustc the exported descriptor claims.
pub const SIMULATED_RUSTC: &str = "rustc 1.85.0 (4d91de4e4 2025-02-17; simulated)";

#[cfg(feature = "dylib")]
mod export {
    use daedalus::dylib::{PLUGIN_ABI_VERSION, PluginDescriptor, StrView};

    /// Returns the Daedalus dynamic plugin ABI version.
    #[unsafe(no_mangle)]
    pub extern "C" fn daedalus_plugin_abi_version() -> u32 {
        PLUGIN_ABI_VERSION
    }

    /// The plugin's descriptor, claiming another toolchain.
    #[unsafe(no_mangle)]
    pub extern "C" fn daedalus_plugin_descriptor() -> PluginDescriptor {
        let mut descriptor = daedalus::plugin_descriptor!(super::StableAbiPlugin);
        descriptor.info.rustc_version = StrView::from_static(super::SIMULATED_RUSTC);
        descriptor
    }
}
