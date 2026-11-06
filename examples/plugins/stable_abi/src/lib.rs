//! A plugin run through the stable handler path (see `docs/dynamic-plugins.md`).
//!
//! Its `cdylib` (`--features dylib`) reports another rustc in its descriptor, as a plugin built
//! by another toolchain would, so a host refuses its Rust ABI and installs it through the stable
//! entry points. Its nodes cover what crosses that path: scalars (`u64` beyond `i64::MAX`
//! included), a derived struct, bytes, an optional input, node state, a frame read through
//! `daedalus:frame` without copying, a conditional output, a `fire = "all"` join, and a panic. The facade's `dylib_stable` test runs them statically and dynamically and compares.
//! The [`frames`] nodes take and return owned `SharedFrame`s (the `dylib_frames` test); the
//! [`pods`] nodes take and return a `StablePod` struct and `Vec`s of them (`dylib_stable_pod`);
//! the [`locals`] nodes pass a plugin-local object by handle (`dylib_stable_local`), which its
//! codec converts to a `Value` and bytes (`dylib_stable_codecs`).
//! `SharedBytes` nodes show buffers crossing by reference both ways, and the `generation` node
//! changes with the `reload-v2` / `reload-incompatible` features, so the `dylib_reload` test can
//! rebuild the plugin between loads.

use daedalus::data::model::Value;
use daedalus::macros::{node, plugin};
use daedalus::runtime::{ExecutionContext, NodeError};
use daedalus::transport::FrameView;
use daedalus::{DaedalusToValue, DaedalusTypeExpr, SharedBytes};

pub mod frames;
pub mod locals;
pub mod pods;
#[allow(clippy::wildcard_imports)] // The node macro's generated items.
use frames::*;
#[allow(clippy::wildcard_imports)]
use locals::*;
#[allow(clippy::wildcard_imports)]
use pods::*;

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
    let pixels = frame
        .plane_bytes(0)
        .ok_or_else(|| NodeError::InvalidInput("plane 0 not CPU-readable".into()))?;
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

/// `u64` values above `i64::MAX` cross unchanged.
#[node(id = "successor", inputs("value"), outputs("out"))]
fn successor(value: u64) -> Result<u64, NodeError> {
    Ok(value.wrapping_add(1))
}

/// A conditional output: pushes `tick` only when it is a multiple of `every`.
#[node(id = "every", inputs("tick", "every"), outputs("out"))]
fn every(tick: i64, every: i64) -> Result<Option<i64>, NodeError> {
    Ok((tick % every == 0).then_some(tick))
}

/// A cross-tick join (`fire = "all"`): waits for both inputs.
#[node(id = "join", inputs("a", "b"), outputs("out"), fire = "all")]
fn join(a: i64, b: i64) -> Result<i64, NodeError> {
    Ok(a * 100 + b)
}

/// Reads the host's buffer in place: its sum, the address read (to check nothing was copied)
/// and its tail, returned as a slice of the same buffer.
#[node(id = "shared_sum", inputs("bytes"), outputs("sum", "ptr", "tail"))]
fn shared_sum(bytes: SharedBytes) -> Result<(u64, i64, SharedBytes), NodeError> {
    let sum = bytes.iter().map(|&b| u64::from(b)).sum();
    let tail = bytes.slice(1.min(bytes.len())..);
    Ok((sum, bytes.as_ptr() as i64, tail))
}

/// A buffer the plugin allocates and hands to the host without copying, and its address.
#[node(id = "shared_fill", inputs("len", "value"), outputs("bytes", "ptr"))]
fn shared_fill(len: i64, value: i64) -> Result<(SharedBytes, i64), NodeError> {
    let bytes = SharedBytes::new(vec![value as u8; len.max(0) as usize]);
    let ptr = bytes.as_ptr() as i64;
    Ok((bytes, ptr))
}

/// Hands a buffer straight back by reference (the `SharedBytes` handoff alone).
#[node(id = "bytes_pass", inputs("bytes"), outputs("bytes"))]
fn bytes_pass(bytes: SharedBytes) -> Result<SharedBytes, NodeError> {
    Ok(bytes)
}

/// Hands a buffer straight back by value (through the stable path: a `Value` copy each way).
#[node(id = "vec_pass", inputs("bytes"), outputs("bytes"))]
fn vec_pass(bytes: Vec<u8>) -> Result<Vec<u8>, NodeError> {
    Ok(bytes)
}

/// Which build runs: `tick + 1000 * generation` (generation 2 with `reload-v2`).
#[cfg(not(feature = "reload-incompatible"))]
#[node(id = "generation", inputs("tick"), outputs("out"))]
fn generation(tick: i64) -> Result<i64, NodeError> {
    Ok(tick + 1000 * GENERATION)
}

/// The same node with another port: a build a host cannot reload in place.
#[cfg(feature = "reload-incompatible")]
#[node(id = "generation", inputs("tick", "extra"), outputs("out"))]
fn generation(tick: i64, extra: i64) -> Result<i64, NodeError> {
    Ok(tick + extra + 1000 * GENERATION)
}

/// The fixture build's generation.
pub const GENERATION: i64 = if cfg!(feature = "reload-v2") { 2 } else { 1 };

#[plugin(
    id = "stable_abi",
    install = install_codecs,
    nodes(
        scale,
        point,
        describe,
        checksum,
        or_default,
        accumulate,
        frame_sum,
        explode,
        successor,
        every,
        join,
        shared_sum,
        shared_fill,
        bytes_pass,
        vec_pass,
        generation,
        frame_invert,
        frame_pass,
        frame_tag,
        frame_hold,
        metrics_probe,
        frame_explode,
        frame_info,
        pod_scale,
        pod_make,
        pod_shift,
        pod_count,
        local_make,
        local_touch,
        local_read,
        local_drops
    )
)]
pub struct StableAbiPlugin;

/// The rustc the exported descriptor claims.
pub const SIMULATED_RUSTC: &str = "rustc 1.85.0 (4d91de4e4 2025-02-17; simulated)";

#[cfg(feature = "dylib")]
mod export {
    use daedalus::dylib::{PLUGIN_ABI_VERSION, PluginDescriptor, StableExtension, StrView};

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

    /// The stable extension (const inputs sent once, see `StableExtension`).
    #[unsafe(no_mangle)]
    pub extern "C" fn daedalus_plugin_stable_extension() -> StableExtension {
        daedalus::dylib::stable_extension()
    }

    /// The codec table (see `StableCodecTable`).
    #[unsafe(no_mangle)]
    pub extern "C" fn daedalus_plugin_stable_codecs() -> *const daedalus::dylib::StableCodecTable {
        daedalus::plugin_codec_table!(super::StableAbiPlugin)
    }

    /// The stable ABI version, as data (see `STABLE_VERSION_SYMBOL`).
    #[unsafe(no_mangle)]
    #[allow(non_upper_case_globals)]
    pub static daedalus_plugin_stable_version: u32 = daedalus::dylib::STABLE_ABI_VERSION;
}

/// Records a counter per call and its graph's `title` metadata as a text metric, through the
/// host's telemetry (the `dylib_stable_telemetry` test reads them back).
#[node(id = "metrics_probe", inputs("x"), outputs("out"))]
pub fn metrics_probe(x: i64, ctx: &ExecutionContext) -> Result<i64, NodeError> {
    ctx.increment_metric("probe_calls", 1);
    let title = match ctx.graph_metadata.get("title") {
        Some(Value::String(title)) => title.to_string(),
        _ => "none".to_string(),
    };
    ctx.text_metric("probe_title", title);
    Ok(x + 1)
}
