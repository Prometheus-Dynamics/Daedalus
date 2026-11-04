//! The staged-shape stage: one handle in, one handle out, a frame input, and optionally ten
//! constants, each read the way a config struct reads them. The handle passes straight on (the
//! same allocation, with no new payload), so a chain of these measures what crossing a handle
//! on each edge costs, not the stage's work.

use std::hint::black_box;

use daedalus::data::model::{TypeExpr, ValueType};
use daedalus::macros::node;
use daedalus::runtime::{ExecutionContext, NodeError, NodeIo, RuntimeNode};
use daedalus::transport::FrameView;

/// Passes the handle and reads the frame's metadata (no constants).
#[node(
    id = "stage0",
    inputs(
        port(name = "det", type_key = "stable_bench:detections_view"),
        port(name = "frame", type_key = "daedalus:frame")
    ),
    outputs(port(name = "det", type_key = "stable_bench:detections_view"))
)]
pub fn stage0(
    _node: &RuntimeNode,
    _ctx: &ExecutionContext,
    io: &mut NodeIo,
) -> Result<(), NodeError> {
    pass(io)
}

/// Passes the handle, reads the frame and ten constants typed as `config_m10` would be (two strings).
#[node(
    id = "stage10",
    inputs(
        port(name = "det", type_key = "stable_bench:detections_view"),
        port(name = "frame", type_key = "daedalus:frame"),
        port(name = "c0", ty = TypeExpr::Scalar(ValueType::Int)),
        port(name = "c1", ty = TypeExpr::Scalar(ValueType::Float)),
        port(name = "c2", ty = TypeExpr::Scalar(ValueType::Bool)),
        port(name = "c3", ty = TypeExpr::Scalar(ValueType::String)),
        port(name = "c4", ty = TypeExpr::Scalar(ValueType::Float)),
        port(name = "c5", ty = TypeExpr::Scalar(ValueType::Bool)),
        port(name = "c6", ty = TypeExpr::Scalar(ValueType::Int)),
        port(name = "c7", ty = TypeExpr::Scalar(ValueType::String)),
        port(name = "c8", ty = TypeExpr::Scalar(ValueType::Bool)),
        port(name = "c9", ty = TypeExpr::Scalar(ValueType::Int))
    ),
    outputs(port(name = "det", type_key = "stable_bench:detections_view"))
)]
pub fn stage10(
    _node: &RuntimeNode,
    _ctx: &ExecutionContext,
    io: &mut NodeIo,
) -> Result<(), NodeError> {
    let mut sum = 0_i64;
    sum = sum.wrapping_add(io.get_typed::<i64>("c0").unwrap_or_default());
    sum = sum.wrapping_add(io.get_typed::<f64>("c1").unwrap_or_default() as i64);
    sum = sum.wrapping_add(i64::from(io.get_typed::<bool>("c2").unwrap_or_default()));
    sum = sum.wrapping_add(io.get_typed::<String>("c3").unwrap_or_default().len() as i64);
    sum = sum.wrapping_add(io.get_typed::<f64>("c4").unwrap_or_default() as i64);
    sum = sum.wrapping_add(i64::from(io.get_typed::<bool>("c5").unwrap_or_default()));
    sum = sum.wrapping_add(io.get_typed::<i64>("c6").unwrap_or_default());
    sum = sum.wrapping_add(io.get_typed::<String>("c7").unwrap_or_default().len() as i64);
    sum = sum.wrapping_add(i64::from(io.get_typed::<bool>("c8").unwrap_or_default()));
    sum = sum.wrapping_add(io.get_typed::<i64>("c9").unwrap_or_default());
    black_box(sum);
    pass(io)
}

fn pass(io: &mut NodeIo) -> Result<(), NodeError> {
    {
        let frame: FrameView<'_> = io.get_foreign("frame")?;
        black_box((frame.width(), frame.height(), frame.sequence()));
    }
    let payload = io
        .take_input_payload("det")
        .ok_or_else(|| NodeError::InvalidInput("missing `det`".into()))?;
    io.push_correlated_payload("det", payload);
    Ok(())
}
