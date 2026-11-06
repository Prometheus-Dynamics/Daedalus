//! Plain-old-data nodes: a `StablePod` struct and a `Vec` of them crossing the stable boundary
//! as typed byte views (the facade's `dylib_stable_pod` test).

use daedalus::macros::node;
use daedalus::runtime::NodeError;
use daedalus::{StablePod, type_key};

/// A detection box: plain data, `#[repr(C)]`, no padding.
#[derive(Clone, Copy, Debug, Default, PartialEq, StablePod)]
#[repr(C)]
#[type_key("stable_abi:box")]
pub struct BoxPod {
    pub id: u32,
    pub score: f32,
    pub corners: [[i32; 2]; 4],
}

/// The port key of a `Vec<BoxPod>`.
pub const BOXES_KEY: &str = "stable_abi:boxes";

/// Scales a box's corners by `k` and bumps its id.
#[node(id = "pod_scale", inputs("b", "k"), outputs("out"))]
pub fn pod_scale(b: BoxPod, k: i32) -> Result<BoxPod, NodeError> {
    Ok(BoxPod {
        id: b.id + 1,
        score: b.score * 2.0,
        corners: b.corners.map(|[x, y]| [x * k, y * k]),
    })
}

/// Makes `n` boxes.
#[node(
    id = "pod_make",
    inputs("n"),
    outputs(port(name = "boxes", type_key = "stable_abi:boxes"))
)]
pub fn pod_make(n: i64) -> Result<Vec<BoxPod>, NodeError> {
    Ok((0..n.max(0) as u32)
        .map(|id| BoxPod {
            id,
            score: id as f32 / 2.0,
            corners: [
                [id as i32, 0],
                [id as i32 + 1, 0],
                [id as i32 + 1, 1],
                [id as i32, 1],
            ],
        })
        .collect())
}

/// Shifts every box by `dx`.
#[node(
    id = "pod_shift",
    inputs(port(name = "boxes", type_key = "stable_abi:boxes"), "dx"),
    outputs(port(name = "out", type_key = "stable_abi:boxes"))
)]
pub fn pod_shift(boxes: Vec<BoxPod>, dx: i32) -> Result<Vec<BoxPod>, NodeError> {
    Ok(boxes
        .into_iter()
        .map(|b| BoxPod {
            corners: b.corners.map(|[x, y]| [x + dx, y]),
            ..b
        })
        .collect())
}

/// The number of boxes and the sum of their ids and corners.
#[node(
    id = "pod_count",
    inputs(port(name = "boxes", type_key = "stable_abi:boxes")),
    outputs("count", "sum")
)]
#[allow(clippy::ptr_arg)] // A reference to the decoded `Vec`, so its allocation is reused.
pub fn pod_count(boxes: &Vec<BoxPod>) -> Result<(i64, i64), NodeError> {
    let sum = boxes
        .iter()
        .map(|b| {
            i64::from(b.id)
                + b.corners
                    .iter()
                    .flatten()
                    .map(|&v| i64::from(v))
                    .sum::<i64>()
        })
        .sum();
    Ok((boxes.len() as i64, sum))
}
