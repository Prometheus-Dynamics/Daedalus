//! The same `Detections` as plain data: a `Vec` of `#[derive(StablePod)]` tags that crosses the
//! stable boundary as a typed byte view (one copy, no `Value`), a source and a sink.

#![allow(clippy::cast_possible_truncation, clippy::cast_possible_wrap)]

use daedalus::macros::node;
use daedalus::runtime::NodeError;
use daedalus::{StablePod, type_key};

use super::detections::Detections;

/// One tag of a `Detections` as plain data: id, rotation, hamming distance, the four integer
/// corners and the four sub-pixel corners.
#[derive(Clone, Copy, Debug, Default, PartialEq, StablePod)]
#[repr(C)]
#[type_key("stable_bench:tag_pod")]
pub struct TagPod {
    pub id: u32,
    pub rotation: u8,
    pub hamming_distance: u8,
    pub _pad: [u8; 2],
    pub corners: [[i32; 2]; 4],
    pub subpixel: [[f32; 2]; 4],
}

/// The port key of a `Vec<TagPod>`.
pub const TAGS_KEY: &str = "stable_bench:tag_pods";

/// The tags of `Detections::sample(tags)`.
pub fn sample(tags: usize) -> Vec<TagPod> {
    let det = Detections::sample(tags);
    det.detections
        .iter()
        .zip(&det.subpixel)
        .map(|(tag, quad)| TagPod {
            id: tag.id,
            rotation: tag.rotation,
            hamming_distance: tag.hamming_distance,
            _pad: [0; 2],
            corners: tag.corners.map(|c| [c.x, c.y]),
            subpixel: quad.map(|c| [c.x, c.y]),
        })
        .collect()
}

/// `Detections::checksum` of the same tags (frame size 1920x1080).
pub fn checksum(tags: &[TagPod]) -> i64 {
    let mut sum = 1920 + 1080;
    for tag in tags {
        sum += i64::from(tag.id) + i64::from(tag.rotation) + i64::from(tag.hamming_distance);
        for [x, y] in tag.corners {
            sum += i64::from(x) + i64::from(y);
        }
    }
    for tag in tags {
        for [x, y] in tag.subpixel {
            sum += x as i64 + y as i64;
        }
    }
    sum
}

/// Makes `tags` tags as plain data.
#[node(
    id = "pod_source",
    inputs("tags"),
    outputs(port(name = "det", type_key = "stable_bench:tag_pods"))
)]
pub fn pod_source(tags: i64) -> Result<Vec<TagPod>, NodeError> {
    Ok(sample(tags.max(0) as usize))
}

/// Reads every field and returns the checksum `value_sink` does.
#[node(
    id = "pod_sink",
    inputs(port(name = "det", type_key = "stable_bench:tag_pods")),
    outputs("out")
)]
#[allow(clippy::ptr_arg)] // A reference to the decoded `Vec`, so its allocation is reused.
pub fn pod_sink(det: &Vec<TagPod>) -> Result<i64, NodeError> {
    Ok(checksum(det))
}
