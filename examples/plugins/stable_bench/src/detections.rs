//! Eidos's `Detections` (`crates/daedalus/src/types.rs` in Eidos), mirrored for the benchmark.
//!
//! Same fields, order and `Value` shape as Eidos's `to_value`: `frame_width`, `frame_height`,
//! `detections` (per tag: `id`, `rotation`, `hamming_distance`, four integer `corners`) and
//! `subpixel` (per tag: four float corners). Differences from Eidos: plain `Vec`s for Eidos's
//! `ArucoDetections` and `RefinedCorners` wrappers, and the refinement flag is not in the value.
//!
//! Eidos has no way back from a `Value`, so [`DaedalusTypeExpr::from_value`] below is written
//! here: it is what a consumer plugin would need to take `Detections` through the stable path.

#![allow(
    clippy::cast_possible_truncation,
    clippy::cast_possible_wrap,
    clippy::cast_precision_loss
)]

use daedalus::data::daedalus_type::DaedalusTypeExpr;
use daedalus::data::model::{StructField, StructFieldValue, TypeExpr, Value, ValueType};
use daedalus::data::to_value::ToValue;
use daedalus::macros::node;
use daedalus::runtime::NodeError;

/// An integer corner (Eidos's `PointI`).
#[derive(Clone, Copy, Debug, PartialEq)]
pub struct PointI {
    pub x: i32,
    pub y: i32,
}

/// A sub-pixel corner (Eidos's `PointF`).
#[derive(Clone, Copy, Debug, PartialEq)]
pub struct PointF {
    pub x: f32,
    pub y: f32,
}

/// One tag: its id, quarter-turn rotation, hamming distance and four integer corners.
#[derive(Clone, Debug, PartialEq)]
pub struct Detection {
    pub id: u32,
    pub rotation: u8,
    pub hamming_distance: u8,
    pub corners: [PointI; 4],
}

/// Eidos's `Detections`: the tags of one frame and their sub-pixel corners, one entry per tag.
#[derive(Clone, Debug, PartialEq, Default)]
pub struct Detections {
    pub frame_width: u32,
    pub frame_height: u32,
    pub detections: Vec<Detection>,
    pub subpixel: Vec<[PointF; 4]>,
}

impl Detections {
    /// A frame of `tags` deterministic tags (`1920x1080`), shaped like a camera's detections.
    pub fn sample(tags: usize) -> Self {
        Self {
            frame_width: 1920,
            frame_height: 1080,
            detections: (0..tags)
                .map(|tag| {
                    let base_x = (tag as i32 * 97) % 1800 + 40;
                    let base_y = 40 + tag as i32 * 3;
                    Detection {
                        id: tag as u32,
                        rotation: (tag % 4) as u8,
                        hamming_distance: (tag % 2) as u8,
                        corners: std::array::from_fn(|k| PointI {
                            x: base_x + 60 * (k as i32 % 2),
                            y: base_y + 60 * (k as i32 / 2),
                        }),
                    }
                })
                .collect(),
            subpixel: (0..tags)
                .map(|tag| {
                    std::array::from_fn(|k| {
                        let (base_x, base_y) = (
                            ((tag as i32 * 97) % 1800 + 40 + 60 * (k as i32 % 2)) as f32,
                            (40 + tag as i32 * 3 + 60 * (k as i32 / 2)) as f32,
                        );
                        PointF {
                            x: base_x + 0.37,
                            y: base_y + 0.61,
                        }
                    })
                })
                .collect(),
        }
    }

    /// A fold over every field, so a consumer reads all of it (the sum is not meaningful).
    pub fn checksum(&self) -> i64 {
        let mut sum = i64::from(self.frame_width) + i64::from(self.frame_height);
        for tag in &self.detections {
            sum += i64::from(tag.id) + i64::from(tag.rotation) + i64::from(tag.hamming_distance);
            for corner in &tag.corners {
                sum += i64::from(corner.x) + i64::from(corner.y);
            }
        }
        for quad in &self.subpixel {
            for corner in quad {
                sum += corner.x as i64 + corner.y as i64;
            }
        }
        sum
    }
}

fn field(name: &str, value: Value) -> StructFieldValue {
    StructFieldValue {
        name: name.to_owned(),
        value,
    }
}

fn int(value: i64) -> Value {
    Value::Int(value)
}

fn int_point(corner: &PointI) -> Value {
    Value::Struct(vec![
        field("x", int(i64::from(corner.x))),
        field("y", int(i64::from(corner.y))),
    ])
}

fn float_point(corner: &PointF) -> Value {
    Value::Struct(vec![
        field("x", Value::Float(f64::from(corner.x))),
        field("y", Value::Float(f64::from(corner.y))),
    ])
}

fn detection_value(tag: &Detection) -> Value {
    Value::Struct(vec![
        field("id", int(i64::from(tag.id))),
        field("rotation", int(i64::from(tag.rotation))),
        field("hamming_distance", int(i64::from(tag.hamming_distance))),
        field(
            "corners",
            Value::List(tag.corners.iter().map(int_point).collect()),
        ),
    ])
}

impl ToValue for Detections {
    fn to_value(&self) -> Value {
        Value::Struct(vec![
            field("frame_width", int(i64::from(self.frame_width))),
            field("frame_height", int(i64::from(self.frame_height))),
            field(
                "detections",
                Value::List(self.detections.iter().map(detection_value).collect()),
            ),
            field(
                "subpixel",
                Value::List(
                    self.subpixel
                        .iter()
                        .map(|quad| Value::List(quad.iter().map(float_point).collect()))
                        .collect(),
                ),
            ),
        ])
    }
}

fn struct_type(fields: &[(&str, TypeExpr)]) -> TypeExpr {
    TypeExpr::Struct(
        fields
            .iter()
            .map(|(name, ty)| StructField {
                name: (*name).to_owned(),
                ty: ty.clone(),
            })
            .collect(),
    )
}

fn scalar_int() -> TypeExpr {
    TypeExpr::Scalar(ValueType::Int)
}

fn scalar_float() -> TypeExpr {
    TypeExpr::Scalar(ValueType::Float)
}

fn corners_type(component: TypeExpr) -> TypeExpr {
    TypeExpr::List(Box::new(struct_type(&[
        ("x", component.clone()),
        ("y", component),
    ])))
}

fn detection_type() -> TypeExpr {
    struct_type(&[
        ("id", scalar_int()),
        ("rotation", scalar_int()),
        ("hamming_distance", scalar_int()),
        ("corners", corners_type(scalar_int())),
    ])
}

impl DaedalusTypeExpr for Detections {
    const TYPE_KEY: &'static str = "stable_bench:detections";

    fn type_expr() -> TypeExpr {
        struct_type(&[
            ("frame_width", scalar_int()),
            ("frame_height", scalar_int()),
            ("detections", TypeExpr::List(Box::new(detection_type()))),
            (
                "subpixel",
                TypeExpr::List(Box::new(corners_type(scalar_float()))),
            ),
        ])
    }

    /// Eidos has no inverse of `to_value`; this is the one a consumer plugin needs.
    fn from_value(value: &Value) -> Option<Self> {
        Some(Self {
            frame_width: u32::try_from(as_int(value.field("frame_width")?)?).ok()?,
            frame_height: u32::try_from(as_int(value.field("frame_height")?)?).ok()?,
            detections: as_list(value.field("detections")?)?
                .iter()
                .map(detection_from_value)
                .collect::<Option<_>>()?,
            subpixel: as_list(value.field("subpixel")?)?
                .iter()
                .map(|quad| {
                    let points = as_list(quad)?
                        .iter()
                        .map(float_point_from_value)
                        .collect::<Option<Vec<_>>>()?;
                    points.try_into().ok()
                })
                .collect::<Option<_>>()?,
        })
    }
}

fn as_int(value: &Value) -> Option<i64> {
    match value {
        Value::Int(value) => Some(*value),
        _ => None,
    }
}

fn as_list(value: &Value) -> Option<&[Value]> {
    match value {
        Value::List(items) => Some(items),
        _ => None,
    }
}

fn int_point_from_value(value: &Value) -> Option<PointI> {
    Some(PointI {
        x: i32::try_from(as_int(value.field("x")?)?).ok()?,
        y: i32::try_from(as_int(value.field("y")?)?).ok()?,
    })
}

fn float_point_from_value(value: &Value) -> Option<PointF> {
    let as_float = |value: &Value| match value {
        Value::Float(value) => Some(*value as f32),
        _ => None,
    };
    Some(PointF {
        x: as_float(value.field("x")?)?,
        y: as_float(value.field("y")?)?,
    })
}

fn detection_from_value(value: &Value) -> Option<Detection> {
    let corners = as_list(value.field("corners")?)?
        .iter()
        .map(int_point_from_value)
        .collect::<Option<Vec<_>>>()?;
    Some(Detection {
        id: u32::try_from(as_int(value.field("id")?)?).ok()?,
        rotation: u8::try_from(as_int(value.field("rotation")?)?).ok()?,
        hamming_distance: u8::try_from(as_int(value.field("hamming_distance")?)?).ok()?,
        corners: corners.try_into().ok()?,
    })
}

/// Makes `tags` tags and returns them as a `Value` through the stable path.
#[node(id = "value_source", inputs("tags"), outputs("det"))]
pub fn value_source(tags: i64) -> Result<Detections, NodeError> {
    Ok(Detections::sample(tags.max(0) as usize))
}

/// Reads every field of the `Detections` it is given.
#[node(id = "value_sink", inputs("det"), outputs("out"))]
pub fn value_sink(det: Detections) -> Result<i64, NodeError> {
    Ok(det.checksum())
}
