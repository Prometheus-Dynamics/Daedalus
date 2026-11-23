//! The worker the sweep in `process-timings` runs: four nodes that each read a long list and give
//! back a checksum, so only the list crosses. Two read a `list<f32>`, two a list of 28-byte
//! records (five `f32` and two `i32`, a keypoint's shape); of each pair, one reads the items
//! where the host put them (`&[f32]`, and `&[Point]` for a [`Flat`] record) and one decodes the
//! value first, as an SDK with no view does.

#![forbid(unsafe_code)]

use daedalus_lang_rust::{Flat, Wire};
use daedalus_macros::node;
use daedalus_registry::NodeRecord;
use daedalus_types::{Field, Scalar, TypeExpr, TypeKey, Typed};
use daedalus_wire::WireValue;

/// A keypoint's shape: where, how large, which way, how strong, at which octave, of which class.
#[derive(Clone, Copy, Debug, Default, PartialEq)]
pub struct Point {
    pub x: f32,
    pub y: f32,
    pub size: f32,
    pub angle: f32,
    pub response: f32,
    pub octave: i32,
    pub class: i32,
}

const F32: TypeExpr = TypeExpr::Scalar(Scalar::F32);
const I32: TypeExpr = TypeExpr::Scalar(Scalar::I32);
const POINT_FIELDS: &[Field] = &[
    Field { name: "x", ty: F32 },
    Field { name: "y", ty: F32 },
    Field {
        name: "size",
        ty: F32,
    },
    Field {
        name: "angle",
        ty: F32,
    },
    Field {
        name: "response",
        ty: F32,
    },
    Field {
        name: "octave",
        ty: I32,
    },
    Field {
        name: "class",
        ty: I32,
    },
];

impl Typed for Point {
    const KEY: TypeKey = TypeKey::new("structural");
    const EXPR: TypeExpr = TypeExpr::Struct(POINT_FIELDS);
    const STRUCTURAL: bool = true;
}

fn f32_at(bytes: &[u8], at: usize) -> f32 {
    f32::from_le_bytes([bytes[at], bytes[at + 1], bytes[at + 2], bytes[at + 3]])
}

fn i32_at(bytes: &[u8], at: usize) -> i32 {
    i32::from_le_bytes([bytes[at], bytes[at + 1], bytes[at + 2], bytes[at + 3]])
}

impl Flat for Point {
    const SIZE: usize = 28;
    fn read(bytes: &[u8]) -> Point {
        Point {
            x: f32_at(bytes, 0),
            y: f32_at(bytes, 4),
            size: f32_at(bytes, 8),
            angle: f32_at(bytes, 12),
            response: f32_at(bytes, 16),
            octave: i32_at(bytes, 20),
            class: i32_at(bytes, 24),
        }
    }
}

/// A list of floats, decoded from its wire value.
pub struct Floats(Vec<f32>);

impl Typed for Floats {
    const KEY: TypeKey = <Vec<f32> as Typed>::KEY;
    const EXPR: TypeExpr = <Vec<f32> as Typed>::EXPR;
    const STRUCTURAL: bool = true;
}

impl Wire for Floats {
    fn to_wire(&self) -> WireValue {
        WireValue::List(self.0.iter().map(|v| WireValue::F32(*v)).collect())
    }
    fn from_wire(value: &WireValue) -> Option<Floats> {
        let WireValue::List(items) = value else {
            return None;
        };
        let floats = items.iter().map(|item| match item {
            WireValue::F32(v) => Some(*v),
            _ => None,
        });
        floats.collect::<Option<Vec<f32>>>().map(Floats)
    }
}

/// A list of points, decoded from its wire value.
pub struct Points(Vec<Point>);

impl Typed for Points {
    const KEY: TypeKey = <Vec<Point> as Typed>::KEY;
    const EXPR: TypeExpr = <Vec<Point> as Typed>::EXPR;
    const STRUCTURAL: bool = true;
}

impl Wire for Points {
    fn to_wire(&self) -> WireValue {
        let point = |p: &Point| {
            WireValue::Struct(vec![
                WireValue::F32(p.x),
                WireValue::F32(p.y),
                WireValue::F32(p.size),
                WireValue::F32(p.angle),
                WireValue::F32(p.response),
                WireValue::I32(p.octave),
                WireValue::I32(p.class),
            ])
        };
        WireValue::List(self.0.iter().map(point).collect())
    }
    fn from_wire(value: &WireValue) -> Option<Points> {
        let WireValue::List(items) = value else {
            return None;
        };
        let f = |v: &WireValue| match v {
            WireValue::F32(v) => Some(*v),
            _ => None,
        };
        let i = |v: &WireValue| match v {
            WireValue::I32(v) => Some(*v),
            _ => None,
        };
        let point = |item: &WireValue| match item {
            WireValue::Struct(fields) if fields.len() == 7 => Some(Point {
                x: f(&fields[0])?,
                y: f(&fields[1])?,
                size: f(&fields[2])?,
                angle: f(&fields[3])?,
                response: f(&fields[4])?,
                octave: i(&fields[5])?,
                class: i(&fields[6])?,
            }),
            _ => None,
        };
        items
            .iter()
            .map(point)
            .collect::<Option<Vec<Point>>>()
            .map(Points)
    }
}

fn sum(floats: &[f32]) -> u32 {
    floats
        .iter()
        .fold(0u32, |sum, v| sum.wrapping_add(v.to_bits()))
}

fn point_sum(points: &[Point]) -> u32 {
    points.iter().fold(0u32, |sum, p| {
        sum.wrapping_add(p.x.to_bits())
            .wrapping_add(p.response.to_bits())
            .wrapping_add(p.class as u32)
    })
}

#[node(id = "sweep.floats_view", inputs("in"), outputs(sum), backend = daedalus_lang_rust)]
pub fn floats_view(r#in: &[f32]) -> u32 {
    sum(r#in)
}

#[node(id = "sweep.floats_decode", inputs("in"), outputs(sum), backend = daedalus_lang_rust)]
pub fn floats_decode(r#in: Floats) -> u32 {
    sum(&r#in.0)
}

#[node(id = "sweep.points_view", inputs("in"), outputs(sum), backend = daedalus_lang_rust)]
pub fn points_view(r#in: &[Point]) -> u32 {
    point_sum(r#in)
}

#[node(id = "sweep.points_decode", inputs("in"), outputs(sum), backend = daedalus_lang_rust)]
pub fn points_decode(r#in: Points) -> u32 {
    point_sum(&r#in.0)
}

#[node(id = "sweep.bytes_view", inputs("in"), outputs(sum), backend = daedalus_lang_rust)]
pub fn bytes_view(r#in: &[u8]) -> u32 {
    r#in.iter()
        .fold(0u32, |sum, byte| sum.wrapping_add(u32::from(*byte)))
}

const NODES: &[&NodeRecord] = &[
    &BYTES_VIEW_NODE,
    &FLOATS_VIEW_NODE,
    &FLOATS_DECODE_NODE,
    &POINTS_VIEW_NODE,
    &POINTS_DECODE_NODE,
];

fn main() {
    if let Err(error) = daedalus_lang_rust::serve(NODES) {
        let _ = std::io::Write::write_fmt(&mut std::io::stderr(), format_args!("{error}\n"));
        std::process::exit(1);
    }
}
