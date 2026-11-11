//! A detector-shaped graph: five stages (mask prep into runs, quads from runs, decode, validate,
//! refine), the input fanned out to four of them, constants on every stage, state on two, and
//! struct outputs, with bodies that only touch their inputs. What remains is the graph's own cost
//! per frame. It mirrors the detector graph the baseline figures were measured on.

use std::sync::Arc;

use daedalus_cpu_author::Ctx;
use daedalus_graph::{GraphBody, Lit, NodeDoc};
use daedalus_host::LoadedHost;
use daedalus_macros::node;
use daedalus_registry::NodeRecord;
use daedalus_types::{TypeExpr, TypeKey, Typed};

use crate::{bridge, edge, load};

macro_rules! opaque {
    ($($ty:ident => $key:literal),* $(,)?) => {$(
        impl Typed for $ty {
            const KEY: TypeKey = TypeKey::new($key);
            const EXPR: TypeExpr = TypeExpr::Opaque(Self::KEY);
        }
    )*};
}

/// The input: a frame's worth of bytes behind one handle.
pub struct Picture {
    pub width: u32,
    pub height: u32,
    pub bytes: Vec<u8>,
}

pub struct Runs {
    pub count: u32,
    pub spans: [u32; 16],
}

pub struct Quads {
    pub count: u32,
    pub corners: [[u32; 4]; 8],
}

pub struct Decoded {
    pub count: u32,
    pub codes: [u64; 8],
}

pub struct Detections {
    pub count: u32,
    pub ids: [u32; 8],
}

pub struct Rejected {
    pub count: u32,
}

pub struct Corners {
    pub count: u32,
    pub points: [[f32; 2]; 8],
}

opaque!(
    Picture => "budget:picture", Runs => "budget:runs", Quads => "budget:quads",
    Decoded => "budget:decoded", Detections => "budget:detections",
    Rejected => "budget:rejected", Corners => "budget:corners",
);

#[derive(Default)]
struct Seen {
    frames: u32,
}

#[node(
    id = "budget.mask",
    inputs(picture, radius, offset, invert),
    outputs(runs),
    state(Seen)
)]
fn mask(
    picture: &Picture,
    radius: u32,
    offset: i64,
    invert: bool,
    seen: &mut Seen,
    cx: &Ctx,
) -> Runs {
    seen.frames = seen.frames.wrapping_add(1);
    let first = u32::from(picture.bytes[0]) + radius + offset as u32 + u32::from(invert);
    let mut spans = [0; 16];
    spans[0] = first;
    spans[1] = cx.now_ns as u32;
    Runs { count: 2, spans }
}

#[node(
    id = "budget.quads",
    inputs(runs, picture, min_width, min_points),
    outputs(quads)
)]
fn quads(runs: &Runs, picture: &Picture, min_width: u32, min_points: u32) -> Quads {
    let mut corners = [[0; 4]; 8];
    corners[0] = [runs.spans[0], picture.width, min_width, min_points];
    Quads {
        count: runs.count.min(1),
        corners,
    }
}

#[node(
    id = "budget.decode",
    inputs(quads, picture, bits, threshold),
    outputs(decoded)
)]
fn decode(quads: &Quads, picture: &Picture, bits: u32, threshold: f64) -> Decoded {
    let mut codes = [0; 8];
    codes[0] = u64::from(quads.corners[0][0]) + u64::from(picture.height) + u64::from(bits);
    codes[1] = threshold.to_bits();
    Decoded {
        count: quads.count,
        codes,
    }
}

#[node(
    id = "budget.validate",
    inputs(decoded, picture, max_errors),
    outputs(detections, rejected),
    state(Seen)
)]
fn validate(
    decoded: &Decoded,
    picture: &Picture,
    max_errors: u32,
    seen: &mut Seen,
) -> (Detections, Rejected) {
    seen.frames = seen.frames.wrapping_add(1);
    let mut ids = [0; 8];
    ids[0] = decoded.codes[0] as u32 + picture.width;
    (
        Detections {
            count: decoded.count,
            ids,
        },
        Rejected { count: max_errors },
    )
}

#[node(
    id = "budget.refine",
    inputs(detections, picture, window, iterations),
    outputs(corners)
)]
fn refine(detections: &Detections, picture: &Picture, window: u32, iterations: u32) -> Corners {
    let mut points = [[0.0; 2]; 8];
    points[0] = [
        detections.ids[0] as f32,
        (picture.height + window + iterations) as f32,
    ];
    Corners {
        count: detections.count,
        points,
    }
}

const NODES: [&NodeRecord; 5] = [
    &MASK_NODE,
    &QUADS_NODE,
    &DECODE_NODE,
    &VALIDATE_NODE,
    &REFINE_NODE,
];

fn stage(id: &str, consts: &[(&str, Lit)]) -> NodeDoc {
    let mut doc = NodeDoc::new(id);
    for (port, value) in consts {
        doc.consts.insert(port.to_string(), value.clone());
    }
    doc
}

/// The detector graph, loaded. The host port `picture` feeds four stages, and the host reads
/// `detections`, `rejected` and `corners`.
pub fn detector() -> LoadedHost {
    detector_with(true)
}

/// The detector graph, loaded with the backend free to fuse its nodes (`merge`) or not.
pub fn detector_with(merge: bool) -> LoadedHost {
    let nodes = vec![
        bridge(&[
            ("picture", "budget:picture"),
            ("detections", "budget:detections"),
            ("rejected", "budget:rejected"),
            ("corners", "budget:corners"),
        ]),
        stage(
            "budget.mask",
            &[
                ("radius", Lit::Int(3)),
                ("offset", Lit::Int(7)),
                ("invert", Lit::Bool(true)),
            ],
        ),
        stage(
            "budget.quads",
            &[("min_width", Lit::Int(4)), ("min_points", Lit::Int(4))],
        ),
        stage(
            "budget.decode",
            &[("bits", Lit::Int(36)), ("threshold", Lit::Float(0.5))],
        ),
        stage("budget.validate", &[("max_errors", Lit::Int(2))]),
        stage(
            "budget.refine",
            &[("window", Lit::Int(5)), ("iterations", Lit::Int(3))],
        ),
    ];
    let mut edges = vec![
        edge(1, "runs", 2, "runs"),
        edge(2, "quads", 3, "quads"),
        edge(3, "decoded", 4, "decoded"),
        edge(4, "detections", 5, "detections"),
        edge(4, "detections", 0, "detections"),
        edge(4, "rejected", 0, "rejected"),
        edge(5, "corners", 0, "corners"),
    ];
    for stage in 1..=5 {
        edges.push(edge(0, "picture", stage, "picture"));
    }
    let graph = GraphBody {
        nodes,
        edges,
        groups: Vec::new(),
    };
    load(&graph, &NODES, merge)
}

pub fn picture() -> Arc<Picture> {
    Arc::new(Picture {
        width: 640,
        height: 480,
        bytes: vec![7; 640 * 480],
    })
}

/// One frame through the detector: fed by handle, stepped, and all three outputs taken.
pub fn frame(host: &mut LoadedHost, picture: &Arc<Picture>) -> u32 {
    let io = host.host();
    io.feed_typed("picture", Arc::clone(picture))
        .expect("a feed");
    io.step().expect("a tick");
    let mut taken = 0;
    for port in ["detections", "rejected", "corners"] {
        taken += u32::from(io.take_value(port).expect("a take").is_some());
    }
    taken
}
