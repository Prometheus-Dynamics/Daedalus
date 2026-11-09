//! A small library and plan the blob tests share.

#![allow(dead_code)]

use daedalus_cpu::blob::{
    ConstDesc, EdgeDesc, HostDesc, Library, Overflow, PlanDesc, ScalarKind, Source, StepDesc,
    name_hash,
};
use daedalus_cpu_author::{FanIn, Fault};
use daedalus_macros::node;
use daedalus_registry::NodeRecord;

#[derive(Default)]
pub struct Total(pub u32);

#[node(id = "b.gain", inputs(a, by), outputs(out))]
pub fn gain(a: u32, by: u32) -> u32 {
    a * by
}

#[node(id = "b.sum", inputs(a), outputs(out), state(Total))]
pub fn sum(a: u32, total: &mut Total) -> u32 {
    total.0 += a;
    total.0
}

#[node(id = "b.first", inputs(items), outputs(out), fire = "any")]
pub fn first(items: FanIn<'_, u32>) -> Option<u32> {
    items.iter().next().map(|item| *item)
}

#[node(id = "b.opt", inputs(a), outputs(out))]
pub fn opt(a: Option<u32>) -> u32 {
    a.unwrap_or(77)
}

#[node(id = "b.bad", inputs(a), outputs(out))]
pub fn bad(a: u32) -> Result<u32, Fault> {
    Err(Fault {
        code: a,
        reason: "no",
    })
}

#[node(id = "b.heavy", inputs(a), outputs(out))]
pub fn heavy(a: u32) -> String {
    a.to_string()
}

const NODES: &[&NodeRecord] = &[
    &GAIN_NODE,
    &SUM_NODE,
    &FIRST_NODE,
    &OPT_NODE,
    &BAD_NODE,
    &HEAVY_NODE,
];
pub static LIBRARY: Library = Library::new(NODES);
pub const GAIN: u32 = 0;
pub const SUM: u32 = 1;
pub const FIRST: u32 = 2;
pub const OPT: u32 = 3;
pub const BAD: u32 = 4;
pub const HEAVY: u32 = 5;

pub fn u32_key() -> u32 {
    name_hash("u32")
}

pub fn edge() -> EdgeDesc {
    EdgeDesc {
        capacity: 2,
        overflow: Overflow::Error,
        size: 4,
        key: u32_key(),
    }
}

pub fn host(name: &str, edges: Vec<u32>) -> HostDesc {
    HostDesc {
        name: name.to_string(),
        key: u32_key(),
        edges,
    }
}

/// `in` times 3, running-summed, out to `out`.
pub fn plan() -> PlanDesc {
    PlanDesc {
        library: LIBRARY.hash,
        steps: vec![
            StepDesc {
                entry: GAIN,
                node: 0,
                inputs: vec![Source::Edge(0), Source::Const(0)],
                outputs: vec![(0, 1)],
            },
            StepDesc {
                entry: SUM,
                node: 1,
                inputs: vec![Source::Edge(1)],
                outputs: vec![(0, 2)],
            },
        ],
        edges: vec![edge(), edge(), edge()],
        consts: vec![ConstDesc {
            kind: ScalarKind::U32,
            bits: 3,
        }],
        params: Vec::new(),
        host_inputs: vec![host("in", vec![0])],
        host_outputs: vec![host("out", vec![2])],
    }
}
