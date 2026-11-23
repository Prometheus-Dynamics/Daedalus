//! A crate of `#[node]` functions, as any crate of nodes is written, and the one line that
//! ships it as a plugin. Linked into a host its nodes register from [`NODES`]; built as a
//! library a host loads, the same nodes arrive through the plugin entry.

#![forbid(unsafe_code)]

use daedalus_cpu::{Ctx, Fault};
use daedalus_derive::{Type, WireForm};
use daedalus_macros::node;
use daedalus_registry::NodeRecord;

/// A node with nothing but its arguments.
#[node(id = "np.triple", inputs(x), outputs(y))]
fn triple(x: u32) -> u32 {
    x.wrapping_mul(3)
}

#[derive(Default)]
struct Total(u32);

/// A node that keeps state between ticks.
#[node(id = "np.total", inputs(x), outputs(sum), state(Total))]
fn total(x: u32, state: &mut Total) -> u32 {
    state.0 = state.0.wrapping_add(x);
    state.0
}

/// A node that fails on one input, and keeps a total otherwise: what a restart starts again.
#[node(id = "np.fragile", inputs(x), outputs(sum), state(Total))]
fn fragile(x: u32, state: &mut Total) -> Result<u32, Fault> {
    if x == 13 {
        return Err(Fault {
            code: 13,
            reason: "thirteen",
        });
    }
    state.0 = state.0.wrapping_add(x);
    Ok(state.0)
}

/// A node that reports on its own work.
#[node(id = "np.counted", inputs(x), outputs(y), metrics(seen = "count"))]
fn counted(x: u32, ctx: &Ctx) -> u32 {
    ctx.metric(CountedMetric::Seen, u64::from(x));
    x
}

/// A value of the crate's own, with a crossing form.
#[derive(Clone, Debug, PartialEq, Type, WireForm)]
#[type_key("np.pair")]
pub struct Pair {
    pub low: u32,
    pub high: u32,
}

#[node(id = "np.split", inputs(x), outputs(pair))]
fn split(x: u32) -> Pair {
    Pair {
        low: x & 0xffff,
        high: x >> 16,
    }
}

#[node(id = "np.join", inputs(pair), outputs(y))]
fn join(pair: &Pair) -> u32 {
    pair.low + pair.high
}

/// A megabyte made from a number, and read back: bytes pass by handle.
#[node(id = "np.fill", inputs(x), outputs(bytes))]
fn fill(x: u32) -> Vec<u8> {
    vec![x as u8; 1 << 20]
}

#[node(id = "np.first", inputs(bytes), outputs(y))]
fn first(bytes: &[u8]) -> u32 {
    u32::from(bytes[0]) + bytes.len() as u32
}

/// Every node of this crate: what a host that links it registers.
pub const NODES: [&NodeRecord; 8] = [
    &TRIPLE_NODE,
    &TOTAL_NODE,
    &COUNTED_NODE,
    &SPLIT_NODE,
    &JOIN_NODE,
    &FILL_NODE,
    &FIRST_NODE,
    &FRAGILE_NODE,
];

daedalus_cpu_plugin::export_nodes!("node-plugin", NODES);
