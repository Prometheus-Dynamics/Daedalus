//! A crate of `#[node]` functions, as any crate of nodes is written, and the one line that
//! ships it as a plugin. Linked into a host its nodes register from [`NODES`]; built as a
//! library a host loads, the same nodes arrive through the plugin entry.

#![forbid(unsafe_code)]

use daedalus_cpu::{Ctx, Fault};
use daedalus_cpu_author::FanIn;
use daedalus_derive::{NodeConfig, Type, WireForm};
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

/// A node with an input a graph gives as a constant.
#[node(id = "np.scale", inputs(x, by), outputs(y))]
fn scale(x: u32, by: u32) -> u32 {
    x.wrapping_mul(by)
}

/// A node's config.
#[derive(Clone, Default, Type, NodeConfig)]
#[type_key("np.shift")]
pub struct Shift {
    #[port(default = 1)]
    pub add: u32,
    #[port(default = 0)]
    pub then_double: u32,
}

/// A node with a config.
#[node(id = "np.shifted", inputs(x, config = Shift), outputs(y))]
fn shifted(x: u32, config: &Shift) -> u32 {
    let moved = x.wrapping_add(config.add);
    if config.then_double != 0 {
        moved.wrapping_mul(2)
    } else {
        moved
    }
}

/// A node that takes a fan-in, which a plugin's node cannot yet: the plugin refuses it when a
/// graph uses it, and says why.
#[node(id = "np.sum", inputs(items), outputs(y))]
fn sum(items: FanIn<'_, u32>) -> u32 {
    items.iter().fold(0, |sum, item| sum.wrapping_add(*item))
}

/// A value of the crate's own with no crossing form: it goes between two of the plugin's nodes as
/// it is, and cannot leave the plugin.
#[derive(Clone, Debug, PartialEq, Type)]
#[type_key("np.kept")]
pub struct Kept {
    pub items: Vec<u32>,
}

#[node(id = "np.keep", inputs(x), outputs(kept))]
fn keep(x: u32) -> Kept {
    Kept {
        items: (0..x % 64).collect(),
    }
}

#[node(id = "np.weigh", inputs(kept), outputs(y))]
fn weigh(kept: &Kept) -> u32 {
    kept.items.iter().sum::<u32>() + kept.items.len() as u32
}

/// Reads the value where it is, and fails on a weight of 15: the value is still in its cell when
/// the step fails.
#[node(id = "np.weigh_or_fail", inputs(kept), outputs(y))]
fn weigh_or_fail(kept: &Kept) -> Result<u32, Fault> {
    let weight = kept.items.iter().sum::<u32>();
    if weight == 15 {
        return Err(Fault {
            code: 15,
            reason: "fifteen",
        });
    }
    Ok(weight)
}

/// How many values this build handed from one of its nodes to another and the host has not yet
/// released: zero when no cell holds one.
#[node(id = "np.held", inputs(x), outputs(y))]
fn held(x: u32) -> u32 {
    let _ = x;
    daedalus_cpu::values_held() as u32
}

/// Where the bytes it was given are, the low 32 bits of their address: to tell bytes read where
/// another node made them from a copy.
#[node(id = "np.at", inputs(bytes), outputs(y))]
fn at(bytes: &[u8]) -> u32 {
    bytes.as_ptr() as usize as u32
}

/// Every node of this crate: what a host that links it registers.
pub const NODES: [&NodeRecord; 16] = [
    &TRIPLE_NODE,
    &TOTAL_NODE,
    &COUNTED_NODE,
    &SPLIT_NODE,
    &JOIN_NODE,
    &FILL_NODE,
    &FIRST_NODE,
    &FRAGILE_NODE,
    &SCALE_NODE,
    &SHIFTED_NODE,
    &SUM_NODE,
    &KEEP_NODE,
    &WEIGH_NODE,
    &WEIGH_OR_FAIL_NODE,
    &HELD_NODE,
    &AT_NODE,
];

daedalus_cpu_plugin::export_nodes!("node-plugin", NODES);
