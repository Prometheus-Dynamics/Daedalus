//! Random graphs give the same calls, in the same order, with and without merging.

#![cfg(feature = "merge")]

use std::cell::RefCell;

use daedalus_cpu::CpuBackend;
use daedalus_cpu_author::Fault;
use daedalus_graph::{EdgeDoc, EdgePolicyDoc, GraphBody, Lit, NodeDoc, PortRefDoc};
use daedalus_host::{LoadOptions, LoadedHost};
use daedalus_macros::node;
use daedalus_planner::{Config, plan};
use daedalus_registry::{NodeRecord, Registry};

thread_local! {
    static CALLS: RefCell<Vec<(u32, Vec<u32>, u32)>> = const { RefCell::new(Vec::new()) };
}

fn log(id: u32, inputs: &[u32], out: u32) -> u32 {
    CALLS.with(|calls| calls.borrow_mut().push((id, inputs.to_vec(), out)));
    out
}

#[derive(Default)]
struct Seen(u32);

#[node(id = "f.src", inputs(id), outputs(out))]
fn src(id: u32) -> u32 {
    log(id, &[], id * 3 + 1)
}

#[node(id = "f.two", inputs(id), outputs(a, b))]
fn two(id: u32) -> (u32, u32) {
    (log(id, &[], id + 2), id + 3)
}

#[node(id = "f.map", inputs(id, a), outputs(out))]
fn map(id: u32, a: u32) -> u32 {
    log(id, &[a], a.wrapping_mul(7) ^ id)
}

#[node(id = "f.opt", inputs(id, a), outputs(out))]
fn opt(id: u32, a: Option<u32>) -> u32 {
    log(id, &[a.unwrap_or(9999)], a.unwrap_or(5) + id)
}

#[node(id = "f.join", inputs(id, a, b), outputs(out))]
fn join(id: u32, a: u32, b: u32) -> u32 {
    log(id, &[a, b], a ^ b.rotate_left(3))
}

#[node(id = "f.all", inputs(id, a, b), outputs(out), fire = "all")]
fn all(id: u32, a: u32, b: u32) -> u32 {
    log(id, &[a, b], a.wrapping_add(b))
}

#[node(id = "f.cond", inputs(id, a), outputs(out))]
fn cond(id: u32, a: u32) -> Option<u32> {
    log(id, &[a], a);
    a.is_multiple_of(2).then_some(a / 2)
}

#[node(id = "f.state", inputs(id, a), outputs(out), state(Seen))]
fn stateful(id: u32, a: u32, seen: &mut Seen) -> u32 {
    seen.0 = seen.0.wrapping_add(a);
    log(id, &[a], seen.0)
}

/// Fails on its second call only. Faults that repeat back off the whole merged step, where an
/// unmerged run backs off one node, and that difference is the point of merging as a unit.
#[node(id = "f.fail", inputs(id, a), outputs(out), state(Seen))]
fn fail(id: u32, a: u32, calls: &mut Seen) -> Result<u32, Fault> {
    log(id, &[a], 0);
    calls.0 += 1;
    if calls.0 == 2 {
        return Err(Fault {
            code: id,
            reason: "second call",
        });
    }
    Ok(a + 1)
}

const RECORDS: &[&NodeRecord] = &[
    &SRC_NODE,
    &TWO_NODE,
    &MAP_NODE,
    &OPT_NODE,
    &JOIN_NODE,
    &ALL_NODE,
    &COND_NODE,
    &STATEFUL_NODE,
    &FAIL_NODE,
];

struct Rng(u64);

impl Rng {
    fn below(&mut self, limit: u32) -> u32 {
        self.0 = self
            .0
            .wrapping_mul(6_364_136_223_846_793_005)
            .wrapping_add(1);
        ((self.0 >> 33) as u32) % limit
    }
}

fn graph(seed: u64) -> GraphBody {
    let mut rng = Rng(seed);
    let mut nodes = Vec::new();
    let mut edges: Vec<EdgeDoc> = Vec::new();
    // Every output made so far.
    let mut outputs: Vec<(u32, &str)> = Vec::new();
    let count = 3 + rng.below(14);
    let mut failing = false;
    for at in 0..count {
        let mut kind = if at < 2 {
            rng.below(2)
        } else {
            2 + rng.below(7)
        };
        // One failing node a graph: repeated faults in one merged step back it off as a whole.
        if kind == 8 && failing {
            kind = 2;
        }
        failing |= kind == 8;
        let id = [
            "f.src", "f.two", "f.map", "f.opt", "f.join", "f.all", "f.cond", "f.state", "f.fail",
        ][kind as usize];
        let mut doc = NodeDoc::new(id);
        doc.consts.insert("id".to_string(), Lit::Int(i64::from(at)));
        if rng.below(8) == 0 {
            doc.metadata
                .insert("daedalus.node.fusion".to_string(), Lit::Bool(false));
        }
        let inputs: &[&str] = match kind {
            0 | 1 => &[],
            4 | 5 => &["a", "b"],
            _ => &["a"],
        };
        for &input in inputs {
            if kind == 3 && rng.below(3) == 0 {
                continue;
            }
            // Mostly the newest output, so there are chains to merge.
            let pick = if rng.below(4) == 0 {
                rng.below(outputs.len() as u32) as usize
            } else {
                outputs.len() - 1
            };
            let (from, port) = outputs[pick];
            let mut policy = EdgePolicyDoc::default();
            if rng.below(6) == 0 {
                policy = EdgePolicyDoc::LatestOnly;
            }
            edges.push(EdgeDoc {
                from: PortRefDoc {
                    node: from,
                    port: port.to_string(),
                },
                to: PortRefDoc {
                    node: at,
                    port: input.to_string(),
                },
                policy,
            });
        }
        nodes.push(doc);
        match kind {
            1 => {
                outputs.push((at, "a"));
                outputs.push((at, "b"));
            }
            _ => outputs.push((at, "out")),
        }
    }
    GraphBody {
        nodes,
        edges,
        groups: Vec::new(),
    }
}

type Observed = (Vec<(u32, Vec<u32>, u32)>, Vec<(Option<String>, String)>);

fn run(graph: &GraphBody, merge: bool) -> Observed {
    let mut registry = Registry::new();
    registry.register_all(RECORDS).unwrap();
    let config = Config {
        merge,
        ..Config::default()
    };
    let planned = plan(graph, &registry, &[&CpuBackend], &config).unwrap();
    let options = LoadOptions::default();
    let mut host = LoadedHost::load(planned, options).unwrap();
    CALLS.with(|calls| calls.borrow_mut().clear());
    for tick in 0..6 {
        host.step()
            .unwrap_or_else(|error| panic!("tick {tick}, merge {merge}: {error:?}\n{graph:#?}"));
    }
    let faults = host
        .fault_reports()
        .map(|report| {
            (
                report.node().map(str::to_string),
                report.reason().to_string(),
            )
        })
        .collect();
    (CALLS.with(|calls| calls.borrow().clone()), faults)
}

#[test]
fn merged_and_unmerged_runs_agree() {
    for seed in 0..300_u64 {
        let graph = graph(seed.wrapping_mul(2_654_435_761).wrapping_add(17));
        let (merged, unmerged) = (run(&graph, true), run(&graph, false));
        assert_eq!(merged.0, unmerged.0, "calls differ for seed {seed}");
        assert_eq!(merged.1, unmerged.1, "faults differ for seed {seed}");
    }
}
