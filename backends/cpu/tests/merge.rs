//! Which nodes merge, and what a merged step does.

#![cfg(feature = "merge")]

mod common;

use common::value;
use daedalus_abi::testing::run_once;
use daedalus_abi::{Slot, Slots, StepCx, StepEnd};
use daedalus_backend::Compiled;
use daedalus_cpu::CpuBackend;
use daedalus_cpu_author::Fault;
use daedalus_graph::{EdgeDoc, EdgePolicyDoc, GraphBody, Lit, NodeDoc, PortRefDoc};
use daedalus_macros::node;
use daedalus_planner::{Config, Planned, plan};
use daedalus_registry::Registry;

#[node(id = "m.src", outputs(out))]
fn src() -> u32 {
    1
}

#[node(id = "m.inc", inputs(a), outputs(out))]
fn inc(a: u32) -> u32 {
    a + 1
}

#[node(id = "m.join", inputs(a, b), outputs(out))]
fn join(a: u32, b: u32) -> u32 {
    a * 100 + b
}

#[node(id = "m.cond", inputs(a), outputs(out))]
fn cond(a: u32) -> Option<u32> {
    (a > 100).then_some(a)
}

#[node(id = "m.bad", inputs(a), outputs(out))]
fn bad(a: u32) -> Result<u32, Fault> {
    Err(Fault {
        code: a,
        reason: "no",
    })
}

fn registry() -> Registry {
    let mut registry = Registry::new();
    registry
        .register_all(&[&SRC_NODE, &INC_NODE, &JOIN_NODE, &COND_NODE, &BAD_NODE])
        .unwrap();
    registry
}

fn wire(from: u32, port: &str, to: u32, into: &str) -> EdgeDoc {
    EdgeDoc {
        from: PortRefDoc {
            node: from,
            port: port.to_string(),
        },
        to: PortRefDoc {
            node: to,
            port: into.to_string(),
        },
        policy: EdgePolicyDoc::default(),
    }
}

fn planned(ids: &[&str], edges: Vec<EdgeDoc>) -> Planned {
    let nodes = ids.iter().map(|id| NodeDoc::new(*id)).collect();
    let graph = GraphBody {
        nodes,
        edges,
        groups: Vec::new(),
    };
    plan(&graph, &registry(), &[&CpuBackend], &Config::default()).unwrap()
}

fn covers(planned: &Planned) -> Vec<Vec<u32>> {
    let mut all = Vec::new();
    for region in &planned.regions.regions {
        let Compiled::Steps(units) = &region.compiled else {
            panic!("the CPU backend compiles to steps");
        };
        all.extend(
            units
                .iter()
                .map(|unit| unit.info.covers.iter().map(|node| node.0).collect()),
        );
    }
    all
}

fn chain(len: u32) -> Planned {
    let mut ids = vec!["m.src"];
    ids.extend((1..len).map(|_| "m.inc"));
    let edges = (1..len).map(|at| wire(at - 1, "out", at, "a")).collect();
    planned(&ids, edges)
}

#[test]
fn chain_runs_as_one_merged_unit() {
    let mut planned = chain(16);
    assert_eq!(covers(&planned), [(0..16).collect::<Vec<u32>>()]);
    let Compiled::Steps(units) = &mut planned.regions.regions[0].compiled else {
        panic!("steps");
    };
    let mut cells: [Slot; 0] = [];
    let mut slots = Slots::of(&mut cells);
    let end = run_once(&mut units[0].step, &mut slots, &mut StepCx::new());
    assert!(matches!(end, StepEnd::Done));
}

#[test]
fn the_last_value_of_a_merged_step_reaches_its_output() {
    let mut planned = planned(
        &["m.src", "m.inc", "m.inc", "m.join"],
        vec![
            wire(0, "out", 1, "a"),
            wire(1, "out", 2, "a"),
            wire(2, "out", 3, "a"),
            wire(0, "out", 3, "b"),
        ],
    );
    assert_eq!(covers(&planned), [vec![0], vec![1, 2], vec![3]]);
    let Compiled::Steps(units) = &mut planned.regions.regions[0].compiled else {
        panic!("steps");
    };
    let (mut buf, mut input) = ([0; 16], Slot::EMPTY);
    daedalus_runner::write_inline(&5_u32, &mut buf, &mut input);
    let mut cells = [input, Slot::EMPTY];
    let mut slots = Slots::of(&mut cells);
    let end = run_once(&mut units[1].step, &mut slots, &mut StepCx::new());
    assert!(matches!(end, StepEnd::Done));
    assert_eq!(value::<u32>(&cells[1]), 7);
}

#[test]
fn a_node_waiting_on_two_inputs_starts_its_own_step() {
    let fan_in = planned(
        &["m.src", "m.src", "m.join"],
        vec![wire(0, "out", 2, "a"), wire(1, "out", 2, "b")],
    );
    assert_eq!(covers(&fan_in), [vec![0], vec![1], vec![2]]);
}

#[test]
fn fan_out_a_bounded_edge_and_an_opt_out_stop_a_merge() {
    let fan_out = planned(
        &["m.src", "m.inc", "m.inc"],
        vec![wire(0, "out", 1, "a"), wire(0, "out", 2, "a")],
    );
    assert_eq!(covers(&fan_out), [vec![0], vec![1], vec![2]]);

    let mut edge = wire(0, "out", 1, "a");
    edge.policy = EdgePolicyDoc::Bounded {
        capacity: 4,
        overflow: "error".to_string(),
    };
    assert_eq!(
        covers(&planned(&["m.src", "m.inc"], vec![edge])),
        [vec![0], vec![1]]
    );

    let mut opted = NodeDoc::new("m.inc");
    opted
        .metadata
        .insert("daedalus.node.fusion".to_string(), Lit::Bool(false));
    let graph = GraphBody {
        nodes: vec![NodeDoc::new("m.src"), opted],
        edges: vec![wire(0, "out", 1, "a")],
        groups: Vec::new(),
    };
    let planned = plan(&graph, &registry(), &[&CpuBackend], &Config::default()).unwrap();
    assert_eq!(covers(&planned), [vec![0], vec![1]]);
}

#[test]
fn merging_off_gives_one_step_per_node() {
    let nodes = ["m.src", "m.inc"]
        .iter()
        .map(|id| NodeDoc::new(*id))
        .collect();
    let graph = GraphBody {
        nodes,
        edges: vec![wire(0, "out", 1, "a")],
        groups: Vec::new(),
    };
    let config = Config {
        merge: false,
        ..Config::default()
    };
    let planned = plan(&graph, &registry(), &[&CpuBackend], &config).unwrap();
    assert_eq!(covers(&planned), [vec![0], vec![1]]);
}

#[test]
fn a_fault_in_the_middle_stops_the_step_and_names_that_node() {
    let mut planned = planned(
        &["m.src", "m.bad", "m.inc"],
        vec![wire(0, "out", 1, "a"), wire(1, "out", 2, "a")],
    );
    let Compiled::Steps(units) = &mut planned.regions.regions[0].compiled else {
        panic!("steps");
    };
    let mut cells: [Slot; 0] = [];
    let mut slots = Slots::of(&mut cells);
    let mut cx = StepCx::new();
    let end = run_once(&mut units[0].step, &mut slots, &mut cx);
    assert!(matches!(end, StepEnd::Fault));
    assert_eq!(cx.at, 1);
    assert_eq!(cx.code, 1);
}

#[test]
fn a_producer_with_nothing_to_send_leaves_its_consumer_unrun() {
    let mut planned = planned(
        &["m.src", "m.cond", "m.inc"],
        vec![wire(0, "out", 1, "a"), wire(1, "out", 2, "a")],
    );
    let Compiled::Steps(units) = &mut planned.regions.regions[0].compiled else {
        panic!("steps");
    };
    let mut cells: [Slot; 0] = [];
    let mut slots = Slots::of(&mut cells);
    let end = run_once(&mut units[0].step, &mut slots, &mut StepCx::new());
    assert!(
        matches!(end, StepEnd::Skipped),
        "the consumer needed a value, so it skipped and nothing past it is woken"
    );
}

#[test]
fn a_value_already_waiting_is_what_the_first_node_gets() {
    let mut planned = chain_after_input();
    let Compiled::Steps(units) = &mut planned.regions.regions[0].compiled else {
        panic!("steps");
    };
    let (mut one, mut two) = ([0; 16], [0; 16]);
    let (mut a, mut b) = (Slot::EMPTY, Slot::EMPTY);
    daedalus_runner::write_inline(&1_u32, &mut one, &mut a);
    daedalus_runner::write_inline(&2_u32, &mut two, &mut b);
    let mut cells = [a, b, Slot::EMPTY];
    let mut slots = Slots::of(&mut cells);
    run_once(&mut units[0].step, &mut slots, &mut StepCx::new());
    assert_eq!(value::<u32>(&cells[2]), 104);
}

/// `join -> inc -> inc`, fed by steps before it and feeding a step after it.
fn chain_after_input() -> Planned {
    let mut region = planned(
        &["m.src", "m.src", "m.join", "m.inc", "m.inc", "m.join"],
        vec![
            wire(0, "out", 2, "a"),
            wire(1, "out", 2, "b"),
            wire(2, "out", 3, "a"),
            wire(3, "out", 4, "a"),
            wire(4, "out", 5, "a"),
            wire(1, "out", 5, "b"),
        ],
    );
    let Compiled::Steps(units) = &mut region.regions.regions[0].compiled else {
        panic!("steps");
    };
    units.retain(|unit| unit.info.covers.len() == 3);
    region
}
