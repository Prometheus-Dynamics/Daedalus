//! Nodes run through their steps and give the values their functions compute.

mod common;

use std::sync::Arc;

use common::{NODE, call, inline, step_of, step_with, value};
use daedalus_abi::{Slot, SlotState, Slots, StepCx, StepEnd};
use daedalus_backend::{Compiled, ConstOf};
use daedalus_cpu::CpuBackend;
use daedalus_cpu_author::{Ctx, FanIn};
use daedalus_graph::{EdgeDoc, EdgePolicyDoc, GraphBody, Lit, NodeDoc, PortRefDoc};
use daedalus_macros::node;
use daedalus_planner::stitch::Item;
use daedalus_planner::{Config, plan};
use daedalus_registry::Registry;
use daedalus_runner::{put_typed, typed_view};
use daedalus_types::{SpaceIndex, TypeIndex};

#[node(id = "t.zero", outputs(out))]
fn zero() -> u32 {
    7
}

#[node(id = "t.inc", inputs(a), outputs(out))]
fn inc(a: u32) -> u32 {
    a + 1
}

#[node(id = "t.three", inputs(a, b, c), outputs(sum, product, last))]
fn three(a: u32, b: &u32, c: u32) -> (u32, u32, u32) {
    (a + b + c, a * b * c, c)
}

#[node(id = "t.opt", inputs(a), outputs(out))]
fn opt(a: Option<u32>) -> u32 {
    a.map_or(100, |a| a * 2)
}

#[node(id = "t.maybe", inputs(a), outputs(out))]
fn maybe(a: u32) -> Option<u32> {
    (a > 1).then_some(a)
}

#[derive(Default)]
struct Count(u32);

#[node(id = "t.count", inputs(a), outputs(out), state(Count))]
fn count(a: u32, state: &mut Count) -> u32 {
    state.0 += a;
    state.0
}

#[node(id = "t.ctx", outputs(out))]
fn ctx(cx: &Ctx) -> u64 {
    cx.now_ns
}

#[node(id = "t.fan", inputs(items), outputs(out))]
fn fan(items: FanIn<'_, u32>) -> u32 {
    items.iter().fold(0, |sum, item| sum * 10 + *item)
}

#[derive(Clone, PartialEq, Debug)]
struct Bag {
    items: Vec<u32>,
}

impl daedalus_types::Typed for Bag {
    const KEY: daedalus_types::TypeKey = daedalus_types::TypeKey::new("t:bag");
    const EXPR: daedalus_types::TypeExpr = <u32 as daedalus_types::Typed>::EXPR;
}

#[node(id = "t.bag", inputs(a), outputs(out))]
fn bag(a: u32) -> Bag {
    Bag { items: vec![a; 2] }
}

#[node(id = "t.len", inputs(bag), outputs(out))]
fn bag_len(bag: &Bag) -> u32 {
    bag.items.len() as u32
}

#[node(id = "t.own", inputs(bag), outputs(out))]
fn bag_own(mut bag: Bag) -> Bag {
    bag.items.push(9);
    bag
}

#[node(id = "t.edit", inputs(bag), outputs(out))]
fn bag_edit(bag: &mut Bag) -> u32 {
    bag.items.push(1);
    bag.items.len() as u32
}

#[test]
fn a_node_with_no_inputs_writes_its_value() {
    let mut step = step_of(&ZERO_NODE);
    let mut cells = [Slot::EMPTY];
    let end = call(&mut step, &mut cells, &mut StepCx::new());
    assert!(matches!(end, StepEnd::Done));
    assert_eq!(value::<u32>(&cells[0]), 7);
}

#[test]
fn one_input_one_output() {
    let mut step = step_of(&INC_NODE);
    let mut buf = [0; 16];
    let mut cells = [inline(41_u32, &mut buf), Slot::EMPTY];
    assert!(matches!(
        call(&mut step, &mut cells, &mut StepCx::new()),
        StepEnd::Done
    ));
    assert_eq!(value::<u32>(&cells[1]), 42);
}

#[test]
fn three_inputs_three_outputs() {
    let mut step = step_of(&THREE_NODE);
    let mut bufs = [[0; 16]; 3];
    let [a, b, c] = &mut bufs;
    let mut cells = [
        inline(2_u32, a),
        inline(3_u32, b),
        inline(4_u32, c),
        Slot::EMPTY,
        Slot::EMPTY,
        Slot::EMPTY,
    ];
    call(&mut step, &mut cells, &mut StepCx::new());
    let got: Vec<u32> = cells[3..].iter().map(value::<u32>).collect();
    assert_eq!(got, [9, 24, 4]);
}

#[test]
fn a_required_input_with_nothing_skips_and_takes_nothing() {
    let mut step = step_of(&THREE_NODE);
    let mut buf = [0; 16];
    let mut cells = [
        inline(2_u32, &mut buf),
        Slot::EMPTY,
        Slot::EMPTY,
        Slot::EMPTY,
        Slot::EMPTY,
        Slot::EMPTY,
    ];
    assert!(matches!(
        call(&mut step, &mut cells, &mut StepCx::new()),
        StepEnd::Skipped
    ));
    assert_eq!(cells[0].state, SlotState::Full as u32);
    assert_eq!(cells[3].state, SlotState::Empty as u32);
}

#[test]
fn an_optional_input_is_none_when_empty() {
    let mut step = step_of(&OPT_NODE);
    let mut cells = [Slot::EMPTY, Slot::EMPTY];
    call(&mut step, &mut cells, &mut StepCx::new());
    assert_eq!(value::<u32>(&cells[1]), 100);

    let mut buf = [0; 16];
    let mut cells = [inline(4_u32, &mut buf), Slot::EMPTY];
    call(&mut step, &mut cells, &mut StepCx::new());
    assert_eq!(value::<u32>(&cells[1]), 8);
}

#[test]
fn a_conditional_output_writes_nothing_on_none() {
    let mut step = step_of(&MAYBE_NODE);
    let mut buf = [0; 16];
    let mut cells = [inline(1_u32, &mut buf), Slot::EMPTY];
    assert!(matches!(
        call(&mut step, &mut cells, &mut StepCx::new()),
        StepEnd::Done
    ));
    assert_eq!(cells[1].state, SlotState::Empty as u32);
}

#[test]
fn state_lasts_between_calls_and_is_per_instance_and_resets_on_restart() {
    let (mut first, mut second) = (step_of(&COUNT_NODE), step_of(&COUNT_NODE));
    let mut buf = [0; 16];
    let input = inline(5_u32, &mut buf);
    let mut cx = StepCx::new();
    let run = |step: &mut daedalus_abi::Step, cx: &mut StepCx| {
        let mut cells = [input, Slot::EMPTY];
        call(step, &mut cells, cx);
        value::<u32>(&cells[1])
    };
    assert_eq!(run(&mut first, &mut cx), 5);
    assert_eq!(run(&mut first, &mut cx), 10);
    assert_eq!(run(&mut second, &mut cx), 5, "another instance has its own");
    cx.flags = daedalus_abi::RESTART;
    assert_eq!(run(&mut first, &mut cx), 5);
}

#[test]
fn ctx_is_the_runners() {
    let mut step = step_of(&CTX_NODE);
    let mut cells = [Slot::EMPTY];
    let mut cx = StepCx::new();
    cx.now_ns = 99;
    call(&mut step, &mut cells, &mut cx);
    assert_eq!(value::<u64>(&cells[0]), 99);
}

#[test]
fn a_constant_fills_an_unwired_input_every_tick() {
    let literal = Lit::Int(6);
    let consts = [ConstOf {
        node: NODE,
        port: "a",
        value: &literal,
    }];
    let mut step = step_with(&INC_NODE, 1, &consts);
    for _ in 0..3 {
        let mut cells = [Slot::EMPTY, Slot::EMPTY];
        assert!(matches!(
            call(&mut step, &mut cells, &mut StepCx::new()),
            StepEnd::Done
        ));
        assert_eq!(value::<u32>(&cells[1]), 7);
    }
}

#[test]
fn fan_in_sees_every_port_in_order() {
    let mut step = step_with(&FAN_NODE, 3, &[]);
    let mut bufs = [[0; 16]; 2];
    let [a, b] = &mut bufs;
    let mut cells = [inline(1_u32, a), Slot::EMPTY, inline(3_u32, b), Slot::EMPTY];
    call(&mut step, &mut cells, &mut StepCx::new());
    assert_eq!(value::<u32>(&cells[3]), 13, "the empty port is passed over");
}

fn typed(bag: Bag) -> Slot {
    let mut slot = Slot::EMPTY;
    put_typed(&mut slot, TypeIndex(0), SpaceIndex(0), Arc::new(bag));
    slot
}

#[test]
fn a_large_value_travels_by_reference() {
    let mut step = step_of(&BAG_NODE);
    let mut buf = [0; 16];
    let mut cells = [inline(3_u32, &mut buf), Slot::EMPTY];
    call(&mut step, &mut cells, &mut StepCx::new());
    assert_eq!(typed_view::<Bag>(&cells[1]).unwrap().items, [3, 3]);
    daedalus_runner::drop_owned(cells[1]);

    let mut step = step_of(&BAG_LEN_NODE);
    let held = typed(Bag { items: vec![1; 4] });
    let mut cells = [held, Slot::EMPTY];
    call(&mut step, &mut cells, &mut StepCx::new());
    assert_eq!(value::<u32>(&cells[1]), 4);
    assert_eq!(cells[0].state, SlotState::Full as u32, "a read leaves it");
    daedalus_runner::drop_owned(held);
}

#[test]
fn a_value_taken_by_a_step_is_its_to_keep() {
    let mut step = step_of(&BAG_OWN_NODE);
    let mut cells = [typed(Bag { items: vec![1] }), Slot::EMPTY];
    call(&mut step, &mut cells, &mut StepCx::new());
    assert_eq!(cells[0].state, SlotState::Moved as u32);
    assert_eq!(typed_view::<Bag>(&cells[1]).unwrap().items, [1, 9]);
    daedalus_runner::drop_owned(cells[1]);

    let mut step = step_of(&BAG_EDIT_NODE);
    let mut cells = [typed(Bag { items: vec![1] }), Slot::EMPTY];
    call(&mut step, &mut cells, &mut StepCx::new());
    assert_eq!(cells[0].state, SlotState::Moved as u32);
    assert_eq!(value::<u32>(&cells[1]), 2);
}

fn wire(from: u32, from_port: &str, to: u32, to_port: &str) -> EdgeDoc {
    EdgeDoc {
        from: PortRefDoc {
            node: from,
            port: from_port.to_string(),
        },
        to: PortRefDoc {
            node: to,
            port: to_port.to_string(),
        },
        policy: EdgePolicyDoc::default(),
    }
}

#[test]
fn a_planned_graph_runs_through_the_backend() {
    let mut registry = Registry::new();
    registry
        .register_all(&[&ZERO_NODE, &INC_NODE, &OPT_NODE])
        .unwrap();
    let graph = GraphBody {
        nodes: ["t.zero", "t.inc", "t.opt"]
            .into_iter()
            .map(NodeDoc::new)
            .collect(),
        edges: vec![wire(0, "out", 1, "a"), wire(1, "out", 2, "a")],
        groups: Vec::new(),
    };
    let mut planned = plan(&graph, &registry, &[&CpuBackend], &Config::default()).unwrap();
    let mut cells = vec![Slot::EMPTY; planned.stitched.slots.len()];
    for item in planned.stitched.layout.clone() {
        let Item::Run {
            covers,
            reads,
            writes,
            ..
        } = item
        else {
            continue;
        };
        let mut local: Vec<Slot> = reads
            .iter()
            .map(|&slot| cells[slot as usize])
            .chain(writes.iter().map(|_| Slot::EMPTY))
            .collect();
        let units = planned
            .regions
            .regions
            .iter_mut()
            .find_map(|region| match &mut region.compiled {
                Compiled::Steps(units) => units
                    .iter_mut()
                    .find(|unit| unit.info.covers[0].0 == covers[0]),
                _ => None,
            })
            .unwrap();
        let mut slots = Slots::of(&mut local);
        let end = daedalus_abi::testing::run_once(&mut units.step, &mut slots, &mut StepCx::new());
        assert!(matches!(end, StepEnd::Done));
        for (at, &slot) in writes.iter().enumerate() {
            if slot != daedalus_abi::NO_SLOT {
                cells[slot as usize] = local[reads.len() + at];
            }
        }
    }
    let last = planned.stitched.slots.len() - 1;
    let values: Vec<u32> = (0..=last).map(|slot| value::<u32>(&cells[slot])).collect();
    assert_eq!(
        values,
        [7, 8],
        "t.opt's output has no edge, so it has no slot"
    );
}

#[test]
fn fan_in_ports_are_numbered_not_in_edge_order() {
    let mut registry = Registry::new();
    registry.register_all(&[&ZERO_NODE, &FAN_NODE]).unwrap();
    let graph = GraphBody {
        nodes: ["t.zero", "t.zero", "t.fan"]
            .into_iter()
            .map(NodeDoc::new)
            .collect(),
        edges: vec![wire(0, "out", 2, "items5"), wire(1, "out", 2, "items2")],
        groups: Vec::new(),
    };
    let planned = plan(&graph, &registry, &[&CpuBackend], &Config::default()).unwrap();
    let names: Vec<&str> = planned.graph.nodes[2]
        .inputs
        .iter()
        .map(|port| port.name.as_str())
        .collect();
    assert_eq!(names, ["items2", "items5"]);
}
