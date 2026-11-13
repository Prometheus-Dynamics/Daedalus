//! A graph gives a list or a struct as a constant, and the node gets the decoded value.

mod common;

use common::{NODE, call, step_with, value};
use daedalus_abi::{Slot, StepCx, StepEnd};
use daedalus_backend::ConstOf;
use daedalus_cpu::CpuBackend;
use daedalus_cpu::steps::{CpuBody, Setup};
use daedalus_derive::{FromLiteral, Type};
use daedalus_graph::{GraphBody, Lit, LitField, NodeDoc};
use daedalus_macros::node;
use daedalus_planner::{Config, plan};
use daedalus_registry::{NodeRecord, Registry};

#[derive(Type, FromLiteral, Clone, Debug, PartialEq)]
#[type_key("t:gains")]
struct Gains {
    low: u32,
    high: u32,
    taps: Vec<f32>,
}

#[node(id = "t.sum", inputs(items), outputs(out))]
fn sum(items: &[u32]) -> u32 {
    items.iter().sum()
}

#[node(id = "t.rows", inputs(rows), outputs(out))]
fn row_count(rows: &[Vec<u32>]) -> u32 {
    rows.iter().map(|row| row.len() as u32).sum()
}

#[node(id = "t.span", inputs(gains), outputs(out))]
fn span(gains: &Gains) -> u32 {
    gains.high - gains.low + gains.taps.len() as u32
}

fn ints(items: &[i64]) -> Lit {
    Lit::List(items.iter().map(|&item| Lit::Int(item)).collect())
}

fn gains(fields: &[(&str, Lit)]) -> Lit {
    Lit::Struct(
        fields
            .iter()
            .map(|(name, value)| LitField {
                name: (*name).to_string(),
                value: value.clone(),
            })
            .collect(),
    )
}

fn whole_gains() -> Lit {
    gains(&[
        ("low", Lit::Int(2)),
        ("high", Lit::Int(10)),
        ("taps", Lit::List(vec![Lit::Float(0.5), Lit::Int(1)])),
    ])
}

/// Runs `record`'s step three times with `literal` on `port`, and gives its output each time.
fn run_with(record: &NodeRecord, port: &'static str, literal: &Lit) -> [u32; 3] {
    let consts = [ConstOf {
        node: NODE,
        port,
        value: literal,
    }];
    let mut step = step_with(record, 1, &consts);
    core::array::from_fn(|_| {
        let mut cells = [Slot::EMPTY, Slot::EMPTY];
        assert!(matches!(
            call(&mut step, &mut cells, &mut StepCx::new()),
            StepEnd::Done
        ));
        value::<u32>(&cells[1])
    })
}

/// Why `record` refuses `literal` on `port`.
fn refusal(record: &NodeRecord, port: &'static str, literal: &Lit) -> String {
    let body = record.body.code.downcast_ref::<CpuBody>().unwrap();
    let consts = [ConstOf {
        node: NODE,
        port,
        value: literal,
    }];
    let setup = Setup {
        node: NODE,
        inputs: 1,
        consts: &consts,
        keep: &[],
    };
    match (body.make)(&setup) {
        Ok(_) => panic!("the constant was taken"),
        Err(why) => why.to_string(),
    }
}

#[test]
fn a_list_constant_fills_a_list_input_every_tick() {
    assert_eq!(run_with(&SUM_NODE, "items", &ints(&[1, 2, 3, 4])), [10; 3]);
    assert_eq!(run_with(&SUM_NODE, "items", &ints(&[])), [0; 3]);
    let nested = Lit::List(vec![ints(&[1, 2]), ints(&[]), ints(&[3, 4, 5])]);
    assert_eq!(run_with(&ROW_COUNT_NODE, "rows", &nested), [5; 3]);
}

#[test]
fn a_struct_constant_fills_a_struct_input_by_field_name() {
    assert_eq!(run_with(&SPAN_NODE, "gains", &whole_gains()), [10; 3]);
    // The order the constant names its fields in doesn't matter.
    let reordered = gains(&[
        ("taps", Lit::List(Vec::new())),
        ("high", Lit::Int(7)),
        ("low", Lit::Int(7)),
    ]);
    assert_eq!(run_with(&SPAN_NODE, "gains", &reordered), [0; 3]);
}

#[test]
fn a_constant_of_the_wrong_shape_is_refused_with_its_port() {
    let text = Lit::Str("no".to_string());
    for (record, port, bad) in [
        (&SUM_NODE, "items", Lit::Int(3)),
        (
            &SUM_NODE,
            "items",
            Lit::List(vec![Lit::Int(1), text.clone()]),
        ),
        (&SUM_NODE, "items", ints(&[-1])),
        (&SPAN_NODE, "gains", ints(&[1, 2])),
        (&SPAN_NODE, "gains", gains(&[("low", Lit::Int(1))])),
        (
            &SPAN_NODE,
            "gains",
            gains(&[
                ("low", Lit::Int(1)),
                ("high", Lit::Int(2)),
                ("taps", Lit::List(Vec::new())),
                ("extra", Lit::Int(0)),
            ]),
        ),
    ] {
        let why = refusal(record, port, &bad);
        assert!(why.contains(port), "{why}");
        assert!(why.contains("doesn't fit"), "{why}");
    }
}

fn planned(port: &str, id: &str, value: Lit) -> Result<(), String> {
    let mut registry = Registry::new();
    registry.register_all(&[&SUM_NODE, &SPAN_NODE]).unwrap();
    let mut doc = NodeDoc::new(id);
    doc.consts.insert(port.to_string(), value);
    let graph = GraphBody {
        nodes: vec![doc],
        ..GraphBody::default()
    };
    plan(&graph, &registry, &[&CpuBackend], &Config::default())
        .map(drop)
        .map_err(|error| format!("{error:?}"))
}

#[test]
fn the_planner_takes_the_shapes_and_refuses_the_wrong_ones() {
    planned("items", "t.sum", ints(&[1, 2, 3])).unwrap();
    planned("gains", "t.span", whole_gains()).unwrap();
    assert!(planned("items", "t.sum", Lit::Int(1)).is_err());
    assert!(planned("gains", "t.span", gains(&[("low", Lit::Int(1))])).is_err());
}
