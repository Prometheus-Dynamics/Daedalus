//! A CPU node reads a list of fixed-size records through a `ListView`: from the list's wire form
//! where it is, record by record, with no decode and no allocation, and from the list itself when
//! it is a Rust value.

use crate::common;

use std::sync::Arc;

use daedalus_abi::counting::counted;
use daedalus_abi::{Slot, StepCx, StepEnd};
use daedalus_cpu::steps::{CpuBody, ListView, Setup, list_viewed};
use daedalus_macros::node;
use daedalus_registry::NodeRecord;
use daedalus_types::{SpaceIndex, TypeIndex, WireForm};

use common::{NODE, call, value};

/// A keypoint's shape: 28 bytes, five `f32` and two `i32`.
#[derive(
    Clone, Copy, Debug, Default, PartialEq, daedalus_derive::Type, daedalus_derive::WireForm,
)]
#[structural]
struct Point {
    x: f32,
    y: f32,
    size: f32,
    angle: f32,
    response: f32,
    octave: i32,
    class: i32,
}

#[node(id = "list.sum", inputs(port(name = "items")), outputs(sum))]
fn sum(items: ListView<'_, Point>) -> i64 {
    items
        .iter()
        .map(|p| p.x as i64 + p.class as i64)
        .sum::<i64>()
        + items.len() as i64
}

#[node(id = "list.last", inputs(port(name = "items")), outputs(last))]
fn last(items: ListView<'_, Point>) -> i32 {
    match items.is_empty() {
        true => -1,
        false => items.get(items.len() - 1).map_or(-2, |p| p.octave),
    }
}

fn step(record: &NodeRecord, wire: bool) -> daedalus_abi::Step {
    let body = record.body.code.downcast_ref::<CpuBody>().unwrap();
    let setup = Setup {
        node: NODE,
        inputs: 1,
        consts: &[],
        keep: &[],
        wire_inputs: if wire {
            daedalus_cpu::steps::ports::wire_bit(0)
        } else {
            0
        },
        id: record.decl.id,
        lent: &[],
        readers: None,
        plugin: false,
    };
    (body.make)(&setup).unwrap()
}

fn points(n: i32) -> Vec<Point> {
    (0..n)
        .map(|at| Point {
            x: at as f32,
            class: at % 7,
            octave: at,
            ..Point::default()
        })
        .collect()
}

fn wire_cell(list: &Vec<Point>) -> Slot {
    let mut bytes = vec![0u8; list.wire_len()];
    assert_eq!(list.write_wire(&mut bytes), bytes.len());
    let mut cell = Slot::EMPTY;
    daedalus_runner::put_shared(&mut cell, TypeIndex(0), SpaceIndex(0), Arc::from(bytes));
    cell
}

fn expected(list: &[Point]) -> i64 {
    list.iter()
        .map(|p| p.x as i64 + p.class as i64)
        .sum::<i64>()
        + list.len() as i64
}

#[test]
fn records_of_a_fixed_size_are_viewed_in_their_wire_form() {
    assert!(list_viewed::<Point>());
    assert!(!list_viewed::<String>(), "no fixed size");
    assert!(!list_viewed::<u8>(), "a list of bytes has no count");
}

#[test]
fn a_list_in_wire_form_is_read_where_it_is_with_no_allocation() {
    let list = points(1000);
    let mut input = wire_cell(&list);
    let mut step = step(&SUM_NODE, true);
    let mut cx = StepCx::new();
    let mut cells = [input, Slot::EMPTY];
    // Warm: the step's own first tick.
    assert!(matches!(
        call(&mut step, &mut cells, &mut cx),
        StepEnd::Done
    ));
    let mut sums = Vec::with_capacity(100);
    let ((), allocations) = counted(|| {
        for _ in 0..100 {
            if matches!(call(&mut step, &mut cells, &mut cx), StepEnd::Done) {
                sums.push(value::<i64>(&cells[1]));
            }
        }
    });
    assert_eq!(allocations, 0, "no decode and no list made");
    assert_eq!(sums.len(), 100);
    assert!(sums.iter().all(|sum| *sum == expected(&list)));
    daedalus_runner::put(&mut cells[1], Slot::EMPTY);
    daedalus_runner::put(&mut input, Slot::EMPTY);
}

#[test]
fn get_reads_one_record_and_an_empty_list_is_empty() {
    let mut cx = StepCx::new();
    for (n, want) in [(5, 4), (0, -1)] {
        let list = points(n);
        let mut input = wire_cell(&list);
        let mut step = step(&LAST_NODE, true);
        let mut cells = [input, Slot::EMPTY];
        assert!(matches!(
            call(&mut step, &mut cells, &mut cx),
            StepEnd::Done
        ));
        assert_eq!(value::<i32>(&cells[1]), want);
        daedalus_runner::put(&mut input, Slot::EMPTY);
    }
}

#[test]
fn a_list_held_as_a_value_is_borrowed() {
    let list = points(300);
    let mut input = Slot::EMPTY;
    daedalus_runner::put_typed(
        &mut input,
        TypeIndex(0),
        SpaceIndex(0),
        Arc::new(list.clone()),
    );
    let mut step = step(&SUM_NODE, false);
    let mut cx = StepCx::new();
    let mut cells = [input, Slot::EMPTY];
    assert!(matches!(
        call(&mut step, &mut cells, &mut cx),
        StepEnd::Done
    ));
    assert_eq!(value::<i64>(&cells[1]), expected(&list));
    daedalus_runner::put(&mut cells[0], Slot::EMPTY);
}

#[test]
fn a_wire_form_whose_count_disagrees_with_its_bytes_is_decoded_or_refused() {
    let list = points(10);
    let mut bytes = vec![0u8; list.wire_len()];
    list.write_wire(&mut bytes);
    // Say 11 records where there are 10: the view refuses to read past the bytes.
    bytes[..4].copy_from_slice(&11u32.to_le_bytes());
    assert!(ListView::<Point>::wire(&bytes).is_none());
    bytes[..4].copy_from_slice(&10u32.to_le_bytes());
    let view = ListView::<Point>::wire(&bytes).expect("a whole list");
    assert_eq!(view.to_vec(), list);
    assert!(view.in_place());
}
