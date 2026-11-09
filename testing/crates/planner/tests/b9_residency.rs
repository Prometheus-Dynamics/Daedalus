//! Where a value lives, under generic space names.

use daedalus_backend::{SpaceKind, TransferCost};
use daedalus_planner::stitch::{Item, Stitched};
use daedalus_planner_tests::{Mem, decl, edge, node, stitched, u8_ty};
use daedalus_registry::Registry;

fn registry() -> Registry {
    let mut registry = Registry::new();
    for backend in ["host", "dev1", "dev2"] {
        decl(&format!("t.{backend}"))
            .optional("x", u8_ty())
            .output("out", u8_ty())
            .body(backend)
            .add(&mut registry);
    }
    registry
}

const COPY: TransferCost = TransferCost::Copy;

fn host() -> Mem {
    Mem {
        id: "host",
        spaces: vec![("m.host", SpaceKind::Host, true)],
        ..Mem::default()
    }
}

fn device(id: &'static str, space: &'static str) -> Mem {
    Mem {
        id,
        spaces: vec![(space, SpaceKind::Device, true)],
        ..Mem::default()
    }
}

fn run(from: &str, to: &str, a: &Mem, b: &Mem) -> Stitched {
    let nodes = vec![node(&format!("t.{from}")), node(&format!("t.{to}"))];
    stitched(&registry(), nodes, vec![edge(0, "out", 1, "x")], &[a, b])
        .unwrap()
        .1
}

fn spaces_moved(result: &Stitched) -> Vec<(String, String)> {
    result
        .layout
        .iter()
        .filter_map(|item| match item {
            Item::Move {
                from_slot, to_slot, ..
            } => Some((
                result.spaces[result.slots[*from_slot as usize].space as usize].clone(),
                result.spaces[result.slots[*to_slot as usize].space as usize].clone(),
            )),
            _ => None,
        })
        .collect()
}

fn pair(a: &str, b: &str) -> (String, String) {
    (a.to_string(), b.to_string())
}

#[test]
fn a_producer_stays_where_its_consumer_can_read_it() {
    let mut consumer = device("dev1", "m.dev1");
    consumer.spaces.push(("m.host", SpaceKind::Host, true));
    let result = run("host", "dev1", &host(), &consumer);
    assert!(spaces_moved(&result).is_empty());
}

#[test]
fn a_host_side_producer_moves_to_its_device_consumers_space() {
    let mut source = host();
    source.moves = vec![("t.up", "m.host", "m.dev1", COPY)];
    let result = run("host", "dev1", &source, &device("dev1", "m.dev1"));
    assert_eq!(spaces_moved(&result), [pair("m.host", "m.dev1")]);
}

#[test]
fn a_value_moves_once_between_two_devices() {
    let mut first = device("dev1", "m.dev1");
    first.moves = vec![("t.across", "m.dev1", "m.dev2", COPY)];
    let result = run("dev1", "dev2", &first, &device("dev2", "m.dev2"));
    assert_eq!(spaces_moved(&result), [pair("m.dev1", "m.dev2")]);
}

#[test]
fn a_device_producer_wins_over_a_consumer_on_another_device() {
    let mut first = device("dev1", "m.dev1");
    first.moves = vec![("t.down", "m.dev1", "m.host", COPY)];
    let result = run("dev1", "host", &first, &host());
    assert_eq!(
        result.slots[0].space as usize,
        result.spaces.iter().position(|s| s == "m.dev1").unwrap()
    );
    assert_eq!(spaces_moved(&result), [pair("m.dev1", "m.host")]);
}
