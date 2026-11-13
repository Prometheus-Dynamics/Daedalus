use daedalus_backend::{SpaceKind, TransferCost};
use daedalus_graph::{EdgeDoc, NodeDoc};
use daedalus_plan::encode;
use daedalus_planner::regions::RegionError;
use daedalus_planner::stitch::{Item, StitchError, Stitched};
use daedalus_planner_tests::{Mem, decl, edge, node, stitched, u8_ty};
use daedalus_registry::Registry;

fn registry() -> Registry {
    let mut registry = Registry::new();
    for backend in ["a", "b", "c"] {
        decl(&format!("t.{backend}"))
            .optional("x", u8_ty())
            .optional("y", u8_ty())
            .output("out", u8_ty())
            .body(backend)
            .add(&mut registry);
    }
    registry
}

fn own(id: &'static str, space: &'static str) -> Mem {
    Mem {
        id,
        spaces: vec![(space, SpaceKind::Device, true)],
        ..Mem::default()
    }
}

fn run(
    nodes: Vec<NodeDoc>,
    edges: Vec<EdgeDoc>,
    a: &Mem,
    b: &Mem,
) -> Result<Stitched, RegionError> {
    stitched(&registry(), nodes, edges, &[a, b]).map(|(_, stitched)| stitched)
}

fn line(ids: &[&str]) -> (Vec<NodeDoc>, Vec<EdgeDoc>) {
    let nodes = ids.iter().map(|id| node(id)).collect();
    let edges = (1..ids.len() as u32)
        .map(|i| edge(i - 1, "out", i, "x"))
        .collect();
    (nodes, edges)
}

fn moves(stitched: &Stitched) -> Vec<&str> {
    stitched
        .layout
        .iter()
        .filter_map(|item| match item {
            Item::Move { transfer, .. } => Some(stitched.transfers[*transfer as usize].id.as_str()),
            _ => None,
        })
        .collect()
}

#[test]
fn value_stays_when_consumer_can_read_producer_space() {
    let a = own("a", "m.a");
    let mut b = own("b", "m.b");
    b.spaces.push(("m.a", SpaceKind::Device, true));
    let (nodes, edges) = line(&["t.a", "t.b"]);
    let result = run(nodes, edges, &a, &b).unwrap();
    assert!(moves(&result).is_empty());
    assert_eq!(result.slots.len(), 1);
}

const IMPORT: TransferCost = TransferCost::Import;
const COPY: TransferCost = TransferCost::Copy;

#[test]
fn transfer_takes_cheapest_of_two_paths() {
    let mut a = own("a", "m.a");
    a.moves = vec![
        ("t.copy", "m.a", "m.b", COPY),
        ("t.first", "m.a", "m.mid", IMPORT),
        ("t.second", "m.mid", "m.b", IMPORT),
    ];
    let b = own("b", "m.b");
    let (nodes, edges) = line(&["t.a", "t.b"]);
    assert_eq!(
        moves(&run(nodes, edges, &a, &b).unwrap()),
        ["t.first", "t.second"]
    );
}

#[test]
fn tie_by_hops_then_space_id() {
    let mut a = own("a", "m.a");
    a.moves = vec![
        ("t.long1", "m.a", "m.z", COPY),
        ("t.long2", "m.z", "m.b", COPY),
        ("t.short", "m.a", "m.b", COPY),
    ];
    let b = own("b", "m.b");
    let (nodes, edges) = line(&["t.a", "t.b"]);
    assert_eq!(moves(&run(nodes, edges, &a, &b).unwrap()), ["t.short"]);

    let mut a = own("a", "m.a");
    a.moves = vec![
        ("t.via_z", "m.a", "m.z", COPY),
        ("t.z_b", "m.z", "m.b", COPY),
        ("t.via_y", "m.a", "m.y", COPY),
        ("t.y_b", "m.y", "m.b", COPY),
    ];
    let (nodes, edges) = line(&["t.a", "t.b"]);
    assert_eq!(
        moves(&run(nodes, edges, &a, &b).unwrap()),
        ["t.via_y", "t.y_b"]
    );
}

#[test]
fn no_path_names_port_and_spaces() {
    let (nodes, edges) = line(&["t.a", "t.b"]);
    let error = run(nodes, edges, &own("a", "m.a"), &own("b", "m.b")).unwrap_err();
    assert_eq!(
        error,
        RegionError::Stitch(StitchError::NoTransfer {
            port: "t.b.x".into(),
            from_port: "t.a.out".into(),
            from_space: "m.a".into(),
            to_space: "m.b".into(),
        })
    );
}

/// `a` can also write its output into `m.share`, which `b` reads without a move.
fn placing() -> Mem {
    let mut a = own("a", "m.a");
    a.spaces.push(("m.share", SpaceKind::Shared, true));
    a.placements = vec!["m.share"];
    a.moves = vec![("t.copy", "m.a", "m.b", COPY)];
    a
}

fn slot_spaces(stitched: &Stitched) -> Vec<&str> {
    stitched
        .slots
        .iter()
        .map(|slot| stitched.spaces[slot.space as usize].as_str())
        .collect()
}

#[test]
fn an_output_is_written_where_its_reader_needs_no_move() {
    let mut b = own("b", "m.b");
    b.spaces.push(("m.share", SpaceKind::Shared, true));
    let (nodes, edges) = line(&["t.a", "t.b"]);
    let placed = run(nodes, edges, &placing(), &b).unwrap();
    assert!(moves(&placed).is_empty(), "{:?}", moves(&placed));
    assert_eq!(slot_spaces(&placed), ["m.share"]);

    // Without the placement the same graph pays a copy.
    let mut plain = placing();
    plain.placements.clear();
    let (nodes, edges) = line(&["t.a", "t.b"]);
    assert_eq!(moves(&run(nodes, edges, &plain, &b).unwrap()), ["t.copy"]);
}

#[test]
fn an_output_stays_home_when_a_placement_costs_its_readers_more() {
    // `b` reads `m.a` and nothing reaches it from `m.share`.
    let mut b = own("b", "m.b");
    b.spaces.push(("m.a", SpaceKind::Device, true));
    let (nodes, edges) = line(&["t.a", "t.b"]);
    let stitched = run(nodes, edges, &placing(), &b).unwrap();
    assert!(moves(&stitched).is_empty());
    assert_eq!(slot_spaces(&stitched), ["m.a"]);
}

fn mover() -> Mem {
    let mut a = own("a", "m.a");
    a.moves = vec![("t.move", "m.a", "m.b", COPY)];
    a
}

#[test]
fn one_transfer_for_two_consumers_in_same_space() {
    let (a, b) = (mover(), own("b", "m.b"));
    let result = run(
        vec![node("t.a"), node("t.b"), node("t.b")],
        vec![edge(0, "out", 1, "x"), edge(0, "out", 2, "x")],
        &a,
        &b,
    )
    .unwrap();
    assert_eq!(moves(&result), ["t.move"]);
    let consumers: Vec<u32> = result.slots.iter().map(|slot| slot.consumers).collect();
    assert!(
        consumers.contains(&2),
        "the destination slot is read twice: {consumers:?}"
    );
}

#[test]
fn slot_count_equals_peak_live() {
    let a = Mem {
        id: "a",
        spaces: vec![("m", SpaceKind::Shared, true)],
        ..Mem::default()
    };
    let b = Mem {
        id: "b",
        spaces: vec![("m", SpaceKind::Shared, true)],
        ..Mem::default()
    };
    let (nodes, edges) = line(&["t.a", "t.b", "t.a", "t.b"]);
    let result = run(nodes, edges, &a, &b).unwrap();
    assert_eq!(result.slots.len(), 2);
}

#[test]
fn wait_before_first_cross_region_reader() {
    let mut a = mover();
    a.may_pend = true;
    let b = own("b", "m.b");
    let (nodes, edges) = line(&["t.a", "t.b"]);
    let result = run(nodes, edges, &a, &b).unwrap();
    assert!(matches!(
        result.layout[..],
        [
            Item::Run { .. },
            Item::Wait { .. },
            Item::Move { .. },
            Item::Run { .. }
        ]
    ));
}

#[test]
fn a_unit_that_cannot_pend_gets_no_wait() {
    let (a, b) = (mover(), own("b", "m.b"));
    let (nodes, edges) = line(&["t.a", "t.b"]);
    let result = run(nodes, edges, &a, &b).unwrap();
    assert!(
        !result
            .layout
            .iter()
            .any(|item| matches!(item, Item::Wait { .. }))
    );
}

#[test]
fn every_node_covered_in_layout() {
    let (a, b) = (mover(), own("b", "m.b"));
    let (nodes, edges) = line(&["t.a", "t.a", "t.b"]);
    let result = run(nodes, edges, &a, &b).unwrap();
    let mut covered: Vec<u32> = result
        .layout
        .iter()
        .flat_map(|item| match item {
            Item::Run { covers, .. } => covers.clone(),
            _ => Vec::new(),
        })
        .collect();
    covered.sort_unstable();
    assert_eq!(covered, [0, 1, 2]);
}

#[test]
fn a_consumer_with_a_lower_index_still_runs_after_its_producer() {
    let (a, b) = (mover(), own("b", "m.b"));
    let result = run(
        vec![node("t.b"), node("t.a")],
        vec![edge(1, "out", 0, "x")],
        &a,
        &b,
    )
    .unwrap();
    let first = result.layout.iter().find_map(|item| match item {
        Item::Run { covers, .. } => Some(covers[0]),
        _ => None,
    });
    assert_eq!(first, Some(1));
}

#[test]
fn layout_matches_golden_bytes() {
    let mut a = mover();
    a.may_pend = true;
    let b = own("b", "m.b");
    let (nodes, edges) = line(&["t.a", "t.a", "t.b"]);
    let result = run(nodes, edges, &a, &b).unwrap();
    let mut bytes = Vec::new();
    encode(&result.plan, &mut bytes).unwrap();
    let text: String = bytes.iter().map(|byte| format!("{byte:02x}")).collect();
    let path = concat!(env!("CARGO_MANIFEST_DIR"), "/fixtures/b9_layout.hex");
    if std::env::var_os("BLESS").is_some() {
        std::fs::write(path, &text).unwrap();
    }
    assert_eq!(text, std::fs::read_to_string(path).unwrap());
}
