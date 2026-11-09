//! What a unit names in `UnitInfo` becomes its cells, in that order.

use std::borrow::Cow;

use daedalus_abi::{NO_SLOT, Slots, Step, StepCx, StepEnd, StepWord};
use daedalus_backend::{
    Backend, CompileCx, Compiled, MemorySpace, NodeRef, Offer, PortRef, Refusal, Region, SpaceKind,
    StepUnit, Transfer, UnitInfo,
};
use daedalus_planner::stitch::{Item, Stitched};
use daedalus_planner_tests::{decl, edge, node, stitched, u8_ty};
use daedalus_registry::Registry;
use daedalus_types::{NodeIndex, PortIndex};

struct Nothing;

impl StepWord for Nothing {
    fn run(_: usize, _: &mut Slots, _: &mut StepCx) -> StepEnd {
        StepEnd::Done
    }
}

/// One step per node, naming every declared port. `ports[node]` is (inputs, outputs).
struct PerNode {
    ports: Vec<(u16, u16)>,
}

impl Backend for PerNode {
    fn id(&self) -> &str {
        "steps"
    }

    fn memory(&self) -> Vec<MemorySpace> {
        vec![MemorySpace {
            id: Cow::Borrowed("m.steps"),
            kind: SpaceKind::Host,
            readable: true,
        }]
    }

    fn transfers(&self) -> Vec<Transfer> {
        Vec::new()
    }

    fn can_take(&self, offer: &Offer<'_>) -> Vec<NodeRef> {
        offer
            .nodes
            .iter()
            .map(|node| NodeRef {
                node: node.node,
                body: node.body,
            })
            .collect()
    }

    fn compile(&self, region: &Region<'_>, _: &CompileCx<'_>) -> Result<Compiled, Refusal> {
        let unit = |node: &NodeIndex| {
            let (inputs, outputs) = self.ports[node.0 as usize];
            let ports = |count: u16| -> Vec<PortRef> {
                (0..count)
                    .map(|port| PortRef {
                        node: *node,
                        port: PortIndex(port),
                    })
                    .collect()
            };
            StepUnit {
                step: Step::of::<Nothing>(0),
                info: UnitInfo {
                    covers: vec![*node],
                    reads: ports(inputs),
                    writes: ports(outputs),
                    absorbed: Vec::new(),
                    may_pend: false,
                },
            }
        };
        Ok(Compiled::Steps(region.nodes.iter().map(unit).collect()))
    }
}

fn registry() -> Registry {
    let mut registry = Registry::new();
    decl("t.source")
        .output("out", u8_ty())
        .body("steps")
        .add(&mut registry);
    decl("t.join")
        .optional("x", u8_ty())
        .optional("y", u8_ty())
        .output("unused", u8_ty())
        .output("out", u8_ty())
        .body("steps")
        .add(&mut registry);
    decl("t.sink")
        .optional("a", u8_ty())
        .body("steps")
        .add(&mut registry);
    registry
}

fn run_of(result: &Stitched, node: u32) -> (Vec<u32>, Vec<u32>) {
    result
        .layout
        .iter()
        .find_map(|item| match item {
            Item::Run {
                covers,
                reads,
                writes,
                ..
            } if covers == &[node] => Some((reads.clone(), writes.clone())),
            _ => None,
        })
        .expect("the node runs")
}

#[test]
fn steps_in_one_region_share_slots_and_unwired_ports_have_none() {
    let backend = PerNode {
        ports: vec![(0, 1), (2, 2), (1, 0)],
    };
    let nodes = vec![node("t.source"), node("t.join"), node("t.sink")];
    let edges = vec![edge(0, "out", 1, "y"), edge(1, "out", 2, "a")];
    let (_, result) = stitched(&registry(), nodes, edges, &[&backend]).unwrap();

    let (reads, writes) = run_of(&result, 0);
    assert!(reads.is_empty());
    let [first] = writes[..] else {
        panic!("one output: {writes:?}");
    };
    assert_ne!(first, NO_SLOT, "an internal edge has a slot");

    let (reads, writes) = run_of(&result, 1);
    assert_eq!(
        reads,
        vec![NO_SLOT, first],
        "x has no edge, y reads the source"
    );
    assert_eq!(writes[0], NO_SLOT, "`unused` has no edge");
    assert_ne!(writes[1], NO_SLOT);

    assert_eq!(run_of(&result, 2), (vec![writes[1]], Vec::new()));
    assert!(
        !result
            .layout
            .iter()
            .any(|item| matches!(item, Item::Move { .. })),
        "an edge inside a region moves nothing"
    );
}
