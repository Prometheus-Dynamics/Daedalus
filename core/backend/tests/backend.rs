use std::borrow::Cow;

use daedalus_abi::{Slots, Step, StepCx, StepEnd, StepWord};
use daedalus_backend::*;
use daedalus_types::{BackendIndex, NodeIndex, TypeKey};

struct Pass;
impl StepWord for Pass {
    fn run(_: usize, _: &mut Slots, _: &mut StepCx) -> StepEnd {
        StepEnd::Done
    }
}

fn make_pass(_: &CompileCx<'_>) -> Result<Step, Refusal> {
    Ok(Step::of::<Pass>(0))
}

fn transfer(id: &'static str, key: Option<TypeKey>, cost: TransferCost) -> Transfer {
    Transfer {
        id,
        key,
        from: Cow::Borrowed("a"),
        to: Cow::Borrowed("b"),
        cost,
        make: make_pass,
    }
}

struct Fake;

impl Backend for Fake {
    fn id(&self) -> &str {
        "fake"
    }
    fn memory(&self) -> Vec<MemorySpace> {
        vec![MemorySpace {
            id: Cow::Borrowed("fake.host"),
            kind: SpaceKind::Host,
            readable: true,
        }]
    }
    fn transfers(&self) -> Vec<Transfer> {
        vec![transfer("t", None, TransferCost::Copy)]
    }
    fn can_take(&self, offer: &Offer<'_>) -> Vec<NodeRef> {
        offer
            .nodes
            .iter()
            .map(|n| NodeRef {
                node: n.node,
                body: n.body,
            })
            .collect()
    }
    fn compile(&self, region: &Region<'_>, _: &CompileCx<'_>) -> Result<Compiled, Refusal> {
        Ok(Compiled::Steps(vec![StepUnit {
            step: Step::of::<Pass>(0),
            info: UnitInfo {
                covers: region.nodes.to_vec(),
                ..UnitInfo::default()
            },
        }]))
    }
}

fn cx<'a>() -> CompileCx<'a> {
    CompileCx {
        types: &[],
        consts: &[],
        spaces: &[],
        bodies: &[],
        edges: &[],
        host_inputs: &[],
        host_outputs: &[],
        fifo_capacity: 4,
        merge_allowed: true,
        freeze_params: false,
    }
}

#[test]
fn a_backend_is_object_safe() {
    let backend: Box<dyn Backend> = Box::new(Fake);
    assert_eq!(backend.id(), "fake");
    assert_eq!(backend.memory()[0].id, "fake.host");
    let node = OfferNode {
        node: NodeIndex(4),
        body: BodyIndex(1),
        inputs: &[],
        outputs: &[],
        needs: &[],
    };
    let taken = backend.can_take(&Offer { nodes: &[node] });
    assert_eq!(
        taken,
        [NodeRef {
            node: NodeIndex(4),
            body: BodyIndex(1)
        }]
    );
    let region = Region {
        backend: BackendIndex(0),
        nodes: &[NodeIndex(4)],
        inputs: &[],
        outputs: &[],
        incoming: &[],
        rate: RateId(0),
    };
    let Compiled::Steps(units) = backend.compile(&region, &cx()).unwrap() else {
        panic!("expected steps");
    };
    assert_eq!(units[0].info.covers, [NodeIndex(4)]);
}

#[test]
fn compiled_forms_carry_their_tables() {
    let units = vec![UnitInfo {
        covers: vec![NodeIndex(1)],
        ..UnitInfo::default()
    }];
    let blob = Compiled::Blob {
        bytes: vec![1],
        units: units.clone(),
    };
    let source = Compiled::Source {
        code: GeneratedCode {
            file: "graph.rs".into(),
            text: String::new(),
        },
        units,
    };
    assert!(matches!(blob, Compiled::Blob { units, .. } if units.len() == 1));
    assert!(matches!(source, Compiled::Source { code, .. } if code.file == "graph.rs"));
}

#[test]
fn refusals_are_bounded_and_can_cover_a_region() {
    let long = "é".repeat(200);
    let refusal = Refusal::new(vec![NodeIndex(2)], &long);
    assert_eq!(refusal.reason.len(), 256);
    assert_eq!(refusal.nodes, [NodeIndex(2)]);
    assert!(Refusal::whole_region("no").nodes.is_empty());
}

#[test]
fn transfer_lookup_prefers_the_exact_type_then_the_cheapest() {
    let key = TypeKey::new("acme.buffer");
    let other = TypeKey::new("acme.other");
    let table = [
        transfer("any-copy", None, TransferCost::Copy),
        transfer("any-import", None, TransferCost::Import),
        transfer("mine-copy", Some(key.clone()), TransferCost::Copy),
    ];
    assert_eq!(
        find_transfer(&table, &key, "a", "b").unwrap().id,
        "mine-copy"
    );
    assert_eq!(
        find_transfer(&table, &other, "a", "b").unwrap().id,
        "any-import"
    );
    assert!(find_transfer(&table, &key, "b", "a").is_none());
}

#[test]
fn cost_classes_are_ordered() {
    assert!(TransferCost::Handoff < TransferCost::Import);
    assert!(TransferCost::Import < TransferCost::Copy);
}

#[test]
fn a_transfer_makes_its_step() {
    let t = transfer("t", None, TransferCost::Handoff);
    assert!((t.make)(&cx()).is_ok());
}
