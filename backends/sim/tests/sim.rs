use daedalus_abi::testing::{release, run_once};
use daedalus_abi::{Slot, Slots, StepCx, StepEnd};
use daedalus_backend::*;
use daedalus_sim::*;
use daedalus_types::{BackendIndex, NodeIndex, PortIndex};

fn ids(nodes: &[u32]) -> Vec<NodeIndex> {
    nodes.iter().map(|n| NodeIndex(*n)).collect()
}

fn output(node: u32) -> PortRef {
    PortRef {
        node: NodeIndex(node),
        port: PortIndex(0),
    }
}

fn compile(
    backend: &SimBackend,
    nodes: &[u32],
    names: &[&'static str],
) -> Result<Compiled, Refusal> {
    let list = ids(nodes);
    let outputs: Vec<PortRef> = nodes.iter().map(|n| output(*n)).collect();
    let bodies: Vec<NodeBody<'_>> = list
        .iter()
        .zip(names)
        .map(|(node, text)| NodeBody {
            node: *node,
            body: BodyIndex(0),
            inputs: 0,
            outputs: 0,
            id: "test",
            merges: true,
            fire_all: false,
            label: None,
            metadata: &daedalus_backend::NO_METADATA,
            code: &(),
            text,
        })
        .collect();
    let region = Region {
        backend: BackendIndex(0),
        nodes: &list,
        inputs: &[],
        outputs: &outputs,
        incoming: &[],
        placed: &[],
        rate: RateId(0),
    };
    let cx = CompileCx {
        types: &[],
        consts: &[],
        spaces: &[],
        bodies: &bodies,
        edges: &[],
        host_inputs: &[],
        host_outputs: &[],
        fifo_capacity: 4,
        merge_allowed: true,
        freeze_params: false,
        sharing: &[],
    };
    backend.compile(&region, &cx)
}

fn units(compiled: Compiled) -> Vec<StepUnit> {
    match compiled {
        Compiled::Steps(units) => units,
        other => panic!("expected steps: {other:?}"),
    }
}

fn run(unit: StepUnit) -> (StepEnd, StepCx) {
    let mut cells: [Slot; 0] = [];
    let mut slots = Slots::of(&mut cells);
    let mut cx = StepCx::new();
    let mut step = unit.step;
    let end = run_once(&mut step, &mut slots, &mut cx);
    release(step);
    (end, cx)
}

fn merging() -> SimBackend {
    SimBackend::new(SimConfig {
        merge: true,
        ..SimConfig::default()
    })
}

#[test]
fn merging_makes_one_step_covering_the_region() {
    let done = units(compile(&merging(), &[1, 2, 3], &["a", "b", "c"]).unwrap());
    assert_eq!(done.len(), 1);
    assert_eq!(done[0].info.covers, ids(&[1, 2, 3]));
    assert_eq!(done[0].info.writes.len(), 3);
}

#[test]
fn without_merging_each_node_is_a_step() {
    let done = units(compile(&SimBackend::default(), &[4, 5], &["a", "b"]).unwrap());
    let covers: Vec<_> = done.iter().map(|u| u.info.covers.clone()).collect();
    assert_eq!(covers, [ids(&[4]), ids(&[5])]);
}

#[test]
fn merging_is_off_when_the_planner_says_so() {
    let backend = merging();
    let nodes = ids(&[6, 7]);
    let region = Region {
        backend: BackendIndex(0),
        nodes: &nodes,
        inputs: &[],
        outputs: &[],
        incoming: &[],
        placed: &[],
        rate: RateId(0),
    };
    let cx = CompileCx {
        types: &[],
        consts: &[],
        spaces: &[],
        bodies: &[],
        edges: &[],
        host_inputs: &[],
        host_outputs: &[],
        fifo_capacity: 4,
        merge_allowed: false,
        freeze_params: false,
        sharing: &[],
    };
    assert_eq!(units(backend.compile(&region, &cx).unwrap()).len(), 2);
}

#[test]
fn a_refusal_lists_exactly_the_refused_nodes() {
    let backend = SimBackend::new(SimConfig {
        refuse: &["b", "d"],
        ..SimConfig::default()
    });
    let refusal = compile(&backend, &[8, 9, 10, 11], &["a", "b", "c", "d"]).unwrap_err();
    assert_eq!(refusal.nodes, ids(&[9, 11]));
    assert_eq!(refusal.reason, "sim refuses");
}

#[test]
fn can_take_keeps_the_offer_order_and_skips_unavailable_bodies() {
    let offered = |node, needs: &'static [&'static str]| OfferNode {
        node: NodeIndex(node),
        body: BodyIndex(0),
        inputs: &[],
        outputs: &[],
        needs,
    };
    let nodes = [offered(3, &[]), offered(1, &[UNAVAILABLE]), offered(2, &[])];
    let offer = Offer { nodes: &nodes };
    let backend = SimBackend::default();
    let taken: Vec<u32> = backend.can_take(&offer).iter().map(|n| n.node.0).collect();
    assert_eq!(taken, [3, 2]);
    assert_eq!(backend.can_take(&offer), backend.can_take(&offer));
}

#[test]
fn the_transfer_table_has_both_directions_as_copies() {
    let table = SimBackend::default().transfers();
    let row: Vec<_> = table
        .iter()
        .map(|t| (t.id, &*t.from, &*t.to, t.cost))
        .collect();
    assert_eq!(
        row,
        [
            (TO_DEVICE, HOST, DEVICE, TransferCost::Copy),
            (TO_HOST, DEVICE, HOST, TransferCost::Copy),
        ]
    );
    let off = SimBackend::new(SimConfig {
        no_transfers: true,
        ..SimConfig::default()
    });
    assert!(
        off.transfers().is_empty(),
        "two spaces with no way between them"
    );
}

#[test]
fn a_fault_names_the_node_and_the_reason() {
    let backend = SimBackend::new(SimConfig {
        faults: &[12],
        ..SimConfig::default()
    });
    let mut done = units(compile(&backend, &[12], &["a"]).unwrap());
    let (end, cx) = run(done.remove(0));
    assert!(matches!(end, StepEnd::Fault));
    assert_eq!(cx.at, 12);
    assert_eq!(cx.reason(), "sim fault");
}

#[test]
fn a_fault_in_a_merged_step_stops_at_that_node() {
    let backend = SimBackend::new(SimConfig {
        merge: true,
        faults: &[14],
        ..SimConfig::default()
    });
    let mut done = units(compile(&backend, &[13, 14, 15], &["a", "b", "c"]).unwrap());
    let (end, cx) = run(done.remove(0));
    assert!(matches!(end, StepEnd::Fault));
    assert_eq!(cx.at, 14);
    assert_eq!((ran(13), ran(14), ran(15)), (1, 1, 0));
}

#[test]
fn counters_show_which_steps_ran() {
    let mut done = units(compile(&SimBackend::default(), &[20, 21], &["a", "b"]).unwrap());
    assert!(matches!(run(done.remove(0)).0, StepEnd::Done));
    assert_eq!((ran(20), ran(21)), (1, 0));
}

#[test]
fn blob_and_source_come_back_when_asked_for() {
    let form = |form| {
        SimBackend::new(SimConfig {
            form,
            ..SimConfig::default()
        })
    };
    match compile(&form(SimForm::Blob), &[30, 31], &["a", "b"]).unwrap() {
        Compiled::Blob { bytes, units } => {
            assert_eq!(bytes, [30, 0, 0, 0, 31, 0, 0, 0]);
            assert_eq!(units.len(), 2);
        }
        other => panic!("{other:?}"),
    }
    match compile(&form(SimForm::Source), &[30, 31], &["a", "b"]).unwrap() {
        Compiled::Source { code, units } => {
            assert!(code.text.contains("30 31"));
            assert_eq!(units.len(), 2);
        }
        other => panic!("{other:?}"),
    }
}

#[test]
fn an_import_is_absorbed_by_the_step_that_reads_it() {
    let nodes = ids(&[40, 41]);
    let incoming = [
        Incoming {
            input: output(41),
            from: POOL,
            to: DEVICE,
            transfer: IMPORT,
        },
        Incoming {
            input: output(40),
            from: HOST,
            to: DEVICE,
            transfer: TO_DEVICE,
        },
    ];
    let region = Region {
        backend: BackendIndex(0),
        nodes: &nodes,
        inputs: &[],
        outputs: &[],
        incoming: &incoming,
        placed: &[],
        rate: RateId(0),
    };
    let cx = CompileCx {
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
        sharing: &[],
    };
    let done = units(SimBackend::default().compile(&region, &cx).unwrap());
    assert!(done[0].info.absorbed.is_empty(), "a copy is not absorbed");
    assert_eq!(done[1].info.absorbed, [0]);
    let off = SimBackend::new(SimConfig {
        absorb: false,
        ..SimConfig::default()
    });
    assert!(
        units(off.compile(&region, &cx).unwrap())[1]
            .info
            .absorbed
            .is_empty()
    );
}

#[test]
fn the_memory_only_backend_has_memory_and_runs_nothing() {
    let memory = SimMemory;
    assert_eq!(memory.memory()[0].id, POOL);
    let costs: Vec<_> = memory.transfers().iter().map(|t| (t.id, t.cost)).collect();
    assert_eq!(
        costs,
        [
            (IMPORT, TransferCost::Import),
            (POOL_TO_HOST, TransferCost::Handoff)
        ]
    );
    assert!(memory.can_take(&Offer { nodes: &[] }).is_empty());
}

#[test]
fn the_contract_breaking_knobs_break_it() {
    let dropped = SimBackend::new(SimConfig {
        drop_output: true,
        ..SimConfig::default()
    });
    let done = units(compile(&dropped, &[50, 51], &["a", "b"]).unwrap());
    assert!(done[1].info.writes.is_empty());
    assert_eq!(done[0].info.writes.len(), 1);
    let bare = SimBackend::new(SimConfig {
        omit_covers: true,
        ..SimConfig::default()
    });
    assert!(
        units(compile(&bare, &[52], &["a"]).unwrap())[0]
            .info
            .covers
            .is_empty()
    );
}
