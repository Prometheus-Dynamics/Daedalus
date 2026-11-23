//! A host input's policy decides whether its value is context or work, whatever number of readers
//! its slot has: a held input keeps its value for every tick even with one reader, and a consumed
//! input with several readers is work a domain ticks for.

use std::sync::Arc;

use daedalus_abi::{OwnedStep, SlotState, Slots, Step, StepCx, StepEnd};
use daedalus_backend::{
    Backend, CompileCx, Compiled, MemorySpace, NodeRef, Offer, Refusal, Region, SpaceKind,
    StepUnit, Transfer, UnitInfo,
};
use daedalus_graph::{GraphBody, HostInputPolicy, NodeDoc, NodeKindDoc};
use daedalus_host::domain::Domain;
use daedalus_host::{Dir, LoadOptions, LoadedHost};
use daedalus_made::encode;
use daedalus_planner::{Config, Planned, plan};
use daedalus_planner_tests::{decl, edge, node, u32_ty};
use daedalus_registry::Registry;
use daedalus_runner::{put_typed, typed_view};
use daedalus_types::{SpaceIndex, TypeIndex};

/// Writes the sum of the inputs that hold a value.
struct Sum;

impl OwnedStep for Sum {
    fn run(&mut self, slots: &mut Slots, _: &mut StepCx) -> StepEnd {
        let last = slots.len() - 1;
        let mut sum = 0;
        for at in 0..last {
            let cell = *slots.cell(at).unwrap();
            if cell.state == SlotState::Full as u32 {
                sum += *typed_view::<u32>(&cell).unwrap();
            }
        }
        put_typed(
            slots.cell(last).unwrap(),
            TypeIndex(0),
            SpaceIndex(0),
            Arc::new(sum),
        );
        StepEnd::Done
    }
}

struct Test;

impl Backend for Test {
    fn id(&self) -> &str {
        "t"
    }

    fn memory(&self) -> Vec<MemorySpace> {
        vec![MemorySpace {
            id: "t.host".into(),
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
            .map(|n| NodeRef {
                node: n.node,
                body: n.body,
            })
            .collect()
    }

    fn compile(&self, region: &Region<'_>, _: &CompileCx<'_>) -> Result<Compiled, Refusal> {
        let units = region
            .nodes
            .iter()
            .map(|&node| StepUnit {
                step: Step::boxed(Sum),
                info: UnitInfo {
                    covers: vec![node],
                    reads: region
                        .inputs
                        .iter()
                        .filter(|port| port.node == node)
                        .copied()
                        .collect(),
                    writes: region
                        .outputs
                        .iter()
                        .filter(|port| port.node == node)
                        .copied()
                        .collect(),
                    absorbed: Vec::new(),
                    may_pend: false,
                    decodes: Vec::new(),
                },
            })
            .collect();
        Ok(Compiled::Steps(units))
    }
}

fn registry() -> Registry {
    let mut registry = Registry::new();
    decl("t.add")
        .input("in", u32_ty())
        .output("out", u32_ty())
        .body("t")
        .add(&mut registry);
    decl("t.pair")
        .input("in", u32_ty())
        .input("cal", u32_ty())
        .output("out", u32_ty())
        .body("t")
        .add(&mut registry);
    registry
}

fn bridge(ports: &[&str], held: &[&str]) -> NodeDoc {
    let mut doc = node("host");
    doc.kind = NodeKindDoc::HostBridge;
    for port in ports {
        doc.port_types.insert(port.to_string(), "u32".to_string());
    }
    for port in held {
        doc.host_inputs
            .insert(port.to_string(), HostInputPolicy::Held);
    }
    doc
}

fn labelled(id: &str, label: &str) -> NodeDoc {
    let mut doc = node(id);
    doc.label = Some(label.to_string());
    doc
}

/// `x` and the held `cal` into one reader each: `y = x + cal`.
fn calibrated(policy: HostInputPolicy) -> GraphBody {
    let mut host = bridge(&["x", "cal", "y"], &[]);
    host.host_inputs.insert("cal".to_string(), policy);
    GraphBody {
        nodes: vec![host, labelled("t.pair", "n")],
        edges: vec![
            edge(0, "x", 1, "in"),
            edge(0, "cal", 1, "cal"),
            edge(1, "out", 0, "y"),
        ],
        groups: Vec::new(),
    }
}

/// The consumed `x` into two readers, `y` and `z`.
fn fan_out() -> GraphBody {
    GraphBody {
        nodes: vec![
            bridge(&["x", "y", "z"], &[]),
            labelled("t.add", "a"),
            labelled("t.add", "b"),
        ],
        edges: vec![
            edge(0, "x", 1, "in"),
            edge(0, "x", 2, "in"),
            edge(1, "out", 0, "y"),
            edge(2, "out", 0, "z"),
        ],
        groups: Vec::new(),
    }
}

fn planned(graph: &GraphBody) -> Planned {
    plan(graph, &registry(), &[&Test], &Config::default()).unwrap()
}

fn load(graph: &GraphBody) -> LoadedHost {
    LoadedHost::load(planned(graph), LoadOptions::default()).unwrap()
}

/// The same plan written in the stored format, read back and loaded.
fn load_stored(graph: &GraphBody) -> LoadedHost {
    let bytes = encode(&planned(graph).made().unwrap()).unwrap();
    LoadedHost::load_made(
        &bytes,
        &registry(),
        &[&Test as &dyn Backend],
        LoadOptions::default(),
    )
    .unwrap()
}

fn take(host: &mut LoadedHost, port: &str) -> Option<u32> {
    let value = host.host().take_value(port).unwrap()?;
    value.typed::<u32>().copied()
}

fn feed(host: &mut LoadedHost, port: &str, value: u32) {
    host.host().feed_typed(port, Arc::new(value)).unwrap();
}

fn held_of(host: &mut LoadedHost, port: &str) -> bool {
    host.host()
        .port_infos()
        .find(|info| info.name == port && info.dir == Dir::Input)
        .unwrap()
        .held
}

#[test]
fn a_held_input_with_one_reader_keeps_its_value_for_later_ticks() {
    for mut host in [
        load(&calibrated(HostInputPolicy::Held)),
        load_stored(&calibrated(HostInputPolicy::Held)),
    ] {
        feed(&mut host, "cal", 10);
        for round in 1..4 {
            feed(&mut host, "x", round);
            host.step().unwrap();
            assert_eq!(take(&mut host, "y"), Some(round + 10), "round {round}");
        }
        assert!(held_of(&mut host, "cal"));
        assert!(!held_of(&mut host, "x"));
    }
}

#[test]
fn a_held_input_is_context_and_wakes_nothing() {
    let mut host = load(&calibrated(HostInputPolicy::Held));
    feed(&mut host, "x", 1);
    feed(&mut host, "cal", 10);
    let report = host.host().feed_typed("cal", Arc::new(20_u32)).unwrap();
    assert_eq!(report.woke, 0);
}

#[test]
fn a_consumed_input_with_one_reader_is_read_once() {
    let mut host = load(&calibrated(HostInputPolicy::Consume));
    feed(&mut host, "cal", 10);
    feed(&mut host, "x", 1);
    host.step().unwrap();
    assert_eq!(take(&mut host, "y"), Some(11));
    feed(&mut host, "x", 2);
    host.step().unwrap();
    // `cal` was consumed by the first run, so the second waits for a new one.
    assert_eq!(take(&mut host, "y"), None);
    assert!(!held_of(&mut host, "cal"));
}

#[test]
fn a_domain_ticks_a_graph_whose_consumed_input_has_several_readers() {
    for host in [load(&fan_out()), load_stored(&fan_out())] {
        let mut domain = Domain::new();
        domain.add("g", host).unwrap();
        assert!(!held_of(domain.graph("g").unwrap(), "x"));
        for round in 0..3 {
            domain.push_typed("g", "x", Arc::new(round)).unwrap();
            let tick = domain.tick();
            assert_eq!(
                (tick.ran, tick.idle, tick.failed),
                (1, 0, 0),
                "round {round}"
            );
            let graph = domain.graph("g").unwrap();
            assert_eq!(take(graph, "y"), Some(round));
            assert_eq!(take(graph, "z"), Some(round));
            // Both readers ran on the value: nothing is left to do until the next one.
            let tick = domain.tick();
            assert_eq!((tick.ran, tick.idle), (0, 1), "round {round}");
        }
    }
}

#[test]
fn a_domain_does_not_tick_for_a_held_input_alone() {
    let mut domain = Domain::new();
    domain
        .add("g", load(&calibrated(HostInputPolicy::Held)))
        .unwrap();
    domain.push_typed("g", "cal", Arc::new(10_u32)).unwrap();
    let tick = domain.tick();
    assert_eq!((tick.ran, tick.idle), (0, 1));
    domain.push_typed("g", "x", Arc::new(1_u32)).unwrap();
    let tick = domain.tick();
    assert_eq!(tick.ran, 1);
    assert_eq!(take(domain.graph("g").unwrap(), "y"), Some(11));
}
