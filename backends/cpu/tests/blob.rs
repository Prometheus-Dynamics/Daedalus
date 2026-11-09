//! Graphs stored as blobs and run by the small runner.

#![cfg(feature = "blob")]

#[path = "shared/blob_fixture.rs"]
mod fixture;

use daedalus_abi::counting::{CountingAlloc, counted};
use daedalus_cpu::blob::{BlobBackend, Interpreter, Overflow, RunError, Source, StepDesc, encode};
use daedalus_graph::{EdgeDoc, EdgePolicyDoc, GraphBody, Lit, NodeDoc, NodeKindDoc, PortRefDoc};
use daedalus_planner::{Config, Form, PlanError, plan as make_plan};
use daedalus_registry::Registry;
use fixture::{
    BAD, BAD_NODE, FIRST, FIRST_NODE, GAIN_NODE, HEAVY_NODE, LIBRARY, OPT, OPT_NODE, SUM, SUM_NODE,
    edge, host, plan,
};

#[global_allocator]
static ALLOC: CountingAlloc = CountingAlloc;

type Running = Interpreter<2048>;

fn running(plan: &daedalus_cpu::blob::PlanDesc) -> Box<Running> {
    let mut running = Box::new(Running::new(&LIBRARY));
    running.load(&encode(plan).unwrap()).unwrap();
    running
}

fn wire(from: u32, port: &str, to: u32, input: &str) -> EdgeDoc {
    EdgeDoc {
        from: PortRefDoc {
            node: from,
            port: port.to_string(),
        },
        to: PortRefDoc {
            node: to,
            port: input.to_string(),
        },
        policy: EdgePolicyDoc::default(),
    }
}

fn bridge() -> NodeDoc {
    let mut bridge = NodeDoc::new("host");
    bridge.kind = NodeKindDoc::HostBridge;
    for port in ["in", "out"] {
        bridge
            .port_types
            .insert(port.to_string(), "u32".to_string());
    }
    bridge
}

fn graph() -> GraphBody {
    let mut gain = NodeDoc::new("b.gain");
    gain.consts.insert("by".to_string(), Lit::Int(3));
    GraphBody {
        nodes: vec![bridge(), gain, NodeDoc::new("b.sum")],
        edges: vec![
            wire(0, "in", 1, "a"),
            wire(1, "out", 2, "a"),
            wire(2, "out", 0, "out"),
        ],
        groups: Vec::new(),
    }
}

fn stored(graph: &GraphBody) -> Result<Vec<u8>, PlanError> {
    let mut registry = Registry::new();
    registry
        .register_all(&[
            &GAIN_NODE,
            &SUM_NODE,
            &FIRST_NODE,
            &OPT_NODE,
            &BAD_NODE,
            &HEAVY_NODE,
        ])
        .unwrap();
    let backend = BlobBackend { library: &LIBRARY };
    let config = Config {
        form: Form::Blob,
        ..Config::default()
    };
    make_plan(graph, &registry, &[&backend], &config)?.blob()
}

#[test]
fn a_planned_graph_loads_and_runs() {
    let bytes = stored(&graph()).unwrap();
    assert_eq!(bytes, stored(&graph()).unwrap(), "the bytes are repeatable");
    let mut running = Box::new(Running::new(&LIBRARY));
    running.load(&bytes).unwrap();
    let (input, output) = (running.input("in").unwrap(), running.output("out").unwrap());
    assert_eq!(running.input("nope"), None);
    for (value, expected) in [(1_u32, 3_u32), (2, 9), (10, 39)] {
        running.push(input, value).unwrap();
        running.tick().unwrap();
        assert_eq!(running.pop::<u32>(output), Ok(Some(expected)));
    }
    assert_eq!(running.pop::<u32>(output), Ok(None));
}

#[test]
fn nothing_runs_until_its_inputs_arrive() {
    let mut running = running(&plan());
    let output = running.output("out").unwrap();
    running.tick().unwrap();
    assert_eq!(running.pop::<u32>(output), Ok(None));
}

#[test]
fn a_port_of_another_type_is_refused() {
    let mut running = running(&plan());
    let (input, output) = (running.input("in").unwrap(), running.output("out").unwrap());
    assert_eq!(running.push(input, 1_i16), Err(RunError::Port { port: 0 }));
    assert_eq!(running.pop::<f32>(output), Err(RunError::Port { port: 0 }));
    assert_eq!(running.push(9, 1_u32), Err(RunError::Port { port: 9 }));
}

#[test]
fn a_full_queue_follows_its_policy() {
    let run = |overflow| {
        let mut desc = plan();
        desc.edges[0].overflow = overflow;
        let mut running = running(&desc);
        let (input, output) = (running.input("in").unwrap(), running.output("out").unwrap());
        running.push(input, 1_u32).unwrap();
        running.push(input, 2_u32).unwrap();
        let third = running.push(input, 4_u32);
        running.tick().unwrap();
        (third, running.pop::<u32>(output).unwrap())
    };
    assert_eq!(run(Overflow::DropOldest), (Ok(()), Some(6)));
    assert_eq!(run(Overflow::DropNewest), (Ok(()), Some(3)));
    assert_eq!(
        run(Overflow::Error),
        (Err(RunError::QueueFull { edge: 0 }), Some(3))
    );
}

#[test]
fn a_host_input_feeds_every_edge_it_names() {
    let mut desc = plan();
    desc.edges.push(edge());
    desc.host_inputs[0].edges.push(3);
    desc.host_outputs.push(host("copy", vec![3]));
    let mut running = running(&desc);
    let (input, copy) = (
        running.input("in").unwrap(),
        running.output("copy").unwrap(),
    );
    running.push(input, 5_u32).unwrap();
    assert_eq!(running.pop::<u32>(copy), Ok(Some(5)));
}

#[test]
fn a_failing_node_reports_its_step_and_code() {
    let mut desc = plan();
    desc.steps[1].entry = BAD;
    let mut running = running(&desc);
    let input = running.input("in").unwrap();
    running.push(input, 2_u32).unwrap();
    assert_eq!(running.tick(), Err(RunError::Node { node: 1, code: 6 }));
}

#[test]
fn an_empty_optional_input_still_runs_the_node() {
    let mut desc = plan();
    desc.steps = vec![StepDesc {
        entry: OPT,
        node: 0,
        inputs: vec![Source::Absent],
        outputs: vec![(0, 2)],
    }];
    desc.edges[0].capacity = 1;
    desc.host_inputs.clear();
    desc.edges.drain(..2);
    desc.steps[0].outputs = vec![(0, 0)];
    desc.host_outputs[0].edges = vec![0];
    desc.consts.clear();
    let mut running = running(&desc);
    let output = running.output("out").unwrap();
    running.tick().unwrap();
    assert_eq!(running.pop::<u32>(output), Ok(Some(77)));
}

/// `a` and `b` into one fan-in, whose first value goes out.
fn fan_in() -> daedalus_cpu::blob::PlanDesc {
    let mut desc = plan();
    desc.steps = vec![StepDesc {
        entry: FIRST,
        node: 0,
        inputs: vec![Source::Edge(0), Source::Edge(1)],
        outputs: vec![(0, 2)],
    }];
    desc.consts.clear();
    desc.host_inputs = vec![host("a", vec![0]), host("b", vec![1])];
    desc
}

#[test]
fn fan_in_takes_the_first_edge_with_a_value_and_drops_the_rest() {
    let mut running = running(&fan_in());
    let (a, b, output) = (
        running.input("a").unwrap(),
        running.input("b").unwrap(),
        running.output("out").unwrap(),
    );
    running.push(b, 8_u32).unwrap();
    running.tick().unwrap();
    assert_eq!(running.pop::<u32>(output), Ok(Some(8)));

    running.push(a, 1_u32).unwrap();
    running.push(a, 2_u32).unwrap();
    running.push(b, 9_u32).unwrap();
    running.tick().unwrap();
    assert_eq!(running.pop::<u32>(output), Ok(Some(1)));
    // Firing drains every edge, so nothing is left for a later tick.
    running.tick().unwrap();
    assert_eq!(running.pop::<u32>(output), Ok(None));
}

#[test]
fn rejects_blobs_the_fan_in_rules_forbid() {
    use daedalus_cpu::blob::LoadError;

    let mut no_fan = plan();
    no_fan.steps[1].entry = SUM;
    no_fan.steps[1].inputs.push(Source::Absent);
    let mut loading = Box::new(Running::new(&LIBRARY));
    assert_eq!(
        loading.load(&encode(&no_fan).unwrap()),
        Err(LoadError::Node { node: 1 })
    );

    let mut mistyped = fan_in();
    mistyped.edges[1].key ^= 1;
    assert_eq!(
        loading.load(&encode(&mistyped).unwrap()),
        Err(LoadError::Type { edge: 1 })
    );
}

#[test]
fn a_refused_load_leaves_the_running_plan_alone_and_a_new_one_starts_clean() {
    let mut running = running(&plan());
    let (input, output) = (running.input("in").unwrap(), running.output("out").unwrap());
    running.push(input, 1_u32).unwrap();
    running.tick().unwrap();
    assert_eq!(running.pop::<u32>(output), Ok(Some(3)));

    let mut broken = encode(&plan()).unwrap();
    let at = broken.len() / 2;
    broken[at] ^= 1;
    assert!(running.load(&broken).is_err());
    running.push(input, 1_u32).unwrap();
    running.tick().unwrap();
    assert_eq!(
        running.pop::<u32>(output),
        Ok(Some(6)),
        "the sum carried on"
    );

    running.load(&encode(&plan()).unwrap()).unwrap();
    running.push(input, 1_u32).unwrap();
    running.tick().unwrap();
    assert_eq!(running.pop::<u32>(output), Ok(Some(3)), "the sum restarted");
    assert!(running.used() > 0);
    assert!(running.plan_hash().is_some());
    running.unload();
    assert_eq!(running.plan_hash(), None);
}

#[test]
fn a_node_with_a_heap_value_is_refused_while_planning() {
    let mut desc = graph();
    desc.nodes[2] = NodeDoc::new("b.heavy");
    desc.edges.truncate(2);
    let error = stored(&desc).unwrap_err().to_string();
    assert!(error.contains("can't run in a blob"), "{error}");
}

#[test]
fn loading_and_ticking_allocate_nothing() {
    let bytes = encode(&plan()).unwrap();
    let mut running = Box::new(Running::new(&LIBRARY));
    let (_, allocations) = counted(|| {
        running.load(&bytes).unwrap();
        let (input, output) = (running.input("in").unwrap(), running.output("out").unwrap());
        for value in 0..50_u32 {
            running.push(input, value).unwrap();
            running.tick().unwrap();
            running.pop::<u32>(output).unwrap();
        }
    });
    assert_eq!(allocations, 0);
}
