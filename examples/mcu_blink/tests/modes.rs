//! The tunable and loaded modes of the blink graph, natively: both match the compiled graph (or
//! the node functions called by hand) for the same graph and parameters, never allocate per
//! tick, and the interpreter rejects invalid blobs while the running plan keeps running.

mod common;

use common::{Params, Reference, allocations, signal};
use daedalus_mcu::loaded::LoadError;
use daedalus_mcu::{McuError, ParamError, Scalar, Tunable};
use daedalus_mcu_blink::loaded::{ARENA, Interpreter, LIBRARY, PLAN_A, PLAN_B};
use daedalus_mcu_blink::{Graph, TunableGraph, tunable};
use daedalus_mcu_build::{Endpoint, LibraryManifest, McuPlan, PlanManifest, Value};

const GRAPH: &str = include_str!("../graph.json");
const LIBRARY_JSON: &str = include_str!(concat!(env!("OUT_DIR"), "/library.json"));
const PLAN_A_JSON: &str = include_str!(concat!(env!("OUT_DIR"), "/plan_a.json"));
const TUNABLE_JSON: &str = include_str!(concat!(env!("OUT_DIR"), "/tunable.json"));

/// The blink graph in loaded mode, by port name.
struct Loaded {
    interp: Box<Interpreter>,
    ports: [u16; 4],
}

impl Loaded {
    fn new(blob: &[u8]) -> Self {
        let mut loaded = Self {
            interp: Box::new(Interpreter::new(&LIBRARY)),
            ports: [0; 4],
        };
        loaded.load(blob);
        loaded
    }

    /// Load `blob` and look its ports up by name.
    fn load(&mut self, blob: &[u8]) {
        self.interp.load(blob).unwrap();
        let interp = &self.interp;
        self.ports = [
            interp.input("sample").unwrap(),
            interp.output("level").unwrap(),
            interp.output("led").unwrap(),
            interp.output("rises").unwrap(),
        ];
    }

    fn step(&mut self, raw: u16) -> (Option<f32>, Option<bool>, Option<u32>) {
        let [sample, level, led, rises] = self.ports;
        self.interp.push(sample, raw).unwrap();
        self.interp.tick().unwrap();
        (
            self.interp.pop(level).unwrap(),
            self.interp.pop(led).unwrap(),
            self.interp.pop(rises).unwrap(),
        )
    }
}

fn compiled_step(graph: &mut Graph, raw: u16) -> (Option<f32>, Option<bool>, Option<u32>) {
    graph.push_sample(raw).unwrap();
    graph.tick().unwrap();
    (graph.pop_level(), graph.pop_led(), graph.pop_rises())
}

fn tunable_step(graph: &mut TunableGraph, raw: u16) -> (Option<f32>, Option<bool>, Option<u32>) {
    graph.push_sample(raw).unwrap();
    graph.tick().unwrap();
    (graph.pop_level(), graph.pop_led(), graph.pop_rises())
}

fn expected(reference: &mut Reference, raw: u16) -> (Option<f32>, Option<bool>, Option<u32>) {
    let (level, led, rises) = reference.step(raw);
    (Some(level), Some(led), rises)
}

/// Tuned values (`alpha` 0.5, `on` 2.5, `period` 2) and their reference.
fn tuned() -> Reference {
    let mut reference = Reference::a();
    reference.params = Params {
        alpha: 0.5,
        on: 2.5,
        period: 2,
        ..Params::A
    };
    reference
}

#[test]
fn tunable_matches_compiled_then_follows_its_parameters() {
    let (mut compiled, mut graph) = (Graph::new(), TunableGraph::new());
    let before = allocations();
    for raw in signal() {
        assert_eq!(
            tunable_step(&mut graph, raw),
            compiled_step(&mut compiled, raw)
        );
    }
    assert_eq!(allocations(), before);
    assert_eq!(
        tunable::PARAM_NAMES,
        [
            "lowpass.alpha",
            "threshold.on",
            "threshold.off",
            "blink.period"
        ]
    );

    // Range and type checks, with the planner's rules for constants.
    assert_eq!(graph.set_lowpass_alpha(1.5), Err(ParamError::Range));
    assert_eq!(graph.set_lowpass_alpha(f32::NAN), Err(ParamError::Range));
    assert_eq!(graph.set_param(3, Scalar::F64(2.5)), Err(ParamError::Type));
    assert_eq!(graph.set_param(3, Scalar::I64(0)), Err(ParamError::Range));
    assert_eq!(
        graph.set_param(9, Scalar::U32(1)),
        Err(ParamError::UnknownId)
    );
    assert_eq!(graph.param(0), Some(Scalar::F32(0.25)));

    // Typed setters, generic ids, and messages encoded on the host from the manifest.
    let manifest = PlanManifest::from_json(TUNABLE_JSON).unwrap();
    let message = manifest
        .param_update("threshold.on", &Value::Float(2.5))
        .unwrap();
    assert_eq!(message, [0x01, 0x09, 0x00, 0x00, 0x20, 0x40]);
    let mut graph = TunableGraph::new();
    let before = allocations();
    graph.set_lowpass_alpha(0.5).unwrap();
    graph.set_param(3, Scalar::I64(2)).unwrap();
    graph.apply_update(&message).unwrap();
    assert_eq!(allocations(), before, "parameter updates allocate nothing");
    assert_eq!(graph.param(1), Some(Scalar::F32(2.5)));
    assert!(
        manifest
            .param_update("threshold.on", &Value::Float(9.0))
            .is_err()
    );
    assert_eq!(
        graph.apply_update(&message[..2]),
        Err(ParamError::Malformed)
    );

    let mut reference = tuned();
    for raw in signal() {
        assert_eq!(tunable_step(&mut graph, raw), expected(&mut reference, raw));
    }
}

#[test]
fn loaded_plan_matches_compiled_without_allocating() {
    assert!(Interpreter::required(&LIBRARY, PLAN_A).unwrap() <= ARENA);
    assert!(Interpreter::required(&LIBRARY, PLAN_B).unwrap() <= ARENA);
    let mut loaded = Loaded::new(PLAN_A);
    let mut compiled = Graph::new();
    let before = allocations();
    for _ in 0..2 {
        for raw in signal() {
            assert_eq!(loaded.step(raw), compiled_step(&mut compiled, raw));
        }
    }
    assert_eq!(
        allocations(),
        before,
        "loaded pushes, ticks and pops allocate nothing"
    );

    // `rises` is bounded to 8 (drop oldest), as in the compiled graph.
    let mut loaded = Loaded::new(PLAN_A);
    let [sample, _, _, rises] = loaded.ports;
    for _ in 0..10 {
        for raw in [4000, 0] {
            for _ in 0..8 {
                loaded.interp.push(sample, raw as u16).unwrap();
                loaded.interp.tick().unwrap();
            }
        }
    }
    let kept: Vec<u32> = std::iter::from_fn(|| loaded.interp.pop(rises).unwrap()).collect();
    assert_eq!(kept, (3..=10).collect::<Vec<_>>());
    // Host ports are typed.
    assert_eq!(
        loaded.interp.push(sample, 1.0_f32),
        Err(McuError::Port { port: sample })
    );
    assert_eq!(
        loaded.interp.pop::<u16>(rises),
        Err(McuError::Port { port: rises })
    );
}

#[test]
fn swaps_to_another_plan_at_run_time() {
    let mut loaded = Loaded::new(PLAN_A);
    let mut reference = Reference::a();
    for raw in signal().take(50) {
        assert_eq!(loaded.step(raw), expected(&mut reference, raw));
    }
    let plan_a = loaded.interp.plan_hash();
    let before = allocations();
    loaded.load(PLAN_B);
    assert_ne!(loaded.interp.plan_hash(), plan_a);
    let mut reference = Reference::b();
    for raw in signal() {
        assert_eq!(loaded.step(raw), expected(&mut reference, raw));
    }
    assert_eq!(allocations(), before);
}

#[test]
fn loaded_parameters_match_the_tunable_graph() {
    let manifest = PlanManifest::from_json(PLAN_A_JSON).unwrap();
    let id = |name: &str| manifest.params.iter().find(|p| p.name == name).unwrap().id;
    let mut loaded = Loaded::new(PLAN_A);
    let interp = &mut loaded.interp;
    assert_eq!(interp.param(id("lowpass.alpha")), Some(Scalar::F32(0.25)));
    interp
        .set_param(id("lowpass.alpha"), Scalar::F64(0.5))
        .unwrap();
    interp
        .set_param(id("blink.period"), Scalar::U32(2))
        .unwrap();
    let message = manifest
        .param_update("threshold.on", &Value::Float(2.5))
        .unwrap();
    interp.apply_update(&message).unwrap();
    assert_eq!(
        interp.set_param(id("threshold.off"), Scalar::F32(-1.0)),
        Err(ParamError::Range)
    );
    assert_eq!(
        interp.set_param(4, Scalar::U32(2)),
        Err(ParamError::UnknownId)
    );
    let mut reference = tuned();
    for raw in signal() {
        assert_eq!(loaded.step(raw), expected(&mut reference, raw));
    }
}

/// Plan A for `library`, with `edit` applied to the device plan first.
fn blob_a(library: &LibraryManifest, edit: impl FnOnce(&mut McuPlan)) -> Vec<u8> {
    let graph = daedalus_mcu_build::GraphDocument::from_json(GRAPH)
        .unwrap()
        .graph;
    let mut plan = daedalus_mcu_build::compile_loaded(graph, library, &Default::default()).unwrap();
    edit(&mut plan);
    plan.to_blob(library).unwrap()
}

#[test]
fn rejects_invalid_blobs_and_keeps_the_running_plan() {
    let library = LibraryManifest::from_json(LIBRARY_JSON).unwrap();
    assert_eq!(library.hash, LIBRARY.hash);
    assert_eq!(blob_a(&library, |_| {}), PLAN_A, "blobs are deterministic");
    let mut loaded = Loaded::new(PLAN_A);
    let mut reference = Reference::a();
    for raw in signal().take(30) {
        assert_eq!(loaded.step(raw), expected(&mut reference, raw));
    }
    let running = loaded.interp.plan_hash();

    let mut version = PLAN_A.to_vec();
    version[4] = 9;
    let mut other = library.clone();
    other.hash ^= 1;
    let edge = |plan: &McuPlan, label: &str| {
        let edges = plan.blob_edges();
        let graph_edge = plan.edges.iter().position(|e| e.label == label).unwrap();
        (
            graph_edge,
            edges.iter().position(|&e| e == graph_edge).unwrap() as u16,
        )
    };
    let mut type_edge = 0;
    // `scale.volts` (f32) into `blink.active` (bool).
    let mismatch = blob_a(&library, |plan| {
        let (graph_edge, blob_edge) = edge(plan, "threshold.active -> blink.active");
        plan.edges[graph_edge].from = Endpoint::Node { node: 0, output: 0 };
        type_edge = blob_edge;
    });
    let mut order_edge = 0;
    // `threshold.active` (node 2) into `lowpass.x` (node 1): a producer after its consumer.
    let order = blob_a(&library, |plan| {
        let (graph_edge, blob_edge) = edge(plan, "scale.volts -> lowpass.x");
        plan.edges[graph_edge].from = Endpoint::Node { node: 2, output: 0 };
        order_edge = blob_edge;
    });
    let cases: [(&[u8], LoadError); 7] = [
        (&version, LoadError::Version { found: 9 }),
        (&PLAN_A[1..], LoadError::Malformed),
        (&PLAN_A[..PLAN_A.len() - 1], LoadError::Malformed),
        (&[PLAN_A, &[0]].concat(), LoadError::Malformed),
        (
            &blob_a(&other, |_| {}),
            LoadError::Library {
                expected: LIBRARY.hash,
                found: other.hash,
            },
        ),
        (&mismatch, LoadError::Type { edge: type_edge }),
        (&order, LoadError::Edge { edge: order_edge }),
    ];
    for (blob, error) in cases {
        assert_eq!(loaded.interp.load(blob), Err(error));
        assert_eq!(loaded.interp.plan_hash(), running);
    }

    // Too large for a small arena.
    let needed = Interpreter::required(&LIBRARY, PLAN_A).unwrap();
    let mut small = daedalus_mcu::loaded::Interpreter::<64>::new(&LIBRARY);
    assert_eq!(
        small.load(PLAN_A),
        Err(LoadError::Arena {
            needed: needed as u32
        })
    );
    assert_eq!(small.tick(), Ok(()), "an empty interpreter ticks nothing");

    // The running plan carries on where it was.
    for raw in signal().skip(30) {
        assert_eq!(loaded.step(raw), expected(&mut reference, raw));
    }
}
