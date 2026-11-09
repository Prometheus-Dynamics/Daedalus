//! The host's feed, step and take: round trips, refusals that name their port, idle steps, and
//! allocation-free steady state.

use std::sync::{Arc, Mutex};

use daedalus_abi::counting::{CountingAlloc, counted};
use daedalus_abi::{Slots, Step, StepCx, StepEnd, StepWord};
use daedalus_host::{Dir, Host, HostError, PortSpec, Ports};
use daedalus_plan::Span;
use daedalus_runner::{Memory, Plan, Runner, SlotKind, SlotSpec, StepMeta};
use daedalus_types::{SpaceIndex, TypeIndex, TypeKey};
use daedalus_value::{Owner, Value};

#[global_allocator]
static ALLOC: CountingAlloc = CountingAlloc;

const TY_A: TypeIndex = TypeIndex(0);
const TY_B: TypeIndex = TypeIndex(1);
const SP: SpaceIndex = SpaceIndex(0);

/// Moves input slot 0 to output slot 1.
struct Pass;

impl StepWord for Pass {
    fn run(_: usize, slots: &mut Slots, _: &mut StepCx) -> StepEnd {
        let value = *slots.cell(0).unwrap();
        *slots.cell(1).unwrap() = value;
        slots.cell(0).unwrap().state = daedalus_abi::SlotState::Moved as u32;
        StepEnd::Done
    }
}

/// Owns one graph: slot 0 in, slot 1 out, and a single step that passes values through.
struct Graph {
    steps: Vec<Step>,
    meta: Vec<StepMeta>,
    table: Vec<u32>,
    specs: Vec<SlotSpec>,
    cells: Vec<daedalus_abi::Slot>,
    queues: Vec<daedalus_runner::Queue>,
    storage: Vec<daedalus_abi::Slot>,
    dirty: Vec<u32>,
    carry: Vec<u32>,
    always: Vec<u32>,
    pending: Vec<daedalus_runner::Pending>,
    health: Vec<daedalus_runner::StepHealth>,
    activity: Vec<u32>,
    inline: Vec<[u8; 16]>,
}

impl Graph {
    fn new() -> Graph {
        // Step 0 reads slot 0 and writes slot 1, and wakes nothing.
        let table = vec![0, 1];
        let span = |start, len| Span { start, len };
        let meta = vec![StepMeta {
            wake: span(0, 0),
            inputs: span(0, 1),
            outputs: span(1, 1),
            always: false,
            may_pend: false,
            recovery: daedalus_runner::Recovery::Fail,
            fire: daedalus_runner::Fire::Any,
            required: Span { start: 0, len: 0 },
        }];
        let specs = vec![
            SlotSpec {
                cap: 2,
                kind: SlotKind::Fifo,
            },
            SlotSpec {
                cap: 2,
                kind: SlotKind::Fifo,
            },
        ];
        let storage_len = 4;
        Graph {
            steps: vec![Step::of::<Pass>(0)],
            meta,
            table,
            specs,
            cells: vec![daedalus_abi::Slot::EMPTY; 2],
            queues: vec![daedalus_runner::Queue::default(); 2],
            storage: vec![daedalus_abi::Slot::EMPTY; storage_len],
            dirty: vec![0; 1],
            carry: vec![0; 1],
            always: vec![0; 1],
            pending: Vec::new(),
            health: vec![daedalus_runner::StepHealth::default(); 1],
            activity: vec![0; 1],
            inline: vec![[0; 16]; storage_len],
        }
    }

    fn runner(&mut self) -> Runner<'_> {
        Runner::load(
            Plan {
                steps: &mut self.steps,
                meta: &self.meta,
                table: &self.table,
                slots: &self.specs,
            },
            Memory {
                cells: &mut self.cells,
                queues: &mut self.queues,
                storage: &mut self.storage,
                dirty: &mut self.dirty,
                carry: &mut self.carry,
                always: &mut self.always,
                pending: &mut self.pending,
                health: &mut self.health,
                activity: &mut self.activity,
                inline: &mut self.inline,
            },
        )
        .unwrap()
    }
}

static PORTS: [PortSpec<'static>; 2] = [
    PortSpec {
        name: "in",
        dir: Dir::Input,
        slot: 0,
        ty: TY_A,
        space: SP,
        key: &KEY_A,
        wake: &[0],
    },
    PortSpec {
        name: "out",
        dir: Dir::Output,
        slot: 1,
        ty: TY_A,
        space: SP,
        key: &KEY_A,
        wake: &[],
    },
];
static KEY_A: TypeKey = TypeKey::new("demo.a");
static TYPES: [TypeKey; 2] = [TypeKey::new("demo.a"), TypeKey::new("demo.b")];

fn bytes(ty: TypeIndex, text: &'static [u8]) -> Value {
    Value {
        ty,
        space: SP,
        owner: Owner::Static(text),
    }
}

fn text_of(value: &Value) -> &[u8] {
    value.bytes().unwrap()
}

#[test]
fn three_values_go_through_in_order() {
    let mut graph = Graph::new();
    let mut host = Host::new(graph.runner(), Ports::new(&PORTS).unwrap(), &TYPES);
    let words: [&'static [u8]; 3] = [b"one", b"two", b"three"];
    for text in words {
        host.feed("in", bytes(TY_A, text)).unwrap();
        host.step().unwrap();
        let out = host.take_value("out").unwrap().expect("a value per step");
        assert_eq!(text_of(&out), text);
    }
}

#[test]
fn a_wrong_typed_feed_names_the_port_and_both_keys() {
    let mut graph = Graph::new();
    let mut host = Host::new(graph.runner(), Ports::new(&PORTS).unwrap(), &TYPES);
    let error = host.feed("in", bytes(TY_B, b"x")).unwrap_err();
    let message = error.to_string();
    assert!(message.contains("in"), "{message}");
    assert!(message.contains("demo.a"), "{message}");
    assert!(message.contains("demo.b"), "{message}");
    assert!(matches!(error, HostError::TypeMismatch { .. }));
}

#[test]
fn an_unknown_port_names_itself() {
    let mut graph = Graph::new();
    let mut host = Host::new(graph.runner(), Ports::new(&PORTS).unwrap(), &TYPES);
    let error = host.feed("nope", bytes(TY_A, b"x")).unwrap_err();
    assert_eq!(error.to_string(), "no port named nope");
}

#[test]
fn an_idle_step_calls_no_body() {
    let mut graph = Graph::new();
    let mut host = Host::new(graph.runner(), Ports::new(&PORTS).unwrap(), &TYPES);
    let report = host.step().unwrap();
    assert!(report.idle);
    assert_eq!(report.step_calls, 0);
}

#[test]
fn a_steady_feed_step_take_loop_allocates_nothing_after_warm_up() {
    let mut graph = Graph::new();
    let mut host = Host::new(graph.runner(), Ports::new(&PORTS).unwrap(), &TYPES);
    let text = bytes(TY_A, b"warm");
    host.feed("in", text).unwrap();
    host.step().unwrap();
    drop(host.take_value("out").unwrap());

    let ((), allocations) = counted(|| {
        for _ in 0..1000 {
            host.feed("in", bytes(TY_A, b"steady")).unwrap();
            host.step().unwrap();
            drop(host.take_value("out").unwrap());
        }
    });
    assert_eq!(allocations, 0);
}

/// A payload that is not `Copy`: it records its id in a log when it is dropped.
#[derive(Clone)]
struct Tracked {
    id: u32,
    log: Arc<Mutex<Vec<u32>>>,
}

impl Drop for Tracked {
    fn drop(&mut self) {
        self.log.lock().unwrap().push(self.id);
    }
}

#[test]
fn a_typed_payload_is_taken_back_and_dropped_once() {
    let log = Arc::new(Mutex::new(Vec::new()));
    let mut graph = Graph::new();
    let mut host = Host::new(graph.runner(), Ports::new(&PORTS).unwrap(), &TYPES);
    host.feed_typed(
        "in",
        Arc::new(Tracked {
            id: 1,
            log: log.clone(),
        }),
    )
    .unwrap();
    host.step().unwrap();
    let back: Tracked = host.take("out").unwrap().expect("one value");
    assert_eq!(back.id, 1);
    drop(back);
    assert_eq!(*log.lock().unwrap(), vec![1]);
}

#[test]
fn a_payload_left_in_a_queue_is_dropped_once_at_shutdown() {
    let log = Arc::new(Mutex::new(Vec::new()));
    let mut graph = Graph::new();
    {
        let mut host = Host::new(graph.runner(), Ports::new(&PORTS).unwrap(), &TYPES);
        host.feed_typed(
            "in",
            Arc::new(Tracked {
                id: 2,
                log: log.clone(),
            }),
        )
        .unwrap();
        assert!(log.lock().unwrap().is_empty());
    }
    assert_eq!(*log.lock().unwrap(), vec![2]);
}

#[test]
fn a_bad_port_table_is_refused() {
    let unsorted = [
        PortSpec {
            name: "b",
            dir: Dir::Input,
            slot: 0,
            ty: TY_A,
            space: SP,
            key: &KEY_A,
            wake: &[],
        },
        PortSpec {
            name: "a",
            dir: Dir::Input,
            slot: 0,
            ty: TY_A,
            space: SP,
            key: &KEY_A,
            wake: &[],
        },
    ];
    assert!(Ports::new(&unsorted).is_err());
}

#[test]
fn a_feed_to_a_held_input_replaces_it_and_wakes_nothing() {
    let mut graph = Graph::new();
    graph.specs[0] = SlotSpec {
        cap: 1,
        kind: SlotKind::Held,
    };
    let mut host = Host::new(graph.runner(), Ports::new(&PORTS).unwrap(), &TYPES);
    assert!(host.is_held("in").unwrap());
    assert_eq!(host.feed("in", bytes(TY_A, b"first")).unwrap().woke, 0);
    // A second feed replaces the value even though the slot holds only one.
    assert_eq!(host.feed("in", bytes(TY_A, b"second")).unwrap().woke, 0);
    assert_eq!(host.step().unwrap().step_calls, 0);
    host.clear_input("in").unwrap();
    assert_eq!(host.runner().queue(0).unwrap().len, 0);
}
