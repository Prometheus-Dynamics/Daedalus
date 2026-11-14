//! A node made at run time from a plain closure plans, runs, and passes values to and from
//! `#[node]` functions.

use std::sync::{Arc, Mutex};

use daedalus_abi::{Slot, Slots, StepCx, StepEnd};
use daedalus_backend::Compiled;
use daedalus_cpu::closure::{ClosureError, closure_node};
use daedalus_cpu::{CpuBackend, Fault};
use daedalus_graph::{EdgeDoc, EdgePolicyDoc, GraphBody, NodeDoc, PortRefDoc};
use daedalus_macros::node;
use daedalus_planner::stitch::Item;
use daedalus_planner::{Config, plan};
use daedalus_registry::Registry;

/// A node written the usual way, between two made from closures.
#[node(id = "fixed.double", inputs(a), outputs(out))]
fn double(a: u32) -> u32 {
    a * 2
}

/// Another, with a value too large to sit in a cell. It borrows its input: this test's stand-in
/// for the runner copies cells as they are, so nothing here may take a value out of one.
#[node(id = "fixed.shout", inputs(text), outputs(out))]
#[allow(clippy::ptr_arg)] // the port's type is `String`, which `&str` would not say
fn shout(text: &String) -> String {
    text.to_uppercase()
}

fn wire(from: u32, from_port: &str, to: u32, to_port: &str) -> EdgeDoc {
    EdgeDoc {
        from: PortRefDoc {
            node: from,
            port: from_port.to_string(),
        },
        to: PortRefDoc {
            node: to,
            port: to_port.to_string(),
        },
        policy: EdgePolicyDoc::default(),
    }
}

/// Plans `nodes` wired by `edges` and runs each step once, in order, as the runner would. Gives
/// how each step ended, with its fault's code and reason.
fn run(registry: &Registry, nodes: &[&str], edges: Vec<EdgeDoc>) -> Vec<(bool, u32, String)> {
    let graph = GraphBody {
        nodes: nodes.iter().copied().map(NodeDoc::new).collect(),
        edges,
        groups: Vec::new(),
    };
    let config = Config {
        merge: false,
        ..Config::default()
    };
    let mut planned = plan(&graph, registry, &[&CpuBackend], &config).expect("the graph plans");
    let mut cells = vec![Slot::EMPTY; planned.stitched.slots.len()];
    let mut ends = Vec::new();
    for item in planned.stitched.layout.clone() {
        let Item::Run {
            covers,
            reads,
            writes,
            ..
        } = item
        else {
            continue;
        };
        let mut local: Vec<Slot> = reads
            .iter()
            .map(|&slot| cells[slot as usize])
            .chain(writes.iter().map(|_| Slot::EMPTY))
            .collect();
        let unit = planned
            .regions
            .regions
            .iter_mut()
            .find_map(|region| match &mut region.compiled {
                Compiled::Steps(units) => units
                    .iter_mut()
                    .find(|unit| unit.info.covers[0].0 == covers[0]),
                _ => None,
            })
            .expect("a step for the node");
        let mut slots = Slots::of(&mut local);
        let mut cx = StepCx::new();
        let end = daedalus_abi::testing::run_once(&mut unit.step, &mut slots, &mut cx);
        ends.push((
            matches!(end, StepEnd::Done),
            cx.code,
            cx.reason().to_owned(),
        ));
        for (at, &slot) in writes.iter().enumerate() {
            if slot != daedalus_abi::NO_SLOT {
                cells[slot as usize] = local[reads.len() + at];
            }
        }
    }
    for cell in cells {
        daedalus_runner::drop_owned(cell);
    }
    ends
}

#[test]
fn closures_and_node_functions_pass_small_and_large_values_to_each_other() {
    // What the closures hold is only known now.
    let (start, limit) = (21u32, 40u32);
    let seen = Arc::new(Mutex::new(Vec::new()));
    let sink = Arc::clone(&seen);
    let mut registry = Registry::new();
    registry.register(&DOUBLE_NODE).expect("registers");
    registry.register(&SHOUT_NODE).expect("registers");
    for decl in [
        closure_node("rt.start", "a test", &[], Some("out"), move || {
            Ok::<u32, Fault>(start)
        }),
        closure_node(
            "rt.judge",
            "a test",
            &["level"],
            Some("word"),
            move |level: u32| {
                Ok::<String, Fault>(if level > limit { "over" } else { "under" }.to_string())
            },
        ),
        closure_node(
            "rt.keep",
            "a test",
            &["text", "level"],
            None,
            move |text: String, level: u32| {
                sink.lock().unwrap().push((text, level));
                Ok::<(), Fault>(())
            },
        ),
    ] {
        registry
            .register_owned(decl.expect("the closure fits its ports"))
            .expect("registers");
    }
    // start -> double (a #[node]) -> judge -> shout (a #[node]) -> keep, which also takes the
    // doubled number.
    let ends = run(
        &registry,
        &[
            "rt.start",
            "fixed.double",
            "rt.judge",
            "fixed.shout",
            "rt.keep",
        ],
        vec![
            wire(0, "out", 1, "a"),
            wire(1, "out", 2, "level"),
            wire(2, "word", 3, "text"),
            wire(3, "out", 4, "text"),
            wire(1, "out", 4, "level"),
        ],
    );
    assert!(ends.iter().all(|(done, _, _)| *done), "{ends:?}");
    assert_eq!(*seen.lock().unwrap(), [("OVER".to_string(), 42)]);
}

#[test]
fn a_handle_goes_through_a_closure_without_its_value_being_copied() {
    let made = Arc::new(String::from("one megabyte, say"));
    let (first, kept) = (Arc::clone(&made), Arc::new(Mutex::new(None)));
    let sink = Arc::clone(&kept);
    let mut registry = Registry::new();
    for decl in [
        closure_node("rt.make", "a test", &[], Some("out"), move || {
            Ok::<Arc<String>, Fault>(Arc::clone(&first))
        }),
        closure_node(
            "rt.pass",
            "a test",
            &["text"],
            Some("out"),
            |text: Arc<String>| Ok::<Arc<String>, Fault>(text),
        ),
        closure_node(
            "rt.end",
            "a test",
            &["text"],
            None,
            move |text: Arc<String>| {
                *sink.lock().unwrap() = Some(text);
                Ok::<(), Fault>(())
            },
        ),
    ] {
        registry
            .register_owned(decl.expect("the closure fits its ports"))
            .expect("registers");
    }
    let ends = run(
        &registry,
        &["rt.make", "rt.pass", "rt.end"],
        vec![wire(0, "out", 1, "text"), wire(1, "out", 2, "text")],
    );
    assert!(ends.iter().all(|(done, _, _)| *done), "{ends:?}");
    let got = kept.lock().unwrap().take().expect("it arrived");
    assert!(Arc::ptr_eq(&got, &made), "the same value, not a copy of it");
}

#[test]
fn a_closures_error_is_the_steps_fault_with_its_code() {
    let mut registry = Registry::new();
    registry
        .register_owned(
            closure_node("rt.refuse", "a test", &[], Some("out"), || {
                Err::<u32, Fault>(Fault {
                    code: 31,
                    reason: "nothing to give",
                })
            })
            .expect("fits"),
        )
        .expect("registers");
    let ends = run(&registry, &["rt.refuse"], Vec::new());
    assert_eq!(ends, [(false, 31, "nothing to give".to_string())]);
}

#[test]
fn a_closure_that_doesnt_fit_the_ports_named_is_refused_in_words() {
    let two = closure_node(
        "rt.x",
        "a test",
        &["only"],
        Some("out"),
        |a: u32, b: u32| Ok::<u32, Fault>(a + b),
    )
    .map(drop);
    assert_eq!(two, Err(ClosureError::Inputs { named: 1, takes: 2 }));
    assert_eq!(
        two.unwrap_err().to_string(),
        "1 inputs were named and the closure takes 2"
    );
    let silent = closure_node("rt.y", "a test", &[], Some("out"), || Ok::<(), Fault>(()));
    assert_eq!(
        silent.map(drop).unwrap_err().to_string(),
        "an output was named and the closure gives nothing"
    );
    let unnamed = closure_node("rt.z", "a test", &[], None, || Ok::<u32, Fault>(1));
    assert_eq!(
        unnamed.map(drop).unwrap_err().to_string(),
        "the closure gives a value and no output was named"
    );
}
