//! The graph with a `command` input that overrides the threshold, in every mode.

mod common;

use common::{Loaded, Out, Reference, signal};
use daedalus_abi::counting::counted;
use daedalus_chip_blink::loaded::{FANIN_A, FANIN_ARENA, FANIN_B, Interpreter, LIBRARY};
use daedalus_chip_blink::{fanin, fanin_tunable};
use daedalus_cpu::blob::LoadError;
use daedalus_cpu::chip::Tunable;
use daedalus_plan::VERSION;

fn compiled_step(graph: &mut fanin::Graph, now: usize, raw: u16) -> Out {
    graph.push_sample(raw).unwrap();
    graph.tick_at(now as u64).unwrap();
    (graph.pop_level(), graph.pop_led(), graph.pop_rises())
}

fn tunable_step(graph: &mut fanin_tunable::Graph, now: usize, raw: u16) -> Out {
    graph.push_sample(raw).unwrap();
    graph.tick_at(now as u64).unwrap();
    (graph.pop_level(), graph.pop_led(), graph.pop_rises())
}

#[test]
fn plans_fit_their_arena_and_name_the_fan_in() {
    for plan in [FANIN_A, FANIN_B] {
        let needed = Interpreter::required(&LIBRARY, plan);
        assert!(needed.is_ok_and(|size| size <= FANIN_ARENA), "{needed:?}");
    }
    assert!(fanin::NODE_IDS.contains(&"blink.latest"));
    assert!(fanin::NODE_IDS.contains(&"blink.command_active"));
    let loaded = Loaded::new(FANIN_A);
    assert!(loaded.running.input("command").is_some());
}

#[test]
fn compiled_fan_in_matches_the_node_functions_without_allocating() {
    let mut graph = fanin::Graph::new();
    let mut reference = Reference::a();
    let ((), allocated) = counted(|| {
        for (now, raw) in signal().enumerate() {
            assert_eq!(compiled_step(&mut graph, now, raw), reference.step(raw));
        }
    });
    assert_eq!(allocated, 0);
}

#[test]
fn a_pushed_command_takes_precedence_over_the_threshold() {
    let mut graph = fanin::Graph::new();
    // A low level holds the threshold off, so the led is dark on its own.
    assert_eq!(compiled_step(&mut graph, 0, 100).1, Some(false));
    graph.push_command(1).unwrap();
    assert_eq!(compiled_step(&mut graph, 0, 100).1, Some(true));

    // A high level holds the threshold on, and a zero command turns it off.
    for now in 1..40 {
        compiled_step(&mut graph, now, 4000);
    }
    assert_eq!(compiled_step(&mut graph, 0, 4000).1, Some(true));
    graph.push_command(0).unwrap();
    assert_eq!(compiled_step(&mut graph, 0, 4000).1, Some(false));
}

#[test]
fn tunable_fan_in_matches_compiled_and_follows_its_parameters() {
    let (mut compiled, mut graph) = (fanin::Graph::new(), fanin_tunable::Graph::new());
    for (now, raw) in signal().enumerate() {
        assert_eq!(
            tunable_step(&mut graph, now, raw),
            compiled_step(&mut compiled, now, raw)
        );
    }
    assert_eq!(fanin_tunable::PARAM_NAMES.len(), 4);

    let mut graph = fanin_tunable::Graph::new();
    graph.set_lowpass_alpha(0.5).unwrap();
    graph.set_threshold_on(2.5).unwrap();
    graph.set_blink_period(2).unwrap();
    assert!(graph.param(0).is_some());
    let mut reference = Reference::tuned();
    for (now, raw) in signal().enumerate() {
        assert_eq!(tunable_step(&mut graph, now, raw), reference.step(raw));
    }
}

#[test]
fn loaded_fan_in_plan_matches_compiled_and_plan_b_ignores_the_command() {
    let mut loaded = Loaded::new(FANIN_A);
    let command = loaded.running.input("command").unwrap();
    let mut compiled = fanin::Graph::new();
    let ((), allocated) = counted(|| {
        for (now, raw) in signal().enumerate() {
            if now % 25 == 7 {
                let code = (now % 2) as u8;
                loaded.running.push(command, code).unwrap();
                compiled.push_command(code).unwrap();
            }
            let (a, b) = (loaded.step(raw), compiled_step(&mut compiled, now, raw));
            assert_eq!(a, b, "at {now}");
        }
    });
    assert_eq!(allocated, 0);

    let mut loaded = Loaded::new(FANIN_B);
    assert_eq!(loaded.running.input("command"), None);
    let mut reference = Reference::fanin_b();
    for raw in signal() {
        assert_eq!(loaded.step(raw), reference.step(raw));
    }
}

#[test]
fn rejects_the_blob_format_of_another_version() {
    let mut plan = FANIN_A.to_vec();
    let found = VERSION + 1;
    plan[8..10].copy_from_slice(&found.to_le_bytes());
    let mut running = Box::new(Interpreter::new());
    assert_eq!(
        running.load(&LIBRARY, &plan),
        Err(LoadError::Version { found })
    );
}
