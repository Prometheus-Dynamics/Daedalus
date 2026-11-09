//! Joins, held inputs and fan-in order, with plain-function steps.

mod support;

use std::sync::{Arc, Mutex};

use daedalus_abi::{OwnedStep, Slot, SlotState, Slots, Step, StepCx, StepEnd};
use daedalus_runner::{Fire, Runner, SlotKind, SlotSpec, fanin_order};
use support::{Fixture, Wiring, cell, tagged};

fn spec(cap: u32, kind: SlotKind) -> SlotSpec {
    SlotSpec { cap, kind }
}

type Seen = Arc<Mutex<Option<usize>>>;

/// Joins slots 0 and 1 and records `a * 100 + b`. Empty inputs read as 0.
struct Pair(Seen);

impl OwnedStep for Pair {
    fn run(&mut self, slots: &mut Slots, _: &mut StepCx) -> StepEnd {
        let a = value_of(*cell(slots, 0));
        let b = value_of(*cell(slots, 1));
        *self.0.lock().unwrap() = Some(a * 100 + b);
        StepEnd::Done
    }
}

fn value_of(slot: Slot) -> usize {
    if slot.state == SlotState::Full as u32 {
        slot.len
    } else {
        0
    }
}

fn pair_fixture(fire: Fire, required: &[u32], specs: Vec<SlotSpec>) -> (Fixture, Seen) {
    let seen = Seen::default();
    let mut wiring = Wiring::default();
    wiring.step(&[], &[0, 1], &[]);
    wiring.set_join(0, fire, required);
    let steps = vec![Step::boxed(Pair(Arc::clone(&seen)))];
    (Fixture::new(steps, wiring, specs), seen)
}

/// Feeds slot 0 every tick where `round % 2 == 0` and slot 1 where `round % 3 == 0`, then ticks.
fn drive(runner: &mut Runner<'_>, seen: &Seen, rounds: usize) -> Vec<Option<usize>> {
    (1..=rounds)
        .map(|round| {
            if round.is_multiple_of(2) {
                runner.push(0, tagged(round * 100)).unwrap();
                runner.wake(0);
            }
            if round.is_multiple_of(3) {
                runner.push(1, tagged(round)).unwrap();
                runner.wake(0);
            }
            runner.tick().unwrap();
            seen.lock().unwrap().take()
        })
        .collect()
}

fn fifo(cap: u32) -> SlotSpec {
    spec(cap, SlotKind::Fifo)
}

#[test]
fn all_joins_producers_at_different_rates_across_ticks() {
    let specs = vec![fifo(4), fifo(4)];
    let (mut fixture, seen) = pair_fixture(Fire::All, &[0, 1], specs);
    let mut runner = fixture.load().unwrap();
    let got = drive(&mut runner, &seen, 6);
    assert_eq!(got[..2], [None, None]);
    assert_eq!(got[2], Some(20_000 + 3));
    assert_eq!(got[5], Some(40_000 + 6));
    // The third value of slot 0 is still queued, waiting for its partner.
    assert_eq!(runner.queue(0).unwrap().len, 1);
}

#[test]
fn any_drops_the_arrivals_of_a_skipped_tick() {
    let specs = vec![fifo(4), fifo(4)];
    let (mut fixture, seen) = pair_fixture(Fire::Any, &[0, 1], specs);
    let mut runner = fixture.load().unwrap();
    let got = drive(&mut runner, &seen, 6);
    assert_eq!(got, [None, None, None, None, None, Some(60_000 + 6)]);
    assert_eq!(runner.queue(0).unwrap().len, 0);
    assert_eq!(runner.queue(1).unwrap().len, 0);
}

#[test]
fn fifo_pairs_values_in_arrival_order() {
    let specs = vec![fifo(8), fifo(8)];
    let (mut fixture, seen) = pair_fixture(Fire::All, &[0, 1], specs);
    let mut runner = fixture.load().unwrap();
    let got = drive(&mut runner, &seen, 9);
    let paired: Vec<usize> = got.into_iter().flatten().collect();
    assert_eq!(paired, [20_000 + 3, 40_000 + 6, 60_000 + 9]);
}

#[test]
fn a_held_value_reaches_every_tick_until_replaced_or_cleared() {
    let specs = vec![spec(1, SlotKind::Held), fifo(4)];
    let (mut fixture, seen) = pair_fixture(Fire::All, &[0, 1], specs);
    let mut runner = fixture.load().unwrap();
    assert!(runner.is_held(0));
    runner.replace_held(0, tagged(7)).unwrap();
    let round = |runner: &mut Runner<'_>, b: usize| {
        runner.push(1, tagged(b)).unwrap();
        runner.wake(0);
        runner.tick().unwrap();
        seen.lock().unwrap().take()
    };
    assert_eq!(round(&mut runner, 1), Some(701));
    assert_eq!(round(&mut runner, 2), Some(702));
    runner.replace_held(0, tagged(9)).unwrap();
    assert_eq!(round(&mut runner, 3), Some(903));
    runner.clear_held(0).unwrap();
    // A required held input that is cleared blocks the step. All keeps the other input queued.
    assert_eq!(round(&mut runner, 4), None);
    assert_eq!(runner.queue(1).unwrap().len, 1);
}

#[test]
fn an_unconnected_optional_input_runs_with_none() {
    let specs = vec![fifo(4), fifo(4)];
    let (mut fixture, seen) = pair_fixture(Fire::All, &[0], specs);
    let mut runner = fixture.load().unwrap();
    runner.push(0, tagged(3)).unwrap();
    runner.wake(0);
    let report = runner.tick().unwrap();
    assert_eq!(report.step_calls, 1);
    assert_eq!(*seen.lock().unwrap(), Some(300));
}

#[test]
fn fan_in_order_follows_the_numeric_suffix() {
    let ports = ["ins10", "other", "ins2", "ins0", "ins", "ins1x"];
    assert_eq!(fanin_order("ins", &ports), [3, 2, 0]);
}

#[test]
fn a_step_without_required_inputs_skips_the_join_check() {
    let specs = vec![fifo(4), fifo(4)];
    let (mut fixture, seen) = pair_fixture(Fire::All, &[], specs);
    let mut runner = fixture.load().unwrap();
    runner.wake(0);
    assert_eq!(runner.tick().unwrap().step_calls, 1);
    assert_eq!(*seen.lock().unwrap(), Some(0));
}
