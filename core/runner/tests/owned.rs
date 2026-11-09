//! Owned payloads: each one is dropped exactly once on every path out of the runner.

mod support;

use std::sync::{Arc, Mutex};

use daedalus_abi::{OwnedStep, Slots, Step, StepCx, StepEnd};
use daedalus_runner::{Runner, SlotKind, SlotSpec, put_typed};
use daedalus_types::{SpaceIndex, TypeIndex};
use daedalus_value::{InlineBytes, Owner, Value};
use support::{Fixture, Wiring, cell};

const TY: TypeIndex = TypeIndex(7);
const SP: SpaceIndex = SpaceIndex(0);

/// A payload that is not `Copy`, and records its id in `log` when it is dropped.
struct Counted {
    id: u32,
    log: Arc<Mutex<Vec<u32>>>,
}

impl Drop for Counted {
    fn drop(&mut self) {
        self.log.lock().unwrap().push(self.id);
    }
}

fn counted(id: u32, log: &Arc<Mutex<Vec<u32>>>) -> Arc<Counted> {
    Arc::new(Counted {
        id,
        log: log.clone(),
    })
}

#[derive(Clone, Copy)]
enum Mode {
    /// Writes two payloads to the output, so the first is overwritten.
    Twice,
    /// Moves the input to the output and marks the input moved.
    Move,
    /// Reads the input and writes nothing.
    Read,
}

struct Writer {
    input: usize,
    output: usize,
    mode: Mode,
    log: Arc<Mutex<Vec<u32>>>,
}

impl OwnedStep for Writer {
    fn run(&mut self, slots: &mut Slots, _: &mut StepCx) -> StepEnd {
        match self.mode {
            Mode::Twice => {
                put_typed(cell(slots, self.output), TY, SP, counted(1, &self.log));
                put_typed(cell(slots, self.output), TY, SP, counted(2, &self.log));
            }
            Mode::Move => {
                let moved = *cell(slots, self.input);
                *cell(slots, self.output) = moved;
                cell(slots, self.input).state = daedalus_abi::SlotState::Moved as u32;
            }
            Mode::Read => {}
        }
        StepEnd::Done
    }
}

fn fifo(cap: u32) -> SlotSpec {
    SlotSpec {
        cap,
        kind: SlotKind::Fifo,
    }
}

/// One step on slots `input` and `output` (either may be absent).
fn one_step(
    input: Option<u32>,
    output: Option<u32>,
    mode: Mode,
    log: &Arc<Mutex<Vec<u32>>>,
) -> Fixture {
    let mut wiring = Wiring::default();
    wiring.step(
        &[],
        &input.into_iter().collect::<Vec<_>>(),
        &output.into_iter().collect::<Vec<_>>(),
    );
    let step = Step::boxed(Writer {
        input: 0,
        output: usize::from(input.is_some()),
        mode,
        log: log.clone(),
    });
    Fixture::new(vec![step], wiring, vec![fifo(2), fifo(2)])
}

#[test]
fn an_overwritten_payload_is_dropped_once_and_the_rest_once_when_taken() {
    let log = Arc::new(Mutex::new(Vec::new()));
    let mut fixture = one_step(None, Some(1), Mode::Twice, &log);
    let mut runner = fixture.load().unwrap();
    runner.wake(0);
    runner.tick().unwrap();
    assert_eq!(
        *log.lock().unwrap(),
        vec![1],
        "the overwritten one drops at the overwrite"
    );

    let value = runner.take_value(1).expect("the output was published");
    drop(value);
    assert_eq!(
        *log.lock().unwrap(),
        vec![1, 2],
        "the taken one drops once, when released"
    );
}

#[test]
fn a_consumed_payload_is_released_once_by_the_step_that_read_it() {
    let log = Arc::new(Mutex::new(Vec::new()));
    let mut fixture = one_step(Some(0), None, Mode::Read, &log);
    let mut runner = fixture.load().unwrap();
    runner.feed_typed(0, TY, SP, counted(5, &log)).unwrap();
    runner.wake(0);
    runner.tick().unwrap();
    assert_eq!(*log.lock().unwrap(), vec![5]);
    assert_eq!(runner.queue(0).unwrap().len, 0);
}

#[test]
fn a_moved_payload_is_not_released_by_the_input_but_by_its_new_holder() {
    let log = Arc::new(Mutex::new(Vec::new()));
    let mut fixture = one_step(Some(0), Some(1), Mode::Move, &log);
    let mut runner = fixture.load().unwrap();
    runner.feed_typed(0, TY, SP, counted(7, &log)).unwrap();
    runner.wake(0);
    runner.tick().unwrap();
    assert_eq!(
        *log.lock().unwrap(),
        Vec::<u32>::new(),
        "moved, so nothing dropped yet"
    );

    drop(runner.take_value(1).unwrap());
    assert_eq!(
        *log.lock().unwrap(),
        vec![7],
        "dropped once, by the output's holder"
    );
}

#[test]
fn a_payload_left_in_a_queue_is_dropped_once_at_shutdown() {
    let log = Arc::new(Mutex::new(Vec::new()));
    let mut fixture = one_step(Some(0), None, Mode::Read, &log);
    {
        let mut runner: Runner<'_> = fixture.load().unwrap();
        runner.feed_typed(0, TY, SP, counted(9, &log)).unwrap();
        assert!(log.lock().unwrap().is_empty());
    }
    assert_eq!(*log.lock().unwrap(), vec![9]);
}

#[test]
fn an_inline_value_round_trips_through_its_queue() {
    let log = Arc::new(Mutex::new(Vec::new()));
    let mut fixture = one_step(None, None, Mode::Read, &log);
    let mut runner = fixture.load().unwrap();
    let value = Value {
        ty: TYPE_OF_BYTES,
        space: SP,
        owner: Owner::Inline(InlineBytes::from_slice(b"abc").unwrap()),
    };
    runner.feed_value(1, value).unwrap();
    let back = runner.take_value(1).unwrap();
    assert_eq!(back.bytes(), Some(&b"abc"[..]));
    assert_eq!(back.ty, TYPE_OF_BYTES);
}

const TYPE_OF_BYTES: TypeIndex = TypeIndex(3);

#[test]
fn a_typed_value_cannot_be_fed_as_a_plain_value() {
    let log = Arc::new(Mutex::new(Vec::new()));
    let mut fixture = one_step(None, None, Mode::Read, &log);
    let mut runner = fixture.load().unwrap();
    let typed: Arc<dyn std::any::Any + Send + Sync> = counted(4, &log);
    let value = Value {
        ty: TY,
        space: SP,
        owner: Owner::Typed(typed),
    };
    assert_eq!(
        runner.feed_value(0, value),
        Err(daedalus_runner::RunError::Untyped { slot: 0 })
    );
    assert_eq!(
        *log.lock().unwrap(),
        vec![4],
        "refused value is dropped once"
    );
}
