mod support;

use daedalus_abi::Step;
use daedalus_runner::{MaskError, SlotKind, SlotSpec};
use support::{Fixture, Link, Wiring, chain, link_word, tagged};

fn fifo_specs(n: usize) -> Vec<SlotSpec> {
    (0..n)
        .map(|_| SlotSpec {
            cap: 1,
            kind: SlotKind::Fifo,
        })
        .collect()
}

#[test]
fn a_masked_off_step_never_runs_and_its_consumer_sees_nothing() {
    let (wiring, words) = chain(2);
    let steps = words.into_iter().map(Step::of::<Link>).collect();
    let mut fixture = Fixture::new(steps, wiring, fifo_specs(2));
    let mut runner = fixture.load().unwrap();
    // Step 0 off, step 1 on.
    runner.set_activity(Some(&[0b10])).unwrap();
    runner.push(0, tagged(1)).unwrap();
    runner.wake(0);
    let report = runner.tick().unwrap();
    assert_eq!(report.step_calls, 0);
    assert_eq!(runner.queue(1).unwrap().len, 0, "consumer saw nothing");
    assert_eq!(runner.queue(0).unwrap().len, 1, "input stays queued");
}

#[test]
fn a_bad_mask_length_is_refused_with_the_expected_length() {
    let (wiring, words) = chain(16);
    let steps = words.into_iter().map(Step::of::<Link>).collect();
    let mut fixture = Fixture::new(steps, wiring, fifo_specs(16));
    let mut runner = fixture.load().unwrap();
    assert_eq!(
        runner.set_activity(Some(&[0, 0])),
        Err(MaskError { expected: 1 })
    );
    assert!(runner.set_activity(Some(&[u32::MAX])).is_ok());
    assert!(runner.set_activity(None).is_ok());
}

#[test]
fn a_selected_branch_runs_and_the_other_does_not() {
    // Two independent branches: steps 0 and 1 are one branch, steps 2 and 3 another.
    let mut wiring = Wiring::default();
    wiring.step(&[1], &[0], &[]);
    wiring.step(&[], &[1], &[]);
    wiring.step(&[3], &[2], &[]);
    wiring.step(&[], &[3], &[]);
    let steps = vec![
        Step::of::<Link>(link_word(0, None)),
        Step::of::<Link>(link_word(1, None)),
        Step::of::<Link>(link_word(2, None)),
        Step::of::<Link>(link_word(3, None)),
    ];
    let mut fixture = Fixture::new(steps, wiring, fifo_specs(4));
    let mut runner = fixture.load().unwrap();
    runner.set_activity(Some(&[0b1100])).unwrap();
    runner.push(0, tagged(1)).unwrap();
    runner.push(2, tagged(2)).unwrap();
    runner.wake(0);
    runner.wake(2);
    let report = runner.tick().unwrap();
    assert_eq!(report.step_calls, 2, "the selected branch ran: its root and its consumer");
    assert_eq!(runner.queue(2).unwrap().len, 0);
    assert_eq!(
        runner.queue(0).unwrap().len,
        1,
        "the other branch stayed queued"
    );
    runner.tick().unwrap();
    assert_eq!(runner.queue(0).unwrap().len, 1);
}
