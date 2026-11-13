//! An `Err` from a node becomes a fault that names the node and says why.

mod common;

use common::{NODE, call, inline, step_of};
use daedalus_abi::{Slot, StepCx, StepEnd};
use daedalus_cpu_author::{Ctx, Fault};
use daedalus_macros::node;

#[node(id = "t.check", inputs(a), outputs(out))]
fn check(a: u32) -> Result<u32, Fault> {
    if a == 0 {
        return Err(Fault {
            code: 7,
            reason: "zero isn't allowed",
        });
    }
    Ok(a)
}

#[node(id = "t.long", outputs(out))]
fn long() -> Result<u32, &'static str> {
    Err(
        "a reason that goes on well past the hundred and twenty bytes a fault keeps, so that what the host reads is only its start",
    )
}

/// Finds every odd number up to `a`, and fails when there is one, saying which.
#[node(id = "t.odd", inputs(a), outputs(out))]
fn odd(a: u32, ctx: &Ctx) -> Result<u32, Fault> {
    let mut found = false;
    for n in (1..=a).step_by(2) {
        ctx.detail_fmt(format_args!("{n} is odd\n"));
        found = true;
    }
    if found {
        return Err(Fault {
            code: 3,
            reason: "there are odd numbers",
        });
    }
    Ok(a)
}

#[test]
fn a_node_gives_the_host_more_than_its_reason_holds() {
    let mut step = step_of(&ODD_NODE);
    let mut buf = [0; 16];
    let mut cells = [inline(60_u32, &mut buf), Slot::EMPTY];
    let mut cx = StepCx::new();
    let kept = daedalus_abi::testing::keep_detail(&mut cx);
    assert!(matches!(
        call(&mut step, &mut cells, &mut cx),
        StepEnd::Fault
    ));
    assert_eq!(cx.reason(), "there are odd numbers");
    assert!(!cx.reason_cut());
    let expected: String = (1..=60)
        .step_by(2)
        .map(|n| format!("{n} is odd\n"))
        .collect();
    assert_eq!(kept.text(), expected);
    assert!(expected.len() > daedalus_abi::REASON_CAPACITY);

    // With nobody keeping a detail the same body runs the same, and writes nowhere.
    let mut buf = [0; 16];
    let mut cells = [inline(60_u32, &mut buf), Slot::EMPTY];
    let mut cx = StepCx::new();
    assert!(!cx.wants_detail());
    assert!(matches!(
        call(&mut step, &mut cells, &mut cx),
        StepEnd::Fault
    ));
    assert_eq!(cx.reason(), "there are odd numbers");
}

#[test]
fn an_error_is_a_fault_with_the_node_and_the_reason() {
    let mut step = step_of(&CHECK_NODE);
    let mut buf = [0; 16];
    let mut cells = [inline(0_u32, &mut buf), Slot::EMPTY];
    let mut cx = StepCx::new();
    assert!(matches!(
        call(&mut step, &mut cells, &mut cx),
        StepEnd::Fault
    ));
    assert_eq!(cx.at, NODE.0);
    assert_eq!(cx.code, 7);
    assert_eq!(cx.reason(), "zero isn't allowed");
}

#[test]
fn an_ok_writes_the_value() {
    let mut step = step_of(&CHECK_NODE);
    let mut buf = [0; 16];
    let mut cells = [inline(3_u32, &mut buf), Slot::EMPTY];
    assert!(matches!(
        call(&mut step, &mut cells, &mut StepCx::new()),
        StepEnd::Done
    ));
}

#[test]
fn a_long_reason_is_cut() {
    let mut step = step_of(&LONG_NODE);
    let mut cells = [Slot::EMPTY];
    let mut cx = StepCx::new();
    assert!(matches!(
        call(&mut step, &mut cells, &mut cx),
        StepEnd::Fault
    ));
    assert_eq!(cx.reason().len(), daedalus_abi::REASON_CAPACITY);
    assert!(cx.reason_cut(), "and the fault says it was cut");
}
