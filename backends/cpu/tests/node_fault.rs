//! An `Err` from a node becomes a fault that names the node and says why.

mod common;

use common::{NODE, call, inline, step_of};
use daedalus_abi::{Slot, StepCx, StepEnd};
use daedalus_cpu_author::Fault;
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
    Err("a reason that is well past the sixty four bytes an error may give the fault record")
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
    assert_eq!(cx.reason().len(), daedalus_abi::ERROR_REASON_CAPACITY);
}
