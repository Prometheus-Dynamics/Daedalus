//! The shared graph in loaded mode, natively: the plan blob, loaded into the interpreter over the
//! node library, matches the shared node functions called by hand.

mod common;

use common::{Reference, signal};

/// The generated node library and its exports; the tests use part of it.
#[allow(dead_code)]
mod library {
    include!("../generated/library.rs");
}

/// Arena bytes of the interpreter; the plan needs fewer (checked below).
const ARENA: usize = 512;
type Interpreter = daedalus_mcu::loaded::Interpreter<ARENA>;
const PLAN: &[u8] = include_bytes!("../generated/plan.bin");

#[test]
fn loaded_plan_matches_the_node_functions() {
    assert!(Interpreter::required(&library::LIBRARY, PLAN).unwrap() <= ARENA);
    let mut interp = Box::new(Interpreter::new(&library::LIBRARY));
    interp.load(PLAN).unwrap();
    let sample = interp.input("sample").unwrap();
    let level = interp.output("level").unwrap();
    let active = interp.output("active").unwrap();
    let rises = interp.output("rises").unwrap();
    let mut reference = Reference::new();
    let mut count = 0;
    for raw in signal() {
        interp.push(sample, raw).unwrap();
        interp.tick().unwrap();
        let expected = reference.step(raw);
        assert_eq!(interp.pop::<f32>(level).unwrap(), Some(expected.level));
        assert_eq!(interp.pop::<bool>(active).unwrap(), Some(expected.active));
        assert_eq!(interp.pop::<u32>(rises).unwrap(), expected.rises);
        count += usize::from(expected.rises.is_some());
    }
    assert_eq!(count, 2);
}
