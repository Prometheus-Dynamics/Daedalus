//! The stored plan, through the small runner, matches the node functions.

mod common;

use common::{Reference, signal};
use daedalus_chip_shared::LIBRARY;
use daedalus_cpu::blob::Interpreter;

/// More than a chip needs, because a 64-bit host's steps are larger.
const ARENA: usize = 640;
const PLAN: &[u8] = include_bytes!("../generated/plan.bin");

#[test]
fn loaded_plan_matches_the_node_functions() {
    let needed = Interpreter::<ARENA>::required(&LIBRARY, PLAN).unwrap();
    assert!(needed <= ARENA, "{needed}");
    let mut running = Box::new(Interpreter::<ARENA>::new());
    running.load(&LIBRARY, PLAN).unwrap();
    let sample = running.input("sample").unwrap();
    let level = running.output("level").unwrap();
    let active = running.output("active").unwrap();
    let rises = running.output("rises").unwrap();
    let mut reference = Reference::default();
    let mut count = 0;
    for raw in signal() {
        running.push(sample, raw).unwrap();
        running.tick().unwrap();
        let expected = reference.step(raw);
        assert_eq!(
            (
                running.pop::<f32>(level).unwrap(),
                running.pop::<bool>(active).unwrap(),
                running.pop::<u32>(rises).unwrap(),
            ),
            expected
        );
        count += usize::from(expected.2.is_some());
    }
    assert_eq!(count, 2);
}
