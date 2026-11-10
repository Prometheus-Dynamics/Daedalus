//! A plugin step allocates nothing once it has run once.

mod support;

use daedalus_abi::counting::{CountingAlloc, counted};
use daedalus_abi::testing::{release, run_once};
use daedalus_abi::{BoundaryPort, Slot, SlotState, Slots, StepCx, StepEnd};
use daedalus_loader::{Loader, RegionIn};

#[global_allocator]
static ALLOCATOR: CountingAlloc = CountingAlloc;

#[test]
fn a_plugin_tick_allocates_nothing_after_warm_up() {
    let mut loader = Loader::new();
    let plugin = loader.open(support::copy_fixture("alloc")).expect("opens");
    let backend = plugin.backend("fixture.cpu").expect("backend");
    let nodes = ["fixture.pass"];
    let port = BoundaryPort {
        node: 0,
        port: 0,
        reserved: 0,
        type_index: 0,
        slot: 0,
    };
    let inputs = [port];
    let outputs = [port];
    let region = RegionIn {
        nodes: &nodes,
        inputs: &inputs,
        outputs: &outputs,
    };
    let mut units = backend.compile(&region).expect("compiles");
    let mut step = units.pop().expect("unit").step;
    let mut payload = *b"tick";
    let mut cells = [Slot::EMPTY, Slot::EMPTY];
    cells[0] = Slot {
        state: SlotState::Full as u32,
        data: payload.as_mut_ptr(),
        len: 4,
        cap: 4,
        ..Slot::EMPTY
    };
    let mut cx = StepCx::new();
    assert!(matches!(
        run_once(&mut step, &mut Slots::of(&mut cells), &mut cx),
        StepEnd::Done
    ));

    let (ends, allocations) = counted(|| {
        let mut last = StepEnd::Fault;
        for _ in 0..1000 {
            last = run_once(&mut step, &mut Slots::of(&mut cells), &mut cx);
        }
        last
    });
    assert!(matches!(ends, StepEnd::Done));
    assert_eq!(allocations, 0, "a warm tick allocates nothing");
    release(step);
}
