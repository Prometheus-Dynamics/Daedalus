//! What each way of passing a large value costs a tick, counted. The copy and reference
//! counters are the process's, so the tests that read them take turns.

use std::sync::{Mutex, MutexGuard, PoisonError};

use daedalus_abi::counting::CountingAlloc;
use daedalus_handoff_bench::{Cost, Path, Shape, cost, plans};

#[global_allocator]
static COUNTING: CountingAlloc = CountingAlloc;

static TURN: Mutex<()> = Mutex::new(());

fn turn() -> MutexGuard<'static, ()> {
    TURN.lock().unwrap_or_else(PoisonError::into_inner)
}

const STAGES: u32 = 8;
const LEN: usize = 1 << 16;
const TICKS: u64 = 200;

const NOTHING: Cost = Cost {
    allocations: 0,
    copies: 0,
    retains: 0,
};

#[test]
fn handle_steady_state_allocates_nothing() {
    let _turn = turn();
    // Read through a reference, by one node or by eight: nothing is allocated, copied or
    // counted.
    assert_eq!(cost(Path::Handle, Shape::Readers(1), LEN, TICKS), NOTHING);
    assert_eq!(
        cost(Path::Handle, Shape::Readers(STAGES), LEN, TICKS),
        NOTHING
    );
}

#[test]
fn shared_bytes_steady_state_allocates_nothing() {
    let _turn = turn();
    assert_eq!(cost(Path::Shared, Shape::Readers(1), LEN, TICKS), NOTHING);
    assert_eq!(
        cost(Path::Shared, Shape::Readers(STAGES), LEN, TICKS),
        NOTHING
    );
}

/// A stage that takes the value itself, `T`, and returns one allocates nothing once warm. The
/// value moves from the stage that made it to the next, and each stage refills the memory the
/// value was kept in, so a chain of stages costs no allocation a tick. The first tick makes the
/// memory, and that is not counted here: the cost is taken from a warm graph.
#[test]
fn a_value_taken_out_of_its_handle_and_returned_allocates_nothing_once_warm() {
    let _turn = turn();
    for path in [Path::Handle, Path::Shared] {
        for stages in [1, STAGES] {
            assert_eq!(
                cost(path, Shape::Chain(stages), LEN, TICKS),
                NOTHING,
                "{path:?}, {stages} stages"
            );
        }
    }
}

#[test]
fn a_value_handed_on_by_its_handle_costs_nothing() {
    let _turn = turn();
    // A stage that takes `Arc<T>` and returns it hands the same handle on: no allocation, no
    // copy, no count raised, through one stage or eight, whatever the value is.
    for path in Path::ALL {
        for stages in [1, STAGES] {
            assert_eq!(
                cost(path, Shape::Handed(stages), LEN, TICKS),
                NOTHING,
                "{path:?}, {stages} stages"
            );
        }
    }
    // Taking the handle is still taking: two stages can't each take one value's handle, and
    // the planner says so. Stages that share a value read it through a reference.
    let refused = plans(Path::Handle, Shape::HandTakers(2)).expect_err("two takers");
    assert!(refused.contains("CopyMissing"), "{refused}");
    assert_eq!(
        cost(Path::Handle, Shape::HandTakers(1), LEN, TICKS),
        NOTHING
    );
}

#[test]
fn a_value_is_changed_in_place_through_its_handle() {
    use daedalus_handoff_bench::{feed, graph, tick, touched, value};
    let _turn = turn();
    // Eight stages each change the value through `Arc::make_mut` and hand it on, and the last
    // node reads it through a borrowed handle. Nobody else holds it, so nothing is copied, and
    // every stage's change is there at the end.
    let shape = Shape::Edited(STAGES);
    let mut host = graph(Path::Handle, shape);
    let before = touched();
    for _ in 0..4 {
        feed(&mut host, value(Path::Handle, LEN));
        assert_eq!(tick(&mut host), 3 + u64::from(STAGES));
    }
    assert_eq!(touched(), before, "changed in place: no copy of the bytes");
    // Held by the host as well, the value is copied by the first stage that changes it, once,
    // and the host's own is untouched.
    let kept = value(Path::Handle, LEN);
    feed(&mut host, kept.again());
    assert_eq!(tick(&mut host), 3 + u64::from(STAGES));
    assert_eq!(touched().0 - before.0, 1, "one copy, by the first stage");
    let daedalus_handoff_bench::Fed::Frame(kept) = kept else {
        panic!("a frame");
    };
    assert_eq!(kept.bytes[1], 0, "the host's value is as it was");
}

#[test]
fn vec_path_allocates_per_tick() {
    let _turn = turn();
    // Each stage makes its own vector, so one allocation a stage at least, and the bytes are
    // copied once a stage. The handle paths above allocate nothing: the contrast is real.
    let vec = cost(Path::Vec, Shape::Chain(STAGES), LEN, TICKS);
    assert!(vec.allocations >= u64::from(STAGES), "{vec:?}");
    assert_eq!(vec.copies, u64::from(STAGES));
    let handle = cost(Path::Handle, Shape::Chain(STAGES), LEN, TICKS);
    assert!(vec.allocations > handle.allocations && handle.copies == 0);
    // Read through a reference, even a vector costs nothing.
    assert_eq!(cost(Path::Vec, Shape::Readers(STAGES), LEN, TICKS), NOTHING);
}

#[test]
fn one_consumer_touches_no_reference_count() {
    let _turn = turn();
    // Along a chain every value has one consumer, which takes it: the shared bytes inside it
    // are moved, and their count is never raised. What is counted here is the value's own
    // `Clone`. The runner's count on the handle around it can't be seen from outside: the
    // instruction rows hold that.
    for path in [Path::Shared, Path::Handle] {
        let chain = cost(path, Shape::Chain(STAGES), LEN, TICKS);
        assert_eq!((chain.retains, chain.copies), (0, 0), "{path:?}");
        let one = cost(path, Shape::Takers(1), LEN, TICKS);
        assert_eq!((one.retains, one.copies), (0, 0), "{path:?}");
    }
    // Two nodes that each take one value can't both have it. The planner says so before
    // anything runs, and names the port.
    for path in Path::ALL {
        if path == Path::Vec {
            // The vector's stage reads through a reference, so two of them is fine.
            assert_eq!(plans(path, Shape::Takers(2)), Ok(()));
            continue;
        }
        let refused = plans(path, Shape::Takers(2)).expect_err("two takers of one value");
        assert!(
            refused.contains("CopyMissing") && refused.contains("value"),
            "{refused}"
        );
    }
}
