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

/// The design's goal is that a value handed on costs nothing, and it isn't met: a CPU node
/// that takes a value and returns it has the result put behind a new handle, once a stage.
/// The bytes are never copied. When this fails because the count fell, lower it here and in
/// `testing/budgets/budgets.toml`.
#[test]
fn a_value_handed_on_allocates_once_a_stage_today() {
    let _turn = turn();
    let handed_on = Cost {
        allocations: u64::from(STAGES),
        copies: 0,
        retains: 0,
    };
    assert_eq!(
        cost(Path::Handle, Shape::Chain(STAGES), LEN, TICKS),
        handed_on
    );
    assert_eq!(
        cost(Path::Shared, Shape::Chain(STAGES), LEN, TICKS),
        handed_on
    );
    // It is a cost of each stage, not of the graph.
    let one = cost(Path::Handle, Shape::Chain(1), LEN, TICKS);
    assert_eq!(one.allocations, 1);
}

#[test]
fn vec_path_allocates_per_tick() {
    let _turn = turn();
    // Each stage makes its own vector: at least the vector and its handle, and the bytes are
    // copied once a stage. The contrast with the two paths above is real.
    let vec = cost(Path::Vec, Shape::Chain(STAGES), LEN, TICKS);
    assert!(vec.allocations >= 2 * u64::from(STAGES), "{vec:?}");
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
