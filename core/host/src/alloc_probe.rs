//! The allocation probe (std, feature `alloc-probe`): counts allocations attributed to the runner,
//! to bodies and to the host. It only reads what the global counting allocator records, so a build
//! without the feature has none of this.

#![cfg(feature = "alloc-probe")]

use core::cell::Cell;

use daedalus_abi::counting::counted;

/// Who an allocation is attributed to.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum Scope {
    Runner,
    Body,
    Host,
}

std::thread_local! {
    static COUNTS: Cell<[u64; 3]> = const { Cell::new([0; 3]) };
}

/// Runs `body`, attributing its allocations to `scope`.
pub fn scoped<R>(scope: Scope, body: impl FnOnce() -> R) -> R {
    let (out, allocations) = counted(body);
    let index = match scope {
        Scope::Runner => 0,
        Scope::Body => 1,
        Scope::Host => 2,
    };
    COUNTS.with(|counts| {
        let mut current = counts.get();
        current[index] += allocations as u64;
        counts.set(current);
    });
    out
}

/// This thread's counts so far, per scope: runner, body, host.
pub fn counts() -> [u64; 3] {
    COUNTS.with(Cell::get)
}

/// Resets this thread's counts.
pub fn reset() {
    COUNTS.with(|counts| counts.set([0; 3]));
}
