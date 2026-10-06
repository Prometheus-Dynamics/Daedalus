//! Counting global allocator that attributes heap allocations to node handlers, the runtime or
//! host-bridge calls (feature `alloc-probe`).
//!
//! Install it in the application (or test/bench binary) whose allocations should be counted:
//!
//! ```ignore
//! #[global_allocator]
//! static ALLOC: daedalus::alloc_probe::CountingAllocator =
//!     daedalus::alloc_probe::CountingAllocator::system();
//! ```
//!
//! Every allocation (and reallocation) is counted under the [`AllocScope`] of the thread that
//! makes it. The executor marks its threads [`AllocScope::Runtime`] while a tick runs and
//! [`AllocScope::Node`] around each handler call; host-bridge feeds and takes are
//! [`AllocScope::Host`]; everything else is [`AllocScope::Other`]. Scopes are only switched once
//! the allocator has served an allocation, so a build with the feature but without the allocator
//! installed pays one relaxed load per handler call. Counters are process-wide: measure one
//! graph at a time. `HostGraph::frame_overhead` reports them per tick.

use core::cell::Cell;
use core::sync::atomic::{AtomicBool, AtomicU64, Ordering::Relaxed};
use std::alloc::{GlobalAlloc, Layout, System};

/// Who a thread is allocating for.
#[derive(Clone, Copy, Debug, Default, PartialEq, Eq, Hash)]
#[repr(u8)]
pub enum AllocScope {
    /// Outside any Daedalus call (application code, other threads).
    #[default]
    Other = 0,
    /// Host-bridge feeds and takes (`HostBridgeHandle::feed_payload`, `try_pop_payload`, ...).
    Host = 1,
    /// Executor bookkeeping while a tick runs (scheduling, queues, adapters, host I/O).
    Runtime = 2,
    /// A node handler call.
    Node = 3,
}

const SCOPES: usize = 4;

static INSTALLED: AtomicBool = AtomicBool::new(false);
static COUNTS: [AtomicU64; SCOPES] = [const { AtomicU64::new(0) }; SCOPES];
static BYTES: [AtomicU64; SCOPES] = [const { AtomicU64::new(0) }; SCOPES];

thread_local! {
    // `const`-initialized and without a destructor, so the allocator can read it at any time.
    static SCOPE: Cell<u8> = const { Cell::new(0) };
}

/// Global allocator wrapper counting allocations per [`AllocScope`]; forwards to `A`.
#[derive(Clone, Copy, Debug, Default)]
pub struct CountingAllocator<A = System> {
    inner: A,
}

impl CountingAllocator<System> {
    /// Counting wrapper around the system allocator.
    pub const fn system() -> Self {
        Self { inner: System }
    }
}

impl<A> CountingAllocator<A> {
    /// Counting wrapper around `inner`.
    pub const fn new(inner: A) -> Self {
        Self { inner }
    }
}

#[inline]
fn record(bytes: usize) {
    if !INSTALLED.load(Relaxed) {
        INSTALLED.store(true, Relaxed);
    }
    let scope = SCOPE.try_with(Cell::get).unwrap_or(0) as usize;
    COUNTS[scope].fetch_add(1, Relaxed);
    BYTES[scope].fetch_add(bytes as u64, Relaxed);
}

// Safety: every call forwards to `A` unchanged; counting touches only atomics and a
// destructor-free thread-local, neither of which allocates.
unsafe impl<A: GlobalAlloc> GlobalAlloc for CountingAllocator<A> {
    unsafe fn alloc(&self, layout: Layout) -> *mut u8 {
        record(layout.size());
        // Safety: the caller upholds `GlobalAlloc::alloc`'s contract.
        unsafe { self.inner.alloc(layout) }
    }

    unsafe fn alloc_zeroed(&self, layout: Layout) -> *mut u8 {
        record(layout.size());
        // Safety: as above.
        unsafe { self.inner.alloc_zeroed(layout) }
    }

    unsafe fn dealloc(&self, ptr: *mut u8, layout: Layout) {
        // Safety: as above.
        unsafe { self.inner.dealloc(ptr, layout) }
    }

    unsafe fn realloc(&self, ptr: *mut u8, layout: Layout, new_size: usize) -> *mut u8 {
        record(new_size);
        // Safety: as above.
        unsafe { self.inner.realloc(ptr, layout, new_size) }
    }
}

/// Allocation counters per scope (counts and requested bytes), as read by [`counts`].
#[derive(Clone, Copy, Debug, Default, PartialEq, Eq, serde::Serialize, serde::Deserialize)]
pub struct AllocCounts {
    pub other: u64,
    pub host: u64,
    pub runtime: u64,
    pub node: u64,
    pub other_bytes: u64,
    pub host_bytes: u64,
    pub runtime_bytes: u64,
    pub node_bytes: u64,
}

impl AllocCounts {
    /// Counts made between `earlier` and `self`.
    pub fn since(&self, earlier: &AllocCounts) -> AllocCounts {
        AllocCounts {
            other: self.other.saturating_sub(earlier.other),
            host: self.host.saturating_sub(earlier.host),
            runtime: self.runtime.saturating_sub(earlier.runtime),
            node: self.node.saturating_sub(earlier.node),
            other_bytes: self.other_bytes.saturating_sub(earlier.other_bytes),
            host_bytes: self.host_bytes.saturating_sub(earlier.host_bytes),
            runtime_bytes: self.runtime_bytes.saturating_sub(earlier.runtime_bytes),
            node_bytes: self.node_bytes.saturating_sub(earlier.node_bytes),
        }
    }

    /// Allocations in every scope.
    pub fn total(&self) -> u64 {
        self.other + self.host + self.runtime + self.node
    }
}

/// Process-wide counters since start.
pub fn counts() -> AllocCounts {
    let count = |scope: AllocScope| COUNTS[scope as usize].load(Relaxed);
    let bytes = |scope: AllocScope| BYTES[scope as usize].load(Relaxed);
    AllocCounts {
        other: count(AllocScope::Other),
        host: count(AllocScope::Host),
        runtime: count(AllocScope::Runtime),
        node: count(AllocScope::Node),
        other_bytes: bytes(AllocScope::Other),
        host_bytes: bytes(AllocScope::Host),
        runtime_bytes: bytes(AllocScope::Runtime),
        node_bytes: bytes(AllocScope::Node),
    }
}

/// Whether a [`CountingAllocator`] is the global allocator (it has served an allocation).
pub fn is_installed() -> bool {
    INSTALLED.load(Relaxed)
}

/// The current thread's scope.
pub fn current_scope() -> AllocScope {
    match SCOPE.try_with(Cell::get).unwrap_or(0) {
        1 => AllocScope::Host,
        2 => AllocScope::Runtime,
        3 => AllocScope::Node,
        _ => AllocScope::Other,
    }
}

/// Restores the previous scope of its thread when dropped (see [`enter`]).
#[must_use = "the scope ends when the guard is dropped"]
pub struct ScopeGuard {
    previous: Option<u8>,
}

/// Attribute this thread's allocations to `scope` until the guard drops. A no-op while no
/// [`CountingAllocator`] is installed.
#[inline]
pub fn enter(scope: AllocScope) -> ScopeGuard {
    if !INSTALLED.load(Relaxed) {
        return ScopeGuard { previous: None };
    }
    ScopeGuard {
        previous: SCOPE.try_with(|cell| cell.replace(scope as u8)).ok(),
    }
}

impl Drop for ScopeGuard {
    #[inline]
    fn drop(&mut self) {
        if let Some(previous) = self.previous {
            let _ = SCOPE.try_with(|cell| cell.set(previous));
        }
    }
}
