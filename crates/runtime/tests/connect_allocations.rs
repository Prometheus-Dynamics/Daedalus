//! Heap allocations per `connect` while building a graph, counted on the calling thread only.
//! A connect must not copy the node registry: with a registry of `NODES` declarations, one
//! connect (node to node, and through the host bridge) stays far below one allocation per
//! declaration. Before the registry was shared, each connect cloned all of it.
#![allow(clippy::print_stderr)]

use std::alloc::{GlobalAlloc, Layout, System};
use std::cell::Cell;

use daedalus_registry::capability::{CapabilityRegistry, NodeDecl, PortDecl};
use daedalus_runtime::graph_builder::GraphBuilder;

struct Counting;

thread_local! {
    static COUNTING: Cell<bool> = const { Cell::new(false) };
    static ALLOCS: Cell<usize> = const { Cell::new(0) };
}

// SAFETY: forwards to `System`, counting this thread's allocations while `COUNTING` is set.
unsafe impl GlobalAlloc for Counting {
    unsafe fn alloc(&self, layout: Layout) -> *mut u8 {
        let _ = COUNTING.try_with(|on| {
            if on.get() {
                let _ = ALLOCS.try_with(|n| n.set(n.get() + 1));
            }
        });
        // SAFETY: as `System::alloc`.
        unsafe { System.alloc(layout) }
    }
    unsafe fn dealloc(&self, ptr: *mut u8, layout: Layout) {
        // SAFETY: as `System::dealloc`.
        unsafe { System.dealloc(ptr, layout) }
    }
}

#[global_allocator]
static ALLOC: Counting = Counting;

/// Allocations this thread makes while `f` runs, and its result.
fn count<T>(f: impl FnOnce() -> T) -> (usize, T) {
    ALLOCS.with(|n| n.set(0));
    COUNTING.with(|on| on.set(true));
    let out = f();
    COUNTING.with(|on| on.set(false));
    (ALLOCS.with(Cell::get), out)
}

const NODES: usize = 2000;
const BOOL: &str = "typeexpr:{\"Scalar\":\"Bool\"}";

fn registry() -> CapabilityRegistry {
    let mut capabilities = CapabilityRegistry::new();
    for i in 0..NODES {
        capabilities
            .register_node(
                NodeDecl::new(format!("demo.n{i}"))
                    .input(PortDecl::new("in", BOOL))
                    .output(PortDecl::new("out", BOOL)),
            )
            .unwrap();
    }
    capabilities
}

#[test]
fn connect_does_not_copy_the_registry() {
    let builder = GraphBuilder::new(registry())
        .node_from_id("demo.n0", "a")
        .node_from_id("demo.n1", "b");

    let (nodes, builder) = count(|| builder.connect_by_id(("a", "out"), ("b", "in")));
    let (host_in, builder) = count(|| builder.connect_by_id(("host", "x"), ("a", "in")));
    let (host_out, builder) = count(|| builder.connect_by_id(("b", "out"), ("host", "y")));
    eprintln!("allocations per connect: node {nodes}, host in {host_in}, host out {host_out}");

    let bound = NODES / 10;
    assert!(nodes < bound, "node-to-node connect allocated {nodes}");
    assert!(host_in < bound, "host-input connect allocated {host_in}");
    assert!(host_out < bound, "host-output connect allocated {host_out}");
    assert_eq!(builder.build().edges.len(), 3);
}
