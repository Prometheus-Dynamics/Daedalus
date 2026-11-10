//! Blocks, the pool and the two moves. The memfd tests run anywhere Linux does. The dma-heap
//! test needs `/dev/dma_heap/system` and says why it skipped when it isn't there.

#![cfg(target_os = "linux")]
#![allow(clippy::print_stderr)]

use std::sync::Arc;

use daedalus_abi::counting::counted;
use daedalus_abi::testing::{release, run_once};
use daedalus_abi::{Slot, Slots, StepCx, StepEnd};
use daedalus_backend::{Backend, CompileCx, TransferCost};
use daedalus_conformance::conformance;
use daedalus_dmabuf::{
    Access, Block, COPY, DmabufBackend, Error, HOST_SPACE, KEPT, MAP, Pool, SPACE, Source,
};
use daedalus_runner::{drop_owned, put_shared, put_typed, take_typed};
use daedalus_types::{SpaceIndex, TypeIndex};

// The conformance suite installs the counting allocator.
conformance!(DmabufBackend::default());

fn pattern(len: usize) -> Vec<u8> {
    (0..len).map(|at| (at * 13 + 5) as u8).collect()
}

#[test]
fn memfd_blocks_round_trip_bytes_and_identity() {
    let pool = Pool::new(Source::Memfd);
    let block = pool.take(10_000).expect("a block");
    assert_eq!((block.len(), block.is_empty()), (10_000, false));
    {
        let guard = block.access(Access::Write).expect("access");
        guard.write(0, &pattern(10_000)).expect("in range");
        assert_eq!(
            guard.write(9_999, &[0, 0]),
            Err(Error::Range),
            "past what was asked for"
        );
    }
    let mut seen = vec![0; 4_000];
    let guard = block.access(Access::Read).expect("access");
    guard.read(6_000, &mut seen).expect("in range");
    assert_eq!(seen, pattern(10_000)[6_000..]);
    assert_eq!(
        guard.write(0, &[1]),
        Err(Error::Range),
        "read access doesn't write"
    );
    assert_eq!(guard.read(9_000, &mut seen), Err(Error::Range));
    drop(guard);

    // The fd names the same memory, and another block is other memory.
    let again = daedalus_dma_sys::DmaBufRef::from_fd(block.as_fd());
    assert_eq!(again.identity(), block.identity());
    let mapped = again.map_read_only().expect("a mapping");
    mapped.read(6_000, &mut seen).expect("in range");
    assert_eq!(
        seen,
        pattern(10_000)[6_000..],
        "a second mapping sees the bytes"
    );
    assert_ne!(
        pool.take(10_000).expect("a block").identity(),
        block.identity()
    );
    assert_eq!(pool.take(0).err(), Some(Error::Range));
}

#[test]
fn the_pool_reuses_up_to_four_idle_blocks_and_drops_the_fifth() {
    let pool = Pool::new(Source::Memfd);
    let five: Vec<Arc<Block>> = (0..KEPT + 1)
        .map(|_| pool.take(4096).expect("a block"))
        .collect();
    let first = five[0].identity();
    let watch: Vec<_> = five.iter().map(Arc::downgrade).collect();
    let stats = pool.stats();
    assert_eq!((stats.allocated, stats.lent, stats.idle), (5, 5, 0));

    // All five come back. Four are kept, and the fifth is let go, once.
    drop(five);
    let stats = pool.stats();
    assert_eq!((stats.idle, stats.lent, stats.dropped), (KEPT, 0, 1));
    let gone = watch
        .iter()
        .filter(|block| block.upgrade().is_none())
        .count();
    assert_eq!(gone, 1, "one block is gone, and the other four are kept");
    assert_eq!(pool.stats().dropped, 1, "and it was let go exactly once");

    // The next four takes are served by the kept blocks, and a fifth has to be made.
    let again: Vec<Arc<Block>> = (0..KEPT + 1)
        .map(|_| pool.take(4096).expect("a block"))
        .collect();
    let stats = pool.stats();
    assert_eq!((stats.reused, stats.allocated), (4, 6));
    assert!(again.iter().any(|block| block.identity() == first));
    // Another size has its own four.
    drop(pool.take(8192).expect("a block"));
    assert_eq!(pool.stats().idle, 1);

    // A block still held outlives the pool, and the pool's idle blocks go with it.
    let held = Arc::clone(&again[0]);
    let idle = Arc::downgrade(&pool.take(8192).expect("the idle one"));
    drop(again);
    drop(pool);
    assert!(
        idle.upgrade().is_none(),
        "dropping the pool released what it held"
    );
    assert_eq!(held.len(), 4096);
    assert!(held.access(Access::Read).is_ok());
}

#[test]
fn steady_state_reuse_allocates_nothing() {
    let pool = Pool::new(Source::Memfd);
    let bytes = pattern(4096);
    let cycle = || {
        let (a, b) = (
            pool.take(4096).expect("a block"),
            pool.take(4096).expect("a block"),
        );
        a.access(Access::Write)
            .expect("access")
            .write(0, &bytes)
            .expect("in range");
        let mut seen = [0; 16];
        b.access(Access::Read)
            .expect("access")
            .read(0, &mut seen)
            .expect("in range");
    };
    for _ in 0..8 {
        cycle();
    }
    let ((), allocated) = counted(|| {
        for _ in 0..1000 {
            cycle();
        }
    });
    assert_eq!(allocated, 0);
    let stats = pool.stats();
    assert_eq!((stats.allocated, stats.reused, stats.dropped), (2, 2014, 0));
}

#[test]
fn a_dma_heap_block_round_trips_through_its_guard() {
    let pool = Pool::new(Source::Heap("system".into()));
    let block = match pool.take(1 << 20) {
        Ok(block) => block,
        Err(error) => {
            eprintln!("skipped: /dev/dma_heap/system: {error}");
            return;
        }
    };
    block
        .access(Access::Write)
        .expect("begin")
        .write(4096, &pattern(8192))
        .expect("in range");
    let mut seen = vec![0; 8192];
    block
        .access(Access::Read)
        .expect("begin")
        .read(4096, &mut seen)
        .expect("in range");
    assert_eq!(seen, pattern(8192));
    drop(block);
    assert_eq!(
        pool.take(1 << 20).map(|_| pool.stats().reused).ok(),
        Some(1)
    );
    assert!(
        Pool::new(Source::Heap("no-such-heap".into()))
            .take(4096)
            .is_err()
    );
}

fn cx() -> CompileCx<'static> {
    CompileCx {
        types: &[],
        consts: &[],
        spaces: &[],
        bodies: &[],
        edges: &[],
        host_inputs: &[],
        host_outputs: &[],
        fifo_capacity: 1,
        merge_allowed: true,
        freeze_params: false,
    }
}

#[test]
fn the_backend_declares_one_space_and_two_moves() {
    let backend = DmabufBackend::default();
    assert_eq!(backend.id(), "dmabuf");
    assert_eq!(backend.source(), &Source::Memfd);
    let spaces = backend.memory();
    assert_eq!(spaces.len(), 1);
    assert_eq!(spaces[0].id, SPACE);
    let moves: Vec<_> = backend
        .transfers()
        .iter()
        .map(|t| {
            (
                t.id,
                t.from.to_string(),
                t.to.to_string(),
                t.cost,
                t.key.is_none(),
            )
        })
        .collect();
    assert_eq!(
        moves,
        [
            (
                MAP,
                SPACE.to_string(),
                HOST_SPACE.to_string(),
                TransferCost::Import,
                true
            ),
            (
                COPY,
                HOST_SPACE.to_string(),
                SPACE.to_string(),
                TransferCost::Copy,
                true
            ),
        ]
    );
}

#[test]
fn the_moves_carry_bytes_into_a_block_and_a_block_to_the_cpu() {
    let backend = DmabufBackend::default();
    let transfers = backend.transfers();
    let mut copy = (transfers[1].make)(&cx()).expect("a step");
    let mut map = (transfers[0].make)(&cx()).expect("a step");
    let (ty, space) = (TypeIndex(3), SpaceIndex(0));
    let bytes = pattern(5000);

    // CPU bytes go into a block.
    let mut cells = [Slot::EMPTY; 2];
    put_shared(&mut cells[0], ty, space, Arc::from(bytes.as_slice()));
    let end = run_once(&mut copy, &mut Slots::of(&mut cells), &mut StepCx::new());
    assert!(matches!(end, StepEnd::Done));
    drop_owned(cells[0]);

    // The block goes on to the CPU side, where a guard reads it.
    let mut next = [cells[1], Slot::EMPTY];
    let end = run_once(&mut map, &mut Slots::of(&mut next), &mut StepCx::new());
    assert!(matches!(end, StepEnd::Done));
    let block = take_typed::<Block>(&mut next[1]).expect("a block came out");
    let mut seen = vec![0; 5000];
    block
        .access(Access::Read)
        .expect("access")
        .read(0, &mut seen)
        .expect("in range");
    assert_eq!(seen, bytes);

    // A block that arrives at the copy is passed through, not copied.
    let before = block.identity();
    let mut cells = [Slot::EMPTY; 2];
    put_typed(&mut cells[0], ty, space, block);
    assert!(matches!(
        run_once(&mut copy, &mut Slots::of(&mut cells), &mut StepCx::new()),
        StepEnd::Done
    ));
    let same = take_typed::<Block>(&mut cells[1]).expect("a block came out");
    assert_eq!(same.identity(), before);

    // Once warm, a copy into a block allocates nothing.
    let shared: Arc<[u8]> = Arc::from(bytes.as_slice());
    let mut round = || {
        let mut cells = [Slot::EMPTY; 2];
        put_shared(&mut cells[0], ty, space, Arc::clone(&shared));
        let end = run_once(&mut copy, &mut Slots::of(&mut cells), &mut StepCx::new());
        drop_owned(cells[0]);
        drop_owned(cells[1]);
        end
    };
    for _ in 0..4 {
        assert!(matches!(round(), StepEnd::Done));
    }
    let (end, allocated) = counted(&mut round);
    assert!(matches!(end, StepEnd::Done));
    assert_eq!(allocated, 0, "the block and its reference are both reused");

    // What isn't a block faults, and says so.
    let mut cells = [Slot::EMPTY; 2];
    let mut step_cx = StepCx::new();
    assert!(matches!(
        run_once(&mut map, &mut Slots::of(&mut cells), &mut step_cx),
        StepEnd::Fault
    ));
    assert!(step_cx.reason().contains("isn't a block"));
    release(copy);
    release(map);
}
