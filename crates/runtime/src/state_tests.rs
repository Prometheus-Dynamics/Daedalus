use std::collections::BTreeMap;
use std::panic::{self, AssertUnwindSafe};
use std::sync::{
    Arc,
    atomic::{AtomicBool, AtomicUsize, Ordering},
    mpsc,
};
use std::thread;
use std::time::Duration;

use super::{
    ExecutionContext, ManagedResource, NodeResourceSnapshot, ResourceClass, ResourceLifecycleEvent,
    StateError, StateStore,
};

#[test]
fn begin_resource_frame_clears_only_frame_scratch_live_bytes() {
    let state = StateStore::default();
    state
        .record_node_resource_usage("node", "scratch", ResourceClass::FrameScratch, 64, 128)
        .unwrap();
    state
        .record_node_resource_usage("node", "cache", ResourceClass::WarmCache, 32, 96)
        .unwrap();

    state.begin_node_resource_frame("node").unwrap();

    let snapshot = state.snapshot_node_resources("node").unwrap();
    assert_eq!(
        snapshot,
        NodeResourceSnapshot {
            frame_scratch: super::ResourceUsage {
                live_bytes: 0,
                retained_bytes: 128,
                touched_bytes: 0,
                allocation_events: 0,
            },
            warm_cache: super::ResourceUsage {
                live_bytes: 32,
                retained_bytes: 96,
                touched_bytes: 32,
                allocation_events: 0,
            },
            persistent_state: super::ResourceUsage::default(),
        }
    );
}

#[test]
fn snapshot_node_resources_aggregates_by_class() {
    let state = StateStore::default();
    state
        .record_node_resource_usage("node", "scratch-a", ResourceClass::FrameScratch, 10, 20)
        .unwrap();
    state
        .record_node_resource_usage("node", "scratch-b", ResourceClass::FrameScratch, 5, 12)
        .unwrap();
    state
        .record_node_resource_usage("node", "persistent", ResourceClass::PersistentState, 7, 9)
        .unwrap();

    let snapshot = state.snapshot_node_resources("node").unwrap();
    assert_eq!(snapshot.frame_scratch.live_bytes, 15);
    assert_eq!(snapshot.frame_scratch.retained_bytes, 32);
    assert_eq!(snapshot.persistent_state.live_bytes, 7);
    assert_eq!(snapshot.persistent_state.retained_bytes, 9);
}

#[test]
fn memory_pressure_compacts_caches_and_drops_frame_scratch() {
    let state = StateStore::default();
    state
        .record_node_resource_usage("node", "scratch", ResourceClass::FrameScratch, 10, 20)
        .unwrap();
    state
        .record_node_resource_usage("node", "cache", ResourceClass::WarmCache, 8, 30)
        .unwrap();

    state
        .apply_node_resource_lifecycle("node", ResourceLifecycleEvent::MemoryPressure)
        .unwrap();

    let snapshot = state.snapshot_node_resources("node").unwrap();
    assert_eq!(snapshot.frame_scratch.live_bytes, 0);
    assert_eq!(snapshot.frame_scratch.retained_bytes, 0);
    assert_eq!(snapshot.warm_cache.live_bytes, 8);
    assert_eq!(snapshot.warm_cache.retained_bytes, 8);
}

#[test]
fn stop_lifecycle_removes_node_resources() {
    let state = StateStore::default();
    state
        .record_node_resource_usage("node", "persistent", ResourceClass::PersistentState, 5, 9)
        .unwrap();

    state
        .apply_node_resource_lifecycle("node", ResourceLifecycleEvent::Stop)
        .unwrap();

    assert_eq!(
        state.snapshot_node_resources("node").unwrap(),
        NodeResourceSnapshot::default()
    );
}

struct TestManagedResource {
    live: u64,
    retained: u64,
    before_frame_runs: Arc<AtomicUsize>,
    after_frame_runs: Arc<AtomicUsize>,
    memory_pressure_runs: Arc<AtomicUsize>,
    idle_runs: Arc<AtomicUsize>,
    stop_runs: Arc<AtomicUsize>,
}

impl ManagedResource for TestManagedResource {
    fn live_bytes(&self) -> u64 {
        self.live
    }

    fn retained_bytes(&self) -> u64 {
        self.retained
    }

    fn before_frame(&mut self) {
        self.live = 0;
        self.before_frame_runs.fetch_add(1, Ordering::SeqCst);
    }

    fn after_frame(&mut self) {
        self.after_frame_runs.fetch_add(1, Ordering::SeqCst);
    }

    fn on_memory_pressure(&mut self) {
        self.retained = self.live;
        self.memory_pressure_runs.fetch_add(1, Ordering::SeqCst);
    }

    fn on_idle(&mut self) {
        self.live = 0;
        self.idle_runs.fetch_add(1, Ordering::SeqCst);
    }

    fn on_stop(&mut self) {
        self.stop_runs.fetch_add(1, Ordering::SeqCst);
    }
}

struct EmptyManagedResource;

impl ManagedResource for EmptyManagedResource {
    fn live_bytes(&self) -> u64 {
        0
    }

    fn retained_bytes(&self) -> u64 {
        0
    }
}

fn test_context(state: StateStore) -> ExecutionContext {
    ExecutionContext {
        state,
        node_id: Arc::<str>::from("node"),
        metadata: Arc::new(BTreeMap::new()),
        graph_metadata: Arc::new(BTreeMap::new()),
        capabilities: Arc::new(crate::capabilities::CapabilityRegistry::new()),
        #[cfg(feature = "gpu")]
        gpu: None,
    }
}

#[test]
fn managed_resources_are_reused_and_snapshotted() {
    let state = StateStore::default();
    let ctx = test_context(state);
    let before = Arc::new(AtomicUsize::new(0));
    let after = Arc::new(AtomicUsize::new(0));
    let pressure = Arc::new(AtomicUsize::new(0));
    let idle = Arc::new(AtomicUsize::new(0));
    let stop = Arc::new(AtomicUsize::new(0));

    ctx.with_warm_cache(
        "cache",
        || TestManagedResource {
            live: 10,
            retained: 20,
            before_frame_runs: before.clone(),
            after_frame_runs: after.clone(),
            memory_pressure_runs: pressure.clone(),
            idle_runs: idle.clone(),
            stop_runs: stop.clone(),
        },
        |resource| {
            resource.live = 18;
            resource.retained = 30;
        },
    )
    .unwrap();

    ctx.with_warm_cache::<TestManagedResource, _, _, _>(
        "cache",
        || unreachable!("managed resource should already exist"),
        |resource| {
            resource.live = 22;
        },
    )
    .unwrap();

    let snapshot = ctx.snapshot_resources().unwrap();
    assert_eq!(snapshot.warm_cache.live_bytes, 22);
    assert_eq!(snapshot.warm_cache.retained_bytes, 30);

    ctx.begin_resource_frame().unwrap();
    ctx.end_resource_frame().unwrap();
    ctx.apply_memory_pressure().unwrap();
    ctx.notify_idle().unwrap();
    ctx.release_resources().unwrap();

    assert_eq!(before.load(Ordering::SeqCst), 1);
    assert_eq!(after.load(Ordering::SeqCst), 1);
    assert_eq!(pressure.load(Ordering::SeqCst), 1);
    assert_eq!(idle.load(Ordering::SeqCst), 1);
    assert_eq!(stop.load(Ordering::SeqCst), 1);
    assert_eq!(
        ctx.snapshot_resources().unwrap(),
        NodeResourceSnapshot::default()
    );
}

#[test]
fn independent_node_resource_callbacks_can_run_concurrently() {
    let state = StateStore::default();
    let release = Arc::new(AtomicBool::new(false));
    let (entered, entered_rx) = mpsc::channel();

    let mut workers = Vec::new();
    for node_id in ["a", "b"] {
        let state = state.clone();
        let release = Arc::clone(&release);
        let entered = entered.clone();
        workers.push(thread::spawn(move || {
            state
                .with_node_resource::<EmptyManagedResource, _, _, _>(
                    node_id,
                    "resource",
                    ResourceClass::WarmCache,
                    || EmptyManagedResource,
                    |_| {
                        entered.send(()).expect("receiver should stay open");
                        while !release.load(Ordering::SeqCst) {
                            thread::yield_now();
                        }
                    },
                )
                .unwrap();
        }));
    }
    drop(entered);

    let mut entered_count = 0;
    for _ in 0..2 {
        if entered_rx.recv_timeout(Duration::from_secs(1)).is_ok() {
            entered_count += 1;
        }
    }

    release.store(true, Ordering::SeqCst);
    for worker in workers {
        worker.join().expect("worker thread panicked");
    }

    assert_eq!(
        entered_count, 2,
        "independent node resource callbacks should not serialize behind the resource registry lock"
    );
}

#[test]
fn reentrant_same_resource_access_returns_error_instead_of_deadlocking() {
    let state = StateStore::default();
    let nested_state = state.clone();
    let nested_returned = Arc::new(AtomicBool::new(false));
    let nested_returned_for_closure = nested_returned.clone();

    state
        .with_node_resource::<EmptyManagedResource, _, _, _>(
            "node",
            "resource",
            ResourceClass::WarmCache,
            || EmptyManagedResource,
            |_| {
                let nested = nested_state
                    .with_node_resource::<EmptyManagedResource, _, _, _>(
                        "node",
                        "resource",
                        ResourceClass::WarmCache,
                        || EmptyManagedResource,
                        |_| {},
                    )
                    .expect_err("same resource should already be borrowed");
                assert!(matches!(nested, StateError::ResourceAlreadyBorrowed { .. }));
                nested_returned_for_closure.store(true, Ordering::SeqCst);
            },
        )
        .unwrap();

    assert!(nested_returned.load(Ordering::SeqCst));
}

#[test]
fn managed_resource_is_restored_after_panic() {
    let state = StateStore::default();

    let panic_result = panic::catch_unwind(AssertUnwindSafe(|| {
        state
            .with_node_resource::<EmptyManagedResource, _, _, _>(
                "node",
                "resource",
                ResourceClass::WarmCache,
                || EmptyManagedResource,
                |_| panic!("simulated resource user panic"),
            )
            .unwrap();
    }));

    assert!(panic_result.is_err());
    state
        .with_node_resource::<EmptyManagedResource, _, _, _>(
            "node",
            "resource",
            ResourceClass::WarmCache,
            || EmptyManagedResource,
            |_| {},
        )
        .expect("resource should be restored after panic");
}

#[test]
fn native_type_mismatch_returns_typed_error() {
    let state = StateStore::default();
    state.set_native("value", 7_u32).unwrap();

    let err = state
        .get_native::<String>("value")
        .expect_err("wrong native type should be reported");

    assert!(matches!(err, StateError::StateTypeMismatch { .. }));
}

#[test]
fn managed_byte_buffer_helpers_track_touch_and_reuse_capacity() {
    let state = StateStore::default();
    let ctx = test_context(state);

    ctx.with_frame_scratch_bytes("scratch", 16, |bytes| {
        bytes[0] = 7;
        bytes[15] = 9;
    })
    .unwrap();

    let first = ctx.snapshot_resources().unwrap();
    let first_retained = first.frame_scratch.retained_bytes;
    assert_eq!(first.frame_scratch.live_bytes, 16);
    assert_eq!(first.frame_scratch.touched_bytes, 16);
    assert_eq!(first.frame_scratch.allocation_events, 1);
    assert!(first_retained >= 16);

    ctx.begin_resource_frame().unwrap();

    let reset = ctx.snapshot_resources().unwrap();
    assert_eq!(reset.frame_scratch.live_bytes, 0);
    assert_eq!(reset.frame_scratch.touched_bytes, 0);
    assert_eq!(reset.frame_scratch.retained_bytes, first_retained);
    assert_eq!(reset.frame_scratch.allocation_events, 1);

    ctx.with_frame_scratch_bytes("scratch", 8, |bytes| bytes.fill(0xAB))
        .unwrap();

    let second = ctx.snapshot_resources().unwrap();
    assert_eq!(second.frame_scratch.live_bytes, 8);
    assert_eq!(second.frame_scratch.touched_bytes, 8);
    assert_eq!(second.frame_scratch.retained_bytes, first_retained);
    assert_eq!(second.frame_scratch.allocation_events, 1);
}
