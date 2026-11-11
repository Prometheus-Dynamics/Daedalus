//! Values a plugin made: they keep the generation open, share their bytes, and check plain data.

mod support;

use std::sync::Arc;
use std::sync::atomic::{AtomicBool, Ordering};

use daedalus_abi::counting::{CountingAlloc, counted};
use daedalus_abi::testing::{release, run_once};
use daedalus_abi::{BoundaryPort, Slot, SlotState, Slots, StepCx};
use daedalus_loader::{Loader, PodBlock, PodError, RegionIn};
use support::copy_fixture;

#[global_allocator]
static ALLOCATOR: CountingAlloc = CountingAlloc;

#[test]
fn shared_bytes_cross_as_handles_without_copying() {
    let mut loader = Loader::new();
    let plugin = loader.open(copy_fixture("owned-handles")).expect("opens");
    let bytes = plugin.share(&[1, 2, 3, 4]);
    let mut handles = Vec::with_capacity(100);
    let ((), allocations) = counted(|| {
        // Passing the value on, many times, clones a handle and copies nothing.
        for _ in 0..100 {
            handles.push(bytes.clone());
        }
    });
    assert_eq!(allocations, 0, "clones allocate nothing");
    assert_eq!(handles.len(), 100);
    assert_eq!(handles[7].bytes(), &[1, 2, 3, 4]);
}

#[test]
fn a_value_keeps_its_generation_until_it_is_dropped() {
    let mut loader = Loader::new();
    let plugin = loader.open(copy_fixture("owned-pin")).expect("opens");
    let bytes = plugin.share(&[9]);
    let probe = plugin.generation_probe();
    drop(plugin);
    assert!(
        probe.upgrade().is_some(),
        "the value holds the library open"
    );
    drop(bytes);
    assert!(
        probe.upgrade().is_none(),
        "the generation closes with its last value"
    );
}

#[test]
fn a_reload_closes_the_old_generation_when_its_values_are_gone_across_threads() {
    let mut loader = Loader::new();
    let first = loader.open(copy_fixture("threads-1")).expect("opens");
    let probe = first.generation_probe();
    let values: Vec<_> = (0..8).map(|i| first.share(&[i as u8])).collect();
    drop(first);
    let finished = Arc::new(AtomicBool::new(false));
    let threads: Vec<_> = values
        .into_iter()
        .map(|value| {
            let finished = finished.clone();
            std::thread::spawn(move || {
                let copies: Vec<_> = (0..50).map(|_| value.clone()).collect();
                drop(value);
                drop(copies);
                finished.store(true, Ordering::SeqCst);
            })
        })
        .collect();
    for thread in threads {
        thread.join().expect("the thread finishes");
    }
    assert!(finished.load(Ordering::SeqCst));
    assert!(
        probe.upgrade().is_none(),
        "the old generation closed after its values dropped"
    );
    let second = loader.open(copy_fixture("threads-2")).expect("reloads");
    assert_eq!(second.generation().number(), 2);
}

#[test]
fn another_layout_under_the_same_key_is_a_typed_error_naming_both() {
    let mut loader = Loader::new();
    let _plugin = loader.open(copy_fixture("pod")).expect("opens");
    let block = PodBlock::new("demo::Point", 0xAAAA, &[0; 8]);
    assert_eq!(
        block
            .accept("demo::Point", 0xAAAA)
            .expect("same layout")
            .len(),
        8
    );
    match block.accept("demo::Point", 0xBBBB) {
        Err(error @ PodError::Layout { .. }) => {
            let text = error.to_string();
            assert!(
                text.contains("0x000000000000aaaa") && text.contains("0x000000000000bbbb"),
                "{text}"
            );
        }
        other => panic!("expected a layout error, got {other:?}"),
    }
    assert!(matches!(
        block.accept("demo::Other", 0xAAAA),
        Err(PodError::Key { .. })
    ));
}

#[test]
fn a_plugin_value_passes_through_a_plugin_step_and_back() {
    let mut loader = Loader::new();
    let plugin = loader.open(copy_fixture("roundtrip")).expect("opens");
    let backend = plugin.backend("fixture.cpu").expect("backend");
    let nodes = ["fixture.pass"];
    let port = BoundaryPort {
        node: 0,
        port: 0,
        reserved: 0,
        type_index: 0,
        slot: 0,
    };
    let region = RegionIn {
        nodes: &nodes,
        inputs: &[port],
        outputs: &[port],
    };
    let mut units = backend.compile(&region).expect("compiles");
    let mut step = units.pop().expect("unit").step;
    // Host value, held by the plugin's value, goes into the input cell as its bytes.
    let value = plugin.share(b"plugin-bytes");
    let data = value.bytes().as_ptr().cast_mut();
    let mut cells = [
        Slot {
            state: SlotState::Full as u32,
            data,
            len: value.bytes().len(),
            cap: value.bytes().len(),
            ..Slot::EMPTY
        },
        Slot::EMPTY,
    ];
    let mut cx = StepCx::new();
    assert!(matches!(
        run_once(&mut step, &mut Slots::of(&mut cells), &mut cx),
        daedalus_abi::StepEnd::Done
    ));
    let back = cells[1].bytes().expect("an output");
    assert_eq!(back, b"plugin-bytes");
    release(step);
}
