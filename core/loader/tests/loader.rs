//! Opening, joining, running and retiring plugins built as dynamic libraries.

mod support;

use std::path::Path;
use std::sync::atomic::{AtomicBool, Ordering};

use daedalus_abi::testing::{release, run_once};
use daedalus_abi::{BoundaryPort, Slot, SlotState, Slots, Status, StepCx, StepEnd};
use daedalus_loader::{LoadError, Loader, RegionIn, Retry, load_with_retry};
use daedalus_registry::Registry;
use support::{copy_fixture, fixture_library};

fn port(node: u32) -> BoundaryPort {
    BoundaryPort {
        node,
        port: 0,
        reserved: 0,
        type_index: 0,
        slot: 0,
    }
}

fn region_of(node: &'static str) -> (Vec<&'static str>, Vec<BoundaryPort>, Vec<BoundaryPort>) {
    (vec![node], vec![port(0)], vec![port(0)])
}

#[test]
fn a_plugin_declares_its_nodes_and_backend() {
    let mut loader = Loader::new();
    let plugin = loader.open(copy_fixture("declares")).expect("opens");
    assert_eq!(plugin.name(), "dylib-fixture");
    let ids: Vec<&str> = plugin.nodes().iter().map(|node| node.id.as_str()).collect();
    assert_eq!(
        ids,
        [
            "fixture.pass",
            "fixture.boom",
            "fixture.abort",
            "fixture.gated"
        ]
    );
    assert_eq!(plugin.backends().len(), 1);
    assert!(plugin.backend("fixture.cpu").is_some());
}

#[test]
fn the_nodes_join_a_registry() {
    let mut loader = Loader::new();
    let plugin = loader.open(copy_fixture("joins")).expect("opens");
    let mut registry = Registry::new();
    plugin.join(&mut registry).expect("joins");
    assert!(registry.entry("fixture.pass").is_some());
    assert!(registry.entry("fixture.boom").is_some());
    // The same body joined twice is a duplicate, as a compiled-in record would be.
    assert!(matches!(
        plugin.join(&mut registry),
        Err(LoadError::Register { .. })
    ));
}

#[test]
fn a_node_runs_through_its_compiled_step() {
    let mut loader = Loader::new();
    let plugin = loader.open(copy_fixture("runs")).expect("opens");
    let backend = plugin.backend("fixture.cpu").expect("backend");
    let (nodes, inputs, outputs) = region_of("fixture.pass");
    let region = RegionIn {
        nodes: &nodes,
        inputs: &inputs,
        outputs: &outputs,
    };
    assert_eq!(backend.can_take(&region).expect("can take"), vec![true]);

    let mut units = backend.compile(&region).expect("compiles");
    assert_eq!(units.len(), 1);
    let mut payload = *b"hi";
    let mut cells = [Slot::EMPTY, Slot::EMPTY];
    cells[0] = Slot {
        state: SlotState::Full as u32,
        data: payload.as_mut_ptr(),
        len: payload.len(),
        cap: payload.len(),
        ..Slot::EMPTY
    };
    let mut cx = StepCx::new();
    let mut slots = Slots::of(&mut cells);
    let unit = units.pop().expect("one unit");
    let mut step = unit.step;
    assert!(matches!(
        run_once(&mut step, &mut slots, &mut cx),
        StepEnd::Done
    ));
    assert_eq!(cells[1].state, SlotState::Full as u32);
    assert_eq!(cells[1].len, 2);
    release(step);
}

#[test]
fn a_panicking_node_is_a_fault_and_the_host_keeps_running() {
    let mut loader = Loader::new();
    let plugin = loader.open(copy_fixture("panics")).expect("opens");
    let backend = plugin.backend("fixture.cpu").expect("backend");

    let (nodes, inputs, outputs) = region_of("fixture.boom");
    let region = RegionIn {
        nodes: &nodes,
        inputs: &inputs,
        outputs: &outputs,
    };
    let mut units = backend.compile(&region).expect("compiles");
    let mut payload = *b"x";
    let mut cells = [Slot::EMPTY, Slot::EMPTY];
    cells[0] = Slot {
        state: SlotState::Full as u32,
        data: payload.as_mut_ptr(),
        len: 1,
        cap: 1,
        ..Slot::EMPTY
    };
    let mut cx = StepCx::new();
    let mut step = units.pop().expect("unit").step;
    let end = run_once(&mut step, &mut Slots::of(&mut cells), &mut cx);
    assert!(matches!(end, StepEnd::Fault));
    assert_eq!(cx.at, 0, "the fault names the node");
    assert_eq!(cx.reason(), "node panicked");
    release(step);

    // The same backend still runs a passing node afterwards.
    let (nodes, inputs, outputs) = region_of("fixture.pass");
    let region = RegionIn {
        nodes: &nodes,
        inputs: &inputs,
        outputs: &outputs,
    };
    let mut units = backend.compile(&region).expect("compiles");
    let mut step = units.pop().expect("unit").step;
    assert!(matches!(
        run_once(&mut step, &mut Slots::of(&mut cells), &mut cx),
        StepEnd::Done
    ));
    release(step);
}

#[test]
fn a_layout_mismatch_is_refused_before_any_record_is_read() {
    // The loader always passes its own hash, so this calls the entry directly with another.
    use daedalus_abi::{ENTRY_SYMBOL, EntryFn, LAYOUT_HASH, PluginEntry};
    use std::mem::MaybeUninit;

    // SAFETY: the fixture is a Daedalus plugin built from this workspace, as the test requires.
    let library = unsafe { libloading::Library::new(fixture_library()) }.expect("opens");
    // SAFETY: the symbol is the entry point, whose signature is `EntryFn`.
    let entry: libloading::Symbol<'_, EntryFn> =
        unsafe { library.get(ENTRY_SYMBOL) }.expect("symbol");
    let mut out = MaybeUninit::<PluginEntry>::uninit();
    let wrong = LAYOUT_HASH ^ 1;
    // SAFETY: `out` is writable, and a plugin with another layout writes only its layout into it.
    let status = unsafe { (*entry)(wrong, out.as_mut_ptr()) };
    assert_eq!(status, Status::VERSION_MISMATCH);
    // SAFETY: the plugin wrote its layout hash into the first field, at offset zero.
    let found = unsafe { std::ptr::addr_of!((*out.as_ptr()).layout).read() };
    assert_eq!(found, LAYOUT_HASH, "the plugin names its own layout");
}

#[test]
fn a_missing_plugin_is_retried_with_a_growing_delay_until_it_loads() {
    let dest = copy_fixture("late");
    std::fs::remove_file(&dest).expect("remove the copy");
    let stop = AtomicBool::new(false);
    let mut retry = Retry::new();
    let mut loader = Loader::new();
    let mut delays = Vec::new();
    let mut sleeps = 0;
    let plugin = load_with_retry(&mut loader, &dest, &stop, &mut retry, |delay| {
        delays.push(delay);
        sleeps += 1;
        if sleeps == 3 {
            std::fs::copy(fixture_library(), &dest).expect("the plugin appears");
        }
    })
    .expect("loads once the file appears");
    assert_eq!(delays.len(), 3);
    assert!(
        delays.windows(2).all(|pair| pair[1] > pair[0]),
        "{delays:?}"
    );
    assert_eq!(plugin.name(), "dylib-fixture");
    assert_eq!(retry.failures(), 0, "a load that works clears the count");
}

#[test]
fn a_failed_reload_leaves_the_loaded_library_serving_and_is_retried() {
    let mut loader = Loader::new();
    let current = loader.open(copy_fixture("serving")).expect("opens");

    let bad = copy_fixture("bad-reload");
    std::fs::write(&bad, b"not a library").expect("write garbage");
    let stop = AtomicBool::new(false);
    let mut retry = Retry::new();
    let mut tries = 0;
    let outcome = load_with_retry(&mut loader, &bad, &stop, &mut retry, |_| {
        tries += 1;
        if tries == 2 {
            stop.store(true, Ordering::SeqCst);
        }
    });
    assert!(
        matches!(outcome, Err(LoadError::Stopped { .. })),
        "{outcome:?}"
    );
    assert!(retry.failures() >= 2, "the failed reload was retried");

    // The library that was already loaded still runs its node.
    let backend = current.backend("fixture.cpu").expect("backend");
    let (nodes, inputs, outputs) = region_of("fixture.pass");
    let region = RegionIn {
        nodes: &nodes,
        inputs: &inputs,
        outputs: &outputs,
    };
    let mut units = backend.compile(&region).expect("still compiles");
    let mut payload = *b"ok";
    let mut cells = [Slot::EMPTY, Slot::EMPTY];
    cells[0] = Slot {
        state: SlotState::Full as u32,
        data: payload.as_mut_ptr(),
        len: 2,
        cap: 2,
        ..Slot::EMPTY
    };
    let mut step = units.pop().expect("unit").step;
    assert!(matches!(
        run_once(&mut step, &mut Slots::of(&mut cells), &mut StepCx::new()),
        StepEnd::Done
    ));
    release(step);
}

#[test]
fn stopping_by_hand_halts_attempts_until_resumed() {
    let missing = copy_fixture("halted");
    std::fs::remove_file(&missing).expect("remove the copy");
    let stop = AtomicBool::new(false);
    let mut retry = Retry::new();
    let mut loader = Loader::new();
    let mut attempts_at_stop = 0u32;
    let mut sleeps = 0u32;
    let outcome = load_with_retry(&mut loader, &missing, &stop, &mut retry, |_| {
        sleeps += 1;
        if sleeps == 2 {
            stop.store(true, Ordering::SeqCst);
            attempts_at_stop = sleeps;
        }
    });
    assert!(matches!(outcome, Err(LoadError::Stopped { .. })));
    assert_eq!(
        retry.failures(),
        attempts_at_stop,
        "no attempt after the stop"
    );
    assert_eq!(attempts_at_stop, 2);

    // Resumed by hand, the same load finds the file and succeeds.
    std::fs::copy(fixture_library(), &missing).expect("the file exists now");
    stop.store(false, Ordering::SeqCst);
    load_with_retry(&mut loader, &missing, &stop, &mut retry, |_| {}).expect("resumed load works");
}

#[test]
fn a_reload_is_a_new_generation_and_the_old_one_goes_when_its_steps_do() {
    let mut loader = Loader::new();
    let first = loader.open(copy_fixture("gen-a")).expect("opens");
    assert_eq!(first.generation().number(), 1);
    let (nodes, inputs, outputs) = region_of("fixture.pass");
    let region = RegionIn {
        nodes: &nodes,
        inputs: &inputs,
        outputs: &outputs,
    };
    let units = first
        .backend("fixture.cpu")
        .expect("backend")
        .compile(&region)
        .expect("compiles");
    let probe = first.generation_probe();
    drop(first);
    assert!(
        probe.upgrade().is_some(),
        "a live step keeps its generation"
    );

    let second = loader.open(copy_fixture("gen-b")).expect("reloads");
    assert_eq!(second.generation().number(), 2);

    for unit in units {
        release(unit.step);
    }
    assert!(
        probe.upgrade().is_none(),
        "the generation is retired with its last step"
    );
}

#[test]
fn a_path_that_is_not_a_library_is_a_typed_error_naming_the_file() {
    let bad = copy_fixture("not-a-library");
    std::fs::write(&bad, b"nope").expect("write garbage");
    let mut loader = Loader::new();
    match loader.open(Path::new(&bad)) {
        Err(error @ LoadError::Open { .. }) => {
            assert!(error.to_string().contains("not-a-library"), "{error}");
        }
        other => panic!("expected an open error, got {other:?}"),
    }
}
