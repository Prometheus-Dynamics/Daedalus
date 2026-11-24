//! A value one node of a plugin hands another node of the same plugin goes as it is, in a box of
//! the plugin's own: read in place, moved out by the last reader, and released once. A record
//! that holds a value of another type, or that another build made, is never read as the reader's
//! type: the step fails, in words.
//!
//! The planner gives a plugin inner edges only between nodes of one region whose ports agree, so
//! the wrong pairings here are made by building the steps by hand, as a wrong or stale plan would.

use std::sync::{Arc, Mutex, MutexGuard};

use daedalus_abi::testing::{release, settle};
use daedalus_abi::{OWNER_HANDED, SharedBytes, Slot, SlotState, Step, StepCx, StepEnd};
use daedalus_cpu::plugin::{Place, boundary_step};
use daedalus_cpu::values_held;
use daedalus_cpu_author::{OutView, Refused};
use daedalus_derive::{Type, WireForm};
use daedalus_macros::node;
use daedalus_registry::NodeRecord;
use daedalus_types::{SpaceIndex, TypeIndex};

use crate::common::{call, inline, value};

/// A value with no crossing form.
#[derive(Clone, Debug, PartialEq, Type)]
#[type_key("inside.bag")]
pub struct Bag {
    items: Vec<u32>,
}

/// The same fields under another type.
#[derive(Clone, Debug, PartialEq, Type)]
#[type_key("inside.other")]
pub struct Other {
    items: Vec<u32>,
}

/// A value whose wire form fits in a cell.
#[derive(Clone, Debug, PartialEq, Type, WireForm)]
#[type_key("inside.small")]
pub struct Small {
    low: u32,
    high: u32,
}

#[node(id = "inside.small", inputs(n), outputs(small))]
fn make_small(n: u32) -> Small {
    Small {
        low: n,
        high: n * 2,
    }
}

#[node(id = "inside.add", inputs(small), outputs(sum))]
fn add(small: &Small) -> u32 {
    small.low + small.high
}

#[node(id = "inside.bag", inputs(n), outputs(bag))]
fn make_bag(n: u32) -> Bag {
    Bag {
        items: (0..n).collect(),
    }
}

#[node(id = "inside.other", inputs(n), outputs(other))]
fn make_other(n: u32) -> Other {
    Other {
        items: (0..n).collect(),
    }
}

/// Reads the bag where it is.
#[node(id = "inside.peek", inputs(bag), outputs(total))]
fn peek(bag: &Bag) -> u32 {
    bag.items.iter().sum()
}

/// Takes the bag, and says whether its items are where the maker put them.
#[node(id = "inside.take", inputs(bag), outputs(at))]
fn take(bag: Bag) -> u64 {
    bag.items.as_ptr() as u64
}

/// A handle on a value, made inside the plugin.
#[node(id = "inside.shared", inputs(n), outputs(bag))]
fn make_shared(n: u32) -> Arc<Bag> {
    Arc::new(Bag {
        items: (0..n).collect(),
    })
}

#[node(id = "inside.count", inputs(bag), outputs(len))]
fn count(bag: Arc<Bag>) -> u32 {
    bag.items.len() as u32
}

/// A list written in place.
#[node(id = "inside.list", inputs(port(name = "n")))]
fn list(n: u32, mut out: OutView<'_, u32>) -> Result<(), Refused> {
    for (at, item) in out.items_mut(n as usize)?.iter_mut().enumerate() {
        *item = at as u32;
    }
    Ok(())
}

#[node(id = "inside.sum", inputs(items), outputs(sum))]
fn sum(items: &[u32]) -> u32 {
    items.iter().sum()
}

/// The count of values in boxes is one per build, and the tests share this one.
fn serial() -> MutexGuard<'static, ()> {
    static SERIAL: Mutex<()> = Mutex::new(());
    SERIAL
        .lock()
        .unwrap_or_else(|poisoned| poisoned.into_inner())
}

/// A maker's step, its one output on an edge inside the plugin.
fn maker(record: &NodeRecord) -> Step {
    let place = Place {
        inner_outputs: &[0],
        ..Place::default()
    };
    boundary_step(record, 0, &place).expect("the maker builds")
}

/// A reader's step, its one input on an edge inside the plugin.
fn reader(record: &NodeRecord) -> Step {
    let place = Place {
        inner_inputs: &[0],
        ..Place::default()
    };
    boundary_step(record, 1, &place).expect("the reader builds")
}

/// Runs the maker for `n`, settles its hand-over as a host does, and gives the cell a reader
/// takes.
fn made(step: &mut Step, n: u32) -> Slot {
    let mut buf = [0; daedalus_runner::INLINE_BYTES];
    let mut cells = [inline(n, &mut buf), Slot::EMPTY];
    let mut cx = StepCx::new();
    let end = call(step, &mut cells, &mut cx);
    assert!(matches!(end, StepEnd::Done), "{}", cx.reason());
    assert_eq!(cells[1].owner, OWNER_HANDED, "the value is handed over");
    let bytes = cx.handed(0).expect("room for the hand-over");
    assert_eq!(bytes.len, 0, "nothing is encoded");
    settle(&mut cells[1], bytes);
    cells[1]
}

/// Runs the reader on `input` and gives its end, its output and its words.
fn read(step: &mut Step, input: &mut Slot) -> (StepEnd, Slot, String) {
    let mut cells = [*input, Slot::EMPTY];
    let mut cx = StepCx::new();
    let end = call(step, &mut cells, &mut cx);
    *input = cells[0];
    (end, cells[1], cx.reason().to_string())
}

/// Gives back the count a cell holds, if it holds one.
fn drop_cell(cell: &mut Slot) {
    drop(daedalus_runner::take_handed(cell));
}

#[test]
fn a_value_inside_the_plugin_is_read_where_it_is_and_released_once() {
    let _serial = serial();
    let held = values_held();
    let mut make = maker(&MAKE_BAG_NODE);
    let mut peek = reader(&PEEK_NODE);
    let mut cell = made(&mut make, 4);
    assert_eq!(values_held(), held + 1);
    let (end, out, words) = read(&mut peek, &mut cell);
    assert!(matches!(end, StepEnd::Done), "{words}");
    assert_eq!(value::<u32>(&out), 1 + 2 + 3);
    assert_eq!(
        cell.state,
        SlotState::Full as u32,
        "a read leaves the value"
    );
    drop_cell(&mut cell);
    assert_eq!(values_held(), held, "the last count releases the box");
    release(make);
    release(peek);
}

#[test]
fn the_last_reader_moves_the_value_out_and_a_shared_one_is_cloned() {
    let _serial = serial();
    let held = values_held();
    let mut make = maker(&MAKE_BAG_NODE);
    let mut take = reader(&TAKE_NODE);

    let at = |cell: &Slot| {
        daedalus_runner::inside_view::<Bag>(cell).map(|bag| bag.items.as_ptr() as u64)
    };

    // The cell's count is the only one: the items move, at the address the maker gave them.
    let mut cell = made(&mut make, 3);
    let made_at = at(&cell);
    let (end, out, words) = read(&mut take, &mut cell);
    assert!(matches!(end, StepEnd::Done), "{words}");
    assert_ne!(
        cell.state,
        SlotState::Full as u32,
        "the taker gives up the cell"
    );
    assert_eq!(values_held(), held, "the box went with the take");
    assert_eq!(Some(value::<u64>(&out)), made_at, "the items moved");

    // Another count on the record: the taker clones, and the box stays for the other.
    let mut cell = made(&mut make, 3);
    let made_at = at(&cell);
    let other = daedalus_runner::share_handed(&cell).expect("another count");
    let (end, out, words) = read(&mut take, &mut cell);
    assert!(matches!(end, StepEnd::Done), "{words}");
    assert_ne!(
        Some(value::<u64>(&out)),
        made_at,
        "a shared value is cloned"
    );
    assert_eq!(values_held(), held + 1, "the other count keeps the box");
    drop(other);
    assert_eq!(values_held(), held, "the last count releases it");
    release(make);
    release(take);
}

#[test]
fn a_value_of_another_type_is_refused_by_the_reader_in_words() {
    let _serial = serial();
    let held = values_held();
    let mut make = maker(&MAKE_OTHER_NODE);
    let mut peek = reader(&PEEK_NODE);
    let mut take = reader(&TAKE_NODE);
    let mut cell = made(&mut make, 4);
    for step in [&mut peek, &mut take] {
        let (end, _, words) = read(step, &mut cell);
        assert!(matches!(end, StepEnd::Fault), "an `Other` read as a `Bag`");
        assert_eq!(words, "an input held a value of another form");
        assert_eq!(
            cell.state,
            SlotState::Full as u32,
            "the cell keeps its count"
        );
    }
    drop_cell(&mut cell);
    assert_eq!(values_held(), held);
    release(make);
    release(peek);
    release(take);
}

#[test]
fn a_record_another_build_made_is_never_read_as_a_value_inside() {
    let _serial = serial();
    let mut peek = reader(&PEEK_NODE);
    let mut take = reader(&TAKE_NODE);
    // Bytes of no length, as the hand-over of a value inside is, made by other code.
    for bytes in [Vec::new(), vec![7; 64]] {
        let mut cell = Slot::EMPTY;
        let shared = SharedBytes::from_vec(bytes);
        daedalus_runner::put_handed(&mut cell, TypeIndex(0), SpaceIndex(0), shared);
        assert!(!daedalus_runner::is_inside(&cell));
        for step in [&mut peek, &mut take] {
            let (end, _, words) = read(step, &mut cell);
            assert!(matches!(end, StepEnd::Fault));
            assert_eq!(words, "an input held a value of another form");
        }
        drop_cell(&mut cell);
    }
    release(peek);
    release(take);
}

#[test]
fn a_released_box_carries_the_next_value_and_outlives_its_step_only_while_held() {
    let _serial = serial();
    let held = values_held();
    let mut make = maker(&MAKE_BAG_NODE);
    let at = |cell: &Slot| daedalus_runner::inside_view::<Bag>(cell).map(core::ptr::from_ref);

    // Released before the next value: the next one goes in the same box.
    let mut cell = made(&mut make, 2);
    let first = at(&cell);
    drop_cell(&mut cell);
    let mut cell = made(&mut make, 5);
    assert_eq!(at(&cell), first, "the box came back");
    assert_eq!(
        daedalus_runner::inside_view::<Bag>(&cell).map(|bag| bag.items.len()),
        Some(5)
    );

    // Still held: the next value gets a box of its own.
    let mut next = made(&mut make, 1);
    assert_ne!(at(&next), at(&cell));
    assert_eq!(values_held(), held + 2);

    // The step goes while both are held; each box is freed when its cell lets go.
    release(make);
    drop_cell(&mut cell);
    drop_cell(&mut next);
    assert_eq!(values_held(), held);
}

#[test]
fn a_value_whose_wire_form_fits_in_its_cell_is_written_there_and_read_back() {
    let _serial = serial();
    let held = values_held();
    let mut make = maker(&MAKE_SMALL_NODE);
    let mut add = reader(&ADD_NODE);
    let mut buf = [0; daedalus_runner::INLINE_BYTES];
    let mut cells = [inline(7u32, &mut buf), Slot::EMPTY];
    let mut cx = StepCx::new();
    assert!(matches!(
        call(&mut make, &mut cells, &mut cx),
        StepEnd::Done
    ));
    assert_ne!(cells[1].owner, OWNER_HANDED, "in place, with no box");
    assert_eq!(values_held(), held);
    let (end, out, words) = read(&mut add, &mut cells[1]);
    assert!(matches!(end, StepEnd::Done), "{words}");
    assert_eq!(value::<u32>(&out), 21);
    release(make);
    release(add);
}

#[test]
fn a_handle_and_a_list_written_in_place_go_in_a_box_too() {
    let _serial = serial();
    let held = values_held();
    for (maker_of, reader_of, n, expected) in [
        (&MAKE_SHARED_NODE, &COUNT_NODE, 6, 6),
        (&LIST_NODE, &SUM_NODE, 5, 10),
    ] {
        let mut make = maker(maker_of);
        let mut read_it = reader(reader_of);
        let mut cell = made(&mut make, n);
        assert!(daedalus_runner::is_inside(&cell), "{}", maker_of.decl.id);
        assert_eq!(values_held(), held + 1);
        let (end, out, words) = read(&mut read_it, &mut cell);
        assert!(matches!(end, StepEnd::Done), "{words}");
        assert_eq!(value::<u32>(&out), expected, "{}", maker_of.decl.id);
        drop_cell(&mut cell);
        assert_eq!(values_held(), held);
        release(make);
        release(read_it);
    }
}
