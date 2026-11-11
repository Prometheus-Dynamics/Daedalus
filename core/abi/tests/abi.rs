use std::alloc::{GlobalAlloc, Layout, System};
use std::cell::Cell;
use std::sync::atomic::{AtomicUsize, Ordering};

use daedalus_abi::*;

struct Counting;
thread_local! {
    static ALLOCATIONS: Cell<usize> = const { Cell::new(0) };
}

// SAFETY: forwards every call to `System` unchanged and only adds a counter.
unsafe impl GlobalAlloc for Counting {
    unsafe fn alloc(&self, layout: Layout) -> *mut u8 {
        let _ = ALLOCATIONS.try_with(|count| count.set(count.get() + 1));
        // SAFETY: the caller's contract is passed straight through.
        unsafe { System.alloc(layout) }
    }
    unsafe fn dealloc(&self, ptr: *mut u8, layout: Layout) {
        // SAFETY: the caller's contract is passed straight through.
        unsafe { System.dealloc(ptr, layout) }
    }
}

#[global_allocator]
static GLOBAL: Counting = Counting;

#[test]
fn status_errno_round_trip() {
    let s = Status::from_errno(22);
    assert_eq!(s.errno(), Some(22));
    assert!(!s.is_ok());
    assert_eq!(Status::OK.into_result(), Ok(0));
    assert_eq!(Status(7).into_result(), Ok(7));
    assert_eq!(s.into_result(), Err(s));
    assert_eq!(Status::STALE_HANDLE.errno(), None);
    assert_eq!(Status::BUFFER_TOO_SMALL.0, -4098);
}

#[test]
fn handle_index_generation_round_trip() {
    let h = Handle::new(7, 9);
    assert_eq!((h.index(), h.generation()), (7, 9));
    assert!(!h.is_null());
}

#[test]
fn handle_generation_zero_is_null() {
    assert!(Handle::NULL.is_null());
    assert!(Handle::new(3, 0).is_null());
}

#[test]
fn abi_version_accepts_rules() {
    let host = AbiVersion::new(1, 2, 99);
    assert_eq!(host.accepts(&AbiVersion::new(1, 2, 99)), Status::OK);
    assert_eq!(host.accepts(&AbiVersion::new(1, 1, 99)), Status::OK);
    assert_eq!(
        host.accepts(&AbiVersion::new(1, 3, 99)),
        Status::VERSION_MISMATCH
    );
    assert_eq!(
        host.accepts(&AbiVersion::new(2, 2, 99)),
        Status::VERSION_MISMATCH
    );
    assert_eq!(
        host.accepts(&AbiVersion::new(1, 2, 98)),
        Status::VERSION_MISMATCH
    );
    let mut short = AbiVersion::new(1, 2, 99);
    short.struct_size = 8;
    assert_eq!(host.accepts(&short), Status::VERSION_MISMATCH);
}

#[cfg(target_pointer_width = "64")]
#[test]
fn layout_pinned() {
    assert_eq!(
        LAYOUT_HASH, PINNED,
        "a shape changed: update the constant in the same commit"
    );
}

#[cfg(target_pointer_width = "64")]
const PINNED: u64 = 920140562815411997;

#[test]
fn write_out_is_all_or_nothing() {
    let mut buf = [9u8; 3];
    let w = write_out(b"abcd", Some(&mut buf));
    assert_eq!((w.status, w.needed), (Status::BUFFER_TOO_SMALL, 4));
    assert_eq!(buf, [9; 3]);
    let w = write_out(b"abc", Some(&mut buf));
    assert_eq!((w.status, w.needed), (Status::OK, 3));
    assert_eq!(&buf, b"abc");
    assert_eq!(write_out(b"abcd", None).needed, 4);

    let w = write_text("abc", Some(&mut buf));
    assert_eq!((w.status, w.needed), (Status::BUFFER_TOO_SMALL, 4));
    let mut room = [9u8; 4];
    assert_eq!(write_text("abc", Some(&mut room)).status, Status::OK);
    assert_eq!(&room, b"abc\0");
}

#[test]
fn stepcx_fault_truncates_on_char_boundary() {
    let long = "é".repeat(100);
    let before = ALLOCATIONS.with(Cell::get);
    let mut cx = StepCx::new();
    cx.fault(Some(4), &long);
    let after = ALLOCATIONS.with(Cell::get);
    assert_eq!(cx.at, 4);
    assert_eq!(cx.reason().len(), 120);
    assert!(cx.reason().chars().all(|c| c == 'é'));
    assert_eq!(after, before, "writing a fault must not allocate");

    let mut odd = StepCx::new();
    odd.fault(None, &format!("a{long}"));
    assert_eq!(odd.at, NO_NODE);
    assert_eq!(odd.reason().len(), 119);
}

#[test]
fn step_is_three_words() {
    assert_eq!(size_of::<Step>(), 3 * size_of::<usize>());
}

#[test]
fn guard_turns_panic_into_status() {
    assert_eq!(guard(|| Status::OK), Status::OK);
    assert_eq!(guard(|| panic!("boom")), Status::PANIC);
}

struct AddWord;
impl StepWord for AddWord {
    fn run(word: usize, slots: &mut Slots, cx: &mut StepCx) -> StepEnd {
        slots.reserved += word as u32;
        cx.progress(1, 2);
        if word == 99 {
            panic!("a body panicked");
        }
        StepEnd::Done
    }
}

fn call(step: &Step, slots: &mut Slots, cx: &mut StepCx) -> StepEnd {
    // SAFETY: both pointers come from live exclusive references.
    unsafe { (step.run)(step.data, slots, cx) }
}

fn no_slots() -> Slots {
    Slots::of(&mut [])
}

#[test]
fn step_of_carries_its_word_and_turns_a_panic_into_a_fault() {
    let mut slots = no_slots();
    let cells = Progress::new();
    let mut cx = StepCx::new();
    cx.progress = &cells;
    let step = Step::of::<AddWord>(5);
    assert!(step.release.is_none());
    assert!(matches!(call(&step, &mut slots, &mut cx), StepEnd::Done));
    assert_eq!(slots.reserved, 5);
    assert_eq!((cells.done.get(), cells.total.get()), (1, 2));
    assert!(matches!(
        call(&Step::of::<AddWord>(99), &mut slots, &mut cx),
        StepEnd::Fault
    ));
}

struct Owned(u32, std::sync::Arc<AtomicUsize>);
impl OwnedStep for Owned {
    fn run(&mut self, slots: &mut Slots, _: &mut StepCx) -> StepEnd {
        self.0 += 1;
        slots.reserved = self.0;
        StepEnd::Skipped
    }
}
impl Drop for Owned {
    fn drop(&mut self) {
        self.1.fetch_add(1, Ordering::Relaxed);
    }
}

#[test]
fn boxed_step_keeps_state_and_releases_once() {
    let dropped = std::sync::Arc::new(AtomicUsize::new(0));
    let step = Step::boxed(Owned(0, dropped.clone()));
    let (mut slots, mut cx) = (no_slots(), StepCx::new());
    call(&step, &mut slots, &mut cx);
    assert!(matches!(call(&step, &mut slots, &mut cx), StepEnd::Skipped));
    assert_eq!(slots.reserved, 2);
    // SAFETY: released once, and the step isn't used again.
    unsafe { step.release.expect("boxed steps release")(step.data) };
    assert_eq!(dropped.load(Ordering::Relaxed), 1);
}

#[test]
fn cancel_word_and_restart_flag_read_back() {
    let word = Word::new(0);
    let mut cx = StepCx::new();
    assert!(!cx.cancelled());
    cx.cancel = &word;
    assert!(!cx.cancelled());
    word.set(1);
    assert!(cx.cancelled());
    assert!(!cx.restarting());
    cx.flags |= RESTART;
    assert!(cx.restarting());
}

struct ReadyAt;
impl FenceWord for ReadyAt {
    fn poll(word: usize) -> FencePoll {
        if word == 1 {
            FencePoll::Ready
        } else {
            FencePoll::Pending
        }
    }
}

#[test]
fn fences_poll() {
    // SAFETY: these fences carry only a word.
    unsafe {
        assert_eq!(Fence::NONE.poll(), FencePoll::Ready);
        assert_eq!(Fence::of::<ReadyAt>(1).poll(), FencePoll::Ready);
        assert_eq!(Fence::of::<ReadyAt>(2).poll(), FencePoll::Pending);
    }
}

#[test]
fn features_keep_unknown_bits() {
    let f = Features::NONE.with(1).with(1 << 40);
    assert!(f.contains(Features(1 << 40)));
    assert_eq!(f.require(Features(2)), Status::NOT_SUPPORTED);
    assert_eq!(f.require(Features(1)), Status::OK);
}

extern "C" fn count_bytes(context: *mut core::ffi::c_void, _: *const u8, len: usize) {
    // SAFETY: the test passes a live `usize` as the context.
    unsafe { *context.cast::<usize>() += len };
}

#[test]
fn message_sink_writes_in_pieces() {
    let mut total = 0usize;
    let sink = MessageSink {
        context: (&raw mut total).cast(),
        write: Some(count_bytes),
    };
    sink.write("abc");
    sink.write_fmt(format_args!("{}-{}", 12, "x"));
    assert_eq!(total, 7);
    MessageSink::NONE.write("nothing happens");
}
