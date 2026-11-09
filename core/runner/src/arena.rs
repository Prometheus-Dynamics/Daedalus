//! Steps kept in one fixed block of memory, for a runner with no heap. A plan places each step
//! here once, and the block belongs to it until the next plan.

use core::mem::{align_of, needs_drop, size_of};
use core::ptr;

use daedalus_abi::{OwnedStep, Slot, SlotState, Slots, StepCx, StepEnd};
use daedalus_value::OwnerKind;

use crate::owned::{INLINE_BYTES, InlineCell};

/// A step that a runner can build in place.
pub trait Fresh: OwnedStep {
    fn fresh(node: u32, inputs: u32) -> Self;
}

/// How to build and call one kind of step without knowing its type.
#[derive(Clone, Copy, Debug)]
pub struct ArenaBody {
    size: u16,
    align: u8,
    /// Value bytes of each fixed input, then of each output.
    pub inputs: &'static [u16],
    pub outputs: &'static [u16],
    /// Value bytes of a fan-in port. `None` if the node has no fan-in.
    pub fan: Option<u16>,
    place: unsafe fn(*mut u8, u32, u32),
    run: Run,
}

type Run = unsafe fn(*mut u8, &mut Slots, &mut StepCx) -> StepEnd;

// SAFETY: `slot` is aligned for a `T`, holds room for one and nothing lives there.
unsafe fn place_as<T: Fresh>(slot: *mut u8, node: u32, inputs: u32) {
    unsafe { slot.cast::<T>().write(T::fresh(node, inputs)) }
}

// SAFETY: `slot` holds a `T` from `place_as`, and nothing else is using it.
unsafe fn run_as<T: Fresh>(slot: *mut u8, slots: &mut Slots, cx: &mut StepCx) -> StepEnd {
    unsafe { (*slot.cast::<T>()).run(slots, cx) }
}

impl ArenaBody {
    /// # Panics
    ///
    /// At compile time, if a `T` needs dropping or doesn't fit the layout.
    pub const fn of<T: Fresh>(
        inputs: &'static [u16],
        fan: Option<u16>,
        outputs: &'static [u16],
    ) -> ArenaBody {
        assert!(!needs_drop::<T>(), "a step in an arena is never dropped");
        assert!(size_of::<T>() <= u16::MAX as usize && align_of::<T>() <= MAX_ALIGN);
        ArenaBody {
            size: size_of::<T>() as u16,
            align: align_of::<T>() as u8,
            inputs,
            outputs,
            fan,
            place: place_as::<T>,
            run: run_as::<T>,
        }
    }
}

const MAX_ALIGN: usize = 8;

/// Rounds up to `align`, a power of two. No division, for chips that have none.
pub const fn align_up(value: usize, align: usize) -> usize {
    (value + align - 1) & !(align - 1)
}

#[derive(Clone, Copy)]
struct Placed {
    run: Run,
    at: u32,
}

/// The bytes `count` steps take: their directory, then each step in order.
pub fn steps_end(count: usize, bodies: impl Iterator<Item = ArenaBody>) -> usize {
    let mut end = count * size_of::<Placed>();
    for body in bodies {
        end = align_up(end, usize::from(body.align)) + usize::from(body.size);
    }
    align_up(end, MAX_ALIGN)
}

/// The arena was too small, or more steps were placed than it was begun with.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub struct ArenaFull;

#[repr(C, align(8))]
#[derive(Debug)]
struct Bytes<const N: usize>([u8; N]);

/// `N` bytes holding a directory of steps, the steps, then plain bytes for the runner.
#[derive(Debug)]
pub struct StepArena<const N: usize> {
    bytes: Bytes<N>,
    count: u32,
    placed: u32,
    end: u32,
}

impl<const N: usize> Default for StepArena<N> {
    fn default() -> Self {
        Self::new()
    }
}

impl<const N: usize> StepArena<N> {
    pub const fn new() -> Self {
        StepArena {
            bytes: Bytes([0; N]),
            count: 0,
            placed: 0,
            end: 0,
        }
    }

    /// Forgets every step and starts a new directory of `count`.
    pub fn begin(&mut self, count: usize) -> Result<(), ArenaFull> {
        // Steps may have left padding bytes unwritten, and `data` shows this memory as bytes.
        let used = self.end as usize;
        self.bytes.0[..used].fill(0);
        (self.count, self.placed, self.end) = (0, 0, 0);
        let end = count
            .checked_mul(size_of::<Placed>())
            .filter(|end| *end <= N)
            .ok_or(ArenaFull)?;
        self.count = count as u32;
        self.end = end as u32;
        Ok(())
    }

    /// Builds the next step.
    pub fn place(&mut self, body: &ArenaBody, node: u32, inputs: u32) -> Result<(), ArenaFull> {
        if self.placed >= self.count {
            return Err(ArenaFull);
        }
        let at = align_up(self.end as usize, usize::from(body.align));
        let end = at.checked_add(usize::from(body.size)).ok_or(ArenaFull)?;
        if end > N {
            return Err(ArenaFull);
        }
        let base = self.bytes.0.as_mut_ptr();
        // SAFETY: `at` is aligned for the step and `at..end` is inside the block and unused.
        unsafe { (body.place)(base.add(at), node, inputs) };
        let entry = Placed {
            run: body.run,
            at: at as u32,
        };
        // SAFETY: `begin` kept the directory for `count` entries, `placed` is below that, and the
        // block is aligned for a `Placed`.
        unsafe { base.cast::<Placed>().add(self.placed as usize).write(entry) };
        self.placed += 1;
        self.end = end as u32;
        Ok(())
    }

    /// Runs step `index`. `None` if there's no such step.
    pub fn run(&mut self, index: u32, slots: &mut Slots, cx: &mut StepCx) -> Option<StepEnd> {
        if index >= self.placed {
            return None;
        }
        let base = self.bytes.0.as_mut_ptr();
        // SAFETY: `index` is below `placed`, so that entry was written by `place`.
        let entry = unsafe { base.cast::<Placed>().add(index as usize).read() };
        // SAFETY: `entry.at` holds the step `entry.run` belongs to, and `&mut self` makes this the
        // only use of it.
        Some(unsafe { (entry.run)(base.add(entry.at as usize), slots, cx) })
    }

    /// The bytes after the last step. Zero until written.
    pub fn data(&mut self) -> &mut [u8] {
        let end = align_up(self.end as usize, MAX_ALIGN).min(N);
        &mut self.bytes.0[end..]
    }

    pub fn data_ref(&self) -> &[u8] {
        let end = align_up(self.end as usize, MAX_ALIGN).min(N);
        &self.bytes.0[end..]
    }
}

/// Makes `slot` a full inline cell of the bytes in `buf`.
pub fn fill_inline(buf: &mut InlineCell, len: usize, slot: &mut Slot) {
    *slot = Slot {
        state: SlotState::Full as u32,
        space: 0,
        type_index: 0,
        owner: OwnerKind::Inline as u32,
        version: 0,
        reserved: 0,
        data: buf.as_mut_ptr(),
        len: len.min(INLINE_BYTES),
        cap: len.min(INLINE_BYTES),
    };
}

/// Copies an inline cell's bytes to `out` and returns how many. `None` for any other cell.
pub fn copy_inline(slot: &Slot, out: &mut InlineCell) -> Option<usize> {
    let inline = slot.state == SlotState::Full as u32 && slot.owner == OwnerKind::Inline as u32;
    if !inline || slot.len > INLINE_BYTES {
        return None;
    }
    // SAFETY: a full inline cell's `data` is valid for `len` bytes, and `out` is at least that long.
    unsafe { ptr::copy(slot.data.cast_const(), out.as_mut_ptr(), slot.len) };
    Some(slot.len)
}

/// The bytes of a `Copy` value.
pub fn bytes_of<T: Copy>(value: &T, out: &mut InlineCell) -> usize {
    assert!(size_of::<T>() <= INLINE_BYTES);
    // SAFETY: `out` holds at least a `T`'s bytes and doesn't overlap `value`. Padding stays as it
    // was, and these bytes are only ever read back as a `T`.
    unsafe {
        ptr::copy_nonoverlapping(
            (value as *const T).cast::<u8>(),
            out.as_mut_ptr(),
            size_of::<T>(),
        );
    }
    size_of::<T>()
}

/// The `T` in `bytes`, which [`bytes_of`] wrote for a value of a port the plan proved carries `T`.
/// `None` if `len` isn't a `T`'s size.
pub fn value_of<T: Copy>(bytes: &InlineCell, len: usize) -> Option<T> {
    // SAFETY: `bytes` holds `len` bytes, which is a `T`'s size, and they came from a `T` because
    // the plan proved this port carries one. The read doesn't need alignment.
    (len == size_of::<T>()).then(|| unsafe { ptr::read_unaligned(bytes.as_ptr().cast::<T>()) })
}
