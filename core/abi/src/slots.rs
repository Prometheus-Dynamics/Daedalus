//! The numbered value cells a step reads and writes. `daedalus-value` gives the fields meaning.

/// In a plan's slot lists, a port with no edge. A step still gets a cell for it: an empty one for
/// an input, and one the runner throws away for an output.
pub const NO_SLOT: u32 = u32::MAX;

/// What a slot holds right now.
#[repr(u32)]
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum SlotState {
    Empty = 0,
    Full = 1,
    Moved = 2,
}

/// One value cell. `space` and `type_index` are indexes the planner assigned. `owner` is an
/// `OwnerKind` from `daedalus-value`, and `data`, `len` and `cap` hold that owner's payload.
/// `version` goes up on each write, so a step can tell whether an input changed.
#[repr(C)]
#[derive(Clone, Copy, Debug)]
pub struct Slot {
    pub state: u32,
    pub space: u32,
    pub type_index: u32,
    pub owner: u32,
    pub version: u32,
    pub reserved: u32,
    pub data: *mut u8,
    pub len: usize,
    pub cap: usize,
}

impl Slot {
    pub const EMPTY: Slot = Slot {
        state: SlotState::Empty as u32,
        space: 0,
        type_index: 0,
        owner: 0,
        version: 0,
        reserved: 0,
        data: core::ptr::null_mut(),
        len: 0,
        cap: 0,
    };
}

/// The cells a step sees: its frame. Inputs come first, in the order its unit declares them, then
/// its outputs. Index 0 is always the first input, so a step needs no slot numbers.
#[repr(C)]
#[derive(Clone, Copy, Debug)]
pub struct Slots {
    pub(crate) cells: *mut Slot,
    pub(crate) len: u32,
    pub reserved: u32,
}

impl Slots {
    /// A view of `cells`. The borrow ends when this function returns: keeping the `Slots` longer
    /// is fine, and using it needs `unsafe` at the point that dereferences it.
    pub fn of(cells: &mut [Slot]) -> Slots {
        Slots {
            cells: cells.as_mut_ptr(),
            len: cells.len() as u32,
            reserved: 0,
        }
    }

    /// How many cells this view holds.
    pub fn len(&self) -> usize {
        self.len as usize
    }

    pub fn is_empty(&self) -> bool {
        self.len == 0
    }

    /// Every cell, for a body that binds them once each.
    #[inline]
    pub fn as_mut_slice(&mut self) -> &mut [Slot] {
        // SAFETY: `of` built this from a live, exclusive slice of `len` cells.
        unsafe { core::slice::from_raw_parts_mut(self.cells, self.len as usize) }
    }

    /// The cell at `index`, or `None` past the end. Only `of` builds a `Slots`, so every view
    /// points at cells that stay valid for as long as the view is used.
    pub fn cell(&mut self, index: usize) -> Option<&mut Slot> {
        if index >= self.len() {
            return None;
        }
        // SAFETY: `cells` came from `of`, which borrowed `len` valid cells for as long as this view
        // is used, and `index` was checked against `len` above.
        Some(unsafe { &mut *self.cells.add(index) })
    }
}
