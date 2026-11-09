//! A reference to a host-owned object.

/// Low 32 bits are the slot index, high 32 the generation. A generation is never 0, so
/// [`Handle::NULL`] is never a live object. The table that hands these out lives in the host.
#[repr(transparent)]
#[derive(Clone, Copy, Debug, PartialEq, Eq, Hash)]
pub struct Handle(pub u64);

impl Handle {
    pub const NULL: Handle = Handle(0);

    pub const fn new(index: u32, generation: u32) -> Handle {
        Handle((generation as u64) << 32 | index as u64)
    }

    pub const fn index(self) -> u32 {
        self.0 as u32
    }

    pub const fn generation(self) -> u32 {
        (self.0 >> 32) as u32
    }

    /// True when the generation is 0, which no live handle has.
    pub const fn is_null(self) -> bool {
        self.generation() == 0
    }
}
