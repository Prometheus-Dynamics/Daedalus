//! Moving whole values in and out of a loaded graph, and the helpers a step body uses to write an
//! owned output. The raw cells stay the step interface; this is the safe layer over them.

use daedalus_abi::{Slot, SlotState};
use daedalus_types::{SpaceIndex, TypeIndex};
use daedalus_value::Value;

use crate::Runner;
use crate::error::RunError;
use crate::owned::{self, INLINE_BYTES};

impl Runner<'_> {
    /// The queue index for `slot`, or `NoSlot`.
    pub(crate) fn queue_index(&self, slot: u32) -> Result<usize, RunError> {
        let index = slot as usize;
        if index >= self.slots {
            return Err(RunError::NoSlot { slot });
        }
        Ok(index)
    }

    /// Stores `value` at the back of `slot`'s queue. It doesn't wake consumers; the host does that.
    /// A typed payload is refused, since its concrete type is needed to store it: see
    /// [`Runner::feed_typed`].
    pub fn feed_value(&mut self, slot: u32, value: Value) -> Result<(), RunError> {
        let index = self.queue_index(slot)?;
        if self.queues[index].is_full() {
            return Err(RunError::Overflow { slot });
        }
        let at = self.queues[index].tail_index();
        let raw = owned::encode(value, &mut self.inline[at])
            .map_err(|owned::Untyped| RunError::Untyped { slot })?;
        self.queues[index].push(self.storage, raw);
        Ok(())
    }

    /// Stores a typed payload of type `ty` in `space` at the back of `slot`'s queue.
    #[cfg(feature = "alloc")]
    pub fn feed_typed<T: Send + Sync + 'static>(
        &mut self,
        slot: u32,
        ty: TypeIndex,
        space: SpaceIndex,
        value: alloc::sync::Arc<T>,
    ) -> Result<(), RunError> {
        let index = self.queue_index(slot)?;
        if self.queues[index].is_full() {
            return Err(RunError::Overflow { slot });
        }
        let raw = owned::typed_raw(ty, space, value);
        self.queues[index].push(self.storage, raw);
        Ok(())
    }

    /// Takes the oldest value out of `slot`'s queue, and hands its ownership to the caller.
    pub fn take_value(&mut self, slot: u32) -> Option<Value> {
        let index = self.queue_index(slot).ok()?;
        let queue = self.queues[index];
        if queue.len == 0 {
            return None;
        }
        // SAFETY: the queue owns this entry, and it leaves the queue on the next line, so the
        // reference moves to the value and is released exactly once, by the value.
        let value = unsafe { owned::decode(&self.storage[queue.head_index()]) };
        self.queues[index].pop_head();
        Some(value)
    }

    /// Removes and releases the oldest value in `slot`, for a policy that gives it up.
    pub fn drop_oldest(&mut self, slot: u32) -> Result<(), RunError> {
        let index = self.queue_index(slot)?;
        let queue = self.queues[index];
        if queue.len > 0 {
            // SAFETY: the queue owns this entry, and the pop below forgets it.
            unsafe { owned::release(&self.storage[queue.head_index()]) };
            self.queues[index].pop_head();
        }
        Ok(())
    }

    /// Sets what pushes to `slot` do at capacity. A loader calls this with the edge's policy.
    pub fn set_pressure(
        &mut self,
        slot: u32,
        pressure: daedalus_value::Pressure,
    ) -> Result<(), RunError> {
        let index = self.queue_index(slot)?;
        self.queues[index].pressure = pressure;
        Ok(())
    }

    /// The type of the oldest value in `slot`, without taking it.
    pub fn peek_type(&self, slot: u32) -> Option<TypeIndex> {
        let index = self.queue_index(slot).ok()?;
        let queue = self.queues[index];
        (queue.len > 0).then(|| TypeIndex(self.storage[queue.head_index()].type_index))
    }
}

/// Writes an owned payload into an output cell, releasing what the cell owned before. The cell
/// must be one the step is writing: the view of an input owns nothing and must not be overwritten
/// this way.
pub fn put(cell: &mut Slot, raw: Slot) {
    if cell.state == SlotState::Full as u32 {
        // SAFETY: a `Full` output cell owns its reference (a body writes one only through these
        // helpers), and it is overwritten here, so the reference is released once.
        unsafe { owned::release(cell) };
    }
    *cell = raw;
}

/// Writes a typed payload into an output cell, releasing the cell's previous payload.
#[cfg(feature = "alloc")]
pub fn put_typed<T: Send + Sync + 'static>(
    cell: &mut Slot,
    ty: TypeIndex,
    space: SpaceIndex,
    value: alloc::sync::Arc<T>,
) {
    put(cell, owned::typed_raw(ty, space, value));
}

/// Writes shared bytes into an output cell, releasing the cell's previous payload.
#[cfg(feature = "alloc")]
pub fn put_shared(
    cell: &mut Slot,
    ty: TypeIndex,
    space: SpaceIndex,
    bytes: alloc::sync::Arc<[u8]>,
) {
    let len = bytes.len();
    let data = alloc::sync::Arc::into_raw(bytes).cast::<u8>().cast_mut();
    put(
        cell,
        Slot {
            state: SlotState::Full as u32,
            space: u32::from(space.0),
            type_index: ty.0,
            owner: daedalus_value::OwnerKind::Shared as u32,
            version: 0,
            reserved: 0,
            data,
            len,
            cap: len,
        },
    );
}

/// Writes static bytes into an output cell, releasing the cell's previous payload.
pub fn put_static(cell: &mut Slot, ty: TypeIndex, space: SpaceIndex, bytes: &'static [u8]) {
    put(
        cell,
        Slot {
            state: SlotState::Full as u32,
            space: u32::from(space.0),
            type_index: ty.0,
            owner: daedalus_value::OwnerKind::Static as u32,
            version: 0,
            reserved: 0,
            data: bytes.as_ptr().cast_mut(),
            len: bytes.len(),
            cap: bytes.len(),
        },
    );
}

/// Writes up to [`INLINE_BYTES`] bytes into an output cell. The bytes are copied when the step's
/// outputs are published, so they only need to live until the step returns. `None` when `bytes`
/// is too long.
pub fn put_inline(cell: &mut Slot, ty: TypeIndex, space: SpaceIndex, bytes: &[u8]) -> Option<()> {
    if bytes.len() > INLINE_BYTES {
        return None;
    }
    put(
        cell,
        Slot {
            state: SlotState::Full as u32,
            space: u32::from(space.0),
            type_index: ty.0,
            owner: daedalus_value::OwnerKind::Inline as u32,
            version: 0,
            reserved: 0,
            data: bytes.as_ptr().cast_mut(),
            len: bytes.len(),
            cap: bytes.len(),
        },
    );
    Some(())
}
