use core::cell::UnsafeCell;

use smallvec::SmallVec;

use crate::sync::Mutex as ParkingMutex;

use super::CorrelatedPayload;

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub(crate) enum DirectSlotAccess {
    Serial,
    Shared,
}

/// Payloads one edge hands from its producer to its consumer: the newest one, or (for a
/// buffer-all edge) every one in order. One payload is held inline, so a steady stream of one per
/// tick allocates nothing.
type Held = SmallVec<[CorrelatedPayload; 1]>;

pub(crate) struct DirectSlot {
    lock: ParkingMutex<()>,
    payload: UnsafeCell<Held>,
    /// Keep every payload (`PressurePolicy::BufferAll`) rather than only the newest.
    keep_all: bool,
}

// SAFETY: DirectSlot serializes shared access with `lock` for all parallel execution paths.
// The serial handle bypasses the lock only while one mutable executor owner is running a single
// graph tick in schedule order. Executor snapshots choose `Shared` for scoped/pool segments and
// retained parallel ticks, while `Serial` is only constructed for single-owner serial ticks and
// direct-host fast paths. `reset_run_storage` clears slots before retained ticks can switch access
// modes. Regression coverage lives in `executor::tests::direct_slot_*` and
// `runtime/tests/it/parallel_invariants.rs` tests for retained serial/parallel ticks, latest-only
// direct-slot transfer, and serial-to-parallel access switching.
unsafe impl Sync for DirectSlot {}

impl DirectSlot {
    /// A slot keeping the newest payload.
    #[cfg(test)]
    pub(crate) fn empty() -> Self {
        Self::new(false)
    }

    /// A slot keeping every payload in order (`keep_all`) or only the newest.
    pub(crate) fn new(keep_all: bool) -> Self {
        Self {
            lock: ParkingMutex::new(()),
            payload: UnsafeCell::new(Held::new()),
            keep_all,
        }
    }

    /// Whether the slot keeps every payload rather than only the newest.
    pub(crate) fn keeps_all(&self) -> bool {
        self.keep_all
    }

    pub(crate) fn serial(&self) -> SerialDirectSlot<'_> {
        SerialDirectSlot { slot: self }
    }

    pub(crate) fn shared(&self) -> SharedDirectSlot<'_> {
        SharedDirectSlot { slot: self }
    }

    #[inline]
    pub(crate) fn access(&self, access: DirectSlotAccess) -> DirectSlotHandle<'_> {
        match access {
            DirectSlotAccess::Serial => DirectSlotHandle::Serial(self.serial()),
            DirectSlotAccess::Shared => DirectSlotHandle::Shared(self.shared()),
        }
    }

    pub(crate) fn clear(&self) {
        let _guard = self.lock.lock();
        // SAFETY: the slot mutex is held for the mutation.
        unsafe { (*self.payload.get()).clear() }
    }

    /// Store `payload` in `held` over the one it replaces (never for `keep_all`), dropping that one
    /// in place; whether one was replaced. A slot that keeps only the newest overwrites its last
    /// entry, so the payload moves once instead of out of the slot and back in.
    #[inline]
    fn put_in_place(&self, held: &mut Held, payload: CorrelatedPayload) -> bool {
        if !self.keep_all
            && let Some(newest) = held.last_mut()
        {
            *newest = payload;
            return true;
        }
        held.push(payload);
        false
    }

    /// [`Self::put_in_place`] returning the payload it replaces, for a caller that must drop it
    /// outside a lock.
    fn replace_in(&self, held: &mut Held, payload: CorrelatedPayload) -> Option<CorrelatedPayload> {
        if !self.keep_all
            && let Some(newest) = held.last_mut()
        {
            return Some(core::mem::replace(newest, payload));
        }
        held.push(payload);
        None
    }

    /// The oldest payload of `held`.
    #[inline]
    fn take_from(held: &mut Held) -> Option<CorrelatedPayload> {
        (!held.is_empty()).then(|| held.remove(0))
    }
}

pub(crate) enum DirectSlotHandle<'a> {
    Serial(SerialDirectSlot<'a>),
    Shared(SharedDirectSlot<'a>),
}

impl DirectSlotHandle<'_> {
    /// Store `payload`; whether it replaced one.
    #[inline]
    pub(crate) fn put(self, payload: CorrelatedPayload) -> bool {
        match self {
            DirectSlotHandle::Serial(slot) => slot.put(payload),
            DirectSlotHandle::Shared(slot) => slot.put(payload),
        }
    }

    #[inline]
    pub(crate) fn take(self) -> Option<CorrelatedPayload> {
        match self {
            DirectSlotHandle::Serial(slot) => slot.take(),
            DirectSlotHandle::Shared(slot) => slot.take(),
        }
    }

    /// Whether the slot holds a payload, without taking it.
    pub(crate) fn occupied(self) -> bool {
        match self {
            // SAFETY: see `SerialDirectSlot::put`.
            DirectSlotHandle::Serial(slot) => unsafe { !(*slot.slot.payload.get()).is_empty() },
            DirectSlotHandle::Shared(slot) => {
                let _guard = slot.slot.lock.lock();
                // SAFETY: shared execution holds the slot mutex while reading.
                unsafe { !(*slot.slot.payload.get()).is_empty() }
            }
        }
    }
}

pub(crate) struct SerialDirectSlot<'a> {
    slot: &'a DirectSlot,
}

impl SerialDirectSlot<'_> {
    #[inline]
    pub(crate) fn put(self, payload: CorrelatedPayload) -> bool {
        // SAFETY: serial execution owns the graph tick and accesses each direct slot in schedule
        // order, so no shared segment can concurrently touch this slot.
        self.slot
            .put_in_place(unsafe { &mut *self.slot.payload.get() }, payload)
    }

    #[inline]
    pub(crate) fn take(self) -> Option<CorrelatedPayload> {
        // SAFETY: see `put`; the serial accessor is only constructed for single-owner ticks.
        DirectSlot::take_from(unsafe { &mut *self.slot.payload.get() })
    }
}

pub(crate) struct SharedDirectSlot<'a> {
    slot: &'a DirectSlot,
}

impl SharedDirectSlot<'_> {
    #[inline]
    pub(crate) fn put(self, payload: CorrelatedPayload) -> bool {
        // The replaced payload is dropped after the mutex is released, as it always was.
        let replaced = {
            let _guard = self.slot.lock.lock();
            // SAFETY: shared execution holds the slot mutex for the whole mutation.
            self.slot
                .replace_in(unsafe { &mut *self.slot.payload.get() }, payload)
        };
        replaced.is_some()
    }

    #[inline]
    pub(crate) fn take(self) -> Option<CorrelatedPayload> {
        let _guard = self.slot.lock.lock();
        // SAFETY: shared execution holds the slot mutex for the whole mutation.
        DirectSlot::take_from(unsafe { &mut *self.slot.payload.get() })
    }
}
