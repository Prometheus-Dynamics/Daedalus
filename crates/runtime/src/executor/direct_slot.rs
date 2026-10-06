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
// `runtime/tests/parallel_invariants.rs` tests for retained serial/parallel ticks, latest-only
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

    pub(crate) fn serial(&self) -> SerialDirectSlot<'_> {
        SerialDirectSlot { slot: self }
    }

    pub(crate) fn shared(&self) -> SharedDirectSlot<'_> {
        SharedDirectSlot { slot: self }
    }

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

    /// Store `payload` in `held`, returning the one it replaces (never for `keep_all`).
    fn put_in(&self, held: &mut Held, payload: CorrelatedPayload) -> Option<CorrelatedPayload> {
        let replaced = if self.keep_all { None } else { held.pop() };
        held.push(payload);
        replaced
    }

    /// The oldest payload of `held`.
    fn take_from(held: &mut Held) -> Option<CorrelatedPayload> {
        (!held.is_empty()).then(|| held.remove(0))
    }
}

pub(crate) enum DirectSlotHandle<'a> {
    Serial(SerialDirectSlot<'a>),
    Shared(SharedDirectSlot<'a>),
}

impl DirectSlotHandle<'_> {
    /// Store `payload`, returning the one it replaces.
    pub(crate) fn put(self, payload: CorrelatedPayload) -> Option<CorrelatedPayload> {
        match self {
            DirectSlotHandle::Serial(slot) => slot.put(payload),
            DirectSlotHandle::Shared(slot) => slot.put(payload),
        }
    }

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
    pub(crate) fn put(self, payload: CorrelatedPayload) -> Option<CorrelatedPayload> {
        // SAFETY: serial execution owns the graph tick and accesses each direct slot in schedule
        // order, so no shared segment can concurrently touch this slot.
        self.slot
            .put_in(unsafe { &mut *self.slot.payload.get() }, payload)
    }

    pub(crate) fn take(self) -> Option<CorrelatedPayload> {
        // SAFETY: see `put`; the serial accessor is only constructed for single-owner ticks.
        DirectSlot::take_from(unsafe { &mut *self.slot.payload.get() })
    }
}

pub(crate) struct SharedDirectSlot<'a> {
    slot: &'a DirectSlot,
}

impl SharedDirectSlot<'_> {
    pub(crate) fn put(self, payload: CorrelatedPayload) -> Option<CorrelatedPayload> {
        let _guard = self.slot.lock.lock();
        // SAFETY: shared execution holds the slot mutex for the whole mutation.
        self.slot
            .put_in(unsafe { &mut *self.slot.payload.get() }, payload)
    }

    pub(crate) fn take(self) -> Option<CorrelatedPayload> {
        let _guard = self.slot.lock.lock();
        // SAFETY: shared execution holds the slot mutex for the whole mutation.
        DirectSlot::take_from(unsafe { &mut *self.slot.payload.get() })
    }
}
