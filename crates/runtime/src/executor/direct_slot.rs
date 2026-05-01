use std::cell::UnsafeCell;

use parking_lot::Mutex as ParkingMutex;

use super::CorrelatedPayload;

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub(crate) enum DirectSlotAccess {
    Serial,
    Shared,
}

pub(crate) struct DirectSlot {
    lock: ParkingMutex<()>,
    payload: UnsafeCell<Option<CorrelatedPayload>>,
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
    pub(crate) fn empty() -> Self {
        Self {
            lock: ParkingMutex::new(()),
            payload: UnsafeCell::new(None),
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
        unsafe {
            *self.payload.get() = None;
        }
    }
}

pub(crate) enum DirectSlotHandle<'a> {
    Serial(SerialDirectSlot<'a>),
    Shared(SharedDirectSlot<'a>),
}

impl DirectSlotHandle<'_> {
    pub(crate) fn put(self, payload: CorrelatedPayload) {
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
}

pub(crate) struct SerialDirectSlot<'a> {
    slot: &'a DirectSlot,
}

impl SerialDirectSlot<'_> {
    pub(crate) fn put(self, payload: CorrelatedPayload) {
        // SAFETY: serial execution owns the graph tick and accesses each direct slot in schedule
        // order, so no shared segment can concurrently touch this slot.
        unsafe {
            *self.slot.payload.get() = Some(payload);
        }
    }

    pub(crate) fn take(self) -> Option<CorrelatedPayload> {
        // SAFETY: see `put`; the serial accessor is only constructed for single-owner ticks.
        unsafe { (*self.slot.payload.get()).take() }
    }
}

pub(crate) struct SharedDirectSlot<'a> {
    slot: &'a DirectSlot,
}

impl SharedDirectSlot<'_> {
    pub(crate) fn put(self, payload: CorrelatedPayload) {
        let _guard = self.slot.lock.lock();
        // SAFETY: shared execution holds the slot mutex for the whole mutation.
        unsafe {
            *self.slot.payload.get() = Some(payload);
        }
    }

    pub(crate) fn take(self) -> Option<CorrelatedPayload> {
        let _guard = self.slot.lock.lock();
        // SAFETY: shared execution holds the slot mutex for the whole mutation.
        unsafe { (*self.slot.payload.get()).take() }
    }
}
