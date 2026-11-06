//! One payload allocation reused across calls (see [`PayloadSlot`]).

use crate::portable::Arc;
use core::any::Any;

use super::foreign::ForeignStorage;
use super::inline::Body;
use super::{Payload, PayloadStorage, ResidencyCache};
use crate::{ForeignHandle, PayloadLineage, Residency, TypeKey};

/// A decoder's payload allocation, reused call after call: [`PayloadSlot::owned`] and
/// [`PayloadSlot::foreign`] build the payload in the allocation the previous one used when
/// nothing else holds it any more, and allocate only when it is still held or held another
/// type. [`PayloadSlot::release`] drops the value the slot last held (a buffer or a frame goes
/// back to its owner) and keeps the allocation.
#[derive(Default)]
pub struct PayloadSlot {
    storage: Option<Arc<dyn PayloadStorage>>,
    /// Drops the held value in place, leaving a placeholder.
    clear: Option<fn(&mut dyn Any)>,
}

impl PayloadSlot {
    /// A payload owning `value`, as [`Payload::owned`], in the slot's allocation when free.
    pub fn owned<T>(&mut self, type_key: impl Into<TypeKey>, value: T) -> Payload
    where
        T: Default + Send + Sync + 'static,
    {
        let type_key = type_key.into();
        #[cfg(target_has_atomic = "ptr")]
        if let Some(slot) = self.free::<super::ArcValue<T>>() {
            slot.0 = value;
            return self.payload(type_key, Residency::Cpu);
        }
        self.storage = Some(super::arc_storage(&type_key, Arc::new(value)));
        self.clear = Some(|any| {
            #[cfg(target_has_atomic = "ptr")]
            if let Some(slot) = any.downcast_mut::<super::ArcValue<T>>() {
                slot.0 = T::default();
            }
            #[cfg(not(target_has_atomic = "ptr"))]
            let _ = any;
        });
        self.payload(type_key, Residency::Cpu)
    }

    /// A payload owning a [`Reusable`](crate::Reusable) value: when the slot is free and holds
    /// a `T`, `refill` refills it in place (a `Vec` keeps its capacity), else `make` builds one.
    /// [`Self::release`] [resets](crate::Reusable::reset) it rather than dropping it.
    pub fn owned_reused<T: crate::Reusable>(
        &mut self,
        type_key: impl Into<TypeKey>,
        make: impl FnOnce() -> T,
        refill: impl FnOnce(&mut T),
    ) -> Payload {
        let type_key = type_key.into();
        #[cfg(target_has_atomic = "ptr")]
        if let Some(slot) = self.free::<super::ArcValue<T>>() {
            refill(&mut slot.0);
            return self.payload(type_key, Residency::Cpu);
        }
        let _ = refill;
        self.storage = Some(super::arc_storage(&type_key, Arc::new(make())));
        self.clear = Some(|any| {
            #[cfg(target_has_atomic = "ptr")]
            if let Some(slot) = any.downcast_mut::<super::ArcValue<T>>() {
                slot.0.reset();
            }
            #[cfg(not(target_has_atomic = "ptr"))]
            let _ = any;
        });
        self.payload(type_key, Residency::Cpu)
    }

    /// A payload carrying `handle`, as [`Payload::foreign`] (or [`Payload::foreign_local`] when
    /// `local`), in the slot's allocation when free.
    pub fn foreign(
        &mut self,
        type_key: impl Into<TypeKey>,
        handle: ForeignHandle,
        residency: Residency,
        local: bool,
    ) -> Payload {
        if let Some(slot) = self.free::<ForeignStorage>() {
            slot.handle = Some(handle);
            slot.local = local;
        } else {
            self.storage = Some(crate::portable::arc_dyn!(ForeignStorage {
                handle: Some(handle),
                local
            }));
            self.clear = Some(|any| {
                if let Some(slot) = any.downcast_mut::<ForeignStorage>() {
                    slot.handle = None;
                }
            });
        }
        self.payload(type_key.into(), residency)
    }

    /// Drop the value the slot holds when no payload holds it any more (keeping the
    /// allocation), else let go of it: the slot never keeps a value alive.
    pub fn release(&mut self) {
        let clear = self.clear;
        match self.storage.as_mut().and_then(Arc::get_mut) {
            Some(storage) => {
                if let Some(clear) = clear {
                    clear(storage.as_any_mut());
                }
            }
            None => self.storage = None,
        }
    }

    /// The slot's storage as `S`, when nothing else holds it.
    fn free<S: 'static>(&mut self) -> Option<&mut S> {
        Arc::get_mut(self.storage.as_mut()?)?
            .as_any_mut()
            .downcast_mut::<S>()
    }

    fn payload(&self, type_key: TypeKey, residency: Residency) -> Payload {
        Payload {
            type_key,
            storage: Body::Shared(self.storage.clone().expect("filled above")),
            residency,
            layout: None,
            residency_cache: ResidencyCache::default(),
            lineage: PayloadLineage::new(),
        }
    }
}

#[cfg(all(test, target_has_atomic = "ptr"))]
mod tests {
    use super::*;

    #[test]
    fn a_free_slot_reuses_its_allocation_and_a_held_one_does_not() {
        let mut slot = PayloadSlot::default();
        let first = slot.owned("i64", 1_i64);
        let address = first.get_ref::<i64>().map(|v| v as *const i64);
        drop(first);
        let second = slot.owned("i64", 2_i64);
        assert_eq!(second.get_ref::<i64>(), Some(&2));
        assert_eq!(second.get_ref::<i64>().map(|v| v as *const i64), address);
        // Still held: the next payload gets its own allocation.
        let third = slot.owned("i64", 3_i64);
        assert_eq!(
            (second.get_ref::<i64>(), third.get_ref::<i64>()),
            (Some(&2), Some(&3))
        );
        assert!(
            second.get_ref::<i64>().map(|v| v as *const i64)
                != third.get_ref::<i64>().map(|v| v as *const i64)
        );
    }

    #[test]
    fn release_drops_the_value_and_never_keeps_a_held_one() {
        let value = Arc::new(());
        let mut slot = PayloadSlot::default();
        drop(slot.owned("arc", Some(value.clone())));
        assert_eq!(Arc::strong_count(&value), 2, "kept until released");
        slot.release();
        assert_eq!(Arc::strong_count(&value), 1, "released in place");
        let held = slot.owned("arc", Some(value.clone()));
        slot.release();
        assert_eq!(Arc::strong_count(&value), 2, "the payload still holds it");
        drop(held);
        assert_eq!(Arc::strong_count(&value), 1, "the slot let go of it");
    }
}
