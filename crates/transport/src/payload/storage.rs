//! Type-erased payload storage backends.

use crate::portable::Arc;
use core::any::{Any, TypeId};
use core::fmt;

use crate::{ForeignBorrow, ForeignHandle, ReleaseMode, TypeKey};

/// Type-erased payload storage.
pub trait PayloadStorage: Send + Sync + fmt::Debug {
    fn as_any(&self) -> &dyn Any;
    fn as_any_mut(&mut self) -> &mut dyn Any;
    fn value_any(&self) -> Option<&dyn Any> {
        None
    }
    /// Borrow the stored value as a thread-safe `Any`, when the storage exposes one.
    ///
    /// Unlike [`PayloadStorage::value_any`], this keeps the `Send + Sync` bounds so the value can
    /// be handed to serializer maps keyed by `TypeId` that expect `&(dyn Any + Send + Sync)`.
    fn value_any_sync(&self) -> Option<&(dyn Any + Send + Sync)> {
        None
    }
    fn rust_type_name(&self) -> Option<&'static str> {
        None
    }
    /// `TypeId` of the stored Rust value; `None` for storage that holds no Rust value of its
    /// own (bytes).
    fn rust_type_id(&self) -> Option<TypeId> {
        None
    }
    fn bytes_estimate(&self) -> Option<u64> {
        None
    }
    fn release_mode(&self) -> ReleaseMode {
        ReleaseMode::ImmediateNonBlocking
    }
    /// The [`ForeignHandle`] the payload carries (see [`Payload::foreign`](crate::Payload::foreign)).
    fn foreign_handle(&self) -> Option<&ForeignHandle> {
        None
    }
    /// The value lent through a foreign interface: the carried handle's, or an owner value its
    /// provider exposes (see [`Payload::provide_foreign`](crate::Payload::provide_foreign)).
    fn foreign_borrow(&self) -> Option<ForeignBorrow<'_>> {
        self.foreign_handle().map(ForeignHandle::borrow)
    }
    /// An owning handle of [`Self::foreign_borrow`]'s value.
    fn to_foreign_handle(&self) -> Option<ForeignHandle> {
        self.foreign_handle().cloned()
    }
}

pub(super) struct TypedStorage<T: Send + Sync + 'static> {
    pub(super) type_key: TypeKey,
    pub(super) value: Arc<T>,
    pub(super) bytes_estimate: Option<u64>,
}

impl<T: Send + Sync + 'static> fmt::Debug for TypedStorage<T> {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.debug_struct("TypedStorage")
            .field("type_key", &self.type_key)
            .field("rust_type_name", &core::any::type_name::<T>())
            .field("bytes_estimate", &self.bytes_estimate)
            .finish_non_exhaustive()
    }
}

impl<T: Send + Sync + 'static> PayloadStorage for TypedStorage<T> {
    fn as_any(&self) -> &dyn Any {
        self
    }

    fn as_any_mut(&mut self) -> &mut dyn Any {
        self
    }

    fn value_any(&self) -> Option<&dyn Any> {
        Some(self.value.as_ref())
    }

    fn value_any_sync(&self) -> Option<&(dyn Any + Send + Sync)> {
        Some(self.value.as_ref())
    }

    fn rust_type_name(&self) -> Option<&'static str> {
        Some(core::any::type_name::<T>())
    }

    fn rust_type_id(&self) -> Option<TypeId> {
        Some(TypeId::of::<T>())
    }

    fn bytes_estimate(&self) -> Option<u64> {
        self.bytes_estimate
    }
}

/// A Rust value as payload storage in its own `Arc` allocation: `repr(transparent)` over `T`,
/// so an `Arc<T>` becomes payload storage by changing only its vtable ([`arc_storage`]) and a
/// shared value costs no wrapper allocation. Only ever lives inside such an `Arc`.
#[repr(transparent)]
pub(super) struct ArcValue<T>(pub(super) T);

impl<T: Send + Sync + 'static> fmt::Debug for ArcValue<T> {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.debug_struct("ArcValue")
            .field("rust_type_name", &core::any::type_name::<T>())
            .finish_non_exhaustive()
    }
}

impl<T: Send + Sync + 'static> PayloadStorage for ArcValue<T> {
    fn as_any(&self) -> &dyn Any {
        self
    }

    fn as_any_mut(&mut self) -> &mut dyn Any {
        self
    }

    fn value_any(&self) -> Option<&dyn Any> {
        Some(&self.0)
    }

    fn value_any_sync(&self) -> Option<&(dyn Any + Send + Sync)> {
        Some(&self.0)
    }

    fn rust_type_name(&self) -> Option<&'static str> {
        Some(core::any::type_name::<T>())
    }

    fn rust_type_id(&self) -> Option<TypeId> {
        Some(TypeId::of::<T>())
    }
}

/// `value` as payload storage: its own allocation retyped as [`ArcValue<T>`] where `Arc` can
/// unsize-coerce, else wrapped in [`TypedStorage`] (`portable-atomic-util::Arc`).
pub(super) fn arc_storage<T: Send + Sync + 'static>(
    type_key: &TypeKey,
    value: Arc<T>,
) -> Arc<dyn PayloadStorage> {
    #[cfg(target_has_atomic = "ptr")]
    {
        let _ = type_key;
        // Safety: `ArcValue<T>` is `repr(transparent)` over `T` (same size, alignment and
        // drop), so the allocation is a valid `ArcInner<ArcValue<T>>` holding this reference.
        let value: Arc<ArcValue<T>> =
            unsafe { Arc::from_raw(Arc::into_raw(value).cast::<ArcValue<T>>()) };
        value
    }
    #[cfg(not(target_has_atomic = "ptr"))]
    {
        crate::portable::arc_dyn!(TypedStorage {
            type_key: type_key.clone(),
            value,
            bytes_estimate: None,
        })
    }
}

/// The `Arc<T>` behind `storage` when it is [`ArcValue<T>`] storage ([`arc_storage`]), sharing
/// its allocation (one more strong reference).
pub(super) fn arc_value<T: Send + Sync + 'static>(
    storage: &Arc<dyn PayloadStorage>,
) -> Option<Arc<T>> {
    let value = storage.as_any().downcast_ref::<ArcValue<T>>()?;
    if !core::ptr::addr_eq(value, Arc::as_ptr(storage)) {
        return None;
    }
    #[cfg(target_has_atomic = "ptr")]
    {
        // Safety: the allocation is an `ArcInner<ArcValue<T>>` (checked above), layout-equal to
        // `ArcInner<T>`; the clone's reference moves into the returned `Arc`.
        Some(unsafe { Arc::from_raw(Arc::into_raw(storage.clone()).cast::<T>()) })
    }
    #[cfg(not(target_has_atomic = "ptr"))]
    None
}

/// Move `storage` back into its `Arc<T>` when it is [`ArcValue<T>`] storage; `Err` hands it back.
pub(super) fn into_arc_value<T: Send + Sync + 'static>(
    storage: Arc<dyn PayloadStorage>,
) -> Result<Arc<T>, Arc<dyn PayloadStorage>> {
    let is_value = storage
        .as_any()
        .downcast_ref::<ArcValue<T>>()
        .is_some_and(|value| core::ptr::addr_eq(value, Arc::as_ptr(&storage)));
    #[cfg(target_has_atomic = "ptr")]
    if is_value {
        // Safety: as in `arc_value`; the reference moves into the returned `Arc`.
        return Ok(unsafe { Arc::from_raw(Arc::into_raw(storage).cast::<T>()) });
    }
    let _ = is_value;
    Err(storage)
}

#[derive(Debug)]
pub(super) struct BytesStorage {
    pub(super) bytes: Arc<[u8]>,
}

impl PayloadStorage for BytesStorage {
    fn as_any(&self) -> &dyn Any {
        self
    }

    fn as_any_mut(&mut self) -> &mut dyn Any {
        self
    }

    fn value_any(&self) -> Option<&dyn Any> {
        Some(&self.bytes)
    }

    fn value_any_sync(&self) -> Option<&(dyn Any + Send + Sync)> {
        Some(&self.bytes)
    }

    fn rust_type_name(&self) -> Option<&'static str> {
        Some(core::any::type_name::<Arc<[u8]>>())
    }

    fn bytes_estimate(&self) -> Option<u64> {
        Some(self.bytes.len() as u64)
    }
}

/// The `T` in `storage`: a shared value's own allocation or typed storage.
pub(super) fn typed_ref<T: Send + Sync + 'static>(storage: &dyn PayloadStorage) -> Option<&T> {
    let any = storage.as_any();
    if let Some(value) = any.downcast_ref::<ArcValue<T>>() {
        return Some(&value.0);
    }
    any.downcast_ref::<TypedStorage<T>>()
        .map(|storage| storage.value.as_ref())
}

/// The `Arc<T>` in `storage` (see [`typed_ref`]).
pub(super) fn typed_arc<T: Send + Sync + 'static>(
    storage: &Arc<dyn PayloadStorage>,
) -> Option<Arc<T>> {
    arc_value::<T>(storage).or_else(|| {
        storage
            .as_any()
            .downcast_ref::<TypedStorage<T>>()
            .map(|storage| storage.value.clone())
    })
}
