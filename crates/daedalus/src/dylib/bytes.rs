//! [`SharedBytes`]: an immutable, reference-counted byte buffer that crosses the stable plugin
//! boundary by reference, and `daedalus:bytes` ([`BytesInterface`]), the foreign interface it
//! crosses as.
//!
//! A `Vec<u8>` port copies its bytes once into the receiving side's allocation (the host and a
//! separately built plugin do not share an allocator, so neither can adopt the other's `Vec`).
//! A `SharedBytes` port instead lends the buffer: the receiver gets a `SharedBytes` that reads
//! the sender's memory in place and keeps it alive through the sender's own reference count.
//!
//! Lifetime rules:
//!
//! - The bytes are immutable and stay at the same address while any `SharedBytes` (or
//!   [`ForeignHandle`]) referring to them lives, on either side; cloning, slicing and crossing
//!   the boundary never copy them.
//! - A buffer a plugin allocated is released by the plugin's code, so the host keeps the plugin
//!   build that produced it loaded while it holds the buffer: a reloaded plugin's old library is
//!   only closed once every `SharedBytes` it produced is dropped.
//! - Building one from a `Vec<u8>` moves the vector (no copy); from a slice or a `Value::Bytes`
//!   constant copies it once.

use std::any::Any;
use std::ffi::c_void;
use std::fmt;
use std::ops::{Deref, RangeBounds};
use std::sync::Arc;

use crate::data::daedalus_type::DaedalusTypeExpr;
use crate::data::model::{TypeExpr, Value};
use crate::data::to_value::ToValue;
use crate::transport::{ForeignHandle, ForeignInterfaceMismatch, ProvideForeign};

/// [`SharedBytes`]'s type key.
pub const SHARED_BYTES_KEY: &str = "daedalus:shared_bytes";

crate::transport::foreign_interface! {
    /// Read access to an immutable byte buffer owned by another build (`daedalus:bytes` v1).
    /// [`SharedBytes`] crosses the stable plugin boundary as a handle of it.
    pub interface BytesInterface("daedalus:bytes", version = 1);
    /// Accessors of `daedalus:bytes` v1: the buffer's address and length, both fixed while the
    /// owner is referenced.
    pub struct BytesVTable {
        pub ptr: unsafe extern "C" fn(data: *const c_void) -> *const u8,
        pub len: unsafe extern "C" fn(data: *const c_void) -> usize,
    }
}

unsafe extern "C" fn vec_ptr(data: *const c_void) -> *const u8 {
    // Safety: the vtable is only used with a `Vec<u8>` data pointer.
    unsafe { (*data.cast::<Vec<u8>>()).as_ptr() }
}

unsafe extern "C" fn vec_len(data: *const c_void) -> usize {
    // Safety: as above.
    unsafe { (*data.cast::<Vec<u8>>()).len() }
}

static VEC_VTABLE: BytesVTable = BytesVTable {
    ptr: vec_ptr,
    len: vec_len,
};

// Safety: the functions read the `Vec<u8>` behind the data pointer; a shared `Vec` is immutable.
unsafe impl ProvideForeign<BytesInterface> for Vec<u8> {
    fn vtable() -> &'static BytesVTable {
        &VEC_VTABLE
    }
}

unsafe extern "C" fn shared_ptr(data: *const c_void) -> *const u8 {
    // Safety: the vtable is only used with a `SharedBytes` data pointer.
    unsafe { (*data.cast::<SharedBytes>()).ptr }
}

unsafe extern "C" fn shared_len(data: *const c_void) -> usize {
    // Safety: as above.
    unsafe { (*data.cast::<SharedBytes>()).len }
}

static SHARED_VTABLE: BytesVTable = BytesVTable {
    ptr: shared_ptr,
    len: shared_len,
};

// Safety: the functions read the view of the `SharedBytes` behind the data pointer, which never
// changes.
unsafe impl ProvideForeign<BytesInterface> for SharedBytes {
    fn vtable() -> &'static BytesVTable {
        &SHARED_VTABLE
    }
}

/// An immutable byte buffer shared by reference count, here or across the stable plugin boundary
/// (see the [module docs](self) for the lifetime rules). Derefs to `[u8]`; clones and slices
/// share the buffer.
#[derive(Clone)]
pub struct SharedBytes {
    ptr: *const u8,
    len: usize,
    owner: Owner,
}

#[derive(Clone)]
enum Owner {
    Local(Arc<Vec<u8>>),
    /// Fields drop in order: the handle (released through its owner's code) before the
    /// keepalive that keeps that code loaded.
    Foreign {
        handle: ForeignHandle,
        keepalive: Option<Arc<dyn Any + Send + Sync>>,
    },
}

// Safety: the buffer is immutable and its owners (`Arc`, `ForeignHandle`) are `Send + Sync`.
unsafe impl Send for SharedBytes {}
// Safety: see above.
unsafe impl Sync for SharedBytes {}

impl SharedBytes {
    /// Share `bytes` (moved, not copied).
    pub fn new(bytes: Vec<u8>) -> Self {
        Self::from_arc(Arc::new(bytes))
    }

    /// Share the vector in `bytes` (not copied).
    pub fn from_arc(bytes: Arc<Vec<u8>>) -> Self {
        Self {
            ptr: bytes.as_ptr(),
            len: bytes.len(),
            owner: Owner::Local(bytes),
        }
    }

    /// Read the buffer behind a `daedalus:bytes` handle in place.
    ///
    /// `keepalive` is held until the last clone drops, after the handle: pass what keeps the
    /// handle's vtable and owner code loaded when it belongs to a library that may be unloaded.
    pub fn from_handle(
        handle: ForeignHandle,
        keepalive: Option<Arc<dyn Any + Send + Sync>>,
    ) -> Result<Self, ForeignInterfaceMismatch> {
        let view = handle.view::<BytesInterface>()?;
        // Safety: a `daedalus:bytes` provider accepts its data pointer and reports a buffer
        // that stays valid while the handle holds its owner.
        let (ptr, len) = unsafe {
            (
                (view.vtable().ptr)(view.data()),
                (view.vtable().len)(view.data()),
            )
        };
        Ok(Self {
            ptr,
            len,
            owner: Owner::Foreign { handle, keepalive },
        })
    }

    /// The bytes in `range`, sharing the buffer.
    ///
    /// # Panics
    /// When `range` is out of bounds, as slice indexing does.
    pub fn slice(&self, range: impl RangeBounds<usize>) -> Self {
        let bytes = &self[(range.start_bound().cloned(), range.end_bound().cloned())];
        Self {
            ptr: bytes.as_ptr(),
            len: bytes.len(),
            owner: self.owner.clone(),
        }
    }

    /// Whether the buffer belongs to another build (it crossed the plugin boundary).
    pub fn is_foreign(&self) -> bool {
        matches!(self.owner, Owner::Foreign { .. })
    }

    /// A `daedalus:bytes` handle of these bytes for another build: the vector's own reference
    /// count for a whole local buffer, a retain of the handle for a whole foreign buffer that
    /// needs no keepalive, otherwise a handle owning a clone of `self` (one allocation). Never
    /// copies the bytes.
    pub fn to_handle(&self) -> ForeignHandle {
        match &self.owner {
            Owner::Local(vec) if vec.as_ptr() == self.ptr && vec.len() == self.len => {
                ForeignHandle::from_arc::<Vec<u8>, BytesInterface>(vec.clone())
            }
            Owner::Foreign {
                handle,
                keepalive: None,
            } if self.is_whole_handle(handle) => handle.clone(),
            _ => ForeignHandle::from_arc::<Self, BytesInterface>(Arc::new(self.clone())),
        }
    }

    /// Whether `self` views exactly `handle`'s buffer (not a slice of it).
    fn is_whole_handle(&self, handle: &ForeignHandle) -> bool {
        let Ok(view) = handle.view::<BytesInterface>() else {
            return false;
        };
        // Safety: as in `from_handle`.
        unsafe {
            (view.vtable().ptr)(view.data()) == self.ptr
                && (view.vtable().len)(view.data()) == self.len
        }
    }
}

impl Deref for SharedBytes {
    type Target = [u8];

    fn deref(&self) -> &[u8] {
        if self.len == 0 {
            return &[];
        }
        // Safety: `owner` keeps `len` immutable bytes at `ptr` alive.
        unsafe { std::slice::from_raw_parts(self.ptr, self.len) }
    }
}

impl AsRef<[u8]> for SharedBytes {
    fn as_ref(&self) -> &[u8] {
        self
    }
}

impl Default for SharedBytes {
    fn default() -> Self {
        Self::new(Vec::new())
    }
}

impl From<Vec<u8>> for SharedBytes {
    fn from(bytes: Vec<u8>) -> Self {
        Self::new(bytes)
    }
}

impl From<&[u8]> for SharedBytes {
    fn from(bytes: &[u8]) -> Self {
        Self::new(bytes.to_vec())
    }
}

impl PartialEq for SharedBytes {
    fn eq(&self, other: &Self) -> bool {
        **self == **other
    }
}

impl Eq for SharedBytes {}

impl fmt::Debug for SharedBytes {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.debug_struct("SharedBytes")
            .field("len", &self.len)
            .field("foreign", &self.is_foreign())
            .finish()
    }
}

impl DaedalusTypeExpr for SharedBytes {
    const TYPE_KEY: &'static str = SHARED_BYTES_KEY;

    fn type_expr() -> TypeExpr {
        TypeExpr::Opaque(SHARED_BYTES_KEY.to_string())
    }

    /// A `Value::Bytes` constant, copied once.
    fn from_value(value: &Value) -> Option<Self> {
        match value {
            Value::Bytes(bytes) => Some(Self::from(&bytes[..])),
            _ => None,
        }
    }
}

impl ToValue for SharedBytes {
    /// `Value::Bytes`, a copy (for inspection and serialization).
    fn to_value(&self) -> Value {
        Value::Bytes(self.to_vec().into())
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn shared_bytes_cross_as_handles_without_copying() {
        let bytes = SharedBytes::new(vec![1, 2, 3, 4]);
        let address = bytes.as_ptr();
        assert!(!bytes.is_foreign());

        // A whole local buffer lends its vector; the other side reads it in place.
        let received = SharedBytes::from_handle(bytes.to_handle(), None).unwrap();
        assert!(received.is_foreign());
        assert_eq!(
            (received.as_ptr(), &*received),
            (address, &[1, 2, 3, 4][..])
        );
        // Passing it back retains the same handle.
        let back = SharedBytes::from_handle(received.to_handle(), None).unwrap();
        assert_eq!(back.as_ptr(), address);

        // Slices share the buffer, also across a handle.
        let tail = received.slice(2..);
        assert_eq!(
            (&*tail, tail.as_ptr()),
            (&[3, 4][..], address.wrapping_add(2))
        );
        let tail = SharedBytes::from_handle(tail.to_handle(), None).unwrap();
        assert_eq!(
            (&*tail, tail.as_ptr()),
            (&[3, 4][..], address.wrapping_add(2))
        );

        // The keepalive outlives the buffer's handle.
        let keepalive = Arc::new(());
        let kept = SharedBytes::from_handle(bytes.to_handle(), Some(keepalive.clone())).unwrap();
        assert_eq!(Arc::strong_count(&keepalive), 2);
        let wrapped = SharedBytes::from_handle(kept.to_handle(), None).unwrap();
        drop(kept);
        assert_eq!(
            Arc::strong_count(&keepalive),
            2,
            "the wrapping handle keeps it"
        );
        drop(wrapped);
        assert_eq!(Arc::strong_count(&keepalive), 1);

        assert_eq!(
            SharedBytes::from_value(&Value::Bytes(vec![9].into())),
            Some(SharedBytes::from(&[9][..]))
        );
        assert!(SharedBytes::default().is_empty());
    }
}
