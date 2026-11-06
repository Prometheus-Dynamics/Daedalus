//! Plugin-local objects: [`Local<T>`], a shared object of one build that crosses the stable
//! plugin boundary as an opaque [`ForeignHandle`] and comes back to that build as the same
//! object, never encoded.
//!
//! A `Local<T>` is an `Arc` of the value behind a [`LocalHeader`]. On the stable path it becomes a
//! handle of the `daedalus:plugin_local` interface ([`LocalInterface`]) whose data pointer is the
//! `Arc`'s and whose retain and release are the creating build's own functions, so the host
//! carries it along graph edges without reading it, and a node of the same build reads it back as
//! `Local<T>` after checking the header: the creating build (by the address of a static in that
//! build's copy of this crate) and `T`. A handle from another build, including an earlier or later
//! build of the same plugin across a reload, is refused ([`LocalError`]), never cast.
//!
//! The object stays at one address for its whole life: [`Local::as_ptr`],
//! [`ForeignHandle::data`] of [`Local::handle`], and the handle's data on every edge and tick are
//! the same pointer.

use alloc::string::String;
use core::any::TypeId;
use core::ffi::c_void;
use core::fmt;
use core::ops::Deref;

use crate::portable::Arc;
use crate::{ForeignHandle, ProvideForeign};

crate::foreign_interface! {
    /// An object of the build that created it, carried opaquely (see [`Local`]).
    pub interface LocalInterface("daedalus:plugin_local", version = 1);
    /// Accessors of `daedalus:plugin_local` v1.
    pub struct LocalVTable {
        /// The object's [`LocalHeader`].
        pub header: unsafe extern "C" fn(data: *const c_void) -> *const LocalHeader,
    }
}

/// One byte per build of this crate: its address tells builds apart.
static BUILD_MARK: u8 = 0;

fn build_mark() -> *const c_void {
    (&raw const BUILD_MARK).cast()
}

/// What any build reads of a plugin-local object: the creating build and the Rust type's name.
/// `#[repr(C)]` and first in the object, so every build agrees on these fields; the creating
/// build's own fields follow.
#[repr(C)]
#[derive(Debug)]
pub struct LocalHeader {
    /// The address of the creating build's mark.
    pub build: *const c_void,
    pub type_name_ptr: *const u8,
    pub type_name_len: usize,
}

impl LocalHeader {
    /// The Rust type's name (the creating build's `type_name`).
    ///
    /// # Safety
    /// The header must be a live object's, read during that object's life.
    pub unsafe fn type_name(&self) -> &str {
        // Safety: the creating build's static string; forwarded from the caller.
        let bytes = unsafe { core::slice::from_raw_parts(self.type_name_ptr, self.type_name_len) };
        core::str::from_utf8(bytes).unwrap_or("<invalid>")
    }
}

#[repr(C)]
struct LocalBox<T> {
    header: LocalHeader,
    /// Read only by the creating build (the build mark matched).
    type_id: TypeId,
    value: T,
}

// Safety: the header's pointers are to statics of the creating build.
unsafe impl<T: Send> Send for LocalBox<T> {}
// Safety: as above; the value is only shared.
unsafe impl<T: Sync> Sync for LocalBox<T> {}

unsafe extern "C" fn header(data: *const c_void) -> *const LocalHeader {
    // The header is the first field of the `#[repr(C)]` box.
    data.cast()
}

// Safety: `header` takes a pointer to a live `LocalBox<T>` and returns its first field without
// unwinding.
unsafe impl<T: Send + Sync + 'static> ProvideForeign<LocalInterface> for LocalBox<T> {
    fn vtable() -> &'static LocalVTable {
        static VTABLE: LocalVTable = LocalVTable { header };
        &VTABLE
    }
}

/// A shared object of this build that crosses the stable plugin boundary as an opaque
/// `daedalus:plugin_local` handle ([`LocalInterface`]) and comes back to this build as the same
/// object; another build's handle is refused ([`LocalError`]). It stays at one address for its
/// life: [`Self::as_ptr`] is [`ForeignHandle::data`] of every handle of it. Cloning shares it. `Default` is an empty `Local`, which only a reused
/// payload slot holds; dereferencing one panics.
pub struct Local<T: Send + Sync + 'static>(Option<Arc<LocalBox<T>>>);

impl<T: Send + Sync + 'static> Local<T> {
    pub fn new(value: T) -> Self {
        let type_name = core::any::type_name::<T>();
        Self(Some(Arc::new(LocalBox {
            header: LocalHeader {
                build: build_mark(),
                type_name_ptr: type_name.as_ptr(),
                type_name_len: type_name.len(),
            },
            type_id: TypeId::of::<T>(),
            value,
        })))
    }

    fn inner(&self) -> &Arc<LocalBox<T>> {
        self.0
            .as_ref()
            .expect("an empty `Local` (a released payload slot's)")
    }

    /// The value, or `None` for an empty `Local`.
    pub fn try_get(&self) -> Option<&T> {
        self.0.as_ref().map(|inner| &inner.value)
    }

    /// The object's address, which is also [`ForeignHandle::data`] of its handles. It does not
    /// change while any clone or handle of the object lives.
    pub fn as_ptr(&self) -> *const c_void {
        Arc::as_ptr(self.inner()).cast()
    }

    /// Whether `a` and `b` are the same object.
    pub fn ptr_eq(a: &Self, b: &Self) -> bool {
        a.as_ptr() == b.as_ptr()
    }

    /// A handle of the object (one more reference; the data pointer is [`Self::as_ptr`]).
    pub fn handle(&self) -> ForeignHandle {
        ForeignHandle::from_arc::<LocalBox<T>, LocalInterface>(self.inner().clone())
    }

    /// The object of `handle`, when this build created it as a `Local<T>`.
    pub fn from_handle(handle: &ForeignHandle) -> Result<Self, LocalError> {
        let view = handle
            .view::<LocalInterface>()
            .map_err(|_| LocalError::NotLocal {
                interface: String::from(handle.interface().key()),
            })?;
        // Safety: the vtable is the interface's; the creating build returns the header of the
        // live object, which the handle keeps alive.
        let header = unsafe { &*(view.vtable().header)(view.data()) };
        if header.build != build_mark() {
            return Err(LocalError::OtherBuild {
                // Safety: as above.
                type_name: String::from(unsafe { header.type_name() }),
            });
        }
        let object = view.data().cast::<LocalBox<T>>();
        // Safety: this build created the object, so it is a `LocalBox<_>` of this build's layout;
        // in a `#[repr(C)]` box `type_id` follows the header at an offset that does not depend on
        // the value type, so it is readable before `T` is known to match.
        if unsafe { (*object).type_id } != TypeId::of::<T>() {
            return Err(LocalError::OtherType {
                // Safety: as above.
                found: String::from(unsafe { header.type_name() }),
                expected: core::any::type_name::<T>(),
            });
        }
        // Safety: `object` came from `Arc::into_raw` of an `Arc<LocalBox<T>>` of this build
        // (`ForeignHandle::from_arc`) and is alive; one more strong reference for the new `Arc`.
        unsafe {
            Arc::increment_strong_count(object);
            Ok(Self(Some(Arc::from_raw(object))))
        }
    }
}

impl<T: Send + Sync + 'static> Clone for Local<T> {
    fn clone(&self) -> Self {
        Self(self.0.clone())
    }
}

impl<T: Send + Sync + 'static> Default for Local<T> {
    fn default() -> Self {
        Self(None)
    }
}

impl<T: Send + Sync + 'static> Deref for Local<T> {
    type Target = T;
    fn deref(&self) -> &T {
        &self.inner().value
    }
}

impl<T: Send + Sync + fmt::Debug + 'static> fmt::Debug for Local<T> {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.debug_tuple("Local").field(&self.try_get()).finish()
    }
}

/// A handle could not be read back as a [`Local<T>`].
#[derive(Clone, Debug, PartialEq, Eq)]
pub enum LocalError {
    /// The handle is not a plugin-local object.
    NotLocal { interface: String },
    /// Another build created it: another plugin, or another build of this one (a reload).
    OtherBuild { type_name: String },
    /// This build created it, as another type.
    OtherType {
        found: String,
        expected: &'static str,
    },
}

impl fmt::Display for LocalError {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Self::NotLocal { interface } => {
                write!(
                    f,
                    "expected a plugin-local object, found a `{interface}` handle"
                )
            }
            Self::OtherBuild { type_name } => write!(
                f,
                "plugin-local `{type_name}` was created by another build (another plugin, or this \
                 plugin before a reload)"
            ),
            Self::OtherType { found, expected } => {
                write!(f, "plugin-local object is `{found}`, expected `{expected}`")
            }
        }
    }
}

impl core::error::Error for LocalError {}

/// The stable path codec of a `Local<T>` port, as plain function pointers.
#[derive(Clone, Copy)]
pub struct LocalCodec {
    pub type_id: TypeId,
    /// A handle of a port value (`None` when it is not a `Local<T>`, or is empty).
    pub to_handle: fn(&(dyn core::any::Any + Send + Sync)) -> Option<ForeignHandle>,
    /// A `Local<T>` payload of a handle, in `slot`'s allocation when it is free.
    pub decode: fn(
        slot: &mut crate::PayloadSlot,
        key: crate::TypeKey,
        handle: &ForeignHandle,
    ) -> Result<crate::Payload, LocalError>,
}

impl fmt::Debug for LocalCodec {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.debug_struct("LocalCodec").finish_non_exhaustive()
    }
}

/// A `Local<T>` port type, with its [`LocalCodec`].
pub trait LocalPort: Send + Sync + 'static {
    fn codec() -> LocalCodec;
}

impl<T: Send + Sync + 'static> LocalPort for Local<T> {
    fn codec() -> LocalCodec {
        fn to_handle<T: Send + Sync + 'static>(
            value: &(dyn core::any::Any + Send + Sync),
        ) -> Option<ForeignHandle> {
            let local = value.downcast_ref::<Local<T>>()?;
            local.0.as_ref()?;
            Some(local.handle())
        }
        fn decode<T: Send + Sync + 'static>(
            slot: &mut crate::PayloadSlot,
            key: crate::TypeKey,
            handle: &ForeignHandle,
        ) -> Result<crate::Payload, LocalError> {
            Ok(slot.owned(key, Local::<T>::from_handle(handle)?))
        }
        LocalCodec {
            type_id: TypeId::of::<Local<T>>(),
            to_handle: to_handle::<T>,
            decode: decode::<T>,
        }
    }
}

#[cfg(all(test, target_has_atomic = "ptr"))]
mod tests {
    use super::*;

    #[test]
    fn a_local_round_trips_through_its_handle_at_one_address() {
        let local = Local::new(41_u32);
        let handle = local.handle();
        assert_eq!(handle.data(), local.as_ptr());
        let back = Local::<u32>::from_handle(&handle).unwrap();
        assert!(Local::ptr_eq(&back, &local));
        assert_eq!(*back, 41);
        assert!(matches!(
            Local::<u64>::from_handle(&handle),
            Err(LocalError::OtherType { .. })
        ));
        drop((local, handle));
        assert_eq!(*back, 41, "the last clone keeps it");
    }
}
