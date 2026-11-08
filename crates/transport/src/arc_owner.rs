//! The owner functions of an `Arc` owner, and the type-erased operations a frame needs on one:
//! a weak reference, its upgrade, and the strong count.
//!
//! The owner's retain and release ([`arc_retain`], [`arc_release`]) are generic: each `T` has its
//! own addresses, which is how [`ArcOps`] and a frame's owner are matched to the same `T`.

use core::any::TypeId;
use core::ffi::c_void;
use core::mem::ManuallyDrop;

use crate::portable::{Arc, Weak};

/// One more strong reference of the `Arc<T>` whose data is `ptr` (a handle's retain).
///
/// # Safety
/// `ptr` must come from `Arc::<T>::into_raw`, and the caller must hold a strong reference.
pub(crate) unsafe extern "C" fn arc_retain<T>(ptr: *const c_void) {
    // Safety: the caller holds a strong reference (see the contract).
    unsafe { Arc::increment_strong_count(ptr.cast::<T>()) }
}

/// One fewer strong reference of the `Arc<T>` whose data is `ptr` (a handle's release).
///
/// # Safety
/// `ptr` must come from `Arc::<T>::into_raw`, and the caller must give up a strong reference.
pub(crate) unsafe extern "C" fn arc_release<T>(ptr: *const c_void) {
    // Safety: the caller gives up a strong reference (see the contract).
    unsafe { Arc::decrement_strong_count(ptr.cast::<T>()) }
}

/// The type-erased operations on an `Arc<T>`, for one `T` ([`ArcOps::of`]).
///
/// A "weak" here is a [`Weak<T>`] as its data pointer (`Weak::into_raw`): the pointer is the
/// `Arc`'s data pointer, and the operations rebuild the `Weak<T>` from it. The table is `'static`
/// and built once per type; a frame carries it only when its owner is the `Arc<T>` it was built
/// for, so the table's `T` is the value's type.
#[doc(hidden)]
pub struct ArcOps {
    type_id: fn() -> TypeId,
    weak: unsafe fn(*const c_void) -> *const c_void,
    clone_weak: unsafe fn(*const c_void) -> *const c_void,
    drop_weak: unsafe fn(*const c_void),
    upgrade: unsafe fn(*const c_void) -> Option<*const c_void>,
    strong_count: unsafe fn(*const c_void) -> usize,
}

impl ArcOps {
    /// The operations of `Arc<T>` (a table per `T`, promoted to `'static`).
    pub(crate) fn of<T: 'static>() -> &'static Self {
        &Self {
            type_id: TypeId::of::<T>,
            weak: weak_of::<T>,
            clone_weak: clone_weak::<T>,
            drop_weak: drop_weak::<T>,
            upgrade: upgrade::<T>,
            strong_count: strong_count::<T>,
        }
    }

    /// Whether the table's `T` is `T`.
    pub(crate) fn is<T: 'static>(&self) -> bool {
        (self.type_id)() == TypeId::of::<T>()
    }

    /// A weak reference to the `Arc` whose data is `ptr` (a strong reference is held).
    ///
    /// # Safety
    /// `ptr` is the data pointer of a live `Arc` of this table's type.
    pub(crate) unsafe fn downgrade(&self, ptr: *const c_void) -> *const c_void {
        // Safety: forwarded.
        unsafe { (self.weak)(ptr) }
    }

    /// A clone of the weak reference `ptr`.
    ///
    /// # Safety
    /// `ptr` is a weak reference from [`Self::downgrade`] or [`Self::clone_weak`], not yet dropped.
    pub(crate) unsafe fn clone_weak(&self, ptr: *const c_void) -> *const c_void {
        // Safety: forwarded.
        unsafe { (self.clone_weak)(ptr) }
    }

    /// Drops the weak reference `ptr`.
    ///
    /// # Safety
    /// `ptr` is a weak reference from [`Self::downgrade`] or [`Self::clone_weak`], dropped once.
    pub(crate) unsafe fn drop_weak(&self, ptr: *const c_void) {
        // Safety: forwarded.
        unsafe { (self.drop_weak)(ptr) }
    }

    /// One more strong reference of the weak `ptr`'s value, when one is alive: the data pointer
    /// of the `Arc`, whose strong count the caller now owns.
    ///
    /// # Safety
    /// `ptr` is a weak reference not yet dropped.
    pub(crate) unsafe fn upgrade(&self, ptr: *const c_void) -> Option<*const c_void> {
        // Safety: forwarded.
        unsafe { (self.upgrade)(ptr) }
    }

    /// The strong count of the weak `ptr`'s value.
    ///
    /// # Safety
    /// `ptr` is a weak reference not yet dropped.
    pub(crate) unsafe fn strong_count(&self, ptr: *const c_void) -> usize {
        // Safety: forwarded.
        unsafe { (self.strong_count)(ptr) }
    }
}

unsafe fn weak_of<T>(ptr: *const c_void) -> *const c_void {
    // Safety: the caller's `Arc` keeps the allocation; this borrow is not dropped.
    let strong = ManuallyDrop::new(unsafe { Arc::from_raw(ptr.cast::<T>()) });
    Weak::into_raw(Arc::downgrade(&strong)).cast()
}

unsafe fn clone_weak<T>(ptr: *const c_void) -> *const c_void {
    // Safety: `ptr` is a weak reference (see the table); the borrow is not dropped.
    let weak = ManuallyDrop::new(unsafe { Weak::from_raw(ptr.cast::<T>()) });
    Weak::into_raw(Weak::clone(&weak)).cast()
}

unsafe fn drop_weak<T>(ptr: *const c_void) {
    // Safety: `ptr` is a weak reference given up here.
    drop(unsafe { Weak::from_raw(ptr.cast::<T>()) });
}

unsafe fn upgrade<T>(ptr: *const c_void) -> Option<*const c_void> {
    // Safety: `ptr` is a weak reference; the borrow is not dropped.
    let weak = ManuallyDrop::new(unsafe { Weak::from_raw(ptr.cast::<T>()) });
    weak.upgrade().map(|strong| Arc::into_raw(strong).cast())
}

unsafe fn strong_count<T>(ptr: *const c_void) -> usize {
    // Safety: `ptr` is a weak reference; the borrow is not dropped.
    let weak = ManuallyDrop::new(unsafe { Weak::from_raw(ptr.cast::<T>()) });
    weak.strong_count()
}
