//! Recycled owner payloads: a payload whose owner value lives in a node that a [`RecyclePool`]
//! keeps for reuse, so a warm payload allocates nothing (see [`Payload::providing_recycled`]).
//!
//! A node is one allocation: a [`Header`] (the holder count, the owner's `TypeId`, the pool and
//! the owner's drop and free functions) and the owner value. The payload holds a
//! [`ForeignHandle`] over the node, whose owner functions are this module's [`retain`] and
//! [`release`]. The last holder releases the owner value first (its buffers close at the same
//! moment they do for an `Arc`), then the node goes back on its pool's free list, or is freed when
//! the pool is gone.
//!
//! Same-build reads ([`Payload::get_ref`]) recognise a node without trusting the handle's data:
//! see [`recycled_ref`].

use core::any::TypeId;
use core::ffi::c_void;
use core::marker::PhantomData;
use core::mem::MaybeUninit;
use core::ptr::{addr_of, null_mut};

use alloc::boxed::Box;

use super::inline::Body;
use super::residency::ResidencyCache;
use super::{Payload, PayloadStorage};
use crate::portable::{Arc, AtomicUsize, Mutex, lock_recover};
use crate::{
    ForeignBorrow, ForeignHandle, ForeignInterface, ForeignOwner, PayloadLineage, ProvideForeign,
    Residency, TypeKey,
};
use core::any::Any;
use core::sync::atomic::Ordering;

/// Recycled owner nodes of one owner type `O`: a payload built by
/// [`Payload::providing_recycled`] takes a node from the pool, and the node goes back when the
/// payload's last holder lets go.
///
/// Nodes are reused, not freed, so once the pool has grown to the number of payloads in flight,
/// building one allocates nothing. The pool is shared by its nodes: dropping the pool frees the
/// idle nodes at once, and a node still held by a payload is freed when that payload's last
/// holder lets go. A pool is `Send + Sync`; one per camera or per process is the usual shape.
pub struct RecyclePool<O> {
    inner: Arc<PoolInner>,
    owner: PhantomData<fn() -> O>,
}

impl<O: Send + Sync + 'static> RecyclePool<O> {
    /// An empty pool: its first payloads allocate their nodes.
    pub fn new() -> Self {
        Self {
            inner: Arc::new(PoolInner {
                state: Mutex::new(FreeList {
                    head: null_mut(),
                    len: 0,
                    closed: false,
                }),
            }),
            owner: PhantomData,
        }
    }

    /// Idle nodes: ones a released payload left for reuse. A warm pool has at least one per
    /// payload it recycled, once they are all released.
    pub fn idle(&self) -> usize {
        lock_recover(&self.inner.state).len
    }
}

impl<O: Send + Sync + 'static> Default for RecyclePool<O> {
    fn default() -> Self {
        Self::new()
    }
}

impl<O> core::fmt::Debug for RecyclePool<O> {
    fn fmt(&self, f: &mut core::fmt::Formatter<'_>) -> core::fmt::Result {
        f.debug_struct("RecyclePool")
            .field("idle", &lock_recover(&self.inner.state).len)
            .finish_non_exhaustive()
    }
}

impl<O> Drop for RecyclePool<O> {
    fn drop(&mut self) {
        // Closing the pool under its lock makes every later release free its node instead of
        // listing it, so the idle nodes listed now are the last ones this pool owns.
        let mut node = {
            let mut state = lock_recover(&self.inner.state);
            state.closed = true;
            state.len = 0;
            core::mem::replace(&mut state.head, null_mut())
        };
        while !node.is_null() {
            // Safety: listed nodes are idle (no payload holds them), owned by the list alone.
            let next = unsafe { (*node).next };
            unsafe { ((*node).free)(node) };
            node = next;
        }
    }
}

/// The state a pool shares with its nodes: the free list and whether the pool is gone.
struct PoolInner {
    state: Mutex<FreeList>,
}

struct FreeList {
    /// Idle nodes, linked through [`Header::next`]. Only touched under the lock.
    head: *mut Header,
    len: usize,
    /// Set when the pool is dropped: released nodes are freed rather than listed.
    closed: bool,
}

// Safety: the list's nodes are owned by the pool while listed, and only the lock reads or writes
// the list.
unsafe impl Send for FreeList {}

impl PoolInner {
    /// An idle node, taken off the list (`None` when there is none).
    fn take(&self) -> Option<*mut Header> {
        let mut state = lock_recover(&self.state);
        let node = state.head;
        if node.is_null() {
            return None;
        }
        // Safety: a listed node is alive and owned by the list.
        state.head = unsafe { (*node).next };
        state.len -= 1;
        Some(node)
    }
}

/// The header every node starts with (`repr(C)`, so a node pointer is a header pointer).
#[repr(C)]
struct Header {
    /// Payloads and handles holding the owner value. Zero while the node is idle.
    holders: AtomicUsize,
    /// The owner's `TypeId`, which [`recycled_ref`] checks before reading the value as `T`.
    type_id: TypeId,
    /// The pool, as an `Arc::into_raw` reference held while the node is out; null when idle.
    pool: *const PoolInner,
    /// The next idle node, while listed.
    next: *mut Header,
    /// The owner's type name, for [`PayloadStorage::rust_type_name`].
    type_name: &'static str,
    /// Drops the owner value in place.
    drop_value: unsafe fn(*mut Header),
    /// Frees the node's allocation (its owner value must already be dropped or never written).
    free: unsafe fn(*mut Header),
}

/// A node: the header, then the owner value (written when the node is taken, dropped when its
/// last holder lets go, and never dropped while idle).
#[repr(C)]
struct Node<O> {
    header: Header,
    value: MaybeUninit<O>,
}

unsafe fn drop_value<O>(node: *mut Header) {
    // Safety: the last holder calls this once, on a node whose value a payload wrote.
    unsafe { (*node.cast::<Node<O>>()).value.assume_init_drop() };
}

unsafe fn free_node<O>(node: *mut Header) {
    // Safety: the node came from `Box::into_raw` of a `Node<O>`, and holds no owner value now.
    drop(unsafe { Box::from_raw(node.cast::<Node<O>>()) });
}

/// One more holder of the node at `ptr`.
///
/// Non-generic, and the only retain of this module: [`recycled_ref`] recognises a node by this
/// function's address (see there), so it is never generic.
#[inline(never)]
unsafe extern "C" fn retain(ptr: *const c_void) {
    // Safety: a handle over the node holds a reference, so the node is alive.
    unsafe {
        (*ptr.cast::<Header>())
            .holders
            .fetch_add(1, Ordering::Relaxed)
    };
}

/// One fewer holder. The last one drops the owner value, then gives the node back to its pool.
///
/// Non-generic (see [`retain`]): the owner-specific work goes through the header's function
/// pointers.
#[inline(never)]
unsafe extern "C" fn release(ptr: *const c_void) {
    let node = ptr.cast_mut().cast::<Header>();
    // Safety: the releasing holder has a reference, so the node is alive.
    if unsafe { (*node).holders.fetch_sub(1, Ordering::AcqRel) } != 1 {
        return;
    }
    // Safety: the last holder is alone with the node, and the value was written by the payload
    // that created it. The owner's drop runs first, so its buffers close before the node is idle.
    unsafe { ((*node).drop_value)(node) };
    // Safety: the node's pool reference (set when it was taken) is ours to give back.
    let pool = unsafe { core::mem::replace(&mut (*node).pool, null_mut()) };
    unsafe { give_back(pool, node) };
}

/// Give `node` back to `pool` (an `Arc::into_raw` reference this call takes over): listed when the
/// pool is open, freed when it is closed.
///
/// # Safety
/// `pool` must be a reference a node holds, and `node` a node whose owner value is dropped.
unsafe fn give_back(pool: *const PoolInner, node: *mut Header) {
    // Safety: `pool` is the `Arc::into_raw` reference that this call takes back.
    let pool = unsafe { Arc::from_raw(pool) };
    {
        let mut state = lock_recover(&pool.state);
        if state.closed {
            drop(state);
            // Safety: the value is dropped and no holder is left.
            unsafe { ((*node).free)(node) };
        } else {
            // Safety: the node is idle and owned by this call until it is listed.
            unsafe { (*node).next = state.head };
            state.head = node;
            state.len += 1;
        }
    }
    drop(pool);
}

/// The owner-function table of every node: this module's [`retain`] and [`release`].
fn owner_functions() -> (
    unsafe extern "C" fn(*const c_void),
    unsafe extern "C" fn(*const c_void),
) {
    let retain: unsafe extern "C" fn(*const c_void) = retain;
    let release: unsafe extern "C" fn(*const c_void) = release;
    (retain, release)
}

impl Payload {
    /// A payload of key `key` sharing `value`, readable through interface `I`, whose owner node
    /// comes from `pool` rather than a fresh `Arc`: the same contract as [`Self::providing`]
    /// (same key, same `I` view for a consumer of another build, `get_ref::<O>()` for this build),
    /// with no allocation once `pool` holds an idle node.
    ///
    /// The node is released with the payload's last holder: `value` is dropped first, then the
    /// node goes back to `pool` (or is freed, if the pool was dropped meanwhile).
    ///
    /// The owner's `Drop` must not panic: the last holder drops it inside an `extern "C"` release
    /// function, where a panic cannot unwind and aborts the process (the same contract as a
    /// foreign handle's owner, see [`ForeignOwner`](crate::ForeignOwner)).
    ///
    /// [`Self::get_ref`] returns the `O`; [`Self::get_arc`], [`Self::get_mut`] and
    /// [`Self::try_into_owned`] do not apply to a recycled node (there is no `Arc<O>` to hand
    /// out), so they return `None` or hand the payload back.
    pub fn providing_recycled<O, I>(
        key: impl Into<TypeKey>,
        pool: &RecyclePool<O>,
        value: O,
        residency: Residency,
    ) -> Self
    where
        I: ForeignInterface,
        O: ProvideForeign<I>,
    {
        let node = match pool.inner.take() {
            Some(node) => node.cast::<Node<O>>(),
            None => Box::into_raw(Box::new(Node::<O> {
                header: Header {
                    holders: AtomicUsize::new(0),
                    type_id: TypeId::of::<O>(),
                    pool: null_mut(),
                    next: null_mut(),
                    type_name: core::any::type_name::<O>(),
                    drop_value: drop_value::<O>,
                    free: free_node::<O>,
                },
                value: MaybeUninit::uninit(),
            })),
        };
        let (retain, release) = owner_functions();
        // Safety: the node is idle (taken from the pool's list or new), so this thread alone
        // writes it; the value is written before the handle is built, and the node's header
        // gets its first holder (the handle, below) and its pool reference.
        let (data, owner) = unsafe {
            (*node).value.write(value);
            (*node).header.holders.store(1, Ordering::Relaxed);
            (*node).header.pool = Arc::into_raw(pool.inner.clone());
            let data = addr_of!((*node).value).cast::<c_void>();
            let owner = ForeignOwner {
                ptr: node.cast::<c_void>(),
                retain,
                release,
            };
            (data, owner)
        };
        // Safety: `data` is the node's value, which lives as long as the handle's reference
        // (the node is freed or idled only when the last holder releases), and `O` provides `I`.
        let handle = unsafe {
            ForeignHandle::from_raw_parts::<I>(data, <O as ProvideForeign<I>>::vtable(), owner)
        };
        Payload {
            type_key: key.into(),
            storage: Body::Recycled(RecycledStorage(handle), residency),
            layout: None,
            residency_cache: ResidencyCache::default(),
            lineage: PayloadLineage::new(),
        }
    }
}

/// A recycled node's handle, held in place ([`Body::Recycled`]). It lends the owner through its
/// interface and retains for a consumer, as a provided payload's storage does, but it is not the
/// payload's foreign handle: [`PayloadStorage::foreign_handle`] is `None`, so a stable-path lend
/// takes a retained handle, exactly as for [`Payload::providing`].
#[derive(Clone, Debug)]
pub(super) struct RecycledStorage(pub(super) ForeignHandle);

impl RecycledStorage {
    /// Whether both hold one owner: the same value in the same node.
    pub(super) fn same_value(&self, other: &Self) -> bool {
        core::ptr::eq(self.0.data(), other.0.data())
            && core::ptr::eq(self.0.owner().ptr, other.0.owner().ptr)
    }
}

impl PayloadStorage for RecycledStorage {
    fn as_any(&self) -> &dyn Any {
        self
    }

    fn as_any_mut(&mut self) -> &mut dyn Any {
        self
    }

    fn rust_type_name(&self) -> Option<&'static str> {
        recycled_header(&self.0).map(|header| header.type_name)
    }

    fn rust_type_id(&self) -> Option<TypeId> {
        recycled_header(&self.0).map(|header| header.type_id)
    }

    fn foreign_borrow(&self) -> Option<ForeignBorrow<'_>> {
        Some(self.0.borrow())
    }

    fn to_foreign_handle(&self) -> Option<ForeignHandle> {
        Some(self.0.clone())
    }
}

/// The header of the node `handle` holds, when `handle` is one of this module's: its owner
/// functions are this module's [`release`], compared by address (`fn_addr_eq`, not a generic
/// instantiation).
///
/// The address is unique to this build's code, and only this module builds owners with it
/// ([`owner_functions`]): a handle from another build carries that build's function, so it is
/// never read as a node, and nothing is read from its data or owner pointers. Only a handle that
/// passes the check reads the node's header.
fn recycled_header(handle: &ForeignHandle) -> Option<&Header> {
    // Safety: a node pointer is alive for the handle's borrow (see `recycled_node`).
    recycled_node(handle).map(|node| unsafe { &*node })
}

/// The node `handle` holds, as a raw pointer with the provenance of the whole node (the handle's
/// owner pointer), when `handle` is one of this module's: its owner functions are this module's
/// [`release`], compared by address (`fn_addr_eq`, not a generic instantiation).
///
/// The address is unique to this build's code, and only this module builds owners with it
/// ([`owner_functions`]): a handle from another build carries that build's function, so it is
/// never read as a node, and nothing is read from its data or owner pointers. Only a handle that
/// passes the check reads the node's header. The handle holds a reference to the node, so the
/// node is alive for the handle's borrow.
fn recycled_node(handle: &ForeignHandle) -> Option<*const Header> {
    let owner = handle.owner();
    let (_, release_fn) = owner_functions();
    if !core::ptr::fn_addr_eq(owner.release, release_fn) {
        return None;
    }
    Some(owner.ptr.cast::<Header>())
}

/// The `T` a recycled handle holds, when `T` is the owner type of its node: the node's `TypeId`
/// must be `T`'s, and the handle's data must be the node's value.
pub(super) fn recycled_ref<T: 'static>(handle: &ForeignHandle) -> Option<&T> {
    let node = recycled_node(handle)?;
    // Safety: `node` is a live node of this module (see `recycled_node`).
    if unsafe { (*node).type_id } != TypeId::of::<T>() {
        return None;
    }
    // The value is reached through the node pointer itself (the owner pointer, which covers the
    // whole node), never through a reference to the header alone.
    let node = node.cast::<Node<T>>();
    // Safety: the header is the first field of a `Node<T>` (`repr(C)`, and `T` is the owner the
    // node was built for, by the `TypeId` check), so the value's address is in the node.
    let value = unsafe { addr_of!((*node).value) }.cast::<T>();
    if !core::ptr::eq(value.cast::<c_void>(), handle.data()) {
        return None;
    }
    // Safety: the handle holds the node's value, initialized until its last holder drops it.
    Some(unsafe { &*value })
}

#[cfg(test)]
mod tests;
