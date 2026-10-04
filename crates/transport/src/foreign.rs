//! Foreign interfaces: C-safe accessor vtables that let separately built code read a value
//! without sharing its Rust type.
//!
//! A Rust type only means the same thing to two binaries when Cargo compiled its crate
//! identically for both. A dynamic plugin built in another cargo invocation can resolve a shared
//! third-party crate (a camera library, say) with other features, so its `FrameLease` is a
//! different type from the host's even under the same key. A foreign interface sidesteps that:
//!
//! - The interface is a `#[repr(C)]` struct of `extern "C"` accessor functions (a vtable)
//!   declared with [`foreign_interface!`](crate::foreign_interface), identified by a key (`"daedalus:frame"`), a version,
//!   and a layout hash of the vtable's field signatures, size and alignment
//!   ([`ForeignInterfaceInfo`]). Two separately compiled copies of the same declaration agree on
//!   all three; a changed declaration does not.
//! - The owner of a type implements [`ProvideForeign<I>`] once (the accessors read its own
//!   type), and the runtime wraps a shared value as a [`ForeignHandle`]: data pointer, vtable
//!   pointer, interface info and a reference-counted [`ForeignOwner`] keepalive. Building one is
//!   an `Arc` clone; nothing is copied.
//! - A consumer checks the handle against its own copy of the interface
//!   ([`ForeignHandle::view`]) and reads through [`ForeignRef`]. Every field it touches is a C
//!   type, and every function it calls is the owner's code, so it never depends on how the
//!   consumer built the owner's crate.

use alloc::sync::Arc;
use core::ffi::c_void;
use core::fmt;
use core::marker::PhantomData;

use crate::Payload;

/// Identity of a foreign interface: key, version and vtable layout hash.
///
/// `#[repr(C)]` so it can be exported in plugin descriptors. Built only by [`Self::new`] from
/// `'static` data (normally inside [`foreign_interface!`](crate::foreign_interface)).
#[repr(C)]
#[derive(Clone, Copy)]
pub struct ForeignInterfaceInfo {
    key_ptr: *const u8,
    key_len: usize,
    /// Semantic version of the interface; bump it whenever the vtable changes meaning or shape.
    pub version: u32,
    /// [`foreign_layout_hash`] of the vtable declaration.
    pub layout_hash: u64,
}

// Safety: the key points to `'static`, immutable string data.
unsafe impl Send for ForeignInterfaceInfo {}
// Safety: see above.
unsafe impl Sync for ForeignInterfaceInfo {}

impl ForeignInterfaceInfo {
    /// Describe an interface. `signature` lists the vtable fields (see [`foreign_layout_hash`]).
    pub const fn new(
        key: &'static str,
        version: u32,
        signature: &'static str,
        vtable_size: usize,
        vtable_align: usize,
    ) -> Self {
        Self {
            key_ptr: key.as_ptr(),
            key_len: key.len(),
            version,
            layout_hash: foreign_layout_hash(signature, vtable_size, vtable_align),
        }
    }

    /// The interface key, e.g. `daedalus:frame` (`<invalid>` for a corrupt plugin table).
    pub fn key(&self) -> &'static str {
        if self.key_ptr.is_null() {
            return "<invalid>";
        }
        // Safety: `new` only accepts `&'static str` (plugin libraries are never unloaded).
        let bytes = unsafe { core::slice::from_raw_parts(self.key_ptr, self.key_len) };
        core::str::from_utf8(bytes).unwrap_or("<invalid>")
    }

    /// Whether both describe the same interface (key, version and layout).
    pub fn same_interface(&self, other: &Self) -> bool {
        self.key() == other.key()
            && self.version == other.version
            && self.layout_hash == other.layout_hash
    }
}

impl PartialEq for ForeignInterfaceInfo {
    fn eq(&self, other: &Self) -> bool {
        self.same_interface(other)
    }
}

impl Eq for ForeignInterfaceInfo {}

impl fmt::Debug for ForeignInterfaceInfo {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.debug_struct("ForeignInterfaceInfo")
            .field("key", &self.key())
            .field("version", &self.version)
            .field("layout_hash", &format_args!("{:016x}", self.layout_hash))
            .finish()
    }
}

impl fmt::Display for ForeignInterfaceInfo {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(
            f,
            "`{}` v{} (layout {:016x})",
            self.key(),
            self.version,
            self.layout_hash
        )
    }
}

/// FNV-1a hash of a vtable declaration: its field signature text with ASCII whitespace removed
/// (so formatting does not matter), then its size and alignment.
pub const fn foreign_layout_hash(signature: &str, size: usize, align: usize) -> u64 {
    const PRIME: u64 = 0x0000_0100_0000_01b3;
    let bytes = signature.as_bytes();
    let mut hash: u64 = 0xcbf2_9ce4_8422_2325;
    let mut i = 0;
    while i < bytes.len() {
        if !bytes[i].is_ascii_whitespace() {
            hash = (hash ^ bytes[i] as u64).wrapping_mul(PRIME);
        }
        i += 1;
    }
    let tail = [size as u64, align as u64];
    let mut j = 0;
    while j < tail.len() {
        let word = tail[j].to_le_bytes();
        let mut k = 0;
        while k < word.len() {
            hash = (hash ^ word[k] as u64).wrapping_mul(PRIME);
            k += 1;
        }
        j += 1;
    }
    hash
}

/// A foreign interface: a C-safe vtable type plus its identity.
///
/// Declare interfaces with [`foreign_interface!`](crate::foreign_interface) rather than implementing this by hand.
///
/// # Safety
/// `VTable` must be `#[repr(C)]` with only C-safe fields, and [`Self::info`] must return this
/// interface's key and version with the layout hash of exactly this vtable declaration, so a
/// matching hash lets a consumer reinterpret a vtable pointer from another build as `VTable`.
pub unsafe trait ForeignInterface: 'static {
    type VTable: Copy + Send + Sync + 'static;
    const KEY: &'static str;
    const VERSION: u32;
    fn info() -> &'static ForeignInterfaceInfo;
}

/// Implemented by the owner of a type to expose it through interface `I`.
///
/// # Safety
/// Every function in [`Self::vtable`] must accept a pointer to a live `Self` as its data
/// argument and must not unwind.
pub unsafe trait ProvideForeign<I: ForeignInterface>: Send + Sync + 'static {
    fn vtable() -> &'static I::VTable;
}

/// Keepalive of a [`ForeignHandle`]: an opaque pointer with retain/release functions.
///
/// The functions belong to the owner's build, so the owner's own code adjusts its reference
/// count and eventually drops the value with its own allocator.
#[repr(C)]
#[derive(Clone, Copy, Debug)]
pub struct ForeignOwner {
    pub ptr: *const c_void,
    pub retain: unsafe extern "C" fn(*const c_void),
    pub release: unsafe extern "C" fn(*const c_void),
}

impl ForeignOwner {
    /// Take over one strong reference of `value`.
    pub fn from_arc<T: Send + Sync + 'static>(value: Arc<T>) -> Self {
        unsafe extern "C" fn retain<T>(ptr: *const c_void) {
            // Safety: `ptr` came from `Arc::into_raw` and the caller holds a reference.
            unsafe { Arc::increment_strong_count(ptr.cast::<T>()) }
        }
        unsafe extern "C" fn release<T>(ptr: *const c_void) {
            // Safety: `ptr` came from `Arc::into_raw` and the caller gives up a reference.
            unsafe { Arc::decrement_strong_count(ptr.cast::<T>()) }
        }
        Self {
            ptr: Arc::into_raw(value).cast(),
            retain: retain::<T>,
            release: release::<T>,
        }
    }
}

/// A shared value seen through a foreign interface: data pointer, vtable, interface identity and
/// an owning reference. `#[repr(C)]`, `Clone` retains and `Drop` releases.
#[repr(C)]
pub struct ForeignHandle {
    data: *const c_void,
    vtable: *const c_void,
    interface: *const ForeignInterfaceInfo,
    owner: ForeignOwner,
}

// Safety: providers are `Send + Sync`, the vtable and interface info are immutable statics, and
// the owner's retain/release are thread-safe reference counts.
unsafe impl Send for ForeignHandle {}
// Safety: see above.
unsafe impl Sync for ForeignHandle {}

impl ForeignHandle {
    /// Wrap a shared value through its provider for `I`. Costs one `Arc` move, no copy.
    pub fn from_arc<O, I>(value: Arc<O>) -> Self
    where
        I: ForeignInterface,
        O: ProvideForeign<I>,
    {
        let data = Arc::as_ptr(&value).cast();
        // Safety: `data` points into the `Arc` the owner keeps alive, and `O` provides `I`.
        unsafe { Self::from_raw_parts::<I>(data, O::vtable(), ForeignOwner::from_arc(value)) }
    }

    /// Wrap the `O` inside `payload` (an `Arc` clone when the payload shares it, otherwise the
    /// payload itself is kept alive). `None` when the payload does not hold an `O`.
    pub fn from_payload<O, I>(payload: &Payload) -> Option<Self>
    where
        I: ForeignInterface,
        O: ProvideForeign<I>,
    {
        if let Some(value) = payload.get_arc::<O>() {
            return Some(Self::from_arc::<O, I>(value));
        }
        let data: *const c_void = (payload.get_ref::<O>()? as *const O).cast();
        // The value lives in the payload's shared storage; a clone pins that storage (it can no
        // longer be taken out or mutated, which both require a unique payload).
        let owner = ForeignOwner::from_arc(Arc::new(payload.clone()));
        // Safety: `data` stays valid while `owner` holds the payload clone.
        Some(unsafe { Self::from_raw_parts::<I>(data, O::vtable(), owner) })
    }

    /// Assemble a handle from parts; it takes over one reference of `owner`.
    ///
    /// # Safety
    /// `vtable`'s functions must accept `data`, which must stay valid while `owner` holds a
    /// reference.
    pub unsafe fn from_raw_parts<I: ForeignInterface>(
        data: *const c_void,
        vtable: &'static I::VTable,
        owner: ForeignOwner,
    ) -> Self {
        Self {
            data,
            vtable: (vtable as *const I::VTable).cast(),
            interface: I::info(),
            owner,
        }
    }

    /// The interface the handle was built for.
    pub fn interface(&self) -> &ForeignInterfaceInfo {
        // Safety: `interface` points to a static `ForeignInterfaceInfo`.
        unsafe { &*self.interface }
    }

    /// The opaque data pointer the vtable functions take.
    pub fn data(&self) -> *const c_void {
        self.data
    }

    /// The owner's keepalive (e.g. to compare it with the source `Arc`).
    pub fn owner(&self) -> ForeignOwner {
        self.owner
    }

    /// Check the handle against this build's copy of `I` and view it through `I`.
    pub fn view<I: ForeignInterface>(&self) -> Result<ForeignRef<'_, I>, ForeignInterfaceMismatch> {
        let found = self.interface();
        // Same build: same static. Otherwise compare the identities.
        if !core::ptr::eq(found, I::info()) && !found.same_interface(I::info()) {
            return Err(ForeignInterfaceMismatch {
                expected: *I::info(),
                found: *found,
            });
        }
        Ok(ForeignRef {
            handle: self,
            // Safety: key, version and layout hash match, so the vtable has `I::VTable`'s layout.
            vtable: unsafe { &*self.vtable.cast::<I::VTable>() },
            interface: PhantomData,
        })
    }
}

impl Clone for ForeignHandle {
    fn clone(&self) -> Self {
        // Safety: the handle holds a reference, so the owner is alive.
        unsafe { (self.owner.retain)(self.owner.ptr) };
        Self {
            data: self.data,
            vtable: self.vtable,
            interface: self.interface,
            owner: self.owner,
        }
    }
}

impl Drop for ForeignHandle {
    fn drop(&mut self) {
        // Safety: gives up the reference this handle holds.
        unsafe { (self.owner.release)(self.owner.ptr) };
    }
}

impl fmt::Debug for ForeignHandle {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.debug_struct("ForeignHandle")
            .field("interface", self.interface())
            .field("data", &self.data)
            .finish_non_exhaustive()
    }
}

/// A handle's interface differs from the consumer's copy of it.
#[derive(Clone, Copy, Debug, PartialEq, Eq, thiserror::Error)]
#[error("foreign interface mismatch: expected {expected}, found {found}")]
pub struct ForeignInterfaceMismatch {
    pub expected: ForeignInterfaceInfo,
    pub found: ForeignInterfaceInfo,
}

/// A checked view of a [`ForeignHandle`] through interface `I`.
///
/// Call the vtable's functions with [`Self::data`]; the handle keeps the value alive for `'a`.
/// Interfaces add typed accessors on top (see [`FrameView`](crate::FrameView)).
pub struct ForeignRef<'a, I: ForeignInterface> {
    handle: &'a ForeignHandle,
    vtable: &'a I::VTable,
    interface: PhantomData<I>,
}

impl<I: ForeignInterface> Clone for ForeignRef<'_, I> {
    fn clone(&self) -> Self {
        *self
    }
}

impl<I: ForeignInterface> Copy for ForeignRef<'_, I> {}

impl<'a, I: ForeignInterface> ForeignRef<'a, I> {
    pub fn vtable(&self) -> &'a I::VTable {
        self.vtable
    }

    pub fn data(&self) -> *const c_void {
        self.handle.data
    }

    pub fn handle(&self) -> &'a ForeignHandle {
        self.handle
    }
}

impl<I: ForeignInterface> fmt::Debug for ForeignRef<'_, I> {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        self.handle.fmt(f)
    }
}

/// Types a node can take as a foreign-interface input (`ForeignRef<'_, I>` and aliases such as
/// [`FrameView`](crate::FrameView)); the node macros fetch them with `NodeIo::get_foreign`.
pub trait ForeignView<'a>: Sized {
    type Interface: ForeignInterface;
    fn from_handle(handle: &'a ForeignHandle) -> Result<Self, ForeignInterfaceMismatch>;
}

impl<'a, I: ForeignInterface> ForeignView<'a> for ForeignRef<'a, I> {
    type Interface = I;

    fn from_handle(handle: &'a ForeignHandle) -> Result<Self, ForeignInterfaceMismatch> {
        handle.view::<I>()
    }
}

/// Declare a foreign interface: a marker type implementing [`ForeignInterface`] and its
/// `#[repr(C)]` vtable.
///
/// The layout hash covers the field names and types as written (whitespace ignored) plus the
/// vtable's size and alignment, so keep the declaration stable and bump the version for any
/// change. Fields should be `unsafe extern "C" fn` pointers taking the data pointer first.
///
/// ```
/// use core::ffi::c_void;
///
/// daedalus_transport::foreign_interface! {
///     /// A counter value.
///     pub interface CounterInterface("example:counter_view", version = 1);
///     /// Accessors of `example:counter_view` v1.
///     pub struct CounterVTable {
///         pub value: unsafe extern "C" fn(data: *const c_void) -> i64,
///     }
/// }
/// assert_eq!(
///     <CounterInterface as daedalus_transport::ForeignInterface>::info().key(),
///     "example:counter_view"
/// );
/// ```
#[macro_export]
macro_rules! foreign_interface {
    (
        $(#[$imeta:meta])*
        $ivis:vis interface $iface:ident ($key:expr, version = $version:expr);
        $(#[$vmeta:meta])*
        $vvis:vis struct $vtable:ident {
            $( $(#[$fmeta:meta])* $fvis:vis $field:ident : $fty:ty ),* $(,)?
        }
    ) => {
        $(#[$imeta])*
        #[derive(Clone, Copy, Debug)]
        $ivis enum $iface {}

        $(#[$vmeta])*
        #[repr(C)]
        #[derive(Clone, Copy, Debug)]
        $vvis struct $vtable {
            $( $(#[$fmeta])* $fvis $field: $fty ),*
        }

        // Safety: the vtable is `#[repr(C)]` and the info hashes exactly its declaration.
        unsafe impl $crate::ForeignInterface for $iface {
            type VTable = $vtable;
            const KEY: &'static str = $key;
            const VERSION: u32 = $version;

            fn info() -> &'static $crate::ForeignInterfaceInfo {
                static INFO: $crate::ForeignInterfaceInfo = $crate::ForeignInterfaceInfo::new(
                    $key,
                    $version,
                    ::core::concat!($( ::core::stringify!($field: $fty), ";" ),*),
                    ::core::mem::size_of::<$vtable>(),
                    ::core::mem::align_of::<$vtable>(),
                );
                &INFO
            }
        }
    };
}

#[cfg(test)]
mod tests;
