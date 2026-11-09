//! A value owned by code outside this build, across a boundary. It names no kind of value: an
//! interface is a key, a version and a table of functions, and a handle is data, a table and an
//! owner to retain and release.

use core::ffi::c_void;
use core::fmt;
use core::marker::PhantomData;

/// Which interface a table implements. Compared by key, version and layout hash, since each
/// build has its own copy of this struct.
#[repr(C)]
#[derive(Clone, Copy)]
pub struct ForeignInterfaceInfo {
    pub(crate) key_ptr: *const u8,
    pub(crate) key_len: usize,
    pub version: u32,
    pub layout_hash: u64,
}

// SAFETY: `key_ptr` points at a `&'static str` and is only ever read.
unsafe impl Send for ForeignInterfaceInfo {}
// SAFETY: as above: the fields never change and the text is immutable.
unsafe impl Sync for ForeignInterfaceInfo {}

impl ForeignInterfaceInfo {
    pub const fn new(
        key: &'static str,
        version: u32,
        signature: &'static str,
        table_size: usize,
        table_align: usize,
    ) -> Self {
        ForeignInterfaceInfo {
            key_ptr: key.as_ptr(),
            key_len: key.len(),
            version,
            layout_hash: foreign_layout_hash(signature, table_size, table_align),
        }
    }

    pub fn key(&self) -> &'static str {
        if self.key_ptr.is_null() {
            return "<invalid>";
        }
        // SAFETY: `new` is the only constructor, so this is the text of a `&'static str`. A table
        // from another build is checked by the handshake before any handle reaches this.
        let bytes = unsafe { core::slice::from_raw_parts(self.key_ptr, self.key_len) };
        core::str::from_utf8(bytes).unwrap_or("<invalid>")
    }

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
        fmt::Display::fmt(self, f)
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

/// FNV-1a over a table's signature text with whitespace removed, then its size and alignment.
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

/// A named table of functions. Write these with [`foreign_interface!`](crate::foreign_interface).
///
/// # Safety
///
/// `info` must describe `VTable` truthfully: readers cast a table to `VTable` when the info
/// matches.
// SAFETY: the contract is the section above.
pub unsafe trait ForeignInterface: 'static {
    type VTable: Copy + Send + Sync + 'static;
    const KEY: &'static str;
    const VERSION: u32;
    fn info() -> &'static ForeignInterfaceInfo;
}

/// A type that implements an interface.
///
/// # Safety
///
/// Every function in the table must be sound to call with a pointer to a live `Self`.
pub unsafe trait ProvideForeign<I: ForeignInterface>: Send + Sync + 'static {
    fn vtable() -> &'static I::VTable;
}

/// Keeps a value alive. `release` is called once for every retain, and once for the original.
#[repr(C)]
#[derive(Clone, Copy, Debug)]
pub struct ForeignOwner {
    pub ptr: *const c_void,
    pub retain: unsafe extern "C" fn(*const c_void),
    pub release: unsafe extern "C" fn(*const c_void),
}

#[cfg(feature = "alloc")]
// SAFETY: must be called with a pointer from `Arc::into_raw` for this `T`, still alive.
unsafe extern "C" fn arc_retain<T>(ptr: *const c_void) {
    // SAFETY: `ForeignOwner::from_arc` is the only source of this pointer, and it is still held.
    unsafe { alloc::sync::Arc::increment_strong_count(ptr.cast::<T>()) }
}

#[cfg(feature = "alloc")]
// SAFETY: must be called with a pointer from `Arc::into_raw` for this `T`, once per reference.
unsafe extern "C" fn arc_release<T>(ptr: *const c_void) {
    // SAFETY: `ForeignOwner::from_arc` is the only source of this pointer, and this call gives up
    // exactly one of the references.
    unsafe { alloc::sync::Arc::decrement_strong_count(ptr.cast::<T>()) }
}

#[cfg(feature = "alloc")]
impl ForeignOwner {
    pub fn from_arc<T: Send + Sync + 'static>(value: alloc::sync::Arc<T>) -> ForeignOwner {
        ForeignOwner {
            ptr: alloc::sync::Arc::into_raw(value).cast(),
            retain: arc_retain::<T>,
            release: arc_release::<T>,
        }
    }
}

/// A shared reference to a value owned elsewhere. Cloning retains and dropping releases.
#[repr(C)]
pub struct ForeignHandle {
    pub(crate) data: *const c_void,
    pub(crate) vtable: *const c_void,
    pub(crate) interface: *const ForeignInterfaceInfo,
    pub(crate) owner: ForeignOwner,
}

// SAFETY: `ProvideForeign` requires `Send + Sync` of the value, and the owner's functions are
// reference counting, which is safe from any thread.
unsafe impl Send for ForeignHandle {}
// SAFETY: as above: everything reachable through a shared handle is `Sync`.
unsafe impl Sync for ForeignHandle {}

impl ForeignHandle {
    /// A handle to `value`, implementing `I`.
    #[cfg(feature = "alloc")]
    pub fn from_arc<O, I>(value: alloc::sync::Arc<O>) -> ForeignHandle
    where
        I: ForeignInterface,
        O: ProvideForeign<I>,
    {
        let data = alloc::sync::Arc::as_ptr(&value).cast();
        // SAFETY: `data` stays alive as long as the owner made from the same `Arc`, and
        // `ProvideForeign` promises the table works on it.
        unsafe { Self::from_raw_parts::<I>(data, O::vtable(), ForeignOwner::from_arc(value)) }
    }

    /// # Safety
    ///
    /// `owner` must keep `data` alive, and every function in `vtable` must be sound to call with
    /// `data`. This handle takes over one reference, which it releases when dropped.
    // SAFETY: the contract is the section above.
    pub unsafe fn from_raw_parts<I: ForeignInterface>(
        data: *const c_void,
        vtable: &'static I::VTable,
        owner: ForeignOwner,
    ) -> ForeignHandle {
        ForeignHandle {
            data,
            vtable: core::ptr::from_ref(vtable).cast(),
            interface: I::info(),
            owner,
        }
    }

    pub fn interface(&self) -> &ForeignInterfaceInfo {
        // SAFETY: `interface` came from a `&'static` in the constructor.
        unsafe { &*self.interface }
    }

    pub fn data(&self) -> *const c_void {
        self.data
    }

    /// The table, if this handle implements `I`.
    pub fn view<I: ForeignInterface>(&self) -> Result<ForeignRef<'_, I>, ForeignInterfaceMismatch> {
        let found = self.interface();
        if !core::ptr::eq(found, I::info()) && !found.same_interface(I::info()) {
            return Err(ForeignInterfaceMismatch {
                expected: *I::info(),
                found: *found,
            });
        }
        Ok(ForeignRef {
            data: self.data,
            // SAFETY: the info matched, and `ForeignInterface` promises it describes `VTable`.
            vtable: unsafe { &*self.vtable.cast::<I::VTable>() },
            interface: PhantomData,
        })
    }
}

impl Clone for ForeignHandle {
    fn clone(&self) -> Self {
        // SAFETY: this handle holds a reference, so the value is alive to be retained.
        unsafe { (self.owner.retain)(self.owner.ptr) };
        ForeignHandle {
            data: self.data,
            vtable: self.vtable,
            interface: self.interface,
            owner: self.owner,
        }
    }
}

impl Drop for ForeignHandle {
    fn drop(&mut self) {
        // SAFETY: this handle holds exactly one reference, and this gives it up.
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

/// A handle's table was for a different interface.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub struct ForeignInterfaceMismatch {
    pub expected: ForeignInterfaceInfo,
    pub found: ForeignInterfaceInfo,
}

impl fmt::Display for ForeignInterfaceMismatch {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(
            f,
            "foreign interface mismatch: expected {}, found {}",
            self.expected, self.found
        )
    }
}

impl core::error::Error for ForeignInterfaceMismatch {}

/// A checked view of a handle: the table for `I` and the data to call it with.
pub struct ForeignRef<'a, I: ForeignInterface> {
    data: *const c_void,
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
        self.data
    }
}

impl<I: ForeignInterface> fmt::Debug for ForeignRef<'_, I> {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.debug_struct("ForeignRef")
            .field("interface", I::info())
            .field("data", &self.data)
            .finish()
    }
}

/// Says a type implements an interface through a table. For crates that forbid `unsafe`: a table
/// of safe functions can't do anything unsound with the pointer it is given.
#[macro_export]
macro_rules! provide_foreign {
    ($owner:ty : $iface:ty = $table:expr) => {
        // SAFETY: the table's functions are the caller's, and are sound with a live `$owner`.
        unsafe impl $crate::ProvideForeign<$iface> for $owner {
            fn vtable() -> &'static <$iface as $crate::ForeignInterface>::VTable {
                $table
            }
        }
    };
}

/// Declares an interface and its table.
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

        // SAFETY: the info below is built from this table's own field text, size and alignment.
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
