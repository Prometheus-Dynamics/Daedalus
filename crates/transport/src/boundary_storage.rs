use std::any::{Any, TypeId};
use std::collections::HashMap;
use std::fmt;
use std::os::raw::c_void;
use std::ptr::NonNull;
use std::sync::{Mutex, OnceLock};

use thiserror::Error;

use crate::{
    BoundaryCapabilities, BoundaryContractError, BoundaryTypeContract, PayloadStorage, TypeKey,
};

pub struct BoundaryVTable {
    pub drop_owned: unsafe fn(NonNull<c_void>),
    pub clone_shared: unsafe fn(NonNull<c_void>) -> Option<BoundaryStorage>,
    pub bytes_estimate: unsafe fn(NonNull<c_void>) -> u64,
    pub rust_type_name: &'static str,
}

impl fmt::Debug for BoundaryVTable {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.debug_struct("BoundaryVTable")
            .field("rust_type_name", &self.rust_type_name)
            .finish_non_exhaustive()
    }
}

pub struct BoundaryStorage {
    pub(crate) type_key: TypeKey,
    contract: BoundaryTypeContract,
    ptr: Option<NonNull<c_void>>,
    vtable: &'static BoundaryVTable,
}

// Safety: BoundaryStorage only constructs pointers from Box<T> where T: Send + Sync + 'static.
// The pointer is uniquely owned by this storage, dropped exactly once in Drop unless it is moved
// out through try_take_owned, and borrowed access requires &self or &mut self respectively.
unsafe impl Send for BoundaryStorage {}
// Safety: Shared references expose only &T and mutable access requires &mut BoundaryStorage.
// The stored T is Sync, and ownership is still tracked by the Option<NonNull<c_void>> slot.
unsafe impl Sync for BoundaryStorage {}

impl fmt::Debug for BoundaryStorage {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.debug_struct("BoundaryStorage")
            .field("type_key", &self.type_key)
            .field("contract", &self.contract)
            .field("rust_type_name", &self.vtable.rust_type_name)
            .finish_non_exhaustive()
    }
}

impl BoundaryStorage {
    pub fn owned<T>(
        type_key: impl Into<TypeKey>,
        value: T,
        capabilities: BoundaryCapabilities,
    ) -> Self
    where
        T: Send + Sync + 'static,
    {
        let type_key = type_key.into();
        let contract = BoundaryTypeContract::for_type::<T>(type_key.clone(), capabilities);
        let ptr = Box::into_raw(Box::new(value)).cast::<c_void>();
        Self {
            type_key,
            contract,
            ptr: NonNull::new(ptr),
            vtable: boundary_vtable::<T>(),
        }
    }

    pub fn contract(&self) -> &BoundaryTypeContract {
        &self.contract
    }

    pub fn is_taken(&self) -> bool {
        self.ptr.is_none()
    }

    pub fn try_take_owned<T>(
        &mut self,
        required: &BoundaryTypeContract,
    ) -> Result<T, BoundaryTakeError>
    where
        T: Send + Sync + 'static,
    {
        self.contract
            .compatible_with(required)
            .map_err(BoundaryTakeError::Incompatible)?;
        if self.vtable.rust_type_name != std::any::type_name::<T>() {
            return Err(BoundaryTakeError::RustType {
                expected: std::any::type_name::<T>(),
                found: self.vtable.rust_type_name,
            });
        }
        let ptr = self.ptr.take().ok_or(BoundaryTakeError::AlreadyTaken)?;
        // SAFETY: `owned::<T>` created this pointer from `Box<T>`, the vtable
        // type check above ensures the requested `T` matches that allocation,
        // and taking `self.ptr` prevents Drop from freeing it a second time.
        let boxed = unsafe { Box::from_raw(ptr.as_ptr().cast::<T>()) };
        Ok(*boxed)
    }

    pub fn try_borrow_ref<T>(
        &self,
        required: &BoundaryTypeContract,
    ) -> Result<&T, BoundaryTakeError>
    where
        T: Send + Sync + 'static,
    {
        self.contract
            .compatible_with(required)
            .map_err(BoundaryTakeError::Incompatible)?;
        if !self.contract.capabilities.borrow_ref {
            return Err(BoundaryTakeError::Capability("borrow_ref"));
        }
        if self.vtable.rust_type_name != std::any::type_name::<T>() {
            return Err(BoundaryTakeError::RustType {
                expected: std::any::type_name::<T>(),
                found: self.vtable.rust_type_name,
            });
        }
        let ptr = self.ptr.ok_or(BoundaryTakeError::AlreadyTaken)?;
        // SAFETY: the pointer came from `Box<T>`, remains owned by `self`, and
        // shared access is gated by both the contract and `&self`.
        Ok(unsafe { &*ptr.as_ptr().cast::<T>() })
    }

    pub fn try_borrow_mut<T>(
        &mut self,
        required: &BoundaryTypeContract,
    ) -> Result<&mut T, BoundaryTakeError>
    where
        T: Send + Sync + 'static,
    {
        self.contract
            .compatible_with(required)
            .map_err(BoundaryTakeError::Incompatible)?;
        if !self.contract.capabilities.borrow_mut {
            return Err(BoundaryTakeError::Capability("borrow_mut"));
        }
        if self.vtable.rust_type_name != std::any::type_name::<T>() {
            return Err(BoundaryTakeError::RustType {
                expected: std::any::type_name::<T>(),
                found: self.vtable.rust_type_name,
            });
        }
        let ptr = self.ptr.ok_or(BoundaryTakeError::AlreadyTaken)?;
        // SAFETY: the pointer came from `Box<T>`, remains owned by `self`, and
        // mutable access is gated by both the contract and `&mut self`.
        Ok(unsafe {
            ptr.as_ptr()
                .cast::<T>()
                .as_mut()
                .expect("non-null boundary pointer")
        })
    }

    pub fn bytes_estimate(&self) -> Option<u64> {
        // SAFETY: vtable functions are created by `boundary_vtable::<T>` for
        // the same concrete allocation stored in `self.ptr`; no ownership is
        // transferred by bytes estimation.
        self.ptr
            .map(|ptr| unsafe { (self.vtable.bytes_estimate)(ptr) })
    }
}

impl Drop for BoundaryStorage {
    fn drop(&mut self) {
        if let Some(ptr) = self.ptr.take() {
            // SAFETY: `self.ptr` is the unique owner of the original `Box<T>`.
            // Taking it here ensures the allocation is dropped at most once.
            unsafe { (self.vtable.drop_owned)(ptr) };
        }
    }
}

impl PayloadStorage for BoundaryStorage {
    fn as_any(&self) -> &dyn Any {
        self
    }

    fn as_any_mut(&mut self) -> &mut dyn Any {
        self
    }

    fn into_any(self: Box<Self>) -> Box<dyn Any + Send + Sync> {
        self
    }

    fn type_key(&self) -> &TypeKey {
        &self.type_key
    }

    fn rust_type_name(&self) -> Option<&'static str> {
        Some(self.vtable.rust_type_name)
    }

    fn bytes_estimate(&self) -> Option<u64> {
        self.bytes_estimate()
    }
}

#[derive(Debug, Error, PartialEq, Eq)]
pub enum BoundaryTakeError {
    #[error("{0}")]
    Incompatible(#[from] BoundaryContractError),
    #[error("boundary payload was already taken")]
    AlreadyTaken,
    #[error("boundary rust type mismatch: expected {expected}, found {found}")]
    RustType {
        expected: &'static str,
        found: &'static str,
    },
    #[error("boundary payload is shared")]
    Shared,
    #[error("payload is not boundary storage")]
    NotBoundary,
    #[error("boundary payload is missing required capability {0}")]
    Capability(&'static str),
}

fn boundary_vtable<T>() -> &'static BoundaryVTable
where
    T: Send + Sync + 'static,
{
    static VTABLES: OnceLock<Mutex<HashMap<TypeId, &'static BoundaryVTable>>> = OnceLock::new();

    unsafe fn drop_owned<T>(ptr: NonNull<c_void>) {
        // SAFETY: every vtable is installed only for values allocated by
        // `BoundaryStorage::owned::<T>`, so this pointer is a live `Box<T>`
        // unless ownership has already been taken out of the storage.
        drop(unsafe { Box::from_raw(ptr.as_ptr().cast::<T>()) });
    }

    unsafe fn clone_shared(_ptr: NonNull<c_void>) -> Option<BoundaryStorage> {
        None
    }

    unsafe fn bytes_estimate<T>(_ptr: NonNull<c_void>) -> u64 {
        // SAFETY: this implementation does not dereference the erased pointer.
        std::mem::size_of::<T>() as u64
    }

    let mut vtables = VTABLES
        .get_or_init(|| Mutex::new(HashMap::new()))
        .lock()
        .unwrap_or_else(|poisoned| poisoned.into_inner());
    if let Some(vtable) = vtables.get(&TypeId::of::<T>()).copied() {
        return vtable;
    }
    let vtable = Box::leak(Box::new(BoundaryVTable {
        drop_owned: drop_owned::<T>,
        clone_shared,
        bytes_estimate: bytes_estimate::<T>,
        rust_type_name: std::any::type_name::<T>(),
    }));
    vtables.insert(TypeId::of::<T>(), vtable);
    vtable
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::BoundaryCapabilities;
    use std::sync::{
        Arc,
        atomic::{AtomicUsize, Ordering},
    };

    #[derive(Debug)]
    struct DropProbe {
        drops: Arc<AtomicUsize>,
    }

    impl Drop for DropProbe {
        fn drop(&mut self) {
            self.drops.fetch_add(1, Ordering::SeqCst);
        }
    }

    fn required<T>(
        type_key: &'static str,
        capabilities: BoundaryCapabilities,
    ) -> BoundaryTypeContract
    where
        T: 'static,
    {
        BoundaryTypeContract::for_type::<T>(type_key, capabilities)
    }

    #[test]
    fn boundary_vtable_is_reused_per_type() {
        let first = BoundaryStorage::owned("test:u32", 1u32, BoundaryCapabilities::owned());
        let second = BoundaryStorage::owned("test:u32", 2u32, BoundaryCapabilities::owned());

        assert!(std::ptr::eq(first.vtable, second.vtable));
    }

    #[test]
    fn owned_take_prevents_double_take_and_double_drop() {
        let drops = Arc::new(AtomicUsize::new(0));
        let mut storage = BoundaryStorage::owned(
            "test:probe",
            DropProbe {
                drops: Arc::clone(&drops),
            },
            BoundaryCapabilities::owned(),
        );
        let contract = required::<DropProbe>("test:probe", BoundaryCapabilities::owned());

        let probe = storage
            .try_take_owned::<DropProbe>(&contract)
            .expect("take probe");
        assert!(storage.is_taken());
        assert_eq!(
            storage.try_take_owned::<DropProbe>(&contract).unwrap_err(),
            BoundaryTakeError::AlreadyTaken
        );
        assert_eq!(drops.load(Ordering::SeqCst), 0);

        drop(probe);
        assert_eq!(drops.load(Ordering::SeqCst), 1);
        drop(storage);
        assert_eq!(drops.load(Ordering::SeqCst), 1);
    }

    #[test]
    fn drop_releases_owned_value_without_take() {
        let drops = Arc::new(AtomicUsize::new(0));
        let storage = BoundaryStorage::owned(
            "test:probe",
            DropProbe {
                drops: Arc::clone(&drops),
            },
            BoundaryCapabilities::owned(),
        );

        drop(storage);

        assert_eq!(drops.load(Ordering::SeqCst), 1);
    }

    #[test]
    fn borrow_ref_and_mut_respect_capabilities_and_taken_state() {
        let mut storage =
            BoundaryStorage::owned("test:u32", 7u32, BoundaryCapabilities::rust_value());
        let read = required::<u32>(
            "test:u32",
            BoundaryCapabilities {
                borrow_ref: true,
                ..BoundaryCapabilities::default()
            },
        );
        let write = required::<u32>(
            "test:u32",
            BoundaryCapabilities {
                borrow_mut: true,
                ..BoundaryCapabilities::default()
            },
        );

        assert_eq!(
            *storage.try_borrow_ref::<u32>(&read).expect("borrow ref"),
            7
        );
        *storage.try_borrow_mut::<u32>(&write).expect("borrow mut") = 9;
        assert_eq!(
            *storage.try_borrow_ref::<u32>(&read).expect("borrow ref"),
            9
        );

        let owned = required::<u32>("test:u32", BoundaryCapabilities::owned());
        assert_eq!(storage.try_take_owned::<u32>(&owned).expect("take"), 9);
        assert_eq!(
            storage.try_borrow_ref::<u32>(&read).unwrap_err(),
            BoundaryTakeError::AlreadyTaken
        );
        assert_eq!(
            storage.try_borrow_mut::<u32>(&write).unwrap_err(),
            BoundaryTakeError::AlreadyTaken
        );
    }

    #[test]
    fn send_sync_storage_can_move_across_thread() {
        let storage = BoundaryStorage::owned("test:u32", 11u32, BoundaryCapabilities::owned());
        let handle = std::thread::spawn(move || {
            let mut storage = storage;
            let contract = required::<u32>("test:u32", BoundaryCapabilities::owned());
            storage.try_take_owned::<u32>(&contract).expect("take")
        });

        assert_eq!(handle.join().expect("thread join"), 11);
    }
}
