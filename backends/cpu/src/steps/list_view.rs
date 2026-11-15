//! A `ListView<'_, T>` parameter: a list read item by item where its bytes are, with no decode of
//! the whole list and no allocation.
//!
//! A list in `cpu.wire` holds its wire form: a `u32` count, then the items. When `T`'s wire form
//! has a fixed size (`WireForm::FLAT`), the view reads item `i` from its bytes when asked, so a
//! list of records a worker wrote is read where it is. A `&[T]` of records would need their memory
//! to be their wire form and every bit pattern to be a valid value, which only `unsafe` code could
//! promise for a type a node author declares; the view needs neither. Anywhere else (a list in
//! `cpu.host`, or items of no fixed size, which are decoded first as the plan lists) the view
//! borrows the list itself.

use alloc::vec::Vec;
use core::marker::PhantomData;

use daedalus_types::WireForm;

/// Whether a `ListView<'_, T>` reads a list's wire form where it is, with no decode: when `T`'s
/// items have a fixed size.
pub const fn list_viewed<T: WireForm>() -> bool {
    T::FLAT && !T::BYTE
}

/// A list of `T`, read where it is: see the module.
pub struct ListView<'a, T> {
    inner: Inner<'a, T>,
}

enum Inner<'a, T> {
    /// The list itself.
    Items(&'a [T]),
    /// The items' wire forms, `size` bytes each, after the count.
    Wire {
        bytes: &'a [u8],
        size: usize,
        _of: PhantomData<fn() -> T>,
    },
}

// A view is a borrow, so it copies whatever its items are.
impl<T> Clone for ListView<'_, T> {
    fn clone(&self) -> Self {
        *self
    }
}

impl<T> Copy for ListView<'_, T> {}

impl<T> Clone for Inner<'_, T> {
    fn clone(&self) -> Self {
        *self
    }
}

impl<T> Copy for Inner<'_, T> {}

impl<'a, T: WireForm + Clone> ListView<'a, T> {
    /// A view of the list itself.
    #[doc(hidden)]
    pub fn of(items: &'a [T]) -> ListView<'a, T> {
        ListView {
            inner: Inner::Items(items),
        }
    }

    /// A view of a list's wire form, when `T`'s items have a fixed size and the bytes are a whole
    /// list of them. `None` otherwise: then the list is decoded.
    #[doc(hidden)]
    pub fn wire(cell: &'a [u8]) -> Option<ListView<'a, T>> {
        if !list_viewed::<T>() {
            return None;
        }
        let size = core::mem::size_of::<T>();
        let (count, bytes) = cell.split_first_chunk::<4>()?;
        let count = u32::from_le_bytes(*count) as usize;
        if size == 0 || count.checked_mul(size)? != bytes.len() {
            return None;
        }
        Some(ListView {
            inner: Inner::Wire {
                bytes,
                size,
                _of: PhantomData,
            },
        })
    }

    /// How many items the list holds.
    pub fn len(&self) -> usize {
        match self.inner {
            Inner::Items(items) => items.len(),
            Inner::Wire { bytes, size, .. } => bytes.len() / size,
        }
    }

    pub fn is_empty(&self) -> bool {
        self.len() == 0
    }

    /// Item `index`, or `None` past the end.
    #[inline]
    pub fn get(&self, index: usize) -> Option<T> {
        match self.inner {
            Inner::Items(items) => items.get(index).cloned(),
            Inner::Wire { bytes, size, .. } => {
                let at = index.checked_mul(size)?;
                let item = bytes.get(at..at.checked_add(size)?)?;
                T::read_wire(item).map(|(item, _)| item)
            }
        }
    }

    /// Each item in order.
    #[inline]
    pub fn iter(&self) -> impl Iterator<Item = T> + 'a {
        let inner = self.inner;
        let (items, wire) = match inner {
            Inner::Items(items) => (Some(items.iter().cloned()), None),
            Inner::Wire { bytes, size, .. } => (
                None,
                Some(
                    bytes
                        .chunks_exact(size)
                        .filter_map(|item| T::read_wire(item).map(|(item, _)| item)),
                ),
            ),
        };
        items
            .into_iter()
            .flatten()
            .chain(wire.into_iter().flatten())
    }

    /// The items as a list of their own, for a node that needs one.
    pub fn to_vec(&self) -> Vec<T> {
        self.iter().collect()
    }

    /// Whether the items are read from the list's wire form where it is.
    pub fn in_place(&self) -> bool {
        matches!(self.inner, Inner::Wire { .. })
    }
}
