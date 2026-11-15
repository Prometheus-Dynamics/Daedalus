//! A `&[T]` parameter: a list read as a slice of its items, viewed where its bytes are when the
//! list is in `cpu.wire` and its items are numbers.

use core::marker::PhantomData;

#[cfg(feature = "alloc")]
use alloc::vec::Vec;
#[cfg(feature = "alloc")]
use daedalus_abi::{LeItem, le_items};
#[cfg(feature = "alloc")]
use daedalus_types::WireForm;

/// A list whose items a `&[T]` parameter borrows.
pub trait Listed {
    type Item;
    fn items(&self) -> &[Self::Item];
}

#[cfg(feature = "alloc")]
impl<T> Listed for Vec<T> {
    type Item = T;

    #[inline]
    fn items(&self) -> &[T] {
        self
    }
}

/// How a list's wire form is viewed as its items, with no copy. `None` when it can't be.
pub type ItemsView<I> = Option<fn(&[u8]) -> Option<&[I]>>;

/// Whether a list's wire form can be viewed as its items.
pub struct ViewOf<L>(PhantomData<L>);

#[cfg(feature = "alloc")]
impl<T: LeItem + WireForm> ViewOf<Vec<T>> {
    pub const VIEW: ItemsView<T> = Some(view::<T>);
}

/// The answer for a list whose items aren't plain numbers.
pub trait NotView<I> {
    const VIEW: ItemsView<I> = None;
}

#[cfg(feature = "alloc")]
impl<T> NotView<T> for ViewOf<Vec<T>> {}

/// The items of a list's wire form where they are: a count and the items, or the bytes alone for a
/// list of bytes. `None` when they aren't aligned for `T` on this machine, or aren't a whole list.
#[cfg(feature = "alloc")]
fn view<T: LeItem + WireForm>(cell: &[u8]) -> Option<&[T]> {
    if T::BYTE {
        return le_items(cell);
    }
    let (count, items) = cell.split_first_chunk::<4>()?;
    let items = le_items::<T>(items)?;
    (u32::from_le_bytes(*count) as usize == items.len()).then_some(items)
}

#[cfg(all(test, feature = "alloc"))]
mod tests {
    use super::*;

    #[test]
    fn a_list_in_its_wire_form_is_viewed_where_it_is() {
        let numbers = alloc::vec![1.5f32, -2.0, 8.25];
        let mut buf = [0u8; 64];
        // Where the items, four bytes after the count, fall aligned for f32.
        let at = (buf[4..].as_ptr().align_offset(4)) % 4;
        let bytes = &mut buf[at..at + 16];
        assert_eq!(numbers.write_cell(bytes), 16);
        let read = ViewOf::<Vec<f32>>::VIEW.expect("numbers can be viewed");
        if cfg!(target_endian = "little") {
            assert_eq!(read(bytes), Some(&numbers[..]));
        }
        assert_eq!(read(&bytes[..15]), None, "not a whole list");
        assert_eq!(read(&bytes[1..]), None, "not aligned");
    }

    #[test]
    fn a_list_of_records_is_not_viewed() {
        #[allow(unused_imports)]
        use super::NotView as _;
        assert!(ViewOf::<Vec<(u8, u32)>>::VIEW.is_none());
    }
}
