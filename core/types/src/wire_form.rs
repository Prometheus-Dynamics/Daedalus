//! A value's wire form, written by its type: what a value is as bytes when it crosses a process.
//!
//! The wire form is little-endian and packed. A struct or tuple is its fields in order, a list or
//! map a `u32` count and then its items, an optional value a `0` or a `1` and then the value, an
//! enum its variant's `u32` index and then the payload, and a string or bytes nested in another
//! value a `u32` length and then the bytes. A cell holding a whole string or bytes value holds the
//! bytes alone ([`WireForm::write_cell`]).
//!
//! A type writes and reads its own form: the backends that move a value never look inside it.
//! `#[derive(Type)]` gives a struct or enum this trait, field by field in the order its type
//! expression lists them.

#[cfg(feature = "alloc")]
use alloc::{collections::BTreeMap, string::String, sync::Arc, vec::Vec};

/// A type that writes and reads its value's wire form.
pub trait WireForm: Sized {
    /// The value's memory is its wire form: a little-endian machine, its fields in order, and no
    /// padding.
    const FLAT: bool = false;
    /// The type is `u8`: a list of it is bytes.
    #[doc(hidden)]
    const BYTE: bool = false;

    /// How many bytes the value's wire form takes inside another value.
    fn wire_len(&self) -> usize;
    /// Writes the value's wire form at the front of `out`, which holds at least
    /// [`WireForm::wire_len`] bytes, and gives how many it wrote.
    fn write_wire(&self, out: &mut [u8]) -> usize;
    /// Reads a value from the front of `bytes`, and gives it and how many bytes it took.
    fn read_wire(bytes: &[u8]) -> Option<(Self, usize)>;

    /// How many bytes a cell holding the value holds: its wire form, or the bytes alone for a
    /// string or bytes value.
    fn cell_len(&self) -> usize {
        self.wire_len()
    }
    /// Writes what a cell holding the value holds.
    fn write_cell(&self, out: &mut [u8]) -> usize {
        self.write_wire(out)
    }
    /// Reads a value from all of a cell's bytes. Bytes left over are refused.
    fn read_cell(bytes: &[u8]) -> Option<Self> {
        match Self::read_wire(bytes)? {
            (value, used) if used == bytes.len() => Some(value),
            _ => None,
        }
    }
}

fn take<const N: usize>(bytes: &[u8]) -> Option<[u8; N]> {
    bytes.get(..N)?.try_into().ok()
}

fn put(out: &mut [u8], bytes: &[u8]) -> usize {
    out[..bytes.len()].copy_from_slice(bytes);
    bytes.len()
}

const LITTLE: bool = cfg!(target_endian = "little");

macro_rules! numbers {
    ($($ty:ty),*) => {$(
        impl WireForm for $ty {
            const FLAT: bool = LITTLE || core::mem::size_of::<$ty>() == 1;
            fn wire_len(&self) -> usize {
                core::mem::size_of::<$ty>()
            }
            fn write_wire(&self, out: &mut [u8]) -> usize {
                put(out, &self.to_le_bytes())
            }
            fn read_wire(bytes: &[u8]) -> Option<($ty, usize)> {
                Some((<$ty>::from_le_bytes(take(bytes)?), core::mem::size_of::<$ty>()))
            }
        }
    )*};
}

numbers!(i8, i16, i32, i64, u16, u32, u64, f32, f64);

impl WireForm for u8 {
    const FLAT: bool = true;
    const BYTE: bool = true;
    fn wire_len(&self) -> usize {
        1
    }
    fn write_wire(&self, out: &mut [u8]) -> usize {
        put(out, &[*self])
    }
    fn read_wire(bytes: &[u8]) -> Option<(u8, usize)> {
        Some((*bytes.first()?, 1))
    }
}

/// `isize` and `usize` travel as eight bytes, whatever the machine's word.
macro_rules! words {
    ($($ty:ty => $wide:ty),*) => {$(
        impl WireForm for $ty {
            const FLAT: bool = LITTLE && core::mem::size_of::<$ty>() == 8;
            fn wire_len(&self) -> usize {
                8
            }
            fn write_wire(&self, out: &mut [u8]) -> usize {
                put(out, &(*self as $wide).to_le_bytes())
            }
            fn read_wire(bytes: &[u8]) -> Option<($ty, usize)> {
                let wide = <$wide>::from_le_bytes(take(bytes)?);
                Some((<$ty>::try_from(wide).ok()?, 8))
            }
        }
    )*};
}

words!(isize => i64, usize => u64);

impl WireForm for bool {
    const FLAT: bool = true;
    fn wire_len(&self) -> usize {
        1
    }
    fn write_wire(&self, out: &mut [u8]) -> usize {
        put(out, &[u8::from(*self)])
    }
    fn read_wire(bytes: &[u8]) -> Option<(bool, usize)> {
        match bytes.first()? {
            0 => Some((false, 1)),
            1 => Some((true, 1)),
            _ => None,
        }
    }
}

impl WireForm for () {
    const FLAT: bool = true;
    fn wire_len(&self) -> usize {
        0
    }
    fn write_wire(&self, _: &mut [u8]) -> usize {
        0
    }
    fn read_wire(_: &[u8]) -> Option<((), usize)> {
        Some(((), 0))
    }
}

/// A `u32` count or length, and the place after it.
#[cfg(feature = "alloc")]
fn count(bytes: &[u8]) -> Option<(usize, usize)> {
    Some((u32::from_le_bytes(take(bytes)?) as usize, 4))
}

#[cfg(feature = "alloc")]
fn put_count(out: &mut [u8], len: usize) -> usize {
    // A count past `u32` can't be written; the length check in `wire_len` callers keeps it out.
    put(out, &(len as u32).to_le_bytes())
}

#[cfg(feature = "alloc")]
impl WireForm for String {
    fn wire_len(&self) -> usize {
        4 + self.len()
    }
    fn write_wire(&self, out: &mut [u8]) -> usize {
        let at = put_count(out, self.len());
        at + put(&mut out[at..], self.as_bytes())
    }
    fn read_wire(bytes: &[u8]) -> Option<(String, usize)> {
        let (len, at) = count(bytes)?;
        let text = core::str::from_utf8(bytes.get(at..at.checked_add(len)?)?).ok()?;
        Some((String::from(text), at + len))
    }
    fn cell_len(&self) -> usize {
        self.len()
    }
    fn write_cell(&self, out: &mut [u8]) -> usize {
        put(out, self.as_bytes())
    }
    fn read_cell(bytes: &[u8]) -> Option<String> {
        core::str::from_utf8(bytes).ok().map(String::from)
    }
}

/// A list. A `Vec<u8>` is bytes: inside another value the same `u32` count and bytes, and in a cell
/// the bytes alone.
#[cfg(feature = "alloc")]
impl<T: WireForm> WireForm for Vec<T> {
    fn wire_len(&self) -> usize {
        4 + items_len(self)
    }
    fn write_wire(&self, out: &mut [u8]) -> usize {
        let at = put_count(out, self.len());
        at + write_items(self, &mut out[at..])
    }
    fn read_wire(bytes: &[u8]) -> Option<(Vec<T>, usize)> {
        let (len, at) = count(bytes)?;
        let (items, used) = read_items(&bytes[at..], Some(len))?;
        Some((items, at + used))
    }
    fn cell_len(&self) -> usize {
        if T::BYTE { self.len() } else { self.wire_len() }
    }
    fn write_cell(&self, out: &mut [u8]) -> usize {
        if T::BYTE {
            write_items(self, out)
        } else {
            self.write_wire(out)
        }
    }
    fn read_cell(bytes: &[u8]) -> Option<Vec<T>> {
        if T::BYTE {
            return read_items(bytes, None).map(|(items, _)| items);
        }
        match Self::read_wire(bytes)? {
            (value, used) if used == bytes.len() => Some(value),
            _ => None,
        }
    }
}

#[cfg(feature = "alloc")]
fn items_len<T: WireForm>(items: &[T]) -> usize {
    match items.first() {
        // A flat item has one size, so the list's is known without walking it.
        Some(first) if T::FLAT => first.wire_len() * items.len(),
        _ => items.iter().map(WireForm::wire_len).sum(),
    }
}

#[cfg(feature = "alloc")]
fn write_items<T: WireForm>(items: &[T], out: &mut [u8]) -> usize {
    let mut at = 0;
    for item in items {
        at += item.write_wire(&mut out[at..]);
    }
    at
}

/// `len` items from the front of `bytes`, or as many as fill all of it when `len` is `None`.
#[cfg(feature = "alloc")]
fn read_items<T: WireForm>(bytes: &[u8], len: Option<usize>) -> Option<(Vec<T>, usize)> {
    // Never reserve more than the bytes could hold, whatever a count claims.
    let mut items = Vec::with_capacity(len.unwrap_or(bytes.len()).min(bytes.len()));
    let mut at = 0;
    while len.map_or(at < bytes.len(), |len| items.len() < len) {
        let (item, used) = T::read_wire(&bytes[at..])?;
        if used == 0 && len.is_none() {
            return None;
        }
        items.push(item);
        at += used;
    }
    Some((items, at))
}

impl<T: WireForm> WireForm for Option<T> {
    fn wire_len(&self) -> usize {
        1 + self.as_ref().map_or(0, WireForm::wire_len)
    }
    fn write_wire(&self, out: &mut [u8]) -> usize {
        match self {
            None => put(out, &[0]),
            Some(value) => {
                1 + {
                    out[0] = 1;
                    value.write_wire(&mut out[1..])
                }
            }
        }
    }
    fn read_wire(bytes: &[u8]) -> Option<(Option<T>, usize)> {
        match bytes.first()? {
            0 => Some((None, 1)),
            1 => T::read_wire(&bytes[1..]).map(|(value, used)| (Some(value), 1 + used)),
            _ => None,
        }
    }
}

#[cfg(feature = "alloc")]
impl<K: WireForm + Ord, V: WireForm> WireForm for BTreeMap<K, V> {
    fn wire_len(&self) -> usize {
        4 + self
            .iter()
            .map(|(key, value)| key.wire_len() + value.wire_len())
            .sum::<usize>()
    }
    fn write_wire(&self, out: &mut [u8]) -> usize {
        let mut at = put_count(out, self.len());
        for (key, value) in self {
            at += key.write_wire(&mut out[at..]);
            at += value.write_wire(&mut out[at..]);
        }
        at
    }
    fn read_wire(bytes: &[u8]) -> Option<(BTreeMap<K, V>, usize)> {
        let (len, mut at) = count(bytes)?;
        let mut map = BTreeMap::new();
        for _ in 0..len {
            let (key, used) = K::read_wire(&bytes[at..])?;
            at += used;
            let (value, used) = V::read_wire(&bytes[at..])?;
            at += used;
            map.insert(key, value);
        }
        Some((map, at))
    }
}

/// A handle's value is the value's: what crosses is what it points to.
#[cfg(feature = "alloc")]
impl<T: WireForm> WireForm for Arc<T> {
    fn wire_len(&self) -> usize {
        (**self).wire_len()
    }
    fn write_wire(&self, out: &mut [u8]) -> usize {
        (**self).write_wire(out)
    }
    fn read_wire(bytes: &[u8]) -> Option<(Arc<T>, usize)> {
        T::read_wire(bytes).map(|(value, used)| (Arc::new(value), used))
    }
    fn cell_len(&self) -> usize {
        (**self).cell_len()
    }
    fn write_cell(&self, out: &mut [u8]) -> usize {
        (**self).write_cell(out)
    }
    fn read_cell(bytes: &[u8]) -> Option<Arc<T>> {
        T::read_cell(bytes).map(Arc::new)
    }
}

/// Whether a type of `fields` whose `n`th field is at `offsets[n]` is laid out as its wire form:
/// every field flat, each right after the last, and nothing after the last. For the derive.
#[doc(hidden)]
pub const fn flat_layout(size: usize, fields: &[(bool, usize, usize)]) -> bool {
    let mut at = 0;
    let mut n = 0;
    while n < fields.len() {
        let (flat, offset, len) = fields[n];
        if !flat || offset != at {
            return false;
        }
        at += len;
        n += 1;
    }
    at == size
}

macro_rules! tuples {
    ($(($($name:ident $index:tt),+))*) => {$(
        impl<$($name: WireForm),+> WireForm for ($($name,)+) {
            const FLAT: bool = flat_layout(
                core::mem::size_of::<Self>(),
                &[$(($name::FLAT, core::mem::offset_of!(Self, $index), core::mem::size_of::<$name>())),+],
            );
            fn wire_len(&self) -> usize {
                0 $(+ self.$index.wire_len())+
            }
            fn write_wire(&self, out: &mut [u8]) -> usize {
                let mut at = 0;
                $(at += self.$index.write_wire(&mut out[at..]);)+
                at
            }
            #[allow(non_snake_case)]
            fn read_wire(bytes: &[u8]) -> Option<(Self, usize)> {
                let mut at = 0;
                $(
                    let ($name, used) = $name::read_wire(&bytes[at..])?;
                    at += used;
                )+
                Some((($($name,)+), at))
            }
        }
    )*};
}

tuples!((A 0, B 1)(A 0, B 1, C 2)(A 0, B 1, C 2, D 3));
