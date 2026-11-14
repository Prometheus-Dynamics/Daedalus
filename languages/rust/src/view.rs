//! Inputs read where the host put them.
//!
//! A host that agreed values by reference of any type places a large list as its wire form: a
//! `u32` count, then its items, little-endian and packed, with the items on an aligned address.
//! So a list of numbers is, on a little-endian machine, already a slice of them. A node's `&[f32]`
//! parameter (or `&[u32]`, `&[i64]` and the other numbers) borrows it with no copy; a small list,
//! which comes in the packet, is decoded.
//!
//! A list of records reads the same way through [`View`]: a record type says how its fixed-size
//! bytes read ([`Flat`]), and the view reads each item from where it is.

use std::borrow::Cow;
use std::marker::PhantomData;

use daedalus_abi::{LeItem, le_items};
use daedalus_types::Scalar;
use daedalus_wire::{Layout, WireType, WireValue};

use crate::codec::{Inputs, Reading, Value, found, shape};
use crate::serve::NodeError;

/// A number a list input can be viewed as.
pub trait Number: LeItem {
    const SCALAR: Scalar;
    fn from_wire(value: &WireValue) -> Option<Self>;
}

macro_rules! numbers {
    ($($t:ty => $scalar:ident / $variant:ident),*) => {$(
        impl Number for $t {
            const SCALAR: Scalar = Scalar::$scalar;
            fn from_wire(value: &WireValue) -> Option<$t> {
                match value {
                    WireValue::$variant(v) => Some(*v as $t),
                    _ => None,
                }
            }
        }

        impl Reading<Vec<$t>> {
            /// The list, borrowed where the host put it when it can be, and decoded otherwise.
            pub fn hold<'a>(&self, inputs: &Inputs<'a>, name: &str) -> Result<Cow<'a, [$t]>, NodeError> {
                numbers::<$t>(inputs, name)
            }
        }
    )*};
}

numbers!(
    i16 => I16 / I16, u16 => U16 / U16, i32 => I32 / I32, u32 => U32 / U32,
    i64 => Int / I64, u64 => U64 / U64, f32 => F32 / F32, f64 => Float / F64
);

/// A list input of numbers: borrowed where the host put it, when it is placed, aligned, and this
/// machine is little-endian; decoded otherwise.
pub fn numbers<'a, T: Number>(inputs: &Inputs<'a>, name: &str) -> Result<Cow<'a, [T]>, NodeError> {
    match found(inputs, name)? {
        Value::Placed { ty, bytes, schema } => {
            if let WireType::List(item) = ty
                && **item == WireType::Scalar(T::SCALAR)
                && let Some(items) = bytes.get(4..).and_then(le_items::<T>)
            {
                return Ok(Cow::Borrowed(items));
            }
            let value = daedalus_wire::decode_placed(ty, bytes, schema).map_err(|_| shape(name))?;
            decode_list(&value)
                .map(Cow::Owned)
                .ok_or_else(|| shape(name))
        }
        Value::Wire(value) => decode_list(value)
            .map(Cow::Owned)
            .ok_or_else(|| shape(name)),
        Value::Bytes(_) => Err(shape(name)),
    }
}

fn decode_list<T: Number>(value: &WireValue) -> Option<Vec<T>> {
    match value {
        WireValue::List(items) => items.iter().map(T::from_wire).collect(),
        _ => None,
    }
}

/// A record of fixed size, read from its little-endian bytes as the wire packs it: its fields in
/// order, with no padding.
pub trait Flat: Sized {
    /// The record's size in bytes.
    const SIZE: usize;
    /// The record in `bytes`, which are exactly [`Flat::SIZE`] long.
    fn read(bytes: &[u8]) -> Self;
}

/// A list input whose items are [`Flat`] records, read where they are.
pub struct View<'a, T> {
    /// The header before the items (the fixed fields of a struct that ends in the list), then
    /// the items.
    bytes: Cow<'a, [u8]>,
    items_at: usize,
    len: usize,
    _of: PhantomData<T>,
}

impl<T: Flat> View<'_, T> {
    /// How many items there are.
    pub fn len(&self) -> usize {
        self.len
    }

    pub fn is_empty(&self) -> bool {
        self.len == 0
    }

    /// Item `at`, read from where it is.
    pub fn get(&self, at: usize) -> Option<T> {
        let start = self.items_at.checked_add(at.checked_mul(T::SIZE)?)?;
        let bytes = self.bytes.get(start..start.checked_add(T::SIZE)?)?;
        (at < self.len).then(|| T::read(bytes))
    }

    pub fn iter(&self) -> impl Iterator<Item = T> + '_ {
        (0..self.len).filter_map(|at| self.get(at))
    }

    /// The fixed fields before the list's count, for a struct that ends in the list.
    pub fn header(&self) -> &[u8] {
        &self.bytes[..self.items_at.saturating_sub(4)]
    }

    /// Whether the items are read where the host put them, not from a copy.
    pub fn in_place(&self) -> bool {
        matches!(self.bytes, Cow::Borrowed(_))
    }
}

/// A list input of [`Flat`] records, or a struct of fixed fields that ends in one: read where the
/// host put it when it is placed, and from the value's fixed fields when it came in the packet.
pub fn input_view<'a, T: Flat>(inputs: &Inputs<'a>, name: &str) -> Result<View<'a, T>, NodeError> {
    let (bytes, items_at) = match found(inputs, name)? {
        Value::Placed { ty, bytes, .. } => match ty.layout() {
            Layout::Array { offset, stride } if stride == T::SIZE => (Cow::Borrowed(bytes), offset),
            _ => return Err(shape(name)),
        },
        Value::Wire(value) => {
            let mut out = Vec::new();
            let items_at = flat(value, &mut out, None).ok_or_else(|| shape(name))?;
            (Cow::Owned(out), items_at.ok_or_else(|| shape(name))?)
        }
        Value::Bytes(_) => return Err(shape(name)),
    };
    let count = bytes
        .get(items_at.wrapping_sub(4)..items_at)
        .map(|raw| u32::from_le_bytes([raw[0], raw[1], raw[2], raw[3]]) as usize)
        .ok_or_else(|| shape(name))?;
    if Some(bytes.len())
        != count
            .checked_mul(T::SIZE)
            .and_then(|len| len.checked_add(items_at))
    {
        return Err(shape(name));
    }
    Ok(View {
        bytes,
        items_at,
        len: count,
        _of: PhantomData,
    })
}

/// Writes a value of fixed fields and at most one list as the wire packs it, and gives where the
/// list's items start. `None` when the value has a part of no fixed size, or a second list.
fn flat(value: &WireValue, out: &mut Vec<u8>, items_at: Option<usize>) -> Option<Option<usize>> {
    let mut items_at = items_at;
    match value {
        WireValue::Bool(v) => out.push(u8::from(*v)),
        WireValue::I8(v) => out.extend_from_slice(&v.to_le_bytes()),
        WireValue::U8(v) => out.push(*v),
        WireValue::I16(v) => out.extend_from_slice(&v.to_le_bytes()),
        WireValue::U16(v) => out.extend_from_slice(&v.to_le_bytes()),
        WireValue::I32(v) => out.extend_from_slice(&v.to_le_bytes()),
        WireValue::U32(v) => out.extend_from_slice(&v.to_le_bytes()),
        WireValue::I64(v) | WireValue::ISize(v) => out.extend_from_slice(&v.to_le_bytes()),
        WireValue::U64(v) | WireValue::USize(v) => out.extend_from_slice(&v.to_le_bytes()),
        WireValue::F32(v) => out.extend_from_slice(&v.to_le_bytes()),
        WireValue::F64(v) => out.extend_from_slice(&v.to_le_bytes()),
        WireValue::Struct(fields) | WireValue::Tuple(fields) => {
            for field in fields {
                items_at = flat(field, out, items_at)?;
            }
        }
        WireValue::List(items) if items_at.is_none() => {
            out.extend_from_slice(&u32::try_from(items.len()).ok()?.to_le_bytes());
            let at = out.len();
            for item in items {
                // An item is a fixed record: it has no list of its own.
                if flat(item, out, None)?.is_some() {
                    return None;
                }
            }
            items_at = Some(at);
        }
        _ => return None,
    }
    Some(items_at)
}

/// The items of a list of [`Flat`] records, read in one pass from where the host put them. The
/// `#[node]` parameter `&[T]` of a `Flat` type is read this way.
pub fn records<T: Flat>(inputs: &Inputs<'_>, name: &str) -> Result<Vec<T>, NodeError> {
    Ok(input_view::<T>(inputs, name)?.iter().collect())
}

impl<T: Flat> Reading<Vec<T>> {
    /// The records, read from where the host put them, or from the packet.
    pub fn hold(&self, inputs: &Inputs<'_>, name: &str) -> Result<Vec<T>, NodeError> {
        records(inputs, name)
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use daedalus_wire::codec::Writer;
    use daedalus_wire::{Schema, encode_bare};

    /// `items` as a list's wire form, in a buffer where the items, after the count, sit on an
    /// address aligned to `align`.
    fn placed(raw: &[[u8; 4]], align: usize) -> (Vec<u8>, usize) {
        let mut buf = vec![0; align + 4 + raw.len() * 4];
        let pad = (align - (buf.as_ptr() as usize + 4) % align) % align;
        buf[pad..pad + 4].copy_from_slice(&(raw.len() as u32).to_le_bytes());
        for (at, item) in raw.iter().enumerate() {
            buf[pad + 4 + at * 4..pad + 8 + at * 4].copy_from_slice(item);
        }
        (buf, pad)
    }

    fn list(scalar: Scalar) -> WireType {
        WireType::List(Box::new(WireType::Scalar(scalar)))
    }

    #[test]
    fn a_placed_list_of_numbers_is_borrowed_where_it_is() {
        let raw: Vec<[u8; 4]> = [1.5f32, -2.0, 8.25].map(f32::to_le_bytes).to_vec();
        let (buf, pad) = placed(&raw, 4);
        let bytes = &buf[pad..pad + 16];
        let (ty, schema) = (list(Scalar::F32), Schema::new());
        let inputs = [(
            "in",
            Value::Placed {
                ty: &ty,
                bytes,
                schema: &schema,
            },
        )];
        let held = numbers::<f32>(&inputs, "in").expect("a list of f32");
        assert!(matches!(held, Cow::Borrowed(_)), "borrowed, not copied");
        assert_eq!(held.as_ptr() as usize, bytes.as_ptr() as usize + 4);
        assert_eq!(&*held, &[1.5, -2.0, 8.25]);
        // The same bytes asked as another number are decoded, or refused, never misread.
        assert!(numbers::<u32>(&inputs, "in").is_err());
    }

    #[test]
    fn a_list_in_the_packet_or_misaligned_is_decoded() {
        let value = WireValue::List(vec![WireValue::U32(7), WireValue::U32(9)]);
        let inputs = [("in", Value::Wire(&value))];
        let held = numbers::<u32>(&inputs, "in").expect("a list of u32");
        assert!(matches!(held, Cow::Owned(_)));
        assert_eq!(&*held, &[7, 9]);

        let raw: Vec<[u8; 4]> = [3u32, 4].map(u32::to_le_bytes).to_vec();
        let (buf, pad) = placed(&raw, 8);
        // One byte off the aligned start: the items are no longer on a `u32` boundary.
        let mut shifted = vec![0u8; buf.len() + 1];
        let start = (0..4)
            .find(|at| !(shifted.as_ptr() as usize + at + 4).is_multiple_of(4))
            .expect("a misaligned start");
        shifted[start..start + 12].copy_from_slice(&buf[pad..pad + 12]);
        let (ty, schema) = (list(Scalar::U32), Schema::new());
        let inputs = [(
            "in",
            Value::Placed {
                ty: &ty,
                bytes: &shifted[start..start + 12],
                schema: &schema,
            },
        )];
        let held = numbers::<u32>(&inputs, "in").expect("decoded");
        assert!(matches!(held, Cow::Owned(_)));
        assert_eq!(&*held, &[3, 4]);
    }

    #[derive(Debug, PartialEq)]
    struct Point {
        x: f32,
        id: u32,
    }

    impl Flat for Point {
        const SIZE: usize = 8;
        fn read(bytes: &[u8]) -> Point {
            Point {
                x: f32::from_le_bytes([bytes[0], bytes[1], bytes[2], bytes[3]]),
                id: u32::from_le_bytes([bytes[4], bytes[5], bytes[6], bytes[7]]),
            }
        }
    }

    #[test]
    fn a_list_of_records_is_read_in_place_or_from_the_packet() {
        let point = WireType::Struct(vec![
            ("x".into(), WireType::Scalar(Scalar::F32)),
            ("id".into(), WireType::Scalar(Scalar::U32)),
        ]);
        let ty = WireType::List(Box::new(point));
        let schema = Schema::new();
        let value = WireValue::List(vec![
            WireValue::Struct(vec![WireValue::F32(0.5), WireValue::U32(1)]),
            WireValue::Struct(vec![WireValue::F32(2.5), WireValue::U32(2)]),
        ]);
        let inputs = [("in", Value::Wire(&value))];
        let view = input_view::<Point>(&inputs, "in").expect("a view");
        assert!(!view.in_place());
        let points: Vec<Point> = view.iter().collect();
        assert_eq!(points, [Point { x: 0.5, id: 1 }, Point { x: 2.5, id: 2 }]);

        let mut bare = Writer::new();
        encode_bare(&ty, &value, &schema, &mut bare).expect("encodes");
        let inputs = [(
            "in",
            Value::Placed {
                ty: &ty,
                bytes: &bare.buf,
                schema: &schema,
            },
        )];
        let view = input_view::<Point>(&inputs, "in").expect("a view");
        assert!(view.in_place());
        assert_eq!(view.len(), 2);
        assert_eq!(view.get(1), Some(Point { x: 2.5, id: 2 }));
        assert_eq!(view.get(2), None);
    }
}
