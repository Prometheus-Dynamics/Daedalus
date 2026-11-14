//! How a value of a type lies in its bytes, from the type's shape alone.
//!
//! A value a place names (see [`crate::shared`]) can be read where it is, with no decode, when
//! its layout allows it. Every number on the wire is little-endian, and a list is its `u32`
//! count followed by its items, so:
//!
//! - **Fixed:** a fixed-size scalar, or a tuple or struct of fixed types. It is always `size`
//!   bytes, its fields packed in order.
//! - **Array:** a list of a fixed type, or a struct or tuple of fixed fields that ends in one.
//!   That is a header of `offset` bytes (the fixed fields, then the count), then `stride`-byte
//!   items, packed. A reader views the items as an array.
//! - **Walkable:** fixed fields, strings, byte fields and lists of fixed items, in a struct or
//!   tuple, or a list of walkable items. A reader finds each part by stepping over counts, and
//!   views each array where it is.
//! - **Decoded:** anything else (a map, an option, an enum with payloads, an opaque value). It is
//!   decoded.
//!
//! A type's layout is worked out from its shape, never from its name.

use daedalus_types::Scalar;

use crate::WireType;

/// How a value of a type lies in its bytes.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum Layout {
    /// Always `size` bytes, packed.
    Fixed { size: usize },
    /// A header of `offset` bytes, the last four of which are the item count, then the items,
    /// `stride` bytes each.
    Array { offset: usize, stride: usize },
    /// Read in place by stepping over counts.
    Walkable,
    /// Decoded to be read.
    Decoded,
}

impl Layout {
    /// Where the items start in a value of this layout, for an array: so a host can place the
    /// value with its items on an aligned address.
    pub fn items_at(self) -> Option<usize> {
        match self {
            Layout::Array { offset, .. } => Some(offset),
            _ => None,
        }
    }
}

/// The byte width of a scalar with a fixed width. Strings and byte fields have none.
pub fn fixed_size(scalar: Scalar) -> Option<usize> {
    Some(match scalar {
        Scalar::Unit => 0,
        Scalar::Bool | Scalar::I8 | Scalar::U8 => 1,
        Scalar::I16 | Scalar::U16 => 2,
        Scalar::I32 | Scalar::U32 | Scalar::F32 => 4,
        Scalar::Int | Scalar::ISize | Scalar::U64 | Scalar::USize | Scalar::Float => 8,
        Scalar::String | Scalar::Bytes => return None,
    })
}

impl WireType {
    /// How a value of this type lies in its bytes.
    pub fn layout(&self) -> Layout {
        match self {
            WireType::Scalar(scalar) => match fixed_size(*scalar) {
                Some(size) => Layout::Fixed { size },
                None => Layout::Walkable,
            },
            WireType::List(inner) => match inner.layout() {
                Layout::Fixed { size } => Layout::Array {
                    offset: 4,
                    stride: size,
                },
                Layout::Decoded => Layout::Decoded,
                _ => Layout::Walkable,
            },
            WireType::Tuple(items) => fields(items.iter()),
            WireType::Struct(items) => fields(items.iter().map(|(_, ty)| ty)),
            WireType::Optional(_) | WireType::Map(..) | WireType::Enum(_) | WireType::Opaque(_) => {
                Layout::Decoded
            }
        }
    }
}

/// The layout of fields packed in order.
fn fields<'a>(types: impl Iterator<Item = &'a WireType>) -> Layout {
    let mut size = 0usize;
    let mut array = None;
    let mut walkable = false;
    for ty in types {
        let layout = ty.layout();
        if array.is_some() {
            // Anything after an array is found by stepping over its items.
            walkable = true;
        }
        match layout {
            Layout::Decoded => return Layout::Decoded,
            Layout::Fixed { size: field } => size += field,
            Layout::Array { offset, stride } if !walkable && array.is_none() => {
                array = Some((size + offset, stride));
            }
            _ => walkable = true,
        }
    }
    match (walkable, array) {
        (true, _) => Layout::Walkable,
        (false, Some((offset, stride))) => Layout::Array { offset, stride },
        (false, None) => Layout::Fixed { size },
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn s(scalar: Scalar) -> WireType {
        WireType::Scalar(scalar)
    }

    fn list(ty: WireType) -> WireType {
        WireType::List(Box::new(ty))
    }

    fn record(fields: &[WireType]) -> WireType {
        WireType::Struct(
            fields
                .iter()
                .enumerate()
                .map(|(at, ty)| (format!("f{at}"), ty.clone()))
                .collect(),
        )
    }

    #[test]
    fn a_list_of_records_of_numbers_is_an_array() {
        // A keypoint: five f32 and two i32.
        let point = record(&[
            s(Scalar::F32),
            s(Scalar::F32),
            s(Scalar::F32),
            s(Scalar::F32),
            s(Scalar::F32),
            s(Scalar::I32),
            s(Scalar::I32),
        ]);
        assert_eq!(point.layout(), Layout::Fixed { size: 28 });
        assert_eq!(
            list(point).layout(),
            Layout::Array {
                offset: 4,
                stride: 28
            }
        );
    }

    #[test]
    fn a_header_then_one_list_is_an_array_after_the_header() {
        // A grid of labels: width, height, then the labels.
        let grid = record(&[s(Scalar::U32), s(Scalar::U32), list(s(Scalar::U32))]);
        assert_eq!(
            grid.layout(),
            Layout::Array {
                offset: 12,
                stride: 4
            }
        );
        assert_eq!(grid.layout().items_at(), Some(12));
    }

    #[test]
    fn several_lists_or_strings_are_walked_and_a_map_is_decoded() {
        let tensor = record(&[s(Scalar::U8), list(s(Scalar::U64)), s(Scalar::Bytes)]);
        assert_eq!(tensor.layout(), Layout::Walkable);
        assert_eq!(list(list(s(Scalar::I32))).layout(), Layout::Walkable);
        assert_eq!(
            record(&[list(s(Scalar::U8)), s(Scalar::U32)]).layout(),
            Layout::Walkable
        );
        let map = WireType::Map(Box::new(s(Scalar::String)), Box::new(s(Scalar::Int)));
        assert_eq!(map.layout(), Layout::Decoded);
        assert_eq!(list(map).layout(), Layout::Decoded);
        assert_eq!(WireType::Opaque("t:x".into()).layout(), Layout::Decoded);
    }
}
