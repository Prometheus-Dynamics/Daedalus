//! Field helpers the service messages share: lists, optional text, and a typed value.

use crate::codec::{Reader, Writer};
use crate::{Schema, WireError, WireType, WireValue, decode_value, encode_value};

pub(crate) fn put_list<T>(
    out: &mut Writer,
    items: &[T],
    mut each: impl FnMut(&mut Writer, &T) -> Result<(), WireError>,
) -> Result<(), WireError> {
    out.len(items.len())?;
    for item in items {
        each(out, item)?;
    }
    Ok(())
}

pub(crate) fn get_list<T>(
    input: &mut Reader<'_>,
    mut each: impl FnMut(&mut Reader<'_>) -> Result<T, WireError>,
) -> Result<Vec<T>, WireError> {
    // The count is checked against the bytes left, and every item takes at least one byte.
    let count = input.count()?;
    let mut items = Vec::with_capacity(count);
    for _ in 0..count {
        items.push(each(input)?);
    }
    Ok(items)
}

pub(crate) fn put_strs(out: &mut Writer, items: &[String]) -> Result<(), WireError> {
    put_list(out, items, |out, item| out.str(item))
}

pub(crate) fn get_strs(input: &mut Reader<'_>) -> Result<Vec<String>, WireError> {
    get_list(input, |input| input.str())
}

pub(crate) fn put_opt_str(out: &mut Writer, text: &Option<String>) -> Result<(), WireError> {
    match text {
        None => out.u8(0),
        Some(text) => {
            out.u8(1);
            out.str(text)?;
        }
    }
    Ok(())
}

pub(crate) fn get_opt_str(input: &mut Reader<'_>) -> Result<Option<String>, WireError> {
    Ok(match flag(input, "optional flag")? {
        false => None,
        true => Some(input.str()?),
    })
}

/// A zero or a one. Any other byte is a bad tag named `what`.
pub(crate) fn flag(input: &mut Reader<'_>, what: &'static str) -> Result<bool, WireError> {
    match input.u8()? {
        0 => Ok(false),
        1 => Ok(true),
        tag => Err(bad(what, u32::from(tag))),
    }
}

pub(crate) fn bad(what: &'static str, tag: u32) -> WireError {
    WireError::BadTag { what, tag }
}

/// A value with its type. The service has no port declarations to type a value by, so each value
/// carries its own type tree, then the G1 encoding of the value as that type.
#[derive(Clone, Debug, PartialEq)]
pub struct TypedValue {
    pub ty: WireType,
    pub value: WireValue,
}

impl TypedValue {
    pub fn new(ty: WireType, value: WireValue) -> TypedValue {
        TypedValue { ty, value }
    }

    /// Refuses a value that does not have the shape of its type, and an opaque type whose key has
    /// no codec in `codecs`.
    pub fn encode(&self, out: &mut Writer, codecs: &Schema) -> Result<(), WireError> {
        self.ty.encode(out)?;
        encode_value(&self.ty, &self.value, codecs, out)
    }

    pub fn decode(input: &mut Reader<'_>, codecs: &Schema) -> Result<TypedValue, WireError> {
        let ty = WireType::decode(input, 0)?;
        let value = decode_value(&ty, input, codecs)?;
        Ok(TypedValue { ty, value })
    }
}
