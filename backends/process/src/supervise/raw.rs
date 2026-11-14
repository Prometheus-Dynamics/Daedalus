//! A value as the worker's packet carries it, from and to the bytes an input or output cell holds.
//! A fixed-size scalar is its bytes, a string, byte field or opaque value has its length before
//! them, and a compound value's cell holds its wire form already.

use daedalus_wire::codec::{Reader, Writer};
use daedalus_wire::layout::fixed_size;
use daedalus_wire::{Schema, WireError, WireType};

/// Writes one input's bytes as its port type's wire form. Scalars are their own bytes, strings and
/// byte fields are length-prefixed, and a compound value is already its wire form.
pub(super) fn put_raw(ty: &WireType, bytes: &[u8], out: &mut Writer) -> Result<(), String> {
    match ty {
        WireType::Scalar(scalar) => match fixed_size(*scalar) {
            Some(size) if bytes.len() == size => {
                out.buf.extend_from_slice(bytes);
                Ok(())
            }
            Some(size) => Err(format!(
                "a {scalar:?} input is {} bytes, not {size}",
                bytes.len()
            )),
            None => out.bytes(bytes).map_err(|error| error.to_string()),
        },
        WireType::Opaque(_) => out.bytes(bytes).map_err(|error| error.to_string()),
        _ => {
            out.buf.extend_from_slice(bytes);
            Ok(())
        }
    }
}

/// Reads one output's bytes from the wire into `out`, in the slot form `put_raw` writes back.
pub(super) fn read_raw(
    ty: &WireType,
    input: &mut Reader<'_>,
    schema: &Schema,
    out: &mut Vec<u8>,
) -> Result<(), WireError> {
    match ty {
        WireType::Scalar(scalar) => match fixed_size(*scalar) {
            Some(size) => out.extend_from_slice(input.take(size)?),
            None => {
                let len = input.count()?;
                out.extend_from_slice(input.take(len)?);
            }
        },
        WireType::Opaque(_) => {
            let len = input.count()?;
            out.extend_from_slice(input.take(len)?);
        }
        _ => {
            let from = input.pos();
            daedalus_wire::decode_bare(ty, input, schema)?;
            out.extend_from_slice(input.span(from));
        }
    }
    Ok(())
}
