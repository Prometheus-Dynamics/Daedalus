//! Converts between the C layouts and the wire's typed values. A C type is read once, when the
//! worker starts, into a `WireType`. A C value is read through its type into a `WireValue`, and a
//! `WireValue` is written into a run's arena as a C value, whose pointers the arena keeps valid
//! until the run ends.

use std::ffi::{CStr, c_char};
use std::slice;

use daedalus_types::Scalar;
use daedalus_wire::{MAX_DEPTH, WireType, WireValue};

use crate::arena::RunArena;
use crate::ffi::{
    DdBytes, DdEnum, DdPort, DdSeq, DdType, DdValue, KIND_ENUM, KIND_LIST, KIND_MAP, KIND_OPAQUE,
    KIND_OPTIONAL, KIND_SCALAR, KIND_STRUCT, KIND_TUPLE,
};

/// A C value or type the wire cannot carry as its type says: a pointer that is missing, a number
/// out of its type's range, text that is not UTF-8, or a count that does not fit.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub struct Refused;

/// The scalars in the order `dd_scalar` numbers them, which is the wire's tag order.
const SCALARS: [Scalar; 16] = [
    Scalar::Unit,
    Scalar::Bool,
    Scalar::I8,
    Scalar::I16,
    Scalar::I32,
    Scalar::Int,
    Scalar::ISize,
    Scalar::U8,
    Scalar::U16,
    Scalar::U32,
    Scalar::U64,
    Scalar::USize,
    Scalar::F32,
    Scalar::Float,
    Scalar::String,
    Scalar::Bytes,
];

/// The text of a C string. A null pointer is the empty text.
///
/// # Safety
/// `text` is null, or a NUL-terminated string that stays valid for the call.
pub unsafe fn c_text(text: *const c_char) -> String {
    if text.is_null() {
        return String::new();
    }
    // SAFETY: the pointer is not null, and the caller's contract makes it a NUL-terminated string.
    unsafe { CStr::from_ptr(text) }
        .to_string_lossy()
        .into_owned()
}

/// `count` type pointers from a C array, which is empty when `count` is 0.
///
/// # Safety
/// `items` is null when `count` is 0, and otherwise points to `count` type pointers.
unsafe fn child_types(
    items: *const *const DdType,
    count: usize,
) -> Result<Vec<*const DdType>, Refused> {
    if count == 0 {
        return Ok(Vec::new());
    }
    if items.is_null() {
        return Err(Refused);
    }
    // SAFETY: `items` is not null and points to `count` pointers, as the caller's contract says.
    Ok(unsafe { slice::from_raw_parts(items, count) }.to_vec())
}

/// `count` names from a C array of strings.
///
/// # Safety
/// `names` is null when `count` is 0, and otherwise points to `count` NUL-terminated strings.
unsafe fn child_names(names: *const *const c_char, count: usize) -> Result<Vec<String>, Refused> {
    if count == 0 {
        return Ok(Vec::new());
    }
    if names.is_null() {
        return Err(Refused);
    }
    // SAFETY: `names` is not null and points to `count` pointers, as the caller's contract says.
    let pointers = unsafe { slice::from_raw_parts(names, count) };
    let mut out = Vec::with_capacity(count);
    for &pointer in pointers {
        // SAFETY: each entry is a NUL-terminated string, as the caller's contract says.
        out.push(unsafe { c_text(pointer) });
    }
    Ok(out)
}

/// The wire type a C type names. A type nested past the wire's depth limit is refused.
///
/// # Safety
/// `ty` is null, or points to a type, and the tables it names, that stay valid for the call.
pub unsafe fn wire_type(ty: *const DdType, depth: u32) -> Result<WireType, Refused> {
    if depth > MAX_DEPTH {
        return Err(Refused);
    }
    // SAFETY: the caller's contract makes a non-null `ty` point to a live type.
    let Some(ty) = (unsafe { ty.as_ref() }) else {
        return Err(Refused);
    };
    let deeper = depth + 1;
    Ok(match ty.kind {
        KIND_SCALAR => {
            let index = usize::try_from(ty.scalar).map_err(|_| Refused)?;
            WireType::Scalar(*SCALARS.get(index).ok_or(Refused)?)
        }
        KIND_OPAQUE => {
            if ty.key.is_null() {
                return Err(Refused);
            }
            // SAFETY: the key is not null, and a type's key is a NUL-terminated string.
            WireType::Opaque(unsafe { c_text(ty.key) })
        }
        KIND_OPTIONAL | KIND_LIST => {
            // SAFETY: the kind names one item, so the table holds at least one pointer.
            let items = unsafe { child_types(ty.items, 1)? };
            let item = nested(items[0], deeper)?;
            if ty.kind == KIND_OPTIONAL {
                WireType::Optional(Box::new(item))
            } else {
                WireType::List(Box::new(item))
            }
        }
        KIND_MAP => {
            // SAFETY: the kind names two items, so the table holds two pointers.
            let items = unsafe { child_types(ty.items, 2)? };
            WireType::Map(
                Box::new(nested(items[0], deeper)?),
                Box::new(nested(items[1], deeper)?),
            )
        }
        KIND_TUPLE => {
            // SAFETY: the table holds `count` items, as the type's own count says.
            let items = unsafe { child_types(ty.items, ty.count)? };
            let mut out = Vec::with_capacity(items.len());
            for item in items {
                out.push(nested(item, deeper)?);
            }
            WireType::Tuple(out)
        }
        KIND_STRUCT => {
            // SAFETY: the table holds `count` field types and `count` field names.
            let items = unsafe { child_types(ty.items, ty.count)? };
            let names = unsafe { child_names(ty.names, ty.count)? };
            let mut fields = Vec::with_capacity(items.len());
            for (name, item) in names.into_iter().zip(items) {
                fields.push((name, nested(item, deeper)?));
            }
            WireType::Struct(fields)
        }
        KIND_ENUM => {
            // SAFETY: the table holds `count` payload types and `count` variant names.
            let items = unsafe { child_types(ty.items, ty.count)? };
            let names = unsafe { child_names(ty.names, ty.count)? };
            let mut variants = Vec::with_capacity(items.len());
            for (name, item) in names.into_iter().zip(items) {
                let payload = if item.is_null() {
                    None
                } else {
                    Some(nested(item, deeper)?)
                };
                variants.push((name, payload));
            }
            WireType::Enum(variants)
        }
        _ => return Err(Refused),
    })
}

/// The wire type of one nested type pointer. Its only callers are `wire_type`'s own items, whose
/// tables the caller of `wire_type` keeps valid for the whole walk.
fn nested(ty: *const DdType, depth: u32) -> Result<WireType, Refused> {
    // SAFETY: `ty` is an item of a table that `wire_type`'s caller keeps valid, as its contract says.
    unsafe { wire_type(ty, depth) }
}

/// The wire types of a node's ports, by name.
///
/// # Safety
/// `ports` is null when `count` is 0, and otherwise points to `count` ports whose types and names
/// stay valid for the call.
pub unsafe fn ports(
    ports: *const DdPort,
    count: usize,
) -> Result<Vec<(String, WireType)>, Refused> {
    if count == 0 {
        return Ok(Vec::new());
    }
    if ports.is_null() {
        return Err(Refused);
    }
    // SAFETY: `ports` is not null and points to `count` ports, as the caller's contract says.
    let entries = unsafe { slice::from_raw_parts(ports, count) };
    let mut out = Vec::with_capacity(count);
    for port in entries {
        // SAFETY: a port's name and type are valid for the call, as the caller's contract says.
        let name = unsafe { c_text(port.name) };
        // SAFETY: as above, for the port's type.
        out.push((name, unsafe { wire_type(port.ty, 0) }?));
    }
    Ok(out)
}

/// Every opaque type key a wire type names, once each: the codecs a worker declares.
pub fn opaque_keys(ty: &WireType, keys: &mut Vec<String>) {
    match ty {
        WireType::Opaque(key) => {
            if !keys.contains(key) {
                keys.push(key.clone());
            }
        }
        WireType::Scalar(_) => {}
        WireType::Optional(inner) | WireType::List(inner) => opaque_keys(inner, keys),
        WireType::Map(key, value) => {
            opaque_keys(key, keys);
            opaque_keys(value, keys);
        }
        WireType::Tuple(items) => items.iter().for_each(|item| opaque_keys(item, keys)),
        WireType::Struct(fields) => fields
            .iter()
            .for_each(|(_, field)| opaque_keys(field, keys)),
        WireType::Enum(variants) => {
            for payload in variants.iter().filter_map(|(_, payload)| payload.as_ref()) {
                opaque_keys(payload, keys);
            }
        }
    }
}

/// Reads a C value of `ty` as a wire value.
///
/// # Safety
/// `value` holds a value of `ty` as the C side wrote it, and every pointer it holds is valid for
/// the reads `ty` names.
pub unsafe fn to_wire(ty: &WireType, value: &DdValue, depth: u32) -> Result<WireValue, Refused> {
    if depth > MAX_DEPTH {
        return Err(Refused);
    }
    let deeper = depth + 1;
    Ok(match ty {
        WireType::Scalar(scalar) => {
            // SAFETY: the type names the scalar the value holds, as the caller's contract says.
            unsafe { scalar_value(*scalar, value)? }
        }
        WireType::Opaque(_) => {
            // SAFETY: an opaque value holds bytes, as the caller's contract says.
            WireValue::Opaque(unsafe { bytes_of(value)? })
        }
        WireType::Optional(inner) => {
            if !value.present {
                return Ok(WireValue::Optional(None));
            }
            // SAFETY: a present optional holds its one value at the sequence's first item.
            let seq = unsafe { seq_of(value) };
            // SAFETY: as above, for the one item.
            let items = unsafe { items_of(seq.items, 1)? };
            // SAFETY: the item is the value of the optional's type, as the caller's contract says.
            WireValue::Optional(Some(Box::new(unsafe {
                to_wire(inner, &items[0], deeper)?
            })))
        }
        WireType::List(inner) => {
            // SAFETY: a list holds its items in its sequence.
            let seq = unsafe { seq_of(value) };
            // SAFETY: as above, for the `count` items.
            let items = unsafe { items_of(seq.items, seq.count)? };
            let mut out = Vec::with_capacity(items.len());
            for item in items {
                // SAFETY: each item is a value of the list's item type.
                out.push(unsafe { to_wire(inner, item, deeper)? });
            }
            WireValue::List(out)
        }
        WireType::Map(key_ty, value_ty) => {
            // SAFETY: a map holds its key and value pairs in its sequence.
            let seq = unsafe { seq_of(value) };
            if !seq.count.is_multiple_of(2) {
                return Err(Refused);
            }
            // SAFETY: as above, for the `count` items.
            let items = unsafe { items_of(seq.items, seq.count)? };
            let mut pairs = Vec::with_capacity(items.len() / 2);
            let (pairs_of_items, _) = items.as_chunks::<2>();
            for pair in pairs_of_items {
                // SAFETY: each pair is a key and a value of the map's types.
                let key = unsafe { to_wire(key_ty, &pair[0], deeper)? };
                // SAFETY: as above, for the value.
                let item = unsafe { to_wire(value_ty, &pair[1], deeper)? };
                pairs.push((key, item));
            }
            WireValue::Map(pairs)
        }
        WireType::Tuple(types) => {
            // SAFETY: a tuple holds one value per field in its sequence.
            WireValue::Tuple(unsafe { seq_values(value, types.iter(), deeper)? })
        }
        WireType::Struct(fields) => {
            // SAFETY: a struct holds one value per field, in the field order.
            WireValue::Struct(unsafe {
                seq_values(value, fields.iter().map(|(_, ty)| ty), deeper)?
            })
        }
        WireType::Enum(variants) => {
            // SAFETY: an enum holds its variant and, for a variant with a payload, the payload.
            let en = unsafe { enum_of(value) };
            let (_, payload_ty) = variants.get(en.variant).ok_or(Refused)?;
            let payload = match payload_ty {
                None => None,
                Some(ty) => {
                    // SAFETY: a variant with a payload holds its payload's pointer, as the contract says.
                    let item = unsafe { en.payload.as_ref() }.ok_or(Refused)?;
                    // SAFETY: the payload is a value of the variant's type.
                    Some(Box::new(unsafe { to_wire(ty, item, deeper)? }))
                }
            };
            WireValue::Enum {
                variant: u32::try_from(en.variant).map_err(|_| Refused)?,
                payload,
            }
        }
    })
}

/// The values of a sequence, one per type, in order.
///
/// # Safety
/// `value` holds a sequence of values of `types`, as the caller's contract says.
unsafe fn seq_values<'t>(
    value: &DdValue,
    types: impl ExactSizeIterator<Item = &'t WireType>,
    depth: u32,
) -> Result<Vec<WireValue>, Refused> {
    // SAFETY: the value holds a sequence, as the caller's contract says.
    let seq = unsafe { seq_of(value) };
    if seq.count != types.len() {
        return Err(Refused);
    }
    // SAFETY: the sequence holds `count` values.
    let items = unsafe { items_of(seq.items, seq.count)? };
    let mut out = Vec::with_capacity(items.len());
    for (ty, item) in types.zip(items) {
        // SAFETY: each item is a value of its field's type.
        out.push(unsafe { to_wire(ty, item, depth)? });
    }
    Ok(out)
}

/// A scalar value. Each integer is read from the field every integer shares.
///
/// # Safety
/// `value` holds a scalar of `scalar`'s kind, as the C side wrote it.
unsafe fn scalar_value(scalar: Scalar, value: &DdValue) -> Result<WireValue, Refused> {
    Ok(match scalar {
        Scalar::Unit => WireValue::Unit,
        // SAFETY: a bool is one byte, which the C side wrote.
        Scalar::Bool => WireValue::Bool(unsafe { bool_byte(value) } != 0),
        // SAFETY: an integer is in the value's eight bytes, which the C side wrote.
        Scalar::I8 => {
            WireValue::I8(i8::try_from(unsafe { int_bits(value) } as i64).map_err(|_| Refused)?)
        }
        // SAFETY: as above.
        Scalar::I16 => {
            WireValue::I16(i16::try_from(unsafe { int_bits(value) } as i64).map_err(|_| Refused)?)
        }
        // SAFETY: as above.
        Scalar::I32 => {
            WireValue::I32(i32::try_from(unsafe { int_bits(value) } as i64).map_err(|_| Refused)?)
        }
        // SAFETY: as above.
        Scalar::Int => WireValue::I64(unsafe { int_bits(value) } as i64),
        // SAFETY: as above.
        Scalar::ISize => WireValue::ISize(unsafe { int_bits(value) } as i64),
        // SAFETY: as above.
        Scalar::U8 => WireValue::U8(u8::try_from(unsafe { int_bits(value) }).map_err(|_| Refused)?),
        // SAFETY: as above.
        Scalar::U16 => {
            WireValue::U16(u16::try_from(unsafe { int_bits(value) }).map_err(|_| Refused)?)
        }
        // SAFETY: as above.
        Scalar::U32 => {
            WireValue::U32(u32::try_from(unsafe { int_bits(value) }).map_err(|_| Refused)?)
        }
        // SAFETY: as above.
        Scalar::U64 => WireValue::U64(unsafe { int_bits(value) }),
        // SAFETY: as above.
        Scalar::USize => WireValue::USize(unsafe { int_bits(value) }),
        // SAFETY: a float is the value's double, which the C side wrote.
        Scalar::F32 => WireValue::F32(unsafe { float_bits(value) } as f32),
        // SAFETY: as above.
        Scalar::Float => WireValue::F64(unsafe { float_bits(value) }),
        Scalar::String => {
            // SAFETY: a string holds bytes, as the caller's contract says.
            let bytes = unsafe { bytes_of(value)? };
            WireValue::String(String::from_utf8(bytes).map_err(|_| Refused)?)
        }
        Scalar::Bytes => {
            // SAFETY: a `bytes` value holds bytes, as the caller's contract says.
            WireValue::Bytes(unsafe { bytes_of(value)? })
        }
    })
}

/// The eight bytes an integer value holds. Every bit pattern is an integer.
///
/// # Safety
/// `value` holds an integer, which the C side wrote in its `i` or `u` field.
unsafe fn int_bits(value: &DdValue) -> u64 {
    // SAFETY: every bit pattern of eight bytes is a valid `u64`, and the field was written.
    unsafe { value.data.u }
}

/// The double a float value holds.
///
/// # Safety
/// `value` holds a float, which the C side wrote in its `f` field.
unsafe fn float_bits(value: &DdValue) -> f64 {
    // SAFETY: the field holds a double the C side wrote.
    unsafe { value.data.f }
}

/// The one byte a bool value holds, read as a byte so an unset byte is never read as a bool.
///
/// # Safety
/// `value` holds a bool, which the C side wrote in its `b` field.
unsafe fn bool_byte(value: &DdValue) -> u8 {
    // SAFETY: the field's first byte is the bool the C side wrote, and a byte is always valid.
    unsafe { std::ptr::addr_of!(value.data.b).cast::<u8>().read() }
}

/// The bytes a string, bytes or opaque value holds, copied out.
///
/// # Safety
/// `value` holds bytes, as the C side wrote them: `len` readable bytes at `data`.
unsafe fn bytes_of(value: &DdValue) -> Result<Vec<u8>, Refused> {
    // SAFETY: the value holds bytes, so its `bytes` field is the one written.
    let bytes: DdBytes = unsafe { value.data.bytes };
    if bytes.len == 0 {
        return Ok(Vec::new());
    }
    if bytes.data.is_null() {
        return Err(Refused);
    }
    // SAFETY: the pointer is not null and names `len` readable bytes, as the contract says.
    Ok(unsafe { slice::from_raw_parts(bytes.data, bytes.len) }.to_vec())
}

/// The sequence field of a list, tuple, struct, map or optional value.
///
/// # Safety
/// `value` holds a sequence, which the C side wrote in its `seq` field.
unsafe fn seq_of(value: &DdValue) -> DdSeq {
    // SAFETY: the value holds a sequence, so its `seq` field is the one written.
    unsafe { value.data.seq }
}

/// The enum field of an enum value.
///
/// # Safety
/// `value` holds an enum, which the C side wrote in its `en` field.
unsafe fn enum_of(value: &DdValue) -> DdEnum {
    // SAFETY: the value holds an enum, so its `en` field is the one written.
    unsafe { value.data.en }
}

/// `count` values at `items`, as a slice that borrows them. Empty when `count` is 0.
///
/// # Safety
/// `items` is null when `count` is 0, and otherwise points to `count` values that stay valid for
/// the returned borrow.
unsafe fn items_of<'a>(items: *mut DdValue, count: usize) -> Result<&'a [DdValue], Refused> {
    if count == 0 {
        return Ok(&[]);
    }
    if items.is_null() {
        return Err(Refused);
    }
    // SAFETY: `items` is not null and points to `count` values, as the caller's contract says.
    Ok(unsafe { slice::from_raw_parts(items, count) })
}

/// Writes a wire value as a C value. The arena keeps the pointers the value holds until the run
/// ends. A place (a by-reference value) never reaches the C side, since the worker does not agree
/// to them, so it is written as nothing.
pub fn from_wire(value: WireValue, arena: &mut RunArena) -> DdValue {
    let mut out = DdValue::ZERO;
    match value {
        WireValue::Unit | WireValue::Optional(None) | WireValue::Placed(_) => {}
        WireValue::Bool(b) => out.data.b = b,
        WireValue::I8(v) => out.data.i = i64::from(v),
        WireValue::I16(v) => out.data.i = i64::from(v),
        WireValue::I32(v) => out.data.i = i64::from(v),
        WireValue::I64(v) | WireValue::ISize(v) => out.data.i = v,
        WireValue::U8(v) => out.data.u = u64::from(v),
        WireValue::U16(v) => out.data.u = u64::from(v),
        WireValue::U32(v) => out.data.u = u64::from(v),
        WireValue::U64(v) | WireValue::USize(v) => out.data.u = v,
        WireValue::F32(v) => out.data.f = f64::from(v),
        WireValue::F64(v) => out.data.f = v,
        WireValue::String(text) => out.data.bytes = bytes_slot(arena, text.into_bytes()),
        WireValue::Bytes(bytes) | WireValue::Opaque(bytes) => {
            out.data.bytes = bytes_slot(arena, bytes);
        }
        WireValue::Optional(Some(item)) => {
            out.present = true;
            let item = from_wire(*item, arena);
            out.data.seq = seq_slot(arena, vec![item]);
        }
        WireValue::List(items) | WireValue::Tuple(items) | WireValue::Struct(items) => {
            let mut values = Vec::with_capacity(items.len());
            for item in items {
                values.push(from_wire(item, arena));
            }
            out.data.seq = seq_slot(arena, values);
        }
        WireValue::Map(pairs) => {
            let mut values = Vec::with_capacity(pairs.len() * 2);
            for (key, item) in pairs {
                values.push(from_wire(key, arena));
                values.push(from_wire(item, arena));
            }
            out.data.seq = seq_slot(arena, values);
        }
        WireValue::Enum { variant, payload } => {
            let payload = match payload {
                None => std::ptr::null_mut(),
                Some(item) => {
                    let item = from_wire(*item, arena);
                    arena.keep_values(vec![item])
                }
            };
            out.data.en = DdEnum {
                variant: variant as usize,
                payload,
            };
        }
    }
    out
}

/// A byte field: the bytes kept by the arena, or empty.
fn bytes_slot(arena: &mut RunArena, bytes: Vec<u8>) -> DdBytes {
    let len = bytes.len();
    let data = if len == 0 {
        std::ptr::null()
    } else {
        arena.keep_bytes(bytes).cast_const()
    };
    DdBytes { data, len }
}

/// A sequence field: the values kept by the arena, or empty.
fn seq_slot(arena: &mut RunArena, values: Vec<DdValue>) -> DdSeq {
    let count = values.len();
    let items = if count == 0 {
        std::ptr::null_mut()
    } else {
        arena.keep_values(values)
    };
    DdSeq { items, count }
}
