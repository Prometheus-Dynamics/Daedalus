//! The codec, exported for the C++ test: encode one value of a type, and decode bytes as one value
//! of a type. A worker does not call these; they are the SDK's own check that a value round-trips.

use std::ffi::{c_int, c_void};
use std::ptr;
use std::slice;

use daedalus_wire::codec::{Reader, Writer};
use daedalus_wire::{Schema, WireType, decode_value, encode_value};

use crate::arena::RunArena;
use crate::convert::{from_wire, opaque_keys, to_wire, wire_type};
use crate::ffi::{DdType, DdValue};

/// A schema that has a codec for every opaque key the type names, which is all a bare value needs.
fn codec_schema(ty: &WireType) -> Schema {
    let mut keys = Vec::new();
    opaque_keys(ty, &mut keys);
    let mut schema = Schema::new();
    for key in &keys {
        schema.codec(key);
    }
    schema
}

/// Encodes `*v` as `*ty`. On success `*out` holds `*len` bytes, freed by `dd_internal_free_bytes`.
/// Returns -1 when the value does not fit its type.
///
/// # Safety
/// `ty` and `v` are valid for the call, and `out` and `len` are writable.
#[unsafe(no_mangle)]
pub unsafe extern "C" fn dd_internal_encode(
    ty: *const DdType,
    v: *const DdValue,
    out: *mut *mut u8,
    len: *mut usize,
) -> c_int {
    // SAFETY: the caller's contract makes `ty` a valid type table.
    let Ok(wire) = (unsafe { wire_type(ty, 0) }) else {
        return -1;
    };
    // SAFETY: the caller's contract makes `v` a valid value of that type.
    let Some(value) = (unsafe { v.as_ref() }) else {
        return -1;
    };
    // SAFETY: as above, the value is of the type.
    let Ok(wire_value) = (unsafe { to_wire(&wire, value, 0) }) else {
        return -1;
    };
    let mut body = Writer::new();
    if encode_value(&wire, &wire_value, &codec_schema(&wire), &mut body).is_err() {
        return -1;
    }
    // SAFETY: malloc with at least one byte, so the result is either null or a fresh allocation.
    let copy = unsafe { libc::malloc(body.buf.len().max(1)) }.cast::<u8>();
    if copy.is_null() {
        return -1;
    }
    // SAFETY: `copy` is a fresh allocation of at least `body.buf.len()` bytes, which `body` does not
    // overlap.
    unsafe { ptr::copy_nonoverlapping(body.buf.as_ptr(), copy, body.buf.len()) };
    // SAFETY: `out` is writable, as the caller's contract says.
    unsafe { out.write(copy) };
    // SAFETY: `len` is writable, as the caller's contract says.
    unsafe { len.write(body.buf.len()) };
    0
}

/// Decodes exactly `len` bytes as one value of `ty`. The value's pointers live in an arena that
/// `*keep` owns, freed by `dd_internal_free_arena`. Returns -1 if the bytes do not decode exactly.
///
/// # Safety
/// `p` is readable for `len` bytes, and `out` and `keep` are writable.
#[unsafe(no_mangle)]
pub unsafe extern "C" fn dd_internal_decode(
    ty: *const DdType,
    p: *const u8,
    len: usize,
    out: *mut DdValue,
    keep: *mut *mut c_void,
) -> c_int {
    // SAFETY: the caller's contract makes `ty` a valid type table.
    let Ok(wire) = (unsafe { wire_type(ty, 0) }) else {
        return -1;
    };
    if len > 0 && p.is_null() {
        return -1;
    }
    let bytes: &[u8] = if len == 0 {
        &[]
    } else {
        // SAFETY: `p` is not null and readable for `len` bytes, as the caller's contract says.
        unsafe { slice::from_raw_parts(p, len) }
    };
    let schema = codec_schema(&wire);
    let mut input = Reader::new(bytes);
    let Ok(value) = decode_value(&wire, &mut input, &schema) else {
        return -1;
    };
    if input.finish().is_err() {
        return -1;
    }
    let mut arena = RunArena::default();
    let c_value = from_wire(value, &mut arena);
    // SAFETY: `out` is writable, as the caller's contract says.
    unsafe { out.write(c_value) };
    // SAFETY: `keep` is writable, and the arena moves to the heap for the caller to free.
    unsafe { keep.write(Box::into_raw(Box::new(arena)).cast()) };
    0
}

/// Frees bytes that `dd_internal_encode` returned.
///
/// # Safety
/// `p` is null, or a pointer `dd_internal_encode` returned and that has not been freed.
#[unsafe(no_mangle)]
pub unsafe extern "C" fn dd_internal_free_bytes(p: *mut u8) {
    // SAFETY: the pointer came from malloc in `dd_internal_encode`, and is freed once.
    unsafe { libc::free(p.cast()) };
}

/// Frees the arena that `dd_internal_decode` kept.
///
/// # Safety
/// `keep` is null, or an arena `dd_internal_decode` kept that has not been freed.
#[unsafe(no_mangle)]
pub unsafe extern "C" fn dd_internal_free_arena(keep: *mut c_void) {
    if keep.is_null() {
        return;
    }
    // SAFETY: the pointer is the arena `dd_internal_decode` boxed, and it is freed once.
    unsafe { drop(Box::from_raw(keep.cast::<RunArena>())) };
}
