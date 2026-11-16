//! Values in and out: integers, floats, booleans and bytes, by a port's name or its index.
//!
//! A C caller has four kinds of value, and a graph's ports have more types than that. A push
//! of an integer goes into a port of any integer type it fits, and a take of one comes out of
//! any integer type. The same for floats, with `f32` and `f64`. A number that doesn't fit the
//! port's type is refused, never cut.

use std::sync::Arc;

use daedalus_host::{Dir, Host};
use daedalus_value::Value;

use crate::Handle;
use crate::graph::{GRAPHS, GraphEntry};
use crate::guard::{copy_out, fail, guard};
use crate::handles::lock;
use crate::host::text;
use crate::status::Status;

/// What a port's type is, to a C caller.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
enum Kind {
    Int { bytes: usize, signed: bool },
    Float { bytes: usize },
    Bool,
    Bytes,
}

fn kind_of(key: &str) -> Option<Kind> {
    let int = |bytes, signed| Some(Kind::Int { bytes, signed });
    match key {
        "i8" => int(1, true),
        "i16" => int(2, true),
        "i32" => int(4, true),
        "i64" => int(8, true),
        "isize" => int(size_of::<isize>(), true),
        "u8" => int(1, false),
        "u16" => int(2, false),
        "u32" => int(4, false),
        "u64" => int(8, false),
        "usize" => int(size_of::<usize>(), false),
        "f32" => Some(Kind::Float { bytes: 4 }),
        "f64" => Some(Kind::Float { bytes: 8 }),
        "bool" => Some(Kind::Bool),
        "bytes" => Some(Kind::Bytes),
        _ => None,
    }
}

/// A port, by what the caller gave: its name, or its index.
#[derive(Clone, Copy)]
pub(crate) enum Port<'a> {
    Name(&'a str),
    Index(usize),
}

/// The port's index, name and kind, checked to go the way the call needs.
fn resolve(
    host: &Host<'static>,
    port: Port<'_>,
    dir: Dir,
) -> Result<(usize, &'static str, Option<Kind>), Status> {
    let ports = host.ports();
    let index = match port {
        Port::Name(name) => ports.index_of(name).ok_or(Status::UNKNOWN_PORT)?,
        Port::Index(index) => index,
    };
    let spec = ports.at(index).ok_or(Status::UNKNOWN_PORT)?;
    if spec.dir != dir {
        return Err(Status::WRONG_DIRECTION);
    }
    Ok((index, spec.name, kind_of(spec.key.as_str())))
}

/// A value a caller pushes.
#[derive(Clone, Copy)]
pub(crate) enum Pushed<'a> {
    I64(i64),
    F64(f64),
    Bool(bool),
    Bytes(&'a [u8]),
}

/// The bytes `value` is stored as in a port of `kind`: little-endian, in the port's own width.
/// `Err` when the value isn't of that kind, or doesn't fit.
fn encode(value: Pushed<'_>, kind: Option<Kind>) -> Result<([u8; 8], usize), Status> {
    let mut out = [0; 8];
    let len = match (value, kind) {
        (Pushed::I64(value), Some(Kind::Int { bytes, signed })) => {
            let bits = bytes as u32 * 8;
            let fits = match signed {
                true => {
                    bits == 64 || (-(1i64 << (bits - 1))..(1i64 << (bits - 1))).contains(&value)
                }
                false => value >= 0 && (bits == 64 || value < (1i64 << bits)),
            };
            if !fits {
                return Err(Status::OUT_OF_RANGE);
            }
            out.copy_from_slice(&value.to_le_bytes());
            bytes
        }
        (Pushed::F64(value), Some(Kind::Float { bytes: 8 })) => {
            out.copy_from_slice(&value.to_le_bytes());
            8
        }
        (Pushed::F64(value), Some(Kind::Float { .. })) => {
            let narrow = value as f32;
            // A finite number too large for an f32 would become infinite.
            if value.is_finite() && !narrow.is_finite() {
                return Err(Status::OUT_OF_RANGE);
            }
            out[..4].copy_from_slice(&narrow.to_le_bytes());
            4
        }
        (Pushed::Bool(value), Some(Kind::Bool)) => {
            out[0] = u8::from(value);
            1
        }
        _ => return Err(Status::TYPE_MISMATCH),
    };
    Ok((out, len))
}

/// Checks one push without making it.
pub(crate) fn check(host: &Host<'static>, port: Port<'_>, value: Pushed<'_>) -> Result<(), Status> {
    let (_, _, kind) = resolve(host, port, Dir::Input)?;
    match (value, kind) {
        (Pushed::Bytes(_), Some(Kind::Bytes)) => Ok(()),
        (Pushed::Bytes(_), _) => Err(Status::TYPE_MISMATCH),
        _ => encode(value, kind).map(|_| ()),
    }
}

pub(crate) fn push(entry: &mut GraphEntry, port: Port<'_>, value: Pushed<'_>) -> Status {
    let host = entry.loaded.host();
    let (_, name, kind) = match resolve(host, port, Dir::Input) {
        Ok(found) => found,
        Err(status) => return status,
    };
    let fed = match (value, kind) {
        (Pushed::Bytes(bytes), Some(Kind::Bytes)) => host.feed_shared(name, Arc::from(bytes)),
        (Pushed::Bytes(_), _) => return Status::TYPE_MISMATCH,
        _ => match encode(value, kind) {
            Ok((bytes, len)) => host.feed_inline(name, &bytes[..len]),
            Err(status) => return status,
        },
    };
    match fed {
        Ok(_) => {
            entry.telemetry.pushes += 1;
            Status::OK
        }
        Err(error) => fail(Status::RUN_FAILED, error),
    }
}

/// The number a value of an integer port holds, widened.
fn int_of(value: &Value, bytes: usize, signed: bool) -> Option<i128> {
    if let Some(stored) = value.bytes() {
        if stored.len() != bytes {
            return None;
        }
        let mut wide = [0; 16];
        wide[..bytes].copy_from_slice(stored);
        let raw = u128::from_le_bytes(wide);
        let shift = 128 - bytes as u32 * 8;
        // Sign-extend from the port's width.
        return Some(match signed {
            true => ((raw << shift) as i128) >> shift,
            false => raw as i128,
        });
    }
    // A node may hold even a small number behind a reference.
    macro_rules! typed {
        ($($ty:ty),*) => {
            None$(.or_else(|| value.typed::<$ty>().map(|held| *held as i128)))*
        };
    }
    typed!(i8, i16, i32, i64, isize, u8, u16, u32, u64, usize)
}

fn float_of(value: &Value, bytes: usize) -> Option<f64> {
    match (value.bytes(), bytes) {
        (Some(stored), 4) => Some(f64::from(f32::from_le_bytes(stored.try_into().ok()?))),
        (Some(stored), _) => Some(f64::from_le_bytes(stored.try_into().ok()?)),
        (None, _) => value
            .typed::<f64>()
            .copied()
            .or_else(|| value.typed::<f32>().map(|held| f64::from(*held))),
    }
}

/// What a take found, for a kind of value.
enum Taken {
    I64(i64),
    F64(f64),
    Bool(bool),
}

/// Takes the next value of the output `port` and reads it as `want`. A value that can't be
/// read that way stays where it is, for a take of the right kind.
fn take(entry: &mut GraphEntry, port: Port<'_>, want: Kind) -> Result<Option<Taken>, Status> {
    take_in(entry, port, want, (i64::MIN, i64::MAX))
}

/// As `take`, but an integer outside `range` (both ends included) is refused, and stays in the
/// port for the next take.
fn take_in(
    entry: &mut GraphEntry,
    port: Port<'_>,
    want: Kind,
    range: (i64, i64),
) -> Result<Option<Taken>, Status> {
    let (index, name, kind) = resolve(entry.loaded.host(), port, Dir::Output)?;
    let fits = matches!(
        (want, kind),
        (Kind::Int { .. }, Some(Kind::Int { .. }))
            | (Kind::Float { .. }, Some(Kind::Float { .. }))
            | (Kind::Bool, Some(Kind::Bool))
    );
    if !fits {
        return Err(Status::TYPE_MISMATCH);
    }
    let value = match entry.waiting.remove(&index) {
        Some(value) => value,
        None => match entry.loaded.host().take_value(name) {
            Ok(Some(value)) => value,
            Ok(None) => return Ok(None),
            Err(error) => return Err(fail(Status::RUN_FAILED, error)),
        },
    };
    let read = match kind {
        Some(Kind::Int { bytes, signed }) => match int_of(&value, bytes, signed) {
            Some(wide) => i64::try_from(wide)
                .ok()
                .filter(|held| (range.0..=range.1).contains(held))
                .map(Taken::I64)
                .ok_or(Status::OUT_OF_RANGE),
            None => Err(Status::TYPE_MISMATCH),
        },
        Some(Kind::Float { bytes }) => float_of(&value, bytes)
            .map(Taken::F64)
            .ok_or(Status::TYPE_MISMATCH),
        _ => match (value.bytes(), value.typed::<bool>()) {
            (Some([byte]), _) => Ok(Taken::Bool(*byte != 0)),
            (None, Some(held)) => Ok(Taken::Bool(*held)),
            _ => Err(Status::TYPE_MISMATCH),
        },
    };
    match read {
        Ok(taken) => {
            entry.telemetry.takes += 1;
            Ok(Some(taken))
        }
        Err(status) => {
            // Not lost: the next take of this port finds it.
            entry.waiting.insert(index, value);
            Err(status)
        }
    }
}

/// Runs `body` on the graph a handle names, with the port a caller gave by name.
///
/// # Safety
/// `name` is null or points to `len` readable bytes.
unsafe fn named(
    graph: Handle,
    name: *const u8,
    len: usize,
    body: impl FnOnce(&mut GraphEntry, Port<'_>) -> Status,
) -> Status {
    guard(|| {
        // SAFETY: the caller's contract is this function's.
        let name = match unsafe { text(name, len) } {
            Ok(name) => name,
            Err(status) => return status,
        };
        match GRAPHS.get(graph) {
            Ok(entry) => body(&mut lock(&entry), Port::Name(name)),
            Err(status) => status,
        }
    })
}

fn indexed(
    graph: Handle,
    index: usize,
    body: impl FnOnce(&mut GraphEntry, Port<'_>) -> Status,
) -> Status {
    guard(|| match GRAPHS.get(graph) {
        Ok(entry) => body(&mut lock(&entry), Port::Index(index)),
        Err(status) => status,
    })
}

/// Writes a take's result: `present` says whether there was a value, and `out` gets it.
///
/// # Safety
/// `out` and `present` are null or point to a `T` and a `uint8_t`.
unsafe fn give<T>(taken: Result<Option<T>, Status>, out: *mut T, present: *mut u8) -> Status {
    if out.is_null() || present.is_null() {
        return Status::NULL_POINTER;
    }
    match taken {
        Ok(Some(value)) => {
            // SAFETY: neither is null, and the caller says what they point to.
            unsafe {
                out.write(value);
                present.write(1);
            }
            Status::OK
        }
        Ok(None) => {
            // SAFETY: not null, and the caller says it points to a `uint8_t`.
            unsafe { present.write(0) };
            Status::OK
        }
        Err(status) => status,
    }
}

const INT: Kind = Kind::Int {
    bytes: 8,
    signed: true,
};
const FLOAT: Kind = Kind::Float { bytes: 8 };

fn take_i64(entry: &mut GraphEntry, port: Port<'_>) -> Result<Option<i64>, Status> {
    take_i64_within(entry, port, i64::MIN, i64::MAX)
}

/// Takes an integer if it lies in `low` to `high`, both included. Outside that range it is refused
/// and stays in the port.
fn take_i64_within(
    entry: &mut GraphEntry,
    port: Port<'_>,
    low: i64,
    high: i64,
) -> Result<Option<i64>, Status> {
    Ok(
        take_in(entry, port, INT, (low, high))?.and_then(|taken| match taken {
            Taken::I64(value) => Some(value),
            _ => None,
        }),
    )
}

fn take_f64(entry: &mut GraphEntry, port: Port<'_>) -> Result<Option<f64>, Status> {
    Ok(take(entry, port, FLOAT)?.and_then(|taken| match taken {
        Taken::F64(value) => Some(value),
        _ => None,
    }))
}

fn take_bool(entry: &mut GraphEntry, port: Port<'_>) -> Result<Option<u8>, Status> {
    Ok(
        take(entry, port, Kind::Bool)?.and_then(|taken| match taken {
            Taken::Bool(value) => Some(u8::from(value)),
            _ => None,
        }),
    )
}

/// Takes bytes into a caller's buffer. With a buffer too small the value waits, the length it
/// needs is written, and the next take of the port gets it.
///
/// # Safety
/// `buf` is null or points to `cap` writable bytes; `len` and `present` are null or point to a
/// `size_t` and a `uint8_t`.
unsafe fn take_bytes(
    entry: &mut GraphEntry,
    port: Port<'_>,
    buf: *mut u8,
    cap: usize,
    len: *mut usize,
    present: *mut u8,
) -> Status {
    if len.is_null() || present.is_null() {
        return Status::NULL_POINTER;
    }
    let (index, name, kind) = match resolve(entry.loaded.host(), port, Dir::Output) {
        Ok(found) => found,
        Err(status) => return status,
    };
    if kind != Some(Kind::Bytes) {
        return Status::TYPE_MISMATCH;
    }
    let value = match entry.waiting.remove(&index) {
        Some(value) => value,
        None => match entry.loaded.host().take_value(name) {
            Ok(Some(value)) => value,
            Ok(None) => {
                // SAFETY: neither is null, and the caller says what they point to.
                unsafe {
                    len.write(0);
                    present.write(0);
                }
                return Status::OK;
            }
            Err(error) => return fail(Status::RUN_FAILED, error),
        },
    };
    let stored: &[u8] = match value.bytes() {
        Some(stored) => stored,
        None => match value.typed::<Vec<u8>>() {
            Some(held) => held,
            None => {
                entry.waiting.insert(index, value);
                return Status::TYPE_MISMATCH;
            }
        },
    };
    // SAFETY: `present` is not null, and the rest is the caller's contract.
    let status = unsafe {
        present.write(1);
        copy_out(stored, buf, cap, len)
    };
    match status {
        Status::OK => entry.telemetry.takes += 1,
        _ => {
            entry.waiting.insert(index, value);
        }
    }
    status
}

macro_rules! push_calls {
    ($($(#[$doc:meta])* $by_name:ident, $by_index:ident, $ty:ty, |$value:ident| $pushed:expr;)*) => {$(
        $(#[$doc])*
        ///
        /// # Safety
        /// `name` points to `len` bytes of UTF-8.
        // SAFETY: the name is this library\'s own, with its prefix: no other symbol has it.
        #[unsafe(no_mangle)]
        pub unsafe extern "C" fn $by_name(graph: Handle, name: *const u8, len: usize, $value: $ty) -> Status {
            // SAFETY: the caller's contract is this function's.
            unsafe { named(graph, name, len, |entry, port| push(entry, port, $pushed)) }
        }

        $(#[$doc])*
        /// The port is given by its index.
        // SAFETY: the name is this library\'s own, with its prefix: no other symbol has it.
        #[unsafe(no_mangle)]
        pub extern "C" fn $by_index(graph: Handle, index: usize, $value: $ty) -> Status {
            indexed(graph, index, |entry, port| push(entry, port, $pushed))
        }
    )*};
}

push_calls! {
    /// Pushes an integer to an input port of any integer type it fits.
    daedalus_push_i64, daedalus_push_i64_at, i64, |value| Pushed::I64(value);
    /// Pushes a float to an input port of type `f64` or `f32`.
    daedalus_push_f64, daedalus_push_f64_at, f64, |value| Pushed::F64(value);
    /// Pushes a boolean to an input port of type `bool`. Zero is false, anything else true.
    daedalus_push_bool, daedalus_push_bool_at, u8, |value| Pushed::Bool(value != 0);
}

/// The bytes a caller gave: `len` of them at `bytes`, which may be null when `len` is 0.
///
/// # Safety
/// `bytes` is null or points to `len` readable bytes.
pub(crate) unsafe fn caller_bytes<'a>(bytes: *const u8, len: usize) -> Result<&'a [u8], Status> {
    match (bytes.is_null(), len) {
        (_, 0) => Ok(&[]),
        (true, _) => Err(Status::NULL_POINTER),
        // SAFETY: not null, and the caller says `len` bytes are readable for the call.
        (false, _) => Ok(unsafe { std::slice::from_raw_parts(bytes, len) }),
    }
}

/// Pushes bytes to an input port of type `bytes`. They are copied: the caller keeps its own.
///
/// # Safety
/// `name` points to `name_len` bytes of UTF-8, and `bytes` to `len` readable bytes.
// SAFETY: the name is this library\'s own, with its prefix: no other symbol has it.
#[unsafe(no_mangle)]
pub unsafe extern "C" fn daedalus_push_bytes(
    graph: Handle,
    name: *const u8,
    name_len: usize,
    bytes: *const u8,
    len: usize,
) -> Status {
    // SAFETY: the caller's contract is this function's.
    unsafe {
        named(graph, name, name_len, |entry, port| {
            match caller_bytes(bytes, len) {
                Ok(bytes) => push(entry, port, Pushed::Bytes(bytes)),
                Err(status) => status,
            }
        })
    }
}

/// Pushes bytes to the input port at `index`.
///
/// # Safety
/// `bytes` points to `len` readable bytes.
// SAFETY: the name is this library\'s own, with its prefix: no other symbol has it.
#[unsafe(no_mangle)]
pub unsafe extern "C" fn daedalus_push_bytes_at(
    graph: Handle,
    index: usize,
    bytes: *const u8,
    len: usize,
) -> Status {
    // SAFETY: the caller's contract is this function's.
    indexed(graph, index, |entry, port| {
        match unsafe { caller_bytes(bytes, len) } {
            Ok(bytes) => push(entry, port, Pushed::Bytes(bytes)),
            Err(status) => status,
        }
    })
}

macro_rules! take_calls {
    ($($(#[$doc:meta])* $by_name:ident, $by_index:ident, $ty:ty, $take:ident;)*) => {$(
        $(#[$doc])*
        /// `present` is set to 1 and `out` written when the port had a value, and `present`
        /// is set to 0 when it had none.
        ///
        /// # Safety
        /// `name` points to `len` bytes of UTF-8, and `out` and `present` to their types.
        // SAFETY: the name is this library\'s own, with its prefix: no other symbol has it.
        #[unsafe(no_mangle)]
        pub unsafe extern "C" fn $by_name(
            graph: Handle,
            name: *const u8,
            len: usize,
            out: *mut $ty,
            present: *mut u8,
        ) -> Status {
            // SAFETY: the caller's contract is this function's.
            unsafe { named(graph, name, len, |entry, port| give($take(entry, port), out, present)) }
        }

        $(#[$doc])*
        /// The port is given by its index.
        ///
        /// # Safety
        /// `out` and `present` point to their types.
        // SAFETY: the name is this library\'s own, with its prefix: no other symbol has it.
        #[unsafe(no_mangle)]
        pub unsafe extern "C" fn $by_index(
            graph: Handle,
            index: usize,
            out: *mut $ty,
            present: *mut u8,
        ) -> Status {
            // SAFETY: the caller's contract is this function's.
            indexed(graph, index, |entry, port| unsafe { give($take(entry, port), out, present) })
        }
    )*};
}

take_calls! {
    /// Takes the oldest value of an output port of any integer type.
    daedalus_take_i64, daedalus_take_i64_at, i64, take_i64;
    /// Takes the oldest value of an output port of type `f64` or `f32`.
    daedalus_take_f64, daedalus_take_f64_at, f64, take_f64;
    /// Takes the oldest value of an output port of type `bool`, as 0 or 1.
    daedalus_take_bool, daedalus_take_bool_at, u8, take_bool;
}

/// Takes the oldest value of an integer output port if it lies in `low` to `high`, both included.
/// A number outside that range gives `DAEDALUS_OUT_OF_RANGE` and stays in the port: a take
/// that is refused loses nothing. `present` is set to 1 and `out` written when the port had a
/// value, and `present` is set to 0 when it had none.
///
/// # Safety
/// `name` points to `len` bytes of UTF-8, and `out` and `present` to their types.
// SAFETY: the name is this library's own, with its prefix: no other symbol has it.
#[unsafe(no_mangle)]
pub unsafe extern "C" fn daedalus_take_i64_within(
    graph: Handle,
    name: *const u8,
    len: usize,
    low: i64,
    high: i64,
    out: *mut i64,
    present: *mut u8,
) -> Status {
    // SAFETY: the caller's contract is this function's.
    unsafe {
        named(graph, name, len, |entry, port| {
            give(take_i64_within(entry, port, low, high), out, present)
        })
    }
}

/// As `daedalus_take_i64_within`, with the port given by its index.
///
/// # Safety
/// `out` and `present` point to their types.
// SAFETY: the name is this library's own, with its prefix: no other symbol has it.
#[unsafe(no_mangle)]
pub unsafe extern "C" fn daedalus_take_i64_within_at(
    graph: Handle,
    index: usize,
    low: i64,
    high: i64,
    out: *mut i64,
    present: *mut u8,
) -> Status {
    // SAFETY: the caller's contract is this function's.
    indexed(graph, index, |entry, port| unsafe {
        give(take_i64_within(entry, port, low, high), out, present)
    })
}

/// Takes the oldest value of an output port of type `bytes` into `buf`, and writes its length
/// to `len`. When `buf` is null or too small the value waits: the length it needs is written,
/// the status is `DAEDALUS_BUFFER_TOO_SMALL`, and the next take of the port gets the same
/// value.
///
/// # Safety
/// `name` points to `name_len` bytes of UTF-8; `buf` is null or points to `cap` writable
/// bytes; `len` and `present` point to their types.
// SAFETY: the name is this library\'s own, with its prefix: no other symbol has it.
#[unsafe(no_mangle)]
pub unsafe extern "C" fn daedalus_take_bytes(
    graph: Handle,
    name: *const u8,
    name_len: usize,
    buf: *mut u8,
    cap: usize,
    len: *mut usize,
    present: *mut u8,
) -> Status {
    // SAFETY: the caller's contract is this function's.
    unsafe {
        named(graph, name, name_len, |entry, port| {
            take_bytes(entry, port, buf, cap, len, present)
        })
    }
}

/// Takes bytes from the output port at `index`.
///
/// # Safety
/// `buf` is null or points to `cap` writable bytes; `len` and `present` point to their types.
// SAFETY: the name is this library\'s own, with its prefix: no other symbol has it.
#[unsafe(no_mangle)]
pub unsafe extern "C" fn daedalus_take_bytes_at(
    graph: Handle,
    index: usize,
    buf: *mut u8,
    cap: usize,
    len: *mut usize,
    present: *mut u8,
) -> Status {
    // SAFETY: the caller's contract is this function's.
    indexed(graph, index, |entry, port| unsafe {
        take_bytes(entry, port, buf, cap, len, present)
    })
}

#[cfg(test)]
mod tests {
    use super::{Kind, Pushed, encode, kind_of};
    use crate::status::Status;

    #[test]
    fn a_number_goes_into_any_port_it_fits_and_no_other() {
        let u8_port = kind_of("u8");
        assert_eq!(
            encode(Pushed::I64(255), u8_port).map(|(b, n)| (b[0], n)),
            Ok((255, 1))
        );
        assert_eq!(
            encode(Pushed::I64(256), u8_port).err(),
            Some(Status::OUT_OF_RANGE)
        );
        assert_eq!(
            encode(Pushed::I64(-1), u8_port).err(),
            Some(Status::OUT_OF_RANGE)
        );
        let i16_port = kind_of("i16");
        assert_eq!(
            encode(Pushed::I64(-32768), i16_port).map(|(b, n)| (b[..2].to_vec(), n)),
            Ok((vec![0, 0x80], 2))
        );
        assert_eq!(
            encode(Pushed::I64(32768), i16_port).err(),
            Some(Status::OUT_OF_RANGE)
        );
        assert!(encode(Pushed::I64(i64::MIN), kind_of("i64")).is_ok());
        assert!(encode(Pushed::I64(i64::MAX), kind_of("u64")).is_ok());
        assert_eq!(
            encode(Pushed::I64(-1), kind_of("u64")).err(),
            Some(Status::OUT_OF_RANGE)
        );
        assert_eq!(
            encode(Pushed::F64(1.5), kind_of("f32"))
                .map(|(b, n)| (f32::from_le_bytes(b[..4].try_into().unwrap()), n)),
            Ok((1.5, 4))
        );
        assert_eq!(
            encode(Pushed::F64(1e300), kind_of("f32")).err(),
            Some(Status::OUT_OF_RANGE)
        );
        assert!(encode(Pushed::F64(f64::INFINITY), kind_of("f32")).is_ok());
        // A kind of value goes only into its kind of port.
        assert_eq!(
            encode(Pushed::I64(1), kind_of("f64")).err(),
            Some(Status::TYPE_MISMATCH)
        );
        assert_eq!(
            encode(Pushed::F64(1.0), kind_of("i64")).err(),
            Some(Status::TYPE_MISMATCH)
        );
        assert_eq!(
            encode(Pushed::Bool(true), kind_of("u8")).err(),
            Some(Status::TYPE_MISMATCH)
        );
        assert_eq!(
            encode(Pushed::I64(1), kind_of("my:type")).err(),
            Some(Status::TYPE_MISMATCH)
        );
        assert_eq!(kind_of("bytes"), Some(Kind::Bytes));
    }
}
