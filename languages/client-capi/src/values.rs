//! Values: what the feed calls put on a port, and what the take calls take off one. A value
//! taken that the caller's buffer or type can't hold yet waits in the link, for the next take of
//! the same run and port, so no value is lost to a call that asked for it again.

use daedalus_client::Client;
use daedalus_types::Scalar;
use daedalus_wire::codec::{Reader, Writer};
use daedalus_wire::service::TypedValue;
use daedalus_wire::{Schema, WireType, WireValue};

use crate::guard::{copy_or_note, fail, guard, non_null, read_bytes, read_text, write_out};
use crate::handles::{Handle, lock};
use crate::link::{Held, Link, link, refuse};
use crate::shapes::Fed;
use crate::status::Status;

/// The connection's codec table: the opaque types its server has codecs for.
fn schema(client: &Client) -> Schema {
    client
        .server()
        .map(|hello| hello.codecs())
        .unwrap_or_default()
}

/// Decodes a typed value from `len` bytes at `bytes`: the encoding `TypedValue` has in a service
/// message, which is its type and then its value, with nothing after.
///
/// # Safety
/// As [`read_bytes`].
unsafe fn decode(bytes: *const u8, len: usize, schema: &Schema) -> Result<TypedValue, Status> {
    // SAFETY: the caller's contract is this function's.
    let bytes = unsafe { read_bytes(bytes, len) }?;
    let mut reader = Reader::new(bytes);
    let value =
        TypedValue::decode(&mut reader, schema).map_err(|error| fail(Status::WIRE, error))?;
    reader.finish().map_err(|error| fail(Status::WIRE, error))?;
    Ok(value)
}

/// Feeds the value that `value` makes to the input port `port`. Writes what the feed did to
/// `out`, which may be null.
///
/// # Safety
/// `port` is as [`read_text`] requires. `out` is null or points to a `daedalus_client_fed_t`.
unsafe fn feed_value(
    client: Handle,
    run: u64,
    port: *const u8,
    port_len: usize,
    out: *mut Fed,
    value: impl FnOnce(&Schema) -> Result<TypedValue, Status>,
) -> Status {
    guard(|| {
        let link = link(client)?;
        // SAFETY: the caller's contract is this function's.
        let port = unsafe { read_text(port, port_len) }?;
        let value = value(&schema(&link.client))?;
        let fed = link.client.feed(run, port, value).map_err(refuse)?;
        if out.is_null() {
            return Ok(());
        }
        let fed = Fed {
            woke: fed.woke,
            dropped: u8::from(fed.dropped),
        };
        // SAFETY: the caller's contract is this function's, and `out` is not null.
        unsafe { write_out(out, fed) }
    })
}

/// Feeds an `i64` to a port that carries `i64`.
///
/// # Safety
/// `port` is null or points to `port_len` readable bytes that stay as they are for the call. `out` is null or points to a
/// `daedalus_client_fed_t`.
// SAFETY: the name is this library's own, with its prefix: no other symbol has it.
#[unsafe(no_mangle)]
pub unsafe extern "C" fn daedalus_client_feed_i64(
    client: Handle,
    run: u64,
    port: *const u8,
    port_len: usize,
    value: i64,
    out: *mut Fed,
) -> Status {
    // SAFETY: the caller's contract is this function's.
    unsafe {
        feed_value(client, run, port, port_len, out, |_| {
            Ok(TypedValue::new(
                WireType::Scalar(Scalar::Int),
                WireValue::I64(value),
            ))
        })
    }
}

/// Feeds an `f64` to a port that carries `f64`.
///
/// # Safety
/// As [`daedalus_client_feed_i64`].
// SAFETY: the name is this library's own, with its prefix: no other symbol has it.
#[unsafe(no_mangle)]
pub unsafe extern "C" fn daedalus_client_feed_f64(
    client: Handle,
    run: u64,
    port: *const u8,
    port_len: usize,
    value: f64,
    out: *mut Fed,
) -> Status {
    // SAFETY: the caller's contract is this function's.
    unsafe {
        feed_value(client, run, port, port_len, out, |_| {
            Ok(TypedValue::new(
                WireType::Scalar(Scalar::Float),
                WireValue::F64(value),
            ))
        })
    }
}

/// Feeds a boolean, non-zero for true, to a port that carries `bool`.
///
/// # Safety
/// As [`daedalus_client_feed_i64`].
// SAFETY: the name is this library's own, with its prefix: no other symbol has it.
#[unsafe(no_mangle)]
pub unsafe extern "C" fn daedalus_client_feed_bool(
    client: Handle,
    run: u64,
    port: *const u8,
    port_len: usize,
    value: u8,
    out: *mut Fed,
) -> Status {
    // SAFETY: the caller's contract is this function's.
    unsafe {
        feed_value(client, run, port, port_len, out, |_| {
            Ok(TypedValue::new(
                WireType::Scalar(Scalar::Bool),
                WireValue::Bool(value != 0),
            ))
        })
    }
}

/// Feeds bytes, copied, to a port that carries `bytes`.
///
/// # Safety
/// As [`daedalus_client_feed_i64`], and `bytes` is null or points to `len` readable bytes.
// SAFETY: the name is this library's own, with its prefix: no other symbol has it.
#[unsafe(no_mangle)]
pub unsafe extern "C" fn daedalus_client_feed_bytes(
    client: Handle,
    run: u64,
    port: *const u8,
    port_len: usize,
    bytes: *const u8,
    len: usize,
    out: *mut Fed,
) -> Status {
    // SAFETY: the caller's contract is this function's.
    unsafe {
        feed_value(client, run, port, port_len, out, |_| {
            let bytes = read_bytes(bytes, len)?;
            Ok(TypedValue::new(
                WireType::Scalar(Scalar::Bytes),
                WireValue::Bytes(bytes.to_vec()),
            ))
        })
    }
}

/// Feeds UTF-8 text, copied, to a port that carries `string`.
///
/// # Safety
/// As [`daedalus_client_feed_bytes`], and the text is UTF-8.
// SAFETY: the name is this library's own, with its prefix: no other symbol has it.
#[unsafe(no_mangle)]
pub unsafe extern "C" fn daedalus_client_feed_str(
    client: Handle,
    run: u64,
    port: *const u8,
    port_len: usize,
    text: *const u8,
    len: usize,
    out: *mut Fed,
) -> Status {
    // SAFETY: the caller's contract is this function's.
    unsafe {
        feed_value(client, run, port, port_len, out, |_| {
            let text = read_text(text, len)?;
            Ok(TypedValue::new(
                WireType::Scalar(Scalar::String),
                WireValue::String(text.to_owned()),
            ))
        })
    }
}

/// Feeds a value of any type, given as its encoding as a typed value: the type, then the value,
/// in the encoding a service message carries. Its type must be the port's.
///
/// # Safety
/// `bytes` is null or points to `len` readable bytes. Other arguments as
/// [`daedalus_client_feed_i64`].
// SAFETY: the name is this library's own, with its prefix: no other symbol has it.
#[unsafe(no_mangle)]
pub unsafe extern "C" fn daedalus_client_feed_wire(
    client: Handle,
    run: u64,
    port: *const u8,
    port_len: usize,
    bytes: *const u8,
    len: usize,
    out: *mut Fed,
) -> Status {
    // SAFETY: the caller's contract is this function's.
    unsafe {
        feed_value(client, run, port, port_len, out, |schema| {
            decode(bytes, len, schema)
        })
    }
}

/// The next value of a port: the held one first, then one from the server.
fn next(link: &Link, run: u64, port: &str) -> Result<Option<TypedValue>, Status> {
    {
        let mut held = lock(&link.held);
        if let Some(at) = held
            .iter()
            .position(|held| held.run == run && held.port == port)
        {
            return Ok(Some(held.remove(at).value));
        }
    }
    link.client.take(run, port).map_err(refuse)
}

/// Keeps a value taken from the server, for the next take of its run and port.
fn hold(link: &Link, run: u64, port: &str, value: TypedValue) {
    lock(&link.held).push(Held {
        run,
        port: port.to_owned(),
        value,
    });
}

#[allow(clippy::too_many_arguments)] // One argument for each part of the take.
/// A take of a scalar: its value into `out`, and `present` set to 1 when there was one.
///
/// # Safety
/// `port` is as [`read_text`] requires. `out` and `present` are not null, and `out` points to a
/// `T`.
unsafe fn take_scalar<T>(
    client: Handle,
    run: u64,
    port: *const u8,
    port_len: usize,
    out: *mut T,
    present: *mut u8,
    want: WireType,
    pick: fn(&WireValue) -> Option<T>,
) -> Status {
    guard(|| {
        non_null(out, "the output value")?;
        non_null(present, "the presence flag")?;
        let link = link(client)?;
        // SAFETY: the caller's contract is this function's.
        let port = unsafe { read_text(port, port_len) }?;
        let Some(value) = next(&link, run, port)? else {
            // SAFETY: the caller's contract is this function's, and `present` was checked above.
            return unsafe { write_out(present, 0) };
        };
        // SAFETY: the caller's contract is this function's, and `present` was checked above.
        unsafe { write_out(present, 1) }?;
        if value.ty != want {
            let why = format!(
                "port `{port}` carries {}, not {}",
                value.ty.name(),
                want.name()
            );
            hold(&link, run, port, value);
            return Err(fail(Status::TYPE_MISMATCH, why));
        }
        let Some(found) = pick(&value.value) else {
            return Err(fail(
                Status::WIRE,
                "a value of the port's type has another shape",
            ));
        };
        // SAFETY: the caller's contract is this function's, and `out` was checked above.
        unsafe { write_out(out, found) }
    })
}

/// Takes the oldest `i64` of an output port. `present` is 1 and `out` written when there was one.
///
/// # Safety
/// `port` is null or points to `port_len` readable bytes. `out` points to an `int64_t`, `present` to a `uint8_t`.
// SAFETY: the name is this library's own, with its prefix: no other symbol has it.
#[unsafe(no_mangle)]
pub unsafe extern "C" fn daedalus_client_take_i64(
    client: Handle,
    run: u64,
    port: *const u8,
    port_len: usize,
    out: *mut i64,
    present: *mut u8,
) -> Status {
    // SAFETY: the caller's contract is this function's.
    unsafe {
        take_scalar(
            client,
            run,
            port,
            port_len,
            out,
            present,
            WireType::Scalar(Scalar::Int),
            |value| match value {
                WireValue::I64(value) => Some(*value),
                _ => None,
            },
        )
    }
}

/// Takes the oldest `f64` of an output port.
///
/// # Safety
/// As [`daedalus_client_take_i64`], with a `double` for `out`.
// SAFETY: the name is this library's own, with its prefix: no other symbol has it.
#[unsafe(no_mangle)]
pub unsafe extern "C" fn daedalus_client_take_f64(
    client: Handle,
    run: u64,
    port: *const u8,
    port_len: usize,
    out: *mut f64,
    present: *mut u8,
) -> Status {
    // SAFETY: the caller's contract is this function's.
    unsafe {
        take_scalar(
            client,
            run,
            port,
            port_len,
            out,
            present,
            WireType::Scalar(Scalar::Float),
            |value| match value {
                WireValue::F64(value) => Some(*value),
                _ => None,
            },
        )
    }
}

/// Takes the oldest `bool` of an output port, as 0 or 1.
///
/// # Safety
/// As [`daedalus_client_take_i64`], with a `uint8_t` for `out`.
// SAFETY: the name is this library's own, with its prefix: no other symbol has it.
#[unsafe(no_mangle)]
pub unsafe extern "C" fn daedalus_client_take_bool(
    client: Handle,
    run: u64,
    port: *const u8,
    port_len: usize,
    out: *mut u8,
    present: *mut u8,
) -> Status {
    // SAFETY: the caller's contract is this function's.
    unsafe {
        take_scalar(
            client,
            run,
            port,
            port_len,
            out,
            present,
            WireType::Scalar(Scalar::Bool),
            |value| match value {
                WireValue::Bool(value) => Some(u8::from(*value)),
                _ => None,
            },
        )
    }
}

#[allow(clippy::too_many_arguments)] // One argument for each part of the take.
/// A take of a value's bytes, or of its encoding: `encode` says which. A value the buffer can't
/// hold waits for the next take of its port, and the call says how big it is.
///
/// # Safety
/// `port` as [`read_text`] requires. `buf` is null or points to `cap` writable bytes. `len` and
/// `present` point to a `size_t` and a `uint8_t`.
unsafe fn take_bytes_of(
    client: Handle,
    run: u64,
    port: *const u8,
    port_len: usize,
    buf: *mut u8,
    cap: usize,
    len: *mut usize,
    present: *mut u8,
    encode: fn(&TypedValue, &Schema) -> Result<Vec<u8>, Status>,
) -> Status {
    guard(|| {
        non_null(len, "the length")?;
        non_null(present, "the presence flag")?;
        let link = link(client)?;
        // SAFETY: the caller's contract is this function's.
        let port = unsafe { read_text(port, port_len) }?;
        let Some(value) = next(&link, run, port)? else {
            // SAFETY: the caller's contract is this function's, and both were checked above.
            unsafe { write_out(present, 0) }?;
            return unsafe { write_out(len, 0) };
        };
        // SAFETY: the caller's contract is this function's, and `present` was checked above.
        unsafe { write_out(present, 1) }?;
        let bytes = match encode(&value, &schema(&link.client)) {
            Ok(bytes) => bytes,
            Err(status) => {
                hold(&link, run, port, value);
                return Err(status);
            }
        };
        // SAFETY: the caller's contract is this function's, and `len` was checked above.
        match unsafe { copy_or_note(&bytes, buf, cap, len) } {
            Ok(()) => Ok(()),
            Err(status) => {
                hold(&link, run, port, value);
                Err(status)
            }
        }
    })
}

/// The bytes of an output port's value, which must be `bytes`. With a buffer too small the call
/// fails with `DAEDALUS_CLIENT_BUFFER_TOO_SMALL`, writes the size, and the value waits for the
/// next take of its port.
///
/// # Safety
/// `port` is null or points to `port_len` readable bytes. `buf` is null or points to `cap` writable
/// bytes. `len` and `present` point to a `size_t` and a `uint8_t`.
#[allow(clippy::too_many_arguments)]
// One argument for each part of the take.
// SAFETY: the name is this library's own, with its prefix: no other symbol has it.
#[unsafe(no_mangle)]
pub unsafe extern "C" fn daedalus_client_take_bytes(
    client: Handle,
    run: u64,
    port: *const u8,
    port_len: usize,
    buf: *mut u8,
    cap: usize,
    len: *mut usize,
    present: *mut u8,
) -> Status {
    // SAFETY: the caller's contract is this function's.
    unsafe {
        take_bytes_of(
            client,
            run,
            port,
            port_len,
            buf,
            cap,
            len,
            present,
            |value, _| match &value.value {
                WireValue::Bytes(bytes) if value.ty == WireType::Scalar(Scalar::Bytes) => {
                    Ok(bytes.clone())
                }
                _ => Err(fail(
                    Status::TYPE_MISMATCH,
                    format!("the port carries {}, not bytes", value.ty.name()),
                )),
            },
        )
    }
}

/// The oldest value of an output port of any type, as its encoding as a typed value: the type,
/// then the value. Pass the bytes to `daedalus_client_feed_wire` to put it back. With a buffer too
/// small it waits as `daedalus_client_take_bytes` does.
///
/// # Safety
/// `port` is null or points to `port_len` readable bytes. `buf` is null or points to `cap` writable
/// bytes. `len` and `present` point to a `size_t` and a `uint8_t`.
#[allow(clippy::too_many_arguments)]
// One argument for each part of the take.
// SAFETY: the name is this library's own, with its prefix: no other symbol has it.
#[unsafe(no_mangle)]
pub unsafe extern "C" fn daedalus_client_take_wire(
    client: Handle,
    run: u64,
    port: *const u8,
    port_len: usize,
    buf: *mut u8,
    cap: usize,
    len: *mut usize,
    present: *mut u8,
) -> Status {
    // SAFETY: the caller's contract is this function's.
    unsafe {
        take_bytes_of(
            client,
            run,
            port,
            port_len,
            buf,
            cap,
            len,
            present,
            |value, schema| {
                let mut writer = Writer::new();
                value
                    .encode(&mut writer, schema)
                    .map_err(|error| fail(Status::WIRE, error))?;
                Ok(writer.buf)
            },
        )
    }
}
