//! Per-frame calls by port index (surface ABI 1.1): the `*_at` pushes and takes, and a batch push.
//!
//! A port's index is its position in the graph's port table (`daedalus_surface_graph_ports`, or
//! `daedalus_surface_graph_input_index` / `_output_index` by name). A caller resolves each port
//! once; a call then checks the index and the port's kind and goes straight to the engine, with no
//! name to validate or match. A batch pushes several values under one lock and one panic guard.

use lemnos_abi::{Handle, Status};

use crate::ffi::{self, run};
use crate::io::{push_bytes_into, push_scalar, take_bytes_into, take_scalar, write};
use crate::state::{Graph, Kind, lock};

/// [`PushEntry::kind`]: push [`PushEntry::int_value`] into an `int` port.
pub const PUSH_I64: u32 = 1;
/// [`PushEntry::kind`]: push [`PushEntry::float_value`] into a `float` port.
pub const PUSH_F64: u32 = 2;
/// [`PushEntry::kind`]: push [`PushEntry::int_value`] (0 or non-zero) into a `bool` port.
pub const PUSH_BOOL: u32 = 3;
/// [`PushEntry::kind`]: push the `len` bytes at [`PushEntry::data`] into a `bytes` port.
pub const PUSH_BYTES: u32 = 4;

/// One push of a `daedalus_surface_push_batch`: the input port's index, what is pushed
/// (`PUSH_*`), and the value in the field that kind reads. The other fields are ignored.
#[repr(C)]
#[derive(Clone, Copy, Debug)]
pub struct PushEntry {
    pub port: u32,
    pub kind: u32,
    /// The value of a `PUSH_I64`, or a `PUSH_BOOL` (0 or non-zero).
    pub int_value: i64,
    /// The value of a `PUSH_F64`.
    pub float_value: f64,
    /// The bytes of a `PUSH_BYTES`: `len` bytes, copied during the call.
    pub data: *const u8,
    pub len: usize,
}

/// Pushes an `i64` into input port `port` (an index, see the module docs).
/// [`Status::NOT_FOUND`] past the last input, [`Status::INVALID_ARGUMENT`] for a port of another
/// kind, [`Status::AGAIN`] when the input is full.
///
/// # Safety
/// Any arguments are accepted.
#[unsafe(no_mangle)]
pub unsafe extern "C" fn daedalus_surface_push_i64_at(
    graph: Handle,
    port: u32,
    value: i64,
) -> Status {
    run(|| {
        let tables = lock();
        let graph = tables.graphs.get(graph)?;
        push_scalar(graph, graph.input_at(port, Kind::Int)?, value)
    })
}

/// Pushes an `f64` into input port `port`; as [`daedalus_surface_push_i64_at`].
///
/// # Safety
/// Any arguments are accepted.
#[unsafe(no_mangle)]
pub unsafe extern "C" fn daedalus_surface_push_f64_at(
    graph: Handle,
    port: u32,
    value: f64,
) -> Status {
    run(|| {
        let tables = lock();
        let graph = tables.graphs.get(graph)?;
        push_scalar(graph, graph.input_at(port, Kind::Float)?, value)
    })
}

/// Pushes a `bool` (0 or non-zero) into input port `port`; as [`daedalus_surface_push_i64_at`].
///
/// # Safety
/// Any arguments are accepted.
#[unsafe(no_mangle)]
pub unsafe extern "C" fn daedalus_surface_push_bool_at(
    graph: Handle,
    port: u32,
    value: u8,
) -> Status {
    run(|| {
        let tables = lock();
        let graph = tables.graphs.get(graph)?;
        push_scalar(graph, graph.input_at(port, Kind::Bool)?, value != 0)
    })
}

/// Pushes `len` bytes into input port `port`, copied once into the port's reused vector; as
/// [`daedalus_surface_push_i64_at`] and `daedalus_surface_push_bytes`.
///
/// # Safety
/// `data` must be valid for `len` bytes.
#[unsafe(no_mangle)]
pub unsafe extern "C" fn daedalus_surface_push_bytes_at(
    graph: Handle,
    port: u32,
    data: *const u8,
    len: usize,
) -> Status {
    run(|| {
        // Safety: forwarded.
        let data = unsafe { ffi::bytes(data, len) }?;
        let mut tables = lock();
        let graph = tables.graphs.get_mut(graph)?;
        let index = graph.input_at(port, Kind::Bytes)?;
        push_bytes_into(graph, index, data)
    })
}

/// Pushes `count` entries in order, under one lock and one panic guard. Every entry is checked
/// first (its port index, its kind against the port's, its bytes pointer), so a bad entry pushes
/// nothing: [`Status::NOT_FOUND`] or [`Status::INVALID_ARGUMENT`] with `*done` 0. A push the
/// engine refuses ([`Status::AGAIN`] for a full input) stops the batch there: `*done` is the
/// number of entries pushed before it. `done` may be null.
///
/// # Safety
/// `entries` must be valid for `count` reads (any pointer when `count` is 0); each `PUSH_BYTES`
/// entry's `data` must be valid for `len` bytes; `done` must be null or valid for a write of a
/// `usize`.
#[unsafe(no_mangle)]
pub unsafe extern "C" fn daedalus_surface_push_batch(
    graph: Handle,
    entries: *const PushEntry,
    count: usize,
    done: *mut usize,
) -> Status {
    run(|| {
        let mut pushed = 0;
        // Safety: forwarded.
        let result = unsafe { batch(graph, entries, count, &mut pushed) };
        if !done.is_null() {
            // Safety: the caller passes a writable `usize` or null.
            unsafe { done.write(pushed) };
        }
        result
    })
}

/// # Safety
/// As [`daedalus_surface_push_batch`].
unsafe fn batch(
    graph: Handle,
    entries: *const PushEntry,
    count: usize,
    pushed: &mut usize,
) -> Result<(), Status> {
    let entries: &[PushEntry] = if count == 0 {
        &[]
    } else if entries.is_null() {
        return Err(Status::INVALID_ARGUMENT);
    } else {
        // Safety: the caller passes `count` readable entries.
        unsafe { core::slice::from_raw_parts(entries, count) }
    };
    let mut tables = lock();
    let graph: &mut Graph = tables.graphs.get_mut(graph)?;
    for entry in entries {
        graph.input_at(entry.port, kind_of(entry.kind)?)?;
        if entry.kind == PUSH_BYTES && entry.len != 0 && entry.data.is_null() {
            return Err(Status::INVALID_ARGUMENT);
        }
    }
    for entry in entries {
        let index = entry.port as usize;
        match entry.kind {
            PUSH_I64 => push_scalar(graph, index, entry.int_value),
            PUSH_F64 => push_scalar(graph, index, entry.float_value),
            PUSH_BOOL => push_scalar(graph, index, entry.int_value != 0),
            _ => {
                // Safety: checked above (non-null for a non-zero length); the caller vouches for
                // the length.
                let data = unsafe { ffi::bytes(entry.data, entry.len) }?;
                push_bytes_into(graph, index, data)
            }
        }?;
        *pushed += 1;
    }
    Ok(())
}

fn kind_of(push: u32) -> Result<Kind, Status> {
    match push {
        PUSH_I64 => Ok(Kind::Int),
        PUSH_F64 => Ok(Kind::Float),
        PUSH_BOOL => Ok(Kind::Bool),
        PUSH_BYTES => Ok(Kind::Bytes),
        _ => Err(Status::INVALID_ARGUMENT),
    }
}

/// Takes the next `i64` from output port `port` (an index): [`Status::OK`] with the value,
/// [`Status::AGAIN`] when nothing is queued, [`Status::NOT_FOUND`] past the last output,
/// [`Status::INVALID_ARGUMENT`] for a port of another kind or a null `out`.
///
/// # Safety
/// `out` must be valid for a write of an `i64`.
#[unsafe(no_mangle)]
pub unsafe extern "C" fn daedalus_surface_take_i64_at(
    graph: Handle,
    port: u32,
    out: *mut i64,
) -> Status {
    run(|| {
        if out.is_null() {
            return Err(Status::INVALID_ARGUMENT);
        }
        let tables = lock();
        let graph = tables.graphs.get(graph)?;
        let value = take_scalar::<i64>(graph, graph.output_at(port, Kind::Int)?)?;
        // Safety: forwarded.
        unsafe { write(out, value) }
    })
}

/// Takes the next `f64` from output port `port`; as [`daedalus_surface_take_i64_at`].
///
/// # Safety
/// `out` must be valid for a write of an `f64`.
#[unsafe(no_mangle)]
pub unsafe extern "C" fn daedalus_surface_take_f64_at(
    graph: Handle,
    port: u32,
    out: *mut f64,
) -> Status {
    run(|| {
        if out.is_null() {
            return Err(Status::INVALID_ARGUMENT);
        }
        let tables = lock();
        let graph = tables.graphs.get(graph)?;
        let value = take_scalar::<f64>(graph, graph.output_at(port, Kind::Float)?)?;
        // Safety: forwarded.
        unsafe { write(out, value) }
    })
}

/// Takes the next `bool` from output port `port` (written as 0 or 1); as
/// [`daedalus_surface_take_i64_at`].
///
/// # Safety
/// `out` must be valid for a write of a `u8`.
#[unsafe(no_mangle)]
pub unsafe extern "C" fn daedalus_surface_take_bool_at(
    graph: Handle,
    port: u32,
    out: *mut u8,
) -> Status {
    run(|| {
        if out.is_null() {
            return Err(Status::INVALID_ARGUMENT);
        }
        let tables = lock();
        let graph = tables.graphs.get(graph)?;
        let value = take_scalar::<bool>(graph, graph.output_at(port, Kind::Bool)?)?;
        // Safety: forwarded.
        unsafe { write(out, u8::from(value)) }
    })
}

/// Takes the next bytes from output port `port` into the caller's buffer; the needed-size
/// protocol of `daedalus_surface_take_bytes` applies (too small a buffer keeps the bytes for the
/// next take).
///
/// # Safety
/// `needed` must be valid for a write of a `usize`; `buf` must be valid for `cap` bytes when the
/// bytes fit.
#[unsafe(no_mangle)]
pub unsafe extern "C" fn daedalus_surface_take_bytes_at(
    graph: Handle,
    port: u32,
    buf: *mut u8,
    cap: usize,
    needed: *mut usize,
) -> Status {
    run(|| {
        let mut tables = lock();
        let graph = tables.graphs.get_mut(graph)?;
        let index = graph.output_at(port, Kind::Bytes)?;
        // Safety: forwarded.
        unsafe { take_bytes_into(graph, index, buf, cap, needed) }
    })
}
