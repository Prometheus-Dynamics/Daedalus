//! Per-frame calls: push a scalar or bytes into a host port, tick the graph, take from a host
//! port. One call each, never per node. A warm frame allocates nothing here: ports are looked up
//! by name without copying, the engine's port ids are reused, and bytes go through recycled
//! vectors.

use std::sync::Arc;

use daedalus::dylib::SharedBytes;
use daedalus::transport::FeedOutcome;
use lemnos_abi::{Handle, MessageSink, Status};

use crate::ffi::{self, run};
use crate::state::{Graph, Kind, lock};

/// Maps the engine's feed outcome: accepted (or replaced an older payload) is success, a full
/// or backpressured input is [`Status::AGAIN`], a closed one is [`Status::STALE_HANDLE`], and a
/// payload the input's type does not take is [`Status::INVALID_ARGUMENT`].
pub(crate) fn feed(outcome: FeedOutcome) -> Result<(), Status> {
    match outcome {
        FeedOutcome::Accepted { .. } | FeedOutcome::Replaced { .. } => Ok(()),
        FeedOutcome::Dropped { .. } | FeedOutcome::Backpressured => Err(Status::AGAIN),
        FeedOutcome::Closed => Err(Status::STALE_HANDLE),
        FeedOutcome::Rejected(_) => Err(Status::INVALID_ARGUMENT),
    }
}

/// Pushes an `i64` into input `port`.
///
/// # Safety
/// `port` must be valid for `port_len` bytes.
#[unsafe(no_mangle)]
pub unsafe extern "C" fn daedalus_surface_push_i64(
    graph: Handle,
    port: *const u8,
    port_len: usize,
    value: i64,
) -> Status {
    run(|| {
        // Safety: forwarded.
        let name = unsafe { ffi::text(port, port_len) }?;
        let mut tables = lock();
        let graph = tables.graphs.get_mut(graph)?;
        let index = graph.input(name, Kind::Int)?;
        feed(graph.graph.push(graph.inputs[index].id.clone(), value))
    })
}

/// Pushes an `f64` into input `port`.
///
/// # Safety
/// As [`daedalus_surface_push_i64`].
#[unsafe(no_mangle)]
pub unsafe extern "C" fn daedalus_surface_push_f64(
    graph: Handle,
    port: *const u8,
    port_len: usize,
    value: f64,
) -> Status {
    run(|| {
        // Safety: forwarded.
        let name = unsafe { ffi::text(port, port_len) }?;
        let mut tables = lock();
        let graph = tables.graphs.get_mut(graph)?;
        let index = graph.input(name, Kind::Float)?;
        feed(graph.graph.push(graph.inputs[index].id.clone(), value))
    })
}

/// Pushes a `bool` (0 or non-zero) into input `port`.
///
/// # Safety
/// As [`daedalus_surface_push_i64`].
#[unsafe(no_mangle)]
pub unsafe extern "C" fn daedalus_surface_push_bool(
    graph: Handle,
    port: *const u8,
    port_len: usize,
    value: u8,
) -> Status {
    run(|| {
        // Safety: forwarded.
        let name = unsafe { ffi::text(port, port_len) }?;
        let mut tables = lock();
        let graph = tables.graphs.get_mut(graph)?;
        let index = graph.input(name, Kind::Bool)?;
        feed(graph.graph.push(graph.inputs[index].id.clone(), value != 0))
    })
}

/// Pushes `len` bytes into input `port`. The bytes are copied into a vector the surface keeps for
/// the port and reuses once the graph has let go of the last one; the graph shares it, not
/// copies it.
///
/// # Safety
/// `port` valid for `port_len` bytes, `data` valid for `len` bytes.
#[unsafe(no_mangle)]
pub unsafe extern "C" fn daedalus_surface_push_bytes(
    graph: Handle,
    port: *const u8,
    port_len: usize,
    data: *const u8,
    len: usize,
) -> Status {
    run(|| {
        // Safety: forwarded.
        let name = unsafe { ffi::text(port, port_len) }?;
        // Safety: forwarded.
        let data = unsafe { ffi::bytes(data, len) }?;
        let mut tables = lock();
        let graph: &mut Graph = tables.graphs.get_mut(graph)?;
        let index = graph.input(name, Kind::Bytes)?;
        // Reuse the port's vector when the graph no longer holds the last one; otherwise start a
        // new one (the old one is still in flight).
        let mut shared = graph.spare[index]
            .take()
            .filter(|arc| Arc::strong_count(arc) == 1)
            .unwrap_or_else(|| Arc::new(Vec::with_capacity(data.len())));
        let vector = Arc::get_mut(&mut shared).ok_or(Status::BUSY)?;
        vector.clear();
        vector.extend_from_slice(data);
        graph.spare[index] = Some(Arc::clone(&shared));
        feed(graph.graph.push(
            graph.inputs[index].id.clone(),
            SharedBytes::from_arc(shared),
        ))
    })
}

/// Ticks the graph once. The telemetry of the tick is kept for `daedalus_surface_graph_telemetry`.
/// A failing node's message goes to `sink` with [`Status::IO`].
///
/// # Safety
/// Any handle is accepted; a stale or null one is [`Status::STALE_HANDLE`].
#[unsafe(no_mangle)]
pub unsafe extern "C" fn daedalus_surface_tick(graph: Handle, sink: MessageSink) -> Status {
    run(|| {
        let mut tables = lock();
        let graph = tables.graphs.get_mut(graph)?;
        match graph.graph.tick() {
            Ok(telemetry) => {
                graph.telemetry = Some(telemetry);
                Ok(())
            }
            Err(error) => {
                ffi::report(sink, &error);
                Err(Status::IO)
            }
        }
    })
}

/// Takes the next `i64` from output `port`: [`Status::OK`] with the value, or [`Status::AGAIN`]
/// when nothing is queued.
///
/// # Safety
/// `port` valid for `port_len` bytes; `out` valid for a write of an `i64`.
#[unsafe(no_mangle)]
pub unsafe extern "C" fn daedalus_surface_take_i64(
    graph: Handle,
    port: *const u8,
    port_len: usize,
    out: *mut i64,
) -> Status {
    run(|| {
        // Safety: forwarded.
        let name = unsafe { ffi::text(port, port_len) }?;
        if out.is_null() {
            return Err(Status::INVALID_ARGUMENT);
        }
        let mut tables = lock();
        let graph = tables.graphs.get_mut(graph)?;
        graph.output(name, Kind::Int)?;
        let value = graph.graph.take::<i64>(name).ok_or(Status::AGAIN)?;
        // Safety: `out` is writable (checked above).
        unsafe { out.write(value) };
        Ok(())
    })
}

/// Takes the next `f64` from output `port`; as [`daedalus_surface_take_i64`].
///
/// # Safety
/// As [`daedalus_surface_take_i64`].
#[unsafe(no_mangle)]
pub unsafe extern "C" fn daedalus_surface_take_f64(
    graph: Handle,
    port: *const u8,
    port_len: usize,
    out: *mut f64,
) -> Status {
    run(|| {
        // Safety: forwarded.
        let name = unsafe { ffi::text(port, port_len) }?;
        if out.is_null() {
            return Err(Status::INVALID_ARGUMENT);
        }
        let mut tables = lock();
        let graph = tables.graphs.get_mut(graph)?;
        graph.output(name, Kind::Float)?;
        let value = graph.graph.take::<f64>(name).ok_or(Status::AGAIN)?;
        // Safety: `out` is writable (checked above).
        unsafe { out.write(value) };
        Ok(())
    })
}

/// Takes the next `bool` from output `port` (written as 0 or 1); as [`daedalus_surface_take_i64`].
///
/// # Safety
/// As [`daedalus_surface_take_i64`], with `out` valid for a write of a `u8`.
#[unsafe(no_mangle)]
pub unsafe extern "C" fn daedalus_surface_take_bool(
    graph: Handle,
    port: *const u8,
    port_len: usize,
    out: *mut u8,
) -> Status {
    run(|| {
        // Safety: forwarded.
        let name = unsafe { ffi::text(port, port_len) }?;
        if out.is_null() {
            return Err(Status::INVALID_ARGUMENT);
        }
        let mut tables = lock();
        let graph = tables.graphs.get_mut(graph)?;
        graph.output(name, Kind::Bool)?;
        let value = graph.graph.take::<bool>(name).ok_or(Status::AGAIN)?;
        // Safety: `out` is writable (checked above).
        unsafe { out.write(u8::from(value)) };
        Ok(())
    })
}

/// Takes the next bytes from output `port` into the caller's buffer, and writes their length to
/// `*needed`. [`Status::OK`] when copied, [`Status::AGAIN`] when nothing is queued, and
/// [`Status::BUFFER_TOO_SMALL`] with the length set when `cap` is too small: the bytes are kept
/// and the next take of the port returns them first, so the caller retries with a larger buffer.
///
/// # Safety
/// `port` valid for `port_len` bytes; `needed` valid for a `usize`; `buf` valid for `cap` bytes
/// when the bytes fit.
#[unsafe(no_mangle)]
pub unsafe extern "C" fn daedalus_surface_take_bytes(
    graph: Handle,
    port: *const u8,
    port_len: usize,
    buf: *mut u8,
    cap: usize,
    needed: *mut usize,
) -> Status {
    run(|| {
        // Safety: forwarded.
        let name = unsafe { ffi::text(port, port_len) }?;
        if needed.is_null() {
            return Err(Status::INVALID_ARGUMENT);
        }
        let mut tables = lock();
        let graph = tables.graphs.get_mut(graph)?;
        let index = graph.output(name, Kind::Bytes)?;
        let bytes = match graph.stash.iter().position(|(i, _)| *i == index) {
            Some(at) => graph.stash.swap_remove(at).1,
            None => graph.graph.take::<SharedBytes>(name).ok_or(Status::AGAIN)?,
        };
        // Safety: forwarded (`buf`, `cap`, `needed`).
        match unsafe { ffi::write_out(&bytes, buf, cap, needed) } {
            Err(status) if status == Status::BUFFER_TOO_SMALL => {
                graph.stash.push((index, bytes));
                Err(status)
            }
            other => other,
        }
    })
}
