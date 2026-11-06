//! Ports by index: [`InputPort`] and [`OutputPort`] resolved once from a name, the pushes and takes
//! that take them, and [`Graph::push_batch`].
//!
//! A [`Graph`] reads its port table when it is compiled, so the by-name calls
//! ([`Graph::push_i64`], ...) find the index in the caller, outside the surface's lock, and call
//! the surface by index. Resolving a port once with [`Graph::input`] or [`Graph::output`] skips
//! even that lookup.

use std::marker::PhantomData;

use lemnos_abi::Status;

use crate::error::HostError;
use crate::host::Graph;
use crate::surface::check;

/// An input port of one [`Graph`], by index. Used with another graph, it names that graph's port
/// at the same index (or fails with `NOT_FOUND` or `INVALID_ARGUMENT`); it is never unsafe.
#[derive(Clone, Copy, Debug, PartialEq, Eq, Hash)]
pub struct InputPort(pub(crate) u32);

/// An output port of one [`Graph`], by index; as [`InputPort`].
#[derive(Clone, Copy, Debug, PartialEq, Eq, Hash)]
pub struct OutputPort(pub(crate) u32);

impl InputPort {
    /// The port's index: its position in the `inputs` of [`Graph::ports`].
    pub fn index(self) -> u32 {
        self.0
    }
}

impl OutputPort {
    /// The port's index: its position in the `outputs` of [`Graph::ports`].
    pub fn index(self) -> u32 {
        self.0
    }
}

// The surface's `PUSH_*` kinds.
const PUSH_I64: u32 = 1;
const PUSH_F64: u32 = 2;
const PUSH_BOOL: u32 = 3;
const PUSH_BYTES: u32 = 4;

/// One push of a [`Graph::push_batch`]. Laid out as the surface's `daedalus_push_entry`, so a
/// slice of them is handed to the surface as it is; the bytes are borrowed for `'a`.
#[repr(C)]
#[derive(Clone, Copy, Debug)]
pub struct Push<'a> {
    port: u32,
    kind: u32,
    int_value: i64,
    float_value: f64,
    data: *const u8,
    len: usize,
    bytes: PhantomData<&'a [u8]>,
}

impl<'a> Push<'a> {
    fn new(port: InputPort, kind: u32) -> Self {
        Self {
            port: port.0,
            kind,
            int_value: 0,
            float_value: 0.0,
            data: core::ptr::null(),
            len: 0,
            bytes: PhantomData,
        }
    }

    /// Push an `i64` into `port`.
    pub fn i64(port: InputPort, value: i64) -> Self {
        Self {
            int_value: value,
            ..Self::new(port, PUSH_I64)
        }
    }

    /// Push an `f64` into `port`.
    pub fn f64(port: InputPort, value: f64) -> Self {
        Self {
            float_value: value,
            ..Self::new(port, PUSH_F64)
        }
    }

    /// Push a `bool` into `port`.
    pub fn bool(port: InputPort, value: bool) -> Self {
        Self {
            int_value: i64::from(value),
            ..Self::new(port, PUSH_BOOL)
        }
    }

    /// Push a copy of `bytes` into `port`.
    pub fn bytes(port: InputPort, bytes: &'a [u8]) -> Self {
        Self {
            data: bytes.as_ptr(),
            len: bytes.len(),
            ..Self::new(port, PUSH_BYTES)
        }
    }
}

impl Graph<'_> {
    /// The input port `name`, resolved once: a `NOT_FOUND` error for an unknown name.
    pub fn input(&self, name: &str) -> Result<InputPort, HostError> {
        self.input_named(name, "daedalus_surface_graph_input_index")
    }

    /// The output port `name`, resolved once: a `NOT_FOUND` error for an unknown name.
    pub fn output(&self, name: &str) -> Result<OutputPort, HostError> {
        self.output_named(name, "daedalus_surface_graph_output_index")
    }

    pub(crate) fn input_named(
        &self,
        name: &str,
        call: &'static str,
    ) -> Result<InputPort, HostError> {
        match self.inputs.iter().position(|port| **port == *name) {
            Some(index) => Ok(InputPort(index as u32)),
            None => Err(not_found(call, "input", name)),
        }
    }

    pub(crate) fn output_named(
        &self,
        name: &str,
        call: &'static str,
    ) -> Result<OutputPort, HostError> {
        match self.outputs.iter().position(|port| **port == *name) {
            Some(index) => Ok(OutputPort(index as u32)),
            None => Err(not_found(call, "output", name)),
        }
    }

    /// Pushes an `i64` into `port`.
    pub fn push_i64_at(&self, port: InputPort, value: i64) -> Result<(), HostError> {
        // Safety: scalars only.
        check("daedalus_surface_push_i64_at", unsafe {
            (self.api().push_i64_at)(self.handle, port.0, value)
        })
    }

    /// Pushes an `f64` into `port`.
    pub fn push_f64_at(&self, port: InputPort, value: f64) -> Result<(), HostError> {
        // Safety: scalars only.
        check("daedalus_surface_push_f64_at", unsafe {
            (self.api().push_f64_at)(self.handle, port.0, value)
        })
    }

    /// Pushes a `bool` into `port`.
    pub fn push_bool_at(&self, port: InputPort, value: bool) -> Result<(), HostError> {
        // Safety: scalars only.
        check("daedalus_surface_push_bool_at", unsafe {
            (self.api().push_bool_at)(self.handle, port.0, u8::from(value))
        })
    }

    /// Pushes a copy of `bytes` into `port` (the surface copies them once, into a vector it
    /// reuses).
    pub fn push_bytes_at(&self, port: InputPort, bytes: &[u8]) -> Result<(), HostError> {
        // Safety: `bytes` is valid for its length for the call.
        check("daedalus_surface_push_bytes_at", unsafe {
            (self.api().push_bytes_at)(self.handle, port.0, bytes.as_ptr(), bytes.len())
        })
    }

    /// Pushes every entry in order, in one call: one lock and one panic guard in the surface.
    /// The surface checks every entry before pushing any, so a wrong port or kind pushes nothing.
    /// A push the engine refuses (`AGAIN` for a full input) stops the batch there; the error's
    /// status says why, and the entries before it are pushed.
    pub fn push_batch(&self, pushes: &[Push<'_>]) -> Result<(), HostError> {
        let mut done = 0usize;
        // Safety: `Push` is laid out as `daedalus_push_entry`; the slice and the bytes its entries
        // borrow are valid for the call, and `done` is a valid out-pointer.
        check("daedalus_surface_push_batch", unsafe {
            (self.api().push_batch)(self.handle, pushes.as_ptr(), pushes.len(), &mut done)
        })
    }

    /// Takes the next `i64` from `port`, or `None` when nothing is queued.
    pub fn take_i64_at(&self, port: OutputPort) -> Result<Option<i64>, HostError> {
        let mut value = 0i64;
        // Safety: `value` is a valid out-pointer.
        let status = unsafe { (self.api().take_i64_at)(self.handle, port.0, &mut value) };
        optional("daedalus_surface_take_i64_at", status, value)
    }

    /// Takes the next `f64` from `port`, or `None` when nothing is queued.
    pub fn take_f64_at(&self, port: OutputPort) -> Result<Option<f64>, HostError> {
        let mut value = 0f64;
        // Safety: `value` is a valid out-pointer.
        let status = unsafe { (self.api().take_f64_at)(self.handle, port.0, &mut value) };
        optional("daedalus_surface_take_f64_at", status, value)
    }

    /// Takes the next `bool` from `port`, or `None` when nothing is queued.
    pub fn take_bool_at(&self, port: OutputPort) -> Result<Option<bool>, HostError> {
        let mut value = 0u8;
        // Safety: `value` is a valid out-pointer.
        let status = unsafe { (self.api().take_bool_at)(self.handle, port.0, &mut value) };
        optional("daedalus_surface_take_bool_at", status, value != 0)
    }

    /// Takes the next bytes from `port` into `out` (replacing its contents). Returns `false` when
    /// nothing is queued. `out` keeps its capacity between calls, so a steady stream of takes into
    /// one vector allocates nothing, and nothing is written but the bytes taken.
    pub fn take_bytes_at(&self, port: OutputPort, out: &mut Vec<u8>) -> Result<bool, HostError> {
        let api = self.api();
        loop {
            out.clear();
            let spare = out.spare_capacity_mut();
            let cap = spare.len();
            let mut needed = 0usize;
            // Safety: `spare` is `cap` writable (uninitialized) bytes, which the surface only
            // writes; `needed` is a valid out-pointer.
            let status = unsafe {
                (api.take_bytes_at)(
                    self.handle,
                    port.0,
                    spare.as_mut_ptr().cast::<u8>(),
                    cap,
                    &mut needed,
                )
            };
            match status {
                Status::AGAIN => return Ok(false),
                Status::BUFFER_TOO_SMALL => {
                    // The surface kept the bytes for the next take: grow to fit and retry.
                    out.reserve_exact(needed);
                }
                status => {
                    check("daedalus_surface_take_bytes_at", status)?;
                    if needed > cap {
                        // Never trusted past the buffer: a surface that reports success writes
                        // at most `cap` bytes.
                        return Err(HostError::Call {
                            call: "daedalus_surface_take_bytes_at",
                            status: Status::IO,
                            message: format!("reported {needed} bytes into a {cap}-byte buffer"),
                        });
                    }
                    // Safety: the surface wrote `needed` (at most `cap`) bytes at the start of
                    // the spare capacity, so they are initialized.
                    unsafe { out.set_len(needed) };
                    return Ok(true);
                }
            }
        }
    }
}

/// A scalar take's result: `None` for `AGAIN`.
fn optional<T>(call: &'static str, status: Status, value: T) -> Result<Option<T>, HostError> {
    if status == Status::AGAIN {
        return Ok(None);
    }
    check(call, status)?;
    Ok(Some(value))
}

fn not_found(call: &'static str, direction: &str, name: &str) -> HostError {
    HostError::Call {
        call,
        status: Status::NOT_FOUND,
        message: format!("the graph has no {direction} port named `{name}`"),
    }
}
