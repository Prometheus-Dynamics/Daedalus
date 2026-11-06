//! RAII handles over the surface: a [`Host`] (plugin registry and engine) and the [`Graph`]s
//! compiled from it. `Drop` releases each; a graph borrows its host, so a host outlives its graphs.

use std::path::Path;

use lemnos_abi::{Handle, Status};

use crate::error::HostError;
use crate::surface::{Messages, Surface, check, check_with};

/// A host in the loaded surface. Released on drop.
pub struct Host<'s> {
    surface: &'s Surface,
    handle: Handle,
}

/// A compiled graph. Released on drop; it borrows the [`Host`] it was compiled from.
pub struct Graph<'a> {
    host: &'a Host<'a>,
    handle: Handle,
}

impl Surface {
    /// Creates a host: an empty plugin registry, then [`Host::install`] plugins into it.
    pub fn create_host(&self) -> Result<Host<'_>, HostError> {
        let mut handle = Handle::NULL;
        // Safety: `handle` is a valid out-pointer for the call.
        check("daedalus_surface_host_create", unsafe {
            (self.api.host_create)(&mut handle)
        })?;
        Ok(Host {
            surface: self,
            handle,
        })
    }
}

impl Host<'_> {
    /// Installs the plugin library at `path` through the stable handler path.
    pub fn install(&self, path: impl AsRef<Path>) -> Result<(), HostError> {
        let path = path
            .as_ref()
            .to_str()
            .ok_or(HostError::NotUtf8("the plugin path"))?;
        let mut messages = Messages::new();
        // Safety: `path` is valid for its length for the call; the sink outlives the call.
        let status = unsafe {
            (self.surface.api.host_install)(self.handle, path.as_ptr(), path.len(), messages.sink())
        };
        check_with("daedalus_surface_host_install", status, messages)
    }

    /// The node schemas of the installed plugins, as a JSON array.
    pub fn schemas(&self) -> Result<String, HostError> {
        let api = &self.surface.api;
        read_json("daedalus_surface_host_schemas", |buf, cap, needed| {
            // Safety: the buffer and out-pointer are valid for the call (see `read_json`).
            unsafe { (api.host_schemas)(self.handle, buf, cap, needed) }
        })
    }

    /// Compiles a `daedalus.graph` document (its JSON text) and prepares it.
    pub fn compile(&self, document: &str) -> Result<Graph<'_>, HostError> {
        let mut handle = Handle::NULL;
        let mut messages = Messages::new();
        // Safety: the document is valid for its length, `handle` is a valid out-pointer, and the
        // sink outlives the call.
        let status = unsafe {
            (self.surface.api.graph_compile)(
                self.handle,
                document.as_ptr(),
                document.len(),
                &mut handle,
                messages.sink(),
            )
        };
        check_with("daedalus_surface_graph_compile", status, messages)?;
        Ok(Graph { host: self, handle })
    }
}

impl Drop for Host<'_> {
    fn drop(&mut self) {
        // Safety: the handle is this host's. A failure (live graphs) leaves it in the surface; the
        // borrow rules already keep the graphs from outliving it.
        let _ = unsafe { (self.surface.api.host_release)(self.handle) };
    }
}

impl Graph<'_> {
    /// Pushes an `i64` into the input port `port`.
    pub fn push_i64(&self, port: &str, value: i64) -> Result<(), HostError> {
        let api = &self.host.surface.api;
        // Safety: `port` is valid for its length for the call.
        check("daedalus_surface_push_i64", unsafe {
            (api.push_i64)(self.handle, port.as_ptr(), port.len(), value)
        })
    }

    /// Pushes an `f64` into the input port `port`.
    pub fn push_f64(&self, port: &str, value: f64) -> Result<(), HostError> {
        let api = &self.host.surface.api;
        // Safety: as `push_i64`.
        check("daedalus_surface_push_f64", unsafe {
            (api.push_f64)(self.handle, port.as_ptr(), port.len(), value)
        })
    }

    /// Pushes a `bool` into the input port `port`.
    pub fn push_bool(&self, port: &str, value: bool) -> Result<(), HostError> {
        let api = &self.host.surface.api;
        // Safety: as `push_i64`.
        check("daedalus_surface_push_bool", unsafe {
            (api.push_bool)(self.handle, port.as_ptr(), port.len(), u8::from(value))
        })
    }

    /// Pushes bytes into the input port `port` (the surface copies them once, into a vector it
    /// reuses).
    pub fn push_bytes(&self, port: &str, bytes: &[u8]) -> Result<(), HostError> {
        let api = &self.host.surface.api;
        // Safety: both slices are valid for their lengths for the call.
        check("daedalus_surface_push_bytes", unsafe {
            (api.push_bytes)(
                self.handle,
                port.as_ptr(),
                port.len(),
                bytes.as_ptr(),
                bytes.len(),
            )
        })
    }

    /// Ticks the graph once.
    pub fn tick(&self) -> Result<(), HostError> {
        let mut messages = Messages::new();
        // Safety: the sink outlives the call.
        let status = unsafe { (self.host.surface.api.tick)(self.handle, messages.sink()) };
        check_with("daedalus_surface_tick", status, messages)
    }

    /// Takes the next `i64` from the output port `port`, or `None` when nothing is queued.
    pub fn take_i64(&self, port: &str) -> Result<Option<i64>, HostError> {
        let mut value = 0i64;
        let api = &self.host.surface.api;
        // Safety: `port` is valid for its length; `value` is a valid out-pointer.
        match unsafe { (api.take_i64)(self.handle, port.as_ptr(), port.len(), &mut value) } {
            Status::AGAIN => Ok(None),
            status => {
                check("daedalus_surface_take_i64", status)?;
                Ok(Some(value))
            }
        }
    }

    /// Takes the next `f64` from the output port `port`, or `None` when nothing is queued.
    pub fn take_f64(&self, port: &str) -> Result<Option<f64>, HostError> {
        let mut value = 0f64;
        let api = &self.host.surface.api;
        // Safety: as `take_i64`.
        match unsafe { (api.take_f64)(self.handle, port.as_ptr(), port.len(), &mut value) } {
            Status::AGAIN => Ok(None),
            status => {
                check("daedalus_surface_take_f64", status)?;
                Ok(Some(value))
            }
        }
    }

    /// Takes the next `bool` from the output port `port`, or `None` when nothing is queued.
    pub fn take_bool(&self, port: &str) -> Result<Option<bool>, HostError> {
        let mut value = 0u8;
        let api = &self.host.surface.api;
        // Safety: as `take_i64`.
        match unsafe { (api.take_bool)(self.handle, port.as_ptr(), port.len(), &mut value) } {
            Status::AGAIN => Ok(None),
            status => {
                check("daedalus_surface_take_bool", status)?;
                Ok(Some(value != 0))
            }
        }
    }

    /// Takes the next bytes from the output port `port` into `out` (replacing its contents).
    /// Returns `false` when nothing is queued. `out` keeps its capacity between calls, so a
    /// steady stream of takes into one vector allocates nothing.
    pub fn take_bytes(&self, port: &str, out: &mut Vec<u8>) -> Result<bool, HostError> {
        let api = &self.host.surface.api;
        loop {
            // Zero-fill up to the capacity: no allocation, the surface writes into it.
            let cap = out.capacity();
            out.clear();
            out.resize(cap, 0);
            let mut needed = 0usize;
            // Safety: `port` is valid for its length; `out` has `cap` writable bytes; `needed` is
            // a valid out-pointer.
            let status = unsafe {
                (api.take_bytes)(
                    self.handle,
                    port.as_ptr(),
                    port.len(),
                    out.as_mut_ptr(),
                    cap,
                    &mut needed,
                )
            };
            match status {
                Status::AGAIN => {
                    out.clear();
                    return Ok(false);
                }
                Status::BUFFER_TOO_SMALL => {
                    // The surface kept the bytes for the next take: grow to fit and retry.
                    out.reserve_exact(needed.saturating_sub(out.len()));
                }
                status => {
                    check("daedalus_surface_take_bytes", status)?;
                    out.truncate(needed);
                    return Ok(true);
                }
            }
        }
    }

    /// The graph's host ports, as JSON (`{"inputs": [...], "outputs": [...]}`).
    pub fn ports(&self) -> Result<String, HostError> {
        let api = &self.host.surface.api;
        read_json("daedalus_surface_graph_ports", |buf, cap, needed| {
            // Safety: forwarded to `read_json`'s buffer contract.
            unsafe { (api.graph_ports)(self.handle, buf, cap, needed) }
        })
    }

    /// The graph's plan explanation, as JSON.
    pub fn explain(&self) -> Result<String, HostError> {
        let api = &self.host.surface.api;
        read_json("daedalus_surface_graph_explain", |buf, cap, needed| {
            // Safety: as `ports`.
            unsafe { (api.graph_explain)(self.handle, buf, cap, needed) }
        })
    }

    /// The telemetry of the last tick, as JSON (`null` before the first tick).
    pub fn telemetry(&self) -> Result<String, HostError> {
        let api = &self.host.surface.api;
        read_json("daedalus_surface_graph_telemetry", |buf, cap, needed| {
            // Safety: as `ports`.
            unsafe { (api.graph_telemetry)(self.handle, buf, cap, needed) }
        })
    }
}

impl Drop for Graph<'_> {
    fn drop(&mut self) {
        // Safety: the handle is this graph's.
        let _ = unsafe { (self.host.surface.api.graph_release)(self.handle) };
    }
}

/// Runs a call that writes JSON into a caller buffer, growing the buffer to the size the call
/// reports when it is too small.
fn read_json(
    call: &'static str,
    mut run: impl FnMut(*mut u8, usize, *mut usize) -> Status,
) -> Result<String, HostError> {
    let mut buf = vec![0u8; 4096];
    loop {
        let mut needed = 0usize;
        let status = run(buf.as_mut_ptr(), buf.len(), &mut needed);
        if status == Status::BUFFER_TOO_SMALL {
            buf.resize(needed, 0);
            continue;
        }
        check(call, status)?;
        buf.truncate(needed);
        return Ok(String::from_utf8_lossy(&buf).into_owned());
    }
}
