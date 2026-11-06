//! Graphs: compile a `daedalus.graph` document into a graph handle, release it, and read its
//! port table, plan explanation and last telemetry as JSON.

use daedalus::GraphDocument;
use daedalus::dylib::SharedBytes;
use daedalus::runtime::HostPortDescriptor;
use daedalus::transport::{PayloadSlot, TypeKey};
use lemnos_abi::{Handle, MessageSink, Status};

use crate::ffi::{self, run};
use crate::state::{Graph, Kind, Port, intern, lock, position};

/// Compiles the graph document at `json` (UTF-8, `daedalus.graph` format, not NUL-terminated)
/// against host `host`'s registry, prepares it, and writes its handle to `out`. The
/// document's `requires` must be installed in the host; the reason for any failure goes to
/// `sink`.
///
/// # Safety
/// `json` must be valid for `json_len` bytes; `out` must be valid for a write of a `Handle`.
#[unsafe(no_mangle)]
pub unsafe extern "C" fn daedalus_surface_graph_compile(
    host: Handle,
    json: *const u8,
    json_len: usize,
    out: *mut Handle,
    sink: MessageSink,
) -> Status {
    run(|| {
        if out.is_null() {
            return Err(Status::INVALID_ARGUMENT);
        }
        // Safety: forwarded.
        let text = unsafe { ffi::text(json, json_len) }?;
        let document = GraphDocument::from_json(text).map_err(|error| {
            ffi::report(sink, &error);
            Status::INVALID_ARGUMENT
        })?;
        let mut tables = lock();
        let tables = &mut *tables;
        let host_state = tables.hosts.get_mut(host)?;
        let mut graph = host_state
            .engine
            .compile_document(&host_state.registry, document)
            .map_err(|error| {
                ffi::report(sink, &error);
                Status::INVALID_ARGUMENT
            })?;
        graph.prepare().map_err(|error| {
            ffi::report(sink, &error);
            Status::INVALID_ARGUMENT
        })?;
        let inputs = ports(graph.host_inputs());
        let outputs = ports(graph.host_outputs());
        let spare = (0..inputs.len()).map(|_| None).collect();
        let slots = (0..inputs.len()).map(|_| PayloadSlot::default()).collect();
        let bytes_key = graph
            .type_index()
            .key_of::<SharedBytes>()
            .ok()
            .map(|key| TypeKey::from_static(intern(key.as_str())));
        #[cfg(target_os = "linux")]
        let frame_spare = (0..inputs.len()).map(|_| None).collect();
        let handle = tables.graphs.insert(Graph {
            host,
            graph,
            inputs,
            outputs,
            spare,
            slots,
            bytes_key,
            stash: Vec::new(),
            #[cfg(target_os = "linux")]
            frame_spare,
            #[cfg(target_os = "linux")]
            frame_stash: Vec::new(),
            telemetry: None,
        })?;
        host_state.graphs += 1;
        // Safety: `out` is writable (checked above).
        unsafe { out.write(handle) };
        Ok(())
    })
}

fn ports(descriptors: Vec<HostPortDescriptor>) -> Vec<Port> {
    descriptors
        .into_iter()
        .map(|d| Port::new(d.name.as_str(), Kind::of(d.type_expr.as_ref())))
        .collect()
}

/// Releases a graph. Its host may then be released.
///
/// # Safety
/// Any handle value is accepted; a stale or null one is [`Status::STALE_HANDLE`].
#[unsafe(no_mangle)]
pub unsafe extern "C" fn daedalus_surface_graph_release(graph: Handle) -> Status {
    run(|| {
        let mut tables = lock();
        let tables = &mut *tables;
        let released = tables.graphs.remove(graph)?;
        if let Ok(host) = tables.hosts.get_mut(released.host) {
            host.graphs -= 1;
        }
        Ok(())
    })
}

/// Writes the graph's host ports as JSON: `{"inputs":[{"name","kind"}],"outputs":[...]}`, where
/// `kind` is `int`, `float`, `bool`, `bytes`, `frame` or `other` (the ABI moves the first five).
///
/// # Safety
/// As `ffi::write_out`: `needed` valid for a `usize`, `buf` for `cap` bytes when it fits.
#[unsafe(no_mangle)]
pub unsafe extern "C" fn daedalus_surface_graph_ports(
    graph: Handle,
    buf: *mut u8,
    cap: usize,
    needed: *mut usize,
) -> Status {
    run(|| {
        let json = {
            let tables = lock();
            let graph = tables.graphs.get(graph)?;
            let describe = |ports: &[Port]| -> Vec<serde_json::Value> {
                ports
                    .iter()
                    .map(|p| serde_json::json!({ "name": p.name, "kind": p.kind.name() }))
                    .collect()
            };
            serde_json::json!({
                "inputs": describe(&graph.inputs),
                "outputs": describe(&graph.outputs),
            })
            .to_string()
            .into_bytes()
        };
        // Safety: forwarded.
        unsafe { ffi::write_out(&json, buf, cap, needed) }
    })
}

/// Writes the graph's plan explanation (`RuntimePlanExplanation`) as JSON.
///
/// # Safety
/// As [`daedalus_surface_graph_ports`].
#[unsafe(no_mangle)]
pub unsafe extern "C" fn daedalus_surface_graph_explain(
    graph: Handle,
    buf: *mut u8,
    cap: usize,
    needed: *mut usize,
) -> Status {
    run(|| {
        let json = {
            let tables = lock();
            let graph = tables.graphs.get(graph)?;
            serde_json::to_vec(&graph.graph.explain_plan()).map_err(|_| Status::IO)?
        };
        // Safety: forwarded.
        unsafe { ffi::write_out(&json, buf, cap, needed) }
    })
}

/// Writes the telemetry of the graph's last tick as JSON (`ExecutionTelemetry`), or `null` before
/// the first tick.
///
/// # Safety
/// As [`daedalus_surface_graph_ports`].
#[unsafe(no_mangle)]
pub unsafe extern "C" fn daedalus_surface_graph_telemetry(
    graph: Handle,
    buf: *mut u8,
    cap: usize,
    needed: *mut usize,
) -> Status {
    run(|| {
        let json = {
            let tables = lock();
            let graph = tables.graphs.get(graph)?;
            serde_json::to_vec(&graph.telemetry).map_err(|_| Status::IO)?
        };
        // Safety: forwarded.
        unsafe { ffi::write_out(&json, buf, cap, needed) }
    })
}

/// Writes the index of input port `port` to `*index`: its position in the `inputs` array of
/// [`daedalus_surface_graph_ports`], whatever its kind. The `*_at` calls take it in place of the
/// name, so a caller resolves each port once instead of on every push. [`Status::NOT_FOUND`] for
/// an unknown name.
///
/// # Safety
/// `port` must be valid for `port_len` bytes; `index` must be valid for a write of a `u32`.
#[unsafe(no_mangle)]
pub unsafe extern "C" fn daedalus_surface_graph_input_index(
    graph: Handle,
    port: *const u8,
    port_len: usize,
    index: *mut u32,
) -> Status {
    // Safety: forwarded.
    run(|| unsafe { port_index(graph, port, port_len, index, |graph| &graph.inputs) })
}

/// Writes the index of output port `port` to `*index`: its position in the `outputs` array of
/// [`daedalus_surface_graph_ports`]; as [`daedalus_surface_graph_input_index`].
///
/// # Safety
/// As [`daedalus_surface_graph_input_index`].
#[unsafe(no_mangle)]
pub unsafe extern "C" fn daedalus_surface_graph_output_index(
    graph: Handle,
    port: *const u8,
    port_len: usize,
    index: *mut u32,
) -> Status {
    // Safety: forwarded.
    run(|| unsafe { port_index(graph, port, port_len, index, |graph| &graph.outputs) })
}

/// # Safety
/// As [`daedalus_surface_graph_input_index`].
unsafe fn port_index(
    graph: Handle,
    port: *const u8,
    port_len: usize,
    index: *mut u32,
    ports: impl Fn(&Graph) -> &[Port],
) -> Result<(), Status> {
    // Safety: forwarded.
    let name = unsafe { ffi::text(port, port_len) }?;
    if index.is_null() {
        return Err(Status::INVALID_ARGUMENT);
    }
    let tables = lock();
    let graph = tables.graphs.get(graph)?;
    let at = position(ports(graph), name).ok_or(Status::NOT_FOUND)?;
    let at = u32::try_from(at).map_err(|_| Status::NOT_FOUND)?;
    // Safety: `index` is writable (checked above).
    unsafe { index.write(at) };
    Ok(())
}
