//! Graphs: compile a `daedalus.graph` document into a graph handle, release it, and read its
//! port table, plan explanation and last telemetry as JSON.

use daedalus::GraphDocument;
use daedalus::runtime::HostPortDescriptor;
use lemnos_abi::{Handle, MessageSink, Status};

use crate::ffi::{self, run};
use crate::state::{Graph, Kind, Port, lock};

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
        let handle = tables.graphs.insert(Graph {
            host,
            graph,
            inputs,
            outputs,
            spare,
            stash: Vec::new(),
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
        .map(|d| Port {
            name: d.name.as_str().to_string(),
            kind: Kind::of(d.type_expr.as_ref()),
            id: d.name,
        })
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
/// `kind` is `int`, `float`, `bool`, `bytes` or `other` (the ABI moves only the first four).
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
