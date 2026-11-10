//! A graph: compiled on a host, then pushed to, ticked and taken from.

use std::collections::BTreeMap;

use daedalus_backend::Backend;
use daedalus_graph::GraphDoc;
use daedalus_host::{Dir, LoadOptions, LoadedHost};
use daedalus_planner::explain::explain_plan;
use daedalus_planner::{Config, plan};
use daedalus_value::Value;

use crate::Handle;
use crate::guard::{copy_out, fail, guard};
use crate::handles::{Table, lock};
use crate::host::{HOSTS, text};
use crate::status::Status;

pub(crate) struct GraphEntry {
    host: Handle,
    pub(crate) loaded: LoadedHost,
    explain: String,
    /// A value taken for a caller whose buffer was too small, by port index. The next take of
    /// that port gets it.
    pub(crate) waiting: BTreeMap<usize, Value>,
    pub(crate) telemetry: Telemetry,
}

pub(crate) static GRAPHS: Table<GraphEntry> = Table::new(2);

/// One host port of a graph. `name` and `type_key` point into the graph and are valid until
/// it is released. Neither has a NUL after it.
#[repr(C)]
#[derive(Clone, Copy, Debug)]
pub struct PortInfo {
    pub name: *const u8,
    pub name_len: usize,
    pub type_key: *const u8,
    pub type_key_len: usize,
    /// 1 for a port the caller pushes to, 0 for one it takes from.
    pub is_input: u8,
}

/// What a graph has done since it was compiled.
#[repr(C)]
#[derive(Clone, Copy, Debug, Default, PartialEq, Eq)]
pub struct Telemetry {
    pub ticks: u64,
    /// Ticks that found nothing to do.
    pub idle_ticks: u64,
    pub step_calls: u64,
    pub faults: u64,
    pub pushes: u64,
    /// Takes that found a value.
    pub takes: u64,
}

/// What one tick did.
#[repr(C)]
#[derive(Clone, Copy, Debug, Default, PartialEq, Eq)]
pub struct TickInfo {
    /// 1 when the tick found nothing to do.
    pub idle: u8,
    pub step_calls: u32,
    /// Steps that failed. The graph keeps running, and their outputs are missing.
    pub faults: u32,
}

fn compile(host: Handle, json: &str) -> Result<Handle, Status> {
    let entry = HOSTS.get(host)?;
    let mut entry = lock(&entry);
    let doc = GraphDoc::parse(json).map_err(|error| fail(Status::GRAPH_INVALID, error))?;
    let backends: Vec<&dyn Backend> = entry.backends.iter().map(|backend| &**backend).collect();
    let planned = plan(&doc.graph, &entry.registry, &backends, &Config::default())
        .map_err(|error| fail(Status::PLAN_FAILED, error))?;
    let explain = serde_json::to_string(&explain_plan(&planned))
        .map_err(|error| fail(Status::PLAN_FAILED, error))?;
    let loaded = LoadedHost::load(planned, LoadOptions::default())
        .map_err(|error| fail(Status::LOAD_FAILED, error))?;
    entry.graphs += 1;
    Ok(GRAPHS.insert(GraphEntry {
        host,
        loaded,
        explain,
        waiting: BTreeMap::new(),
        telemetry: Telemetry::default(),
    }))
}

/// Compiles a graph, given as JSON text in the graph format, on `host`, and writes its handle
/// to `out`. The graph is proved against the host's nodes before anything runs, and a graph
/// that can't be planned says why through `daedalus_last_error`.
///
/// # Safety
/// `json` points to `len` bytes of UTF-8, and `out` points to a `daedalus_handle_t`.
// SAFETY: the name is this library\'s own, with its prefix: no other symbol has it.
#[unsafe(no_mangle)]
pub unsafe extern "C" fn daedalus_graph_compile(
    host: Handle,
    json: *const u8,
    len: usize,
    out: *mut Handle,
) -> Status {
    guard(|| {
        if out.is_null() {
            return Status::NULL_POINTER;
        }
        // SAFETY: the caller's contract is this function's.
        let json = match unsafe { text(json, len) } {
            Ok(json) => json,
            Err(status) => return status,
        };
        match compile(host, json) {
            Ok(graph) => {
                // SAFETY: not null, and the caller says it points to a handle.
                unsafe { out.write(graph) };
                Status::OK
            }
            Err(status) => status,
        }
    })
}

/// Releases a graph. Its host can be released once its last graph is.
// SAFETY: the name is this library\'s own, with its prefix: no other symbol has it.
#[unsafe(no_mangle)]
pub extern "C" fn daedalus_graph_release(graph: Handle) -> Status {
    guard(|| {
        let entry = match GRAPHS.remove(graph, |_| None) {
            Ok(entry) => entry,
            Err(status) => return status,
        };
        let host = lock(&entry).host;
        if let Ok(host) = HOSTS.get(host) {
            let mut host = lock(&host);
            host.graphs = host.graphs.saturating_sub(1);
        }
        Status::OK
    })
}

/// Writes how many host ports the graph has to `count`. A port's index is its place in the
/// ports sorted by name, and it doesn't change while the graph lives.
///
/// # Safety
/// `count` points to a `size_t`.
// SAFETY: the name is this library\'s own, with its prefix: no other symbol has it.
#[unsafe(no_mangle)]
pub unsafe extern "C" fn daedalus_graph_port_count(graph: Handle, count: *mut usize) -> Status {
    guard(|| {
        if count.is_null() {
            return Status::NULL_POINTER;
        }
        let entry = match GRAPHS.get(graph) {
            Ok(entry) => entry,
            Err(status) => return status,
        };
        let ports = lock(&entry).loaded.host().ports().len();
        // SAFETY: not null, and the caller says it points to a `size_t`.
        unsafe { count.write(ports) };
        Status::OK
    })
}

/// Describes the host port at `index`.
///
/// # Safety
/// `out` points to a `daedalus_port_t`.
// SAFETY: the name is this library\'s own, with its prefix: no other symbol has it.
#[unsafe(no_mangle)]
pub unsafe extern "C" fn daedalus_graph_port(
    graph: Handle,
    index: usize,
    out: *mut PortInfo,
) -> Status {
    guard(|| {
        if out.is_null() {
            return Status::NULL_POINTER;
        }
        let entry = match GRAPHS.get(graph) {
            Ok(entry) => entry,
            Err(status) => return status,
        };
        let mut entry = lock(&entry);
        let Some(spec) = entry.loaded.host().ports().at(index) else {
            return Status::UNKNOWN_PORT;
        };
        let key = spec.key.as_str();
        // SAFETY: not null, and the caller says it points to a port. The strings are the
        // loaded graph's own, which live until it is released.
        unsafe {
            out.write(PortInfo {
                name: spec.name.as_ptr(),
                name_len: spec.name.len(),
                type_key: key.as_ptr(),
                type_key_len: key.len(),
                is_input: u8::from(spec.dir == Dir::Input),
            });
        }
        Status::OK
    })
}

/// Writes the index of the host port called `name` to `index`.
///
/// # Safety
/// `name` points to `len` bytes of UTF-8, and `index` points to a `size_t`.
// SAFETY: the name is this library\'s own, with its prefix: no other symbol has it.
#[unsafe(no_mangle)]
pub unsafe extern "C" fn daedalus_graph_port_index(
    graph: Handle,
    name: *const u8,
    len: usize,
    index: *mut usize,
) -> Status {
    guard(|| {
        if index.is_null() {
            return Status::NULL_POINTER;
        }
        // SAFETY: the caller's contract is this function's.
        let name = match unsafe { text(name, len) } {
            Ok(name) => name,
            Err(status) => return status,
        };
        let entry = match GRAPHS.get(graph) {
            Ok(entry) => entry,
            Err(status) => return status,
        };
        match lock(&entry).loaded.host().ports().index_of(name) {
            Some(at) => {
                // SAFETY: not null, and the caller says it points to a `size_t`.
                unsafe { index.write(at) };
                Status::OK
            }
            None => Status::UNKNOWN_PORT,
        }
    })
}

/// How the graph was planned, as JSON text with no NUL: where each node runs and why, and what
/// was put between them. Call with a null buffer for the size.
///
/// # Safety
/// `buf` is null or points to `cap` writable bytes, and `len` points to a `size_t`.
// SAFETY: the name is this library\'s own, with its prefix: no other symbol has it.
#[unsafe(no_mangle)]
pub unsafe extern "C" fn daedalus_graph_explain(
    graph: Handle,
    buf: *mut u8,
    cap: usize,
    len: *mut usize,
) -> Status {
    guard(|| {
        let entry = match GRAPHS.get(graph) {
            Ok(entry) => entry,
            Err(status) => return status,
        };
        let entry = lock(&entry);
        // SAFETY: the caller's contract is this function's.
        unsafe { copy_out(entry.explain.as_bytes(), buf, cap, len) }
    })
}

/// Writes what the graph has done since it was compiled to `out`.
///
/// # Safety
/// `out` points to a `daedalus_telemetry_t`.
// SAFETY: the name is this library\'s own, with its prefix: no other symbol has it.
#[unsafe(no_mangle)]
pub unsafe extern "C" fn daedalus_graph_telemetry(graph: Handle, out: *mut Telemetry) -> Status {
    guard(|| {
        if out.is_null() {
            return Status::NULL_POINTER;
        }
        let entry = match GRAPHS.get(graph) {
            Ok(entry) => entry,
            Err(status) => return status,
        };
        let telemetry = lock(&entry).telemetry;
        // SAFETY: not null, and the caller says it points to a telemetry struct.
        unsafe { out.write(telemetry) };
        Status::OK
    })
}

/// Runs one tick: every node with new input runs once. `out` may be null.
///
/// # Safety
/// `out` is null or points to a `daedalus_tick_t`.
// SAFETY: the name is this library\'s own, with its prefix: no other symbol has it.
#[unsafe(no_mangle)]
pub unsafe extern "C" fn daedalus_tick(graph: Handle, out: *mut TickInfo) -> Status {
    guard(|| {
        let entry = match GRAPHS.get(graph) {
            Ok(entry) => entry,
            Err(status) => return status,
        };
        let mut entry = lock(&entry);
        let report = match entry.loaded.step() {
            Ok(report) => report,
            Err(error) => return fail(Status::RUN_FAILED, error),
        };
        entry.telemetry.ticks += 1;
        entry.telemetry.idle_ticks += u64::from(report.idle);
        entry.telemetry.step_calls += u64::from(report.step_calls);
        entry.telemetry.faults += u64::from(report.faults);
        if !out.is_null() {
            // SAFETY: not null, and the caller says it points to a tick struct.
            unsafe {
                out.write(TickInfo {
                    idle: u8::from(report.idle),
                    step_calls: report.step_calls,
                    faults: report.faults,
                });
            }
        }
        Status::OK
    })
}
