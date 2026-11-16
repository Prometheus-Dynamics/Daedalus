//! The calls that store, plan, start and run graphs, and read what they did. The ones that
//! answer with text write it through the caller's buffer, with the size-query convention.

use daedalus_wire::service::{InspectTarget, StartConfig};

use crate::guard::{copy_or_note, guard, json_bytes, non_null, read_text, write_out};
use crate::handles::{Handle, lock};
use crate::link::{link, refuse};
use crate::shapes::{
    Carry, Config, PlanOptions, Step, Stopped, plan_options, start_config, stop_mode,
};
use crate::status::Status;

/// Stores a graph document under `name`. Its lints come back from `daedalus_client_plan`, which
/// plans it.
///
/// # Safety
/// `name` and `json` point to as many bytes of UTF-8 as their lengths say, or are null with zero.
// SAFETY: the name is this library's own, with its prefix: no other symbol has it.
#[unsafe(no_mangle)]
pub unsafe extern "C" fn daedalus_client_put_graph(
    client: Handle,
    name: *const u8,
    name_len: usize,
    json: *const u8,
    json_len: usize,
) -> Status {
    guard(|| {
        let link = link(client)?;
        // SAFETY: the caller's contract is this function's.
        let name = unsafe { read_text(name, name_len) }?;
        // SAFETY: the caller's contract is this function's.
        let json = unsafe { read_text(json, json_len) }?;
        link.client.put_graph(name, json).map_err(refuse)?;
        Ok(())
    })
}

/// Applies a patch document to a stored graph.
///
/// # Safety
/// As [`daedalus_client_put_graph`].
// SAFETY: the name is this library's own, with its prefix: no other symbol has it.
#[unsafe(no_mangle)]
pub unsafe extern "C" fn daedalus_client_patch_graph(
    client: Handle,
    graph: *const u8,
    graph_len: usize,
    patch: *const u8,
    patch_len: usize,
) -> Status {
    guard(|| {
        let link = link(client)?;
        // SAFETY: the caller's contract is this function's.
        let graph = unsafe { read_text(graph, graph_len) }?;
        // SAFETY: the caller's contract is this function's.
        let patch = unsafe { read_text(patch, patch_len) }?;
        link.client.patch_graph(graph, patch).map_err(refuse)?;
        Ok(())
    })
}

/// Loads the plugin at `path` into the server. Its nodes and backends are listed by
/// `daedalus_client_inspect_host`.
///
/// # Safety
/// As [`daedalus_client_put_graph`], for `path`.
// SAFETY: the name is this library's own, with its prefix: no other symbol has it.
#[unsafe(no_mangle)]
pub unsafe extern "C" fn daedalus_client_load_plugin(
    client: Handle,
    path: *const u8,
    len: usize,
) -> Status {
    guard(|| {
        let link = link(client)?;
        // SAFETY: the caller's contract is this function's.
        let path = unsafe { read_text(path, len) }?;
        link.client.load_plugin(path).map_err(refuse)?;
        Ok(())
    })
}

/// Plans a stored graph and writes the plan as JSON, with the lints the planner found. A plan
/// asked for again is planned again: a call with a buffer too small may be repeated.
///
/// # Safety
/// `graph` as [`daedalus_client_put_graph`]. `options` points to a `daedalus_client_plan_options_t`.
/// `buf` is null or points to `cap` writable bytes, and `len` points to a `size_t`.
// SAFETY: the name is this library's own, with its prefix: no other symbol has it.
#[unsafe(no_mangle)]
pub unsafe extern "C" fn daedalus_client_plan(
    client: Handle,
    graph: *const u8,
    graph_len: usize,
    options: *const PlanOptions,
    buf: *mut u8,
    cap: usize,
    len: *mut usize,
) -> Status {
    guard(|| {
        non_null(len, "the length")?;
        let link = link(client)?;
        // SAFETY: the caller's contract is this function's.
        let graph = unsafe { read_text(graph, graph_len) }?;
        // SAFETY: the caller's contract is this function's, for the options and their list.
        let (form, inspect, priority) = unsafe { plan_options(options) }?;
        let summary = link
            .client
            .plan(
                graph,
                daedalus_wire::service::PlanOptions {
                    form,
                    inspect,
                    priority,
                },
            )
            .map_err(refuse)?;
        let text = json_bytes(&daedalus_client::json::plan(&summary));
        // SAFETY: the caller's contract is this function's, and `len` was checked above.
        unsafe { copy_or_note(&text, buf, cap, len) }
    })
}

/// Plans a stored graph into a made plan: writes its hash to `hash` and its bytes to the buffer.
/// A made plan is what a host loads with no planner. Asking again plans again.
///
/// # Safety
/// `graph` as [`daedalus_client_put_graph`]. `config` points to a `daedalus_client_config_t`.
/// `hash` points to a `uint64_t`. `buf` and `len` are as [`daedalus_client_plan`].
#[allow(clippy::too_many_arguments)]
// One argument for each field of the reply and the buffer.
// SAFETY: the name is this library's own, with its prefix: no other symbol has it.
#[unsafe(no_mangle)]
pub unsafe extern "C" fn daedalus_client_make_plan(
    client: Handle,
    graph: *const u8,
    graph_len: usize,
    config: *const Config,
    hash: *mut u64,
    buf: *mut u8,
    cap: usize,
    len: *mut usize,
) -> Status {
    guard(|| {
        non_null(hash, "the output hash")?;
        non_null(len, "the length")?;
        let link = link(client)?;
        // SAFETY: the caller's contract is this function's.
        let graph = unsafe { read_text(graph, graph_len) }?;
        // SAFETY: the caller's contract is this function's, for the config and its list.
        let config = unsafe { start_config(config) }?;
        let (found, bytes) = link.client.make_plan(graph, config).map_err(refuse)?;
        // SAFETY: the caller's contract is this function's, and `hash` was checked above.
        unsafe { write_out(hash, found) }?;
        // SAFETY: the caller's contract is this function's, and `len` was checked above.
        unsafe { copy_or_note(&bytes, buf, cap, len) }
    })
}

/// Stores a made plan under `name`. A run started on that name uses it.
///
/// # Safety
/// `name` as [`daedalus_client_put_graph`]. `bytes` is null only when `len` is zero, else points
/// to `len` readable bytes.
// SAFETY: the name is this library's own, with its prefix: no other symbol has it.
#[unsafe(no_mangle)]
pub unsafe extern "C" fn daedalus_client_put_plan(
    client: Handle,
    name: *const u8,
    name_len: usize,
    bytes: *const u8,
    len: usize,
) -> Status {
    guard(|| {
        let link = link(client)?;
        // SAFETY: the caller's contract is this function's.
        let name = unsafe { read_text(name, name_len) }?;
        // SAFETY: the caller's contract is this function's.
        let bytes = unsafe { crate::guard::read_bytes(bytes, len) }?;
        link.client.put_plan(name, bytes).map_err(refuse)?;
        Ok(())
    })
}

/// Plans a stored graph and starts a run of it. Writes the run's number to `run`. `name` may
/// also name a stored made plan.
///
/// # Safety
/// `graph` as [`daedalus_client_put_graph`]. `config` points to a `daedalus_client_config_t`.
/// `run` points to a `uint64_t`.
// SAFETY: the name is this library's own, with its prefix: no other symbol has it.
#[unsafe(no_mangle)]
pub unsafe extern "C" fn daedalus_client_start(
    client: Handle,
    graph: *const u8,
    graph_len: usize,
    config: *const Config,
    run: *mut u64,
) -> Status {
    guard(|| {
        non_null(run, "the output run")?;
        let link = link(client)?;
        // SAFETY: the caller's contract is this function's.
        let graph = unsafe { read_text(graph, graph_len) }?;
        // SAFETY: the caller's contract is this function's, for the config and its list.
        let config: StartConfig = unsafe { start_config(config) }?;
        let started = link.client.start(graph, config).map_err(refuse)?;
        // SAFETY: the caller's contract is this function's, and `run` was checked above.
        unsafe { write_out(run, started) }
    })
}

/// Runs `count` ticks of a run and writes what they did to `out`, which may be null.
///
/// # Safety
/// `out` is null or points to a `daedalus_client_step_t`.
// SAFETY: the name is this library's own, with its prefix: no other symbol has it.
#[unsafe(no_mangle)]
pub unsafe extern "C" fn daedalus_client_step(
    client: Handle,
    run: u64,
    count: u32,
    out: *mut Step,
) -> Status {
    guard(|| {
        let link = link(client)?;
        let summary = link.client.step(run, count).map_err(refuse)?;
        if out.is_null() {
            return Ok(());
        }
        let step = Step {
            tick: summary.tick,
            step_calls: summary.step_calls,
            faults: summary.faults,
            skipped: summary.skipped,
            ticks: summary.ticks,
            idle: u8::from(summary.idle),
        };
        // SAFETY: the caller's contract is this function's, and `out` is not null.
        unsafe { write_out(out, step) }
    })
}

/// Cancels a run: its nodes that look see it, and the run stays until it is stopped.
// SAFETY: the name is this library's own, with its prefix: no other symbol has it.
#[unsafe(no_mangle)]
pub extern "C" fn daedalus_client_cancel(client: Handle, run: u64) -> Status {
    guard(|| {
        let link = link(client)?;
        link.client.cancel(run).map_err(refuse)?;
        Ok(())
    })
}

/// Stops a run: `mode` is `DAEDALUS_CLIENT_STOP_DRAIN` or `DAEDALUS_CLIENT_STOP_IMMEDIATE`. Writes
/// how it ended to `out`, which may be null.
///
/// # Safety
/// `out` is null or points to a `daedalus_client_stopped_t`.
// SAFETY: the name is this library's own, with its prefix: no other symbol has it.
#[unsafe(no_mangle)]
pub unsafe extern "C" fn daedalus_client_stop(
    client: Handle,
    run: u64,
    mode: u32,
    out: *mut Stopped,
) -> Status {
    guard(|| {
        let mode = stop_mode(mode)?;
        let link = link(client)?;
        let stopped = link.client.stop(run, mode).map_err(refuse)?;
        if out.is_null() {
            return Ok(());
        }
        let result = Stopped {
            tick: stopped.tick,
            dropped: stopped.dropped,
        };
        // SAFETY: the caller's contract is this function's, and `out` is not null.
        unsafe { write_out(out, result) }
    })
}

/// Replaces a run's graph with a stored one, and writes how many nodes' state was carried,
/// started fresh, added and removed, to `out`, which may be null.
///
/// # Safety
/// `graph` as [`daedalus_client_put_graph`]. `out` is null or points to a `daedalus_client_carry_t`.
// SAFETY: the name is this library's own, with its prefix: no other symbol has it.
#[unsafe(no_mangle)]
pub unsafe extern "C" fn daedalus_client_reload(
    client: Handle,
    run: u64,
    graph: *const u8,
    graph_len: usize,
    out: *mut Carry,
) -> Status {
    guard(|| {
        let link = link(client)?;
        // SAFETY: the caller's contract is this function's.
        let graph = unsafe { read_text(graph, graph_len) }?;
        let report = link.client.reload(run, graph).map_err(refuse)?;
        if out.is_null() {
            return Ok(());
        }
        let carry = Carry {
            carried: report.carried.total,
            fresh: report.fresh.total,
            added: report.added.total,
            removed: report.removed.total,
            removed_ports: report.removed_ports.total,
            dropped_values: report.dropped_values,
        };
        // SAFETY: the caller's contract is this function's, and `out` is not null.
        unsafe { write_out(out, carry) }
    })
}

/// Writes an inspect's JSON to the buffer, with the size-query convention.
fn inspect(
    client: Handle,
    target: InspectTarget,
    buf: *mut u8,
    cap: usize,
    len: *mut usize,
) -> Result<(), Status> {
    non_null(len, "the length")?;
    let link = link(client)?;
    let found = link.client.inspect(target).map_err(refuse)?;
    let text = json_bytes(&daedalus_client::json::inspection(&found));
    // SAFETY: the caller's contract is the surface's, and `len` was checked above.
    unsafe { copy_or_note(&text, buf, cap, len) }
}

/// Inspects a run as JSON: its state, ports and nodes.
///
/// # Safety
/// `buf` is null or points to `cap` writable bytes, and `len` points to a `size_t`.
// SAFETY: the name is this library's own, with its prefix: no other symbol has it.
#[unsafe(no_mangle)]
pub unsafe extern "C" fn daedalus_client_inspect_run(
    client: Handle,
    run: u64,
    buf: *mut u8,
    cap: usize,
    len: *mut usize,
) -> Status {
    guard(|| inspect(client, InspectTarget::Run(run), buf, cap, len))
}

/// Inspects a stored graph as JSON: its name and its document.
///
/// # Safety
/// `name` as [`daedalus_client_put_graph`]. `buf` and `len` as [`daedalus_client_inspect_run`].
// SAFETY: the name is this library's own, with its prefix: no other symbol has it.
#[unsafe(no_mangle)]
pub unsafe extern "C" fn daedalus_client_inspect_graph(
    client: Handle,
    name: *const u8,
    name_len: usize,
    buf: *mut u8,
    cap: usize,
    len: *mut usize,
) -> Status {
    guard(|| {
        // SAFETY: the caller's contract is this function's.
        let name = unsafe { read_text(name, name_len) }?.to_owned();
        inspect(client, InspectTarget::Graph(name), buf, cap, len)
    })
}

/// Inspects the plan cache as JSON: its entries, hits and misses.
///
/// # Safety
/// `buf` and `len` as [`daedalus_client_inspect_run`].
// SAFETY: the name is this library's own, with its prefix: no other symbol has it.
#[unsafe(no_mangle)]
pub unsafe extern "C" fn daedalus_client_inspect_cache(
    client: Handle,
    buf: *mut u8,
    cap: usize,
    len: *mut usize,
) -> Status {
    guard(|| inspect(client, InspectTarget::Cache, buf, cap, len))
}

/// Inspects a run's telemetry as JSON: one row per step below the detailed level's, none above.
///
/// # Safety
/// `buf` and `len` as [`daedalus_client_inspect_run`].
// SAFETY: the name is this library's own, with its prefix: no other symbol has it.
#[unsafe(no_mangle)]
pub unsafe extern "C" fn daedalus_client_inspect_telemetry(
    client: Handle,
    run: u64,
    buf: *mut u8,
    cap: usize,
    len: *mut usize,
) -> Status {
    guard(|| inspect(client, InspectTarget::Telemetry(run), buf, cap, len))
}

/// Inspects the host as JSON: its sessions, graphs, runs and plugins.
///
/// # Safety
/// `buf` and `len` as [`daedalus_client_inspect_run`].
// SAFETY: the name is this library's own, with its prefix: no other symbol has it.
#[unsafe(no_mangle)]
pub unsafe extern "C" fn daedalus_client_inspect_host(
    client: Handle,
    buf: *mut u8,
    cap: usize,
    len: *mut usize,
) -> Status {
    guard(|| inspect(client, InspectTarget::Host, buf, cap, len))
}

/// Empties the server's plan cache.
// SAFETY: the name is this library's own, with its prefix: no other symbol has it.
#[unsafe(no_mangle)]
pub extern "C" fn daedalus_client_cache_clear(client: Handle) -> Status {
    guard(|| {
        let link = link(client)?;
        link.client.cache_clear().map_err(refuse)?;
        Ok(())
    })
}

/// The newest `limit` requests the server served, oldest first, as JSON. Zero is all it keeps. A
/// trace too big for the buffer is kept for the retry with the same limit, since the trace grows
/// with every request served, the retry's own included.
///
/// # Safety
/// `buf` and `len` as [`daedalus_client_inspect_run`].
// SAFETY: the name is this library's own, with its prefix: no other symbol has it.
#[unsafe(no_mangle)]
pub unsafe extern "C" fn daedalus_client_trace(
    client: Handle,
    limit: u32,
    buf: *mut u8,
    cap: usize,
    len: *mut usize,
) -> Status {
    guard(|| {
        non_null(len, "the length")?;
        let link = link(client)?;
        let kept = match lock(&link.trace).take() {
            Some((asked, text)) if asked == limit => Some(text),
            _ => None,
        };
        let text = match kept {
            Some(text) => text,
            None => {
                let entries = link.client.trace(limit).map_err(refuse)?;
                json_bytes(&daedalus_client::json::trace(&entries))
            }
        };
        // SAFETY: the caller's contract is this function's, and `len` was checked above.
        match unsafe { copy_or_note(&text, buf, cap, len) } {
            Ok(()) => Ok(()),
            Err(status) => {
                *lock(&link.trace) = Some((limit, text));
                Err(status)
            }
        }
    })
}
