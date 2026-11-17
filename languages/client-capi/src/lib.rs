//! The client's C surface: a client to the daemon, over a unix socket or loopback TCP, that puts
//! graphs, plans and starts them, feeds and takes their ports, steps, cancels and stops runs,
//! reloads, inspects and watches. `include/daedalus_client.h` is the contract, written by hand,
//! and a test holds every function here to it.
//!
//! The surface wraps `daedalus-client`, and holds no logic of its own beyond the C shapes: the
//! protocol is the client's, and its values are the wire's. A client handle is a connection. A
//! watch handle is one subscription. Both are 64-bit numbers that name a place and a generation,
//! so a handle that was closed, or was never one, is answered with a status and never followed.
//!
//! Every call returns a [`Status`]. Results come back through pointers the caller gives, and text
//! through a buffer with the size-query convention. No call lets a panic cross into C: each runs
//! under a guard that turns one into `DAEDALUS_CLIENT_PANIC`.
//!
//! Threads: a client handle may be used from any thread, and so may a watch handle. Calls on one
//! handle run side by side, as the client's requests do. Closing a handle while another thread
//! calls on it makes that call fail with a stale-handle status. The reason for the last failed
//! call is kept per thread, as the host surface keeps it.

use daedalus_wire::service::{SERVICE_LAYOUT_HASH, SERVICE_VERSION};

mod calls;
mod codes;
mod guard;
mod handles;
mod link;
mod shapes;
mod status;
mod values;
mod watch;

pub use calls::{
    daedalus_client_cache_clear, daedalus_client_cancel, daedalus_client_graph_text,
    daedalus_client_inspect_cache, daedalus_client_inspect_graph, daedalus_client_inspect_host,
    daedalus_client_inspect_run, daedalus_client_inspect_telemetry, daedalus_client_load_plugin,
    daedalus_client_load_plugin_report, daedalus_client_make_plan, daedalus_client_patch_graph,
    daedalus_client_patch_graph_report, daedalus_client_plan, daedalus_client_plan_raw,
    daedalus_client_put_graph, daedalus_client_put_graph_report, daedalus_client_put_plan,
    daedalus_client_reload, daedalus_client_reload_report, daedalus_client_report,
    daedalus_client_start, daedalus_client_step, daedalus_client_stop, daedalus_client_trace,
};
#[doc(hidden)]
pub use codes::CONSTANTS;
pub use codes::{
    ALL_RUNS, EVENT_ALL, EVENT_FAULT, EVENT_TELEMETRY, FORM_BLOB, FORM_SOURCE, FORM_STEPS,
    INSPECT_CACHE, INSPECT_GRAPH, INSPECT_HOST, INSPECT_RUN, INSPECT_TELEMETRY, STOP_DRAIN,
    STOP_IMMEDIATE, TELEMETRY_BASIC, TELEMETRY_DETAILED, TELEMETRY_OFF, TELEMETRY_TRACE,
    WATCH_ENDED, WATCH_EVENT, WATCH_IDLE,
};
pub use guard::{
    Text, daedalus_client_last_error, daedalus_client_last_error_code,
    daedalus_client_selftest_panic,
};
pub use handles::Handle;
pub use link::{
    daedalus_client_close, daedalus_client_connect, daedalus_client_in_session,
    daedalus_client_ping,
};
pub use shapes::{Carry, Config, Fed, PlanOptions, Step, Stopped, Version};
pub use status::{Status, daedalus_client_status_name};
pub use values::{
    daedalus_client_feed_bool, daedalus_client_feed_bytes, daedalus_client_feed_f64,
    daedalus_client_feed_i64, daedalus_client_feed_str, daedalus_client_feed_wire,
    daedalus_client_take_bool, daedalus_client_take_bytes, daedalus_client_take_f64,
    daedalus_client_take_i64, daedalus_client_take_wire,
};
pub use watch::{
    daedalus_client_watch_close, daedalus_client_watch_next, daedalus_client_watch_open,
};

/// Writes the surface's version: the protocol's version, and the hash of the layout of its
/// messages. A caller built against another layout should not trust what it reads.
///
/// # Safety
/// `out` points to a `daedalus_client_version_t`.
// SAFETY: the name is this library's own, with its prefix: no other symbol has it.
#[unsafe(no_mangle)]
pub unsafe extern "C" fn daedalus_client_version(out: *mut Version) -> Status {
    guard::guard(|| {
        // SAFETY: the caller's contract is this function's.
        unsafe {
            guard::write_out(
                out,
                Version {
                    protocol: SERVICE_VERSION,
                    layout_hash: SERVICE_LAYOUT_HASH,
                },
            )
        }
    })
}

/// How many client and watch handles are live in this process. For tests, so nothing is left
/// behind.
#[doc(hidden)]
pub fn live_handles() -> (usize, usize) {
    (link::LINKS.live(), watch::WATCHES.live())
}

#[cfg(test)]
mod tests {
    use super::Status;

    #[test]
    fn the_shared_numbers_are_core_capi_numbers() {
        // The statuses this library and core/capi share, by their numbers in core/capi's header.
        let core = std::fs::read_to_string(
            std::path::Path::new(env!("CARGO_MANIFEST_DIR"))
                .join("../../core/capi/include/daedalus.h"),
        )
        .expect("core/capi's header");
        for (name, ours) in [
            ("DAEDALUS_OK", Status::OK),
            ("DAEDALUS_NULL_POINTER", Status::NULL_POINTER),
            ("DAEDALUS_BAD_HANDLE", Status::BAD_HANDLE),
            ("DAEDALUS_STALE_HANDLE", Status::STALE_HANDLE),
            ("DAEDALUS_BAD_TEXT", Status::BAD_TEXT),
            ("DAEDALUS_BUFFER_TOO_SMALL", Status::BUFFER_TOO_SMALL),
            ("DAEDALUS_PANIC", Status::PANIC),
        ] {
            let line = core
                .lines()
                .find(|line| line.starts_with(&format!("#define {name} ")))
                .unwrap_or_else(|| panic!("core/capi lacks {name}"));
            let number: i32 = line.rsplit(' ').next().unwrap().parse().unwrap();
            assert_eq!(number, ours.0, "{name}");
        }
    }
}
