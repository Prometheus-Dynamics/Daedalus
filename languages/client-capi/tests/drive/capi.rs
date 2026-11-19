//! The reference driver's `Service` over this crate's C ABI: each method makes the calls a C caller
//! makes, and reads their structs, JSON and wire bytes back into the protocol's values. The
//! commands, their parsing and the answer lines are the reference driver's.

use std::cell::Cell;

use daedalus_client::{Fed, Stopped};
use daedalus_client_capi::{self as c, Handle, Status};
use daedalus_drive_conformance::reference::{Events, Refused, Service};
use daedalus_wire::Schema;
use daedalus_wire::codec::{Reader, Writer};
use daedalus_wire::service::{
    CarryReport, Diagnostic, ErrorCode, Event, EventKinds, InspectTarget, Inspection, PlanForm,
    PlanOptions, PlanSummary, PluginInfo, StartConfig, StepSummary, StopMode, TelemetryLevel,
    TraceEntry, TypedValue, WatchTarget,
};
use serde_json::Value;

use crate::json;

/// The first buffer a call is given. A call the daemon serves is made once, as the C++ wrapper
/// makes it: only a result larger than this makes a second call, since a second call repeats the
/// request on the daemon and its trace records it.
const FIRST_BUFFER: usize = 256 * 1024;

/// How long one poll of a watch waits before it asks again.
const POLL_MS: u32 = 200;

/// Why a call failed: the daemon's code when it refused the call, read on the failing thread.
fn refused(status: Status) -> Refused {
    if status != Status::SERVICE_ERROR {
        return Refused::Local;
    }
    let mut code = 0;
    // SAFETY: `code` is a `uint32_t` to write.
    if unsafe { c::daedalus_client_last_error_code(&mut code) } != Status::OK {
        return Refused::Local;
    }
    Refused::Code(ErrorCode::from_code(
        u16::try_from(code).unwrap_or(u16::MAX),
    ))
}

fn check(status: Status) -> Result<(), Refused> {
    if status == Status::OK {
        Ok(())
    } else {
        Err(refused(status))
    }
}

/// What a call writes into a buffer the caller gives, read until it fits.
fn sized(mut call: impl FnMut(*mut u8, usize, *mut usize) -> Status) -> Result<Vec<u8>, Refused> {
    let mut buf = vec![0; FIRST_BUFFER];
    let mut len = 0;
    let mut status = call(buf.as_mut_ptr(), buf.len(), &mut len);
    while status == Status::BUFFER_TOO_SMALL {
        buf = vec![0; len];
        status = call(buf.as_mut_ptr(), buf.len(), &mut len);
    }
    check(status)?;
    buf.truncate(len);
    Ok(buf)
}

fn json_of(call: impl FnMut(*mut u8, usize, *mut usize) -> Status) -> Result<Value, Refused> {
    serde_json::from_slice(&sized(call)?).map_err(|_| Refused::Local)
}

/// A value read from the surface's JSON, or `error local` when it isn't there.
fn read<T>(found: Option<T>) -> Result<T, Refused> {
    found.ok_or(Refused::Local)
}

/// The names as the surface's texts. They borrow `names`, which outlives the call.
fn texts(names: &[String]) -> Vec<c::Text> {
    names
        .iter()
        .map(|name| c::Text {
            data: name.as_ptr(),
            len: name.len(),
        })
        .collect()
}

/// A list as the surface takes it: null with a zero count when it is empty.
fn list(texts: &[c::Text]) -> *const c::Text {
    if texts.is_empty() {
        std::ptr::null()
    } else {
        texts.as_ptr()
    }
}

fn config(config: &StartConfig, priority: &[c::Text]) -> c::Config {
    c::Config {
        fifo_capacity: config.fifo_capacity,
        fault_reports: config.fault_reports,
        telemetry: match config.telemetry {
            TelemetryLevel::Off => c::TELEMETRY_OFF,
            TelemetryLevel::Basic => c::TELEMETRY_BASIC,
            TelemetryLevel::Detailed => c::TELEMETRY_DETAILED,
            TelemetryLevel::Trace => c::TELEMETRY_TRACE,
        },
        unmerged: u8::from(config.unmerged),
        priority: list(priority),
        priority_len: priority.len(),
    }
}

/// A client handle: the connection, in one session. A clone is the same handle.
#[derive(Clone)]
pub struct Session {
    handle: Handle,
}

impl Session {
    /// Connects to `endpoint` (`tcp:127.0.0.1:PORT` or `unix:PATH`), with reconnect off.
    pub fn connect(endpoint: &str) -> Result<Session, String> {
        let mut handle = 0;
        // SAFETY: the endpoint is `len` bytes, and `handle` is a handle to write.
        let status = unsafe {
            c::daedalus_client_connect(endpoint.as_ptr(), endpoint.len(), 0, &mut handle)
        };
        if status != Status::OK {
            return Err(format!("connect {endpoint}: {status:?}"));
        }
        Ok(Session { handle })
    }

    /// The JSON the last report form kept on this handle.
    fn report(&self) -> Result<Value, Refused> {
        // SAFETY: the buffer is `cap` bytes, and `len` is a `usize` to write.
        json_of(|buf, cap, len| unsafe { c::daedalus_client_report(self.handle, buf, cap, len) })
    }
}

impl Service for Session {
    type Watch = Watch;

    fn in_session(&self, name: &str) -> Result<Session, Refused> {
        let mut handle = 0;
        // SAFETY: the name is `len` bytes, and `handle` is a handle to write.
        check(unsafe {
            c::daedalus_client_in_session(self.handle, name.as_ptr(), name.len(), &mut handle)
        })?;
        Ok(Session { handle })
    }

    fn ping(&self) -> Result<u32, Refused> {
        let mut version = 0;
        // SAFETY: `version` is a `uint32_t` to write.
        check(unsafe { c::daedalus_client_ping(self.handle, &mut version) })?;
        Ok(version)
    }

    fn load_plugin(&self, path: &str) -> Result<PluginInfo, Refused> {
        // SAFETY: the path is `len` bytes.
        check(unsafe {
            c::daedalus_client_load_plugin_report(self.handle, path.as_ptr(), path.len())
        })?;
        read(json::plugin(&self.report()?))
    }

    fn put_graph(&self, name: &str, text: &str) -> Result<Vec<Diagnostic>, Refused> {
        // SAFETY: the name and the document are each their length in bytes.
        check(unsafe {
            c::daedalus_client_put_graph_report(
                self.handle,
                name.as_ptr(),
                name.len(),
                text.as_ptr(),
                text.len(),
            )
        })?;
        read(json::diagnostics(&self.report()?))
    }

    fn patch_graph(&self, name: &str, patch: &str) -> Result<Vec<Diagnostic>, Refused> {
        // SAFETY: the name and the patch are each their length in bytes.
        check(unsafe {
            c::daedalus_client_patch_graph_report(
                self.handle,
                name.as_ptr(),
                name.len(),
                patch.as_ptr(),
                patch.len(),
            )
        })?;
        read(json::diagnostics(&self.report()?))
    }

    fn plan(&self, graph: &str, options: PlanOptions) -> Result<PlanSummary, Refused> {
        let priority = texts(&options.priority);
        let raw = c::PlanOptions {
            form: match options.form {
                PlanForm::Steps => c::FORM_STEPS,
                PlanForm::Blob => c::FORM_BLOB,
                PlanForm::Source => c::FORM_SOURCE,
            },
            inspect: u8::from(options.inspect),
            priority: list(&priority),
            priority_len: priority.len(),
        };
        let found = json_of(|buf, cap, len| {
            // SAFETY: the graph is its length in bytes, the options and their names live through
            // the call, and the buffer is `cap` bytes.
            unsafe {
                c::daedalus_client_plan_raw(
                    self.handle,
                    graph.as_ptr(),
                    graph.len(),
                    &raw,
                    buf,
                    cap,
                    len,
                )
            }
        })?;
        read(json::plan(&found))
    }

    fn make_plan(&self, graph: &str, start: StartConfig) -> Result<(u64, Vec<u8>), Refused> {
        let priority = texts(&start.priority);
        let raw = config(&start, &priority);
        let mut hash = 0;
        let bytes = sized(|buf, cap, len| {
            // SAFETY: the graph is its length in bytes, the config and its names live through the
            // call, `hash` is a `uint64_t` to write, and the buffer is `cap` bytes.
            unsafe {
                c::daedalus_client_make_plan(
                    self.handle,
                    graph.as_ptr(),
                    graph.len(),
                    &raw,
                    &mut hash,
                    buf,
                    cap,
                    len,
                )
            }
        })?;
        Ok((hash, bytes))
    }

    fn put_plan(&self, name: &str, bytes: &[u8]) -> Result<(), Refused> {
        // SAFETY: the name and the bytes are each their length.
        check(unsafe {
            c::daedalus_client_put_plan(
                self.handle,
                name.as_ptr(),
                name.len(),
                bytes.as_ptr(),
                bytes.len(),
            )
        })
    }

    fn start(&self, graph: &str, start: StartConfig) -> Result<u64, Refused> {
        let priority = texts(&start.priority);
        let raw = config(&start, &priority);
        let mut run = 0;
        // SAFETY: the graph is its length in bytes, the config and its names live through the
        // call, and `run` is a `uint64_t` to write.
        check(unsafe {
            c::daedalus_client_start(self.handle, graph.as_ptr(), graph.len(), &raw, &mut run)
        })?;
        Ok(run)
    }

    fn feed(&self, run: u64, port: &str, value: TypedValue) -> Result<Fed, Refused> {
        let mut writer = Writer::new();
        value
            .encode(&mut writer, &Schema::new())
            .map_err(|_| Refused::Local)?;
        let mut out = c::Fed::default();
        // SAFETY: the port and the encoding are each their length, and `out` is a struct to write.
        check(unsafe {
            c::daedalus_client_feed_wire(
                self.handle,
                run,
                port.as_ptr(),
                port.len(),
                writer.buf.as_ptr(),
                writer.buf.len(),
                &mut out,
            )
        })?;
        Ok(Fed {
            woke: out.woke,
            dropped: out.dropped != 0,
        })
    }

    fn step(&self, run: u64, count: u32) -> Result<StepSummary, Refused> {
        let mut out = c::Step::default();
        // SAFETY: `out` is a struct to write.
        check(unsafe { c::daedalus_client_step(self.handle, run, count, &mut out) })?;
        Ok(StepSummary {
            ticks: out.ticks,
            tick: out.tick,
            step_calls: out.step_calls,
            faults: out.faults,
            skipped: out.skipped,
            idle: out.idle != 0,
        })
    }

    fn take(&self, run: u64, port: &str) -> Result<Option<TypedValue>, Refused> {
        let mut present = 0;
        let bytes = sized(|buf, cap, len| {
            // SAFETY: the port is its length in bytes, the buffer is `cap` bytes, and `len` and
            // `present` are written.
            unsafe {
                c::daedalus_client_take_wire(
                    self.handle,
                    run,
                    port.as_ptr(),
                    port.len(),
                    buf,
                    cap,
                    len,
                    &mut present,
                )
            }
        })?;
        if present == 0 {
            return Ok(None);
        }
        let value = TypedValue::decode(&mut Reader::new(&bytes), &Schema::new())
            .map_err(|_| Refused::Local)?;
        Ok(Some(value))
    }

    fn watch(&self, target: WatchTarget, kinds: EventKinds) -> Result<Watch, Refused> {
        let run = match target {
            WatchTarget::All => c::ALL_RUNS,
            WatchTarget::Run(run) => run,
        };
        let mut handle = 0;
        // SAFETY: `handle` is a handle to write.
        check(unsafe {
            c::daedalus_client_watch_open(self.handle, run, u32::from(kinds.0), &mut handle)
        })?;
        Ok(Watch {
            handle,
            closed: Cell::new(false),
        })
    }

    fn reload(&self, run: u64, graph: &str) -> Result<CarryReport, Refused> {
        // SAFETY: the graph is its length in bytes.
        check(unsafe {
            c::daedalus_client_reload_report(self.handle, run, graph.as_ptr(), graph.len())
        })?;
        read(json::carry(&self.report()?))
    }

    fn cancel(&self, run: u64) -> Result<(), Refused> {
        check(c::daedalus_client_cancel(self.handle, run))
    }

    fn stop(&self, run: u64, mode: StopMode) -> Result<Stopped, Refused> {
        let mode = match mode {
            StopMode::Drain => c::STOP_DRAIN,
            StopMode::Immediate => c::STOP_IMMEDIATE,
        };
        let mut out = c::Stopped::default();
        // SAFETY: `out` is a struct to write.
        check(unsafe { c::daedalus_client_stop(self.handle, run, mode, &mut out) })?;
        Ok(Stopped {
            tick: out.tick,
            dropped: out.dropped,
        })
    }

    fn inspect(&self, target: InspectTarget) -> Result<Inspection, Refused> {
        let handle = self.handle;
        Ok(match target {
            InspectTarget::Run(run) => {
                Inspection::Run(read(json::run(&json_of(|buf, cap, len| {
                    // SAFETY: the buffer is `cap` bytes, and `len` is a `usize` to write.
                    unsafe { c::daedalus_client_inspect_run(handle, run, buf, cap, len) }
                })?))?)
            }
            InspectTarget::Graph(name) => {
                let text = sized(|buf, cap, len| {
                    // SAFETY: the name is its length in bytes, the buffer is `cap` bytes, and `len`
                    // is a `usize` to write.
                    unsafe {
                        c::daedalus_client_graph_text(
                            handle,
                            name.as_ptr(),
                            name.len(),
                            buf,
                            cap,
                            len,
                        )
                    }
                })?;
                let json = String::from_utf8(text).map_err(|_| Refused::Local)?;
                Inspection::Graph { name, json }
            }
            InspectTarget::Cache => {
                Inspection::Cache(read(json::cache(&json_of(|buf, cap, len| {
                    // SAFETY: the buffer is `cap` bytes, and `len` is a `usize` to write.
                    unsafe { c::daedalus_client_inspect_cache(handle, buf, cap, len) }
                })?))?)
            }
            InspectTarget::Telemetry(run) => {
                Inspection::Telemetry(read(json::telemetry(&json_of(|buf, cap, len| {
                    // SAFETY: the buffer is `cap` bytes, and `len` is a `usize` to write.
                    unsafe { c::daedalus_client_inspect_telemetry(handle, run, buf, cap, len) }
                })?))?)
            }
            InspectTarget::Host => {
                Inspection::Host(read(json::host(&json_of(|buf, cap, len| {
                    // SAFETY: the buffer is `cap` bytes, and `len` is a `usize` to write.
                    unsafe { c::daedalus_client_inspect_host(handle, buf, cap, len) }
                })?))?)
            }
        })
    }

    fn cache_clear(&self) -> Result<(), Refused> {
        check(c::daedalus_client_cache_clear(self.handle))
    }

    fn trace(&self, limit: u32) -> Result<Vec<TraceEntry>, Refused> {
        // SAFETY: the buffer is `cap` bytes, and `len` is a `usize` to write.
        let found = json_of(|buf, cap, len| unsafe {
            c::daedalus_client_trace(self.handle, limit, buf, cap, len)
        })?;
        read(json::trace(&found))
    }
}

/// A watch handle. Closing it releases the handle, so a closed watch has no more events.
pub struct Watch {
    handle: Handle,
    closed: Cell<bool>,
}

impl Events for Watch {
    fn next_event(&self) -> Option<Event> {
        if self.closed.get() {
            return None;
        }
        loop {
            let mut state = 0;
            let found = sized(|buf, cap, len| {
                // SAFETY: the buffer is `cap` bytes, and `len` and `state` are written.
                unsafe {
                    c::daedalus_client_watch_next(self.handle, POLL_MS, buf, cap, len, &mut state)
                }
            })
            .ok()?;
            match state {
                c::WATCH_IDLE => continue,
                c::WATCH_EVENT => return json::event(&serde_json::from_slice(&found).ok()?),
                _ => return None,
            }
        }
    }

    fn close(&self) {
        if !self.closed.replace(true) {
            c::daedalus_client_watch_close(self.handle);
        }
    }
}

impl Drop for Watch {
    fn drop(&mut self) {
        Events::close(self);
    }
}
