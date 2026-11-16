//! Watches: the events of one run, or of every run, pulled one at a time with a timeout. An event
//! the caller's buffer can't hold waits for its next `daedalus_client_watch_next`.

use std::sync::Mutex;
use std::time::Duration;

use daedalus_client::{Watch, WatchPoll};
use daedalus_wire::service::{Event, EventKinds, WatchTarget};

use crate::codes::{ALL_RUNS, EVENT_ALL, WATCH_ENDED, WATCH_EVENT, WATCH_IDLE};
use crate::guard::{copy_or_note, fail, guard, json_bytes, non_null, write_out};
use crate::handles::{Handle, Table, lock};
use crate::link::{link, refuse};
use crate::status::Status;

/// One watch handle: the client's stream of its events, and an event waiting for a buffer.
pub(crate) struct WatchLink {
    stream: Mutex<Watch>,
    held: Mutex<Option<Event>>,
}

pub(crate) static WATCHES: Table<WatchLink> = Table::new(2);

/// Subscribes to the events of `run` (or of every run, with `DAEDALUS_CLIENT_ALL_RUNS`) that are
/// in `kinds`: `DAEDALUS_CLIENT_EVENT_FAULT`, `_TELEMETRY`, or both. Writes the watch's handle.
/// A watch ends when its run ends, when it is closed, or when the connection does.
///
/// # Safety
/// `out` points to a `daedalus_client_handle_t`.
// SAFETY: the name is this library's own, with its prefix: no other symbol has it.
#[unsafe(no_mangle)]
pub unsafe extern "C" fn daedalus_client_watch_open(
    client: Handle,
    run: u64,
    kinds: u32,
    out: *mut Handle,
) -> Status {
    guard(|| {
        non_null(out, "the output handle")?;
        if kinds > EVENT_ALL {
            return Err(fail(
                Status::BAD_ARGUMENT,
                format!("{kinds} is not a set of event kinds"),
            ));
        }
        let link = link(client)?;
        let target = match run {
            ALL_RUNS => WatchTarget::All,
            run => WatchTarget::Run(run),
        };
        let stream = link
            .client
            .watch(target, EventKinds(kinds as u8))
            .map_err(refuse)?;
        let handle = WATCHES.insert(WatchLink {
            stream: Mutex::new(stream),
            held: Mutex::new(None),
        });
        // SAFETY: the caller's contract is this function's, and `out` was checked above.
        unsafe { write_out(out, handle) }
    })
}

/// Waits up to `timeout_ms` for the next event of a watch. `state` is `DAEDALUS_CLIENT_WATCH_EVENT`
/// when an event came, with it as JSON in the buffer; `DAEDALUS_CLIENT_WATCH_IDLE` when none came
/// in time; and `DAEDALUS_CLIENT_WATCH_ENDED` when the watch has no more events, after which
/// `state` stays that. Writes the JSON's length to `len` in every case.
///
/// # Safety
/// `buf` is null or points to `cap` writable bytes. `len` points to a `size_t`, and `state` to a
/// `uint32_t`.
// SAFETY: the name is this library's own, with its prefix: no other symbol has it.
#[unsafe(no_mangle)]
pub unsafe extern "C" fn daedalus_client_watch_next(
    watch: Handle,
    timeout_ms: u32,
    buf: *mut u8,
    cap: usize,
    len: *mut usize,
    state: *mut u32,
) -> Status {
    guard(|| {
        non_null(len, "the length")?;
        non_null(state, "the state")?;
        let found = WATCHES.get(watch)?;
        let waiting = lock(&found.held).take();
        let event = match waiting {
            Some(event) => Some(event),
            None => {
                let stream = lock(&found.stream);
                match stream.next_event_timeout(Duration::from_millis(u64::from(timeout_ms))) {
                    WatchPoll::Event(event) => Some(event),
                    WatchPoll::Timeout => None,
                    WatchPoll::Ended => {
                        // SAFETY: the caller's contract is this function's, and `state` was checked above.
                        unsafe { write_out(state, WATCH_ENDED) }?;
                        // SAFETY: the caller's contract is this function's, and `len` was checked above.
                        return unsafe { write_out(len, 0) };
                    }
                }
            }
        };
        let Some(event) = event else {
            // SAFETY: the caller's contract is this function's, and both were checked above.
            unsafe { write_out(state, WATCH_IDLE) }?;
            return unsafe { write_out(len, 0) };
        };
        let text = json_bytes(&daedalus_client::json::event(&event));
        // SAFETY: the caller's contract is this function's, and both were checked above.
        unsafe { write_out(state, WATCH_EVENT) }?;
        // SAFETY: the caller's contract is this function's, and `len` was checked above.
        match unsafe { copy_or_note(&text, buf, cap, len) } {
            Ok(()) => Ok(()),
            Err(status) => {
                *lock(&found.held) = Some(event);
                Err(status)
            }
        }
    })
}

/// Ends a watch and releases its handle. The server is told the watch has ended.
// SAFETY: the name is this library's own, with its prefix: no other symbol has it.
#[unsafe(no_mangle)]
pub extern "C" fn daedalus_client_watch_close(watch: Handle) -> Status {
    guard(|| {
        WATCHES.remove(watch)?;
        Ok(())
    })
}

#[cfg(test)]
mod tests {
    use crate::handles::Table;

    #[test]
    fn a_watch_handle_is_not_a_client_handle() {
        let table: Table<u8> = Table::new(2);
        let other: Table<u8> = Table::new(1);
        let handle = table.insert(7);
        assert!(other.get(handle).is_err());
        assert_eq!(*table.get(handle).unwrap(), 7);
    }
}
