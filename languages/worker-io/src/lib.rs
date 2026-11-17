//! The worker side's I/O: the stream a worker's packets travel on.
//!
//! A worker speaks the wire protocol on its stdin and stdout. Anything a node body prints to
//! stdout would land in that stream and corrupt it, so the worker takes its packets from a copy of
//! stdout, and points stdout at stderr. [`packet_stream`] does both, once, and every worker that
//! serves a protocol on stdio calls it before its first packet.
//!
//! Every worker SDK that serves stdio shares the rest of the transport: [`transport`] reads the
//! host's packets in order on a reader thread, asks for shared memory, carries the cancel flag,
//! and hands over a run that the host lent memory for. [`run`] begins a run's descriptor reads and
//! places large outputs in lent memory. The Rust and C worker SDKs call these, so a value by
//! reference travels the same way in both.
//!
//! This is the only crate in the worker SDKs that uses `unsafe`, and only for the descriptor
//! calls below. Each of them is a `libc` call on the process's own standard streams.

use std::fs::File;
use std::io;
use std::sync::atomic::{AtomicBool, Ordering};

pub mod run;
pub mod transport;

pub use run::{fd_slots, place_outputs, with_run};
pub use transport::{Arrival, FEATURES, Transport};

static TAKEN: AtomicBool = AtomicBool::new(false);

/// The worker's packet stream: a copy of stdout. After this call stdout is stderr, so a print in a
/// node cannot corrupt the stream.
///
/// Call it once per process, before the first packet. A second call is an error, because stdout is
/// already stderr by then and a copy of it would carry packets to stderr. On a platform without
/// descriptors to copy (neither unix nor Windows) it is an error of kind `Unsupported`.
pub fn packet_stream() -> io::Result<File> {
    if TAKEN.load(Ordering::SeqCst) {
        return Err(io::Error::other("the packet stream is already taken"));
    }
    let packets = take()?;
    TAKEN.store(true, Ordering::SeqCst);
    Ok(packets)
}

/// Copies stdout for the packets, then points stdout at stderr.
#[cfg(unix)]
fn take() -> io::Result<File> {
    use std::os::fd::FromRawFd;

    const STDOUT: libc::c_int = 1;
    const STDERR: libc::c_int = 2;
    // SAFETY: descriptor 1 is this process's stdout. Duplicating it opens a new descriptor, which
    // nothing else owns.
    let fd = unsafe { libc::dup(STDOUT) };
    if fd < 0 {
        return Err(io::Error::last_os_error());
    }
    // SAFETY: `fd` is a new descriptor that nothing else owns, so the file takes it over. If the
    // next call fails, the file is dropped and closes it.
    let packets = unsafe { File::from_raw_fd(fd) };
    // SAFETY: descriptor 2 is open, and dup2 makes descriptor 1 a copy of it, so stdout writes to
    // stderr from here on.
    if unsafe { libc::dup2(STDERR, STDOUT) } < 0 {
        return Err(io::Error::last_os_error());
    }
    Ok(packets)
}

/// Copies stdout for the packets, then points stdout at stderr. The C runtime's descriptors are
/// used, and the packets are written as bytes, with no text translation.
#[cfg(windows)]
fn take() -> io::Result<File> {
    use std::os::windows::io::{FromRawHandle, RawHandle};

    const STDOUT: libc::c_int = 1;
    const STDERR: libc::c_int = 2;
    // SAFETY: descriptor 1 is this process's stdout. Duplicating it opens a new descriptor, which
    // nothing else owns.
    let fd = unsafe { libc::dup(STDOUT) };
    if fd < 0 {
        return Err(io::Error::last_os_error());
    }
    // SAFETY: descriptor 2 is open, and dup2 makes descriptor 1 a copy of it, so stdout writes to
    // stderr from here on.
    if unsafe { libc::dup2(STDERR, STDOUT) } < 0 {
        return Err(io::Error::last_os_error());
    }
    // SAFETY: the handle behind the new descriptor is open, and the file is its one owner: the C
    // runtime never closes the descriptor.
    let handle = unsafe { libc::get_osfhandle(fd) } as RawHandle;
    // SAFETY: `handle` is the open handle of the new descriptor, and only this file closes it.
    Ok(unsafe { File::from_raw_handle(handle) })
}

/// Neither unix nor Windows: there are no descriptors to copy.
#[cfg(not(any(unix, windows)))]
fn take() -> io::Result<File> {
    Err(io::Error::new(
        io::ErrorKind::Unsupported,
        "the worker's packet stream needs unix or Windows",
    ))
}
