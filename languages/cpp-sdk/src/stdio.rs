//! `dd_serve`: the worker on stdin and stdout. Packets go to a copy of stdout, and stdout itself
//! becomes stderr, so a `printf` in a node cannot corrupt the stream.

use std::ffi::{c_char, c_int};
#[cfg(any(unix, windows))]
use std::fs::File;
use std::io;
use std::slice;

use crate::convert::c_text;
use crate::ffi::{DdNode, MAX_NODES};
#[cfg(any(unix, windows))]
use crate::serve::serve_io;

/// Serves the nodes on stdin and stdout until the host shuts the worker down or closes the pipe.
/// Returns 0 on a clean end, and -1 when the stream could not be read or written.
///
/// # Safety
/// `nodes` holds `count` entries, and `worker_name` is a NUL-terminated string.
#[unsafe(no_mangle)]
pub unsafe extern "C" fn dd_serve(
    nodes: *const DdNode,
    count: usize,
    worker_name: *const c_char,
) -> c_int {
    if count > MAX_NODES || (count > 0 && nodes.is_null()) {
        return -1;
    }
    let table: &[DdNode] = if count == 0 {
        &[]
    } else {
        // SAFETY: the pointer is not null and names `count` entries, as the caller's contract says.
        unsafe { slice::from_raw_parts(nodes, count) }
    };
    // SAFETY: the caller's worker name is a NUL-terminated string.
    let name = unsafe { c_text(worker_name) };
    match serve_stdio(table, &name) {
        Ok(()) => 0,
        Err(_) => -1,
    }
}

/// The stdio loop: the packet stream is a copy of stdout, and stdout is pointed at stderr first.
#[cfg(any(unix, windows))]
fn serve_stdio(table: &[DdNode], name: &str) -> io::Result<()> {
    const STDOUT: c_int = 1;
    const STDERR: c_int = 2;
    // SAFETY: descriptor 1 is this process's stdout. Duplicating it opens a new descriptor for the
    // packets.
    let packet_fd = unsafe { libc::dup(STDOUT) };
    if packet_fd < 0 {
        return Err(io::Error::last_os_error());
    }
    // SAFETY: descriptor 2 is open, and dup2 makes descriptor 1 a copy of it, so node output goes
    // to stderr.
    if unsafe { libc::dup2(STDERR, STDOUT) } < 0 {
        return Err(io::Error::last_os_error());
    }
    // SAFETY: `packet_fd` is a new descriptor that nothing else owns, so the file takes it over.
    #[cfg(unix)]
    let packets = unsafe { <File as std::os::fd::FromRawFd>::from_raw_fd(packet_fd) };
    // The C runtime never closes the new descriptor, so the file is its handle's one owner. A
    // handle is written as bytes, with no text translation.
    #[cfg(windows)]
    // SAFETY: the handle behind the new descriptor is open, and only this file closes it.
    let packets = unsafe {
        <File as std::os::windows::io::FromRawHandle>::from_raw_handle(libc::get_osfhandle(
            packet_fd,
        )
            as std::os::windows::io::RawHandle)
    };
    let stdin = io::stdin();
    serve_io(table, &mut stdin.lock(), Box::new(packets), name)
}

/// The stdio worker needs a descriptor of its own for the packet stream.
#[cfg(not(any(unix, windows)))]
fn serve_stdio(_table: &[DdNode], _name: &str) -> io::Result<()> {
    Err(io::Error::other(
        "the C worker SDK serves over stdio on unix and Windows only",
    ))
}
