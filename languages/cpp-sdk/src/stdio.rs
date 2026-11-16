//! `dd_serve`: the worker on stdin and stdout. Packets go to the worker's packet stream
//! (`daedalus-worker-io`), and stdout itself becomes stderr, so a `printf` in a node cannot corrupt
//! the stream.

use std::ffi::{c_char, c_int};
use std::io;
use std::slice;

use crate::convert::c_text;
use crate::ffi::{DdNode, MAX_NODES};
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

/// The stdio loop: the packets are on the worker's packet stream, and stdout is stderr from then on.
fn serve_stdio(table: &[DdNode], name: &str) -> io::Result<()> {
    let packets = daedalus_worker_io::packet_stream()?;
    let stdin = io::stdin();
    serve_io(table, &mut stdin.lock(), Box::new(packets), name)
}
