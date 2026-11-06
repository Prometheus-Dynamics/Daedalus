//! Raw-pointer helpers and the status plumbing every entry point shares.

use lemnos_abi::{MessageSink, Status};

/// Runs an entry point's body under the panic guard. `Err` is the status it returns; a panic
/// becomes [`Status::PANIC`].
pub fn run(body: impl FnOnce() -> Result<(), Status>) -> Status {
    lemnos_abi::guard(|| match body() {
        Ok(()) => Status::OK,
        Err(status) => status,
    })
}

/// The bytes at `ptr`, `len` long; empty for a zero length, whatever the pointer.
///
/// # Safety
/// `ptr` must be valid for reads of `len` bytes when `len` is not 0.
pub unsafe fn bytes<'a>(ptr: *const u8, len: usize) -> Result<&'a [u8], Status> {
    if len == 0 {
        return Ok(&[]);
    }
    if ptr.is_null() {
        return Err(Status::INVALID_ARGUMENT);
    }
    // Safety: the caller guarantees `len` readable bytes at a non-null pointer.
    Ok(unsafe { core::slice::from_raw_parts(ptr, len) })
}

/// The UTF-8 text at `ptr`, `len` long (not NUL-terminated).
///
/// # Safety
/// As [`bytes`].
pub unsafe fn text<'a>(ptr: *const u8, len: usize) -> Result<&'a str, Status> {
    // Safety: forwarded.
    let raw = unsafe { bytes(ptr, len) }?;
    core::str::from_utf8(raw).map_err(|_| Status::INVALID_ARGUMENT)
}

/// Writes `data` into the caller's buffer (`buf`, `cap` bytes) and always writes its length to
/// `*needed`. Too small a buffer is [`Status::BUFFER_TOO_SMALL`] with the length set, so the
/// caller can allocate that much and call again.
///
/// # Safety
/// `needed` must be valid for a write of a `usize`; `buf` must be valid for writes of `cap`
/// bytes when `data` fits.
pub unsafe fn write_out(
    data: &[u8],
    buf: *mut u8,
    cap: usize,
    needed: *mut usize,
) -> Result<(), Status> {
    if needed.is_null() {
        return Err(Status::INVALID_ARGUMENT);
    }
    // Safety: `needed` is writable.
    unsafe { needed.write(data.len()) };
    if data.len() > cap {
        return Err(Status::BUFFER_TOO_SMALL);
    }
    if data.is_empty() {
        return Ok(());
    }
    if buf.is_null() {
        return Err(Status::INVALID_ARGUMENT);
    }
    // Safety: `buf` has room for `data`; the caller's buffer and this call's own data do not
    // overlap.
    unsafe { core::ptr::copy_nonoverlapping(data.as_ptr(), buf, data.len()) };
    Ok(())
}

/// Writes an error's message to the caller's sink.
pub fn report(sink: MessageSink, message: impl core::fmt::Display) {
    sink.write(&message.to_string());
}
