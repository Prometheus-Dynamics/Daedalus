//! Nothing unwinds into C: every entry point runs under [`guard`]. Why a call failed is kept as
//! text, with the server's error code when the server refused, for the thread that made the call.
//! Also the checked reads of a caller's text and bytes, and the writes of a call's results.

use std::cell::{Cell, RefCell};
use std::panic::{AssertUnwindSafe, catch_unwind};

use crate::status::Status;

struct Last {
    text: String,
    service: u32,
}

thread_local! {
    static LAST: RefCell<Last> = const { RefCell::new(Last { text: String::new(), service: 0 }) };
    /// Whether the call on this thread that is running noted its failure with `fail`.
    static NOTED: Cell<bool> = const { Cell::new(false) };
}

/// Notes why the call on this thread is failing, and gives the status back.
pub(crate) fn fail(status: Status, why: impl std::fmt::Display) -> Status {
    set(why.to_string(), 0);
    status
}

/// Notes a refusal by the server: its code and its message, and gives `SERVICE_ERROR` back.
pub(crate) fn fail_service(code: u32, why: impl std::fmt::Display) -> Status {
    set(why.to_string(), code);
    Status::SERVICE_ERROR
}

fn set(text: String, service: u32) {
    LAST.with(|last| *last.borrow_mut() = Last { text, service });
    NOTED.with(|noted| noted.set(true));
}

/// Runs an entry point's body. A failure is its status, which the body has already noted. A
/// panic becomes `PANIC`. A failure the body did not note clears the reason an earlier failure
/// left, so the last error never names a failure that is not the last one.
pub(crate) fn guard(body: impl FnOnce() -> Result<(), Status>) -> Status {
    let status = guard_keep(body);
    if status != Status::OK && !NOTED.with(Cell::get) {
        LAST.with(|last| {
            *last.borrow_mut() = Last {
                text: String::new(),
                service: 0,
            }
        });
    }
    status
}

/// As [`guard`], but a failure the body did not note leaves the stored reason alone. The
/// last-error queries use it: asking for the reason is not a failure that replaces it.
pub(crate) fn guard_keep(body: impl FnOnce() -> Result<(), Status>) -> Status {
    NOTED.with(|noted| noted.set(false));
    match catch_unwind(AssertUnwindSafe(body)) {
        Ok(Ok(())) => Status::OK,
        Ok(Err(status)) => status,
        Err(payload) => {
            let text = daedalus_abi::panic_message(&*payload);
            fail(Status::PANIC, format!("the call panicked: {text}"))
        }
    }
}

/// Copies `text` into the caller's buffer of `cap` bytes, and writes its length to `len`. With a
/// null buffer, or one too small, only the length is written and the call fails with
/// `BUFFER_TOO_SMALL`, so a caller asks once for the size and once for the text. Empty text
/// needs no buffer.
///
/// # Safety
/// `buf` is null or points to `cap` writable bytes, and `len` is null or points to a `usize`.
pub(crate) unsafe fn copy_out(
    text: &[u8],
    buf: *mut u8,
    cap: usize,
    len: *mut usize,
) -> Result<(), Status> {
    let len = non_null(len, "a length")?;
    // SAFETY: `len` is not null, and the caller says it points to a `usize`.
    unsafe { len.write(text.len()) };
    if text.is_empty() {
        return Ok(());
    }
    if buf.is_null() || cap < text.len() {
        return Err(Status::BUFFER_TOO_SMALL);
    }
    // SAFETY: `buf` holds `cap >= text.len()` bytes by the check above, and the caller's buffer
    // doesn't overlap this library's text.
    unsafe { std::ptr::copy_nonoverlapping(text.as_ptr(), buf, text.len()) };
    Ok(())
}

/// [`copy_out`], and the reason noted when the buffer is too small.
///
/// # Safety
/// As [`copy_out`].
pub(crate) unsafe fn copy_or_note(
    text: &[u8],
    buf: *mut u8,
    cap: usize,
    len: *mut usize,
) -> Result<(), Status> {
    // SAFETY: the caller's contract is this function's.
    unsafe { copy_out(text, buf, cap, len) }.map_err(|status| {
        if status == Status::BUFFER_TOO_SMALL {
            fail(
                status,
                format!(
                    "the buffer has {cap} bytes and the text needs {}",
                    text.len()
                ),
            )
        } else {
            status
        }
    })
}

/// A JSON value as the bytes a call writes into a caller's buffer.
pub(crate) fn json_bytes(value: &serde_json::Value) -> Vec<u8> {
    // A value the calls build always serialises.
    serde_json::to_vec(value).unwrap_or_default()
}

/// Writes `value` to the caller's pointer, which must not be null.
///
/// # Safety
/// `out` is null or points to a `T` the caller may write.
pub(crate) unsafe fn write_out<T>(out: *mut T, value: T) -> Result<(), Status> {
    let out = non_null(out, "an output")?;
    // SAFETY: `out` is not null, and the caller says it points to a `T` it may write.
    unsafe { out.write(value) };
    Ok(())
}

/// Checks a pointer the call writes to or reads from.
pub(crate) fn non_null<T>(ptr: *mut T, what: &str) -> Result<*mut T, Status> {
    if ptr.is_null() {
        Err(fail(Status::NULL_POINTER, format!("{what} is null")))
    } else {
        Ok(ptr)
    }
}

/// `len` bytes at `ptr`. Empty input needs no memory, so a null pointer is allowed with zero.
///
/// # Safety
/// `ptr` is null or points to `len` readable bytes that stay as they are for `'a`.
pub(crate) unsafe fn read_bytes<'a>(ptr: *const u8, len: usize) -> Result<&'a [u8], Status> {
    if len == 0 {
        return Ok(&[]);
    }
    if ptr.is_null() {
        return Err(fail(Status::NULL_POINTER, "a byte string is null"));
    }
    // SAFETY: `ptr` is not null, and the caller says it points to `len` readable bytes.
    Ok(unsafe { std::slice::from_raw_parts(ptr, len) })
}

/// UTF-8 text of `len` bytes at `ptr`.
///
/// # Safety
/// As [`read_bytes`].
pub(crate) unsafe fn read_text<'a>(ptr: *const u8, len: usize) -> Result<&'a str, Status> {
    // SAFETY: the caller's contract is this function's.
    let bytes = unsafe { read_bytes(ptr, len) }?;
    std::str::from_utf8(bytes).map_err(|_| fail(Status::BAD_TEXT, "the text is not UTF-8"))
}

/// A piece of text from the caller: `len` bytes at `data`, with no NUL.
#[repr(C)]
#[derive(Clone, Copy, Debug)]
pub struct Text {
    pub data: *const u8,
    pub len: usize,
}

/// A list of text. An empty list may have a null pointer.
///
/// # Safety
/// `items` is null, when `count` is zero, or points to `count` `Text`s that each hold readable
/// bytes, all of which stay as they are for the call.
pub(crate) unsafe fn read_list(items: *const Text, count: usize) -> Result<Vec<String>, Status> {
    if count == 0 {
        return Ok(Vec::new());
    }
    if items.is_null() {
        return Err(fail(Status::NULL_POINTER, "a list of text is null"));
    }
    // SAFETY: `items` is not null, and the caller says it points to `count` texts.
    let items = unsafe { std::slice::from_raw_parts(items, count) };
    items
        .iter()
        // SAFETY: each entry holds readable bytes, by the caller's contract.
        .map(|item| unsafe { read_text(item.data, item.len) }.map(str::to_owned))
        .collect()
}

/// Why the last failed call on this thread failed, as UTF-8 text with no NUL. Empty when nothing
/// has failed. A call that succeeds leaves it as it was. For a refusal by the server the text
/// starts with the error's name, such as `unknown_run:`.
///
/// # Safety
/// `buf` is null or points to `cap` writable bytes, and `len` points to a `size_t`.
// SAFETY: the name is this library's own, with its prefix: no other symbol has it.
#[unsafe(no_mangle)]
pub unsafe extern "C" fn daedalus_client_last_error(
    buf: *mut u8,
    cap: usize,
    len: *mut usize,
) -> Status {
    guard_keep(|| {
        let text = LAST.with(|last| last.borrow().text.clone());
        // SAFETY: the caller's contract is this function's.
        unsafe { copy_out(text.as_bytes(), buf, cap, len) }
    })
}

/// The server's error code for the last failed call on this thread: the number the protocol
/// gives the error (1 to 12, or 65535 for a code this side doesn't know), and zero when the last
/// failure was not a refusal by the server.
///
/// # Safety
/// `code` points to a `uint32_t`.
// SAFETY: the name is this library's own, with its prefix: no other symbol has it.
#[unsafe(no_mangle)]
pub unsafe extern "C" fn daedalus_client_last_error_code(code: *mut u32) -> Status {
    guard_keep(|| {
        let service = LAST.with(|last| last.borrow().service);
        // SAFETY: the caller's contract is this function's.
        unsafe { write_out(code, service) }
    })
}

/// Panics on purpose, for a caller's own test that a panic comes back as `DAEDALUS_CLIENT_PANIC`.
// SAFETY: the name is this library's own, with its prefix: no other symbol has it.
#[unsafe(no_mangle)]
pub extern "C" fn daedalus_client_selftest_panic() -> Status {
    guard(|| panic!("asked to, by daedalus_client_selftest_panic"))
}

#[cfg(test)]
mod tests {
    use crate::status::Status;

    #[test]
    fn a_failure_is_noted_with_its_service_code() {
        let status = super::fail_service(12, "unknown_run: nope");
        assert_eq!(status, Status::SERVICE_ERROR);
        let mut code = 0;
        // SAFETY: `code` is a `u32` to write.
        assert_eq!(
            unsafe { super::daedalus_client_last_error_code(&mut code) },
            Status::OK
        );
        assert_eq!(code, 12);
        super::fail(Status::BAD_HANDLE, "plain");
        // SAFETY: `code` is a `u32` to write.
        unsafe { super::daedalus_client_last_error_code(&mut code) };
        assert_eq!(code, 0, "a failure that isn't the server's has no code");
    }
}
