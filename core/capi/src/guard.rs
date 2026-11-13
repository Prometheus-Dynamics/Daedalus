//! Nothing unwinds into C: every entry point runs under [`guard`]. And the reason a call
//! failed, as text, kept for the thread that made it.

use std::cell::RefCell;
use std::panic::{AssertUnwindSafe, catch_unwind};

use crate::status::Status;

thread_local! {
    static LAST: RefCell<String> = const { RefCell::new(String::new()) };
}

/// Notes why the call on this thread is failing, and gives the status back.
pub(crate) fn fail(status: Status, why: impl std::fmt::Display) -> Status {
    let text = why.to_string();
    LAST.with(|last| *last.borrow_mut() = text);
    status
}

/// Runs an entry point's body. A panic becomes a status.
pub(crate) fn guard(body: impl FnOnce() -> Status) -> Status {
    match catch_unwind(AssertUnwindSafe(body)) {
        Ok(status) => status,
        Err(payload) => {
            let text = daedalus_abi::panic_message(&*payload);
            fail(Status::PANIC, format!("the call panicked: {text}"))
        }
    }
}

/// Copies `text` into the caller's buffer of `cap` bytes, and writes its length to `len`. With
/// a null buffer or one too small, only the length is written and the status says so, so a
/// caller asks once for the size and once for the text.
///
/// # Safety
/// `buf` is null or points to `cap` writable bytes, and `len` is null or points to a `usize`.
pub(crate) unsafe fn copy_out(text: &[u8], buf: *mut u8, cap: usize, len: *mut usize) -> Status {
    if len.is_null() {
        return Status::NULL_POINTER;
    }
    // SAFETY: `len` is not null, and the caller says it points to a `usize`.
    unsafe { len.write(text.len()) };
    if buf.is_null() || cap < text.len() {
        return Status::BUFFER_TOO_SMALL;
    }
    // SAFETY: `buf` holds at least `text.len()` bytes, by the check above and the caller's word,
    // and a caller's buffer doesn't overlap this library's text.
    unsafe { std::ptr::copy_nonoverlapping(text.as_ptr(), buf, text.len()) };
    Status::OK
}

/// Why the last failed call on this thread failed, as UTF-8 text with no NUL. Empty when
/// nothing has failed. A call that succeeds leaves it as it was.
///
/// # Safety
/// `buf` is null or points to `cap` writable bytes, and `len` points to a `size_t`.
// SAFETY: the name is this library\'s own, with its prefix: no other symbol has it.
#[unsafe(no_mangle)]
pub unsafe extern "C" fn daedalus_last_error(buf: *mut u8, cap: usize, len: *mut usize) -> Status {
    guard(|| {
        let text = LAST.with(|last| last.borrow().clone());
        // SAFETY: the caller's contract is this function's.
        unsafe { copy_out(text.as_bytes(), buf, cap, len) }
    })
}

/// Panics on purpose, for a caller's own test that a panic comes back as `DAEDALUS_PANIC`.
// SAFETY: the name is this library\'s own, with its prefix: no other symbol has it.
#[unsafe(no_mangle)]
pub extern "C" fn daedalus_selftest_panic() -> Status {
    guard(|| panic!("asked to, by daedalus_selftest_panic"))
}

#[cfg(test)]
mod tests {
    use super::{daedalus_last_error, daedalus_selftest_panic};
    use crate::status::Status;

    #[test]
    fn a_panic_is_a_status_and_its_message_is_kept() {
        assert_eq!(daedalus_selftest_panic(), Status::PANIC);
        let mut len = 0;
        // SAFETY: a null buffer asks for the size, and `len` is a `usize`.
        let status = unsafe { daedalus_last_error(std::ptr::null_mut(), 0, &mut len) };
        assert_eq!(status, Status::BUFFER_TOO_SMALL);
        let mut text = vec![0; len];
        // SAFETY: the buffer is `len` bytes.
        let status = unsafe { daedalus_last_error(text.as_mut_ptr(), text.len(), &mut len) };
        assert_eq!(status, Status::OK);
        let text = String::from_utf8(text).expect("UTF-8");
        assert!(text.contains("asked to"), "{text}");
        // SAFETY: a null length is refused before anything is written.
        let status = unsafe { daedalus_last_error(std::ptr::null_mut(), 0, std::ptr::null_mut()) };
        assert_eq!(status, Status::NULL_POINTER);
    }
}
