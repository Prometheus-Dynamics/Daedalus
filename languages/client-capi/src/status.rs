//! The one table of status codes. A number never changes meaning. The numbers core/capi gives
//! the statuses both libraries share are used here for the same meanings, so one number means
//! one thing in either library. Numbers this library has no status for are left empty (4, and
//! 7 to 16, are core/capi's own), so a status of this library never takes another's number.

use std::ffi::c_char;

/// What a call returns. Zero is success.
#[repr(transparent)]
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub struct Status(pub i32);

macro_rules! statuses {
    ($($(#[$doc:meta])* $name:ident = $code:literal, $text:literal;)*) => {
        impl Status {
            $($(#[$doc])* pub const $name: Status = Status($code);)*

            /// Every status, in the order of its number.
            pub const ALL: &'static [(Status, &'static str)] = &[$((Status::$name, $text),)*];

            /// The status's name, as the header spells it, with a NUL after it.
            pub fn name(self) -> &'static str {
                match self.0 {
                    $($code => concat!($text, "\0"),)*
                    _ => "DAEDALUS_CLIENT_UNKNOWN_STATUS\0",
                }
            }
        }
    };
}

statuses! {
    OK = 0, "DAEDALUS_CLIENT_OK";
    /// A pointer the call needs was null.
    NULL_POINTER = 1, "DAEDALUS_CLIENT_NULL_POINTER";
    /// Not a handle of the kind the call takes: zero, or a number that was never one.
    BAD_HANDLE = 2, "DAEDALUS_CLIENT_BAD_HANDLE";
    /// A handle that was released or closed.
    STALE_HANDLE = 3, "DAEDALUS_CLIENT_STALE_HANDLE";
    /// Text that isn't UTF-8.
    BAD_TEXT = 5, "DAEDALUS_CLIENT_BAD_TEXT";
    /// The caller's buffer is too small. The size needed is in the length the call wrote, and
    /// the value or text is kept for the next call that asks for it.
    BUFFER_TOO_SMALL = 6, "DAEDALUS_CLIENT_BUFFER_TOO_SMALL";
    /// The call panicked. The handle it was given is still valid.
    PANIC = 17, "DAEDALUS_CLIENT_PANIC";
    /// The endpoint text is neither `unix:/path` nor `tcp:address`, or the address is not on the
    /// loopback interface.
    BAD_ENDPOINT = 18, "DAEDALUS_CLIENT_BAD_ENDPOINT";
    /// The client has no connection. Nothing was sent.
    DISCONNECTED = 19, "DAEDALUS_CLIENT_DISCONNECTED";
    /// The connection dropped after the request was sent and before its answer came. The server
    /// may or may not have acted on it.
    UNKNOWN_DELIVERY = 20, "DAEDALUS_CLIENT_UNKNOWN_DELIVERY";
    /// The server refused the request. Its code is in `daedalus_client_last_error_code`, and its
    /// message in `daedalus_client_last_error`.
    SERVICE_ERROR = 21, "DAEDALUS_CLIENT_SERVICE_ERROR";
    /// The server speaks another protocol version or layout, so the connection was refused.
    REFUSED = 22, "DAEDALUS_CLIENT_REFUSED";
    /// A value or message could not be encoded or decoded, such as bytes that are not a typed
    /// value.
    WIRE = 23, "DAEDALUS_CLIENT_WIRE";
    /// A request larger than the server reads.
    TOO_LARGE = 24, "DAEDALUS_CLIENT_TOO_LARGE";
    /// A socket error. The message says which.
    IO = 25, "DAEDALUS_CLIENT_IO";
    /// A session name that is empty or longer than the server keeps.
    BAD_SESSION = 26, "DAEDALUS_CLIENT_BAD_SESSION";
    /// The server answered with a reply that does not belong to the call.
    UNEXPECTED = 27, "DAEDALUS_CLIENT_UNEXPECTED";
    /// A take asked for a type the port's value does not have. The value is kept for a take of its
    /// own type, or for `daedalus_client_take_wire`.
    TYPE_MISMATCH = 28, "DAEDALUS_CLIENT_TYPE_MISMATCH";
    /// A number that names no option: a telemetry level, a stop mode, an event kind or a plan form.
    BAD_ARGUMENT = 29, "DAEDALUS_CLIENT_BAD_ARGUMENT";
}

/// The name of a status, for messages. Never null, and never freed.
// SAFETY: the name is this library's own, with its prefix: no other symbol has it.
#[unsafe(no_mangle)]
pub extern "C" fn daedalus_client_status_name(status: Status) -> *const c_char {
    status.name().as_ptr().cast()
}

#[cfg(test)]
mod tests {
    use super::Status;

    #[test]
    fn the_numbers_rise_and_each_has_its_name() {
        let mut last = -1;
        for (status, text) in Status::ALL {
            assert!(status.0 > last, "{text} is out of order");
            last = status.0;
            assert_eq!(status.name(), format!("{text}\0"));
        }
        assert_eq!(Status(999).name(), "DAEDALUS_CLIENT_UNKNOWN_STATUS\0");
    }
}
