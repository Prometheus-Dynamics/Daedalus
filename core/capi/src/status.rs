//! The one table of status codes. A number never changes meaning, and new ones go at the end.

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
                    _ => "DAEDALUS_UNKNOWN_STATUS\0",
                }
            }
        }
    };
}

statuses! {
    OK = 0, "DAEDALUS_OK";
    /// A pointer the call needs was null.
    NULL_POINTER = 1, "DAEDALUS_NULL_POINTER";
    /// Not a handle of the kind the call takes: zero, or a number that was never one.
    BAD_HANDLE = 2, "DAEDALUS_BAD_HANDLE";
    /// A handle that was released.
    STALE_HANDLE = 3, "DAEDALUS_STALE_HANDLE";
    /// The host still has graphs.
    BUSY = 4, "DAEDALUS_BUSY";
    /// Text that isn't UTF-8.
    BAD_TEXT = 5, "DAEDALUS_BAD_TEXT";
    /// The caller's buffer is too small. The size needed is in the length the call wrote.
    BUFFER_TOO_SMALL = 6, "DAEDALUS_BUFFER_TOO_SMALL";
    /// The graph has no host port of that name, or that index.
    UNKNOWN_PORT = 7, "DAEDALUS_UNKNOWN_PORT";
    /// A push to an output, or a take from an input.
    WRONG_DIRECTION = 8, "DAEDALUS_WRONG_DIRECTION";
    /// The port's type isn't one the call's kind of value goes into or comes out of.
    TYPE_MISMATCH = 9, "DAEDALUS_TYPE_MISMATCH";
    /// The number doesn't fit the port's type.
    OUT_OF_RANGE = 10, "DAEDALUS_OUT_OF_RANGE";
    /// The plugin couldn't be loaded, or its nodes couldn't join the host's.
    PLUGIN_FAILED = 11, "DAEDALUS_PLUGIN_FAILED";
    /// The graph's text couldn't be read.
    GRAPH_INVALID = 12, "DAEDALUS_GRAPH_INVALID";
    /// The graph was read and can't be planned.
    PLAN_FAILED = 13, "DAEDALUS_PLAN_FAILED";
    /// The plan couldn't be loaded.
    LOAD_FAILED = 14, "DAEDALUS_LOAD_FAILED";
    /// The graph refused the value, or the tick failed.
    RUN_FAILED = 15, "DAEDALUS_RUN_FAILED";
    /// A batch with an entry that can't be pushed. Nothing was pushed.
    BAD_BATCH = 16, "DAEDALUS_BAD_BATCH";
    /// The call panicked. The handle it was given is still valid.
    PANIC = 17, "DAEDALUS_PANIC";
}

/// The name of a status, for messages. Never null, and never freed.
// SAFETY: the name is this library\'s own, with its prefix: no other symbol has it.
#[unsafe(no_mangle)]
pub extern "C" fn daedalus_status_name(status: Status) -> *const c_char {
    status.name().as_ptr().cast()
}

#[cfg(test)]
mod tests {
    use super::Status;

    #[test]
    fn the_numbers_are_in_order_and_each_has_its_name() {
        for (at, (status, text)) in Status::ALL.iter().enumerate() {
            assert_eq!(status.0, at as i32, "{text}");
            assert_eq!(status.name(), format!("{text}\0"));
        }
        assert_eq!(Status(999).name(), "DAEDALUS_UNKNOWN_STATUS\0");
    }
}
