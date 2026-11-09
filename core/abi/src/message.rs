//! Text a callee hands back to its caller, in pieces, without allocating.

use core::ffi::c_void;
use core::fmt;

#[repr(C)]
#[derive(Clone, Copy, Debug)]
pub struct MessageSink {
    pub context: *mut c_void,
    pub write: Option<extern "C" fn(context: *mut c_void, text: *const u8, len: usize)>,
}

impl MessageSink {
    pub const NONE: MessageSink = MessageSink {
        context: core::ptr::null_mut(),
        write: None,
    };

    pub fn write(&self, text: &str) {
        if let Some(write) = self.write {
            write(self.context, text.as_ptr(), text.len());
        }
    }

    pub fn write_fmt(&self, args: fmt::Arguments<'_>) {
        struct Adapter<'a>(&'a MessageSink);
        impl fmt::Write for Adapter<'_> {
            fn write_str(&mut self, text: &str) -> fmt::Result {
                self.0.write(text);
                Ok(())
            }
        }
        // The adapter never fails, so neither does this.
        let _ = fmt::write(&mut Adapter(self), args);
    }
}
