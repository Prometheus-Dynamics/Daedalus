//! All-or-nothing copies into a caller's buffer.

use crate::Status;

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub struct Written {
    pub status: Status,
    /// The size the value needs, always set, even when it didn't fit.
    pub needed: usize,
}

/// Copies `value` into `out` if it fits. `None` only asks for the size.
pub fn write_out(value: &[u8], out: Option<&mut [u8]>) -> Written {
    let status = match out {
        None => Status::OK,
        Some(out) if out.len() >= value.len() => {
            out[..value.len()].copy_from_slice(value);
            Status::OK
        }
        Some(_) => Status::BUFFER_TOO_SMALL,
    };
    Written {
        status,
        needed: value.len(),
    }
}

/// As [`write_out`], for text: adds a NUL, and needs room for it.
pub fn write_text(value: &str, out: Option<&mut [u8]>) -> Written {
    let needed = value.len() + 1;
    let status = match out {
        None => Status::OK,
        Some(out) if out.len() >= needed => {
            out[..value.len()].copy_from_slice(value.as_bytes());
            out[value.len()] = 0;
            Status::OK
        }
        Some(_) => Status::BUFFER_TOO_SMALL,
    };
    Written { status, needed }
}
