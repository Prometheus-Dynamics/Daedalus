//! The result code every boundary call returns.

/// `0` is OK. `-1` to `-4095` is a negated Linux errno. Below that are this crate's codes.
/// A positive value is a successful count.
#[repr(transparent)]
#[derive(Clone, Copy, Debug, PartialEq, Eq, Hash)]
pub struct Status(pub i32);

impl Status {
    pub const OK: Status = Status(0);
    /// `EOPNOTSUPP`, negated.
    pub const NOT_SUPPORTED: Status = Status(-95);
    pub const STALE_HANDLE: Status = Status(-4096);
    pub const VERSION_MISMATCH: Status = Status(-4097);
    pub const BUFFER_TOO_SMALL: Status = Status(-4098);
    pub const PANIC: Status = Status(-4099);

    pub const fn from_errno(errno: i32) -> Status {
        Status(-errno)
    }

    /// The errno this status carries, if it is one.
    pub const fn errno(self) -> Option<i32> {
        if self.0 < 0 && self.0 > -4096 {
            Some(-self.0)
        } else {
            None
        }
    }

    pub const fn is_ok(self) -> bool {
        self.0 >= 0
    }

    pub const fn into_result(self) -> Result<u32, Status> {
        if self.0 >= 0 {
            Ok(self.0 as u32)
        } else {
            Err(self)
        }
    }
}
