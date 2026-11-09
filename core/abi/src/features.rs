//! Optional capabilities, as bits. An unknown bit is kept, never an error.

use crate::Status;

#[repr(transparent)]
#[derive(Clone, Copy, Debug, PartialEq, Eq, Hash)]
pub struct Features(pub u64);

impl Features {
    pub const NONE: Features = Features(0);

    pub const fn with(self, bit: u64) -> Features {
        Features(self.0 | bit)
    }

    pub const fn contains(self, wanted: Features) -> bool {
        self.0 & wanted.0 == wanted.0
    }

    pub const fn require(self, wanted: Features) -> Status {
        if self.contains(wanted) {
            Status::OK
        } else {
            Status::NOT_SUPPORTED
        }
    }
}
