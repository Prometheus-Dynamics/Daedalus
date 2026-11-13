//! The handshake a host runs against a plugin before any other call.

use crate::{FROZEN_HASH, Status};

/// The symbol a plugin exports, holding one static [`AbiVersion`].
pub const ABI_SYMBOL: &str = "daedalus_abi";

/// The ABI this build speaks. The major stays 0 until a release is called. Its layout is
/// [`FROZEN_HASH`]: what can't change without breaking a build made before. A field added to the
/// end of a growable shape doesn't move it, so a caller built before the field is still accepted.
pub const DAEDALUS_ABI: AbiVersion = AbiVersion::new(0, 1, FROZEN_HASH);

#[repr(C)]
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub struct AbiVersion {
    pub struct_size: u32,
    pub major: u16,
    pub minor: u16,
    pub layout: u64,
}

impl AbiVersion {
    pub const fn new(major: u16, minor: u16, layout: u64) -> AbiVersion {
        AbiVersion {
            struct_size: core::mem::size_of::<AbiVersion>() as u32,
            major,
            minor,
            layout,
        }
    }

    /// Whether code built against `self` can serve a `caller`.
    pub const fn accepts(&self, caller: &AbiVersion) -> Status {
        let too_small = (caller.struct_size as usize) < core::mem::size_of::<AbiVersion>();
        if too_small
            || caller.major != self.major
            || caller.minor > self.minor
            || caller.layout != self.layout
        {
            Status::VERSION_MISMATCH
        } else {
            Status::OK
        }
    }
}
