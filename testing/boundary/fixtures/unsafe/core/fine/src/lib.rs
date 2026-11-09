#![forbid(unsafe_code)]
pub fn a() { unsafe { core::hint::unreachable_unchecked() } }
