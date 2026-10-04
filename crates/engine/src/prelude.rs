//! The `alloc` names the `std` prelude provides, for `no_std` builds (`use crate::prelude::*`),
//! and the runtime's hash maps.

pub(crate) use alloc::boxed::Box;
pub(crate) use alloc::string::{String, ToString};
pub(crate) use alloc::vec::Vec;

pub(crate) use daedalus_runtime::collections::{HashMap, HashSet};
