//! The `alloc` names the `std` prelude provides, for `no_std` builds (`use crate::prelude::*`),
//! and the hash maps of [`crate::collections`].

pub(crate) use alloc::boxed::Box;
pub(crate) use alloc::string::{String, ToString};
pub(crate) use alloc::vec::Vec;

pub(crate) use crate::collections::{HashMap, HashSet, hash_map};
