//! Host-owned copies of the strings a plugin reports, so nothing the host keeps (registries,
//! [`PluginInfo`](super::PluginInfo), boundary and foreign interface tables) points into a library
//! that may be unloaded.

use std::collections::HashSet;
use std::sync::{Mutex, OnceLock};

use crate::transport::ForeignInterfaceInfo;

/// A `'static` host copy of `text`, allocated once per distinct string for the process (a
/// rebuilt plugin reports the same names, versions and keys, so reloading allocates nothing).
pub(super) fn intern(text: &str) -> &'static str {
    static STRINGS: OnceLock<Mutex<HashSet<&'static str>>> = OnceLock::new();
    let mut strings = STRINGS
        .get_or_init(Mutex::default)
        .lock()
        .unwrap_or_else(|err| err.into_inner());
    if let Some(interned) = strings.get(text) {
        return interned;
    }
    let interned: &'static str = Box::leak(text.to_owned().into_boxed_str());
    strings.insert(interned);
    interned
}

/// `ForeignInterfaceInfo`'s `#[repr(C)]` layout, part of the stable ABI
/// ([`STABLE_ABI_VERSION`](super::STABLE_ABI_VERSION)).
#[repr(C)]
#[derive(Clone, Copy)]
struct InfoLayout {
    key_ptr: *const u8,
    key_len: usize,
    version: u32,
    layout_hash: u64,
}

const _: () = {
    assert!(size_of::<InfoLayout>() == size_of::<ForeignInterfaceInfo>());
    assert!(align_of::<InfoLayout>() == align_of::<ForeignInterfaceInfo>());
};

/// `info` with its key pointing at a host copy instead of the plugin's static.
pub(super) fn foreign_interface(info: ForeignInterfaceInfo) -> ForeignInterfaceInfo {
    let key = intern(info.key());
    // Safety: `ForeignInterfaceInfo` is `#[repr(C)]` with exactly `InfoLayout`'s fields (a
    // stable ABI layout, checked in size and alignment above), and the new key is `'static`.
    unsafe {
        let mut layout: InfoLayout = std::mem::transmute(info);
        layout.key_ptr = key.as_ptr();
        layout.key_len = key.len();
        std::mem::transmute::<InfoLayout, ForeignInterfaceInfo>(layout)
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::transport::{ForeignInterface, FrameInterface};

    #[test]
    fn interned_strings_and_rekeyed_interfaces_are_host_copies() {
        let owned = String::from("plugin:key");
        let interned = intern(&owned);
        assert_eq!(interned, "plugin:key");
        assert_ne!(interned.as_ptr(), owned.as_ptr());
        assert!(
            std::ptr::eq(interned, intern("plugin:key")),
            "allocated once"
        );

        let info = *FrameInterface::info();
        let rekeyed = foreign_interface(info);
        assert!(rekeyed.same_interface(&info));
        assert_eq!(rekeyed.version, info.version);
        assert_eq!(rekeyed.layout_hash, info.layout_hash);
        assert!(std::ptr::eq(rekeyed.key(), intern(info.key())));
    }
}
