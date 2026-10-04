//! The C-safe table of foreign interfaces a plugin uses, and the host-side check that refuses a
//! plugin whose copy of an interface differs from the host's.

use super::StrSink;
use crate::runtime::plugins::PluginRegistry;
use crate::transport::{ForeignInterfaceInfo, TypeKey};
use std::fmt;

/// A `'static` array of [`ForeignInterfaceInfo`] owned by the plugin: every foreign interface
/// its nodes take and its providers implement
/// ([`PluginRegistry::foreign_interfaces`]).
#[repr(C)]
#[derive(Copy, Clone, Debug)]
pub struct ForeignInterfaceTable {
    pub entries: *const ForeignInterfaceInfo,
    pub len: usize,
}

/// Fills `table` with the plugin's foreign interfaces (or writes an error to the sink); returns
/// whether it succeeded.
pub type ForeignInterfacesFn =
    unsafe extern "C" fn(table: *mut ForeignInterfaceTable, sink: StrSink) -> bool;

/// An interface key the host and a plugin declare with different versions or vtable layouts.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub struct ForeignInterfaceMismatch {
    pub host: ForeignInterfaceInfo,
    pub plugin: ForeignInterfaceInfo,
}

impl fmt::Display for ForeignInterfaceMismatch {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        let (host, plugin) = (&self.host, &self.plugin);
        write!(
            f,
            "`{}`: host v{} (layout {:016x}), plugin v{} (layout {:016x})",
            host.key(),
            host.version,
            host.layout_hash,
            plugin.version,
            plugin.layout_hash
        )
    }
}

/// Join mismatches for an error message.
pub(super) fn describe(mismatches: &[ForeignInterfaceMismatch]) -> String {
    mismatches
        .iter()
        .map(ToString::to_string)
        .collect::<Vec<_>>()
        .join("; ")
}

/// Every plugin interface whose key the host's `registry` uses with another version or layout.
/// Interfaces the host does not use are fine.
pub(super) fn mismatches(
    registry: &PluginRegistry,
    plugin: &[ForeignInterfaceInfo],
) -> Vec<ForeignInterfaceMismatch> {
    let host = registry.foreign_interfaces();
    plugin
        .iter()
        .filter_map(|plugin| {
            let host = host.get(&TypeKey::new(plugin.key()))?;
            (!host.same_interface(plugin)).then_some(ForeignInterfaceMismatch {
                host: *host,
                plugin: *plugin,
            })
        })
        .collect()
}

/// Read a table returned by a plugin's [`ForeignInterfacesFn`].
///
/// # Safety
/// `table` must point to `len` entries that stay valid for the process lifetime.
pub(super) unsafe fn read_table(
    table: ForeignInterfaceTable,
) -> Result<Vec<ForeignInterfaceInfo>, String> {
    if table.len == 0 {
        return Ok(Vec::new());
    }
    if table.entries.is_null() {
        return Err("foreign interface table is null".to_string());
    }
    // Safety: guaranteed by the caller.
    let entries = unsafe { std::slice::from_raw_parts(table.entries, table.len) };
    Ok(entries.to_vec())
}

/// Plugin side: leak `registry`'s foreign interfaces as a `'static` table.
pub(super) fn leak_table(registry: &PluginRegistry) -> ForeignInterfaceTable {
    let entries: &'static [ForeignInterfaceInfo] = registry
        .foreign_interfaces()
        .values()
        .copied()
        .collect::<Vec<_>>()
        .leak();
    ForeignInterfaceTable {
        entries: entries.as_ptr(),
        len: entries.len(),
    }
}
