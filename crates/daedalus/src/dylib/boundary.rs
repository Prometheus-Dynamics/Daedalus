//! The C-safe table of boundary types a plugin exports. The host compares it with its registry
//! ([`PluginRegistry::boundary_type_conflicts`]) and refuses a plugin whose Rust types differ
//! from the host's for the same `TypeKey`.

use super::{StrSink, StrView};
use crate::runtime::plugins::{BoundaryTypeConflict, PluginRegistry};
use crate::transport::{RustTypeIdentity, TypeKey};

/// One `TypeKey` a plugin consumes, produces or registers, with the Rust type behind it in the
/// plugin's build (see [`PluginRegistry::boundary_types`]).
#[repr(C)]
#[derive(Copy, Clone, Debug)]
pub struct BoundaryTypeEntry {
    pub type_key: StrView,
    pub type_name: StrView,
    /// Hash of the type's `TypeId`; only comparable between builds of the same rustc.
    pub type_id_hash: u64,
    pub size: usize,
    pub align: usize,
}

/// A `'static` array of [`BoundaryTypeEntry`] owned by the plugin.
#[repr(C)]
#[derive(Copy, Clone, Debug)]
pub struct BoundaryTypeTable {
    pub entries: *const BoundaryTypeEntry,
    pub len: usize,
}

/// Fills `table` with the plugin's boundary types (or writes an error to the sink); returns
/// whether it succeeded.
pub type BoundaryTypesFn =
    unsafe extern "C" fn(table: *mut BoundaryTypeTable, sink: StrSink) -> bool;

/// Join conflicts for an error message.
pub(super) fn describe(conflicts: &[BoundaryTypeConflict]) -> String {
    conflicts
        .iter()
        .map(ToString::to_string)
        .collect::<Vec<_>>()
        .join("; ")
}

/// Read a table returned by a plugin's [`BoundaryTypesFn`].
///
/// # Safety
/// `table` must point to `len` entries that stay valid for the process lifetime.
pub(super) unsafe fn read_table(
    table: BoundaryTypeTable,
) -> Result<Vec<(TypeKey, RustTypeIdentity)>, String> {
    if table.len == 0 {
        return Ok(Vec::new());
    }
    if table.entries.is_null() {
        return Err("boundary type table is null".to_string());
    }
    // Safety: guaranteed by the caller.
    let entries = unsafe { std::slice::from_raw_parts(table.entries, table.len) };
    entries
        .iter()
        .map(|entry| {
            let text = |view: StrView, field: &str| {
                view.as_str()
                    .ok_or_else(|| format!("boundary type {field} is null or not valid UTF-8"))
            };
            let identity = RustTypeIdentity {
                type_name: text(entry.type_name, "type_name")?,
                type_id_hash: entry.type_id_hash,
                size: entry.size,
                align: entry.align,
            };
            Ok((TypeKey::new(text(entry.type_key, "type_key")?), identity))
        })
        .collect()
}

/// Plugin side: leak `registry`'s boundary types as a `'static` table (built once per load).
pub(super) fn leak_table(registry: &PluginRegistry) -> BoundaryTypeTable {
    let leak = |text: &str| -> &'static str { Box::leak(text.to_owned().into_boxed_str()) };
    let entries: &'static [BoundaryTypeEntry] = registry
        .boundary_types()
        .iter()
        .map(|(key, identity)| BoundaryTypeEntry {
            type_key: StrView::from_static(leak(key.as_str())),
            type_name: StrView::from_static(identity.type_name),
            type_id_hash: identity.type_id_hash,
            size: identity.size,
            align: identity.align,
        })
        .collect::<Vec<_>>()
        .leak();
    BoundaryTypeTable {
        entries: entries.as_ptr(),
        len: entries.len(),
    }
}
