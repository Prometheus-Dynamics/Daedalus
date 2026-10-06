//! The C-safe table of boundary types a plugin exports. The host compares it with its registry
//! ([`PluginRegistry::boundary_type_conflicts`]) and refuses a plugin whose Rust types differ
//! from the host's for the same `TypeKey`.

use super::{StrSink, StrView};
use crate::runtime::plugins::{
    BoundaryTypeConflict, CrateBuildDiff, CrateBuildInfo, PluginRegistry,
};
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

/// Describe conflicts for an error message, grouped by the crate defining the plugin's type
/// ([`RustTypeIdentity::defining_crate`]), each group led by that crate's build difference when
/// both sides registered one ([`PluginRegistry::register_crate_build`]), e.g. ``crate
/// `styx_core` 0.4.0: host features `framelease,v4l2`, plugin features `framelease` (missing in
/// plugin: v4l2) — key `styx:framelease`: host `...` (...) vs plugin `...` (...)``. A crate
/// built identically on both sides (`same`) differs through its dependencies.
pub(super) fn describe(
    conflicts: &[BoundaryTypeConflict],
    crate_builds: &[CrateBuildDiff],
    same: &[CrateBuildInfo],
) -> String {
    let crate_of = |conflict: &BoundaryTypeConflict| {
        conflict
            .new
            .defining_crate()
            .or_else(|| conflict.registered.defining_crate())
    };
    let mut crates: Vec<Option<&str>> = Vec::new();
    for krate in conflicts.iter().map(crate_of) {
        if !crates.contains(&krate) {
            crates.push(krate);
        }
    }
    crates
        .into_iter()
        .map(|krate| {
            let keys = conflicts
                .iter()
                .filter(|conflict| crate_of(conflict) == krate)
                .map(|c| format!("key `{}`: host {} vs plugin {}", c.key, c.registered, c.new))
                .collect::<Vec<_>>()
                .join(", ");
            let Some(krate) = krate else {
                return keys;
            };
            if let Some(diff) = crate_builds.iter().find(|diff| diff.host.name == krate) {
                return format!("{diff} — {keys}");
            }
            match same.iter().find(|info| info.name == krate) {
                Some(info) => format!(
                    "crate `{krate}` {} has the same features on both sides (`{}`) but resolved \
                     differently in the plugin's build through its dependency graph (one of its \
                     dependencies has other features or versions) — {keys}",
                    info.version,
                    info.feature_list().join(",")
                ),
                None => format!(
                    "crate `{krate}` resolved differently in the plugin's build (different \
                     features, version or dependency graph) — {keys}"
                ),
            }
        })
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
