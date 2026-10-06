//! The C-safe table of third-party crate builds a plugin exports
//! ([`PluginRegistry::crate_builds`]), which the host compares with its own
//! ([`PluginLibrary::crate_build_diff`](super::PluginLibrary::crate_build_diff)) to name the
//! exact version and feature differences behind boundary type conflicts.

use super::{StrSink, StrView};
use crate::runtime::plugins::{CrateBuildInfo, PluginRegistry};

/// One [`CrateBuildInfo`] in C-safe form.
#[repr(C)]
#[derive(Copy, Clone, Debug)]
pub struct CrateBuildEntry {
    pub name: StrView,
    pub version: StrView,
    /// Comma-separated enabled features.
    pub features: StrView,
}

/// A `'static` array of [`CrateBuildEntry`] owned by the plugin.
#[repr(C)]
#[derive(Copy, Clone, Debug)]
pub struct CrateBuildTable {
    pub entries: *const CrateBuildEntry,
    pub len: usize,
}

/// Fills `table` with the plugin's crate builds (or writes an error to the sink); returns
/// whether it succeeded.
pub type CrateBuildsFn = unsafe extern "C" fn(table: *mut CrateBuildTable, sink: StrSink) -> bool;

/// Read a table returned by a plugin's [`CrateBuildsFn`].
///
/// # Safety
/// `table` must point to `len` entries that stay valid for the process lifetime.
pub(super) unsafe fn read_table(table: CrateBuildTable) -> Result<Vec<CrateBuildInfo>, String> {
    if table.len == 0 {
        return Ok(Vec::new());
    }
    if table.entries.is_null() {
        return Err("crate build table is null".to_string());
    }
    // Safety: guaranteed by the caller.
    let entries = unsafe { std::slice::from_raw_parts(table.entries, table.len) };
    entries
        .iter()
        .map(|entry| {
            let text = |view: StrView, field: &str| {
                view.as_str()
                    .ok_or_else(|| format!("crate build {field} is null or not valid UTF-8"))
            };
            Ok(CrateBuildInfo {
                name: text(entry.name, "name")?,
                version: text(entry.version, "version")?,
                features: text(entry.features, "features")?,
            })
        })
        .collect()
}

/// Plugin side: leak `registry`'s crate builds as a `'static` table.
pub(super) fn leak_table(registry: &PluginRegistry) -> CrateBuildTable {
    let entries: &'static [CrateBuildEntry] = registry
        .crate_builds()
        .values()
        .map(|info| CrateBuildEntry {
            name: StrView::from_static(info.name),
            version: StrView::from_static(info.version),
            features: StrView::from_static(info.features),
        })
        .collect::<Vec<_>>()
        .leak();
    CrateBuildTable {
        entries: entries.as_ptr(),
        len: entries.len(),
    }
}
