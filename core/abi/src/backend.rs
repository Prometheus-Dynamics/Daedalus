//! The shapes a plugin's nodes and backends cross the boundary in. Everything here is `repr(C)`,
//! and [`crate::SHAPES`] covers each one, so a host and a plugin built apart refuse each other
//! before they read a field they don't share.
//!
//! The entry point is `daedalus_plugin_entry(host_layout, out)`. It checks the host's layout hash
//! first. On a mismatch it writes only its own hash, into the first field, and returns
//! [`Status::VERSION_MISMATCH`], so the host never reads the rest of a struct it can't trust.

use core::ptr;

use crate::{Status, Step};

/// Text a plugin hands the host: an address and a length. The bytes must stay valid, unchanged,
/// for as long as the plugin's library is loaded.
#[repr(C)]
#[derive(Clone, Copy, Debug)]
pub struct Text {
    pub ptr: *const u8,
    pub len: usize,
}

// SAFETY: a `Text` is an address and a length of immutable bytes. Nothing writes through it.
unsafe impl Send for Text {}
// SAFETY: as above: shared reads of immutable bytes are safe from any thread.
unsafe impl Sync for Text {}

impl Text {
    pub const EMPTY: Text = Text {
        ptr: ptr::null(),
        len: 0,
    };

    pub const fn of(text: &'static str) -> Text {
        Text {
            ptr: text.as_ptr(),
            len: text.len(),
        }
    }

    /// Reads the text.
    ///
    /// # Safety
    /// `ptr` and `len` must describe valid UTF-8 that stays valid and unchanged for `'a`.
    pub unsafe fn as_str<'a>(self) -> &'a str {
        if self.len == 0 {
            return "";
        }
        // SAFETY: the caller promises the bytes are valid UTF-8 for `'a`, and a non-empty text
        // has a non-null pointer.
        unsafe { core::str::from_utf8_unchecked(core::slice::from_raw_parts(self.ptr, self.len)) }
    }
}

/// One port of a node: its name, its type key, and whether it may stay unwired.
#[repr(C)]
#[derive(Clone, Copy, Debug)]
pub struct PortC {
    pub name: Text,
    pub key: Text,
    pub optional: bool,
}

/// One node a plugin declares.
#[repr(C)]
#[derive(Clone, Copy, Debug)]
pub struct NodeC {
    pub id: Text,
    pub source: Text,
    /// The backend id this node's body is for.
    pub backend: Text,
    pub text: Text,
    pub inputs: *const PortC,
    pub input_count: usize,
    pub outputs: *const PortC,
    pub output_count: usize,
}

/// One port of a region as a backend sees it. `node` is the node's index in the region.
#[repr(C)]
#[derive(Clone, Copy, Debug)]
pub struct BoundaryPort {
    pub node: u32,
    pub port: u16,
    pub reserved: u16,
    pub type_index: u32,
    pub slot: u32,
}

/// A region as a backend sees it: its node ids in order, and its boundary ports.
#[repr(C)]
#[derive(Clone, Copy, Debug)]
pub struct RegionView {
    pub nodes: *const Text,
    pub node_count: usize,
    pub inputs: *const BoundaryPort,
    pub input_count: usize,
    pub outputs: *const BoundaryPort,
    pub output_count: usize,
}

/// One unit of compiled work: a step, and the index of the region node it runs.
#[repr(C)]
#[derive(Debug)]
pub struct UnitOut {
    pub step: Step,
    pub node: u32,
    pub reserved: u32,
}

/// Writes one byte per region node: 1 when the backend takes it. Uses the all-or-nothing buffer
/// pattern: with `cap` too small it sets `needed` and writes nothing.
pub type CanTakeFn = unsafe extern "C" fn(
    view: *const RegionView,
    out: *mut u8,
    cap: usize,
    needed: *mut usize,
) -> Status;

/// Writes one [`UnitOut`] per taken node, with the same buffer pattern as [`CanTakeFn`].
pub type CompileFn = unsafe extern "C" fn(
    view: *const RegionView,
    out: *mut UnitOut,
    cap: usize,
    needed: *mut usize,
) -> Status;

/// A backend a plugin provides.
#[repr(C)]
#[derive(Clone, Copy, Debug)]
pub struct BackendTable {
    pub id: Text,
    pub can_take: CanTakeFn,
    pub compile: CompileFn,
}

/// Frees what a plugin's entry points to: its node, port and backend tables. The host calls it
/// once, after it has copied every table, and does not read the entry again.
pub type ReleaseEntryFn = unsafe extern "C" fn(entry: *const PluginEntry);

/// What a plugin declares. The loader copies each string into host-owned storage at load, then
/// calls `release` so the tables do not outlive the load.
#[repr(C)]
#[derive(Clone, Copy, Debug)]
pub struct PluginEntry {
    /// Must be first: a mismatch is reported through this field alone.
    pub layout: u64,
    pub name: Text,
    pub nodes: *const NodeC,
    pub node_count: usize,
    pub backends: *const BackendTable,
    pub backend_count: usize,
    /// Frees the tables above. A plugin that leaves it unset keeps its tables for its lifetime.
    pub release: Option<ReleaseEntryFn>,
}

/// The entry point's signature. `out` must point at a `PluginEntry` the caller owns.
pub type EntryFn = unsafe extern "C" fn(host_layout: u64, out: *mut PluginEntry) -> Status;

/// The symbol a plugin exports its entry point under, NUL-terminated for `dlsym`.
pub const ENTRY_SYMBOL: &[u8] = b"daedalus_plugin_entry\0";
