//! The shapes a plugin's nodes and backends cross the boundary in. Everything here is `repr(C)`,
//! and [`crate::SHAPES`] covers each one, so a host and a plugin built apart refuse each other
//! before they read a field they don't share.
//!
//! A plugin exports two symbols. `daedalus_plugin_shapes(out)` writes its shape descriptors,
//! which the host checks against its own with [`crate::fits`]. Only when they fit does the host
//! call `daedalus_plugin_describe(out, room)`, which writes the plugin's entry. So the host
//! never reads a struct it can't trust, and a plugin built against an older Daedalus, with
//! fewer fields at the end of a growable shape, still loads.

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

/// Turns one byte buffer into another, with the all-or-nothing buffer pattern of `write_out` (see `out.rs`): with
/// `cap` too small it sets `needed` and writes nothing.
pub type ByteCodecFn = unsafe extern "C" fn(
    input: *const u8,
    input_len: usize,
    out: *mut u8,
    cap: usize,
    needed: *mut usize,
) -> Status;

/// A codec a plugin declares for one of its plain-data types. It converts the type's bytes to and
/// from two other forms: the host's value form, named by `value_key`, and the wire form, named by
/// `wire_key`. Each conversion is a function over bytes, and each may refuse its input.
#[repr(C)]
#[derive(Clone, Copy, Debug)]
pub struct CodecC {
    /// The plugin's type key.
    pub key: Text,
    /// The host value type the `to_value` and `from_value` functions convert to and from.
    pub value_key: Text,
    /// The wire type the `to_bytes` and `from_bytes` functions convert to and from.
    pub wire_key: Text,
    /// The backend the conversion nodes name.
    pub backend: Text,
    /// The layout hash of the plugin's type.
    pub layout: u64,
    /// The cost of a conversion, for the planner.
    pub cost: u32,
    /// Whether a conversion can lose information.
    pub lossy: bool,
    /// The plugin's bytes to the host value form.
    pub to_value: ByteCodecFn,
    /// The host value form to the plugin's bytes.
    pub from_value: ByteCodecFn,
    /// The plugin's bytes to the wire form.
    pub to_bytes: ByteCodecFn,
    /// The wire form to the plugin's bytes.
    pub from_bytes: ByteCodecFn,
}

/// Frees what a plugin's entry points to: its node, port and backend tables. The host calls it
/// once, after it has copied every table, and does not read the entry again.
pub type ReleaseEntryFn = unsafe extern "C" fn(entry: *const PluginEntry);

/// Frees bytes a plugin handed out, given the `data` it handed out with them. The host calls it once,
/// when the last host value of those bytes is dropped, and the plugin's library is still loaded then.
pub type BytesReleaseFn = unsafe extern "C" fn(data: *mut ());

/// Bytes a plugin hands the host without a copy. The bytes at `ptr` stay valid and unchanged for `len`
/// bytes until `release` is called with `data`. A plugin that hands out no bytes leaves `release` unset.
#[repr(C)]
#[derive(Clone, Copy, Debug)]
pub struct BytesC {
    pub ptr: *const u8,
    pub len: usize,
    pub data: *mut (),
    pub release: Option<BytesReleaseFn>,
}

/// Bytes that are none: the value a hand-over slot starts from.
pub const NO_BYTES: BytesC = BytesC {
    ptr: core::ptr::null(),
    len: 0,
    data: core::ptr::null_mut(),
    release: None,
};

/// Makes the bytes for `index`, into `out`, which the host owns. Writes `out` only on OK. A plugin
/// returns NOT_SUPPORTED for an index it does not make.
pub type ProduceBytesFn = unsafe extern "C" fn(index: u32, out: *mut BytesC) -> Status;

/// What a plugin declares. The loader copies each string into host-owned storage at load, then
/// calls `release` so the tables do not outlive the load.
#[repr(C)]
#[derive(Clone, Copy, Debug)]
pub struct PluginEntry {
    pub name: Text,
    pub nodes: *const NodeC,
    pub node_count: usize,
    pub backends: *const BackendTable,
    pub backend_count: usize,
    pub codecs: *const CodecC,
    pub codec_count: usize,
    /// Makes bytes for the host, without a copy. Unset for a plugin that makes none.
    pub produce: Option<ProduceBytesFn>,
    /// Frees the tables above. A plugin that leaves it unset keeps its tables for its lifetime.
    pub release: Option<ReleaseEntryFn>,
}

/// The entry point's signature. `out` points at `room` zeroed bytes the caller owns, at least a
/// `PluginEntry` as the plugin has it: the host checked the plugin's shapes first. A plugin that
/// is somehow given less room writes nothing and refuses.
pub type EntryFn = unsafe extern "C" fn(out: *mut PluginEntry, room: usize) -> Status;

/// The symbol a plugin exports its entry point under, NUL-terminated for `dlsym`.
pub const ENTRY_SYMBOL: &[u8] = b"daedalus_plugin_describe\0";

/// Writes the plugin's shape descriptors to `out`. The host calls it before anything else, and
/// calls nothing else of a plugin whose shapes don't fit.
pub type ShapesFn = unsafe extern "C" fn(out: *mut crate::AbiDesc) -> Status;

/// The symbol a plugin exports [`ShapesFn`] under, NUL-terminated for `dlsym`.
pub const SHAPES_SYMBOL: &[u8] = b"daedalus_plugin_shapes\0";

/// The entry symbol of plugins built before plugins described their shapes. A library that has
/// it and not [`SHAPES_SYMBOL`] has to be built again, once.
pub const OLD_ENTRY_SYMBOL: &[u8] = b"daedalus_plugin_entry\0";

// SAFETY: each is a growable shape whose fields are text, pointers, counts and functions that
// every build has: none has been added since, so there is nothing to be zero yet. A field added
// later has to be one that is valid, and means absent, as zeroes.
unsafe impl crate::Grown for NodeC {
    const ID: u32 = 17;
}
// SAFETY: as above.
unsafe impl crate::Grown for BackendTable {
    const ID: u32 = 21;
}
// SAFETY: as above.
unsafe impl crate::Grown for CodecC {
    const ID: u32 = 22;
}
