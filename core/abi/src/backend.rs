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

/// A constant the graph gives one input of a region's node. `node` is the node's index in the
/// region. The value is the constant in the form a stored plan keeps it in, for the backend to
/// decode: `len` bytes at `value`, valid for the call.
#[repr(C)]
#[derive(Clone, Copy, Debug)]
pub struct ConstC {
    pub node: u32,
    pub reserved: u32,
    pub port: Text,
    pub value: *const u8,
    pub len: usize,
}

/// An edge between two nodes of a region whose value no node outside the region reads: the
/// producer's output `from_port` of node `from_node`, read by input `to_port` of node `to_node`.
/// Nodes are by index in the region.
#[repr(C)]
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub struct InnerEdgeC {
    pub from_node: u32,
    pub to_node: u32,
    pub from_port: u16,
    pub to_port: u16,
    pub reserved: u32,
}

/// A region as a backend sees it: its node ids in order, its boundary ports, the constants
/// of its nodes, and the edges inside it whose value stays inside it. `reason` is room for the words of a refusal: a backend that cannot compile the
/// region writes why, up to `reason_cap` bytes, and how many it wrote to `reason_len`.
#[repr(C)]
#[derive(Clone, Copy, Debug)]
pub struct RegionView {
    pub nodes: *const Text,
    pub node_count: usize,
    pub inputs: *const BoundaryPort,
    pub input_count: usize,
    pub outputs: *const BoundaryPort,
    pub output_count: usize,
    #[cfg(not(daedalus_abi_baseline))]
    pub consts: *const ConstC,
    #[cfg(not(daedalus_abi_baseline))]
    pub const_count: usize,
    #[cfg(not(daedalus_abi_baseline))]
    pub reason: *mut u8,
    #[cfg(not(daedalus_abi_baseline))]
    pub reason_cap: usize,
    #[cfg(not(daedalus_abi_baseline))]
    pub reason_len: *mut usize,
    #[cfg(not(daedalus_abi_baseline))]
    pub edges: *const InnerEdgeC,
    #[cfg(not(daedalus_abi_baseline))]
    pub edge_count: usize,
}

impl RegionView {
    /// A view of the nodes and ports alone: no constants, and nowhere to say why.
    pub fn of(nodes: &[Text], inputs: &[BoundaryPort], outputs: &[BoundaryPort]) -> RegionView {
        RegionView {
            nodes: nodes.as_ptr(),
            node_count: nodes.len(),
            inputs: inputs.as_ptr(),
            input_count: inputs.len(),
            outputs: outputs.as_ptr(),
            output_count: outputs.len(),
            #[cfg(not(daedalus_abi_baseline))]
            consts: core::ptr::null(),
            #[cfg(not(daedalus_abi_baseline))]
            const_count: 0,
            #[cfg(not(daedalus_abi_baseline))]
            reason: core::ptr::null_mut(),
            #[cfg(not(daedalus_abi_baseline))]
            reason_cap: 0,
            #[cfg(not(daedalus_abi_baseline))]
            reason_len: core::ptr::null_mut(),
            #[cfg(not(daedalus_abi_baseline))]
            edges: core::ptr::null(),
            #[cfg(not(daedalus_abi_baseline))]
            edge_count: 0,
        }
    }

    /// The view with the edges inside the region whose value stays inside it. A build from
    /// before a view had them leaves them out.
    #[allow(unused_mut, unused_variables)]
    pub fn with_edges(mut self, edges: &[InnerEdgeC]) -> RegionView {
        #[cfg(not(daedalus_abi_baseline))]
        {
            self.edges = edges.as_ptr();
            self.edge_count = edges.len();
        }
        self
    }

    /// The edges inside the region whose value stays inside it. Empty for a view that carries
    /// none.
    ///
    /// # Safety
    /// The view's `edges` must be valid for `edge_count` entries for `'a`.
    pub unsafe fn edges<'a>(&self) -> &'a [InnerEdgeC] {
        #[cfg(not(daedalus_abi_baseline))]
        if !self.edges.is_null() && self.edge_count != 0 {
            // SAFETY: the caller promises `edge_count` valid entries at `edges` for `'a`.
            return unsafe { core::slice::from_raw_parts(self.edges, self.edge_count) };
        }
        &[]
    }

    /// The view with the constants of its nodes. A build from before a view had them leaves
    /// them out.
    #[allow(unused_mut, unused_variables)]
    pub fn with_consts(mut self, consts: &[ConstC]) -> RegionView {
        #[cfg(not(daedalus_abi_baseline))]
        {
            self.consts = consts.as_ptr();
            self.const_count = consts.len();
        }
        self
    }

    /// The view with room for a refusal's words: `reason` for the bytes and `len` for how many
    /// were written.
    #[allow(unused_mut, unused_variables)]
    pub fn with_reason(mut self, reason: &mut [u8], len: &mut usize) -> RegionView {
        #[cfg(not(daedalus_abi_baseline))]
        {
            self.reason = reason.as_mut_ptr();
            self.reason_cap = reason.len();
            self.reason_len = core::ptr::from_mut(len);
        }
        self
    }

    /// The constants of the region's nodes. Empty for a view that carries none.
    ///
    /// # Safety
    /// The view's `consts` must be valid for `const_count` entries for `'a`.
    pub unsafe fn consts<'a>(&self) -> &'a [ConstC] {
        #[cfg(not(daedalus_abi_baseline))]
        if !self.consts.is_null() && self.const_count != 0 {
            // SAFETY: the caller promises `const_count` valid entries at `consts` for `'a`.
            return unsafe { core::slice::from_raw_parts(self.consts, self.const_count) };
        }
        &[]
    }

    /// Writes why the region is refused, as much as there is room for. Nothing is written
    /// when the view has no room.
    ///
    /// # Safety
    /// The view's `reason` must be writable for `reason_cap` bytes, and `reason_len` writable.
    #[allow(unused_variables)]
    pub unsafe fn refuse(&self, why: &str) {
        #[cfg(not(daedalus_abi_baseline))]
        if !self.reason.is_null() && !self.reason_len.is_null() {
            let mut end = why.len().min(self.reason_cap);
            while !why.is_char_boundary(end) {
                end -= 1;
            }
            // SAFETY: the caller promises `reason_cap` writable bytes at `reason`, `end` is no
            // more than that, and `reason_len` is writable.
            unsafe {
                core::ptr::copy_nonoverlapping(why.as_ptr(), self.reason, end);
                self.reason_len.write(end);
            }
        }
    }
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
