//! The stable handler path: running a plugin's nodes through C-ABI entry points, so a plugin
//! built with another toolchain or Daedalus patch release (same [`PLUGIN_ABI_VERSION`]) can be
//! installed and run, not just inspected.
//!
//! **Plugin side.** [`export_plugin!`](crate::export_plugin) adds [`StableHandlers`] to the
//! descriptor. On the first [`InvokeFn`] call the plugin builds a private registry (its linked
//! dependencies, then itself, with port codecs recorded, see
//! [`PluginRegistry::record_stable_codecs`](crate::PluginRegistry::record_stable_codecs)) and
//! keeps it for the process. Each call names a node by its index in the plugin's
//! [`PluginSchema`](super::PluginSchema) `nodes`, decodes the inputs into payloads of the
//! handler's Rust types (builtin scalars directly, other types through the node port codecs the
//! node macros register: `DaedalusTypeExpr::from_value` or serde), runs the handler and encodes
//! its outputs (builtins directly, `Value`s, `ToValue` types through the port codecs or the
//! registry's value serializers). Node state stays in the plugin, in one state store per host
//! node instance (`instance`, released with [`ReleaseFn`]). Errors and panics come back as a
//! [`status`] code plus a message through the [`StrSink`].
//!
//! **Values.** Inputs and outputs cross as [`StableValue`]s: scalars inline (no allocation),
//! strings, bytes and nested values borrowed for the duration of the call (one copy into the
//! receiving side's owned Rust value, no serialization), and frames or other host-owned values
//! as borrowed [`ForeignHandle`](crate::transport::ForeignHandle)s that the receiver clones
//! (zero copy: the plugin reads the host's buffer through the owner's vtable).
//!
//! **Host side.** [`PluginLibrary::install_into`](super::PluginLibrary::install_into) uses this
//! path when [`rust_abi`](super::PluginLibrary::rust_abi) reports a mismatch and
//! [`StableHandlers::version`] equals [`STABLE_ABI_VERSION`]: it registers the schema's node
//! declarations ([`daedalus_ffi_host::node_decl_from_schema`]) and, per node, a handler that
//! encodes the inputs, calls `invoke` and decodes the outputs (builtin port types into their
//! Rust types, everything else as a `Value` payload under the port's key).

mod builtin;
mod host;
mod plugin;
mod value;

pub(super) use host::StablePlugin;
#[doc(hidden)]
pub use plugin::{StableRuntimeCell, invoke as __invoke, release as __release};
pub use value::{Arena, StableEnum, StableField, StableValue, tag, to_value};

use super::StrSink;
use std::ffi::c_void;

/// Version of the stable handler ABI ([`StableHandlers`], [`StableValue`], [`StableInput`],
/// [`StableOutputSink`], the [`status`] codes and the `#[repr(C)]` layouts of
/// `ForeignHandle`/`ForeignInterfaceInfo`). A plugin is installed through the stable path only
/// when its version equals the host's.
pub const STABLE_ABI_VERSION: u32 = 1;

/// One node input: borrowed UTF-8 port name and value.
#[repr(C)]
#[derive(Copy, Clone, Debug)]
pub struct StableInput {
    pub port_ptr: *const u8,
    pub port_len: usize,
    pub value: StableValue,
}

/// Receives one output (port name and value, both borrowed for the call); returns whether the
/// host accepted it.
pub type OutputPushFn = unsafe extern "C" fn(
    ctx: *mut c_void,
    port_ptr: *const u8,
    port_len: usize,
    value: *const StableValue,
) -> bool;

/// The host's output callback for one `invoke` call.
#[repr(C)]
#[derive(Copy, Clone, Debug)]
pub struct StableOutputSink {
    pub ctx: *mut c_void,
    pub push: Option<OutputPushFn>,
}

/// `invoke` results. Anything but [`OK`](status::OK) comes with a message.
pub mod status {
    pub const OK: u32 = 0;
    /// The handler failed (`NodeError::Handler`).
    pub const HANDLER_ERROR: u32 = 1;
    /// An input was missing or could not be converted (`NodeError::InvalidInput`).
    pub const INVALID_INPUT: u32 = 2;
    /// `NodeError::BackpressureDrop`.
    pub const BACKPRESSURE: u32 = 3;
    /// The handler panicked; the plugin caught it.
    pub const PANIC: u32 = 4;
    /// Anything else: unknown node, plugin registry failed to build, output rejected.
    pub const FAILED: u32 = 5;
}

/// Runs schema node `node` for host node instance `instance` with `inputs`, pushing outputs to
/// `outputs`; returns a [`status`] code, writing a message to `error` unless it is `OK`.
pub type InvokeFn = unsafe extern "C" fn(
    node: u32,
    instance: u64,
    inputs: *const StableInput,
    len: usize,
    outputs: StableOutputSink,
    error: StrSink,
) -> u32;

/// Drops the plugin-side state of host node instance `instance`.
pub type ReleaseFn = unsafe extern "C" fn(instance: u64);

/// The stable entry points in a [`PluginDescriptor`](super::PluginDescriptor).
#[repr(C)]
#[derive(Copy, Clone, Debug)]
pub struct StableHandlers {
    /// The plugin's [`STABLE_ABI_VERSION`].
    pub version: u32,
    pub invoke: InvokeFn,
    pub release: ReleaseFn,
}
