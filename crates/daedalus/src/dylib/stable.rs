//! The stable handler path: running a plugin's nodes through C-ABI entry points, so a plugin
//! built with another toolchain or Daedalus patch release (same
//! [`PLUGIN_ABI_VERSION`](super::PLUGIN_ABI_VERSION)) can be installed and run, not just
//! inspected.
//!
//! **Plugin side.** [`export_plugin!`](crate::export_plugin) adds [`StableHandlers`] to the
//! descriptor. The first [`CreateFn`] call builds a private registry (its linked dependencies,
//! then itself, with port codecs recorded, see
//! [`PluginRegistry::record_stable_codecs`](crate::PluginRegistry::record_stable_codecs)) and
//! resolves every schema node's ports once; [`UnloadFn`] drops it again. `create` makes the
//! state of one host node instance (its execution context, so its node state) and returns an
//! opaque handle; [`InvokeFn`] runs that instance with no lookup or lock, decoding the inputs
//! (ports by index into the schema node's `inputs`, plus a fan-in index) into payloads of the
//! handler's Rust types (builtin scalars directly, other types through the node port codecs the
//! node macros register: `DaedalusTypeExpr::from_value` or serde), running the handler and
//! encoding its outputs (by index into `outputs`; builtins directly, `Value`s, `ToValue` types
//! through the port codecs or the registry's value serializers). [`ReleaseFn`] frees an
//! instance. Errors and panics come back as a [`status`] code plus a message through the
//! [`StrSink`].
//!
//! **Values.** Inputs and outputs cross as [`StableValue`]s: scalars inline (no allocation),
//! strings, bytes and nested values borrowed for the duration of the call (one copy into the
//! receiving side's owned Rust value, no serialization), and frames or other foreign values as
//! borrowed [`ForeignHandle`](crate::transport::ForeignHandle)s that the receiver clones (zero
//! copy: the receiver reads the sender's buffer through the owner's vtable).
//! [`SharedBytes`](crate::SharedBytes) ports use that for byte buffers: the host lends its
//! buffer, and the plugin returns a slice of it or a buffer of its own, all without copying (see
//! [`crate::bytes`] for the lifetime rules); [`SharedFrame`](crate::transport::SharedFrame)
//! ports do the same for frames. A handle the sender's own build created (an owner payload its
//! provider lent, or a foreign payload marked
//! [`Payload::foreign_local`](crate::transport::Payload::foreign_local), as a frame the plugin
//! made is) carries [`tag::HANDLE_FROM_SENDER`]; the host then keeps the plugin loaded. A
//! plugin built against `daedalus:frame` v2 gets the host's v3 frames lent as v2.
//!
//! **Host side.** [`PluginLibrary::install_into`](super::PluginLibrary::install_into) uses this
//! path when [`rust_abi`](super::PluginLibrary::rust_abi) reports a mismatch and
//! [`StableHandlers::version`] equals [`STABLE_ABI_VERSION`]: it registers the schema's node
//! declarations ([`daedalus_ffi_host::node_decl_from_schema`]) and, per node, a handler that
//! calls the plugin's current generation (see the [`dylib`](super) docs, "Reloading"). The
//! handler keeps one instance per host node instance in its node state, with the instance's
//! input port indices cached by position and its call buffers reused; it encodes the inputs,
//! calls `invoke` and decodes the outputs (builtin port types into their Rust types,
//! `SharedBytes` in place, everything else as a `Value` payload under the port's key).

mod builtin;
mod host;
mod plugin;
mod value;

pub(super) use host::{Generation, PluginSlot, StablePlugin};
#[doc(hidden)]
pub use plugin::{
    StableRuntimeCell, create as __create, invoke as __invoke, release as __release,
    unload as __unload,
};
pub use value::{Arena, StableEnum, StableField, StableValue, tag, to_value};

use super::StrSink;
use std::ffi::c_void;

/// Version of the stable handler ABI ([`StableHandlers`], [`StableValue`], [`StableInput`],
/// [`StableOutputSink`], the [`status`] codes and the `#[repr(C)]` layouts of
/// `ForeignHandle`/`ForeignInterfaceInfo`). A plugin is installed through the stable path only
/// when its version equals the host's.
///
/// Version 2: per-instance handles ([`CreateFn`]), ports by index ([`StableInput::port`],
/// [`OutputPushFn`]), [`UnloadFn`], and the [`tag::HANDLE_FROM_SENDER`] flag.
pub const STABLE_ABI_VERSION: u32 = 2;

/// [`StableInput::fanin`] of an input that is not an indexed fan-in port.
pub const NO_FANIN: u32 = u32::MAX;

/// One node input, by port index into the schema node's `inputs`.
#[repr(C)]
#[derive(Copy, Clone, Debug)]
pub struct StableInput {
    /// The declared port (index into the schema node's `inputs`).
    pub port: u32,
    /// The fan-in index of an indexed port (`in3` of declared `in` is 3), else [`NO_FANIN`].
    pub fanin: u32,
    pub value: StableValue,
}

/// Receives one output (port index into the schema node's `outputs`, value borrowed for the
/// call); returns whether the host accepted it.
pub type OutputPushFn =
    unsafe extern "C" fn(ctx: *mut c_void, port: u32, value: *const StableValue) -> bool;

/// The host's output callback for one `invoke` call.
#[repr(C)]
#[derive(Copy, Clone, Debug)]
pub struct StableOutputSink {
    pub ctx: *mut c_void,
    pub push: Option<OutputPushFn>,
}

/// `create` and `invoke` results. Anything but [`OK`](status::OK) comes with a message.
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
    /// `NodeError::InputDropped`: the tick's input was unusable and is skipped (hosts that
    /// predate it read it as a failure).
    pub const INPUT_DROPPED: u32 = 6;
}

/// Creates the plugin-side state of one host node instance of schema node `node` (its
/// execution context and node state) and writes its handle to `instance`; returns a [`status`]
/// code, writing a message to `error` unless it is `OK`. The host passes the handle to
/// [`InvokeFn`] (never concurrently for one handle) and frees it with [`ReleaseFn`].
pub type CreateFn = unsafe extern "C" fn(node: u32, instance: *mut u64, error: StrSink) -> u32;

/// Runs the node of `instance` with `inputs`, pushing outputs to `outputs`; returns a
/// [`status`] code, writing a message to `error` unless it is `OK`.
pub type InvokeFn = unsafe extern "C" fn(
    instance: u64,
    inputs: *const StableInput,
    len: usize,
    outputs: StableOutputSink,
    error: StrSink,
) -> u32;

/// Drops the plugin-side state of `instance`.
pub type ReleaseFn = unsafe extern "C" fn(instance: u64);

/// Drops the plugin's stable runtime (its private registry). The host calls it once it released
/// every instance and is about to close the library; a later `create` builds a new runtime.
pub type UnloadFn = unsafe extern "C" fn();

/// The stable entry points in a [`PluginDescriptor`](super::PluginDescriptor).
#[repr(C)]
#[derive(Copy, Clone, Debug)]
pub struct StableHandlers {
    /// The plugin's [`STABLE_ABI_VERSION`].
    pub version: u32,
    pub create: CreateFn,
    pub invoke: InvokeFn,
    pub release: ReleaseFn,
    pub unload: UnloadFn,
}
