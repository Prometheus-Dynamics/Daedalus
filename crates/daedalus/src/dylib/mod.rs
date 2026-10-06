//! Native Rust plugins shipped as `cdylib` shared libraries.
//!
//! A plugin crate builds as a `cdylib` and invokes [`export_plugin!`](crate::export_plugin)
//! on a `Plugin + Default` type. A host loads the resulting `.so` / `.dylib` / `.dll` with
//! [`PluginLibrary::load`], inspects it, and installs it into a
//! [`PluginRegistry`](crate::PluginRegistry) with [`PluginLibrary::install_into`]. Both sides
//! enable the `dylib-plugins` feature.
//!
//! Only load-at-startup is supported: libraries are never unloaded (see below).
//!
//! # ABI layers
//!
//! **Stable descriptor (C ABI).** `daedalus_plugin_abi_version` is frozen as
//! `extern "C" fn() -> u32`. For a matching [`PLUGIN_ABI_VERSION`], `daedalus_plugin_descriptor`
//! returns a `#[repr(C)]` [`PluginDescriptor`] built only from C types: [`StrView`] strings
//! ([`PluginInfo`]) and `extern "C"` entry points. Its `schema` entry point serializes the
//! plugin's manifest and node declarations as a [`PluginSchema`] (JSON), the language-neutral
//! schema FFI packages use, computed entirely inside the plugin. Reading the descriptor and the
//! schema therefore does not depend on the toolchain, Daedalus version or feature set that
//! built the plugin: `load` accepts any plugin with a matching ABI version and exposes its
//! schema through [`PluginLibrary::schema`].
//!
//! **Rust-ABI install.** Node handlers are Rust closures over `Payload` (`Arc<dyn Any>`), so
//! installation passes a `*mut PluginRegistry` across the boundary and is only sound when host
//! and plugin agree on every Rust layout. [`check_rust_abi`] requires all of the following to
//! match the host exactly ([`PluginLibrary::rust_abi`] names the mismatch; forcing this path
//! with [`PluginLibrary::install_into_as`] then fails with
//! [`PluginLibraryError::Incompatible`]):
//!
//! 1. The Daedalus version ([`crate::version`]).
//! 2. The `rustc --version` string that compiled Daedalus ([`RUSTC_VERSION`]).
//! 3. A build fingerprint ([`build_fingerprint`]) covering the target, the enabled boundary
//!    Cargo features of the facade, core, transport, data, registry, planner and runtime crates
//!    (`std` included: it swaps lock and map types), listed readably plus a stable hash, and
//!    the size/alignment of the types that plugin installation touches (`PluginRegistry`,
//!    `HandlerRegistry`, `Payload`, `TypeKey`, `NodeDecl`, ...). Features classified as `host-only-features` in a crate's
//!    `[package.metadata.daedalus]` (`engine`, `executor-pool`, `metrics`, ...) are excluded,
//!    so a plugin built with just `dylib-plugins` installs into a host built with
//!    `engine-full,dylib-plugins`. On mismatch the error names the differing segments, e.g.
//!    ``features.runtime: host `gpu,plugins`, plugin `plugins` ``.
//!
//! The fingerprint is a readable `key=value;...` string: `target` and `pointer_width`,
//! `features.<crate>=...` per crate plus a stable FNV-1a `features.hash`, and
//! `layout.<type>=size/align`. Every feature of those crates is classified next to its
//! definition in the crate's `Cargo.toml`; a facade test fails when a declared feature is
//! unclassified or classified twice:
//!
//! ```toml
//! [package.metadata.daedalus]
//! boundary-features = ["gpu", "gpu-mock", "plugins"]
//! host-only-features = ["executor-pool", "lockfree-queues", "metrics", "snapshots"]
//! ```
//!
//! **Boundary types.** The fingerprint only covers Daedalus crates. A payload type from another
//! crate (a camera library's frame, say) is a different Rust type in a plugin whose cargo build
//! resolved that crate with other features, even with the same key and type name. The
//! descriptor's `boundary_types` entry point therefore exports a C-safe table
//! ([`BoundaryTypeTable`](crate::dylib::BoundaryTypeTable)) of every type key the plugin
//! consumes, produces or registers, with the Rust type behind it in the plugin's build
//! (`TypeId` hash, size, align, type name; see
//! [`PluginRegistry::boundary_types`](crate::PluginRegistry::boundary_types)).
//! [`PluginLibrary::install_into`] compares it with the host registry's entries for the same
//! keys and, before installing anything, fails with
//! [`PluginLibraryError::BoundaryTypeConflict`] listing every differing key. Keys the host does
//! not know are accepted, so the host should register the types it wraps in payloads itself
//! (install the owning crate's Daedalus plugin, or `register_boundary_type::<T>(key)`).
//!
//! The error groups the conflicts by the crate defining each type
//! ([`RustTypeIdentity::defining_crate`](crate::transport::RustTypeIdentity::defining_crate)).
//! Crates that register their build ([`CrateBuildInfo`](crate::CrateBuildInfo): name, version,
//! features; `#[plugin(.., crate_build)]` or
//! [`PluginRegistry::register_crate_build`](crate::PluginRegistry::register_crate_build)) are
//! also exported by the descriptor's `crate_builds` entry point
//! ([`CrateBuildTable`](crate::dylib::CrateBuildTable)); the error then names the exact version
//! and feature differences, and [`PluginLibrary::crate_build_diff`] lists them even when no type
//! conflicts.
//!
//! **Foreign interfaces.** A plugin that has to be built separately reads host-owned types
//! through a foreign interface instead of sharing them (node inputs typed `FrameView<'_>` or
//! `ForeignRef<'_, I>`; see [`crate::transport::ForeignInterface`]). Its ports carry the
//! interface key, not a Rust type, so they add no boundary type entry; instead the descriptor's
//! `foreign_interfaces` entry point exports a C-safe table
//! ([`ForeignInterfaceTable`](crate::dylib::ForeignInterfaceTable)) of the interfaces the plugin
//! uses (key, version, vtable layout hash). `install_into` fails with
//! [`PluginLibraryError::ForeignInterfaceMismatch`] when the host registry uses one of those keys
//! with another version or layout.
//!
//! **Dependencies.** The schema lists the plugins the plugin depends on (`#[plugin(deps(...))]`
//! and the plugins linked with `export_plugin!(.., deps [..])`); `install_into` fails with
//! [`PluginLibraryError::MissingDependencies`] unless the host registry installed them first.
//! The introspection entry points install the linked plugins before the plugin into their
//! private registry, so keys they own or map resolve there; port types from other crates that
//! still have no key are recorded as `external_types` in the schema's plugin metadata instead of
//! failing (installing fails on them).
//!
//! These checks catch the common mismatches (stale plugin, different toolchain, different
//! feature set, separately built dependency). They cannot prove layout identity: build host and
//! plugins in the same cargo build (one workspace and lockfile, one toolchain, one boundary
//! Daedalus feature set: `plugins`, `gpu-types`/`gpu-runtime`/backends, `schema`, `proto`, and
//! every shared dependency resolved with the same features), or let mismatched plugins take the
//! stable path below.
//!
//! **Stable install.** A plugin the Rust-ABI checks refuse still installs when its
//! [`StableHandlers::version`](crate::dylib::StableHandlers::version) equals
//! [`STABLE_ABI_VERSION`](crate::dylib::STABLE_ABI_VERSION): [`PluginLibrary::install_into`]
//! registers its schema's nodes with handlers that call the plugin's C-ABI `invoke`, exchanging
//! [`StableValue`](crate::dylib::StableValue)s (scalars inline, strings/bytes/nested values
//! borrowed, frames as foreign handles) instead of Rust types, so no boundary type check applies
//! (foreign interface and dependency checks still do). Only nodes install that way;
//! [`InstallPath`](crate::dylib::InstallPath) reports and
//! [`PluginLibrary::install_into_as`] chooses the path. See [`stable`](crate::dylib::stable) for
//! the protocol and `docs/dynamic-plugins.md` ("Install Paths") for what crosses and what it
//! costs.
//!
//! # Known limitations
//!
//! - **Separate process globals.** A `cdylib` statically links its own copy of Daedalus
//!   (and of `std`). Any process-global state inside Daedalus — e.g. the global boundary
//!   contract registry, global type/serializer registries, `OnceLock`/`static` caches — exists
//!   twice: once in the host and once in each plugin. Writes the plugin makes to its globals
//!   are invisible to the host. Plugins must register *everything* through the
//!   `PluginRegistry` passed to them (which is what `declare_plugin!`/`Plugin::install`
//!   already do).
//! - **Allocator.** Values allocated inside the plugin (strings, boxed handlers, ...) are
//!   dropped by the host. Both sides must therefore use the same, shared allocator: keep the
//!   default system allocator and do not install a `#[global_allocator]` in the host or the
//!   plugins. (The descriptor layer only passes borrowed bytes and needs no shared allocator.)
//! - **Type identity.** `TypeId`s can differ between the host and a plugin whenever Cargo
//!   compiles the defining crate with different metadata (different feature sets or dependency
//!   graphs). Keys identify types at the boundary; the boundary type check above rejects keys
//!   whose Rust types differ, and payload downcasts that still fail report
//!   [`TransportError::RustTypeMismatch`](crate::transport::TransportError::RustTypeMismatch).
//!   This applies to Daedalus's own types too: a plugin built in another cargo invocation can
//!   pass the fingerprint check yet see other `TypeId`s for Daedalus types (any dependency of
//!   Daedalus resolved with other features changes them). Payload accessors therefore never rely
//!   on the storage wrapper's `TypeId` alone (`Payload::get_ref` falls back to the value's own
//!   `Any`, foreign handles are reached through a trait method), and shared third-party types
//!   cross through foreign interfaces.
//! - **No unloading.** Loaded libraries are intentionally leaked. Registered handlers,
//!   vtables and `&'static str`s point into the library's code and data for as long as the
//!   registry (and anything built from it) lives; unloading would leave them dangling.
//!   Unloading Rust `cdylib`s is also unreliable in general (thread-locals with destructors).

mod boundary;
mod crate_builds;
mod extract;
mod fingerprint;
mod foreign;
mod loader;
pub mod stable;

pub use boundary::{BoundaryTypeEntry, BoundaryTypeTable, BoundaryTypesFn};
pub use crate_builds::{CrateBuildEntry, CrateBuildTable, CrateBuildsFn};
pub use daedalus_ffi_host::core::PluginSchema;
pub use fingerprint::{boundary_features, build_fingerprint, describe_fingerprint_mismatch};
pub use foreign::{ForeignInterfaceMismatch, ForeignInterfaceTable, ForeignInterfacesFn};
pub use loader::{
    InstallPath, PluginLibrary, PluginLibraryError, RustAbiMismatch, check_rust_abi,
    discover_plugin_libraries,
};
pub use stable::{STABLE_ABI_VERSION, StableHandlers, StableInput, StableOutputSink, StableValue};

use std::ffi::c_void;

/// Symbol exported by dynamic plugins that returns [`PLUGIN_ABI_VERSION`]. Its signature,
/// `extern "C" fn() -> u32`, never changes.
pub const PLUGIN_ABI_SYMBOL: &str = "daedalus_plugin_abi_version";
/// Symbol exported by dynamic plugins that returns their [`PluginDescriptor`].
pub const PLUGIN_DESCRIPTOR_SYMBOL: &str = "daedalus_plugin_descriptor";
/// Version of the [`PluginDescriptor`] format.
///
/// Bumped whenever the descriptor (or anything it contains) changes shape.
/// Version 5 introduced the descriptor and its stable `schema` entry point; version 6 added
/// `boundary_types`; version 7 added `foreign_interfaces`; version 8 added `stable` (the stable
/// handler path, versioned on its own by [`STABLE_ABI_VERSION`]); version 9 added
/// `crate_builds`.
pub const PLUGIN_ABI_VERSION: u32 = 9;
/// `rustc --version` of the compiler that built this copy of Daedalus.
pub const RUSTC_VERSION: &str = env!("DAEDALUS_RUSTC_VERSION");

/// FFI-safe view of a `'static` UTF-8 string.
///
/// Can only be built from `&'static str`, so views returned by a plugin stay valid for the
/// process lifetime (plugin libraries are never unloaded).
#[repr(C)]
#[derive(Copy, Clone, Debug)]
pub struct StrView {
    ptr: *const u8,
    len: usize,
}

// Safety: a `StrView` only points to immutable `'static` string data.
unsafe impl Send for StrView {}
// Safety: see above.
unsafe impl Sync for StrView {}

impl StrView {
    /// Wrap a static string.
    pub const fn from_static(value: &'static str) -> Self {
        Self {
            ptr: value.as_ptr(),
            len: value.len(),
        }
    }

    /// Return the string, or `None` if the view is null or not valid UTF-8.
    pub fn as_str(&self) -> Option<&'static str> {
        if self.ptr.is_null() {
            return None;
        }
        // Safety: `StrView` can only be constructed from a `&'static str` (in this crate or in
        // a plugin's copy of it), and plugin libraries are never unloaded.
        let bytes = unsafe { std::slice::from_raw_parts(self.ptr, self.len) };
        std::str::from_utf8(bytes).ok()
    }
}

impl From<&'static str> for StrView {
    fn from(value: &'static str) -> Self {
        Self::from_static(value)
    }
}

/// C-safe callback through which a plugin entry point hands a string (a schema or an error
/// message) to the host. The bytes are only borrowed for the duration of `write`.
#[repr(C)]
#[derive(Copy, Clone, Debug)]
pub struct StrSink {
    pub ctx: *mut c_void,
    pub write: Option<unsafe extern "C" fn(ctx: *mut c_void, ptr: *const u8, len: usize)>,
}

impl StrSink {
    /// Hand `value` to the host.
    ///
    /// # Safety
    /// `ctx` and `write` must be the pair provided by the host for the current call.
    pub unsafe fn write(&self, value: &str) {
        if let Some(write) = self.write {
            // Safety: guaranteed by the caller.
            unsafe { write(self.ctx, value.as_ptr(), value.len()) };
        }
    }
}

/// Metadata a dynamic plugin reports about itself and the Daedalus build it was compiled with.
#[repr(C)]
#[derive(Copy, Clone, Debug)]
pub struct PluginInfo {
    pub plugin_name: StrView,
    pub plugin_version: StrView,
    pub daedalus_version: StrView,
    pub rustc_version: StrView,
    pub build_fingerprint: StrView,
}

impl PluginInfo {
    /// Info for a plugin built against this copy of Daedalus.
    pub fn for_plugin(plugin_name: &'static str, plugin_version: &'static str) -> Self {
        Self {
            plugin_name: StrView::from_static(plugin_name),
            plugin_version: StrView::from_static(plugin_version),
            daedalus_version: StrView::from_static(crate::version()),
            rustc_version: StrView::from_static(RUSTC_VERSION),
            build_fingerprint: StrView::from_static(build_fingerprint()),
        }
    }
}

/// Writes the plugin's [`PluginSchema`] as JSON (or an error message) to the sink; returns
/// whether it succeeded.
pub type SchemaFn = unsafe extern "C" fn(sink: StrSink) -> bool;
/// Installs into a `*mut PluginRegistry` (Rust ABI), reporting errors to the sink; returns
/// whether it succeeded.
pub type InstallFn = unsafe extern "C" fn(registry: *mut c_void, sink: StrSink) -> bool;

/// Everything a dynamic plugin exports, returned by `daedalus_plugin_descriptor`.
///
/// Built only from C types, so a host can read it whatever toolchain built the plugin (once the
/// ABI version matches). `schema`, `boundary_types`, `foreign_interfaces` and `crate_builds` are
/// always safe to call; `register_boundary_contracts` and `register` only once
/// [`check_rust_abi`] accepted `info`; `stable` once its version matches.
#[repr(C)]
#[derive(Copy, Clone, Debug)]
pub struct PluginDescriptor {
    pub info: PluginInfo,
    pub schema: SchemaFn,
    pub boundary_types: BoundaryTypesFn,
    pub foreign_interfaces: ForeignInterfacesFn,
    pub crate_builds: CrateBuildsFn,
    pub register_boundary_contracts: InstallFn,
    pub register: InstallFn,
    /// The stable handler entry points, callable once [`StableHandlers::version`] matches the
    /// host's [`STABLE_ABI_VERSION`] (see [`stable`]).
    pub stable: StableHandlers,
}

#[doc(hidden)]
#[path = "support.rs"]
pub mod __support;
