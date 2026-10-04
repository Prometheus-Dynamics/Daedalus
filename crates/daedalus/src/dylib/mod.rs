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
//! match the host exactly, and `install_into` fails with
//! [`PluginLibraryError::Incompatible`] (naming the mismatch) otherwise:
//!
//! 1. The Daedalus version ([`crate::version`]).
//! 2. The `rustc --version` string that compiled Daedalus ([`RUSTC_VERSION`]).
//! 3. A build fingerprint ([`build_fingerprint`]) covering the target, the enabled boundary
//!    Cargo features of the facade, core, data, registry, planner and runtime crates (listed
//!    readably plus a stable hash), and the size/alignment of the types that plugin
//!    installation touches (`PluginRegistry`, `HandlerRegistry`, `Payload`, `TypeKey`,
//!    `NodeDecl`, ...). Features classified as `host-only-features` in a crate's
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
//! These checks catch the common mismatches (stale plugin, different toolchain, different
//! feature set, separately built dependency). They cannot prove layout identity: build host and
//! plugins in the same cargo build (one workspace and lockfile, one toolchain, one boundary
//! Daedalus feature set: `plugins`, `gpu-types`/`gpu-runtime`/backends, `schema`, `proto`, and
//! every shared dependency resolved with the same features). `docs/dynamic-plugins.md` sketches
//! the stable handler path that would lift this requirement for wire-representable payloads.
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
mod fingerprint;
mod foreign;
mod loader;

pub use boundary::{BoundaryTypeEntry, BoundaryTypeTable, BoundaryTypesFn};
pub use daedalus_ffi_host::core::PluginSchema;
pub use fingerprint::{boundary_features, build_fingerprint, describe_fingerprint_mismatch};
pub use foreign::{ForeignInterfaceMismatch, ForeignInterfaceTable, ForeignInterfacesFn};
pub use loader::{
    PluginLibrary, PluginLibraryError, RustAbiMismatch, check_rust_abi, discover_plugin_libraries,
};

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
/// `boundary_types`; version 7 added `foreign_interfaces`.
pub const PLUGIN_ABI_VERSION: u32 = 7;
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
/// ABI version matches). `schema`, `boundary_types` and `foreign_interfaces` are always safe to call;
/// `register_boundary_contracts` and `register` only once [`check_rust_abi`] accepted `info`.
#[repr(C)]
#[derive(Copy, Clone, Debug)]
pub struct PluginDescriptor {
    pub info: PluginInfo,
    pub schema: SchemaFn,
    pub boundary_types: BoundaryTypesFn,
    pub foreign_interfaces: ForeignInterfacesFn,
    pub register_boundary_contracts: InstallFn,
    pub register: InstallFn,
}

#[doc(hidden)]
pub mod __support {
    use super::{BoundaryTypeTable, ForeignInterfaceTable, StrSink};
    use crate::runtime::plugins::{Plugin, PluginRegistry, PluginResult, RegistryPluginExt};
    use std::ffi::c_void;
    use std::panic::{AssertUnwindSafe, catch_unwind};

    /// Run `body`, reporting its error or panic through `sink` instead of unwinding across the
    /// FFI boundary.
    ///
    /// # Safety
    /// `sink` must be valid for the call.
    unsafe fn guarded(sink: StrSink, body: impl FnOnce() -> Result<(), String>) -> bool {
        let message = match catch_unwind(AssertUnwindSafe(body)) {
            Ok(Ok(())) => return true,
            Ok(Err(message)) => message,
            Err(payload) => format!(
                "plugin panicked: {}",
                crate::runtime::executor::panic_message(&*payload)
            ),
        };
        // Safety: forwarded from the caller.
        unsafe { sink.write(&message) };
        false
    }

    /// Run `install` against a host-provided registry pointer.
    ///
    /// # Safety
    /// `registry` must be null or a valid, exclusive pointer to a `PluginRegistry` with the
    /// same layout as this crate's, and `sink` must be valid for the call.
    pub unsafe fn install(
        registry: *mut c_void,
        sink: StrSink,
        install: impl FnOnce(&mut PluginRegistry) -> PluginResult<()>,
    ) -> bool {
        // Safety: forwarded from the caller.
        unsafe {
            guarded(sink, || {
                let registry = registry.cast::<PluginRegistry>().as_mut();
                let registry = registry.ok_or("registry pointer was null")?;
                install(registry).map_err(|err| err.to_string())
            })
        }
    }

    /// Install `P` into the registry (the `register` entry point).
    pub fn install_plugin<P: Plugin + Default>(registry: &mut PluginRegistry) -> PluginResult<()> {
        registry.install_plugin(&P::default())
    }

    /// Install `P` into a private registry and write its [`PluginSchema`](super::PluginSchema)
    /// as JSON to `sink`. Only this plugin's own code and types are involved.
    ///
    /// # Safety
    /// `sink` must be valid for the call.
    pub unsafe fn schema<P: Plugin + Default>(sink: StrSink) -> bool {
        // Safety: forwarded from the caller.
        unsafe {
            guarded(sink, || {
                let plugin = P::default();
                let mut registry = PluginRegistry::new();
                registry
                    .install_plugin(&plugin)
                    .map_err(|err| err.to_string())?;
                // Not `combined_transport_capabilities`: its validation fails on the plugin's
                // `deps`, which only the host's registry provides (and checks at install).
                let mut capabilities = registry.transport_capabilities.clone();
                for manifest in registry.plugin_manifests.values() {
                    if capabilities.plugin_manifest(&manifest.id).is_none() {
                        capabilities
                            .register_plugin(manifest.clone())
                            .map_err(|err| err.to_string())?;
                    }
                }
                let schema =
                    daedalus_ffi_host::export_registry_plugin_schema(&capabilities, plugin.id())
                        .map_err(|err| err.to_string())?;
                let json = serde_json::to_string(&schema).map_err(|err| err.to_string())?;
                sink.write(&json);
                Ok(())
            })
        }
    }

    /// Install `P` into a private registry and hand its
    /// [`boundary_types`](PluginRegistry::boundary_types) to the host as a `'static` table.
    ///
    /// # Safety
    /// `table` must be valid for writes and `sink` valid for the call.
    pub unsafe fn boundary_types<P: Plugin + Default>(
        table: *mut BoundaryTypeTable,
        sink: StrSink,
    ) -> bool {
        // Safety: forwarded from the caller.
        unsafe {
            guarded(sink, || {
                let table = table.as_mut().ok_or("table pointer was null")?;
                let mut registry = PluginRegistry::new();
                registry
                    .install_plugin(&P::default())
                    .map_err(|err| err.to_string())?;
                *table = super::boundary::leak_table(&registry);
                Ok(())
            })
        }
    }

    /// Install `P` into a private registry and hand its
    /// [`foreign_interfaces`](PluginRegistry::foreign_interfaces) to the host as a `'static`
    /// table.
    ///
    /// # Safety
    /// `table` must be valid for writes and `sink` valid for the call.
    pub unsafe fn foreign_interfaces<P: Plugin + Default>(
        table: *mut ForeignInterfaceTable,
        sink: StrSink,
    ) -> bool {
        // Safety: forwarded from the caller.
        unsafe {
            guarded(sink, || {
                let table = table.as_mut().ok_or("table pointer was null")?;
                let mut registry = PluginRegistry::new();
                registry
                    .install_plugin(&P::default())
                    .map_err(|err| err.to_string())?;
                *table = super::foreign::leak_table(&registry);
                Ok(())
            })
        }
    }
}

/// Export a [`Plugin`](crate::Plugin) implementor (which must also implement `Default`) for
/// dynamic loading from a `cdylib`.
///
/// Generates the `daedalus_plugin_abi_version` and `daedalus_plugin_descriptor` symbols that
/// [`PluginLibrary`] expects. The symbols are unmangled, so invoke it at most once per final
/// `cdylib`, including across plugin crates linked into it as `rlib`s.
///
/// ```ignore
/// #[derive(Default)]
/// struct DemoPlugin;
/// // impl daedalus::Plugin for DemoPlugin { ... }
///
/// daedalus::export_plugin!(DemoPlugin);
/// // or, with extra boundary contracts registered before the plugin installs:
/// daedalus::export_plugin!(DemoPlugin, boundary_contracts [my_contract()]);
/// ```
#[macro_export]
macro_rules! export_plugin {
    ($ty:ty) => {
        $crate::export_plugin!($ty, boundary_contracts []);
    };
    ($ty:ty, boundary_contracts [ $( $contract:expr ),* $(,)? ]) => {
        /// Returns the Daedalus dynamic plugin ABI version.
        #[unsafe(no_mangle)]
        pub extern "C" fn daedalus_plugin_abi_version() -> u32 {
            $crate::dylib::PLUGIN_ABI_VERSION
        }

        /// Returns this plugin's descriptor: metadata and entry points.
        #[unsafe(no_mangle)]
        pub extern "C" fn daedalus_plugin_descriptor() -> $crate::dylib::PluginDescriptor {
            use $crate::dylib::{__support, StrSink};
            use ::core::ffi::c_void;

            unsafe extern "C" fn schema(sink: StrSink) -> bool {
                // Safety: the host passes a sink valid for the call.
                unsafe { __support::schema::<$ty>(sink) }
            }

            unsafe extern "C" fn boundary_types(
                table: *mut $crate::dylib::BoundaryTypeTable,
                sink: StrSink,
            ) -> bool {
                // Safety: the host passes a writable table and a sink valid for the call.
                unsafe { __support::boundary_types::<$ty>(table, sink) }
            }

            unsafe extern "C" fn foreign_interfaces(
                table: *mut $crate::dylib::ForeignInterfaceTable,
                sink: StrSink,
            ) -> bool {
                // Safety: the host passes a writable table and a sink valid for the call.
                unsafe { __support::foreign_interfaces::<$ty>(table, sink) }
            }

            unsafe extern "C" fn register_boundary_contracts(
                registry: *mut c_void,
                sink: StrSink,
            ) -> bool {
                // Safety: the host passes a layout-compatible registry and a valid sink.
                unsafe {
                    __support::install(registry, sink, |__registry| {
                        $( __registry.register_boundary_contract($contract)?; )*
                        let _ = __registry;
                        Ok(())
                    })
                }
            }

            unsafe extern "C" fn register(registry: *mut c_void, sink: StrSink) -> bool {
                // Safety: the host passes a layout-compatible registry and a valid sink.
                unsafe { __support::install(registry, sink, __support::install_plugin::<$ty>) }
            }

            $crate::dylib::PluginDescriptor {
                info: $crate::dylib::PluginInfo::for_plugin(
                    env!("CARGO_PKG_NAME"),
                    env!("CARGO_PKG_VERSION"),
                ),
                schema,
                boundary_types,
                foreign_interfaces,
                register_boundary_contracts,
                register,
            }
        }
    };
}
