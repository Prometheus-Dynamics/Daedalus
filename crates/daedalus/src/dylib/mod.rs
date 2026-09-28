//! Native Rust plugins shipped as `cdylib` shared libraries.
//!
//! A plugin crate builds as a `cdylib` and invokes [`export_plugin!`](crate::export_plugin)
//! on a `Plugin + Default` type. A host enables the `dylib-plugins` feature and loads the
//! resulting `.so` / `.dylib` / `.dll` with `PluginLibrary::load`, then installs it into a
//! [`PluginRegistry`](crate::PluginRegistry) with `PluginLibrary::install_into`.
//!
//! Only load-at-startup is supported: libraries are never unloaded (see below).
//!
//! # Compatibility checks
//!
//! Rust has no stable ABI, and `daedalus_plugin_register` passes a `&mut PluginRegistry`
//! across the library boundary. Before any Rust type crosses that boundary the loader calls
//! C-safe symbols and rejects the plugin with a typed `PluginLibraryError` unless all of
//! the following match the host exactly:
//!
//! 1. [`PLUGIN_ABI_VERSION`] (the shape of the exported symbols and [`PluginInfo`]).
//! 2. The Daedalus version ([`crate::version`]).
//! 3. The `rustc --version` string that compiled Daedalus ([`RUSTC_VERSION`]).
//! 4. A build fingerprint ([`build_fingerprint`]) covering the target, the enabled
//!    boundary-relevant Cargo features of the facade, core, data, registry, planner and runtime
//!    crates (listed readably plus a stable hash), and the size/alignment of the types that
//!    plugin installation touches (`PluginRegistry`, `HandlerRegistry`, `Payload`, `TypeKey`,
//!    `NodeDecl`, ...). Host-only features such as `engine`, `executor-pool` and `metrics`
//!    ([`HOST_ONLY_FEATURES`](crate::dylib::HOST_ONLY_FEATURES)) are excluded, so a plugin
//!    built with just `plugins` loads into a host built with `engine-full`. On mismatch the
//!    error names the differing segments.
//!
//! These checks catch the common mismatches (stale plugin, different toolchain, different
//! feature set). They cannot prove layout identity: build host and plugins from the same
//! workspace/lockfile, toolchain, and Daedalus feature set.
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
//!   plugins.
//! - **Type identity.** `TypeId`s of Daedalus-defined types can differ between the host and
//!   a plugin when Cargo compiles Daedalus with different metadata (different feature sets
//!   or dependency graphs). Prefer transport type keys over Rust `TypeId` at the boundary.
//! - **No unloading.** Loaded libraries are intentionally leaked. Registered handlers,
//!   vtables and `&'static str`s point into the library's code and data for as long as the
//!   registry (and anything built from it) lives; unloading would leave them dangling.
//!   Unloading Rust `cdylib`s is also unreliable in general (thread-locals with destructors).

mod fingerprint;
#[cfg(feature = "dylib-plugins")]
mod loader;

pub use fingerprint::{
    HOST_ONLY_FEATURES, boundary_features, build_fingerprint, describe_fingerprint_mismatch,
};

#[cfg(feature = "dylib-plugins")]
pub use loader::{PluginLibrary, PluginLibraryError, check_plugin_info, discover_plugin_libraries};

use std::ffi::c_void;

/// Symbol exported by dynamic plugins that installs the plugin into a registry.
pub const REGISTER_SYMBOL: &str = "daedalus_plugin_register";
/// Symbol exported by dynamic plugins that returns [`PluginInfo`].
pub const PLUGIN_INFO_SYMBOL: &str = "daedalus_plugin_info";
/// Symbol exported by dynamic plugins that returns [`PLUGIN_ABI_VERSION`].
pub const PLUGIN_ABI_SYMBOL: &str = "daedalus_plugin_abi_version";
/// Symbol exported by dynamic plugins that registers boundary contracts.
pub const BOUNDARY_CONTRACTS_SYMBOL: &str = "daedalus_plugin_register_boundary_contracts";
/// Version of the dynamic plugin symbol/struct format.
///
/// Bumped whenever the exported symbols or [`PluginInfo`] change shape.
/// Version 4 added rustc/build-fingerprint checks and error reporting.
pub const PLUGIN_ABI_VERSION: u32 = 4;
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

/// Metadata a dynamic plugin reports about itself and the Daedalus build it was compiled with.
#[repr(C)]
#[derive(Copy, Clone, Debug)]
pub struct PluginInfo {
    pub abi_version: u32,
    pub daedalus_version: StrView,
    pub rustc_version: StrView,
    pub build_fingerprint: StrView,
    pub plugin_name: StrView,
    pub plugin_version: StrView,
}

impl PluginInfo {
    /// Info for a plugin built against this copy of Daedalus.
    pub fn for_plugin(plugin_name: &'static str, plugin_version: &'static str) -> Self {
        Self {
            abi_version: PLUGIN_ABI_VERSION,
            daedalus_version: StrView::from_static(crate::version()),
            rustc_version: StrView::from_static(RUSTC_VERSION),
            build_fingerprint: StrView::from_static(build_fingerprint()),
            plugin_name: StrView::from_static(plugin_name),
            plugin_version: StrView::from_static(plugin_version),
        }
    }
}

/// C-safe callback a host passes to plugin entry points to receive an error message.
#[repr(C)]
#[derive(Copy, Clone, Debug)]
pub struct PluginErrorSink {
    pub ctx: *mut c_void,
    pub write: Option<unsafe extern "C" fn(ctx: *mut c_void, ptr: *const u8, len: usize)>,
}

impl PluginErrorSink {
    /// A sink that discards messages.
    pub const fn discard() -> Self {
        Self {
            ctx: std::ptr::null_mut(),
            write: None,
        }
    }

    /// Report a message to the host.
    ///
    /// # Safety
    /// `ctx` and `write` must be the pair provided by the host for the current call.
    pub unsafe fn report(&self, message: &str) {
        if let Some(write) = self.write {
            // Safety: guaranteed by the caller.
            unsafe { write(self.ctx, message.as_ptr(), message.len()) };
        }
    }
}

#[doc(hidden)]
pub mod __support {
    use super::PluginErrorSink;
    use crate::runtime::plugins::{PluginRegistry, PluginResult};
    use std::panic::{AssertUnwindSafe, catch_unwind};

    /// Run `install` against a host-provided registry pointer, reporting errors and panics
    /// through `sink` instead of unwinding across the FFI boundary.
    ///
    /// # Safety
    /// `registry` must be null or a valid, exclusive pointer to a `PluginRegistry` with the
    /// same layout as this crate's, and `sink` must be valid for the call.
    pub unsafe fn run_install(
        registry: *mut PluginRegistry,
        sink: PluginErrorSink,
        install: impl FnOnce(&mut PluginRegistry) -> PluginResult<()>,
    ) -> bool {
        if registry.is_null() {
            // Safety: forwarded from the caller.
            unsafe { sink.report("registry pointer was null") };
            return false;
        }
        // Safety: non-null and valid per the caller contract.
        let registry = unsafe { &mut *registry };
        let message = match catch_unwind(AssertUnwindSafe(|| install(registry))) {
            Ok(Ok(())) => return true,
            Ok(Err(err)) => err.to_string(),
            Err(payload) => {
                let detail = payload
                    .downcast_ref::<&str>()
                    .map(|s| (*s).to_string())
                    .or_else(|| payload.downcast_ref::<String>().cloned())
                    .unwrap_or_else(|| "non-string panic payload".to_string());
                format!("plugin panicked: {detail}")
            }
        };
        // Safety: forwarded from the caller.
        unsafe { sink.report(&message) };
        false
    }
}

/// Export a [`Plugin`](crate::Plugin) implementor (which must also implement `Default`) for
/// dynamic loading from a `cdylib`.
///
/// Generates the `daedalus_plugin_abi_version`, `daedalus_plugin_info`,
/// `daedalus_plugin_register_boundary_contracts` and `daedalus_plugin_register` symbols that
/// `PluginLibrary` (feature `dylib-plugins`) expects. The symbols are unmangled, so invoke it
/// at most once per final `cdylib`, including across plugin crates linked into it as `rlib`s.
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

        /// Returns metadata about this plugin and the Daedalus build it links.
        #[unsafe(no_mangle)]
        pub extern "C" fn daedalus_plugin_info() -> $crate::dylib::PluginInfo {
            $crate::dylib::PluginInfo::for_plugin(
                env!("CARGO_PKG_NAME"),
                env!("CARGO_PKG_VERSION"),
            )
        }

        /// Registers this plugin's boundary contracts.
        ///
        /// # Safety
        /// `registry` must be null or a valid exclusive pointer to a layout-compatible
        /// `PluginRegistry`; `sink` must be valid for the call.
        #[unsafe(no_mangle)]
        pub unsafe extern "C" fn daedalus_plugin_register_boundary_contracts(
            registry: *mut $crate::runtime::plugins::PluginRegistry,
            sink: $crate::dylib::PluginErrorSink,
        ) -> bool {
            // Safety: forwarded from this function's contract.
            unsafe {
                $crate::dylib::__support::run_install(registry, sink, |__registry| {
                    $( __registry.register_boundary_contract($contract)?; )*
                    let _ = __registry;
                    Ok(())
                })
            }
        }

        /// Installs this plugin into the host registry.
        ///
        /// # Safety
        /// `registry` must be null or a valid exclusive pointer to a layout-compatible
        /// `PluginRegistry`; `sink` must be valid for the call.
        #[unsafe(no_mangle)]
        pub unsafe extern "C" fn daedalus_plugin_register(
            registry: *mut $crate::runtime::plugins::PluginRegistry,
            sink: $crate::dylib::PluginErrorSink,
        ) -> bool {
            // Safety: forwarded from this function's contract.
            unsafe {
                $crate::dylib::__support::run_install(registry, sink, |__registry| {
                    let __plugin: $ty = <$ty as ::core::default::Default>::default();
                    $crate::runtime::plugins::RegistryPluginExt::install_plugin(
                        __registry, &__plugin,
                    )
                })
            }
        }
    };
}
