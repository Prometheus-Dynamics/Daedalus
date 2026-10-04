//! Support code for [`export_plugin!`](crate::export_plugin) and
//! [`plugin_descriptor!`](crate::plugin_descriptor); not a public API.

use super::{BoundaryTypeTable, ForeignInterfaceTable, StrSink, extract};
use crate::runtime::plugins::{Plugin, PluginRegistry, PluginResult, RegistryPluginExt};
use crate::transport::BoundaryTypeContract;
use std::ffi::c_void;
use std::panic::{AssertUnwindSafe, catch_unwind};

pub use super::stable::{__invoke as invoke, __release as release, StableRuntimeCell};

/// A dependency plugin linked into a dynamic plugin (`export_plugin!(P, deps [D])`).
pub type LinkedDep = fn() -> Box<dyn Plugin>;
/// The boundary contracts passed to `export_plugin!(.., boundary_contracts [..])`.
pub type Contracts = fn() -> Vec<BoundaryTypeContract>;

/// The [`LinkedDep`] for `D`.
pub fn linked<D: Plugin + Default + 'static>() -> Box<dyn Plugin> {
    Box::new(D::default())
}

/// Run `body`, reporting its error or panic through `sink` instead of unwinding across the FFI
/// boundary.
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
/// `registry` must be null or a valid, exclusive pointer to a `PluginRegistry` with the same
/// layout as this crate's, and `sink` must be valid for the call.
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

/// Register the `export_plugin!` boundary contracts (the `register_boundary_contracts` entry
/// point).
pub fn install_contracts(contracts: Contracts, registry: &mut PluginRegistry) -> PluginResult<()> {
    contracts()
        .into_iter()
        .try_for_each(|contract| registry.register_boundary_contract(contract))
}

/// Install `P` into the registry (the `register` entry point). Its dependencies are the host's
/// to install first (`PluginLibrary::install_into` checks them); the linked ones already ran in
/// this library during schema extraction, so the type mappings they register are known to its
/// node macros.
pub fn install_plugin<P: Plugin + Default>(registry: &mut PluginRegistry) -> PluginResult<()> {
    registry.install_plugin(&P::default())
}

/// Write `P`'s [`PluginSchema`](super::PluginSchema) as JSON to `sink`: its manifest (with the
/// linked dependencies added to `dependencies` and the `export_plugin!` boundary contracts to
/// `boundary_contracts`) and nodes, and the recorded external port types as
/// `plugin.metadata.external_types`.
///
/// # Safety
/// `sink` must be valid for the call.
pub unsafe fn schema<P: Plugin + Default>(
    deps: &[LinkedDep],
    contracts: Contracts,
    sink: StrSink,
) -> bool {
    // Safety: forwarded from the caller.
    unsafe {
        guarded(sink, || {
            let (registry, plugin) = extract::registry::<P>(deps, false)?;
            let mut schema = extract::schema(&registry, &plugin, deps)?;
            schema.boundary_contracts.extend(contracts());
            schema
                .boundary_contracts
                .sort_by(|a, b| a.type_key.cmp(&b.type_key));
            schema
                .boundary_contracts
                .dedup_by(|a, b| a.type_key == b.type_key);
            let json = serde_json::to_string(&schema).map_err(|err| err.to_string())?;
            sink.write(&json);
            Ok(())
        })
    }
}

/// Hand the extraction registry's [`boundary_types`](PluginRegistry::boundary_types) (`P`'s and
/// its linked dependencies') to the host as a `'static` table.
///
/// # Safety
/// `table` must be valid for writes and `sink` valid for the call.
pub unsafe fn boundary_types<P: Plugin + Default>(
    deps: &[LinkedDep],
    table: *mut BoundaryTypeTable,
    sink: StrSink,
) -> bool {
    // Safety: forwarded from the caller.
    unsafe {
        guarded(sink, || {
            let table = table.as_mut().ok_or("table pointer was null")?;
            let (registry, _) = extract::registry::<P>(deps, false)?;
            *table = super::boundary::leak_table(&registry);
            Ok(())
        })
    }
}

/// Hand the extraction registry's [`foreign_interfaces`](PluginRegistry::foreign_interfaces)
/// to the host as a `'static` table.
///
/// # Safety
/// `table` must be valid for writes and `sink` valid for the call.
pub unsafe fn foreign_interfaces<P: Plugin + Default>(
    deps: &[LinkedDep],
    table: *mut ForeignInterfaceTable,
    sink: StrSink,
) -> bool {
    // Safety: forwarded from the caller.
    unsafe {
        guarded(sink, || {
            let table = table.as_mut().ok_or("table pointer was null")?;
            let (registry, _) = extract::registry::<P>(deps, false)?;
            *table = super::foreign::leak_table(&registry);
            Ok(())
        })
    }
}

/// Export a [`Plugin`](crate::Plugin) implementor (which must also implement `Default`) for
/// dynamic loading from a `cdylib`.
///
/// Generates the `daedalus_plugin_abi_version` and `daedalus_plugin_descriptor` symbols that
/// [`PluginLibrary`](crate::PluginLibrary) expects; the descriptor is
/// [`plugin_descriptor!`](crate::plugin_descriptor)'s. The symbols are unmangled, so invoke it at
/// most once per final `cdylib`, including across plugin crates linked into it as `rlib`s.
///
/// `deps [...]` links dependency plugins (`Plugin + Default`, typically the Daedalus integration
/// of a library whose types the plugin's nodes use): the descriptor's entry points install them
/// before the plugin into their private registry, so the keys they own or map resolve, and the
/// schema lists them in `dependencies`. They are never installed into the host:
/// [`PluginLibrary::install_into`](crate::PluginLibrary::install_into) requires the host to have
/// installed every dependency first. `boundary_contracts [...]` are listed in the schema's
/// `boundary_contracts` and registered before the plugin on the Rust-ABI install path.
///
/// ```ignore
/// #[derive(Default)]
/// struct DemoPlugin;
/// // impl daedalus::Plugin for DemoPlugin { ... }
///
/// daedalus::export_plugin!(DemoPlugin);
/// // or, linking the plugin of a library whose types the nodes use:
/// daedalus::export_plugin!(DemoPlugin, deps [styx_core::daedalus_integration::StyxPlugin]);
/// // or, with extra boundary contracts:
/// daedalus::export_plugin!(DemoPlugin, boundary_contracts [my_contract()]);
/// daedalus::export_plugin!(DemoPlugin, deps [Dep], boundary_contracts [my_contract()]);
/// ```
#[macro_export]
macro_rules! export_plugin {
    ($($args:tt)*) => {
        /// Returns the Daedalus dynamic plugin ABI version.
        #[unsafe(no_mangle)]
        pub extern "C" fn daedalus_plugin_abi_version() -> u32 {
            $crate::dylib::PLUGIN_ABI_VERSION
        }

        /// Returns this plugin's descriptor: metadata and entry points.
        #[unsafe(no_mangle)]
        pub extern "C" fn daedalus_plugin_descriptor() -> $crate::dylib::PluginDescriptor {
            $crate::plugin_descriptor!($($args)*)
        }
    };
}

/// The [`PluginDescriptor`](crate::dylib::PluginDescriptor) of a [`Plugin`](crate::Plugin)
/// implementor (which must also implement `Default`), exactly as
/// [`export_plugin!`](crate::export_plugin) exports it but without exporting any symbol; takes
/// the same arguments.
///
/// For plugins that export the descriptor themselves, e.g. to adjust it: the stable-path tests
/// simulate a plugin built by another toolchain by replacing `info.rustc_version`. Each
/// expansion owns the plugin's stable runtime, so expand it in a single function.
#[macro_export]
macro_rules! plugin_descriptor {
    ($ty:ty $(,)?) => {
        $crate::plugin_descriptor!($ty, deps [], boundary_contracts [])
    };
    ($ty:ty, boundary_contracts [ $( $contract:expr ),* $(,)? ] $(,)?) => {
        $crate::plugin_descriptor!($ty, deps [], boundary_contracts [ $( $contract ),* ])
    };
    ($ty:ty, deps [ $( $dep:ty ),* $(,)? ] $(,)?) => {
        $crate::plugin_descriptor!($ty, deps [ $( $dep ),* ], boundary_contracts [])
    };
    (
        $ty:ty,
        deps [ $( $dep:ty ),* $(,)? ],
        boundary_contracts [ $( $contract:expr ),* $(,)? ] $(,)?
    ) => {{
        use $crate::dylib::{__support, StrSink};
        use ::core::ffi::c_void;

        const DEPS: &[__support::LinkedDep] = &[ $( __support::linked::<$dep> ),* ];

        fn contracts() -> ::std::vec::Vec<$crate::transport::BoundaryTypeContract> {
            ::std::vec![ $( $contract ),* ]
        }

        static STABLE: __support::StableRuntimeCell = __support::StableRuntimeCell::new();

        unsafe extern "C" fn schema(sink: StrSink) -> bool {
            // Safety: the host passes a sink valid for the call.
            unsafe { __support::schema::<$ty>(DEPS, contracts, sink) }
        }

        unsafe extern "C" fn boundary_types(
            table: *mut $crate::dylib::BoundaryTypeTable,
            sink: StrSink,
        ) -> bool {
            // Safety: the host passes a writable table and a sink valid for the call.
            unsafe { __support::boundary_types::<$ty>(DEPS, table, sink) }
        }

        unsafe extern "C" fn foreign_interfaces(
            table: *mut $crate::dylib::ForeignInterfaceTable,
            sink: StrSink,
        ) -> bool {
            // Safety: the host passes a writable table and a sink valid for the call.
            unsafe { __support::foreign_interfaces::<$ty>(DEPS, table, sink) }
        }

        unsafe extern "C" fn register_boundary_contracts(
            registry: *mut c_void,
            sink: StrSink,
        ) -> bool {
            // Safety: the host passes a layout-compatible registry and a valid sink.
            unsafe {
                __support::install(registry, sink, |registry| {
                    __support::install_contracts(contracts, registry)
                })
            }
        }

        unsafe extern "C" fn register(registry: *mut c_void, sink: StrSink) -> bool {
            // Safety: the host passes a layout-compatible registry and a valid sink.
            unsafe { __support::install(registry, sink, __support::install_plugin::<$ty>) }
        }

        unsafe extern "C" fn invoke(
            node: u32,
            instance: u64,
            inputs: *const $crate::dylib::StableInput,
            len: usize,
            outputs: $crate::dylib::StableOutputSink,
            error: StrSink,
        ) -> u32 {
            // Safety: the host follows `InvokeFn`'s contract.
            unsafe {
                __support::invoke::<$ty>(&STABLE, DEPS, node, instance, inputs, len, outputs, error)
            }
        }

        unsafe extern "C" fn release(instance: u64) {
            __support::release(&STABLE, instance)
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
            stable: $crate::dylib::StableHandlers {
                version: $crate::dylib::STABLE_ABI_VERSION,
                invoke,
                release,
            },
        }
    }};
}
