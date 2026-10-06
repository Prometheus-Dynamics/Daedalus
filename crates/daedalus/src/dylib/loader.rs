use super::boundary::{self, BoundaryTypeTable};
use super::crate_builds::{self, CrateBuildTable};
use super::foreign::{self, ForeignInterfaceMismatch, ForeignInterfaceTable};
use super::stable::{STABLE_ABI_VERSION, StablePlugin};
use super::{
    InstallFn, PLUGIN_ABI_SYMBOL, PLUGIN_ABI_VERSION, PLUGIN_DESCRIPTOR_SYMBOL, PluginDescriptor,
    PluginInfo, PluginSchema, StrSink, StrView,
};
use crate::runtime::plugins::{
    BoundaryTypeConflict, CrateBuildDiff, CrateBuildInfo, PluginError, PluginRegistry,
    RegistryPluginExt,
};
use crate::transport::{ForeignInterfaceInfo, RustTypeIdentity, TypeKey};
use daedalus_ffi_host::core::BackendKind;
use libloading::Library;
use std::collections::BTreeMap;
use std::ffi::c_void;
use std::path::{Path, PathBuf};
use thiserror::Error;

/// Why a plugin cannot use the Rust-ABI install path of this host (see the [module docs](super)).
#[derive(Clone, Debug, Error, PartialEq, Eq)]
#[non_exhaustive]
pub enum RustAbiMismatch {
    #[error("plugin was built against Daedalus {found}, host uses Daedalus {expected}")]
    DaedalusVersion { expected: String, found: String },
    #[error("plugin was built with `{found}`, host was built with `{expected}`")]
    Rustc { expected: String, found: String },
    #[error("build fingerprint mismatch ({differences}); host `{expected}`, plugin `{found}`")]
    BuildFingerprint {
        expected: String,
        found: String,
        /// Human-readable list of the differing fingerprint segments.
        differences: String,
    },
}

/// How [`PluginLibrary::install_into`] installs a plugin (see the [module docs](super)).
#[derive(Clone, Copy, Debug, PartialEq, Eq, Hash)]
pub enum InstallPath {
    /// The plugin registers its Rust handlers into the host registry: no per-call overhead,
    /// every capability (adapters, serializers, ...) installs, but it needs the host's exact
    /// build ([`PluginLibrary::rust_abi`]).
    RustAbi,
    /// The host registers the schema's nodes with handlers that call the plugin's C-ABI
    /// `invoke`: works across toolchains and Daedalus patch releases with the same
    /// [`STABLE_ABI_VERSION`](super::STABLE_ABI_VERSION), for nodes whose values are builtins,
    /// `Value`s, `ToValue`/`Deserialize` types or foreign interface handles.
    Stable,
}

/// Errors that can occur while loading or installing a dynamic plugin library.
#[derive(Debug, Error)]
#[non_exhaustive]
pub enum PluginLibraryError {
    #[error("failed to load plugin library `{}`: {source}", path.display())]
    Load {
        path: PathBuf,
        #[source]
        source: libloading::Error,
    },
    #[error("plugin library `{}` does not export symbol `{symbol}`", path.display())]
    MissingSymbol { path: PathBuf, symbol: &'static str },
    #[error("plugin ABI mismatch: host expects {expected}, plugin reports {found}")]
    AbiMismatch { expected: u32, found: u32 },
    #[error("plugin info field `{field}` is null or not valid UTF-8")]
    InvalidInfo { field: &'static str },
    #[error("plugin `{plugin}` did not provide a valid schema: {message}")]
    Schema { plugin: String, message: String },
    /// The plugin loaded and its schema is readable, but it cannot be installed into this host
    /// through the Rust ABI ([`InstallPath::RustAbi`] was requested).
    #[error("plugin `{plugin}` cannot be installed into this host: {mismatch}")]
    Incompatible {
        plugin: String,
        #[source]
        mismatch: RustAbiMismatch,
    },
    /// The plugin's stable handler ABI differs from the host's, so it cannot be installed
    /// through the stable path either; `rust` says why the Rust-ABI path was not taken (`None`
    /// when [`InstallPath::Stable`] was requested).
    #[error(
        "plugin `{plugin}` cannot be installed into this host: its stable handler ABI is \
         version {found}, the host's is {expected}{}",
        rust.as_ref().map(|m| format!(", and the Rust ABI differs too ({m})")).unwrap_or_default()
    )]
    StableAbiMismatch {
        plugin: String,
        expected: u32,
        found: u32,
        rust: Option<RustAbiMismatch>,
    },
    /// The plugin depends on plugins (`#[plugin(deps(...))]`, `export_plugin!(.., deps [..])`)
    /// the host registry has not installed. Nothing was installed.
    #[error(
        "plugin `{plugin}` depends on plugins the host has not installed: {}; install them \
         before this plugin",
        missing.iter().map(|id| format!("`{id}`")).collect::<Vec<_>>().join(", ")
    )]
    MissingDependencies {
        plugin: String,
        missing: Vec<String>,
    },
    /// The plugin maps type keys the host registry also uses to different Rust types
    /// (typically a dependency such as a frame library resolved with other features in a
    /// separate build); `registered` is the host's type, `new` the plugin's. The message groups
    /// them by the crate defining the types. Nothing was installed.
    #[error(
        "plugin `{plugin}` uses type keys for different Rust types than the host: {}. Rust-ABI \
         plugins must come from the same cargo build as the host: build the host and its plugins \
         in one cargo invocation, so every shared dependency resolves with one feature set{}",
        boundary::describe(conflicts, crate_builds, same_crate_builds),
        if *stable_compatible {
            "; or install it with `install_into_as(InstallPath::Stable)`: no node port of the \
             plugin uses these keys, so none of these types crosses the stable path"
        } else {
            ""
        }
    )]
    BoundaryTypeConflict {
        plugin: String,
        conflicts: Vec<BoundaryTypeConflict>,
        /// How the crates behind `conflicts` were built differently, for crates both the host
        /// and the plugin registered ([`PluginLibrary::crate_build_diff`]).
        crate_builds: Vec<CrateBuildDiff>,
        /// Crates both sides registered with the same version and features: their types
        /// differ through the crates' own dependencies.
        same_crate_builds: Vec<CrateBuildInfo>,
        /// The plugin installs through [`InstallPath::Stable`] without any of these types
        /// crossing: its stable ABI matches and no node port uses a conflicting key (the
        /// conflicting types are only registered, e.g. by a linked dependency plugin).
        stable_compatible: bool,
    },
    /// The plugin uses a foreign interface key with another version or vtable layout than the
    /// host. Nothing was installed.
    #[error(
        "plugin `{plugin}` uses foreign interfaces incompatible with the host ({}); rebuild it \
         against the same interface versions",
        foreign::describe(mismatches)
    )]
    ForeignInterfaceMismatch {
        plugin: String,
        mismatches: Vec<ForeignInterfaceMismatch>,
    },
    #[error("plugin failed to register boundary contracts: {message}")]
    BoundaryContractsFailed { message: String },
    #[error("plugin registration failed: {message}")]
    RegisterFailed { message: String },
}

type AbiFn = unsafe extern "C" fn() -> u32;
type DescriptorFn = unsafe extern "C" fn() -> PluginDescriptor;

/// A loaded native Rust plugin library (built with [`export_plugin!`](crate::export_plugin)).
///
/// The underlying library is leaked and never unloaded; see the [module docs](super).
pub struct PluginLibrary {
    path: PathBuf,
    descriptor: PluginDescriptor,
    schema: PluginSchema,
    boundary_types: Vec<(TypeKey, RustTypeIdentity)>,
    foreign_interfaces: Vec<ForeignInterfaceInfo>,
    crate_builds: Vec<CrateBuildInfo>,
    rust_abi: Result<(), RustAbiMismatch>,
}

/// The plugin id `StablePlugin` needs as `&'static str` (plugin libraries are never unloaded).
fn leak_id(id: &str) -> &'static str {
    Box::leak(id.to_owned().into_boxed_str())
}

impl std::fmt::Debug for PluginLibrary {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        let info = &self.descriptor.info;
        f.debug_struct("PluginLibrary")
            .field("path", &self.path)
            .field("plugin_name", &info.plugin_name.as_str())
            .field("plugin_version", &info.plugin_version.as_str())
            .field("rust_abi", &self.rust_abi)
            .field("stable_abi", &self.descriptor.stable.version)
            .finish()
    }
}

impl PluginLibrary {
    /// Load a plugin library and read its descriptor and schema.
    ///
    /// Uses only C-ABI calls: checks the ABI version, validates the [`PluginInfo`] strings, and
    /// fetches and validates the plugin's [`PluginSchema`]. A plugin built with another
    /// toolchain, Daedalus version or feature set still loads, so it can be inspected;
    /// [`rust_abi`](Self::rust_abi) reports whether [`install_into`](Self::install_into) will
    /// accept it. The library stays loaded for the rest of the process, even when loading fails
    /// after it was opened.
    ///
    /// # Safety
    /// Loading a library runs its initialisers, and the exported symbols are trusted to have
    /// the signatures generated by `export_plugin!`. The caller must ensure `path` points to a
    /// trusted Daedalus plugin.
    pub unsafe fn load(path: impl AsRef<Path>) -> Result<Self, PluginLibraryError> {
        let path = path.as_ref().to_path_buf();
        // Safety: forwarded from the caller.
        let library =
            unsafe { Library::new(&path) }.map_err(|source| PluginLibraryError::Load {
                path: path.clone(),
                source,
            })?;
        // Never unload: registered handlers and static data point into the library.
        let library: &'static Library = Box::leak(Box::new(library));

        // Safety (symbol lookups and calls): signatures are defined by `export_plugin!`; the
        // descriptor symbol is only called once the ABI version pins its shape.
        let abi: AbiFn = unsafe { symbol(library, &path, PLUGIN_ABI_SYMBOL) }?;
        check_abi_version(unsafe { abi() }, PLUGIN_ABI_VERSION)?;
        let descriptor: DescriptorFn = unsafe { symbol(library, &path, PLUGIN_DESCRIPTOR_SYMBOL) }?;
        // Safety: the descriptor comes from a plugin with a matching ABI version.
        unsafe { Self::from_descriptor(path, descriptor()) }
    }

    /// # Safety
    /// `descriptor` must come from a plugin with ABI version [`PLUGIN_ABI_VERSION`].
    unsafe fn from_descriptor(
        path: PathBuf,
        descriptor: PluginDescriptor,
    ) -> Result<Self, PluginLibraryError> {
        let info = &descriptor.info;
        let plugin = info_field(info.plugin_name, "plugin_name")?.to_string();
        info_field(info.plugin_version, "plugin_version")?;
        info_field(info.daedalus_version, "daedalus_version")?;
        info_field(info.rustc_version, "rustc_version")?;
        info_field(info.build_fingerprint, "build_fingerprint")?;

        let schema_error = |message: String| PluginLibraryError::Schema {
            plugin: plugin.clone(),
            message,
        };
        // Safety: `schema` only exchanges borrowed bytes through the C-ABI sink.
        let json = call(|sink| unsafe { (descriptor.schema)(sink) }).map_err(schema_error)?;
        let schema: PluginSchema =
            serde_json::from_str(&json).map_err(|err| schema_error(err.to_string()))?;
        schema
            .validate_backend_kind(BackendKind::Rust)
            .map_err(|err| schema_error(err.to_string()))?;
        let mut table = BoundaryTypeTable {
            entries: std::ptr::null(),
            len: 0,
        };
        // Safety: `boundary_types` only writes the table (a `'static` array the plugin leaks)
        // and borrowed bytes through the C-ABI sink.
        call(|sink| unsafe { (descriptor.boundary_types)(&mut table, sink) })
            .map_err(|message| schema_error(format!("boundary types: {message}")))?;
        // Safety: the plugin returned a `'static` table of `len` entries.
        let boundary_types = unsafe { boundary::read_table(table) }
            .map_err(|message| schema_error(format!("boundary types: {message}")))?;
        let mut table = ForeignInterfaceTable {
            entries: std::ptr::null(),
            len: 0,
        };
        // Safety: as for `boundary_types`.
        call(|sink| unsafe { (descriptor.foreign_interfaces)(&mut table, sink) })
            .map_err(|message| schema_error(format!("foreign interfaces: {message}")))?;
        // Safety: the plugin returned a `'static` table of `len` entries.
        let foreign_interfaces = unsafe { foreign::read_table(table) }
            .map_err(|message| schema_error(format!("foreign interfaces: {message}")))?;
        let mut table = CrateBuildTable {
            entries: std::ptr::null(),
            len: 0,
        };
        // Safety: as for `boundary_types`.
        call(|sink| unsafe { (descriptor.crate_builds)(&mut table, sink) })
            .map_err(|message| schema_error(format!("crate builds: {message}")))?;
        // Safety: the plugin returned a `'static` table of `len` entries.
        let crate_builds = unsafe { crate_builds::read_table(table) }
            .map_err(|message| schema_error(format!("crate builds: {message}")))?;
        Ok(Self {
            path,
            rust_abi: check_rust_abi(info),
            descriptor,
            schema,
            boundary_types,
            foreign_interfaces,
            crate_builds,
        })
    }

    /// How [`install_into`](Self::install_into) installs this plugin: through the Rust ABI
    /// when [`rust_abi`](Self::rust_abi) accepts it, else through the stable path when the
    /// stable ABI versions match, else `None`.
    pub fn install_mode(&self) -> Option<InstallPath> {
        if self.rust_abi.is_ok() {
            Some(InstallPath::RustAbi)
        } else if self.descriptor.stable.version == STABLE_ABI_VERSION {
            Some(InstallPath::Stable)
        } else {
            None
        }
    }

    /// Install the plugin into `registry` through [`install_mode`](Self::install_mode)'s path
    /// and return it; see [`install_into_as`](Self::install_into_as). Fails with
    /// [`PluginLibraryError::StableAbiMismatch`] (naming the Rust ABI mismatch too) when
    /// neither path is available.
    pub fn install_into(
        &self,
        registry: &mut PluginRegistry,
    ) -> Result<InstallPath, PluginLibraryError> {
        let path = self
            .install_mode()
            .ok_or_else(|| PluginLibraryError::StableAbiMismatch {
                plugin: self.schema.plugin.name.clone(),
                expected: STABLE_ABI_VERSION,
                found: self.descriptor.stable.version,
                rust: self.rust_abi.clone().err(),
            })?;
        self.install_into_as(registry, path)?;
        Ok(path)
    }

    /// Install the plugin into `registry` through `path` (e.g. [`InstallPath::Stable`] for a
    /// plugin [`install_into`](Self::install_into) would install through the Rust ABI, to test
    /// the stable path).
    ///
    /// Both paths first fail without calling into the plugin with
    /// [`PluginLibraryError::MissingDependencies`] when `registry` lacks a plugin the schema
    /// lists in `dependencies`, and with [`PluginLibraryError::ForeignInterfaceMismatch`] when
    /// one of its [`foreign_interfaces`](Self::foreign_interfaces) has another version or layout
    /// in `registry` ([`PluginRegistry::foreign_interfaces`]).
    ///
    /// - [`InstallPath::RustAbi`] fails with [`PluginLibraryError::Incompatible`] when
    ///   [`rust_abi`](Self::rust_abi) reports a mismatch and with
    ///   [`PluginLibraryError::BoundaryTypeConflict`] when one of the plugin's
    ///   [`boundary_types`](Self::boundary_types) is a key `registry` already maps to another
    ///   Rust type ([`PluginRegistry::boundary_types`]); it then registers the boundary
    ///   contracts and the plugin, and records its boundary types.
    /// - [`InstallPath::Stable`] fails with [`PluginLibraryError::StableAbiMismatch`] when the
    ///   stable ABI versions differ. No Rust type crosses it, so boundary types are not
    ///   compared; it registers the schema's nodes, each with a handler calling the plugin.
    pub fn install_into_as(
        &self,
        registry: &mut PluginRegistry,
        path: InstallPath,
    ) -> Result<(), PluginLibraryError> {
        let plugin = || self.schema.plugin.name.clone();
        match path {
            InstallPath::RustAbi => {
                if let Err(mismatch) = &self.rust_abi {
                    return Err(PluginLibraryError::Incompatible {
                        plugin: plugin(),
                        mismatch: mismatch.clone(),
                    });
                }
            }
            InstallPath::Stable => {
                let found = self.descriptor.stable.version;
                if found != STABLE_ABI_VERSION {
                    return Err(PluginLibraryError::StableAbiMismatch {
                        plugin: plugin(),
                        expected: STABLE_ABI_VERSION,
                        found,
                        rust: None,
                    });
                }
            }
        }
        let missing: Vec<String> = self
            .schema
            .dependencies
            .iter()
            .filter(|dep| !registry.plugin_manifests.contains_key(*dep))
            .cloned()
            .collect();
        if !missing.is_empty() {
            return Err(PluginLibraryError::MissingDependencies {
                plugin: plugin(),
                missing,
            });
        }
        let mismatches = foreign::mismatches(registry, &self.foreign_interfaces);
        if !mismatches.is_empty() {
            return Err(PluginLibraryError::ForeignInterfaceMismatch {
                plugin: plugin(),
                mismatches,
            });
        }
        match path {
            InstallPath::RustAbi => self.install_rust_abi(registry),
            InstallPath::Stable => registry
                .install_plugin(&StablePlugin {
                    id: leak_id(&self.schema.plugin.name),
                    schema: &self.schema,
                    handlers: self.descriptor.stable,
                    foreign_interfaces: &self.foreign_interfaces,
                })
                .map_err(|err| PluginLibraryError::RegisterFailed {
                    message: err.to_string(),
                }),
        }
    }

    /// [`PluginLibraryError::BoundaryTypeConflict`] for `conflicts` with `registry`.
    fn boundary_type_conflict(
        &self,
        registry: &PluginRegistry,
        conflicts: Vec<BoundaryTypeConflict>,
    ) -> PluginLibraryError {
        let quoted: Vec<String> = conflicts
            .iter()
            .map(|conflict| format!("\"{}\"", conflict.key))
            .collect();
        let uses_conflicting_key = |port: &daedalus_ffi_host::core::WirePort| {
            let ty = serde_json::to_string(&port.ty).unwrap_or_default();
            conflicts
                .iter()
                .any(|conflict| port.type_key.as_ref() == Some(&conflict.key))
                || quoted.iter().any(|key| ty.contains(key.as_str()))
        };
        let stable_compatible = self.descriptor.stable.version == STABLE_ABI_VERSION
            && !self
                .schema
                .nodes
                .iter()
                .flat_map(|node| node.inputs.iter().chain(&node.outputs))
                .any(uses_conflicting_key);
        let same_crate_builds = self
            .crate_builds
            .iter()
            .filter(|info| {
                let host = registry.crate_builds().get(info.name);
                host.is_some_and(|host| CrateBuildDiff::new(*host, **info).is_none())
            })
            .copied()
            .collect();
        PluginLibraryError::BoundaryTypeConflict {
            plugin: self.schema.plugin.name.clone(),
            conflicts,
            crate_builds: self.crate_build_diff(registry),
            same_crate_builds,
            stable_compatible,
        }
    }

    fn install_rust_abi(&self, registry: &mut PluginRegistry) -> Result<(), PluginLibraryError> {
        let conflicts = registry.boundary_type_conflicts(&self.boundary_types);
        if !conflicts.is_empty() {
            return Err(self.boundary_type_conflict(registry, conflicts));
        }
        let install = |entry: InstallFn, registry: &mut PluginRegistry| {
            let registry = (registry as *mut PluginRegistry).cast::<c_void>();
            // Safety: `check_rust_abi` accepted the plugin; the registry pointer is exclusive
            // for the call and the sink outlives it.
            call(|sink| unsafe { entry(registry, sink) })
        };
        install(self.descriptor.register_boundary_contracts, registry)
            .map_err(|message| PluginLibraryError::BoundaryContractsFailed { message })?;
        install(self.descriptor.register, registry)
            .map_err(|message| PluginLibraryError::RegisterFailed { message })?;
        // Record every exported boundary type, including any the install did not touch, so later
        // plugins and fed payloads are checked against them.
        match registry.register_boundary_identities(&self.boundary_types) {
            Ok(()) => Ok(()),
            Err(PluginError::BoundaryTypeConflict(conflict)) => {
                Err(self.boundary_type_conflict(registry, vec![conflict]))
            }
            Err(other) => Err(PluginLibraryError::RegisterFailed {
                message: other.to_string(),
            }),
        }
    }

    /// Metadata reported by the plugin.
    pub fn info(&self) -> PluginInfo {
        self.descriptor.info
    }

    /// The plugin's manifest and node declarations, readable whatever built the plugin.
    pub fn schema(&self) -> &PluginSchema {
        &self.schema
    }

    /// Every type key the plugin consumes, produces or registers, with the Rust type behind it
    /// in the plugin's build. Readable whatever built the plugin; the identities are only
    /// comparable with the host's when [`rust_abi`](Self::rust_abi) is `Ok`.
    pub fn boundary_types(&self) -> &[(TypeKey, RustTypeIdentity)] {
        &self.boundary_types
    }

    /// Every foreign interface the plugin's nodes take or its providers implement (key, version,
    /// vtable layout hash). Comparable with the host's whatever built the plugin.
    pub fn foreign_interfaces(&self) -> &[ForeignInterfaceInfo] {
        &self.foreign_interfaces
    }

    /// The builds of third-party crates the plugin registered (its own crate build, those of
    /// its linked dependency plugins; see [`PluginRegistry::register_crate_build`]).
    pub fn crate_builds(&self) -> &[CrateBuildInfo] {
        &self.crate_builds
    }

    /// Every crate the plugin and `registry` both registered a build of, built differently
    /// (version or features). Not an error by itself (a plugin that shares none of the crate's
    /// types installs fine), but the likely cause of a [`PluginLibraryError::BoundaryTypeConflict`]
    /// (whose message includes it) or of payload type mismatches: worth logging as a warning
    /// before [`install_into`](Self::install_into).
    pub fn crate_build_diff(&self, registry: &PluginRegistry) -> Vec<CrateBuildDiff> {
        registry.crate_build_diffs(&self.crate_builds)
    }

    /// Whether the plugin can be installed into this host through the Rust ABI.
    pub fn rust_abi(&self) -> Result<(), &RustAbiMismatch> {
        self.rust_abi.as_ref().copied()
    }

    /// The plugin's [`STABLE_ABI_VERSION`](super::STABLE_ABI_VERSION); the stable install path
    /// requires the host's.
    pub fn stable_abi_version(&self) -> u32 {
        self.descriptor.stable.version
    }

    /// Path the library was loaded from.
    pub fn path(&self) -> &Path {
        &self.path
    }
}

/// # Safety
/// `T` must be the function pointer type matching the exported symbol.
unsafe fn symbol<T: Copy>(
    library: &'static Library,
    path: &Path,
    name: &'static str,
) -> Result<T, PluginLibraryError> {
    // Safety: forwarded from the caller.
    unsafe { library.get::<T>(name.as_bytes()) }
        .map(|sym| *sym)
        .map_err(|_| PluginLibraryError::MissingSymbol {
            path: path.to_path_buf(),
            symbol: name,
        })
}

pub(crate) unsafe extern "C" fn write_string(ctx: *mut c_void, ptr: *const u8, len: usize) {
    if ctx.is_null() || ptr.is_null() {
        return;
    }
    // Safety: `ctx` is the `Option<String>` owned by `call`, and the plugin passes a valid
    // `(ptr, len)` byte slice for the duration of this call.
    let (slot, bytes) = unsafe {
        (
            &mut *ctx.cast::<Option<String>>(),
            std::slice::from_raw_parts(ptr, len),
        )
    };
    *slot = Some(String::from_utf8_lossy(bytes).into_owned());
}

/// Call a plugin entry point with a sink; return the string it wrote (empty if none) on
/// success, or its error message on failure.
fn call(entry: impl FnOnce(StrSink) -> bool) -> Result<String, String> {
    let mut written: Option<String> = None;
    let sink = StrSink {
        ctx: (&mut written as *mut Option<String>).cast(),
        write: Some(write_string),
    };
    match (entry(sink), written) {
        (true, written) => Ok(written.unwrap_or_default()),
        (false, Some(message)) => Err(message),
        (false, None) => Err("plugin reported failure without a message".to_string()),
    }
}

fn info_field(view: StrView, field: &'static str) -> Result<&'static str, PluginLibraryError> {
    view.as_str()
        .ok_or(PluginLibraryError::InvalidInfo { field })
}

fn check_abi_version(found: u32, expected: u32) -> Result<(), PluginLibraryError> {
    if found == expected {
        Ok(())
    } else {
        Err(PluginLibraryError::AbiMismatch { expected, found })
    }
}

/// Check whether a plugin with this metadata can be installed into this host through the Rust
/// ABI: its Daedalus version, rustc version and build fingerprint must match the host's.
pub fn check_rust_abi(info: &PluginInfo) -> Result<(), RustAbiMismatch> {
    check_rust_abi_against(info, &PluginInfo::for_plugin("host", "host"))
}

fn check_rust_abi_against(info: &PluginInfo, host: &PluginInfo) -> Result<(), RustAbiMismatch> {
    let pair = |f: fn(&PluginInfo) -> StrView| {
        let text = |info: &PluginInfo| f(info).as_str().unwrap_or_default().to_string();
        (text(host), text(info))
    };
    let (expected, found) = pair(|i| i.daedalus_version);
    if expected != found {
        return Err(RustAbiMismatch::DaedalusVersion { expected, found });
    }
    let (expected, found) = pair(|i| i.rustc_version);
    if expected != found {
        return Err(RustAbiMismatch::Rustc { expected, found });
    }
    let (expected, found) = pair(|i| i.build_fingerprint);
    if expected != found {
        return Err(RustAbiMismatch::BuildFingerprint {
            differences: super::describe_fingerprint_mismatch(&expected, &found),
            expected,
            found,
        });
    }
    Ok(())
}

/// Find plugin libraries (`.so`, `.dylib`, `.dll`) in `dirs`.
///
/// Directories that do not exist are skipped. When several directories contain a file with
/// the same name, the one from the earliest directory wins. The result is sorted by file name
/// so load order is deterministic.
pub fn discover_plugin_libraries<I, P>(dirs: I) -> std::io::Result<Vec<PathBuf>>
where
    I: IntoIterator<Item = P>,
    P: AsRef<Path>,
{
    let mut found: BTreeMap<std::ffi::OsString, PathBuf> = BTreeMap::new();
    for dir in dirs {
        let entries = match std::fs::read_dir(dir.as_ref()) {
            Ok(entries) => entries,
            Err(err) if err.kind() == std::io::ErrorKind::NotFound => continue,
            Err(err) => return Err(err),
        };
        for entry in entries {
            let path = entry?.path();
            let is_library = path
                .extension()
                .and_then(|ext| ext.to_str())
                .is_some_and(|ext| matches!(ext, "so" | "dylib" | "dll"));
            if !is_library || !path.is_file() {
                continue;
            }
            if let Some(name) = path.file_name() {
                found.entry(name.to_os_string()).or_insert(path);
            }
        }
    }
    Ok(found.into_values().collect())
}

#[cfg(test)]
mod tests;
