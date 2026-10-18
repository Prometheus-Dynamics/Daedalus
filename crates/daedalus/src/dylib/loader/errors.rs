//! The errors and install paths of [`PluginLibrary`](super::PluginLibrary).

use super::*;
use thiserror::Error;

/// Why a plugin cannot use the Rust-ABI install path of this host (see the [module docs](super::super)).
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

/// How [`PluginLibrary::install_into`] installs a plugin (see the [module docs](super::super)).
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
    #[error("failed to copy plugin library `{}` for loading: {source}", path.display())]
    ShadowCopy {
        path: PathBuf,
        #[source]
        source: std::io::Error,
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
    /// [`PluginLibrary::reload`] refused: the plugin was installed through the Rust ABI, whose
    /// handlers are the plugin's own code in the host registry. Nothing changed.
    #[error("plugin `{plugin}` cannot be reloaded: {reason}")]
    NotReloadable { plugin: String, reason: String },
    /// [`PluginLibrary::reload`] refused: the rebuilt plugin changed what its stable installs
    /// registered (nodes, ports, dependencies, foreign interfaces). The old build keeps running;
    /// install the new one into a fresh registry instead.
    #[error(
        "plugin `{plugin}` cannot be reloaded in place: {}; install it into a new registry",
        changes.join("; ")
    )]
    ReloadIncompatible {
        plugin: String,
        changes: Vec<String>,
    },
    #[error("plugin failed to register boundary contracts: {message}")]
    BoundaryContractsFailed { message: String },
    #[error("plugin registration failed: {message}")]
    RegisterFailed { message: String },
}
