//! The package a worker binary is shipped in: the plugin's schema with one persistent-worker
//! backend per node, built by the same packager the other language SDKs use.

use std::collections::BTreeMap;

use daedalus_ffi_core::{
    BackendConfig, BackendKind, FfiContractError, LanguagePackageInput, LanguagePackager,
    PluginPackage, PluginSchema,
};

/// The package of `schema` served by the worker binary at `executable`, started with `args`
/// (for example the plugin's own worker mode). Every node points at the same persistent worker,
/// and its entry symbol is the node's entrypoint. The package carries no artifacts: the worker
/// binary is the artifact, and the host starts it from `executable`.
pub fn rust_worker_package(
    schema: PluginSchema,
    executable: impl Into<String>,
    args: impl IntoIterator<Item = impl Into<String>>,
) -> Result<PluginPackage, FfiContractError> {
    let executable = executable.into();
    let args: Vec<String> = args.into_iter().map(Into::into).collect();
    let backends = schema
        .nodes
        .iter()
        .map(|node| {
            let config = BackendConfig::persistent_worker(
                BackendKind::Rust,
                executable.clone(),
                node.entrypoint.clone(),
            )
            .with_args(args.clone());
            (node.id.clone(), config)
        })
        .collect::<BTreeMap<_, _>>();
    LanguagePackager::new(BackendKind::Rust, "rust", "daedalus-ffi-rust").build(
        LanguagePackageInput {
            schema,
            backends,
            artifacts: Vec::new(),
            lockfile: None,
            metadata: BTreeMap::new(),
        },
    )
}
