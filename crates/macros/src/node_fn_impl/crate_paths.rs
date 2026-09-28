use proc_macro2::TokenStream;

use crate::helpers::crate_path;

pub(super) struct CratePaths {
    pub runtime_crate: TokenStream,
    pub registry_crate: TokenStream,
    pub data_crate: TokenStream,
    pub core_crate: TokenStream,
    pub gpu_crate: TokenStream,
}

impl CratePaths {
    pub(super) fn detect() -> Self {
        Self {
            runtime_crate: crate_path("daedalus-runtime", "runtime"),
            registry_crate: crate_path("daedalus-registry", "registry"),
            data_crate: crate_path("daedalus-data", "data"),
            core_crate: crate_path("daedalus-core", "core"),
            gpu_crate: crate_path("daedalus", "gpu"),
        }
    }
}
