use proc_macro2::TokenStream;

use crate::helpers::DaedalusCrate;

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
            runtime_crate: DaedalusCrate::Runtime.path(),
            registry_crate: DaedalusCrate::Registry.path(),
            data_crate: DaedalusCrate::Data.path(),
            core_crate: DaedalusCrate::Core.path(),
            gpu_crate: DaedalusCrate::Gpu.path(),
        }
    }
}
