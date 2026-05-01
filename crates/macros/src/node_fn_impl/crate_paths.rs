use proc_macro_crate::{FoundCrate, crate_name};
use proc_macro2::{Span, TokenStream};
use quote::quote;

pub(super) struct CratePaths {
    pub runtime_crate: TokenStream,
    pub registry_crate: TokenStream,
    pub data_crate: TokenStream,
    pub core_crate: TokenStream,
    pub gpu_crate: TokenStream,
}

impl CratePaths {
    pub(super) fn detect() -> Self {
        let daedalus_root: Option<String> = crate_name("daedalus-rs")
            .or_else(|_| crate_name("daedalus"))
            .ok()
            .map(|fc| match fc {
                FoundCrate::Itself => "daedalus".to_string(),
                FoundCrate::Name(name) => {
                    if name == "daedalus_rs" {
                        "daedalus".to_string()
                    } else {
                        name
                    }
                }
            });
        let crate_path = |pkg: &str, fallback: &str, subpath: Option<&str>| {
            if let Some(root) = &daedalus_root {
                let root_ident = syn::Ident::new(root, Span::call_site());
                if let Some(sub) = subpath {
                    let sub_ident = syn::Ident::new(sub, Span::call_site());
                    return quote! { ::#root_ident::#sub_ident };
                }
                return quote! { ::#root_ident };
            }
            let name = crate_name(pkg)
                .ok()
                .map(|fc| match fc {
                    FoundCrate::Itself => pkg.replace('-', "_"),
                    FoundCrate::Name(name) => name,
                })
                .unwrap_or_else(|| fallback.to_string());
            let ident = syn::Ident::new(&name, Span::call_site());
            quote! { ::#ident }
        };

        Self {
            runtime_crate: crate_path("daedalus-runtime", "daedalus_runtime", Some("runtime")),
            registry_crate: crate_path("daedalus-registry", "daedalus_registry", Some("registry")),
            data_crate: crate_path("daedalus-data", "daedalus_data", Some("data")),
            core_crate: crate_path("daedalus-core", "daedalus_core", Some("core")),
            gpu_crate: crate_path("daedalus", "daedalus", Some("gpu")),
        }
    }
}
