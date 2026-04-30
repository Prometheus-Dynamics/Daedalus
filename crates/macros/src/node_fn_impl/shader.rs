use proc_macro2::TokenStream;
use quote::quote;
use syn::LitStr;

pub(super) fn shader_tokens(
    shader_specs: &[(TokenStream, Option<LitStr>)],
    shader_path: Option<&LitStr>,
    shader_entry: &LitStr,
    shader_workgroup: Option<[u32; 3]>,
    shader_bindings: &[TokenStream],
    shader_paths: &[LitStr],
    gpu_crate: &TokenStream,
) -> Option<TokenStream> {
    if !shader_specs.is_empty() {
        let specs_only: Vec<TokenStream> = shader_specs.iter().map(|(ts, _)| ts.clone()).collect();
        let names: Vec<TokenStream> = shader_specs
            .iter()
            .enumerate()
            .map(|(i, (_, asset))| {
                if let Some(a) = asset {
                    quote! { #a }
                } else {
                    let n = format!("shader{}", i);
                    let lit = LitStr::new(&n, proc_macro2::Span::call_site());
                    quote! { #lit }
                }
            })
            .collect();
        let count = specs_only.len();
        let indices: Vec<syn::Index> = (0..count).map(syn::Index::from).collect();
        Some(quote! {
            use #gpu_crate::shader::{ShaderContext, ShaderInstance, ShaderSpec};
            const __SHADER_SPECS: [ShaderSpec; #count] = [ #(#specs_only),* ];
            const __SHADER_INSTANCES: [ShaderInstance; #count] = [
                #(ShaderInstance {
                    name: #names,
                    spec: &__SHADER_SPECS[#indices],
                }),*
            ];
            let __shader_ctx = ShaderContext { shaders: &__SHADER_INSTANCES, gpu: __ctx_gpu.clone() };
        })
    } else if let Some(shader_literal) = shader_path {
        let entry = shader_entry.clone();
        let wg_size = shader_workgroup
            .map(|[x, y, z]| quote! { Some([#x, #y, #z]) })
            .unwrap_or(quote! { None });
        let binding_tokens = shader_bindings.to_vec();
        let binding_len = binding_tokens.len();
        let binding_array = quote! { [ #(#binding_tokens),* ] };
        Some(quote! {
            use #gpu_crate::shader::{BindingSpec, ShaderContext, ShaderInstance, ShaderSpec};
            const __SHADER_SRC: &str = include_str!(#shader_literal);
            const __SHADER_BINDINGS: [BindingSpec; #binding_len] = #binding_array;
            const __SHADER_SPEC: ShaderSpec = ShaderSpec {
                name: "default",
                src: __SHADER_SRC,
                entry: #entry,
                workgroup_size: #wg_size,
                bindings: &__SHADER_BINDINGS,
            };
            const __SHADER_INSTANCES: [ShaderInstance; 1] = [ShaderInstance {
                name: "default",
                spec: &__SHADER_SPEC,
            }];
            let __shader_ctx = ShaderContext { shaders: &__SHADER_INSTANCES, gpu: __ctx_gpu.clone() };
        })
    } else if !shader_paths.is_empty() {
        let entry = shader_entry.clone();
        let wg_size = shader_workgroup
            .map(|[x, y, z]| quote! { Some([#x, #y, #z]) })
            .unwrap_or(quote! { None });
        let binding_tokens = shader_bindings.to_vec();
        let binding_len = binding_tokens.len();
        let binding_array = quote! { [ #(#binding_tokens),* ] };
        let shader_insts: Vec<TokenStream> = shader_paths
            .iter()
            .map(|p| {
                let name = std::path::Path::new(&p.value())
                    .file_stem()
                    .map(|s| s.to_string_lossy().to_string())
                    .unwrap_or_else(|| "shader".into());
                let lit_name = LitStr::new(&name, p.span());
                quote! {
                    #gpu_crate::shader::ShaderInstance {
                        name: #lit_name,
                        spec: &#gpu_crate::shader::ShaderSpec {
                            name: #lit_name,
                            src: include_str!(#p),
                            entry: #entry,
                            workgroup_size: #wg_size,
                            bindings: &__SHADER_BINDINGS,
                        },
                    }
                }
            })
            .collect();
        let count = shader_insts.len();
        Some(quote! {
            use #gpu_crate::shader::{Access, BindingKind, BindingSpec, ShaderContext, ShaderInstance};
            const __SHADER_BINDINGS: [#gpu_crate::shader::BindingSpec; #binding_len] = #binding_array;
            const __SHADER_INSTANCES: [ShaderInstance; #count] = [ #(#shader_insts),* ];
            let __shader_ctx = ShaderContext { shaders: &__SHADER_INSTANCES, gpu: __ctx_gpu.clone() };
        })
    } else {
        None
    }
}
