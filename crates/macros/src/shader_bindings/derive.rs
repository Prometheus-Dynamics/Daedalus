use proc_macro::TokenStream;
use proc_macro2::TokenStream as TokenStream2;
use quote::quote;
use syn::{DeriveInput, parse_macro_input};

use super::classify::classify_fields;
use super::emit::{emit_bindings, emit_invocation_hint, emit_spec_const};
use super::infer::infer_spec;
use super::parse::{parse_fields, parse_spec};

pub fn gpu_bindings(item: TokenStream) -> TokenStream {
    let input = parse_macro_input!(item as DeriveInput);
    match impl_gpu_bindings(&input) {
        Ok(ts) => ts.into(),
        Err(e) => e.to_compile_error().into(),
    }
}

fn impl_gpu_bindings(input: &DeriveInput) -> syn::Result<TokenStream2> {
    let spec = parse_spec(input)?;

    let data = match &input.data {
        syn::Data::Struct(ds) => ds,
        _ => {
            return Err(syn::Error::new_spanned(
                &input.ident,
                "GpuBindings only supports structs",
            ));
        }
    };

    let parsed_fields = parse_fields(data)?;
    let inferred = infer_spec(&spec)?;
    let classified = classify_fields(parsed_fields, &inferred.bindings)?;

    let name = &input.ident;
    let vis = &input.vis;
    let emitted = emit_bindings(
        &classified.fields,
        classified.auto_sampler_binding,
        &classified.inferred_map,
    );
    let binding_specs = emitted.binding_specs;
    let binding_inits = emitted.binding_inits;
    let invocation_hint_body = emit_invocation_hint(&classified.fields);
    let spec_tokens = emit_spec_const(name, vis, &spec, inferred.workgroup, &binding_specs);

    Ok(quote! {
        #spec_tokens

        impl<'a> ::daedalus::gpu::shader::GpuBindings<'a> for #name<'a> {
            fn spec() -> &'static ::daedalus::gpu::shader::ShaderSpec {
                &#name::SPEC
            }

            fn bindings(&'a self, gpu: Option<&::daedalus::gpu::GpuContextHandle>) -> Result<Vec<::daedalus::gpu::shader::ShaderBinding<'a>>, ::daedalus::gpu::GpuError> {
                Ok(vec![ #(#binding_inits),* ])
            }

            fn invocation_hint(&'a self) -> Option<[u32; 3]> {
                #invocation_hint_body
            }
        }
    })
}
