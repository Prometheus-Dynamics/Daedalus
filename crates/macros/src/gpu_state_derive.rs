use proc_macro::TokenStream;
use quote::quote;
use syn::{DeriveInput, Meta, parse_macro_input};

use crate::helpers::{AttributeArgs, NestedMeta};

pub fn gpu_stateful(item: TokenStream) -> TokenStream {
    let input = parse_macro_input!(item as DeriveInput);
    let name = &input.ident;
    let mut zeroed = false;
    let mut readback = false;

    for attr in &input.attrs {
        if !attr.path().is_ident("gpu_state") {
            continue;
        }
        if let Ok(args) = attr.parse_args_with(AttributeArgs::parse_terminated) {
            for nested in args {
                match nested {
                    NestedMeta::Meta(Meta::Path(p)) if p.is_ident("zeroed") => zeroed = true,
                    NestedMeta::Meta(Meta::Path(p)) if p.is_ident("readback") => readback = true,
                    _ => {}
                }
            }
        }
    }

    quote! {
        impl ::daedalus::gpu::shader::GpuStateful for #name {
            const ZEROED: bool = #zeroed;
            const READBACK: bool = #readback;
        }
    }
    .into()
}
