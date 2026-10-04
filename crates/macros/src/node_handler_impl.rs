use proc_macro::TokenStream;
use quote::quote;
use syn::{ItemFn, LitStr, parse_macro_input};

use crate::helpers::{
    AttributeArgs, DaedalusCrate, NestedMeta, compile_error, lit_str_arg, litstr_from_ident,
    str_expr,
};

pub fn node_handler(args: TokenStream, item: TokenStream) -> TokenStream {
    let args = parse_macro_input!(args with AttributeArgs::parse_terminated);
    let input = parse_macro_input!(item as ItemFn);

    let mut id: Option<syn::Expr> = None;
    let mut outputs: Vec<LitStr> = Vec::new();
    let runtime_crate = DaedalusCrate::Runtime.path();

    for arg in args {
        match arg {
            NestedMeta::Meta(syn::Meta::NameValue(nv)) if nv.path.is_ident("id") => {
                match str_expr(&nv.value, "id") {
                    Ok(s) => id = Some(s),
                    Err(err) => return TokenStream::from(err),
                }
            }
            NestedMeta::Meta(syn::Meta::NameValue(nv)) if nv.path.is_ident("outputs") => {
                match lit_str_arg(&nv.value, "outputs") {
                    Ok(s) => outputs.push(s),
                    Err(err) => return TokenStream::from(err),
                }
            }
            _ => return TokenStream::from(compile_error("expected id = \"...\"".into())),
        }
    }
    let id = match id {
        Some(v) => v,
        None => return TokenStream::from(compile_error("missing required argument `id`".into())),
    };

    let fn_name = input.sig.ident.clone();
    let helper_name = syn::Ident::new(
        &format!("register_{}", fn_name),
        proc_macro2::Span::call_site(),
    );

    // Determine if signature is low-level (node, ctx, io) or typed (args only).
    let is_low_level = crate::helpers::is_low_level_handler(&input.sig);

    let r#gen = if is_low_level {
        quote! {
            #input
            pub fn #helper_name() -> #runtime_crate::handler_registry::HandlerRegistry {
                let mut reg = #runtime_crate::handler_registry::HandlerRegistry::new();
                reg.on(#id, #fn_name);
                reg
            }
        }
    } else {
        // Typed signature: extract arg idents/types and generate shim.
        let mut arg_idents = Vec::new();
        let mut arg_names = Vec::new();
        let mut arg_types = Vec::new();
        for arg in &input.sig.inputs {
            if let syn::FnArg::Typed(pat) = arg
                && let syn::Pat::Ident(id) = &*pat.pat
            {
                arg_idents.push(id.ident.clone());
                arg_names.push(litstr_from_ident(&id.ident));
                arg_types.push((*pat.ty).clone());
            }
        }
        let out_port = outputs
            .first()
            .cloned()
            .unwrap_or_else(|| LitStr::new("out", proc_macro2::Span::call_site()));
        let call = quote! { #fn_name(#(#arg_idents),*) };
        let ret_handling = if !matches!(input.sig.output, syn::ReturnType::Default) {
            quote! {
                let result = #call;
                match result {
                    Ok(val) => {
                        io.push(Some(#out_port), val)?;
                        Ok(())
                    }
                    Err(e) => Err(e),
                }
            }
        } else {
            quote! { #call; Ok(()) }
        };

        quote! {
            #input
            pub fn #helper_name() -> #runtime_crate::handler_registry::HandlerRegistry {
                let mut reg = #runtime_crate::handler_registry::HandlerRegistry::new();
                reg.on(#id, |_, _, io| {
                    #(let #arg_idents = {
                        io.take_owned::<#arg_types>(#arg_names)
                            .ok_or_else(|| #runtime_crate::NodeError::InvalidInput(format!("missing {}", #arg_names)))?
                    }; )*
                    #ret_handling
                });
                reg
            }
        }
    };

    TokenStream::from(r#gen)
}
