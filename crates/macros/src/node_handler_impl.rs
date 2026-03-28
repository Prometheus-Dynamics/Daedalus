use proc_macro::TokenStream;
use proc_macro_crate::{FoundCrate, crate_name};
use quote::quote;
use syn::{ItemFn, LitStr, parse_macro_input};

use crate::helpers::{AttributeArgs, NestedMeta, compile_error, lit_from_expr, litstr_from_ident};

pub fn node_handler(args: TokenStream, item: TokenStream) -> TokenStream {
    let args = parse_macro_input!(args with AttributeArgs::parse_terminated);
    let input = parse_macro_input!(item as ItemFn);

    let mut id: Option<LitStr> = None;
    let mut outputs: Vec<LitStr> = Vec::new();

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
            let root_ident = syn::Ident::new(root, proc_macro2::Span::call_site());
            if let Some(sub) = subpath {
                let sub_ident = syn::Ident::new(sub, proc_macro2::Span::call_site());
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
        let ident = syn::Ident::new(&name, proc_macro2::Span::call_site());
        quote! { ::#ident }
    };
    let runtime_crate = crate_path("daedalus-runtime", "daedalus_runtime", Some("runtime"));
    let gpu_crate = crate_path("daedalus", "daedalus", Some("gpu"));

    for arg in args {
        match arg {
            NestedMeta::Meta(syn::Meta::NameValue(nv)) if nv.path.is_ident("id") => {
                match lit_from_expr(&nv.value) {
                    Some(syn::Lit::Str(s)) => id = Some(s),
                    _ => {
                        return TokenStream::from(compile_error(
                            "id must be a string literal".into(),
                        ));
                    }
                }
            }
            NestedMeta::Meta(syn::Meta::NameValue(nv)) if nv.path.is_ident("outputs") => {
                match lit_from_expr(&nv.value) {
                    Some(syn::Lit::Str(s)) => outputs.push(s),
                    _ => {
                        return TokenStream::from(compile_error(
                            "outputs must be a string literal".into(),
                        ));
                    }
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
    let is_low_level = {
        let inputs = &input.sig.inputs;
        inputs.len() == 3
            && matches!(&inputs[0], syn::FnArg::Typed(pat) if matches!(*pat.ty, syn::Type::Reference(_)))
    };

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
                        io.push_any(Some(#out_port), val);
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
                        #[cfg(feature = "gpu")]
                        {
                            // Special case: accept erased GPU payloads when requested.
                            if std::any::TypeId::of::<#arg_types>() == std::any::TypeId::of::<#gpu_crate::DataCell>() {
                                io.get_data_cell(#arg_names)
                                    .cloned()
                                    .ok_or_else(|| #runtime_crate::NodeError::InvalidInput(format!("missing {}", #arg_names)))?
                            } else {
                                io.get_any::<#arg_types>(#arg_names)
                                    .ok_or_else(|| #runtime_crate::NodeError::InvalidInput(format!("missing {}", #arg_names)))?
                            }
                        }
                        #[cfg(not(feature = "gpu"))]
                        {
                            io.get_any::<#arg_types>(#arg_names)
                                .ok_or_else(|| #runtime_crate::NodeError::InvalidInput(format!("missing {}", #arg_names)))?
                        }
                    }; )*
                    #ret_handling
                });
                reg
            }
        }
    };

    TokenStream::from(r#gen)
}
