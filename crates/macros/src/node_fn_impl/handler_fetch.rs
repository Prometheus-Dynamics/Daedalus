use proc_macro2::{Span, TokenStream};
use quote::quote;
use syn::LitStr;

use crate::foreign_type::is_foreign_view;
use crate::helpers::{generic_arg, last_ident_is, strip_ref};

pub(super) struct FetchInputs<'a> {
    pub(super) arg_idents: &'a [syn::Ident],
    pub(super) arg_types: &'a [syn::Type],
    pub(super) arg_mut_bindings: &'a [bool],
    pub(super) port_names: &'a [LitStr],
    pub(super) runtime_crate: &'a TokenStream,
}

pub(super) fn input_fetch_stmts(inputs: FetchInputs<'_>) -> (Vec<TokenStream>, Vec<TokenStream>) {
    let FetchInputs {
        arg_idents,
        arg_types,
        arg_mut_bindings,
        port_names,
        runtime_crate,
    } = inputs;
    let mut arg_fetch_mut_stmts: Vec<TokenStream> = Vec::new();
    let mut arg_fetch_ref_stmts: Vec<TokenStream> = Vec::new();
    for idx in 0..arg_idents.len() {
        let ident = &arg_idents[idx];
        let ty = &arg_types[idx];
        let port = &port_names[idx];
        let ty_core = strip_ref(ty);
        let (is_ref, is_ref_mut) = match ty {
            syn::Type::Reference(r) => (true, r.mutability.is_some()),
            _ => (false, false),
        };
        let is_binding_mut = arg_mut_bindings.get(idx).copied().unwrap_or(false);
        let mode = if is_ref {
            if is_ref_mut {
                "borrowed_mut"
            } else {
                "borrowed"
            }
        } else if is_binding_mut {
            "owned_mut"
        } else {
            "owned"
        };
        let is_payload = last_ident_is(ty_core, "Compute");
        // `io.<method>::<ty>(port)`, failing the node when the input is missing.
        let get = |method: &str, ty: &syn::Type| {
            let method = syn::Ident::new(method, Span::call_site());
            quote! {
                io.#method::<#ty>(#port)
                    .ok_or_else(|| #runtime_crate::NodeError::InvalidInput(format!("missing {}", #port)))?
            }
        };
        // Bind `value` to the argument directly, or through a `{prefix}_{idx}` temporary
        // that the argument borrows (mutably when `mutable`).
        let bind = |value: TokenStream, borrow: Option<(&str, bool)>| match borrow {
            None => quote! { let #ident = #value; },
            Some((prefix, mutable)) => {
                let tmp_ident = syn::Ident::new(&format!("{prefix}_{idx}"), Span::call_site());
                if mutable {
                    quote! { let mut #tmp_ident = #value; let #ident = &mut #tmp_ident; }
                } else {
                    quote! { let #tmp_ident = #value; let #ident = &#tmp_ident; }
                }
            }
        };

        if is_foreign_view(ty) {
            // Borrows `io`, so it runs with the other shared borrows.
            arg_fetch_ref_stmts.push(quote! { let #ident = io.get_foreign::<#ty>(#port)?; });
            continue;
        }
        let fetch = if let Some(inner_ty) = generic_arg(ty_core, "FanIn", 0) {
            let tmp_ident = syn::Ident::new(&format!("__fanin_indexed_{idx}"), Span::call_site());
            quote! {
                let #tmp_ident = io.get_all_fanin_indexed::<#inner_ty>(#port);
                let #ident = #runtime_crate::FanIn::<#inner_ty>::from_indexed(#tmp_ident);
            }
        } else if let Some(inner_ty) = generic_arg(ty, "Option", 0) {
            // Optional input: `None` when the port has no value this tick.
            if let syn::Type::Reference(inner) = inner_ty {
                let inner_ty = &inner.elem;
                arg_fetch_ref_stmts.push(quote! { let #ident = io.get_ref::<#inner_ty>(#port); });
                continue;
            }
            match generic_arg(inner_ty, "Arc", 0) {
                Some(arc_ty) => quote! { let #ident = io.get_arc::<#arc_ty>(#port); },
                None => quote! { let #ident = io.get_typed::<#inner_ty>(#port); },
            }
        } else if let Some(inner_ty) = generic_arg(ty_core, "Arc", 0) {
            let value = get("get_arc", inner_ty);
            match mode {
                "borrowed" => bind(value, Some(("__arc", false))),
                "borrowed_mut" => bind(value, Some(("__arc_mut", true))),
                _ => bind(value, None),
            }
        } else if let Some(inner_ty) = generic_arg(ty_core, "Compute", 0) {
            match mode {
                "borrowed" => bind(get("get_compute", inner_ty), Some(("__payload_ref", false))),
                "borrowed_mut" => bind(
                    get("get_compute_mut", inner_ty),
                    Some(("__payload_mut", true)),
                ),
                "owned_mut" => bind(get("get_compute_mut", inner_ty), None),
                _ => bind(get("get_compute", inner_ty), None),
            }
        } else if last_ident_is(ty_core, "Arc") || last_ident_is(ty_core, "Compute") {
            // `Arc` / `Compute` without a type argument.
            bind(get("take_owned", ty_core), None)
        } else {
            match mode {
                // A `Value` input (a graph constant) is coerced into a local the argument borrows.
                "borrowed" if super::port_types::coercible(ty_core) => {
                    let coerced = syn::Ident::new(&format!("__coerced_{idx}"), Span::call_site());
                    quote! {
                        let #coerced: #ty_core;
                        let #ident = match io.get_ref::<#ty_core>(#port) {
                            Some(value) => value,
                            None => {
                                #coerced = io.coerce_input::<#ty_core>(#port).ok_or_else(|| {
                                    #runtime_crate::NodeError::InvalidInput(format!("missing {}", #port))
                                })?;
                                &#coerced
                            }
                        };
                    }
                }
                "borrowed" => bind(get("get_ref", ty_core), None),
                "borrowed_mut" => bind(get("take_modify", ty_core), Some(("__borrowed_mut", true))),
                _ => bind(get("take_owned", ty_core), None),
            }
        };
        let needs_immut_borrow = mode == "borrowed" && !is_payload;
        if needs_immut_borrow {
            arg_fetch_ref_stmts.push(fetch);
        } else {
            arg_fetch_mut_stmts.push(fetch);
        }
    }
    (arg_fetch_mut_stmts, arg_fetch_ref_stmts)
}
