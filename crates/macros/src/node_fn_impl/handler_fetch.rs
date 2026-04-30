use proc_macro2::{Span, TokenStream};
use quote::quote;
use syn::LitStr;

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
        let (ty_core, is_ref, is_ref_mut) = if let syn::Type::Reference(r) = ty {
            (&*r.elem, true, r.mutability.is_some())
        } else {
            (ty, false, false)
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
        let is_payload = if let syn::Type::Path(tp) = ty_core
            && tp.qself.is_none()
            && let Some(seg) = tp.path.segments.last()
        {
            seg.ident == "Compute"
        } else {
            false
        };

        let fanin_inner = if let syn::Type::Path(tp) = ty_core
            && tp.qself.is_none()
            && let Some(seg) = tp.path.segments.last()
            && seg.ident == "FanIn"
            && let syn::PathArguments::AngleBracketed(ab) = &seg.arguments
            && let Some(syn::GenericArgument::Type(inner_ty)) = ab.args.first()
        {
            Some(inner_ty)
        } else {
            None
        };
        let option_inner = if let syn::Type::Path(tp) = ty_core
            && tp.qself.is_none()
            && let Some(seg) = tp.path.segments.last()
            && seg.ident == "Option"
            && let syn::PathArguments::AngleBracketed(ab) = &seg.arguments
            && let Some(syn::GenericArgument::Type(inner_ty)) = ab.args.first()
        {
            Some(inner_ty)
        } else {
            None
        };

        let fetch = if let Some(inner_ty) = fanin_inner {
            let tmp_ident = syn::Ident::new(&format!("__fanin_indexed_{idx}"), Span::call_site());
            quote! {
                let #tmp_ident = io.get_all_fanin_indexed::<#inner_ty>(#port);
                let #ident = #runtime_crate::FanIn::<#inner_ty>::from_indexed(#tmp_ident);
            }
        } else if let Some(inner_ty) = option_inner
            && mode == "owned"
        {
            quote! {
                let #ident = io.get_typed::<#inner_ty>(#port);
            }
        } else if let syn::Type::Path(tp) = ty_core {
            if let Some(seg) = tp.path.segments.last() {
                let ident_seg = &seg.ident;
                if ident_seg == "Arc" {
                    if let syn::PathArguments::AngleBracketed(ab) = &seg.arguments {
                        if let Some(syn::GenericArgument::Type(inner_ty)) = ab.args.first() {
                            match mode {
                                "borrowed" => {
                                    let tmp_ident =
                                        syn::Ident::new(&format!("__arc_{idx}"), Span::call_site());
                                    quote! {
                                        let #tmp_ident = io
                                            .get_arc::<#inner_ty>(#port)
                                            .ok_or_else(|| #runtime_crate::NodeError::InvalidInput(format!("missing {}", #port)))?;
                                        let #ident = &#tmp_ident;
                                    }
                                }
                                "borrowed_mut" => {
                                    let tmp_ident = syn::Ident::new(
                                        &format!("__arc_mut_{idx}"),
                                        Span::call_site(),
                                    );
                                    quote! {
                                        let mut #tmp_ident = io
                                            .get_arc::<#inner_ty>(#port)
                                            .ok_or_else(|| #runtime_crate::NodeError::InvalidInput(format!("missing {}", #port)))?;
                                        let #ident = &mut #tmp_ident;
                                    }
                                }
                                _ => quote! {
                                    let #ident = io
                                        .get_arc::<#inner_ty>(#port)
                                        .ok_or_else(|| #runtime_crate::NodeError::InvalidInput(format!("missing {}", #port)))?;
                                },
                            }
                        } else {
                            quote! {
                                let #ident = io
                                    .take_owned::<#ty_core>(#port)
                                    .ok_or_else(|| #runtime_crate::NodeError::InvalidInput(format!("missing {}", #port)))?;
                            }
                        }
                    } else {
                        quote! {
                            let #ident = io
                                .take_owned::<#ty_core>(#port)
                                .ok_or_else(|| #runtime_crate::NodeError::InvalidInput(format!("missing {}", #port)))?;
                        }
                    }
                } else if ident_seg == "Compute" {
                    if let syn::PathArguments::AngleBracketed(ab) = &seg.arguments {
                        if let Some(syn::GenericArgument::Type(inner_ty)) = ab.args.first() {
                            match mode {
                                "borrowed" => {
                                    let tmp_ident = syn::Ident::new(
                                        &format!("__payload_ref_{idx}"),
                                        Span::call_site(),
                                    );
                                    quote! {
                                        let #tmp_ident = io
                                            .get_compute::<#inner_ty>(#port)
                                            .ok_or_else(|| #runtime_crate::NodeError::InvalidInput(format!("missing {}", #port)))?;
                                        let #ident = &#tmp_ident;
                                    }
                                }
                                "borrowed_mut" => {
                                    let tmp_ident = syn::Ident::new(
                                        &format!("__payload_mut_{idx}"),
                                        Span::call_site(),
                                    );
                                    quote! {
                                        let mut #tmp_ident = io
                                            .get_compute_mut::<#inner_ty>(#port)
                                            .ok_or_else(|| #runtime_crate::NodeError::InvalidInput(format!("missing {}", #port)))?;
                                        let #ident = &mut #tmp_ident;
                                    }
                                }
                                "owned_mut" => quote! {
                                    let #ident = io
                                        .get_compute_mut::<#inner_ty>(#port)
                                        .ok_or_else(|| #runtime_crate::NodeError::InvalidInput(format!("missing {}", #port)))?;
                                },
                                _ => quote! {
                                    let #ident = io
                                        .get_compute::<#inner_ty>(#port)
                                        .ok_or_else(|| #runtime_crate::NodeError::InvalidInput(format!("missing {}", #port)))?;
                                },
                            }
                        } else {
                            quote! {
                                let #ident = io
                                    .take_owned::<#ty_core>(#port)
                                    .ok_or_else(|| #runtime_crate::NodeError::InvalidInput(format!("missing {}", #port)))?;
                            }
                        }
                    } else {
                        quote! {
                            let #ident = io
                                .take_owned::<#ty_core>(#port)
                                .ok_or_else(|| #runtime_crate::NodeError::InvalidInput(format!("missing {}", #port)))?;
                        }
                    }
                } else {
                    match mode {
                        "borrowed" => quote! {
                            let #ident = io
                                .get_ref::<#ty_core>(#port)
                                .ok_or_else(|| #runtime_crate::NodeError::InvalidInput(format!("missing {}", #port)))?;
                        },
                        "borrowed_mut" => {
                            let tmp_ident = syn::Ident::new(
                                &format!("__borrowed_mut_{idx}"),
                                Span::call_site(),
                            );
                            quote! {
                                let mut #tmp_ident = io
                                    .take_modify::<#ty_core>(#port)
                                    .ok_or_else(|| #runtime_crate::NodeError::InvalidInput(format!("missing {}", #port)))?;
                                let #ident = &mut #tmp_ident;
                            }
                        }
                        "owned_mut" => quote! {
                            let #ident = io
                                .take_owned::<#ty_core>(#port)
                                .ok_or_else(|| #runtime_crate::NodeError::InvalidInput(format!("missing {}", #port)))?;
                        },
                        _ => quote! {
                            let #ident = io
                                .take_owned::<#ty_core>(#port)
                                .ok_or_else(|| #runtime_crate::NodeError::InvalidInput(format!("missing {}", #port)))?;
                        },
                    }
                }
            } else {
                match mode {
                    "borrowed" => quote! {
                        let #ident = io
                            .get_ref::<#ty_core>(#port)
                            .ok_or_else(|| #runtime_crate::NodeError::InvalidInput(format!("missing {}", #port)))?;
                    },
                    "borrowed_mut" => {
                        let tmp_ident =
                            syn::Ident::new(&format!("__borrowed_mut_{idx}"), Span::call_site());
                        quote! {
                            let mut #tmp_ident = io
                                .take_modify::<#ty_core>(#port)
                                .ok_or_else(|| #runtime_crate::NodeError::InvalidInput(format!("missing {}", #port)))?;
                            let #ident = &mut #tmp_ident;
                        }
                    }
                    "owned_mut" => quote! {
                        let #ident = io
                            .take_owned::<#ty_core>(#port)
                            .ok_or_else(|| #runtime_crate::NodeError::InvalidInput(format!("missing {}", #port)))?;
                    },
                    _ => quote! {
                        let #ident = io
                            .take_owned::<#ty_core>(#port)
                            .ok_or_else(|| #runtime_crate::NodeError::InvalidInput(format!("missing {}", #port)))?;
                    },
                }
            }
        } else {
            match mode {
                "borrowed" => quote! {
                    let #ident = io
                        .get_ref::<#ty_core>(#port)
                        .ok_or_else(|| #runtime_crate::NodeError::InvalidInput(format!("missing {}", #port)))?;
                },
                "borrowed_mut" => {
                    let tmp_ident =
                        syn::Ident::new(&format!("__borrowed_mut_{idx}"), Span::call_site());
                    quote! {
                        let mut #tmp_ident = io
                            .take_modify::<#ty_core>(#port)
                            .ok_or_else(|| #runtime_crate::NodeError::InvalidInput(format!("missing {}", #port)))?;
                        let #ident = &mut #tmp_ident;
                    }
                }
                "owned_mut" => quote! {
                    let #ident = io
                        .take_owned::<#ty_core>(#port)
                        .ok_or_else(|| #runtime_crate::NodeError::InvalidInput(format!("missing {}", #port)))?;
                },
                _ => quote! {
                    let #ident = io
                        .take_owned::<#ty_core>(#port)
                        .ok_or_else(|| #runtime_crate::NodeError::InvalidInput(format!("missing {}", #port)))?;
                },
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
