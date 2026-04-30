use proc_macro2::TokenStream;
use quote::{ToTokens, quote};
use syn::LitStr;

pub(super) fn ok_type_from_return(ret: &syn::ReturnType) -> Option<&syn::Type> {
    let ty = match ret {
        syn::ReturnType::Default => return None,
        syn::ReturnType::Type(_, ty) => ty.as_ref(),
    };

    match ty {
        syn::Type::Path(p) if p.qself.is_none() => {
            let last = p.path.segments.last()?;
            if last.ident != "Result" {
                return Some(ty);
            }
            match &last.arguments {
                syn::PathArguments::AngleBracketed(ab) => ab.args.first().and_then(|arg| {
                    if let syn::GenericArgument::Type(inner) = arg {
                        Some(inner)
                    } else {
                        None
                    }
                }),
                _ => None,
            }
        }
        _ => Some(ty),
    }
}

pub(super) fn payload_inner_type(ty: &syn::Type) -> Option<&syn::Type> {
    let tp = match ty {
        syn::Type::Path(tp) if tp.qself.is_none() => tp,
        _ => return None,
    };
    let last = tp.path.segments.last()?;
    if last.ident != "Compute" {
        return None;
    }
    match &last.arguments {
        syn::PathArguments::AngleBracketed(ab) => ab.args.first().and_then(|arg| {
            if let syn::GenericArgument::Type(inner) = arg {
                Some(inner)
            } else {
                None
            }
        }),
        _ => None,
    }
}

pub(super) fn arc_inner_type(ty: &syn::Type) -> Option<&syn::Type> {
    let tp = match ty {
        syn::Type::Path(tp) if tp.qself.is_none() => tp,
        _ => return None,
    };
    let last = tp.path.segments.last()?;
    if last.ident != "Arc" {
        return None;
    }
    match &last.arguments {
        syn::PathArguments::AngleBracketed(ab) => ab.args.first().and_then(|arg| {
            if let syn::GenericArgument::Type(inner) = arg {
                Some(inner)
            } else {
                None
            }
        }),
        _ => None,
    }
}

pub(super) fn is_unit_type(ty: &syn::Type) -> bool {
    matches!(ty, syn::Type::Tuple(t) if t.elems.is_empty())
}

pub(super) fn type_expr_for(
    ty: &syn::Type,
    generic_type_params: &::std::collections::HashSet<::std::string::String>,
    data_crate: &TokenStream,
) -> Option<TokenStream> {
    match ty {
        syn::Type::Path(p) if p.qself.is_none() => {
            if p.path.segments.len() == 1
                && matches!(
                    p.path.segments.first().map(|s| &s.arguments),
                    Some(syn::PathArguments::None)
                )
            {
                let ident = p.path.segments.first()?.ident.to_string();
                if generic_type_params.contains(&ident) {
                    let lit = LitStr::new("generic", proc_macro2::Span::call_site());
                    return Some(quote! {
                        #data_crate::model::TypeExpr::Opaque(::std::string::String::from(#lit))
                    });
                }
            }
            let ident = p.path.segments.last().map(|s| s.ident.to_string())?;
            match ident.as_str() {
                "Cpu" | "Gpu" => p
                    .path
                    .segments
                    .last()
                    .and_then(|s| match &s.arguments {
                        syn::PathArguments::AngleBracketed(ab) => ab.args.first(),
                        _ => None,
                    })
                    .and_then(|arg| {
                        if let syn::GenericArgument::Type(inner) = arg {
                            type_expr_for(inner, generic_type_params, data_crate)
                        } else {
                            None
                        }
                    }),
                "Device" => p
                    .path
                    .segments
                    .last()
                    .and_then(|s| match &s.arguments {
                        syn::PathArguments::AngleBracketed(ab) => ab.args.iter().nth(1),
                        _ => None,
                    })
                    .and_then(|arg| {
                        if let syn::GenericArgument::Type(inner) = arg {
                            type_expr_for(inner, generic_type_params, data_crate)
                        } else {
                            None
                        }
                    }),
                "Result" => p
                    .path
                    .segments
                    .last()
                    .and_then(|s| match &s.arguments {
                        syn::PathArguments::AngleBracketed(ab) => ab.args.first(),
                        _ => None,
                    })
                    .and_then(|arg| {
                        if let syn::GenericArgument::Type(inner) = arg {
                            type_expr_for(inner, generic_type_params, data_crate)
                        } else {
                            None
                        }
                    }),
                "Vec" => p
                    .path
                    .segments
                    .last()
                    .and_then(|s| match &s.arguments {
                        syn::PathArguments::AngleBracketed(ab) => ab.args.first(),
                        _ => None,
                    })
                    .and_then(|arg| {
                        if let syn::GenericArgument::Type(inner) = arg
                            && let Some(inner_ty) =
                                type_expr_for(inner, generic_type_params, data_crate)
                        {
                            return Some(quote! {
                                if let Some(explicit) = #data_crate::typing::override_type_expr::<#ty>() {
                                    explicit
                                } else {
                                    #data_crate::model::TypeExpr::List(Box::new(#inner_ty))
                                }
                            });
                        }
                        None
                    }),
                "Option" => p
                    .path
                    .segments
                    .last()
                    .and_then(|s| match &s.arguments {
                        syn::PathArguments::AngleBracketed(ab) => ab.args.first(),
                        _ => None,
                    })
                    .and_then(|arg| {
                        if let syn::GenericArgument::Type(inner) = arg
                            && let Some(inner_ty) =
                                type_expr_for(inner, generic_type_params, data_crate)
                        {
                            return Some(quote! {
                                if let Some(explicit) = #data_crate::typing::override_type_expr::<#ty>() {
                                    explicit
                                } else {
                                    #data_crate::model::TypeExpr::Optional(Box::new(#inner_ty))
                                }
                            });
                        }
                        None
                    }),
                _ => Some(quote! { #data_crate::typing::type_expr::<#ty>() }),
            }
        }
        syn::Type::Reference(r) => {
            if let syn::Type::Path(p) = &*r.elem {
                let ident = p.path.segments.last().map(|s| s.ident.to_string())?;
                match ident.as_str() {
                    "str" => Some(
                        quote! { #data_crate::model::TypeExpr::Scalar(#data_crate::model::ValueType::String) },
                    ),
                    _ => type_expr_for(&r.elem, generic_type_params, data_crate),
                }
            } else {
                type_expr_for(&r.elem, generic_type_params, data_crate)
            }
        }
        syn::Type::Tuple(t) => {
            if t.elems.is_empty() {
                return Some(
                    quote! { #data_crate::model::TypeExpr::Scalar(#data_crate::model::ValueType::Unit) },
                );
            }
            let mut elems = Vec::new();
            for elem in &t.elems {
                if let Some(ts) = type_expr_for(elem, generic_type_params, data_crate) {
                    elems.push(ts);
                } else {
                    return None;
                }
            }
            Some(quote! { #data_crate::model::TypeExpr::Tuple(vec![#(#elems),*]) })
        }
        _ => None,
    }
}

pub(super) fn opaque_fallback_type_expr_for(
    ty: &syn::Type,
    data_crate: &TokenStream,
) -> TokenStream {
    let mut raw = ty.to_token_stream().to_string();
    raw.retain(|c| !c.is_whitespace());
    let lit = LitStr::new(&format!("rust:{raw}"), proc_macro2::Span::call_site());
    quote! { #data_crate::model::TypeExpr::Opaque(::std::string::String::from(#lit)) }
}

pub(super) fn contract_type_for(ty: &syn::Type) -> Option<&syn::Type> {
    let ty = if let syn::Type::Reference(r) = ty {
        &*r.elem
    } else {
        ty
    };
    let syn::Type::Path(path) = ty else {
        return None;
    };
    if path.qself.is_some() {
        return None;
    }
    let ident = path.path.segments.last()?.ident.to_string();
    if matches!(
        ident.as_str(),
        "FanIn"
            | "Payload"
            | "CorrelatedPayload"
            | "NodeIo"
            | "RuntimeNode"
            | "ExecutionContext"
            | "ShaderContext"
            | "GraphCtx"
    ) {
        return None;
    }
    Some(ty)
}

pub(super) fn output_contract_types(ret: &syn::ReturnType, outputs_len: usize) -> Vec<&syn::Type> {
    fn peel_result_or_option(ty: &syn::Type) -> &syn::Type {
        if let syn::Type::Path(path) = ty
            && path.qself.is_none()
            && let Some(seg) = path.path.segments.last()
        {
            let ident = seg.ident.to_string();
            if matches!(ident.as_str(), "Result" | "Option")
                && let syn::PathArguments::AngleBracketed(args) = &seg.arguments
                && let Some(syn::GenericArgument::Type(inner)) = args.args.first()
            {
                return peel_result_or_option(inner);
            }
        }
        ty
    }

    let syn::ReturnType::Type(_, ty) = ret else {
        return Vec::new();
    };
    let base = peel_result_or_option(ty);
    if let syn::Type::Tuple(tuple) = base
        && tuple.elems.len() == outputs_len
    {
        return tuple.elems.iter().collect();
    }
    if outputs_len == 1 {
        return vec![base];
    }
    Vec::new()
}

pub(super) fn result_ok_type(ret: &syn::ReturnType) -> Option<&syn::Type> {
    let syn::ReturnType::Type(_, ty) = ret else {
        return None;
    };
    let syn::Type::Path(path) = ty.as_ref() else {
        return None;
    };
    let last = path.path.segments.last()?;
    if last.ident != "Result" {
        return None;
    }
    let syn::PathArguments::AngleBracketed(args) = &last.arguments else {
        return None;
    };
    args.args.first().and_then(|arg| {
        if let syn::GenericArgument::Type(inner) = arg {
            Some(inner)
        } else {
            None
        }
    })
}

pub(super) fn direct_payload_plain_type(ty: &syn::Type) -> Option<&syn::Type> {
    match ty {
        syn::Type::Path(path) if path.qself.is_none() => {
            let last = path.path.segments.last()?;
            if matches!(
                last.ident.to_string().as_str(),
                "Arc" | "Compute" | "Option"
            ) {
                return None;
            }
            Some(ty)
        }
        _ => None,
    }
}

pub(super) fn direct_payload_same_type(left: &syn::Type, right: &syn::Type) -> bool {
    let mut left = left.to_token_stream().to_string();
    left.retain(|c| !c.is_whitespace());
    let mut right = right.to_token_stream().to_string();
    right.retain(|c| !c.is_whitespace());
    left == right
}
