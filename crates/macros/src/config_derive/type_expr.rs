//! `TypeExpr` token generation for `#[derive(NodeConfig)]` field types.

use proc_macro2::Span;
use quote::{ToTokens, quote};
use syn::LitStr;

pub(super) fn type_expr_for(
    ty: &syn::Type,
    generic_type_params: &::std::collections::HashSet<::std::string::String>,
    data_crate: &proc_macro2::TokenStream,
) -> Option<proc_macro2::TokenStream> {
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
                    return None;
                }
            }
            let ident = p.path.segments.last().map(|s| s.ident.to_string())?;
            match ident.as_str() {
                "Result" => p.path.segments.last().and_then(|s| match &s.arguments {
                    syn::PathArguments::AngleBracketed(ab) => ab.args.first(),
                    _ => None,
                }).and_then(|arg| {
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
                            && let Some(inner_ty) = type_expr_for(inner, generic_type_params, data_crate) {
                                return Some(
                                    quote! {
                                        if let Some(explicit) = #data_crate::typing::override_type_expr::<#ty>() {
                                            explicit
                                        } else {
                                            #data_crate::model::TypeExpr::List(Box::new(#inner_ty))
                                        }
                                    },
                                );
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
                            && let Some(inner_ty) = type_expr_for(inner, generic_type_params, data_crate) {
                                return Some(
                                    quote! {
                                        if let Some(explicit) = #data_crate::typing::override_type_expr::<#ty>() {
                                            explicit
                                        } else {
                                            #data_crate::model::TypeExpr::Optional(Box::new(#inner_ty))
                                        }
                                    },
                                );
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
    data_crate: &proc_macro2::TokenStream,
) -> proc_macro2::TokenStream {
    let mut raw = ty.to_token_stream().to_string();
    raw.retain(|c| !c.is_whitespace());
    let lit = LitStr::new(&format!("rust:{raw}"), Span::call_site());
    quote! { #data_crate::model::TypeExpr::Opaque(::std::string::String::from(#lit)) }
}
