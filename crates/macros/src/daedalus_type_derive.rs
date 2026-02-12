use proc_macro::TokenStream;
use proc_macro2::Span;
use quote::{ToTokens, quote};
use syn::{
    Data, DeriveInput, Fields, Lit, LitStr, Meta, MetaList, MetaNameValue, parse_macro_input,
};

use crate::helpers::{
    NestedMeta, SerdeRenameAll, compile_error, parse_nested, parse_serde_rename_all,
    serde_name_for_ident,
};

fn parse_type_key(attrs: &[syn::Attribute]) -> Result<Option<LitStr>, proc_macro2::TokenStream> {
    for attr in attrs {
        if !attr.path().is_ident("daedalus") {
            continue;
        }
        let Meta::List(MetaList { .. }) = &attr.meta else {
            continue;
        };
        let Meta::List(list) = attr.meta.clone() else {
            continue;
        };
        let items = parse_nested(&list)?;
        for item in items {
            let NestedMeta::Meta(Meta::NameValue(MetaNameValue { path, value, .. })) = item else {
                continue;
            };
            let Some(ident) = path.get_ident() else {
                continue;
            };
            if ident != "type_key" && ident != "key" {
                continue;
            }
            let syn::Expr::Lit(expr_lit) = value else {
                return Err(compile_error(
                    "daedalus(type_key=...) must be a string literal".into(),
                ));
            };
            let Lit::Str(s) = expr_lit.lit else {
                return Err(compile_error(
                    "daedalus(type_key=...) must be a string literal".into(),
                ));
            };
            return Ok(Some(s));
        }
    }
    Ok(None)
}

fn type_expr_for(
    ty: &syn::Type,
    data_crate: &proc_macro2::TokenStream,
) -> proc_macro2::TokenStream {
    // Reuse the same strategy as NodeConfig/node macro: prefer explicit runtime overrides (typing registry)
    // but fall back to a structural encoding for common containers and primitives.
    fn inner(
        ty: &syn::Type,
        data_crate: &proc_macro2::TokenStream,
    ) -> Option<proc_macro2::TokenStream> {
        match ty {
            syn::Type::Path(p) if p.qself.is_none() => {
                let ident = p.path.segments.last().map(|s| s.ident.to_string())?;
                match ident.as_str() {
                    "Box" | "Arc" => p.path.segments.last().and_then(|s| match &s.arguments {
                        syn::PathArguments::AngleBracketed(ab) => ab.args.first(),
                        _ => None,
                    }).and_then(|arg| {
                        if let syn::GenericArgument::Type(inner_ty) = arg {
                            inner(inner_ty, data_crate)
                        } else {
                            None
                        }
                    }),
                    "Vec" => p.path.segments.last().and_then(|s| match &s.arguments {
                        syn::PathArguments::AngleBracketed(ab) => ab.args.first(),
                        _ => None,
                    }).and_then(|arg| {
                        if let syn::GenericArgument::Type(inner_ty) = arg {
                            let inner_ts = inner(inner_ty, data_crate)?;
                            Some(quote! {
                                if let Some(explicit) = #data_crate::typing::override_type_expr::<#ty>() {
                                    explicit
                                } else {
                                    #data_crate::model::TypeExpr::List(Box::new(#inner_ts))
                                }
                            })
                        } else {
                            None
                        }
                    }),
                    "Option" => p.path.segments.last().and_then(|s| match &s.arguments {
                        syn::PathArguments::AngleBracketed(ab) => ab.args.first(),
                        _ => None,
                    }).and_then(|arg| {
                        if let syn::GenericArgument::Type(inner_ty) = arg {
                            let inner_ts = inner(inner_ty, data_crate)?;
                            Some(quote! {
                                if let Some(explicit) = #data_crate::typing::override_type_expr::<#ty>() {
                                    explicit
                                } else {
                                    #data_crate::model::TypeExpr::Optional(Box::new(#inner_ts))
                                }
                            })
                        } else {
                            None
                        }
                    }),
                    _ => Some(quote! { #data_crate::typing::type_expr::<#ty>() }),
                }
            }
            syn::Type::Array(a) => {
                let elem_ty = &a.elem;
                let inner_ts = inner(elem_ty, data_crate)?;
                Some(quote! {
                    if let Some(explicit) = #data_crate::typing::override_type_expr::<#ty>() {
                        explicit
                    } else {
                        #data_crate::model::TypeExpr::List(Box::new(#inner_ts))
                    }
                })
            }
            syn::Type::Reference(r) => inner(&r.elem, data_crate),
            syn::Type::Tuple(t) => {
                if t.elems.is_empty() {
                    return Some(
                        quote! { #data_crate::model::TypeExpr::Scalar(#data_crate::model::ValueType::Unit) },
                    );
                }
                let mut elems = Vec::new();
                for elem in &t.elems {
                    elems.push(inner(elem, data_crate)?);
                }
                Some(quote! { #data_crate::model::TypeExpr::Tuple(vec![#(#elems),*]) })
            }
            _ => None,
        }
    }

    if let Some(ts) = inner(ty, data_crate) {
        ts
    } else {
        // Opaque fallback for weird types; still stable and schema'd.
        let mut raw = ty.to_token_stream().to_string();
        raw.retain(|c| !c.is_whitespace());
        let lit = LitStr::new(&format!("rust:{raw}"), Span::call_site());
        quote! { #data_crate::model::TypeExpr::Opaque(::std::string::String::from(#lit)) }
    }
}

pub fn daedalus_type_expr(item: TokenStream) -> TokenStream {
    let input = parse_macro_input!(item as DeriveInput);
    let name = input.ident.clone();
    if !input.generics.params.is_empty() {
        return TokenStream::from(compile_error(
            "DaedalusTypeExpr does not support generics yet".into(),
        ));
    }

    let data_crate: proc_macro2::TokenStream = quote! { ::daedalus_data };
    let rename_all: Option<SerdeRenameAll> = parse_serde_rename_all(&input.attrs);
    let type_key_tokens: proc_macro2::TokenStream = match parse_type_key(&input.attrs) {
        Ok(Some(s)) => quote! { #s },
        Ok(None) => {
            // Default to a stable Rust-path key in the *consumer* crate.
            quote! { ::core::concat!("rust:", ::core::module_path!(), "::", ::core::stringify!(#name)) }
        }
        Err(e) => return TokenStream::from(e),
    };

    let type_expr_body: proc_macro2::TokenStream = match &input.data {
        Data::Struct(s) => match &s.fields {
            Fields::Named(fields) => {
                let mut out_fields = Vec::new();
                for field in &fields.named {
                    let Some(ident) = &field.ident else { continue };
                    let fname = serde_name_for_ident(ident, &field.attrs, rename_all);
                    let fty = type_expr_for(&field.ty, &data_crate);
                    out_fields.push(quote! {
                        #data_crate::model::StructField {
                            name: ::std::string::String::from(#fname),
                            ty: #fty,
                        }
                    });
                }
                quote! { #data_crate::model::TypeExpr::Struct(vec![#(#out_fields),*]) }
            }
            Fields::Unit => {
                quote! { #data_crate::model::TypeExpr::Scalar(#data_crate::model::ValueType::Unit) }
            }
            Fields::Unnamed(fields) => {
                if fields.unnamed.len() == 1 {
                    let ty = &fields.unnamed.first().unwrap().ty;
                    let inner = type_expr_for(ty, &data_crate);
                    quote! { #inner }
                } else {
                    return TokenStream::from(compile_error(
                        "DaedalusTypeExpr only supports tuple structs with a single field".into(),
                    ));
                }
            }
        },
        Data::Enum(e) => {
            let mut variants = Vec::new();
            for v in &e.variants {
                let vname = serde_name_for_ident(&v.ident, &v.attrs, rename_all);
                let ty_opt = match &v.fields {
                    Fields::Unit => quote! { None },
                    Fields::Unnamed(f) if f.unnamed.len() == 1 => {
                        let inner_ty = &f.unnamed.first().unwrap().ty;
                        let inner_ts = type_expr_for(inner_ty, &data_crate);
                        quote! { Some(#inner_ts) }
                    }
                    Fields::Named(f) if f.named.len() == 1 => {
                        let inner_ty = &f.named.first().unwrap().ty;
                        let inner_ts = type_expr_for(inner_ty, &data_crate);
                        quote! { Some(#inner_ts) }
                    }
                    _ => {
                        return TokenStream::from(compile_error(
                            "DaedalusTypeExpr enum variants must be unit or single-payload".into(),
                        ));
                    }
                };
                variants.push(quote! {
                    #data_crate::model::EnumVariant {
                        name: ::std::string::String::from(#vname),
                        ty: #ty_opt,
                    }
                });
            }
            quote! { #data_crate::model::TypeExpr::Enum(vec![#(#variants),*]) }
        }
        Data::Union(_) => {
            return TokenStream::from(compile_error(
                "DaedalusTypeExpr does not support unions".into(),
            ));
        }
    };

    let expanded = quote! {
        impl #data_crate::daedalus_type::DaedalusTypeExpr for #name {
            const TYPE_KEY: &'static str = #type_key_tokens;
            fn type_expr() -> #data_crate::model::TypeExpr {
                #type_expr_body
            }
        }
    };

    TokenStream::from(expanded)
}
