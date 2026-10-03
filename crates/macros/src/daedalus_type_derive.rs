use proc_macro::TokenStream;
use quote::{ToTokens, quote};
use syn::{Data, DeriveInput, Expr, Fields, Meta, MetaNameValue, parse_macro_input};

use crate::helpers::{
    DaedalusCrate, NestedMeta, SerdeRenameAll, compile_error, parse_nested, parse_serde_rename_all,
    serde_name_for_ident, str_expr,
};
use crate::type_expr::{GenericParams, TypeExprOptions};

fn parse_type_key(attrs: &[syn::Attribute]) -> Result<Option<Expr>, proc_macro2::TokenStream> {
    for attr in attrs {
        if !attr.path().is_ident("daedalus") {
            continue;
        }
        let Meta::List(list) = &attr.meta else {
            continue;
        };
        for item in parse_nested(list)? {
            let NestedMeta::Meta(Meta::NameValue(MetaNameValue { path, value, .. })) = item else {
                continue;
            };
            if path.is_ident("type_key") || path.is_ident("key") {
                return str_expr(&value, "daedalus(type_key = ...)").map(Some);
            }
        }
    }
    Ok(None)
}

pub fn daedalus_type_expr(item: TokenStream) -> TokenStream {
    let input = parse_macro_input!(item as DeriveInput);
    let name = input.ident.clone();
    if !input.generics.params.is_empty() {
        return TokenStream::from(compile_error(
            "DaedalusTypeExpr does not support generics yet".into(),
        ));
    }

    let data_crate = DaedalusCrate::Data.path();
    // Leaf types (resolved through the typing registry) are reported as dependencies.
    let mut leaves = Vec::new();
    let type_exprs = TypeExprOptions {
        data_crate: &data_crate,
        generics: GenericParams::None,
        transparent: &[("Box", 0), ("Arc", 0)],
        str_as_string: false,
        arrays_as_lists: true,
    };
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
                    let fty = type_exprs.type_expr_collecting(&field.ty, &mut leaves);
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
                    let inner = type_exprs.type_expr_collecting(ty, &mut leaves);
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
                        let inner_ts = type_exprs.type_expr_collecting(inner_ty, &mut leaves);
                        quote! { Some(#inner_ts) }
                    }
                    Fields::Named(f) if f.named.len() == 1 => {
                        let inner_ty = &f.named.first().unwrap().ty;
                        let inner_ts = type_exprs.type_expr_collecting(inner_ty, &mut leaves);
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

    let mut seen = std::collections::BTreeSet::new();
    leaves.retain(|ty| seen.insert(ty.to_token_stream().to_string()));
    let visit_dependencies = (!leaves.is_empty()).then(|| {
        let support = quote! { #data_crate::daedalus_type::derive_support };
        quote! {
            fn visit_dependencies<V: #data_crate::daedalus_type::DaedalusTypeVisitor>(
                __visitor: &mut V,
            ) {
                // Only the trait matching each probe is used; which one depends on the fields.
                #[allow(unused_imports)]
                use #support::{VisitTyped as _, VisitUntyped as _};
                #(
                    (&#support::Probe::<#leaves>(::core::marker::PhantomData))
                        .visit_into(__visitor);
                )*
            }
        }
    });
    let expanded = quote! {
        impl #data_crate::daedalus_type::DaedalusTypeExpr for #name {
            const TYPE_KEY: &'static str = #type_key_tokens;
            fn type_expr() -> #data_crate::model::TypeExpr {
                #type_expr_body
            }
            #visit_dependencies
        }
    };

    TokenStream::from(expanded)
}
