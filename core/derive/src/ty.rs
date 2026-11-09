//! `#[derive(Type)]`.

use proc_macro2::TokenStream;
use quote::quote;
use syn::{Data, DeriveInput, Error, Expr, Fields, Result, Type};

use crate::local_key_check;

fn expr_of(ty: &Type) -> TokenStream {
    quote!(<#ty as ::daedalus_registry::__private::Typed>::EXPR)
}

/// The expression for a set of fields, plus the checks on their types.
fn fields_expr(fields: &Fields, checks: &mut TokenStream) -> TokenStream {
    let p = quote!(::daedalus_registry::__private);
    for field in fields {
        checks.extend(local_key_check(&field.ty));
    }
    match fields {
        Fields::Named(named) => {
            let rows = named.named.iter().map(|field| {
                let name = field.ident.as_ref().map(ToString::to_string);
                let ty = expr_of(&field.ty);
                quote!(#p::Field { name: #name, ty: #ty })
            });
            quote!({
                const FIELDS: &[#p::Field] = &[#(#rows),*];
                #p::TypeExpr::Struct(FIELDS)
            })
        }
        Fields::Unnamed(unnamed) if unnamed.unnamed.len() == 1 => expr_of(&unnamed.unnamed[0].ty),
        Fields::Unnamed(unnamed) => {
            let rows = unnamed.unnamed.iter().map(|field| expr_of(&field.ty));
            quote!({
                const ITEMS: &[#p::TypeExpr] = &[#(#rows),*];
                #p::TypeExpr::Tuple(ITEMS)
            })
        }
        Fields::Unit => quote!(#p::TypeExpr::Scalar(#p::Scalar::Unit)),
    }
}

fn key_expr(input: &DeriveInput) -> Result<TokenStream> {
    let mut explicit = None;
    for attr in &input.attrs {
        if attr.path().is_ident("type_key") {
            explicit = Some(attr.parse_args::<Expr>()?);
        }
    }
    Ok(match explicit {
        Some(key) => quote!(#key),
        None => {
            let name = input.ident.to_string().to_lowercase();
            quote!(::core::concat!("rust:", ::core::module_path!(), "::", #name))
        }
    })
}

pub(crate) fn derive(input: DeriveInput) -> Result<TokenStream> {
    if !input.generics.params.is_empty() {
        return Err(Error::new_spanned(
            &input.generics,
            "#[derive(Type)] takes no generics",
        ));
    }
    let p = quote!(::daedalus_registry::__private);
    let name = &input.ident;
    let key = key_expr(&input)?;
    let mut checks = TokenStream::new();
    let expr = match &input.data {
        Data::Struct(data) => fields_expr(&data.fields, &mut checks),
        Data::Enum(data) => {
            let mut rows = Vec::new();
            for variant in &data.variants {
                let text = variant.ident.to_string();
                rows.push(if matches!(variant.fields, Fields::Unit) {
                    quote!(#p::Variant { name: #text, payload: ::core::option::Option::None })
                } else {
                    let payload = fields_expr(&variant.fields, &mut checks);
                    quote!(#p::Variant { name: #text, payload: ::core::option::Option::Some(#payload) })
                });
            }
            quote!({
                const VARIANTS: &[#p::Variant] = &[#(#rows),*];
                #p::TypeExpr::Enum(VARIANTS)
            })
        }
        Data::Union(data) => {
            return Err(Error::new_spanned(
                data.union_token,
                "#[derive(Type)] can't describe a union",
            ));
        }
    };
    Ok(quote! {
        #checks

        impl #p::Typed for #name {
            const KEY: #p::TypeKey = #p::TypeKey::new(#key);
            const EXPR: #p::TypeExpr = #expr;
        }
    })
}
