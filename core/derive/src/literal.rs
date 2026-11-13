//! `#[derive(FromLiteral)]`.

use proc_macro2::TokenStream;
use quote::quote;
use syn::{Data, DeriveInput, Error, Fields, Result};

/// A struct with named fields, read by field name from a struct constant. Every field must be in
/// the constant, and the constant may name no other, which is what the planner checks too.
pub(crate) fn derive(input: DeriveInput) -> Result<TokenStream> {
    let name = &input.ident;
    if !input.generics.params.is_empty() {
        return Err(Error::new_spanned(
            &input.generics,
            "FromLiteral is derived for types with no parameters",
        ));
    }
    let Data::Struct(data) = &input.data else {
        return Err(Error::new_spanned(
            name,
            "FromLiteral is derived for structs with named fields",
        ));
    };
    let Fields::Named(fields) = &data.fields else {
        return Err(Error::new_spanned(
            name,
            "FromLiteral is derived for structs with named fields",
        ));
    };
    let p = quote!(::daedalus_registry::__private);
    let count = fields.named.len();
    let rows = fields.named.iter().map(|field| {
        let ident = &field.ident;
        let text = ident.as_ref().map(ToString::to_string);
        quote!(#ident: #p::literal_field(fields, #text)?)
    });
    Ok(quote! {
        impl #p::FromLiteral for #name {
            fn from_literal(value: &#p::Literal) -> ::core::result::Result<Self, #p::Error> {
                let #p::Literal::Struct(fields) = value else {
                    return ::core::result::Result::Err(#p::Error::new(
                        #p::Code::InvalidLiteral,
                        "expected a struct",
                    ));
                };
                if fields.len() != #count {
                    return ::core::result::Result::Err(#p::Error::new(
                        #p::Code::InvalidLiteral,
                        "the struct constant has a field the type does not",
                    ));
                }
                ::core::result::Result::Ok(#name { #(#rows),* })
            }
        }
    })
}
