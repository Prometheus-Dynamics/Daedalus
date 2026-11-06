//! `#[derive(StablePod)]`: a `#[repr(C)]` struct of plain-old-data fields that crosses the stable
//! plugin boundary as a typed byte view (see `daedalus_transport::StablePod`).

use proc_macro::TokenStream;
use quote::quote;
use syn::{Data, DeriveInput, Fields, LitStr, parse_macro_input};

use crate::helpers::{DaedalusCrate, compile_error};

pub fn stable_pod(item: TokenStream) -> TokenStream {
    let input = parse_macro_input!(item as DeriveInput);
    match expand(&input) {
        Ok(tokens) => tokens.into(),
        Err(message) => compile_error(message).into(),
    }
}

fn expand(input: &DeriveInput) -> Result<proc_macro2::TokenStream, String> {
    let ident = &input.ident;
    if !input.generics.params.is_empty() {
        return Err("StablePod does not support generic structs".into());
    }
    check_repr(input)?;
    let mut name = ident.to_string();
    for attr in &input.attrs {
        if attr.path().is_ident("stable_pod") {
            attr.parse_nested_meta(|meta| {
                if meta.path.is_ident("name") {
                    name = meta.value()?.parse::<LitStr>()?.value();
                    Ok(())
                } else {
                    Err(meta.error("expected `name = \"...\"`"))
                }
            })
            .map_err(|err| err.to_string())?;
        }
    }
    let Data::Struct(data) = &input.data else {
        return Err("StablePod is for `#[repr(C)]` structs".into());
    };
    let fields: Vec<(String, proc_macro2::TokenStream, &syn::Type)> = match &data.fields {
        Fields::Named(fields) => fields
            .named
            .iter()
            .map(|field| {
                let member = field.ident.clone().expect("named");
                (member.to_string(), quote! { #member }, &field.ty)
            })
            .collect(),
        Fields::Unnamed(fields) => fields
            .unnamed
            .iter()
            .enumerate()
            .map(|(index, field)| {
                let member = syn::Index::from(index);
                (index.to_string(), quote! { #member }, &field.ty)
            })
            .collect(),
        Fields::Unit => return Err("StablePod needs at least one field".into()),
    };
    let transport = DaedalusCrate::Transport.path();
    let entries = fields.iter().map(|(name, member, ty)| {
        quote! {
            #transport::PodField {
                name: #name,
                offset: ::core::mem::offset_of!(Self, #member),
                hash: <#ty as #transport::StablePod>::LAYOUT.hash,
            }
        }
    });
    let sizes = fields
        .iter()
        .map(|(_, _, ty)| quote! { ::core::mem::size_of::<#ty>() });
    Ok(quote! {
        // Safety: `#[repr(C)]` (checked by the derive), every field is `StablePod` (the bound on
        // each field's layout below), no padding (asserted below), alignment at most
        // `POD_MAX_ALIGN` (asserted below); the layout is computed from the type itself.
        unsafe impl #transport::StablePod for #ident {
            const LAYOUT: #transport::PodLayout = #transport::PodLayout::structure(
                #name,
                ::core::mem::size_of::<Self>(),
                ::core::mem::align_of::<Self>(),
                &[#(#entries),*],
            );
        }
        const _: () = {
            assert!(
                ::core::mem::size_of::<#ident>() == 0 #(+ #sizes)*,
                "StablePod structs must have no padding: add explicit padding fields",
            );
            assert!(
                ::core::mem::align_of::<#ident>() <= #transport::POD_MAX_ALIGN,
                "StablePod structs are aligned to at most 8 bytes",
            );
        };
    })
}

fn check_repr(input: &DeriveInput) -> Result<(), String> {
    let mut repr_c = false;
    for attr in input
        .attrs
        .iter()
        .filter(|attr| attr.path().is_ident("repr"))
    {
        attr.parse_nested_meta(|meta| {
            if meta.path.is_ident("C") {
                repr_c = true;
                Ok(())
            } else {
                Err(meta.error("StablePod allows only `#[repr(C)]`"))
            }
        })
        .map_err(|err| err.to_string())?;
    }
    if repr_c {
        Ok(())
    } else {
        Err("StablePod structs must be `#[repr(C)]`".into())
    }
}
