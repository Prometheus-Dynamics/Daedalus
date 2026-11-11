//! `#[derive(PlainData)]`: a `repr(C)` struct whose bytes are its value. The derive gives it the
//! key named in `#[plain(key = "...")]` and a layout hash of its size, alignment and field offsets.

use proc_macro2::{Span, TokenStream as Tokens};
use quote::quote;

use syn::{Attribute, Data, DeriveInput, Error, Fields, Index, LitStr, Member, Result};

pub(crate) fn derive(input: DeriveInput) -> Result<Tokens> {
    let name = &input.ident;
    if !input.generics.params.is_empty() {
        return Err(Error::new_spanned(
            &input.generics,
            "PlainData cannot be derived for a generic type",
        ));
    }
    if !is_repr_c(&input.attrs) {
        return Err(Error::new_spanned(name, "PlainData needs #[repr(C)]"));
    }
    let key = key_attr(&input.attrs)?;
    let Data::Struct(data) = &input.data else {
        return Err(Error::new_spanned(
            name,
            "PlainData can only be derived for a struct",
        ));
    };
    let fields: Vec<(Member, syn::Type)> = match &data.fields {
        Fields::Named(named) => named
            .named
            .iter()
            .filter_map(|field| {
                let ident = field.ident.clone()?;
                Some((Member::Named(ident), field.ty.clone()))
            })
            .collect(),
        Fields::Unnamed(unnamed) => unnamed
            .unnamed
            .iter()
            .enumerate()
            .map(|(index, field)| (Member::Unnamed(Index::from(index)), field.ty.clone()))
            .collect(),
        Fields::Unit => Vec::new(),
    };
    let offsets = fields
        .iter()
        .map(|(member, _)| quote!(::core::mem::offset_of!(Self, #member) as u64));
    let types = fields.iter().map(|(_, ty)| ty);
    // The keyword is written as a token, so this crate's source holds no unsafe code of its own.
    let unsafe_token = syn::token::Unsafe {
        span: Span::call_site(),
    };
    Ok(quote! {
        // SAFETY: the struct is repr(C), has no padding (the check below), and its fields are plain
        // data, so its bytes are its value.
        #unsafe_token impl ::daedalus_abi::PlainData for #name {
            const KEY: &'static str = #key;
            const LAYOUT: u64 = ::daedalus_abi::layout_hash(&[
                ::core::mem::size_of::<Self>() as u64,
                ::core::mem::align_of::<Self>() as u64,
                #(#offsets,)*
            ]);
        }
        const _: () = assert!(
            0 #(+ ::core::mem::size_of::<#types>())* == ::core::mem::size_of::<#name>(),
            "a PlainData type has no padding"
        );
    })
}

/// Whether the struct is `repr(C)`. A `repr` with other items, such as `packed`, still counts.
fn is_repr_c(attrs: &[Attribute]) -> bool {
    attrs.iter().any(|attr| {
        if !attr.path().is_ident("repr") {
            return false;
        }
        let mut c = false;
        let _ = attr.parse_nested_meta(|meta| {
            if meta.path.is_ident("C") {
                c = true;
            }
            Ok(())
        });
        c
    })
}

/// The `key` in `#[plain(key = "...")]`. It is required: a plain-data type has a name on the wire.
fn key_attr(attrs: &[Attribute]) -> Result<LitStr> {
    for attr in attrs {
        if !attr.path().is_ident("plain") {
            continue;
        }
        let mut key = None;
        attr.parse_nested_meta(|meta| {
            if meta.path.is_ident("key") {
                key = Some(meta.value()?.parse::<LitStr>()?);
                Ok(())
            } else {
                Err(meta.error("expected key = \"...\""))
            }
        })?;
        if let Some(key) = key {
            return Ok(key);
        }
    }
    Err(Error::new(
        Span::call_site(),
        "PlainData needs #[plain(key = \"...\")]",
    ))
}
