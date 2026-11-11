//! `#[derive(NodeConfig)]` and `#[derive(Type)]`. A proc-macro crate exports only macros, and the
//! attribute macro has its own crate, so these live here.

#![forbid(unsafe_code)]

mod config;
mod plain;
mod ty;

use proc_macro::TokenStream;
use proc_macro2::TokenStream as Tokens;
use quote::quote_spanned;
use syn::spanned::Spanned;

/// Generates `FIELDS`, `RULES`, `CHECKERS` and `from_literals` on a config struct.
#[proc_macro_derive(NodeConfig, attributes(port, validate))]
pub fn derive_node_config(input: TokenStream) -> TokenStream {
    let input = syn::parse_macro_input!(input as syn::DeriveInput);
    config::derive(input)
        .unwrap_or_else(syn::Error::into_compile_error)
        .into()
}

/// Generates `Typed` for a struct or enum.
#[proc_macro_derive(Type, attributes(type_key))]
pub fn derive_type(input: TokenStream) -> TokenStream {
    let input = syn::parse_macro_input!(input as syn::DeriveInput);
    ty::derive(input)
        .unwrap_or_else(syn::Error::into_compile_error)
        .into()
}

/// A check that this type's key is usable from the current crate.
fn local_key_check(ty: &syn::Type) -> Tokens {
    quote_spanned! {ty.span()=>
        const _: () = {
            static KEY: ::daedalus_registry::__private::TypeKey =
                <#ty as ::daedalus_registry::__private::Typed>::KEY;
            ::daedalus_registry::__private::require_local_key(&KEY, ::core::module_path!())
        };
    }
}

/// Gives a `repr(C)` struct the key in `#[plain(key = "...")]` and a layout hash, as
/// `daedalus_abi::PlainData`. The crate using it must depend on `daedalus-abi`.
#[proc_macro_derive(PlainData, attributes(plain))]
pub fn derive_plain_data(input: TokenStream) -> TokenStream {
    let input = syn::parse_macro_input!(input as syn::DeriveInput);
    plain::derive(input)
        .unwrap_or_else(syn::Error::into_compile_error)
        .into()
}
