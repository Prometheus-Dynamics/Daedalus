//! The attribute a node's author writes. It emits constant records and names no backend: the
//! `backend` path is copied into the body call and resolves in the caller's scope.

#![forbid(unsafe_code)]

mod args;
mod emit;
mod ports;
mod transfer;

use proc_macro::TokenStream;

/// Declares a node on a function, or on a const holding its source. See `docs/design.md`.
#[proc_macro_attribute]
pub fn node(args: TokenStream, item: TokenStream) -> TokenStream {
    let args = syn::parse_macro_input!(args as args::NodeArgs);
    let item = syn::parse_macro_input!(item as syn::Item);
    emit::expand(args, item)
        .unwrap_or_else(syn::Error::into_compile_error)
        .into()
}

/// Declares a memory handler on a function. See `docs/design.md`.
#[proc_macro_attribute]
pub fn transfer(args: TokenStream, item: TokenStream) -> TokenStream {
    let args = syn::parse_macro_input!(args as transfer::TransferArgs);
    let item = syn::parse_macro_input!(item as syn::ItemFn);
    transfer::expand(args, item)
        .unwrap_or_else(syn::Error::into_compile_error)
        .into()
}
