//! What `#[node(backend = daedalus_wgpu_author(..))]` expands to. See the README.

#![forbid(unsafe_code)]

mod bindings;
mod element;
mod infer;

use proc_macro::TokenStream;

/// The body macro `#[node]` calls for a shader held in a const. Not called by hand.
#[doc(hidden)]
#[proc_macro]
pub fn __daedalus_body(input: TokenStream) -> TokenStream {
    match bindings::expand(input.into()) {
        Ok(tokens) => tokens.into(),
        Err(error) => error.to_compile_error().into(),
    }
}
