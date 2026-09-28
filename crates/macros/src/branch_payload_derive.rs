use crate::helpers::crate_path;
use proc_macro::TokenStream;
use quote::quote;
use syn::{DeriveInput, parse_macro_input};

pub fn branch_payload(item: TokenStream) -> TokenStream {
    let input = parse_macro_input!(item as DeriveInput);
    let ident = input.ident;
    let generics = input.generics;
    let (impl_generics, ty_generics, where_clause) = generics.split_for_impl();
    let transport_crate = crate_path("daedalus-transport", "transport");

    quote! {
        impl #impl_generics #transport_crate::BranchPayload for #ident #ty_generics #where_clause {
            const BRANCH_KIND: #transport_crate::BranchKind = #transport_crate::BranchKind::Clone;

            fn branch_payload(&self) -> Self {
                self.clone()
            }
        }
    }
    .into()
}
