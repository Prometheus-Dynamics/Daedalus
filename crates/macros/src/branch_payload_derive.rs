use proc_macro::TokenStream;
use proc_macro_crate::{FoundCrate, crate_name};
use proc_macro2::Span;
use quote::quote;
use syn::{DeriveInput, parse_macro_input};

fn crate_path(
    pkg: &str,
    fallback: &str,
    via_root: Option<&str>,
    daedalus_root: &Option<String>,
) -> proc_macro2::TokenStream {
    if let Some(root) = daedalus_root
        && let Some(via) = via_root
    {
        let root_ident = syn::Ident::new(root, Span::call_site());
        let via_ident = syn::Ident::new(via, Span::call_site());
        return quote! { ::#root_ident::#via_ident };
    }
    let name = crate_name(pkg)
        .map(|found| match found {
            FoundCrate::Itself => pkg.replace('-', "_"),
            FoundCrate::Name(name) => name,
        })
        .unwrap_or_else(|_| fallback.to_string());
    let ident = syn::Ident::new(&name, Span::call_site());
    quote! { ::#ident }
}

pub fn branch_payload(item: TokenStream) -> TokenStream {
    let input = parse_macro_input!(item as DeriveInput);
    let ident = input.ident;
    let generics = input.generics;
    let (impl_generics, ty_generics, where_clause) = generics.split_for_impl();
    let daedalus_root: Option<String> = crate_name("daedalus-rs")
        .or_else(|_| crate_name("daedalus"))
        .ok()
        .map(|found| match found {
            FoundCrate::Itself => "daedalus".to_string(),
            FoundCrate::Name(name) => {
                if name == "daedalus_rs" {
                    "daedalus".to_string()
                } else {
                    name
                }
            }
        });
    let transport_crate = crate_path(
        "daedalus-transport",
        "daedalus_transport",
        Some("transport"),
        &daedalus_root,
    );

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
