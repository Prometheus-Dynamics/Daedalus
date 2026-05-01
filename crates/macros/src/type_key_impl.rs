use proc_macro::TokenStream;
use proc_macro_crate::{FoundCrate, crate_name};
use proc_macro2::Span;
use quote::quote;
use syn::{Item, Lit, LitStr, Meta, MetaNameValue, Visibility, parse_macro_input};

use crate::helpers::{AttributeArgs, NestedMeta, compile_error, lit_from_expr};

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

fn daedalus_root() -> Option<String> {
    crate_name("daedalus-rs")
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
        })
}

fn parse_type_key(args: AttributeArgs) -> Result<LitStr, proc_macro2::TokenStream> {
    let mut key = None;
    for arg in args {
        match arg {
            NestedMeta::Lit(Lit::Str(value)) => key = Some(value),
            NestedMeta::Meta(Meta::NameValue(MetaNameValue { path, value, .. }))
                if path.is_ident("key") || path.is_ident("type_key") =>
            {
                let Some(Lit::Str(value)) = lit_from_expr(&value) else {
                    return Err(compile_error("type_key must be a string literal".into()));
                };
                key = Some(value);
            }
            _ => {
                return Err(compile_error(
                    "type_key must use `#[type_key(\"...\")]` or `#[type_key(key = \"...\")]`"
                        .into(),
                ));
            }
        }
    }
    key.ok_or_else(|| compile_error("missing type_key value".into()))
}

fn snake_case(raw: &str) -> String {
    let mut out = String::new();
    for (idx, ch) in raw.chars().enumerate() {
        if ch.is_ascii_uppercase() {
            if idx != 0 {
                out.push('_');
            }
            out.push(ch.to_ascii_lowercase());
        } else {
            out.push(ch);
        }
    }
    out
}

pub fn type_key(args: TokenStream, item: TokenStream) -> TokenStream {
    let args = parse_macro_input!(args with AttributeArgs::parse_terminated);
    let input = parse_macro_input!(item as Item);
    let key = match parse_type_key(args) {
        Ok(key) => key,
        Err(err) => return err.into(),
    };

    let (ident, vis): (syn::Ident, Visibility) = match &input {
        Item::Struct(item) => {
            if !item.generics.params.is_empty() {
                return compile_error("type_key structs cannot be generic yet".into()).into();
            }
            (item.ident.clone(), item.vis.clone())
        }
        Item::Enum(item) => {
            if !item.generics.params.is_empty() {
                return compile_error("type_key enums cannot be generic yet".into()).into();
            }
            (item.ident.clone(), item.vis.clone())
        }
        _ => {
            return compile_error("type_key currently supports structs and enums".into()).into();
        }
    };

    let root = daedalus_root();
    let data_crate = crate_path("daedalus-data", "daedalus_data", Some("data"), &root);
    let runtime_crate = crate_path(
        "daedalus-runtime",
        "daedalus_runtime",
        Some("runtime"),
        &root,
    );
    let register_ident = syn::Ident::new(
        &format!("register_{}_type", snake_case(&ident.to_string())),
        ident.span(),
    );

    let expanded = quote! {
        #input

        impl #data_crate::daedalus_type::DaedalusTypeExpr for #ident {
            const TYPE_KEY: &'static str = #key;

            fn type_expr() -> #data_crate::model::TypeExpr {
                #data_crate::model::TypeExpr::Opaque(::std::string::String::from(#key))
            }
        }

        #vis fn #register_ident(
            into: &mut #runtime_crate::plugins::PluginRegistry,
        ) -> #runtime_crate::plugins::PluginResult<()> {
            into.register_daedalus_type::<#ident>(#data_crate::named_types::HostExportPolicy::None)
        }
    };

    expanded.into()
}
