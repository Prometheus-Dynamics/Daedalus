use proc_macro::TokenStream;
use quote::quote;
use syn::{Expr, Item, Meta, MetaNameValue, parse_macro_input};

use crate::helpers::{AttributeArgs, NestedMeta, compile_error, crate_path, str_expr};

const USAGE: &str =
    "type_key must use `#[type_key(\"...\")]`, `#[type_key(CONST)]` or `#[type_key(key = ...)]`";

fn parse_type_key(args: AttributeArgs) -> Result<Expr, proc_macro2::TokenStream> {
    let mut key = None;
    for arg in args {
        let value = match arg {
            NestedMeta::Lit(lit) => Expr::Lit(syn::ExprLit { attrs: vec![], lit }),
            NestedMeta::Meta(Meta::Path(path)) => Expr::Path(syn::ExprPath {
                attrs: vec![],
                qself: None,
                path,
            }),
            NestedMeta::Meta(Meta::NameValue(MetaNameValue { path, value, .. }))
                if path.is_ident("key") || path.is_ident("type_key") =>
            {
                value
            }
            _ => return Err(compile_error(USAGE.into())),
        };
        if key.replace(str_expr(&value, "type_key")?).is_some() {
            return Err(compile_error(USAGE.into()));
        }
    }
    key.ok_or_else(|| compile_error("missing type_key value".into()))
}

pub fn type_key(args: TokenStream, item: TokenStream) -> TokenStream {
    let args = parse_macro_input!(args with AttributeArgs::parse_terminated);
    let input = parse_macro_input!(item as Item);
    let key = match parse_type_key(args) {
        Ok(key) => key,
        Err(err) => return err.into(),
    };

    let (ident, generics, kind) = match &input {
        Item::Struct(item) => (&item.ident, &item.generics, "structs"),
        Item::Enum(item) => (&item.ident, &item.generics, "enums"),
        _ => {
            return compile_error("type_key currently supports structs and enums".into()).into();
        }
    };
    if !generics.params.is_empty() {
        return compile_error(format!("type_key {kind} cannot be generic yet")).into();
    }

    let data_crate = crate_path("daedalus-data", "data");
    quote! {
        #input

        impl #data_crate::daedalus_type::DaedalusTypeExpr for #ident {
            const TYPE_KEY: &'static str = #key;

            fn type_expr() -> #data_crate::model::TypeExpr {
                #data_crate::model::TypeExpr::Opaque(::std::string::String::from(
                    <Self as #data_crate::daedalus_type::DaedalusTypeExpr>::TYPE_KEY,
                ))
            }
        }
    }
    .into()
}
