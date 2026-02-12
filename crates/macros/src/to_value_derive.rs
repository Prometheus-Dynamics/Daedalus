use proc_macro::TokenStream;
use quote::quote;
use syn::{Data, DeriveInput, Fields, parse_macro_input};

use crate::helpers::{compile_error, parse_serde_rename_all, serde_name_for_ident};

pub fn daedalus_to_value(item: TokenStream) -> TokenStream {
    let input = parse_macro_input!(item as DeriveInput);
    let name = input.ident.clone();
    if !input.generics.params.is_empty() {
        return TokenStream::from(compile_error(
            "DaedalusToValue does not support generics yet".into(),
        ));
    }

    let data_crate: proc_macro2::TokenStream = quote! { ::daedalus_data };
    let rename_all = parse_serde_rename_all(&input.attrs);

    let body: proc_macro2::TokenStream = match &input.data {
        Data::Struct(s) => match &s.fields {
            Fields::Named(fields) => {
                let mut items = Vec::new();
                for field in &fields.named {
                    let Some(ident) = &field.ident else { continue };
                    let fname = serde_name_for_ident(ident, &field.attrs, rename_all);
                    items.push(quote! {
                        #data_crate::model::StructFieldValue {
                            name: ::std::string::String::from(#fname),
                            value: #data_crate::to_value::ToValue::to_value(&self.#ident),
                        }
                    });
                }
                quote! { #data_crate::model::Value::Struct(vec![#(#items),*]) }
            }
            Fields::Unit => quote! { #data_crate::model::Value::Unit },
            Fields::Unnamed(fields) => {
                if fields.unnamed.len() == 1 {
                    quote! { #data_crate::to_value::ToValue::to_value(&self.0) }
                } else {
                    return TokenStream::from(compile_error(
                        "DaedalusToValue only supports tuple structs with a single field".into(),
                    ));
                }
            }
        },
        Data::Enum(e) => {
            let mut arms = Vec::new();
            for v in &e.variants {
                let vident = &v.ident;
                let vname = serde_name_for_ident(vident, &v.attrs, rename_all);
                let arm = match &v.fields {
                    Fields::Unit => quote! {
                        Self::#vident => #data_crate::model::Value::Enum(#data_crate::model::EnumValue {
                            name: ::std::string::String::from(#vname),
                            value: None,
                        })
                    },
                    Fields::Unnamed(f) if f.unnamed.len() == 1 => quote! {
                        Self::#vident(inner) => #data_crate::model::Value::Enum(#data_crate::model::EnumValue {
                            name: ::std::string::String::from(#vname),
                            value: Some(Box::new(#data_crate::to_value::ToValue::to_value(inner))),
                        })
                    },
                    Fields::Named(f) if f.named.len() == 1 => {
                        let fname = f.named.first().unwrap().ident.as_ref().unwrap();
                        quote! {
                            Self::#vident{ #fname } => #data_crate::model::Value::Enum(#data_crate::model::EnumValue {
                                name: ::std::string::String::from(#vname),
                                value: Some(Box::new(#data_crate::to_value::ToValue::to_value(#fname))),
                            })
                        }
                    }
                    _ => {
                        return TokenStream::from(compile_error(
                            "DaedalusToValue enum variants must be unit or single-payload".into(),
                        ));
                    }
                };
                arms.push(arm);
            }
            quote! { match self { #(#arms),* } }
        }
        Data::Union(_) => {
            return TokenStream::from(compile_error(
                "DaedalusToValue does not support unions".into(),
            ));
        }
    };

    let expanded = quote! {
        impl #data_crate::to_value::ToValue for #name {
            fn to_value(&self) -> #data_crate::model::Value {
                #body
            }
        }
    };

    TokenStream::from(expanded)
}
