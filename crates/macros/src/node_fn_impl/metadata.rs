use proc_macro2::{Span, TokenStream};
use quote::quote;
use syn::{Lit, LitStr};

use super::parse::{OutputPortMeta, PortMeta};

pub(super) struct MetadataInputs<'a> {
    pub(super) summary_attr: Option<&'a LitStr>,
    pub(super) description_attr: Option<&'a LitStr>,
    pub(super) inputs: &'a [PortMeta],
    pub(super) input_access: &'a [(LitStr, LitStr)],
    pub(super) outputs: &'a [OutputPortMeta],
    pub(super) fallback_attr: Option<&'a LitStr>,
    pub(super) config_types: &'a [syn::Type],
    pub(super) data_crate: &'a TokenStream,
    pub(super) runtime_crate: &'a TokenStream,
}

pub(super) fn metadata_tokens(inputs: MetadataInputs<'_>) -> TokenStream {
    let MetadataInputs {
        summary_attr,
        description_attr,
        inputs,
        input_access,
        outputs,
        fallback_attr,
        config_types,
        data_crate,
        runtime_crate,
    } = inputs;
    let mut inserts: Vec<TokenStream> = Vec::new();
    if let Some(summary) = summary_attr {
        inserts.push(quote! {
            __meta.insert(
                ::std::string::String::from("summary"),
                #data_crate::model::Value::String(::std::borrow::Cow::from(#summary)),
            );
        });
    }
    if let Some(description) = description_attr {
        inserts.push(quote! {
            __meta.insert(
                ::std::string::String::from("description"),
                #data_crate::model::Value::String(::std::borrow::Cow::from(#description)),
            );
        });
    }
    for port in inputs {
        if let Some(desc) = &port.description {
            let key = LitStr::new(
                &format!("inputs.{}.description", port.name.value()),
                Span::call_site(),
            );
            inserts.push(quote! {
                __meta.insert(
                    ::std::string::String::from(#key),
                    #data_crate::model::Value::String(::std::borrow::Cow::from(#desc)),
                );
            });
        }
        for (meta_key, meta_value) in &port.meta {
            let key = LitStr::new(
                &format!("inputs.{}.{}", port.name.value(), meta_key.value()),
                Span::call_site(),
            );
            let value = lit_to_value(meta_value, data_crate);
            inserts.push(quote! {
                __meta.insert(
                    ::std::string::String::from(#key),
                    #value,
                );
            });
        }
    }
    for (name, access) in input_access {
        let key = LitStr::new(
            &format!("inputs.{}.access", name.value()),
            Span::call_site(),
        );
        inserts.push(quote! {
            __meta.insert(
                ::std::string::String::from(#key),
                #data_crate::model::Value::String(::std::borrow::Cow::from(#access)),
            );
        });
    }
    for port in outputs {
        if let Some(desc) = &port.description {
            let key = LitStr::new(
                &format!("outputs.{}.description", port.name.value()),
                Span::call_site(),
            );
            inserts.push(quote! {
                __meta.insert(
                    ::std::string::String::from(#key),
                    #data_crate::model::Value::String(::std::borrow::Cow::from(#desc)),
                );
            });
        }
        for (meta_key, meta_value) in &port.meta {
            let key = LitStr::new(
                &format!("outputs.{}.{}", port.name.value(), meta_key.value()),
                Span::call_site(),
            );
            let value = lit_to_value(meta_value, data_crate);
            inserts.push(quote! {
                __meta.insert(
                    ::std::string::String::from(#key),
                    #value,
                );
            });
        }
    }
    if let Some(fallback) = fallback_attr {
        inserts.push(quote! {
            __meta.insert(
                ::std::string::String::from("fallback"),
                #data_crate::model::Value::String(::std::borrow::Cow::from(#fallback)),
            );
        });
    }

    quote! {{
        let mut __meta: ::std::collections::BTreeMap<
            ::std::string::String,
            #data_crate::model::Value,
        > = ::std::collections::BTreeMap::new();
        #(#inserts)*
        #(
            __meta.extend(<#config_types as #runtime_crate::config::NodeConfig>::metadata());
        )*
        __meta
    }}
}

fn lit_to_value(lit: &Lit, data_crate: &TokenStream) -> TokenStream {
    match lit {
        Lit::Str(s) => {
            quote! { #data_crate::model::Value::String(::std::borrow::Cow::from(#s)) }
        }
        Lit::Int(i) => {
            let v: i64 = i.base10_parse().unwrap_or(0);
            quote! { #data_crate::model::Value::Int(#v) }
        }
        Lit::Float(f) => {
            let v: f64 = f.base10_parse().unwrap_or(0.0);
            quote! { #data_crate::model::Value::Float(#v) }
        }
        Lit::Bool(b) => {
            let v = b.value;
            quote! { #data_crate::model::Value::Bool(#v) }
        }
        _ => quote! { #data_crate::model::Value::Unit },
    }
}
