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
    pub(super) fire_attr: Option<&'a LitStr>,
    pub(super) shareable: bool,
    /// Per output: an `Option` return, recorded as `outputs.<port>.conditional`.
    pub(super) conditional_outputs: &'a [bool],
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
        fire_attr,
        shareable,
        conditional_outputs,
        config_types,
        data_crate,
        runtime_crate,
    } = inputs;
    let mut entries: Vec<Entry> = Vec::new();
    if let Some(summary) = summary_attr {
        entries.push(Entry::text("summary".into(), summary));
    }
    if let Some(description) = description_attr {
        entries.push(Entry::text("description".into(), description));
    }
    for port in inputs {
        if let Some(desc) = &port.description {
            entries.push(Entry::text(
                format!("inputs.{}.description", port.name.value()),
                desc,
            ));
        }
        for (meta_key, meta_value) in &port.meta {
            let key = format!("inputs.{}.{}", port.name.value(), meta_key.value());
            entries.push(meta_entry(key, meta_value, data_crate));
        }
    }
    for (name, access) in input_access {
        entries.push(Entry::text(
            format!("inputs.{}.access", name.value()),
            access,
        ));
    }
    for (port, _) in outputs
        .iter()
        .zip(conditional_outputs)
        .filter(|(_, conditional)| **conditional)
    {
        let key = format!("outputs.{}.conditional", port.name.value());
        entries.push(Entry::Raw(quote! {
            __meta.insert(
                ::std::string::String::from(#key),
                #data_crate::model::Value::Bool(true),
            );
        }));
    }
    if let Some(fire) = fire_attr {
        entries.push(Entry::Raw(quote! {
            __meta.insert(
                ::std::string::String::from(#runtime_crate::NODE_FIRE_META_KEY),
                #data_crate::model::Value::String(::std::borrow::Cow::from(#fire)),
            );
        }));
    }
    if shareable {
        entries.push(Entry::Raw(quote! {
            __meta.insert(
                ::std::string::String::from(#runtime_crate::NODE_SHAREABLE_META_KEY),
                #data_crate::model::Value::Bool(true),
            );
        }));
    }
    for port in outputs {
        if let Some(desc) = &port.description {
            entries.push(Entry::text(
                format!("outputs.{}.description", port.name.value()),
                desc,
            ));
        }
        for (meta_key, meta_value) in &port.meta {
            let key = format!("outputs.{}.{}", port.name.value(), meta_key.value());
            entries.push(meta_entry(key, meta_value, data_crate));
        }
    }
    if let Some(fallback) = fallback_attr {
        entries.push(Entry::text("fallback".into(), fallback));
    }

    // Consecutive string entries become one static table and one loop; the typed ones keep
    // their own statements. The order of insertion is the order of `entries`.
    let mut inserts: Vec<TokenStream> = Vec::new();
    let mut run: Vec<(String, LitStr)> = Vec::new();
    for entry in entries {
        match entry {
            Entry::Text(key, value) => run.push((key, value)),
            Entry::Raw(tokens) => {
                inserts.push(text_table(std::mem::take(&mut run), data_crate));
                inserts.push(tokens);
            }
        }
    }
    inserts.push(text_table(run, data_crate));

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

/// One metadata entry: a string, which goes into the node's static text table, or a statement.
enum Entry {
    Text(String, LitStr),
    Raw(TokenStream),
}

impl Entry {
    fn text(key: String, value: &LitStr) -> Self {
        Self::Text(key, value.clone())
    }
}

/// A port's metadata entry `key = value`: a string literal joins the text table, other literals
/// are inserted as their typed values.
fn meta_entry(key: String, value: &Lit, data_crate: &TokenStream) -> Entry {
    match value {
        Lit::Str(s) => Entry::text(key, s),
        _ => {
            let key = LitStr::new(&key, Span::call_site());
            let value = lit_to_value(value, data_crate);
            Entry::Raw(quote! {
                __meta.insert(::std::string::String::from(#key), #value);
            })
        }
    }
}

/// The string entries `run`, in order, as one table of `(key, value)` pairs and one loop that
/// inserts them (nothing for an empty run).
fn text_table(run: Vec<(String, LitStr)>, data_crate: &TokenStream) -> TokenStream {
    if run.is_empty() {
        return TokenStream::new();
    }
    let rows = run.iter().map(|(key, value)| {
        let key = LitStr::new(key, Span::call_site());
        quote! { (#key, #value) }
    });
    quote! {
        for (__key, __value) in [#(#rows),*] {
            __meta.insert(
                ::std::string::String::from(__key),
                #data_crate::model::Value::String(::std::borrow::Cow::from(__value)),
            );
        }
    }
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
