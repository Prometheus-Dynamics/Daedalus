//! Per-port code generation for `#[derive(NodeConfig)]`.

use std::collections::HashSet;

use proc_macro2::{Span, TokenStream};
use quote::quote;
use syn::{Lit, LitStr};

use super::model::{NumberKind, PortSpec, number_kind};
use crate::type_expr::{GenericParams, TypeExprOptions};

/// `PortDecl` construction tokens for one config port.
pub(super) fn port_decl_tokens(
    spec: &PortSpec,
    generic_type_params: &HashSet<String>,
    runtime_crate: &TokenStream,
    registry_crate: &TokenStream,
    data_crate: &TokenStream,
) -> TokenStream {
    let name = &spec.name;
    let source = spec
        .source
        .as_ref()
        .map(|s| quote! { ::core::option::Option::Some(::std::string::String::from(#s)) })
        .unwrap_or_else(|| quote! { ::core::option::Option::<::std::string::String>::None });
    let ty_expr = if let Some(ty) = &spec.ty_override {
        quote! { (#ty) }
    } else {
        TypeExprOptions {
            data_crate,
            generics: GenericParams::Fallback(generic_type_params),
            transparent: &[("Result", 0)],
            str_as_string: true,
            arrays_as_lists: false,
        }
        .type_expr(&spec.field_ty)
    };
    let default_value = spec
        .default_value
        .as_ref()
        .map(|lit| match lit {
            Lit::Str(s) => {
                quote! { Some(#data_crate::model::Value::String(::std::borrow::Cow::from(#s))) }
            }
            Lit::Int(i) => {
                let v: i64 = i.base10_parse().unwrap_or(0);
                quote! { Some(#data_crate::model::Value::Int(#v)) }
            }
            Lit::Float(f) => {
                let v: f64 = f.base10_parse().unwrap_or(0.0);
                quote! { Some(#data_crate::model::Value::Float(#v)) }
            }
            Lit::Bool(b) => {
                let v = b.value;
                quote! { Some(#data_crate::model::Value::Bool(#v)) }
            }
            _ => quote! { ::core::option::Option::<#data_crate::model::Value>::None },
        })
        .unwrap_or_else(|| quote! { ::core::option::Option::<#data_crate::model::Value>::None });
    quote! {
        {
            let __ty = #ty_expr;
            let __key = #runtime_crate::transport::typeexpr_transport_key(&__ty);
            let mut __port = #registry_crate::capability::PortDecl::new(#name, __key)
                .schema(__ty);
            if let Some(__source) = #source {
                __port = __port.source(__source.as_str());
            }
            if let Some(__default) = #default_value {
                __port = __port.const_value(__default);
            }
            __port
        }
    }
}

/// `NodeConfig::metadata` insertions (description, policy, bounds, custom meta) for one port.
pub(super) fn metadata_entries(spec: &PortSpec, data_crate: &TokenStream) -> Vec<TokenStream> {
    let mut entries = Vec::new();
    if let Some(desc) = &spec.description {
        let key = LitStr::new(
            &format!("inputs.{}.description", spec.name.value()),
            Span::call_site(),
        );
        entries.push(quote! {
            __meta.insert(
                ::std::string::String::from(#key),
                #data_crate::model::Value::String(::std::borrow::Cow::from(#desc)),
            );
        });
    }
    if let Some(policy) = &spec.policy {
        let key = LitStr::new(
            &format!("inputs.{}.policy", spec.name.value()),
            Span::call_site(),
        );
        entries.push(quote! {
            __meta.insert(
                ::std::string::String::from(#key),
                #data_crate::model::Value::String(::std::borrow::Cow::from(#policy)),
            );
        });
    }
    if spec.odd {
        let key = LitStr::new(
            &format!("inputs.{}.odd", spec.name.value()),
            Span::call_site(),
        );
        entries.push(quote! {
            __meta.insert(
                ::std::string::String::from(#key),
                #data_crate::model::Value::Bool(true),
            );
        });
    }
    if let Some(min) = &spec.min_value {
        let key = LitStr::new(
            &format!("inputs.{}.min", spec.name.value()),
            Span::call_site(),
        );
        let val = match min {
            Lit::Int(i) => {
                let v: i64 = i.base10_parse().unwrap_or(0);
                quote! { #data_crate::model::Value::Int(#v) }
            }
            Lit::Float(f) => {
                let v: f64 = f.base10_parse().unwrap_or(0.0);
                quote! { #data_crate::model::Value::Float(#v) }
            }
            _ => quote! { #data_crate::model::Value::Int(0) },
        };
        entries.push(quote! {
            __meta.insert(
                ::std::string::String::from(#key),
                #val,
            );
        });
    }
    if let Some(max) = &spec.max_value {
        let key = LitStr::new(
            &format!("inputs.{}.max", spec.name.value()),
            Span::call_site(),
        );
        let val = match max {
            Lit::Int(i) => {
                let v: i64 = i.base10_parse().unwrap_or(0);
                quote! { #data_crate::model::Value::Int(#v) }
            }
            Lit::Float(f) => {
                let v: f64 = f.base10_parse().unwrap_or(0.0);
                quote! { #data_crate::model::Value::Float(#v) }
            }
            _ => quote! { #data_crate::model::Value::Int(0) },
        };
        entries.push(quote! {
            __meta.insert(
                ::std::string::String::from(#key),
                #val,
            );
        });
    }
    if !spec.meta.is_empty() {
        for (meta_key, meta_value) in &spec.meta {
            let key = LitStr::new(
                &format!("inputs.{}.{}", spec.name.value(), meta_key.value()),
                Span::call_site(),
            );
            let value = match meta_value {
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
            };
            entries.push(quote! {
                __meta.insert(
                    ::std::string::String::from(#key),
                    #value,
                );
            });
        }
    }
    entries
}

/// `NodeConfig::sanitize` statements for one field (clamp/error policy for numeric bounds).
pub(super) fn sanitize_field_tokens(
    idx: usize,
    spec: &PortSpec,
    runtime_crate: &TokenStream,
    data_crate: &TokenStream,
) -> TokenStream {
    let ident = &spec.field_ident;
    let needs_sanitize = spec.min_value.is_some() || spec.max_value.is_some() || spec.odd;
    if !needs_sanitize {
        return quote! {
            let #ident = self.#ident;
        };
    }
    let name = &spec.name;
    let policy_str = spec
        .policy
        .clone()
        .unwrap_or_else(|| LitStr::new("clamp", Span::call_site()));
    let policy_variant = match policy_str.value().as_str() {
        "error" => quote! { #runtime_crate::config::ConfigPolicy::Error },
        _ => quote! { #runtime_crate::config::ConfigPolicy::Clamp },
    };
    let number_kind = number_kind(&spec.field_ty);
    let to_value = match number_kind {
        Some(NumberKind::Float) => {
            quote! { #data_crate::model::Value::Float(value as f64) }
        }
        _ => quote! { #data_crate::model::Value::Int(value as i64) },
    };
    let min_check = spec.min_value.as_ref().map(|min| {
        quote! {
            if value < #min {
                if matches!(#policy_variant, #runtime_crate::config::ConfigPolicy::Error) {
                    return Err(#runtime_crate::config::ConfigError::for_port(
                        #name,
                        format!("must be >= {}", #min),
                    ));
                }
                value = #min;
                changed = true;
            }
        }
    });
    let max_check = spec.max_value.as_ref().map(|max| {
        quote! {
            if value > #max {
                if matches!(#policy_variant, #runtime_crate::config::ConfigPolicy::Error) {
                    return Err(#runtime_crate::config::ConfigError::for_port(
                        #name,
                        format!("must be <= {}", #max),
                    ));
                }
                value = #max;
                changed = true;
            }
        }
    });
    let odd_check = if spec.odd {
        let min_guard = spec.min_value.as_ref().map(|min| {
            quote! {
                if candidate < #min {
                    candidate = value + 1;
                }
            }
        });
        let max_guard = spec.max_value.as_ref().map(|max| {
            quote! {
                if candidate > #max {
                    candidate = value - 1;
                }
            }
        });
        Some(quote! {
            if value % 2 == 0 {
                if matches!(#policy_variant, #runtime_crate::config::ConfigPolicy::Error) {
                    return Err(#runtime_crate::config::ConfigError::for_port(
                        #name,
                        "must be odd",
                    ));
                }
                let mut candidate = value + 1;
                #max_guard
                #min_guard
                if candidate == value {
                    return Err(#runtime_crate::config::ConfigError::for_port(
                        #name,
                        "unable to coerce even value to odd",
                    ));
                }
                value = candidate;
                changed = true;
            }
        })
    } else {
        None
    };
    if number_kind.is_none() {
        return quote! { compile_error!("numeric constraints require numeric types"); };
    }
    let change_ident = syn::Ident::new(&format!("__cfg_change_{idx}"), Span::call_site());
    quote! {
        let mut #ident = self.#ident;
        let original = #ident;
        let mut value = #ident;
        let mut changed = false;
        #min_check
        #max_check
        #odd_check
        if changed {
            let #change_ident = #runtime_crate::config::ConfigChange {
                port: #name,
                previous: { let value = original; #to_value },
                next: { let value = value; #to_value },
                policy: #policy_variant,
            };
            changes.push(#change_ident);
        }
        #ident = value;
    }
}
