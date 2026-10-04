//! `register_port_types`: records the Rust type behind every port key and rejects unkeyed
//! foreign types (see `PluginRegistry::register_port_type`).

use std::collections::HashSet;

use proc_macro2::TokenStream;
use quote::quote;
use syn::LitStr;

use super::descriptor::is_fanin_ty;
use super::parse::{OutputPortMeta, PortMeta};
use super::type_analysis::{contract_type_for, node_type_leaves, payload_value_type};
use crate::helpers::generic_arg;
use crate::type_expr::{leaf_declared_key, leaf_type_key};

pub(super) struct PortTypeInputs<'a> {
    /// Low-level and generic nodes declare no Rust port types.
    pub(super) skip: bool,
    pub(super) effective_inputs_for_args: &'a [PortMeta],
    pub(super) arg_types: &'a [syn::Type],
    pub(super) output_contract_tys: &'a [&'a syn::Type],
    pub(super) outputs: &'a [OutputPortMeta],
    pub(super) generic_type_params: &'a HashSet<String>,
    pub(super) data_crate: &'a TokenStream,
    pub(super) runtime_crate: &'a TokenStream,
}

pub(super) fn register_port_types_fn(inputs: PortTypeInputs<'_>) -> TokenStream {
    let PortTypeInputs {
        skip,
        effective_inputs_for_args,
        arg_types,
        output_contract_tys,
        outputs,
        generic_type_params,
        data_crate,
        runtime_crate,
    } = inputs;
    let mut ports: Vec<(&LitStr, Option<&LitStr>, bool, &syn::Type)> = Vec::new();
    if !skip {
        for (port, raw_ty) in effective_inputs_for_args.iter().zip(arg_types) {
            let ty = if is_fanin_ty(raw_ty) {
                generic_arg(crate::helpers::strip_ref(raw_ty), "FanIn", 0)
            } else {
                contract_type_for(raw_ty)
            };
            if let Some(ty) = ty {
                let typed = port.ty_override.is_some();
                ports.push((&port.name, port.type_key.as_ref(), typed, ty));
            }
        }
        for (port, ty) in outputs.iter().zip(output_contract_tys) {
            let typed = port.ty_override.is_some();
            ports.push((&port.name, port.type_key.as_ref(), typed, ty));
        }
    }
    let stmts = ports.into_iter().flat_map(|(name, type_key, typed, ty)| {
        let register = |ty: &syn::Type, key: TokenStream, declared: TokenStream| {
            quote! {
                into.register_port_type::<#ty>(
                    #runtime_crate::plugins::PortTypeUse {
                        owner: node,
                        port: #name,
                        defined_in: ::core::module_path!(),
                    },
                    #key,
                    #declared,
                )?;
            }
        };
        match type_key {
            Some(key) => vec![register(
                payload_value_type(ty),
                quote! { #runtime_crate::transport_types::TypeKey::new(#key) },
                quote! { true },
            )],
            // A `ty = <TypeExpr>` override names no Rust type for its key.
            None if typed => Vec::new(),
            None => node_type_leaves(ty, generic_type_params, data_crate)
                .iter()
                .map(|leaf| {
                    let declared = leaf_declared_key(leaf);
                    register(leaf, leaf_type_key(leaf), quote! { #declared.is_some() })
                })
                .collect(),
        }
    });
    quote! {
        #[doc(hidden)]
        pub fn register_port_types(
            into: &mut #runtime_crate::plugins::PluginRegistry,
            node: &str,
        ) -> #runtime_crate::plugins::PluginResult<()> {
            #(#stmts)*
            let _ = (into, node);
            Ok(())
        }
    }
}
