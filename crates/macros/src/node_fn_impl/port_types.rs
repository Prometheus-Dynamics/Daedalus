//! `register_port_types`: records the Rust type behind every port key, rejects unkeyed foreign
//! types (see `PluginRegistry::register_port_type`), and registers const coercers for input and
//! config types (see `daedalus_runtime::const_coerce`).

use std::collections::HashSet;

use proc_macro2::TokenStream;
use quote::{ToTokens, quote};
use syn::LitStr;

use super::descriptor::is_fanin_ty;
use super::parse::{OutputPortMeta, PortMeta};
use super::type_analysis::{contract_type_for, node_type_leaves, payload_value_type};
use crate::helpers::{
    any_token, const_coercer_registration, generic_arg, last_ident_is, strip_ref,
};
use crate::type_expr::{leaf_declared_key, leaf_type_key};

pub(super) struct PortTypeInputs<'a> {
    /// Low-level and generic nodes declare no Rust port types.
    pub(super) skip: bool,
    pub(super) effective_inputs_for_args: &'a [PortMeta],
    pub(super) arg_types: &'a [syn::Type],
    pub(super) output_contract_tys: &'a [&'a syn::Type],
    pub(super) outputs: &'a [OutputPortMeta],
    pub(super) config_types: &'a [syn::Type],
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
        config_types,
        generic_type_params,
        data_crate,
        runtime_crate,
    } = inputs;
    let coercers = quote! { &into.const_coercers };
    let mut coercions: Vec<TokenStream> = config_types
        .iter()
        .map(|ty| quote! { <#ty as #runtime_crate::config::NodeConfig>::register_const_coercers(#coercers); })
        .collect();
    let mut ports: Vec<(&LitStr, Option<&LitStr>, bool, &syn::Type)> = Vec::new();
    let mut foreign_views = Vec::new();
    if !skip {
        for (port, raw_ty) in effective_inputs_for_args.iter().zip(arg_types) {
            if crate::foreign_type::is_foreign_view(raw_ty) {
                foreign_views.push(quote! { into.register_foreign_port::<#raw_ty>()?; });
                continue;
            }
            if let Some(ty) = coerced_input_type(raw_ty) {
                coercions.push(const_coercer_registration(ty, &coercers, runtime_crate));
            }
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
            #(#foreign_views)*
            #(#coercions)*
            let _ = (into, node);
            Ok(())
        }
    }
}

/// The type a handler argument coerces a `Value` input to (as `NodeIo::take_owned`,
/// `coerce_input` and `get_typed` see it), if any: `T` for `T`, `&T`, `&mut T` and an owned
/// `Option<T>`, when [`coercible`].
fn coerced_input_type(raw_ty: &syn::Type) -> Option<&syn::Type> {
    let ty = strip_ref(raw_ty);
    let ty = match raw_ty {
        syn::Type::Reference(_) => ty,
        _ => generic_arg(ty, "Option", 0).unwrap_or(ty),
    };
    coercible(ty).then_some(ty)
}

/// Whether a `Value` input can be coerced to `ty`: not for fan-in, `Arc` and `Compute`, which
/// only take typed payloads, nor for unsized or borrowing types (`str`, slices, lifetimes).
pub(super) fn coercible(ty: &syn::Type) -> bool {
    let sized_path = matches!(ty, syn::Type::Path(p) if p.qself.is_none())
        && !["FanIn", "Arc", "Compute", "str"]
            .iter()
            .any(|name| last_ident_is(ty, name));
    sized_path
        && !any_token(
            ty.to_token_stream(),
            &|token| matches!(token, proc_macro2::TokenTree::Punct(p) if p.as_char() == '\''),
        )
}
