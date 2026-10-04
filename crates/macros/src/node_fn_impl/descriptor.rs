use proc_macro2::{Span, TokenStream};
use quote::quote;
use syn::LitStr;

use super::parse::{OutputPortMeta, PortMeta};
use super::type_analysis::{contract_type_for, node_type_expr, peel_result_or_option};
use crate::helpers::{generic_arg, last_ident_is, last_segment, strip_ref};
use crate::type_expr::leaf_type_expr;

pub(super) fn is_fanin_ty(ty: &syn::Type) -> bool {
    last_ident_is(strip_ref(ty), "FanIn")
}

/// `X` of an optional input or conditional output `Option<X>` (not `&Option<X>`).
pub(super) fn optional_input_type(ty: &syn::Type) -> Option<&syn::Type> {
    generic_arg(ty, "Option", 0)
}

pub(super) struct InputDeclInputs<'a> {
    pub(super) is_low_level: bool,
    pub(super) inputs: &'a [PortMeta],
    pub(super) effective_inputs_for_args: &'a [PortMeta],
    pub(super) arg_types: &'a [syn::Type],
    pub(super) arg_mut_bindings: &'a [bool],
    pub(super) generic_type_params: &'a ::std::collections::HashSet<::std::string::String>,
    pub(super) data_crate: &'a TokenStream,
    pub(super) runtime_crate: &'a TokenStream,
    pub(super) registry_crate: &'a TokenStream,
}

pub(super) fn node_input_port_decl_tokens(inputs: InputDeclInputs<'_>) -> Vec<TokenStream> {
    let InputDeclInputs {
        is_low_level,
        inputs,
        effective_inputs_for_args,
        arg_types,
        arg_mut_bindings,
        generic_type_params,
        data_crate,
        runtime_crate,
        registry_crate,
    } = inputs;
    if is_low_level {
        return inputs
            .iter()
            .map(|port| {
                let name = &port.name;
                let source = option_string(&port.source);
                let default = if let Some(ts) = &port.default_value {
                    quote! { ::core::option::Option::Some(#ts) }
                } else {
                    quote! { ::core::option::Option::<#data_crate::model::Value>::None }
                };
                let ty_expr = if let Some(ty) = port.ty_override.as_ref() {
                    quote! { (#ty) }
                } else {
                    let lit = LitStr::new("rust:unknown", Span::call_site());
                    quote! { #data_crate::model::TypeExpr::Opaque(::std::string::String::from(#lit)) }
                };
                port_decl_token(PortDeclToken {
                    name,
                    source,
                    default,
                    ty_expr,
                    access: quote! { #runtime_crate::transport_types::AccessMode::Read },
                    residency: quote! {},
                    // The handler reads `NodeIo` itself and sees whatever arrived.
                    optional: true,
                    runtime_crate,
                    registry_crate,
                })
            })
            .collect();
    }

    effective_inputs_for_args
        .iter()
        .enumerate()
        .filter_map(|(idx, port)| {
            let raw_aty = arg_types.get(idx)?;
            // `Option<X>` is an optional input carrying `X`: same key and schema as `X`, so
            // producers of `X` connect directly. `Option<T>` clones, so it only reads.
            let optional = optional_input_type(raw_aty);
            let raw_aty = optional.unwrap_or(raw_aty);
            let is_binding_mut =
                optional.is_none() && arg_mut_bindings.get(idx).copied().unwrap_or(false);
            let is_ref = matches!(raw_aty, syn::Type::Reference(_));
            let is_ref_mut = matches!(raw_aty, syn::Type::Reference(r) if r.mutability.is_some());
            let aty = strip_ref(raw_aty);
            let is_arc = last_ident_is(aty, "Arc");
            let access = if is_ref_mut || is_binding_mut {
                quote! { #runtime_crate::transport_types::AccessMode::Modify }
            } else if is_ref || is_arc || optional.is_some() {
                quote! { #runtime_crate::transport_types::AccessMode::Read }
            } else {
                quote! { #runtime_crate::transport_types::AccessMode::Move }
            };
            let residency = residency_for_ty(raw_aty, runtime_crate)
                .map(|residency| quote! { __port = __port.residency(#residency); })
                .unwrap_or_default();
            if is_fanin_ty(aty) {
                return None;
            }

            let name = &port.name;
            let source = option_string(&port.source);
            let default = if let Some(ts) = &port.default_value {
                quote! { ::core::option::Option::Some(#ts) }
            } else {
                quote! { ::core::option::Option::<#data_crate::model::Value>::None }
            };
            let foreign = crate::foreign_type::is_foreign_view(raw_aty);
            let access = if foreign {
                quote! { #runtime_crate::transport_types::AccessMode::Read }
            } else {
                access
            };
            let ty_expr = if let Some(ty) = port.ty_override.as_ref() {
                quote! { (#ty) }
            } else if foreign {
                quote! { #runtime_crate::foreign::view_type_expr::<#raw_aty>() }
            } else {
                node_type_expr(aty, generic_type_params, data_crate)
            };

            Some(port_decl_token(PortDeclToken {
                name,
                source,
                default,
                ty_expr,
                access,
                residency,
                optional: optional.is_some(),
                runtime_crate,
                registry_crate,
            }))
        })
        .collect()
}

struct PortDeclToken<'a> {
    name: &'a LitStr,
    source: TokenStream,
    default: TokenStream,
    ty_expr: TokenStream,
    access: TokenStream,
    residency: TokenStream,
    optional: bool,
    runtime_crate: &'a TokenStream,
    registry_crate: &'a TokenStream,
}

fn port_decl_token(input: PortDeclToken<'_>) -> TokenStream {
    let PortDeclToken {
        name,
        source,
        default,
        ty_expr,
        access,
        residency,
        optional,
        runtime_crate,
        registry_crate,
    } = input;
    let optional = optional.then(|| quote! { __port = __port.optional(); });
    quote! {
        {
            let __ty = #ty_expr;
            let mut __port = #registry_crate::capability::PortDecl::new(
                #name,
                #runtime_crate::transport::typeexpr_transport_key(&__ty),
            )
            .schema(__ty)
            .access(#access);
            #residency
            #optional
            if let Some(__source) = #source {
                __port = __port.source(__source.as_str());
            }
            if let Some(__default) = #default {
                __port = __port.const_value(__default);
            }
            __port
        }
    }
}

pub(super) fn output_type_exprs(
    ret: &syn::ReturnType,
    outputs: &[OutputPortMeta],
    generic_type_params: &::std::collections::HashSet<::std::string::String>,
    data_crate: &TokenStream,
) -> Vec<TokenStream> {
    let explicit: Vec<Option<TokenStream>> = outputs
        .iter()
        .map(|p| p.ty_override.as_ref().map(|ts| quote! { (#ts) }))
        .collect();

    let mut out: Vec<TokenStream> = Vec::new();
    if let syn::ReturnType::Type(_, ty) = ret {
        let base_ty = peel_result_or_option(ty);

        if let syn::Type::Tuple(t) = base_ty {
            if t.elems.len() == outputs.len() {
                for (idx, elem) in t.elems.iter().enumerate() {
                    if let Some(ts) = explicit.get(idx).and_then(|v| v.clone()) {
                        out.push(ts);
                        continue;
                    }
                    let elem = optional_input_type(elem).unwrap_or(elem);
                    out.push(node_type_expr(elem, generic_type_params, data_crate));
                }
            }
        } else if outputs.len() == 1 {
            if let Some(ts) = explicit.first().and_then(|v| v.clone()) {
                out.push(ts);
            } else {
                out.push(node_type_expr(base_ty, generic_type_params, data_crate));
            }
        }
    }
    while out.len() < outputs.len() {
        let idx = out.len();
        if let Some(ts) = explicit.get(idx).and_then(|v| v.clone()) {
            out.push(ts);
        } else {
            let lit = LitStr::new("rust:unknown", Span::call_site());
            out.push(
                quote! { #data_crate::model::TypeExpr::Opaque(::std::string::String::from(#lit)) },
            );
        }
    }
    out
}

pub(super) struct BoundaryInputs<'a> {
    pub(super) is_low_level: bool,
    pub(super) has_fn_generics: bool,
    pub(super) effective_inputs_for_args: &'a [PortMeta],
    pub(super) arg_types: &'a [syn::Type],
    pub(super) output_contract_tys: &'a [&'a syn::Type],
    pub(super) outputs: &'a [OutputPortMeta],
    pub(super) data_crate: &'a TokenStream,
    pub(super) runtime_crate: &'a TokenStream,
    pub(super) fn_impl_generics: &'a TokenStream,
    pub(super) fn_where_clause: &'a TokenStream,
}

pub(super) fn boundary_contracts_fn(inputs: BoundaryInputs<'_>) -> TokenStream {
    let BoundaryInputs {
        is_low_level,
        has_fn_generics,
        effective_inputs_for_args,
        arg_types,
        output_contract_tys,
        outputs,
        data_crate,
        runtime_crate,
        fn_impl_generics,
        fn_where_clause,
    } = inputs;
    let boundary_input_contracts_for: Vec<TokenStream> = if is_low_level {
        Vec::new()
    } else {
        effective_inputs_for_args
            .iter()
            .enumerate()
            .filter_map(|(idx, port)| {
                let raw_ty = arg_types.get(idx)?;
                let contract_ty = contract_type_for(raw_ty)?;
                let ty_expr = if let Some(ty) = port.ty_override.as_ref() {
                    quote! { (#ty) }
                } else {
                    leaf_type_expr(contract_ty, data_crate)
                };
                Some(boundary_contract_push(
                    ty_expr,
                    quote! { #contract_ty },
                    runtime_crate,
                ))
            })
            .collect()
    };

    let boundary_output_contracts_for: Vec<TokenStream> = if is_low_level {
        Vec::new()
    } else {
        output_contract_tys
            .iter()
            .enumerate()
            .filter_map(|(idx, raw_ty)| {
                let contract_ty = contract_type_for(raw_ty)?;
                let ty_expr =
                    if let Some(ts) = outputs.get(idx).and_then(|port| port.ty_override.as_ref()) {
                        quote! { (#ts) }
                    } else {
                        leaf_type_expr(contract_ty, data_crate)
                    };
                Some(boundary_contract_push(
                    ty_expr,
                    quote! { #contract_ty },
                    runtime_crate,
                ))
            })
            .collect()
    };

    let boundary_input_contracts = if has_fn_generics {
        Vec::new()
    } else {
        boundary_input_contracts_for.clone()
    };
    let boundary_output_contracts = if has_fn_generics {
        Vec::new()
    } else {
        boundary_output_contracts_for.clone()
    };

    let types_ty = quote! { &#data_crate::typing::TypeRegistry };
    quote! {
        /// [`Self::boundary_contracts_in`] resolving through no registry.
        pub fn boundary_contracts() -> Result<Vec<#runtime_crate::transport_types::BoundaryTypeContract>, &'static str> {
            Self::boundary_contracts_in(#data_crate::typing::TypeRegistry::empty())
        }

        /// Boundary contracts of the port types, resolved through `__types`.
        pub fn boundary_contracts_in(
            __types: #types_ty,
        ) -> Result<Vec<#runtime_crate::transport_types::BoundaryTypeContract>, &'static str> {
            let mut __contracts: Vec<#runtime_crate::transport_types::BoundaryTypeContract> = Vec::new();
            #(#boundary_input_contracts)*
            #(#boundary_output_contracts)*
            __contracts.sort_by(|a, b| a.type_key.cmp(&b.type_key));
            __contracts.dedup_by(|a, b| a.type_key == b.type_key);
            Ok(__contracts)
        }

        pub fn boundary_contracts_for #fn_impl_generics (
            __types: #types_ty,
        ) -> Result<Vec<#runtime_crate::transport_types::BoundaryTypeContract>, &'static str> #fn_where_clause {
            let mut __contracts: Vec<#runtime_crate::transport_types::BoundaryTypeContract> = Vec::new();
            #(#boundary_input_contracts_for)*
            #(#boundary_output_contracts_for)*
            __contracts.sort_by(|a, b| a.type_key.cmp(&b.type_key));
            __contracts.dedup_by(|a, b| a.type_key == b.type_key);
            Ok(__contracts)
        }
    }
}

fn boundary_contract_push(
    ty_expr: TokenStream,
    contract_ty: TokenStream,
    runtime_crate: &TokenStream,
) -> TokenStream {
    quote! {
        {
            let __ty = #ty_expr;
            let __key = #runtime_crate::transport::typeexpr_transport_key(&__ty);
            __contracts.push(
                #runtime_crate::transport_types::BoundaryTypeContract::for_schema::<#contract_ty>(
                    __key,
                    format!("{:?}", __ty),
                    #runtime_crate::transport_types::BoundaryCapabilities::rust_value(),
                )
            );
        }
    }
}

pub(super) struct FanInInputs<'a> {
    pub(super) is_low_level: bool,
    pub(super) effective_inputs_for_args: &'a [PortMeta],
    pub(super) arg_types: &'a [syn::Type],
    pub(super) generic_type_params: &'a ::std::collections::HashSet<::std::string::String>,
    pub(super) data_crate: &'a TokenStream,
    pub(super) runtime_crate: &'a TokenStream,
    pub(super) registry_crate: &'a TokenStream,
}

pub(super) fn fanin_input_decl_tokens(inputs: FanInInputs<'_>) -> Vec<TokenStream> {
    let FanInInputs {
        is_low_level,
        effective_inputs_for_args,
        arg_types,
        generic_type_params,
        data_crate,
        runtime_crate,
        registry_crate,
    } = inputs;
    if is_low_level {
        return Vec::new();
    }
    effective_inputs_for_args
        .iter()
        .enumerate()
        .filter_map(|(idx, port)| {
            let aty = arg_types.get(idx)?;
            let inner_ty = generic_arg(aty, "FanIn", 0)?;

            let prefix = &port.name;
            let ty_expr = if let Some(ty) = port.ty_override.as_ref() {
                quote! { (#ty) }
            } else {
                node_type_expr(inner_ty, generic_type_params, data_crate)
            };
            Some(quote! {
                {
                    let __ty = #ty_expr;
                    #registry_crate::capability::FanInDecl::new(
                        #prefix,
                        0,
                        #runtime_crate::transport::typeexpr_transport_key(&__ty),
                    )
                    .schema(__ty)
                }
            })
        })
        .collect()
}

pub(super) struct NodeDeclInputs<'a> {
    pub(super) has_generics: bool,
    pub(super) fn_impl_generics: &'a TokenStream,
    pub(super) registry_crate: &'a TokenStream,
    pub(super) fn_where_clause: &'a TokenStream,
    pub(super) id: &'a syn::Expr,
    pub(super) node_input_port_decl_tokens: &'a [TokenStream],
    pub(super) config_inputs_extend: &'a [TokenStream],
    pub(super) fanin_input_decl_tokens: &'a [TokenStream],
    pub(super) output_type_exprs: &'a [TokenStream],
    pub(super) output_names: &'a [LitStr],
    pub(super) runtime_crate: &'a TokenStream,
    pub(super) output_sources: &'a [TokenStream],
    pub(super) metadata_tokens: &'a TokenStream,
    pub(super) data_crate: &'a TokenStream,
}

pub(super) fn node_decl_fn(inputs: NodeDeclInputs<'_>) -> TokenStream {
    let NodeDeclInputs {
        has_generics,
        fn_impl_generics,
        registry_crate,
        fn_where_clause,
        id,
        node_input_port_decl_tokens,
        config_inputs_extend,
        fanin_input_decl_tokens,
        output_type_exprs,
        output_names,
        runtime_crate,
        output_sources,
        metadata_tokens,
        data_crate,
    } = inputs;
    let body = node_decl_body(NodeDeclBody {
        node_input_port_decl_tokens,
        config_inputs_extend,
        fanin_input_decl_tokens,
        output_type_exprs,
        output_names,
        runtime_crate,
        registry_crate,
        output_sources,
        metadata_tokens,
    });
    let types_ty = quote! { &#data_crate::typing::TypeRegistry };
    if has_generics {
        quote! {
            /// The node declaration under `id`; port types without a key of their own resolve
            /// through `__types` (the installing registry's typing registry).
            pub fn node_decl_for #fn_impl_generics (
                id: impl Into<String>,
                __types: #types_ty,
            ) -> Result<#registry_crate::capability::NodeDecl, &'static str> #fn_where_clause {
                let id_str = id.into();
                let mut __node = #registry_crate::capability::NodeDecl::new(id_str);
                #body
            }
        }
    } else {
        quote! {
            /// [`Self::node_decl_in`] resolving through no registry.
            pub fn node_decl() -> Result<#registry_crate::capability::NodeDecl, &'static str> {
                Self::node_decl_in(#data_crate::typing::TypeRegistry::empty())
            }

            /// The node declaration; port types without a key of their own resolve through
            /// `__types` (the installing registry's typing registry).
            pub fn node_decl_in(
                __types: #types_ty,
            ) -> Result<#registry_crate::capability::NodeDecl, &'static str> {
                let mut __node = #registry_crate::capability::NodeDecl::new(#id);
                #body
            }
        }
    }
}

struct NodeDeclBody<'a> {
    node_input_port_decl_tokens: &'a [TokenStream],
    config_inputs_extend: &'a [TokenStream],
    fanin_input_decl_tokens: &'a [TokenStream],
    output_type_exprs: &'a [TokenStream],
    output_names: &'a [LitStr],
    runtime_crate: &'a TokenStream,
    registry_crate: &'a TokenStream,
    output_sources: &'a [TokenStream],
    metadata_tokens: &'a TokenStream,
}

fn node_decl_body(input: NodeDeclBody<'_>) -> TokenStream {
    let NodeDeclBody {
        node_input_port_decl_tokens,
        config_inputs_extend,
        fanin_input_decl_tokens,
        output_type_exprs,
        output_names,
        runtime_crate,
        registry_crate,
        output_sources,
        metadata_tokens,
    } = input;
    quote! {
        let mut __inputs = vec![#(#node_input_port_decl_tokens),*];
        #(#config_inputs_extend)*
        for __input in __inputs {
            __node = __node.input(__input);
        }
        for __fanin in vec![#(#fanin_input_decl_tokens),*] {
            __node = __node.fanin_input(__fanin);
        }
        #(
            {
                let __ty = #output_type_exprs;
                let mut __port = #registry_crate::capability::PortDecl::new(
                    #output_names,
                    #runtime_crate::transport::typeexpr_transport_key(&__ty),
                )
                .schema(__ty)
                .access(#runtime_crate::transport_types::AccessMode::Read);
                if let Some(__source) = #output_sources {
                    __port = __port.source(__source.as_str());
                }
                __node = __node.output(__port);
            }
        )*
        for (__key, __value) in #metadata_tokens {
            __node = __node.metadata(__key, __value);
        }
        Ok(__node)
    }
}

fn option_string(value: &Option<LitStr>) -> TokenStream {
    if let Some(value) = value {
        quote! { ::core::option::Option::Some(::std::string::String::from(#value)) }
    } else {
        quote! { ::core::option::Option::<::std::string::String>::None }
    }
}

fn residency_for_ty(ty: &syn::Type, runtime_crate: &TokenStream) -> Option<TokenStream> {
    let ident = last_segment(strip_ref(ty))?.ident.to_string();
    match ident.as_str() {
        "Cpu" => Some(quote! { #runtime_crate::transport_types::Residency::Cpu }),
        "Gpu" | "Device" => Some(quote! { #runtime_crate::transport_types::Residency::Gpu }),
        _ => None,
    }
}
