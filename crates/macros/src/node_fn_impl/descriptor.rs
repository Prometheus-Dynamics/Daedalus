use proc_macro2::{Span, TokenStream};
use quote::quote;
use syn::LitStr;

use super::parse::{OutputPortMeta, PortMeta};
use super::type_analysis::{contract_type_for, opaque_fallback_type_expr_for, type_expr_for};

pub(super) fn is_fanin_ty(ty: &syn::Type) -> bool {
    let ty = if let syn::Type::Reference(r) = ty {
        &*r.elem
    } else {
        ty
    };
    if let syn::Type::Path(tp) = ty
        && tp.qself.is_none()
        && let Some(seg) = tp.path.segments.last()
        && seg.ident == "FanIn"
    {
        return true;
    }
    false
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
            let is_binding_mut = arg_mut_bindings.get(idx).copied().unwrap_or(false);
            let is_ref = matches!(raw_aty, syn::Type::Reference(_));
            let is_ref_mut = matches!(raw_aty, syn::Type::Reference(r) if r.mutability.is_some());
            let aty = if let syn::Type::Reference(r) = raw_aty {
                &*r.elem
            } else {
                raw_aty
            };
            let is_arc = if let syn::Type::Path(tp) = aty
                && tp.qself.is_none()
                && let Some(seg) = tp.path.segments.last()
            {
                seg.ident == "Arc"
            } else {
                false
            };
            let access = if is_ref_mut || is_binding_mut {
                quote! { #runtime_crate::transport_types::AccessMode::Modify }
            } else if is_ref || is_arc {
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
            let ty_expr = if let Some(ty) = port.ty_override.as_ref() {
                quote! { (#ty) }
            } else if let Some(ts) = type_expr_for(aty, generic_type_params, data_crate) {
                ts
            } else {
                opaque_fallback_type_expr_for(aty, data_crate)
            };

            Some(port_decl_token(PortDeclToken {
                name,
                source,
                default,
                ty_expr,
                access,
                residency,
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
        runtime_crate,
        registry_crate,
    } = input;
    quote! {
        {
            let __ty = #ty_expr;
            let mut __port = #registry_crate::capability::PortDecl::new(
                #name,
                #runtime_crate::transport::typeexpr_transport_key(&__ty)
                    .map_err(|_| "invalid node input type key")?,
            )
            .schema(__ty)
            .access(#access);
            #residency
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
    fn peel_wrapped(ty: &syn::Type) -> &syn::Type {
        if let syn::Type::Path(p) = ty
            && let Some(seg) = p.path.segments.last()
        {
            let ident = seg.ident.to_string();
            if (ident == "Result" || ident == "Option")
                && let syn::PathArguments::AngleBracketed(ab) = &seg.arguments
                && let Some(syn::GenericArgument::Type(inner)) = ab.args.first()
            {
                return inner;
            }
        }
        ty
    }

    let explicit: Vec<Option<TokenStream>> = outputs
        .iter()
        .map(|p| p.ty_override.as_ref().map(|ts| quote! { (#ts) }))
        .collect();

    let mut out: Vec<TokenStream> = Vec::new();
    if let syn::ReturnType::Type(_, ty) = ret {
        let mut base_ty: &syn::Type = ty.as_ref();
        loop {
            let next = peel_wrapped(base_ty);
            if std::ptr::eq(next, base_ty) {
                break;
            }
            base_ty = next;
        }

        if let syn::Type::Tuple(t) = base_ty {
            if t.elems.len() == outputs.len() {
                for (idx, elem) in t.elems.iter().enumerate() {
                    if let Some(ts) = explicit.get(idx).and_then(|v| v.clone()) {
                        out.push(ts);
                        continue;
                    }
                    out.push(
                        type_expr_for(elem, generic_type_params, data_crate)
                            .unwrap_or_else(|| opaque_fallback_type_expr_for(elem, data_crate)),
                    );
                }
            }
        } else if outputs.len() == 1 {
            if let Some(ts) = explicit.first().and_then(|v| v.clone()) {
                out.push(ts);
            } else if let Some(ts) = type_expr_for(base_ty, generic_type_params, data_crate) {
                out.push(ts);
            } else {
                out.push(opaque_fallback_type_expr_for(base_ty, data_crate));
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
                    quote! { #data_crate::typing::type_expr::<#contract_ty>() }
                };
                Some(boundary_contract_push(
                    ty_expr,
                    quote! { #contract_ty },
                    "invalid node input boundary type key",
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
                        quote! { #data_crate::typing::type_expr::<#contract_ty>() }
                    };
                Some(boundary_contract_push(
                    ty_expr,
                    quote! { #contract_ty },
                    "invalid node output boundary type key",
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

    quote! {
        pub fn boundary_contracts() -> Result<Vec<#runtime_crate::transport_types::BoundaryTypeContract>, &'static str> {
            let mut __contracts: Vec<#runtime_crate::transport_types::BoundaryTypeContract> = Vec::new();
            #(#boundary_input_contracts)*
            #(#boundary_output_contracts)*
            __contracts.sort_by(|a, b| a.type_key.cmp(&b.type_key));
            __contracts.dedup_by(|a, b| a.type_key == b.type_key);
            Ok(__contracts)
        }

        pub fn boundary_contracts_for #fn_impl_generics () -> Result<Vec<#runtime_crate::transport_types::BoundaryTypeContract>, &'static str> #fn_where_clause {
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
    error: &'static str,
    runtime_crate: &TokenStream,
) -> TokenStream {
    quote! {
        {
            let __ty = #ty_expr;
            let __key = #runtime_crate::transport::typeexpr_transport_key(&__ty)
                .map_err(|_| #error)?;
            __contracts.push(
                #runtime_crate::transport_types::BoundaryTypeContract::for_type::<#contract_ty>(
                    __key,
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
            let syn::Type::Path(tp) = aty else {
                return None;
            };
            let seg = tp.path.segments.last()?;
            if seg.ident != "FanIn" {
                return None;
            }
            let syn::PathArguments::AngleBracketed(ab) = &seg.arguments else {
                return None;
            };
            let Some(syn::GenericArgument::Type(inner_ty)) = ab.args.first() else {
                return None;
            };

            let prefix = &port.name;
            let ty_expr = if let Some(ty) = port.ty_override.as_ref() {
                quote! { (#ty) }
            } else {
                type_expr_for(inner_ty, generic_type_params, data_crate)
                    .unwrap_or_else(|| opaque_fallback_type_expr_for(inner_ty, data_crate))
            };
            Some(quote! {
                {
                    let __ty = #ty_expr;
                    #registry_crate::capability::FanInDecl::new(
                        #prefix,
                        0,
                        #runtime_crate::transport::typeexpr_transport_key(&__ty)
                            .map_err(|_| "invalid fan-in input type key")?,
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
    pub(super) id: &'a LitStr,
    pub(super) node_input_port_decl_tokens: &'a [TokenStream],
    pub(super) config_inputs_extend: &'a [TokenStream],
    pub(super) fanin_input_decl_tokens: &'a [TokenStream],
    pub(super) output_type_exprs: &'a [TokenStream],
    pub(super) output_names: &'a [LitStr],
    pub(super) runtime_crate: &'a TokenStream,
    pub(super) output_sources: &'a [TokenStream],
    pub(super) metadata_tokens: &'a TokenStream,
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
    if has_generics {
        quote! {
            pub fn node_decl_for #fn_impl_generics (id: impl Into<String>) -> Result<#registry_crate::capability::NodeDecl, &'static str> #fn_where_clause {
                let id_str = id.into();
                let mut __node = #registry_crate::capability::NodeDecl::new(id_str);
                #body
            }
        }
    } else {
        quote! {
            pub fn node_decl() -> Result<#registry_crate::capability::NodeDecl, &'static str> {
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
                    #runtime_crate::transport::typeexpr_transport_key(&__ty)
                        .map_err(|_| "invalid node output type key")?,
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
    let ty = if let syn::Type::Reference(r) = ty {
        &*r.elem
    } else {
        ty
    };
    let syn::Type::Path(path) = ty else {
        return None;
    };
    let ident = path.path.segments.last()?.ident.to_string();
    match ident.as_str() {
        "Cpu" => Some(quote! { #runtime_crate::transport_types::Residency::Cpu }),
        "Gpu" | "Device" => Some(quote! { #runtime_crate::transport_types::Residency::Gpu }),
        _ => None,
    }
}
