use proc_macro2::TokenStream;
use quote::quote;
use syn::LitStr;

use super::parse::PortMeta;
use super::type_analysis::{direct_payload_plain_type, direct_payload_same_type};
use crate::helpers::result_ok_type;
use crate::type_expr::value_type_key;

pub(super) struct DirectPayloadInputs<'a> {
    pub(super) is_low_level: bool,
    pub(super) has_generics: bool,
    pub(super) is_graph_node: bool,
    pub(super) runtime_node_present: bool,
    pub(super) exec_ctx_present: bool,
    pub(super) node_io_present: bool,
    pub(super) shader_ctx_present: bool,
    pub(super) state_ty_attr: bool,
    pub(super) config_types_empty: bool,
    pub(super) capability_attr: bool,
    pub(super) arg_types: &'a [syn::Type],
    pub(super) effective_inputs_for_args: &'a [PortMeta],
    pub(super) output_names: &'a [LitStr],
    pub(super) output_type_key: Option<&'a LitStr>,
    pub(super) ret: &'a syn::ReturnType,
    pub(super) same_payload_attr: bool,
    pub(super) inner_fn_ident: &'a syn::Ident,
    pub(super) runtime_crate: &'a TokenStream,
}

pub(super) fn direct_payload_registration(inputs: DirectPayloadInputs<'_>) -> TokenStream {
    let DirectPayloadInputs {
        is_low_level,
        has_generics,
        is_graph_node,
        runtime_node_present,
        exec_ctx_present,
        node_io_present,
        shader_ctx_present,
        state_ty_attr,
        config_types_empty,
        capability_attr,
        arg_types,
        effective_inputs_for_args,
        output_names,
        output_type_key,
        ret,
        same_payload_attr,
        inner_fn_ident,
        runtime_crate,
    } = inputs;

    let simple_typed_node = !is_low_level
        && !has_generics
        && !is_graph_node
        && !runtime_node_present
        && !exec_ctx_present
        && !node_io_present
        && !shader_ctx_present
        && !state_ty_attr
        && config_types_empty
        && !capability_attr
        && arg_types.len() == 1
        && effective_inputs_for_args.len() == 1
        && output_names.len() == 1;
    let ok_ty = result_ok_type(ret).and_then(direct_payload_plain_type);
    if !simple_typed_node {
        return quote! {};
    }
    let (Some(input_ty), Some(output_ty)) = (arg_types.first(), ok_ty) else {
        return quote! {};
    };
    if crate::foreign_type::is_foreign_view(input_ty) {
        return quote! {};
    }
    let input_port = &effective_inputs_for_args[0].name;
    let output_key = value_type_key(output_ty, output_type_key);
    let output_port = &output_names[0];
    let input_value_ty = if let syn::Type::Reference(reference) = input_ty {
        Some(reference.elem.as_ref())
    } else {
        Some(input_ty)
    };
    if same_payload_attr
        && input_value_ty
            .is_some_and(|input_value_ty| direct_payload_same_type(input_value_ty, output_ty))
    {
        return quote! {
            reg.on_direct_payload(Self::ID, |_node, _ctx, payload| {
                Ok(Some(payload))
            });
            let _ = #input_port;
            let _ = #output_port;
        };
    }

    let fetch_and_call = if let syn::Type::Reference(reference) = input_ty {
        if reference.mutability.is_some() {
            None
        } else {
            let inner = &reference.elem;
            Some(quote! {
                let __input = payload
                    .get_ref::<#inner>()
                    .ok_or_else(|| #runtime_crate::NodeError::missing_input(#input_port))?;
                #inner_fn_ident(__input)
            })
        }
    } else {
        Some(quote! {
            let __input = payload
                .try_into_owned::<#input_ty>()
                .map_err(|_| #runtime_crate::NodeError::missing_input(#input_port))?;
            #inner_fn_ident(__input)
        })
    };
    if let Some(fetch_and_call) = fetch_and_call {
        quote! {
            let __direct_output_key: #runtime_crate::transport_types::TypeKey = #output_key;
            reg.on_direct_payload(Self::ID, move |_node, _ctx, payload| {
                match {
                    #fetch_and_call
                } {
                    Ok(__value) => Ok(Some(#runtime_crate::transport_types::Payload::owned(
                        __direct_output_key.clone(),
                        __value,
                    ))),
                    Err(__error) => Err(__error),
                }
            });
            let _ = #output_port;
        }
    } else {
        quote! {}
    }
}

pub(super) struct HandlerRegistryInputs<'a> {
    pub(super) is_graph_node: bool,
    pub(super) has_generics: bool,
    pub(super) fn_impl_generics: &'a TokenStream,
    pub(super) fn_where_clause: &'a TokenStream,
    pub(super) runtime_crate: &'a TokenStream,
    pub(super) data_crate: &'a TokenStream,
    pub(super) handler_body: &'a TokenStream,
    pub(super) output_keys: &'a [TokenStream],
    pub(super) direct_payload_registration: &'a TokenStream,
}

pub(super) fn handler_registry_fn(inputs: HandlerRegistryInputs<'_>) -> TokenStream {
    let HandlerRegistryInputs {
        is_graph_node,
        has_generics,
        fn_impl_generics,
        fn_where_clause,
        runtime_crate,
        data_crate,
        handler_body,
        output_keys,
        direct_payload_registration,
    } = inputs;
    let registry = quote! { #runtime_crate::handler_registry::HandlerRegistry };
    let types_ty = quote! { &#data_crate::typing::TypeRegistry };
    let key_count = output_keys.len();
    // Output keys resolve once per registry, not per push; the handler owns them.
    let output_keys = quote! {
        let __output_keys: [#runtime_crate::transport_types::TypeKey; #key_count] =
            [#(#output_keys),*];
    };
    match (is_graph_node, has_generics) {
        (true, true) => quote! {
            pub fn handler_registry_for #fn_impl_generics (
                id: impl Into<String>,
                __types: #types_ty,
            ) -> #registry #fn_where_clause {
                let _ = (id, __types);
                #registry::new()
            }
        },
        (true, false) => quote! {
            pub fn handler_registry() -> #registry {
                #registry::new()
            }

            pub fn handler_registry_in(__types: #types_ty) -> #registry {
                let _ = __types;
                #registry::new()
            }
        },
        (false, true) => quote! {
            /// The handler under `id`; output keys resolve through `__types`.
            pub fn handler_registry_for #fn_impl_generics (
                id: impl Into<String>,
                __types: #types_ty,
            ) -> #registry #fn_where_clause {
                let id_str = id.into();
                #output_keys
                let mut reg = #registry::new();
                reg.on(&id_str, move |node, ctx, io| {
                    #handler_body
                });
                reg
            }
        },
        (false, false) => quote! {
            /// [`Self::handler_registry_in`] resolving through no registry.
            pub fn handler_registry() -> #registry {
                Self::handler_registry_in(#data_crate::typing::TypeRegistry::empty())
            }

            /// The node's handlers; output keys resolve once through `__types` (the installing
            /// registry's typing registry).
            pub fn handler_registry_in(__types: #types_ty) -> #registry {
                let mut reg = #registry::new();
                #direct_payload_registration
                #output_keys
                reg.on(Self::ID, move |node, ctx, io| {
                    #handler_body
                });
                reg
            }
        },
    }
}

pub(super) struct GraphRegisterInputs<'a> {
    pub(super) is_graph_node: bool,
    pub(super) graph_port_names: &'a [LitStr],
    pub(super) output_names: &'a [LitStr],
    pub(super) runtime_crate: &'a TokenStream,
    pub(super) graph_input_bindings: &'a [TokenStream],
    pub(super) inner_fn_ident: &'a syn::Ident,
    pub(super) graph_call_args: &'a [TokenStream],
    pub(super) graph_output_bindings: &'a TokenStream,
    pub(super) data_crate: &'a TokenStream,
}

pub(super) fn graph_register_tokens(inputs: GraphRegisterInputs<'_>) -> TokenStream {
    let GraphRegisterInputs {
        is_graph_node,
        graph_port_names,
        output_names,
        runtime_crate,
        graph_input_bindings,
        inner_fn_ident,
        graph_call_args,
        graph_output_bindings,
        data_crate,
    } = inputs;
    if !is_graph_node {
        return quote! {};
    }
    let input_names = graph_port_names.to_vec();
    let output_names = output_names.to_vec();
    quote! {
        let __graph_inputs = [#(#input_names),*];
        let __graph_outputs = [#(#output_names),*];
        let mut __graph_ctx = #runtime_crate::graph_builder::GraphCtx::new(
            into.combined_transport_capabilities()?,
            &__graph_inputs,
            &__graph_outputs,
        );
        #(#graph_input_bindings)*
        let __graph_ret = #inner_fn_ident(#(#graph_call_args),*);
        #graph_output_bindings
        let __graph = __graph_ctx.build();
        let __graph_json = #runtime_crate::graph_builder::graph_to_json(&__graph)
            .map_err(|_| "graph serialization failed")?;
        decl = decl.metadata(
            #runtime_crate::EMBEDDED_GRAPH_KEY,
            #data_crate::model::Value::String(::std::borrow::Cow::Owned(__graph_json)),
        );
        decl = decl.metadata(
            #runtime_crate::EMBEDDED_HOST_KEY,
            #data_crate::model::Value::String(::std::borrow::Cow::from("host")),
        );
    }
}

pub(super) struct RegisterFnInputs<'a> {
    pub(super) has_generics: bool,
    pub(super) is_graph_node: bool,
    pub(super) fn_impl_generics: &'a TokenStream,
    pub(super) runtime_crate: &'a TokenStream,
    pub(super) handle_ident: &'a syn::Ident,
    pub(super) fn_where_clause: &'a TokenStream,
    pub(super) fn_turbofish_generics: &'a TokenStream,
    pub(super) struct_ident: &'a syn::Ident,
    pub(super) graph_register_tokens: &'a TokenStream,
}

pub(super) fn register_fn(inputs: RegisterFnInputs<'_>) -> TokenStream {
    let RegisterFnInputs {
        has_generics,
        is_graph_node,
        fn_impl_generics,
        runtime_crate,
        handle_ident,
        fn_where_clause,
        fn_turbofish_generics,
        struct_ident,
        graph_register_tokens,
    } = inputs;
    if !has_generics {
        return quote! {};
    }
    if is_graph_node {
        quote! {
            pub fn register_for #fn_impl_generics (
                into: &mut #runtime_crate::plugins::PluginRegistry,
                id: impl Into<String>,
            ) -> #runtime_crate::plugins::PluginResult<#handle_ident> #fn_where_clause {
                let local_id: String = id.into();
                let full_id = if let Some(prefix) = &into.current_prefix {
                    #runtime_crate::apply_node_prefix(prefix, &local_id)
                } else {
                    local_id.clone()
                };
                for __contract in #struct_ident::boundary_contracts_for #fn_turbofish_generics (&into.type_registry)? {
                    into.register_boundary_contract(__contract)?;
                }
                let mut decl = #struct_ident::node_decl_for #fn_turbofish_generics (full_id.clone(), &into.type_registry)?;
                #graph_register_tokens
                into.register_node_decl(decl)?;
                Ok(#handle_ident::new_with_id(full_id))
            }
        }
    } else {
        quote! {
            pub fn register_for #fn_impl_generics (
                into: &mut #runtime_crate::plugins::PluginRegistry,
                id: impl Into<String>,
            ) -> #runtime_crate::plugins::PluginResult<#handle_ident> #fn_where_clause {
                let local_id: String = id.into();
                let full_id = if let Some(prefix) = &into.current_prefix {
                    #runtime_crate::apply_node_prefix(prefix, &local_id)
                } else {
                    local_id.clone()
                };
                for __contract in #struct_ident::boundary_contracts_for #fn_turbofish_generics (&into.type_registry)? {
                    into.register_boundary_contract(__contract)?;
                }
                let decl = #struct_ident::node_decl_for #fn_turbofish_generics (full_id.clone(), &into.type_registry)?;
                into.register_node_decl(decl)?;
                let handlers = #struct_ident::handler_registry_for #fn_turbofish_generics (full_id.clone(), &into.type_registry);
                into.handlers.merge(handlers);
                Ok(#handle_ident::new_with_id(full_id))
            }
        }
    }
}

pub(super) fn capability_helper(
    capability_attr: Option<&LitStr>,
    inputs_len: usize,
    cap_impl_generics: &TokenStream,
    runtime_crate: &TokenStream,
    cap_where_clause: &TokenStream,
    cap_type_param: &syn::Ident,
    inner_fn_ident: &syn::Ident,
) -> TokenStream {
    if let Some(cap) = capability_attr {
        match inputs_len {
            2 => quote! {
                pub fn register_capability #cap_impl_generics (
                    into: &mut #runtime_crate::plugins::PluginRegistry,
                ) #cap_where_clause {
                    into.register_capability_typed::<#cap_type_param, _>(#cap, |a, b| #inner_fn_ident(a.clone(), b.clone()));
                }
            },
            3 => quote! {
                pub fn register_capability #cap_impl_generics (
                    into: &mut #runtime_crate::plugins::PluginRegistry,
                ) #cap_where_clause {
                    into.register_capability_typed3::<#cap_type_param, _>(#cap, |x, lo, hi| #inner_fn_ident(x.clone(), lo.clone(), hi.clone()));
                }
            },
            _ => {
                quote! { compile_error!("capability nodes currently support only 2 or 3 inputs"); }
            }
        }
    } else {
        quote! {}
    }
}

pub(super) struct NodeInstallInputs<'a> {
    pub(super) has_generics: bool,
    pub(super) capability_attr: bool,
    pub(super) is_graph_node: bool,
    pub(super) runtime_crate: &'a TokenStream,
    pub(super) struct_ident: &'a syn::Ident,
    pub(super) registry_crate: &'a TokenStream,
    pub(super) graph_register_tokens: &'a TokenStream,
}

pub(super) fn node_install_impl(inputs: NodeInstallInputs<'_>) -> TokenStream {
    let NodeInstallInputs {
        has_generics,
        capability_attr,
        is_graph_node,
        runtime_crate,
        struct_ident,
        registry_crate,
        graph_register_tokens,
    } = inputs;
    if has_generics && !capability_attr {
        return quote! {};
    }
    if is_graph_node {
        quote! {
            impl #runtime_crate::plugins::NodeInstall for #struct_ident {
                fn register(into: &mut #runtime_crate::plugins::PluginRegistry) -> #runtime_crate::plugins::PluginResult<()> {
                    for __contract in #struct_ident::boundary_contracts_in(&into.type_registry)? {
                        into.register_boundary_contract(__contract)?;
                    }
                    let mut decl = #struct_ident::node_decl_in(&into.type_registry)?;
                    if let Some(prefix) = &into.current_prefix {
                        let full_id = #runtime_crate::apply_node_prefix(prefix, #struct_ident::ID);
                        decl.id = #registry_crate::ids::NodeId::new(&full_id);
                    }
                    #struct_ident::register_port_types(into, &decl.id.0)?;
                    #graph_register_tokens
                    into.register_node_decl(decl)?;
                    Ok(())
                }
            }
        }
    } else {
        quote! {
            impl #runtime_crate::plugins::NodeInstall for #struct_ident {
                fn register(into: &mut #runtime_crate::plugins::PluginRegistry) -> #runtime_crate::plugins::PluginResult<()> {
                    for __contract in #struct_ident::boundary_contracts_in(&into.type_registry)? {
                        into.register_boundary_contract(__contract)?;
                    }
                    let mut decl = #struct_ident::node_decl_in(&into.type_registry)?;
                    if let Some(prefix) = &into.current_prefix {
                        let full_id = #runtime_crate::apply_node_prefix(prefix, #struct_ident::ID);
                        decl.id = #registry_crate::ids::NodeId::new(&full_id);
                    }
                    #struct_ident::register_port_types(into, &decl.id.0)?;
                    into.register_node_decl(decl)?;
                    let handlers = #struct_ident::handler_registry_in(&into.type_registry);
                    let handlers = match &into.current_prefix {
                        Some(prefix) => handlers.with_prefix(prefix),
                        None => handlers,
                    };
                    into.handlers.merge(handlers);
                    Ok(())
                }
            }
        }
    }
}
