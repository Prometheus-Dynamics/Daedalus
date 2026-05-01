use proc_macro2::TokenStream;
use quote::quote;
use syn::LitStr;

use super::parse::PortMeta;
use super::type_analysis::{direct_payload_plain_type, direct_payload_same_type, result_ok_type};

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
    pub(super) ret: &'a syn::ReturnType,
    pub(super) same_payload_attr: bool,
    pub(super) inner_fn_ident: &'a syn::Ident,
    pub(super) runtime_crate: &'a TokenStream,
    pub(super) data_crate: &'a TokenStream,
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
        ret,
        same_payload_attr,
        inner_fn_ident,
        runtime_crate,
        data_crate,
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
    let input_port = &effective_inputs_for_args[0].name;
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
                    .ok_or_else(|| #runtime_crate::NodeError::InvalidInput(format!("missing {}", #input_port)))?;
                #inner_fn_ident(__input)
            })
        }
    } else {
        Some(quote! {
            let __input = payload
                .try_into_owned::<#input_ty>()
                .map_err(|_| #runtime_crate::NodeError::InvalidInput(format!("missing {}", #input_port)))?;
            #inner_fn_ident(__input)
        })
    };
    if let Some(fetch_and_call) = fetch_and_call {
        quote! {
            reg.on_direct_payload(Self::ID, |_node, _ctx, payload| {
                match {
                    #fetch_and_call
                } {
                    Ok(__value) => {
                        static __OUTPUT_TYPE_KEY: ::std::sync::OnceLock<
                            Result<#runtime_crate::transport_types::TypeKey, ::std::string::String>
                        > = ::std::sync::OnceLock::new();
                        let __key = match __OUTPUT_TYPE_KEY.get_or_init(|| {
                            let __ty = #data_crate::typing::type_expr::<#output_ty>();
                            #runtime_crate::transport::typeexpr_transport_key(&__ty)
                                .map_err(|err| err.to_string())
                        }) {
                            Ok(__key) => __key.clone(),
                            Err(__err) => {
                                return Err(#runtime_crate::NodeError::Handler(__err.clone()));
                            }
                        };
                        Ok(Some(#runtime_crate::transport_types::Payload::owned(__key, __value)))
                    }
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
    pub(super) handler_body: &'a TokenStream,
    pub(super) direct_payload_registration: &'a TokenStream,
}

pub(super) fn handler_registry_fn(inputs: HandlerRegistryInputs<'_>) -> TokenStream {
    let HandlerRegistryInputs {
        is_graph_node,
        has_generics,
        fn_impl_generics,
        fn_where_clause,
        runtime_crate,
        handler_body,
        direct_payload_registration,
    } = inputs;
    if is_graph_node {
        if has_generics {
            quote! {
                pub fn handler_registry_for #fn_impl_generics (id: impl Into<String>) -> #runtime_crate::handler_registry::HandlerRegistry #fn_where_clause {
                    let _ = id;
                    #runtime_crate::handler_registry::HandlerRegistry::new()
                }
            }
        } else {
            quote! {
                pub fn handler_registry() -> #runtime_crate::handler_registry::HandlerRegistry {
                    #runtime_crate::handler_registry::HandlerRegistry::new()
                }
            }
        }
    } else if has_generics {
        quote! {
            pub fn handler_registry_for #fn_impl_generics (id: impl Into<String>) -> #runtime_crate::handler_registry::HandlerRegistry #fn_where_clause {
                let id_str = id.into();
                let mut reg = #runtime_crate::handler_registry::HandlerRegistry::new();
                reg.on(&id_str, |node, ctx, io| {
                    #handler_body
                });
                reg
            }
        }
    } else {
        quote! {
            pub fn handler_registry() -> #runtime_crate::handler_registry::HandlerRegistry {
                let mut reg = #runtime_crate::handler_registry::HandlerRegistry::new();
                reg.on(Self::ID, |node, ctx, io| {
                    #handler_body
                });
                #direct_payload_registration
                reg
            }
        }
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
                for __contract in #struct_ident::boundary_contracts_for #fn_turbofish_generics ()? {
                    into.register_boundary_contract(__contract)?;
                }
                let mut decl = #struct_ident::node_decl_for #fn_turbofish_generics (full_id.clone())?;
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
                for __contract in #struct_ident::boundary_contracts_for #fn_turbofish_generics ()? {
                    into.register_boundary_contract(__contract)?;
                }
                let decl = #struct_ident::node_decl_for #fn_turbofish_generics (full_id.clone())?;
                into.register_node_decl(decl)?;
                let handlers = #struct_ident::handler_registry_for #fn_turbofish_generics (full_id.clone());
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
                    for __contract in #struct_ident::boundary_contracts()? {
                        into.register_boundary_contract(__contract)?;
                    }
                    let mut decl = #struct_ident::node_decl()?;
                    if let Some(prefix) = &into.current_prefix {
                        let full_id = #runtime_crate::apply_node_prefix(prefix, #struct_ident::ID);
                        decl.id = #registry_crate::ids::NodeId::new(&full_id);
                    }
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
                    for __contract in #struct_ident::boundary_contracts()? {
                        into.register_boundary_contract(__contract)?;
                    }
                    let mut decl = #struct_ident::node_decl()?;
                    if let Some(prefix) = &into.current_prefix {
                        let full_id = #runtime_crate::apply_node_prefix(prefix, #struct_ident::ID);
                        decl.id = #registry_crate::ids::NodeId::new(&full_id);
                    }
                    into.register_node_decl(decl)?;
                    let handlers = if let Some(prefix) = &into.current_prefix {
                        #struct_ident::handler_registry().with_prefix(prefix)
                    } else {
                        #struct_ident::handler_registry()
                    };
                    into.handlers.merge(handlers);
                    Ok(())
                }
            }
        }
    }
}
