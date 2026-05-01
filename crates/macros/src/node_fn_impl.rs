use proc_macro::TokenStream;
use proc_macro2::Span;
use quote::quote;
use syn::{ItemFn, LitStr, parse_macro_input, parse_quote};

use crate::helpers::{AttributeArgs, compile_error};

mod crate_paths;
mod descriptor;
mod final_tokens;
mod handler;
mod handler_fetch;
mod idents;
mod metadata;
mod parse;
mod registration;
mod shader;
mod type_analysis;

use crate_paths::CratePaths;
use idents::{node_struct_ident, port_ident};
use parse::NodeArgs;
use type_analysis::output_contract_types;

pub fn node(args: TokenStream, item: TokenStream) -> TokenStream {
    let args = parse_macro_input!(args with AttributeArgs::parse_terminated);
    let mut input = parse_macro_input!(item as ItemFn);

    let CratePaths {
        runtime_crate,
        registry_crate,
        data_crate,
        core_crate,
        gpu_crate,
    } = CratePaths::detect();

    let NodeArgs {
        id,
        summary_attr,
        description_attr,
        generics_attr,
        inputs,
        config_types,
        outputs,
        shader_path,
        shader_paths,
        shader_entry,
        shader_workgroup,
        shader_bindings,
        shader_specs,
        state_ty_attr,
        compute_attr,
        sync_groups_attr,
        capability_attr,
        fallback_attr,
        same_payload_attr,
    } = match parse::parse_node_args(args, &data_crate, &gpu_crate) {
        Ok(args) => args,
        Err(err) => return TokenStream::from(err),
    };
    let fn_ident = input.sig.ident.clone();
    let struct_ident = node_struct_ident(&fn_ident);
    let handle_ident = syn::Ident::new(&format!("{}Handle", struct_ident), Span::call_site());
    let inputs_ident = syn::Ident::new(&format!("{}Inputs", struct_ident), Span::call_site());
    let outputs_ident = syn::Ident::new(&format!("{}Outputs", struct_ident), Span::call_site());
    let inner_fn_ident = syn::Ident::new(&format!("{}_impl", fn_ident), Span::call_site());

    // Move the user function body into an inner helper to keep the external API clean.
    input.sig.ident = inner_fn_ident.clone();
    input.attrs.push(parse_quote! {
        #[allow(dead_code)]
    });
    let sig_for_ports = input.sig.clone();

    // Detect low-level vs typed signature (same heuristic as node_handler).
    // Low-level is only the raw (node, ctx, io) triad; otherwise we treat as typed
    // and allow RuntimeNode/ExecutionContext/NodeIo to appear anywhere.
    let is_low_level = {
        let inputs_sig = &input.sig.inputs;
        inputs_sig.len() == 3
            && inputs_sig.iter().all(|arg| {
                if let syn::FnArg::Typed(pat) = arg {
                    matches!(&*pat.ty, syn::Type::Reference(_))
                } else {
                    false
                }
            })
    };

    let has_shaders = shader_path.is_some() || !shader_specs.is_empty() || !shader_paths.is_empty();
    let _compute_expr: proc_macro2::TokenStream = if let Some(ts) = compute_attr.clone() {
        quote! { #ts }
    } else if has_shaders {
        quote! { #core_crate::compute::ComputeAffinity::GpuRequired }
    } else {
        quote! { #core_crate::compute::ComputeAffinity::CpuOnly }
    };
    // Common descriptor payload.
    let inputs_vec = inputs.clone();
    let outputs_vec = outputs.clone();
    let input_names: Vec<LitStr> = inputs_vec.iter().map(|p| p.name.clone()).collect();
    let output_names: Vec<LitStr> = outputs_vec.iter().map(|p| p.name.clone()).collect();
    let input_idents: Vec<syn::Ident> = inputs_vec
        .iter()
        .map(|p| port_ident(&p.name.value()))
        .collect();
    let output_sources: Vec<proc_macro2::TokenStream> = outputs_vec
        .iter()
        .map(|p| {
            if let Some(s) = &p.source {
                quote! { ::core::option::Option::Some(::std::string::String::from(#s)) }
            } else {
                quote! { ::core::option::Option::<::std::string::String>::None }
            }
        })
        .collect();
    let output_idents: Vec<syn::Ident> = outputs_vec
        .iter()
        .map(|p| port_ident(&p.name.value()))
        .collect();

    let handler_build = match handler::build_handler(handler::HandlerInputs {
        is_low_level,
        input: &input,
        inputs_vec: &inputs_vec,
        outputs_len: outputs_vec.len(),
        output_names: &output_names,
        output_idents: &output_idents,
        config_types: &config_types,
        shader_path: shader_path.as_ref(),
        shader_paths: &shader_paths,
        shader_entry: &shader_entry,
        shader_workgroup,
        shader_bindings: &shader_bindings,
        shader_specs: &shader_specs,
        state_ty_attr: state_ty_attr.as_ref(),
        capability_attr: capability_attr.as_ref(),
        inner_fn_ident: &inner_fn_ident,
        data_crate: &data_crate,
        runtime_crate: &runtime_crate,
        gpu_crate: &gpu_crate,
    }) {
        Ok(handler) => handler,
        Err(err) => return TokenStream::from(err),
    };
    let handler::HandlerBuild {
        handler_body,
        effective_inputs_for_args,
        arg_types,
        arg_idents,
        arg_mut_bindings,
        graph_ctx_arg,
        runtime_node_present,
        exec_ctx_present,
        node_io_present,
        shader_ctx_present,
    } = handler_build;

    if let Some(arg) = &graph_ctx_arg
        && !arg.is_mut_ref
    {
        return TokenStream::from(compile_error(
            "GraphCtx parameter must be passed as &mut GraphCtx".into(),
        ));
    }

    let is_graph_node = graph_ctx_arg.is_some();
    if is_graph_node {
        if state_ty_attr.is_some() {
            return TokenStream::from(compile_error(
                "graph-backed nodes cannot use state(...)".into(),
            ));
        }
        if !config_types.is_empty() {
            return TokenStream::from(compile_error(
                "graph-backed nodes cannot use config(...) types".into(),
            ));
        }
        if capability_attr.is_some() {
            return TokenStream::from(compile_error(
                "graph-backed nodes cannot use capability(...)".into(),
            ));
        }
        if runtime_node_present || exec_ctx_present || node_io_present || shader_ctx_present {
            return TokenStream::from(compile_error(
                "graph-backed nodes cannot use RuntimeNode/ExecutionContext/NodeIo/ShaderContext parameters".into(),
            ));
        }
    }

    let has_fanin_inputs = !is_low_level
        && arg_types.iter().any(|ty| {
            if let syn::Type::Path(tp) = ty
                && tp.qself.is_none()
                && let Some(seg) = tp.path.segments.last()
            {
                return seg.ident == "FanIn";
            }
            false
        });

    let _sync_groups_tokens: proc_macro2::TokenStream = if let Some(ts) = sync_groups_attr {
        ts
    } else if has_fanin_inputs {
        // Disable implicit AllReady sync across indexed fan-in ports. FanIn ports are dynamic and
        // do not share correlation ids, so the default alignment would otherwise suppress firing.
        quote! {
            vec![#core_crate::sync::SyncGroup {
                name: "__fanin".into(),
                policy: #core_crate::sync::SyncPolicy::AllReady,
                backpressure: None,
                capacity: None,
                ports: Vec::new(),
            }]
        }
    } else {
        quote! { Vec::<#core_crate::sync::SyncGroup>::new() }
    };

    let has_generics = generics_attr.is_some();
    let fn_generics = input.sig.generics.clone();
    if has_generics && fn_generics.params.is_empty() {
        return TokenStream::from(compile_error(
            "generics(...) specified but function has no generic parameters".into(),
        ));
    }
    let (fn_impl_generics, fn_ty_generics, fn_where_clause) = fn_generics.split_for_impl();
    let fn_turbofish_generics: proc_macro2::TokenStream = quote! { ::#fn_ty_generics };
    let fn_where_clause_ts: proc_macro2::TokenStream = quote! { #fn_where_clause };

    let generic_type_params: ::std::collections::HashSet<::std::string::String> = input
        .sig
        .generics
        .type_params()
        .map(|tp| tp.ident.to_string())
        .collect();

    let node_input_port_decl_tokens =
        descriptor::node_input_port_decl_tokens(descriptor::InputDeclInputs {
            is_low_level,
            inputs: &inputs_vec,
            effective_inputs_for_args: &effective_inputs_for_args,
            arg_types: &arg_types,
            arg_mut_bindings: &arg_mut_bindings,
            generic_type_params: &generic_type_params,
            data_crate: &data_crate,
            runtime_crate: &runtime_crate,
            registry_crate: &registry_crate,
        });

    let output_type_exprs = descriptor::output_type_exprs(
        &input.sig.output,
        &outputs_vec,
        &generic_type_params,
        &data_crate,
    );

    let has_fn_generics = !fn_generics.params.is_empty();
    let output_contract_tys = output_contract_types(&input.sig.output, outputs_vec.len());
    let fn_impl_generics_ts: proc_macro2::TokenStream = quote! { #fn_impl_generics };
    let boundary_contracts_fn = descriptor::boundary_contracts_fn(descriptor::BoundaryInputs {
        is_low_level,
        has_fn_generics,
        effective_inputs_for_args: &effective_inputs_for_args,
        arg_types: &arg_types,
        output_contract_tys: &output_contract_tys,
        outputs: &outputs_vec,
        data_crate: &data_crate,
        runtime_crate: &runtime_crate,
        fn_impl_generics: &fn_impl_generics_ts,
        fn_where_clause: &fn_where_clause_ts,
    });

    let graph_port_names: Vec<LitStr> = effective_inputs_for_args
        .iter()
        .map(|p| p.name.clone())
        .collect();

    let graph_call_args: Vec<proc_macro2::TokenStream> = if is_graph_node {
        let mut args = Vec::new();
        for arg in &sig_for_ports.inputs {
            if let syn::FnArg::Typed(pat) = arg
                && let syn::Pat::Ident(id) = &*pat.pat
            {
                if let Some(ctx) = &graph_ctx_arg
                    && id.ident == ctx.ident
                {
                    args.push(quote! { &mut __graph_ctx });
                    continue;
                }
                let ident = &id.ident;
                args.push(quote! { #ident });
            }
        }
        args
    } else {
        Vec::new()
    };

    let graph_input_bindings: Vec<proc_macro2::TokenStream> = if is_graph_node {
        arg_idents
            .iter()
            .zip(graph_port_names.iter())
            .map(|(ident, port)| {
                quote! {
                    let #ident = __graph_ctx.input(#port);
                }
            })
            .collect()
    } else {
        Vec::new()
    };

    let graph_output_bindings: proc_macro2::TokenStream = if is_graph_node {
        if output_names.is_empty() {
            quote! {}
        } else if output_names.len() == 1 {
            let name = &output_names[0];
            quote! {
                __graph_ctx.bind_output(#name, &__graph_ret);
            }
        } else {
            let out_idents: Vec<syn::Ident> = (0..output_names.len())
                .map(|i| syn::Ident::new(&format!("__graph_out_{i}"), Span::call_site()))
                .collect();
            let bind_calls: Vec<proc_macro2::TokenStream> = output_names
                .iter()
                .zip(out_idents.iter())
                .map(|(name, ident)| {
                    quote! { __graph_ctx.bind_output(#name, &#ident); }
                })
                .collect();
            quote! {
                let (#(#out_idents),*) = __graph_ret;
                #(#bind_calls)*
            }
        }
    } else {
        quote! {}
    };

    let fanin_input_decl_tokens = descriptor::fanin_input_decl_tokens(descriptor::FanInInputs {
        is_low_level,
        effective_inputs_for_args: &effective_inputs_for_args,
        arg_types: &arg_types,
        generic_type_params: &generic_type_params,
        data_crate: &data_crate,
        runtime_crate: &runtime_crate,
        registry_crate: &registry_crate,
    });

    let (handle_input_idents, handle_input_names): (Vec<syn::Ident>, Vec<LitStr>) = if is_low_level
    {
        (input_idents.clone(), input_names.clone())
    } else {
        let mut idents = Vec::new();
        let mut names = Vec::new();
        for (idx, port) in effective_inputs_for_args.iter().enumerate() {
            let Some(ty) = arg_types.get(idx) else {
                continue;
            };
            if descriptor::is_fanin_ty(ty) {
                continue;
            }
            idents.push(port_ident(&port.name.value()));
            names.push(port.name.clone());
        }
        (idents, names)
    };

    let input_access: Vec<(LitStr, LitStr)> = if is_low_level {
        Vec::new()
    } else {
        let mut out = Vec::new();
        for (idx, port) in effective_inputs_for_args.iter().enumerate() {
            let Some(ty) = arg_types.get(idx) else {
                continue;
            };
            if descriptor::is_fanin_ty(ty) {
                continue;
            }
            let (is_ref, is_ref_mut) = if let syn::Type::Reference(r) = ty {
                (true, r.mutability.is_some())
            } else {
                (false, false)
            };
            let is_binding_mut = arg_mut_bindings.get(idx).copied().unwrap_or(false);
            let access = if is_ref {
                if is_ref_mut { "mutable" } else { "borrowed" }
            } else if is_binding_mut {
                "mutable"
            } else {
                "owned"
            };
            out.push((port.name.clone(), LitStr::new(access, Span::call_site())));
        }
        out
    };

    let metadata_tokens: proc_macro2::TokenStream =
        metadata::metadata_tokens(metadata::MetadataInputs {
            summary_attr: summary_attr.as_ref(),
            description_attr: description_attr.as_ref(),
            inputs: &inputs_vec,
            input_access: &input_access,
            outputs: &outputs_vec,
            fallback_attr: fallback_attr.as_ref(),
            config_types: &config_types,
            data_crate: &data_crate,
            runtime_crate: &runtime_crate,
        });

    let config_inputs_extend: Vec<proc_macro2::TokenStream> = config_types
        .iter()
        .map(|ty| {
            quote! {
                for __port in <#ty as #runtime_crate::config::NodeConfig>::ports() {
                    __inputs.push(__port);
                }
            }
        })
        .collect();

    let node_decl_fn = descriptor::node_decl_fn(descriptor::NodeDeclInputs {
        has_generics,
        fn_impl_generics: &fn_impl_generics_ts,
        registry_crate: &registry_crate,
        fn_where_clause: &fn_where_clause_ts,
        id: &id,
        node_input_port_decl_tokens: &node_input_port_decl_tokens,
        config_inputs_extend: &config_inputs_extend,
        fanin_input_decl_tokens: &fanin_input_decl_tokens,
        output_type_exprs: &output_type_exprs,
        output_names: &output_names,
        runtime_crate: &runtime_crate,
        output_sources: &output_sources,
        metadata_tokens: &metadata_tokens,
    });

    let direct_payload_registration =
        registration::direct_payload_registration(registration::DirectPayloadInputs {
            is_low_level,
            has_generics,
            is_graph_node,
            runtime_node_present,
            exec_ctx_present,
            node_io_present,
            shader_ctx_present,
            state_ty_attr: state_ty_attr.is_some(),
            config_types_empty: config_types.is_empty(),
            capability_attr: capability_attr.is_some(),
            arg_types: &arg_types,
            effective_inputs_for_args: &effective_inputs_for_args,
            output_names: &output_names,
            ret: &input.sig.output,
            same_payload_attr,
            inner_fn_ident: &inner_fn_ident,
            runtime_crate: &runtime_crate,
            data_crate: &data_crate,
        });

    let handler_registry_fn =
        registration::handler_registry_fn(registration::HandlerRegistryInputs {
            is_graph_node,
            has_generics,
            fn_impl_generics: &fn_impl_generics_ts,
            fn_where_clause: &fn_where_clause_ts,
            runtime_crate: &runtime_crate,
            handler_body: &handler_body,
            direct_payload_registration: &direct_payload_registration,
        });

    let graph_register_tokens =
        registration::graph_register_tokens(registration::GraphRegisterInputs {
            is_graph_node,
            graph_port_names: &graph_port_names,
            output_names: &output_names,
            runtime_crate: &runtime_crate,
            graph_input_bindings: &graph_input_bindings,
            inner_fn_ident: &inner_fn_ident,
            graph_call_args: &graph_call_args,
            graph_output_bindings: &graph_output_bindings,
            data_crate: &data_crate,
        });

    let register_fn = registration::register_fn(registration::RegisterFnInputs {
        has_generics,
        is_graph_node,
        fn_impl_generics: &fn_impl_generics_ts,
        runtime_crate: &runtime_crate,
        handle_ident: &handle_ident,
        fn_where_clause: &fn_where_clause_ts,
        fn_turbofish_generics: &fn_turbofish_generics,
        struct_ident: &struct_ident,
        graph_register_tokens: &graph_register_tokens,
    });

    let fn_generics = input.sig.generics.clone();
    let (cap_impl_generics, _cap_ty_generics, cap_where_clause) = fn_generics.split_for_impl();
    let cap_impl_generics_ts: proc_macro2::TokenStream = quote! { #cap_impl_generics };
    let cap_where_clause_ts: proc_macro2::TokenStream = quote! { #cap_where_clause };
    let cap_type_param = fn_generics
        .params
        .iter()
        .find_map(|p| match p {
            syn::GenericParam::Type(ty) => Some(ty.ident.clone()),
            _ => None,
        })
        .unwrap_or_else(|| syn::Ident::new("T", Span::call_site()));

    let capability_helper = registration::capability_helper(
        capability_attr.as_ref(),
        inputs_vec.len(),
        &cap_impl_generics_ts,
        &runtime_crate,
        &cap_where_clause_ts,
        &cap_type_param,
        &inner_fn_ident,
    );

    let node_install_impl = registration::node_install_impl(registration::NodeInstallInputs {
        has_generics,
        capability_attr: capability_attr.is_some(),
        is_graph_node,
        runtime_crate: &runtime_crate,
        struct_ident: &struct_ident,
        registry_crate: &registry_crate,
        graph_register_tokens: &graph_register_tokens,
    });

    if is_graph_node {
        let port_ty: syn::Type = syn::parse2(quote! { #runtime_crate::handles::PortHandle })
            .expect("failed to build PortHandle type");
        for arg in input.sig.inputs.iter_mut() {
            if let syn::FnArg::Typed(pat) = arg
                && let syn::Pat::Ident(id) = &*pat.pat
            {
                if let Some(ctx) = &graph_ctx_arg
                    && id.ident == ctx.ident
                {
                    continue;
                }
                *pat.ty = port_ty.clone();
            }
        }
        let output_len = output_names.len();
        input.sig.output = if output_len == 0 {
            syn::ReturnType::Default
        } else if output_len == 1 {
            syn::ReturnType::Type(Default::default(), Box::new(port_ty.clone()))
        } else {
            let tuple_elems: Vec<syn::Type> = (0..output_len).map(|_| port_ty.clone()).collect();
            let tuple_ty: syn::Type =
                syn::parse2(quote! { (#(#tuple_elems),*) }).expect("failed to build tuple type");
            syn::ReturnType::Type(Default::default(), Box::new(tuple_ty))
        };
    }

    TokenStream::from(final_tokens::render_final(final_tokens::FinalTokens {
        input,
        struct_ident,
        id,
        node_decl_fn,
        boundary_contracts_fn,
        handler_registry_fn,
        register_fn,
        capability_helper,
        node_install_impl,
        inputs_ident,
        handle_input_idents,
        runtime_crate,
        handle_input_names,
        outputs_ident,
        output_idents,
        output_names,
        handle_ident,
    }))
}
