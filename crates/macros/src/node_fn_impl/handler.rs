use proc_macro2::{Span, TokenStream};
use quote::quote;
use syn::{ItemFn, LitStr};

use crate::helpers::compile_error;

use super::handler_fetch;
use super::parse::PortMeta;
use super::shader;
use super::type_analysis::{arc_inner_type, is_unit_type, ok_type_from_return, payload_inner_type};

pub(super) struct GraphCtxArg {
    pub(super) ident: syn::Ident,
    pub(super) is_mut_ref: bool,
}

pub(super) struct HandlerInputs<'a> {
    pub(super) is_low_level: bool,
    pub(super) input: &'a ItemFn,
    pub(super) inputs_vec: &'a [PortMeta],
    pub(super) outputs_len: usize,
    pub(super) output_names: &'a [LitStr],
    pub(super) output_idents: &'a [syn::Ident],
    pub(super) config_types: &'a [syn::Type],
    pub(super) shader_path: Option<&'a LitStr>,
    pub(super) shader_paths: &'a [LitStr],
    pub(super) shader_entry: &'a LitStr,
    pub(super) shader_workgroup: Option<[u32; 3]>,
    pub(super) shader_bindings: &'a [TokenStream],
    pub(super) shader_specs: &'a [(TokenStream, Option<LitStr>)],
    pub(super) state_ty_attr: Option<&'a syn::Type>,
    pub(super) capability_attr: Option<&'a LitStr>,
    pub(super) inner_fn_ident: &'a syn::Ident,
    pub(super) data_crate: &'a TokenStream,
    pub(super) runtime_crate: &'a TokenStream,
    pub(super) gpu_crate: &'a TokenStream,
}

pub(super) struct HandlerBuild {
    pub(super) handler_body: TokenStream,
    pub(super) effective_inputs_for_args: Vec<PortMeta>,
    pub(super) arg_types: Vec<syn::Type>,
    pub(super) arg_idents: Vec<syn::Ident>,
    pub(super) arg_mut_bindings: Vec<bool>,
    pub(super) graph_ctx_arg: Option<GraphCtxArg>,
    pub(super) runtime_node_present: bool,
    pub(super) exec_ctx_present: bool,
    pub(super) node_io_present: bool,
    pub(super) shader_ctx_present: bool,
}

pub(super) fn build_handler(inputs: HandlerInputs<'_>) -> Result<HandlerBuild, TokenStream> {
    let HandlerInputs {
        is_low_level,
        input,
        inputs_vec,
        outputs_len,
        output_names,
        output_idents,
        config_types,
        shader_path,
        shader_paths,
        shader_entry,
        shader_workgroup,
        shader_bindings,
        shader_specs,
        state_ty_attr,
        capability_attr,
        inner_fn_ident,
        data_crate,
        runtime_crate,
        gpu_crate,
    } = inputs;
    // Captured so we can reuse when building descriptors.
    let mut arg_types: Vec<syn::Type> = Vec::new();
    let mut arg_idents: Vec<syn::Ident> = Vec::new();
    let mut arg_names: Vec<LitStr> = Vec::new();
    let mut arg_mut_bindings: Vec<bool> = Vec::new();
    let mut effective_inputs_for_args: Vec<PortMeta> = Vec::new();

    let mut graph_ctx_arg: Option<GraphCtxArg> = None;
    let mut runtime_node_present = false;
    let mut exec_ctx_present = false;
    let mut node_io_present = false;
    let mut shader_ctx_present = false;

    let handler_body = if is_low_level {
        quote! { #inner_fn_ident(node, ctx, io) }
    } else {
        struct ConfigArg {
            ident: syn::Ident,
            ty: syn::Type,
            is_ref: bool,
            is_mut: bool,
        }
        let mut config_args: Vec<ConfigArg> = Vec::new();
        let config_type_keys: Vec<String> = config_types
            .iter()
            .map(|ty| {
                let mut raw = quote! { #ty }.to_string();
                raw.retain(|c| !c.is_whitespace());
                raw
            })
            .collect();
        let has_shader_metadata =
            shader_path.is_some() || !shader_specs.is_empty() || !shader_paths.is_empty();
        let mut shader_ctx_ident: Option<syn::Ident> = None;
        let mut runtime_node_ident: Option<syn::Ident> = None;
        let mut exec_ctx_ident: Option<syn::Ident> = None;
        let mut node_io_ident: Option<syn::Ident> = None;
        let state_ty = state_ty_attr.cloned();
        let mut state_param: Option<syn::Ident> = None;
        for arg in &input.sig.inputs {
            if let syn::FnArg::Typed(pat) = arg
                && let syn::Pat::Ident(id) = &*pat.pat
            {
                let last_ident = match &*pat.ty {
                    syn::Type::Path(tp) => tp.path.segments.last().map(|s| s.ident.to_string()),
                    syn::Type::Reference(r) => {
                        if let syn::Type::Path(tp) = &*r.elem {
                            tp.path.segments.last().map(|s| s.ident.to_string())
                        } else {
                            None
                        }
                    }
                    _ => None,
                };
                match last_ident.as_deref() {
                    Some("GraphCtx") => {
                        let is_mut_ref = matches!(
                            &*pat.ty,
                            syn::Type::Reference(r) if r.mutability.is_some()
                        );
                        graph_ctx_arg = Some(GraphCtxArg {
                            ident: id.ident.clone(),
                            is_mut_ref,
                        });
                        continue;
                    }
                    Some("ShaderContext") => {
                        // Many node crates gate ShaderContext behind `#[cfg(feature = "gpu")]`.
                        // In non-GPU builds, the cfg removes the parameter after macro
                        // expansion; avoid treating it as required unless shader metadata is
                        // present on this node.
                        let cfg_gated = pat.attrs.iter().any(|a| a.path().is_ident("cfg"));
                        if has_shader_metadata || !cfg_gated {
                            shader_ctx_present = true;
                            shader_ctx_ident = Some(id.ident.clone());
                        }
                        continue;
                    }
                    Some("RuntimeNode") => {
                        runtime_node_present = true;
                        runtime_node_ident = Some(id.ident.clone());
                        continue;
                    }
                    Some("ExecutionContext") => {
                        exec_ctx_present = true;
                        exec_ctx_ident = Some(id.ident.clone());
                        continue;
                    }
                    Some("NodeIo") => {
                        node_io_present = true;
                        node_io_ident = Some(id.ident.clone());
                        continue;
                    }
                    _ => {}
                }
                if matches!(
                    last_ident.as_deref(),
                    Some(
                        "NodeIo"
                            | "RuntimeNode"
                            | "ExecutionContext"
                            | "ShaderContext"
                            | "GraphCtx"
                    )
                ) {
                    continue;
                }
                // State parameter detection: match type or &/&mut of type.
                let is_state = if let Some(sty) = &state_ty {
                    let ty_str = quote! { #sty }.to_string();
                    let match_ty = match &*pat.ty {
                        syn::Type::Path(tp2) => quote! { #tp2 }.to_string() == ty_str,
                        syn::Type::Reference(r) => quote! { #r.elem }.to_string() == ty_str,
                        _ => false,
                    };
                    match_ty || id.ident == "state"
                } else {
                    false
                };
                if is_state {
                    state_param = Some(id.ident.clone());
                    continue;
                }
                if !config_type_keys.is_empty() {
                    let mut matched_config = None;
                    let mut is_ref = false;
                    let mut is_mut = false;
                    match &*pat.ty {
                        syn::Type::Path(tp) => {
                            let mut raw = quote! { #tp }.to_string();
                            raw.retain(|c| !c.is_whitespace());
                            if config_type_keys.iter().any(|k| k == &raw) {
                                matched_config = Some((*pat.ty).clone());
                            }
                        }
                        syn::Type::Reference(r) => {
                            if let syn::Type::Path(tp) = &*r.elem {
                                let mut raw = quote! { #tp }.to_string();
                                raw.retain(|c| !c.is_whitespace());
                                if config_type_keys.iter().any(|k| k == &raw) {
                                    matched_config = Some((*r.elem).clone());
                                    is_ref = true;
                                    is_mut = r.mutability.is_some();
                                }
                            }
                        }
                        _ => {}
                    }
                    if let Some(cfg_ty) = matched_config {
                        config_args.push(ConfigArg {
                            ident: id.ident.clone(),
                            ty: cfg_ty,
                            is_ref,
                            is_mut,
                        });
                        continue;
                    }
                }
                arg_idents.push(id.ident.clone());
                arg_names.push(LitStr::new(&id.ident.to_string(), Span::call_site()));
                arg_types.push((*pat.ty).clone());
                arg_mut_bindings.push(id.mutability.is_some());
            }
        }
        if state_ty.is_some() && state_param.is_none() {
            return Err(compile_error(
                "state(...) specified but no matching state parameter found in signature".into(),
            ));
        }

        if shader_ctx_ident.is_some() && !has_shader_metadata {
            return Err(compile_error(
                "ShaderContext parameter requires shader metadata (missing shaders(...))".into(),
            ));
        }

        let is_fanin_ty = |ty: &syn::Type| -> bool {
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
        };

        // Determine the effective port metadata for each typed argument.
        //
        // Rules:
        // - If `inputs(...)` matches all typed args, use it (including FanIn prefixes).
        // - If the node has any `FanIn` params and `inputs(...)` is provided, it must match all
        //   typed args (to avoid confusing mixed naming).
        // - Otherwise, ignore `inputs(...)` for port naming and use parameter names.
        let fanin_mask: Vec<bool> = arg_types.iter().map(is_fanin_ty).collect();
        let has_fanin = fanin_mask.iter().any(|b| *b);
        effective_inputs_for_args = if inputs_vec.is_empty() {
            arg_names.iter().cloned().map(PortMeta::name_only).collect()
        } else if inputs_vec.len() == arg_types.len() {
            inputs_vec.to_vec()
        } else if has_fanin {
            return Err(compile_error(
                "FanIn params require inputs(...) entries for all typed args (include the FanIn prefix).".into(),
            ));
        } else {
            arg_names.iter().cloned().map(PortMeta::name_only).collect()
        };

        let mut call_args: Vec<proc_macro2::TokenStream> = Vec::new();
        for arg in &input.sig.inputs {
            if let syn::FnArg::Typed(pat) = arg {
                let is_shader_ctx_param = match &*pat.ty {
                    syn::Type::Path(tp) => tp
                        .path
                        .segments
                        .last()
                        .map(|s| s.ident == "ShaderContext")
                        .unwrap_or(false),
                    syn::Type::Reference(r) => match &*r.elem {
                        syn::Type::Path(tp) => tp
                            .path
                            .segments
                            .last()
                            .map(|s| s.ident == "ShaderContext")
                            .unwrap_or(false),
                        _ => false,
                    },
                    _ => false,
                };
                if is_shader_ctx_param && shader_ctx_ident.is_none() {
                    continue;
                }
                if let syn::Pat::Ident(id) = &*pat.pat {
                    let ident = &id.ident;
                    if let Some(n) = &runtime_node_ident
                        && ident == n
                    {
                        call_args.push(quote! { node });
                        continue;
                    }
                    if let Some(c) = &exec_ctx_ident
                        && ident == c
                    {
                        call_args.push(quote! { ctx });
                        continue;
                    }
                    if let Some(ioid) = &node_io_ident
                        && ident == ioid
                    {
                        call_args.push(quote! { io });
                        continue;
                    }
                    if let Some(ctx) = &shader_ctx_ident
                        && ident == ctx
                    {
                        call_args.push(quote! { __shader_ctx });
                        continue;
                    }
                    if let Some(st) = &state_param
                        && ident == st
                    {
                        call_args.push(quote! { #ident });
                        continue;
                    }
                    call_args.push(quote! { #ident });
                }
            }
        }

        let shader_tokens = shader::shader_tokens(
            shader_specs,
            shader_path,
            shader_entry,
            shader_workgroup,
            shader_bindings,
            shader_paths,
            gpu_crate,
        );

        let call = quote! { #inner_fn_ident(#(#call_args),*) };
        let out_port = output_names
            .first()
            .cloned()
            .unwrap_or_else(|| LitStr::new("out", Span::call_site()));

        let port_names: Vec<LitStr> = effective_inputs_for_args
            .iter()
            .map(|p| p.name.clone())
            .collect();

        let ret_handling = if node_io_ident.is_some() {
            let ok_ty = ok_type_from_return(&input.sig.output);
            if outputs_len > 0 && ok_ty.is_some_and(|t| !is_unit_type(t)) {
                quote! {
                    compile_error!("nodes that take `NodeIo` must return `()` (push outputs via `io.push_*`) when outputs(...) are declared");
                    Ok(())
                }
            } else if matches!(input.sig.output, syn::ReturnType::Default) {
                quote! { #call; Ok(()) }
            } else {
                quote! {
                    match #call {
                        Ok(_) => Ok(()),
                        Err(e) => Err(e),
                    }
                }
            }
        } else if !matches!(input.sig.output, syn::ReturnType::Default) {
            let ok_ty = ok_type_from_return(&input.sig.output);
            if outputs_len > 1 {
                let out_ports = output_names;
                let out_idents = output_idents;
                let out_push_stmts: Vec<proc_macro2::TokenStream> = match ok_ty {
                    Some(syn::Type::Tuple(tuple)) => tuple
                        .elems
                        .iter()
                        .zip(out_ports.iter())
                        .zip(out_idents.iter())
                        .map(|((elem_ty, port), ident)| {
                            if let Some(inner) = payload_inner_type(elem_ty) {
                                quote! { io.push_compute::<#inner>(Some(#port), #ident); }
                            } else if let Some(inner) = arc_inner_type(elem_ty) {
                                quote! {
                                    {
                                        let __ty = #data_crate::typing::type_expr::<#inner>();
                                        let __key = #runtime_crate::transport::typeexpr_transport_key(&__ty)
                                            .map_err(|err| #runtime_crate::NodeError::Handler(err.to_string()))?;
                                        io.push_arc_as(Some(#port), __key, #ident);
                                    }
                                }
                            } else {
                                quote! {
                                    {
                                        let __ty = #data_crate::typing::type_expr::<#elem_ty>();
                                        let __key = #runtime_crate::transport::typeexpr_transport_key(&__ty)
                                            .map_err(|err| #runtime_crate::NodeError::Handler(err.to_string()))?;
                                        io.push_as(Some(#port), __key, #ident);
                                    }
                                }
                            }
                        })
                        .collect(),
                    _ => out_ports
                        .iter()
                        .zip(out_idents.iter())
                        .map(|(port, ident)| quote! {
                            {
                                let __ty = #data_crate::model::TypeExpr::Opaque(::std::string::String::from("rust:unknown"));
                                let __key = #runtime_crate::transport::typeexpr_transport_key(&__ty)
                                    .map_err(|err| #runtime_crate::NodeError::Handler(err.to_string()))?;
                                io.push_as(Some(#port), __key, #ident);
                            }
                        })
                        .collect(),
                };
                quote! {
                    match #call {
                        Ok(val) => {
                            let (#(#out_idents),*) = val;
                            #(#out_push_stmts)*
                            Ok(())
                        }
                        Err(e) => Err(e),
                    }
                }
            } else {
                let push_stmt = ok_ty
                    .as_ref()
                    .and_then(|ty| payload_inner_type(ty))
                    .map(|inner| {
                        quote! { io.push_compute::<#inner>(Some(#out_port), val); }
                    })
                    .unwrap_or_else(|| {
                        if let Some(ok_ty) = ok_ty.as_ref() {
                            if let Some(inner) = arc_inner_type(ok_ty) {
                                quote! {
                                    {
                                        let __ty = #data_crate::typing::type_expr::<#inner>();
                                        let __key = #runtime_crate::transport::typeexpr_transport_key(&__ty)
                                            .map_err(|err| #runtime_crate::NodeError::Handler(err.to_string()))?;
                                        io.push_arc_as(Some(#out_port), __key, val);
                                    }
                                }
                            } else {
                                quote! {
                                    {
                                        let __ty = #data_crate::typing::type_expr::<#ok_ty>();
                                        let __key = #runtime_crate::transport::typeexpr_transport_key(&__ty)
                                            .map_err(|err| #runtime_crate::NodeError::Handler(err.to_string()))?;
                                        io.push_as(Some(#out_port), __key, val);
                                    }
                                }
                            }
                        } else {
                            quote! {
                                {
                                    let __ty = #data_crate::model::TypeExpr::Opaque(::std::string::String::from("rust:unknown"));
                                    let __key = #runtime_crate::transport::typeexpr_transport_key(&__ty)
                                        .map_err(|err| #runtime_crate::NodeError::Handler(err.to_string()))?;
                                    io.push_as(Some(#out_port), __key, val);
                                }
                            }
                        }
                    });
                quote! {
                    match #call {
                        Ok(val) => {
                            #push_stmt
                            Ok(())
                        }
                        Err(e) => Err(e),
                    }
                }
            }
        } else {
            quote! { #call; Ok(()) }
        };

        let state_binding = if let (Some(sty), Some(id)) = (state_ty.clone(), state_param.clone()) {
            Some(quote! {
                let __state_key = ::std::format!(
                    "macro_state:{}:{}",
                    ctx.node_id,
                    ::std::any::type_name::<#sty>()
                );
                let mut __state_value: #sty = ctx.state
                    .take_native::<#sty>(&__state_key)
                    .map_err(|err| #runtime_crate::NodeError::Handler(err.to_string()))?
                    .unwrap_or_default();
                let #id: &mut #sty = &mut __state_value;
            })
        } else {
            None
        };
        let ret_handling = if state_binding.is_some() {
            quote! {
                let __state_result = { #ret_handling };
                ctx.state
                    .set_native(&__state_key, __state_value)
                    .map_err(|err| #runtime_crate::NodeError::Handler(err.to_string()))?;
                __state_result
            }
        } else {
            ret_handling
        };

        if let Some(cap_str) = capability_attr.cloned() {
            let cap_lit = cap_str;
            let port_idents: Vec<LitStr> = port_names.clone();
            quote! {
                let mut args_any: Vec<&dyn ::std::any::Any> = Vec::new();
                #(args_any.push(
                    io.payload_raw(#port_idents)
                        .ok_or_else(|| #runtime_crate::NodeError::InvalidInput(format!("missing {}", #port_idents)))?
                );)*
                {
                    let entries = ctx.capabilities
                        .get(#cap_lit)
                        .ok_or_else(|| #runtime_crate::NodeError::InvalidInput("missing capability entries".into()))?;
                    let mut dispatched = false;
                    for entry in entries {
                        if args_any.len() == entry.type_ids.len()
                            && args_any
                                .iter()
                                .zip(entry.type_ids.iter())
                                .all(|(a, tid)| a.type_id() == *tid)
                        {
                            let out = (entry.func)(&args_any)?;
                            io.push_payload(#out_port, out);
                            dispatched = true;
                            break;
                        }
                    }
                    if !dispatched {
                        return Err(#runtime_crate::NodeError::InvalidInput("unsupported capability type".into()));
                    }
                    Ok(())
                }
            }
        } else {
            let mut config_fetch_stmts: Vec<proc_macro2::TokenStream> = Vec::new();
            for (idx, cfg) in config_args.iter().enumerate() {
                let ident = &cfg.ident;
                let ty = &cfg.ty;
                let owned_ident = syn::Ident::new(&format!("__cfg_owned_{idx}"), Span::call_site());
                let sanitized_ident =
                    syn::Ident::new(&format!("__cfg_sanitized_{idx}"), Span::call_site());
                let value_ident = syn::Ident::new(&format!("__cfg_value_{idx}"), Span::call_site());
                let assign = if cfg.is_ref {
                    if cfg.is_mut {
                        quote! { let #ident = &mut #value_ident; }
                    } else {
                        quote! { let #ident = &#value_ident; }
                    }
                } else {
                    quote! { let #ident = #value_ident; }
                };
                config_fetch_stmts.push(quote! {
                    let #owned_ident = <#ty as #runtime_crate::config::NodeConfig>::from_io(io)?;
                    let #sanitized_ident = <#ty as #runtime_crate::config::NodeConfig>::sanitize(#owned_ident)
                        .map_err(|e| #runtime_crate::NodeError::InvalidInput(e.to_string()))?;
                    if !#sanitized_ident.changes.is_empty() {
                        #runtime_crate::config::log_config_changes(&node.id, &#sanitized_ident.changes);
                    }
                    let mut #value_ident = #sanitized_ident.value;
                    <#ty as #runtime_crate::config::NodeConfig>::validate(&#value_ident)
                        .map_err(|e| #runtime_crate::NodeError::InvalidInput(e.to_string()))?;
                    #assign
                });
            }
            let (arg_fetch_mut_stmts, arg_fetch_ref_stmts) =
                handler_fetch::input_fetch_stmts(handler_fetch::FetchInputs {
                    arg_idents: &arg_idents,
                    arg_types: &arg_types,
                    arg_mut_bindings: &arg_mut_bindings,
                    port_names: &port_names,
                    runtime_crate,
                });

            let shader_gpu_init = if shader_tokens.is_some() {
                quote! { let __ctx_gpu: Option<#gpu_crate::GpuContextHandle> = ctx.gpu.clone(); }
            } else {
                quote! {}
            };

            quote! {
                #(#config_fetch_stmts)*
                #(#arg_fetch_mut_stmts)*
                #(#arg_fetch_ref_stmts)*
                #shader_gpu_init
                #state_binding
                #shader_tokens
                #ret_handling
            }
        }
    };

    Ok(HandlerBuild {
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
    })
}
